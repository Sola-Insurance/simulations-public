import argparse
import copy
import functools
import logging
import multiprocessing
import os
import random
import tenacity
import time
import json

from db import mongo
from db.v2 import geo_types
from functions import simulation_logging, simulation_output
from geojson import Feature, MultiPolygon, Point, FeatureCollection
from random import randint
from storm import storm
from tqdm import tqdm
from typing import List, Union
from turfpy.measurement import points_within_polygon


"""Generate Losses, running multiple simulations of storms across states, outputting the results.

This script runs simulations of randomly generated storms, measuring the dollar impact of each storm against
samples of properties. Each simulation:
  1. Iterates through a fixed list of states, for each state:
  2. Selects a group of properties for the state. Each simulation has a pre-chosen sample of properties available. It
      then chooses a random group of them.
  3. Generates a storm for the state
  4. Tallies storm impact: total premiums, total exposure, loss from storm ratio of loss/premium
  5. Outputs totals

The script is setup to run multi-process, running N simulations across P processes. 

More info: 
https://docs.google.com/document/d/1T1ILFRcgrEmGT6QCx6OmqEyT0efc7FC3tUWM_aPD0PU/edit#

Currently produces, four outputs for state, county and zip-code, all in USD:
* Premiums: Total premiums from the randomly selected properties
* Exposures: Total potential damage that could hit the randomly selected properties
* Losses: Total loss (against fixed damage per size of storm) for properties hit by random storm
* NLR: Net Loss Ratio of total loss / ratio.

Currently, each simulation relies heavily on MongoDB. Each simulation's sample properties have been pre-generated
and stored in a DB collection. During the run, the simulation stores its randomly selected properties from the sample
into a temporary collection. It then uses MongoDB's geospatial queries to determine which properties are hit by a storm.
The temp collection is cleared at the end of the simulation. Wach simulation outputs 1x row per output, with
columns representing the calculated total in each state, county, zip.

Finally, the script supports two different modes, running single-threaded and with multiple processes. See the
--num_processes flag.

TODO: # Columns is going to be a problem, new schema needed.
TODO: Run performance tests to see if turfpy really is slower than MongoDB geospatial.

Usage:
```
cmd=(python functions/generate_losses.py)

# See all the available flags
$cmd -h

# Run simulations, default N=1000 sims, multiprocessing on by default, outputting to local CSV. Logging to stderr.
$cmd --output_csv

# Run 100 simulations, multiprocessing (on by default), outputting CSV files and overwrite them if they exist.
# Log to a file instead of stderr
$cmd -n 100 --output_csv --csv_overwrite --no-log_stderr --log_file sim.log

# Run simulations, uploading the resulting CSV to bigquery OVERWRITING the tables.
$cmd --output_csv --upload_csv_to_bigquery_overwrite  --project_id <gcp_project_id> 

# Run 100 simulations writing both CSV and streaming to bigquery, APPENDING the tables.
$cmd -n 100 --output_csv --output_bigquery --project_id <gcp_project_id> 

# Run 10 simulations single threaded, writing local CSV.
$cmd -n 10 --no-multiprocess --output_csv

# Run 10 simulations single threaded, for Tennessee and Georgia, no outputs.
$cmd -n 10 --no-multiprocess --states TN GA
"""


# Default number of simulations to run, split in parallel across a number of processes.
DEFAULT_NUM_SIMULATIONS = 1000
DEFAULT_SIMULATIONS_CSV_OUTPUT_DIR = f'files/output'
DEFAULT_LOG_FORMAT = '[%(levelname)s] %(message)s'
DATED_LOG_FORMAT = '%(asctime)s|' + DEFAULT_LOG_FORMAT
SIMULATION_MAX_RETRIES = 3

FLAT_ROW_OUTPUT_SCHEMA_VERSION = 1
GEO_TYPE_ROW_OUTPUT_SCHEMA_VERSION = 2
DEFAULT_OUTPUT_SCHEMA_VERSION = GEO_TYPE_ROW_OUTPUT_SCHEMA_VERSION


# These are the current states that we're simulating policyholders and storms in
STATES = ['AR', 'GA', 'IA', 'IL', 'IN', 'KY', 'MO', 'OH', 'TN']

# Aggregation limits for each zipcode, state, county, etc.
total_agg_limit = 200000000
state_agg_limit = 80000000
county_agg_limit = 30000000
zip_agg_limit = 5000000


"""
--------------
Assumptions
--------------
"""


zip_to_prem_file = open('../lib/premium/zip_to_prem.json', 'r')
zip_to_prem = json.load(zip_to_prem_file)


def generate_loss_ratio(premium, loss):
    if premium != 0:
        return round(loss / premium, 4)
    return 0


@tenacity.retry(
    stop=tenacity.stop_after_attempt(SIMULATION_MAX_RETRIES),
    wait=tenacity.wait_random_exponential(multiplier=1, max=60)
)
def run_losses(sim_id: int,
               runtime_timestamp: int,
               outputs: simulation_output.OutputFanout,
               output_schema_version: int = None,
               states: List[str] = None,
               label: str = None,
               logging_queue: multiprocessing.Queue = None,
               log_level: Union[int, str] = None):
    """
    Overlays synthetic storms over randomized exposures (geojson points)
    :param sim_id: id of simulation
    :param runtime_timestamp: Timestamp when all simulations were started.
    :param outputs: OutputFanout that manages the output destinations (like CSV, Bigquery) and when the output is
        written. In single-threaded runs, the outputs may be written immediately. In multiprocess runs, it may queue
        output data for a background process.
    :param output_schema_version: Version id of the flavor of output to write. This is needed because
        we call outputs.send(...) at different points in code based on the flavor of the output.
    :param states: List of two-letter abbreviation of states to simulate storms in. Currently, fairly hardcoded to only
        the supported states we have property data; and, currently, still outputs ALL columns. By default, runs all
        states.
    :param label: Optional string to help disambiguate a set of simulations from each other.
    :param logging_queue: Queue to wrap in a logging QueueHandler to synchronize logging between processes.
    :param log_level: Logging level to use in the sub-process' logging.
    :return: id of simulation
    """

    def build_geo_type_row(geo_type_str: str, geography: str, total: float) -> dict:
        """Internal utility function for building v2 geo-type rows."""
        return dict(
            run_timestamp=runtime_timestamp,
            sim_id=sim_id,
            geo_type=geo_types.str_to_int(geo_type_str),
            geography=geography,
            total=total
        )

    logger = simulation_logging.get_logger(sim_id, logging_queue, log_level=log_level)
    # simulation_db_key = f'{label if label else ""}{"-" if label else ""}sample houses select{sim_id}'
    runtime_timestamp = runtime_timestamp or int(time.time())
    output_schema_version = output_schema_version or DEFAULT_OUTPUT_SCHEMA_VERSION
    states = states or STATES

    logger.debug(f'Starting simulation #{sim_id} on {states}')

    # Connect to the MongoDB.
    # Note: This originally resided in the global namespace and so it /seemed/ like all simulations were sharing the
    # same DB connection. However, in python multiprocessing, the global namespace is reinitialized for each spawned
    # process. So, for N simulations we get N db connections. It's clearer to manage the connection directly in the
    # function.
    db = mongo.get_database()

    storms = []
    loss_dictionary = {"total": 0,
                       "state": {"AR": 0, "TX": 0, "GA": 0, "IA": 0, "IL": 0, "NE": 0, "IN": 0, "KY": 0, "MO": 0,
                                 "OH": 0, "TN": 0},
                       "county": {"Stone": 0, "Madison": 0, "Newton": 0, "Carroll": 0, "Saline": 0, "Monroe": 0,
                                  "Dallas": 0, "Washington": 0, "Benton": 0, "Mississippi": 0, "Garland": 0,
                                  "Sebastian": 0, "Columbia": 0, "Faulkner": 0, "Conway": 0, "Perry": 0, "Pope": 0,
                                  "Craighead": 0, "Sevier": 0, "Boone": 0, "Crittenden": 0, "Cross": 0, "Miller": 0,
                                  "Bowie": 0, "Phillips": 0, "Greene": 0, "Catoosa": 0, "Pierce": 0, "Ware": 0,
                                  "Lanier": 0, "Coweta": 0, "Heard": 0, "Troup": 0, "Decatur": 0, "White": 0, "Hall": 0,
                                  "Cherokee": 0, "Henry": 0, "Walton": 0, "Gwinnett": 0, "Wayne": 0, "DeKalb": 0,
                                  "Houston": 0, "Crawford": 0, "Fulton": 0, "Chatham": 0, "Effingham": 0, "Laurens": 0,
                                  "Dodge": 0, "Oconee": 0, "Cobb": 0, "Morgan": 0, "Allamakee": 0, "Linn": 0, "Polk": 0,
                                  "Lucas": 0, "Emmet": 0, "Hamilton": 0, "Jasper": 0, "Des Moines": 0, "Dubuque": 0,
                                  "Jo Daviess": 0, "Scott": 0, "Wapello": 0, "Woodbury": 0, "Dakota": 0, "Iowa": 0,
                                  "Johnson": 0, "O'Brien": 0, "Sioux": 0, "Osceola": 0, "Humboldt": 0, "Kossuth": 0,
                                  "Pocahontas": 0, "Marion": 0, "Bremer": 0, "Black Hawk": 0, "Story": 0, "Cook": 0,
                                  "Will": 0, "Randolph": 0, "Williamson": 0, "Lake": 0, "Peoria": 0, "DuPage": 0,
                                  "Kane": 0, "Tazewell": 0, "LaSalle": 0, "Pike": 0, "Mason": 0, "Shelby": 0,
                                  "McHenry": 0, "St. Clair": 0, "Macoupin": 0, "Macon": 0, "De Witt": 0, "Logan": 0,
                                  "Jackson": 0, "Floyd": 0, "Clark": 0, "Whitley": 0, "Kosciusko": 0, "Wabash": 0,
                                  "Allen": 0, "Sullivan": 0, "Vigo": 0, "Vanderburgh": 0, "Jay": 0, "Hendricks": 0,
                                  "Owen": 0, "LaPorte": 0, "Clay": 0, "Jefferson": 0, "Grayson": 0, "Estill": 0,
                                  "Powell": 0, "Metcalfe": 0, "Trigg": 0, "Christian": 0, "Fayette": 0, "Jessamine": 0,
                                  "Kenton": 0, "Hopkins": 0, "Pulaski": 0, "Henderson": 0, "Harrison": 0, "Bourbon": 0,
                                  "Laurel": 0, "Muhlenberg": 0, "Daviess": 0, "Bullitt": 0, "Meade": 0,
                                  "Breckinridge": 0, "Clinton": 0, "Dent": 0, "Schuyler": 0, "Davis": 0,
                                  "St. Louis City": 0, "St. Louis": 0, "Barry": 0, "Reynolds": 0, "Cole": 0,
                                  "St. Charles": 0, "Calhoun": 0, "Stoddard": 0, "Howell": 0, "Lewis": 0, "Platte": 0,
                                  "Butler": 0, "Geauga": 0, "Cuyahoga": 0, "Wood": 0, "Hancock": 0, "Delaware": 0,
                                  "Stark": 0, "Muskingum": 0, "Hocking": 0, "Ross": 0, "Trumbull": 0, "Portage": 0,
                                  "Clermont": 0, "Mahoning": 0, "Licking": 0, "Scioto": 0, "Greenup": 0, "Brown": 0,
                                  "Ottawa": 0, "Sandusky": 0, "Sumner": 0, "Simpson": 0, "Crockett": 0, "Davidson": 0,
                                  "Knox": 0, "Blount": 0, "Wilson": 0, "Roane": 0, "Gibson": 0, "Warren": 0,
                                  "Claiborne": 0, "Walker": 0, "Chester": 0, "Bedford": 0, "Van Buren": 0,
                                  "Lauderdale": 0, "Montgomery": 0, "Hamblen": 0, "Overton": 0},
                       "zip": {"72560": 0, "72051": 0, "72680": 0, "72742": 0, "72740": 0, "72670": 0, "72616": 0,
                               "72002": 0, "72022": 0, "72011": 0, "72019": 0, "72021": 0, "72015": 0, "71742": 0,
                               "72764": 0, "72745": 0, "72762": 0, "72442": 0, "72438": 0, "72751": 0, "72756": 0,
                               "72732": 0, "72712": 0, "72087": 0, "71909": 0, "71901": 0, "72903": 0, "72904": 0,
                               "72901": 0, "71753": 0, "71861": 0, "72032": 0, "72058": 0, "72034": 0, "72181": 0,
                               "72127": 0, "72016": 0, "72107": 0, "72823": 0, "72802": 0, "72858": 0, "72404": 0,
                               "72401": 0, "71832": 0, "71842": 0, "72601": 0, "72327": 0, "72331": 0, "72376": 0,
                               "72373": 0, "71854": 0, "75503": 0, "75501": 0, "72419": 0, "72342": 0, "72390": 0,
                               "72355": 0, "72450": 0, "72443": 0, "30736": 0, "31516": 0, "31503": 0, "31501": 0,
                               "31635": 0, "30263": 0, "30217": 0, "30230": 0, "39817": 0, "30528": 0, "30527": 0,
                               "30116": 0, "30117": 0, "30108": 0, "30118": 0, "30102": 0, "30252": 0, "30248": 0,
                               "30253": 0, "30052": 0, "30078": 0, "31545": 0, "31555": 0, "30038": 0, "30058": 0,
                               "30035": 0, "30034": 0, "30037": 0, "31069": 0, "31016": 0, "31078": 0, "30310": 0,
                               "30314": 0, "30311": 0, "30344": 0, "30318": 0, "30331": 0, "31322": 0, "31302": 0,
                               "31419": 0, "31318": 0, "31075": 0, "31009": 0, "31019": 0, "31023": 0, "30677": 0,
                               "30621": 0, "30066": 0, "30060": 0, "30144": 0, "30064": 0, "30663": 0, "30025": 0,
                               "30055": 0, "30655": 0, "30014": 0, "30346": 0, "30338": 0, "30319": 0, "30328": 0,
                               "30342": 0, "30327": 0, "30068": 0, "52172": 0, "52336": 0, "52302": 0, "52314": 0,
                               "52403": 0, "52202": 0, "52402": 0, "52328": 0, "52233": 0, "50317": 0, "50316": 0,
                               "50313": 0, "50320": 0, "50319": 0, "50309": 0, "50315": 0, "50314": 0, "50310": 0,
                               "50311": 0, "50312": 0, "50049": 0, "50578": 0, "50514": 0, "50271": 0, "50135": 0,
                               "50208": 0, "52623": 0, "52638": 0, "52001": 0, "52003": 0, "61025": 0, "52722": 0,
                               "52807": 0, "50322": 0, "50324": 0, "50325": 0, "50265": 0, "50266": 0, "52566": 0,
                               "52553": 0, "52501": 0, "51054": 0, "51052": 0, "51106": 0, "51111": 0, "51105": 0,
                               "68731": 0, "52401": 0, "52404": 0, "52405": 0, "52228": 0, "52351": 0, "52318": 0,
                               "52203": 0, "52338": 0, "51248": 0, "51201": 0, "51231": 0, "51238": 0, "51232": 0,
                               "50570": 0, "50597": 0, "50581": 0, "52803": 0, "52806": 0, "52804": 0, "50150": 0,
                               "50138": 0, "50116": 0, "50057": 0, "50668": 0, "50703": 0, "50622": 0, "50677": 0,
                               "50613": 0, "50647": 0, "50626": 0, "50010": 0, "50014": 0, "60475": 0, "60417": 0,
                               "60411": 0, "60466": 0, "60618": 0, "60647": 0, "60625": 0, "60651": 0, "62244": 0,
                               "62277": 0, "62918": 0, "62921": 0, "62915": 0, "62278": 0, "60047": 0, "60010": 0,
                               "60616": 0, "60607": 0, "60661": 0, "60608": 0, "60642": 0, "60622": 0, "60612": 0,
                               "60609": 0, "60632": 0, "60623": 0, "60624": 0, "61606": 0, "61604": 0, "61614": 0,
                               "61605": 0, "61615": 0, "60502": 0, "60505": 0, "60504": 0, "60506": 0, "60542": 0,
                               "60188": 0, "60185": 0, "61611": 0, "61610": 0, "61602": 0, "61554": 0, "61607": 0,
                               "61364": 0, "60191": 0, "60101": 0, "60106": 0, "60143": 0, "62314": 0, "62312": 0,
                               "62655": 0, "60173": 0, "60193": 0, "60401": 0, "62565": 0, "60148": 0, "60137": 0,
                               "60033": 0, "60152": 0, "61038": 0, "62221": 0, "62220": 0, "62226": 0, "62014": 0,
                               "62685": 0, "62012": 0, "61008": 0, "62024": 0, "62002": 0, "62095": 0, "62035": 0,
                               "61756": 0, "61749": 0, "62543": 0, "62518": 0, "62548": 0, "62573": 0, "62901": 0,
                               "47150": 0, "47172": 0, "46787": 0, "46562": 0, "46962": 0, "46580": 0, "46510": 0,
                               "46764": 0, "46131": 0, "46184": 0, "46227": 0, "46142": 0, "46217": 0, "46845": 0,
                               "46825": 0, "46407": 0, "46402": 0, "46404": 0, "47850": 0, "47866": 0, "47802": 0,
                               "47879": 0, "47714": 0, "47715": 0, "47711": 0, "47716": 0, "47713": 0, "47710": 0,
                               "47708": 0, "47371": 0, "46123": 0, "46231": 0, "46122": 0, "47433": 0, "47460": 0,
                               "46342": 0, "46409": 0, "46410": 0, "46408": 0, "46319": 0, "46375": 0, "46745": 0,
                               "46816": 0, "46819": 0, "46360": 0, "46218": 0, "46201": 0, "46205": 0, "46202": 0,
                               "46203": 0, "46220": 0, "46204": 0, "46208": 0, "46225": 0, "46222": 0, "46228": 0,
                               "47834": 0, "47857": 0, "40217": 0, "40204": 0, "40203": 0, "40208": 0, "40202": 0,
                               "40214": 0, "40209": 0, "40210": 0, "40215": 0, "40216": 0, "40299": 0, "40291": 0,
                               "42754": 0, "40472": 0, "40380": 0, "42129": 0, "42211": 0, "42240": 0, "40517": 0,
                               "40515": 0, "40503": 0, "40356": 0, "40514": 0, "40513": 0, "41094": 0, "41051": 0,
                               "41042": 0, "41091": 0, "42413": 0, "42431": 0, "42501": 0, "42503": 0, "42533": 0,
                               "42553": 0, "42420": 0, "40502": 0, "40508": 0, "40069": 0, "41031": 0, "40361": 0,
                               "41522": 0, "41501": 0, "40737": 0, "40701": 0, "42339": 0, "42303": 0, "42366": 0,
                               "42355": 0, "40047": 0, "40161": 0, "40157": 0, "40171": 0, "40170": 0, "40272": 0,
                               "40258": 0, "63801": 0, "65534": 0, "65583": 0, "65556": 0, "64477": 0, "64492": 0,
                               "64454": 0, "64014": 0, "64015": 0, "63010": 0, "63052": 0, "64865": 0, "64149": 0,
                               "64134": 0, "64030": 0, "64137": 0, "64138": 0, "64131": 0, "64146": 0, "65560": 0,
                               "63536": 0, "52537": 0, "64126": 0, "64125": 0, "64120": 0, "64123": 0, "64127": 0,
                               "64124": 0, "64117": 0, "64116": 0, "64108": 0, "64106": 0, "64105": 0, "65721": 0,
                               "65753": 0, "63012": 0, "63050": 0, "63051": 0, "63016": 0, "63118": 0, "63111": 0,
                               "63116": 0, "62206": 0, "63139": 0, "63125": 0, "63123": 0, "63109": 0, "63110": 0,
                               "62240": 0, "63020": 0, "65745": 0, "63629": 0, "65032": 0, "63026": 0, "63049": 0,
                               "64855": 0, "64755": 0, "64832": 0, "63088": 0, "63021": 0, "63301": 0, "62013": 0,
                               "63960": 0, "63104": 0, "63103": 0, "63106": 0, "63113": 0, "63108": 0, "65775": 0,
                               "64801": 0, "64804": 0, "63440": 0, "63057": 0, "64154": 0, "64151": 0, "64152": 0,
                               "64153": 0, "45011": 0, "45069": 0, "45044": 0, "45015": 0, "45014": 0, "45248": 0,
                               "45002": 0, "45052": 0, "45001": 0, "44023": 0, "44022": 0, "45872": 0, "45889": 0,
                               "43082": 0, "44721": 0, "44641": 0, "44632": 0, "44652": 0, "44705": 0, "44720": 0,
                               "44685": 0, "43551": 0, "43402": 0, "43537": 0, "43566": 0, "43565": 0, "43821": 0,
                               "43842": 0, "43822": 0, "44135": 0, "44111": 0, "44142": 0, "44126": 0, "44116": 0,
                               "44070": 0, "44145": 0, "43135": 0, "45647": 0, "44062": 0, "44491": 0, "44231": 0,
                               "44080": 0, "44021": 0, "45140": 0, "45249": 0, "45242": 0, "45505": 0, "45502": 0,
                               "44138": 0, "44436": 0, "44505": 0, "44405": 0, "44471": 0, "44506": 0, "44502": 0,
                               "44514": 0, "43080": 0, "43031": 0, "43055": 0, "45662": 0, "41175": 0, "41174": 0,
                               "45154": 0, "43302": 0, "43342": 0, "43449": 0, "43442": 0, "43420": 0, "43416": 0,
                               "45858": 0, "45840": 0, "43403": 0, "43451": 0, "37148": 0, "42134": 0, "38001": 0,
                               "38050": 0, "38034": 0, "37215": 0, "37205": 0, "37918": 0, "37938": 0, "37912": 0,
                               "37849": 0, "37754": 0, "37886": 0, "37865": 0, "37066": 0, "37087": 0, "37763": 0,
                               "37748": 0, "38233": 0, "38369": 0, "38330": 0, "37067": 0, "37064": 0, "37069": 0,
                               "37934": 0, "37922": 0, "37932": 0, "37604": 0, "37614": 0, "37110": 0, "37825": 0,
                               "37879": 0, "37870": 0, "37862": 0, "37882": 0, "37421": 0, "37412": 0, "30741": 0,
                               "37411": 0, "": 0, "38340": 0, "37160": 0, "37122": 0, "37138": 0, "37076": 0,
                               "37075": 0, "38585": 0, "38063": 0, "37042": 0, "37620": 0, "37860": 0, "38573": 0,
                               "38570": 0, "38541": 0}}
    exposure_dictionary = copy.deepcopy(loss_dictionary)
    premium_dictionary = copy.deepcopy(loss_dictionary)

    random.shuffle(states)

    exposures = []

    # Generate exposures and check for aggregate limits

    for loss_state in states:
        logger.debug(f'Simulating storms in {loss_state}')

        state_sample_size = randint(1700, 5000)
        logger.debug(f'Randomly choosing ({state_sample_size}) properties in {loss_state}')
        curr_props = db[f"{loss_state} houses random"].aggregate([
            {
                "$match": {
                    "state": loss_state
                }
            },
            {
                "$sample": {
                    "size": state_sample_size
                }
            }
        ])

        for curr_prop in curr_props:
            if (
                exposure_dictionary['total'] > (total_agg_limit - 15000) or
                exposure_dictionary['state'][curr_prop['state']] > (state_agg_limit - 15000)
            ):
                logger.debug(f'Reached exposure ceiling. Stopping after ({len(exposures)}) properties.')
                break

            curr_state = curr_prop['state']
            curr_county = curr_prop['county']
            curr_zip = curr_prop['zip_code']
            curr_limit = curr_prop['limit']

            premium = max(100, zip_to_prem[curr_zip])

            exposure_dictionary['total'] += curr_limit
            exposure_dictionary['state'][curr_state] += curr_limit
            exposure_dictionary['county'][curr_county] += curr_limit
            exposure_dictionary['zip'][curr_zip] += curr_limit
            premium_dictionary['total'] += premium
            premium_dictionary['state'][curr_state] += premium
            premium_dictionary['county'][curr_county] += premium
            premium_dictionary['zip'][curr_zip] += premium

            exposure_props = {
                "state": curr_state,
                "county": curr_county,
                "zip_code": curr_zip
            }

            exposure_point = Point(coordinates=curr_prop['location']['coordinates'])

            exposures.append(Feature(geometry=exposure_point, properties=exposure_props))

        if exposure_dictionary['total'] > (total_agg_limit - 15000):
            logger.debug(f'Reached total exposure ceiling. Stopping states')
            break

    exposure_fc = FeatureCollection(features=exposures)

    # logger.debug(f'Using temporary MongoDb collection: "{simulation_db_key}"')
    # db[simulation_db_key].delete_many({})
    # db[simulation_db_key].insert_many(exposures)

    #
    # TODO: Extract out the output row generation into a separate module.
    # Would be nice to separate out the knowledge of how to build v1 vs v2 rows.
    # Potentially, the static loss_dictionary should go with it.
    #
    logger.info('Writing Exposure and Premiums')
    exposure_row = dict(sim_id=sim_id)
    premium_row = dict(sim_id=sim_id)

    with simulation_output.BufferedOutputStream(outputs, logger=logger) as output_stream:
        for geo_type, value in exposure_dictionary.items():
            if geo_type == 'total':
                exposure_row[geo_type] = value
                premium_row[geo_type] = value
                total = exposure_row[geo_type]
                premium_total = premium_row[geo_type]

                if output_schema_version == GEO_TYPE_ROW_OUTPUT_SCHEMA_VERSION:
                    if total:
                        output_stream.add(simulation_output.OUTPUT_EXPOSURES,
                                          build_geo_type_row(geo_type, geo_type, total))
                    if premium_total:
                        output_stream.add(simulation_output.OUTPUT_EXPOSURES,
                                          build_geo_type_row(geo_type, geo_type, premium_total))
            else:
                # Flatten the geographies all into a single dict, which will be output as a gigantic row.
                for geography, total in value.items():
                    column_name = simulation_output.format_column_name_for_output(geography)
                    premium_total = premium_dictionary[geo_type][geography]
                    exposure_row[column_name] = total
                    premium_row[column_name] = premium_total

                    if output_schema_version == GEO_TYPE_ROW_OUTPUT_SCHEMA_VERSION:
                        # Write one row per (geo_type, geography, total) triple.
                        if total:
                            output_stream.add(simulation_output.OUTPUT_EXPOSURES,
                                              build_geo_type_row(geo_type, geography, total))
                        if premium_total:
                            output_stream.add(simulation_output.OUTPUT_PREMIUMS,
                                              build_geo_type_row(geo_type, geography, premium_total))

    if output_schema_version == FLAT_ROW_OUTPUT_SCHEMA_VERSION:
        outputs.send(simulation_output.OUTPUT_EXPOSURES, exposure_row)
        outputs.send(simulation_output.OUTPUT_PREMIUMS, premium_row)

    num_storms = 0
    limit = randint(950, 2050)
    logger.info(f'Generating {limit} storms')
    while num_storms <= limit:
        sample_storm = storm.generate_polygon()
        if not sample_storm:
            pass
        else:
            storms.append(sample_storm)
            num_storms += 1


    # Search exposures that fall within the storm
    logger.info('Calculating storm impact')

    for storm_poly in storms:
        if len(storm_poly['geometry']['coordinates']) > 0:
            damaged_homes = points_within_polygon(exposure_fc, storm_poly)

            payout = 10000

            for damaged_home in damaged_homes['features']:
                loss_dictionary['total'] += payout
                loss_dictionary['state'][damaged_home['properties']['state']] += payout
                loss_dictionary['county'][damaged_home['properties']['county']] += payout
                loss_dictionary['zip'][damaged_home['properties']['zip_code']] += payout

    logger.info('Writing losses and nlr')
    loss_row = dict(SimId=sim_id)
    nlr_row = dict(SimId=sim_id)
    with simulation_output.BufferedOutputStream(outputs, logger=logger) as output_stream:
        for geo_type, value in loss_dictionary.items():
            if geo_type == 'total':
                loss_row[geo_type] = value
                nlr_row[geo_type] = generate_loss_ratio(premium_dictionary[geo_type], value)

                if output_schema_version == GEO_TYPE_ROW_OUTPUT_SCHEMA_VERSION:
                    # Write one row per (geo_type, geography, total) triple.
                    if value:
                        output_stream.add(simulation_output.OUTPUT_LOSSES,
                                          build_geo_type_row(geo_type, geo_type, value))

                    if nlr_value:
                        output_stream.add(simulation_output.OUTPUT_NLR,
                                          build_geo_type_row(geo_type, geo_type, generate_loss_ratio(premium_dictionary[geo_type], value)))
            else:
                for geography, total in value.items():
                    column_name = simulation_output.format_column_name_for_output(geography)
                    nlr_value = generate_loss_ratio(premium_dictionary[geo_type][geography], total)
                    loss_row[column_name] = total
                    nlr_row[column_name] = nlr_value

                    if output_schema_version == GEO_TYPE_ROW_OUTPUT_SCHEMA_VERSION:
                        # Write one row per (geo_type, geography, total) triple.
                        if total:
                            output_stream.add(simulation_output.OUTPUT_LOSSES,
                                              build_geo_type_row(geo_type, geography, total))

                        if nlr_value:
                            output_stream.add(simulation_output.OUTPUT_NLR,
                                              build_geo_type_row(geo_type, geography, nlr_value))

    if output_schema_version == FLAT_ROW_OUTPUT_SCHEMA_VERSION:
        outputs.send(simulation_output.OUTPUT_LOSSES, loss_row)
        outputs.send(simulation_output.OUTPUT_NLR, nlr_row)

    logger.info('Simulation complete!')
    return sim_id


def setup_logging(args):
    """Called by main(). Initialize the logger outputs, based on CLI arguments.

    Processes flags:
    --log_stderr: If True logs are streamed to stderr. If False, no logs are written to stderr.
    --log_file <FILENAME>: If used, logs are appeneded to the given filename. Since this may encompass multiple runs,
        the logs are written with the datetime.
    """
    logger = logging.getLogger()
    if args.log_level:
        # Set all loggers to the desired level.
        logger.setLevel(args.log_level)

    if args.log_stderr:
        formatter = logging.Formatter(fmt=DEFAULT_LOG_FORMAT)
        stderr_handler = logging.StreamHandler()
        if args.log_level:
            stderr_handler.setLevel(args.log_level)
        stderr_handler.setFormatter(formatter)
        logger.addHandler(stderr_handler)
    elif args.log_file:
        # Logs will be going to a file, and we don't want them going to console.
        # Logging has a pass-thru logger that writes stderr by default. Disable that if the flag is not set.
        logger.propagate = False

    if args.log_file:
        formatter = logging.Formatter(fmt=DATED_LOG_FORMAT)
        file_handler = logging.FileHandler(args.log_file)
        if args.log_level:
            file_handler.setLevel(args.log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


def setup_bigquery_writer(project_id: str, dataset: str) -> simulation_output.BigqueryRowWriter:
    """Build a BigqueryRowWriter from the CLI args."""
    logging.debug('Preparing Bigquery Output Writer')
    if not project_id:
        raise Exception('--project_id is REQUIRED when using the --output_bigquery flag')
    return simulation_output.BigqueryRowWriter(project_id, dataset=dataset)


def setup_csv_writer(csv_output_dir: str, overwrite: bool) -> simulation_output.LocalCsvWriter:
    """Build a LocalCsvWriter from the CLI args."""
    logging.debug('Preparing CSV Output Writer')
    return simulation_output.LocalCsvWriter(csv_output_dir, overwrite=overwrite)


def setup_arguments(parser: argparse.ArgumentParser):
    """Adds all the argument groups and flags to the parser."""
    sim_group = parser.add_argument_group('Simulation')
    sim_group.add_argument('--num_simulations', '-n',
                           help='Number of iterations, each with a unique storm/sample',
                           type=int,
                           default=DEFAULT_NUM_SIMULATIONS)
    sim_group.add_argument('--multiprocessing',
                           help='When true, runs simulations in a process pool to parallelize the work.',
                           type=bool,
                           action=argparse.BooleanOptionalAction,
                           default=True)
    sim_group.add_argument('--num_processes', '-p',
                           help='Number of parallel processes to devote to running the simulations. '
                                'Note, this is constrained by the # of cpu cores on the machine. Too many parallel '
                                'procs could cause thrashing. If --multiprocess is set, code looks to this flag for '
                                'how many processes to create. If unset, by default, uses cpu count',
                           type=int,
                           default=None)
    sim_group.add_argument('--states',
                           help=f'Space separated list of states to run simulations against. Default is {STATES}',
                           nargs='+',
                           default=None)
    sim_group.add_argument('--label',
                           help='Optional string label to attach to all simulations. Useful for saving output/DB data.',
                           default=None)
    sim_group.add_argument('--seed',
                           help='The random seed used in all simulations, by default uses random\'s internal seed',
                           type=int,
                           # TODO: Consider setting the default to `int(datetime.date.today().strftime('%s'))`. T
                           #    his will lock in the random values for the day so if one needs to rerun a simulation
                           #    with the same outputs.
                           default=None)
    sim_group.add_argument('--output_schema_version',
                           help='Switch the schema versions of output written by the simulation',
                           type=int,
                           default=GEO_TYPE_ROW_OUTPUT_SCHEMA_VERSION)

    env_group = parser.add_argument_group('Environment')
    env_group.add_argument('--log_level',
                           help='Sets the logging output level',
                           # choices=logging.getLevelNamesMapping().keys(),  Re-add once fully on Python 3.11
                           default=None)
    env_group.add_argument('--log_stderr',
                           help='When true outputs log data to stderr.',
                           type=bool,
                           action=argparse.BooleanOptionalAction,
                           default=False)
    env_group.add_argument('--log_file',
                           help='If set, directs log output to the given filename',
                           default=None)

    csv_group = parser.add_argument_group('CSV Output')
    csv_group.add_argument('--output_csv',
                           help='When set, writes output to local CSV files',
                           type=bool,
                           action=argparse.BooleanOptionalAction,
                           default=False)
    csv_group.add_argument('--csv_output_dir',
                           help='Directory to write CSV output files, when writing CSV files. '
                                'Will create dir if needed',
                           default=DEFAULT_SIMULATIONS_CSV_OUTPUT_DIR)
    csv_group.add_argument('--csv_overwrite',
                           help='When true and outputting CSV, overwrites existing CSV files',
                           type=bool,
                           action=argparse.BooleanOptionalAction,
                           default=False)
    csv_group.add_argument('--upload_csv_to_bigquery_overwrite',
                           help='If set, uploads the final output CSV files to Bigquery. This flag is independent of '
                                'the other bigquery flags. NOTE: This overwrites the existing Bigquery tables! This is '
                                'useful if not storing any versions/partitions in the tables. Once using partitioning, '
                                'it is better to stream rows into their tables.',
                           type=bool,
                           action=argparse.BooleanOptionalAction,
                           default=False)

    bigquery_group = parser.add_argument_group('Bigquery Output')
    bigquery_group.add_argument('--output_bigquery',
                                help='When set, writes output to Bigquery. '
                                     'Output tables must already exist in the dataset.',
                                type=bool,
                                action=argparse.BooleanOptionalAction,
                                default=False)
    bigquery_group.add_argument('--bigquery_dataset',
                                help='Name of the dataset to use, when writing to bigquery.',
                                default=simulation_output.BigqueryRowWriter.DEFAULT_DATASET)

    webhook_group = parser.add_argument_group('Webhook Output')
    webhook_group.add_argument('--output_webhook',
                               help='When set, writes output to a webhook.',
                               type=bool,
                               action=argparse.BooleanOptionalAction,
                               default=False)
    webhook_group.add_argument('--webhook_url',
                               help='URL to send output data, when outputting via webhook',
                               default=simulation_output.WebhookWriter.DEFAULT_URL)

    gcp_group = parser.add_argument_group('Google Cloud')
    gcp_group.add_argument('--project_id',
                           help='Google Cloud project ID to use with any GCP client',
                           default=os.getenv('PROJECT_ID'))


def validate_arguments(args):
    """Check the CLI arguments to make sure there are no prerequisites violated."""
    if args.upload_csv_to_bigquery_overwrite:
        # Make sure the right combination of flags are/aren't set.
        if not args.output_csv:
            raise Exception('Must set --output_csv flag to use --upload_csv_to_bigquery_overwrite')
        if args.output_bigquery:
            raise Exception('Flag --output_bigquery and --upload_csv_to_bigquery_overwrite are both set. '
                            'This is redundant. The former streams rows into tables that will be overwritten by the '
                            'latter.')


def main():
    parser = argparse.ArgumentParser('Run storm simulations.')
    setup_arguments(parser)
    args = parser.parse_args()
    validate_arguments(args)

    random.seed(args.seed)
    setup_logging(args)

    # Establish a unique run id for all simulations run by this instnace, using epoch now.
    runtime_timestamp = int(time.time())
    logging.info(f'Runtime id is now: {runtime_timestamp}')

    output_writers = []
    csv_writer = None
    if args.output_csv:
        csv_writer = setup_csv_writer(args.csv_output_dir, args.csv_overwrite)
        output_writers.append(csv_writer)

    bigquery_writer = None
    if args.output_bigquery or args.upload_csv_to_bigquery_overwrite:
        # We will talk to bigquery at some point.
        bigquery_writer = setup_bigquery_writer(args.project_id, args.bigquery_dataset)

        if args.output_bigquery:
            # Only add to the output_writers if we're streaming rows.
            # If we're uploading CSV files, we don't want to insert rows because they'll just get overwritten.
            output_writers.append(bigquery_writer)
        else:
            # Initialize the bigquery client now for use at the end (better to find out if there are problems early).
            bigquery_writer.lazy_initialize()

    if not output_writers:
        logging.warning('No writers were specified. Simulation will run, but nothing will be output.')

    sims = range(args.num_simulations)
    logging.info(f'Starting ({args.num_simulations}) simulations, with random seed ({args.seed})')

    #
    # We support two "modes" of running.:
    # * Single threaded, single process - in which all the simulations are run by a single process.
    #
    # * Multi-process - running N separate processes, splitting the simulations between them. In this mode, we can
    #     leverage a multi-core CPU. But, we have to be aware of when we're in the main process versus any of the
    #     forked processes from the multiprocessing.Pool. Python has several tricks (like the multiprocessing.Manager)
    #     to share datga between processes, but that data has to be pickle-able. We also have to be careful of resource
    #     contention, such as parallel writes to a file. So, in this mode, all output is managed by a single, solo
    #     process. Any simulation-process that wants to output passes data to the output-process via a Queue.
    if not args.multiprocessing or args.num_processes == 1:
        logging.debug('Running all simulations in a single process.')
        output = simulation_output.SerialOutput(output_writers)
        list(map(functools.partial(run_losses,
                                   runtime_timestamp=runtime_timestamp,
                                   outputs=output,
                                   output_schema_version=args.output_schema_version,
                                   states=args.states,
                                   label=args.label),
                 sims)
             )
    else:
        # Run N simulations across P processes. Each simulation does identical work so will probably all finish in the
        # same time, more or less. We create one extra process that will handle all output.
        num_processes = (args.num_processes or multiprocessing.cpu_count()) + 1

        # Make the output writers available to all the simulation-processes.
        manager = multiprocessing.Manager()
        manager.list(output_writers)

        # Output rows will be sent via the output_queue, processed by the output-process.
        output_queue = manager.Queue()
        outputs = simulation_output.MultiprocessingOutput(output_queue)
        pool = multiprocessing.Pool(processes=num_processes)
        logging.info(f'Multiprocessing pool initiated with ({num_processes}) total processes')

        # Start a thread running in this main process for writing all logs.
        # Logging from sub-processes can be tricky -- they don't have access to the handler in the main process and
        # if they try to write to the same file they can collide. So all sub-process logging is sent via to the
        # thread via the logging_queue.
        logging_queue = manager.Queue()
        logging_thread = simulation_logging.LoggerThread(logging_queue)
        logging_thread.start()

        # Start a process for reading output data from the output_queue and sending the data on its way via writers.
        # The process is stopped by injecting a single on the output_queue via `outputs.stop()`.
        output_process = multiprocessing.Process(
            name='OutputProc',
            target=outputs.process_queue,
            args=(output_queue, output_writers, logging_queue, args.log_level))
        output_process.start()

        # This can be a little confusing to parse:
        # - tqdm(iterable, total=N): displays a progress bar for each item in the iterable up to the total N.
        # - functools.partial(f, args, kwargs): is a trick to pass multiple arguments pool.imap_unordered(f, iterable).
        #   When the pool maps, it sends each item from the iterable as the sole argument into f(). To pass additional,
        #   static data, we use functools.partial which wraps the given function.
        #   So imap_unordered(functools.partial(f, args, kwargs), iterable) results in a call signature of:
        #       f(ith_item_iterable, args, kwargs)
        list(tqdm(
            pool.imap_unordered(
                functools.partial(run_losses,
                                  runtime_timestamp=runtime_timestamp,
                                  outputs=outputs,
                                  output_schema_version=args.output_schema_version,
                                  states=args.states,
                                  label=args.label,
                                  logging_queue=logging_queue,
                                  log_level=args.log_level),
                sims),
            total=args.num_simulations)
        )

        # Wait for the last output to be processed.
        outputs.stop()
        output_process.join()

        if args.upload_csv_to_bigquery_overwrite:
            # Upload CSV files to Bigquery. We do this after all the outputs are done, to make sure we get the final
            # rows in the queue.
            logging.info('Uploading CSV Files to *OVERWRITE* Bigquery.')
            simulation_output.upload_csv_to_bigquery(csv_writer, bigquery_writer)

        logging.info('Simulations complete, cleaning up')
        logging_thread.stop()
        logging_thread.join()
        pool.close()
        pool.join()


if __name__ == '__main__':
    main()
