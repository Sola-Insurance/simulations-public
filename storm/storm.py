import json
import random

from geojson import Feature, Point
from turfpy.measurement import bbox, boolean_point_in_polygon
from turfpy.random import random_position
from turfpy.transformation import circle

from geo import country_us

# These files represent assumptions that we gathered from previous storms
damage_file = open('data/storms/damage.json')
matrix_file = open('data/storms/matrix.json')
size_file = open('data/storms/size.json')


damage_matrix = json.load(damage_file)
state_matrix = json.load(matrix_file)
size_matrix = json.load(size_file)


def generate_sample_storm() -> dict:
    """
    Function that randomly generates stats for a synthetic hail storm using historical assumptions
    :return: dictionary with hail storm stats
    """
    dmg_choices = list(damage_matrix.keys())
    dmg_weights = list(damage_matrix.values())

    sim_dmg = random.choices(dmg_choices, k=1, weights=dmg_weights)[0]

    state_choices = list(state_matrix[sim_dmg].keys())
    state_weights = list(state_matrix[sim_dmg].values())

    sim_state = random.choices(state_choices, k=1, weights=state_weights)[0]

    size_choices = list(size_matrix[sim_dmg].keys())
    size_weights = list(size_matrix[sim_dmg].values())

    sim_size = random.choices(size_choices, k=1, weights=size_weights)[0]

    return {
        "severity": sim_dmg,
        "state": sim_state,
        "size": sim_size
    }


def generate_polygon():
    """
    Function that generates hail shape file in geojson polygon format using hail statistics

    :return: hail shape file
    """
    storm_data = generate_sample_storm()

    state = storm_data["state"]
    severity = storm_data["severity"]
    size = storm_data["size"]

    state_poly = country_us.state_polys[state]
    state_bbox = bbox(state_poly)

    us_feature = Feature(geometry=country_us.us_poly)

    center = Feature(geometry=Point(coordinates=random_position(state_bbox)))

    if not boolean_point_in_polygon(center, us_feature):
        return False

    polygon = circle(center, radius=float(size), steps=10, units='mi')

    polygon["properties"] = {
        "payout": 10000,
        "severity": severity
    }

    return polygon
