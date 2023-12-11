# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import base64
import functions_framework
import json
import logging
import os
import time
import traceback

from functions import generate_losses, simulation_output
from typing import Optional, Iterable

from gcp.pubsub import Publisher

"""Google Cloud Function for running storm simulations.

There are TWO functions:
1) `trigger_simulation_events` - HTTP triggered, for kicking off N parallel simulations
This function is triggered by a an HTTP POST request. The POST data may contain optional config about the storm type, 
how many simulations to run, and whether to output simulation results. Upon receipt of a request, this function will 
publish N=(# of desired simulations) requests, one per simulation, into the PubSub topic. Those events will fan-out to
the  'run_simulation_function' Cloud Function in parallel.  


2) `run_storm_simulation` - CloudEvent/PubSub triggered, for running a single simulation
This function is triggered by Pub/Sub Cloud Events. These are persistent queued messages, guaranteed to be processed
at least once. The intent of this is that a single (HTTP) request is used to fire off the trigger function, which then 
queues N Pub/Sub events, one per desired simulation. The PubSub events will be processed in parallel by the Cloud 
Function infrastructure. 

See the README for running locally & deploying.
"""

STORM_TYPE_HAIL = 'hail'
SUPPORTED_STORM_TYPES = [STORM_TYPE_HAIL]
DEFAULT_STORM_TYPE = STORM_TYPE_HAIL
DEFAULT_NUM_SIMS = 3
DEFAULT_OUTPUT_BIGQUERY = True
LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)
PROJECT_ID = os.getenv('PROJECT_ID')
PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC', 'run-storm-simulation')
SKIP_PUBSUB = os.getenv('SKIP_PUBSUB', False)  # Use locally, to log what would happen, without pub subbing.
BIGQUERY_OUTPUT_DATASET = os.getenv('BIGQUERY_DATASET', simulation_output.BigqueryRowWriter.DEFAULT_DATASET)


if os.getenv('CLOUD_LOGGING'):
    # TODO: Enable cloud logging  https://cloud.google.com/logging/docs/setup/python
    # from google.cloud.logging
    # client = google.cloud.logging.Client()
    # client.setup_logging()
    pass
else:
    import logging
    logging.getLogger().setLevel(LOG_LEVEL)


def _trigger_simulations(
        storm_type: str,
        num_sims: int,
        output_bigquery: bool,
        states: Optional[Iterable[str]],
        topic: str):
    """Send N POSTs reqs to the given URL, each with simulation id, to run simulations in parallel (on Cloud Functions).

    :param storm_type: The type of storm to simulate.
    :param num_sims: Number of simulations to start.
    :param output_bigquery: If true, the simulation output will be written to bigquery.
    :param states: List of states to simulate storms over.
    :param topic: PubSub topic to publish events into.
    """

    # Setup values, using overrides if set.
    storm_type = storm_type or DEFAULT_STORM_TYPE
    num_sims = num_sims or DEFAULT_NUM_SIMS
    output_bigquery = output_bigquery if output_bigquery is not None else DEFAULT_OUTPUT_BIGQUERY
    topic = topic or PUBSUB_TOPIC
    run_timestamp = int(time.time())

    if storm_type not in SUPPORTED_STORM_TYPES:
        logging.error(f'Unsupported storm type {storm_type}. Ignoring request')

    logging.info(f'Starting {storm_type} simulations. N={num_sims} @ {run_timestamp} ==> {topic}')

    client = Publisher.client()
    for sim_id in range(num_sims):
        message = dict(
            storm_type=storm_type,
            sim_id=sim_id,
            output_bigquery=output_bigquery,
            state=states,
            run_timestamp=run_timestamp
        )
        if SKIP_PUBSUB:
            logging.info(f'***DEBUG MODE*** Skipping Sim#{sim_id}: {message}')
        else:
            logging.info(f'Triggering Sim#{sim_id}: {message}')
            Publisher.publish(client, PROJECT_ID, topic, message)

    logging.info(f'All simulation events sent.')

    return str(run_timestamp)


def _start_simulation(
        storm_type: str,
        run_timestamp: int,
        sim_id: int,
        output_bigquery: bool,
        states: Optional[Iterable[str]]):
    """Run a single storm simulation.

    :param storm_type: Type of storm to simulate.
    :param run_timestamp: Epoch seconds of when the simulations started.
    :param sim_id: Enumerant of the simulation, tying to pre-selected property samples.
    :param output_bigquery: If true, write results to Bigquery.
    :param states: Optional list of two-letter state codes to include in the simulation.
    """
    if storm_type not in SUPPORTED_STORM_TYPES:
        logging.error(f'Unsupported storm type {storm_type}. Ignoring request')
        return

    logging.info(f'Starting simulation {sim_id} @ {run_timestamp}')
    start_time = int(time.time())

    if storm_type == STORM_TYPE_HAIL:
        output_writers = []
        if output_bigquery:
            output_writers.append(
                generate_losses.setup_bigquery_writer(PROJECT_ID, BIGQUERY_OUTPUT_DATASET)
            )
        else:
            logging.warning('Running simulation without output.')

        outputs = simulation_output.SerialOutput(output_writers)
        try:
            generate_losses.run_losses(
                sim_id,
                run_timestamp,
                outputs,
                states=states,
                label=None,  # TODO: Consider using a label like 'cloud-function-{run_timestamp} to avoid collisions.
                             #       but to do so, also drop the collection outright at the end.
                log_level=LOG_LEVEL  # Unused, since we're in a single-thread, not multiprocessing,
                                                     # but may as well pass it.
            )
        except Exception as e:
            logging.exception('Simulation failed.', e)
            tb = traceback.format_exc()
            logging.error(f'Exception from simulation:\n {tb}')
    else:
        logging.warning(f'Did nothing for storm: {storm_type}')

    sim_duration = int(time.time()) - start_time
    logging.info(f'Simulation ({sim_id}) took ({sim_duration})s')


def _decode_message_data(cloud_event) -> dict:
    """Unpack the b64-encoded bytes inside the PubSub Cloud Event, returning the expected dict."""
    return json.loads(
        base64.b64decode(cloud_event.data['message']['data']).decode()
    )


@functions_framework.cloud_event
def run_storm_simulation(cloud_event):
    """CloudEvent (via Pubsub) trigger to start a single simulation.

    Expects a dict in the event with:
    {
        storm_type: currently only supports 'hail',
        sim_id: <int>,
        run_timestamp: <int>, # Used to uniquely identify a run.
                              # Technically it's possible two runs trigger simultaneously. It's unlikely, but if it
                              # does happen, we'll see duplicate rows per sim_id with the same timestamp in the output.
                              # If this happens, to fix, either also create a run_uuid and store that to disambiguate,
                              # or add a checker to 'lock' a (storm_type, sim_id). The lock would have to be some
                              # atomic entity, like a row in SQL.
        output_bigquery: <bool>,
        states: [str of two-letter ISO code] States to simulate storm exposures against
    }
    """
    try:
        data = _decode_message_data(cloud_event)
        logging.debug(f'Received simulation trigger: {data}')

        # Pull required fields from event.
        sim_id = data['sim_id']
        run_timestamp = data['run_timestamp']

        # Pull optional fields from event.
        storm_type = data.get('storm_type', DEFAULT_STORM_TYPE)
        output_bigquery = data.get('output_bigquery', DEFAULT_OUTPUT_BIGQUERY)
        states = data.get('state')

        _start_simulation(storm_type, run_timestamp, sim_id, output_bigquery, states)

    except Exception as e:
        # Not much we can do, just log the error and consume the event.
        logging.exception(f'Failed to process simulation event {cloud_event}', e)


@functions_framework.http
def trigger_simulation_events(request):
    """Handle an HTTP request to trigger all the simulations in parallel via Pubsub and Cloud Functions.

    Request structure (in JSON):
    Trigger a fan-out of all simulations, sending PubSub events for each sim.

    Optional POST data to override defaults:
    {
        storm_type: currently only supports 'hail',
        num_sims: <int>,
        output_bigquery: <bool>, whether the simulations should write Bigquery
        states: [str of two-letter ISO code] states to simulate storm exposures against
        topic: <str>, publish simulation pubsub events to this topic
    }
    """
    if request.method == 'GET':
        # Ignore, but don't reject, any GETs, they could be health checks.
        return 'OK'

    request_json = request.get_json(silent=True)
    if not request_json:
        logging.error('Handler called with no JSON request data.')
        return 'OK'

    logging.info(f'Incoming simulation request {request_json}')
    storm_type = request_json.get('storm_type')
    output_bigquery = request_json.get('output_bigquery')
    states = request_json.get('states')
    topic = request_json.get('topic')

    try:
        num_sims = int(request_json.get('num_sims', DEFAULT_NUM_SIMS))
        return _trigger_simulations(storm_type, num_sims, output_bigquery, states, topic)
    except Exception as e:
        logging.exception(f'Failed to trigger simulations. Failing quietly.', e)
    return 'OK'
