import argparse
import logging
import time

import main as cloud_functions_main

from typing import Optional, Iterable, Union

from gcp.pubsub import Publisher


"""Trigger a single simulation run in the Cloud by sending a PubSub event.

Use this if you'd like to re-run specific storm simulations by their numeric id. Sends a pubsub event akin to what the 
trigger_simulation_events Cloud Function does.

Usage:
```
export PROJECT_ID=...

# Rerun a single simulation.
python tools/send_simulation_pubsub.py <NUM>

# Rerun a selection of simulations 
python tools/send_simulation_pubsub.py 1,24,300-400

# Debug mode, print the messages but don't send them
python tools/send_simulation_pubsub.py <NUM> -n

# Other flags available.
python tools/send_simulation_pubsub.py -h
```
"""



def build_message(
        storm_type: str,
        sim_id: int,
        run_timestamp: int,
        output_bigquery: bool,
        states: Optional[Iterable[str]]) -> dict:
    """Builds the dict to publish, triggering a simulation.

    See main.py:run_storm_simulation() for the spec on the dict type expected via PubSub.

    :param storm_type: Type of the storm to simulate
    :param sim_id: Numeric id of the simulation to run, tied to pre-sampled properties.
    :param run_timestamp: Epoch seconds to associate the simulation with.
    :param output_bigquery: Boolean, if True write results to the Bigquery tables.
    :param states: List of two-letter state ids to simulate.
    :return: Populated dict to publish
    """
    return dict(
        storm_type=storm_type,
        sim_id=sim_id,
        run_timestamp=run_timestamp,
        output_bigquery=output_bigquery,
        states=states
    )


def publish(project_id: str, topic: str, messages: Iterable[dict], debug: bool = False):
    logging.info(f'{"***DEBUG***, NOT " if debug else ""}Publishing [{len(messages)}] to {project_id}:{topic}')
    client = Publisher.client()
    message_ids = []
    for i, message in enumerate(messages):
        logging.info(f'[{i}]: {message}')
        if not debug:
            message_id = Publisher.publish(client, project_id, topic, message)
            message_ids.append(message_id)

    if message_ids:
        logging.info(f'Published Message IDS: {[", ".join(message_ids)]}')


def parse_sim_ids(sim_id_str: str):
    """Turn an arg string of []"""
    sim_ids = []
    id_args = sim_id_str.split(',')
    for id_arg in id_args:
        try:
            if '-' in id_arg:
                start_id, end_id = [int(v) for v in id_arg.split('-')]
                if start_id < end_id:
                    sim_ids.extend(range(start_id, end_id))
                    sim_ids.append(end_id)
            else:
                sim_ids.append(int(id_arg))
        except ValueError:
            logging.warning(f'Could not parse sim_id: {id_arg}')
            continue
    return sim_ids


def main():
    parser = argparse.ArgumentParser('Trigger a simulation Cloud Function with Pubsub')
    parser.add_argument('sim_id',
                        help='The simulation ID to run. Can either be a single id, or a comma separated list of '
                             'ids, or an inclusive range [id1-id2]')
    parser.add_argument('--storm_type',
                        help='Type of storm to simulate',
                        default=cloud_functions_main.DEFAULT_STORM_TYPE)
    parser.add_argument('--run_timestamp',
                        help='Epoch seconds to associate with the simulation(s). Default is now')
    parser.add_argument('--output_bigquery',
                        help='If set, the simulations will write their output to Bigquery.',
                        default=True,
                        action=argparse.BooleanOptionalAction)
    parser.add_argument('--states',
                        help='Two-letter state codes to run in the simulation',
                        nargs='+')
    parser.add_argument('--project_id',
                        help='Override the GCP project id from the environment')
    parser.add_argument('--topic',
                        help='Override the PubSub topic to publish onto',
                        default=cloud_functions_main.PUBSUB_TOPIC)
    parser.add_argument('--log-level', help='Set the logging level. Or "OFF".', default=logging.INFO)
    parser.add_argument('--debug', '-n',
                        help='If set, logs messages, but does not publish them',
                        default=False,
                        action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    if args.log_level != 'OFF':
        logging.getLogger().setLevel(args.log_level)

    project_id = args.project_id or cloud_functions_main.PROJECT_ID
    run_timestamp = args.run_timestamp or int(time.time())
    sim_ids = parse_sim_ids(args.sim_id)
    if not sim_ids:
        logging.error(f'No sim_ids parsed from argument "{args.sim_ids}"')
        print(f'ERROR: No Sim Ids to run')
        return

    logging.info(f'Generating event(s) to trigger storm({args.storm_type}) for sims: {sim_ids}')
    messages = [build_message(args.storm_type, sim_id, run_timestamp, args.output_bigquery, args.states)
                for sim_id in sim_ids]
    publish(project_id, args.topic, messages, debug=args.debug)


if __name__ == '__main__':
    main()

