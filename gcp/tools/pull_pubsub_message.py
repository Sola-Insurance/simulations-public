# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import argparse
import json
import logging

from gcp.pubsub import Subscriber


"""Utility to pull+ck a single message from a pubsub subscription.

Usage:
python gcp/tools/pull_pubsub_message.py <PROJECT> <SUBSCRIPTION>
or
python gcp/tools/pull_pubsub_message.py <PROJECT> <SUBSCRIPTION>

Optional flags:
--no_ack: Do not ack the message
--timeout: How long to wait for the message.
"""

ack_message = True


def process_message(msg):
    """Callback triggered when a message is received."""
    logging.debug(f'Received raw message {msg}')
    try:
        data = json.loads(msg.data)
        logging.info(f'Got message: ({data})')
        if ack_message:
            logging.info(f'Acking message')
            msg.ack()
        else:
            logging.info(f'Releasing (without Acking) message')
            msg.release()
    except Exception as e:
        msg.release()
        logging.exception('Failed to process message', e)


def pull(project_id, subscription, timeout: int):
    """Send the data on the pubsub topic."""
    client = Subscriber.client()
    Subscriber.pull_one(client, project_id, subscription, process_message, timeout=timeout)


def main():
    global ack_message

    parser = argparse.ArgumentParser('Pull a message from pubsub')
    parser.add_argument('project_id', help='Google Project ID')
    parser.add_argument('subscription', help='Name of the subscription (not the full path)')
    parser.add_argument('--acknowledge', '--ack',
                        help='If set, acknowledge the pulled message.',
                        type=bool,
                        action=argparse.BooleanOptionalAction,
                        default=True)
    parser.add_argument('--timeout',
                        help='How long to wait for a message (secs)',
                        type=int,
                        default=10)
    parser.add_argument('--log_level', default=logging.INFO)
    args = parser.parse_args()
    ack_message = args.acknowledge

    logging.getLogger().setLevel(args.log_level)
    pull(args.project_id, args.subscription, args.timeout)


if __name__ == '__main__':
    main()
