# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import argparse
import json
import logging
from typing import Union

from gcp.pubsub import Publisher

"""Utility to publish a single message on a pubsub topic.

Usage:
python gcp/tools/publish_pubsub_message.py <PROJECT> <TOPIC> "hello"
or
python gcp/tools/publish_pubsub_message.py <PROJECT> <TOPIC> '{"dict_example": 1}'
"""


def unpack_message(msg: str) -> Union[dict, str]:
    """Convert the string message from CLI args, to a dict or a quoted string."""
    try:
        return json.loads(msg)
    except:
        return f'"{msg}"'


def publish(project_id, topic, data):
    """Send the data on the pubsub topic."""
    client = Publisher.client()
    try:
        message_id = Publisher.publish(client, project_id, topic, data)
        logging.info(f'Published message: {message_id}')
    except Exception as e:
        logging.exception('Failed to send message', e)


def main():
    parser = argparse.ArgumentParser('Publish a message on pubsub')
    parser.add_argument('project_id', help='Google Project ID')
    parser.add_argument('topic', help='Name of the topic (not the full path)')
    parser.add_argument('msg', help='JSONified dict / String message to publish')
    parser.add_argument('--log_level', default=logging.INFO)
    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level)

    data = unpack_message(args.msg)
    publish(args.project_id, args.topic, data)


if __name__ == '__main__':
    main()
