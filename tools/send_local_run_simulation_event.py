# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import argparse
import base64
import json
import requests
import time


"""Sends an HTTP request to simulate the recipe of a Cloud Event, running a simulation.

Use this when locally testing the run_simulation cloud function. It sends a message to the functions-framework server 
mimicking an incoming PubSub event with simulation data in it. 

Usage:
1) Start the function, see info in the README.md
2) Run this program in a different shell with `python tools/send_local_run_simulation_event.py`


Based on:
https://cloud.google.com/functions/docs/running/calling#sending_requests_to_local_functions
"""


def main():
    parser = argparse.ArgumentParser('Sends a test.json CloudEvent via HTTP to the run_simulation function')
    parser.add_argument('--url', default='http://localhost:8080')
    args = parser.parse_args()

    data = dict(
        storm_type='hail',
        sim_id=1,
        output_bigquery=False,
        state=None,
        run_timestamp=int(time.time())
    )
    message = dict(
        message=dict(
            data=base64.b64encode(json.dumps(data).encode('utf-8')).decode()
        )
    )
    print(f'Sending message to {args.url} : {data}')
    requests.post(args.url, json=message, headers={
        'ce-id': "123451234512345",
        'ce-specversion': "1.0",
        "ce-time": "2020-01-02T12:34:56.789Z",
        "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
        "ce-source": "//pubsub.googleapis.com/projects/fake-project/topics/fake-topic",
    })


if __name__ == '__main__':
    main()
