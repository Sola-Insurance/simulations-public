# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import json
import logging
from typing import Callable, Union

from google.cloud import pubsub_v1


"""Helper class for using Google PubSub

PubSub is a persistent message service. One can publish messages to a "topic" and then 0..N subscribers of that topic 
are guaranteed to receive the message at least once. Because there's a guarantee of >=1 receipt every subscriber is 
responsible for being idempotent -- if it receives a duplicate message it must have no effect. PubSub messages are 
byte-encoded for transit.

Key concepts:
   * A message is byte-encoded blob of data, published throught a "topic". A topic has a name and then is referenced
     by a path of the format "/projects/{project_id}/topics/{topic_name}"
   * Each subscription also has a name and a path like "/projects/{project_id}/subscriptions/{subscription_name}". When
     subscriptions are configured they can have retention/expiration rules set.
   * PubSub supports Push and Pull model subscribers. An example Push would be using a PubSub event trigger to call 
     a Cloud Function. An example Pull would be a running server, reading messages from the subscription queue.
   * Each message must be acknowledged before a timeout, otherwise it will be requeued and redelivered.

See:
https://cloud.google.com/pubsub/docs/overview
"""


class Publisher:
    """Utility class for publishing messages to a PubSub Topic."""
    @staticmethod
    def client() -> pubsub_v1.PublisherClient:
        return pubsub_v1.PublisherClient()

    @staticmethod
    def publish(
            client: pubsub_v1.PublisherClient,
            project_id: str,
            topic: str,
            data: Union[dict, str]) -> str:
        """Send the given message onto the Pubsub Topic.

        Based on:
        https://cloud.google.com/pubsub/docs/publisher#publish_messages

        :param client: Client for publishing messages onto topics.
        :param project_id: Google Cloud Project ID.
        :param topic: Name of the topic to publish onto.
        :param data: Dict or str, to be sent on the topic.
        :returns: Waits for the message to be published and returns the published message id.
        """
        byte_data = json.dumps(data).encode('utf-8')
        topic_path = client.topic_path(project_id, topic)
        logging.debug(f'Pushing message onto {topic_path}: {str(data)[:20]}...')
        future = client.publish(topic_path, data=byte_data)
        return future.result()


class Subscriber:
    """Utility class for pulling messages from a PubSub Subscription."""
    @staticmethod
    def client() -> pubsub_v1.SubscriberClient:
        return pubsub_v1.SubscriberClient()

    @staticmethod
    def pull_one(
            client: pubsub_v1.SubscriberClient,
            project_id: str,
            subscription_id: str,
            callback: Callable,
            timeout: int = None):
        """Attempt to pull one message from the subscription, triggering the callback. Caller acks the message.

        Based on:
        https://cloud.google.com/pubsub/docs/pull#streamingpull_and_high-level_client_library_code_samples

        Callback will receive a message of this type (the python typehints don't appear to work):
        https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.message.Message

        :param client: The Pubsub Subscription client to pull from.
        :param project_id: Google Cloud Project ID.
        :param subscription_id: String name of the subscription to pull from.
        :param callback: Function to receive the pubsub_v1.subscriber.message.Message.
                         Caller is expected to ack the message.
        :param timeout: Duration to wait for a message, if None, blocks.
        """
        subscription_path = client.subscription_path(project_id, subscription_id)
        streaming_pull_future = client.subscribe(subscription_path, callback=callback)

        with client:
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                logging.debug('Timeout occurred')
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.
