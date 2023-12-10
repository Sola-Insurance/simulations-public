import json
import unittest
from unittest import mock

from gcp.pubsub import PubSub


class PubSubTestCase(unittest.TestCase):
    @mock.patch('gcp.pubsub.pubsub_v1.PublisherClient')
    def test_publisher_client(self, publisher_client_init):
        self.assertIs(publisher_client_init.return_value, PubSub.publisher_client())

    def test_publish(self):
        project_id = 'fake-project'
        topic = 'fake-topic'
        future = mock.Mock()
        publisher_client = mock.Mock()
        publisher_client.publish.return_value = future

        data = {'fake': 'data'}
        msg_id = PubSub.publish(publisher_client, project_id, topic, data)
        self.assertIs(msg_id, future.result.return_value)
        publisher_client.topic_path.assert_called_once_with(project_id, topic)
        publisher_client.publish.assert_called_once_with(
            publisher_client.topic_path.return_value,
            data=json.dumps(data).encode('utf-8')
        )


if __name__ == '__main__':
    unittest.main()
