#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


from unittest import TestCase, main
import time
from pulsar import Client, MessageId, \
            CompressionType, ConsumerType

from _pulsar import ProducerConfiguration, ConsumerConfiguration

try:
    # For Python 3.0 and later
    from urllib.request import urlopen, Request
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen, Request


def doHttpPost(url, data):
    req = Request(url, data.encode())
    req.add_header('Content-Type', 'application/json')
    urlopen(req)


class PulsarTest(TestCase):

    serviceUrl = 'pulsar://localhost:8885'
    adminUrl = 'http://localhost:8765'

    def test_producer_config(self):
        conf = ProducerConfiguration()
        conf.send_timeout_millis(12)
        self.assertEqual(conf.send_timeout_millis(), 12)

        self.assertEqual(conf.compression_type(), CompressionType.NONE)
        conf.compression_type(CompressionType.LZ4)
        self.assertEqual(conf.compression_type(), CompressionType.LZ4)

        conf.max_pending_messages(120)
        self.assertEqual(conf.max_pending_messages(), 120)

    def test_consumer_config(self):
        conf = ConsumerConfiguration()
        self.assertEqual(conf.consumer_type(), ConsumerType.Exclusive)
        conf.consumer_type(ConsumerType.Shared)
        self.assertEqual(conf.consumer_type(), ConsumerType.Shared)

        self.assertEqual(conf.consumer_name(), '')
        conf.consumer_name("my-name")
        self.assertEqual(conf.consumer_name(), "my-name")

    def test_simple_producer(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer('persistent://sample/standalone/ns/my-python-topic')
        producer.send('hello')
        producer.close()
        client.close()

    def test_producer_send_async(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer('persistent://sample/standalone/ns/my-python-topic')

        sent_messages = []

        def send_callback(producer, msg):
            sent_messages.append(msg)

        producer.send_async('hello', send_callback)
        producer.send_async('hello', send_callback)
        producer.send_async('hello', send_callback)

        time.sleep(0.1)
        self.assertEqual(len(sent_messages), 3)
        client.close()

    def test_producer_consumer(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe('persistent://sample/standalone/ns/my-python-topic-producer-consumer',
                                    'my-sub',
                                    consumer_type=ConsumerType.Shared)
        producer = client.create_producer('persistent://sample/standalone/ns/my-python-topic-producer-consumer')
        producer.send('hello')

        msg = consumer.receive(1000)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b'hello')

        try:
            msg = consumer.receive(100)
            self.assertTrue(False)  # Should not reach this point
        except:
            pass  # Exception is expected

        client.close()

    def test_message_listener(self):
        client = Client(self.serviceUrl)

        received_messages = []

        def listener(consumer, msg):
            print("Got message: %s" % msg)
            received_messages.append(msg)
            consumer.acknowledge(msg)

        client.subscribe('persistent://sample/standalone/ns/my-python-topic-listener',
                         'my-sub',
                         consumer_type=ConsumerType.Exclusive,
                         message_listener=listener)
        producer = client.create_producer('persistent://sample/standalone/ns/my-python-topic-listener')
        producer.send('hello-1')
        producer.send('hello-2')
        producer.send('hello-3')

        time.sleep(0.1)
        self.assertEqual(len(received_messages), 3)
        self.assertEqual(received_messages[0].data(), b"hello-1")
        self.assertEqual(received_messages[1].data(), b"hello-2")
        self.assertEqual(received_messages[2].data(), b"hello-3")
        client.close()

    def test_reader_simple(self):
        client = Client(self.serviceUrl)
        reader = client.create_reader('persistent://sample/standalone/ns/my-python-topic-reader-simple',
                                      MessageId.earliest)

        producer = client.create_producer('persistent://sample/standalone/ns/my-python-topic-reader-simple')
        producer.send('hello')

        msg = reader.read_next()
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b'hello')

        try:
            msg = reader.read_next(100)
            self.assertTrue(False)  # Should not reach this point
        except:
            pass  # Exception is expected

        reader.close()
        client.close()

    def test_reader_on_last_message(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer('persistent://sample/standalone/ns/my-python-topic-reader-on-last-message')

        for i in range(10):
            producer.send('hello-%d' % i)

        reader = client.create_reader('persistent://sample/standalone/ns/my-python-topic-reader-on-last-message',
                                      MessageId.latest)

        for i in range(10, 20):
            producer.send('hello-%d' % i)

        for i in range(10, 20):
            msg = reader.read_next()
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b'hello-%d' % i)

        reader.close()
        client.close()

    def test_reader_on_specific_message(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer(
            'persistent://sample/standalone/ns/my-python-topic-reader-on-specific-message')

        for i in range(10):
            producer.send('hello-%d' % i)

        reader1 = client.create_reader(
                'persistent://sample/standalone/ns/my-python-topic-reader-on-specific-message',
                MessageId.earliest)

        for i in range(5):
            msg = reader1.read_next()
            last_msg_id = msg.message_id()

        reader2 = client.create_reader(
                'persistent://sample/standalone/ns/my-python-topic-reader-on-specific-message',
                last_msg_id)

        for i in range(5, 10):
            msg = reader2.read_next()
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b'hello-%d' % i)

        reader1.close()
        reader2.close()
        client.close()

    def test_reader_on_specific_message_with_batches(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer(
            'persistent://sample/standalone/ns/my-python-topic-reader-on-specific-message-with-batches',
            batching_enabled=True,
            batching_max_publish_delay_ms=1000)

        for i in range(10):
            producer.send_async('hello-%d' % i, None)

        # Send one sync message to make sure everything was published
        producer.send('hello-10')

        reader1 = client.create_reader(
                'persistent://sample/standalone/ns/my-python-topic-reader-on-specific-message-with-batches',
                MessageId.earliest)

        for i in range(5):
            msg = reader1.read_next()
            last_msg_id = msg.message_id()

        reader2 = client.create_reader(
                'persistent://sample/standalone/ns/my-python-topic-reader-on-specific-message-with-batches',
                last_msg_id)

        for i in range(5, 11):
            msg = reader2.read_next()
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b'hello-%d' % i)

        reader1.close()
        reader2.close()
        client.close()

    def test_producer_sequence_after_reconnection(self):
        # Enable deduplication on namespace
        doHttpPost(self.adminUrl + '/admin/namespaces/sample/standalone/ns1/deduplication',
                   'true')
        client = Client(self.serviceUrl)

        topic = 'persistent://sample/standalone/ns1/my-python-test-producer-sequence-after-reconnection-' \
            + str(time.time())

        producer = client.create_producer(topic, producer_name='my-producer-name')
        self.assertEqual(producer.last_sequence_id(), -1)

        for i in range(10):
            producer.send('hello-%d' % i)
            self.assertEqual(producer.last_sequence_id(), i)

        producer.close()

        producer = client.create_producer(topic, producer_name='my-producer-name')
        self.assertEqual(producer.last_sequence_id(), 9)

        for i in range(10, 20):
            producer.send('hello-%d' % i)
            self.assertEqual(producer.last_sequence_id(), i)

    def test_producer_deduplication(self):
        # Enable deduplication on namespace
        doHttpPost(self.adminUrl + '/admin/namespaces/sample/standalone/ns1/deduplication',
                   'true')
        client = Client(self.serviceUrl)

        topic = 'persistent://sample/standalone/ns1/my-python-test-producer-deduplication-' + str(time.time())

        producer = client.create_producer(topic, producer_name='my-producer-name')
        self.assertEqual(producer.last_sequence_id(), -1)

        consumer = client.subscribe(topic, 'my-sub')

        producer.send('hello-0', sequence_id=0)
        producer.send('hello-1', sequence_id=1)
        producer.send('hello-2', sequence_id=2)
        self.assertEqual(producer.last_sequence_id(), 2)

        # Repeat the messages and verify they're not received by consumer
        producer.send('hello-1', sequence_id=1)
        producer.send('hello-2', sequence_id=2)
        self.assertEqual(producer.last_sequence_id(), 2)

        for i in range(3):
            msg = consumer.receive()
            self.assertEqual(msg.data(), b'hello-%d' % i)
            consumer.acknowledge(msg)

        try:
            # No other messages should be received
            consumer.receive(timeout_millis=1000)
            self.assertTrue(False)
        except:
            # Exception is expected
            pass

        producer.close()

        producer = client.create_producer(topic, producer_name='my-producer-name')
        self.assertEqual(producer.last_sequence_id(), 2)

        # Repeat the messages and verify they're not received by consumer
        producer.send('hello-1', sequence_id=1)
        producer.send('hello-2', sequence_id=2)
        self.assertEqual(producer.last_sequence_id(), 2)

        try:
            # No other messages should be received
            consumer.receive(timeout_millis=1000)
            self.assertTrue(False)
        except:
            # Exception is expected
            pass

    def test_message_argument_errors(self):
        client = Client(self.serviceUrl)
        topic = 'persistent://sample/standalone/ns1/my-python-test-producer'
        producer = client.create_producer(topic)

        content = 'test'.encode('utf-8')

        self._check_value_error(lambda: producer.send(5))
        self._check_value_error(lambda: producer.send(content, properties='test'))
        self._check_value_error(lambda: producer.send(content, partition_key=5))
        self._check_value_error(lambda: producer.send(content, sequence_id='test'))
        self._check_value_error(lambda: producer.send(content, replication_clusters=5))
        self._check_value_error(lambda: producer.send(content, disable_replication='test'))
        client.close()

    def test_client_argument_errors(self):
        self._check_value_error(lambda: Client(None))
        self._check_value_error(lambda: Client(self.serviceUrl, authentication="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, operation_timeout_seconds="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, io_threads="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, message_listener_threads="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, concurrent_lookup_requests="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, log_conf_file_path=5))
        self._check_value_error(lambda: Client(self.serviceUrl, use_tls="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, tls_trust_certs_file_path=5))
        self._check_value_error(lambda: Client(self.serviceUrl, tls_allow_insecure_connection="test"))

    def test_producer_argument_errors(self):
        client = Client(self.serviceUrl)

        self._check_value_error(lambda: client.create_producer(None))

        topic = 'persistent://sample/standalone/ns1/my-python-test-producer'

        self._check_value_error(lambda: client.create_producer(topic, producer_name=5))
        self._check_value_error(lambda: client.create_producer(topic, initial_sequence_id='test'))
        self._check_value_error(lambda: client.create_producer(topic, send_timeout_millis='test'))
        self._check_value_error(lambda: client.create_producer(topic, compression_type=None))
        self._check_value_error(lambda: client.create_producer(topic, max_pending_messages='test'))
        self._check_value_error(lambda: client.create_producer(topic, block_if_queue_full='test'))
        self._check_value_error(lambda: client.create_producer(topic, batching_enabled='test'))
        self._check_value_error(lambda: client.create_producer(topic, batching_enabled='test'))
        self._check_value_error(lambda: client.create_producer(topic, batching_max_allowed_size_in_bytes='test'))
        self._check_value_error(lambda: client.create_producer(topic, batching_max_publish_delay_ms='test'))
        client.close()

    def test_consumer_argument_errors(self):
        client = Client(self.serviceUrl)

        topic = 'persistent://sample/standalone/ns1/my-python-test-producer'
        sub_name = 'my-sub-name'

        self._check_value_error(lambda: client.subscribe(None, sub_name))
        self._check_value_error(lambda: client.subscribe(topic, None))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, consumer_type=None))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, receiver_queue_size='test'))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, consumer_name=5))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, unacked_messages_timeout_ms='test'))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, broker_consumer_stats_cache_time_ms='test'))
        client.close()

    def test_reader_argument_errors(self):
        client = Client(self.serviceUrl)
        topic = 'persistent://sample/standalone/ns1/my-python-test-producer'

        # This should not raise exception
        client.create_reader(topic, MessageId.earliest)

        self._check_value_error(lambda: client.create_reader(None, MessageId.earliest))
        self._check_value_error(lambda: client.create_reader(topic, None))
        self._check_value_error(lambda: client.create_reader(topic, MessageId.earliest, receiver_queue_size='test'))
        self._check_value_error(lambda: client.create_reader(topic, MessageId.earliest, reader_name=5))
        client.close()

    def _check_value_error(self, fun):
        try:
            fun()
            # Should throw exception
            self.assertTrue(False)
        except ValueError:
            pass  # Expected


if __name__ == '__main__':
    main()
