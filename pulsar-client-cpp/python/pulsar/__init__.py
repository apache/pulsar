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

"""
The Pulsar Python client library is based on the existing C++ client library.
All the same features are exposed through the Python interface.

Currently, the supported Python versions are 2.7, 3.5, 3.6, 3.7 and 3.8.

## Install from PyPI

Download Python wheel binary files for MacOS and Linux
directly from the PyPI archive.

    #!shell
    $ sudo pip install pulsar-client

## Install from sources

Follow the instructions to compile the Pulsar C++ client library. This method
will also build the Python binding for the library.

To install the Python bindings:

    #!shell
    $ cd pulsar-client-cpp/python
    $ sudo python setup.py install

## Examples

### [Producer](#pulsar.Producer) example

    #!python
    import pulsar

    client = pulsar.Client('pulsar://localhost:6650')

    producer = client.create_producer('my-topic')

    for i in range(10):
        producer.send(('Hello-%d' % i).encode('utf-8'))

    client.close()

#### [Consumer](#pulsar.Consumer) Example

    #!python
    import pulsar

    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe('my-topic', 'my-subscription')

    while True:
        msg = consumer.receive()
        try:
            print("Received message '%s' id='%s'", msg.data().decode('utf-8'), msg.message_id())
            consumer.acknowledge(msg)
        except:
            consumer.negative_acknowledge(msg)

    client.close()

### [Async producer](#pulsar.Producer.send_async) example

    #!python
    import pulsar

    client = pulsar.Client('pulsar://localhost:6650')

    producer = client.create_producer(
                    'my-topic',
                    block_if_queue_full=True,
                    batching_enabled=True,
                    batching_max_publish_delay_ms=10
                )

    def send_callback(res, msg_id):
        print('Message published res=%s', res)

    while True:
        producer.send_async(('Hello-%d' % i).encode('utf-8'), send_callback)

    client.close()
"""

import _pulsar

from _pulsar import Result, CompressionType, ConsumerType, InitialPosition, PartitionsRoutingMode  # noqa: F401

from pulsar.functions.function import Function
from pulsar.functions.context import Context
from pulsar.functions.serde import SerDe, IdentitySerDe, PickleSerDe
from pulsar import schema
_schema = schema

import re
_retype = type(re.compile('x'))

import certifi
from datetime import timedelta


class MessageId:
    """
    Represents a message id
    """

    def __init__(self, partition=-1, ledger_id=-1, entry_id=-1, batch_index=-1):
        self._msg_id = _pulsar.MessageId(partition, ledger_id, entry_id, batch_index)

    'Represents the earliest message stored in a topic'
    earliest = _pulsar.MessageId.earliest

    'Represents the latest message published on a topic'
    latest = _pulsar.MessageId.latest

    def ledger_id(self):
        return self._msg_id.ledger_id()

    def entry_id(self):
        return self._msg_id.entry_id()

    def batch_index(self):
        return self._msg_id.batch_index()

    def partition(self):
        return self._msg_id.partition()

    def serialize(self):
        """
        Returns a bytes representation of the message id.
        This bytes sequence can be stored and later deserialized.
        """
        return self._msg_id.serialize()

    @staticmethod
    def deserialize(message_id_bytes):
        """
        Deserialize a message id object from a previously
        serialized bytes sequence.
        """
        return _pulsar.MessageId.deserialize(message_id_bytes)


class Message:
    """
    Message objects are returned by a consumer, either by calling `receive` or
    through a listener.
    """

    def data(self):
        """
        Returns object typed bytes with the payload of the message.
        """
        return self._message.data()

    def value(self):
        """
        Returns object with the de-serialized version of the message content
        """
        return self._schema.decode(self._message.data())

    def properties(self):
        """
        Return the properties attached to the message. Properties are
        application-defined key/value pairs that will be attached to the
        message.
        """
        return self._message.properties()

    def partition_key(self):
        """
        Get the partitioning key for the message.
        """
        return self._message.partition_key()

    def publish_timestamp(self):
        """
        Get the timestamp in milliseconds with the message publish time.
        """
        return self._message.publish_timestamp()

    def event_timestamp(self):
        """
        Get the timestamp in milliseconds with the message event time.
        """
        return self._message.event_timestamp()

    def message_id(self):
        """
        The message ID that can be used to refere to this particular message.
        """
        return self._message.message_id()

    def topic_name(self):
        """
        Get the topic Name from which this message originated from
        """
        return self._message.topic_name()

    def redelivery_count(self):
        """
        Get the redelivery count for this message
        """
        return self._message.redelivery_count()

    def schema_version(self):
        """
        Get the schema version for this message
        """
        return self._message.schema_version()

    @staticmethod
    def _wrap(_message):
        self = Message()
        self._message = _message
        return self


class MessageBatch:

    def __init__(self):
        self._msg_batch = _pulsar.MessageBatch()

    def with_message_id(self, msg_id):
        if not isinstance(msg_id, _pulsar.MessageId):
            if isinstance(msg_id, MessageId):
                msg_id = msg_id._msg_id
            else:
                raise TypeError("unknown message id type")
        self._msg_batch.with_message_id(msg_id)
        return self

    def parse_from(self, data, size):
        self._msg_batch.parse_from(data, size)
        _msgs = self._msg_batch.messages()
        return list(map(Message._wrap, _msgs))


class Authentication:
    """
    Authentication provider object. Used to load authentication from an external
    shared library.
    """
    def __init__(self, dynamicLibPath, authParamsString):
        """
        Create the authentication provider instance.

        **Args**

        * `dynamicLibPath`: Path to the authentication provider shared library
          (such as `tls.so`)
        * `authParamsString`: Comma-separated list of provider-specific
          configuration params
        """
        _check_type(str, dynamicLibPath, 'dynamicLibPath')
        _check_type(str, authParamsString, 'authParamsString')
        self.auth = _pulsar.Authentication(dynamicLibPath, authParamsString)


class AuthenticationTLS(Authentication):
    """
    TLS Authentication implementation
    """
    def __init__(self, certificate_path, private_key_path):
        """
        Create the TLS authentication provider instance.

        **Args**

        * `certificatePath`: Path to the public certificate
        * `privateKeyPath`: Path to private TLS key
        """
        _check_type(str, certificate_path, 'certificate_path')
        _check_type(str, private_key_path, 'private_key_path')
        self.auth = _pulsar.AuthenticationTLS(certificate_path, private_key_path)


class AuthenticationToken(Authentication):
    """
    Token based authentication implementation
    """
    def __init__(self, token):
        """
        Create the token authentication provider instance.

        **Args**

        * `token`: A string containing the token or a functions that provides a
                   string with the token
        """
        if not (isinstance(token, str) or callable(token)):
            raise ValueError("Argument token is expected to be of type 'str' or a function returning 'str'")
        self.auth = _pulsar.AuthenticationToken(token)


class AuthenticationAthenz(Authentication):
    """
    Athenz Authentication implementation
    """
    def __init__(self, auth_params_string):
        """
        Create the Athenz authentication provider instance.

        **Args**

        * `auth_params_string`: JSON encoded configuration for Athenz client
        """
        _check_type(str, auth_params_string, 'auth_params_string')
        self.auth = _pulsar.AuthenticationAthenz(auth_params_string)

class AuthenticationOauth2(Authentication):
    """
    Oauth2 Authentication implementation
    """
    def __init__(self, auth_params_string):
        """
        Create the Oauth2 authentication provider instance.

        **Args**

        * `auth_params_string`: JSON encoded configuration for Oauth2 client
        """
        _check_type(str, auth_params_string, 'auth_params_string')
        self.auth = _pulsar.AuthenticationOauth2(auth_params_string)

class Client:
    """
    The Pulsar client. A single client instance can be used to create producers
    and consumers on multiple topics.

    The client will share the same connection pool and threads across all
    producers and consumers.
    """

    def __init__(self, service_url,
                 authentication=None,
                 operation_timeout_seconds=30,
                 io_threads=1,
                 message_listener_threads=1,
                 concurrent_lookup_requests=50000,
                 log_conf_file_path=None,
                 use_tls=False,
                 tls_trust_certs_file_path=None,
                 tls_allow_insecure_connection=False,
                 tls_validate_hostname=False,
                 ):
        """
        Create a new Pulsar client instance.

        **Args**

        * `service_url`: The Pulsar service url eg: pulsar://my-broker.com:6650/

        **Options**

        * `authentication`:
          Set the authentication provider to be used with the broker. For example:
          `AuthenticationTls`, AuthenticaionToken, `AuthenticationAthenz`or `AuthenticationOauth2`
        * `operation_timeout_seconds`:
          Set timeout on client operations (subscribe, create producer, close,
          unsubscribe).
        * `io_threads`:
          Set the number of IO threads to be used by the Pulsar client.
        * `message_listener_threads`:
          Set the number of threads to be used by the Pulsar client when
          delivering messages through message listener. The default is 1 thread
          per Pulsar client. If using more than 1 thread, messages for distinct
          `message_listener`s will be delivered in different threads, however a
          single `MessageListener` will always be assigned to the same thread.
        * `concurrent_lookup_requests`:
          Number of concurrent lookup-requests allowed on each broker connection
          to prevent overload on the broker.
        * `log_conf_file_path`:
          Initialize log4cxx from a configuration file.
        * `use_tls`:
          Configure whether to use TLS encryption on the connection. This setting
          is deprecated. TLS will be automatically enabled if the `serviceUrl` is
          set to `pulsar+ssl://` or `https://`
        * `tls_trust_certs_file_path`:
          Set the path to the trusted TLS certificate file. If empty defaults to
          certifi.
        * `tls_allow_insecure_connection`:
          Configure whether the Pulsar client accepts untrusted TLS certificates
          from the broker.
        * `tls_validate_hostname`:
          Configure whether the Pulsar client validates that the hostname of the
          endpoint, matches the common name on the TLS certificate presented by
          the endpoint.
        """
        _check_type(str, service_url, 'service_url')
        _check_type_or_none(Authentication, authentication, 'authentication')
        _check_type(int, operation_timeout_seconds, 'operation_timeout_seconds')
        _check_type(int, io_threads, 'io_threads')
        _check_type(int, message_listener_threads, 'message_listener_threads')
        _check_type(int, concurrent_lookup_requests, 'concurrent_lookup_requests')
        _check_type_or_none(str, log_conf_file_path, 'log_conf_file_path')
        _check_type(bool, use_tls, 'use_tls')
        _check_type_or_none(str, tls_trust_certs_file_path, 'tls_trust_certs_file_path')
        _check_type(bool, tls_allow_insecure_connection, 'tls_allow_insecure_connection')
        _check_type(bool, tls_validate_hostname, 'tls_validate_hostname')

        conf = _pulsar.ClientConfiguration()
        if authentication:
            conf.authentication(authentication.auth)
        conf.operation_timeout_seconds(operation_timeout_seconds)
        conf.io_threads(io_threads)
        conf.message_listener_threads(message_listener_threads)
        conf.concurrent_lookup_requests(concurrent_lookup_requests)
        if log_conf_file_path:
            conf.log_conf_file_path(log_conf_file_path)
        if use_tls or service_url.startswith('pulsar+ssl://') or service_url.startswith('https://'):
            conf.use_tls(True)
        if tls_trust_certs_file_path:
            conf.tls_trust_certs_file_path(tls_trust_certs_file_path)
        else:
            conf.tls_trust_certs_file_path(certifi.where())
        conf.tls_allow_insecure_connection(tls_allow_insecure_connection)
        conf.tls_validate_hostname(tls_validate_hostname)
        self._client = _pulsar.Client(service_url, conf)
        self._consumers = []

    def create_producer(self, topic,
                        producer_name=None,
                        schema=schema.BytesSchema(),
                        initial_sequence_id=None,
                        send_timeout_millis=30000,
                        compression_type=CompressionType.NONE,
                        max_pending_messages=1000,
                        max_pending_messages_across_partitions=50000,
                        block_if_queue_full=False,
                        batching_enabled=False,
                        batching_max_messages=1000,
                        batching_max_allowed_size_in_bytes=128*1024,
                        batching_max_publish_delay_ms=10,
                        message_routing_mode=PartitionsRoutingMode.RoundRobinDistribution,
                        properties=None,
                        ):
        """
        Create a new producer on a given topic.

        **Args**

        * `topic`:
          The topic name

        **Options**

        * `producer_name`:
           Specify a name for the producer. If not assigned,
           the system will generate a globally unique name which can be accessed
           with `Producer.producer_name()`. When specifying a name, it is app to
           the user to ensure that, for a given topic, the producer name is unique
           across all Pulsar's clusters.
        * `schema`:
           Define the schema of the data that will be published by this producer.
           The schema will be used for two purposes:
             - Validate the data format against the topic defined schema
             - Perform serialization/deserialization between data and objects
           An example for this parameter would be to pass `schema=JsonSchema(MyRecordClass)`.
        * `initial_sequence_id`:
           Set the baseline for the sequence ids for messages
           published by the producer. First message will be using
           `(initialSequenceId + 1)`` as its sequence id and subsequent messages will
           be assigned incremental sequence ids, if not otherwise specified.
        * `send_timeout_seconds`:
          If a message is not acknowledged by the server before the
          `send_timeout` expires, an error will be reported.
        * `compression_type`:
          Set the compression type for the producer. By default, message
          payloads are not compressed. Supported compression types are
          `CompressionType.LZ4`, `CompressionType.ZLib`, `CompressionType.ZSTD` and `CompressionType.SNAPPY`.
          ZSTD is supported since Pulsar 2.3. Consumers will need to be at least at that
          release in order to be able to receive messages compressed with ZSTD.
          SNAPPY is supported since Pulsar 2.4. Consumers will need to be at least at that
          release in order to be able to receive messages compressed with SNAPPY.
        * `max_pending_messages`:
          Set the max size of the queue holding the messages pending to receive
          an acknowledgment from the broker.
        * `max_pending_messages_across_partitions`:
          Set the max size of the queue holding the messages pending to receive
          an acknowledgment across partitions from the broker.
        * `block_if_queue_full`: Set whether `send_async` operations should
          block when the outgoing message queue is full.
        * `message_routing_mode`:
          Set the message routing mode for the partitioned producer. Default is `PartitionsRoutingMode.RoundRobinDistribution`,
          other option is `PartitionsRoutingMode.UseSinglePartition`
        * `properties`:
          Sets the properties for the producer. The properties associated with a producer
          can be used for identify a producer at broker side.
        """
        _check_type(str, topic, 'topic')
        _check_type_or_none(str, producer_name, 'producer_name')
        _check_type(_schema.Schema, schema, 'schema')
        _check_type_or_none(int, initial_sequence_id, 'initial_sequence_id')
        _check_type(int, send_timeout_millis, 'send_timeout_millis')
        _check_type(CompressionType, compression_type, 'compression_type')
        _check_type(int, max_pending_messages, 'max_pending_messages')
        _check_type(int, max_pending_messages_across_partitions, 'max_pending_messages_across_partitions')
        _check_type(bool, block_if_queue_full, 'block_if_queue_full')
        _check_type(bool, batching_enabled, 'batching_enabled')
        _check_type(int, batching_max_messages, 'batching_max_messages')
        _check_type(int, batching_max_allowed_size_in_bytes, 'batching_max_allowed_size_in_bytes')
        _check_type(int, batching_max_publish_delay_ms, 'batching_max_publish_delay_ms')
        _check_type_or_none(dict, properties, 'properties')

        conf = _pulsar.ProducerConfiguration()
        conf.send_timeout_millis(send_timeout_millis)
        conf.compression_type(compression_type)
        conf.max_pending_messages(max_pending_messages)
        conf.max_pending_messages_across_partitions(max_pending_messages_across_partitions)
        conf.block_if_queue_full(block_if_queue_full)
        conf.batching_enabled(batching_enabled)
        conf.batching_max_messages(batching_max_messages)
        conf.batching_max_allowed_size_in_bytes(batching_max_allowed_size_in_bytes)
        conf.batching_max_publish_delay_ms(batching_max_publish_delay_ms)
        conf.partitions_routing_mode(message_routing_mode)
        if producer_name:
            conf.producer_name(producer_name)
        if initial_sequence_id:
            conf.initial_sequence_id(initial_sequence_id)
        if properties:
            for k, v in properties.items():
                conf.property(k, v)

        conf.schema(schema.schema_info())

        p = Producer()
        p._producer = self._client.create_producer(topic, conf)
        p._schema = schema
        return p

    def subscribe(self, topic, subscription_name,
                  consumer_type=ConsumerType.Exclusive,
                  schema=schema.BytesSchema(),
                  message_listener=None,
                  receiver_queue_size=1000,
                  max_total_receiver_queue_size_across_partitions=50000,
                  consumer_name=None,
                  unacked_messages_timeout_ms=None,
                  broker_consumer_stats_cache_time_ms=30000,
                  negative_ack_redelivery_delay_ms=60000,
                  is_read_compacted=False,
                  properties=None,
                  pattern_auto_discovery_period=60,
                  initial_position=InitialPosition.Latest
                  ):
        """
        Subscribe to the given topic and subscription combination.

        **Args**

        * `topic`: The name of the topic, list of topics or regex pattern.
                  This method will accept these forms:
                    - `topic='my-topic'`
                    - `topic=['topic-1', 'topic-2', 'topic-3']`
                    - `topic=re.compile('persistent://public/default/topic-*')`
        * `subscription`: The name of the subscription.

        **Options**

        * `consumer_type`:
          Select the subscription type to be used when subscribing to the topic.
        * `schema`:
           Define the schema of the data that will be received by this consumer.
        * `message_listener`:
          Sets a message listener for the consumer. When the listener is set,
          the application will receive messages through it. Calls to
          `consumer.receive()` will not be allowed. The listener function needs
          to accept (consumer, message), for example:

                #!python
                def my_listener(consumer, message):
                    # process message
                    consumer.acknowledge(message)

        * `receiver_queue_size`:
          Sets the size of the consumer receive queue. The consumer receive
          queue controls how many messages can be accumulated by the consumer
          before the application calls `receive()`. Using a higher value could
          potentially increase the consumer throughput at the expense of higher
          memory utilization. Setting the consumer queue size to zero decreases
          the throughput of the consumer by disabling pre-fetching of messages.
          This approach improves the message distribution on shared subscription
          by pushing messages only to those consumers that are ready to process
          them. Neither receive with timeout nor partitioned topics can be used
          if the consumer queue size is zero. The `receive()` function call
          should not be interrupted when the consumer queue size is zero. The
          default value is 1000 messages and should work well for most use
          cases.
        * `max_total_receiver_queue_size_across_partitions`
          Set the max total receiver queue size across partitions.
          This setting will be used to reduce the receiver queue size for individual partitions
        * `consumer_name`:
          Sets the consumer name.
        * `unacked_messages_timeout_ms`:
          Sets the timeout in milliseconds for unacknowledged messages. The
          timeout needs to be greater than 10 seconds. An exception is thrown if
          the given value is less than 10 seconds. If a successful
          acknowledgement is not sent within the timeout, all the unacknowledged
          messages are redelivered.
        * `negative_ack_redelivery_delay_ms`:
           The delay after which to redeliver the messages that failed to be
           processed (with the `consumer.negative_acknowledge()`)
        * `broker_consumer_stats_cache_time_ms`:
          Sets the time duration for which the broker-side consumer stats will
          be cached in the client.
        * `is_read_compacted`:
          Selects whether to read the compacted version of the topic
        * `properties`:
          Sets the properties for the consumer. The properties associated with a consumer
          can be used for identify a consumer at broker side.
        * `pattern_auto_discovery_period`:
          Periods of seconds for consumer to auto discover match topics.
        * `initial_position`:
          Set the initial position of a consumer  when subscribing to the topic.
          It could be either: `InitialPosition.Earliest` or `InitialPosition.Latest`.
          Default: `Latest`.
        """
        _check_type(str, subscription_name, 'subscription_name')
        _check_type(ConsumerType, consumer_type, 'consumer_type')
        _check_type(_schema.Schema, schema, 'schema')
        _check_type(int, receiver_queue_size, 'receiver_queue_size')
        _check_type(int, max_total_receiver_queue_size_across_partitions,
                    'max_total_receiver_queue_size_across_partitions')
        _check_type_or_none(str, consumer_name, 'consumer_name')
        _check_type_or_none(int, unacked_messages_timeout_ms, 'unacked_messages_timeout_ms')
        _check_type(int, broker_consumer_stats_cache_time_ms, 'broker_consumer_stats_cache_time_ms')
        _check_type(int, negative_ack_redelivery_delay_ms, 'negative_ack_redelivery_delay_ms')
        _check_type(int, pattern_auto_discovery_period, 'pattern_auto_discovery_period')
        _check_type(bool, is_read_compacted, 'is_read_compacted')
        _check_type_or_none(dict, properties, 'properties')
        _check_type(InitialPosition, initial_position, 'initial_position')

        conf = _pulsar.ConsumerConfiguration()
        conf.consumer_type(consumer_type)
        conf.read_compacted(is_read_compacted)
        if message_listener:
            conf.message_listener(_listener_wrapper(message_listener, schema))
        conf.receiver_queue_size(receiver_queue_size)
        conf.max_total_receiver_queue_size_across_partitions(max_total_receiver_queue_size_across_partitions)
        if consumer_name:
            conf.consumer_name(consumer_name)
        if unacked_messages_timeout_ms:
            conf.unacked_messages_timeout_ms(unacked_messages_timeout_ms)

        conf.negative_ack_redelivery_delay_ms(negative_ack_redelivery_delay_ms)
        conf.broker_consumer_stats_cache_time_ms(broker_consumer_stats_cache_time_ms)
        if properties:
            for k, v in properties.items():
                conf.property(k, v)
        conf.subscription_initial_position(initial_position)

        conf.schema(schema.schema_info())

        c = Consumer()
        if isinstance(topic, str):
            # Single topic
            c._consumer = self._client.subscribe(topic, subscription_name, conf)
        elif isinstance(topic, list):
            # List of topics
            c._consumer = self._client.subscribe_topics(topic, subscription_name, conf)
        elif isinstance(topic, _retype):
            # Regex pattern
            c._consumer = self._client.subscribe_pattern(topic.pattern, subscription_name, conf)
        else:
            raise ValueError("Argument 'topic' is expected to be of a type between (str, list, re.pattern)")

        c._client = self
        c._schema = schema
        self._consumers.append(c)
        return c

    def create_reader(self, topic, start_message_id,
                      schema=schema.BytesSchema(),
                      reader_listener=None,
                      receiver_queue_size=1000,
                      reader_name=None,
                      subscription_role_prefix=None,
                      is_read_compacted=False
                      ):
        """
        Create a reader on a particular topic

        **Args**

        * `topic`: The name of the topic.
        * `start_message_id`: The initial reader positioning is done by specifying a message id.
           The options are:
            * `MessageId.earliest`: Start reading from the earliest message available in the topic
            * `MessageId.latest`: Start reading from the end topic, only getting messages published
               after the reader was created
            * `MessageId`: When passing a particular message id, the reader will position itself on
               that specific position. The first message to be read will be the message next to the
               specified messageId. Message id can be serialized into a string and deserialized
               back into a `MessageId` object:

                   # Serialize to string
                   s = msg.message_id().serialize()

                   # Deserialize from string
                   msg_id = MessageId.deserialize(s)

        **Options**

        * `schema`:
           Define the schema of the data that will be received by this reader.
        * `reader_listener`:
          Sets a message listener for the reader. When the listener is set,
          the application will receive messages through it. Calls to
          `reader.read_next()` will not be allowed. The listener function needs
          to accept (reader, message), for example:

                def my_listener(reader, message):
                    # process message
                    pass

        * `receiver_queue_size`:
          Sets the size of the reader receive queue. The reader receive
          queue controls how many messages can be accumulated by the reader
          before the application calls `read_next()`. Using a higher value could
          potentially increase the reader throughput at the expense of higher
          memory utilization.
        * `reader_name`:
          Sets the reader name.
        * `subscription_role_prefix`:
          Sets the subscription role prefix.
        * `is_read_compacted`:
          Selects whether to read the compacted version of the topic
        """
        _check_type(str, topic, 'topic')
        _check_type(_pulsar.MessageId, start_message_id, 'start_message_id')
        _check_type(_schema.Schema, schema, 'schema')
        _check_type(int, receiver_queue_size, 'receiver_queue_size')
        _check_type_or_none(str, reader_name, 'reader_name')
        _check_type_or_none(str, subscription_role_prefix, 'subscription_role_prefix')
        _check_type(bool, is_read_compacted, 'is_read_compacted')

        conf = _pulsar.ReaderConfiguration()
        if reader_listener:
            conf.reader_listener(_listener_wrapper(reader_listener, schema))
        conf.receiver_queue_size(receiver_queue_size)
        if reader_name:
            conf.reader_name(reader_name)
        if subscription_role_prefix:
            conf.subscription_role_prefix(subscription_role_prefix)
        conf.schema(schema.schema_info())
        conf.read_compacted(is_read_compacted)

        c = Reader()
        c._reader = self._client.create_reader(topic, start_message_id, conf)
        c._client = self
        c._schema = schema
        self._consumers.append(c)
        return c

    def get_topic_partitions(self, topic):
        """
        Get the list of partitions for a given topic.

        If the topic is partitioned, this will return a list of partition names. If the topic is not
        partitioned, the returned list will contain the topic name itself.

        This can be used to discover the partitions and create Reader, Consumer or Producer
        instances directly on a particular partition.
        :param topic: the topic name to lookup
        :return: a list of partition name
        """
        _check_type(str, topic, 'topic')
        return self._client.get_topic_partitions(topic)

    def close(self):
        """
        Close the client and all the associated producers and consumers
        """
        self._client.close()


class Producer:
    """
    The Pulsar message producer, used to publish messages on a topic.
    """

    def topic(self):
        """
        Return the topic which producer is publishing to
        """
        return self._producer.topic()

    def producer_name(self):
        """
        Return the producer name which could have been assigned by the
        system or specified by the client
        """
        return self._producer.producer_name()

    def last_sequence_id(self):
        """
        Get the last sequence id that was published by this producer.

        This represent either the automatically assigned or custom sequence id
        (set on the `MessageBuilder`) that was published and acknowledged by the broker.

        After recreating a producer with the same producer name, this will return the
        last message that was published in the previous producer session, or -1 if
        there no message was ever published.
        """
        return self._producer.last_sequence_id()

    def send(self, content,
             properties=None,
             partition_key=None,
             sequence_id=None,
             replication_clusters=None,
             disable_replication=False,
             event_timestamp=None,
             deliver_at=None,
             deliver_after=None,
             ):
        """
        Publish a message on the topic. Blocks until the message is acknowledged

        **Args**

        * `content`:
          A `bytes` object with the message payload.

        **Options**

        * `properties`:
          A dict of application-defined string properties.
        * `partition_key`:
          Sets the partition key for message routing. A hash of this key is used
          to determine the message's topic partition.
        * `sequence_id`:
          Specify a custom sequence id for the message being published.
        * `replication_clusters`:
          Override namespace replication clusters. Note that it is the caller's
          responsibility to provide valid cluster names and that all clusters
          have been previously configured as topics. Given an empty list,
          the message will replicate according to the namespace configuration.
        * `disable_replication`:
          Do not replicate this message.
        * `event_timestamp`:
          Timestamp in millis of the timestamp of event creation
        * `deliver_at`:
          Specify the this message should not be delivered earlier than the
          specified timestamp.
          The timestamp is milliseconds and based on UTC
        * `deliver_after`:
          Specify a delay in timedelta for the delivery of the messages.

        """
        msg = self._build_msg(content, properties, partition_key, sequence_id,
                              replication_clusters, disable_replication, event_timestamp,
                              deliver_at, deliver_after)
        return self._producer.send(msg)

    def send_async(self, content, callback,
                   properties=None,
                   partition_key=None,
                   sequence_id=None,
                   replication_clusters=None,
                   disable_replication=False,
                   event_timestamp=None,
                   deliver_at=None,
                   deliver_after=None,
                   ):
        """
        Send a message asynchronously.

        The `callback` will be invoked once the message has been acknowledged
        by the broker.

        Example:

            #!python
            def callback(res, msg_id):
                print('Message published: %s' % res)

            producer.send_async(msg, callback)

        When the producer queue is full, by default the message will be rejected
        and the callback invoked with an error code.

        **Args**

        * `content`:
          A `bytes` object with the message payload.

        **Options**

        * `properties`:
          A dict of application0-defined string properties.
        * `partition_key`:
          Sets the partition key for the message routing. A hash of this key is
          used to determine the message's topic partition.
        * `sequence_id`:
          Specify a custom sequence id for the message being published.
        * `replication_clusters`: Override namespace replication clusters. Note
          that it is the caller's responsibility to provide valid cluster names
          and that all clusters have been previously configured as topics.
          Given an empty list, the message will replicate per the namespace
          configuration.
        * `disable_replication`:
          Do not replicate this message.
        * `event_timestamp`:
          Timestamp in millis of the timestamp of event creation
        * `deliver_at`:
          Specify the this message should not be delivered earlier than the
          specified timestamp.
          The timestamp is milliseconds and based on UTC
        * `deliver_after`:
          Specify a delay in timedelta for the delivery of the messages.
        """
        msg = self._build_msg(content, properties, partition_key, sequence_id,
                              replication_clusters, disable_replication, event_timestamp,
                              deliver_at, deliver_after)
        self._producer.send_async(msg, callback)


    def flush(self):
        """
        Flush all the messages buffered in the client and wait until all messages have been
        successfully persisted
        """
        self._producer.flush()


    def close(self):
        """
        Close the producer.
        """
        self._producer.close()

    def _build_msg(self, content, properties, partition_key, sequence_id,
                   replication_clusters, disable_replication, event_timestamp,
                   deliver_at, deliver_after):
        data = self._schema.encode(content)

        _check_type(bytes, data, 'data')
        _check_type_or_none(dict, properties, 'properties')
        _check_type_or_none(str, partition_key, 'partition_key')
        _check_type_or_none(int, sequence_id, 'sequence_id')
        _check_type_or_none(list, replication_clusters, 'replication_clusters')
        _check_type(bool, disable_replication, 'disable_replication')
        _check_type_or_none(int, event_timestamp, 'event_timestamp')
        _check_type_or_none(int, deliver_at, 'deliver_at')
        _check_type_or_none(timedelta, deliver_after, 'deliver_after')

        mb = _pulsar.MessageBuilder()
        mb.content(data)
        if properties:
            for k, v in properties.items():
                mb.property(k, v)
        if partition_key:
            mb.partition_key(partition_key)
        if sequence_id:
            mb.sequence_id(sequence_id)
        if replication_clusters:
            mb.replication_clusters(replication_clusters)
        if disable_replication:
            mb.disable_replication(disable_replication)
        if event_timestamp:
            mb.event_timestamp(event_timestamp)
        if deliver_at:
            mb.deliver_at(deliver_at)
        if deliver_after:
            mb.deliver_after(deliver_after)
        
        return mb.build()


class Consumer:
    """
    Pulsar consumer.
    """

    def topic(self):
        """
        Return the topic this consumer is subscribed to.
        """
        return self._consumer.topic()

    def subscription_name(self):
        """
        Return the subscription name.
        """
        return self._consumer.subscription_name()

    def unsubscribe(self):
        """
        Unsubscribe the current consumer from the topic.

        This method will block until the operation is completed. Once the
        consumer is unsubscribed, no more messages will be received and
        subsequent new messages will not be retained for this consumer.

        This consumer object cannot be reused.
        """
        return self._consumer.unsubscribe()

    def receive(self, timeout_millis=None):
        """
        Receive a single message.

        If a message is not immediately available, this method will block until
        a new message is available.

        **Options**

        * `timeout_millis`:
          If specified, the receive will raise an exception if a message is not
          available within the timeout.
        """
        if timeout_millis is None:
            msg = self._consumer.receive()
        else:
            _check_type(int, timeout_millis, 'timeout_millis')
            msg = self._consumer.receive(timeout_millis)

        m = Message()
        m._message = msg
        m._schema = self._schema
        return m

    def acknowledge(self, message):
        """
        Acknowledge the reception of a single message.

        This method will block until an acknowledgement is sent to the broker.
        After that, the message will not be re-delivered to this consumer.

        **Args**

        * `message`:
          The received message or message id.
        """
        if isinstance(message, Message):
            self._consumer.acknowledge(message._message)
        else:
            self._consumer.acknowledge(message)

    def acknowledge_cumulative(self, message):
        """
        Acknowledge the reception of all the messages in the stream up to (and
        including) the provided message.

        This method will block until an acknowledgement is sent to the broker.
        After that, the messages will not be re-delivered to this consumer.

        **Args**

        * `message`:
          The received message or message id.
        """
        if isinstance(message, Message):
            self._consumer.acknowledge_cumulative(message._message)
        else:
            self._consumer.acknowledge_cumulative(message)

    def negative_acknowledge(self, message):
        """
        Acknowledge the failure to process a single message.

        When a message is "negatively acked" it will be marked for redelivery after
        some fixed delay. The delay is configurable when constructing the consumer
        with {@link ConsumerConfiguration#setNegativeAckRedeliveryDelayMs}.

        This call is not blocking.

        **Args**

        * `message`:
          The received message or message id.
        """
        if isinstance(message, Message):
            self._consumer.negative_acknowledge(message._message)
        else:
            self._consumer.negative_acknowledge(message)

    def pause_message_listener(self):
        """
        Pause receiving messages via the `message_listener` until
        `resume_message_listener()` is called.
        """
        self._consumer.pause_message_listener()

    def resume_message_listener(self):
        """
        Resume receiving the messages via the message listener.
        Asynchronously receive all the messages enqueued from the time
        `pause_message_listener()` was called.
        """
        self._consumer.resume_message_listener()

    def redeliver_unacknowledged_messages(self):
        """
        Redelivers all the unacknowledged messages. In failover mode, the
        request is ignored if the consumer is not active for the given topic. In
        shared mode, the consumer's messages to be redelivered are distributed
        across all the connected consumers. This is a non-blocking call and
        doesn't throw an exception. In case the connection breaks, the messages
        are redelivered after reconnect.
        """
        self._consumer.redeliver_unacknowledged_messages()

    def seek(self, messageid):
        """
        Reset the subscription associated with this consumer to a specific message id or publish timestamp.
        The message id can either be a specific message or represent the first or last messages in the topic.
        Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
        seek() on the individual partitions.

        **Args**

        * `message`:
          The message id for seek, OR an integer event time to seek to
        """
        self._consumer.seek(messageid)

    def close(self):
        """
        Close the consumer.
        """
        self._consumer.close()
        self._client._consumers.remove(self)


class Reader:
    """
    Pulsar topic reader.
    """

    def topic(self):
        """
        Return the topic this reader is reading from.
        """
        return self._reader.topic()

    def read_next(self, timeout_millis=None):
        """
        Read a single message.

        If a message is not immediately available, this method will block until
        a new message is available.

        **Options**

        * `timeout_millis`:
          If specified, the receive will raise an exception if a message is not
          available within the timeout.
        """
        if timeout_millis is None:
            msg = self._reader.read_next()
        else:
            _check_type(int, timeout_millis, 'timeout_millis')
            msg = self._reader.read_next(timeout_millis)

        m = Message()
        m._message = msg
        m._schema = self._schema
        return m

    def has_message_available(self):
        """
        Check if there is any message available to read from the current position.
        """
        return self._reader.has_message_available();

    def seek(self, messageid):
        """
        Reset this reader to a specific message id or publish timestamp.
        The message id can either be a specific message or represent the first or last messages in the topic.
        Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
        seek() on the individual partitions.

        **Args**

        * `message`:
          The message id for seek, OR an integer event time to seek to
        """
        self._reader.seek(messageid)

    def close(self):
        """
        Close the reader.
        """
        self._reader.close()
        self._client._consumers.remove(self)


def _check_type(var_type, var, name):
    if not isinstance(var, var_type):
        raise ValueError("Argument %s is expected to be of type '%s' and not '%s'"
                         % (name, var_type.__name__, type(var).__name__))


def _check_type_or_none(var_type, var, name):
    if var is not None and not isinstance(var, var_type):
        raise ValueError("Argument %s is expected to be either None or of type '%s'"
                         % (name, var_type.__name__))


def _listener_wrapper(listener, schema):
    def wrapper(consumer, msg):
        c = Consumer()
        c._consumer = consumer
        m = Message()
        m._message = msg
        m._schema = schema
        listener(c, m)
    return wrapper
