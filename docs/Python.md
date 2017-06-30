
# Pulsar Python client library

Pulsar Python client library is based on the existing C++ client library.
All the same features are exposed through Python interface.

## Install

First follow the instructions to compile the Pulsar C++ client library.
That will also build the Python binding for the library.

Currently the only supported Python version is 2.7.

To install the Python bindings:

```shell
$ cd pulsar-client-cpp/python
$ sudo python setup.py install
```

## Examples

#### Producer example

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                'persistent://sample/standalone/ns/my-topic')

for i in range(10):
    producer.send('Hello-%d' % i)

client.close()
```

##### Consumer Example

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe(
        'persistent://sample/standalone/ns/my-topic',
        'my-sub')

while True:
    msg = consumer.receive()
    print("Received message '%s' id='%s'", msg.data(), msg.message_id())
    consumer.acknowledge(msg)

client.close()
```

#### Async Producer example

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                'persistent://sample/standalone/ns/my-topic',
                block_if_queue_full=True,
                batching_enabled=True,
                batching_max_publish_delay_ms=10
            )

def send_callback(res, msg):
    print('Message published res=%s', res)

while True:
    producer.send_async('Hello-%d' % i, send_callback)

client.close()
```

#### PyDoc API Reference

```
CLASSES
    Authentication
    Client
    Consumer
    Producer

    class Authentication
     |  Authentication provider object
     |  
     |  Methods defined here:
     |  
     |  __init__(self, dynamicLibPath, authParamsString)
     |      Create the authentication provider instance
     |      
     |      -- Args --
     |      dynamicLibPath   -- Path to the authentication provider shared library (eg: 'tls.so')
     |      authParamsString -- comma separated list of provider specific configuration params

    class Client
     |  Pulsar client.
     |  
     |  A single client instance can be used to create producers and consumers on
     |  multiple topics.
     |  
     |  The client will share the same connection pool and threads across all the
     |  producers and consumers.
     |  
     |  Methods defined here:
     |  
     |  __init__(self, service_url, authentication=None, operation_timeout_seconds=30,
     |      io_threads=1, message_listener_threads=1, concurrent_lookup_requests=5000,
     |      log_conf_file_path=None, use_tls=False, tls_trust_certs_file_path=None,
     |      tls_allow_insecure_connection=False)
     |      Create a new Pulsar client instance
     |      
     |      -- Args --
     |      service_url  -- The Pulsar service url eg: pulsar://my-broker.com:6650/
     |      
     |      -- Options --
     |      authentication                -- Set the authentication provider to be used with the broker
     |      operation_timeout_seconds     -- Set timeout on client operations (subscribe, create
     |                                       producer, close, unsubscribe)
     |      io_threads                    -- Set the number of IO threads to be used by the Pulsar client.
     |      message_listener_threads      -- Set the number of threads to be used by the Pulsar client
     |                                       when delivering messages through message listener. Default
     |                                       is 1 thread per Pulsar client.
     |                                       If using more than 1 thread, messages for distinct
     |                                       message_listener will be delivered in different threads,
     |                                       however a single MessageListener will always be assigned to
     |                                       the same thread.
     |      concurrent_lookup_requests    -- Number of concurrent lookup-requests allowed on each
     |                                       broker-connection to prevent overload on broker.
     |      log_conf_file_path            -- Initialize log4cxx from a conf file
     |      use_tls                       -- configure whether to use TLS encryption on the connection
     |      tls_trust_certs_file_path     -- Set the path to the trusted TLS certificate file
     |      tls_allow_insecure_connection -- Configure whether the Pulsar client accept untrusted TLS
     |                                       certificate from broker
     |  
     |  close(self)
     |      Close the client and all the associated producers and consumers
     |  
     |  create_producer(self, topic, send_timeout_millis=30000,
     |                  compression_type=_pulsar.CompressionType.None, max_pending_messages=1000,
     |                  block_if_queue_full=False, batching_enabled=False, batching_max_messages=1000,
     |                  batching_max_allowed_size_in_bytes=131072, batching_max_publish_delay_ms=10)
     |      Create a new producer on a given topic
     |      
     |      -- Args --
     |      topic                -- the topic name
     |      
     |      -- Options --
     |      send_timeout_seconds -- If a message is not acknowledged by the server before the
     |                              send_timeout expires, an error will be reported
     |      compression_type     -- Set the compression type for the producer. By default, message
     |                              payloads are not compressed. Supported compression types are:
     |                              CompressionType.LZ4 and CompressionType.ZLib
     |      max_pending_messages -- Set the max size of the queue holding the messages pending to
     |                              receive an acknowledgment from the broker.
     |      block_if_queue_full  -- Set whether send_async} operations should block when the outgoing
     |                              message queue is full.
     |      message_routing_mode -- Set the message routing mode for the partitioned producer
     |      
     |      return a new Producer object
     |  
     |  subscribe(self, topic, subscription_name,
     |            consumer_type=_pulsar.ConsumerType.Exclusive, message_listener=None,
     |            receiver_queue_size=1000, consumer_name=None,
     |            unacked_messages_timeout_ms=None, broker_consumer_stats_cache_time_ms=30000)
     |      Subscribe to the given topic and subscription combination
     |      
     |      -- Args --
     |      topic        -- The name of the topic
     |      subscription -- The name of the subscription
     |      
     |      -- Options --
     |      consumer_type       -- Select the subscription type to be used when subscribing to the topic.
     |      message_listener    -- Sets a message listener for the consumer. When the listener is set,
     |                             application will receive messages through it. Calls to
     |                             consumer.receive() will not be allowed.
     |      receiver_queue_size -- Sets the size of the consumer receive queue. The consumer receive
     |                              ueue controls how many messages can be accumulated by the Consumer
     |                              before the application calls receive(). Using a higher value could
     |                              potentially increase the consumer throughput at the expense of bigger
     |                              memory utilization.
     |      
     |                              Setting the consumer queue size as zero decreases the throughput of
     |                              the consumer, by disabling pre-fetching of messages. This approach
     |                              improves the message distribution on shared subscription, by pushing
     |                              messages only to the consumers that are ready to process them.
     |                              Neither receive with timeout nor Partitioned Topics can be used if
     |                              the consumer queue size is zero. The receive() function call should
     |                              not be interrupted when the consumer queue size is zero. Default
     |                              value is 1000 messages and should be good for most use cases.
     |      consumer_name        -- Sets the consumer name
     |      unacked_messages_timeout_ms  -- Set the timeout in milliseconds for unacknowledged messages,
     |                                      the timeout needs to be greater than 10 seconds. An
     |                                      Exception is thrown if the given value is less than 10 seconds
     |                                      If a successful acknowledgement is not sent within the
     |                                      timeout all the unacknowledged messages are redelivered.
     |      broker_consumer_stats_cache_time_ms -- Set the time duration for which the broker side
     |                                             consumer stats will be cached in the client.
     |      
     |      return a new Consumer object

    class Consumer
     |  Pulsar consumer
     |  
     |  Methods defined here:
     |  
     |  acknowledge(self, message)
     |      Acknowledge the reception of a single message.
     |      
     |      This method will block until an acknowledgement is sent to the broker.
     |      After that, the message will not be re-delivered to this consumer.
     |      
     |      message -- The received message or message id
     |  
     |  acknowledge_cumulative(self, message)
     |      Acknowledge the reception of all the messages in the stream up to (and
     |      including) the provided message.
     |      
     |      This method will block until an acknowledgement is sent to the broker.
     |      After that, the messages will not be re-delivered to this consumer.
     |      
     |      message -- The received message or message id
     |  
     |  close(self)
     |      Close the consumer
     |  
     |  pause_message_listener(self)
     |      Pause receiving messages via the message_listener, until
     |      resume_message_listener() is called.
     |  
     |  receive(self, timeout_millis=None)
     |      Receive a single message.
     |      
     |      If a message is not immediately available, this method will block until
     |      a new message is available.
     |      
     |      timeout_millis -- If specified, the receive will raise an exception if
     |                        a message is not availble withing the timeout
     |  
     |  redeliver_unacknowledged_messages(self)
     |      Redelivers all the unacknowledged messages. In Failover mode, the request
     |      is ignored if the consumer is not active for the given topic. In Shared
     |      mode, the consumers messages to be redelivered are distributed across all
     |      the connected consumers. This is a non blocking call and doesn't throw an
     |      exception. In case the connection breaks, the messages are redelivered
     |      after reconnect.
     |  
     |  resume_message_listener(self)
     |      Resume receiving the messages via the messageListener.
     |      Asynchronously receive all the messages enqueued from time
     |      pause_message_listener() was called.
     |  
     |  subscription_name(self)
     |      return the subscription name
     |  
     |  topic(self)
     |      return the topic this consumer is subscribed to
     |  
     |  unsubscribe(self)
     |      Unsubscribe the current consumer from the topic.
     |      
     |      This method will block until the operation is completed. Once the consumer
     |      is unsubscribed, no more messages will be received and subsequent new
     |      messages will not be retained for this consumer.
     |      
     |      This consumer object cannot be reused.

 class Producer
      |  Producer object
      |  
      |  The producer is used to publish messages on a topic
      |  
      |  Methods defined here:
      |  
      |  close(self)
      |      Close the producer
      |  
      |  send(self, content, properties=None, partition_key=None, replication_clusters=None,
      |       disable_replication=False)
      |      Publish a message on the topic. Blocks until the message is acknowledged
      |      
      |      -- Args --
      |      content -- A string with the message payload
      |      
      |      -- Options --
      |      properties           -- dict of application defined string properties
      |      partition_key        -- set partition key for the message routing. Hash of this key is used to
      |                              determine message's destination partition
      |      replication_clusters -- override namespace replication clusters.  note that it is the
      |                              caller's responsibility to provide valid cluster names, and that
      |                              all clusters have been previously configured as destinations.
      |                              Given an empty list, the message will replicate per the namespace
      |                              configuration.
      |      disable_replication  -- Do not replicate this message
      |  
      |  send_async(self, content, callback, properties=None, partition_key=None,
      |             replication_clusters=None, disable_replication=False)
      |      Send a message asynchronously
      |      
      |      The `callback` will be invoked once the message has been acknowledged
      |      by the broker.
      |      
      |      Example:
      |      def callback(res, msg):
      |          print 'Message published:', res
      |      
      |      producer.send_async(msg, callback)
      |      
      |      When the producer queue is full, by default the message will be rejected
      |      and the callback invoked with an error code.
      |      
      |      -- Args --
      |      content -- A string with the message payload
      |      
      |      -- Options --
      |      properties           -- dict of application defined string properties
      |      partition_key        -- set partition key for the message routing. Hash of this key is used to
      |                              determine message's destination partition
      |      replication_clusters -- override namespace replication clusters.  note that it is the
      |                              caller's responsibility to provide valid cluster names, and that
      |                              all clusters have been previously configured as destinations.
      |                              Given an empty list, the message will replicate per the namespace
      |                              configuration.
      |      disable_replication  -- Do not replicate this message

```
