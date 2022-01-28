---
id: version-2.8.0-develop-binary-protocol
title: Pulsar binary protocol specification
sidebar_label: Binary protocol
original_id: develop-binary-protocol
---

Pulsar uses a custom binary protocol for communications between producers/consumers and brokers. This protocol is designed to support required features, such as acknowledgements and flow control, while ensuring maximum transport and implementation efficiency.

Clients and brokers exchange *commands* with each other. Commands are formatted as binary [protocol buffer](https://developers.google.com/protocol-buffers/) (aka *protobuf*) messages. The format of protobuf commands is specified in the [`PulsarApi.proto`](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto) file and also documented in the [Protobuf interface](#protobuf-interface) section below.

> ### Connection sharing
> Commands for different producers and consumers can be interleaved and sent through the same connection without restriction.

All commands associated with Pulsar's protocol are contained in a
[`BaseCommand`](#pulsar.proto.BaseCommand) protobuf message that includes a [`Type`](#pulsar.proto.Type) [enum](https://developers.google.com/protocol-buffers/docs/proto#enum) with all possible subcommands as optional fields. `BaseCommand` messages can specify only one subcommand.

## Framing

Since protobuf doesn't provide any sort of message frame, all messages in the Pulsar protocol are prepended with a 4-byte field that specifies the size of the frame. The maximum allowable size of a single frame is 5 MB.

The Pulsar protocol allows for two types of commands:

1. **Simple commands** that do not carry a message payload.
2. **Payload commands** that bear a payload that is used when publishing or delivering messages. In payload commands, the protobuf command data is followed by protobuf [metadata](#message-metadata) and then the payload, which is passed in raw format outside of protobuf. All sizes are passed as 4-byte unsigned big endian integers.

> Message payloads are passed in raw format rather than protobuf format for efficiency reasons.

### Simple commands

Simple (payload-free) commands have this basic structure:

| Component   | Description                                                                             | Size (in bytes) |
|:------------|:----------------------------------------------------------------------------------------|:----------------|
| totalSize   | The size of the frame, counting everything that comes after it (in bytes)               | 4               |
| commandSize | The size of the protobuf-serialized command                                             | 4               |
| message     | The protobuf message serialized in a raw binary format (rather than in protobuf format) |                 |

### Payload commands

Payload commands have this basic structure:

| Component    | Description                                                                                 | Size (in bytes) |
|:-------------|:--------------------------------------------------------------------------------------------|:----------------|
| totalSize    | The size of the frame, counting everything that comes after it (in bytes)                   | 4               |
| commandSize  | The size of the protobuf-serialized command                                                 | 4               |
| message      | The protobuf message serialized in a raw binary format (rather than in protobuf format)     |                 |
| magicNumber  | A 2-byte byte array (`0x0e01`) identifying the current format                               | 2               |
| checksum     | A [CRC32-C checksum](http://www.evanjones.ca/crc32c.html) of everything that comes after it | 4               |
| metadataSize | The size of the message [metadata](#message-metadata)                                       | 4               |
| metadata     | The message [metadata](#message-metadata) stored as a binary protobuf message               |                 |
| payload      | Anything left in the frame is considered the payload and can include any sequence of bytes  |                 |

## Message metadata

Message metadata is stored alongside the application-specified payload as a serialized protobuf message. Metadata is created by the producer and passed on unchanged to the consumer.

| Field                                | Description                                                                                                                                                                                                                                               |
|:-------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `producer_name`                      | The name of the producer that published the message                                                                                                                                                                                         |
| `sequence_id`                        | The sequence ID of the message, assigned by producer                                                                                                                                                                                        |
| `publish_time`                       | The publish timestamp in Unix time (i.e. as the number of milliseconds since January 1st, 1970 in UTC)                                                                                                                                                    |
| `properties`                         | A sequence of key/value pairs (using the [`KeyValue`](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto#L32) message). These are application-defined keys and values with no special meaning to Pulsar. |
| `replicated_from` *(optional)*       | Indicates that the message has been replicated and specifies the name of the [cluster](reference-terminology.md#cluster) where the message was originally published                                                                                                             |
| `partition_key` *(optional)*         | While publishing on a partition topic, if the key is present, the hash of the key is used to determine which partition to choose                                                                                                                          |
| `compression` *(optional)*           | Signals that payload has been compressed and with which compression library                                                                                                                                                                               |
| `uncompressed_size` *(optional)*     | If compression is used, the producer must fill the uncompressed size field with the original payload size                                                                                                                                                 |
| `num_messages_in_batch` *(optional)* | If this message is really a [batch](#batch-messages) of multiple entries, this field must be set to the number of messages in the batch                                                                                                                   |

### Batch messages

When using batch messages, the payload will be containing a list of entries,
each of them with its individual metadata, defined by the `SingleMessageMetadata`
object.


For a single batch, the payload format will look like this:


| Field         | Description                                                 |
|:--------------|:------------------------------------------------------------|
| metadataSizeN | The size of the single message metadata serialized Protobuf |
| metadataN     | Single message metadata                                     |
| payloadN      | Message payload passed by application                       |

Each metadata field looks like this;

| Field                      | Description                                             |
|:---------------------------|:--------------------------------------------------------|
| properties                 | Application-defined properties                          |
| partition key *(optional)* | Key to indicate the hashing to a particular partition   |
| payload_size               | Size of the payload for the single message in the batch |

When compression is enabled, the whole batch will be compressed at once.

## Interactions

### Connection establishment

After opening a TCP connection to a broker, typically on port 6650, the client
is responsible to initiate the session.

![Connect interaction](assets/binary-protocol-connect.png)

After receiving a `Connected` response from the broker, the client can
consider the connection ready to use. Alternatively, if the broker doesn't
validate the client authentication, it will reply with an `Error` command and
close the TCP connection.

Example:

```protobuf
message CommandConnect {
  "client_version" : "Pulsar-Client-Java-v1.15.2",
  "auth_method_name" : "my-authentication-plugin",
  "auth_data" : "my-auth-data",
  "protocol_version" : 6
}
```

Fields:
 * `client_version` → String based identifier. Format is not enforced
 * `auth_method_name` → *(optional)* Name of the authentication plugin if auth
   enabled
 * `auth_data` → *(optional)* Plugin specific authentication data
 * `protocol_version` → Indicates the protocol version supported by the
   client. Broker will not send commands introduced in newer revisions of the
   protocol. Broker might be enforcing a minimum version

```protobuf
message CommandConnected {
  "server_version" : "Pulsar-Broker-v1.15.2",
  "protocol_version" : 6
}
```

Fields:
 * `server_version` → String identifier of broker version
 * `protocol_version` → Protocol version supported by the broker. Client
   must not attempt to send commands introduced in newer revisions of the
   protocol

### Keep Alive

To identify prolonged network partitions between clients and brokers or cases
in which a machine crashes without interrupting the TCP connection on the remote
end (eg: power outage, kernel panic, hard reboot...), we have introduced a
mechanism to probe for the availability status of the remote peer.

Both clients and brokers are sending `Ping` commands periodically and they will
close the socket if a `Pong` response is not received within a timeout (default
used by broker is 60s).

A valid implementation of a Pulsar client is not required to send the `Ping`
probe, though it is required to promptly reply after receiving one from the
broker in order to prevent the remote side from forcibly closing the TCP connection.


### Producer

In order to send messages, a client needs to establish a producer. When creating
a producer, the broker will first verify that this particular client is
authorized to publish on the topic.

Once the client gets confirmation of the producer creation, it can publish
messages to the broker, referring to the producer id negotiated before.

![Producer interaction](assets/binary-protocol-producer.png)

##### Command Producer

```protobuf
message CommandProducer {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "producer_id" : 1,
  "request_id" : 1
}
```

Parameters:
 * `topic` → Complete topic name to where you want to create the producer on
 * `producer_id` → Client generated producer identifier. Needs to be unique
    within the same connection
 * `request_id` → Identifier for this request. Used to match the response with
    the originating request. Needs to be unique within the same connection
 * `producer_name` → *(optional)* If a producer name is specified, the name will
    be used, otherwise the broker will generate a unique name. Generated
    producer name is guaranteed to be globally unique. Implementations are
    expected to let the broker generate a new producer name when the producer
    is initially created, then reuse it when recreating the producer after
    reconnections.

The broker will reply with either `ProducerSuccess` or `Error` commands.

##### Command ProducerSuccess

```protobuf
message CommandProducerSuccess {
  "request_id" :  1,
  "producer_name" : "generated-unique-producer-name"
}
```

Parameters:
 * `request_id` → Original id of the `CreateProducer` request
 * `producer_name` → Generated globally unique producer name or the name
    specified by the client, if any.

##### Command Send

Command `Send` is used to publish a new message within the context of an
already existing producer. This command is used in a frame that includes command
as well as message payload, for which the complete format is specified in the
[payload commands](#payload-commands) section.

```protobuf
message CommandSend {
  "producer_id" : 1,
  "sequence_id" : 0,
  "num_messages" : 1
}
```

Parameters:
 * `producer_id` → id of an existing producer
 * `sequence_id` → each message has an associated sequence id which is expected
   to be implemented with a counter starting at 0. The `SendReceipt` that
   acknowledges the effective publishing of a messages will refer to it by
   its sequence id.
 * `num_messages` → *(optional)* Used when publishing a batch of messages at
   once.

##### Command SendReceipt

After a message has been persisted on the configured number of replicas, the
broker will send the acknowledgment receipt to the producer.


```protobuf
message CommandSendReceipt {
  "producer_id" : 1,
  "sequence_id" : 0,
  "message_id" : {
    "ledgerId" : 123,
    "entryId" : 456
  }
}
```

Parameters:
 * `producer_id` → id of producer originating the send request
 * `sequence_id` → sequence id of the published message
 * `message_id` → message id assigned by the system to the published message
   Unique within a single cluster. Message id is composed of 2 longs, `ledgerId`
   and `entryId`, that reflect that this unique id is assigned when appending
   to a BookKeeper ledger


##### Command CloseProducer

**Note**: *This command can be sent by either producer or broker*.

When receiving a `CloseProducer` command, the broker will stop accepting any
more messages for the producer, wait until all pending messages are persisted
and then reply `Success` to the client.

The broker can send a `CloseProducer` command to client when it's performing
a graceful failover (eg: broker is being restarted, or the topic is being unloaded
by load balancer to be transferred to a different broker).

When receiving the `CloseProducer`, the client is expected to go through the
service discovery lookup again and recreate the producer again. The TCP
connection is not affected.

### Consumer

A consumer is used to attach to a subscription and consume messages from it.
After every reconnection, a client needs to subscribe to the topic. If a
subscription is not already there, a new one will be created.

![Consumer](assets/binary-protocol-consumer.png)

#### Flow control

After the consumer is ready, the client needs to *give permission* to the
broker to push messages. This is done with the `Flow` command.

A `Flow` command gives additional *permits* to send messages to the consumer.
A typical consumer implementation will use a queue to accumulate these messages
before the application is ready to consume them.

After the application has dequeued half of the messages in the queue, the consumer 
sends permits to the broker to ask for more messages (equals to half of the messages in the queue).

For example, if the queue size is 1000 and the consumer consumes 500 messages in the queue.
Then the consumer sends permits to the broker to ask for 500 messages.

##### Command Subscribe

```protobuf
message CommandSubscribe {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "subscription" : "my-subscription-name",
  "subType" : "Exclusive",
  "consumer_id" : 1,
  "request_id" : 1
}
```

Parameters:
 * `topic` → Complete topic name to where you want to create the consumer on
 * `subscription` → Subscription name
 * `subType` → Subscription type: Exclusive, Shared, Failover, Key_Shared
 * `consumer_id` → Client generated consumer identifier. Needs to be unique
    within the same connection
 * `request_id` → Identifier for this request. Used to match the response with
    the originating request. Needs to be unique within the same connection
 * `consumer_name` → *(optional)* Clients can specify a consumer name. This
    name can be used to track a particular consumer in the stats. Also, in
    Failover subscription type, the name is used to decide which consumer is
    elected as *master* (the one receiving messages): consumers are sorted by
    their consumer name and the first one is elected master.

##### Command Flow

```protobuf
message CommandFlow {
  "consumer_id" : 1,
  "messagePermits" : 1000
}
```

Parameters:
* `consumer_id` → Id of an already established consumer
* `messagePermits` → Number of additional permits to grant to the broker for
    pushing more messages

##### Command Message

Command `Message` is used by the broker to push messages to an existing consumer,
within the limits of the given permits.


This command is used in a frame that includes the message payload as well, for
which the complete format is specified in the [payload commands](#payload-commands)
section.

```protobuf
message CommandMessage {
  "consumer_id" : 1,
  "message_id" : {
    "ledgerId" : 123,
    "entryId" : 456
  }
}
```


##### Command Ack

An `Ack` is used to signal to the broker that a given message has been
successfully processed by the application and can be discarded by the broker.

In addition, the broker will also maintain the consumer position based on the
acknowledged messages.

```protobuf
message CommandAck {
  "consumer_id" : 1,
  "ack_type" : "Individual",
  "message_id" : {
    "ledgerId" : 123,
    "entryId" : 456
  }
}
```

Parameters:
 * `consumer_id` → Id of an already established consumer
 * `ack_type` → Type of acknowledgment: `Individual` or `Cumulative`
 * `message_id` → Id of the message to acknowledge
 * `validation_error` → *(optional)* Indicates that the consumer has discarded
   the messages due to: `UncompressedSizeCorruption`,
   `DecompressionError`, `ChecksumMismatch`, `BatchDeSerializeError`

##### Command CloseConsumer

***Note***: *This command can be sent by either producer or broker*.

This command behaves the same as [`CloseProducer`](#command-closeproducer)

##### Command RedeliverUnacknowledgedMessages

A consumer can ask the broker to redeliver some or all of the pending messages
that were pushed to that particular consumer and not yet acknowledged.

The protobuf object accepts a list of message ids that the consumer wants to
be redelivered. If the list is empty, the broker will redeliver all the
pending messages.

On redelivery, messages can be sent to the same consumer or, in the case of a
shared subscription, spread across all available consumers.


##### Command ReachedEndOfTopic

This is sent by a broker to a particular consumer, whenever the topic
has been "terminated" and all the messages on the subscription were
acknowledged.

The client should use this command to notify the application that no more
messages are coming from the consumer.

##### Command ConsumerStats

This command is sent by the client to retrieve Subscriber and Consumer level 
stats from the broker.
Parameters:
 * `request_id` → Id of the request, used to correlate the request 
      and the response.
 * `consumer_id` → Id of an already established consumer.

##### Command ConsumerStatsResponse

This is the broker's response to ConsumerStats request by the client. 
It contains the Subscriber and Consumer level stats of the `consumer_id` sent in the request.
If the `error_code` or the `error_message` field is set it indicates that the request has failed.

##### Command Unsubscribe

This command is sent by the client to unsubscribe the `consumer_id` from the associated topic.
Parameters:
 * `request_id` → Id of the request.
 * `consumer_id` → Id of an already established consumer which needs to unsubscribe.


## Service discovery

### Topic lookup

Topic lookup needs to be performed each time a client needs to create or
reconnect a producer or a consumer. Lookup is used to discover which particular
broker is serving the topic we are about to use.

Lookup can be done with a REST call as described in the
[admin API](admin-api-topics.md#lookup-of-topic)
docs.

Since Pulsar-1.16 it is also possible to perform the lookup within the binary
protocol.

For the sake of example, let's assume we have a service discovery component
running at `pulsar://broker.example.com:6650`

Individual brokers will be running at `pulsar://broker-1.example.com:6650`,
`pulsar://broker-2.example.com:6650`, ...

A client can use a connection to the discovery service host to issue a
`LookupTopic` command. The response can either be a broker hostname to
connect to, or a broker hostname to which retry the lookup.

The `LookupTopic` command has to be used in a connection that has already
gone through the `Connect` / `Connected` initial handshake.

![Topic lookup](assets/binary-protocol-topic-lookup.png)

```protobuf
message CommandLookupTopic {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "request_id" : 1,
  "authoritative" : false
}
```

Fields:
 * `topic` → Topic name to lookup
 * `request_id` → Id of the request that will be passed with its response
 * `authoritative` → Initial lookup request should use false. When following a
   redirect response, client should pass the same value contained in the
   response

##### LookupTopicResponse

Example of response with successful lookup:

```protobuf
message CommandLookupTopicResponse {
  "request_id" : 1,
  "response" : "Connect",
  "brokerServiceUrl" : "pulsar://broker-1.example.com:6650",
  "brokerServiceUrlTls" : "pulsar+ssl://broker-1.example.com:6651",
  "authoritative" : true
}
```

Example of lookup response with redirection:

```protobuf
message CommandLookupTopicResponse {
  "request_id" : 1,
  "response" : "Redirect",
  "brokerServiceUrl" : "pulsar://broker-2.example.com:6650",
  "brokerServiceUrlTls" : "pulsar+ssl://broker-2.example.com:6651",
  "authoritative" : true
}
```

In this second case, we need to reissue the `LookupTopic` command request
to `broker-2.example.com` and this broker will be able to give a definitive
answer to the lookup request.

### Partitioned topics discovery

Partitioned topics metadata discovery is used to find out if a topic is a
"partitioned topic" and how many partitions were set up.

If the topic is marked as "partitioned", the client is expected to create
multiple producers or consumers, one for each partition, using the `partition-X`
suffix.

This information only needs to be retrieved the first time a producer or
consumer is created. There is no need to do this after reconnections.

The discovery of partitioned topics metadata works very similar to the topic
lookup. The client send a request to the service discovery address and the
response will contain actual metadata.

##### Command PartitionedTopicMetadata

```protobuf
message CommandPartitionedTopicMetadata {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "request_id" : 1
}
```

Fields:
 * `topic` → the topic for which to check the partitions metadata
 * `request_id` → Id of the request that will be passed with its response


##### Command PartitionedTopicMetadataResponse

Example of response with metadata:

```protobuf
message CommandPartitionedTopicMetadataResponse {
  "request_id" : 1,
  "response" : "Success",
  "partitions" : 32
}
```

## Protobuf interface

All Pulsar's Protobuf definitions can be found {@inject: github:here:/pulsar-common/src/main/proto/PulsarApi.proto}.
