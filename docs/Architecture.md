
# System Overview

<!-- TOC depthFrom:1 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [System Overview](#system-overview)
	- [Architecture](#architecture)
		- [Message Broker](#message-broker)
		- [Persistence Store](#persistence-store)
		- [Managed Ledger](#managed-ledger)
		- [Metadata Store](#metadata-store)
	- [Design](#design)
		- [Topic](#topic)
		- [Subscription](#subscription)
		- [Subscription Modes](#subscription-modes)
		- [Property and Namespace](#property-and-namespace)
		- [Producer](#producer)
		- [Consumer](#consumer)
		- [Partitioned Topic](#partitioned-topic)
		- [Persistence](#persistence)
		- [Replication](#replication)
		- [Authentication and Authorization](#authentication-and-authorization)
- [Client Library](#client-library)
	- [Client Setup Phase](#client-setup-phase)
	- [PulsarClient](#pulsarclient)
	- [Consumer API](#consumer-api)
	- [Producer API](#producer-api)

<!-- /TOC -->

Pulsar is a multi-tenant, high-performance solution for server to server messaging. Key features include:

- Java language bindings with simple API
- Multiple subscription modes: pub/sub, load balancer, and failover consumer modes
- Seamless geo-replication of messages
- Low publish and end to end latency
- Guaranteed message delivery with persistent messaging; persistence storage consists of configurable multiple copies across multiple hosts.

## Architecture

At a high level, a Pulsar instance is composed of one or multiple clusters, each could reside in different geographical regions. A single Pulsar cluster is composed of a set of message brokers and bookkeepers, plus zookeeper ensembles for coordination and configuration management. Finally, a client library is provided with easy-to-use APIs.

![Architecture Diagram](img/pulsar_system_architecture.png)


### Message Broker

The Pulsar broker is a state-less component which primarily runs two different components: a HTTP server that exposes a REST interface for topic lookup and administrative tasks, and a  dispatcher, which is an asynchronous TCP server over a custom binary protocol used for all data transfers.

The messages are typically dispatched out of the managed ledger cache, unless the backlog exceeds the cache size, in which case
the broker will start reading entries from Bookkeeper.

Finally, to support geo replication on global topics, the broker manages the replicators which tail the entries published in the local region and republish them to the remote region using the Pulsar client library itself.

### Persistence Store

Pulsar uses Apache Bookkeeper as durable storage which is a distributed write-ahead log system. With Bookkeeper, applications can create many independent logs, called ledgers. A ledger is an append-only data structure with a single writer that is assigned to multiple storage nodes (or bookies) and whose entries are replicated to multiple of these nodes. The semantics of a ledger are very simple: a process can create a ledger, append entries and close the ledger. After the ledger has been closed, either explicitly or because the writer process crashed, it can only be opened in read-only mode. Finally, when the entries contained in the ledger are no longer needed, the whole ledger can be deleted from the system.

The main strength of Bookkeeper is to guarantee the read consistency of the ledger in the presence of failures. Since the ledger can only be written by a single process, this process is free to append entries very efficiently (without need for further consensus) and after a failure, the ledger will go through a recovery process that will finalize the state of the ledger and establish which entry was last committed to the log. After that point, all readers of the ledger are guaranteed to see the exact same content.

Pulsar uses BookKeeper since it is a very efficient sequential store that handles entry replication, node failures, and it is horizontally scalable in capacity and throughput. From an operational perspective, the capacity could be immediately increased by simply adding more bookies to a Pulsar cluster. The other strength of Bookkeeper is that the bookies are designed to handle thousands of ledgers with concurrent reads and writes and, by using multiple disk devices (one for journal and another for general storage) are able to isolate the effects of read operations from the latency of ongoing write operations.

### Managed Ledger

Given that Bookkeeper ledgers provide a single log abstraction, a library was developed on top of the ledger called managed ledger which represents the storage layer for a single topic. A managed ledger represents the abstraction of a stream of messages with a single writer that keeps appending at the end of the stream and multiple cursors that are consuming the stream, each with its own associated position.

Internally, a single managed ledger uses multiple Bookkeeper ledgers to store the data. There are two reasons to have multiple ledgers: first, after a failure a ledger is not writable anymore and we need to create a new one, and second we want to rollover ledgers periodically so we may delete a ledger when all cursors have consumed the messages it contains.

### Metadata Store

Pulsar uses Apache Zookeeper for metadata, cluster configuration and coordination.
- *Global Zookeeper* stores user provisioning data like properties, namespaces and policies which should be global consistent.
- Each cluster has a *local zookeeper* ensemble which stores cluster specific configuration and coordination data, like ownership metadata, broker load reports, bookkeeper ledgers' metadata.


## Design

### Topic

A **topic** is a logical endpoint for publishing and consuming messages. Producers publish messages to the topic and consumers subscribe to the topic, to consume messages published to the topic. Pulsar allows multiple subscription modes on a topic to support pub/sub, load balancer, and failover use-cases.

A normal topic (except partitioned topic) does not need to be explicitly created, it is created on the fly when client try to publish/consume messages on the topic.

### Subscription

Subscription is a durable resource that gets created the first time a consumer subscribes to the topic with the given subscription name. It receives all the messages published on the topic after its own creation. If no consumer is attached to this subscription, all the messages published on the topic will be retained as backlog. Finally, a consumer can unsubscribe to remove the subscription from the topic.

### Subscription Modes

Subscription is a configuration rule that determines how messages are delivered to a consumer.

![Subscription Modes](img/pulsar_subscriptions.jpg)

**Exclusive**

- This is the default subscription mode.  Only a single consumer is allowed to attach to the given subscription. If more than one consumer attempts to  subscribe to a topic using the same subscription (name), it gets an error. To support the publish/subscribe model to a topic, multiple consumers subscribe with distinct subscription names. Messages published to the topic are delivered to all subscribed consumers.

**Shared**

- Multiple consumers can attach to the same subscription. Messages are delivered in a round-robin distribution across consumers, and any given message is delivered to only one consumer. When a consumer disconnects, all the messages that were sent to it and not acknowledged, will be rescheduled for sending to remaining consumers.
- Ordering is not guaranteed with shared consumers.

**Failover**

- Multiple consumers can attach to the same subscription. The consumers will be lexically sorted by consumer's name and the first consumer will be the only one (master consumer) receiving messages. When this consumer disconnects, all (non-acked and subsequent) messages will be delivered to the next consumer in line.


### Property and Namespace

Property and namespace are two key concepts of Pulsar to support multi-tenant.

- A **property** identifies a tenant. Pulsar is provisioned for a specified property with appropriate capacity allocated to the property.
- A **namespace** is the administrative unit nomenclature within a property. The configuration policies set on a namespace apply to all the topics created in such namespace. A property may create multiple namespaces via self-administration using REST API and CLI tools. For instance, a property with different applications can create a separate namespace for each application.

E.g.:  `my-property/us-w/my-app1` is a namespace for the application `my-app1` in cluster `us-w` for `my-property`.  
Topics names for such namespaces will look like:

```
persistent://my-property/us-w/my-app1/my-topic-1
persistent://my-property/us-w/my-app1/my-topic-2
...
```


### Producer

A producer attaches to a topic and produces messages.

**sync vs. async send** - message could be sent to broker in sync or async manner:

- sync: producer will wait for broker acknowledgement after sending each message.
- async: producer will put the message in a blocking queue and return immediately, client library will send the messages to broker in background. If the queue is full (max size configurable) producer could be blocked or fail immediately when calling send API, depending on arguments passed to producer.

**compression** - message could be compressed during transportation to save bandwidth (compression and de-compression both are performed by client), below types of compression are supported:

- LZ4
- ZLIB

**batch** - if batching is enabled, producer will try to accumulate and send batch of messages in a single request. Batching size defined by maximum number of messages and maximum publish latency.


### Consumer

A consumer attaches to a subscription and receives messages.

**sync vs. async receive** - sync receive will be blocked until a message is available. Async receive will return immediately with an instance of CompletableFuture, which completes with received message once new message is available.

**acknowledgement** - message could be acknowledged individually one by one or cumulatively. With cumulative acknowledgement consumer only need to acknowledge the last message it received, all messages in the stream up to (and include) the provided message will not be re-delivered to this consumer. Cumulative acknowledgement cannot be used with Shared subscription mode.

**listener** - a customized MessageListener implementation could be passed to consumer, client library will call the listener whenever a new message is received (no need to call consumer receive).


### Partitioned Topic

A normal topic could only be served by a single broker which limits its maximum throughput, partitioned topic as a special type of topic could span across multiple brokers to achieve higher throughput. Partitioned topic need to be explicitly created via admin API/CLI, number of partitions can be specified when creating the topic.

A partitioned topic is actually implemented as N (number of partitions) internal topics, there is no difference between the internal topics and other normal topics on how subscription modes work.

![Partitioned Topic](img/pulsar_partitioned_topic.jpg)

**routing mode** - routing mode decides which partition (internal topic) a message will be published to:

- Key hash: If a key property has been specified on the message, the partitioned producer will hash the key and assign it to a particular partition, ensuring per-key-bucket ordering guarantee.
- Single Default Partition: if no message provided, each producer's message will be routed to a dedicated partition (initially random selected) to achieve per-producer message ordering.
- Round Robin Distribution: if no message provided, all messages will be routered to different partitions in round-robin to achieve maximum throughput, no guarantee on message ordering.
- Custom Routing Policy: message will be routered by a customized MessageRouter implementation.

```java
public interface MessageRouter extends Serializable {
    /**
     * @param msg Message object
     * @return The index of the partition to use for the message
     */
    int choosePartition(Message msg);
}
```

### Persistence

Guaranteed message delivery requires that messages are stored in a durable manner until they are delivered to and acknowledged by consumers. This mode of messaging is commonly called Persistent Messaging.

Message durability is set at the topic level. A topic can either be “persistent” or “non-persistent” and that is included in its own name:
*persistent://my-property/global/my-ns/my-topic*

**persistent**

- All messages are stored and synced on disk and N copies (for example: 4 copies across two servers with Mirrored RAID volumes on each server) are kept until all consumers have consumed the messages. Subscription position for a consumer,  called cursor is also stored on disk. Messages are guaranteed to be delivered at-least once.

**non-persistent**

- Currently, Pulsar does not support best effort delivery - also known as, Non-persistent Messaging. However, we have future plans to support non-persistent messaging.


### Replication

Pulsar enables messages to be produced and consumed in different geo-locations. For instance your application may be publishing data in one geo/market and you would like to process it for consumption in other geos/markets. Global Replication in Pulsar enable you to do that.

### Authentication and Authorization

Pulsar supports a pluggable [authentication](https://github.com/yahoo/pulsar/blob/master/docs/Authentication.md) mechanism which can be configured at broker and it also supports [authorization](https://github.com/yahoo/pulsar/blob/master/docs/Authorization.md) to identify client and its access rights on topics and properties. 


# Client Library


Pulsar exposes a client API with Java language bindings. The client API optimizes and encapsulates client-broker communication protocol and exposes a simple and intuitive API for use by the application. Under the hood, the client library supports transparent reconnection and/or connection failover to a broker, queuing of messages until acknowledged by broker and heuristics such as connection retries with backoff.

## Client Setup Phase

When an application wants to create a producer/consumer, the Pulsar client library will internally initiate the setup phase that is composed of two steps. The first task is to find owner for the topic by sending a lookup HTTP request. The request could reach one of the active brokers which, by looking at the (cached) zookeeper metadata will know who is serving the topic or, in case nobody is serving it, will try to assign it to the least loaded broker.

Once the client library has the broker address, it will create a TCP connection (or reuse an existing connection from the pool) and authenticate it. Within this connection, client and broker exchange binary commands from a custom protocol. At this point the client will send a command to create producer/consumer to the broker, which will comply after having validated the authorization policy.

Whenever the TCP connection breaks, the client will immediately re-initiate this setup phase and will keep trying with exponential back-off to re-establish the producer or consumer until it succeeds.

## PulsarClient

A PulsarClient (TODO: javadocs) instance is needed before producing/consuming messages.

```java
ClientConfiguration config = new ClientConfiguration();
PulsarClient pulsarClient = PulsarClient.create("pulsar://broker.example.com:6650", config);
...
pulsarClient.close();
```

ClientConfiguration (TODO: javadocs) is used to pass arguments to PulsarClient:

```java
// Set the authentication provider to use in the Pulsar client instance.
public void setAuthentication(Authentication authentication);
public void setAuthentication(String authPluginClassName, String authParamsString);
public void setAuthentication(String authPluginClassName, Map<String, String> authParams);

// Set the operation timeout(default: 30 seconds)
public void setOperationTimeout(int operationTimeout, TimeUnit unit);

// Set the number of threads to be used for handling connections to brokers (default: 1 thread)
public void setIoThreads(int numIoThreads);

// Set the number of threads to be used for message listeners(default: 1 thread)
public void setListenerThreads(int numListenerThreads);

// Sets the max number of connection that the client library will open to a single broker.
public void setConnectionsPerBroker(int connectionsPerBroker);

// Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
public void setUseTcpNoDelay(boolean useTcpNoDelay);
```

## Consumer API

Create a Consumer (TODO javadocs) with PulsarClient and receive 10 messages.

```java
ConsumerConfiguration conf = new ConsumerConfiguration();
conf.setSubscriptionType(SubscriptionType.Exclusive);
Consumer consumer = pulsarClient.subscribe(
      "persistent://my-property/us-w/my-ns/my-topic", "my-subscriber-name", conf);

for (int i = 0; i < 10; i++) {
    // Receive a message
    Msg msg = consumer.receive();

    // Do something
    System.out.println("Received: " + new String(msg.getData()));

    // Acknowledge successful message processing
    consumer.acknowledge(msg);
}

consumer.close();
```

ConsumerConfiguration (TODO javadocs) could be used to pass arguments to consumer:

```java
// Set the timeout for unacked messages, truncated to the nearest millisecond.
public ConsumerConfiguration setAckTimeout(long ackTimeout, TimeUnit timeUnit);
// Select the subscription type to be used when subscribing to the topic.
public ConsumerConfiguration setSubscriptionType(SubscriptionType subscriptionType);
// Sets a MessageListener for the consumer
public ConsumerConfiguration setMessageListener(MessageListener messageListener);
// Sets the size of the consumer receive queue.
public ConsumerConfiguration setReceiverQueueSize(int receiverQueueSize);
```


## Producer API

Creates a Producer (TODO javadocs) with PulsarClient and publish 10 messages.
```java
ProducerConfiguration config = new ProducerConfiguration();
Producer producer = pulsarClient.createProducer(
           "persistent://my-property/us-w/my-ns/my-topic", config);
// publish 10 messages to the topic
for (int i = 0; i < 10; i++) {
    producer.send("my-message".getBytes());
}
producer.close();
```

ProducerConfiguration (TODO javadocs) could be used to pass arguments to producer:

```java
// Set the send timeout (default: 30 seconds)
public ProducerConfiguration setSendTimeout(int sendTimeout, TimeUnit unit);
// Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
public ProducerConfiguration setMaxPendingMessages(int maxPendingMessages);
// Set whether the Producer#send and Producer#sendAsync operations should block when the outgoing message queue is full.
public ProducerConfiguration setBlockIfQueueFull(boolean blockIfQueueFull);
// Set the message routing mode for the partitioned producer
public ProducerConfiguration setMessageRoutingMode(MessageRoutingMode messageRouteMode);
// Set the compression type for the producer.
public ProducerConfiguration setCompressionType(CompressionType compressionType);
// Set a custom message routing policy by passing an implementation of MessageRouter
public ProducerConfiguration setMessageRouter(MessageRouter messageRouter);
// Control whether automatic batching of messages is enabled for the producer, default: false.
public ProducerConfiguration setBatchingEnabled(boolean batchMessagesEnabled);
// Set the time period within which the messages sent will be batched, default: 10ms.
public ProducerConfiguration setBatchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit);
// Set the maximum number of messages permitted in a batch, default: 1000.
public ProducerConfiguration setBatchingMaxMessages(int batchMessagesMaxMessagesPerBatch);
```
