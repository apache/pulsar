---
title: Pulsar concepts and architecture
lead: A high-level overview of Pulsar's moving pieces
tags:
- architecture
---

{% popover Pulsar %} is a multi-tenant, high-performance solution for server-to-server messaging originally developed by [Yahoo](http://yahoo.github.io/) and now under the stewardship of the [Apache Software Foundation](https://www.apache.org/).

Pulsar's key features include:

* Native support for multiple {% popover clusters %} in a Pulsar {% popover instance %}, with seamless [geo-replication](../../admin/GeoReplication) of messages across clusters
* Very low publish and end-to-end latency
* Seamless scalability out to over a million topics
* A simple [client API](#client-api) with bindings for [Java](../../applications/JavaClient), [Python](../../applications/PythonClient), and [C++](../../applications/CppClient)
* Multiple [subscription modes](#subscription-modes) for {% popover topics %} ([exclusive](#exclusive), [shared](#shared), and [failover](#failover))
* Guaranteed message delivery with [persistent message storage](#persistent-storage) provided by [Apache BookKeeper](http://bookkeeper.apache.org/)

## Producers, consumers, topics, and subscriptions

Pulsar is built on the [publish-subscribe](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) pattern, aka {% popover pub-sub %}. In this pattern, [producers](#producers) publish messages to [topics](#topics). [Consumers](#consumers) can then [subscribe](#subscription-modes) to those topics and process incoming messages.

Once a {% popover subscription %} has been created, all messages will be [retained](#persistent-storage) by Pulsar, even if the consumer gets disconnected. Retained messages will be discarded only when a consumer {% popover acknowledges %} that they've been successfully processed.

### Producers

A producer is a process that attaches to a topic and publishes messages to a Pulsar {% popover broker %} for processing.

#### Send modes

Producers can send messages to brokers either synchronously (sync) or asynchronously (async).

| Mode       | Description                                                                                                                                                                                                                                                                                                                                                              |
|:-----------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Sync send  | The producer will wait for acknowledgement from the broker after sending each message. If acknowledgment isn't received then the producer will consider the send operation a failure.                                                                                                                                                                                    |
| Async send | The producer will put the message in a blocking queue and return immediately. The client library will then send the message to the broker in the background. If the queue is full (max size [configurable](../../reference/Configuration#broker), the producer could be blocked or fail immediately when calling the API, depending on arguments passed to the producer. |

#### Compression

Messages published by producers can be compressed during transportation in order to save bandwidth. Pulsar currently supports two types of compression:

* [LZ4](https://github.com/lz4/lz4)
* [ZLIB](https://zlib.net/)

#### Batching

If batching is enabled, the producer will accumulate and send a batch of messages in a single request. Batching size is defined by the maximum number of messages and maximum publish latency.

### Consumers

A consumer is a process that attaches to a topic via a subscription and then receives messages.

#### Receive modes

Messages can be received from {% popover brokers %} either synchronously (sync) or asynchronously (async).

| Mode          | Description                                                                                                                                                                                                   |
|:--------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Sync receive  | A sync receive will be blocked until a message is available.                                                                                                                                                  |
| Async receive | An async receive will return immediately with a future value---a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture) in Java, for example---that completes once a new message is available. |

#### Acknowledgement

When a consumer has successfully processed a message, it needs to send an {% popover acknowledgement %} to the broker so that the broker can discard the message (otherwise it [stores](#persistent-storage) the message).

Messages can be acknowledged either one by one or cumulatively. With cumulative acknowledgement, the consumer only needs to acknowledge the last message it received. All messages in the stream up to (and including) the provided message will not be re-delivered to that consumer.

{% include admonition.html type='warning' content='Cumulative acknowledgement cannot be used with [shared subscription mode](#subscription-modes), because shared mode involves multiple consumers having access to the same subscription.' %}

#### Listeners

Client libraries can provide their own listener implementations for consumers. The [Java client](../../applications/JavaClient), for example, provides a {% javadoc MesssageListener client com.yahoo.pulsar.client.api.MessageListener %} interface. In this interface, the `received` method is called whenever a new message is received.

### Topics

As in other pub-sub systems, topics in Pulsar are named channels for transmitting messages from producers to consumers. Topic names are URLs that have a well-defined structure:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

| Topic name component | Description                                                                                                                                                                                                                                  |
|:---------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `persistent`         | Identifies a topic for which all messages are durably [persisted](#persistent-storage) on disk (that means on multiple disks unless the {% popover broker %} is {% popover standalone %}). Pulsar currently only supports persistent topics. |
| `property`           | The topic's {% popover tenant %} within the instance. Tenants are essential to {% popover multi-tenancy %} in Pulsar and can be spread across clusters.                                                                                      |
| `cluster`            | Where the topic is located. Typically there will be one {% popover cluster %} for each geographical region or data center.                                                                                                                   |
| `namespace`          | The administrative unit of the topic, which acts as a grouping mechanism for related topics. Most topic configuration is performed at the namespace level. Each property (tenant) can have multiple namespaces.                              |
| `topic`              | The final part of the name. Topic names are freeform and have no special meaning in a Pulsar instance.                                                                                                                                       |

### Subscription modes

A subscription is a named configuration rule that determines how messages are delivered to {% popover consumers %}. There are three available subscription modes in Pulsar: [exclusive](#exclusive), [shared](#shared), and [failover](#failover). These modes are illustrated in the figure below.

![Subscription Modes]({{ site.baseurl }}img/pulsar_subscriptions.jpg)

#### Exclusive

In *exclusive* mode, only a single consumer is allowed to attach to the subscription. If more than one consumer attempts to subscribe to a topic using the same subscription, the consumer receives an error.

In the diagram above, only **Consumer-A** is allowed to consume messages.

Exclusive mode is the default subscription mode.  

#### Shared

In *shared* or *round robin* mode, multiple consumers can attach to the same subscription. Messages are delivered in a round robin distribution across consumers, and any given message is delivered to only one consumer. When a consumer disconnects, all the messages that were sent to it and not acknowledged will be rescheduled for sending to the remaining consumers.

In the diagram above, **Consumer-B-1** and **Consumer-B-2** are able to subscribe to the topic, but **Consumer-C-1** and others could as well.

{% include message.html id="shared_mode_limitations" %}

#### Failover

In *failover* mode, multiple consumers can attach to the same subscription. The consumers will be lexically sorted by the consumer's name and the first consumer will initially be the only one receiving messages. This consumer is called the *master consumer*.

When the master consumer disconnects, all (non-acked and subsequent) messages will be delivered to the next consumer in line.

In the diagram above, Consumer-C-1 is the master consumer while Consumer-C-2 would be the next in line to receive messages if Consumer-C-2 disconnected.

### Partitioned topics

{% include explanations/partitioned-topics.md %}

## Architecture overview

At the highest level, a Pulsar {% popover instance %} is composed of one or more Pulsar {% popover clusters %}. Clusters within an instance can [replicate](#replicate) data amongst themselves.

In a Pulsar cluster:

* One or more {% popover brokers %} handles and load balances incoming messages from {% popover producers %}, dispatches messages to {% popover consumers %}, communicates with {% popover global ZooKeeper %} to handle various coordination tasks, stores messages in {% popover BookKeeper %} instances (aka {% popover bookies %}), relies on a cluster-specific {% popover ZooKeeper %} cluster for certain tasks, and more.
* A {% popover BookKeeper %} cluster consisting of one more or more {% popover bookies %} handles [persistent storage](#persistent-storage) of messages.
* A {% popover ZooKeeper %} cluster specific to that cluster handles

The diagram below provides an illustration of a Pulsar cluster:

![Architecture Diagram]({{ site.baseurl }}img/pulsar_system_architecture.png)

At the broader {% popover instance %} level, an instance-wide ZooKeeper cluster called {% popover global ZooKeeper %} handles coordination tasks involving multiple clusters, for example [geo-replication](#replication).

## Brokers

The Pulsar message {% popover broker %} is a stateless component that's primarily responsible for running two other components:

* An HTTP server that exposes a REST API for both [administrative tasks](../../reference/RestApi) and [topic lookup](#client-setup-phase) for producers and consumers
* A {% popover dispatcher %}, which is an asynchronous TCP server over a custom [binary protocol](../../reference/BinaryProtocol) used for all data transfers

Messages are typically dispatched out of a [managed ledger](#managed-ledger) cache for the sake of performance, *unless* the backlog exceeds the cache size. If the backlog grows too large for the cache, the broker will start reading entries from {% popover BookKeeper %}.

Finally, to support {% popover geo-replication %} on global topics, the broker manages replicators that tail the entries published in the local region and republish them to the remote region using the Pulsar [Java client library](../../applications/JavaClient).

{% include admonition.html type="info" content="For a guide to managing Pulsar brokers, see the [Clusters and brokers](../../admin/ClustersBrokers#managing-brokers) guide." %}

## Clusters

A Pulsar {% popover instance %} consists of one or more Pulsar *clusters*. Clusters, in turn, consist of:

* One or more Pulsar [brokers](#broker)
* A {% popover ZooKeeper %} quorum used for cluster-level configuration and coordination
* An ensemble of {% popover bookies %} used for [persistent storage](#persistent-storage) of messages

Clusters can replicate amongst themselves using [geo-replication](#geo-replication).

{% include admonition.html type="info" content="For a guide to managing Pulsar clusters, see the [Clusters and brokers](../../admin/ClustersBrokers#managing-clusters) guide." %}

## Metadata store

Pulsar uses [Apache Zookeeper](https://zookeeper.apache.org/) for metadata storage, cluster configuration, and coordination. In a Pulsar instance:

* A {% popover global ZooKeeper %} quorum stores configuration for {% popover properties %}, {% popover namespaces %}, and other entities that need to be globally consistent.
* Each cluster has its own local ZooKeeper ensemble that stores {% popover cluster %}-specific configuration and coordination such as ownership metadata, broker load reports, BookKeeper {% popover ledger %} metadata, and more.

## Persistent storage

![Brokers and bookies]({{ site.baseurl }}img/broker-bookie.png)

Pulsar provides guaranteed message delivery for applications. If a message successfully reaches a Pulsar {% popover broker %}, it will be delivered to its intended target.

This guarantee requires that non-{% popover acknowledged %} messages are stored in a durable manner until they can be delivered to and acknowledged by {% popover consumers %}. This mode of messaging is commonly called *persistent messaging*. In Pulsar, N copies of all messages are stored and synced on disk, for example 4 copies across two servers with mirrored [RAID](https://en.wikipedia.org/wiki/RAID) volumes on each server.

Pulsar uses a system called [Apache BookKeeper](http://bookkeeper.apache.org/) for persistent message storage. BookKeeper is a distributed [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) (WAL) system that provides a number of crucial advantages for Pulsar:

* It enables Pulsar to utilize many independent logs, called [ledgers](#ledgers). Multiple ledgers can be created for {% popover topics %} over time.
* It offers very efficient storage for sequential data that handles entry replication.
* It guarantees read consistency of ledgers in the presence of various system failures.
* It offers even distribution of I/O across bookies.
* It's horizontally scalable in both capacity and throughput. Capacity can be immediately increased by adding more {% popover bookies %} to a cluster.
* {% popover Bookies %} are designed to handle thousands of ledgers with concurrent reads and writes. By using multiple disk devices---one for journal and another for general storage--bookies are able to isolate the effects of read operations from the latency of ongoing write operations.

In addition to message data, *cursors* are also persistently stored in BookKeeper. Cursors are {% popover subscription %} positions for {% popover consumers %}. BookKeeper enables Pulsar to store consumer position in a scalable fashion.

At the moment, Pulsar only supports persistent message storage. This accounts for the `persistent` in all {% popover topic %} names. Here's an example:

{% include topic.html p="my-property" c="global" n="my-namespace" t="my-topic" %}

In the future, Pulsar will support ephemeral message storage.

### Ledgers

A {% popover ledger %} is an append-only data structure with a single writer that is assigned to multiple BookKeeper storage nodes, or {% popover bookies %}. Ledger entries are replicated to multiple bookies. Ledgers themselves have very simple semantics:

* A Pulsar broker can create a ledger, append entries to the ledger, and close the ledger.
* After the ledger has been closed---either explicitly or because the writer process crashed---it can then be opened only in read-only mode.
* Finally, when entries in the ledger are no longer needed, the whole ledger can be deleted from the system (across all bookies).

#### Ledger read consistency

The main strength of Bookkeeper is that it guarantees read consistency in ledgers in the presence of failures. Since the ledger can only be written to by a single process, that process is free to append entries very efficiently, without need to obtain consensus. After a failure, the ledger will go through a recovery process that will finalize the state of the ledger and establish which entry was last committed to the log. After that point, all readers of the ledger are guaranteed to see the exact same content.

#### Managed ledgers

Given that Bookkeeper ledgers provide a single log abstraction, a library was developed on top of the ledger called the *managed ledger* that represents the storage layer for a single topic. A managed ledger represents the abstraction of a stream of messages with a single writer that keeps appending at the end of the stream and multiple {% popover cursors %} that are consuming the stream, each with its own associated position.

Internally, a single managed ledger uses multiple BookKeeper ledgers to store the data. There are two reasons to have multiple ledgers:

1. After a failure, a ledger is no longer writable and a new one needs to be created.
2. A ledger can be deleted when all cursors have consumed the messages it contains. This allows for periodic rollover of ledgers.

### Journal storage

In BookKeeper, *journal* files contain BookKeeper transaction logs. Before making an update to a [ledger](#ledgers)

### Non-persistent storage

A future version of BookKeeper will support *non-persistent messaging* and thus multiple durability modes at the topic level. This will enable you to set the durability mode at the topic level, replacing the `persistent` in topic names with a `non-persistent` indicator.

## Replication

Pulsar enables messages to be produced and consumed in different geo-locations. For instance, your application may be publishing data in one region or market and you would like to process it for consumption in other regions or markets. [Geo-replication](../../admin/GeoReplication) in Pulsar enables you to do that.

## Multi-tenancy

Pulsar was created from the ground up as a {% popover multi-tenant %} system. To support multi-tenancy, Pulsar has a concept of {% popover properties %}. Properties can be spread across {% popover clusters %} and can each have their own [authentication and authorization](../../admin/Authz) scheme applied to them. They are also the administrative unit at which [storage quotas](TODO), [message TTL](TODO), and [isolation policies](TODO) can be managed.

The multi-tenant nature of Pulsar is reflected mostly visibly in topic URLs, which have this structure:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

As you can see, the property is the most basic unit of categorization for topics (and even more fundamental than the {% popover cluster %}).

### Properties and namespaces

{% include explanations/properties-namespaces.md %}

## Authentication and Authorization

Pulsar supports a pluggable [authentication](https://github.com/apache/incubator-pulsar/blob/master/docs/Authentication.md) mechanism which can be configured at broker and it also supports [authorization](https://github.com/apache/incubator-pulsar/blob/master/docs/Authorization.md) to identify client and its access rights on topics and properties.

## Client interface

Pulsar exposes a client API with language bindings for [Java](../../applications/JavaClient) and [C++](../../applications/CppClient). The client API optimizes and encapsulates Pulsar's client-broker communication protocol and exposes a simple and intuitive API for use by applications.

Under the hood, the current official Pulsar client libraries support transparent reconnection and/or connection failover to {% popover brokers %}, queuing of messages until {% popover acknowledged %} by the broker, and heuristics such as connection retries with backoff.

{% include admonition.html type="success" title="Custom client libraries" content="
If you'd like to create your own client library, we recommend consulting the documentation on Pulsar's custom [binary protocol](../../project/BinaryProtocol).
" %}

### Client setup phase

When an application wants to create a producer/consumer, the Pulsar client library will initiate a setup phase that is composed of two steps:

1. The client will attempt to determine the owner of the topic by sending an HTTP lookup request to the {% popover broker %}. The request could reach one of the active brokers which, by looking at the (cached) zookeeper metadata will know who is serving the topic or, in case nobody is serving it, will try to assign it to the least loaded broker.
1. Once the client library has the broker address, it will create a TCP connection (or reuse an existing connection from the pool) and authenticate it. Within this connection, client and broker exchange binary commands from a custom protocol. At this point the client will send a command to create producer/consumer to the broker, which will comply after having validated the authorization policy.

Whenever the TCP connection breaks, the client will immediately re-initiate this setup phase and will keep trying with exponential backoff to re-establish the producer or consumer until the operation succeeds.

## Service discovery

[Clients](../../getting-started/Clients) connecting to Pulsar {% popover brokers %} need to be able to communicate with an entire Pulsar {% popover instance %} using a single URL. Pulsar provides a built-in service discovery mechanism that you can set up using the instructions in the [Deploying a Pulsar instance](../../deployment/InstanceSetup#service-discovery-setup) guide.

You can use your own service discovery system if you'd like. If you use your own system, there is just one requirement: when a client performs an HTTP request to an endpoint, such as `http://pulsar.us-west.example.com:8080`, the client needs to be redirected to *some* active broker in the desired {% popover cluster %}, whether via DNS, an HTTP or IP redirect, or some other means.
