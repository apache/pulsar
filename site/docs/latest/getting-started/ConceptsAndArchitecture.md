---
title: Pulsar concepts and architecture
lead: A high-level overview of Pulsar's moving pieces
tags:
- architecture
- deduplication
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

{% popover Pulsar %} is a multi-tenant, high-performance solution for server-to-server messaging originally developed by [Yahoo](http://yahoo.github.io/) and now under the stewardship of the [Apache Software Foundation](https://www.apache.org/).

Pulsar's key features include:

* Native support for multiple {% popover clusters %} in a Pulsar {% popover instance %}, with seamless [geo-replication](../../admin/GeoReplication) of messages across clusters
* Very low publish and end-to-end latency
* Seamless scalability out to over a million topics
* A simple [client API](#client-api) with bindings for [Java](../../clients/Java), [Python](../../clients/Python), and [C++](../../clients/Cpp)
* Multiple [subscription modes](#subscription-modes) for {% popover topics %} ([exclusive](#exclusive), [shared](#shared), and [failover](#failover))
* Guaranteed message delivery with [persistent message storage](#persistent-storage) provided by [Apache BookKeeper](http://bookkeeper.apache.org/)

## Messages

Messages are the basic "unit" of Pulsar. They're what {% popover producers %} publish to {% popover topics %} and what {% popover consumers %} then consume from topics (and {% popover acknowledge %} when the message has been processed). Messages are the analogue of letters in a postal service system.

Component | Purpose
:---------|:-------
Value / data payload | The data carried by the message. All Pulsar messages carry raw bytes, although message data can also conform to data [schemas](#schema-registry)
Key | Messages can optionally be tagged with keys, which can be useful for things like [topic compaction](#compaction)
Properties | An optional key/value map of user-defined properties
Producer name | The name of the {% popover producer %} that produced the message (producers are automatically given default names, but you can apply your own explicitly as well)
Sequence ID | Each Pulsar message belongs to an ordered sequence on its {% popover topic %}. A message's sequence ID is its ordering in that sequence.
Publish time | The timestamp of when the message was published (automatically applied by the {% popover producer %})
Event time | An optional timestamp that applications can attach to the message representing when something happened, e.g. when the message was processed. The event time of a message is 0 if none is explicitly set.

{% include admonition.html type="info" content="For a more in-depth breakdown of Pulsar message contents, see the documentation on Pulsar's [binary protocol](../../reference/BinaryProtocol)." %}

## Producers, consumers, topics, and subscriptions

Pulsar is built on the [publish-subscribe](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) pattern, aka {% popover pub-sub %}. In this pattern, [producers](#producers) publish messages to [topics](#topics). [Consumers](#consumers) can then [subscribe](#subscription-modes) to those topics, process incoming messages, and send an {% popover acknowledgement %} when processing is complete.

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

Client libraries can provide their own listener implementations for consumers. The [Java client](../../clients/Java), for example, provides a {% javadoc MesssageListener client org.apache.pulsar.client.api.MessageListener %} interface. In this interface, the `received` method is called whenever a new message is received.

### Topics

As in other pub-sub systems, topics in Pulsar are named channels for transmitting messages from {% popover producers %} to {% popover consumers %}. Topic names are URLs that have a well-defined structure:

{% include topic.html type="{persistent|non-persistent}" ten="tenant" n="namespace" t="topic" %}

Topic name component | Description
:--------------------|:-----------
`persistent` / `non-persistent` | This identifies the type of topic. Pulsar supports two kind of topics: [persistent](#persistent-storage) and [non-persistent](#non-persistent-topics) (persistent is the default, so if you don't specify a type the topic will be persistent). With persistent topics, all messages are durably [persisted](#persistent-storage) on disk (that means on multiple disks unless the {% popover broker %} is {% popover standalone %}), whereas data for [non-persistent](#non-persistent-topics) topics isn't persisted to storage disks.
`tenant`             | The topic's {% popover tenant %} within the instance. Tenants are essential to {% popover multi-tenancy %} in Pulsar and can be spread across clusters.
`namespace`          | The administrative unit of the topic, which acts as a grouping mechanism for related topics. Most topic configuration is performed at the [namespace](#namespace) level. Each tenant can have multiple namespaces.
`topic`              | The final part of the name. Topic names are freeform and have no special meaning in a Pulsar instance.

{% include admonition.html type="success" title="No need to explicitly create new topics"
content="You don't need to explicitly create topics in Pulsar. If a client attempts to write or receive messages to/from a topic that does not yet exist, Pulsar will automatically create that topic under the [namespace](#namespace) provided in the [topic name](#topics)." %}

### Namespaces

A namespace is a logical nomenclature within a tenant. A tenant can create multiple namespaces via the [admin API](../../admin-api/namespaces#create). For instance, a tenant with different applications can create a separate namespace for each application. A namespace allows the application to create and manage a hierarchy of topics. The topic `my-tenant/app1` is a namespace for the application `app1` for `my-tenant`. You can create any number of [topics](#topics) under the namespace.

### Subscription modes

A subscription is a named configuration rule that determines how messages are delivered to {% popover consumers %}. There are three available subscription modes in Pulsar: [exclusive](#exclusive), [shared](#shared), and [failover](#failover). These modes are illustrated in the figure below.

{% include figure.html src="/img/pulsar-subscription-modes.png" alt="Subscription modes" width="80" %}

#### Exclusive

In *exclusive* mode, only a single consumer is allowed to attach to the subscription. If more than one consumer attempts to subscribe to a topic using the same subscription, the consumer receives an error.

In the diagram above, only **Consumer-A** is allowed to consume messages.

{% include admonition.html type="info" content="Exclusive mode is the default subscription mode." %}

{% include figure.html src="/img/exclusive-subscriptions.png" alt="Exclusive subscriptions" width="80" %}

#### Shared

In *shared* or *round robin* mode, multiple consumers can attach to the same subscription. Messages are delivered in a round robin distribution across consumers, and any given message is delivered to only one consumer. When a consumer disconnects, all the messages that were sent to it and not acknowledged will be rescheduled for sending to the remaining consumers.

In the diagram above, **Consumer-B-1** and **Consumer-B-2** are able to subscribe to the topic, but **Consumer-C-1** and others could as well.

{% include message.html id="shared_mode_limitations" %}

{% include figure.html src="/img/shared-subscriptions.png" alt="Shared subscriptions" width="80" %}

#### Failover

In *failover* mode, multiple consumers can attach to the same subscription. The consumers will be lexically sorted by the consumer's name and the first consumer will initially be the only one receiving messages. This consumer is called the *master consumer*.

When the master consumer disconnects, all (non-acked and subsequent) messages will be delivered to the next consumer in line.

In the diagram above, Consumer-C-1 is the master consumer while Consumer-C-2 would be the next in line to receive messages if Consumer-C-2 disconnected.

{% include figure.html src="/img/failover-subscriptions.png" alt="Failover subscriptions" width="80" %}

### Multi-topic subscriptions

When a {% popover consumer %} subscribes to a Pulsar {% popover topic %}, by default it subscribes to one specific topic, such as `persistent://public/default/my-topic`. As of Pulsar version 1.23.0-incubating, however, Pulsar consumers can simultaneously subscribe to multiple topics. You can define a list of topics in two ways:

* On the basis of a [**reg**ular **ex**pression](https://en.wikipedia.org/wiki/Regular_expression) (regex), for example `persistent://public/default/finance-.*`
* By explicitly defining a list of topics

{% include admonition.html type="info" content="When subscribing to multiple topics by regex, all topics must be in the same [namespace](#namespaces)." %}

When subscribing to multiple topics, the Pulsar client will automatically make a call to the Pulsar API to discover the topics that match the regex pattern/list and then subscribe to all of them. If any of the topics don't currently exist, the consumer will auto-subscribe to them once the topics are created.

{% include admonition.html type="danger" title="No ordering guarantees"
   content="When a consumer subscribes to multiple topics, all ordering guarantees normally provided by Pulsar on single topics do not hold. If your use case for Pulsar involves any strict ordering requirements, we would strongly recommend against using this feature." %}

Here are some multi-topic subscription examples for Java:

```java
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient pulsarClient = // Instantiate Pulsar client object

// Subscribe to all topics in a namespace
Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default/.*");
Consumer allTopicsConsumer = pulsarClient.subscribe(allTopicsInNamespace, "subscription-1");

// Subscribe to a subsets of topics in a namespace, based on regex
Pattern someTopicsInNamespace = Pattern.compile("persistent://public/default/foo.*");
Consumer someTopicsConsumer = pulsarClient.subscribe(someTopicsInNamespace, "subscription-1");
```

For code examples, see:

* [Java](../../clients/Java#multi-topic-subscriptions)

### Partitioned topics

{% include explanations/partitioned-topics.md %}

### Non-persistent topics

{% include explanations/non-persistent-topics.md %}

{% include admonition.html type="success" content='For more info on using non-persistent topics, see the [Non-persistent messaging cookbook](../../cookbooks/non-persistent-topics).' %}

In non-persistent topics, {% popover brokers %} immediately deliver messages to all connected subscribers *without persisting them* in [BookKeeper](#persistent-storage). If a subscriber is disconnected, the broker will not be able to deliver those in-transit messages, and subscribers will never be able to receive those messages again. Eliminating the persistent storage step makes messaging on non-persistent topics slightly faster than on persistent topics in some cases, but with the caveat that some of the core benefits of Pulsar are lost.

{% include admonition.html type="danger" content="With non-persistent topics, message data lives only in memory. If a message broker fails or message data can otherwise not be retrieved from memory, your message data may be lost. Use non-persistent topics only if you're *certain* that your use case requires it and can sustain it." %}

By default, non-persistent topics are enabled on Pulsar {% popover brokers %}. You can disable them in the broker's [configuration](../../reference/Configuration#broker-enableNonPersistentTopics). You can manage non-persistent topics using the [`pulsar-admin non-persistent`](../../reference/CliTools#pulsar-admin-non-persistent) interface.

#### Performance

Non-persistent messaging is usually faster than persistent messaging because brokers don't persist messages and immediately send acks back to the producer as soon as that message is deliver to all connected subscribers. Producers thus see comparatively low publish latency with non-persistent topic.

#### Client API

Producers and consumers can connect to non-persistent topics in the same way as persistent topics, with the crucial difference that the topic name must start with `non-persistent`. All three subscription modes---[exclusive](#exclusive), [shared](#shared), and [failover](#failover)---are supported for non-persistent topics.

Here's an example [Java consumer](../../clients/Java#consumer) for a non-persistent topic:

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");
String npTopic = "non-persistent://public/default/my-topic";
String subscriptionName = "my-subscription-name";

Consumer consumer = client.subscribe(npTopic, subscriptionName);
```

Here's an example [Java producer](../../clients/Java#producer) for the same non-persistent topic:

```java
Producer producer = client.createProducer(npTopic);
```

## Architecture overview

At the highest level, a Pulsar {% popover instance %} is composed of one or more Pulsar {% popover clusters %}. Clusters within an instance can [replicate](#replicate) data amongst themselves.

In a Pulsar cluster:

* One or more {% popover brokers %} handles and load balances incoming messages from {% popover producers %}, dispatches messages to {% popover consumers %}, communicates with the Pulsar {% popover configuration store %} to handle various coordination tasks, stores messages in {% popover BookKeeper %} instances (aka {% popover bookies %}), relies on a cluster-specific {% popover ZooKeeper %} cluster for certain tasks, and more.
* A {% popover BookKeeper %} cluster consisting of one more or more {% popover bookies %} handles [persistent storage](#persistent-storage) of messages.
* A {% popover ZooKeeper %} cluster specific to that cluster handles

The diagram below provides an illustration of a Pulsar cluster:

{% include figure.html src="/img/pulsar-system-architecture.png" alt="Pulsar architecture diagram" width="90" %}

At the broader {% popover instance %} level, an instance-wide ZooKeeper cluster called the {% popover configuration store %} handles coordination tasks involving multiple clusters, for example [geo-replication](#replication).

## Brokers

The Pulsar message {% popover broker %} is a stateless component that's primarily responsible for running two other components:

* An HTTP server that exposes a REST API for both [administrative tasks](../../reference/RestApi) and [topic lookup](#client-setup-phase) for producers and consumers
* A {% popover dispatcher %}, which is an asynchronous TCP server over a custom [binary protocol](../../reference/BinaryProtocol) used for all data transfers

Messages are typically dispatched out of a [managed ledger](#managed-ledger) cache for the sake of performance, *unless* the backlog exceeds the cache size. If the backlog grows too large for the cache, the broker will start reading entries from {% popover BookKeeper %}.

Finally, to support {% popover geo-replication %} on global topics, the broker manages replicators that tail the entries published in the local region and republish them to the remote region using the Pulsar [Java client library](../../clients/Java).

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

* A {% popover configuration store %} quorum stores configuration for {% popover tenants %}, {% popover namespaces %}, and other entities that need to be globally consistent.
* Each cluster has its own local ZooKeeper ensemble that stores {% popover cluster %}-specific configuration and coordination such as ownership metadata, broker load reports, BookKeeper {% popover ledger %} metadata, and more.

## Persistent storage

Pulsar provides guaranteed message delivery for applications. If a message successfully reaches a Pulsar {% popover broker %}, it will be delivered to its intended target.

This guarantee requires that non-{% popover acknowledged %} messages are stored in a durable manner until they can be delivered to and acknowledged by {% popover consumers %}. This mode of messaging is commonly called *persistent messaging*. In Pulsar, N copies of all messages are stored and synced on disk, for example 4 copies across two servers with mirrored [RAID](https://en.wikipedia.org/wiki/RAID) volumes on each server.

### Apache BookKeeper {#bookkeeper}

Pulsar uses a system called [Apache BookKeeper](http://bookkeeper.apache.org/) for persistent message storage. BookKeeper is a distributed [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) (WAL) system that provides a number of crucial advantages for Pulsar:

* It enables Pulsar to utilize many independent logs, called [ledgers](#ledgers). Multiple ledgers can be created for {% popover topics %} over time.
* It offers very efficient storage for sequential data that handles entry replication.
* It guarantees read consistency of ledgers in the presence of various system failures.
* It offers even distribution of I/O across bookies.
* It's horizontally scalable in both capacity and throughput. Capacity can be immediately increased by adding more {% popover bookies %} to a cluster.
* {% popover Bookies %} are designed to handle thousands of ledgers with concurrent reads and writes. By using multiple disk devices---one for journal and another for general storage--bookies are able to isolate the effects of read operations from the latency of ongoing write operations.

In addition to message data, *cursors* are also persistently stored in BookKeeper. Cursors are {% popover subscription %} positions for {% popover consumers %}. BookKeeper enables Pulsar to store consumer position in a scalable fashion.

At the moment, Pulsar only supports persistent message storage. This accounts for the `persistent` in all {% popover topic %} names. Here's an example:

{% include topic.html ten="my-property" n="my-namespace" t="my-topic" %}

{% include admonition.html type="success" content='Pulsar also supports ephemeral ([non-persistent](#non-persistent-topics)) message storage.' %}

You can see an illustration of how {% popover brokers %} and {% popover bookies %} interact in the diagram below:

{% include figure.html src="/img/broker-bookie.png" alt="Brokers and bookies" width="80" %}

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

In BookKeeper, *journal* files contain BookKeeper transaction logs. Before making an update to a [ledger](#ledgers), a bookie needs to ensure that a transaction describing the update is written to persistent (non-volatile) storage. A new journal file is created once the bookie starts or the older journal file reaches the journal file size threshold (configured using the [`journalMaxSizeMB`](../../reference/Configuration#bookkeeper-journalMaxSizeMB) parameter).

### Non-persistent storage

A future version of BookKeeper will support *non-persistent messaging* and thus multiple durability modes at the topic level. This will enable you to set the durability mode at the topic level, replacing the `persistent` in topic names with a `non-persistent` indicator.

## Message retention and expiry

By default, Pulsar message {% popover brokers %}:

* immediately delete *all* messages that have been {% popover acknowledged %} by a {% popover consumer %}, and
* [persistently store](#persistent-storage) all unacknowledged messages in a message backlog.

Pulsar has two features, however, that enable you to override this default behavior:

* Message **retention** enables you to store messages that have been acknowledged by a consumer
* Message **expiry** enables you to set a time to live (TTL) for messages that have not yet been acknowledged

{% include admonition.html type="info" content='All message retention and expiry is managed at the [namespace](#namespaces) level. For a how-to, see the [Message retention and expiry](../../cookbooks/RetentionExpiry) cookbook.' %}

The diagram below illustrates both concepts:

{% include figure.html src="/img/retention-expiry.png" alt="Message retention and expiry" width="80" %}

With message retention, shown at the top, a <span style="color: #89b557;">retention policy</span> applied to all topics in a {% popover namespace %} dicates that some messages are durably stored in Pulsar even though they've already been acknowledged. Acknowledged messages that are not covered by the retention policy are <span style="color: #bb3b3e;">deleted</span>. Without a retention policy, *all* of the <span style="color: #19967d;">acknowledged messages</span> would be deleted.

With message expiry, shown at the bottom, some messages are <span style="color: #bb3b3e;">deleted</span>, even though they <span style="color: #337db6;">haven't been acknowledged</span>, because they've expired according to the <span style="color: #e39441;">TTL applied to the namespace</span> (for example because a TTL of 5 minutes has been applied and the messages haven't been acknowledged but are 10 minutes old).

## Pulsar Functions

For an in-depth look at Pulsar Functions, see the [Pulsar Functions overview](../../functions/overview).

## Replication

Pulsar enables messages to be produced and consumed in different geo-locations. For instance, your application may be publishing data in one region or market and you would like to process it for consumption in other regions or markets. [Geo-replication](../../admin/GeoReplication) in Pulsar enables you to do that.

## Message deduplication

Message **duplication** occurs when a message is [persisted](#persistent-storage) by Pulsar more than once. Message ***de*duplication** is an optional Pulsar feature that prevents unnecessary message duplication by processing each message only once, *even if the message is received more than once*.

The following diagram illustrates what happens when message deduplication is disabled vs. enabled:

{% include figure.html src="/img/message-deduplication.png" alt="Pulsar message deduplication" width="75" %}

Message deduplication is disabled in the scenario shown at the top. Here, a producer publishes message 1 on a topic; the message reaches a Pulsar {% popover broker %} and is [persisted](#persistent-storage) to BookKeeper. The producer then sends message 1 again (in this case due to some retry logic), and the message is received by the broker and stored in BookKeeper again, which means that duplication has occurred.

In the second scenario at the bottom, the producer publishes message 1, which is received by the broker and persisted, as in the first scenario. When the producer attempts to publish the message again, however, the broker knows that it has already seen message 1 and thus does not persist the message.

{% include admonition.html type="info" content='Message deduplication is handled at the namespace level. For more instructions, see the [message deduplication cookbook](../../cookbooks/message-deduplication).' %}

### Producer idempotency

The other available approach to message deduplication is to ensure that each message is *only produced once*. This approach is typically called **producer idempotency**. The drawback of this approach is that it defers the work of message deduplication to the application. In Pulsar, this is handled at the {% popover broker %} level, which means that you don't need to modify your Pulsar client code. Instead, you only need to make administrative changes (see the [Managing message deduplication](../../cookbooks/message-deduplication) cookbook for a guide).

### Deduplication and effectively-once semantics

Message deduplication makes Pulsar an ideal messaging system to be used in conjunction with stream processing engines (SPEs) and other systems seeking to provide [effectively-once](https://blog.streaml.io/exactly-once/) processing semantics. Messaging systems that don't offer automatic message deduplication require the SPE or other system to guarantee deduplication, which means that strict message ordering comes at the cost of burdening the application with the responsibility of deduplication. With Pulsar, strict ordering guarantees come at no application-level cost.

{% include admonition.html type="info" content='
More in-depth information can be found in [this post](https://blog.streaml.io/pulsar-effectively-once/) on the [Streamlio blog](https://blog.streaml.io).
' %}

## Multi-tenancy

Pulsar was created from the ground up as a {% popover multi-tenant %} system. To support multi-tenancy, Pulsar has a concept of {% popover tenants %}. Tenants can be spread across {% popover clusters %} and can each have their own [authentication and authorization](../../security/overview) scheme applied to them. They are also the administrative unit at which storage quotas, [message TTL](../../cookbooks/RetentionExpiry#time-to-live-ttl), and isolation policies can be managed.

The multi-tenant nature of Pulsar is reflected mostly visibly in topic URLs, which have this structure:

{% include topic.html ten="tenant" n="namespace" t="topic" %}

As you can see, the tenant is the most basic unit of categorization for topics (more fundamental than the {% popover namespace %} and topic name).

### Tenants and namespaces

{% include explanations/tenants-namespaces.md %}

## Authentication and Authorization

Pulsar supports a pluggable [authentication](../../security/overview) mechanism which can be configured at broker and it also supports authorization to identify client and its access rights on topics and tenants.

## Client interface

Pulsar exposes a client API with language bindings for [Java](../../clients/Java) and [C++](../../clients/Cpp). The client API optimizes and encapsulates Pulsar's client-broker communication protocol and exposes a simple and intuitive API for use by applications.

Under the hood, the current official Pulsar client libraries support transparent reconnection and/or connection failover to {% popover brokers %}, queuing of messages until {% popover acknowledged %} by the broker, and heuristics such as connection retries with backoff.

{% include admonition.html type="success" title="Custom client libraries" content="
If you'd like to create your own client library, we recommend consulting the documentation on Pulsar's custom [binary protocol](../../project/BinaryProtocol).
" %}

### Client setup phase

When an application wants to create a producer/consumer, the Pulsar client library will initiate a setup phase that is composed of two steps:

1. The client will attempt to determine the owner of the topic by sending an HTTP lookup request to the {% popover broker %}. The request could reach one of the active brokers which, by looking at the (cached) zookeeper metadata will know who is serving the topic or, in case nobody is serving it, will try to assign it to the least loaded broker.
1. Once the client library has the broker address, it will create a TCP connection (or reuse an existing connection from the pool) and authenticate it. Within this connection, client and broker exchange binary commands from a custom protocol. At this point the client will send a command to create producer/consumer to the broker, which will comply after having validated the authorization policy.

Whenever the TCP connection breaks, the client will immediately re-initiate this setup phase and will keep trying with exponential backoff to re-establish the producer or consumer until the operation succeeds.

## Pulsar proxy

One way for Pulsar clients to interact with a Pulsar [cluster](#clusters) is by connecting to Pulsar message [brokers](#brokers) directly. In some cases, however, this kind of direct connection is either infeasible or undesirable because the client doesn't have direct access to broker addresses. If you're running Pulsar in a cloud environment or on [Kubernetes](https://kubernetes.io) or an analogous platform, for example, then direct client connections to brokers are likely not possible.

The **Pulsar proxy** provides a solution to this problem by acting as a single gateway for all of the brokers in a cluster. If you run the Pulsar proxy (which, again, is optional), all client connections with the Pulsar {% popover cluster %} will flow through the proxy rather than communicating with brokers.

{% include admonition.html type="success" content="For the sake of performance and fault tolerance, you can run as many instances of the Pulsar proxy as you'd like." %}

Architecturally, the Pulsar proxy gets all the information it requires from ZooKeeper. When starting the proxy on a machine, you only need to provide ZooKeeper connection strings for the cluster-specific and instance-wide {% popover configuration store %} clusters. Here's an example:

```bash
$ bin/pulsar proxy \
  --zookeeper-servers zk-0,zk-1,zk-2 \
  --configuration-store-servers zk-0,zk-1,zk-2
```

{% include admonition.html type="info" title="Pulsar proxy docs" content='
For documentation on using the Pulsar proxy, see the [Pulsar proxy admin documentation](../../admin/Proxy).
' %}

Some important things to know about the Pulsar proxy:

* Connecting clients don't need to provide *any* specific configuration to use the Pulsar proxy. You won't need to update the client configuration for existing applications beyond updating the IP used for the service URL (for example if you're running a load balancer over the Pulsar proxy).
* [TLS encryption and authentication](../../security/tls) is supported by the Pulsar proxy

## Service discovery

[Clients](../../getting-started/Clients) connecting to Pulsar {% popover brokers %} need to be able to communicate with an entire Pulsar {% popover instance %} using a single URL. Pulsar provides a built-in service discovery mechanism that you can set up using the instructions in the [Deploying a Pulsar instance](../../deployment/InstanceSetup#service-discovery-setup) guide.

You can use your own service discovery system if you'd like. If you use your own system, there is just one requirement: when a client performs an HTTP request to an endpoint, such as `http://pulsar.us-west.example.com:8080`, the client needs to be redirected to *some* active broker in the desired {% popover cluster %}, whether via DNS, an HTTP or IP redirect, or some other means.

The diagram below illustrates Pulsar service discovery:

{% include figure.html src="/img/pulsar-service-discovery.png" width="50" %}

In this diagram, the Pulsar cluster is addressable via a single DNS name: `pulsar-cluster.acme.com`. A [Python client](../../clients/Python), for example, could access this Pulsar cluster like this:

```python
from pulsar import Client

client = Client('pulsar://pulsar-cluster.acme.com:6650')
```

## Reader interface

In Pulsar, the "standard" [consumer interface](#consumers) involves using {% popover consumers %} to listen on {% popover topics %}, process incoming messages, and finally {% popover acknowledge %} those messages when they've been processed. Whenever a consumer connects to a topic, it automatically begins reading from the earliest un-acked message onward because the topic's cursor is automatically managed by Pulsar.

The **reader interface** for Pulsar enables applications to manually manage cursors. When you use a reader to connect to a topic---rather than a consumer---you need to specify *which* message the reader begins reading from when it connects to a topic. When connecting to a topic, the reader interface enables you to begin with:

* The **earliest** available message in the topic
* The **latest** available message in the topic
* Some other message between the earliest and the latest. If you select this option, you'll need to explicitly provide a message ID. Your application will be responsible for "knowing" this message ID in advance, perhaps fetching it from a persistent data store or cache.

The reader interface is helpful for use cases like using Pulsar to provide [effectively-once](https://blog.streaml.io/exactly-once/) processing semantics for a stream processing system. For this use case, it's essential that the stream processing system be able to "rewind" topics to a specific message and begin reading there. The reader interface provides Pulsar clients with the low-level abstraction necessary to "manually position" themselves within a topic.

{% include figure.html src="/img/pulsar-reader-consumer-interfaces.png" alt="The Pulsar consumer and reader interfaces" width="80" %}

{% include admonition.html type="warning" title="Non-partitioned topics only"
content="The reader interface for Pulsar cannot currently be used with [partitioned topics](#partitioned-topics)." %}

Here's a Java example that begins reading from the earliest available message on a topic:

```java
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;

// Create a reader on a topic and for a specific message (and onward)
Reader<byte[]> reader = pulsarClient.newReader()
    .topic("reader-api-test")
    .startMessageId(MessageId.earliest)
    .create();

while (true) {
    Message message = reader.readNext();

    // Process the message
}
```

To create a reader that will read from the latest available message:

```java
Reader<byte[]> reader = pulsarClient.newReader()
    .topic(topic)
    .startMessageId(MessageId.latest)
    .create();
```

To create a reader that will read from some message between earliest and latest:

```java
byte[] msgIdBytes = // Some byte array
MessageId id = MessageId.fromByteArray(msgIdBytes);
Reader<byte[]> reader = pulsarClient.newReader()
    .topic(topic)
    .startMessageId(id)
    .create();
```

## Topic compaction {#compaction}

Pulsar was built with highly scalable [persistent storage](#persistent-storage) of message data as a primary objective. Pulsar {% popover topics %} enable you to persistently store as many unacknowledged messages as you need while preserving message ordering. By default, Pulsar stores *all* unacknowledged/unprocessed messages produced on a topic. Accumulating many unacknowledged messages on a topic is necessary for many Pulsar use cases but it can also be very time intensive for Pulsar {% popover consumers %} to "rewind" through the entire log of messages.

{% include admonition.html type="success" content="For a more practical guide to topic compaction, see the [Topic compaction cookbook](../../cookbooks/compaction)." %}

For some use cases consumers don't need a complete "image" of the topic log. They may only need a few values to construct a more "shallow" image of the log, perhaps even just the most recent value. For these kinds of use cases Pulsar offers **topic compaction**. When you run compaction on a topic, Pulsar goes through a topic's backlog and removes messages that are *obscured* by later messages, i.e. it goes through the topic on a per-key basis and leaves only the most recent message associated with that key.

Pulsar's topic compaction feature:

* Allows for faster "rewind" through topic logs
* Applies only to [persistent topics](#persistent-storage)
* Triggered automatically when the backlog reaches a certain size or can be triggered manually via the command line. See the [Topic compaction cookbook](../../cookbooks/compaction)
* Is conceptually and operationally distinct from [retention and expiry](#message-retention-and-expiry). Topic compaction *does*, however, respect retention. If retention has removed a message from the message backlog of a topic, the message will also not be readable from the compacted topic ledger.

{% include admonition.html type="info" title="Topic compaction example: the stock ticker"
   content="An example use case for a compacted Pulsar topic would be a stock ticker topic. On a stock ticker topic, each message bears a timestamped dollar value for stocks for purchase (with the message key holding the stock symbol, e.g. `AAPL` or `GOOG`). With a stock ticker you may care only about the most recent value(s) of the stock and have no interest in historical data (i.e. you don't need to construct a complete image of the topic's sequence of messages per key). Compaction would be highly beneficial in this case because it would keep consumers from needing to rewind through obscured messages." %}

### How topic compaction works

When topic compaction is triggered [via the CLI](../../cookbooks/compaction), Pulsar will iterate over the entire topic from beginning to end. For each key that it encounters the compaction routine will keep a record of the latest occurrence of that key.

After that, the broker will create a new [BookKeeper ledger](#ledgers) and make a second iteration through each message on the topic. For each message, if the key matches the latest occurrence of that key, then the key's data payload, message ID, and metadata will be written to the newly created ledger. If the key doesn't match the latest then the message will be skipped and left alone. If any given message has an empty payload, it will be skipped and considered deleted (akin to the concept of [tombstones](https://en.wikipedia.org/wiki/Tombstone_(data_store)) in key-value databases). At the end of this second iteration through the topic, the newly created BookKeeper ledger is closed and two things are written to the topic's metadata: the ID of the BookKeeper ledger and the message ID of the last compacted message (this is known as the **compaction horizon** of the topic). Once this metadata is written compaction is complete.

After the initial compaction operation, the Pulsar {% popover broker %} that owns the topic is notified whenever any future changes are made to the compaction horizon and compacted backlog. When such changes occur:

* Clients (consumers and readers) that have read compacted enabled will attempt to read messages from a topic and either:
  * Read from the topic like normal (if the message ID is greater than or equal to the compaction horizon) or
  * Read beginning at the compaction horizon (if the message ID is lower than the compaction horizon)

## Tiered Storage

Pulsar's segment oriented architecture allows for topic backlogs to grow very large, effectively without limit. However, this can become expensive over time.

One way to alleviate this cost is to use Tiered Storage. With tiered storage, older messages in the backlog can be moved from bookkeeper to a cheaper storage mechanism, while still allowing clients to access the backlog as if nothing had changed. 

{% include figure.html src="/img/pulsar-tiered-storage.png" alt="Tiered Storage" width="80" %}

{% include admonition.html type="info" content="Data written to bookkeeper is replicated to 3 physical machines by default. However, once a segment is sealed in bookkeeper is becomes immutable and can be copied to long term storage. Long term storage can achieve cost savings by using mechanisms such as [Reed-Solomon error correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction) to require fewer physical copies of data." %}

Pulsar currently supports S3 as a long term store. Offloading to S3 triggered via a Rest API or command line interface. The user passes in the amount of topic data they wish to retain on bookkeeper, and the broker will copy the backlog data to S3. The original data will then be deleted from bookkeeper after a configured delay (4 hours by default).

{% include admonition.html type="info" content="For a guide for setting up tiered storage, see the [Tiered storage cookbook](../../cookbooks/tiered-storage)." %}

## Schema registry

Type safety is extremely important in any application built around a message bus like Pulsar. {% popover Producers %} and {% popover consumers %} need some kind of mechanism for coordinating types at the {% popover topic %} level lest a wide variety of potential problems arise (for example serialization and deserialization issues). Applications typically adopt one of two basic approaches to type safety in messaging:

1. A "client-side" approach in which message producers and consumers are responsible for not only serializing and deserializing messages (which consist of raw bytes) but also "knowing" which types are being transmitted via which topics. If a producer is sending temperature sensor data on the topic `topic-1`, consumers of that topic will run into trouble if they attempt to parse that data as, say, moisture sensor readings.
1. A "server-side" approach in which producers and consumers inform the system which data types can be transmitted via the topic. With this approach, the messaging system enforces type safety and ensures that producers and consumers remain synced.

Both approaches are available in Pulsar, and you're free to adopt one or the other or to mix and match on a per-topic basis.

1. For the "client-side" approach, producers and consumers can send and receive messages consisting of raw byte arrays and leave all type safety enforcement to the application on an "out-of-band" basis.
1. For the "server-side" approach, Pulsar has a built-in **schema registry** that enables clients to upload data schemas on a per-topic basis. Those schemas dictate which data types are recognized as valid for that topic.

{% include admonition.html type="info" content="The Pulsar schema registry is currently available only for the [Java client](../../clients/Java)." %}

### Basic architecture

In Pulsar, schemas are uploaded to, fetched from, and update via Pulsar's [REST API](../../reference/RestApi).

{% include admonition.html type="success" title="Other schema registry backends"
  content="Out of the box, Pulsar uses the [Apache BookKeeper](#persistent-storage) log storage system for schema storage. You can, however, use different backends if you wish. Documentation for custom schema storage logic is coming soon." %}

### How schemas work

Pulsar schemas are applied and enforced *at the topic level* (schemas cannot be applied at the {% popover namespace %} or {% popover tenant %} level). Producers and consumers upload schemas to Pulsar {% popover brokers %}.

Pulsar schemas are fairly simple data structures that consist of:

* A **name**. In Pulsar, a schema's name is the {% popover topic %} to which the schema is applied.
* A **payload**, which is a binary representation of the schema
* A schema [**type**](#schema-types)
* User-defined **properties** as a string/string map. Usage of properties is wholly application specific. Possible properties might be the Git hash associated with a schema, an environment like `dev` or `prod`, etc.

### Schema versions

In order to illustrate how schema versioning works, let's walk through an example. Imagine that the Pulsar [Java client](../../clients/Java) created using the code below attempts to connect to Pulsar and begin sending messages:

```java
PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

Producer<SensorReading> producer = client.newProducer(JSONSchema.of(SensorReading.class))
        .topic("sensor-data")
        .sendTimeout(3, TimeUnit.SECONDS)
        .create();
```

The table below lists the possible scenarios when this connection attempt occurs and what will happen in light of each scenario:

Scenario | What happens
:--------|:------------
No schema exists for the topic | The {% popover producer %} is created using the given schema. The schema is transmitted to the {% popover broker %} and stored (since no existing schema is "compatible" with the `SensorReading` schema). Any {% popover consumer %} created using the same schema/topic can consume messages from the `sensor-data` topic.
A schema already exists; the producer connects using the same schema that's already stored | The schema is transmitted to the Pulsar broker. The broker determines that the schema is compatible. The broker attempts to store the schema in [BookKeeper](#persistent-storage) but then determines that it's already stored, so it's then used to tag produced messages.
A schema already exists; the producer connects using a new schema that is compatible | The producer transmits the schema to the broker. The broker determines that the schema is compatible and stores the new schema as the current version (with a new version number).

{% include admonition.html type="info" content="Schemas are versioned in succession. Schema storage happens in the broker that handles the associated topic so that version assignments can be made. Once a version is assigned/fetched to/for a schema, all subsequent messages produced by that producer are tagged with the appropriate version." %}

### Supported schema formats {#schema-types}

The following formats are supported by the Pulsar schema registry:

* None. If no schema is specified for a topic, producers and consumers will handle raw bytes.
* `String` (used for UTF-8-encoded strings)
* [JSON](https://www.json.org/)

For usage instructions, see the documentation for your preferred client library:

* [Java](../../clients/Java#schemas)

{% include admonition.html type="success" content="Support for other schema formats will be added in future releases of Pulsar." %}
