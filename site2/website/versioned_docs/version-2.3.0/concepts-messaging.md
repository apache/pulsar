---
id: version-2.3.0-concepts-messaging
title: Messaging Concepts
sidebar_label: Messaging
original_id: concepts-messaging
---

Pulsar is built on the [publish-subscribe](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) pattern, aka pub-sub. In this pattern, [producers](#producers) publish messages to [topics](#topics). [Consumers](#consumers) can then [subscribe](#subscription-modes) to those topics, process incoming messages, and send an acknowledgement when processing is complete.

Once a subscription has been created, all messages will be [retained](concepts-architecture-overview.md#persistent-storage) by Pulsar, even if the consumer gets disconnected. Retained messages will be discarded only when a consumer acknowledges that they've been successfully processed.

## Messages

Messages are the basic "unit" of Pulsar. They're what producers publish to topics and what consumers then consume from topics (and acknowledge when the message has been processed). Messages are the analogue of letters in a postal service system.

Component | Purpose
:---------|:-------
Value / data payload | The data carried by the message. All Pulsar messages carry raw bytes, although message data can also conform to data [schemas](concepts-schema-registry.md)
Key | Messages can optionally be tagged with keys, which can be useful for things like [topic compaction](concepts-topic-compaction.md)
Properties | An optional key/value map of user-defined properties
Producer name | The name of the producer that produced the message (producers are automatically given default names, but you can apply your own explicitly as well)
Sequence ID | Each Pulsar message belongs to an ordered sequence on its topic. A message's sequence ID is its ordering in that sequence.
Publish time | The timestamp of when the message was published (automatically applied by the producer)
Event time | An optional timestamp that applications can attach to the message representing when something happened, e.g. when the message was processed. The event time of a message is 0 if none is explicitly set.


> For a more in-depth breakdown of Pulsar message contents, see the documentation on Pulsar's [binary protocol](developing-binary-protocol.md).

## Producers

A producer is a process that attaches to a topic and publishes messages to a Pulsar [broker](reference-terminology.md#broker) for processing.

### Send modes

Producers can send messages to brokers either synchronously (sync) or asynchronously (async).

| Mode       | Description                                                                                                                                                                                                                                                                                                                                                              |
|:-----------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Sync send  | The producer will wait for acknowledgement from the broker after sending each message. If acknowledgment isn't received then the producer will consider the send operation a failure.                                                                                                                                                                                    |
| Async send | The producer will put the message in a blocking queue and return immediately. The client library will then send the message to the broker in the background. If the queue is full (max size [configurable](reference-configuration.md#broker), the producer could be blocked or fail immediately when calling the API, depending on arguments passed to the producer. |

### Compression

Messages published by producers can be compressed during transportation in order to save bandwidth. Pulsar currently supports two types of compression:

* [LZ4](https://github.com/lz4/lz4)
* [ZLIB](https://zlib.net/)
* [ZSTD](https://facebook.github.io/zstd/)

### Batching

If batching is enabled, the producer will accumulate and send a batch of messages in a single request. Batching size is defined by the maximum number of messages and maximum publish latency.

## Consumers

A consumer is a process that attaches to a topic via a subscription and then receives messages.

### Receive modes

Messages can be received from [brokers](reference-terminology.md#broker) either synchronously (sync) or asynchronously (async).

| Mode          | Description                                                                                                                                                                                                   |
|:--------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Sync receive  | A sync receive will be blocked until a message is available.                                                                                                                                                  |
| Async receive | An async receive will return immediately with a future value---a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture) in Java, for example---that completes once a new message is available. |

### Acknowledgement

When a consumer has successfully processed a message, it needs to send an acknowledgement to the broker so that the broker can discard the message (otherwise it [stores](concepts-architecture-overview.md#persistent-storage) the message).

Messages can be acknowledged either one by one or cumulatively. With cumulative acknowledgement, the consumer only needs to acknowledge the last message it received. All messages in the stream up to (and including) the provided message will not be re-delivered to that consumer.


> Cumulative acknowledgement cannot be used with [shared subscription mode](#subscription-modes), because shared mode involves multiple consumers having access to the same subscription.

### Listeners

Client libraries can provide their own listener implementations for consumers. The [Java client](client-libraries-java.md), for example, provides a {@inject: javadoc:MesssageListener:/client/org/apache/pulsar/client/api/MessageListener} interface. In this interface, the `received` method is called whenever a new message is received.

## Topics

As in other pub-sub systems, topics in Pulsar are named channels for transmitting messages from [producers](reference-terminology.md#producer) to [consumers](reference-terminology.md#consumer). Topic names are URLs that have a well-defined structure:

```http
{persistent|non-persistent}://tenant/namespace/topic
```

Topic name component | Description
:--------------------|:-----------
`persistent` / `non-persistent` | This identifies the type of topic. Pulsar supports two kind of topics: [persistent](concepts-architecture-overview.md#persistent-storage) and [non-persistent](#non-persistent-topics) (persistent is the default, so if you don't specify a type the topic will be persistent). With persistent topics, all messages are durably [persisted](concepts-architecture-overview.md#persistent-storage) on disk (that means on multiple disks unless the broker is standalone), whereas data for [non-persistent](#non-persistent-topics) topics isn't persisted to storage disks.
`tenant`             | The topic's tenant within the instance. Tenants are essential to multi-tenancy in Pulsar and can be spread across clusters.
`namespace`          | The administrative unit of the topic, which acts as a grouping mechanism for related topics. Most topic configuration is performed at the [namespace](#namespaces) level. Each tenant can have multiple namespaces.
`topic`              | The final part of the name. Topic names are freeform and have no special meaning in a Pulsar instance.


> #### No need to explicitly create new topics
> You don't need to explicitly create topics in Pulsar. If a client attempts to write or receive messages to/from a topic that does not yet exist, Pulsar will automatically create that topic under the [namespace](#namespaces) provided in the [topic name](#topics).
> If no tenant or namespace is specified when a client creates a topic, the topic is created in the default tenant and namespace. You can also create a topic in a specified tenant and namespace, such as `persistent://my-tenant/my-namespace/my-topic`. `persistent://my-tenant/my-namespace/my-topic` means the `my-topic` topic is created in the `my-namespace` namespace of the `my-tenant` tenant.


## Namespaces

A namespace is a logical nomenclature within a tenant. A tenant can create multiple namespaces via the [admin API](admin-api-namespaces.md#create). For instance, a tenant with different applications can create a separate namespace for each application. A namespace allows the application to create and manage a hierarchy of topics. The topic `my-tenant/app1` is a namespace for the application `app1` for `my-tenant`. You can create any number of [topics](#topics) under the namespace.

## Subscription modes

A subscription is a named configuration rule that determines how messages are delivered to consumers. There are three available subscription modes in Pulsar: [exclusive](#exclusive), [shared](#shared), and [failover](#failover). These modes are illustrated in the figure below.

![Subscription modes](assets/pulsar-subscription-modes.png)

### Exclusive

In *exclusive* mode, only a single consumer is allowed to attach to the subscription. If more than one consumer attempts to subscribe to a topic using the same subscription, the consumer receives an error.

In the diagram above, only **Consumer A-0** is allowed to consume messages.

> Exclusive mode is the default subscription mode.

![Exclusive subscriptions](assets/pulsar-exclusive-subscriptions.png)

### Shared

In *shared* or *round robin* mode, multiple consumers can attach to the same subscription. Messages are delivered in a round robin distribution across consumers, and any given message is delivered to only one consumer. When a consumer disconnects, all the messages that were sent to it and not acknowledged will be rescheduled for sending to the remaining consumers.

In the diagram above, **Consumer-B-1** and **Consumer-B-2** are able to subscribe to the topic, but **Consumer-C-1** and others could as well.

> #### Limitations of shared mode
> There are two important things to be aware of when using shared mode:
> * Message ordering is not guaranteed.
> * You cannot use cumulative acknowledgment with shared mode.

![Shared subscriptions](assets/pulsar-shared-subscriptions.png)

### Failover

In *failover* mode, multiple consumers can attach to the same subscription. The consumers will be lexically sorted by the consumer's name and the first consumer will initially be the only one receiving messages. This consumer is called the *master consumer*.

When the master consumer disconnects, all (non-acked and subsequent) messages will be delivered to the next consumer in line.

In the diagram above, Consumer-C-1 is the master consumer while Consumer-C-2 would be the next in line to receive messages if Consumer-C-1 disconnected.

![Failover subscriptions](assets/pulsar-failover-subscriptions.png)

## Multi-topic subscriptions

When a consumer subscribes to a Pulsar topic, by default it subscribes to one specific topic, such as `persistent://public/default/my-topic`. As of Pulsar version 1.23.0-incubating, however, Pulsar consumers can simultaneously subscribe to multiple topics. You can define a list of topics in two ways:

* On the basis of a [**reg**ular **ex**pression](https://en.wikipedia.org/wiki/Regular_expression) (regex), for example `persistent://public/default/finance-.*`
* By explicitly defining a list of topics

> When subscribing to multiple topics by regex, all topics must be in the same [namespace](#namespaces)

When subscribing to multiple topics, the Pulsar client will automatically make a call to the Pulsar API to discover the topics that match the regex pattern/list and then subscribe to all of them. If any of the topics don't currently exist, the consumer will auto-subscribe to them once the topics are created.

> #### No ordering guarantees across multiple topics
> When a producer sends messages to a single topic, all messages are guaranteed to be read from that topic in the same order. However, these guarantees do not hold across multiple topics. So when a producer sends message to multiple topics, the order in which messages are read from those topics is not guaranteed to be the same.

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

* [Java](client-libraries-java.md#multi-topic-subscriptions)

## Partitioned topics

Normal topics can be served only by a single broker, which limits the topic's maximum throughput. *Partitioned topics* are a special type of topic that be handled by multiple brokers, which allows for much higher throughput.

Behind the scenes, a partitioned topic is actually implemented as N internal topics, where N is the number of partitions. When publishing messages to a partitioned topic, each message is routed to one of several brokers. The distribution of partitions across brokers is handled automatically by Pulsar.

The diagram below illustrates this:

![](assets/partitioning.png)

Here, the topic **Topic1** has five partitions (**P0** through **P4**) split across three brokers. Because there are more partitions than brokers, two brokers handle two partitions a piece, while the third handles only one (again, Pulsar handles this distribution of partitions automatically).

Messages for this topic are broadcast to two consumers. The [routing mode](#routing-modes) determines both which broker handles each partition, while the [subscription mode](#subscription-modes) determines which messages go to which consumers.

Decisions about routing and subscription modes can be made separately in most cases. In general, throughput concerns should guide partitioning/routing decisions while subscription decisions should be guided by application semantics.

There is no difference between partitioned topics and normal topics in terms of how subscription modes work, as partitioning only determines what happens between when a message is published by a producer and processed and acknowledged by a consumer.

Partitioned topics need to be explicitly created via the [admin API](admin-api-overview.md). The number of partitions can be specified when creating the topic.

### Routing modes

When publishing to partitioned topics, you must specify a *routing mode*. The routing mode determines which partition---that is, which internal topic---each message should be published to.

There are three {@inject: javadoc:MessageRoutingMode:/client/org/apache/pulsar/client/api/MessageRoutingMode} available:

Mode     | Description 
:--------|:------------
`RoundRobinPartition` | If no key is provided, the producer will publish messages across all partitions in round-robin fashion to achieve maximum throughput. Please note that round-robin is not done per individual message but rather it's set to the same boundary of batching delay, to ensure batching is effective. While if a key is specified on the message, the partitioned producer will hash the key and assign message to a particular partition. This is the default mode. 
`SinglePartition`     | If no key is provided, the producer will randomly pick one single partition and publish all the messages into that partition. While if a key is specified on the message, the partitioned producer will hash the key and assign message to a particular partition.
`CustomPartition`     | Use custom message router implementation that will be called to determine the partition for a particular message. User can create a custom routing mode by using the [Java client](client-libraries-java.md) and implementing the {@inject: javadoc:MessageRouter:/client/org/apache/pulsar/client/api/MessageRouter} interface.

### Ordering guarantee

The ordering of messages is related to MessageRoutingMode and Message Key. Usually, user would want an ordering of Per-key-partition guarantee.

If there is a key attached to message, the messages will be routed to corresponding partitions based on the hashing scheme specified by {@inject: javadoc:HashingScheme:/client/org/apache/pulsar/client/api/HashingScheme} in {@inject: javadoc:ProducerBuilder:/client/org/apache/pulsar/client/api/ProducerBuilder}, when using either `SinglePartition` or `RoundRobinPartition` mode.

Ordering guarantee | Description | Routing Mode and Key
:------------------|:------------|:------------
Per-key-partition  | All the messages with the same key will be in order and be placed in same partition. | Use either `SinglePartition` or `RoundRobinPartition` mode, and Key is provided by each message.
Per-producer       | All the messages from the same producer will be in order. | Use `SinglePartition` mode, and no Key is provided for each message.

### Hashing scheme

{@inject: javadoc:HashingScheme:/client/org/apache/pulsar/client/api/HashingScheme} is an enum that represent sets of standard hashing functions available when choosing the partition to use for a particular message.

There are 2 types of standard hashing functions available: `JavaStringHash` and `Murmur3_32Hash`. 
The default hashing function for producer is `JavaStringHash`.
Please pay attention that `JavaStringHash` is not useful when producers can be from different multiple language clients, under this use case, it is recommended to use `Murmur3_32Hash`.



## Non-persistent topics


By default, Pulsar persistently stores *all* unacknowledged messages on multiple [BookKeeper](concepts-architecture-overview.md#persistent-storage) bookies (storage nodes). Data for messages on persistent topics can thus survive broker restarts and subscriber failover.

Pulsar also, however, supports **non-persistent topics**, which are topics on which messages are *never* persisted to disk and live only in memory. When using non-persistent delivery, killing a Pulsar broker or disconnecting a subscriber to a topic means that all in-transit messages are lost on that (non-persistent) topic, meaning that clients may see message loss.

Non-persistent topics have names of this form (note the `non-persistent` in the name):

```http
non-persistent://tenant/namespace/topic
```

> For more info on using non-persistent topics, see the [Non-persistent messaging cookbook](cookbooks-non-persistent.md).

In non-persistent topics, brokers immediately deliver messages to all connected subscribers *without persisting them* in [BookKeeper](concepts-architecture-overview.md#persistent-storage). If a subscriber is disconnected, the broker will not be able to deliver those in-transit messages, and subscribers will never be able to receive those messages again. Eliminating the persistent storage step makes messaging on non-persistent topics slightly faster than on persistent topics in some cases, but with the caveat that some of the core benefits of Pulsar are lost.

> With non-persistent topics, message data lives only in memory. If a message broker fails or message data can otherwise not be retrieved from memory, your message data may be lost. Use non-persistent topics only if you're *certain* that your use case requires it and can sustain it.

By default, non-persistent topics are enabled on Pulsar brokers. You can disable them in the broker's [configuration](reference-configuration.md#broker-enableNonPersistentTopics). You can manage non-persistent topics using the [`pulsar-admin topics`](referencereference--pulsar-admin/#topics-1) interface.

### Performance

Non-persistent messaging is usually faster than persistent messaging because brokers don't persist messages and immediately send acks back to the producer as soon as that message is deliver to all connected subscribers. Producers thus see comparatively low publish latency with non-persistent topic.

### Client API

Producers and consumers can connect to non-persistent topics in the same way as persistent topics, with the crucial difference that the topic name must start with `non-persistent`. All three subscription modes---[exclusive](#exclusive), [shared](#shared), and [failover](#failover)---are supported for non-persistent topics.

Here's an example [Java consumer](client-libraries-java.md#consumers) for a non-persistent topic:

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");
String npTopic = "non-persistent://public/default/my-topic";
String subscriptionName = "my-subscription-name";

Consumer consumer = client.subscribe(npTopic, subscriptionName);
```

Here's an example [Java producer](client-libraries-java.md#producer) for the same non-persistent topic:

```java
Producer producer = client.createProducer(npTopic);
```

## Message retention and expiry

By default, Pulsar message brokers:

* immediately delete *all* messages that have been acknowledged by a consumer, and
* [persistently store](concepts-architecture-overview.md#persistent-storage) all unacknowledged messages in a message backlog.

Pulsar has two features, however, that enable you to override this default behavior:

* Message **retention** enables you to store messages that have been acknowledged by a consumer
* Message **expiry** enables you to set a time to live (TTL) for messages that have not yet been acknowledged

> All message retention and expiry is managed at the [namespace](#namespaces) level. For a how-to, see the [Message retention and expiry](cookbooks-retention-expiry.md) cookbook.

The diagram below illustrates both concepts:

![Message retention and expiry](assets/retention-expiry.png)

With message retention, shown at the top, a <span style="color: #89b557;">retention policy</span> applied to all topics in a namespace dictates that some messages are durably stored in Pulsar even though they've already been acknowledged. Acknowledged messages that are not covered by the retention policy are <span style="color: #bb3b3e;">deleted</span>. Without a retention policy, *all* of the <span style="color: #19967d;">acknowledged messages</span> would be deleted.

With message expiry, shown at the bottom, some messages are <span style="color: #bb3b3e;">deleted</span>, even though they <span style="color: #337db6;">haven't been acknowledged</span>, because they've expired according to the <span style="color: #e39441;">TTL applied to the namespace</span> (for example because a TTL of 5 minutes has been applied and the messages haven't been acknowledged but are 10 minutes old).

## Message deduplication

Message **duplication** occurs when a message is [persisted](concepts-architecture-overview.md#persistent-storage) by Pulsar more than once. Message ***de*duplication** is an optional Pulsar feature that prevents unnecessary message duplication by processing each message only once, *even if the message is received more than once*.

The following diagram illustrates what happens when message deduplication is disabled vs. enabled:

![Pulsar message deduplication](assets/message-deduplication.png)


Message deduplication is disabled in the scenario shown at the top. Here, a producer publishes message 1 on a topic; the message reaches a Pulsar broker and is [persisted](concepts-architecture-overview.md#persistent-storage) to BookKeeper. The producer then sends message 1 again (in this case due to some retry logic), and the message is received by the broker and stored in BookKeeper again, which means that duplication has occurred.

In the second scenario at the bottom, the producer publishes message 1, which is received by the broker and persisted, as in the first scenario. When the producer attempts to publish the message again, however, the broker knows that it has already seen message 1 and thus does not persist the message.

> Message deduplication is handled at the namespace level. For more instructions, see the [message deduplication cookbook](cookbooks-deduplication.md).


### Producer idempotency

The other available approach to message deduplication is to ensure that each message is *only produced once*. This approach is typically called **producer idempotency**. The drawback of this approach is that it defers the work of message deduplication to the application. In Pulsar, this is handled at the [broker](reference-terminology.md#broker) level, which means that you don't need to modify your Pulsar client code. Instead, you only need to make administrative changes (see the [Managing message deduplication](cookbooks-deduplication.md) cookbook for a guide).

### Deduplication and effectively-once semantics

Message deduplication makes Pulsar an ideal messaging system to be used in conjunction with stream processing engines (SPEs) and other systems seeking to provide [effectively-once](https://streaml.io/blog/exactly-once) processing semantics. Messaging systems that don't offer automatic message deduplication require the SPE or other system to guarantee deduplication, which means that strict message ordering comes at the cost of burdening the application with the responsibility of deduplication. With Pulsar, strict ordering guarantees come at no application-level cost.

> More in-depth information can be found in [this post](https://streaml.io/blog/pulsar-effectively-once/) on the [Streamlio blog](https://streaml.io/blog)


