---
id: concepts-messaging
title: Messaging
sidebar_label: Messaging
---

Pulsar is built on the [publish-subscribe](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) pattern (often abbreviated to pub-sub). In this pattern, [producers](#producers) publish messages to [topics](#topics); [consumers](#consumers) [subscribe](#subscription-types) to those topics, process incoming messages, and send [acknowledgements](#acknowledgement) to the broker when processing is finished.

When a subscription is created, Pulsar [retains](concepts-architecture-overview.md#persistent-storage) all messages, even if the consumer is disconnected. The retained messages are discarded only when a consumer acknowledges that all these messages are processed successfully. 

If the consumption of a message fails and you want this message to be consumed again, you can enable [message redelivery mechanism](#message-redelivery) to request the broker to resend this message.

## Messages

Messages are the basic "unit" of Pulsar. The following table lists the components of messages.

Component | Description
:---------|:-------
Value / data payload | The data carried by the message. All Pulsar messages contain raw bytes, although message data can also conform to data [schemas](schema-get-started.md).
Key | Messages are optionally tagged with keys, which is useful for things like [topic compaction](concepts-topic-compaction.md).
Properties | An optional key/value map of user-defined properties.
Producer name | The name of the producer who produces the message. If you do not specify a producer name, the default name is used. 
Sequence ID | Each Pulsar message belongs to an ordered sequence on its topic. The sequence ID of the message is its order in that sequence.
Publish time | The timestamp of when the message is published. The timestamp is automatically applied by the producer.
Event time | An optional timestamp attached to a message by applications. For example, applications attach a timestamp on when the message is processed. If nothing is set to event time, the value is `0`. 
TypedMessageBuilder | It is used to construct a message. You can set message properties such as the message key, message value with `TypedMessageBuilder`. </br> When you set `TypedMessageBuilder`, set the key as a string. If you set the key as other types, for example, an AVRO object, the key is sent as bytes, and it is difficult to get the AVRO object back on the consumer.

The default size of a message is 5 MB. You can configure the max size of a message with the following configurations.

- In the `broker.conf` file.

    ```bash
    # The max size of a message (in bytes).
    maxMessageSize=5242880
    ```

- In the `bookkeeper.conf` file.

    ```bash
    # The max size of the netty frame (in bytes). Any messages received larger than this value are rejected. The default value is 5 MB.
    nettyMaxFrameSizeBytes=5253120
    ```
> For more information on Pulsar messages, see Pulsar [binary protocol](developing-binary-protocol.md).

## Producers

A producer is a process that attaches to a topic and publishes messages to a Pulsar [broker](reference-terminology.md#broker). The Pulsar broker processes the messages.

### Send modes

Producers send messages to brokers synchronously (sync) or asynchronously (async).

| Mode       | Description |
|:-----------|-----------|
| Sync send  | The producer waits for an acknowledgement from the broker after sending every message. If the acknowledgment is not received, the producer treats the sending operation as a failure.                                                                                                                                                                                    |
| Async send | The producer puts a message in a blocking queue and returns immediately. The client library sends the message to the broker in the background. If the queue is full (you can [configure](reference-configuration.md#broker) the maximum size), the producer is blocked or fails immediately when calling the API, depending on arguments passed to the producer. |

### Access mode

You can have different types of access modes on topics for producers.

|Access mode | Description
|---|---
`Shared`|Multiple producers can publish on a topic. <br><br>This is the **default** setting.
`Exclusive`|Only one producer can publish on a topic. <br><br>If there is already a producer connected, other producers trying to publish on this topic get errors immediately.<br><br>The “old” producer is evicted and a “new” producer is selected to be the next exclusive producer if the “old” producer experiences a network partition with the broker.
`WaitForExclusive`|If there is already a producer connected, the producer creation is pending (rather than timing out) until the producer gets the `Exclusive` access.<br><br>The producer that succeeds in becoming the exclusive one is treated as the leader. Consequently, if you want to implement the leader election scheme for your application, you can use this access mode.

> **Note**
>
> Once an application creates a producer with `Exclusive` or `WaitForExclusive` access mode successfully, the instance of this application is guaranteed to be the **only writer** to the topic. Any other producers trying to produce messages on this topic will either get errors immediately or have to wait until they get the `Exclusive` access. 
> 
> For more information, see [PIP 68: Exclusive Producer](https://github.com/apache/pulsar/wiki/PIP-68:-Exclusive-Producer).

You can set producer access mode through Java Client API. For more information, see `ProducerAccessMode` in [ProducerBuilder.java](https://github.com/apache/pulsar/blob/fc5768ca3bbf92815d142fe30e6bfad70a1b4fc6/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/ProducerBuilder.java) file.


### Compression

You can compress messages published by producers during transportation. Pulsar currently supports the following types of compression:

* [LZ4](https://github.com/lz4/lz4)
* [ZLIB](https://zlib.net/)
* [ZSTD](https://facebook.github.io/zstd/)
* [SNAPPY](https://google.github.io/snappy/)

### Batching

When batching is enabled, the producer accumulates and sends a batch of messages in a single request. The batch size is defined by the maximum number of messages and the maximum publish latency. Therefore, the backlog size represents the total number of batches instead of the total number of messages.

In Pulsar, batches are tracked and stored as single units rather than as individual messages. Consumer unbundles a batch into individual messages. However, scheduled messages (configured through the `deliverAt` or the `deliverAfter` parameter) are always sent as individual messages even batching is enabled.

In general, a batch is acknowledged when all of its messages are acknowledged by a consumer. It means that when **not all** batch messages are acknowledged, then unexpected failures, negative acknowledgements, or acknowledgement timeouts can result in a redelivery of all messages in this batch.

To avoid redelivering acknowledged messages in a batch to the consumer, Pulsar introduces batch index acknowledgement since Pulsar 2.6.0. When batch index acknowledgement is enabled, the consumer filters out the batch index that has been acknowledged and sends the batch index acknowledgement request to the broker. The broker maintains the batch index acknowledgement status and tracks the acknowledgement status of each batch index to avoid dispatching acknowledged messages to the consumer. The batch is deleted when all indices of the messages in it are acknowledged.

By default, batch index acknowledgement is disabled (`acknowledgmentAtBatchIndexLevelEnabled=false`). You can enable batch index acknowledgement by setting the `acknowledgmentAtBatchIndexLevelEnabled` parameter to `true` at the broker side. Enabling batch index acknowledgement results in more memory overheads. 

### Chunking
Message chunking enables Pulsar to process large payload messages by splitting the message into chunks at the producer side and aggregating chunked messages at the consumer side.

With message chunking enabled, when the size of a message exceeds the allowed maximum payload size (the `maxMessageSize` parameter of broker), the workflow of messaging is as follows:
1. The producer splits the original message into chunked messages and publishes them with chunked metadata to the broker separately and in order. 
2. The broker stores the chunked messages in one managed-ledger in the same way as that of ordinary messages, and it uses the `chunkedMessageRate` parameter to record chunked message rate on the topic.
3. The consumer buffers the chunked messages and aggregates them into the receiver queue when it receives all the chunks of a message.
4. The client consumes the aggregated message from the receiver queue. 

**Limitations:** 
- Chunking is only available for persisted topics.
- Chunking is only available for the exclusive and failover subscription types.
- Chunking cannot be enabled simultaneously with batching.

#### Handle consecutive chunked messages with one ordered consumer

The following figure shows a topic with one producer which publishes a large message payload in chunked messages along with regular non-chunked messages. The producer publishes message M1 in three chunks labeled M1-C1, M1-C2 and M1-C3. The broker stores all the three chunked messages in the managed-ledger and dispatches them to the ordered (exclusive/failover) consumer in the same order. The consumer buffers all the chunked messages in memory until it receives all the chunked messages, aggregates them into one message and then hands over the original message M1 to the client.

![](assets/chunking-01.png)

#### Handle interwoven chunked messages with one ordered consumer

When multiple producers publish chunked messages into a single topic, the broker stores all the chunked messages coming from different producers in the same managed-ledger. The chunked messages in the managed-ledger can be interwoven with each other. As shown below, Producer 1 publishes message M1 in three chunks M1-C1, M1-C2 and M1-C3. Producer 2 publishes message M2 in three chunks M2-C1, M2-C2 and M2-C3. All chunked messages of the specific message are still in order but might not be consecutive in the managed-ledger.

![](assets/chunking-02.png)

> **Note**  
> In this case, interwoven chunked messages may bring some memory pressure to the consumer because the consumer keeps a separate buffer for each large message to aggregate all its chunks in one message. You can limit the maximum number of chunked messages a consumer maintains concurrently by configuring the `maxPendingChunkedMessage` parameter. When the threshold is reached, the consumer drops pending messages by silently acknowledging them or asking the broker to redeliver them later, optimizing memory utilization.

#### Enable Message Chunking

**Prerequisite:** Disable batching by setting the `enableBatching` parameter to `false`.

The message chunking feature is OFF by default. 
To enable message chunking, set the `chunkingEnabled` parameter to `true` when creating a producer.

> **Note**  
> If the consumer fails to receive all chunks of a message within a specified time period, it expires incomplete chunks. The default value is 1 minute. For more information about the `expireTimeOfIncompleteChunkedMessage` parameter, refer to [org.apache.pulsar.client.api](https://pulsar.apache.org/api/client/).

## Consumers

A consumer is a process that attaches to a topic via a subscription and then receives messages.

A consumer sends a [flow permit request](developing-binary-protocol.md#flow-control) to a broker to get messages. There is a queue at the consumer side to receive messages pushed from the broker. You can configure the queue size with the [`receiverQueueSize`](client-libraries-java.md#configure-consumer) parameter. The default size is `1000`). Each time `consumer.receive()` is called, a message is dequeued from the buffer.  

### Receive modes

Messages are received from [brokers](reference-terminology.md#broker) either synchronously (sync) or asynchronously (async).

| Mode          | Description                                                                                                                                                                                                   |
|:--------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Sync receive  | A sync receive is blocked until a message is available.                                                                                                                                                  |
| Async receive | An async receive returns immediately with a future value—for example, a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture) in Java—that completes once a new message is available. |

### Listeners

Client libraries provide listener implementation for consumers. For example, the [Java client](client-libraries-java.md) provides a {@inject: javadoc:MesssageListener:/client/org/apache/pulsar/client/api/MessageListener} interface. In this interface, the `received` method is called whenever a new message is received.

### Acknowledgement

The consumer sends an acknowledgement request to the broker after it consumes a message successfully. Then, this consumed message will be permanently stored, and be deleted only after all the subscriptions have acknowledged it. If you want to store the messages that have been acknowledged by a consumer, you need to configure the [message retention policy](concepts-messaging.md#message-retention-and-expiry).

For batch messages, you can enable batch index acknowledgement to avoid dispatching acknowledged messages to the consumer. For details about batch index acknowledgement, see [batching](#batching).

Messages can be acknowledged in one of the following two ways:

- Being acknowledged individually. With individual acknowledgement, the consumer acknowledges each message and sends an acknowledgement request to the broker.
- Being acknowledged cumulatively. With cumulative acknowledgement, the consumer **only** acknowledges the last message it received. All messages in the stream up to (and including) the provided message are not redelivered to that consumer.

If you want to acknowledge messages individually, you can use the following API.

```java
consumer.acknowledge(msg);
```
If you want to acknowledge messages cumulatively, you can use the following API.
```java
consumer.acknowledgeCumulative(msg);
```


> **Note**  
> Cumulative acknowledgement cannot be used in [Shared subscription type](#subscription-types), because Shared subscription type involves multiple consumers which have access to the same subscription. In Shared subscription type, messages are acknowledged individually.

### Negative acknowledgement

The [negative acknowledgement](#negative-acknowledgement) mechanism allows you to send a notification to the broker indicating the consumer did not process a message.  When a consumer fails to consume a message and needs to re-consume it, the consumer sends a negative acknowledgement (nack) to the broker, triggering the broker to redeliver this message to the consumer.

Messages are negatively acknowledged individually or cumulatively, depending on the consumption subscription type.

In Exclusive and Failover subscription types, consumers only negatively acknowledge the last message they receive.

In Shared and Key_Shared subscription types, consumers can negatively acknowledge messages individually.

Be aware that negative acknowledgments on ordered subscription types, such as Exclusive, Failover and Key_Shared, might cause failed messages being sent to consumers out of the original order.

Use the following API to negatively acknowledge message consumption.

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub-negative-ack")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .negativeAckRedeliveryDelay(2, TimeUnit.SECONDS) // the default value is 1 min
                .subscribe();

Message<byte[]> message = consumer.receive();

// call the API to send negative acknowledgement
consumer.negativeAcknowledge(message);

message = consumer.receive();
consumer.acknowledge(message);
```

> **Note**  
> If batching is enabled, all messages in one batch are redelivered to the consumer.

### Negative redelivery backoff

It happens sometimes that consumers fail to process messages successfully. In this case, you can use [negative acknowledgement](#negative-acknowledgement) to redeliver the messages after consumption failures. For the Shared subscription type, the messages are redelivered to other consumers; for other subscription types, the messages are redelivered to the same consumer.

But this is not flexible enough. A better way is to use the **redelivery backoff mechanism**. You can redeliver messages with different delays by setting the number of times the messages are retried.

Use the following API to enable `Negative Redelivery Backoff`.

```java
consumer.negativeAckRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
        .minDelayMs(1000)
        .maxDelayMs(60 * 1000)
        .build())
```

### Acknowledgement timeout

The acknowledgement timeout mechanism allows you to set a time range during which the client tracks the unacknowledged messages. After this acknowledgement timeout (`ackTimeout`) period, the client sends `redeliver unacknowledged messages` request to the broker, thus the broker resends the unacknowledged messages to the consumer.

You can configure the acknowledgement timeout mechanism to redeliver the message if it is not acknowledged after `ackTimeout` or to execute a timer task to check the acknowledgement timeout messages during every `ackTimeoutTickTime` period.

You can also use the redelivery backoff mechanism, redeliver messages with different delays by setting the number 
of times the messages is retried.

If you want to use redelivery backoff, you can use the following API.
```java
consumer.ackTimeout(10, TimeUnit.SECOND)
        .ackTimeoutRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
        .minDelayMs(1000)
        .maxDelayMs(60000)
        .multiplier(2).build())
```

The message redelivery behavior should be as follows.

Redelivery count | Redelivery delay
:--------------------|:-----------
1 | 10 + 1 seconds
2 | 10 + 2 seconds
3 | 10 + 4 seconds
4 | 10 + 8 seconds
5 | 10 + 16 seconds
6 | 10 + 32 seconds
7 | 10 + 60 seconds
8 | 10 + 60 seconds

> **Note**  
> - If batching is enabled, all messages in one batch are redelivered to the consumer.  
> - Compared with acknowledgement timeout, negative acknowledgement is preferred. First, it is difficult to set a timeout value. Second, a broker resends messages when the message processing time exceeds the acknowledgement timeout, but these messages might not need to be re-consumed.

Use the following API to enable acknowledgement timeout.

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .ackTimeout(2, TimeUnit.SECONDS) // the default value is 0
                .ackTimeoutTickTime(1, TimeUnit.SECONDS)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

Message<byte[]> message = consumer.receive();

// wait at least 2 seconds
message = consumer.receive();
consumer.acknowledge(message);
```

### Dead letter topic

Dead letter topic allows you to continue message consumption even some messages are not consumed successfully. The messages that are failed to be consumed are stored in a specific topic, which is called dead letter topic. You can decide how to handle the messages in the dead letter topic.

Enable dead letter topic in a Java client using the default dead letter topic.

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                      .maxRedeliverCount(maxRedeliveryCount)
                      .build())
                .subscribe();
                
```
The default dead letter topic uses this format: 
```
<topicname>-<subscriptionname>-DLQ
```

Use the Java client to specify the name of the dead letter topic.

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                      .maxRedeliverCount(maxRedeliveryCount)
                      .deadLetterTopic("your-topic-name")
                      .build())
                .subscribe();
                
```

By default, there is no subscription during a DLQ topic creation. Without a just-in-time subscription to the DLQ topic, you may lose messages. To automatically create an initial subscription for the DLQ, you can specify the `initialSubscriptionName` parameter. If this parameter is set but the broker's `allowAutoSubscriptionCreation` is disabled, the DLQ producer will fail to be created.

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                      .maxRedeliverCount(maxRedeliveryCount)
                      .deadLetterTopic("your-topic-name")
                      .initialSubscriptionName("init-sub")
                      .build())
                .subscribe();
                
```

Dead letter topic depends on message redelivery. Messages are redelivered either due to [acknowledgement timeout](#acknowledgement-timeout) or [negative acknowledgement](#negative-acknowledgement). If you are going to use negative acknowledgement on a message, make sure it is negatively acknowledged before the acknowledgement timeout. 

> **Note**    
> Currently, dead letter topic is enabled in Shared and Key_Shared subscription types.

### Retry letter topic

For many online business systems, a message is re-consumed when exception occurs in the business logic processing. To configure the delay time for re-consuming the failed messages, you can configure the producer to send messages to both the business topic and the retry letter topic, and enable automatic retry on the consumer. With this setting, the messages that are not consumed will be stored in the retry letter topic. After the specified delay time, the consumer automatically consumes these messages from the retry letter topic.

By default, automatic retry is disabled. You can set `enableRetry` to `true` to enable automatic retry on the consumer.

Use the following API to consume messages from a retry letter topic.

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .receiverQueueSize(100)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(maxRedeliveryCount)
                        .retryLetterTopic("persistent://my-property/my-ns/my-subscription-custom-Retry")
                        .build())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
```

The messages in the retry letter topic contain some special properties that are automatically created by the client.

Special property | Description
:--------------------|:-----------
`REAL_TOPIC` | The real topic name.
`ORIGIN_MESSAGE_ID` | The origin message ID. It is crucial for message tracking.
`RECONSUMETIMES`   | The retry consume times.
`DELAY_TIME`      | Message delay timeMs.
**Example**
```
REAL_TOPIC = persistent://public/default/my-topic
ORIGIN_MESSAGE_ID = 1:0:-1:0
RECONSUMETIMES = 6
DELAY_TIME = 3000
```

Use the following API to store the messages in a retrial queue.

```java
consumer.reconsumeLater(msg, 3, TimeUnit.SECONDS);
```

Use the following API to add custom properties for the `reconsumeLater` function.

```java
Map<String, String> customProperties = new HashMap<String, String>();
customProperties.put("custom-key-1", "custom-value-1");
customProperties.put("custom-key-2", "custom-value-2");
consumer.reconsumeLater(msg, customProperties, 3, TimeUnit.SECONDS);
```

## Topics

As in other pub-sub systems, topics in Pulsar are named channels for transmitting messages from producers to consumers. Topic names are URLs that have a well-defined structure:

```http
{persistent|non-persistent}://tenant/namespace/topic
```

Topic name component | Description
:--------------------|:-----------
`persistent` / `non-persistent` | This identifies the type of topic. Pulsar supports two kind of topics: [persistent](concepts-architecture-overview.md#persistent-storage) and [non-persistent](#non-persistent-topics). The default is persistent, so if you do not specify a type, the topic is persistent. With persistent topics, all messages are durably persisted on disks (if the broker is not standalone, messages are durably persisted on multiple disks), whereas data for non-persistent topics is not persisted to storage disks.
`tenant`             | The topic tenant within the instance. Tenants are essential to multi-tenancy in Pulsar, and spread across clusters.
`namespace`          | The administrative unit of the topic, which acts as a grouping mechanism for related topics. Most topic configuration is performed at the [namespace](#namespaces) level. Each tenant has one or multiple namespaces.
`topic`              | The final part of the name. Topic names have no special meaning in a Pulsar instance.

> **No need to explicitly create new topics**  
> You do not need to explicitly create topics in Pulsar. If a client attempts to write or receive messages to/from a topic that does not yet exist, Pulsar creates that topic under the namespace provided in the [topic name](#topics) automatically.
> If no tenant or namespace is specified when a client creates a topic, the topic is created in the default tenant and namespace. You can also create a topic in a specified tenant and namespace, such as `persistent://my-tenant/my-namespace/my-topic`. `persistent://my-tenant/my-namespace/my-topic` means the `my-topic` topic is created in the `my-namespace` namespace of the `my-tenant` tenant.

## Namespaces

A namespace is a logical nomenclature within a tenant. A tenant creates multiple namespaces via the [admin API](admin-api-namespaces.md#create). For instance, a tenant with different applications can create a separate namespace for each application. A namespace allows the application to create and manage a hierarchy of topics. The topic `my-tenant/app1` is a namespace for the application `app1` for `my-tenant`. You can create any number of [topics](#topics) under the namespace.

## Subscriptions

A subscription is a named configuration rule that determines how messages are delivered to consumers. Four subscription types are available in Pulsar: [exclusive](#exclusive), [shared](#shared), [failover](#failover), and [key_shared](#key_shared). These types are illustrated in the figure below.

![Subscription types](assets/pulsar-subscription-types.png)

> **Pub-Sub or Queuing**  
> In Pulsar, you can use different subscriptions flexibly.
> * If you want to achieve traditional "fan-out pub-sub messaging" among consumers, specify a unique subscription name for each consumer. It is exclusive subscription type.
> * If you want to achieve "message queuing" among consumers, share the same subscription name among multiple consumers(shared, failover, key_shared).
> * If you want to achieve both effects simultaneously, combine exclusive subscription type with other subscription types for consumers.

### Subscription types

When a subscription has no consumers, its subscription type is undefined. The type of a subscription is defined when a consumer connects to it, and the type can be changed by restarting all consumers with a different configuration.

#### Exclusive

In *Exclusive* type, only a single consumer is allowed to attach to the subscription. If multiple consumers subscribe to a topic using the same subscription, an error occurs.

In the diagram below, only **Consumer A-0** is allowed to consume messages.

> Exclusive is the default subscription type.

![Exclusive subscriptions](assets/pulsar-exclusive-subscriptions.png)

#### Failover

In *Failover* type, multiple consumers can attach to the same subscription. A master consumer is picked for non-partitioned topic or each partition of partitioned topic and receives messages. When the master consumer disconnects, all (non-acknowledged and subsequent) messages are delivered to the next consumer in line.

For partitioned topics, broker will sort consumers by priority level and lexicographical order of consumer name. Then broker will try to evenly assigns topics to consumers with the highest priority level.

For non-partitioned topic, broker will pick consumer in the order they subscribe to the non partitioned topic.

In the diagram below, **Consumer-B-0** is the master consumer while **Consumer-B-1** would be the next consumer in line to receive messages if **Consumer-B-0** is disconnected.

![Failover subscriptions](assets/pulsar-failover-subscriptions.png)

#### Shared

In *shared* or *round robin* type, multiple consumers can attach to the same subscription. Messages are delivered in a round robin distribution across consumers, and any given message is delivered to only one consumer. When a consumer disconnects, all the messages that were sent to it and not acknowledged will be rescheduled for sending to the remaining consumers.

In the diagram below, **Consumer-C-1** and **Consumer-C-2** are able to subscribe to the topic, but **Consumer-C-3** and others could as well.

> **Limitations of Shared type**  
> When using Shared type, be aware that:
> * Message ordering is not guaranteed.
> * You cannot use cumulative acknowledgment with Shared type.

![Shared subscriptions](assets/pulsar-shared-subscriptions.png)

#### Key_Shared

In *Key_Shared* type, multiple consumers can attach to the same subscription. Messages are delivered in a distribution across consumers and message with same key or same ordering key are delivered to only one consumer. No matter how many times the message is re-delivered, it is delivered to the same consumer. When a consumer connected or disconnected will cause served consumer change for some key of message.

![Key_Shared subscriptions](assets/pulsar-key-shared-subscriptions.png)

Note that when the consumers are using the Key_Shared subscription type, you need to **disable batching** or **use key-based batching** for the producers. There are two reasons why the key-based batching is necessary for Key_Shared subscription type:
1. The broker dispatches messages according to the keys of the messages, but the default batching approach might fail to pack the messages with the same key to the same batch. 
2. Since it is the consumers instead of the broker who dispatch the messages from the batches, the key of the first message in one batch is considered as the key of all messages in this batch, thereby leading to context errors. 

The key-based batching aims at resolving the above-mentioned issues. This batching method ensures that the producers pack the messages with the same key to the same batch. The messages without a key are packed into one batch and this batch has no key. When the broker dispatches messages from this batch, it uses `NON_KEY` as the key. In addition, each consumer is associated with **only one** key and should receive **only one message batch** for the connected key. By default, you can limit batching by configuring the number of messages that producers are allowed to send. 

Below are examples of enabling the key-based batching under the Key_Shared subscription type, with `client` being the Pulsar client that you created.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```
Producer<byte[]> producer = client.newProducer()
        .topic("my-topic")
        .batcherBuilder(BatcherBuilder.KEY_BASED)
        .create();
```

<!--C++-->
```
ProducerConfiguration producerConfig;
producerConfig.setBatchingType(ProducerConfiguration::BatchingType::KeyBasedBatching);
Producer producer;
client.createProducer("my-topic", producerConfig, producer);
```

<!--Python-->
```
producer = client.create_producer(topic='my-topic', batching_type=pulsar.BatchingType.KeyBased)
```
<!--END_DOCUSAURUS_CODE_TABS-->

> **Limitations of Key_Shared type**  
> When you use Key_Shared type, be aware that:
> * You need to specify a key or orderingKey for messages.
> * You cannot use cumulative acknowledgment with Key_Shared type.

### Subscription modes

#### What is a subscription mode

The subscription mode indicates the cursor type. 

- When a subscription is created, an associated cursor is created to record the last consumed position. 

- When a consumer of the subscription restarts, it can continue consuming from the last message it consumes.

Subscription mode | Description | Note
|---|---|---
`Durable`|The cursor is durable, which retains messages and persists the current position. <br></br>If a broker restarts from a failure, it can recover the cursor from the persistent storage (BookKeeper), so that messages can continue to be consumed from the last consumed position.|`Durable` is the **default** subscription mode.
`NonDurable`|The cursor is non-durable. <br></br>Once a broker stops, the cursor is lost and can never be recovered, so that messages **can not** continue to be consumed from the last consumed position.|Reader’s subscription mode is `NonDurable` in nature and it does not prevent data in a topic from being deleted. Reader’s subscription mode **can not** be changed. 

A [subscription](#concepts-messaging.md/#subscriptions) can have one or more consumers. When a consumer subscribes to a topic, it must specify the subscription name. A durable subscription and a non-durable subscription can have the same name, they are independent of each other. If a consumer specifies a subscription which does not exist before, the subscription is automatically created.

#### When to use

By default, messages of a topic without any durable subscriptions are marked as deleted. If you want to prevent the messages being marked as deleted, you can create a durable subscription for this topic. In this case, only acknowledged messages are marked as deleted. For more information, see [message retention and expiry](cookbooks-retention-expiry.md).

#### How to use

After a consumer is created, the default subscription mode of the consumer is `Durable`. You can change the subscription mode to `NonDurable` by making changes to the consumer’s configuration.

<!--DOCUSAURUS_CODE_TABS-->

<!--Durable-->
 
```java
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-sub")
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
```
 
<!--Non-durable-->
 
```java
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-sub")
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscribe();
```
 
<!--END_DOCUSAURUS_CODE_TABS-->

For how to create, check, or delete a durable subscription, see [manage subscriptions](admin-api-topics.md/#manage-subscriptions).

## Multi-topic subscriptions

When a consumer subscribes to a Pulsar topic, by default it subscribes to one specific topic, such as `persistent://public/default/my-topic`. As of Pulsar version 1.23.0-incubating, however, Pulsar consumers can simultaneously subscribe to multiple topics. You can define a list of topics in two ways:

* On the basis of a [**reg**ular **ex**pression](https://en.wikipedia.org/wiki/Regular_expression) (regex), for example `persistent://public/default/finance-.*`
* By explicitly defining a list of topics

> When subscribing to multiple topics by regex, all topics must be in the same [namespace](#namespaces).

When subscribing to multiple topics, the Pulsar client automatically makes a call to the Pulsar API to discover the topics that match the regex pattern/list, and then subscribe to all of them. If any of the topics do not exist, the consumer auto-subscribes to them once the topics are created.

> **No ordering guarantees across multiple topics**  
> When a producer sends messages to a single topic, all messages are guaranteed to be read from that topic in the same order. However, these guarantees do not hold across multiple topics. So when a producer sends message to multiple topics, the order in which messages are read from those topics is not guaranteed to be the same.

The following are multi-topic subscription examples for Java.

```java
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient pulsarClient = // Instantiate Pulsar client object

// Subscribe to all topics in a namespace
Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default/.*");
Consumer<byte[]> allTopicsConsumer = pulsarClient.newConsumer()
                .topicsPattern(allTopicsInNamespace)
                .subscriptionName("subscription-1")
                .subscribe();

// Subscribe to a subsets of topics in a namespace, based on regex
Pattern someTopicsInNamespace = Pattern.compile("persistent://public/default/foo.*");
Consumer<byte[]> someTopicsConsumer = pulsarClient.newConsumer()
                .topicsPattern(someTopicsInNamespace)
                .subscriptionName("subscription-1")
                .subscribe();
```

For code examples, see [Java](client-libraries-java.md#multi-topic-subscriptions).

## Partitioned topics

Normal topics are served only by a single broker, which limits the maximum throughput of the topic. *Partitioned topics* are a special type of topic that are handled by multiple brokers, thus allowing for higher throughput.

A partitioned topic is actually implemented as N internal topics, where N is the number of partitions. When publishing messages to a partitioned topic, each message is routed to one of several brokers. The distribution of partitions across brokers is handled automatically by Pulsar.

The diagram below illustrates this:

![](assets/partitioning.png)

The **Topic1** topic has five partitions (**P0** through **P4**) split across three brokers. Because there are more partitions than brokers, two brokers handle two partitions a piece, while the third handles only one (again, Pulsar handles this distribution of partitions automatically).

Messages for this topic are broadcast to two consumers. The [routing mode](#routing-modes) determines each message should be published to which partition, while the [subscription type](#subscription-types) determines which messages go to which consumers.

Decisions about routing and subscription modes can be made separately in most cases. In general, throughput concerns should guide partitioning/routing decisions while subscription decisions should be guided by application semantics.

There is no difference between partitioned topics and normal topics in terms of how subscription types work, as partitioning only determines what happens between when a message is published by a producer and processed and acknowledged by a consumer.

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

By default, non-persistent topics are enabled on Pulsar brokers. You can disable them in the broker's [configuration](reference-configuration.md#broker-enableNonPersistentTopics). You can manage non-persistent topics using the `pulsar-admin topics` command. For more information, see [`pulsar-admin`](https://pulsar.apache.org/tools/pulsar-admin/).

### Performance

Non-persistent messaging is usually faster than persistent messaging because brokers don't persist messages and immediately send acks back to the producer as soon as that message is delivered to connected brokers. Producers thus see comparatively low publish latency with non-persistent topic.

### Client API

Producers and consumers can connect to non-persistent topics in the same way as persistent topics, with the crucial difference that the topic name must start with `non-persistent`. All three subscription types---[exclusive](#exclusive), [shared](#shared), and [failover](#failover)---are supported for non-persistent topics.

Here's an example [Java consumer](client-libraries-java.md#consumers) for a non-persistent topic:

```java
PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
String npTopic = "non-persistent://public/default/my-topic";
String subscriptionName = "my-subscription-name";

Consumer<byte[]> consumer = client.newConsumer()
        .topic(npTopic)
        .subscriptionName(subscriptionName)
        .subscribe();
```

Here's an example [Java producer](client-libraries-java.md#producer) for the same non-persistent topic:

```java
Producer<byte[]> producer = client.newProducer()
                .topic(npTopic)
                .create();
```

## Message redelivery

Apache Pulsar supports graceful failure handling and ensures critical data is not lost. Software will always have unexpected conditions and at times messages may not be delivered successfully. Therefore, it is important to have a built-in mechanism that handles failure, particularly in asynchronous messaging as highlighted in the following examples.

- Consumers get disconnected from the database or the HTTP server. When this happens, the database is temporarily offline while the consumer is writing the data to it and the external HTTP server that the consumer calls is momentarily unavailable.
- Consumers get disconnected from a broker due to consumer crashes, broken connections, etc. As a consequence, the unacknowledged messages are delivered to other available consumers.

Apache Pulsar avoids these and other message delivery failures using at-least-once delivery semantics that ensure Pulsar processes a message more than once. 

To utilize message redelivery, you need to enable this mechanism before the broker can resend the unacknowledged messages in Apache Pulsar client. You can activate the message redelivery mechanism in Apache Pulsar using three methods. 

- [Negative Acknowledgment](#negative-acknowledgement)
- [Acknowledgement Timeout](#acknowledgement-timeout)
- [Retry letter topic](#retry-letter-topic)


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

Message duplication occurs when a message is [persisted](concepts-architecture-overview.md#persistent-storage) by Pulsar more than once. Message deduplication is an optional Pulsar feature that prevents unnecessary message duplication by processing each message only once, even if the message is received more than once.

The following diagram illustrates what happens when message deduplication is disabled vs. enabled:

![Pulsar message deduplication](assets/message-deduplication.png)


Message deduplication is disabled in the scenario shown at the top. Here, a producer publishes message 1 on a topic; the message reaches a Pulsar broker and is [persisted](concepts-architecture-overview.md#persistent-storage) to BookKeeper. The producer then sends message 1 again (in this case due to some retry logic), and the message is received by the broker and stored in BookKeeper again, which means that duplication has occurred.

In the second scenario at the bottom, the producer publishes message 1, which is received by the broker and persisted, as in the first scenario. When the producer attempts to publish the message again, however, the broker knows that it has already seen message 1 and thus does not persist the message.

> Message deduplication is handled at the namespace level or the topic level. For more instructions, see the [message deduplication cookbook](cookbooks-deduplication.md).


### Producer idempotency

The other available approach to message deduplication is to ensure that each message is *only produced once*. This approach is typically called **producer idempotency**. The drawback of this approach is that it defers the work of message deduplication to the application. In Pulsar, this is handled at the [broker](reference-terminology.md#broker) level, so you do not need to modify your Pulsar client code. Instead, you only need to make administrative changes. For details, see [Managing message deduplication](cookbooks-deduplication.md).

### Deduplication and effectively-once semantics

Message deduplication makes Pulsar an ideal messaging system to be used in conjunction with stream processing engines (SPEs) and other systems seeking to provide effectively-once processing semantics. Messaging systems that do not offer automatic message deduplication require the SPE or other system to guarantee deduplication, which means that strict message ordering comes at the cost of burdening the application with the responsibility of deduplication. With Pulsar, strict ordering guarantees come at no application-level cost.

> You can find more in-depth information in [this post](https://www.splunk.com/en_us/blog/it/exactly-once-is-not-exactly-the-same.html).

## Delayed message delivery
Delayed message delivery enables you to consume a message later. In this mechanism, a message is stored in BookKeeper. The `DelayedDeliveryTracker` maintains the time index (time -> messageId) in memory after the message is published to a broker. This message will be delivered to a consumer once the specified delay is over.  

Delayed message delivery only works in Shared subscription type. In Exclusive and Failover subscription types, the delayed message is dispatched immediately.

The diagram below illustrates the concept of delayed message delivery:

![Delayed Message Delivery](assets/message_delay.png)

A broker saves a message without any check. When a consumer consumes a message, if the message is set to delay, then the message is added to `DelayedDeliveryTracker`. A subscription checks and gets timeout messages from `DelayedDeliveryTracker`.

### Broker 
Delayed message delivery is enabled by default. You can change it in the broker configuration file as below:

```
# Whether to enable the delayed delivery for messages.
# If disabled, messages are immediately delivered and there is no tracking overhead.
delayedDeliveryEnabled=true

# Control the ticking time for the retry of delayed message delivery,
# affecting the accuracy of the delivery time compared to the scheduled time.
# Note that this time is used to configure the HashedWheelTimer's tick time for the
# InMemoryDelayedDeliveryTrackerFactory (the default DelayedDeliverTrackerFactory).
# Default is 1 second.
delayedDeliveryTickTimeMillis=1000
```

### Producer 
The following is an example of delayed message delivery for a producer in Java:
```java
// message to be delivered at the configured delay interval
producer.newMessage().deliverAfter(3L, TimeUnit.Minute).value("Hello Pulsar!").send();
```
