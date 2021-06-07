---
author: Penghui Li
authorURL: https://twitter.com/lipenghui6
title: Apache Pulsar 2.6.0
---
We are very glad to see the Apache Pulsar community has successfully released the wonderful 2.6.0 version after accumulated hard work. It is a great milestone for this fast-growing project and the whole Pulsar community. This is the result of a huge effort from the community, with over 450 commits and a long list of new features, improvements, and bug fixes.

Here is a selection of some of the most interesting and major features added to Pulsar 2.6.0.

<!--truncate-->

## Core Pulsar

### [PIP-37] Large message size support

This PIP adds support for producing and consuming large size messages by splitting the large message into multiple chunks. This is a very powerful feature for sending and consuming very large messages.

Currently, this feature only works for the non-shared subscription and it has client-side changes. You need to upgrade the Pulsar client version to 2.6.0. You can enable the message trunk at the producer side as below.

```java
client.newProducer()
	.topic("my-topic")
	.enableChunking(true)
	.create();
```

For more information about PIP-37, see [here](https://github.com/apache/pulsar/wiki/PIP-37:-Large-message-size-handling-in-Pulsar). <br> For more information about implementation details, see [PR-4440](https://github.com/apache/pulsar/pull/4400).

### [PIP-39] Namespace change events (system topic)

This PIP introduces the system topic to store namespace change events. Previously, Pulsar only allowed you to set the namespace policy, all topics under the namespace followed the namespace policy. Many users want to set the policy for topics. The main reason for not using the same way as namespace level policy is to avoid introducing more workload on the ZooKeeper. 

The original intention of the system topic is to be able to store topic policy in a topic rather than ZooKeeper. So this is the first step to achieve topic level policy. And we can easily add support for the topic level policy with this feature.

For more information about PIP-39, see [here](https://github.com/apache/pulsar/wiki/PIP-39%3A-Namespace-Change-Events).<br> For more information about implementation details, see  [PR-4955](https://github.com/apache/pulsar/pull/4955).

### [PIP-45] Pluggable metadata interface

We have been advancing to enable Pulsar to use other metastore services rather than ZooKeeper. This PIP converts `ManagedLedger` to use the `MetadataStore` interface. This facilitates the metadata server plug-in process. Through the `MetadataStore` interface, it is easy to add other metadata servers into Pulsar such as [etcd](https://github.com/etcd-io/etcd).

For more information about PIP-45, see [here](https://github.com/apache/pulsar/wiki/PIP-45%3A-Pluggable-metadata-interface). <br> For more information about implementation details, see [PR-5358](https://github.com/apache/pulsar/pull/5358).

### [PIP-54] Support acknowledgment at the batch index level

Previously, the broker only tracked the acknowledged state in the batch message level. If a subset of the batch messages was acknowledged, the consumer could still get the acknowledged message of that batch message while the batch message redelivery happened. 

This PIP adds support for acknowledging the local batch index of a batch. This feature is not enabled by default. You can enable it in the `broker.conf` as below.

```
acknowledgmentAtBatchIndexLevelEnabled=true
```

For more information about PIP-54, see [here](https://github.com/apache/pulsar/wiki/PIP-54:-Support-acknowledgment-at-batch-index-level). <br> For more information about implementation details, see [PR-6052](https://github.com/apache/pulsar/pull/6052).

### [PIP-58] Support consumers setting custom message retry delay

For many online business systems, various exceptions usually occur in business logic processing, so the message needs to be re-consumed, but users hope that this delay time can be controlled flexibly. Previously, processing methods were usually to send messages to special retry topics, because production can specify any delay, so consumers subscribe to the business topic and retry topic at the same time. Now you can set a retry delay for each message as below.

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
    .enableRetry(true)
    .receiverQueueSize(100)
    .deadLetterPolicy(DeadLetterPolicy.builder()
        .maxRedeliverCount(maxRedeliveryCount)
        .retryLetterTopic("persistent://my-property/my-ns/my-subscription-custom-Retry")
        .build())
    .subscribe();

consumer.reconsumeLater(message, 10 , TimeUnit.SECONDS);
```

For more information about PIP-58, see [here](https://github.com/apache/pulsar/wiki/PIP-58-%3A-Support-Consumers--Set-Custom-Retry-Delay). <br> For more information about implementation details, see [PR-6449](https://github.com/apache/pulsar/pull/6449).

### [PIP-60] Support SNI routing to support various proxy servers

Previously, Pulsar did not provide support to use other proxies, such as Apache Traffic Server (ATS), HAProxy, Nginx, and Envoy, which are more scalable and secured. Most of these proxy servers support SNI routing which can route traffic to a destination without having to terminate the SSL connection. This PIP adds SNI routing and makes changes to the Pulsar client.

For more information about PIP-60, see [here](https://github.com/apache/pulsar/wiki/PIP-60:-Support-Proxy-server-with-SNI-routing). <br> For more information about implementation details, see  [PR-6566](https://github.com/apache/pulsar/pull/6566).

### [PIP-61] Advertise multiple addresses

This PIP allows the broker to expose multiple advertised listeners and to support the separation of internal and external network traffic. You can specify multiple advertised listeners in `broker.conf` as below.

```
advertisedListeners=internal:pulsar://192.168.1.11:6660,external:pulsar://110.95.234.50:6650
```

From the client side, you can specify the listener name for the client as below.

```java
PulsarClient.builder()
    .serviceUrl(url)
    .listenerName("internal")
    .build();
```

For more information about PIP-61, see [here](https://github.com/apache/pulsar/wiki/PIP-61%3A-Advertised-multiple-addresses). <br> For more information about implementation details, see [PR-6903](https://github.com/apache/pulsar/pull/6903).

### [PIP-65] Adapt Pulsar IO sources to support `BatchSources`  

This PIP introduces `BatchSource` as a new interface for writing batch-based connectors. It also introduces `BatchSourceTriggerer` as an interface to trigger the data collection of a `BatchSource`. It then provides system implementation in `BatchSourceExecutor`.

For more information about PIP-65, see [here](https://github.com/apache/pulsar/wiki/PIP-65%3A-Adapting-Pulsar-IO-Sources-to-support-Batch-Sources). <br> For more information about implementation details, see [PR-7090](https://github.com/apache/pulsar/pull/7090).

### [Load balancer] Add `ThresholdShedder` strategy for the load balancer

The `ThresholdShedder` strategy is more flexible than `LoadSheddingStrategy` for Pulsar. The `ThresholdShedder` calculates the average resource usage of the brokers, and individual broker resource usage compares with the average value. If it is greater than the average value plus threshold, the overload shedder is triggered. You can enable it in `broker.conf` as below.

```
loadBalancerLoadSheddingStrategy=org.apache.pulsar.broker.loadbalance.impl.ThresholdShedder
```

You can customize more parameters for the `ThresholdShedder` if needed as below.

```
# The broker resource usage threshold.
# When the broker resource usage is greater than the pulsar cluster average resource usage,
# the threshold shedder will be triggered to offload bundles from the broker.
# It only takes effect in ThresholdShedder strategy.
loadBalancerBrokerThresholdShedderPercentage=10

# When calculating new resource usage, the history usage accounts for.
# It only takes effect in ThresholdShedder strategy.
loadBalancerHistoryResourcePercentage=0.9

# The BandWithIn usage weight when calculating new resource usage.
# It only takes effect in ThresholdShedder strategy.
loadBalancerBandwithInResourceWeight=1.0

# The BandWithOut usage weight when calculating new resource usage.
# It only takes effect in ThresholdShedder strategy.
loadBalancerBandwithOutResourceWeight=1.0

# The CPU usage weight when calculating new resource usage.
# It only takes effect in ThresholdShedder strategy.
loadBalancerCPUResourceWeight=1.0

# The heap memory usage weight when calculating new resource usage.
# It only takes effect in ThresholdShedder strategy.
loadBalancerMemoryResourceWeight=1.0

# The direct memory usage weight when calculating new resource usage.
# It only takes effect in ThresholdShedder strategy.
loadBalancerDirectMemoryResourceWeight=1.0

# Bundle unload minimum throughput threshold (MB), avoiding bundle unload frequently.
# It only takes effect in ThresholdShedder strategy.
loadBalancerBundleUnloadMinThroughputThreshold=10
```

For more information about implementation details, see [PR-6772](https://github.com/apache/pulsar/pull/6772).

### [Key Shared] Add consistent hashing in the Key_Shared distribution

Previously, the implementation of the Key_Shared subscription used a mechanism to divide their hash space across the available consumers. This was based on dividing the currently assigned hash ranges when a new consumer joined or left. Pulsar 2.6.0 introduces a new consistent hash distribution for the Key_Shared subscription. You can enable the consistent hash distribution in `broker.conf` and the auto split approach is still selected by default.

```
# On KeyShared subscriptions, with default AUTO_SPLIT mode, use splitting ranges or
# consistent hashing to reassign keys to new consumers
subscriptionKeySharedUseConsistentHashing=false

# On KeyShared subscriptions, number of points in the consistent-hashing ring.
# The higher the number, the more equal the assignment of keys to consumers
subscriptionKeySharedConsistentHashingReplicaPoints=100
```

We plan to use consistent hash distribution by default in the subsequent versions.
For more information about implementation details, see  [PR-6791](https://github.com/apache/pulsar/pull/6791).

### [Key Shared] Fix ordering issue in KeyShared dispatcher when adding consumers

This is a great fix for the Key_Shared subscription. Previously, ordering was broken in a KeyShared dispatcher if a new consumer c2 came in and an existing consumer c1 went out. This was because messages with keys previously assigned to c1 may route to c2, which might break the message ordering dispatch guarantee in the Key_Shared subscription. This PR introduces new consumers joining in a "paused" state until the previous messages are acknowledged to ensure the messages are dispatched orderly. 

If you still want the relaxed ordering, you can set up at the consumer side as below.

```java
pulsarClient.newConsumer()
	.keySharedPolicy(KeySharedPolicy.autoSplitHashRange().setAllowOutOfOrderDelivery(true))
	.subscribe();
```

For more information about implementation details, see [PR-7106](https://github.com/apache/pulsar/pull/7106) and [PR-7108](https://github.com/apache/pulsar/pull/7108).

### [Key Shared] Add support for key hash range reading

This PR supports sticky key hash range reader. A broker only dispatches messages whose hash of the message key contains by a specified key hash range. Besides, multiple key hash ranges can be specified on a reader.

```java
pulsarClient.newReader()
    .topic(topic)
    .startMessageId(MessageId.earliest)
    .keyHashRange(Range.of(0, 10000), Range.of(20001, 30000))
    .create();
```

For more information about implementation details, see  [PR-5928](https://github.com/apache/pulsar/pull/5928).

### Use pure-java Air-Compressor instead of JNI based libraries

Previously, JNI based libraries were used to perform data compression. While these libraries do have an overhead in terms of size and affect the JNI overhead which is typically measurable when compressing many small payloads. This PR replaces compression libraries for LZ4, ZStd, and Snappy with [AirCompressor](https://github.com/airlift/aircompressor), which is a pure Java compression library used by Presto.

For more information about implementation details, see  [PR-5390](https://github.com/apache/pulsar/pull/5390).

### Support multiple Pulsar clusters using the same BookKeeper cluster

This PR allows multiple pulsar clusters to use the specified BookKeeper cluster by pointing BookKeeper client to the ZooKeeper connection string of BookKeeper cluster. This PR adds a configuration (`bookkeeperMetadataServiceUri`) to discover BookKeeper cluster metadata store and uses metadata service URI to initialize BookKeeper clients.

```
# Metadata service uri that bookkeeper is used for loading corresponding metadata driver
# and resolving its metadata service location.
# This value can be fetched using `bookkeeper shell whatisinstanceid` command in BookKeeper cluster.
# For example: zk+hierarchical://localhost:2181/ledgers
# The metadata service uri list can also be semicolon separated values like below:
# zk+hierarchical://zk1:2181;zk2:2181;zk3:2181/ledgers
bookkeeperMetadataServiceUri=
```

For more information about implementation details, see [PR-5985](https://github.com/apache/pulsar/pull/5985).

### Support deleting inactive topics when subscriptions are caught up

Previously, Pulsar supported deleting inactive topics which do not have active producers and subscriptions. This PR supports deleting inactive topics when all subscriptions of the topic are caught up and when there are no active producers or consumers. This PR exposes inactive topic delete mode in `broker.conf`. In the future, we can support a namespace level configuration for the inactive topic delete mode.

```
# Set the inactive topic delete mode. Default is delete_when_no_subscriptions
# 'delete_when_no_subscriptions' mode only delete the topic which has no subscriptions and no active producers
# 'delete_when_subscriptions_caught_up' mode only delete the topic that all subscriptions has no backlogs(caught up)
# and no active producers/consumers
brokerDeleteInactiveTopicsMode=delete_when_no_subscriptions
```

For more information about implementation details, see [PR-6077](https://github.com/apache/pulsar/pull/6077).

### Add a flag to skip broker shutdown on transient OOM

A high dispatch rate on one of the topics may cause a broker to go OOM temporarily. It is a transient error and the broker can recover within a few seconds as soon as some memory gets released. However, in 2.4 release ([#4196](https://github.com/apache/pulsar/pull/4196)), the “restarted broker on OOM” feature can cause huge instability in a cluster, where a topic moves from one broker to another and restarts multiple brokers and disrupts other topics as well. So this PR provides a dynamic flag to skip broker shutdown on OOM to avoid instability in a cluster.

For more information about implementation details, see [PR-6634](https://github.com/apache/pulsar/pull/6634).

### Make ZooKeeper cache expiry time configurable

Previously, ZooKeeper cache expiry time was hardcoded and it needed to be configurable to refresh the value based on various requirements, for example, refreshing the value quickly in case of zk-watch miss, avoiding frequent cache refresh to avoid zk-read or avoiding issue due to zk read timeout, and so on. Now you can configure ZooKeeper cache expiry time in `broker.conf` as below.

```
# ZooKeeper cache expiry time in seconds
zooKeeperCacheExpirySeconds=300
```

For more information about implementation details, see [PR-6668](https://github.com/apache/pulsar/pull/6668).

### Optimize consumer fetch messages in case of batch message

When a consumer sends a fetch request to a broker server, it contains a fetch message number telling the server how many messages should be pushed to a consumer client. However, the broker server stores data in BookKeeper or broker cache according to entry rather than a single message if the producer produces messages using the batch feature. There is a gap to map the number of messages to the number of entries when dealing with consumer fetch requests. This PR adds a variable `avgMessagesPerEntry` to record average messages stored in one entry. It updates when a broker server pushes messages to a consumer. When dealing with consumer fetch requests, it maps fetch request number to entry number. Additionally, this PR exposes the `avgMessagePerEntry` static value to consumer stat metric json.

You can enable `preciseDispatcherFlowControl` in ` broker.conf` as below.

```
# Precise dispatcher flow control according to history message number of each entry
preciseDispatcherFlowControl=false
```

For more information about implementation details, see  [PR-6719](https://github.com/apache/pulsar/pull/6719)

### Introduce precise topic publish rate limiting

Previously, Pulsar supported the publish rate limiting but it is not a precise control. Now, for some use cases that need precise control, you can enable it in `broker.conf` as below.

```
preciseTopicPublishRateLimiterEnable=true
```

For more information about implementation details, see  [PR-7078](https://github.com/apache/pulsar/pull/7078).

### Expose check delay of new entries in `broker.conf`

Previously, the check delay of new entries was 10 ms and could not be changed by users. Currently, for consumption latency sensitive scenarios, you can set the value of check delay of new entries to a smaller value or 0 in `broker.conf` as below. Using a smaller value may degrade consumption throughput. 

```
managedLedgerNewEntriesCheckDelayInMillis=10
```

For more information about implementation details, see  [PR-7154](https://github.com/apache/pulsar/pull/7154).

### [Schema]  Supports `null` key and `null` value in KeyValue schema

For more information about implementation details, see  [PR-7139](https://github.com/apache/pulsar/pull/7139).

### Support triggering ledger rollover when `maxLedgerRolloverTimeMinutes` is met

This PR implements a monitoring thread to check if the current topic ledger meets the constraint of `managedLedgerMaxLedgerRolloverTimeMinutes` and triggers a rollover to make the configuration take effect. Another important idea is that if you trigger a rollover, you can close the current ledger so that you can release the storage of the current ledger. For some less commonly used topics, the current ledger data is likely to be expired and the current rollover logic is only triggered when adding a new entry. Obviously, this results in a waste of disk space. The monitoring thread is scheduled at a fixed time interval and the interval is set to `managedLedgerMaxLedgerRolloverTimeMinutes`. Each inspection makes two judgments at the same time, for example, `currentLedgerEntries > 0` and `currentLedgerIsFull()`. When the number of current entries is equal to 0, it does not trigger a new rollover and you can use this to reduce the ledger creation.

For more information about implementation details, see  [PR-7116](https://github.com/apache/pulsar/pull/7111).

## Proxy

### Add REST API to get connection and topic stats

Previously, Pulsar proxy did not have useful stats to get internal information about the proxy. It is better to have internal-stats of proxy to get information, such as live connections, topic stats (with higher logging level), and so on. This PR adds REST API to get stats for connection and topics served by proxy.

For more information about implementation details, see [PR-6473](https://github.com/apache/pulsar/pull/6473).

## Admin

### Support getting a message by message ID in pulsar-admin

This PR adds a new command `get-message-by-id` to the pulsar-admin. It allows users to check a single message by providing ledger ID and entry ID. 

For more information about implementation details, see [PR-6331](https://github.com/apache/pulsar/pull/6331).

### Support deleting subscriptions forcefully

This PR adds the method `deleteForcefully` to support force deleting subscriptions. 

For more information about implementation details, see [PR-6383](https://github.com/apache/pulsar/pull/6383).

## Functions

### Built-in functions

This PR implements the possibility of creating built-in functions in the same way as adding built-in connectors.

For more information about implementation details, see [PR-6895](https://github.com/apache/pulsar/pull/6895).

### Add Go Function heartbeat (and gRPC service) for production usage
 
For more information about implementation details, see [PR-6031](https://github.com/apache/pulsar/pull/6031).

### Add custom property options to functions

This PR allows users to set custom system properties while submitting functions. This can be used to pass credentials via a system property.

For more information about implementation details, see [PR-6348](https://github.com/apache/pulsar/pull/6348).

### Separate TLS configurations of function worker and broker
 
For more information about implementation details, see [PR-6602](https://github.com/apache/pulsar/pull/6602).

### Add the ability to build consumers in functions and sources

Previously, function and source context give their writers an ability to create publishers but not consumers. This PR fixes this issue.

For more information about implementation details, see [PR-6954](https://github.com/apache/pulsar/pull/6954).

## Pulsar SQL

### Support KeyValue schema

Previously, Pulsar SQL could not read the KeyValue schema data.

This PR adds KeyValue schema support for Pulsar SQL. It adds the prefix `key.` for the key field name and `value.` for the value field name.

For more information about implementation details, see [PR-6325](https://github.com/apache/pulsar/pull/6325).

### Support multiple Avro schema versions

Previously, if you have multiple Avro schema versions for a topic, using the Pulsar SQL to query data from this topic will introduce some problems. With this change, You can evolve the schema of the topic and keep transitive backward compatibility of all schemas of the topic if you want to query data from this topic. 
  
For more information about implementation details, see [PR-4847](https://github.com/apache/pulsar/pull/4847).

## Java client

### Support waiting for inflight messages while closing a producer

Previously, when you closed a producer, the pulsar-client immediately failed inflight messages even if it persisted successfully at the broker. Most of the time, users want to wait for those inflight messages rather than fail them. While the pulsar-client library did not provide a way to wait for inflight messages before closing the producer. This PR supports closing API with a flag where you can control waiting for inflight messages. With this change, you can close a producer by waiting for inflight messages and the pulsar-client does not fail those messages immediately.
Previously, when you closed a producer, the pulsar-client immediately failed inflight messages even if it persisted successfully at the broker. Most of the time, users want to wait for those inflight messages rather than fail them. While the pulsar-client library did not provide a way to wait for inflight messages before closing the producer. This PR supports closing API with a flag where you can control waiting for inflight messages. With this change, you can close a producer by waiting for inflight messages and the pulsar-client does not fail those messages immediately.

For more information about implementation details, see [PR-6648](https://github.com/apache/pulsar/pull/6648).

### Support loading TLS certs/key dynamically from input stream

Previously, the pulsar-client provided TLS authentication support and the default TLS provider `AuthenticationTls` expected file path of cert and key files. However, there were use cases where it was difficult for user applications to store certs/key files locally for TLS authentication. This PR adds stream support in `AuthenticationTls` to provide X509Certs and PrivateKey which also perform auto-refresh when streaming changes in a given provider.

For more information about implementation details, see [PR-6760](https://github.com/apache/pulsar/pull/6760).

### Support returning sequence ID when throwing an exception for async send messages

Previously, when sending messages asynchronously failed, an exception was thrown, but did not know which message was abnormal, and users did not know which messages needed to be retried. This PR makes changes supported on the client side. When throwing an exception, the sequenceId `org.apache.pulsar.client.api.PulsarClientException` is set.

For more information about implementation details, see [PR-6825](https://github.com/apache/pulsar/pull/6825).


## More information

- To download Apache Pulsar 2.6.0, click [here](https://pulsar.apache.org/en/download/).
- For more information about Apache Pulsar 2.6.0, see [2.6.0 release notes](https://pulsar.apache.org/release-notes/#2.6.0) and [2.6.0 PR list](https://github.com/apache/pulsar/pulls?q=milestone%3A2.6.0+-label%3Arelease%2F2.5.2+-label%3Arelease%2F2.5.1+).

If you have any questions or suggestions, contact us with mailing lists or slack.

- [users@pulsar.apache.org](mailto:users@pulsar.apache.org)
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)
- Pulsar slack channel: https://apache-pulsar.slack.com/
- Self-registration at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Pulsar](https://github.com/apache/pulsar).

