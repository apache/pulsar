---
author: Xiaolong Ran
authorURL: https://twitter.com/wolf4j1
title: Apache Pulsar 2.6.2
---
We are excited to see that the Apache Pulsar community has successfully released the 2.6.2 version after a lot of hard work. It is a great milestone for this fast-growing project and the Pulsar community. 2.6.2 is the result of a big effort from the community, with over 154 commits and a long list of improvements and bug fixes.

Here are some highlights and major features added in Pulsar 2.6.2.

<!--truncate-->

## Broker

### Catch `throwable` when starting Pulsar

Before 2.6.2, Pulsar catched exceptions only when `BrokerStarter.start()` failed. Some errors such as `NoSuchMethodError` or `NoClassDefFoundError` could not be caught, and Pulsar was in abnormal status yet no error log was found in the log file.

In 2.6.2, we modify exceptions to use `throwable` to avoid this issue.

For more information about implementation, see [PR-7221](https://github.com/apache/pulsar/pull/7221).

### Handle SubscriptionBusyException in resetCursor API

In `PersistentSubscription.resetCursor` method, `SubscriptionFencedException` is thrown in several places, but it is not handled in `PersistentTopicBase`, so error messages are not clear.

In 2.6.2, we export `SubscriptionBusyException` in `PersistentTopicBase` for `resetCursor`, so error messages in the REST API are clear. 

For more information about implementation, see [PR-7335](https://github.com/apache/pulsar/pull/7335).

### Update Jersey to 2.31

Before 2.6.1, Pulsar used the Jersey 2.27, which has security concerns. In Pulsar 2.6.2, we update the Jersey version to the latest stable version(2.31) to enhance security.

For more information about implementation, see [PR-7515](https://github.com/apache/pulsar/pull/7515).

### Stop to dispatch when consumers using the Key_Shared subscription stuck

Consumers using the `Key_Shared` subscription would encounter disorder messages occasionally. The following are steps to reproduce the situation:

1. Connect Consumer1 to Key_Shared subscription `sub` and stop to receive
  - receiverQueueSize: 500
2. Connect Producer and publish 500 messages with key `(i % 10)`
3. Connect Consumer2 to same subscription and start to receive
  - receiverQueueSize: 1
  - since https://github.com/apache/pulsar/pull/7106 , Consumer2 can't receive (expected)
4. Producer publish more 500 messages with same key generation algorithm
5. After that, Consumer1 start to receive
6. Check Consumer2 message ordering
  - sometimes message ordering was broken in same key

In 2.6.2, when consumers use the Key_Shared subscription, Pulsar stops dispatching messages to consumers that are stuck on delivery to guarantee message order. 

For more information about implementation, see [PR-7553](https://github.com/apache/pulsar/pull/7553).

### Reestablish namespace bundle ownership from false negative releasing and false positive acquiring

In acquiring/releasing namespace bundle ownership, ZooKeeper might be disconnected before or after these operations are persisted in the ZooKeeper cluster. It leads to inconsistency between the local ownership cache and ZooKeeper cluster.

In 2.6.2, we fix the issue with the following:

* In ownership releasing, do not retain ownership in failure.
* In ownership checking, querying and acquiring, reestablish the lost ownership in false negative releasing and false positive acquiring.

For more information about implementation, see [PR-7773](https://github.com/apache/pulsar/pull/7773).

### Enable users to configure the executor pool size

Before 2.6.2, the executor pool size in Pulsar was set to `20` when starting Pulsar services. Users could not configure the executor pool size.

```
private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
           new DefaultThreadFactory("pulsar"));
```

In 2.6.2, users can configure the executor pool size in the `broker.conf` file based on their needs.

For more information about implementation, see [PR-7782](https://github.com/apache/pulsar/pull/7782).

### Add replicated check for `checkInactiveSubscriptions`

After the replicated subscription is deleted by `checkInactiveSubscriptions`, replicated subscriptions are created with `receiveSubscriptionUpdated`. In this case, the position becomes the latest position.

```
topic.createSubscription(update.getSubscriptionName(),
        InitialPosition.Latest, true /* replicateSubscriptionState */);
```

In 2.6.2, the replicated subscription is excluded from automatic deletion by fixing the `PersistentTopic`.

For more information about implementation, see [PR-8066](https://github.com/apache/pulsar/pull/8066).

### Upgrade jetty-util version to 9.4.31

Pulsar client depends on jetty-util. Jetty-util versions earlier than 9.4.30 contain known vulnerabilities.

In 2.6.2, we upgrade the jetty-util version to `9.4.31` to enhance security.

For more information about implementation, see [PR-8035](https://github.com/apache/pulsar/pull/8035).

### Add command to delete a cluster's metadata from ZooKeeper

When we share the same ZooKeeper and BookKeeper cluster among multiple broker clusters, if a cluster was removed, its metadata in ZooKeeper were also removed.

In 2.6.2, we fix the issue in the following ways:

- Add a `PulsarClusterMetadataTeardown` class to delete the relative nodes from ZooKeeper;
- Wrap the class to `bin/pulsar` script.

For more information about implementation, see [PR-8169](https://github.com/apache/pulsar/pull/8169).

### Replace EventLoop with ThreadPoolExecutor to improve performance instead of EventLoop

In 2.6.2, we replace EventLoop with a native JDK thread pool(ThreadPoolExecutor) to improve performance.

The following is the test result with pulsar perf.

Before 2.6.1:

```
Aggregated throughput stats --- 11715556 records received --- 68813.420 msg/s --- 537.605 Mbit/s
```

In 2.6.2：

```
Aggregated throughput stats --- 18392800 records received --- 133314.602 msg/s --- 1041.520 Mbit/s
```

For more information about implementation, see [PR-8208](https://github.com/apache/pulsar/pull/8208).

### Fix deadlock that occurred during topic ownership check

Some broker servers had deadlocks while splitting namespace bundles. When checking the thread dump of the broker, some threads were blocked in `NamespaceService#getBundle()`.

```
"pulsar-ordered-OrderedExecutor-7-0" #34 prio=5 os_prio=0 tid=0x00007eeeab05a800 nid=0x81a5 waiting on condition [0x00007eeeafbd2000]
  java.lang.Thread.State: WAITING (parking)
       at sun.misc.Unsafe.park(Native Method)
       - parking to wait for  <0x00007f17fa965418> (a java.util.concurrent.CompletableFuture$Signaller)
       at java.util.concurrent.locks.LockSupport.park(LockSupport.java:175)
       at org.apache.pulsar.common.naming.NamespaceBundleFactory.getBundles(NamespaceBundleFactory.java:155)
...
```

The reason for the issue is that the `getBundle()` method leads to deadlock in `NamespaceService#isTopicOwned()`. To fix the issue, we remove the `getBundle()` method. When `isTopicOwned()` returns `false`, the bundle metadata is cached and can be got asynchronously. When the client reconnects the next time, Pulsar returns the correct bundle metadata from the cache.

For more information about implementation, see [PR-8406](https://github.com/apache/pulsar/pull/8406).

## Proxy

### Enable users to configure `advertisedAddress` in proxy 

Before 2.6.2, users could not configure `advertisedAddress` on the proxy side. In 2.6.2, users can configure `advertisedAddress` in proxy just as they do in Pulsar broker.

For more information about implementation, see [PR-7542](https://github.com/apache/pulsar/pull/7542).

### Add proxy plugin interface to support user defined additional servlet

To enable users to access the broker flexibly, Pulsar provides plugins similar to broker protocol and broker interceptor. However, users could not access the proxy before 2.6.2.

To enable users to customize data requests in proxy, we add the protocol plugin for proxy in 2.6.2.

For more information about implementation, see [PR-8067](https://github.com/apache/pulsar/pull/8067).

### Fix the null exception when starting the proxy service

When enabling the broker TLS and broker client authentication with OAuth2 plugin,
the proxy service exits with an unexpected null exception.

The reason is that when initializing the flow, authentication is called, so the token client is not initialized before using.

In 2.6.2, we fix the null exception when starting the proxy service.

For more information about implementation, see [PR-8019](https://github.com/apache/pulsar/pull/8019).

## Java Client

### Support input-stream for trustStore cert

In 2.6.1, Pulsar supports dynamic cert loading by using input stream for TLS cert and key file. The feature is mainly used by container. However, container also requires dynamic loading for truststore certs and users cannot store trust-store cert into file-system. 

In 2.6.2, Pulsar supports loading truststore cert dynamically using input stream.

For more information about implementation, see [PR-7442](https://github.com/apache/pulsar/pull/7442).

### Avoid subscribing the same topic

The current key of `MultiTopicsConsumerImpl.topics` is the topic name passed by the user. The `topicNameValid` method checks if the name is valid and `topics` doesn't contain the key.

However, if a multi-topic consumer subscribes a partition of a subscribed partitioned topic,  `subscribeAsync` succeeds and a new `ConsumerImpl` of the same partition is created, which is redundant.

Also, if a multi-topic consumer subscribes `public/default/topic` or `persistent://public/default/topic`, while the initial subscribed topic is `topic`, the redundant consumers would be created.

In 2.6.2, we fix the issue in the following ways to avoid subscribing the same topic again:

- Use the full topic name as key for `MultiTopicsConsumerImpl.topics`.
- Check that both the full topic name and the full partitioned topic name do not exist in `MultiTopicsConsumerImpl.topics` when `subscribeAsync` is called.
- Throw a different exception to a different topic is invalid and the topic is already subscribed

For more information about implementation, see [PR-7823](https://github.com/apache/pulsar/pull/7823).

## CPP Client

### Wait for all seek operations complete

When a partitioned consumer calls `seek`, it waits for only one partition's seek operation completion because each internal consumer calls callback(result) to complete the same promise.

In 2.6.2, we use the following methods to avoid this problem:

- Add a `MultiResultCallback` implementation, the callback completes only when all N events complete successfully or one of N events fails.
- Use `MultiResultCallback` to wrap callback from `PartitionedConsumerImpl::seekAsync`.

For more information about implementation, see [PR-7216](https://github.com/apache/pulsar/pull/7216).

### Make `clear()` thread-safe

Before 2.6.2, the `clear()` methods of `BatchAcknowledgementTracker` and `UnAckedMessageTrackerEnabled` are not thread-safe.

In 2.6.2, we acquire a mutex in these `clear()` methods to make it thread-safe.

For more information about implementation, see [PR-7862](https://github.com/apache/pulsar/pull/7862).

### Add Snappy library to Docker images for building C++ packages

The program crashes when Snappy compression is enabled on the C++ client packaged as RPM/DEB. This is because Snappy library is not included in the Docker image for building the RPM/DEB package.

In 2.6.2, we add the Snappy library to the docker images to avoid the issue.

For more information about implementation, see [PR-8086](https://github.com/apache/pulsar/pull/8086).

### Support key based batching

Support key based batching for the C++ client. In addition, currently, the implementation of `BatchMessageContainer` is coupling to `ProducerImpl` tightly. The batch message container registers a timer to the producer's executor and the timeout callback is also the producer's method. Even its `add` method could call `sendMessage` to send a batch to the producer's pending queue. These should be the producer's work.

In 2.6.2, we implement the feature in the following ways:

- Add a `MessageAndCallbackBatch` to store a `MessageImpl` of serialized single messages and a callback list.
- Add a `BatchMessageContainerBase` to provide interface methods and methods like update/clear message number/bytes, create `OpSendMsg`.
- Let `ProducerImpl` manage the batch timer and determine whether to create `OpSendMsg` from `BatchMessageContainerBase` and send it.
- Make `BatchMessageContainer` inherit `BatchMessageContainerBase`, it only manages a `MessageAndCallbackBatch`.
- Add a `BatchMessageKeyBasedContainer` that inherits `BatchMessageContainerBase`, it manages a map of message key and `MessageAndCallbackBatch`.
- Add a producer config to change batching type.

For more information about implementation, see [PR-7996](https://github.com/apache/pulsar/pull/7996).

## Functions

### Enable Kubernetes runtime to customize function instance class path

Before 2.6.2, the function worker's classpath is used to configure the function instance (runner)'s classpath. When the broker (function worker) uses an image that is different from the function instance (runner) for Kubernetes runtime, the classpath is wrong and the function instance could not load the instance classes.

In 2.6.2, we add a function instance classpath entry to the Kubernetes runtime config, and construct the function launch command accordingly.

For more information about implementation, see [PR-7844](https://github.com/apache/pulsar/pull/7844).

### Set `dryrun` of Kubernetes Runtime to null

Before 2.6.2, we upgraded the `client-java` of Kubernetes to `0.9.2` to enhance security. However, during the creation of statefulsets, secrets, and services, the value of `dryrun` was set to `true`, which was not accepted by Kubernetes. Only `All` is allowed in Kubernetes. 

In 2.6.2, we set the `dryrun` of Kubernetes Runtime to null.

For more information about implementation, see [PR-8064](https://github.com/apache/pulsar/pull/8064).

## Pulsar SQL

### Upgrade Presto version to 332

Upgrade Presto version to 332. Resolve different packages between prestosql and prestodb. Although the latest version is 334, versions higher than 333 require Java 11.

For more information about implementation, see [PR-7194](https://github.com/apache/pulsar/pull/7194).

## pulsar-admin

### Add CLI command to get the last message ID

Add `last-message-id` command in CLI, so users can get the last message ID with this command.

For more information about implementation, see [PR-8082](https://github.com/apache/pulsar/pull/8082).

### Support deleting schema ledgers when deleting topics

Users could not delete schema of topics with the `PersistentTopics#deleteTopic` and `PersistentTopics#deletePartitionedTopic` in REST APIs. After topics were deleted, the schema ledgers still existed with adding an empty schema ledger.

In 2.6.2, we implement the feature in the following ways:

- Add a `deleteSchema` query param to REST APIs of deleting topics/partitioned topics;
- Add a map to record the created ledgers in `BookkeeperSchemaStorage`;
- Expose `deleteSchema` param in pulsar-admin APIs;
- Delete schema ledgers when deleting the cluster with `-a` option.

For more information about implementation, see [PR-8167](https://github.com/apache/pulsar/pull/8167).

### Support deleting all data associated with a cluster

When multiple broker clusters shared the same bookie cluster, if users wanted to remove a broker cluster, the associated ledgers in bookies were not deleted as expected.

In 2.6.2, we add a `cluster delete` command to enable users to delete all the data associated with the cluster.

For more information about implementation, see [PR-8133](https://github.com/apache/pulsar/pull/8133).

## Pulsar Perf

### Enable users to configure ioThread number in pulsar-perf

In pulsar-perf, the default Pulsar client ioThread number is `Runtime.getRuntime().availableProcessors()` and users could not configure it in the command line. When running a pulsar-perf producer, it may cause messages to enqueue competition and lead to high latency.

In 2.6.2, we implement the feature in the following ways:

1. Enable users to configure the ioThread number in the command line;
2. Change the default ioThead number from `Runtime.getRuntime().availableProcessors()` to `1`

For more information about implementation, see [PR-8090](https://github.com/apache/pulsar/pull/8090).

## More information

- To download Apache Pulsar 2.6.2, click [download](https://pulsar.apache.org/en/download/).
- For more information about Apache Pulsar 2.6.2, see [2.6.2 release notes](https://pulsar.apache.org/release-notes/#2.6.2 and [2.6.2 PR list](https://github.com/apache/pulsar/pulls?q=is%3Apr+label%3Arelease%2F2.6.2+is%3Aclosed).

If you have any questions or suggestions, contact us with mailing lists or slack.

- [users@pulsar.apache.org](mailto:users@pulsar.apache.org)
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)
- Pulsar slack channel: https://apache-pulsar.slack.com/
- Self-registration at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Pulsar](https://github.com/apache/pulsar).
