---
author: XiaoLong Ran
authorURL: https://twitter.com/wolf4j1
title: Apache Pulsar 2.6.2
---
We are excited to see that the Apache Pulsar community has successfully released the 2.6.2 version after a lot of hard work. It is a great milestone for this fast-growing project and the Pulsar community. 2.6.2 is the result of a big effort from the community, with over 154 commits and a long list of improvements and bug fixes.

Here are some highlights and major features added in Pulsar 2.6.2.

<!--truncate-->

## Broker

### Catch throwable when start pulsar

Before 2.6.1, pulsar only catch exception when `BrokerStarter.start()` failed, some error such as `NoSuchMethodError` or `NoClassDefFoundError` can not be caught, and pulsar will in abnormal status but no error log will found in the log file. 

In 2.6.1, we modify exception to throwable to avoid this issue.

For more information about implementation, see [PR-7221](https://github.com/apache/pulsar/pull/7221).

### Handle SubscriptionBusyException in resetCursor API

In method `PersistentSubscription.resetCursor`, `SubscriptionFencedException` is thrown in several place, but not handled in `PersistentTopicBase`. It caused the error message not very clear. This fix tries to export `SubscriptionBusyException` in PersistentTopicBase for resetCursor, to make the error in reset API more clear.

For more information about implementation, see [PR-7335](https://github.com/apache/pulsar/pull/7335).

### Converted the namespace bundle unload into async operation

Reworked the namespace bundle unload REST handler to be asynchronous. This avoid getting stuck in the Jetty threads calling into themselves when we try to unload the entire namespace.

For more information about implementation, see [PR-7364](https://github.com/apache/pulsar/pull/7364).

### Support initial namespace of the cluster without startup the broker

Setup the initial namespace of the cluster without startup the broker.

For more information about implementation, see [PR-7434](https://github.com/apache/pulsar/pull/7434).

### Update Jersey to 2.31

Before 2.6.1, The Jersey version we use is 2.27, which has some security vulnerabilities, so in 2.6.2, we update Jersey version to latest stable version(2.31) for bug fixes and security updates.

For more information about implementation, see [PR-7515](https://github.com/apache/pulsar/pull/7515).

### Stop to dispatch when skip message temporally since Key_Shared consumer stuck on delivery

In some case of Key_Shared consumer, messages ordering was broken. Here is how to reproduce:

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

In 2.6.2, we stop to dispatch when skip message temporally since Key_Shared consumer stuck on delivery.

For more information about implementation, see [PR-7553](https://github.com/apache/pulsar/pull/7553).

### Reestablish namespace bundle ownership from false negative releasing and false positive acquiring

In acquiring/releasing namespace bundle ownership, zk may disconnected before or after these operations persisted in zk cluster. This introduce inconsistency between local ownership cache and zk cluster.

In 2.6.2, we do two things:

* In ownership releasing, don't retain ownership in failure.
* In ownership checking, querying and acquiring, reestablish lost ownership in false negative releasing and false positive acquiring.

For more information about implementation, see [PR-7773](https://github.com/apache/pulsar/pull/7773).

### Make pulsar executor pool size configurable

Before 2.6.2, the pulsar executor pool size number is hard code to `20` when pulsar service start, it should be configurable in `broker.conf`.

```
private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
            new DefaultThreadFactory("pulsar"));
```

In 2.6.2, we make the executor pool size configurable in `broker.conf`.

For more information about implementation, see [PR-7782](https://github.com/apache/pulsar/pull/7782).

### Add replicated check to checkInactiveSubscriptions

After the replicated subscription is deleted by checkInactiveSubscriptions, it will be recreated.

Replicated subscriptions are created with `receiveSubscriptionUpdated`. In this case, the position becomes the latest position.

```
 topic.createSubscription(update.getSubscriptionName(), 
         InitialPosition.Latest, true /* replicateSubscriptionState */); 
```

In 2.6.2, we make sure replicated subscription should be excluded from automatic deletion by fixing the `PersistentTopic`.

For more information about implementation, see [PR-8066](https://github.com/apache/pulsar/pull/8066).

### Upgrade jetty-util version to 9.4.31

Apparently pulsar-client depends on a version of jetty-util which contains known vulnerabilities, newer versions of `jetty-util >= 9.4.30` do not suffer from this vulnerability. 

In 2.6.2, we upgrade jetty-util version to `9.4.31`.

For more information about implementation, see [PR-8035](https://github.com/apache/pulsar/pull/8035).

### Add command to delete a cluster's metadata from ZK

When we share the same ZK and BK cluster with multiple broker clusters, if a cluster was removed, its metadata in ZK should also be removed.

In 2.6.2, we do these things to fix the issue:

- Add a `PulsarClusterMetadataTeardown` class to delete the relative nodes from ZK;
- Wrap the class to `bin/pulsar` script.

For more information about implementation, see [PR-8169](https://github.com/apache/pulsar/pull/8169).

### Use ThreadPoolExecutor instead of EventLoop

When Netty's `EventLoop` receives a new task，it will call `eventFdWrite`, and then trigger system calls, such as system_call_fastpath, eventfd_write
After we replaced EventLoop with a native JDK thread pool, the performance improved.

We use pulsar perf for testing

before 2.6.1:

```
Aggregated throughput stats --- 11715556 records received --- 68813.420 msg/s --- 537.605 Mbit/s
```

in 2.6.2：

```
Aggregated throughput stats --- 18392800 records received --- 133314.602 msg/s --- 1041.520 Mbit/s
```

For more information about implementation, see [PR-8208](https://github.com/apache/pulsar/pull/8208).

### Fix deadlock that occurred during topic ownership check

Sometimes, some of user broker servers had deadlocks while splitting namespace bundles. As a result of checking the thread dump of the broker, some threads were blocked in `NamespaceService#getBundle()`.


```
"pulsar-ordered-OrderedExecutor-7-0" #34 prio=5 os_prio=0 tid=0x00007eeeab05a800 nid=0x81a5 waiting on condition [0x00007eeeafbd2000]
   java.lang.Thread.State: WAITING (parking)
        at sun.misc.Unsafe.park(Native Method)
        - parking to wait for  <0x00007f17fa965418> (a java.util.concurrent.CompletableFuture$Signaller)
        at java.util.concurrent.locks.LockSupport.park(LockSupport.java:175)
        at org.apache.pulsar.common.naming.NamespaceBundleFactory.getBundles(NamespaceBundleFactory.java:155)
...
```

This is because the blocking method `getBundle()` should not be used in `NamespaceService#isTopicOwned()`. So, `isTopicOwned()` returns false once, but gets the bundle metadata asynchronously so that the metadata is cached. The next time the client reconnects, the bundle metadata has been cached so it can return the correct result.

For more information about implementation, see [PR-8406](https://github.com/apache/pulsar/pull/8406).

## Proxy

### Add advertisedAddress config field to ProxyConfiguration

Before 2.6.2, There is no way for users to configure advertisedAddress on the proxy side. In 2.6.2, the proxy can have it's `advertisedAddress` configured like pulsar broker.

For more information about implementation, see [PR-7542](https://github.com/apache/pulsar/pull/7542).

### Add proxy plugin interface to support user defined additional servlet

In order to facilitate users' flexible access in the broker, we provide plug-ins similar to broker protocol and broker interceptor. Similarly, in the proxy, sometimes users also need similar plug-in functions to facilitate customizing some data requests. So in 2.6.2, we add the protocol plugin for proxy.

For more information about implementation, see [PR-8067](https://github.com/apache/pulsar/pull/8067).


### Fix the null exception when starting the proxy service

When enable the broker tls and enable broker client authentication with oauth2 plugin,
the proxy service will exit with an unexpected null exception.

Because there are some methods call authentication before initializing the flow
which will cause the token client isn't initialized before using.

For more information about implementation, see [PR-8019](https://github.com/apache/pulsar/pull/8019).

## Java Client

### Support input-stream for trustStore cert

In release 2.6.1, we introduced support of dynamic cert loading by supporting input stream for tls cert and key file. It mainly used by container usecase. This usecase also requires dynamic loading for truststore certs and user can't store trust-store cert into file-system. So in release 2.6.2, support to load truststore cert dynamically using input stream.

For more information about implementation, see [PR-7442](https://github.com/apache/pulsar/pull/7442).

### Avoid subscribing the same topic again

The current key of `MultiTopicsConsumerImpl.topics` is the topic name passed by the user. The `topicNameValid` method checks if the name is valid and `topics` doesn't contain the key.

However, if a multi topics consumer subscribed a partition of a subscribed partitioned topic,  `subscribeAsync` succeeds and a new `ConsumerImpl` of the same partition was created, which is redundant.

Also, if a multi topics consumer subscribed `public/default/topic` or `persistent://public/default/topic`, while the initial subscribed topic is `topic`, the redundant consumers would be created.

In 2.6.2, we did the following things to avoid subscribing the same topic again:

- Use full topic name as key of `MultiTopicsConsumerImpl.topics`
- Check both full topic name and full partitioned topic name not exist in `MultiTopicsConsumerImpl.topics` when `subscribeAsync` is called
- Throw a different exception to different topic is invalid and the topic is already subscribed

For more information about implementation, see [PR-7823](https://github.com/apache/pulsar/pull/7823).

## CPP Client

### Wait for all seek operations completed

When a partitioned consumer calls seek, it waits for only one partition's seek operation completed because each internal consumer calls callback(result) to complete the same Promise. 

In 2.6.2, We use the following methods to avoid this problem:

- Add a `MultiResultCallback` implementation, the callback completes only if all N events completes successfully or one of N events failed.
- Use `MultiResultCallback` to wrap callback from `PartitionedConsumerImpl::seekAsync`.

For more information about implementation, see [PR-7216](https://github.com/apache/pulsar/pull/7216).

### Make `clear()` thread-safe

Before 2.6.1, the `clear()` methods of `BatchAcknowledgementTracker` and `UnAckedMessageTrackerEnabled` are not thread-safe.

In 2.6.2, Acquire a mutex in these `clear()` methods.

For more information about implementation, see [PR-7862](https://github.com/apache/pulsar/pull/7862).

### Add Snappy library to Docker images for building C++ packages

The program crashes when Snappy compression is enabled on the C++ client packaged as RPM/DEB. This is because Snappy library is not included in the Docker image for building the RPM/DEB package.

In 2.6.2, we add the Snappy library to the docker images to avoid the issue.

For more information about implementation, see [PR-8086](https://github.com/apache/pulsar/pull/8086).

### Support key based batching

Support key based batching for C++ client. In addition, currently, the implementation of `BatchMessageContainer` is coupling to `ProducerImpl` tightly. The batch message container registers a timer to the producer's executor and the timeout callback is also the producer's method. Even its `add` method could call `sendMessage` to send a batch to the producer's pending queue. These should be the producer's work.

In 2.6.2, we do these this to implement the feature:

- Add a `MessageAndCallbackBatch` to store a `MessageImpl` of serialized single messages and a callback list.
- Add a `BatchMessageContainerBase` to provide interface methods and methods like update/clear message number/bytes, create `OpSendMsg`.
- Let `ProducerImpl` manage the batch timer and determine whether to create `OpSendMsg` from `BatchMessageContainerBase` and send it.
- Make `BatchMessageContainer` inherit `BatchMessageContainerBase`, it only manages a `MessageAndCallbackBatch`.
- Add a `BatchMessageKeyBasedContainer` that inherits `BatchMessageContainerBase`, it manages a map of message key and `MessageAndCallbackBatch`.
- Add a producer config to change batching type.

For more information about implementation, see [PR-7996](https://github.com/apache/pulsar/pull/7996).


### Client is allocating buffer bigger than needed

The C++ client is using a huge amount of memory (eg: ~10GB) when it's partitioned from brokers and it's using a topic with many partitions. This is kind also present when the producer queue sizes are kind of small.

The reason is that the buffer size for the batch is always allocated with 5MB capacity, even though we're going to need much less memory per each batch.

For more information about implementation, see [PR-8283](https://github.com/apache/pulsar/pull/8283).

## Functions

### During Function update, cleanup should only happen for temp files that were generated

When doing function updates, cleanup should only be done for files that were temporarily created.

For more information about implementation, see [PR-7201](https://github.com/apache/pulsar/pull/7201).

### Allow kubernetes runtime to customize function instance class path

Before 2.6.1, the function worker is using the function worker's classpath to configure the function instance (runner)'s classpath. So when the broker (function worker) is using an image that is different from the function instance (runner) for kubernetes runtime, the classpath will be wrong and the function instance is not able to load the instance classes.

In 2.6.2, we add a function instance classpath entry into the Kubernetes runtime config. And construct the function launch command accordingly.

For more information about implementation, see [PR-7844](https://github.com/apache/pulsar/pull/7844).

### Set dryrun of KubernetesRuntime is null

Due to security issues, we upgrade the `client-java` of Kubernetes to `0.9.2`, this will cause the following error. During the creation of statefulsets, secrets, and services, the code has been configured to set the "dryrun" value of "true". "true" is not accepted by Kubernetes. Only "All" is allowed. So the value should be set to null.

In 2.6.2, we set the `dryrun` of KubernetesRuntime is null.

For more information about implementation, see [PR-8064](https://github.com/apache/pulsar/pull/8064).

## Pulsar SQL

### Upgrade Presto version to 332

Upgrade Presto version to 332. Resolve different package between prestosql and prestodb. Although the latest version is 334, version >= 333 requires Java 11.

For more information about implementation, see [PR-7194](https://github.com/apache/pulsar/pull/7194).

## pulsar-admin

### Add CLI command to get last message ID

Add CLI to support get `last-message-id`

For more information about implementation, see [PR-8082](https://github.com/apache/pulsar/pull/8082).

### Support delete schema ledgers when delete topics

The REST APIs of `PersistentTopics#deleteTopic` and `PersistentTopics#deletePartitionedTopic` didn't support delete schema of topics, which may cause after topics were deleted, the schema ledgers still existed. And the current implementation of delete schema just adds an empty schema ledger but not delete existed schema ledgers.

In 2.6.2, we do these things too implement the feature:

- Add a `deleteSchema` query param to REST APIs of deleting topics/partitioned topics;
- Add a map to record the created ledgers in `BookkeeperSchemaStorage`;
- Expose `deleteSchema` param in pulsar-admin APIs;
- Delete schema ledgers when deleting the cluster with `-a` option.

For more information about implementation, see [PR-8167](https://github.com/apache/pulsar/pull/8167).

### Support delete all data associated with a cluster

When multiple broker clusters shared the same bookie cluster, if user wanted to remove a broker cluster, the associated ledgers in bookies should also be deleted.

In 2.6.2, add an option `cluster delete` command to delete all the data associated with the cluster.

For more information about implementation, see [PR-8133](https://github.com/apache/pulsar/pull/8133).

## Pulsar Perf

### Make pulsar-perf ioThread number configurable

In pulsar-perf, the default pulsar client ioThread number is `Runtime.getRuntime().availableProcessors()` and can't be configured in command line. When running a pulsar-perf producer, it may cause message to enqueue competition and lead to high latency.

In 2.6.2, we do these things to impl the feature:

1. make ioThread number configurable in command line
2. change the default ioThead number from `Runtime.getRuntime().availableProcessors()` to `1`

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
