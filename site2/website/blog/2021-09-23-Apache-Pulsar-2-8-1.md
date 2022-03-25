---
author: Hang Chen, Anonymitaet
title: Apache Pulsar 2.8.1
---

# What’s New in Apache Pulsar 2.8.1

The Apache Pulsar community releases version 2.8.1! 49 contributors provided improvements and bug fixes that delivered 213 commits.

Highlights of this release are as below:

- Key-shared subscriptions no longer stop dispatching to consumers when repeatedly opening and closing consumers. [PR-10920](https://github.com/apache/pulsar/pull/10920)

- System topic no longer has potential data loss when not configured for compaction. [PR-11003](https://github.com/apache/pulsar/pull/11003)

- Consumers are not allowed to read data on topics to which they are not subscribed. [PR-11912](https://github.com/apache/pulsar/pull/11912)

This blog walks through the most noteworthy changes grouped by component. For the complete list including all features, enhancements, and bug fixes, check out the [Pulsar 2.8.1 Release Notes](https://pulsar.apache.org/release-notes/#281-mdash-2021-09-10-a-id281a).

# Notable bug fixes and enhancements

## Broker

### Precise publish rate limit takes effect as expected. [PR-11446](https://github.com/apache/pulsar/pull/11446)

**Issue**: Previously, when setting precise publish rate limits on topics, it did not work.

**Resolution**: Implemented a new `RateLimiter` using the `LeakingBucket` and `FixedWindow` algorithms.

### Messages with the same keys are delivered to the correct consumers on Key-Shared subscriptions. [PR-10762](https://github.com/apache/pulsar/pull/10762)

**Issue**: Messages with the same keys were out of order when message redelivery occurred on a Key-Shared subscription.

**Resolution**: When sending a message to `messagesToRedeliver`, the broker saved the hash value of the key. If the dispatcher attempted to send newer messages to the consumer that had a key corresponding to any one of the saved hash values, they were added to `messagesToRedeliver` instead of being sent. This prevented messages with the same key from being out of order.

### Active producers with the same name are no longer removed from the topic map. [PR-11804](https://github.com/apache/pulsar/pull/11804)

**Issue**: Previously, when there were producers with the same name, an error would be triggered and the old producer would be removed even though it was still writing to a topic.

**Resolution**: Validated producers based on a connection ID (local & remote addresses and unique ID) and a producer ID within that connection rather than a producer name.

### Topics in a fenced state can recover when producers continue to reconnect to brokers. [PR-11737](https://github.com/apache/pulsar/pull/11737)

**Issue**: Previously, when a producer continued to reconnect to a broker, the fenced state of the topic was always set to true, which caused the topic to be unable to recover.

**Resolution**: Add an entry to `ManagedLedgerException` when the polled operation is not equal to the current operation.

### Topic properly initializes the cursor to prevent data loss. [PR-11547](https://github.com/apache/pulsar/pull/11547)

**Issue**: Previously, when subscribing to a topic with the earliest position, data would be lost because `ManagedLedger` used a wrong position to initialize a cursor.

**Resolution**: Added a test to check a cursor's position when subscribing to a topic with the earliest position.

### Deadlock no longer occurs when using `hasMessageAvailableAsync` and `readNextAsync`. [PR-11183](https://github.com/apache/pulsar/pull/11183)

**Issue**: Previously, when messages were added to an incoming queue, a deadlock might occur. The deadlock might happen in two possible scenarios. First, if the message was added to the queue before the message was read. Second, if `readNextAsync` was completed before `future.whenComplete` was called.

**Resolution**: Used an internal thread to process the callback of `hasMessageAvailableAsync`.

### Memory leak does not occur when calling getLastMessageId API. [PR-10977](https://github.com/apache/pulsar/pull/10977)

**Issue**: Previously, the broker ran out of memory when calling the `getLastMessageId` API.

**Resolution**: Added the `entry.release()` call to the `PersistentTopic.getLastMessageId`.

### Compaction is triggered for system topics. [PR-10941](https://github.com/apache/pulsar/pull/10941)

**Issue**: Previously, when a topic had only non-durable subscriptions, the compaction was not triggered because it had 0 estimated backlog size. 

**Resolution**: Used the total backlog size to trigger the compaction. Changed the behavior in the case of no durable subscriptions to use the total backlog size

### Key-shared subscriptions no longer stop dispatching to consumers when repeatedly opening and closing consumers. [PR-10920](https://github.com/apache/pulsar/pull/10920)

**Issue**: Repeatedly opening and closing consumers with a Key-Shared subscription might occasionally stop dispatching messages to all consumers.

**Resolution**: Moved the mark-delete position and removed the consumer from the selector before calling `removeConsumer()`.

### Consumers are not allowed to read data on topics to which they are not subscribed. [PR-11912](https://github.com/apache/pulsar/pull/11912)

**Issue**: Previously, the request ledger was not checked whether it belonged to a consumer’s connected topic, which allowed the consumer to read data that does not belong to the connected topic.

**Resolution**: Added a check on the `ManagedLedger` level before executing read operations. 

## Topic Policy

### Retention policy works as expected. [PR-11021](https://github.com/apache/pulsar/pull/11021)

**Issue**: Previously, the retention policy did not work because it was not set in the `managedLedger` configuration.

**Resolution**: Set the retention policy in the `managedLedger` configuration to the `onUpdate` listener method.

### System topic no longer has potential data loss when not configured for compaction. [PR-11003](https://github.com/apache/pulsar/pull/11003) 

**Issue**: Previously, data might be lost if there were no durable subscriptions on topics.

**Resolution**: Leveraged the topic compaction cursor to retain data.

## Proxy

### Pulsar proxy correctly shuts down outbound connections. [PR-11848](https://github.com/apache/pulsar/pull/11848)

**Issue**: Previously, there was a memory leak of outgoing TCP connections in the Pulsar proxy because the `ProxyConnectionPool` instances were created outside the `PulsarClientImpl` instance and not closed when the client was closed.

**Resolution**: Shut down the `ConnectionPool` correctly.

## Function

### Pulsar Functions support Protobuf schema. [PR-11709](https://github.com/apache/pulsar/pull/11709)

**Issue**: Previously, the exception `GeneratedMessageV3 is not assignable` was thrown when using a Protobuf schema.

**Resolution**: Added the relevant dependencies to the Pulsar instance.

## Client

### Partitioned-topic consumers clean up resources after a failure. [PR-11754](https://github.com/apache/pulsar/pull/11754)

**Issue**: Previously, partitioned-topic consumers did not clean up the resources when failing to create consumers. If this failure occurred with non-recoverable errors, it triggered a memory leak, which made applications unstable.

**Resolution**: Closed and cleaned timer task references.

### Race conditions do not occur on multi-topic consumers. [PR-11764](https://github.com/apache/pulsar/pull/11764)

**Issue**: Previously, there was a race condition between 2 threads when one of the individual consumers was in a "paused" state and the shared queue was full. 

**Resolution**: Validated the state of the shared queue after marking the consumer as "paused". The consumer is not blocked if the other thread has emptied the queue in the meantime. 

### Consumers are not blocked on `batchReceive`. [PR-11691](https://github.com/apache/pulsar/pull/11691)

**Issue**: Previously, consumers were blocked when `Consumer.batchReceive()` was called concurrently by different threads due to a race condition in `ConsumerBase.java`.

**Resolution**: Put `pinnedInternalExecutor` in `ConsumerBase` to allow batch timer, `ConsumerImpl`, and `MultiTopicsConsumerImpl` to submit work in a single thread.

### Python client correctly enables custom logging. [PR-11882](https://github.com/apache/pulsar/pull/11882)

**Issue**: Previously, deadlock might happen when custom logging was enabled in the Python client.

**Resolution**: Detached the worker thread and reduced log level.

# What is Next?

If you are interested in learning more about Pulsar 2.8.1, you can [download](https://pulsar.apache.org/en/download/) and try it out now! 

The first-ever Pulsar Virtual Summit Europe 2021 will take place in October. [Register now](https://hopin.com/events/pulsar-summit-europe-2021) and help us make it an even bigger success by spreading the word on social media!

For more information about the Apache Pulsar project and the progress, visit
the [Pulsar website](https://pulsar.apache.org), follow the project on Twitter
[@apache_pulsar](https://twitter.com/apache_pulsar), and join [Pulsar Slack](https://apache-pulsar.herokuapp.com/)!
