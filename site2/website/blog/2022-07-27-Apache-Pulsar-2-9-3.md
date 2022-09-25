---
title: "What’s New in Apache Pulsar 2.9.3"
date: 2022-07-27
author: "mattisonchao, momo-jun"
---

The Apache Pulsar community releases version 2.9.3! 50 contributors provided improvements and bug fixes that delivered 200+ commits. Thanks for all your contributions.

The highlight of the 2.9.3 release is introducing 30+ transaction fixes and improvements. Earlier-adoption users of Pulsar transactions have documented long-term use in their production environments and reported valuable findings in real applications. This provides the Pulsar community with the opportunity to make a difference. 

This blog walks through the most noteworthy changes. For the complete list including all feature enhancements and bug fixes, check out the [Pulsar 2.9.3 Release Notes](https://pulsar.apache.org/release-notes/versioned/pulsar-2.9.3/).


### Enabled cursor data compression to reduce persistent cursor data size. [14542](https://github.com/apache/pulsar/pull/14542)

#### Issue
The cursor data is managed by the ZooKeeper/Etcd metadata store. When the data size increases, it may take too much time to pull the data, and brokers may end up writing large chunks of data to the ZooKeeper/Etcd metadata store.

#### Resolution
Provide the ability to enable compression mechanisms to reduce cursor data size and the pulling time.


### Reduced the memory occupied by `metadataPositions` and avoid OOM. [15137](https://github.com/apache/pulsar/pull/15137)

#### Issue
The map `metadataPositions` in MLPendingAckStore is used to clear useless data in PendingAck, where the key is the position that is persistent in PendingAck and the value is the max position acked by an operation. It judges whether the max subscription cursor position is smaller than the subscription cursor’s `markDeletePosition`. If the max position is smaller, then the log cursor will mark to delete the position. It causes two main issues:
* In normal cases, this map stores all transaction ack operations. This is a waste of memory and CPU.
* If a transaction that has not been committed for a long time acks a message in a later position, the map will not be cleaned up, which finally leads to OOM (out-of-memory).

#### Resolution
Regularly store a small amount of data according to certain rules. For more detailed implementation, refer to [PIP-153](https://github.com/apache/pulsar/issues/15073).


### Checked `lowWaterMark` before appending transaction entries to Transaction Buffer. [15424](https://github.com/apache/pulsar/pull/15424)

#### Issue
When a client sends messages using a previously committed transaction, these messages are visible to consumers unexpectedly.

#### Resolution
Add a map to store the `lowWaterMark` of Transaction Coordinator in Trasanction Buffer, and check `lowWaterMark` before appending transaction entries to Trasanction Buffer. So when sending messages using an invalid transaction, clients will receive `NotAllowedException`. 


### Fixed the consumption performance regression. [PR-15162](https://github.com/apache/pulsar/pull/15162)

#### Issue
This performance regression was introduced in 2.10.0, 2.9.1, and 2.8.3. You may find a significant performance drop with message listeners while using Java Client. The root cause is each message will introduce the thread switching from the external thread pool to the internal thread poll and then to the external thread pool.

#### Resolution
Avoid the thread switching for each message to improve consumption throughput.


### Fixed a deadlock issue of topic creation. [PR-15570](https://github.com/apache/pulsar/pull/15570)

#### Issue
This deadlock issue occurred during topic creation by trying to re-acquire the same `StampedLock` from the same thread when removing it. This will cause the topic to stop service for a long time, and ultimately with a failure in the deduplication or geo-replication check. The workaround is restarting the broker.


### Optimized the memory usage of brokers.

#### Issue
Pulsar has some internal data structures, such as `ConcurrentLongLongPairHashMap`, and `ConcurrentLongPairHashMap`, which can reduce the memory usage rather than using the Boxing type. However, in earlier versions, the data structures were not supported for shrinking even if the data was removed, which wasted a certain amount of memory in certain situations.

**Pull requests**
* https://github.com/apache/pulsar/pull/15354
* https://github.com/apache/pulsar/pull/15342
* https://github.com/apache/pulsar/pull/14663
* https://github.com/apache/pulsar/pull/14515
* https://github.com/apache/pulsar/pull/14497

#### Resolution
Support the shrinking of the internal data structures, such as `ConcurrentSortedLongPairSet`, `ConcurrentOpenHashMap`, and so on.


# What’s Next?

If you are interested in learning more about Pulsar 2.9.3, you can [download](https://pulsar.apache.org/versions/) and try it out now! 

**Pulsar Summit San Francisco 2022** will take place on August 18th, 2022. [Register now](https://pulsar-summit.org/) and help us make it an even bigger success by spreading the word on social media!

For more information about the Apache Pulsar project and current progress, visit
the [Pulsar website](https://pulsar.apache.org), follow the project on Twitter
[@apache_pulsar](https://twitter.com/apache_pulsar), and join [Pulsar Slack](https://apache-pulsar.herokuapp.com/)!