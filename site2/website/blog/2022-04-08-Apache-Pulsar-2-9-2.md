---
author: gaoran10, Anonymitaet
title: What’s New in Apache Pulsar 2.9.2
---

The Apache Pulsar community releases version 2.9.2! 60 contributors provided improvements and bug fixes that delivered 317 commits.

Highlights of this release are as below:

- Transactions performance test tool is available. [PR-11933](https://github.com/apache/pulsar/pull/11933)
  
- Brokers decrease the number of unacked messages. [PR-13383](https://github.com/apache/pulsar/pull/13383)
  
- Readers continue to read data from the compacted ledgers. [PR-13629](https://github.com/apache/pulsar/pull/13629)

This blog walks through the most noteworthy changes grouped by the affected functionalities. For the complete list including all features, enhancements, and bug fixes, check out the [Pulsar 2.9.2 Release Notes](https://github.com/apache/pulsar/releases/tag/v2.9.2).

# Notable bug fixes and enhancements

### Readers continue to read data from the compacted ledgers. [PR-13629](https://github.com/apache/pulsar/pull/13629)

#### Issue

Previously, when topics were unloaded, some data was lost to be read by readers if they have consumed some messages from some compacted ledgers.

#### Resolution

Rewound the reader cursor to the next message of the mark delete position if `readCompacted = true`.

### Brokers decrease the number of unacked messages. [PR-13383](https://github.com/apache/pulsar/pull/13383)

#### Issue

Previously, brokers did not decrease the number of unacked messages if batch ack was enabled. Consequently, consumers were blocked if they reached `maxUnackedMessagesPerConsumer` limit.

#### Resolution

Decreased the number of unacked messages when `individualAckNormal` was called.

### Chunked messages can be queried through Pulsar SQL. [PR-12720](https://github.com/apache/pulsar/pull/12720)

#### Issue

Previously, chunked messages could not be queried through Pulsar SQL.

#### Resolution

Add a chunked message map in `PulsarRecordCursor` to maintain incomplete chunked messages. If one chunked message was received completely, it would be offered in the message queue to wait for deserialization. 

### Support enable or disable schema upload at the broker level. [PR-12786](https://github.com/apache/pulsar/pull/12786)

#### Issue

Previously, Pulsar didn't support enabling or disabling schema upload at the broker level.

#### Resolution

Added the configuration `isSchemaAutoUploadEnabled` on the broker side.

### Readers can read the latest messages in compacted topics. [PR-14449](https://github.com/apache/pulsar/pull/14449)

#### Issue

Previously, readers were not able to read the latest messages in compacted topics if readers enabled `readCompacted` and all the data of topics has been compacted to compacted ledgers.

#### Resolution

Added the `forceReset` configuration for the managed cursor, so that the cursor could be reset to a given position and readers can read data from compacted ledgers. 

### Transaction sequenceId can be recovered correctly. [PR-13209](https://github.com/apache/pulsar/pull/13209)

#### Issue

Previously, the wrong transaction sequenceId was recovered due to incorrect `managedLedger` properties.

#### Resolution

Used `ManagedLedgerInterceptor ` to update current sequenceId to `managedLedger` properties and more.

### Transactions performance test tool is available. [PR-11933](https://github.com/apache/pulsar/pull/11933)

#### Issue

Previously, it was hard to test transaction performance (such as the delay and rate of sending and consuming messages) when opening a transaction.

#### Resolution

Added `PerformanceTransaction` class to support this enhancement.

### Port exhaustion and connection issues no longer exist in Pulsar Proxy. [PR-14078](https://github.com/apache/pulsar/pull/14078)

#### Issue

Previously, Pulsar proxy would get into a state where it stopped proxying broker connections while Admin API proxying kept working.

#### Resolution

Optimized the proxy connection to fail-fast if the target broker was not active, added connect timeout handling to proxy connection, and more.

### No race condition in `OpSendMsgQueue` when publishing messages. [PR-14231](https://github.com/apache/pulsar/pull/14231)

#### Issue

After the method `getPendingQueueSize()` was called and the send receipt came back, the peek from the `pendingMessages` might get NPE during the process.

#### Resolution

Added a thread-safe message count object in `OpSendMsgQueue` for each compute process.

### Change `ContextClassLoader` to `NarClassLoader` in AdditionalServlet. [PR-13501](https://github.com/apache/pulsar/pull/13501)

#### Issue

Previously, if a class was dynamically loaded by `NarClassLoader`, `ClassNotFoundException` occurred when it was used by the default class load.

#### Resolution

Changed context class loader through `Thread.currentThread().setContextClassLoader(classLoader)` before every plugin calling back and changed the context class loader back to original class loader afterwards.

# What’s Next?

If you are interested in learning more about Pulsar 2.9.2, you can [download](https://pulsar.apache.org/en/versions/) and try it out now! 

**Pulsar Summit San Francisco 2022** will take place on August 18th, 2022. [Register now](https://pulsar-summit.org/) and help us make it an even bigger success by spreading the word on social media!

For more information about the Apache Pulsar project and current progress, visit
the [Pulsar website](https://pulsar.apache.org), follow the project on Twitter
[@apache_pulsar](https://twitter.com/apache_pulsar), and join [Pulsar Slack](https://apache-pulsar.herokuapp.com/)!



