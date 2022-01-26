---
author: Jia Zhai
authorURL: https://twitter.com/Jia_Zhai
title: Apache Pulsar 2.5.2
---

We are proud to publish Apache Pulsar 2.5.2. This is the result of a huge effort from the community, with over 56 commits, general improvements and bug fixes.

For detailed changes related to 2.5.2 release, refer to the <b>[release notes](/release-notes/#2.5.2)</b> and the <b>[PR list for Pulsar 2.5.2](https://github.com/apache/pulsar/pulls?q=is:pr%20label:release/2.5.2%20is:closed)</b>.

The following highlights some improved features and fixed bugs in this release.

<!--truncate-->

## Implement AutoTopicCreation by namespace level override

Introduce a new namespace policy `autoTopicCreationOverride`, which enables an override of broker `autoTopicCreation` settings on the namespace level. You can disable `autoTopicCreation` for the broker while allowing it on a specific namespace.

## Add customized deletionLag and threshold for offloading policies per namespace

Support configuring `deletionLag` and threshold in the offloading policy on the namespace level to remove data from the offloaded tiered storage.

## Invalidate managed ledgers ZooKeeper cache instead of reloading on watcher triggered

The ZooKeeper children cache is reloaded for z-nodes when topics are frequently created or deleted. This creates additional load on the ZooKeeper and the broker, slows down brokers and makes them less stable. In this release, `ZooKeeperManagedLedgerCache` is introduced to invalidate instead of reloading the ZooKeeper cache, when topics are created or deleted. This helps reduce pressures on the ZooKeeper.

## Respect retention policy when there is no traffic

In previous releases, retention is checked when the ledger rollover happens. So if the traffic is stopped, the ledgers are not cleaned up even if all the messages are already acknowledged. In Pulsar 2.5.2, `retentionCheckIntervalInSeconds` is introduced to check if consumed ledgers need to be trimmed between intervals. If the value is set to 0 or a negative number, the system does not check the consumed ledgers.

## Bump Netty version to 4.1.48.Final

The ZlibDecoders in Netty 4.1.x (before 4.1.46) allow for unbounded memory allocation while decoding a ZlibEncoded byte stream. An attacker could send a large ZlibEncoded byte stream to the Netty server, forcing the server to allocate all of its free memory to a single decoder. The bug is fixed in Netty `4.1.48.Final` .

## Increase timeout for loading topics

Loading replicated topics is quite an expensive operation and involves global ZooKeeper lookups and the start of many sub-processes. In Pulsar 2.5.2, we increase the timeout for loading topics which have many replicated clusters to 60 seconds.

## Fix incorrect cursor state for cursor without consumers

If consumers of a subscription are closed, the cursor is set to inactive. But the cursor is set to active during `PulsarStats.updateStats()` when the backlog size is less than `backloggedCursorThresholdEntries`. In Pulsar 2.5.2, we move the `checkBackloggedCursors()` from `ManagedLedger` to `Topic` and check the consumer list to fix this bug.

## Change non-durable cursor to active to improve performance

In non-durable subscription mode, the cursor is not active, which leads to the written entries not being put into cache. This would degrade the reading performance. In Pulsar 2.5.2, we set the `NonDurableCursorImpl` to active and remove three override methods `setActive()`, `isActive()`, `setInactive()` to improve the reading performance.

## Add keystore configurations to TLS

In Pulsar 2.5.2, we add keystore configurations to the TLS to allow users to define their own CA certificates while the internal communication uses an internal CA certificate. This change keeps the original TLS settings untouched, and adds new configurations in needed paths.

## Close producer when the topic does not exists

In previous releases, when we create a producer for a non-existent topic, the `ProducerImpl` object is hanging in the dump. This leads to OOM in micro-service which by mistake tries to produce consistently to a non-existent topic. In Pulsar 2.5.2, we fix the bug in the following two aspects:

- Fix the exception handle for a non-existent topic.
- Change state to `Close` when the producer gets the `TopicDoesNotExists` exception.

## Fix `topicPublishRateLimiter` not effective after restarting broker

In previous releases, when a publishing rate is configured on the namespace, it can limit the publishing rate. But when the broker is restarted, the limit expires. In Pulsar 2.5.2, this bug is fixed.

## Expose pulsar_out_bytes_total and pulsar_out_messages_total for namespace/subscription/consumer

Add pulsar_out_bytes_total and pulsar_out_messages_total for the namespace, subscription, and consumer. This helps to avoid missing the rate to be computed in Prometheus or missing change of rates within the scraping interval.

## Fix `ttlDurationDefaultInSeconds` policy

The TTL for namespaces should be retrieved from the broker configuration if it is not configured at namespace policies. In previous releases, the code only returns the value stored in namespace policies directly without judging if the TTL is configured or not. In Pulsar 2.5.2, we add a condition to test if TTL is configured at namespace policies. If not, the broker retrieves value stored in broker configuration and returns it as the output.

## Fix long field parse in GenricJsonRecord

For messages sent in JSON schema, the long field is decoded as int if its value is smaller than `Integer.MAX_VALUE`. Otherwise, the long field is decoded as a string. Pulsar 2.5.2 introduces a field type check in GenericJsonRecord to fix this bug.

## Fix the leak of cursor reset if message encode fails in Avro schema

If the Avro encode for a message fails after a few bytes are written, the cursor in the stream is not reset. The following `flush()`, which normally resets the cursor, is skipped if there is an exception. In Pulsar 2.5.2, we introduced a `flush()` in the finally block to fix this bug.

## Update topic partitions automatically

In Pulsar 2.5.2, the C++ client supports previously-created producers and consumers to automatically update partitions when the partitions for a topic are updated.

- Add a `boost::asio::deadline_timer` to `PartitionedConsumerImpl` and `PartitionedProducerImpl` to register a lookup task to detect partition changes periodically.
- Add an unsigned int configuration parameter to indicate the period of detecting partition changes.
- Unlock the `mutex_` in `PartitionedConsumerImpl::receive` after `state_` were checked.

## Fix default message ID in sent callback

In previous releases, the `MessageId` in the callback is always the default value (`-1, -1, -1, -1`). In Pulsar 2.5.2, we remove the useless field `messageId` of `BatchMessageContainer::MessageContainer` and add the `const MessageId&` argument to `batchMessageCallBack`. Therefore, we can get the correct message ID in the callback if the message is sent successfully.

## Fix message ID error if messages are sent to partitioned topics

If messages are sent to a partitioned topic, the `partition` field of the message ID is always set to -1 because the `SendReceipt` command only contains the ledger ID and the entry ID. In Pulsar 2.5.2, we fix this bug by adding a `partition` field to `ProducerImpl` and setting the `partition` field of the message ID with it in the `ackReceived` method.

## Support Async mode for Pulsar Functions

In previous releases, Pulsar Functions does not support the Async mode, such as the user passed in a Function in the following format:

```

Function<I, CompletableFuture<O>>

```

This kind of function is useful if the Pulsar Functions use RPCs to call external systems. Therefore, in Pulsar 2.5.2, we introduce Async mode support for Pulsar Functions.


## Fix localrunner netty dependency issue

In Pulsar 2.5.2, we add a Log4j2 configuration file for pulsar-functions-local-runner to log to console by default. This helps troubleshoot the problem that Netty libraries are missing and the class is not found, when pulling in pulsar-functions-local-runner as a dependency and attempting to run Pulsar Functions locally.

## Fix SerDe validation of Pulsar Functions update

In previous releases, the `outputSchemaType` field is improperly used to validate parameters for Pulsar Function updates. In fact, the `outputSerdeClassName` parameter should be used. In Pulsar 2.5.2, we fix this bug.

## Avoid pre-fetching too much data when offloading data to HDFS

If too much data is pre-fetched when data is offloaded to HDFS, it may cause severe OOM. In Pulsar 2.5.2, the `managedLedgerOffloadPrefetchRounds` is introduced, which is used to set the maximum pre-fetch rounds for ledger reading for offloading data.

## JDBC sink handles null fields in schema

JDBC sink does not handle `null` fields. The schema registered in Pulsar allows for it and the table schema in MySQL has a column of the same name. When messages are sent to the JDBC sink without that field, an exception is thrown. In Pulsar 2.5.2, the JDBC sink uses the `setColumnNull` method to properly reflect the null field value in the database row.

## Reference

To download Apache Pulsar 2.5.2, click [here](https://pulsar.apache.org/en/download/).

If you have any questions or suggestions, contact us with mailing lists or slack.

- [users@pulsar.apache.org](mailto:users@pulsar.apache.org)
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)
- Pulsar slack channel: https://apache-pulsar.slack.com/
- Self-registration at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Pulsar](https://github.com/apache/pulsar).
