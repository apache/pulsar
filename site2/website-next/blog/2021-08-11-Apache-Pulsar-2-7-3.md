---
author: Bo Cong, Anonymitaet
title: Apache Pulsar 2.7.3
---

# What’s New in Apache Pulsar 2.7.3

The Apache Pulsar community releases version 2.7.3! 34 contributors provided improvements and bug fixes that delivered 79 commits.

## Highlights

- Cursor reads adhere to the dispatch byte rate limiter setting and no longer cause unexpected results. [PR-11249](https://github.com/apache/pulsar/pull/11249)

- The ledger rollover scheduled task runs as expected. [PR-11226](https://github.com/apache/pulsar/pull/11226)

This blog walks through the most noteworthy changes. For the complete list including all enhancements and bug fixes, check out the [Pulsar 2.7.3 Release Notes](https://pulsar.apache.org/release-notes/#273-mdash-2021-07-27-a-id273a).

# Notable bug fixes and enhancements

## Broker

### Cursor reads adhere to the dispatch byte rate limiter setting. [PR-9826](https://github.com/apache/pulsar/pull/9826)

- **Issue**: When using byte rates, the dispatch rates were not respected (regardless
of being a namespace or topic policy). 

- **Resolution**: Fixed behavior of dispatch byte rate limiter setting. Cursor reads adhere to the setting and no longer cause unexpected results. 

### The ledger rollover scheduled task runs as expected. [PR-11226](https://github.com/apache/pulsar/pull/11226)

- **Issue**: Previously, the ledger rollover scheduled task was executed before reaching the ledger maximum rollover time, which caused the ledger not to roll over in time. 

- **Resolution**: Fixed the timing of the ledger rollover schedule, so the task runs only after the ledger is created successfully. 
  
### The topic-level retention policy works correctly when restarting a broker. [PR-11136](https://github.com/apache/pulsar/pull/11136)

- **Issue**: Previously, when setting a topic-level retention policy for a topic and then restarting the broker, the topic-level retention policy did not work.

- **Resolution**: Fixed behavior of the policy so it replays all policy messages after initiating `policyCacheInitMap` and added a retention policy check test when restarting the broker.

### The lastMessageId API call no longer causes a memory leak. [PR-10977](https://github.com/apache/pulsar/pull/10977)

- **Issue**: Previously, there was a memory leak when calling the `lastMessageId` API, which caused the broker process to be stopped by Kubernetes. 

- **Resolution**: Added the missing entry.release() call to PersistentTopic.getLastMessageId to ensure the broker does not run out of memory. 

### ZooKeeper reads are cached by brokers. [PR-10594](https://github.com/apache/pulsar/pull/10594)

- **Issue**: When performing the admin operation to get the namespace of a tenant, ZooKeeper reads were issued on the ZooKeeper client and not getting cached by the brokers.

- **Resolution**: Fixed ZooKeeper caching when fetching a list of namespaces for a tenant.

### Monitoring threads that call `LeaderService.isLeader()` are no longer blocked. [PR-10512](https://github.com/apache/pulsar/pull/10512)

- **Issue**:  When `LeaderService` changed leadership status, it was locked with a `synchronized` block, which also blocked other threads calling `LeaderService.isLeader()`. 
  
- **Resolution**: Fixed the deadlock condition on the monitoring thread so it is not blocked by `LeaderService.isLeader() by modifying `ClusterServiceCoordinator` and `WorkerStatsManager` to check if it is a leader from `MembershipManager`. 

### `hasMessageAvailable` can read messages successfully. [PR-10414](https://github.com/apache/pulsar/pull/10414)

- **Issue**: When `hasMessageAvailableAsync` returned `true`, it could not read messages because messages were filtered by `acknowledgmentsGroupingTracker`. 

- **Resolution**: Fixed the race conditions by modifying `acknowledgmentsGroupingTracker` to filter duplicate messages, and then cleanup the messages when the connection is open.
  
## Proxy

### Proxy supports creating partitioned topics automatically. [PR-8048](https://github.com/apache/pulsar/pull/8048)

- **Issue**: Proxies were not creating partitions because they were using the current ZooKeeper metadata.

- **Resolution**: Changed the proxy to handle `PartitionMetadataRequest` by selecting and fetching from an available broker instead of using current ZooKeeper metadata.
  
## Pulsar admin

### Flag added to indicate whether or not to create a metadata path on replicated clusters. [PR-11140](https://github.com/apache/pulsar/pull/11140)

- **Issue**: When creating a partitioned topic in a replicated namespace, it did not
create a metadata path `/managed-ledgers` on replicated clusters.

- **Resolution**: Added a flag (createLocalTopicOnly) to indicate whether or not to create a metadata path for a partitioned topic in replicated clusters.

### A topic policy can no longer be set for a non-existent topic. [PR-11131](https://github.com/apache/pulsar/pull/11131)

- **Issue**: Due to a redirect loop in a topic policy, you can set a policy for a non-existing topic or a partition of a partitioned topic. 

- **Resolution**: The fix added an authoritative flag for a topic policy to avoid a redirect loop. You can not set a topic policy for a non-existent topic or a partition of a partitioned topic. If you set a topic policy for a partition of a 0-partition topic, it redirects to the broker. 

### Discovery service no longer hard codes the topic domain as persistent. [PR-10806](https://github.com/apache/pulsar/pull/10806)

- **Issue**: When using the lookup discovery service for a partitioned non-persistent topic, it returned zero rather than the number of partitions. The Pulsar client tried to connect to the topic as if it were a normal topic.

- **Resolution**: Implemented `topicName.getDomain().value()` rather than hard coding `persistent.` Now you can use the discovery service for a partitioned, non-persistent topic successfully.

### Other connectors can now use the Kinesis `Backoff` class. [PR-10744](https://github.com/apache/pulsar/pull/10744)

- **Issue**: The Kinesis sink connector `Backoff` class in the Pulsar client implementation project in combination  with the dependency `org.apache.pulsar:pulsar-client-original` increased the connector size. 

- **Resolution**: Added a new class `Backoff` in the function io-core project so that the Kinesis sink connector and other connectors can use the class.

## Client

### A `FLOW` request with zero permits can not be sent. [PR-10506](https://github.com/apache/pulsar/pull/10506)

- **Issue**: When a broker received a `FLOW` request with zero permits, an exception was thrown and then the connection was closed. This triggered frequent reconnections and caused duplicate or out-of-order messages. 
  
- **Resolution**: Added a validation that verifies the permits of a `FLOW` request before sending it. If the permit is zero, the `FLOW` request can not be sent.

## Function and connector

### The Kinesis sink connector acknowledges successful messages. [PR-10769](https://github.com/apache/pulsar/pull/10769)

- **Issue**: The Kinesis sink connector did not acknowledge messages after they were sent successfully. 
  
- **Resolution**: Added acknowledgement for the Kinesis sink connector once a message is sent successfully. 
 
## Docker

### Function name length cannot exceed 52 characters when using Kubernetes runtime. [PR-10531](https://github.com/apache/pulsar/pull/10531)

- **Issue**: When using Kubernetes runtime, if a function was submitted with a valid length (less than 55 characters), a StatefulSet was created but it was unable to spawn pods. 
  
- **Resolution**: Changed the maximum length of a function name from 55 to 53 characters for Kubernetes runtime. With this fix, the length of a function name can not exceed 52 characters. 

## Dependency 

### `pulsar-admin` connection to proxy is stable when TLS is enabled. [PR-10907](https://github.com/apache/pulsar/pull/10907)

- **Issue**: `pulsar-admin` was unstable over the TLS connection because of the Jetty bug in SSL buffering introduced in Jetty 9.4.39. It caused large function jar uploads to fail frequently.
  
- **Resolution**: Upgraded Jetty to 9.4.42.v20210604, so that `pulsar-admin` connection to proxy is stable when TLS is enabled.

# What is Next?

If you are interested in learning more about Pulsar 2.7.3, you can [download 2.7.3](https://pulsar.apache.org/en/versions/) and try it out now! 

The first-ever Pulsar Virtual Summit Europe 2021 will take place in October. [Register now](https://hopin.com/events/pulsar-summit-europe-2021) and help us make it an even bigger success by spreading the word on social!

For more information about the Apache Pulsar project and the progress, visit
the [Pulsar website](https://pulsar.apache.org), follow the project on Twitter [@apache_pulsar](https://twitter.com/apache_pulsar), and join [Pulsar Slack](https://apache-pulsar.herokuapp.com/)!
