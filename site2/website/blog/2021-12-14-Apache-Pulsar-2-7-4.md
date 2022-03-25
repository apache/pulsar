---
author: Technoboy-, Anonymitaet
title: What’s New in Apache Pulsar 2.7.4
---

The Apache Pulsar community releases version 2.7.4! 32 contributors provided improvements and bug fixes that delivered 98 commits.

Highlights of this release are as below:

- Upgrade Log4j to 2.17.0 - [CVE-2021-45105](https://pulsar.apache.org/blog/2021/12/11/Log4j-CVE/). [PR-13392](https://github.com/apache/pulsar/pull/13392)
  
- `ManagedLedger` can be referenced correctly when `OpAddEntry` is recycled. [PR-12103](https://github.com/apache/pulsar/pull/12103)

- NPE does not occur on `OpAddEntry` while ManagedLedger is closing. [PR-12364](https://github.com/apache/pulsar/pull/12364)

This blog walks through the most noteworthy changes grouped by the affected functionalities. For the complete list including all enhancements and bug fixes, check out the [Pulsar 2.7.4 Release Notes](https://pulsar.apache.org/en/release-notes/#274).

# Notable bug fixes and enhancements

### Upgrade Log4j to 2.17.0 - [CVE-2021-45105](https://pulsar.apache.org/blog/2021/12/11/Log4j-CVE/). [PR-13392](https://github.com/apache/pulsar/pull/13392)

- **Issue**

    A serious vulnerability was reported regarding Log4j that can allow remote execution for attackers. The vulnerability issue is described and tracked under [CVE-2021-44228](https://nvd.nist.gov/vuln/detail/CVE-2021-44228).

- **Resolution**
  
  Pulsar 2.7.4 upgraded Log4j to 2.17.0.

### `ManagedLedger` can be referenced correctly when `OpAddEntry` is recycled. [PR-12103](https://github.com/apache/pulsar/pull/12103)

- **Issue**
  
    Previously, after a write failure, a task was scheduled in the background to force close the ledger and trigger the creation of  a new ledger. If the `OpAddEntry` instance was already recycled, that could lead to either an NPE or undefined behavior.

- **Resolution**
  
    The `ManagedLedgerImpl` object reference is copied to a final variable so the background task will not be dependent on the lifecycle of the `OpAddEntry` instance.

### No potential race condition in the `BlobStoreBackedReadHandler`. [PR-12123](https://github.com/apache/pulsar/pull/12123)

- **Issue**

    Previously, `BlobStoreBackedReadHandler` entered an infinite loop when reading an offload ledger. There was a race condition between the operation of reading entries and closing BlobStoreBackedReadHandler.

- **Resolution**
  
    Added a state check before reading entries and made the `BlobStoreBackedReadHandler` exit loop when the `entryID` is bigger than the `lastEntryID`.

### NPE does not occur on `OpAddEntry` while ManagedLedger is closing. [PR-12364](https://github.com/apache/pulsar/pull/12364)

- **Issue** 

    Previously, the test `ManagedLedgerBkTest#managedLedgerClosed` closed ManagedLedger object on some `asyncAddEntry` operations and failed with NPE.
	
- **Resolution**

    Closed `OpAddEntry`  when ` ManagedLedger` signaled  `OpAddEntry` to fail. In this way, the `OpAddEntry` object was correctly recycled and the failed callback was correctly triggered.

### Set a topic policy through the topic name of a partition correctly. [PR-11294](https://github.com/apache/pulsar/pull/11294)

- **Issue**

    Previously, the topic name of a partition could not be used to set a topic policy.

- **Resolution**

    Allowed setting a topic policy through a topic name of a partition by converting the topic name of a partition in `SystemTopicBasedTopicPoliciesService`.

### Dispatch rate limiter takes effect for consumers. [PR-8611](https://github.com/apache/pulsar/pull/8611)

- **Issue**

    Previously, dispatch rate limiter did not take effect in cases where all consumers started reading in the next second since `acquiredPermits` was reset to 0 every second.

- **Resolution**
    
    Changed the behaviour of `DispatchRateLimiter` by minus `permits` every second instead of reset `acquiredPermits` to 0. Consumers stopped reading entries temporarily until `acquiredPermits` returned to a value less than `permits` .

### NPE does not occur when executing unload bundles operations. [PR-11310](https://github.com/apache/pulsar/pull/11310)

- **Issue**
  
    When performing pressure tests on persistent partitioned topics, NPE occurred when executing unload bundles operations. Concurrently, producers did not write messages.

- **Resolution**
  
    Added more safety checks to fix this issue.

### Fix inconsistent behavior for Namespace bundles cache. [PR-11346](https://github.com/apache/pulsar/pull/11346)

- **Issue**
  
    Previously, namespace bundle cache was not invalidated after a namespace was deleted.

- **Resolution**

    Invalidated namespace policy cache when bundle cache was invalidated.

### Close the replicator and replication client after a cluster is deleted. [PR-11342](https://github.com/apache/pulsar/pull/11342)

- **Issue**
  
    Previously, the replicator and the replication client were not closed after a cluster was deleted. The producer of the replicator would then try to reconnect to the deleted cluster continuously.

- **Resolution**
  
    Closed the relative replicator and replication client.

### Publish rate limiter takes effect as expected. [PR-10384](https://github.com/apache/pulsar/pull/10384)

- **Issue**
  
    Previously, there were various issues if `preciseTopicPublishRateLimiterEnable`  was set to `true` for rate limiting:

    - Updating the limits did not set a boundary when changing the limits from a bounded limit to an unbounded limit.
    
    - Each topic created a scheduler thread for each limiter instance.
    
    - Topics did not release the scheduler thread when the topic was unloaded or the operation closed.
    
    - Updating the limits did not close the scheduler thread related to the replaced limiter instance

- **Resolution**
  
  - Cleaned up the previous limiter instances before creating new limiter instances.

  - Used `brokerService.pulsar().getExecutor()` as the scheduler for the rate limiter instances.

  - Added resource cleanup hooks for topic closing (unload).

### Clean up newly created  ledgers if fails to update ZNode list. [PR-12015](https://github.com/apache/pulsar/pull/12015)

- **Issue**
  
    When updating a ZNode list, ZooKeeper threw an exception and did not clean up the created ledger. Newly created ledgers were not  indexed to a topic `managedLedger` list and could not be cleared up as topic retention. Also, ZNode numbers increased in ZooKeeper if the ZNode version mismatch exception was thrown out.

- **Resolution**
  
    Deleted the created ledger from broker cache and BookKeeper regardless of exception type when the ZNode list failed to update.

# What’s Next?

If you are interested in learning more about Pulsar 2.7.4, you can [download](https://pulsar.apache.org/en/versions/) and try it out now! 

Pulsar Summit Asia 2021 will take place on January 15-16, 2022. [Register now](https://pulsar-summit.org/) and help us make it an even bigger success by spreading the word on social media!

For more information about the Apache Pulsar project and current  progress, visit
the [Pulsar website](https://pulsar.apache.org), follow the project on Twitter
[@apache_pulsar](https://twitter.com/apache_pulsar), and join [Pulsar Slack](https://apache-pulsar.herokuapp.com/)!
