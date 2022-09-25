---
title: "What’s New in Apache Pulsar 2.7.5"
date: 2022-09-09
author: "Jason918, momo-jun"
---

The Apache Pulsar community releases version 2.7.5! 23 contributors provided improvements and bug fixes that delivered 89 commits. Thanks for all your contributions.

The highlight of the 2.7.5 release is that it fixes some critical bugs on broker, proxy, and storage, including message/data loss, broker deadlock, and connection leak. Note that 2.7.5 is the last release of 2.7.x.

This blog walks through the most noteworthy changes. For the complete list, including all feature enhancements and bug fixes, check out the [Pulsar 2.7.5 Release Notes](https://pulsar.apache.org/release-notes/versioned/pulsar-2.7.5/).

### Fixed the deadlock on metadata cache missing while checking replications. [PR-16889](https://github.com/apache/pulsar/pull/16889)

#### Issue
After the changes in [#12340](https://github.com/apache/pulsar/pull/12340), there are still a couple of places making blocking calls. These calls occupy all the ordered scheduler threads preventing the callbacks from completing until the 30 seconds timeout expires.

#### Resolution
Change the blocking calls to async mode on the metadata callback thread.


### Fixed the deadlock when using the key_shared mode. [PR-11965](https://github.com/apache/pulsar/pull/11965)

#### Issue
When the key_shared mode is used in consumers, deadlock may happen in the broker due to some race conditions and result in a lot of `CLOSE_WAIT` status connections.

#### Resolution
Change unlock before the callback in the `asyncDelete` function of `ManagedCursorImpl`.

### Fixed the message loss issue due to ledger rollover. [PR-14703](https://github.com/apache/pulsar/pull/14703)

#### Issue
If users config `managedLedgerMaxLedgerRolloverTimeMinutes > 0`, and the rollover happens when the ManagedLedger state is `CreatingLedger`, the messages written during that time are lost.

#### Resolution
Rollover only when the ledger state is `LedgerOpened`. 
 
### Fixed the port exhaustion and connection issues in Pulsar Proxy. [PR-14078](https://github.com/apache/pulsar/pull/14078)

#### Issue
Pulsar Proxy can get into a state where it stops proxying Broker connections while Admin API proxying keeps working.

#### Resolution
Optimize the proxy connection to fail-fast when the target broker isn't active.
Fix the race conditions in Pulsar Proxy when establishing a connection, leading to invalid states and hanging connections.
Add connection timeout handling to proxy connections. 
Add read timeout handling to incoming connections and proxied connections.


### Fixed the compaction data loss due to missed compaction properties during cursor reset. [PR-16404](https://github.com/apache/pulsar/pull/16404)

#### Issue
The compaction reader seeks the earliest position to read data from the topic, but the compaction properties are missed during cursor reset, which leads to the initialized compaction subscribe without a compaction horizon, so the compaction reader skips the last compacted data. It only happens when initializing the compaction subscription and can be introduced by the load balance or topic unloading manually.

#### Resolution
Keep the properties for resetting the cursor while the cursor is for data compaction.
Copy the properties to the new mark delete entry while advancing the cursor, which is triggered by the managed ledger internal. It's not only for the compacted topic, and the internal task should not lose the properties when trimming the cursor.

# What’s Next?

If you are interested in learning more about Pulsar 2.7.5, you can [download](https://pulsar.apache.org/en/versions/) and try it out now! 

For more information about the Apache Pulsar project and current progress, visit the [Pulsar website](https://pulsar.apache.org), follow the project on Twitter [@apache_pulsar](https://twitter.com/apache_pulsar), and join [Pulsar Slack](https://apache-pulsar.herokuapp.com/)!