# PIP-79: Reduce redundant producers from partitioned producer

* **Status**: Implemented
* **Author**: [Yuri Mizushima](https://github.com/equanz)
* **Pull Request**:
  * Java Client: https://github.com/apache/pulsar/pull/10279
  * C++ Client: https://github.com/apache/pulsar/pull/11570
  * Stats: https://github.com/apache/pulsar/pull/12401
* **Mailing List discussion**: https://mail-archives.apache.org/mod_mbox/pulsar-dev/202102.mbox/%3c5490ED12-C172-4AD6-8E77-2E70C66C9793@yahoo-corp.jp%3e
* **Release**:

### Motivation
When producer connects to partitioned topic, sometimes doesn't need to connect to all partitions depending on rate, routing mode, etc.
Some cases as below.
1. different rate producers
   - we should set number of partitions according to the high-rate producer
   - sometimes low-rate producer doesn't need to connect to all partitions
2. SinglePartition routing mode
   - each producer uses only one partition

In this PIP, we reduce the number of producers to use system resources (e.g. Broker memory) more efficiently.
Here is an image of concept (second one is new concept). As you can see, each partitioned producer connects to part of partitions and the number of producers is less than or equal to 24.

![limit_num_of_partitions_diff_v2](https://user-images.githubusercontent.com/22067228/106077321-b19dfb80-6154-11eb-8e5c-d3dddee0b0b5.png)

Also, this change allows us to scale producers by increasing partitions even where limit number of partition config is set.
For example, suppose system has producer limit. Currently, if topic reaches to its limit, then we have to create new topics or increase limit config. But suppose all producers connect to system with using above features, we can avoid producers from connecting to all partitions and thus scale producers by only increasing partitions (without increasing the max producer limit or creating new topics).

### Proposed Changes
#### Partitioned producer connects to partitions lazily
Currently, partitioned producer always creates producer instances for all partitions at initialization time.
However, as we mentioned before, there are some situations where producer creation for all partitions is unnecessary and excessive.
Therefore, we should modify this behavior to reduce such redundant producer creation.

The idea is "lazy-loading", i.e., producer creation for each partition is delayed until message router elects the partition index at sending time.
This feature allows us to reduce redundant producers, in particular in SinglePartition routing mode.

When client creates partitioned producer, partitioned producer will elect first partition index and try to create its producer. Only if first partition's producer is created correctly then handler state will be `Ready` and client will return partitioned producer.
When message router elects partition index and its producer isn't created yet, then partitioned producer will create its producer each time.
If partitioned producer can't create producer at like above, then partitioned producer will throw exception at runtime.

#### Add round-robin with limit number of partitions routing mode
As described above lazy-load feature is helpful in SinglePartition routing mode, however, it doesn't reduce the number of producers in RoundRobin routing mode since it uses all partitions.
Therefore, we would like to add custom routing mode class which returns partition index as round-robin with limit number of partitions.

This mode has sub list of all partition index and elects partition index from the list by round-robin.
Sample of constructor interface like below.
```
new PartialRoundRobin(int numPartitionsLimit)
```

#### Change PartitionedTopicStats about producer
Currently, if each partition has different producers like above, then PartitionedTopicStats will be incorrect.
We would like to change logic to collect PartitionedTopicStats.

This change allows us to get PartitionedTopicStats correctly not only when producers connect to all partitions, but also only part of partitions.

### Compatibility, Deprecation, and Migration Plan
- What impact (if any) will there be on existing users?
  - producer creation behavior and error handling at partitioned producer will be changed
  - can get PartitionedTopicStats correctly not only when producers connect to all partitions, but also only part of partitions
- If we are changing behavior how will we phase out the older behavior?
  - no phase out needed
- If we need special migration tools, describe them here.
  - don't need
- When will we remove the existing behavior?
  - don't remove any behavior

### Test Plan
We would like to validate these features by unit and E2E test code.

### Rejected Alternatives
* instead of implementing custom routing mode
  - create additional routing mode like SinglePartition, RoundRobin
    - also add additional config interface for routing mode (e.g. limit num of partition, or config params for each routing mode)
      - currently, other routing mode don't need any additional config
      - config interface is redundant for other routing mode
