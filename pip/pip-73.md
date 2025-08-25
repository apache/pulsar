# PIP-73: Configurable data source priority for message reading

- Status: Proposal
- Author: [@Renkai](https://github.com/Renkai)
- Pull Request: https://github.com/apache/pulsar/pull/8717
- Mailing List discussion:
- Release:

# Motivation

Pulsar supports offload data into the tiered storage and usually, the offloaded data will be deleted from the BookKeeper, this will effectively reduce the cost of data storage. But in some cases, users still want to maintain the offloaded data in the BookKeeper cluster for a period of time. 

Currently, if the data offloaded to the tiered storage, the broker will always read data from the tiered storage. This also means the data in the BookKeeper cluster will be used for message reading. So this proposal aims to support configure the data source priority when reading data from the storage layer.

Configurable data source priority might be useful in the below scenarios:

Bookkeeper and second layer storage have different performance characteristics, users may prefer low response time or high throughput, and they can choose performance characteristics by choosing different read layers.
Users may want to keep multiple copies active to achieve extreme performance, for example, users can let business consumers read from BookKeeper, let Flink/Spark/Presto analysis tasks to read from the second storage layer to have a high throughput while business consumers still get low latency.
Pulsar administrators may set the offload lag very low(for example several seconds) so that data in the second layer storage can be available ASAP, but the consumers of corresponding content may still want to read data from BookKeeper to keep their read process stable (not switch between BookKeeper and the second layer storage too frequently).

The options should be enough for now:

- `bookkeeper-first`
- `tiered-first`

# Changes

The tiered read priority can be modified at the `broker.conf` and the namespace, topic policy. 

Broker level configuration:

```
managedLedgerDataReadPriority=tiered-first
```

Namespace level policy:

```java
pulsar-admin namespaces --set-data-read-priority tiered-first  tenant/namespace 
```

Topic level policy:

```java
pulsar-admin topics --set-data-read-priority bookkeeper-first tenant/namespace/topic
```

# Implementation

Implementation of org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl will be changed, ManagedLedgerImpl will choose different ledgers handler by configuration, the change will be totally in brokers, transparent for clients.
# Compatibility

If the data read priority is absent, `tiered-first` is used by default, which will result in the same behavior as the current implementation, with no compatibility issues.

If using the `bookkeeper-first` as the data read priority policy but the BookKeeper does not have the corresponding data. The broker will continue to read data from the tiered storage to ensure data integrity.

# Test Plan

How to verify the data is read from the BookKeeper or Tiered Storage?

Compatibility test: 
Existing tests should not be changed. By default brokers behavior should be exactly the same as before(read from second layer storage as soon as the ledger offloaded), which is covered by existing unit tests.
For the newly created option `bookkeeper-first`, we should ensure brokers read from bookkeeper as long as ledgers were not deleted from bookkeeper, if the ledgers were deleted from bookkeeper, brokers should read from second layer storage directly without trigger non-exists exceptions from bookkeeper.
