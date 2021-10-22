---
id: version-2.6.4-reference-metrics
title: Pulsar Metrics
sidebar_label: Pulsar Metrics
original_id: reference-metrics
---

<style type="text/css">
  table{
    font-size: 80%;
  }
</style>

Pulsar exposes metrics in Prometheus format that can be collected and used for monitoring the health of the cluster.

* [ZooKeeper](#zookeeper)
* [BookKeeper](#bookkeeper)
* [Broker](#broker)
* [Pulsar Functions](#pulsar functions)
* [Proxy](#proxy)
* [Pulsar SQL Worker](#Pulsar SQL Worker)

## Overview

The metrics exposed by Pulsar are in Prometheus format. The types of metrics are:

- [Counter](https://prometheus.io/docs/concepts/metric_types/#counter): a cumulative metric that represents a single monotonically increasing counter whose value can only increase or be reset to zero on restart.
- [Gauge](https://prometheus.io/docs/concepts/metric_types/#gauge): a *gauge* is a metric that represents a single numerical value that can arbitrarily go up and down.
- [Histogram](https://prometheus.io/docs/concepts/metric_types/#histogram): a histogram samples observations (usually things like request durations or response sizes) and counts them in configurable buckets. The `_bucket` suffix is the number of observations within a histogram bucket, configured with parameter `{le="<upper inclusive bound>"}`. The `_count` suffix is the number of observations, shown as a time series and behaves like a counter. The `_sum` suffix is the sum of observed values, also shown as a time series and behaves like a counter. These suffixes are together denoted by `_*` in this doc.
- [Summary](https://prometheus.io/docs/concepts/metric_types/#summary): similar to a histogram, a summary samples observations (usually things like request durations and response sizes). While it also provides a total count of observations and a sum of all observed values, it calculates configurable quantiles over a sliding time window.

## ZooKeeper

The ZooKeeper metrics are exposed under "/metrics" at port 8000. You can use a different port
by configuring the `stats_server_port` system property.

### Server metrics

| Name | Type | Description |
|---|---|---|
| zookeeper_server_znode_count | Gauge | The number of z-nodes stored. |
| zookeeper_server_data_size_bytes | Gauge | The total size of all of z-nodes stored. |
| zookeeper_server_connections | Gauge | The number of currently opened connections. |
| zookeeper_server_watches_count | Gauge | The number of watchers registered. |
| zookeeper_server_ephemerals_count | Gauge | The number of ephemeral z-nodes. |

### Request metrics

| Name | Type | Description |
|---|---|---|
| zookeeper_server_requests | Counter | The total number of requests received by a particular server. |
| zookeeper_server_requests_latency_ms | Summary | The requests latency calculated in milliseconds. <br> Available labels: *type* (write, read). <br> <ul><li>*write*: the requests that write data to ZooKeeper.</li><li>*read*: the requests that read data from ZooKeeper.</li></ul>|

## BookKeeper

The BookKeeper metrics are exposed under "/metrics" at port 8000. You can change the port by updating `prometheusStatsHttpPort`
in `bookkeeper.conf` configuration file.

### Server metrics

| Name | Type | Description |
|---|---|---|
| bookie_SERVER_STATUS | Gauge | The server status for bookie server. <br><ul><li>1: the bookie is running in writable mode.</li><li>0: the bookie is running in readonly mode.</li></ul> |
| bookkeeper_server_ADD_ENTRY_count | Counter | The total number of ADD_ENTRY requests received at the bookie. The `success` label is used to distinguish successes and failures. |
| bookkeeper_server_READ_ENTRY_count | Counter | The total number of READ_ENTRY requests received at the bookie. The `success` label is used to distinguish successes and failures. |
| bookie_WRITE_BYTES | Counter | The total number of bytes written to the bookie. |
| bookie_READ_BYTES | Counter | The total number of bytes read from the bookie. |
| bookkeeper_server_ADD_ENTRY_REQUEST | Histogram | The histogram of request latency of ADD_ENTRY requests at the bookie. The `success` label is used to distinguish successes and failures. |
| bookkeeper_server_READ_ENTRY_REQUEST | Histogram | The histogram of request latency of READ_ENTRY requests at the bookie. The `success` label is used to distinguish successes and failures. |

### Journal metrics

| Name | Type | Description |
|---|---|---|
| bookie_journal_JOURNAL_SYNC_count | Counter | The total number of journal fsync operations happening at the bookie. The `success` label is used to distinguish successes and failures. |
| bookie_journal_JOURNAL_QUEUE_SIZE | Gauge | The total number of requests pending in the journal queue. |
| bookie_journal_JOURNAL_FORCE_WRITE_QUEUE_SIZE | Gauge | The total number of force write (fsync) requests pending in the force-write queue. |
| bookie_journal_JOURNAL_CB_QUEUE_SIZE | Gauge | The total number of callbacks pending in the callback queue. |
| bookie_journal_JOURNAL_ADD_ENTRY | Histogram | The histogram of request latency of adding entries to the journal. |
| bookie_journal_JOURNAL_SYNC | Histogram | The histogram of fsync latency of syncing data to the journal disk. |

### Storage metrics

| Name | Type | Description |
|---|---|---|
| bookie_ledgers_count | Gauge | The total number of ledgers stored in the bookie. |
| bookie_entries_count | Gauge | The total number of entries stored in the bookie. |
| bookie_write_cache_size | Gauge | The bookie write cache size (in bytes). |
| bookie_read_cache_size | Gauge | The bookie read cache size (in bytes). |
| bookie_DELETED_LEDGER_COUNT | Counter | The total number of ledgers deleted since the bookie has started. |
| bookie_ledger_writable_dirs | Gauge | The number of writable directories in the bookie. |

## Broker

The broker metrics are exposed under "/metrics" at port 8080. You can change the port by updating `webServicePort` to a different port
in `broker.conf` configuration file.

All the metrics exposed by a broker are labelled with `cluster=${pulsar_cluster}`. The value of `${pulsar_cluster}` is the pulsar cluster
name you configured in `broker.conf`.

Broker has the following kinds of metrics:

* [Namespace metrics](#namespace-metrics)
    * [Replication metrics](#replication-metrics)
* [Topic metrics](#topic-metrics)
    * [Replication metrics](#replication-metrics-1)
* [ManagedLedgerCache metrics](#managedledgercache-metrics)
* [ManagedLedger metrics](#managedledger-metrics)
* [LoadBalancing metrics](#loadbalancing-metrics)
    * [BundleUnloading metrics](#bundleunloading-metrics)
    * [BundleSplit metrics](#bundlesplit-metrics)
* [Subscription metrics](#subscription-metrics)
* [Consumer metrics](#consumer-metrics)
* [ManagedLedger bookie client metrics](#managed-ledger-bookie-client-metrics)
* [Jetty metrics](#jetty-metrics)

### Namespace metrics

> Namespace metrics are only exposed when `exposeTopicLevelMetricsInPrometheus` is set to `false`.

All the namespace metrics are labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you configured in `broker.conf`.
- *namespace*: `namespace=${pulsar_namespace}`. `${pulsar_namespace}` is the namespace name.

| Name | Type | Description |
|---|---|---|
| pulsar_topics_count | Gauge | The number of Pulsar topics of the namespace owned by this broker. |
| pulsar_subscriptions_count | Gauge | The number of Pulsar subscriptions of the namespace served by this broker. |
| pulsar_producers_count | Gauge | The number of active producers of the namespace connected to this broker. |
| pulsar_consumers_count | Gauge | The number of active consumers of the namespace connected to this broker. |
| pulsar_rate_in | Gauge | The total message rate of the namespace coming into this broker (messages/second). |
| pulsar_rate_out | Gauge | The total message rate of the namespace going out from this broker (messages/second). |
| pulsar_throughput_in | Gauge | The total throughput of the namespace coming into this broker (bytes/second). |
| pulsar_throughput_out | Gauge | The total throughput of the namespace going out from this broker (bytes/second). |
| pulsar_storage_size | Gauge | The total storage size of the topics in this namespace owned by this broker (bytes). |
| pulsar_storage_backlog_size | Gauge | The total backlog size of the topics of this namespace owned by this broker (messages). |
| pulsar_storage_offloaded_size | Gauge | The total amount of the data in this namespace offloaded to the tiered storage (bytes). |
| pulsar_storage_write_rate | Gauge | The total message batches (entries) written to the storage for this namespace (message batches / second). |
| pulsar_storage_read_rate | Gauge | The total message batches (entries) read from the storage for this namespace (message batches / second). |
| pulsar_subscription_delayed | Gauge | The total message batches (entries) are delayed for dispatching. |
| pulsar_storage_write_latency_le_* | Histogram | The entry rate of a namespace that the storage write latency is smaller with a given threshold.<br> Available thresholds: <br><ul><li>pulsar_storage_write_latency_le_0_5: <= 0.5ms </li><li>pulsar_storage_write_latency_le_1: <= 1ms</li><li>pulsar_storage_write_latency_le_5: <= 5ms</li><li>pulsar_storage_write_latency_le_10: <= 10ms</li><li>pulsar_storage_write_latency_le_20: <= 20ms</li><li>pulsar_storage_write_latency_le_50: <= 50ms</li><li>pulsar_storage_write_latency_le_100: <= 100ms</li><li>pulsar_storage_write_latency_le_200: <= 200ms</li><li>pulsar_storage_write_latency_le_1000: <= 1s</li><li>pulsar_storage_write_latency_le_overflow: > 1s</li></ul> |
| pulsar_entry_size_le_* | Histogram | The entry rate of a namespace that the entry size is smaller with a given threshold.<br> Available thresholds: <br><ul><li>pulsar_entry_size_le_128: <= 128 bytes </li><li>pulsar_entry_size_le_512: <= 512 bytes</li><li>pulsar_entry_size_le_1_kb: <= 1 KB</li><li>pulsar_entry_size_le_2_kb: <= 2 KB</li><li>pulsar_entry_size_le_4_kb: <= 4 KB</li><li>pulsar_entry_size_le_16_kb: <= 16 KB</li><li>pulsar_entry_size_le_100_kb: <= 100 KB</li><li>pulsar_entry_size_le_1_mb: <= 1 MB</li><li>pulsar_entry_size_le_overflow: > 1 MB</li></ul> |

#### Replication metrics

If a namespace is configured to be replicated between multiple Pulsar clusters, the corresponding replication metrics will also be exposed when `replicationMetricsEnabled` is enabled.

All the replication metrics will also be labelled with `remoteCluster=${pulsar_remote_cluster}`.

| Name | Type | Description |
|---|---|---|
| pulsar_replication_rate_in | Gauge | The total message rate of the namespace replicating from remote cluster (messages/second). |
| pulsar_replication_rate_out | Gauge | The total message rate of the namespace replicating to remote cluster (messages/second). |
| pulsar_replication_throughput_in | Gauge | The total throughput of the namespace replicating from remote cluster (bytes/second). |
| pulsar_replication_throughput_out | Gauge | The total throughput of the namespace replicating to remote cluster (bytes/second). |
| pulsar_replication_backlog | Gauge | The total backlog of the namespace replicating to remote cluster (messages). |

### Topic metrics

> Topic metrics are only exposed when `exposeTopicLevelMetricsInPrometheus` is set to true.

All the topic metrics are labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you configured in `broker.conf`.
- *namespace*: `namespace=${pulsar_namespace}`. `${pulsar_namespace}` is the namespace name.
- *topic*: `topic=${pulsar_topic}`. `${pulsar_topic}` is the topic name.

| Name | Type | Description |
|---|---|---|
| pulsar_subscriptions_count | Gauge | The number of Pulsar subscriptions of the topic served by this broker. |
| pulsar_producers_count | Gauge | The number of active producers of the topic connected to this broker. |
| pulsar_consumers_count | Gauge | The number of active consumers of the topic connected to this broker. |
| pulsar_rate_in | Gauge | The total message rate of the topic coming into this broker (messages/second). |
| pulsar_rate_out | Gauge | The total message rate of the topic going out from this broker (messages/second). |
| pulsar_throughput_in | Gauge | The total throughput of the topic coming into this broker (bytes/second). |
| pulsar_throughput_out | Gauge | The total throughput of the topic going out from this broker (bytes/second). |
| pulsar_storage_size | Gauge | The total storage size of the topics in this topic owned by this broker (bytes). |
| pulsar_storage_backlog_size | Gauge | The total backlog size of the topics of this topic owned by this broker (messages). |
| pulsar_storage_offloaded_size | Gauge | The total amount of the data in this topic offloaded to the tiered storage (bytes). |
| pulsar_storage_backlog_quota_limit | Gauge | The total amount of the data in this topic that limit the backlog quota (bytes). |
| pulsar_storage_write_rate | Gauge | The total message batches (entries) written to the storage for this topic (message batches / second). |
| pulsar_storage_read_rate | Gauge | The total message batches (entries) read from the storage for this topic (message batches / second). |
| pulsar_subscription_delayed | Gauge | The total message batches (entries) are delayed for dispatching. |
| pulsar_storage_write_latency_le_* | Histogram | The entry rate of a topic that the storage write latency is smaller with a given threshold.<br> Available thresholds: <br><ul><li>pulsar_storage_write_latency_le_0_5: <= 0.5ms </li><li>pulsar_storage_write_latency_le_1: <= 1ms</li><li>pulsar_storage_write_latency_le_5: <= 5ms</li><li>pulsar_storage_write_latency_le_10: <= 10ms</li><li>pulsar_storage_write_latency_le_20: <= 20ms</li><li>pulsar_storage_write_latency_le_50: <= 50ms</li><li>pulsar_storage_write_latency_le_100: <= 100ms</li><li>pulsar_storage_write_latency_le_200: <= 200ms</li><li>pulsar_storage_write_latency_le_1000: <= 1s</li><li>pulsar_storage_write_latency_le_overflow: > 1s</li></ul> |
| pulsar_entry_size_le_* | Histogram | The entry rate of a topic that the entry size is smaller with a given threshold.<br> Available thresholds: <br><ul><li>pulsar_entry_size_le_128: <= 128 bytes </li><li>pulsar_entry_size_le_512: <= 512 bytes</li><li>pulsar_entry_size_le_1_kb: <= 1 KB</li><li>pulsar_entry_size_le_2_kb: <= 2 KB</li><li>pulsar_entry_size_le_4_kb: <= 4 KB</li><li>pulsar_entry_size_le_16_kb: <= 16 KB</li><li>pulsar_entry_size_le_100_kb: <= 100 KB</li><li>pulsar_entry_size_le_1_mb: <= 1 MB</li><li>pulsar_entry_size_le_overflow: > 1 MB</li></ul> |
| pulsar_in_bytes_total | Counter | The total number of bytes received for this topic |
| pulsar_in_messages_total | Counter | The total number of messages received for this topic |
| pulsar_out_bytes_total | Counter | The total number of bytes read from this topic |
| pulsar_out_messages_total | Counter | The total number of messages read from this topic |

#### Replication metrics

If a namespace that a topic belongs to is configured to be replicated between multiple Pulsar clusters, the corresponding replication metrics will also be exposed when `replicationMetricsEnabled` is enabled.

All the replication metrics will also be labelled with `remoteCluster=${pulsar_remote_cluster}`.

| Name | Type | Description |
|---|---|---|
| pulsar_replication_rate_in | Gauge | The total message rate of the topic replicating from remote cluster (messages/second). |
| pulsar_replication_rate_out | Gauge | The total message rate of the topic replicating to remote cluster (messages/second). |
| pulsar_replication_throughput_in | Gauge | The total throughput of the topic replicating from remote cluster (bytes/second). |
| pulsar_replication_throughput_out | Gauge | The total throughput of the topic replicating to remote cluster (bytes/second). |
| pulsar_replication_backlog | Gauge | The total backlog of the topic replicating to remote cluster (messages). |

### ManagedLedgerCache metrics
All the ManagedLedgerCache metrics are labelled with the following labels:
- cluster: cluster=${pulsar_cluster}. ${pulsar_cluster} is the cluster name that you configured in broker.conf.

| Name | Type | Description |
| --- | --- | --- |
| pulsar_ml_cache_evictions | Gauge | The number of cache evictions during the last minute. |
| pulsar_ml_cache_hits_rate | Gauge | The number of cache hits per second. |
| pulsar_ml_cache_hits_throughput | Gauge | The amount of data is retrieved from the cache in byte/s |
| pulsar_ml_cache_misses_rate | Gauge | The number of cache misses per second |
| pulsar_ml_cache_misses_throughput | Gauge | The amount of data is retrieved from the cache in byte/s |
| pulsar_ml_cache_pool_active_allocations | Gauge | The number of currently active allocations in direct arena |
| pulsar_ml_cache_pool_active_allocations_huge | Gauge | The number of currently active huge allocation in direct arena |
| pulsar_ml_cache_pool_active_allocations_normal | Gauge | The number of currently active normal allocations in direct arena |
| pulsar_ml_cache_pool_active_allocations_small | Gauge | The number of currently active small allocations in direct arena |
| pulsar_ml_cache_pool_active_allocations_tiny | Gauge | The number of currently active tiny allocations in direct arena |
| pulsar_ml_cache_pool_allocated | Gauge | The total allocated memory of chunk lists in direct arena |
| pulsar_ml_cache_pool_used | Gauge | The total used memory of chunk lists in direct arena |
| pulsar_ml_cache_used_size | Gauge | The size in byte used to store the entries payloads |
| pulsar_ml_count | Gauge | The number of currently opened managed ledgers  |

### ManagedLedger metrics
All the managedLedger metrics are labelled with the following labels:
- cluster: cluster=${pulsar_cluster}. ${pulsar_cluster} is the cluster name that you configured in broker.conf.
- namespace: namespace=${pulsar_namespace}. ${pulsar_namespace} is the namespace name.
- quantile: quantile=${quantile}. Quantile is only for `Histogram` type metric, and represents the threshold for given Buckets.

| Name | Type | Description |
| --- | --- | --- |
| pulsar_ml_AddEntryBytesRate | Gauge | The bytes/s rate of messages added |
| pulsar_ml_AddEntryErrors | Gauge | The number of addEntry requests that failed |
| pulsar_ml_AddEntryLatencyBuckets | Histogram | The add entry latency of a ledger with a given quantile (threshold).<br> Available quantile: <br><ul><li> quantile="0.0_0.5" is AddEntryLatency between (0.0ms, 0.5ms]</li> <li>quantile="0.5_1.0" is AddEntryLatency between (0.5ms, 1.0ms]</li><li>quantile="1.0_5.0" is AddEntryLatency between (1ms, 5ms]</li><li>quantile="5.0_10.0" is AddEntryLatency between (5ms, 10ms]</li><li>quantile="10.0_20.0" is AddEntryLatency between (10ms, 20ms]</li><li>quantile="20.0_50.0" is AddEntryLatency between (20ms, 50ms]</li><li>quantile="50.0_100.0" is AddEntryLatency between (50ms, 100ms]</li><li>quantile="100.0_200.0" is AddEntryLatency between (100ms, 200ms]</li><li>quantile="200.0_1000.0" is AddEntryLatency between (200ms, 1s]</li></ul>|
| pulsar_ml_AddEntryLatencyBuckets_OVERFLOW | Gauge | The add entry latency > 1s |
| pulsar_ml_AddEntryMessagesRate | Gauge | The msg/s rate of messages added |
| pulsar_ml_AddEntrySucceed | Gauge | The number of addEntry requests that succeeded |
| pulsar_ml_EntrySizeBuckets | Histogram | The add entry size of a ledger with given quantile.<br> Available quantile: <br><ul><li>quantile="0.0_128.0" is EntrySize between (0byte, 128byte]</li><li>quantile="128.0_512.0" is EntrySize between (128byte, 512byte]</li><li>quantile="512.0_1024.0" is EntrySize between (512byte, 1KB]</li><li>quantile="1024.0_2048.0" is EntrySize between (1KB, 2KB]</li><li>quantile="2048.0_4096.0" is EntrySize between (2KB, 4KB]</li><li>quantile="4096.0_16384.0" is EntrySize between (4KB, 16KB]</li><li>quantile="16384.0_102400.0" is EntrySize between (16KB, 100KB]</li><li>quantile="102400.0_1232896.0" is EntrySize between (100KB, 1MB]</li></ul> |
| pulsar_ml_EntrySizeBuckets_OVERFLOW |Gauge  | The add entry size > 1MB |
| pulsar_ml_LedgerSwitchLatencyBuckets | Histogram | The ledger switch latency with given quantile. <br> Available quantile: <br><ul><li>quantile="0.0_0.5" is EntrySize between (0ms, 0.5ms]</li><li>quantile="0.5_1.0" is EntrySize between (0.5ms, 1ms]</li><li>quantile="1.0_5.0" is EntrySize between (1ms, 5ms]</li><li>quantile="5.0_10.0" is EntrySize between (5ms, 10ms]</li><li>quantile="10.0_20.0" is EntrySize between (10ms, 20ms]</li><li>quantile="20.0_50.0" is EntrySize between (20ms, 50ms]</li><li>quantile="50.0_100.0" is EntrySize between (50ms, 100ms]</li><li>quantile="100.0_200.0" is EntrySize between (100ms, 200ms]</li><li>quantile="200.0_1000.0" is EntrySize between (200ms, 1000ms]</li></ul> |
| pulsar_ml_LedgerSwitchLatencyBuckets_OVERFLOW | Gauge | The ledger switch latency > 1s |
| pulsar_ml_MarkDeleteRate | Gauge | The rate of mark-delete ops/s |
| pulsar_ml_NumberOfMessagesInBacklog | Gauge | The number of backlog messages for all the consumers |
| pulsar_ml_ReadEntriesBytesRate | Gauge | The bytes/s rate of messages read |
| pulsar_ml_ReadEntriesErrors | Gauge | The number of readEntries requests that failed |
| pulsar_ml_ReadEntriesRate | Gauge | The msg/s rate of messages read |
| pulsar_ml_ReadEntriesSucceeded | Gauge | The number of readEntries requests that succeeded |
| pulsar_ml_StoredMessagesSize | Gauge | The total size of the messages in active ledgers (accounting for the multiple copies stored) |

### LoadBalancing metrics
All the loadbalancing metrics are labelled with the following labels:
- cluster: cluster=${pulsar_cluster}. ${pulsar_cluster} is the cluster name that you configured in broker.conf.
- broker: broker=${broker}. ${broker} is the ip address of the broker
- metric: metric="loadBalancing".

| Name | Type | Description |
| --- | --- | --- |
| pulsar_lb_bandwidth_in_usage | Gauge | The broker inbound bandwith usage (in percent). |
| pulsar_lb_bandwidth_out_usage | Gauge | The broker outbound bandwith usage (in percent). |
| pulsar_lb_cpu_usage | Gauge | The broker cpu usage (in percent). |
| pulsar_lb_directMemory_usage | Gauge | The broker process direct memory usage (in percent). |
| pulsar_lb_memory_usage | Gauge | The broker process memory usage (in percent). |

#### BundleUnloading metrics
All the bundleUnloading metrics are labelled with the following labels:
- cluster: cluster=${pulsar_cluster}. ${pulsar_cluster} is the cluster name that you configured in broker.conf.
- metric: metric="bundleUnloading".

| Name | Type | Description |
| --- | --- | --- |
| pulsar_lb_unload_broker_count | Counter | Unload broker count in this bundle unloading |
| pulsar_lb_unload_bundle_count | Counter | Bundle unload count in this bundle unloading |

#### BundleSplit metrics
All the bundleUnloading metrics are labelled with the following labels:
- cluster: cluster=${pulsar_cluster}. ${pulsar_cluster} is the cluster name that you configured in broker.conf.
- metric: metric="bundlesSplit".

| Name | Type | Description |
| --- | --- | --- |
| pulsar_lb_bundles_split_count | Counter | bundle split count in this bundle splitting check interval |

### Subscription metrics

> Subscription metrics are only exposed when `exposeTopicLevelMetricsInPrometheus` is set to true.

All the subscription metrics are labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you configured in `broker.conf`.
- *namespace*: `namespace=${pulsar_namespace}`. `${pulsar_namespace}` is the namespace name.
- *topic*: `topic=${pulsar_topic}`. `${pulsar_topic}` is the topic name.
- *subscription*: `subscription=${subscription}`. `${subscription}` is the topic subscription name.

| Name | Type | Description |
|---|---|---|
| pulsar_subscription_back_log | Gauge | The total backlog of a subscription (messages). |
| pulsar_subscription_delayed | Gauge | The total number of messages are delayed to be dispatched for a subscription (messages). |
| pulsar_subscription_msg_rate_redeliver | Gauge | The total message rate for message being redelivered (messages/second). |
| pulsar_subscription_unacked_messages | Gauge | The total number of unacknowledged messages of a subscription (messages). |
| pulsar_subscription_blocked_on_unacked_messages | Gauge | Indicate whether a subscription is blocked on unacknowledged messages or not. <br> <ul><li>1 means the subscription is blocked on waiting unacknowledged messages to be acked.</li><li>0 means the subscription is not blocked on waiting unacknowledged messages to be acked.</li></ul> |
| pulsar_subscription_msg_rate_out | Gauge | The total message dispatch rate for a subscription (messages/second). |
| pulsar_subscription_msg_throughput_out | Gauge | The total message dispatch throughput for a subscription (bytes/second). |

### Consumer metrics

> Consumer metrics are only exposed when both `exposeTopicLevelMetricsInPrometheus` and `exposeConsumerLevelMetricsInPrometheus`
> are set to true.

All the consumer metrics are labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you configured in `broker.conf`.
- *namespace*: `namespace=${pulsar_namespace}`. `${pulsar_namespace}` is the namespace name.
- *topic*: `topic=${pulsar_topic}`. `${pulsar_topic}` is the topic name.
- *subscription*: `subscription=${subscription}`. `${subscription}` is the topic subscription name.
- *consumer_name*: `consumer_name=${consumer_name}`. `${consumer_name}` is the topic consumer name.
- *consumer_id*: `consumer_id=${consumer_id}`. `${consumer_id}` is the topic consumer id.

| Name | Type | Description |
|---|---|---|
| pulsar_consumer_msg_rate_redeliver | Gauge | The total message rate for message being redelivered (messages/second). |
| pulsar_consumer_unacked_messages | Gauge | The total number of unacknowledged messages of a consumer (messages). |
| pulsar_consumer_blocked_on_unacked_messages | Gauge | Indicate whether a consumer is blocked on unacknowledged messages or not. <br> <ul><li>1 means the consumer is blocked on waiting unacknowledged messages to be acked.</li><li>0 means the consumer is not blocked on waiting unacknowledged messages to be acked.</li></ul> |
| pulsar_consumer_msg_rate_out | Gauge | The total message dispatch rate for a consumer (messages/second). |
| pulsar_consumer_msg_throughput_out | Gauge | The total message dispatch throughput for a consumer (bytes/second). |
| pulsar_consumer_available_permits | Gauge | The available permits for for a consumer. |

### Managed ledger bookie client metrics

All the managed ledger bookie client metrics labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you configured in `broker.conf`.

| Name | Type | Description |
| --- | --- | --- |
| pulsar_managedLedger_client_bookkeeper_ml_scheduler_completed_tasks_* | Gauge |  The number of tasks the scheduler executor execute completed. <br>The number of metrics determined by the scheduler executor thread number configured by `managedLedgerNumSchedulerThreads` in `broker.conf`. <br> |
| pulsar_managedLedger_client_bookkeeper_ml_scheduler_queue_* | Gauge | The number of tasks queued in the scheduler executor's queue. <br>The number of metrics determined by scheduler executor's thread number configured by `managedLedgerNumSchedulerThreads` in `broker.conf`. <br> |
| pulsar_managedLedger_client_bookkeeper_ml_scheduler_total_tasks_* | Gauge | The total number of tasks the scheduler executor received. <br>The number of metrics determined by scheduler executor's thread number configured by `managedLedgerNumSchedulerThreads` in `broker.conf`. <br> |
| pulsar_managedLedger_client_bookkeeper_ml_workers_completed_tasks_* | Gauge | The number of tasks the worker executor execute completed. <br>The number of metrics determined by the number of worker task thread number configured by `managedLedgerNumWorkerThreads` in `broker.conf` <br> |
| pulsar_managedLedger_client_bookkeeper_ml_workers_queue_* | Gauge | The number of tasks queued in the worker executor's queue. <br>The number of metrics determined by scheduler executor's thread number configured by `managedLedgerNumWorkerThreads` in `broker.conf`. <br> |
| pulsar_managedLedger_client_bookkeeper_ml_workers_total_tasks_* | Gauge | The total number of tasks the worker executor received. <br>The number of metrics determined by worker executor's thread number configured by `managedLedgerNumWorkerThreads` in `broker.conf`. <br> |
| pulsar_managedLedger_client_bookkeeper_ml_scheduler_task_execution | Summary | The scheduler task execution latency calculated in milliseconds. |
| pulsar_managedLedger_client_bookkeeper_ml_scheduler_task_queued | Summary | The scheduler task queued latency calculated in milliseconds. |
| pulsar_managedLedger_client_bookkeeper_ml_workers_task_execution | Summary | The worker task execution latency calculated in milliseconds. |
| pulsar_managedLedger_client_bookkeeper_ml_workers_task_queued | Summary | The worker task queued latency calculated in milliseconds. |

### Jetty metrics

> For a functions-worker running separately from brokers, its Jetty metrics are only exposed when `includeStandardPrometheusMetrics` is set to `true`.

All the jetty metrics are labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you have configured in the `broker.conf` file.

| Name | Type | Description |
|---|---|---|
| jetty_requests_total | Counter | Number of requests. |
| jetty_requests_active | Gauge | Number of requests currently active. |
| jetty_requests_active_max | Gauge | Maximum number of requests that have been active at once. |
| jetty_request_time_max_seconds | Gauge | Maximum time spent handling requests. |
| jetty_request_time_seconds_total | Counter | Total time spent in all request handling. |
| jetty_dispatched_total | Counter | Number of dispatches. |
| jetty_dispatched_active | Gauge | Number of dispatches currently active. |
| jetty_dispatched_active_max | Gauge | Maximum number of active dispatches being handled. |
| jetty_dispatched_time_max | Gauge | Maximum time spent in dispatch handling. |
| jetty_dispatched_time_seconds_total | Counter | Total time spent in dispatch handling. |
| jetty_async_requests_total | Counter | Total number of async requests. |
| jetty_async_requests_waiting | Gauge | Currently waiting async requests. |
| jetty_async_requests_waiting_max | Gauge | Maximum number of waiting async requests. |
| jetty_async_dispatches_total | Counter | Number of requested that have been asynchronously dispatched. |
| jetty_expires_total | Counter | Number of async requests requests that have expired. |
| jetty_responses_total | Counter | Number of responses, labeled by status code. The `code` label can be "1xx", "2xx", "3xx", "4xx", or "5xx". |
| jetty_stats_seconds | Gauge | Time in seconds stats have been collected for. |
| jetty_responses_bytes_total | Counter | Total number of bytes across all responses. |

# Pulsar Functions

All the Pulsar Functions metrics are labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you configured in `broker.conf`.
- *namespace*: `namespace=${pulsar_namespace}`. `${pulsar_namespace}` is the namespace name.

| Name | Type | Description |
|---|---|---|
| pulsar_function_processed_successfully_total | Counter | Total number of messages processed successfully. |
| pulsar_function_processed_successfully_total_1min | Counter | Total number of messages processed successfully in the last 1 minute. |
| pulsar_function_system_exceptions_total | Counter | Total number of system exceptions. |
| pulsar_function_system_exceptions_total_1min | Counter | Total number of system exceptions in the last 1 minute. |
| pulsar_function_user_exceptions_total | Counter | Total number of user exceptions. |
| pulsar_function_user_exceptions_total_1min | Counter | Total number of user exceptions in the last 1 minute. |
| pulsar_function_process_latency_ms | Summary | Process latency in milliseconds. |
| pulsar_function_process_latency_ms_1min | Summary | Process latency in milliseconds in the last 1 minute. |
| pulsar_function_last_invocation | Gauge | The timestamp of the last invocation of the function. |
| pulsar_function_received_total | Counter | Total number of messages received from source. |
| pulsar_function_received_total_1min | Counter | Total number of messages received from source in the last 1 minute. |

# Proxy

All the proxy metrics are labelled with the following labels:

- *cluster*: `cluster=${pulsar_cluster}`. `${pulsar_cluster}` is the cluster name that you configured in `broker.conf`.
- *kubernetes_pod_name*: `kubernetes_pod_name=${kubernetes_pod_name}`. `${kubernetes_pod_name}` is the kubernetes pod name.

| Name | Type | Description |
|---|---|---|
| pulsar_proxy_active_connections | Gauge | Number of connections currently active in the proxy. |
| pulsar_proxy_new_connections | Counter | Counter of connections being opened in the proxy. |
| pulsar_proxy_rejected_connections | Counter | Counter for connections rejected due to throttling. |
| pulsar_proxy_binary_ops | Counter | Counter of proxy operations. |
| pulsar_proxy_binary_bytes | Counter | Counter of proxy bytes. |

# Pulsar SQL Worker

| Name | Type | Description |
|---|---|---|
| split_bytes_read | Counter | Number of bytes read from BookKeeper. |
| split_num_messages_deserialized | Counter | Number of messages deserialized. |
| split_num_record_deserialized | Counter | Number of records deserialized. |
| split_bytes_read_per_query | Summary | Total number of bytes read per query. |
| split_entry_deserialize_time | Summary | Time spent on derserializing entries. |
| split_entry_deserialize_time_per_query | Summary | Time spent on derserializing entries per query. |
| split_entry_queue_dequeue_wait_time | Summary | Time spend on waiting to get entry from entry queue because it is empty. |
| split_entry_queue_dequeue_wait_time_per_query | Summary | Total time spent on waiting to get entry from entry queue per query. |
| split_message_queue_dequeue_wait_time_per_query | Summary | Time spent on waiting to dequeue from message queue because is is empty per query. |
| split_message_queue_enqueue_wait_time | Summary | Time spent on waiting for message queue enqueue because the message queue is full. |
| split_message_queue_enqueue_wait_time_per_query | Summary | Time spent on waiting for message queue enqueue because the message queue is full per query. |
| split_num_entries_per_batch | Summary | Number of entries per batch. |
| split_num_entries_per_query | Summary | Number of entries per query. |
| split_num_messages_deserialized_per_entry | Summary | Number of messages deserialized per entry. |
| split_num_messages_deserialized_per_query | Summary | Number of messages deserialized per query. |
| split_read_attempts | Summary | Number of read attempts (fail if queues are full). |
| split_read_attempts_per_query | Summary | Number of read attempts per query. |
| split_read_latency_per_batch | Summary | Latency of reads per batch. |
| split_read_latency_per_query | Summary | Total read latency per query. |
| split_record_deserialize_time | Summary | Time spent on deserializing message to record. For example, Avro, JSON, and so on. |
| split_record_deserialize_time_per_query | Summary | Time spent on deserializing message to record per query. |
| split_total_execution_time | Summary | Total execution time . |

## Monitor

You can [set up a Prometheus instance](https://prometheus.io/) to collect all the metrics exposed at Pulsar components and set up
[Grafana](https://grafana.com/) dashboards to display the metrics and monitor your Pulsar cluster.

The following are some Grafana dashboards examples:

- [pulsar-grafana](http://pulsar.apache.org/docs/en/deploy-monitoring/#grafana): A grafana dashboard that displays metrics collected in Prometheus for Pulsar clusters running on Kubernetes.
- [apache-pulsar-grafana-dashboard](https://github.com/streamnative/apache-pulsar-grafana-dashboard): A collection of grafana dashboard templates for different Pulsar components running on both Kubernetes and on-premise machines.
