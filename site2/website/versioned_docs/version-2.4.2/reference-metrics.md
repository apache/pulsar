---
id: version-2.4.2-reference-metrics
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

## Overview

The metrics exposed by Pulsar are in Prometheus format. The types of metrics are:

- [Counter](https://prometheus.io/docs/concepts/metric_types/#counter): a cumulative metric that represents a single monotonically increasing counter whose value can only increase or be reset to zero on restart.
- [Gauge](https://prometheus.io/docs/concepts/metric_types/#gauge): a *gauge* is a metric that represents a single numerical value that can arbitrarily go up and down.
- [Histogram](https://prometheus.io/docs/concepts/metric_types/#histogram): a histogram samples observations (usually things like request durations or response sizes) and counts them in configurable buckets.
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
| bookkeeper_server_ADD_ENTRY_REQUEST | Summary | The summary of request latency of ADD_ENTRY requests at the bookie. The `success` label is used to distinguish successes and failures. | 
| bookkeeper_server_READ_ENTRY_REQUEST | Summary | The summary of request latency of READ_ENTRY requests at the bookie. The `success` label is used to distinguish successes and failures. | 

### Journal metrics

| Name | Type | Description |
|---|---|---|
| bookie_journal_JOURNAL_SYNC_count | Counter | The total number of journal fsync operations happening at the bookie. The `success` label is used to distinguish successes and failures. |
| bookie_journal_JOURNAL_QUEUE_SIZE | Gauge | The total number of requests pending in the journal queue. |
| bookie_journal_JOURNAL_FORCE_WRITE_QUEUE_SIZE | Gauge | The total number of force write (fsync) requests pending in the force-write queue. |
| bookie_journal_JOURNAL_CB_QUEUE_SIZE | Gauge | The total number of callbacks pending in the callback queue. |
| bookie_journal_JOURNAL_ADD_ENTRY | Summary | The summary of request latency of adding entries to the journal. |
| bookie_journal_JOURNAL_SYNC | Summary | The summary of fsync latency of syncing data to the journal disk. |

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
* [Subscription metrics](#subscription-metrics)
* [Consumer metrics](#consumer-metrics)

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
| pulsar_producers_count | Counter | The total number of messages received for this topic |

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

## Monitor

You can [set up a Prometheus instance](https://prometheus.io/) to collect all the metrics exposed at Pulsar components and set up
[Grafana](https://grafana.com/) dashboards to display the metrics and monitor your Pulsar cluster.

The following are some Grafana dashboards examples:

- [pulsar-grafana](http://pulsar.apache.org/docs/en/deploy-monitoring/#grafana): A grafana dashboard that displays metrics collected in Prometheus for Pulsar clusters running on Kubernetes.
- [apache-pulsar-grafana-dashboard](https://github.com/streamnative/apache-pulsar-grafana-dashboard): A collection of grafana dashboard templates for different Pulsar components running on both Kubernetes and on-premise machines.
