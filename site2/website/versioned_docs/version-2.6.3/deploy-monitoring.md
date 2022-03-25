---
id: version-2.6.3-deploy-monitoring
title: Monitoring
sidebar_label: Monitoring
original_id: deploy-monitoring
---

You can use different ways to monitor a Pulsar cluster, exposing both metrics that relate to the usage of topics and the overall health of the individual components of the cluster.

## Collect metrics

You can collect broker stats, ZooKeeper stats, and BookKeeper stats. 

### Broker stats

You can collect Pulsar broker metrics from brokers and export the metrics in JSON format. The Pulsar broker metrics mainly have two types:

* *Destination dumps*, which contain stats for each individual topic. You can fetch the destination dumps using the command below:

  ```shell
  bin/pulsar-admin broker-stats destinations
  ```

* Broker metrics, which contain the broker information and topics stats aggregated at namespace level. You can fetch the broker metrics using the command below:

  ```shell
  bin/pulsar-admin broker-stats monitoring-metrics
  ```

All the message rates are updated every 1min.

The aggregated broker metrics are also exposed in the [Prometheus](https://prometheus.io) format at:

```shell
http://$BROKER_ADDRESS:8080/metrics/
```

### ZooKeeper stats

The local Zookeeper and configuration store server and clients that are shipped with Pulsar have been instrumented to expose detailed stats through Prometheus as well.

```shell
http://$LOCAL_ZK_SERVER:8000/metrics
http://$GLOBAL_ZK_SERVER:8001/metrics
```

The default port of local ZooKeeper is `8000` and the default port of configuration store is `8001`. You can change the default port of local Zookeeper and configuration store by specifying system property `stats_server_port`.

### BookKeeper stats

For BookKeeper you can configure the stats frameworks by changing the `statsProviderClass` in
`conf/bookkeeper.conf`.

The default BookKeeper configuration, which is included with Pulsar distribution, enables the Prometheus exporter.

```shell
http://$BOOKIE_ADDRESS:8000/metrics
```

The default port for bookie is `8000` (instead of `8080`). You can change the port by configuring `prometheusStatsHttpPort` in `conf/bookkeeper.conf`.

## Configure Prometheus

You can use Prometheus to collect and store the metrics data. For details, refer to [Prometheus guide](https://prometheus.io/docs/introduction/getting_started/).

When you run Pulsar on bare metal, you can provide the list of nodes that needs to be probed. When you deploy Pulsar in a Kubernetes cluster, the monitoring is automatically setup with the [provided](deploy-kubernetes.md) instructions.

## Dashboards

When you collect time series statistics, the major problem is to make sure the number of dimensions attached to the data does not explode.

For that reason you only need to collect time series of metrics aggregated at the namespace level.

### Pulsar per-topic dashboard

The per-topic dashboard instructions are available at [Dashboard](administration-dashboard.md).

### Grafana

You can use grafana to easily create dashboard driven by the data that is stored in Prometheus.

When you deploy Pulsar on Kubernetes, a `pulsar-grafana` Docker image is enabled by default. You can use the docker image with the principal dashboards.

Enter the command below to use the dashboard manually:

```shell
docker run -p3000:3000 \
        -e PROMETHEUS_URL=http://$PROMETHEUS_HOST:9090/ \
        apachepulsar/pulsar-grafana:latest
```
