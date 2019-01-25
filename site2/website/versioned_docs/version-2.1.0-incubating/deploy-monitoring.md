---
id: version-2.1.0-incubating-deploy-monitoring
title: Monitoring
sidebar_label: Monitoring
original_id: deploy-monitoring
---

There are different ways to monitor a Pulsar cluster, exposing both metrics relative to the usage of topics and the overall health of the individual components of the cluster.

## Collecting metrics

### Broker stats

Pulsar broker metrics can be collected from brokers and exported in JSON format. There are two main types of metrics:

* *Destination dumps*, which containing stats for each individual topic. They can be fetched using

  ```shell
  bin/pulsar-admin broker-stats destinations
  ```

* Broker metrics, containing broker info and topics stats aggregated at namespace
  level:

  ```shell
  bin/pulsar-admin broker-stats monitoring-metrics
  ```

All the message rates are updated every 1min.

The aggregated broker metrics are also exposed in the [Prometheus](https://prometheus.io) format at:

```shell
http://$BROKER_ADDRESS:8080/metrics
```

### ZooKeeper stats

The local ZooKeeper/configuration store server and clients that are shipped with Pulsar have been instrumented to expose
detailed stats through Prometheus as well.

```shell
http://$LOCAL_ZK_SERVER:8000/metrics
http://$GLOBAL_ZK_SERVER:8001/metrics
```

The default port of local ZooKeeper is `8000` and that of configuration store is `8001`.
These can be changed by specifying system property `stats_server_port`.

### BookKeeper stats

For BookKeeper you can configure the stats frameworks by changing the `statsProviderClass` in
`conf/bookkeeper.conf`.

By default, the default BookKeeper configuration included with Pulsar distribution will enable
the Prometheus exporter.

```shell
http://$BOOKIE_ADDRESS:8000/metrics
```

For bookies, the default port is `8000` (instead of `8080`) and that can be configured by changing
the `prometheusStatsHttpPort` in `conf/bookkeeper.conf`.

## Configuring Prometheus

You can configure Prometheus to collect and store the metrics data by following the Prometheus
[Getting started](https://prometheus.io/docs/introduction/getting_started/) guide.

When running on bare metal, you can provide the list of nodes that needs to be probed. When deploying
in a Kubernetes cluster, the monitoring is automatically setup with the [provided](deploy-kubernetes.md)
instructions.

## Dashboards

When collecting time series statistics, the major problem is to make sure the number of dimensions
attached to the data does not explode.

For that reason we only collect time series of metrics aggregated at the namespace level.

### Pulsar per-topic dashboard

The per-topic dashboard instructions are available at [Dashboard](administration-dashboard.md).

### Grafana

You can use grafana to easily create dashboard driven by the data stored in Prometheus.

There is a `pulsar-grafana` Docker image that is ready to use with the principal dashboards already
in place. This is enabled by default when deploying Pulsar on Kubernetes.

To use the dashboard manually:

```shell
docker run -p3000:3000 \
        -e PROMETHEUS_URL=http://$PROMETHEUS_HOST:9090/ \
        apachepulsar/pulsar-grafana:latest
```
