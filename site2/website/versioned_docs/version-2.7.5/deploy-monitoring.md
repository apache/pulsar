---
id: deploy-monitoring
title: Monitor
sidebar_label: "Monitor"
original_id: deploy-monitoring
---

You can use different ways to monitor a Pulsar cluster, exposing both metrics related to the usage of topics and the overall health of the individual components of the cluster.

## Collect metrics

You can collect broker stats, ZooKeeper stats, and BookKeeper stats. 

### Broker stats

You can collect Pulsar broker metrics from brokers and export the metrics in JSON format. The Pulsar broker metrics mainly have two types:

* *Destination dumps*, which contain stats for each individual topic. You can fetch the destination dumps using the command below:

  ```shell
  
  bin/pulsar-admin broker-stats destinations
  
  ```

* Broker metrics, which contain the broker information and topics stats aggregated at namespace level. You can fetch the broker metrics by using the following command:

  ```shell
  
  bin/pulsar-admin broker-stats monitoring-metrics
  
  ```

All the message rates are updated every minute.

The aggregated broker metrics are also exposed in the [Prometheus](https://prometheus.io) format at:

```shell

http://$BROKER_ADDRESS:8080/metrics

```

### ZooKeeper stats

The local ZooKeeper, configuration store server and clients that are shipped with Pulsar can expose detailed stats through Prometheus.

```shell

http://$LOCAL_ZK_SERVER:8000/metrics
http://$GLOBAL_ZK_SERVER:8001/metrics

```

The default port of local ZooKeeper is `8000` and the default port of configuration store is `8001`. You can change the default port of local ZooKeeper and configuration store by specifying system property `stats_server_port`.

### BookKeeper stats

You can configure the stats frameworks for BookKeeper by modifying the `statsProviderClass` in the `conf/bookkeeper.conf` file.

The default BookKeeper configuration enables the Prometheus exporter. The configuration is included with Pulsar distribution.

```shell

http://$BOOKIE_ADDRESS:8000/metrics

```

The default port for bookie is `8000`. You can change the port by configuring `prometheusStatsHttpPort` in the `conf/bookkeeper.conf` file.

### Managed cursor acknowledgment state
The acknowledgment state is persistent to the ledger first. When the acknowledgment state fails to be persistent to the ledger, they are persistent to ZooKeeper. To track the stats of acknowledgement, you can configure the metrics for the managed cursor. 

```

brk_ml_cursor_persistLedgerSucceed(namespace=", ledger_name="", cursor_name:")
brk_ml_cursor_persistLedgerErrors(namespace="", ledger_name="", cursor_name:"")
brk_ml_cursor_persistZookeeperSucceed(namespace="", ledger_name="", cursor_name:"")
brk_ml_cursor_persistZookeeperErrors(namespace="", ledger_name="", cursor_name:"")

```

Those metrics are added in the Prometheus interface, you can monitor and check the metrics stats in the Grafana.

## Configure Prometheus

You can use Prometheus to collect all the metrics exposed for Pulsar components and set up [Grafana](https://grafana.com/) dashboards to display the metrics and monitor your Pulsar cluster. For details, refer to [Prometheus guide](https://prometheus.io/docs/introduction/getting_started/).

When you run Pulsar on bare metal, you can provide the list of nodes to be probed. When you deploy Pulsar in a Kubernetes cluster, the monitoring is setup automatically. For details, refer to [Kubernetes instructions](helm-deploy). 

## Dashboards

When you collect time series statistics, the major problem is to make sure the number of dimensions attached to the data does not explode. Thus you only need to collect time series of metrics aggregated at the namespace level.

### Pulsar per-topic dashboard

The per-topic dashboard instructions are available at [Pulsar manager](administration-pulsar-manager).

### Grafana

You can use grafana to create dashboard driven by the data that is stored in Prometheus.

When you deploy Pulsar on Kubernetes, a `pulsar-grafana` Docker image is enabled by default. You can use the docker image with the principal dashboards.

Enter the command below to use the dashboard manually:

```shell

docker run -p3000:3000 \
        -e PROMETHEUS_URL=http://$PROMETHEUS_HOST:9090/ \
        apachepulsar/pulsar-grafana:latest

```

The following are some Grafana dashboards examples:

- [pulsar-grafana](http://pulsar.apache.org/docs/en/deploy-monitoring/#grafana): a Grafana dashboard that displays metrics collected in Prometheus for Pulsar clusters running on Kubernetes.
- [apache-pulsar-grafana-dashboard](https://github.com/streamnative/apache-pulsar-grafana-dashboard): a collection of Grafana dashboard templates for different Pulsar components running on both Kubernetes and on-premise machines.

## Alerting rules
You can set alerting rules according to your Pulsar environment. To configure alerting rules for Apache Pulsar, refer to [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/).
