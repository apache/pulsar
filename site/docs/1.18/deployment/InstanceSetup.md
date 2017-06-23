---
title: Deploying a Pulsar instance
tags: [admin, instance]
---

A Pulsar *instance* is a Pulsar installation consisting of multiple {% popover clusters %} that are potentially distributed across data centers or geographical regions.

Deploying a full Pulsar instance involves setting up two [ZooKeeper](#deploying-zookeeper) quorums

{% include admonition.html type="info" title='Running Pulsar locally?' content="
This guide shows you how to deploy Pulsar in production. If you're looking to run a standalone Pulsar cluster for development purposes on a single machine, see the [Setting up a local cluster](../../getting-started/LocalCluster) guide." %}

{% include explanations/install-package.md %}

## Deploying ZooKeeper

{% include explanations/deploying-zk.md %}

## Cluster metadata initialization

When setting up a new cluster, there is some metadata that needs to be initialized
for the first time. The following command will prepare both the BookKeeper
as well as the Pulsar metadata.

```shell
$ bin/pulsar initialize-cluster-metadata \
  --cluster us-west \
  --zookeeper zk1.us-west.example.com:2181 \
  --global-zookeeper zk1.us-west.example.com:2184 \
  --web-service-url http://pulsar.us-west.example.com:8080/ \
  --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/
```

## Deploying BookKeeper

{% include explanations/deploying-bk.md %}

#### Broker

Pulsar brokers do not need any special hardware consideration since they don't
use the local disk. Fast CPUs and 10Gbps NIC are recommended since the software
can take full advantage of that.

Minimal configuration changes in `conf/broker.conf` will include:

```properties
# Local ZK servers
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global Zookeeper quorum connection string. Here we just need to specify the
# servers located in the same cluster
globalZookeeperServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184

clusterName=us-west
```

##### Start broker service

```shell
$ bin/pulsar-daemon start broker
```

## Service discovery

{% include explanations/service-discovery-setup.md %}

#### Admin client and verification

At this point the cluster should be ready to use. We can now configure a client
machine that can serve as the administrative client.

Edit `conf/client.conf` to point the client to the correct service URL:

```properties
serviceUrl=http://pulsar.us-west.example.com:8080/
```

##### Provisioning a new tenant

To allow a new tenant to use the system, we need to create a new property.
Typically this will be done by the Pulsar cluster administrator or by some
automated tool:

```shell
$ bin/pulsar-admin properties create test \
  --allowed-clusters us-west \
  --admin-roles test-admin-role
```

This will allow users who identify with role `test-admin-role` to administer
the configuration for the property `test` which will only be allowed to use the
cluster `us-west`.

The tenant will be able from now on to self manage its resources.

The first step is to create a namespace. A namespace is an administrative unit
that can contain many topic. Common practice is to create a namespace for each
different use case from a single tenant.

```shell
$ bin/pulsar-admin namespaces create test/us-west/ns1
```

##### Testing producer and consumer

Everything is now ready to send and receive messages. The quickest way to test
the system is through the `pulsar-perf` client tool.

Let's use a topic in the namespace we just created. Topics are automatically
created the first time a producer or a consumer tries to use them.

The topic name in this case could be:

{% include topic.html p="test" c="us-west" n="ns1" t="my-topic" %}

Start a consumer that will create a subscription on the topic and will wait
for messages:

```shell
$ bin/pulsar-perf consume persistent://test/us-west/ns1/my-topic
```

Start a producer that publishes messages at a fixed rate and report stats every
10 seconds:

```shell
$ bin/pulsar-perf produce persistent://test/us-west/ns1/my-topic
```

To report the topic stats:

```shell
$ bin/pulsar-admin persistent stats persistent://test/us-west/ns1/my-topic
```

## Monitoring

### Broker stats

The [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool

Pulsar {% popover broker %} metrics can be collected from brokers and exported in JSON format. There are two main types of metrics:

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

### BookKeeper stats

There are several stats frameworks that works with BookKeeper and that can be enabled by changing the `statsProviderClass` in `conf/bookkeeper.conf`.

By following the instructions above, the `DataSketchesMetricsProvider` will be enabled. It features a very efficient way to compute latency quantiles, along with rates and counts.

The stats are dumped every interval into a JSON file that is overwritten each time.

```properties
statsProviderClass=org.apache.bokkeeper.stats.datasketches.DataSketchesMetricsProvider
dataSketchesMetricsJsonFileReporter=data/bookie-stats.json
dataSketchesMetricsUpdateIntervalSeconds=60
```
