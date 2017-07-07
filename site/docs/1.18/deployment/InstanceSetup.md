---
title: Deploying a Pulsar instance
tags: [admin, instance]
---

A Pulsar *instance* consists of multiple Pulsar {% popover clusters %} that are potentially distributed across data centers or geographical regions.

{% include admonition.html type="info" title='Running Pulsar locally?' content="
This guide shows you how to deploy Pulsar in production. If you're looking to run a standalone Pulsar cluster for development purposes on a single machine, see the [Setting up a local cluster](../../getting-started/LocalCluster) guide." %}

{% include explanations/install-package.md %}

## Deploying ZooKeeper

{% include explanations/deploying-zk.md %}

## Cluster metadata initialization

Once you've set up local and global ZooKeeper for your instance, there is some metadata that needs to be written to ZooKeeper for each cluster in your instance. It only needs to be written once.

You can initialize this metadata using the [`initialize-cluster-metadata`](../../reference/CliTools#pulsar-initialize-cluster-metadata) command of the [`pulsar`](../../reference/CliTools#pulsar) CLI tool. Here's an example:

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

As you can see from the example above, the following needs to be specified:

* The name of the cluster
* The local ZooKeeper connection string for the cluster
* The global ZooKeeper connection string for the entire instance
* The web service URL for the cluster
* A broker service URL enabling interaction with the {% popover brokers %} in the cluster

If you're using [TLS](../../admin/Authz#tls-client-auth), you'll also need to specify a TLS web service URL for the cluster as well as a TLS broker service URL for the brokers in the cluster.

Make sure to run `initialize-cluster-metadata` for each cluster in your instance.

## Deploying BookKeeper

{% include explanations/deploying-bk.md %}

## Deploying brokers

Once you've set up ZooKeeper, initialized cluster metadata, and spun up BookKeeper {% popover bookies %}, you can deploy {% popover brokers %}.

### Broker configuration

Brokers can be configured using the [`conf/broker.conf`](../../reference/Configuration#broker) configuration file.

The most important element of broker configuration is ensuring that each broker is aware of its local ZooKeeper quorum as well as the global ZooKeeper quorum. Make sure that you set the [`zookeeperServers`](../../reference/Configuration#broker-zookeeperServers) parameter to reflect the local quorum and the [`globalZookeeperServers`](../../reference/Configuration#broker-globalZookeeperServers) parameter to reflect the global quorum (although you'll need to specify only those global ZooKeeper servers located in the same cluster).

You also need to specify the name of the {% popover cluster %} to which the broker belongs using the [`clusterName`](../../reference/Configuration#broker-clusterName) parameter.

Here's an example configuration:

```properties
# Local ZooKeeper servers
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global Zookeeper quorum connection string.
globalZookeeperServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184

clusterName=us-west
```

### Broker hardware

Pulsar brokers do not require any special hardware since they don't use the local disk. Fast CPUs and 10Gbps [NIC](https://en.wikipedia.org/wiki/Network_interface_controller) are recommended since the software can take full advantage of that.

### Starting the broker service

You can start a broker in the background using [nohup](https://en.wikipedia.org/wiki/Nohup) with the [`pulsar-daemon`](../../reference/CliTools#pulsar-daemon) CLI tool:

```shell
$ bin/pulsar-daemon start broker
```

You can also start brokers in the foreground using [`pulsar broker`](../../reference/CliTools#pulsar-broker):

```shell
$ bin/pulsar broker
```

## Service discovery

{% include explanations/service-discovery-setup.md %}

## Admin client and verification

At this point your Pulsar instance should be ready to use. You can now configure client machines that can serve as [administrative clients](../../admin/AdminInterface) for each cluster. You can use the [`conf/client.conf`](../../reference/Configuration#client) configuration file to configure admin clients.

The most important thing is that you point the [`serviceUrl`](../../reference/Configuration#client-serviceUrl) parameter to the correct service URL for the cluster:

```properties
serviceUrl=http://pulsar.us-west.example.com:8080/
```

## Provisioning a new tenant

Once you've set up an administrative client, 

To allow a new tenant to use the system, we need to create a new {% popover property %}. You can create a new property using the [`pulsar-admin`](../../reference/CliTools#pulsar-admin-properties-create) CLI tool:

```shell
$ bin/pulsar-admin properties create test-prop \
  --allowed-clusters us-west \
  --admin-roles test-admin-role
```

This will allow users who identify with role `test-admin-role` to administer the configuration for the property `test` which will only be allowed to use the cluster `us-west`. From now on, this tenant will be able to self-manage its resources.

Once a tenant has been created, you will need to create {% popover namespaces %} for topics within that property.

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
