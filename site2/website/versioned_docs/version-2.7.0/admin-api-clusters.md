---
id: version-2.7.0-admin-api-clusters
title: Managing Clusters
sidebar_label: Clusters
original_id: admin-api-clusters
---

Pulsar clusters consist of one or more Pulsar [brokers](reference-terminology.md#broker), one or more [BookKeeper](reference-terminology.md#bookkeeper)
servers (aka [bookies](reference-terminology.md#bookie)), and a [ZooKeeper](https://zookeeper.apache.org) cluster that provides configuration and coordination management.

Clusters can be managed via:

* The [`clusters`](reference-pulsar-admin.md#clusters) command of the [`pulsar-admin`](reference-pulsar-admin.md) tool
* The `/admin/v2/clusters` endpoint of the admin {@inject: rest:REST:/} API
* The `clusters` method of the {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object in the [Java API](client-libraries-java.md)

## Clusters resources

### Provision

New clusters can be provisioned using the admin interface.

> Please note that this operation requires superuser privileges.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

You can provision a new cluster using the [`create`](reference-pulsar-admin.md#clusters-create) subcommand. Here's an example:

```shell
$ pulsar-admin clusters create cluster-1 \
  --url http://my-cluster.org.com:8080 \
  --broker-url pulsar://my-cluster.org.com:6650
```

<!--REST API-->

{@inject: endpoint|PUT|/admin/v2/clusters/:cluster|operation/createCluster?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
ClusterData clusterData = new ClusterData(
        serviceUrl,
        serviceUrlTls,
        brokerServiceUrl,
        brokerServiceUrlTls
);
admin.clusters().createCluster(clusterName, clusterData);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Initialize cluster metadata

When provision a new cluster, you need to initialize that cluster's [metadata](concepts-architecture-overview.md#metadata-store). When initializing cluster metadata, you need to specify all of the following:

* The name of the cluster
* The local ZooKeeper connection string for the cluster
* The configuration store connection string for the entire instance
* The web service URL for the cluster
* A broker service URL enabling interaction with the [brokers](reference-terminology.md#broker) in the cluster

You must initialize cluster metadata *before* starting up any [brokers](admin-api-brokers.md) that will belong to the cluster.

> **No cluster metadata initialization through the REST API or the Java admin API**
>
> Unlike most other admin functions in Pulsar, cluster metadata initialization cannot be performed via the admin REST API
> or the admin Java client, as metadata initialization involves communicating with ZooKeeper directly.
> Instead, you can use the [`pulsar`](reference-cli-tools.md#pulsar) CLI tool, in particular
> the [`initialize-cluster-metadata`](reference-cli-tools.md#pulsar-initialize-cluster-metadata) command.

Here's an example cluster metadata initialization command:

```shell
bin/pulsar initialize-cluster-metadata \
  --cluster us-west \
  --zookeeper zk1.us-west.example.com:2181 \
  --configuration-store zk1.us-west.example.com:2184 \
  --web-service-url http://pulsar.us-west.example.com:8080/ \
  --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/
```

You'll need to use `--*-tls` flags only if you're using [TLS authentication](security-tls-authentication.md) in your instance.

### Get configuration

You can fetch the [configuration](reference-configuration.md) for an existing cluster at any time.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`get`](reference-pulsar-admin.md#clusters-get) subcommand and specify the name of the cluster. Here's an example:

```shell
$ pulsar-admin clusters get cluster-1
{
    "serviceUrl": "http://my-cluster.org.com:8080/",
    "serviceUrlTls": null,
    "brokerServiceUrl": "pulsar://my-cluster.org.com:6650/",
    "brokerServiceUrlTls": null
    "peerClusterNames": null
}
```

<!--REST API-->

{@inject: endpoint|GET|/admin/v2/clusters/:cluster|operation/getCluster?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.clusters().getCluster(clusterName);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Update

You can update the configuration for an existing cluster at any time.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`update`](reference-pulsar-admin.md#clusters-update) subcommand and specify new configuration values using flags.

```shell
$ pulsar-admin clusters update cluster-1 \
  --url http://my-cluster.org.com:4081 \
  --broker-url pulsar://my-cluster.org.com:3350
```

<!--REST API-->

{@inject: endpoint|POST|/admin/v2/clusters/:cluster|operation/updateCluster?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
ClusterData clusterData = new ClusterData(
        serviceUrl,
        serviceUrlTls,
        brokerServiceUrl,
        brokerServiceUrlTls
);
admin.clusters().updateCluster(clusterName, clusterData);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Delete

Clusters can be deleted from a Pulsar [instance](reference-terminology.md#instance).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`delete`](reference-pulsar-admin.md#clusters-delete) subcommand and specify the name of the cluster.

```
$ pulsar-admin clusters delete cluster-1
```

<!--REST API-->

{@inject: endpoint|DELETE|/admin/v2/clusters/:cluster|operation/deleteCluster?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.clusters().deleteCluster(clusterName);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### List

You can fetch a list of all clusters in a Pulsar [instance](reference-terminology.md#instance).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`list`](reference-pulsar-admin.md#clusters-list) subcommand.

```shell
$ pulsar-admin clusters list
cluster-1
cluster-2
```

<!--REST API-->

{@inject: endpoint|GET|/admin/v2/clusters|operation/getClusters?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.clusters().getClusters();
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Update peer-cluster data

Peer clusters can be configured for a given cluster in a Pulsar [instance](reference-terminology.md#instance).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`update-peer-clusters`](reference-pulsar-admin.md#clusters-update-peer-clusters) subcommand and specify the list of peer-cluster names.

```
$ pulsar-admin update-peer-clusters cluster-1 --peer-clusters cluster-2
```

<!--REST API-->

{@inject: endpoint|POST|/admin/v2/clusters/:cluster/peers|operation/setPeerClusterNames?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.clusters().updatePeerClusterNames(clusterName, peerClusterList);
```
<!--END_DOCUSAURUS_CODE_TABS-->