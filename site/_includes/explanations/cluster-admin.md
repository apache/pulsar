Pulsar clusters consist of one or more Pulsar {% popover brokers %}, one or more {% popover BookKeeper %} servers (aka {% popover bookies %}), and a {% popover ZooKeeper %} cluster that provides configuration and coordination management.

Clusters can be managed via:

* The [`clusters`](../../reference/CliTools#pulsar-admin-clusters) command of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool
* The `/admin/clusters` endpoint of the admin [REST API](../../reference/RestApi)
* The `clusters` method of the {% javadoc PulsarAdmin admin com.yahoo.pulsar.client.admin.PulsarAdmin %} object in the [Java API](../../applications/JavaClient)

### Provision

New clusters can be provisioned using the admin interface.

{% include message.html id="superuser" %}

#### pulsar-admin

You can provision a new cluster using the [`create`](../../reference/CliTools#pulsar-admin-clusters-create) subcommand. Here's an example:

```shell
$ pulsar-admin clusters create cluster-1 \
  --url http://my-cluster.org.com:8080 \
  --broker-url pulsar://my-cluster.org.com:6650
```

#### REST API

{% endpoint PUT /admin/clusters/:cluster %}

[More info](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
ClusterData clusterData = new ClusterData(
        serviceUrl,
        serviceUrlTls,
        brokerServiceUrl,
        brokerServiceUrlTls
);
admin.clusters().createCluster(clusterName, clusterData);
```

### Initialize cluster metadata

When provision a new cluster, you need to initialize that cluster's [metadata](../../getting-started/ConceptsAndArchitecture#metadata-store). When initializing cluster metadata, you need to specify all of the following:

* The name of the cluster
* The local ZooKeeper connection string for the cluster
* The global ZooKeeper connection string for the entire instance
* The web service URL for the cluster
* A broker service URL enabling interaction with the {% popover brokers %} in the cluster

You must initialize cluster metadata *before* starting up any [brokers](#managing-brokers) that will belong to the cluster.

{% include admonition.html type="warning" title="No cluster metadata initialization through the REST API or the Java admin API" content='
Unlike most other admin functions in Pulsar, cluster metadata initialization cannot be performed via the admin REST API or the admin Java client, as metadata initialization involves communicating with ZooKeeper directly. Instead, you can use the [`pulsar`](../../reference/CliTools#pulsar) CLI tool, in particular the [`initialize-cluster-metadata`](../../reference/CliTools#pulsar-initialize-cluster-metadata) command.
' %}

Here's an example cluster metadata initialization command:

```shell
bin/pulsar initialize-cluster-metadata \
  --cluster us-west \
  --zookeeper zk1.us-west.example.com:2181 \
  --global-zookeeper zk1.us-west.example.com:2184 \
  --web-service-url http://pulsar.us-west.example.com:8080/ \
  --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/
```

You'll need to use `--*-tls` flags only if you're using [TLS authentication](../../admin/Authz#tls-client-auth) in your instance.

Make sure to initialize

### Get configuration

You can fetch the [configuration](../../reference/Configuration) for an existing cluster at any time.

#### pulsar-admin

Use the [`get`](../../reference/CliTools#pulsar-admin-clusters-get) subcommand and specify the name of the cluster. Here's an example:

```shell
$ pulsar-admin clusters get cluster-1
{
    "serviceUrl": "http://my-cluster.org.com:8080/",
    "serviceUrlTls": null,
    "brokerServiceUrl": "pulsar://my-cluster.org.com:6650/",
    "brokerServiceUrlTls": null
}
```

#### REST API

{% endpoint GET /admin/clusters/:cluster %}

[More info](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
admin.clusters().getCluster(clusterName);
```

### Update

You can update the configuration for an existing cluster at any time.

#### pulsar-admin

Use the [`update`](../../reference/CliTools#pulsar-admin-clusters-update) subcommand and specify new configuration values using flags.

```shell
$ pulsar-admin clusters update cluster-1 \
  --url http://my-cluster.org.com:4081 \
  --broker-url pulsar://my-cluster.org.com:3350
```

#### REST

{% endpoint POST /admin/clusters/:cluster %}

[More info](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
ClusterData clusterData = new ClusterData(
        serviceUrl,
        serviceUrlTls,
        brokerServiceUrl,
        brokerServiceUrlTls
);
admin.clusters().updateCluster(clusterName, clusterData);
```

### Delete

Clusters can be deleted from a Pulsar {% popover instance %}.

#### pulsar-admin

Use the [`delete`](../../reference/CliTools#pulsar-admin-clusters-delete) subcommand and specify the name of the cluster.

```
$ pulsar-admin clusters delete cluster-1
```

#### REST API

{% endpoint DELETE /admin/clusters/:cluster %}

[More info](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
admin.clusters().deleteCluster(clusterName);
```

### List

You can fetch a list of all clusters in a Pulsar {% popover instance %}.

#### pulsar-admin

Use the [`list`](../../reference/CliTools#pulsar-admin-clusters-list) subcommand.

```shell
$ pulsar-admin clusters list
cluster-1
cluster-2
```

#### REST API

{% endpoint GET /admin/clusters %}

[More info](../../reference/RestApi#/admin/clusters)

###### Java

```java
admin.clusters().getClusters();
```
