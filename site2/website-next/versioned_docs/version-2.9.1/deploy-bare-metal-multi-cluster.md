---
id: deploy-bare-metal-multi-cluster
title: Deploying a multi-cluster on bare metal
sidebar_label: "Bare metal multi-cluster"
original_id: deploy-bare-metal-multi-cluster
---

:::tip

1. You can use single-cluster Pulsar installation in most use cases, such as experimenting with Pulsar or using Pulsar in a startup or in a single team. If you need to run a multi-cluster Pulsar instance, see the [guide](deploy-bare-metal-multi-cluster).
2. If you want to use all built-in [Pulsar IO](io-overview.md) connectors, you need to download `apache-pulsar-io-connectors`package and install `apache-pulsar-io-connectors` under `connectors` directory in the pulsar directory on every broker node or on every function-worker node if you have run a separate cluster of function workers for [Pulsar Functions](functions-overview).
3. If you want to use [Tiered Storage](concepts-tiered-storage.md) feature in your Pulsar deployment, you need to download `apache-pulsar-offloaders`package and install `apache-pulsar-offloaders` under `offloaders` directory in the Pulsar directory on every broker node. For more details of how to configure this feature, you can refer to the [Tiered storage cookbook](cookbooks-tiered-storage).

:::

A Pulsar instance consists of multiple Pulsar clusters working in unison. You can distribute clusters across data centers or geographical regions and replicate the clusters amongst themselves using [geo-replication](administration-geo).Deploying a  multi-cluster Pulsar instance consists of the following steps:

1. Deploying two separate ZooKeeper quorums: a local quorum for each cluster in the instance and a configuration store quorum for instance-wide tasks
2. Initializing cluster metadata for each cluster
3. Deploying a BookKeeper cluster of bookies in each Pulsar cluster
4. Deploying brokers in each Pulsar cluster


> #### Run Pulsar locally or on Kubernetes?
> This guide shows you how to deploy Pulsar in production in a non-Kubernetes environment. If you want to run a standalone Pulsar cluster on a single machine for development purposes, see the [Setting up a local cluster](getting-started-standalone.md) guide. If you want to run Pulsar on [Kubernetes](https://kubernetes.io), see the [Pulsar on Kubernetes](deploy-kubernetes) guide, which includes sections on running Pulsar on Kubernetes, on Google Kubernetes Engine and on Amazon Web Services.

## System requirement

Currently, Pulsar is available for 64-bit **macOS**, **Linux**, and **Windows**. You need to install 64-bit JRE/JDK 8 or later versions.

:::note

Broker is only supported on 64-bit JVM.

:::

## Install Pulsar

To get started running Pulsar, download a binary tarball release in one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:binary_release_url" download>Pulsar @pulsar:version@ binary release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  
  $ wget 'https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-@pulsar:version@/apache-pulsar-@pulsar:version@-bin.tar.gz' -O apache-pulsar-@pulsar:version@-bin.tar.gz
  
  ```

Once you download the tarball, untar it and `cd` into the resulting directory:

```bash

$ tar xvfz apache-pulsar-@pulsar:version@-bin.tar.gz
$ cd apache-pulsar-@pulsar:version@

```

The Pulsar binary package initially contains the following directories:

Directory | Contains
:---------|:--------
`bin` | [Command-line tools](reference-cli-tools) of Pulsar, such as [`pulsar`](reference-cli-tools.md#pulsar) and [`pulsar-admin`](https://pulsar.apache.org/tools/pulsar-admin/)
`conf` | Configuration files for Pulsar, including for [broker configuration](reference-configuration.md#broker), [ZooKeeper configuration](reference-configuration.md#zookeeper), and more
`examples` | A Java JAR file containing example [Pulsar Functions](functions-overview)
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files that Pulsar uses 
`licenses` | License files, in `.txt` form, for various components of the Pulsar codebase

The following directories are created once you begin running Pulsar:

Directory | Contains
:---------|:--------
`data` | The data storage directory that ZooKeeper and BookKeeper use
`instances` | Artifacts created for [Pulsar Functions](functions-overview)
`logs` | Logs that the installation creates


## Deploy ZooKeeper

Each Pulsar instance relies on two separate ZooKeeper quorums.

* Local ZooKeeper operates at the cluster level and provides cluster-specific configuration management and coordination. Each Pulsar cluster needs a dedicated ZooKeeper cluster.
* Configuration Store operates at the instance level and provides configuration management for the entire system (and thus across clusters). An independent cluster of machines or the same machines that local ZooKeeper uses can provide the configuration store quorum.

You can use an independent cluster of machines or the same machines used by local ZooKeeper to provide the configuration store quorum.


### Deploy local ZooKeeper

ZooKeeper manages a variety of essential coordination-related and configuration-related tasks for Pulsar.

You need to stand up one local ZooKeeper cluster per Pulsar cluster for deploying a Pulsar instance. 

To begin, add all ZooKeeper servers to the quorum configuration specified in the [`conf/zookeeper.conf`](reference-configuration.md#zookeeper) file. Add a `server.N` line for each node in the cluster to the configuration, where `N` is the number of the ZooKeeper node. The following is an example for a three-node cluster:

```properties

server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888

```

On each host, you need to specify the ID of the node in the `myid` file of each node, which is in `data/zookeeper` folder of each server by default (you can change the file location via the [`dataDir`](reference-configuration.md#zookeeper-dataDir) parameter).

:::tip

See the [Multi-server setup guide](https://zookeeper.apache.org/doc/r3.4.10/zookeeperAdmin.html#sc_zkMulitServerSetup) in the ZooKeeper documentation for detailed information on `myid` and more.

:::

On a ZooKeeper server at `zk1.us-west.example.com`, for example, you could set the `myid` value like this:

```shell

$ mkdir -p data/zookeeper
$ echo 1 > data/zookeeper/myid

```

On `zk2.us-west.example.com` the command looks like `echo 2 > data/zookeeper/myid` and so on.

Once you add each server to the `zookeeper.conf` configuration and each server has the appropriate `myid` entry, you can start ZooKeeper on all hosts (in the background, using nohup) with the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```shell

$ bin/pulsar-daemon start zookeeper

```

### Deploy the configuration store 

The ZooKeeper cluster configured and started up in the section above is a local ZooKeeper cluster that you can use to manage a single Pulsar cluster. In addition to a local cluster, however, a full Pulsar instance also requires a configuration store for handling some instance-level configuration and coordination tasks.

If you deploy a single-cluster instance, you do not need a separate cluster for the configuration store. If, however, you deploy a multi-cluster instance, you should stand up a separate ZooKeeper cluster for configuration tasks.

#### Single-cluster Pulsar instance

If your Pulsar instance consists of just one cluster, then you can deploy a configuration store on the same machines as the local ZooKeeper quorum but run on different TCP ports.

To deploy a ZooKeeper configuration store in a single-cluster instance, add the same ZooKeeper servers that the local quorum. You need to use the configuration file in [`conf/global_zookeeper.conf`](reference-configuration.md#configuration-store) using the same method for [local ZooKeeper](#local-zookeeper), but make sure to use a different port (2181 is the default for ZooKeeper). The following is an example that uses port 2184 for a three-node ZooKeeper cluster:

```properties

clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186

```

As before, create the `myid` files for each server on `data/global-zookeeper/myid`.

#### Multi-cluster Pulsar instance

When you deploy a global Pulsar instance, with clusters distributed across different geographical regions, the configuration store serves as a highly available and strongly consistent metadata store that can tolerate failures and partitions spanning whole regions.

The key here is to make sure the ZK quorum members are spread across at least 3 regions, and other regions run as observers.

Again, given the very low expected load on the configuration store servers, you can share the same hosts used for the local ZooKeeper quorum.

For example, assume a Pulsar instance with the following clusters `us-west`, `us-east`, `us-central`, `eu-central`, `ap-south`. Also assume, each cluster has its own local ZK servers named such as the following: 

```

zk[1-3].${CLUSTER}.example.com

```

In this scenario if you want to pick the quorum participants from few clusters and let all the others be ZK observers. For example, to form a 7 servers quorum, you can pick 3 servers from `us-west`, 2 from `us-central` and 2 from `us-east`.

This method guarantees that writes to configuration store is possible even if one of these regions is unreachable.

The ZK configuration in all the servers looks like:

```properties

clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
server.4=zk1.us-central.example.com:2185:2186
server.5=zk2.us-central.example.com:2185:2186
server.6=zk3.us-central.example.com:2185:2186:observer
server.7=zk1.us-east.example.com:2185:2186
server.8=zk2.us-east.example.com:2185:2186
server.9=zk3.us-east.example.com:2185:2186:observer
server.10=zk1.eu-central.example.com:2185:2186:observer
server.11=zk2.eu-central.example.com:2185:2186:observer
server.12=zk3.eu-central.example.com:2185:2186:observer
server.13=zk1.ap-south.example.com:2185:2186:observer
server.14=zk2.ap-south.example.com:2185:2186:observer
server.15=zk3.ap-south.example.com:2185:2186:observer

```

Additionally, ZK observers need to have the following parameters:

```properties

peerType=observer

```

##### Start the service

Once your configuration store configuration is in place, you can start up the service using [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon)

```shell

$ bin/pulsar-daemon start configuration-store

```

## Cluster metadata initialization

Once you set up the cluster-specific ZooKeeper and configuration store quorums for your instance, you need to write some metadata to ZooKeeper for each cluster in your instance. **you only need to write these metadata once**.

You can initialize this metadata using the [`initialize-cluster-metadata`](reference-cli-tools.md#pulsar-initialize-cluster-metadata) command of the [`pulsar`](reference-cli-tools.md#pulsar) CLI tool. The following is an example:

```shell

$ bin/pulsar initialize-cluster-metadata \
  --cluster us-west \
  --zookeeper zk1.us-west.example.com:2181 \
  --configuration-store zk1.us-west.example.com:2184 \
  --web-service-url http://pulsar.us-west.example.com:8080/ \
  --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/

```

As you can see from the example above, you need to specify the following:

* The name of the cluster
* The local ZooKeeper connection string for the cluster
* The configuration store connection string for the entire instance
* The web service URL for the cluster
* A broker service URL enabling interaction with the [brokers](reference-terminology.md#broker) in the cluster

If you use [TLS](security-tls-transport), you also need to specify a TLS web service URL for the cluster as well as a TLS broker service URL for the brokers in the cluster.

Make sure to run `initialize-cluster-metadata` for each cluster in your instance.

## Deploy BookKeeper

BookKeeper provides [persistent message storage](concepts-architecture-overview.md#persistent-storage) for Pulsar.

Each Pulsar broker needs its own cluster of bookies. The BookKeeper cluster shares a local ZooKeeper quorum with the Pulsar cluster.

### Configure bookies

You can configure BookKeeper bookies using the [`conf/bookkeeper.conf`](reference-configuration.md#bookkeeper) configuration file. The most important aspect of configuring each bookie is ensuring that the [`zkServers`](reference-configuration.md#bookkeeper-zkServers) parameter is set to the connection string for the local ZooKeeper of Pulsar cluster.

### Start bookies

You can start a bookie in two ways: in the foreground or as a background daemon.

To start a bookie in the background, use the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```bash

$ bin/pulsar-daemon start bookie

```

You can verify that the bookie works properly using the `bookiesanity` command for the [BookKeeper shell](reference-cli-tools.md#bookkeeper-shell):

```bash

$ bin/bookkeeper shell bookiesanity

```

This command creates a new ledger on the local bookie, writes a few entries, reads them back and finally deletes the ledger.

After you have started all bookies, you can use the `simpletest` command for [BookKeeper shell](reference-cli-tools.md#shell) on any bookie node, to verify that all bookies in the cluster are running.

```bash

$ bin/bookkeeper shell simpletest --ensemble <num-bookies> --writeQuorum <num-bookies> --ackQuorum <num-bookies> --numEntries <num-entries>

```

Bookie hosts are responsible for storing message data on disk. In order for bookies to provide optimal performance, having a suitable hardware configuration is essential for the bookies. The following are key dimensions for bookie hardware capacity.

* Disk I/O capacity read/write
* Storage capacity

Message entries written to bookies are always synced to disk before returning an acknowledgement to the Pulsar broker. To ensure low write latency, BookKeeper is
designed to use multiple devices:

* A **journal** to ensure durability. For sequential writes, having fast [fsync](https://linux.die.net/man/2/fsync) operations on bookie hosts is critical. Typically, small and fast [solid-state drives](https://en.wikipedia.org/wiki/Solid-state_drive) (SSDs) should suffice, or [hard disk drives](https://en.wikipedia.org/wiki/Hard_disk_drive) (HDDs) with a [RAID](https://en.wikipedia.org/wiki/RAID) controller and a battery-backed write cache. Both solutions can reach fsync latency of ~0.4 ms.
* A **ledger storage device** is where data is stored until all consumers acknowledge the message. Writes happen in the background, so write I/O is not a big concern. Reads happen sequentially most of the time and the backlog is drained only in case of consumer drain. To store large amounts of data, a typical configuration involves multiple HDDs with a RAID controller.



## Deploy brokers

Once you set up ZooKeeper, initialize cluster metadata, and spin up BookKeeper bookies, you can deploy brokers.

### Broker configuration

You can configure brokers using the [`conf/broker.conf`](reference-configuration.md#broker) configuration file.

The most important element of broker configuration is ensuring that each broker is aware of its local ZooKeeper quorum as well as the configuration store quorum. Make sure that you set the [`zookeeperServers`](reference-configuration.md#broker-zookeeperServers) parameter to reflect the local quorum and the [`configurationStoreServers`](reference-configuration.md#broker-configurationStoreServers) parameter to reflect the configuration store quorum (although you need to specify only those ZooKeeper servers located in the same cluster).

You also need to specify the name of the [cluster](reference-terminology.md#cluster) to which the broker belongs using the [`clusterName`](reference-configuration.md#broker-clusterName) parameter. In addition, you need to match the broker and web service ports provided when you initialize the metadata (especially when you use a different port from default) of the cluster.

The following is an example configuration:

```properties

# Local ZooKeeper servers
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Configuration store quorum connection string.
configurationStoreServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184

clusterName=us-west

# Broker data port
brokerServicePort=6650

# Broker data port for TLS
brokerServicePortTls=6651

# Port to use to server HTTP request
webServicePort=8080

# Port to use to server HTTPS request
webServicePortTls=8443

```

### Broker hardware

Pulsar brokers do not require any special hardware since they do not use the local disk. You had better choose fast CPUs and 10Gbps [NIC](https://en.wikipedia.org/wiki/Network_interface_controller) so that the software can take full advantage of that.

### Start the broker service

You can start a broker in the background by using [nohup](https://en.wikipedia.org/wiki/Nohup) with the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```shell

$ bin/pulsar-daemon start broker

```

You can also start brokers in the foreground by using [`pulsar broker`](reference-cli-tools.md#broker):

```shell

$ bin/pulsar broker

```

## Service discovery

[Clients](getting-started-clients) connecting to Pulsar brokers need to communicate with an entire Pulsar instance using a single URL.

You can use your own service discovery system. If you use your own system, you only need to satisfy just one requirement: when a client performs an HTTP request to an [endpoint](reference-configuration) for a Pulsar cluster, such as `http://pulsar.us-west.example.com:8080`, the client needs to be redirected to some active brokers in the desired cluster, whether via DNS, an HTTP or IP redirect, or some other means.

> **Service discovery already provided by many scheduling systems**
> Many large-scale deployment systems, such as [Kubernetes](deploy-kubernetes), have service discovery systems built in. If you run Pulsar on such a system, you may not need to provide your own service discovery mechanism.

## Admin client and verification

At this point your Pulsar instance should be ready to use. You can now configure client machines that can serve as [administrative clients](admin-api-overview) for each cluster. You can use the [`conf/client.conf`](reference-configuration.md#client) configuration file to configure admin clients.

The most important thing is that you point the [`serviceUrl`](reference-configuration.md#client-serviceUrl) parameter to the correct service URL for the cluster:

```properties

serviceUrl=http://pulsar.us-west.example.com:8080/

```

## Provision new tenants

Pulsar is built as a fundamentally multi-tenant system.


If a new tenant wants to use the system, you need to create a new one. You can create a new tenant by using the [`pulsar-admin`](reference-pulsar-admin.md#tenants) CLI tool:

```shell

$ bin/pulsar-admin tenants create test-tenant \
  --allowed-clusters us-west \
  --admin-roles test-admin-role

```

In this command, users who identify with `test-admin-role` role can administer the configuration for the `test-tenant` tenant. The `test-tenant` tenant can only use the `us-west` cluster. From now on, this tenant can manage its resources.

Once you create a tenant, you need to create [namespaces](reference-terminology.md#namespace) for topics within that tenant.


The first step is to create a namespace. A namespace is an administrative unit that can contain many topics. A common practice is to create a namespace for each different use case from a single tenant.

```shell

$ bin/pulsar-admin namespaces create test-tenant/ns1

```

##### Test producer and consumer


Everything is now ready to send and receive messages. The quickest way to test the system is through the [`pulsar-perf`](reference-cli-tools.md#pulsar-perf) client tool.


You can use a topic in the namespace that you have just created. Topics are automatically created the first time when a producer or a consumer tries to use them.

The topic name in this case could be:

```http

persistent://test-tenant/ns1/my-topic

```

Start a consumer that creates a subscription on the topic and waits for messages:

```shell

$ bin/pulsar-perf consume persistent://test-tenant/ns1/my-topic

```

Start a producer that publishes messages at a fixed rate and reports stats every 10 seconds:

```shell

$ bin/pulsar-perf produce persistent://test-tenant/ns1/my-topic

```

To report the topic stats:

```shell

$ bin/pulsar-admin topics stats persistent://test-tenant/ns1/my-topic

```

