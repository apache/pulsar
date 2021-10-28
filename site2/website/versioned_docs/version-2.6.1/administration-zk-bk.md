---
id: version-2.6.1-administration-zk-bk
title: ZooKeeper and BookKeeper administration
sidebar_label: ZooKeeper and BookKeeper
original_id: administration-zk-bk
---

Pulsar relies on two external systems for essential tasks:

* [ZooKeeper](https://zookeeper.apache.org/) is responsible for a wide variety of configuration-related and coordination-related tasks.
* [BookKeeper](http://bookkeeper.apache.org/) is responsible for [persistent storage](concepts-architecture-overview.md#persistent-storage) of message data.

ZooKeeper and BookKeeper are both open-source [Apache](https://www.apache.org/) projects.

> Skip to the [How Pulsar uses ZooKeeper and BookKeeper](#how-pulsar-uses-zookeeper-and-bookkeeper) section below for a more schematic explanation of the role of these two systems in Pulsar.


## ZooKeeper

Each Pulsar instance relies on two separate ZooKeeper quorums.

* [Local ZooKeeper](#deploy-local-zookeeper) operates at the cluster level and provides cluster-specific configuration management and coordination. Each Pulsar cluster needs to have a dedicated ZooKeeper cluster.
* [Configuration Store](#deploy-configuration-store) operates at the instance level and provides configuration management for the entire system (and thus across clusters). An independent cluster of machines or the same machines that local ZooKeeper uses can provide the configuration store quorum. 

### Deploy local ZooKeeper

ZooKeeper manages a variety of essential coordination-related and configuration-related tasks for Pulsar.

To deploy a Pulsar instance, you need to stand up one local ZooKeeper cluster *per Pulsar cluster*.

To begin, add all ZooKeeper servers to the quorum configuration specified in the [`conf/zookeeper.conf`](reference-configuration.md#zookeeper) file. Add a `server.N` line for each node in the cluster to the configuration, where `N` is the number of the ZooKeeper node. The following is an example for a three-node cluster:

```properties
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
```

On each host, you need to specify the node ID in `myid` file of each node, which is in `data/zookeeper` folder of each server by default (you can change the file location via the [`dataDir`](reference-configuration.md#zookeeper-dataDir) parameter).

> See the [Multi-server setup guide](https://zookeeper.apache.org/doc/r3.4.10/zookeeperAdmin.html#sc_zkMulitServerSetup) in the ZooKeeper documentation for detailed information on `myid` and more.


On a ZooKeeper server at `zk1.us-west.example.com`, for example, you can set the `myid` value like this:

```shell
$ mkdir -p data/zookeeper
$ echo 1 > data/zookeeper/myid
```

On `zk2.us-west.example.com` the command is `echo 2 > data/zookeeper/myid` and so on.

Once you add each server to the `zookeeper.conf` configuration and each server has the appropriate `myid` entry, you can start ZooKeeper on all hosts (in the background, using nohup) with the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```shell
$ bin/pulsar-daemon start zookeeper
```

### Deploy configuration store

The ZooKeeper cluster configured and started up in the section above is a *local* ZooKeeper cluster that you can use to manage a single Pulsar cluster. In addition to a local cluster, however, a full Pulsar instance also requires a configuration store for handling some instance-level configuration and coordination tasks.

If you deploy a [single-cluster](#single-cluster-pulsar-instance) instance, you do not need a separate cluster for the configuration store. If, however, you deploy a [multi-cluster](#multi-cluster-pulsar-instance) instance, you need to stand up a separate ZooKeeper cluster for configuration tasks.

#### Single-cluster Pulsar instance

If your Pulsar instance consists of just one cluster, then you can deploy a configuration store on the same machines as the local ZooKeeper quorum but run on different TCP ports.

To deploy a ZooKeeper configuration store in a single-cluster instance, add the same ZooKeeper servers that the local quorum uses to the configuration file in [`conf/global_zookeeper.conf`](reference-configuration.md#configuration-store) using the same method for [local ZooKeeper](#local-zookeeper), but make sure to use a different port (2181 is the default for ZooKeeper). The following is an example that uses port 2184 for a three-node ZooKeeper cluster:

```properties
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
```

As before, create the `myid` files for each server on `data/global-zookeeper/myid`.

#### Multi-cluster Pulsar instance

When you deploy a global Pulsar instance, with clusters distributed across different geographical regions, the configuration store serves as a highly available and strongly consistent metadata store that can tolerate failures and partitions spanning whole regions.

The key here is to make sure the ZK quorum members are spread across at least 3 regions and that other regions run as observers.

Again, given the very low expected load on the configuration store servers, you can share the same hosts used for the local ZooKeeper quorum.

For example, you can assume a Pulsar instance with the following clusters `us-west`, `us-east`, `us-central`, `eu-central`, `ap-south`. Also you can assume, each cluster has its own local ZK servers named such as

```
zk[1-3].${CLUSTER}.example.com
```

In this scenario you want to pick the quorum participants from few clusters and let all the others be ZK observers. For example, to form a 7 servers quorum, you can pick 3 servers from `us-west`, 2 from `us-central` and 2 from `us-east`.

This guarantees that writes to configuration store is possible even if one of these regions is unreachable.

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

Additionally, ZK observers need to have:

```properties
peerType=observer
```

##### Start the service

Once your configuration store configuration is in place, you can start up the service using [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon)

```shell
$ bin/pulsar-daemon start configuration-store
```



### ZooKeeper configuration

In Pulsar, ZooKeeper configuration is handled by two separate configuration files in the `conf` directory of your Pulsar installation: `conf/zookeeper.conf` for [local ZooKeeper](#local-zookeeper) and `conf/global-zookeeper.conf` for [configuration store](#configuration-store).

#### Local ZooKeeper

The [`conf/zookeeper.conf`](reference-configuration.md#zookeeper) file handles the configuration for local ZooKeeper. The table below shows the available parameters:

|Name|Description|Default|
|---|---|---|
|tickTime|  The tick is the basic unit of time in ZooKeeper, measured in milliseconds and used to regulate things like heartbeats and timeouts. tickTime is the length of a single tick.  |2000|
|initLimit| The maximum time, in ticks, that the leader ZooKeeper server allows follower ZooKeeper servers to successfully connect and sync. The tick time is set in milliseconds using the tickTime parameter. |10|
|syncLimit| The maximum time, in ticks, that a follower ZooKeeper server is allowed to sync with other ZooKeeper servers. The tick time is set in milliseconds using the tickTime parameter.  |5|
|dataDir| The location where ZooKeeper stores in-memory database snapshots as well as the transaction log of updates to the database. |data/zookeeper|
|clientPort|  The port on which the ZooKeeper server listens for connections. |2181|
|autopurge.snapRetainCount| In ZooKeeper, auto purge determines how many recent snapshots of the database stored in dataDir to retain within the time interval specified by autopurge.purgeInterval (while deleting the rest).  |3|
|autopurge.purgeInterval| The time interval, in hours, which triggers the ZooKeeper database purge task. Setting to a non-zero number enables auto purge; setting to 0 disables. Read this guide before enabling auto purge. |1|
|maxClientCnxns|  The maximum number of client connections. Increase this if you need to handle more ZooKeeper clients. |60|


#### Configuration Store

The [`conf/global-zookeeper.conf`](reference-configuration.md#configuration-store) file handles the configuration for configuration store. The table below shows the available parameters:

## BookKeeper

BookKeeper stores all durable message in Pulsar. BookKeeper is a distributed [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) WAL system that guarantees read consistency of independent message logs calls ledgers. Individual BookKeeper servers are also called *bookies*.

> To manage message persistence, retention, and expiry in Pulsar, refer to [cookbook](cookbooks-retention-expiry.md).

### Hardware requirements

Bookie hosts store message data on disk. To provide optimal performance, ensure that the bookies have a suitable hardware configuration. The following are two key dimensions of bookie hardware capacity:

- Disk I/O capacity read/write
- Storage capacity

Message entries written to bookies are always synced to disk before returning an acknowledgement to the Pulsar broker by default. To ensure low write latency, BookKeeper is designed to use multiple devices:

- A **journal** to ensure durability. For sequential writes, it is critical to have fast [fsync](https://linux.die.net/man/2/fsync) operations on bookie hosts. Typically, small and fast [solid-state drives](https://en.wikipedia.org/wiki/Solid-state_drive) (SSDs) should suffice, or [hard disk drives](https://en.wikipedia.org/wiki/Hard_disk_drive) (HDDs) with a [RAID](https://en.wikipedia.org/wiki/RAID) controller and a battery-backed write cache. Both solutions can reach fsync latency of ~0.4 ms.
- A **ledger storage device** stores data. Writes happen in the background, so write I/O is not a big concern. Reads happen sequentially most of the time and the backlog is drained only in case of consumer drain. To store large amounts of data, a typical configuration involves multiple HDDs with a RAID controller.

### Configure BookKeeper

You can configure BookKeeper bookies using the [`conf/bookkeeper.conf`](reference-configuration.md#bookkeeper) configuration file. When you configure each bookie, ensure that the [`zkServers`](reference-configuration.md#bookkeeper-zkServers) parameter is set to the connection string for local ZooKeeper of the Pulsar cluster.

The minimum configuration changes required in `conf/bookkeeper.conf` are as follows:

```properties
# Change to point to journal disk mount point
journalDirectory=data/bookkeeper/journal

# Point to ledger storage disk mount point
ledgerDirectories=data/bookkeeper/ledgers

# Point to local ZK quorum
zkServers=zk1.example.com:2181,zk2.example.com:2181,zk3.example.com:2181
```

To change the ZooKeeper root path that BookKeeper uses, use `zkLedgersRootPath=/MY-PREFIX/ledgers` instead of `zkServers=localhost:2181/MY-PREFIX`.

> For more information about BookKeeper, refer to the official [BookKeeper docs](http://bookkeeper.apache.org).

### Deploy BookKeeper

BookKeeper provides [persistent message storage](concepts-architecture-overview.md#persistent-storage) for Pulsar. Each Pulsar broker has its own cluster of bookies. The BookKeeper cluster shares a local ZooKeeper quorum with the Pulsar cluster.

### Start bookies manually

You can start a bookie in the foreground or as a background daemon.

To start a bookie in the foreground, use the [`bookkeeper`](reference-cli-tools.md#bookkeeper) CLI tool:

```bash
$ bin/bookkeeper bookie
```

To start a bookie in the background, use the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start bookie
```

You can verify whether the bookie works properly with the `bookiesanity` command for the [BookKeeper shell](reference-cli-tools.md#bookkeeper-shell):

```shell
$ bin/bookkeeper shell bookiesanity
```

When you use this command, you create a new ledger on the local bookie, write a few entries, read them back and finally delete the ledger.

### Decommission bookies cleanly

Before you decommission a bookie, you need to check your environment and meet the following requirements.

1. Ensure the state of your cluster supports decommissioning the target bookie. Check if `EnsembleSize >= Write Quorum >= Ack Quorum` is `true` with one less bookie.

2. Ensure the target bookie is listed after using the `listbookies` command.

3. Ensure that no other process is ongoing (upgrade etc).

And then you can decommission bookies safely. To decommission bookies, complete the following steps.

1. Log in to the bookie node, check if there are underreplicated ledgers. The decommission command force to replicate the underreplicated ledgers.
`$ bin/bookkeeper shell listunderreplicated`

2. Stop the bookie by killing the bookie process. Make sure that no liveness/readiness probes setup for the bookies to spin them back up if you deploy it in a Kubernetes environment.

3. Run the decommission command.
   - If you have logged in to the node to be decommissioned, you do not need to provide `-bookieid`.
   - If you are running the decommission command for the target bookie node from another bookie node, you should mention the target bookie ID in the arguments for `-bookieid`
    `$ bin/bookkeeper shell decommissionbookie`
    or
    `$ bin/bookkeeper shell decommissionbookie -bookieid <target bookieid>`

4. Validate that no ledgers are on the decommissioned bookie.   
`$ bin/bookkeeper shell listledgers -bookieid <target bookieid>`

You can run the following command to check if the bookie you have decommissioned is listed in the bookies list:

```bash
./bookkeeper shell listbookies -rw -h
./bookkeeper shell listbookies -ro -h
```

## BookKeeper persistence policies

In Pulsar, you can set *persistence policies* at the namespace level, which determines how BookKeeper handles persistent storage of messages. Policies determine four things:

* The number of acks (guaranteed copies) to wait for each ledger entry.
* The number of bookies to use for a topic.
* The number of writes to make for each ledger entry.
* The throttling rate for mark-delete operations.

### Set persistence policies

You can set persistence policies for BookKeeper at the [namespace](reference-terminology.md#namespace) level.

#### Pulsar-admin

Use the [`set-persistence`](reference-pulsar-admin.md#namespaces-set-persistence) subcommand and specify a namespace as well as any policies that you want to apply. The available flags are:

Flag | Description | Default
:----|:------------|:-------
`-a`, `--bookkeeper-ack-quorum` | The number of acks (guaranteed copies) to wait on for each entry | 0
`-e`, `--bookkeeper-ensemble` | The number of [bookies](reference-terminology.md#bookie) to use for topics in the namespace | 0
`-w`, `--bookkeeper-write-quorum` | The number of writes to make for each entry | 0
`-r`, `--ml-mark-delete-max-rate` | Throttling rate for mark-delete operations (0 means no throttle) | 0

The following is an example:

```shell
$ pulsar-admin namespaces set-persistence my-tenant/my-ns \
  --bookkeeper-ack-quorum 3 \
  --bookeeper-ensemble 2
```

#### REST API

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/setPersistence?version=[[pulsar:version_number]]}

#### Java

```java
int bkEnsemble = 2;
int bkQuorum = 3;
int bkAckQuorum = 2;
double markDeleteRate = 0.7;
PersistencePolicies policies =
  new PersistencePolicies(ensemble, quorum, ackQuorum, markDeleteRate);
admin.namespaces().setPersistence(namespace, policies);
```

### List persistence policies

You can see which persistence policy currently applies to a namespace.

#### Pulsar-admin

Use the [`get-persistence`](reference-pulsar-admin.md#namespaces-get-persistence) subcommand and specify the namespace.

The following is an example:

```shell
$ pulsar-admin namespaces get-persistence my-tenant/my-ns
{
  "bookkeeperEnsemble": 1,
  "bookkeeperWriteQuorum": 1,
  "bookkeeperAckQuorum", 1,
  "managedLedgerMaxMarkDeleteRate": 0
}
```

#### REST API

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/getPersistence?version=[[pulsar:version_number]]}

#### Java

```java
PersistencePolicies policies = admin.namespaces().getPersistence(namespace);
```

## How Pulsar uses ZooKeeper and BookKeeper

This diagram illustrates the role of ZooKeeper and BookKeeper in a Pulsar cluster:

![ZooKeeper and BookKeeper](assets/pulsar-system-architecture.png)

Each Pulsar cluster consists of one or more message brokers. Each broker relies on an ensemble of bookies.
