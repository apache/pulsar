---
id: version-2.4.1-deploy-bare-metal
title: Deploying a cluster on bare metal
sidebar_label: Bare metal
original_id: deploy-bare-metal
---


> ### Tips
>
> 1. Single-cluster Pulsar installations should be sufficient for all but the most ambitious use cases. If you're interested in experimenting with
> Pulsar or using it in a startup or on a single team, we recommend opting for a single cluster. If you do need to run a multi-cluster Pulsar instance,
> however, see the guide [here](deploy-bare-metal-multi-cluster.md).
>
> 2. If you want to use all builtin [Pulsar IO](io-overview.md) connectors in your Pulsar deployment, you need to download `apache-pulsar-io-connectors`
> package and make sure it is installed under `connectors` directory in the pulsar directory on every broker node or on every function-worker node if you
> have run a separate cluster of function workers for [Pulsar Functions](functions-overview.md).
>
> 3. If you want to use [Tiered Storage](concepts-tiered-storage.md) feature in your Pulsar deployment, you need to download `apache-pulsar-offloaders`
> package and make sure it is installed under `offloaders` directory in the pulsar directory on every broker node. For more details of how to configure
> this feature, you could reference this [Tiered storage cookbook](cookbooks-tiered-storage.md).

Deploying a Pulsar cluster involves doing the following (in order):

* Deploying a [ZooKeeper](#deploying-a-zookeeper-cluster) cluster (optional)
* Initializing [cluster metadata](#initializing-cluster-metadata)
* Deploying a [BookKeeper](#deploying-a-bookkeeper-cluster) cluster
* Deploying one or more Pulsar [brokers](#deploying-pulsar-brokers)

## Preparation

### Requirements

Currently, Pulsar is available for 64-bit **macOS**, **Linux**, and **Windows**. To use Pulsar, you need to install 64-bit JRE/JDK 8 or later versions.

> If you already have an existing ZooKeeper cluster and would like to reuse it, you don't need to prepare the machines
> for running ZooKeeper.

To run Pulsar on bare metal, you are recommended to have:

* At least 6 Linux machines or VMs
  * 3 running [ZooKeeper](https://zookeeper.apache.org)
  * 3 running a Pulsar broker, and a [BookKeeper](https://bookkeeper.apache.org) bookie
* A single [DNS](https://en.wikipedia.org/wiki/Domain_Name_System) name covering all of the Pulsar broker hosts

> However if you don't have enough machines, or are trying out Pulsar in cluster mode (and expand the cluster later),
> you can even deploy Pulsar in one node, where it will run zookeeper, bookie and broker in same machine.

> If you don't have a DNS server, you can use multi-host in service URL instead.

Each machine in your cluster will need to have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher installed.

Here's a diagram showing the basic setup:

![alt-text](assets/pulsar-basic-setup.png)

In this diagram, connecting clients need to be able to communicate with the Pulsar cluster using a single URL, in this case `pulsar-cluster.acme.com`, that abstracts over all of the message-handling brokers. Pulsar message brokers run on machines alongside BookKeeper bookies; brokers and bookies, in turn, rely on ZooKeeper.

### Hardware considerations

When deploying a Pulsar cluster, we have some basic recommendations that you should keep in mind when capacity planning.

#### ZooKeeper

For machines running ZooKeeper, we recommend using lighter-weight machines or VMs. Pulsar uses ZooKeeper only for periodic coordination- and configuration-related tasks, *not* for basic operations. If you're running Pulsar on [Amazon Web Services](https://aws.amazon.com/) (AWS), for example, a [t2.small](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/t2-instances.html) instance would likely suffice.

#### Bookies & Brokers

For machines running a bookie and a Pulsar broker, we recommend using more powerful machines. For an AWS deployment, for example, [i3.4xlarge](https://aws.amazon.com/blogs/aws/now-available-i3-instances-for-demanding-io-intensive-applications/) instances may be appropriate. On those machines we also recommend:

* Fast CPUs and 10Gbps [NIC](https://en.wikipedia.org/wiki/Network_interface_controller) (for Pulsar brokers)
* Small and fast [solid-state drives](https://en.wikipedia.org/wiki/Solid-state_drive) (SSDs) or [hard disk drives](https://en.wikipedia.org/wiki/Hard_disk_drive) (HDDs) with a [RAID](https://en.wikipedia.org/wiki/RAID) controller and a battery-backed write cache (for BookKeeper bookies)

## Installing the Pulsar binary package

> You'll need to install the Pulsar binary package on *each machine in the cluster*, including machines running [ZooKeeper](#deploying-a-zookeeper-cluster) and [BookKeeper](#deploying-a-bookkeeper-cluster).

To get started deploying a Pulsar cluster on bare metal, you'll need to download a binary tarball release in one of the following ways:

* By clicking on the link directly below, which will automatically trigger a download:
  * <a href="pulsar:binary_release_url" download>Pulsar {{pulsar:version}} binary release</a>
* From the Pulsar [downloads page](pulsar:download_page_url)
* From the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest) on [GitHub](https://github.com)
* Using [wget](https://www.gnu.org/software/wget):

```bash
$ wget pulsar:binary_release_url
```

Once you've downloaded the tarball, untar it and `cd` into the resulting directory:

```bash
$ tar xvzf apache-pulsar-{{pulsar:version}}-bin.tar.gz
$ cd apache-pulsar-{{pulsar:version}}
```

The untarred directory contains the following subdirectories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's [command-line tools](reference-cli-tools.md), such as [`pulsar`](reference-cli-tools.md#pulsar) and [`pulsar-admin`](reference-pulsar-admin.md)
`conf` | Configuration files for Pulsar, including for [broker configuration](reference-configuration.md#broker), [ZooKeeper configuration](reference-configuration.md#zookeeper), and more
`data` | The data storage directory used by ZooKeeper and BookKeeper.
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
`logs` | Logs created by the installation.

## Installing Builtin Connectors (optional)

> Since release `2.1.0-incubating`, Pulsar releases a separate binary distribution, containing all the `builtin` connectors.
> If you would like to enable those `builtin` connectors, you can follow the instructions as below; otherwise you can
> skip this section for now.

To get started using builtin connectors, you'll need to download the connectors tarball release on every broker node in
one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:connector_release_url" download>Pulsar IO Connectors {{pulsar:version}} release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:connector_release_url/{connector}-{{pulsar:version}}.nar
  ```

Once the nar file is downloaded, copy the file to directory `connectors` in the pulsar directory, 
for example, if the connector file `pulsar-io-aerospike-{{pulsar:version}}.nar` is downloaded:

```bash
$ mkdir connectors
$ mv pulsar-io-aerospike-{{pulsar:version}}.nar connectors

$ ls connectors
pulsar-io-aerospike-{{pulsar:version}}.nar
...
```

## Installing Tiered Storage Offloaders (optional)

> Since release `2.2.0`, Pulsar releases a separate binary distribution, containing the tiered storage offloaders.
> If you would like to enable tiered storage feature, you can follow the instructions as below; otherwise you can
> skip this section for now.

To get started using tiered storage offloaders, you'll need to download the offloaders tarball release on every broker node in
one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:offloader_release_url" download>Pulsar Tiered Storage Offloaders {{pulsar:version}} release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:offloader_release_url
  ```

Once the tarball is downloaded, in the pulsar directory, untar the offloaders package and copy the offloaders as `offloaders`
in the pulsar directory:

```bash
$ tar xvfz apache-pulsar-offloaders-{{pulsar:version}}-bin.tar.gz

// you will find a directory named `apache-pulsar-offloaders-{{pulsar:version}}` in the pulsar directory
// then copy the offloaders

$ mv apache-pulsar-offloaders-{{pulsar:version}}/offloaders offloaders

$ ls offloaders
tiered-storage-jcloud-{{pulsar:version}}.nar
```

For more details of how to configure tiered storage feature, you could reference this [Tiered storage cookbook](cookbooks-tiered-storage.md)


## Deploying a ZooKeeper cluster

> If you already have an existing zookeeper cluster and would like to use it, you can skip this section.

[ZooKeeper](https://zookeeper.apache.org) manages a variety of essential coordination- and configuration-related tasks for Pulsar. To deploy a Pulsar cluster you'll need to deploy ZooKeeper first (before all other components). We recommend deploying a 3-node ZooKeeper cluster. Pulsar does not make heavy use of ZooKeeper, so more lightweight machines or VMs should suffice for running ZooKeeper.

To begin, add all ZooKeeper servers to the configuration specified in [`conf/zookeeper.conf`](reference-configuration.md#zookeeper) (in the Pulsar directory you created [above](#installing-the-pulsar-binary-package)). Here's an example:

```properties
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
```

> If you have only one machine to deploy Pulsar, you just need to add one server entry in the configuration file.

On each host, you need to specify the ID of the node in each node's `myid` file, which is in each server's `data/zookeeper` folder by default (this can be changed via the [`dataDir`](reference-configuration.md#zookeeper-dataDir) parameter).

> See the [Multi-server setup guide](https://zookeeper.apache.org/doc/r3.4.10/zookeeperAdmin.html#sc_zkMulitServerSetup) in the ZooKeeper documentation for detailed info on `myid` and more.

On a ZooKeeper server at `zk1.us-west.example.com`, for example, you could set the `myid` value like this:

```bash
$ mkdir -p data/zookeeper
$ echo 1 > data/zookeeper/myid
```

On `zk2.us-west.example.com` the command would be `echo 2 > data/zookeeper/myid` and so on.

Once each server has been added to the `zookeeper.conf` configuration and has the appropriate `myid` entry, you can start ZooKeeper on all hosts (in the background, using nohup) with the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start zookeeper
```

> If you are planning to deploy zookeeper with bookie on the same node, you
> need to start zookeeper by using different stats port.

Start zookeeper with [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool like:

```bash
$ PULSAR_EXTRA_OPTS="-Dstats_server_port=8001" bin/pulsar-daemon start zookeeper
```

## Initializing cluster metadata

Once you've deployed ZooKeeper for your cluster, there is some metadata that needs to be written to ZooKeeper for each cluster in your instance. It only needs to be written **once**.

You can initialize this metadata using the [`initialize-cluster-metadata`](reference-cli-tools.md#pulsar-initialize-cluster-metadata) command of the [`pulsar`](reference-cli-tools.md#pulsar) CLI tool. This command can be run on any machine in your ZooKeeper cluster. Here's an example:

```shell
$ bin/pulsar initialize-cluster-metadata \
  --cluster pulsar-cluster-1 \
  --zookeeper zk1.us-west.example.com:2181 \
  --configuration-store zk1.us-west.example.com:2181 \
  --web-service-url http://pulsar.us-west.example.com:8080 \
  --web-service-url-tls https://pulsar.us-west.example.com:8443 \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650 \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651
```

As you can see from the example above, the following needs to be specified:

Flag | Description
:----|:-----------
`--cluster` | A name for the cluster
`--zookeeper` | A "local" ZooKeeper connection string for the cluster. This connection string only needs to include *one* machine in the ZooKeeper cluster.
`--configuration-store` | The configuration store connection string for the entire instance. As with the `--zookeeper` flag, this connection string only needs to include *one* machine in the ZooKeeper cluster.
`--web-service-url` | The web service URL for the cluster, plus a port. This URL should be a standard DNS name. The default port is 8080 (we don't recommend using a different port).
`--web-service-url-tls` | If you're using [TLS](security-tls-transport.md), you'll also need to specify a TLS web service URL for the cluster. The default port is 8443 (we don't recommend using a different port).
`--broker-service-url` | A broker service URL enabling interaction with the brokers in the cluster. This URL should use the same DNS name as the web service URL but should use the `pulsar` scheme instead. The default port is 6650 (we don't recommend using a different port).
`--broker-service-url-tls` | If you're using [TLS](security-tls-transport.md), you'll also need to specify a TLS web service URL for the cluster as well as a TLS broker service URL for the brokers in the cluster. The default port is 6651 (we don't recommend using a different port).

## Deploying a BookKeeper cluster

[BookKeeper](https://bookkeeper.apache.org) handles all persistent data storage in Pulsar. You will need to deploy a cluster of BookKeeper bookies to use Pulsar. We recommend running a **3-bookie BookKeeper cluster**.

BookKeeper bookies can be configured using the [`conf/bookkeeper.conf`](reference-configuration.md#bookkeeper) configuration file. The most important step in configuring bookies for our purposes here is ensuring that the [`zkServers`](reference-configuration.md#bookkeeper-zkServers) is set to the connection string for the ZooKeeper cluster. Here's an example:

```properties
zkServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181
```

Once you've appropriately modified the `zkServers` parameter, you can provide any other configuration modifications you need. You can find a full listing of the available BookKeeper configuration parameters [here](reference-configuration.md#bookkeeper), although we would recommend consulting the [BookKeeper documentation](http://bookkeeper.apache.org/docs/latest/reference/config/) for a more in-depth guide.

Once you've applied the desired configuration in `conf/bookkeeper.conf`, you can start up a bookie on each of your BookKeeper hosts. You can start up each bookie either in the background, using [nohup](https://en.wikipedia.org/wiki/Nohup), or in the foreground.

To start the bookie in the background, use the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start bookie
```

To start the bookie in the foreground:

```bash
$ bin/bookkeeper bookie
```

You can verify that a bookie is working properly by running the `bookiesanity` command for the [BookKeeper shell](reference-cli-tools.md#shell) on it:

```bash
$ bin/bookkeeper shell bookiesanity
```

This will create an ephemeral BookKeeper ledger on the local bookie, write a few entries, read them back, and finally delete the ledger.

After you have started all the bookies, you can use `simpletest` command for [BookKeeper shell](reference-cli-tools.md#shell) on any bookie node, to
verify all the bookies in the cluster are up running.

```bash
$ bin/bookkeeper shell simpletest --ensemble <num-bookies> --writeQuorum <num-bookies> --ackQuorum <num-bookies> --numEntries <num-entries>
```

This command will create a `num-bookies` sized ledger on the cluster, write a few entries, and finally delete the ledger.


## Deploying Pulsar brokers

Pulsar brokers are the last thing you need to deploy in your Pulsar cluster. Brokers handle Pulsar messages and provide Pulsar's administrative interface. We recommend running **3 brokers**, one for each machine that's already running a BookKeeper bookie.

### Configuring Brokers

The most important element of broker configuration is ensuring that each broker is aware of the ZooKeeper cluster that you've deployed. Make sure that the [`zookeeperServers`](reference-configuration.md#broker-zookeeperServers) and [`configurationStoreServers`](reference-configuration.md#broker-configurationStoreServers) parameters. In this case, since we only have 1 cluster and no configuration store setup, the `configurationStoreServers` will point to the same `zookeeperServers`.

```properties
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181
configurationStoreServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181
```

You also need to specify the cluster name (matching the name that you provided when [initializing the cluster's metadata](#initializing-cluster-metadata)):

```properties
clusterName=pulsar-cluster-1
```

In addition, you need to match the broker and web service ports provided when initializing the cluster's metadata (especially when using a different port from default):

```properties
brokerServicePort=6650
brokerServicePortTls=6651
webServicePort=8080
webServicePortTls=8443
```

> If you deploy Pulsar in a one-node cluster, you should update the replication settings in `conf/broker.conf` to `1`
>
> ```properties
> # Number of bookies to use when creating a ledger
> managedLedgerDefaultEnsembleSize=1
>
> # Number of copies to store for each message
> managedLedgerDefaultWriteQuorum=1
> 
> # Number of guaranteed copies (acks to wait before write is complete)
> managedLedgerDefaultAckQuorum=1
> ```

### Enabling Pulsar Functions (optional)

If you want to enable [Pulsar Functions](functions-overview.md), you can follow the instructions as below:

1. Edit `conf/broker.conf` to enable functions worker, by setting `functionsWorkerEnabled` to `true`.

    ```conf
    functionsWorkerEnabled=true
    ```

2. Edit `conf/functions_worker.yml` and set `pulsarFunctionsCluster` to the cluster name that you provided when [initializing the cluster's metadata](#initializing-cluster-metadata). 

    ```conf
    pulsarFunctionsCluster: pulsar-cluster-1
    ```

If you would like to learn more options about deploying functions worker, please checkout [Deploy and manage functions worker](functions-worker.md).

### Starting Brokers

You can then provide any other configuration changes that you'd like in the [`conf/broker.conf`](reference-configuration.md#broker) file. Once you've decided on a configuration, you can start up the brokers for your Pulsar cluster. Like ZooKeeper and BookKeeper, brokers can be started either in the foreground or in the background, using nohup.

You can start a broker in the foreground using the [`pulsar broker`](reference-cli-tools.md#pulsar-broker) command:

```bash
$ bin/pulsar broker
```

You can start a broker in the background using the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start broker
```

Once you've successfully started up all the brokers you intend to use, your Pulsar cluster should be ready to go!

## Connecting to the running cluster

Once your Pulsar cluster is up and running, you should be able to connect with it using Pulsar clients. One such client is the [`pulsar-client`](reference-cli-tools.md#pulsar-client) tool, which is included with the Pulsar binary package. The `pulsar-client` tool can publish messages to and consume messages from Pulsar topics and thus provides a simple way to make sure that your cluster is running properly.

To use the `pulsar-client` tool, first modify the client configuration file in [`conf/client.conf`](reference-configuration.md#client) in your binary package. You'll need to change the values for `webServiceUrl` and `brokerServiceUrl`, substituting `localhost` (which is the default), with the DNS name that you've assigned to your broker/bookie hosts. Here's an example:

```properties
webServiceUrl=http://us-west.example.com:8080/
brokerServiceurl=pulsar://us-west.example.com:6650/
```

Once you've done that, you can publish a message to Pulsar topic:

```bash
$ bin/pulsar-client produce \
  persistent://public/default/test \
  -n 1 \
  -m "Hello Pulsar"
```

> You may need to use a different cluster name in the topic if you specified a cluster name different from `pulsar-cluster-1`.

This will publish a single message to the Pulsar topic. In addition, you can subscribe the Pulsar topic in a different terminal before publishing messages as below:

```bash
$ bin/pulsar-client consume \
  persistent://public/default/test \
  -n 100 \
  -s "consumer-test" \
  -t "Exclusive"
```

Once the message above has been successfully published to the topic, you should see it in the standard output:

```bash
----- got message -----
Hello Pulsar
```

## Running Functions

> If you have [enabled](#enabling-pulsar-functions-optional) Pulsar Functions, you can also tryout pulsar functions now.

Create a ExclamationFunction `exclamation`.

```bash
bin/pulsar-admin functions create \
  --jar examples/api-examples.jar \
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://public/default/exclamation-input \
  --output persistent://public/default/exclamation-output \
  --tenant public \
  --namespace default \
  --name exclamation
```

Check if the function is running as expected by [triggering](functions-deploying.md#triggering-pulsar-functions) the function.

```bash
bin/pulsar-admin functions trigger --name exclamation --trigger-value "hello world"
```

You will see output as below:

```shell
hello world!
```
