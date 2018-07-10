---
title: Deploying a Pulsar cluster on bare metal
tags: [admin, deployment, cluster, bare metal]
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

<!-- Convenience variables to be used for download links -->
{% capture binary_release_url %}http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz{% endcapture %}

{% include admonition.html type="info"
  content="Single-cluster Pulsar installations should be sufficient for all but the most ambitious use cases. If you're interested in experimenting with Pulsar or using it in a startup or on a single team, we recommend opting for a single cluster. If you do need to run a multi-cluster Pulsar instance, however, see the guide [here](../instance)." %}

Deploying a Pulsar {% popover cluster %} involves doing the following (in order):

* Deploying a [ZooKeeper](#deploying-a-zookeeper-cluster) cluster
* Initializing [cluster metadata](#initializing-cluster-metadata)
* Deploying a [BookKeeper](#deploying-a-bookkeeper-cluster) cluster
* Deploying one or more Pulsar [brokers](#deploying-pulsar-brokers)

### Requirements

To run Pulsar on bare metal, you will need:

* At least 6 Linux machines or VMs
  * 3 running [ZooKeeper](https://zookeeper.apache.org)
  * 3 running a Pulsar {% popover broker %} and a [BookKeeper](https://bookkeeper.apache.org) {% popover bookie %}
* A single [DNS](https://en.wikipedia.org/wiki/Domain_Name_System) name covering all of the Pulsar broker hosts

Each machine in your cluster will need to have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher installed.

Here's a diagram showing the basic setup:

{% img https://www.lucidchart.com/publicSegments/view/a096c5c3-e5ec-4f8b-9ee9-61d4fd058566/image.png 80 %}

In this diagram, connecting clients need to be able to communicate with the Pulsar cluster using a single URL, in this case `pulsar-cluster.acme.com`, that abstracts over all of the message-handling brokers. Pulsar message brokers run on machines alongside BookKeeper bookies; brokers and bookies, in turn, rely on ZooKeeper.

### Hardware considerations

When deploying a Pulsar cluster, we have some basic recommendations that you should keep in mind when capacity planning.

For machines running ZooKeeper, we recommend using lighter-weight machines or VMs. Pulsar uses ZooKeeper only for periodic coordination- and configuration-related tasks, *not* for basic operations. If you're running Pulsar on [Amazon Web Services](https://aws.amazon.com/) (AWS), for example, a [t2.small](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/t2-instances.html) instance would likely suffice.

For machines running a {% popover bookie %} and a Pulsar {% popover broker %}, we recommend using more powerful machines. For an AWS deployment, for example, [i3.4xlarge](https://aws.amazon.com/blogs/aws/now-available-i3-instances-for-demanding-io-intensive-applications/) instances may be appropriate. On those machines we also recommend:

* Fast CPUs and 10Gbps [NIC](https://en.wikipedia.org/wiki/Network_interface_controller) (for Pulsar brokers)
* Small and fast [solid-state drives](https://en.wikipedia.org/wiki/Solid-state_drive) (SSDs) or [hard disk drives](https://en.wikipedia.org/wiki/Hard_disk_drive) (HDDs) with a [RAID](https://en.wikipedia.org/wiki/RAID) controller and a battery-backed write cache (for BookKeeper bookies)

## Installing the Pulsar binary package

{% include admonition.html type="info"
   content="You'll need to install the Pulsar binary package on *each machine in the cluster*, including machines running [ZooKeeper](#deploying-a-zookeeper-cluster) and [BookKeeper](#deploying-a-bookkeeper-cluster)." %}

To get started deploying a Pulsar cluster on bare metal, you'll need to download a binary tarball release in one of the following ways:

* By clicking on the link directly below, which will automatically trigger a download:
  * <a href="{{ binary_release_url }}" download>Pulsar {{ site.current_version }} binary release</a>
* From the Pulsar [downloads page](http://pulsar.incubator.apache.org/download)
* From the Pulsar [releases page](https://github.com/apache/incubator-pulsar/releases/latest) on [GitHub](https://github.com)
* Using [wget](https://www.gnu.org/software/wget):

  ```bash
  $ wget http://archive.apache.org/dist/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz
  ```

Once you've downloaded the tarball, untar it and `cd` into the resulting directory:

```bash
$ tar xvzf apache-pulsar-{{ site.current_version }}-bin.tar.gz
$ cd apache-pulsar-{{ site.current_version }}
```

The untarred directory contains the following subdirectories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's [command-line tools](../../reference/CliTools), such as [`pulsar`](../../reference/CliTools#pulsar) and [`pulsar-admin`](../../reference/CliTools#pulsar-admin)
`conf` | Configuration files for Pulsar, including for [broker configuration](../../reference/Configuration#broker), [ZooKeeper configuration](../../reference/Configuration#zookeeper), and more
`data` | The data storage directory used by {% popover ZooKeeper %} and {% popover BookKeeper %}.
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
`logs` | Logs created by the installation.

## Deploying a ZooKeeper cluster

[ZooKeeper](https://zookeeper.apache.org) manages a variety of essential coordination- and configuration-related tasks for Pulsar. To deploy a Pulsar cluster you'll need to deploy ZooKeeper first (before all other components). We recommend deploying a 3-node ZooKeeper cluster. Pulsar does not make heavy use of ZooKeeper, so more lightweight machines or VMs should suffice for running ZooKeeper.

To begin, add all ZooKeeper servers to the configuration specified in [`conf/zookeeper.conf`](../../../reference/Configuration#zookeeper) (in the Pulsar directory you created [above](#installing-the-pulsar-binary-package)). Here's an example:

```properties
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
```

On each host, you need to specify the ID of the node in each node's `myid` file, which is in each server's `data/zookeeper` folder by default (this can be changed via the [`dataDir`](../../../reference/Configuration#zookeeper-dataDir) parameter).

{% include admonition.html type="info" content="
See the [Multi-server setup guide](https://zookeeper.apache.org/doc/r3.4.10/zookeeperAdmin.html#sc_zkMulitServerSetup) in the ZooKeeper documentation for detailed info on `myid` and more.
" %}

On a ZooKeeper server at `zk1.us-west.example.com`, for example, you could set the `myid` value like this:

```bash
$ mkdir -p data/zookeeper
$ echo 1 > data/zookeeper/myid
```

On `zk2.us-west.example.com` the command would be `echo 2 > data/zookeeper/myid` and so on.

Once each server has been added to the `zookeeper.conf` configuration and has the appropriate `myid` entry, you can start ZooKeeper on all hosts (in the background, using nohup) with the [`pulsar-daemon`](../../../reference/CliTools#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start zookeeper
```

## Initializing cluster metadata

Once you've deployed ZooKeeper for your cluster, there is some metadata that needs to be written to ZooKeeper for each cluster in your instance. It only needs to be written **once**.

You can initialize this metadata using the [`initialize-cluster-metadata`](../../../reference/CliTools#pulsar-initialize-cluster-metadata) command of the [`pulsar`](../../../reference/CliTools#pulsar) CLI tool. This command can be run on any machine in your ZooKeeper cluster. Here's an example:

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
`--configuration-store` | The {% popover configuration store %} connection string for the entire instance. As with the `--zookeeper` flag, this connection string only needs to include *one* machine in the ZooKeeper cluster.
`--web-service-url` | The web service URL for the cluster, plus a port. This URL should be a standard DNS name. The default port is 8080 (we don't recommend using a different port).
`--web-service-url-tls` | If you're using [TLS](../../../security/tls), you'll also need to specify a TLS web service URL for the cluster. The default port is 8443 (we don't recommend using a different port).
`--broker-service-url` | A broker service URL enabling interaction with the {% popover brokers %} in the cluster. This URL should use the same DNS name as the web service URL but should use the `pulsar` scheme instead. The default port is 6650 (we don't recommend using a different port).
`--broker-service-url-tls` | If you're using [TLS](../../../security/tls), you'll also need to specify a TLS web service URL for the cluster as well as a TLS broker service URL for the brokers in the cluster. The default port is 6651 (we don't recommend using a different port).

## Deploying a BookKeeper cluster

[BookKeeper](https://bookkeeper.apache.org) handles all persistent data storage in Pulsar. You will need to deploy a cluster of BookKeeper {% popover bookies %} to use Pulsar. We recommend running a **3-bookie BookKeeper cluster**.

BookKeeper bookies can be configured using the [`conf/bookkeeper.conf`](../../../reference/Configuration#bookkeeper) configuration file. The most important step in configuring bookies for our purposes here is ensuring that the [`zkServers`](../../../reference/Configuration#bookkeeper-zkServers) is set to the connection string for the ZooKeeper cluster. Here's an example:

```properties
zkServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181
```

Once you've appropriately modified the `zkServers` parameter, you can provide any other configuration modifications you need. You can find a full listing of the available BookKeeper configuration parameters [here](../../../reference/Configuration/#bookkeeper), although we would recommend consulting the [BookKeeper documentation](http://bookkeeper.apache.org/docs/latest/reference/config/) for a more in-depth guide.

Once you've applied the desired configuration in `conf/bookkeeper.conf`, you can start up a bookie on each of your BookKeeper hosts. You can start up each bookie either in the background, using [nohup](https://en.wikipedia.org/wiki/Nohup), or in the foreground.

To start the bookie in the background, use the [`pulsar-daemon`](../../../reference/CliTools#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start bookie
```

To start the bookie in the foreground:

```bash
$ bin/bookkeeper bookie
```

You can verify that the bookie is working properly using the `bookiesanity` command for the [BookKeeper shell](http://localhost:4000/docs/latest/deployment/reference/CliTools#bookkeeper-shell):

```bash
$ bin/bookkeeper shell bookiesanity
```

This will create an ephemeral BookKeeper {% popover ledger %} on the local bookie, write a few entries, read them back, and finally delete the ledger.

## Deploying Pulsar brokers

Pulsar {% popover brokers %} are the last thing you need to deploy in your Pulsar cluster. Brokers handle Pulsar messages and provide Pulsar's administrative interface. We recommend running **3 brokers**, one for each machine that's already running a BookKeeper bookie.

The most important element of broker configuration is ensuring that that each broker is aware of the ZooKeeper cluster that you've deployed. Make sure that the [`zookeeperServers`](../../../reference/Configuration#broker-zookeeperServers) and [`configurationStoreServers`](../../../reference/Configuration#broker-configurationStoreServers) parameters. In this case, since we only have 1 cluster and no configuration store setup, the `configurationStoreServers` will point to the same `zookeeperServers`.

```properties
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181
configurationStoreServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181
```

You also need to specify the cluster name (matching the name that you provided when [initializing the cluster's metadata](#initializing-cluster-metadata):

```properties
clusterName=pulsar-cluster-1
```

You can then provide any other configuration changes that you'd like in the [`conf/broker.conf`](../../../reference/Configuration#broker) file. Once you've decided on a configuration, you can start up the brokers for your Pulsar cluster. Like ZooKeeper and BookKeeper, brokers can be started either in the foreground or in the background, using nohup.

You can start a broker in the foreground using the [`pulsar broker`](../../../reference/CliTools#pulsar-broker) command:

```bash
$ bin/pulsar broker
```

You can start a broker in the background using the [`pulsar-daemon`](../../../reference/CliTools#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start broker
```

Once you've succesfully started up all the brokers you intend to use, your Pulsar cluster should be ready to go!

## Connecting to the running cluster

Once your Pulsar cluster is up and running, you should be able to connect with it using Pulsar clients. One such client is the [`pulsar-client`](../../../reference/CliTools#pulsar-client) tool, which is included with the Pulsar binary package. The `pulsar-client` tool can publish messages to and consume messages from Pulsar {% popover topics %} and thus provides a simple way to make sure that your cluster is runnning properly.

To use the `pulsar-client` tool, first modify the client configuration file in [`conf/client.conf`](../../../reference/Configuration#client) in your binary package. You'll need to change the values for `webServiceUrl` and `brokerServiceUrl`, substituting `localhost` (which is the default), with the DNS name that you've assigned to your broker/bookie hosts. Here's an example:

```properties
webServiceUrl=http://us-west.example.com:8080/
brokerServiceurl=pulsar://us-west.example.com:6650/
```

Once you've done that, you can publish a message to Pulsar topic:

```bash
$ bin/pulsar-client produce \
  persistent://public/default/test \
  -n 1 \
  -m "Hello, Pulsar"
```

> You may need to use a different cluster name in the topic if you specified a cluster name different from `pulsar-cluster-1`.

This will publish a single message to the Pulsar topic.
