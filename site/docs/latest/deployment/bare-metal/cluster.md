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

### System requirements

To run Pulsar on bare metal, you will need a cluster of at least 6 machines or VMs (3 running ZooKeeper and 3 running {% popover brokers %} and {% popover bookies %}).

Each machine in your cluster will need to have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher installed.

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

[ZooKeeper](https://zookeeper.apache.org) manages a variety of essential coordination- and configuration-related tasks for Pulsar. To deploy a Pulsar cluster you'll need to deploy ZooKeeper first (before all other components). We recommend deploying a **three-node ZooKeeper cluster**. Pulsar does not make heavy use of ZooKeeper, so more lightweight machines or VMs should suffice for running ZooKeeper.

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

Once you've done that, add a server configuration in the [`conf/global_zookeeper.conf`](../../../reference/Configuration#global-zookeeper) configuration file that includes the same ZooKeeper hosts as the `conf/zookeeper.conf` file but specifies different ports, as well as a `clientPort` parameter. Here's an example:

```properties
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
```

As before, create `myid` files for each server in `data/global-zookeeper/myid`, and then make sure that the server IDs match up across `data/global-zookeeper/myid` and `data/zookeeper/myid`.

```bash
$ cat data/zookeeper/myid
$ cat data/global-zookeeper/myid
```

Once each server has been added to the `zookeeper.conf` configuration and has the appropriate `myid` entry, you can start ZooKeeper on all hosts (in the background, using nohup) with the [`pulsar-daemon`](../../../reference/CliTools#pulsar-daemon) CLI tool:

```bash
$ bin/pulsar-daemon start zookeeper
```

## Initializing cluster metadata

Once you've deployed ZooKeeper for your cluster, there is some metadata that needs to be written to ZooKeeper for each cluster in your instance. It only needs to be written **once**.

You can initialize this metadata using the [`initialize-cluster-metadata`](../../../reference/CliTools#pulsar-initialize-cluster-metadata) command of the [`pulsar`](../../../reference/CliTools#pulsar) CLI tool. This command can be run on any machine in your ZooKeeper cluster. Here's an example:

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

Flag | Description
:----|:-----------
`--cluster` | A name for the cluster
`--zookeeper` | A "local" ZooKeeper connection string for the cluster. This connection string only needs to include *one* machine in the ZooKeeper cluster.
`--global-zookeeper` | The "global" ZooKeeper connection string for the entire instance. As with the `--zookeeper` flag, this connection string only needs to include *one* machine in the ZooKeeper cluster.
`--web-service-url` | The web service URL for the cluster, plus a port. This URL should be a standard DNS name. The default port is 8080 (we don't recommend using a different port).
`--web-service-url-tls` | If you're using [TLS](../../../admin/Authz#tls-client-auth), you'll also need to specify a TLS web service URL for the cluster. The default port is 8443 (we don't recommend using a different port).
`--broker-service-url` | A broker service URL enabling interaction with the {% popover brokers %} in the cluster. This URL should use the same DNS name as the web service URL but should use the `pulsar` scheme instead. The default port is 6650 (we don't recommend using a different port).
`--broker-service-url-tls` | If you're using [TLS](../../../admin/Authz#tls-client-auth), you'll also need to specify a TLS web service URL for the cluster as well as a TLS broker service URL for the brokers in the cluster. The default port is 6651 (we don't recommend using a different port).

## Deploying a BookKeeper cluster

[BookKeeper](https://bookkeeper.apache.org) handles all persistent data storage in Pulsar.

We recommend running a **three-bookie BookKeeper cluster**.

## Deploying Pulsar brokers

We recommend deploying 3 brokers, with each broker running on the same machine as a BookKeeper bookie.