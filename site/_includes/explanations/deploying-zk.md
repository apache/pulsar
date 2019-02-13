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

Each Pulsar {% popover instance %} relies on two separate ZooKeeper quorums.

* [Local ZooKeeper](#deploying-local-zookeeper) operates at the {% popover cluster %} level and provides cluster-specific configuration management and coordination. Each Pulsar cluster needs to have a dedicated ZooKeeper cluster.
* [Configuration Store](#deploying-configuration-store) operates at the {% popover instance %} level and provides configuration management for the entire system (and thus across clusters). The configuration store quorum can be provided by an independent cluster of machines or by the same machines used by local ZooKeeper.

### Deploying local ZooKeeper

ZooKeeper manages a variety of essential coordination- and configuration-related tasks for Pulsar.

Deploying a Pulsar instance requires you to stand up one local {% popover ZooKeeper %} cluster *per Pulsar {% popover cluster %}*. 

To begin, add all ZooKeeper servers to the quorum configuration specified in the [`conf/zookeeper.conf`](../../reference/Configuration#zookeeper) file. Add a `server.N` line for each node in the cluster to the configuration, where `N` is the number of the ZooKeeper node. Here's an example for a three-node cluster:

```properties
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
```

On each host, you need to specify the ID of the node in each node's `myid` file, which is in each server's `data/zookeeper` folder by default (this can be changed via the [`dataDir`](../../reference/Configuration#zookeeper-dataDir) parameter).

{% include admonition.html type="info" content="
See the [Multi-server setup guide](https://zookeeper.apache.org/doc/r3.4.10/zookeeperAdmin.html#sc_zkMulitServerSetup) in the ZooKeeper documentation for detailed info on `myid` and more.
" %}

On a ZooKeeper server at `zk1.us-west.example.com`, for example, you could set the `myid` value like this:

```shell
$ mkdir -p data/zookeeper
$ echo 1 > data/zookeeper/myid
```

On `zk2.us-west.example.com` the command would be `echo 2 > data/zookeeper/myid` and so on.

Once each server has been added to the `zookeeper.conf` configuration and has the appropriate `myid` entry, you can start ZooKeeper on all hosts (in the background, using nohup) with the [`pulsar-daemon`](../../reference/CliTools#pulsar-daemon) CLI tool:

```shell
$ bin/pulsar-daemon start zookeeper
```

### Deploying the configuration store {#configuration-store}

The ZooKeeper cluster configured and started up in the section above is a *local* ZooKeeper cluster used to manage a single Pulsar {% popover cluster %}. In addition to a local cluster, however, a full Pulsar {% popover instance %} also requires a {% popover configuration store %} for handling some instance-level configuration and coordination tasks.

If you're deploying a [single-cluster](#single-cluster-pulsar-instance) instance, then you will not need a separate cluster for the configuration store. If, however, you're deploying a [multi-cluster](#multi-cluster-pulsar-instance) instance, then you should stand up a separate ZooKeeper cluster for configuration tasks.

#### Single-cluster Pulsar instance

If your Pulsar {% popover instance %} will consist of just one cluster, then you can deploy a {% popover configuration store %} on the same machines as the local ZooKeeper quorum but running on different TCP ports.

To deploy a ZooKeeper configuration store in a single-cluster instance, add the same ZooKeeper servers used by the local quorom to the configuration file in [`conf/global_zookeeper.conf`](../../reference/Configuration#configuration-store) using the same method for [local ZooKeeper](#local-zookeeper), but make sure to use a different port (2181 is the default for ZooKeeper). Here's an example that uses port 2184 for a three-node ZooKeeper cluster:

```properties
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
```

As before, create the `myid` files for each server on `data/global-zookeeper/myid`.

#### Multi-cluster Pulsar instance

When deploying a global Pulsar instance, with clusters distributed across different geographical regions, the configuration store serves as a highly available and strongly consistent metadata store that can tolerate failures and partitions spanning whole regions.

The key here is to make sure the ZK quorum members are spread across at least 3
regions and that other regions are running as observers.

Again, given the very low expected load on the configuration store servers, we can
share the same hosts used for the local ZooKeeper quorum.

For example, let's assume a Pulsar instance with the following clusters `us-west`,
`us-east`, `us-central`, `eu-central`, `ap-south`. Also let's assume, each cluster
will have its own local ZK servers named such as

```
zk[1-3].${CLUSTER}.example.com
```

In this scenario we want to pick the quorum participants from few clusters and
let all the others be ZK observers. For example, to form a 7 servers quorum, we
can pick 3 servers from `us-west`, 2 from `us-central` and 2 from `us-east`.

This will guarantee that writes to configuration store will be possible even if one
of these regions is unreachable.

The ZK configuration in all the servers will look like:

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

Additionally, ZK observers will need to have:

```properties
peerType=observer
```

##### Starting the service

Once your configuration store configuration is in place, you can start up the service using [`pulsar-daemon`](../../reference/CliTools#pulsar-daemon)

```shell
$ bin/pulsar-daemon start configuration-store
```
