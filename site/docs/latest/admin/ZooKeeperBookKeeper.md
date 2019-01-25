---
title: ZooKeeper and BookKeeper administration
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

Pulsar relies on two external systems for essential tasks:

* [ZooKeeper](https://zookeeper.apache.org/) is responsible for a wide variety of configuration- and coordination-related tasks.
* [BookKeeper](http://bookkeeper.apache.org/) is responsible for [persistent storage](../../getting-started/ConceptsAndArchitecture#persistent-storage) of message data.

ZooKeeper and BookKeeper are both open-source [Apache](https://www.apache.org/) projects.

{% include admonition.html type='info' content='
Skip to the [How Pulsar uses ZooKeeper and BookKeeper](#how-pulsar-uses-zookeeper-and-bookkeeper) section below for a more schematic explanation of the role of these two systems in Pulsar.
' %}

## ZooKeeper

{% include explanations/deploying-zk.md %}

### ZooKeeper configuration

In Pulsar, ZooKeeper configuration is handled by two separate configuration files found in the `conf` directory of your Pulsar installation: `conf/zookeeper.conf` for [local ZooKeeper](#local-zookeeper) and `conf/global-zookeeper.conf` for [Configuration Store](#configuration-store).

#### Local ZooKeeper

Configuration for local ZooKeeper is handled by the [`conf/zookeeper.conf`](../../reference/Configuration#zookeeper) file. The table below shows the available parameters:

{% include config.html id="zookeeper" %}

#### Configuration Store

Configuration for configuration Store is handled by the [`conf/global-zookeeper.conf`](../../reference/Configuration#configuration-store) file. The table below shows the available parameters:

{% include config.html id="configuration-store" %}

## BookKeeper

{% popover BookKeeper %} is responsible for all durable message storage in Pulsar. BookKeeper is a distributed [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) WAL system that guarantees read consistency of independent message logs called {% popover ledgers %}. Individual BookKeeper servers are also called *bookies*.

{% include admonition.html type="info" content="
For a guide to managing message persistence, retention, and expiry in Pulsar, see [this cookbook](../../cookbooks/RetentionExpiry).
" %}

### Deploying BookKeeper

{% include explanations/deploying-bk.md %}

### Configuring BookKeeper

Configurable parameters for BookKeeper bookies can be found in the [`conf/bookkeeper.conf`](../../reference/Configuration#bookkeeper) file.

Minimum configuration changes required  in `conf/bookkeeper.conf` are:

```properties
# Change to point to journal disk mount point
journalDirectory=data/bookkeeper/journal

# Point to ledger storage disk mount point
ledgerDirectories=data/bookkeeper/ledgers

# Point to local ZK quorum
zkServers=zk1.example.com:2181,zk2.example.com:2181,zk3.example.com:2181

# Change the ledger manager type
ledgerManagerType=hierarchical
```

{% include admonition.html type='info' content='Consult the official [BookKeeper docs](http://bookkeeper.apache.org) for more information about BookKeeper.' %}

## BookKeeper persistence policies

In Pulsar, you can set *persistence policies*, at the {% popover namespace %} level, that determine how {% popover BookKeeper %} handles persistent storage of messages. Policies determine four things:

* The number of {% popover acks %} (guaranteed copies) to wait for each ledger entry
* The number of {% popover bookies %} to use for a topic
* How many writes to make for each ledger entry
* The throttling rate for mark-delete operations

### Set persistence policies

You can set persistence policies for BookKeeper at the {% popover namespace %} level.

#### pulsar-admin

Use the [`set-persistence`](../../reference/CliTools#pulsar-admin-namespaces-set-persistence) subcommand and specify a namespace as well as any policies that you want to apply. The available flags are:

Flag | Description | Default
:----|:------------|:-------
`-a`, `--bookkeeper-ack-quorom` | The number of acks (guaranteed copies) to wait on for each entry | 0
`-e`, `--bookkeeper-ensemble` | The number of {% popover bookies %} to use for topics in the namespace | 0
`-w`, `--bookkeeper-write-quorum` | How many writes to make for each entry | 0
`-r`, `--ml-mark-delete-max-rate` | Throttling rate for mark-delete operations (0 means no throttle) | 0

##### Example

```shell
$ pulsar-admin namespaces set-persistence my-prop/my-cluster/my-ns \
  --bookkeeper-ack-quorom 3 \
  --bookeeper-ensemble 2
```

#### REST API

{% endpoint POST /admin/v2/namespaces/:tenant/:namespace/persistence %}

[More info](../../reference/RestApi#/admin/namespaces/:tenant/:namespace/persistence)

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

#### pulsar-admin

Use the [`get-persistence`](../../reference/CliTools#pulsar-admin-namespaces-get-persistence) subcommand and specify the namespace.

##### Example

```shell
$ pulsar-admin namespaces get-persistence my-prop/my-cluster/my-ns
{
  "bookkeeperEnsemble": 1,
  "bookkeeperWriteQuorum": 1,
  "bookkeeperAckQuorum", 1,
  "managedLedgerMaxMarkDeleteRate": 0
}
```

#### REST API

{% endpoint GET /admin/v2/namespaces/:tenant/:namespace/persistence %}

[More info](../../reference/RestApi#/admin/namespaces/:tenant/:namespace/persistence)

#### Java

```java
PersistencePolicies policies = admin.namespaces().getPersistence(namespace);
```

## How Pulsar uses ZooKeeper and BookKeeper

This diagram illustrates the role of ZooKeeper and BookKeeper in a Pulsar cluster:

![ZooKeeper and BookKeeper](/img/pulsar_system_architecture.png)

Each Pulsar {% popover cluster %} consists of one or more message {% popover brokers %}. Each broker relies on an ensemble of {% popover bookies %}.
