---
title: ZooKeeper and BookKeeper administration
---

Pulsar relies on two external systems for a variety of essential tasks:

* [ZooKeeper](https://zookeeper.apache.org/) is responsible for a wide variety of configuration- and coordination-related tasks.
* [BookKeeper](http://bookkeeper.apache.org/) is responsible for [persistent storage](../../getting-started/ConceptsAndArchitecture#persistent-storage) of message data.

ZooKeeper and BookKeeper are both open-source [Apache](https://www.apache.org/) projects.

## ZooKeeper

{% include explanations/deploying-zk.md %}

### ZooKeeper configuration

In Pulsar, ZooKeeper configuration is handled by two separate configuration files found in the `conf` directory of your Pulsar installation: `conf/zookeeper.conf` for [local ZooKeeper](#local-zookeeper) and `conf/global-zookeeper.conf` for [global ZooKeeper](#global-zookeeper).

#### Local ZooKeeper

Configuration for local ZooKeeper is handled by the [`conf/zookeeper.conf`](../../reference/Configuration#zookeeper) file. The table below shows the available parameters:

{% include config.html id="zookeeper" %}

#### Global ZooKeeper

Configuration for global ZooKeeper is handled by the [`conf/global-zookeeper.conf`](../../reference/Configuration#global-zookeeper) file. The table below shows the available parameters:

{% include config.html id="global-zookeeper" %}

## BookKeeper

{% popover BookKeeper %} is responsible for all durable message storage in Pulsar. BookKeeper is a distributed [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) WAL system that guarantees read consistency of independent message logs called {% popover ledgers %}. Individual BookKeeper servers are also called *bookies*.

{% include admonition.html type="info" content="
For a guide to managing message persistence, retention, and expiry in Pulsar, see [this guide](../../concerns/RetentionExpiry).
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

Consult the official [BookKeeper docs](http://bookkeeper.apache.org) for more information about BookKeeper.

## BookKeeper persistence policies

In Pulsar, you can set *persistence policies* at the {% popover namespace %} level. Persistence policies determine how persistent storage of messages is handled by {% popover BookKeeper %}. Policies determine four things:

* The number of {% popover acks %} (guaranteed copies) to wait for for each ledger entry
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

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/persistence %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/persistence)

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

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/persistence %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/persistence)

#### Java

```java
PersistencePolicies policies = admin.namespaces().getPersistence(namespace);
```