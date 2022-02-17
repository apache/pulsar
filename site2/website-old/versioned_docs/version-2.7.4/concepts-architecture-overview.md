---
id: version-2.7.4-concepts-architecture-overview
title: Architecture Overview
sidebar_label: Architecture
original_id: concepts-architecture-overview
---

At the highest level, a Pulsar instance is composed of one or more Pulsar clusters. Clusters within an instance can [replicate](concepts-replication.md) data amongst themselves.

In a Pulsar cluster:

* One or more brokers handles and load balances incoming messages from producers, dispatches messages to consumers, communicates with the Pulsar configuration store to handle various coordination tasks, stores messages in BookKeeper instances (aka bookies), relies on a cluster-specific ZooKeeper cluster for certain tasks, and more.
* A BookKeeper cluster consisting of one or more bookies handles [persistent storage](#persistent-storage) of messages.
* A ZooKeeper cluster specific to that cluster handles coordination tasks between Pulsar clusters.

The diagram below provides an illustration of a Pulsar cluster:

![Pulsar architecture diagram](assets/pulsar-system-architecture.png)

At the broader instance level, an instance-wide ZooKeeper cluster called the configuration store handles coordination tasks involving multiple clusters, for example [geo-replication](concepts-replication.md).

## Brokers

The Pulsar message broker is a stateless component that's primarily responsible for running two other components:

* An HTTP server that exposes a {@inject: rest:REST:/} API for both administrative tasks and [topic lookup](concepts-clients.md#client-setup-phase) for producers and consumers
* A dispatcher, which is an asynchronous TCP server over a custom [binary protocol](developing-binary-protocol.md) used for all data transfers

Messages are typically dispatched out of a [managed ledger](#managed-ledgers) cache for the sake of performance, *unless* the backlog exceeds the cache size. If the backlog grows too large for the cache, the broker will start reading entries from BookKeeper.

Finally, to support geo-replication on global topics, the broker manages replicators that tail the entries published in the local region and republish them to the remote region using the Pulsar [Java client library](client-libraries-java.md).

> For a guide to managing Pulsar brokers, see the [brokers](admin-api-brokers.md) guide.

## Clusters

A Pulsar instance consists of one or more Pulsar *clusters*. Clusters, in turn, consist of:

* One or more Pulsar [brokers](#brokers)
* A ZooKeeper quorum used for cluster-level configuration and coordination
* An ensemble of bookies used for [persistent storage](#persistent-storage) of messages

Clusters can replicate amongst themselves using [geo-replication](concepts-replication.md).

> For a guide to managing Pulsar clusters, see the [clusters](admin-api-clusters.md) guide.

## Metadata store

Pulsar uses [Apache Zookeeper](https://zookeeper.apache.org/) for metadata storage, cluster configuration, and coordination. In a Pulsar instance:

* A configuration store quorum stores configuration for tenants, namespaces, and other entities that need to be globally consistent.
* Each cluster has its own local ZooKeeper ensemble that stores cluster-specific configuration and coordination such as which brokers are responsible for which topics as well as ownership metadata, broker load reports, BookKeeper ledger metadata, and more.

## Persistent storage

Pulsar provides guaranteed message delivery for applications. If a message successfully reaches a Pulsar broker, it will be delivered to its intended target.

This guarantee requires that non-acknowledged messages are stored in a durable manner until they can be delivered to and acknowledged by consumers. This mode of messaging is commonly called *persistent messaging*. In Pulsar, N copies of all messages are stored and synced on disk, for example 4 copies across two servers with mirrored [RAID](https://en.wikipedia.org/wiki/RAID) volumes on each server.

### Apache BookKeeper

Pulsar uses a system called [Apache BookKeeper](http://bookkeeper.apache.org/) for persistent message storage. BookKeeper is a distributed [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) (WAL) system that provides a number of crucial advantages for Pulsar:

* It enables Pulsar to utilize many independent logs, called [ledgers](#ledgers). Multiple ledgers can be created for topics over time.
* It offers very efficient storage for sequential data that handles entry replication.
* It guarantees read consistency of ledgers in the presence of various system failures.
* It offers even distribution of I/O across bookies.
* It's horizontally scalable in both capacity and throughput. Capacity can be immediately increased by adding more bookies to a cluster.
* Bookies are designed to handle thousands of ledgers with concurrent reads and writes. By using multiple disk devices---one for journal and another for general storage--bookies are able to isolate the effects of read operations from the latency of ongoing write operations.

In addition to message data, *cursors* are also persistently stored in BookKeeper. Cursors are [subscription](reference-terminology.md#subscription) positions for [consumers](reference-terminology.md#consumer). BookKeeper enables Pulsar to store consumer position in a scalable fashion.

At the moment, Pulsar supports persistent message storage. This accounts for the `persistent` in all topic names. Here's an example:

```http
persistent://my-tenant/my-namespace/my-topic
```

> Pulsar also supports ephemeral ([non-persistent](concepts-messaging.md#non-persistent-topics)) message storage.


You can see an illustration of how brokers and bookies interact in the diagram below:

![Brokers and bookies](assets/broker-bookie.png)


### Ledgers

A ledger is an append-only data structure with a single writer that is assigned to multiple BookKeeper storage nodes, or bookies. Ledger entries are replicated to multiple bookies. Ledgers themselves have very simple semantics:

* A Pulsar broker can create a ledger, append entries to the ledger, and close the ledger.
* After the ledger has been closed---either explicitly or because the writer process crashed---it can then be opened only in read-only mode.
* Finally, when entries in the ledger are no longer needed, the whole ledger can be deleted from the system (across all bookies).

#### Ledger read consistency

The main strength of Bookkeeper is that it guarantees read consistency in ledgers in the presence of failures. Since the ledger can only be written to by a single process, that process is free to append entries very efficiently, without need to obtain consensus. After a failure, the ledger will go through a recovery process that will finalize the state of the ledger and establish which entry was last committed to the log. After that point, all readers of the ledger are guaranteed to see the exact same content.

#### Managed ledgers

Given that Bookkeeper ledgers provide a single log abstraction, a library was developed on top of the ledger called the *managed ledger* that represents the storage layer for a single topic. A managed ledger represents the abstraction of a stream of messages with a single writer that keeps appending at the end of the stream and multiple cursors that are consuming the stream, each with its own associated position.

Internally, a single managed ledger uses multiple BookKeeper ledgers to store the data. There are two reasons to have multiple ledgers:

1. After a failure, a ledger is no longer writable and a new one needs to be created.
2. A ledger can be deleted when all cursors have consumed the messages it contains. This allows for periodic rollover of ledgers.

### Journal storage

In BookKeeper, *journal* files contain BookKeeper transaction logs. Before making an update to a [ledger](#ledgers), a bookie needs to ensure that a transaction describing the update is written to persistent (non-volatile) storage. A new journal file is created once the bookie starts or the older journal file reaches the journal file size threshold (configured using the [`journalMaxSizeMB`](reference-configuration.md#bookkeeper-journalMaxSizeMB) parameter).

## Pulsar proxy

One way for Pulsar clients to interact with a Pulsar [cluster](#clusters) is by connecting to Pulsar message [brokers](#brokers) directly. In some cases, however, this kind of direct connection is either infeasible or undesirable because the client doesn't have direct access to broker addresses. If you're running Pulsar in a cloud environment or on [Kubernetes](https://kubernetes.io) or an analogous platform, for example, then direct client connections to brokers are likely not possible.

The **Pulsar proxy** provides a solution to this problem by acting as a single gateway for all of the brokers in a cluster. If you run the Pulsar proxy (which, again, is optional), all client connections with the Pulsar cluster will flow through the proxy rather than communicating with brokers.

> For the sake of performance and fault tolerance, you can run as many instances of the Pulsar proxy as you'd like.

Architecturally, the Pulsar proxy gets all the information it requires from ZooKeeper. When starting the proxy on a machine, you only need to provide ZooKeeper connection strings for the cluster-specific and instance-wide configuration store clusters. Here's an example:

```bash
$ bin/pulsar proxy \
  --zookeeper-servers zk-0,zk-1,zk-2 \
  --configuration-store-servers zk-0,zk-1,zk-2
```

> #### Pulsar proxy docs
> For documentation on using the Pulsar proxy, see the [Pulsar proxy admin documentation](administration-proxy.md).


Some important things to know about the Pulsar proxy:

* Connecting clients don't need to provide *any* specific configuration to use the Pulsar proxy. You won't need to update the client configuration for existing applications beyond updating the IP used for the service URL (for example if you're running a load balancer over the Pulsar proxy).
* [TLS encryption](security-tls-transport.md) and [authentication](security-tls-authentication.md) is supported by the Pulsar proxy

## Service discovery

[Clients](getting-started-clients.md) connecting to Pulsar brokers need to be able to communicate with an entire Pulsar instance using a single URL. Pulsar provides a built-in service discovery mechanism that you can set up using the instructions in the [Deploying a Pulsar instance](deploy-bare-metal.md#service-discovery-setup) guide.

You can use your own service discovery system if you'd like. If you use your own system, there is just one requirement: when a client performs an HTTP request to an endpoint, such as `http://pulsar.us-west.example.com:8080`, the client needs to be redirected to *some* active broker in the desired cluster, whether via DNS, an HTTP or IP redirect, or some other means.

The diagram below illustrates Pulsar service discovery:

![alt-text](assets/pulsar-service-discovery.png)

In this diagram, the Pulsar cluster is addressable via a single DNS name: `pulsar-cluster.acme.com`. A [Python client](client-libraries-python.md), for example, could access this Pulsar cluster like this:

```python
from pulsar import Client

client = Client('pulsar://pulsar-cluster.acme.com:6650')
```

> **Note**
> In Pulsar, each topic is handled by only one broker. Initial requests from a client to read, update or delete a topic are sent to a broker that may not be the topic owner. If the broker cannot handle the request for this topic, it redirects the request to the appropriate broker.
