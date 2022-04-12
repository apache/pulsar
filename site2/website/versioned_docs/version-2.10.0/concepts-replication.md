---
id: version-2.10.0-concepts-replication
title: Geo Replication
sidebar_label: Geo Replication
original_id: concepts-replication
---

Regardless of industries, when an unforeseen event occurs and brings day-to-day operations to a halt, an organization needs a well-prepared disaster recovery plan to quickly restore service to clients. However, a disaster recovery plan usually requires a multi-datacenter deployment with geographically dispersed data centers. Such a multi-datacenter deployment requires a geo-replication mechanism to provide additional redundancy in case a data center fails.

Pulsar's geo-replication mechanism is typically used for disaster recovery, enabling the replication of persistently stored message data across multiple data centers. For instance, your application is publishing data in one region and you would like to process it for consumption in other regions. With Pulsar’s geo-replication mechanism, messages can be produced and consumed in different geo-locations. 

The diagram below illustrates the process of [geo-replication](administration-geo.md). Whenever three producers (P1, P2 and P3) respectively publish messages to the T1 topic in three clusters, those messages are instantly replicated across clusters. Once the messages are replicated, two consumers (C1 and C2) can consume those messages from their clusters.

![A typical geo-replication example with full-mesh pattern](assets/full-mesh-replication.svg)

## Replication mechanisms

The geo-replication mechanism can be categorized into synchronous geo-replication and asynchronous geo-replication strategies. Pulsar supports both replication mechanisms.

### Asynchronous geo-replication in Pulsar

An asynchronous geo-replicated cluster is composed of multiple physical clusters set up in different datacenters. Messages produced on a Pulsar topic are first persisted to the local cluster and then replicated asynchronously to the remote clusters by brokers. 

![An example of asynchronous geo-replication mechanism](assets/geo-replication-async.svg)

In normal cases, when there are no connectivity issues, messages are replicated immediately, at the same time as they are dispatched to local consumers. Typically, end-to-end delivery latency is defined by the network round-trip time (RTT) between the data centers. Applications can create producers and consumers in any of the clusters, even when the remote clusters are not reachable (for example, during a network partition).

Asynchronous geo-replication provides lower latency but may result in weaker consistency guarantees due to the potential replication lag that some data hasn’t been replicated. 

### Synchronous geo-replication via BookKeeper

In synchronous geo-replication, data is synchronously replicated to multiple data centers and the client has to wait for an acknowledgment from the other data centers. As illustrated below, when the client issues a write request to one cluster, the written data will be replicated to the other two data centers. The write request is only acknowledged to the client when the majority of data centers (in this example, at least 2 data centers) have acknowledged that the write has been persisted. 

![An example of synchronous geo-replication mechanism](assets/geo-replication-sync.svg)

Synchronous geo-replication in Pulsar is achieved by BookKeeper. A synchronous geo-replicated cluster consists of a cluster of bookies and a cluster of brokers that run in multiple data centers, and a global Zookeeper installation (a ZooKeeper ensemble is running across multiple data centers). You need to configure a BookKeeper region-aware placement policy to store data across multiple data centers and guarantee availability constraints on writes.

Synchronous geo-replication provides the highest availability and also guarantees stronger data consistency between different data centers. However, your applications have to pay an extra latency penalty across data centers.


## Replication patterns

Pulsar provides a great degree of flexibility for customizing your replication strategy. You can set up different replication patterns to serve your replication strategy for an application between multiple data centers.

### Full-mesh replication

Using full-mesh replication and applying the [selective message replication](administration-geo.md/#selective-replication), you can customize your replication strategies and topologies between any number of datacenters.

![An example of full-mesh replication pattern](assets/full-mesh-replication.svg)

### Active-active replication

Active-active replication is a variation of full-mesh replication, with only two data centers. Producers are able to run at any data center to produce messages, and consumers can consume all messages from all data centers.

![An example of active-active replication pattern](assets/active-active-replication.svg)

For how to use active-active replication to migrate data between clusters, refer to [here](administration-geo.md/#migrate-data-between-clusters-using-geo-replication).

### Active-standby replication

Active-standby replication is a variation of active-active replication. Producers send messages to the active data center while messages are replicated to the standby data center for backup. If the active data center goes down, the standby data center takes over and becomes the active one. 

![An example of active-standby replication pattern](assets/active-standby-replication.svg)

### Aggregation replication

The aggregation replication pattern is typically used when replicating messages from the edge to the cloud. For example, assume you have 3 clusters in 3 fronting datacenters and one aggregated cluster in a central data center, and you want to replicate messages from multiple fronting datacenters to the central data center for aggregation purposes. You can then create an individual namespace for the topics used by each fronting data center and assign the aggregated data center to those namespaces.

![An example of aggregation replication pattern](assets/aggregation-replication.svg)
