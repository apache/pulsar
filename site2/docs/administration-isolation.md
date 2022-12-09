---
id: administration-isolation
title: Pulsar isolation
sidebar_label: "Pulsar isolation"
---


In an organization, a Pulsar instance provides services to multiple teams. When organizing the resources across multiple teams, you want to make a suitable isolation plan to avoid resource competition between different teams and applications and provide high-quality messaging service. In this case, you need to consider resource isolation and weigh your intended actions against expected and unexpected consequences.

The multi-layer and segment-centric architecture and hierarchical resource management of Pulsar provide a solid foundation for isolation, which allows you to isolate resources in your desired manner, prevent resource competition, and attain stability.


## Isolation levels

Pulsar supports isolation at either of the following two levels or both. 
* [Broker-level isolation](administration-isolation-broker.md) divides brokers into different groups and assigns broker groups to different namespaces. In this way, you can bind topics in a namespace to a set of brokers that belong to specific groups.
* [Bookie-level isolation](administration-isolation-bookie.md) divides bookies into different racks/regions and assigns data replicas to bookies based on a specified data placement policy for disaster tolerance.

![Isolation levels](/assets/admin-isolation.svg)

:::tip

On top of [broker-level isolation](administration-isolation-broker.md) and [bookie-level isolation](administration-isolation-bookie.md), if you want to guarantee all the data that belongs to a namespace is stored in desired bookies, you can define and configure [bookie affinity groups](administration-isolation-bookie.md#configure-bookie-affinity-groups). See [shared BookKeeper cluster deployment](#shared-bookkeeper-cluster) for more details.

:::


## Deployments to achieve isolation within Pulsar

### Separate Pulsar clusters

The following illustration demonstrates the deployment of separate Pulsar clusters to achieve the highest-level isolation.

![Deployment of separate Pulsar clusters](/assets/isolation-1.png)

Here are some key points for understanding how it works:
- Separate Pulsar clusters use a shared [configuration store](concepts-architecture-overview.md#configuration-store).
- Each cluster exposes its service through a DNS entry point and makes sure a client can access the cluster through the DNS entry point. Clients can use one or multiple Pulsar URLs that the Pulsar cluster exposes as the service URL.
- Each Pulsar cluster has one or multiple brokers and bookies.
- Each Pulsar cluster has one metadata store, which can be separated into [Pulsar metadata store](concepts-architecture-overview.md#metadata-store) and [BookKeeper metadata store](https://bookkeeper.apache.org/docs/latest/getting-started/concepts/#metadata-storage). 

:::note

When using this approach, if you want to achieve namespace isolation, you need to specify a cluster for a namespace. The cluster must be in the allowed cluster list of the tenant. Topics under the namespace are assigned to this cluster. 

:::

### Shared BookKeeper cluster

The following illustration demonstrates the deployment of shared BookKeeper clusters to achieve isolation.

![Deployment of shared BookKeeper cluster](/assets/isolation-2.png)

Here are some key points for understanding how it works:
- Separate Pulsar clusters share a BookKeeper cluster and a [configuration store](concepts-architecture-overview.md#configuration-store).
- Each cluster exposes its service through a DNS entry point and makes sure a client can access the cluster through the DNS entry point. Clients can use one or multiple Pulsar URLs that the Pulsar cluster exposes as the service URL.
- Each Pulsar cluster has one or multiple brokers.
- Each Pulsar cluster has one metadata store. 

As illustrated below, all bookie groups use a shared BookKeeper cluster and a metadata store, and each [bookie affinity group](administration-isolation-bookie.md#configure-bookie-affinity-groups) has one or several bookies. You can specify one or multiple primary/secondary groups for a namespace. Topics under the namespace are created on the bookies in the primary group first and then created on the bookies in the secondary group.

![Storage isolation achieved by bookie affinity groups](/assets/isolation-3.png)

### Single Pulsar cluster

The following illustration demonstrates how to achieve isolation inside a single Pulsar cluster.

![Deployment of a single Pulsar cluster](/assets/isolation-4.png)

Here are some key points for understanding how it works:
- Each cluster exposes its service through a DNS entry point and makes sure a client can access the cluster through the DNS entry point. Clients can use one or multiple Pulsar URLs that the Pulsar cluster exposes as the service URL.
- Broker isolation is achieved by setting a [namespace isolation policy](administration-isolation-broker.md).

