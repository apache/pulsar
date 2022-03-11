---
id: concepts-replication
title: Geo Replication
sidebar_label: Geo Replication
---

Regardless of the industry, when an unforeseen event occurs and brings day-to-day operations to a halt, an organization needs a well-prepared disaster recovery plan to recover as quickly as possible to continue to provide services to its clients. However, a disaster recovery plan usually requires a multi-datacenter deployment where datacenters are geographically dispersed. Such a multi-datacenter deployment requires a geo-replication mechanism to provide additional redundancy in case a data center fails or some other events cannot continue functioning.

Pulsar's [geo-replication](administration-geo.md) mechanism is typically used for disaster recovery, enabling messages to be produced and consumed in different geo-locations. For instance, your application may be publishing data in one region or market and you would like to process it for consumption in other regions or markets.

Pulsar supports both synchronous geo-replication (via Apache BookKeeper) and asynchronous geo-replication (configured at the broker level), and also provides tenants a great degree of flexibility for customizing their replication strategy. You can set up different replication patterns to serve your replication strategy for an application between multiple data centers.


## Replication patterns

### Full-mesh replication

![](assets/full-mesh-geo-replication.png)

### Active-active replication

![](assets/full-mesh-geo-replication.png)

### Active-standby replication

![](assets/full-mesh-geo-replication.png)

### Aggregation replication

![](assets/full-mesh-geo-replication.png)