# PIP-63: Readonly Topic Ownership Support

People usually use Pulsar as an event-bus or event center to unify all
their message data or event data.
One same set of event data will usually be shared across multiple
applications. Problems occur when the number of subscriptions of same topic
increased.

- The bandwidth of a broker limits the number of subscriptions for a single
topic.
- Subscriptions are competing for the network bandwidth on brokers. Different
subscriptions might have different levels of severity.
- When synchronizing cross-city message reading, cross-city access needs to
be minimized.

This proposal is proposing adding read-only topic ownership support. If
Pulsar supports read-only ownership, users can then use it to set up a (few)
separated broker clusters for read-only, to segregate the consumption
traffic by their service severity. And this would also allow Pulsar
supporting a large number of subscriptions.

## Changes
There are a few key changes for supporting read-only topic ownership.

- how does readonly topic owner read data
- how does readonly topic owner keep metadata in-sync
- how does readonly topic owner handle acknowledgment

The first two problems have been well addressed in DistributedLog. We can
just add similar features in the managed ledger.

### How readonly topic owner read data

In order for a readonly topic owner keep reading data in a streaming way,
the managed ledger should be able to refresh its LAC.  The easiest change
is to call `readLastAddConfirmedAsync` when a cursor requests entries
beyond existing LAC. A more advanced approach is to switch the regular read
entries request to bookkeeper’s long poll read requests. However long poll
read requests are not supported in the bookkeeper v2 protocol.

Required Changes:

- Refresh LastAddConfirmed when a managed cursor requests entries beyond
known LAC.
- Enable `explicitLac` at the managed ledger. So the topic writable owner will
periodically advance LAC, which will make sure readonly owner will be able
to catch with the latest data.

### How readonly topic owner keep metadata in-sync

Ledgers are rolled at a given interval. Readonly topic owner should find a
way to know the ledgers has been rolled. There are a couple of options.
These options are categorized into two approaches: notification vs polling.

*Notification*

A) use zookeeper watcher. Readonly topic owner will set a watcher at the
managed ledger’s metadata. So it will be notified when a ledger is rolled.
B) similar to A), introduce a “notification” request between readonly topic
owner and writable topic owner. Writable topic owner notifies readonly
topic owner with metadata changes.

*Polling*

C) Readonly Broker polling zookeeper to see if there is new metadata,
*only* when LAC in the last ledger has not been advanced for a given
interval. Readonly Broker checks zookeeper to see if there is a new ledger
rolled.
D）Readonly Broker polling new metadata by reading events from system topic of
write broker cluster, write broker add the ledger meta change events to the
system topic when mledger metadata update.

Solution C) will be the simplest solution to start with

### How does readonly topic owner handle acknowledgment

Currently, Pulsar deploys a centralized solution for managing cursors and
use cursors for managing data retention. This PIP will not change this
solution. Instead, readonly topic owner will only maintain a cursor cache,
all the actual cursor updates will be sent back to the writable topic
owner.

This requires introducing a set of “cursor” related RPCs between writable
topic owner and readonly topic owners.

- Read `Cursor` of a Subscription

So readonly topic owner will handle following requests using these new
cursor RPCs

- Subscribe : forward the subscribe request to writable topic owner. Upon
successfully subscribe, readonly topic owner caches the corresponding
cursor.
- Unsubscribe: remove cursor from cursor cache, and forward the unsubscribe
request to writable topic owner.
- Consume: when a consumer is connected, it will then `read` the cursor
from writable topic owner and cache it locally.
- Ack: forward the ack request to the writable topic owner, and update the
cursor locally in the cache.

## Compatibility, Deprecation and Migration Plan
Since most of the changes are internally changes to managed ledger, and it
is a new feature which doesn’t change pulsar’s wire protocol and public
api. There is no backward compatibility  issue.

It is a newly added feature. So there is nothing to deprecate or migrate.

## Test Plan
- Unit tests for each individual change
- Integration tests for end-to-end pipeline
- Chaos testing to ensure correctness
- Load testing for ensuring performance

## Rejected Alternatives
### Use Geo Replication to replicate data between clusters

A simplest alternative solution would be using Pulsar’s built-in
geo-replication mechanism to replicate data from one cluster to the other
cluster.

#### Two completely separated clusters

The idea is pretty straightforward - You created two separated clusters,
one cluster is for your online services -  `Cluster-A`, while the other
cluster is for your analytical workloads - `Cluster-B`.  `ClusterA` is used
for serving both write (produce) and read (consume) traffic, while
`ClusterB` is used for serving readonly (consume) traffic. Both `Cluster-A`
and `Cluster-B` have their own zookeeper cluster, bookkeeper cluster, and
brokers. In order to make sure a topic’s data can be replicated between
`Cluster-A` and `Cluster-B`, we need do make sure `Cluster-A` and
`Cluster-B` sharing same configuration storage. There are two approaches to
do so:

a) a completely separated zookeeper cluster as configuration storage.

In this approach, everything is completely separated. So you can treat
these two clusters just as two different regions, and follow the
instructions in [Pulsar geo-replication · Apache Pulsar](
http://pulsar.apache.org/docs/en/administration-geo/) to setup data
replication between these two clusters.

b) `ClusterB` and `ClusterA` share same configuration storage.

The approach in a) requires setting up a separate zookeeper cluster as
configuration storage. But since `ClusterA` and `ClusterB` already have
their own zookeeper clusters, you don’t want to setup another zookeeper
cluster. You can let both `ClusterA` and `ClusterB` use `ClusterA`’s
zookeeper cluster as the configuration store. You can achieve it using
zookeeper’s chroot mechanism to put configuration data in a separate root
in `ClusterA`’s zookeeper cluster.

For example:

- Command to initialize `ClusterA`’s metadata

```
$ bin/pulsar initialize-cluster-metadata \
  --cluster ClusterA \
  --zookeeper zookeeper.cluster-a.example.com:2181 \
  --configuration-store
zookeeper.cluster-a.example.com:2181/configuration-store \
  --web-service-url http://broker.cluster-a.example.com:8080/ \
  --broker-service-url pulsar://broker.cluster-a.example.com:6650/
```

- Command to initialize `ClusterB`’s metadata
```
$ bin/pulsar initialize-cluster-metadata \
  --cluster ClusterB \
  --zookeeper zookeeper.cluster-b.example.com:2181 \
  --configuration-store
zookeeper.cluster-a.example.com:2181/configuration-store \
  --web-service-url http://broker.cluster-b.example.com:8080/ \
  --broker-service-url pulsar://broker.cluster-b.example.com:6650/
```

#### Shared bookkeeper and zookeeper cluster, but separated brokers

Sometimes it is unaffordable to have two completely separated clusters. You
might want to share the existing infrastructures, such as data storage
(bookkeeper) and metadata storage (zookeeper). Similar as the b) solution
described above, you can use zookeeper chroot to achieve that.

Let’s assume there is only one zookeeper cluster and one bookkeeper
cluster. The zookeeper cluster is `zookeeper.shared.example.com:2181`.
You have two clusters of brokers, one cluster of broker is `
broker-a.example.com`, and the other broker cluster is `broker-b.example.com`.
So when you create the clusters, you can use `
zookeeper.shared.example.com:2181/configuration-store` as the shared
configuration storage, and use `
zookeeper.shared.example.com:2181/cluster-a`for `ClusterA`’s local metadata
storage, and use `zookeeper.shared.example.com:2181/cluster-b` for
`ClusterB`’s local metadata storage.

This would allows you have two “broker-separated” clusters sharing same
storage cluster (both zookeeper and bookkeeper).

No matter how the physical clusters are setup, there is a downside of using
geo-replications for isolating the online workloads and analytics workloads
- data has to be replicated at least twice, if you have configured pulsar
topics to store data in 3 replicas, you will end up have at least 6 copies
of data. So “geo-replication” might not be ideal for addressing this use
case.
