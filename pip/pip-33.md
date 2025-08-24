# PIP-33: Replicated subscriptions

* **Status**: Merged
* **Author**: Ivan Kelly, Matteo Merli
* **Pull Request**:
* **Mailing List discussion**:
* **Release**: 2.4.0

## Goal

Provide a mechanism to keep subscription state in-sync, within a
sub-second timeframe, in the context of a topic that is being
asynchronously replicated across multiple geographical regions.

## Current state of affairs

Pulsar supports geo-replication feature, in which a topic can be
configured to be replicated across N regions, (eg: `us-west`, `us-east`
and `eu-central`).

The topic is presented as a virtual "global" entity in which messages
can be published and consumed from any of the configured cluster.

The only limitation is that subscriptions are currently "local" to the
cluster in which they are created. That is, no state for the subscription
is transferred across regions.

If a consumer reconnects to a new region, it will trigger the creation of a
new unrelated subscription, albeit with the same name. This subscription
will be created at the end of the topic in the new region (or at the
beginning, depending on configuration) and at the same time, the
original subscription will be left dangling in the previous region.

The main problem is that the message ids of the messages are not
consistent across different regions.

## Problem we're trying to solve

There are many scenarios in which it would be very convenient for
an application to have the ability to failover consumers from one
region to another.

During this failover event, a consumer should be able to restart
consuming from where it left off in the previous region.

Given that for the very nature of async replication, having the
exact position will be impossible, in most cases restarting "close"
to that point will be already good enough.

## Proposed solution

A Pulsar topic that is being geo-replicated can be seen as a collection
of partially ordered logs.

Since producers can publish messages on each of the regions, each region
can end up having a sequence of messages different from the others, though
messages from one particular region will be always stored in order.

The main idea is to create a consistent distributed snapshot to establish
an association between message ids from different clusters.

The snapshot of message ids will be constructed in a way such that:
 * For a given message `M1-a` (written or replicated into region `a`)
 * We have a set of associated message ids from other regions, eg. `M3-b`
   and `M1-c`
 * When a consumer has acknowledged all messages <= `M1-a`, it will
   imply that it also have received (and acknowledged) all messages in
   region `b` with message id <= `M3-b` and all messages in region `c`
   with message id <= `M1-c`
 * With this, when the "mark-delete" position (the cursor pointer) of
   a given subscription moves past the `M1-a` message in region `a`,
   the broker will be able to instruct brokers in `b` and `c` to
   update the subscription respectively to `M3-b` and `M1-c`.

These snapshots will be stored as "marker" messages in the topic itself
and they will be filtered out by broker before dispatching messages
to consumers.

Similarly, the snapshot themselves will be created by letting "marker"
messages flow inline through the replication channel.

### Advantages

 * The logic is quite simple and straightforward to implement
 * Low and configurable overhead when enabled
 * Zero overhead when disabled

### Limitations

 * The snapshots are taken periodically. Proposed default is every 1 second. That will
   mean that a consumer failing over to a different cluster can potentially receive
   1 second worth of duplicates.
 * For this proposal, we're only targeting to sync the "mark-delete" position (eg: offset),
   without considering the messages deleted out of order after that point. These will
   appear as duplicates after a cluster failover. In future, it might be possible to
   combine different techniques to track individually deleted messages as well.
 * The snapshots can only be taken if all involved clusters are available. This is to
   ensure correctness and avoid skipping over messages published in remote clusters that
   were not yet seen by consumers.
   The practical implication of this is that this proposal is useful to either:
    - Support consumer failover across clusters when there are no failures
    - Support one cluster failure and allow consumer to immediately recover from a different
      cluster.
   After one cluster is down, the snapshots will not be taken, so it will not be possible
   to do another consumer failover to another cluster (and preserve the position) until
   the failed cluster is either brought back online, or removed from the replication list.

## Proposed implementation

### Client API

Applications that want to enable the replication subscription feature
will be able to configure so when creating a consumer. For example:

```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("my-topic")
            .subscriptionName("my-subscription")
            .replicateSubscriptionState(true)
            .subscribe();
```

### Marker messages

Most of the implementation of replicated subscription is based on
establishing a set of points in the topic storage for each region
for which there's strict relation with each region's message ids.

To achieve that, the communication between brokers needs to be done
inline with the same flow of messages replicated across regions and
it will have to establish a new message id that can be referenced
from the other regions.

An additional usage of marker messages will be to store snapshot
information in a scalable way, so that we don't have to keep all
the snapshots together but rather we can reconstruct them while we
fetch the entries from BookKeeper (eg: when a consumer comes back
after a while and starts draining the backlog).

Essentially, marker messages will be a special class of messages that
are used by Pulsar for internal purposes and are stored inline
in the topic.

These messages will be identified on the `MessageMetadata` protobuf
definition with one additional field:

```protobuf
// Contains the enum value with the type of marker
// Each marker message will have a different format defined
// in protobuf
optional int32 marker_type = 18;
```

### Constructing a cursor snapshot

When the Pulsar broker is serving a topic for which at least one
subscription is "replicated", it will activate a periodic task to
create the cursor snapshot.

The frequency of this snapshot will be configurable in `broker.conf` and,
possibly, also as part of namespace policies (though that might not be
necessary in the first implementation).

#### Snapshot steps

##### Source region starts a snapshot

Broker in region `a` will start a new snapshot by writing locally a
marker message like:

```json
"ReplicatedSubscriptionsSnapshotRequest" : {
    "snapshot_id" : "444D3632-F96C-48D7-83DB-041C32164EC1",
    "source_cluster" : "a",
}
```

This marker will get replicated to all other clusters, eg: `b` and `c`.

When replicators in each of the other regions will get this marker
message, they will reply by sending another marker back to region `a`.

```json
"ReplicatedSubscriptionsSnapshotResponse" : {
    "snapshotId" : "444D3632-F96C-48D7-83DB-041C32164EC1",
    "cluster" : {
        "cluster" : "b",
        "message_id" : {
            "ledger_id" : 1234,
            "endtry_id" : 45678
        }
    }
}
```

Broker in region `a` will wait to receive all responses from `b` and `c`
and then it will finalize the snapshot.

The snapshot content will be like:

```json
{
    "snapshot_id" : "444D3632-F96C-48D7-83DB-041C32164EC1",
    "local_message_id" : {
        "ledger_id" : 192,
        "endtry_id" : 123123    
    },
    "clusters" : [
        {
            "cluster" : "b",
            "message_id" : {
                "ledger_id" : 1234,
                "endtry_id" : 45678
            }
        },
        {
            "cluster" : "c",
            "message_id" : {
                "ledger_id" : 7655,
                "endtry_id" : 13421
            }
        }
    ],
}
```

The `local_message_id` field will be set to the the message id (in region
`a`) of the last response that completed the snapshot.

Note, when there are more than 2 clusters involved, like in the above case
with cluster `a`, `b` and `c`, a second round of request-response will be
necessary, to ensure we are including all the message that might have
been exchanged between the remote clusters.

In this situation, for the snapshot we will be using:
 * For remote cluster, the message id reported in the 1st round
 * For `local_message_id` the id of the last response from the 2nd round

Typically there will be only one (or few) in progress snapshots. If a
region doesn't respond within a certain timeout period, the snapshot
will be aborted.

The reason we cannot use partial snapshot is that we might be missing
some of the messages that were originated from that missing region.

This is an example:
 1. `a` start a snapshot
 2. A message `M1-b` from `b` was replicated into `c` (and possibly not
     `a` or with a bigger delay)
 3. `c` returns a message id for the snapshot that includes `M1-b` (as
     replicated in `c`)

If `a` doesn't wait for the snapshot response from `b`, it would then
instruct `b` to skip the `M1-b` message.

As default behavior, to avoid any possible message loss, only completed
snapshot will be applied. In future some configuration or operational
tool could be provided to either:

 * Create partial snapshots after a certain time that a region has been
   disconnected. Eg: after few hours just move on and restart creating
   snapshot, on the assumption that a region might be completely lost.

 * Have a tool to manually re-enable the snapshots creations even in
   presence of failures.

### Storing snapshots

A topic with replicated subscriptions enabled, will be periodically
creating snapshots, for example every 1 or 10 seconds.

These snapshots need to be stored until the all the subscriptions have
moved past a certain point. Additionally, if time-based retention is
enabled, they would need to be stored for the same time as the underlying
data.

In normal scenario, when consumers are caught up with the publishers,
the number of active snapshots will be small, though it would be
increasing if a consumer starts lagging behind.

For this, the proposed solution is to store the snapshot as "marker"
messages in the topic itself, inline with the regular data. Similarly
to the other markers, these will not be propagated to clients and in
this cases they won't either be replicated to other regions.

Given this, each subscription will be able to keep a small cache of
these snapshots (eg: 10 to 100 items) and keep updating as the
subscription read cursor progresses through the topic.

### Updating subscription in remote regions

When a subscription moves the cursor ("mark-delete" position) forward,
it will lookup in the "replicated subscription snapshots cache" for a
snapshot with associated messageId that is <= to the current cursor
position.

If a snapshot matching the criteria is found, the broker will publish
a `ReplicatedSubscriptionsUpdate`:

```json
{
    "subscription_name" : "my-subscription",
    "clusters" : [
        {
            "cluster" : "b",
            "message_id" : {
                "ledger_id" : 1234,
                "endtry_id" : 45678
            }
        },
        {
            "cluster" : "c",
            "message_id" : {
                "ledger_id" : 7655,
                "endtry_id" : 13421
            }
        }
    ],
}
```

The "update" marker is written locally and replicated everywhere.

When a broker in the target region receives the marker message to
update the subscription, it will move the mark-delete cursor to the new
message id for the specific region.

If the subscription doesn't exist yet in that cluster, it will be
automatically created by the "update" marker.
