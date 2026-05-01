# PIP-468: Multi-topic consumers via namespace + property filters

## Goal

Let `QueueConsumer` and `StreamConsumer` subscribe to the union of scalable topics
in a namespace that match a (possibly empty) set of property filters. The
matching set must follow live: when topics enter or leave the filter, the
consumer attaches/detaches automatically. Reliable across reconnects and broker
bounces.

Out of scope: `CheckpointConsumer` (stays single-topic), partitioned and
non-partitioned legacy topics.

## Protocol

New broker command family, parallel to the existing `DagWatchSession` /
`ScalableConsumerSession`. Lives in `ClientCnx.scalableTopicsWatchers` keyed by
watch id.

```
CommandWatchScalableTopics {
    request_id
    watch_id
    consumer_name             // future namespace-coordinator hook
    namespace                 // tenant/ns
    property_filters: Map<String, String>   // empty == match all
}
CommandWatchScalableTopicsClose { watch_id }

ScalableTopicsUpdate {
    watch_id
    event: oneof {
        Snapshot { topics: [string] }     // initial + every reconnect resync
        Diff     { added: [string], removed: [string] }
    }
}
```

Wire size: snapshot bounded by the 5 MB frame limit. Defer pagination — we'll
revisit if a namespace ever exceeds.

## Reliability

Resync-on-reconnect, no durable event log on the broker.

1. **Subscribe.** Broker registers a metadata listener on `/topics/<tenant>/<ns>`
   *first*, computes the initial filtered set, emits `Snapshot{topics}`,
   populates server-side `currentSet`. Events that arrived during snapshot
   computation are replayed after via the same dedup logic the deltas use.
2. **Steady state.** Each metadata event is filter-evaluated. If membership
   changes relative to `currentSet`, broker emits `Diff{added, removed}` and
   updates `currentSet`. Server-side coalescing window (~50–100 ms) batches
   nearby events into one `Diff`.
3. **Disconnect.** Broker drops the session.
4. **Reconnect with hash short-circuit.** Client maintains a hash over its
   current set (CRC32C of the sorted topic names — same function used by
   `CommandGetTopicsOfNamespace`). On reconnect it re-opens the watch and
   passes that hash via `CommandWatchScalableTopics.current_hash`. The broker
   computes the same hash over its freshly evaluated set:
   - **Hash matches** → broker emits **nothing**. The watch is live, the
     client's local state is correct, future deltas flow as usual.
   - **Hash differs** → broker emits a fresh `Snapshot`. Client reconciles
     locally — anything in the new snapshot it didn't know about → open
     per-topic consumer; anything it knew but missing → close + flush.

   First subscribe sends an empty / absent hash, which the broker treats as
   "no prior state" and unconditionally emits the initial `Snapshot`.

Properties: idempotent (snapshot is full-set replace; diffs are set ops);
self-healing across any disconnect duration; no broker affinity (any broker can
serve, every broker has the same metadata events). For the common short-blip
reconnect where membership didn't change, the wire cost collapses to a single
inbound `WatchScalableTopics` frame and zero outbound.

Apply order on `Diff`: `removed` before `added` — covers rapid remove-then-add
of the same topic name.

## Filter evaluation: broker-side

Initial set is computed via the existing `findScalableTopicsByPropertiesAsync`.
Each metadata event:
- `Created` / `Modified`: read the new value, evaluate filter, emit `Diff` if
  membership changed vs `currentSet`.
- `Deleted`: no new value; emit `Removed` if topic was in `currentSet`.

Cost: one filter evaluation per metadata event per watcher. Filters are tiny;
fine.

## Per-topic consumer failure handling

When `Diff{added: [t]}` fires, the multi-topic consumer opens a
`PerTopicConsumer` for `t`. If the per-topic subscribe fails (broker transient,
topic auth, etc.), retry forever with exponential backoff capped at 15–30 min.
Surface a single warn log per topic per backoff cycle, no user-visible error.
Matches v4 consumer reconnect semantics.

## Consumer-side wiring

```
MultiTopicConsumer
  ├─ ScalableTopicsWatcher (long-lived broker session)
  ├─ Map<TopicName, PerTopicConsumer>      // open/close on Diff
  ├─ shared LinkedTransferQueue<MessageV5> // multiplex from per-topic queues
  └─ event handler:
       Snapshot(topics)        → diff against current keyset; close stale, open new
       Diff(added, removed)    → close removed (flush acks first), then open added
       per-topic add failure   → background retry with exp backoff (15–30 min cap)
```

`PerTopicConsumer`:
- For QueueConsumer: existing `ScalableQueueConsumer`.
- For StreamConsumer: existing `ScalableStreamConsumer`.

Same subscription name on every topic. The existing per-topic
`SubscriptionCoordinator` handles per-topic segment assignment.

## Ack routing

### QueueConsumer (no cumulative ack)

Each enqueued `MessageV5` already carries `(topic, segmentId, msgId)`. On
`acknowledge(msg)`:

```java
PerTopicQueueConsumer ptc = perTopicConsumers.get(msg.topic());
if (ptc != null) {
    ptc.acknowledge(msg);   // delegates to existing per-segment routing
}
```

No position vector, no snapshots.

### StreamConsumer (cumulative ack)

State on the multi-topic consumer:

```java
Map<TopicName, Map<SegmentId, MessageId>> latestDeliveredPerTopicSegment;
```

On per-topic message arrival into the multiplexed queue:
1. Atomically update `latestDeliveredPerTopicSegment[topic][seg] = msgId`.
2. Snapshot the whole map (deep copy).
3. Attach snapshot to `MessageV5` before enqueue.

On `acknowledgeCumulative(msg)`:

```java
for ((topic, vector) : msg.snapshot) {
    PerTopicConsumer ptc = perTopicConsumers.get(topic);
    if (ptc != null) {
        ptc.ackUpToVector(vector);   // best-effort, async — fire and forget
    }
    // ptc null = topic was Removed since msg was enqueued; flushed at Remove
}
```

The position vector means:
- For msg's own topic: vector covers up to `msg` itself.
- For every other topic: vector covers whatever was last delivered from that
  topic at the moment `msg` was enqueued. Topics with nothing delivered before
  `msg`: empty / absent inner map → no-op.

`ackUpToVector(Map<SegmentId, MessageId>)` is the existing single-topic
cumulative-ack walk extracted as a public entry point on
`ScalableStreamConsumer`.

### Topic Removed mid-stream

Cleanup order: flush pending acks up to `latestDelivered[topic]` → close
per-topic consumer → remove from the map. Future messages won't reference the
topic in their snapshots. If the topic re-appears later (re-Added), a fresh
per-topic consumer subscribes and resumes from broker-side cursor (Stream) or
new shared subscription state (Queue).

### Cost note (StreamConsumer)

Snapshot size = O(topics × segments-per-topic-currently-assigned). For 100
topics × 1 segment each ≈ 100 entries ≈ a few KB per message. Fine for the
regime we're targeting; revisit if hot.

## API

Add to `QueueConsumerBuilder<T>` and `StreamConsumerBuilder<T>`:

```java
Builder<T> namespace(String namespace);
Builder<T> namespace(String namespace, Map<String, String> propertyFilters);
```

Mutually exclusive with `.topic(name)`; calling both fails at `subscribe()`.
Filters can't be set outside these methods, so users can't accidentally pass
filters without a namespace.

While there:
- Drop `topicPattern(...)` from the v5 builders.
- Tighten `topic(String...)` → `topic(String)`.

## Cross-topic load balancing — deferred

Today: every multi-topic consumer in `(namespace, filter, subscriptionName)`
subscribes to *all* matching topics. Each topic's per-topic
`SubscriptionCoordinator` independently picks one consumer per segment — for
StreamConsumer (Exclusive) the assignment is randomized across consumers, in
expectation roughly balanced but not deterministic.

Future: namespace-level `MultiTopicSubscriptionCoordinator` per
`(namespace, subscriptionName)` that:
- Persists `Map<TopicName, ConsumerSession>` for the group.
- On consumer attach: assigns a slice of the matching topic set.
- On topic Added/Removed: rebalances.
- Server-side filters the watcher's emitted set to the consumer's slice.

The hook in this design: `consumer_name` is part of `CommandWatchScalableTopics`
*now*, so the future coordinator has the identity it needs. Client code doesn't
change — the watcher just emits a constrained `Snapshot` / `Diff`.

## Authz

`CommandWatchScalableTopics`: requires namespace-level READ
(`NamespaceOperation.GET_TOPICS`, same as `listScalableTopics`). Per-topic
subscribe that follows: normal topic-level authz applies.

## Open question kept for later

Property change events that don't move a topic in/out of the filter are silent.
If a future use case needs them, add a `PropertiesChanged` variant.
