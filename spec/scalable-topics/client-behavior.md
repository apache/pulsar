# Implementation Requirements

**Status:** Draft (targeting Stable)

This document specifies what a conformant client MUST do **internally** to realize the
[Client API](client-api.md) contract over the [Wire Protocol](wire-protocol.md). The API states the
observable guarantees; this document states the mechanisms required to provide them. *Segments* are an
internal concept (see [Terminology](terminology.md)) and appear here but never in the application-facing
API.

## 1. Layout tracking

- A client MUST open a DAG-watch session ([Wire Protocol](wire-protocol.md) §4) for each topic it
  produces to or consumes from with a queue consumer, and maintain a local copy of the current
  `ScalableTopicDAG`.
- A client MUST apply a pushed layout only if its `epoch` is greater than the last applied epoch.
- A client MUST react to layout changes (split/merge, broker reassignment) **transparently** — opening
  and closing per-segment producers/consumers as the active set changes — without surfacing the change
  to the application.

## 2. Producer routing

For each message, a client MUST select the destination segment by this algorithm (so every client —
including the v4 partitioned-topic compatibility path — routes a key identically):

1. **Keyed:** `hash = Murmur3_32(key_utf8) & 0xFFFF`; select the single `ACTIVE` segment whose
   `[hash_start, hash_end]` contains `hash`. It is an internal error if no active segment covers the
   hash (the active set MUST always tile the ring).
2. **Keyless:** round-robin across the active segments.
3. **Migration (all-legacy layout):** when every active segment is legacy, route a keyed message by
   `signSafeMod(Murmur3_32(key_utf8), N)` over the `N` legacy segments (v4 partitioned-topic routing).

A client MUST maintain at most one underlying producer per active segment, created **lazily** on first
send to that segment. Under an *exclusive* access mode, a client MUST eagerly attach to every active
segment at create time, so a collision surfaces from *CreateProducer* rather than first send.

## 3. Ordering guarantees (producer side)

- A client MUST publish all messages for one key in *Send*-call order, and MUST preserve that order
  across a split/merge: when the target segment seals, the key's writes move to the active successor
  without reordering.
- For asynchronous sends, a client MUST preserve per-segment dispatch order = caller call order — a
  later message MUST NOT enter a segment's send queue before an earlier one.

## 4. Transparent segment-gone retry

When the target segment is sealed or terminated between routing and send (a split/merge, or a migration
cutover), a client MUST NOT fail the send. It MUST:

1. drop the stale per-segment producer,
2. wait (bounded backoff) for the DAG watch to deliver the new layout,
3. re-route and retry,

up to a bounded number of attempts (the reference client uses 10). Only on exhausting the attempts MUST
the send fail. This retry is invisible to the application.

## 5. Consumer fan-out and multiplexing

A client MUST present each consumer as a single logical stream while reading from multiple segments:

- **Queue consumer:** subscribe (Shared) to **all** active and sealed segments of the topic; multiplex
  deliveries into one receive queue; route each individual ack/nack to the correct per-segment consumer
  using the segment id carried in the message identifier; reconcile the per-segment subscription set
  toward the latest layout as it changes.
- **Stream consumer:** maintain one per-segment subscription (Exclusive) for each **assigned** segment
  ([Wire Protocol](wire-protocol.md) §6.2); multiplex into one receive queue.
- **Checkpoint consumer:** maintain one per-segment Reader for each segment it reads (all segments when
  ungrouped; assigned segments when grouped); track read positions client-side.

## 6. Stream cumulative ack (position vector)

The stream consumer's cumulative ack MUST advance **all** segments, not just the one the acked message
came from. A client MUST:

- record, for each delivered message, a **position vector** — a snapshot of the latest delivered message
  identifier of every segment at the moment the message was enqueued; and
- on a cumulative ack, cumulatively acknowledge every segment up to the positions recorded in that
  message's vector.

For a **namespace** stream consumer the vector spans topics and segments (`{topic → {segmentId →
messageId}}`), and the ack fans out to every per-topic consumer accordingly.

Acknowledgment is **always explicit**. Closing the consumer, and a topic leaving the matching set, MUST
NOT acknowledge anything on the application's behalf: a removed topic's per-topic consumer is simply
detached, and any message that was delivered but not yet acknowledged is redelivered if the topic is
later re-added (**at-least-once**). If a cumulative ack's vector references a topic that has since left
the set, that topic's slice is skipped.

## 7. Checkpoints

A *checkpoint* MUST capture an atomic snapshot of the per-segment read positions of the consumer's
segment set, serialized to an opaque value. Restoring MUST seek each segment's reader to its saved
position. Checkpoint capture and restore involve no broker round-trip and no acknowledgment.

## 8. Assignment session (ordered / grouped consumers)

A client MUST:

- discover the controller leader from the layout (`controller_broker_url`) and connect to it directly
  (scalable-topic URIs are not resolvable via standard lookup);
- register with `CommandScalableTopicSubscribe`, attach to exactly the assigned segments, and on each
  `CommandScalableTopicAssignmentUpdate` apply the diff (attach added, detach removed) only if its
  `layout_epoch` is ≥ the last applied;
- on connection loss, re-run lookup → connect → register → subscribe with backoff; within the
  controller grace period the same assignment returns unchanged, past it the client applies the
  rebalanced diff.

## 9. Resilience and resource lifetime

- A producer/consumer MUST survive broker disconnects, failovers, reassignment, and layout changes by
  reconnecting and re-syncing internally; it MUST remain valid (the application is not required to
  recreate it — see [Client API](client-api.md) §2).
- A client SHOULD coalesce concurrent reconcile/reconnect attempts so a single layout or connection
  event does not trigger redundant work.

## 10. Concurrency and thread-safety

- Producers, consumers, and the client MUST be safe for concurrent use from multiple application threads.
- A client MUST NOT block a network/IO thread on an operation that depends on that same thread's
  response (e.g. creating a per-segment producer whose lookup completes on the IO thread); such paths
  MUST be chained asynchronously.
- Buffers and per-segment resources MUST be released on close; close MUST be idempotent and MUST complete
  in-flight operations (graceful) per [Client API](client-api.md) §2.
