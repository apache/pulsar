# Scalable Key-Shared Consumption (Entry-Bucketing)

**Status:** Draft (targeting Stable)

> **Normative.** This feature is specified by [PIP-486](../../pip/pip-486.md) and is a **normative**
> part of this specification — not Experimental. How much of it a client must implement depends on what
> the client offers (§9): the consumer side is REQUIRED for Stream consumers (the controller may share a
> segment at any time), while producer-side bucketing is exchangeable for not batching. Where this
> document and PIP-486 differ, PIP-486 is the source of truth until this document is finalized.

This document overlays the Stable specification with **entry-bucketing**: the mechanism that lets an
ordered (Stream) subscription scale beyond one consumer per segment with **per-key affinity** (one
consumer per key), *without* coupling the producer's batching to the consumer's mode.

## 1. Motivation (informative)

The Stable [Stream consumer](client-api.md) scales by segments: each segment has one Exclusive owner, so
parallelism is capped at the segment count. Classic Pulsar can go finer (`Key_Shared`) but only by
constraining producer batching, because a batch may mix keys owned by different consumers. Entry-bucketing
removes that coupling: the producer groups its batches by a small routing tag and stamps it in cleartext
entry metadata, and the broker routes **whole entries** to a single consumer by that tag — no per-message
hashing, no decompression, no producer/consumer batching coupling, and no client-side handoff protocol.

## 2. The bucket

A **bucket** is an *intra-segment* routing unit, independent of the segment hash ring.

- A key is hashed **once** with `Murmur3_32` over its UTF-8 bytes; the raw (unmasked) 32-bit hash splits
  into two independent 16-bit halves: the **high half** routes segments (see
  [Wire Protocol](wire-protocol.md) §5) and the **low half** is the key's **bucket hash**. Using disjoint
  bits of one hash is what makes bucket spread independent of the segment's ring-range: the keys a
  segment receives still cover the full 16-bit bucket-hash space.
- A segment's buckets are defined by a **boundary list**: ascending, inclusive, contiguous ranges tiling
  the bucket-hash space `0x0000`–`0xFFFF`. A key belongs to the bucket whose range contains its bucket
  hash. Ranges need not be equal-width (boundaries MAY be placed to balance buckets by traffic).
- The boundary list is **chosen per segment** and is **immutable for that segment's life**. Changing it
  is done by *rebucket rollover* (§5), never by mutating a live segment. It is advertised in the topic
  layout (`SegmentInfoProto.entry_bucket_splits` — the start hashes of buckets `1..N-1`; an empty list
  means a single bucket spanning the whole ring).
- A bucket is owned by **exactly one** consumer at a time within a subscription; this is the source of
  key affinity. A consumer MAY own several buckets.

The bucket count `N` (the boundary-list size) is decoupled from the topic's segment count: segment
scaling and bucket scaling are orthogonal axes.

## 3. Producer: entry-bucketing

A producer that implements entry-bucketing MUST:

- Batch such that **every stored entry (batch) belongs to a single bucket** — i.e. group messages by
  bucket within each destination segment, using the segment's advertised boundary list.
- Stamp, in the entry's **cleartext outer metadata**, the entry's **effective** bucket-hash range:
  `MessageMetadata` `entry_hash_min` / `entry_hash_max` — the smallest and largest bucket hash actually
  present among the entry's messages, both inclusive (16-bit). Because the entry is single-bucket this
  range necessarily lies within one bucket; it is the *tightest* bound, not the bucket's nominal
  boundaries. A single-message entry stamps `min = max =` that message's bucket hash.
- Stamp **always** — including on a single-bucket segment (`N = 1`); the stamp is still the entry's
  effective min/max, never a synthetic whole-ring range.
  The stamp records the entry's *effective* hash range at publish time, so it stays meaningful when the
  entry is later re-examined under a different layout (geo-replication into a cluster with different
  segment or bucket boundaries, or a rebucketed successor): if the stamped range falls entirely inside
  one target bucket, the entry still routes whole.

A producer MUST NOT be required to disable or alter batching to enable key-shared consumption; that is
the entire point. The batching cost of bucketing scales with `1/N`, so a topic with few consumers uses a
small `N` (good batching) and one feeding many consumers uses a larger `N`.

A producer that does **not** implement bucketing MUST NOT batch on a scalable topic: a multi-key batch
that straddles buckets is routed whole to one consumer, silently breaking per-key affinity for every
other consumer of the topic. Publishing each message as its own (possibly unstamped) entry is always
safe — the broker routes an unstamped entry by the message key's bucket hash.

## 4. Consumer: key-shared dispatch

Entry-bucketing extends the Stream consumer's controller assignment ([Wire Protocol](wire-protocol.md)
§6.2). Each assigned segment carries a `bucket_ranges` list that selects the **attach mode**:

- **Empty `bucket_ranges`** — the consumer is the segment's **sole owner**: it subscribes **Exclusive**,
  exactly as in the base protocol. This is the common case and keeps the single-owner fast path (no
  per-message pending-ack tracking, cumulative acks).
- **Non-empty `bucket_ranges`** — the segment is **shared by bucket**. The list is the segment's **full
  boundary list** (§2) — *not* a per-consumer slice — and is **identical for every sharer**. The
  consumer subscribes `Key_Shared` STICKY with the `KeySharedMeta.entryBucketDispatch` flag, declaring
  exactly this boundary list in the subscribe's `KeySharedMeta.hashRanges`.

The controller only decides **which consumers share a segment**; the bucket→consumer spread is computed
**broker-side** from the live membership of the subscription, deterministically (every sharer declares
the same boundaries, so the broker validates them and rejects a mismatch as a stale-layout error).

**Broker dispatch and handoff (normative for brokers, informative for clients).** The broker routes each
entry, whole, to the consumer owning its bucket — by the cleartext stamp, with no per-message key hashing
and no payload decompression. When membership changes (a sharer joins, leaves, or crashes), the broker
re-spreads buckets and preserves per-key order by **draining**: a moving bucket is withheld from its new
owner until every message the prior owner has in flight for that bucket is acknowledged, then flows to
the new owner in order. Consumers keep consuming throughout — they are never rejected, never re-subscribe,
and implement **no handoff protocol**; a bucket move is observable only as a pause on that bucket.

A client that implements entry-bucketing MUST, in addition:

- **Acknowledge individually on shared segments.** `Key_Shared` forbids cumulative acknowledgment. A
  client whose consumer API is cumulative (the Stream consumer's position-vector ack,
  [Implementation Requirements](client-behavior.md) §6) MUST translate it on shared segments by
  individually acknowledging every delivered-but-unacknowledged message at or before the acked position,
  per segment. On Exclusive (sole-owner) segments cumulative acknowledgment is used as normal.
- **Drain before the mode flip.** The only client-visible transition is a segment's `bucket_ranges`
  flipping between empty and non-empty (sole owner ⇄ shared). Applying it means closing the per-segment
  consumer and re-subscribing in the other mode; before closing, the client MUST *drain*: stop delivering
  new messages from that segment and wait until every already-delivered message of the segment is
  acknowledged. This makes the flip invisible to per-key order and prevents redelivery of prefetched
  messages. The same drain applies when a segment leaves the assignment entirely. (Draining is bounded by
  the application acknowledging what it was delivered; a client MUST NOT acknowledge on the application's
  behalf to force progress.)
- **Apply assignments idempotently.** Re-receiving an assignment with an unchanged `bucket_ranges` for a
  segment MUST NOT re-subscribe or otherwise disturb that segment.

The number of consumers that can usefully share a segment is bounded by that segment's bucket count; the
controller MAY leave surplus consumers idle.

## 5. Changing the boundaries: rebucket rollover

Because a segment's boundary list is immutable, changing it (more buckets, fewer, or re-placed
boundaries) is performed as a **rebucket rollover**: the controller seals the segment and creates a
successor with the **same hash range** but a new boundary list. The sealed predecessor drains under its
old boundaries; the successor takes new writes under the new ones. This reuses the seal → successor →
producer-redirect flow of an ordinary split (with an unchanged range), so per-key order across the change
is preserved by the existing mechanism.

A client therefore never observes two different boundary lists for one segment; new boundaries arrive
only as a new (same-range) successor segment, picked up through the normal layout-change handling.

## 6. The segments-vs-buckets lever (informative)

Total consumer parallelism is `segments × N`, but a lower `N` means fewer, fuller batches.
Implementations and operators therefore prefer **adding segments** (each at `N = 1`, best batching) and
raise `N` only when more segments are not the right answer — e.g. at the maximum segment count, for
low-throughput topics that should not materialize many segments, or to drain a sealed segment's backlog
across several consumers. The first segment of a new topic MAY default to a small `N > 1` (e.g. 4) to
allow immediate fan-out before any split.

## 7. Wire additions

Entry-bucketing adds no commands; it adds fields to existing structures (see
[Wire Protocol](wire-protocol.md)):

- `MessageMetadata.entry_hash_min` / `entry_hash_max` — the producer's bucket stamp (§3). Optional,
  inert on classic topics, and ignored by brokers without the feature.
- `SegmentInfoProto.entry_bucket_splits` — the segment's boundary list in the topic layout (§2), so
  producers can bucket and the broker can validate.
- `KeySharedMeta.entryBucketDispatch` — the subscribe flag selecting bucket dispatch, with the
  boundary list carried in the existing `KeySharedMeta.hashRanges` (§4). (These two fields use the
  proto's camelCase naming, unlike the snake_case fields above.)
- `ScalableAssignedSegment.bucket_ranges` — the controller's per-segment mode signal in the Stream
  assignment (§4).

## 8. Encryption

Because routing uses only the cleartext stamp and never reshapes an entry, key-shared consumption works
for end-to-end-encrypted topics: a single-bucket entry routes to its one owner without the broker
decrypting or slicing it. (An encrypting producer publishes each message as its own entry — encrypted
payloads are not batched — but still stamps each entry's bucket range.)

## 9. Conformance

What a client must implement follows from what it offers — the deciding fact is that segment sharing is
**controller-driven**: the registration carries no capability negotiation, so any Stream consumer may be
handed a shared segment. (*Informative:* negotiation is deliberately absent — entry-bucketing ships with
the initial scalable-topics protocol in Pulsar 5.0.0, so no Stream-capable SDK predates it; the consumer
side is simply part of the Stream consumer contract.)

- **Stream consumer:** a client that offers the Stream consumer MUST implement §4 (mode selection from
  `bucket_ranges`, individual acks on shared segments, drain before mode flips). The client-side surface
  is deliberately small — one subscribe flag plus the boundary list, no handoff protocol.
- **Producer:** a client SHOULD implement §3 (bucketing and stamping). A client that does not MUST NOT
  batch on scalable topics (§3) — unbatched, unstamped entries remain fully interoperable, at the cost
  of batching throughput and per-entry key hashing on the broker.
- **Queue and Checkpoint consumers** are unaffected by entry-bucketing; a client offering only those
  need implement nothing from this document.

A client MUST gate producer stamping on observed broker support (brokers without the feature ignore the
fields, so stamping is safe; the gate exists so clients do not bucket-batch — and shrink their batches —
for brokers that cannot use the stamps).
