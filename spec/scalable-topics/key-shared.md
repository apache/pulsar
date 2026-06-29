# Scalable Key-Shared Consumption (Entry-Bucketing)

**Status:** Draft (targeting Stable)

> **Normative, optional capability.** This feature is specified by [PIP-486](../../pip/pip-486.md) and is
> a **normative** part of this specification — not Experimental. Implementing entry-bucketing is an
> **optional** client capability: a client need not implement it, but a client that does MUST satisfy the
> requirements here, and MUST gate the behavior on observed broker support (§9). Where this document and
> PIP-486 differ, PIP-486 is the source of truth until this document is finalized.

This document overlays the Stable specification with **entry-bucketing**: the mechanism that lets a
queue-style subscription deliver messages with **per-key affinity** (one consumer per key) on a scalable
topic, *without* coupling the producer's batching to the consumer's mode.

## 1. Motivation (informative)

The Stable [Queue consumer](client-api.md#42-queue-consumer-parallel-broker-managed) provides parallel
delivery with **no** key affinity. Classic Pulsar can provide key affinity (`Key_Shared`) but only by
constraining producer batching. Entry-bucketing removes that coupling: the producer stamps a small,
cleartext routing tag per stored entry, and the broker routes whole entries to a single consumer by that
tag — no per-message hashing, no decompression, no producer/consumer batching coupling.

## 2. The bucket

A **bucket** is an *intra-segment* routing unit, independent of the segment hash ring.

- Within a segment, a key maps to a bucket by an **independent** hash `hashB` — a different function (or
  salt) from the segment-routing hash: `bucket(key) = hashB(key) mod N`. Independence is REQUIRED so the
  keys a segment actually receives spread evenly across its buckets regardless of the segment's
  ring-range.
- `N` (the segment's bucket count) is **chosen per segment** and is **immutable for that segment's
  life**. Changing a segment's `N` is done by *rebucket rollover* (§5), never by mutating a live segment.
- A bucket is owned by **exactly one** consumer at a time within a subscription; this is the source of
  key affinity. A consumer MAY own several buckets.

`N` is bounded by a configurable per-segment maximum (`N_max`). It is decoupled from the topic's segment
count: segment scaling and bucket scaling are orthogonal axes.

## 3. Producer: entry-bucketing

A producer that implements entry-bucketing MUST:

- Batch such that **every stored entry (batch) belongs to a single bucket** — i.e. group messages by
  `bucket(key)` within each destination segment.
- Stamp, in the entry's **cleartext outer metadata**, the bucket count it used and the bucket id: a pair
  `(bucket_count, bucket_id)` with `bucket_id ∈ [0, bucket_count)`. (Equivalently, the `hashB` sub-range
  the bucket denotes — the exact wire encoding is in [Wire Protocol](wire-protocol.md).)
- Use the **broker-advertised** `N` for each segment, discovered from the topic layout. `N` is read once
  per segment (it is immutable for that segment).

A producer MUST NOT be required to disable or alter batching to enable key-shared consumption; that is
the entire point. The batching cost of bucketing scales with `1/N`, so a producer with few consumers
uses a small `N` (good batching) and a producer feeding many consumers uses a larger `N`.

## 4. Consumer: key-shared dispatch

A subscription operating in key-shared mode delivers with **per-key affinity**:

- The broker routes each entry to the single consumer that currently owns the entry's bucket, by reading
  the cleartext `bucket_id` — **no per-message key hashing and no payload decompression**. Because each
  entry belongs to one bucket and each bucket has one owner, an entry is delivered to **exactly one**
  consumer and is never split.
- Affinity holds: all messages for a key share one bucket and therefore one consumer.
- Scaling consumers up or down (within a segment's fixed `N`) reassigns buckets among consumers. During a
  reassignment handoff the broker MUST preserve per-key order by withholding a moving bucket until the
  prior owner's in-flight messages for it are drained, then handing it to the new owner. A consumer MUST
  NOT receive messages for a bucket it does not currently own; **no consumer-side filtering is required
  in steady state**.

The number of consumers that can share a segment is bounded by that segment's `N`.

## 5. Changing N: rebucket rollover

Because a segment's `N` is immutable, increasing or decreasing the per-segment bucket count is performed
as a **rebucket rollover**: the controller seals the segment and creates a successor with the **same
hash range** but a new `N`. The sealed predecessor drains under its old `N`; the successor takes new
writes under the new `N`. This reuses the seal → successor → producer-redirect flow of an ordinary split
(with an unchanged range), so per-key order across the change is preserved by the existing mechanism.

A client therefore never observes two different `N` values for one segment; a new `N` arrives only as a
new (same-range) successor segment, picked up through the normal layout-change handling.

## 6. The segments-vs-N lever (informative)

Total consumer parallelism is `segments × N`, but a lower `N` means fewer, fuller batches.
Implementations and operators therefore prefer **adding segments** (each at `N = 1`, best batching) and
raise `N` only when more segments are not the right answer — e.g. at the maximum segment count, for
low-throughput topics that should not materialize many segments, or to drain a sealed segment's backlog
across several consumers. The first segment of a new topic MAY default to a small `N > 1` (e.g. 4) to
allow immediate fan-out before any split.

## 7. Wire additions

Entry-bucketing adds:

- Optional `MessageMetadata` fields carrying `(bucket_count, bucket_id)` (or the equivalent `hashB`
  range) — see [Wire Protocol](wire-protocol.md). They are inert on classic topics and ignored by
  brokers without the feature.
- The per-segment bucket count `N` in the topic layout, so producers can bucket and the broker can
  validate. A producer's stamped `bucket_count` that disagrees with the segment's authoritative `N`
  indicates a buggy producer; the broker MAY reject such a publish.

## 8. Encryption

Because routing uses only the cleartext `bucket_id` and never reshapes an entry, key-shared consumption
works for end-to-end-encrypted topics: a single-bucket entry routes to its one owner without the broker
decrypting or slicing it.

## 9. Conformance

Entry-bucketing is a normative but **OPTIONAL** client capability. A client that omits it is conformant.
A client that implements it MUST satisfy §3–§4 for interoperability, and MUST gate the behavior on
observed broker support so that it interoperates safely with brokers that lack the feature.
