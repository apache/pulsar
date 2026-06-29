# Conformance

**Status:** Draft (targeting Stable)

This document defines what it means for a client to **conform** to this specification, separates the
**required core** from **optional capabilities**, and gives a per-requirement checklist.

## 1. Definition

A **conformant client** implements the **required core** (§2) such that it interoperates with any
conformant Apache Pulsar broker and with conformant clients of other languages operating on the same
topics. A client MAY additionally implement any **optional capability** (§3); if it does, it MUST
implement that capability per its specification (partial implementation of a capability is
non-conformant).

The single hardest interoperability requirement is **identical routing**: a conformant client MUST
implement the producer routing algorithm ([Implementation Requirements](client-behavior.md) §2,
[Wire Protocol](wire-protocol.md) §5) exactly, so that any client routes a given key to the same segment
as any other.

## 2. Required core

A conformant client MUST provide all of:

| # | Requirement | Spec |
|---|-------------|------|
| C1 | Resolve a scalable topic and track its layout via the DAG-watch session, applying updates by epoch monotonicity. | [Wire](wire-protocol.md) §4, [Impl](client-behavior.md) §1 |
| C2 | Producer routing: keyed `Murmur3_32 & 0xFFFF` → covering active segment; keyless round-robin; all-legacy mod-N. | [Impl](client-behavior.md) §2 |
| C3 | Per-key publish ordering, preserved across split/merge; async per-segment call-order. | [API](client-api.md) §3.2, [Impl](client-behavior.md) §3 |
| C4 | Transparent segment-gone retry on producing. | [Impl](client-behavior.md) §4 |
| C5 | Deduplication via monotonic sequence identifiers; expose last published id. | [API](client-api.md) §3.2 |
| C6 | Producer access modes (shared, exclusive, exclusive-with-fencing, wait-for-exclusive) with eager exclusive attach. | [API](client-api.md) §3.1, [Impl](client-behavior.md) §2 |
| C7 | Stream consumer: per-key ordered delivery; **cumulative** ack advancing all segments via position vector; broker-managed cursor; controller assignment + rebalance + reconnect/grace. | [API](client-api.md) §4.1, [Impl](client-behavior.md) §6, §8 |
| C8 | Queue consumer: parallel, no-affinity delivery; **individual** ack + negative ack; attach to all active+sealed segments; per-segment ack routing. | [API](client-api.md) §4.2, [Impl](client-behavior.md) §5 |
| C9 | Checkpoint consumer: external position; cross-segment atomic checkpoint capture/restore; ungrouped and grouped modes. | [API](client-api.md) §4.3, [Impl](client-behavior.md) §7 |
| C10 | Opaque, serializable, comparable message identifiers and opaque, serializable checkpoints with cross-version restore. | [Data Model](data-model.md) §4–§5 |
| C11 | Topic-level schema; broker-enforced compatibility surfaced as a typed error. | [Data Model](data-model.md) §6 |
| C12 | Auto-creation honored; not-found surfaced as a distinct error category. | [API](client-api.md) §7, [Errors](error-model.md) §2 |
| C13 | Error model: typed categories, async-never-throws-synchronously, internal-retry classification. | [Errors](error-model.md) |
| C14 | Resource lifetime: long-lived, self-healing producers/consumers; idempotent graceful close; thread-safe. | [API](client-api.md) §2, [Impl](client-behavior.md) §9–§10 |
| C15 | Feature gating: never send scalable-topic commands to a broker that does not advertise support. | [Compatibility](compatibility.md) §3 |

## 3. Optional capabilities

Each is independently optional. A client that omits one is conformant; a client that offers one MUST
satisfy its full specification and gate it on broker support where applicable.

| Capability | Requirement if implemented | Spec |
|------------|----------------------------|------|
| **Transactions** | NewTransaction, transactional send/ack, atomic commit/abort; TC discovery (watch where supported, else lookup). | [API](client-api.md) §6, [Wire](wire-protocol.md) §8 |
| **Namespace subscription** (stream) | Live membership via the namespace watch; cross-topic cumulative ack; establishment-error surfacing. | [API](client-api.md) §4.4, [Wire](wire-protocol.md) §7 |
| **Entry-bucketing / key-shared** | Producer single-bucket batching + metadata stamping; bucket-routed dispatch; rollover handling. Gated on broker support. | [Key-Shared](key-shared.md) |
| **Dead-letter** (queue) | Route messages past the redelivery limit to the dead-letter topic. | [API](client-api.md) §4.2 |
| **End-to-end encryption** | Producer/consumer encryption per the configured failure action. | [API](client-api.md) §3–§4 |
| **Chunking / compression / custom batching** | Producer-local policies; no change to delivery semantics. | [API](client-api.md) §3.1 |
| **Migration bridge** | Accept `persistent://`/short-form names; synthetic-layout + mod-N routing. | [Compatibility](compatibility.md) §1 |

## 4. Non-conformance

A client is **non-conformant** if it: routes keys differently from C2; exposes segments or a partition
count to the application ([Conventions](conventions.md)); surfaces a not-found as a generic error;
throws synchronously from an async operation; invalidates a producer/consumer on a transient error; or
implements an optional capability only partially.

## 5. Test guidance (informative)

Interoperability SHOULD be validated by: routing-vector tests (same key → same segment id across
clients); a split/merge during active produce/consume (no loss, no reorder, no surfaced error); a
broker failover (consumer resumes; producer continues); cumulative-ack-across-segments correctness; and
checkpoint capture/restore round-trips across a layout change.
