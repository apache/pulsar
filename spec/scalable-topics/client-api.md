# Client API

**Status:** Draft (targeting Stable)

This document defines the **observable contract** of a scalable-topics client: the operations an
application invokes, their inputs, and the guarantees they provide. It is transport-agnostic and
language-neutral. *How* a client realizes these guarantees is in
[Implementation Requirements](client-behavior.md); the wire encoding is in
[Wire Protocol](wire-protocol.md); the Java surface is in [Java Reference Mapping](java-mapping.md).

Terms in *italics* are defined in [Terminology](terminology.md). Operation names (e.g. *CreateProducer*,
*Send*) are language-neutral; an SDK MUST provide each operation but SHOULD name and shape it idiomatically.

## 1. Operation style

- Every operation that may block on I/O MUST be available in a non-blocking (future/async) form. A
  client SHOULD also provide a blocking form. The two forms MUST share the same semantics and resources.
- This document states requirements once; they apply to both forms.

## 2. Client and resource lifetime

The **client** is the entry point and the factory for producers, consumers, and transactions.

- A client is heavyweight and thread-safe. An application SHOULD create **one client instance and reuse
  it for the application's lifetime**, sharing it across all producers and consumers. Creating a client
  per operation is an anti-pattern.
- A client MUST provide a **graceful close** (await pending operations) and MAY provide an immediate
  shutdown (abandon pending operations). After close, the client MUST reject new operations.
- Once *CreateProducer* / *Subscribe* / *Create* (consumer) succeeds, the returned producer or consumer
  **MUST remain valid for its lifetime**. The client MUST recover transparently from transient failures
  (disconnects, failovers, broker reassignment, layout changes). An application **MUST NOT** be required
  to discard and recreate a producer or consumer in response to a publish or receive error; it retries
  the *operation* on the same instance. A failed publish indicates that *message* failed, not that the
  producer is broken.

## 3. Producer

A **producer** publishes messages to one scalable topic.

### 3.1 CreateProducer

**Inputs.** A topic name (REQUIRED) and optional configuration: producer name; *access mode* (default
*shared*); send timeout; queue-full behavior; compression, batching, chunking, and encryption policies;
initial sequence identifier; user properties.

**Behavior & output.** On success, returns a ready producer. If the topic does not exist, the broker
MAY *auto-create* it subject to policy (§7); otherwise the operation MUST fail with a *not-found* error.
Under an *exclusive* access mode the operation MUST fail if exclusivity cannot be acquired (rather than
deferring the failure to first publish).

**Access modes.** A client MUST support: *shared* (many concurrent producers), *exclusive* (single
writer), *exclusive-with-fencing* (single writer, fencing older ones), and *wait-for-exclusive* (block
until exclusive can be acquired).

### 3.2 Send

A message is constructed and published with these settable attributes:

| Attribute | Effect |
|-----------|--------|
| value | REQUIRED. Serialized with the producer's schema. |
| key | Establishes *per-key ordering* and *routing*. Optional. |
| properties | Application string key/values. |
| event time | Application timestamp. |
| sequence identifier | Overrides the auto-assigned deduplication id. |
| delayed delivery | Make the message visible to consumers after a delay or at a time. |
| transaction | Publish within a *transaction* (§6). |
| replication clusters | Restrict geo-replication targets. |

*Send* returns the published message's *message identifier*.

**Guarantees.**

- **Per-key ordering (REQUIRED).** All messages published with the same key MUST be delivered to an
  ordered consumer in publish order, and this order MUST be preserved across segment *split/merge*.
- **Keyless messages** carry **no** ordering guarantee.
- **Ordering of concurrent async sends (REQUIRED).** For a single producer, messages MUST be published
  in the order the application issued the *Send* calls (a later send MUST NOT overtake an earlier one
  for the same key).
- **Deduplication.** Each message carries a monotonically increasing *sequence identifier*. When topic
  deduplication is enabled, the broker discards duplicates by (producer name, sequence identifier). A
  client MUST expose the last successfully published sequence identifier.

### 3.3 Other producer operations

- **Flush** MUST await completion of all publishes issued before the flush call (publishes issued
  afterward need not be awaited).
- **Close** MUST complete pending publishes, release the producer's resources, and be idempotent.

## 4. Consumers

A client MUST provide three distinct consumer types. Ordering is always stated **per key** — *segments*
are never an application concept (see [Terminology](terminology.md)).

| Mode | Ordering | Acknowledgment | Position | v4 analog (informative) |
|------|----------|----------------|----------|-------------------------|
| Stream | per-key, ordered | cumulative | broker-managed cursor | Failover (ordered, load shared across the subscription) |
| Queue | none (parallel, no key affinity) | individual + negative | broker-managed cursor | Shared |
| Checkpoint | per-key, ordered | none (external position) | client-held *checkpoint* | reader-style |

A client MUST provide a blocking single-message *Receive*, a *Receive* with timeout (returning nothing
if it elapses), and SHOULD provide a bounded multi-message receive.

### 4.1 Stream consumer (ordered, broker-managed)

**Subscribe inputs.** Exactly one of a topic name **or** a namespace selector (§4.4) is REQUIRED, plus a
subscription name (REQUIRED). Optional: initial position (applied only when the subscription is new),
subscription properties, consumer name, acknowledgment-group time, read-compacted, replicate-subscription-state, encryption policy.

**Guarantees.**

- Messages MUST be delivered in **per-key order**.
- Acknowledgment is **cumulative only**: acknowledging a message MUST mark every message up to and
  including it (across the whole topic) as processed. A client MUST NOT offer individual ack in this mode.
- Position is a **broker-managed cursor** keyed by subscription name; on reconnect the consumer MUST
  resume from the persisted cursor, or from the configured initial position if the subscription is new.
- Multiple consumers on one subscription **share the load** and rebalance as members join or leave,
  while preserving per-key order (Failover-like).
- Acknowledgment MAY be performed within a *transaction*, becoming effective on commit.

### 4.2 Queue consumer (parallel, broker-managed)

**Guarantees.**

- Messages are distributed across the subscription's consumers for parallel processing with **no
  ordering guarantee and no key affinity**: successive messages — even ones sharing a key — MAY go to
  different consumers in any order. A queue consumer does not specify a key.
- Acknowledgment is **individual** (each message separately); **negative acknowledgment** requests
  redelivery of a single message. Position is a broker-managed cursor keyed by subscription name.
- Acknowledgment MAY be performed within a *transaction*.
- A client SHOULD support a **dead-letter** policy: messages exceeding a redelivery limit are routed to
  a dead-letter topic.

> *Informative:* per-key, single-consumer delivery is a property of the ordered modes only. Scalable
> per-key affinity for queue-style consumption is the optional **entry-bucketing** capability
> ([Scalable Key-Shared Consumption](key-shared.md)), specified separately.

### 4.3 Checkpoint consumer (unmanaged, external position)

**Create inputs.** A topic name (REQUIRED); a start position as a *checkpoint* (default *latest*); an
optional consumer-group name; consumer name; encryption policy. The terminal operation is *Create*
(there is no broker subscription).

**Guarantees.**

- There is **no broker-managed cursor**. The application holds position as a *checkpoint*; the consumer
  starts at the configured start position.
- *Checkpoint* MUST capture a **consistent position across all of the topic's segments** and return an
  opaque, serializable value. Restoring from a checkpoint MUST resume reading from exactly that position.
- **Ungrouped** (default): the consumer independently reads the entire topic from its start position.
- **Grouped**: consumers sharing a group name share the topic; each unit is read by exactly one member
  and units rebalance as members join/leave. A group MUST NOT persist a cursor — each member resumes
  from its own start position/restored checkpoint.

### 4.4 Namespace subscription (stream)

A stream consumer MAY subscribe to **all** scalable topics in a namespace, optionally filtered by topic
properties (AND semantics; empty filter ⇒ all). Membership MUST be **live**: topics entering or leaving
the matching set MUST cause the consumer to attach or detach automatically. Cumulative acknowledgment
MUST behave as in §4.1, extended across the topic set. If the namespace watch cannot be established
(e.g. namespace not found, authorization denied), *Subscribe* MUST fail (see [Wire Protocol](wire-protocol.md) §16).

## 5. Errors

Every operation reports failures as typed errors. Notably: a *not-found* error MUST be distinct (so an
application can tell "topic absent" from a generic failure). The full taxonomy, retryability, and the
rule that asynchronous APIs MUST NOT throw synchronously are in [Error Model](error-model.md).

## 6. Transactions

Transactions provide exactly-once semantics across topics and subscriptions: messages published and
acknowledgments made within a transaction MUST become effective **atomically** on commit, or be
discarded on abort or timeout.

- **NewTransaction** returns a transaction handle with an open state and a timeout.
- A message MAY be published within a transaction (§3.2); an acknowledgment MAY be made within a
  transaction (§4.1, §4.2).
- **Commit** MUST make all buffered publishes visible and all buffered acknowledgments durable, across
  all involved topics. **Abort** MUST discard them.
- A client MUST expose the transaction state and MUST fail commit/abort with a typed error when the
  transaction is not in a committable/abortable state (already terminal, timed out, or errored).

## 7. Auto-creation

On connecting a producer or consumer to a non-existent scalable topic, the broker auto-creates it
(with a single initial segment) **iff** auto-topic-creation policy permits, exactly as for classic
topics. When policy forbids it, the connect MUST fail with a *not-found* error. A client MUST NOT assume
a topic exists.
