# Error Model

**Status:** Draft (targeting Stable)

This document defines how a scalable-topics client reports failures: the error categories, which are
retried internally vs. surfaced, and the rules every operation follows. Error *categories* are
language-neutral; the Java exception types are in [Java Reference Mapping](java-mapping.md).

## 1. General rules

- **Typed errors.** Every fallible operation MUST report failure as a **typed** error category (below),
  not an opaque/generic failure, so an application can branch on the cause.
- **Async never throws synchronously.** An operation that returns a future MUST signal all failures by
  completing the future exceptionally; it MUST NOT throw synchronously from the call that creates the
  future. (Blocking variants surface the same category as a thrown exception.)
- **A failed publish/receive does not invalidate the producer/consumer.** Per
  [Client API](client-api.md) §2, a failed *Send* means that *message* failed; the producer remains
  valid. The application retries the operation on the same instance.
- **Internal-only failures are not surfaced.** Transient conditions the client resolves itself
  (reconnect, layout refresh, segment-gone reroute) MUST NOT reach the application unless retries are
  exhausted.

## 2. Error categories

| Category | Meaning | Typical origin |
|----------|---------|----------------|
| **NotFound** | Topic (or namespace) does not exist and could not be auto-created. | Resolve/subscribe with auto-create disallowed. |
| **Authentication** | Credentials rejected. | Connect. |
| **Authorization** | Authenticated but not permitted. | Resolve, produce, subscribe, watch. |
| **Timeout** | Operation did not complete within its deadline. | Send timeout, lookup/op timeout. |
| **AlreadyClosed** | The client/producer/consumer is closed. | Any op after close. |
| **ProducerBusy / Fenced** | Exclusive access mode held by another producer (or this one was fenced). | CreateProducer / send under exclusive. |
| **ConsumerBusy** | Subscription constraint prevents this consumer attaching. | Subscribe. |
| **IncompatibleSchema** | Schema not compatible with the topic's. | Producer/consumer connect. |
| **TopicTerminated** | The target (segment) topic was terminated. | Send to a sealed/migrated segment (internal; see §3). |
| **Connect / NotConnected** | Transport could not connect or is momentarily disconnected. | Any op (usually internal). |
| **ProducerQueueFull** | Pending-send queue is full and blocking is disabled. | Send with `blockIfQueueFull = false`. |
| **MemoryBufferFull** | Client memory limit reached. | Send. |
| **TransactionConflict** | Transaction operation conflicts/cannot proceed. | Transactional send/ack/commit. |
| **Crypto** | Encryption/decryption failure (subject to the configured failure action). | Produce/consume on an encrypted topic. |
| **InvalidConfiguration** | Builder configuration invalid (e.g. topic not set). | Create/subscribe. |
| **InvalidServiceURL** | Malformed service URL. | Client build. |
| **Interrupted** | A blocking call was interrupted. | Blocking ops. |

A client MUST surface **NotFound** as a category distinct from generic failure (so "topic absent" is
distinguishable). For a scalable topic, a not-found result from topic resolution
([Wire Protocol](wire-protocol.md) §4) MUST map to **NotFound**, not a generic protocol error.

## 3. Retryability

A conformant client MUST classify failures as follows.

**Retried internally (not surfaced unless exhausted):**

- **Connect / NotConnected**, and lookup/assignment/watch connection drops — re-establish with backoff
  ([Implementation Requirements](client-behavior.md) §8–§9).
- **TopicTerminated / segment-gone** during producing — drop the per-segment producer, await the new
  layout, re-route, retry up to a bounded number of attempts
  ([Implementation Requirements](client-behavior.md) §4). Only exhaustion surfaces (typically as the
  last underlying error).

**Surfaced to the application (not retried by the client):**

- **NotFound**, **Authentication**, **Authorization**, **IncompatibleSchema**,
  **InvalidConfiguration**, **InvalidServiceURL** — deterministic; retrying without change cannot help.
- **ProducerBusy / Fenced**, **ConsumerBusy** — access-mode/subscription constraints.
- **Timeout**, **ProducerQueueFull**, **MemoryBufferFull**, **TransactionConflict**, **Crypto** —
  surfaced; the **application** decides whether to retry (the SDK does not retry these automatically).

## 4. Watch establishment errors

Each watch family — DAG watch ([Wire Protocol](wire-protocol.md) §4), namespace watch (§7), TC
discovery (§8) — signals an establishment failure via `error`/`message` in its first update. A client
MUST surface that as the failure of the operation that opened the watch (e.g. a namespace watch failing
with not-found/authorization MUST fail the namespace consumer's *Subscribe*), in the corresponding
category from §2.

## 5. Negative acknowledgment is not an error

`NegativeAcknowledge` (queue consumer) is a normal control signal requesting redelivery, not an error
report. A redelivered message carries an incremented redelivery count
([Data Model](data-model.md) §3); exceeding a dead-letter threshold routes the message to the
dead-letter topic rather than raising an error.
