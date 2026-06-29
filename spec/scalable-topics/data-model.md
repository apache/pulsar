# Data Model

**Status:** Draft (targeting Stable)

This document defines the data objects a scalable-topics client exposes and the identity scheme for
topics. It is language-neutral; the Java surface is in [Java Reference Mapping](java-mapping.md). Terms
in *italics* are defined in [Terminology](terminology.md).

## 1. Topic identity

A *scalable topic* is identified by a **canonical name** of the form:

```
topic://<tenant>/<namespace>/<name>
```

- A client MUST accept a canonical `topic://` name and use it for every operation on the topic.
- A client MUST treat the canonical name returned by the broker during topic resolution as
  authoritative, and SHOULD use it when reporting the topic of a producer, consumer, or message.
- A scalable topic has **no application-visible partitions**. A client MUST NOT expose a partition count
  or partition index for a scalable topic.

A client MAY also accept the *migration-bridge* name forms (`persistent://…` and the short form) for
addressing an existing classic topic as a scalable topic; these are defined in
[Compatibility](compatibility.md). New applications use `topic://`.

## 2. Key and the hash ring

Each message MAY carry a **key** (a string). The key has two observable effects:

1. It establishes *per-key ordering* (§ Client API): messages with the same key are delivered in
   publish order to an *ordered consumer*.
2. It determines the message's *routing* to a *segment* (an internal mechanism defined in
   [Implementation Requirements](client-behavior.md)).

A message MAY be **keyless**, in which case it carries no ordering guarantee.

The keyspace is the 16-bit **hash ring** `0x0000`–`0xFFFF`. The mapping from key to ring position, and
from ring position to segment, is specified in [Implementation Requirements](client-behavior.md). The
hash ring is not otherwise application-visible.

## 3. Message

A **message** received from a scalable topic exposes the following. A client MUST provide read access to
every field marked REQUIRED and SHOULD provide the others where the underlying transport supplies them.

| Field | Required | Meaning |
|-------|----------|---------|
| value | REQUIRED | The deserialized payload, per the *schema*. |
| raw payload | REQUIRED | The undecoded payload bytes. |
| message identifier | REQUIRED | The message's *message identifier* (§4). |
| key | OPTIONAL | The message key, absent if the message is keyless. |
| properties | REQUIRED | Application-defined string key/value metadata (possibly empty). |
| publish time | REQUIRED | Broker-assigned publish timestamp. |
| event time | OPTIONAL | Producer-assigned application timestamp, if set. |
| sequence identifier | REQUIRED | Producer-assigned sequence number (deduplication). |
| producer name | OPTIONAL | Name of the publishing producer, if available. |
| topic | REQUIRED | Canonical topic the message belongs to. |
| redelivery count | REQUIRED | Number of prior redeliveries (0 on first delivery). |
| size | REQUIRED | Uncompressed payload size in bytes. |
| replicated-from | OPTIONAL | Source cluster, if the message arrived via geo-replication. |

A message is **immutable** once delivered.

## 4. Message identifier

A **message identifier** uniquely identifies a message within a topic.

- It MUST be **opaque**: a client MUST NOT expose any internal structure (e.g. ledger, entry, segment,
  or partition components) through the public identifier type.
- It MUST be **totally ordered** within a topic: two identifiers from the same topic MUST be comparable,
  and the order MUST match publish order within a single ordered substream.
- It MUST be **serializable** to bytes and restorable from those bytes. A serialized identifier produced
  by one version MUST be restorable by any later version (see [Stability](stability.md)).
- Two sentinel identifiers MUST be provided: **earliest** (oldest available message) and **latest** (the
  next message to be published).

A message identifier MUST NOT be used to express a read position spanning multiple segments; a
*checkpoint* (§5) is used for that.

## 5. Checkpoint

A **checkpoint** is a position value used by the *checkpoint consumer*.

- It MUST be **opaque** and represent a **consistent position across all of a topic's segments** — not a
  single point in one segment.
- It MUST be **serializable** to bytes and restorable from those bytes, with the same cross-version
  forward-compatibility guarantee as a message identifier (see [Stability](stability.md)), because
  applications persist checkpoints in external state and restore them after upgrades.
- Two sentinel checkpoints MUST be provided: **earliest** and **latest**.
- A checkpoint is the **only** position type accepted by the checkpoint consumer; a message identifier
  MUST NOT be used there.

Timestamp-based positioning is not expressed through the checkpoint type; it is an administrative
operation (out of scope for this document).

## 6. Schema

Every producer and consumer is created with a **schema** that governs payload serialization and
deserialization.

- Schema is **topic-level**: a scalable topic has a single schema regardless of how many segments back
  it. A client MUST present one consistent schema for the topic, not a per-segment schema.
- A client MUST support the standard Pulsar schema set (bytes, string, the primitive types, key/value,
  and generic records) as exposed by its schema factory.
- Schema compatibility MUST be enforced by the broker on connect; a client MUST surface a schema
  incompatibility as a typed error (see [Error Model](error-model.md)).
