# Terminology

**Status:** Draft

This document gives the normative definitions of terms used throughout the specification. A term in
*italics* elsewhere in the spec refers to a definition here. Definitions are normative; examples are
informative.

### Scalable topic

A durable, ordered-by-key message log identified by a *topic identity* (`topic://…`). It is the unit an
application produces to and consumes from. A scalable topic has **no application-visible partitioning**;
its only structural concept exposed to applications is the *key*.

### Topic identity

The canonical name of a scalable topic, of the form `topic://<tenant>/<namespace>/<name>`. The broker
resolves alternate input forms (see *migration bridge*) to this canonical identity.

### Key

An application-supplied string attached to a message that determines the message's *routing* and its
*per-key ordering*. A message MAY be keyless.

### Hash ring

The 16-bit key-hash space `0x0000`–`0xFFFF`. A *key* is mapped onto the ring by a defined hash function
(see [Implementation Requirements](client-behavior.md)). *Segments* own contiguous sub-ranges of the
ring.

### Segment

A broker-internal unit that owns a contiguous sub-range of the *hash ring* and stores the messages whose
key-hash falls in that range. Segments are an implementation and scaling detail; they are **not** part
of the application-visible model and MUST NOT be surfaced by the API as an application concept. They are
visible only at the *wire protocol* level and within the *checkpoint* model.

### Segment state

A segment is **active** (currently accepting writes for its range) or **sealed** (closed by a split or
merge; still readable until fully *drained*). New writes for a range always target the single active
segment covering it.

### DAG (segment DAG / layout)

The directed acyclic graph of a scalable topic's segments at a point in time, including each segment's
range, state, and split/merge lineage. The DAG is **per-cluster and dynamic**: it changes as segments
split and merge. It carries a monotonically increasing *epoch*.

### Epoch

A monotonically increasing version number for a topic's *DAG*. Used to order and discard stale layout
updates.

### Split / merge

The operations by which the broker changes the *DAG*: a **split** divides one segment's range across two
new active successor segments and seals the original; a **merge** combines two adjacent segments. These
preserve *per-key ordering* across the boundary.

### Per-key ordering

The guarantee that all messages published with the same *key* are delivered to an *ordered consumer* in
publish order. Because a key maps to one segment lineage, the order is well-defined. Keyless messages
carry no ordering guarantee.

### Routing

The client-side selection, per message, of the destination *segment*, derived from the message's *key*
(or round-robin when keyless). Defined in [Implementation Requirements](client-behavior.md).

### Producer

A client object that publishes messages to one scalable topic.

### Consumer

A client object that reads messages from one scalable topic (or, for the *namespace subscription*, a set
of topics). Three modes exist: *stream consumer*, *queue consumer*, *checkpoint consumer*.

### Stream consumer

An ordered, broker-managed consumer: messages are delivered in *per-key* order with **cumulative**
acknowledgment, and load is shared across the consumers of a subscription (Failover-like behavior).

### Queue consumer

A parallel, broker-managed consumer: messages are distributed across the consumers of a subscription for
parallel processing with **no ordering and no key affinity**, using **individual** acknowledgment and
negative acknowledgment.

### Checkpoint consumer

An unmanaged consumer with no broker-managed cursor: read position is held entirely by the application
as a *checkpoint*. Intended for connector frameworks. MAY be ungrouped (reads all of the topic) or
grouped (shares the topic with other members of a *consumer group*).

### Subscription

A named, broker-managed read position (cursor) shared by the consumers reading a scalable topic under
that name. Used by *stream* and *queue* consumers; not used by *checkpoint* consumers.

### Consumer group (checkpoint)

A named set of *checkpoint consumers* that share a topic's segments via the broker's coordinator without
a persisted cursor; segments rebalance as members join or leave.

### Cumulative acknowledgment

An acknowledgment that marks every message up to and including a given one as processed. Used by
*stream consumers*.

### Individual acknowledgment

An acknowledgment that marks exactly one message as processed. Used by *queue consumers*; paired with
**negative acknowledgment**, which requests redelivery of one message.

### Message identifier

An opaque, serializable, totally-ordered identifier for a message within a topic. It exposes no internal
structure. It is not used to express a position across multiple segments — see *checkpoint*.

### Checkpoint

An opaque, serializable value representing a consistent read position across **all** of a topic's
segments. Created by a *checkpoint consumer*, stored by the application, and used to resume.

### Affinity (key affinity)

The property that, within one subscription, at most one consumer handles a given *key* at a time.
Provided by *ordered* consumption modes; **not** provided by the *queue consumer*.

### Cursor

A broker-managed, persisted read position for a *subscription*.

### Migration bridge

The mechanism by which an existing classic (`persistent://`) topic is addressed as a scalable topic: the
broker wraps it in a synthetic single-segment *DAG*. A *legacy segment* names the wrapped classic topic
rather than a `segment://` topic. Provided only to ease migration.

### Legacy segment

A segment in a synthetic layout that wraps an externally-managed `persistent://` topic instead of a
broker-managed `segment://` topic. See *migration bridge*.

### Controller (leader)

The broker-internal, per-topic authority that manages the *DAG* and assigns segments to *ordered
consumers*. A client discovers and connects to it as part of the consumer-assignment protocol; it is not
otherwise application-visible.

### Replicator / replicated message

In geo-replication, the broker component that re-publishes a topic's messages to other clusters. A
**replicated message** is one received via replication; it is marked with its source cluster.
(Geo-replication of scalable topics is out of scope for this specification version — see
[Stability and Versioning](stability.md).)
