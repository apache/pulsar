# Compatibility

**Status:** Draft (targeting Stable)

This document specifies interoperability between scalable topics and classic (v4) Pulsar: the
migration bridge from existing topics, broker version/feature gating, and forward/backward compatibility
of the protocol additions.

## 1. Migration bridge: addressing a classic topic as a scalable topic

To let an existing deployment adopt scalable topics without rewriting every topic reference, a client
MAY address an existing classic topic through the scalable-topic surface. The broker bridges it with a
**synthetic layout**:

- A name given as `persistent://tenant/ns/name` (or a short form normalized to
  `persistent://public/default/name`) is resolved ([Wire Protocol](wire-protocol.md) §4) to a
  **synthetic single-segment** `ScalableTopicDAG`. The single `SegmentInfoProto` carries
  `legacy_topic_name` set to the wrapped `persistent://` topic and has **no** `segment://` name.
- A client targets the wrapped `persistent://` name directly for that segment (producing, subscribing,
  reading).
- For a **partitioned** classic topic, the synthetic layout presents the partitions as legacy segments;
  a keyed message routes by `signSafeMod(Murmur3_32(key), N)` over the `N` partitions — **identical to
  v4 partitioned-topic routing** — so a topic keeps its existing key→partition mapping while migrated.

This bridge exists only for migration. New applications use canonical `topic://` names
([Data Model](data-model.md) §1).

> *Informative:* a V5-managed connection to a wrapped `persistent://` topic may be marked so the broker's
> migration pre-checks can distinguish it from an ordinary v4 connection. This is a broker-internal
> detail and not part of the client contract.

## 2. Coexistence with classic topics and clients

- A scalable topic (`topic://`) and a classic topic (`persistent://`) are distinct identities; the
  reused per-segment subscriptions and producers behave exactly as classic v4 ones at the segment level
  ([Wire Protocol](wire-protocol.md) §10).
- A v4 client cannot consume a `topic://` scalable topic directly (it has no DAG-watch/assignment
  logic); it interoperates only through the migration bridge against the underlying `persistent://`
  topic during migration.
- Classic-topic clients and scalable-topic clients MAY operate on the same cluster simultaneously.

## 3. Broker version and feature gating

- `segment://` topics are served **only** by brokers that implement scalable topics. The
  scalable-topic command family ([Wire Protocol](wire-protocol.md) §1) MUST NOT be sent to a broker that
  does not advertise support; a client MUST negotiate support before using it.
- **Transaction-coordinator discovery** via the assignment watch is gated by the
  `supports_tc_metadata_discovery` feature flag. A client MUST use the TC discovery watch
  ([Wire Protocol](wire-protocol.md) §8) only against a broker that advertises it, and fall back to the
  standard coordinator-lookup path otherwise.
- **Entry-bucketing** ([Scalable Key-Shared Consumption](key-shared.md)) is an optional capability; a
  client that implements it MUST gate the behavior on observed broker support and remain interoperable
  with brokers that lack it.

## 4. Protocol forward/backward compatibility

- The scalable-topic commands occupy dedicated `BaseCommand` field numbers and are additive; a broker
  or client that does not understand them ignores them. They are never sent on a path where the peer is
  not known to support the feature (§3).
- The entry-bucketing `MessageMetadata` fields are **optional** and inert on classic topics; a broker
  without the feature ignores them, and a producer without the feature simply omits them.
- Serialized **message identifiers** and **checkpoints** MUST remain restorable across version upgrades
  ([Data Model](data-model.md) §4–§5), because applications persist them externally.

## 5. Schema and policies

Schema negotiation, topic-level policies, authentication/authorization, and TLS are the standard Pulsar
mechanisms, applied at the segment level. A scalable topic has a single topic-level schema regardless of
segment count ([Data Model](data-model.md) §6).
