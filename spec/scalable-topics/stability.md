# Stability and Versioning

**Status:** Draft

This document defines how the specification is versioned, how feature maturity is expressed, and what
compatibility guarantees clients and brokers owe each other.

## 1. Specification versioning

This specification carries a version of the form `MAJOR.MINOR`:

- **MAJOR** increments on a backward-incompatible change to a *Stable* requirement, and is **tied to the
  Apache Pulsar LTS cadence**: each new MAJOR corresponds to a new Pulsar **LTS** release. Consequently,
  backward-incompatible changes and removals of Deprecated features (§2.1) land **only at an LTS
  boundary**, never in an interim release.
- **MINOR** increments on backward-compatible additions or clarifications, and may occur in any release.

The current version is declared in the [README](README.md). While the specification is at version `0.x`
it is **Draft**: any part MAY change. Stability guarantees below take full effect from version `1.0`.

A version increment, like any normative change, is enacted through the **change process**: the
specification is not itself a PIP, and every normative change MUST be made via an accepted PIP whose
spec edits merge together with it (see [README → Change process](README.md#change-process)).

## 2. Feature stability tiers

Every feature (operation, guarantee, wire command, configuration item) has a stability tier. When a
document does not state otherwise, the tier is that of its containing document.

- **Stable** — Will not change in a backward-incompatible way without a MAJOR version bump. Conformant
  clients MUST implement all Stable, non-optional requirements.
- **Experimental** — May change in any way, including removal, in a MINOR version. Clients MAY implement
  it; omitting it MUST NOT make a client non-conformant. New, recently-shipped behavior starts here.
- **Deprecated** — Stable but scheduled for removal. Retained for compatibility; new code SHOULD NOT
  rely on it. A Deprecated feature MUST NOT be removed before the next MAJOR version.

A feature's tier is independent of its quality; it expresses only the **change contract**.

### 2.1 Deprecation and removal

A feature is moved to **Deprecated** through the [change process](README.md#change-process) (an accepted
PIP). The deprecating change MUST record:

- the **reason** for deprecation;
- the **replacement**, or an explicit statement that none exists; and
- the **earliest specification version** at which removal is permitted.

While a feature is Deprecated:

- the specification MUST mark it **Deprecated** at its point of use, with a pointer to the replacement;
- a **broker** MUST continue to honor it — the wire forms and behavior of a Deprecated Stable feature
  remain available until the feature is removed;
- a **client** SHOULD migrate to the replacement and SHOULD NOT newly adopt the feature;
- where the deprecation is observable to operators, an implementation SHOULD emit a non-fatal
  deprecation signal (e.g. a one-time log) rather than failing.

**Removal.**

- A Deprecated **Stable** feature MUST NOT be removed before the next **MAJOR** specification version,
  and removal MUST go through the change process. The deprecation window therefore spans at least the
  remainder of the current MAJOR series.
- Removal MUST also align with the Apache Pulsar **LTS (Long-Term Support)** release cadence: a
  Deprecated feature MUST remain available at least until the **next LTS release**, so that operators
  upgrading from one LTS to the next always have a release in which both the deprecated feature and its
  replacement coexist. A spec MAJOR version that removes Deprecated features is therefore tied to an LTS
  boundary, not an arbitrary release.
- An **Experimental** feature has no deprecation protection: it MAY be changed or removed in a MINOR
  version without a deprecation period (it carries no conformance obligation, §2).

## 3. Scope: committed behavior, plus clearly-marked Experimental designs

This specification describes the **committed behavior** of scalable topics — behavior that is either
implemented in a released broker, or **ratified via an accepted PIP and committed to ship**. An SDK
built to it interoperates with current and announced brokers.

A design that is sufficiently specified to document but still **subject to change** MAY be included
marked **Experimental** (§2), confined to its own clearly-delimited sections. Experimental material
informs SDK authors of forthcoming behavior but imposes **no conformance obligation** and may change in
any way, including removal.

On this basis:

- **Scalable key-shared consumption (entry-bucketing, [PIP-486](../../pip/pip-486.md))** is **normative**
  (ratified via PIP-486). Its consumer side is REQUIRED for clients offering the Stream consumer;
  producer-side bucketing is exchangeable for unbatched publishing — see
  [its conformance section](key-shared.md). It is not Experimental.
- **Geo-replication of scalable topics** is **out of scope** — its design is not yet settled (not even
  Experimental).
- No feature is currently in the Experimental tier; the tier remains defined for future use.

## 4. Compatibility guarantees

### 4.1 Wire protocol

- The protocol is **extensible by addition**: new optional fields and new commands MAY be added in a
  MINOR version. Clients and brokers MUST ignore unknown optional fields.
- A client MUST NOT assume the presence of an Experimental command or field unless it has negotiated or
  observed broker support for it.
- A broker MUST continue to accept the wire forms of every Stable command across MINOR versions.

### 4.2 Client API contract

- The observable guarantees in [Client API](client-api.md) marked Stable MUST hold across MINOR
  versions. New operations MAY be added; existing guarantees MUST NOT be weakened without a MAJOR bump.

### 4.3 Serialized formats

- A *message identifier* and a *checkpoint* produced by one version MUST be deserializable by any later
  version (forward-compatible restore). This is REQUIRED because applications persist these values
  externally and restore them after upgrades.

### 4.4 Broker / client version skew

- A scalable topic (`topic://` / `segment://`) is served only by brokers that implement scalable topics;
  the new protocol commands therefore never reach a broker that does not understand them. Optional
  message-metadata additions for scalable topics are inert on classic topics and on brokers without the
  feature.

## 5. Current feature status

| Area | Tier | Notes |
|------|------|-------|
| Topic identity & migration bridge | Draft → targeting Stable | |
| Producer & routing | Draft → targeting Stable | |
| Stream / Queue / Checkpoint consumers | Draft → targeting Stable | |
| Transactions | Draft → targeting Stable | Coordinator discovery via assignment watch. |
| Namespace subscription | Draft → targeting Stable | |
| Wire protocol (`CommandScalableTopic*`, watches) | Draft → targeting Stable | |
| Scalable key-shared consumption (entry-bucketing) | Normative | Ratified via [PIP-486](../../pip/pip-486.md). Consumer side required for Stream consumers; producer side exchangeable for unbatched publishing. |
| Geo-replication of scalable topics | Out of scope | Design not yet settled. |

While the specification is at `0.x`, all rows are **Draft**; the "targeting" column states the intended
tier at `1.0`.
