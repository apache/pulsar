# Scalable Topics — Client Specification

**Specification version:** 0.1 (Draft)
**Status:** Draft — under active development. See [Stability and Versioning](stability.md).

This is the authoritative, language-neutral specification for **Apache Pulsar Scalable Topics** clients.
It defines what a conformant client SDK MUST do, in any language, to interoperate with an Apache Pulsar
broker that implements scalable topics. The Java *V5 client* is the reference implementation; this
specification — not any single implementation — is the source of truth.

It is modeled on the conventions of authoritative cross-implementation specifications (e.g. the
[OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)): formal normative language, a
defined glossary, explicit stability and conformance rules, and a clear separation between the
**API contract** (what an application observes) and the **implementation requirements** (what a client
must do internally).

## Audience

- **SDK implementers** building a scalable-topics client in a new language — read all documents; the
  [Conformance](conformance.md) document is the checklist.
- **Application developers** — the [Client API](client-api.md) and [Data Model](data-model.md) documents
  define observable behavior and guarantees.

## Document set

| # | Document | Status | Summary |
|---|----------|--------|---------|
| 1 | [Overview & Conventions](conventions.md) | Draft | Scope, normative language (BCP 14), document statuses, the language-neutral naming policy and Java mapping. |
| 2 | [Terminology](terminology.md) | Draft | Normative definitions of every term used across the spec. |
| 3 | [Stability and Versioning](stability.md) | Draft | Specification versioning, feature stability tiers, and backward/forward-compatibility guarantees. |
| 4 | [Data Model](data-model.md) | Draft | Topic identity, the key/hash ring, message, message identifier, checkpoint, schema. |
| 5 | [Client API](client-api.md) | Draft | The observable **contract**: producers, the three consumer modes, transactions — operations, inputs, guarantees, errors. Transport-agnostic. |
| 6 | [Implementation Requirements](client-behavior.md) | Draft | What a conformant client MUST do internally: routing, layout tracking, per-segment fan-out, retries, multiplexing, concurrency, resource lifetime. |
| 7 | [Wire Protocol](wire-protocol.md) | Draft | The complete Pulsar binary-protocol binding with sequence diagrams: command catalog, DAG-watch, consumer assignment, producing/consuming, namespace watch, transaction-coordinator discovery, auto-creation. |
| 8 | [Error Model](error-model.md) | Draft | Error taxonomy, typed errors, retryability, and no-throw rules. |
| 9 | [Conformance](conformance.md) | Draft | The binary conformance criteria and the per-feature requirements checklist. |
| 10 | [Compatibility](compatibility.md) | Draft | Interoperability with classic (v4) topics, the migration bridge, and broker version requirements. |
| 11 | [Scalable Key-Shared Consumption](key-shared.md) | Draft | Entry-bucketing ([PIP-486](../../pip/pip-486.md)): per-key affinity for queue-style consumption. Normative, **optional** capability. |
| A | [Java Reference Mapping](java-mapping.md) | Draft | Maps each language-neutral operation/type to the Java V5 client surface. |

> **Status legend:** *Draft* = written, subject to change; *Stable* = ratified and frozen under the
> versioning rules. Per-document and per-feature stability is governed by
> [Stability and Versioning](stability.md).

## Relationship to the design proposals (PIPs)

Scalable topics are designed in the PIP series under [PIP-460](../../pip/pip-460.md): the controller
([PIP-468](../../pip/pip-468.md)), auto split/merge ([PIP-483](../../pip/pip-483.md)), and key-shared
consumption ([PIP-486](../../pip/pip-486.md)). The PIPs are **design rationale**; this specification is
the **normative client contract**, covering behavior that is implemented or ratified via an accepted
PIP. Scalable key-shared consumption ([PIP-486](../../pip/pip-486.md), document 11) is **normative** — an
optional capability, not Experimental. Geo-replication remains out of scope until its design settles. A
design that is documentable but still changing MAY appear marked **Experimental** (none currently). See
[Stability](stability.md).

## Change process

This specification is **not itself a PIP**. It is a living normative document maintained under a
dedicated change process:

- **Every normative change to this specification MUST be made through a [PIP](../../pip/).** The PIP
  carries the design and rationale for the change; reviewers evaluate the proposed behavior and the
  spec edits together.
- **The specification edits land *with* the PIP.** When a PIP that changes scalable-topic client
  behavior is accepted, the corresponding edits to these documents are merged together with it —
  typically in the **same pull request** — so the contract and its decision record never drift.
- Accepted PIPs remain the **design rationale and decision record**; these documents are the
  **normative contract**, kept continuously in sync with the ratified PIPs.

The detailed mechanics (PIP-template fields for spec changes, and the handling of purely-editorial
corrections) are for the project maintainers to define.

## Publishing

These documents are authored in the `apache/pulsar` repository and are intended to be published to the
user-facing docs site, sourced from
[`apache/pulsar-site`](https://github.com/apache/pulsar-site). The mechanism for mirroring them into
`pulsar-site` is a separate task and is not defined here.
