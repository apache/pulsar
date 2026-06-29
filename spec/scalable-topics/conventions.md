# Overview & Conventions

**Status:** Draft

This document defines the scope of the Scalable Topics Client Specification and the conventions every
other document in the set follows.

## 1. Scope

This specification defines the behavior of a **scalable-topics client SDK**: the producer and consumer
surface an application uses, the guarantees that surface provides, and the protocol a client speaks to
an Apache Pulsar broker. It is **language-neutral** — it defines required behavior, not a particular
language's syntax.

In scope:

- The observable **API contract** (operations, inputs, outputs, ordering and delivery guarantees,
  errors) — see [Client API](client-api.md).
- The **implementation requirements** a conformant client MUST meet internally — see
  [Implementation Requirements](client-behavior.md).
- The **wire protocol** between client and broker — see [Wire Protocol](wire-protocol.md).

Out of scope: broker-internal behavior (the controller, split/merge execution, storage) except where it
is observable by a client; and designs whose specification is not yet settled (currently
geo-replication). A documentable-but-still-changing design MAY appear marked **Experimental** per
[Stability and Versioning](stability.md). See that document for what is normative vs. out of scope.

## 2. Normative language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**,
**SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this specification are to be interpreted as
described in [BCP 14](https://www.rfc-editor.org/info/bcp14) ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119),
[RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, they appear in **all
capitals**.

An implementation is **conformant** if and only if it satisfies every applicable MUST / MUST NOT /
REQUIRED / SHALL / SHALL NOT requirement. SHOULD-level requirements are strong recommendations whose
violation requires careful justification; MAY-level items are genuinely optional. The full conformance
model is in [Conformance](conformance.md).

## 3. Normative vs. informative content

Unless explicitly marked, all content is **normative**. Content marked *“Informative”*, *“Example”*,
*“Note”*, or *“Rationale”*, and anything in a block quote, is **non-normative** — it aids understanding
but imposes no requirement. Diagrams are informative.

## 4. Document statuses

Each document and each feature carries a status, defined in [Stability and Versioning](stability.md):

- **Stable** — will not change in backward-incompatible ways without a major specification version bump.
- **Experimental** — may change in any way; implementations MAY support it but MUST NOT be considered
  non-conformant for omitting it.
- **Draft** — not yet ratified; present for review.
- **Deprecated** — retained for compatibility; SHOULD NOT be used by new code.

## 5. Language-neutral naming and the Java mapping

Operations, types, and configuration are named **language-neutrally** in this specification. Concrete
SDKs MUST provide these operations but SHOULD name and shape them idiomatically for their language
(e.g. async style, naming case, error reporting).

The specification uses a small, stable set of operation names — for example *CreateProducer*, *Send*,
*Subscribe*, *Receive*, *Acknowledge*, *AcknowledgeCumulative*, *Checkpoint*, *NewTransaction*,
*Commit* — defined where they are introduced. Each document presents normative behavior against these
names; the **Java V5 reference surface** for every operation and type is collected in
[Java Reference Mapping](java-mapping.md). Where this specification needs to refer to a concrete symbol
(a wire command, a metadata field), it uses the on-the-wire / protocol name, which is language-neutral
by definition.

When a normative requirement is illustrated with a code-like signature, that signature is **illustrative
pseudocode** unless it appears in [Java Reference Mapping](java-mapping.md) or the
[Wire Protocol](wire-protocol.md); the prose is the normative form.

## 6. Reference implementation

The Java *V5 client* in `apache/pulsar` is the reference implementation. It is a guide to intended
behavior, but **this specification is authoritative**: where an implementation (including the reference
one) diverges from a normative requirement here, that is a defect in the implementation or a defect in
this specification, to be reconciled — not a silent redefinition of the contract.
