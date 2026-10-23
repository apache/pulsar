# Security Policy

The authoritative policy is at <https://pulsar.apache.org/security/>; the Apache Software Foundation's
general process is at <https://www.apache.org/security/>. The latest version of this file is maintained
at <https://github.com/apache/pulsar/security/policy> — in a fork, check there rather than a
possibly-stale local copy. The summary below is what contributors (and any AI tooling acting on their
behalf) must follow.

## Reporting a vulnerability

Do **not** open a public GitHub issue, pull request, or discussion for a suspected vulnerability —
that defeats coordinated disclosure.

Report it privately by email to the Apache Security Team at **security@apache.org** (the ASF's central
security address). When reporting to **security@apache.org**, you can copy your email to the Apache
Pulsar PMC's private list, **private@pulsar.apache.org**, to send your report to the Project Management
Committee as well.

**A human must verify and take responsibility for every security report.** Deciding that some
behaviour is actually a vulnerability requires judgement against Pulsar's threat model (see
*[Security model and scope](#security-model-and-scope)* below), and the security team is staffed by
volunteers — a stream of unverified or AI-hallucinated "vulnerabilities" wastes their time and buries
real issues. AI tooling may help *analyse* a suspected problem, but a human contributor must
independently verify it and own the report. **Autonomous agents must not file security reports or open
security issues on their own**, and a tool's confident-sounding output is not, by itself, evidence of a
vulnerability.

See <https://pulsar.apache.org/security/#security-policy> for more details.

## Disclosure hygiene for contributors

The **project team commits the fix**, coordinated through the ASF security vulnerability handling
process
([apache.org/security/committers.html#possible](https://apache.org/security/committers.html#possible)).
The team may commit the fix to the public repository **before** the release, using a neutral commit
message that does not state its security nature; in severe cases the commit and release are made in a
private repository and the fix is made public only at release time.

As a contributor, do **not** push, commit publicly, or open a PR for a fix to a non-public security
issue yourself — **including in a public fork**, since a public-fork commit or PR is itself a
disclosure. When reporting, you may include a suggested fix patch privately in your report to
`private@pulsar.apache.org` — never in a public commit or PR.

The neutrality rules below are for **whoever commits the fix — i.e. the project team**. Until the
vulnerability has been publicly announced, the commit message and PR title/body must **not** reveal its
security nature, even when the fix touches security-adjacent code (authentication/authorization,
deserialization, TLS, networking). Describe the behaviour change neutrally; a commit or PR that
advertises "fixes the CVE", "security fix", or "patches the vulnerability" discloses the issue before
it is announced and defeats coordinated disclosure.

The same discretion applies to **everyone** — and identically to any AI tooling acting on a
contributor's behalf — in public GitHub issues, discussions, and review comments until the
announcement.

**Already-public CVEs in dependencies are an exception.** The rules above concern *non-public* Pulsar
vulnerabilities. A PR or commit that upgrades a dependency to address an **already publicly disclosed**
CVE in that dependency does **not** follow them — the CVE is already public. Name its id directly in
the PR title and/or description (use the description when there are several CVE ids).

## Checking exposure to an already-public CVE

For a CVE that is **already public** (for example, in a dependency) and you want to check Pulsar's
exposure or whether it is already fixed, this is **not** a private disclosure — the right venue is a
GitHub issue on apache/pulsar or a question on the **dev@pulsar.apache.org** mailing list.

Before asking, search PRs and issues with the CVE id at <https://github.com/apache/pulsar/pulls>
(also check issues and **closed** PRs/issues) — the fix may already be merged or the question already
answered.

## Security model and scope

Pulsar's security model is not formally/explicitly defined. Two long-standing design assumptions
matter when deciding whether something is actually a vulnerability:

**Pulsar Functions and connectors execute fully trusted code.** The function instance runtime exists
to run user-provided code — *remote code execution is its core purpose, not a flaw.* (There have been
some reports claiming that "the function instance runtime allows running user-provided code and results
in an RCE"; this is expected, by-design behaviour, not a security issue.) The available execution models also let the code modify its environment:

- The **thread** and **process** runtimes can read or modify any files and state accessible to the
  process they run in.
- The **Kubernetes** runtime, on its own, does not restrict access to resources in the Kubernetes
  cluster. The project provides hooks for custom Kubernetes-runtime *hardening*, but such hardening is
  **not** part of the project.

Therefore, Pulsar Functions and connectors must only run code that is **fully trusted**.

**Clusters rely on network-perimeter security.** Pulsar is designed to be deployed behind a trusted
network perimeter where only trusted users can reach the cluster. The project does **not** implement
explicit controls against malicious **denial-of-service (DoS)** attacks. Rate limiting exists to mitigate
*unintentional* DoS — e.g. improper configuration or thundering-herd effects — but it is **not** a
defense against a deliberate DoS by an attacker.

Reports that amount to "a trusted function can run code / modify its environment" or "a
perimeter-trusted client can cause a denial-of-service" generally fall **outside** Pulsar's threat
model. When in doubt, ask through the reporting channels above rather than assuming.

## Supported versions

Pulsar's supported versions are listed at <https://pulsar.apache.org/contribute/release-policy/>.
Security fixes are made to supported versions; users should upgrade to a supported version to receive
security updates.
