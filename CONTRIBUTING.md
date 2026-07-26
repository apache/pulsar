<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Contributing to Apache Pulsar

We would love for you to contribute to Apache Pulsar and make it even better! The **authoritative**
contributor guide is the [Apache Pulsar Contributing Guide](https://pulsar.apache.org/contribute/) —
please read it before starting. This file is a quick, in-repo reference for the local development
workflow (build, test, PR, CI). For the big-picture module map and the Gradle build infrastructure see
[`ARCHITECTURE.md`](ARCHITECTURE.md); for code style see [`CODING.md`](CODING.md).

## Building

**JDK 21 or 25** is required to build `master` (bytecode targets Java 17; `-PskipJavaVersionCheck`
bypasses the check); `zip` is also needed. Use the bundled wrapper `./gradlew` (Linux/macOS) or
`gradlew.bat` (Windows) — no separate Gradle install. See the
[build-tooling setup guide](https://pulsar.apache.org/contribute/setup-buildtools/) and the
[IDE setup guide](https://pulsar.apache.org/contribute/setup-ide/).

```bash
# Compile and assemble everything (or a single module)
./gradlew assemble
./gradlew :pulsar-broker:assemble

# Source-code conformance across all modules — license headers (rat + spotlessCheck) and checkstyle
# (checkstyleMain + checkstyleTest). Fast: compiles nothing, never builds shadow jars.
./gradlew quickCheck
./gradlew spotlessApply            # auto-fix license headers

# Pre-PR gate — quickCheck plus compiling main + test sources of every module. Modules that depend
# on shaded artifacts on their compile classpath are skipped for compilation so no shadow jar is built.
./gradlew sanityCheck

# Verify bundled-dependency LICENSE/NOTICE coverage (after changing a runtime dependency)
./gradlew checkBinaryLicense

# Start a standalone Pulsar service (broker + bookie + metadata in one JVM)
bin/pulsar standalone

# Build docker images apachepulsar/pulsar(-all):latest
./gradlew docker        # or docker-all
```

For the Gradle build infrastructure and how to change build files (convention plugins, version
catalog, configuration-cache rules), see
[`ARCHITECTURE.md` → Build infrastructure](ARCHITECTURE.md#build-infrastructure).

## Running tests

Most of these per-module "unit tests" are actually **integration-style** — they start a real in-JVM
broker (`MockedPulsarServiceBaseTest` / `pulsarTestContext`) rather than testing a class in isolation.
The **container-based integration tests** that run against a Pulsar Docker image are separate; see
[Integration tests](#integration-tests) below.

```bash
# Always scope test runs with --tests — running a whole module's test task is slow.
# Run a single test class
./gradlew :pulsar-client-original:test --tests "ConsumerBuilderImplTest"
# Run a single test method
./gradlew :pulsar-client-original:test --tests "ConsumerBuilderImplTest.<methodName>"
# Run all tests in a specific package
./gradlew :pulsar-broker:test --tests "org.apache.pulsar.broker.admin.*"
```

> Note the [module-name-vs-directory gotcha](ARCHITECTURE.md#module-name-vs-directory-name-gotcha):
> directory `pulsar-client/` is the Gradle project `:pulsar-client-original`.

### Test groups (TestNG)

Tests use **TestNG** and are tagged with `@Test(groups = "...")`. By default the build **excludes the
`quarantine` and `flaky` groups** (`excludedTestGroups` default = `quarantine,flaky`), so to run a
single test that lives in one of those groups you must clear the exclusion:

```bash
# Run a specific test that is in the flaky/quarantine group (otherwise excluded by default)
./gradlew :pulsar-broker:test -PexcludedTestGroups='' --tests "<SomeFlakyTest>"
```

CI selects whole groups with `-PtestGroups=<groups>` and `-PexcludedTestGroups=<groups>` (e.g.
`broker,broker-admin`); locally prefer `--tests` to scope to specific classes instead of running an
entire group. CI splits `pulsar-broker` tests into groups (see
`pulsar-build/run_unit_group_gradle.sh` and `gradle/verify-test-groups.gradle.kts`). Tests with no
group are treated as `other` at runtime. `./gradlew verifyTestGroups` reports group assignments and
flags tests not covered by any CI group.

Other test-related properties: `-PtestJavaVersion=17` (run tests on a different JDK toolchain),
`-PtestRetryCount=N`, `-PtestFailFast=true|false`, `-PprotobufVersion=4.31.1` (protobuf v4
compatibility tests).

Failed tests are retried once by default (`testRetryCount=1`; `0` when running inside the IDE). When
running tests locally, prefer **`-PtestRetryCount=0`** to catch failures (including flakiness) early
instead of having retries mask them.

### Integration tests

Integration tests live in `tests/` (see `tests/README.md`). They use
[Testcontainers](https://www.testcontainers.org/) to bring up Pulsar services in Docker, so **Docker
must be installed and running**. Build the test image first, then run the tests.

The full integration suite is heavy and slow. **In local development, always run individual
integration tests** rather than the whole suite — pass `--tests` to select a class (TestNG then
discovers it directly from the classpath):

```bash
./gradlew :tests:latest-version-image:dockerBuild     # build the docker test image
./gradlew :tests:integration:integrationTest --tests "org.apache.pulsar.tests.integration.<TestClass>"
```

To run the **entire** integration test set, use **Personal CI** (below) rather than running it
locally. (`integrationTest` also accepts `-PtestGroups` / `-PexcludedTestGroups` and
`-PintegrationTestSuiteFile=<suite>.xml` to pick a specific TestNG suite.)

### Running the full CI pipeline (Personal CI)

The full test suite is large and slow to run locally. While iterating on a change, run only the
narrowly-scoped tests relevant to the change (a single test class or package, see above) rather than
a module's entire test task. To validate a larger change against the **full** CI pipeline, do not run
everything locally — use **Personal CI**, which runs Pulsar's CI workflows in the contributor's own
GitHub fork.

If Personal CI is not yet set up, follow the
[Personal CI documentation](https://pulsar.apache.org/contribute/personal-ci/) to enable it on your
fork. Once it is set up, the loop is:

1. Keep the local `master` up-to-date with `apache/pulsar` and rebase the feature branch on it.
2. Push the feature branch to the **fork** to trigger CI runs there. CI runs against the PR opened in
   your own fork (it is normal to have a PR open in the fork *and* a PR for the same branch open in
   `apache/pulsar` at the same time).
3. Monitor CI status on the fork and fix failures.
4. Open the PR to `apache/pulsar` only after the fork's CI is green.

Once the PR to `apache/pulsar` has been opened, stop rebasing as part of this loop: step 1's rebase
no longer applies — bring in upstream changes by merging instead (see [Pull requests](#pull-requests)).

### Retrying CI after flaky-test failures

Pulsar has a large number of flaky tests, so GitHub Actions jobs on a PR sometimes fail for reasons
unrelated to the change. When a failure appears to be flakiness rather than a regression caused by the
PR, comment **`/pulsarbot rerun`** on the PR to re-run the failed jobs of the workflow run. It only
takes effect once the workflow run has **completed** (all jobs finished), so wait for the run to
finish before commenting.

For a PR from a fork, a project maintainer must **approve** the workflow runs before they execute, and
approval is required again whenever new changes are pushed. This adds latency to each run and makes it
slow to tell flaky failures apart from genuine ones. Setting up **Personal CI** (above) sidesteps this
— the full pipeline runs in your own fork without maintainer approval — so it is especially useful when
a PR has legitimate test failures that you need to iterate on.

If a flaky test that is unrelated to your change blocks your PR, you can move it to the `flaky` group
or disable it within your PR to unblock merging — and report it (search the
[existing flaky-test issues](https://github.com/apache/pulsar/issues?q=is%3Aissue+state%3Aopen+flaky)
first). Don't push empty "trigger CI" commits to force a rerun; use `/pulsarbot rerun` instead.

## Pull requests

PRs must follow `.github/PULL_REQUEST_TEMPLATE.md`. PR titles follow the
`[<type>][<optional scope>] <description>` convention (e.g. `[fix][broker] ...`,
`[improve][build] ...`) — refer to `.github/workflows/ci-semantic-pull-request.yml` for the valid
`[type]` and `[scope]` prefixes, which are enforced by CI. The `<description>` should be in
imperative form, like a good commit message's subject line, and **should not reference issue numbers**
(put `Fixes #N` / `Closes #N` in the description instead).

**Keep each PR focused on one change.** Don't bundle unrelated drive-by refactoring (for example,
de-duplicating code you happened to touch) into a feature or bug-fix PR — it widens the review surface
and the risk; note such improvements as follow-ups instead. Likewise, **don't reformat unrelated files
or lines that aren't part of your change** (whitespace, import reordering, re-wrapping) — drive-by
formatting hides the real change in review and pollutes `git blame`. **Discuss large refactorings on
dev@pulsar.apache.org before investing effort**: there are many similar code duplications across
Pulsar, and a PR for each creates more maintainer burden than value. AI agents make it easy to generate
large volumes of refactoring PRs, and the project pushes back on these. Every change needs a real,
identifiable contributor who takes responsibility for it; unattributed AI-agent-style contributions —
especially larger ones from anonymous profiles or from people not actually using Pulsar — are typically
rejected.

**Describe the change.** The PR description must cover, at minimum, the **Motivation (why?)** and the
**Modifications (what / how?)** — these map to the corresponding sections of the PR template. A title
alone, or a description that only restates the title, is not sufficient. Link the related issue with
`Fixes #N` (or equivalently `Closes #N`) for an issue the PR resolves, `Main Issue: #N` for one task
of a larger issue, or `PIP: #N` for a proposal.

**Never rebase a PR branch once the PR is opened in `apache/pulsar`.** Rebasing rewrites history and
disrupts reviewers (it invalidates review comments and makes incremental diffs unreadable). To bring
in upstream changes, instead fetch from the `apache/pulsar` remote and **merge** its `master` into the
PR branch:

```bash
git fetch <apache-pulsar-remote>          # e.g. `upstream` or `apache`
git merge <apache-pulsar-remote>/master
```

(Rebasing onto an updated `master` is fine *before* the PR is opened — see the Personal CI loop above
— but not after.)

### Branches and backports

Target `master` first. Once the change is merged, **project maintainers handle backporting** a bug or
security fix to the supported maintenance branches (`branch-4.2`, `branch-4.0`, …) when
the bug is also present there, per the
[release/support policy](https://pulsar.apache.org/contribute/release-policy/). New **features** are
**not** added to LTS / maintenance branches without a dev@pulsar.apache.org discussion (and usually a
PIP), to avoid regressions in stable releases.

Backporting is done by **cherry-picking commits in their original merge order**, which avoids
unnecessary merge conflicts; sometimes a dependent change must be cherry-picked before the fix itself.
AI tools are effective at resolving the merge conflicts that arise during a backport.

## Reporting security vulnerabilities

See [`SECURITY.md`](SECURITY.md) (latest at <https://github.com/apache/pulsar/security/policy>) and
<https://pulsar.apache.org/security/>. In short: report a
suspected vulnerability **privately** (never in a public issue, PR, or commit), and never reveal the
security nature of a change in public until it is announced. An **already-public** CVE that you only
want to check Pulsar's exposure to is *not* a private disclosure — search the CVE id in apache/pulsar
PRs/issues first, then ask via a GitHub issue or dev@pulsar.apache.org.

## AI coding agents

If you use an AI coding assistant (Claude Code, Copilot, Cursor, Gemini, Codex, Aider, …), see
[`AGENTS.md`](AGENTS.md) for the agent-facing guidance — a routing index into this guide,
[`ARCHITECTURE.md`](ARCHITECTURE.md), [`CODING.md`](CODING.md), and [`SECURITY.md`](SECURITY.md), plus
the guardrails that apply specifically to AI-made changes.
