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

# Pulsar tests

This directory contains the integration tests, the performance tests and the tests of the packaged Pulsar clients.
[The microbenchmarks](../microbench/README.md) for single classes and methods are in the `microbench` module.

## Integration tests

The integration tests use [Testcontainers](https://testcontainers.com/) to start Pulsar clusters in Docker containers
and run [TestNG](https://testng.org/) tests against them, so Docker must be installed and running.

Run the commands in the root directory of the repository. The full suite is heavy and slow, so run a single test
class locally:

```shell
./gradlew :tests:integration:integrationTest --tests "org.apache.pulsar.tests.integration.<TestClass>"
```

The tests run against the `apachepulsar/pulsar-test-latest-version:latest` Docker image. `integrationTest` builds the
image, and the Pulsar image it is based on, before running the tests. Gradle builds them again only when something
that goes into them has changed, or when the image has been removed, so the tests always run against the current code.

- `-PintegrationTestSuiteFile=<suite>.xml` runs a TestNG suite from
  [`integration/src/test/resources`](integration/src/test/resources), for example `pulsar-messaging.xml`.
- `-PtestGroups=<groups>` and `-PexcludedTestGroups=<groups>` select TestNG groups.
- `-Pdocker.tag=<tag>` builds and uses the images under another tag than `latest`.
- The `PULSAR_TEST_IMAGE_NAME=<image>` environment variable runs the tests against another image. Gradle builds it
  only when it is `apachepulsar/java-test-image:<tag>`.
- `-Pinttest.skipDockerBuild` skips building the image when it has been built or loaded otherwise, as in CI.

To run the whole suite, use
[Personal CI](../CONTRIBUTING.md#running-the-full-ci-pipeline-personal-ci).

### Profiling an integration test

> [!NOTE]
> For performance optimizations, use [the performance tests](performance/README.md) rather than a profiled
> integration test. They run repeatable scenarios with a report for every run, compare two revisions, and profile
> where threads wait as well as where they use CPU. Profiling an integration test shows what the cluster of that
> particular test does.

`profilingIntegrationTest` runs an integration test with
[async-profiler](https://github.com/async-profiler/async-profiler) attached to the cluster's components inside their
containers, such as the broker and the bookies, rather than to the JVM the test runs in. Any integration test can
be profiled without changing it; name it and the components to profile:

```shell
./gradlew :tests:integration:profilingIntegrationTest --tests "<SomeIntegrationTest>" \
  -Pinttest.asyncprofiler.components=broker,bookie
```

- `-Pinttest.asyncprofiler.components` takes `broker`, `proxy`, `functionworker`, `bookie`, `zookeeper` or `all`, and
  defaults to `broker` for this task. `PulsarClusterSpec.profileBroker` and its siblings fall back to it, so it
  doesn't change a test that sets those flags itself. It also enables the manual tests, so it profiles a cluster
  through the plain `integrationTest` task too.
- The task builds `apachepulsar/java-test-image:<tag>-asyncprofiler`, the test image with async-profiler, under a tag
  of its own so that it never replaces the image of the other integration tests. It runs the test with retries off,
  and always runs.
- It relaxes the kernel's `perf_event` limits, which the `cpu` sampling engine needs, from a privileged throwaway
  container. `-Pinttest.asyncprofiler.skipPerfEventTuning` skips that, for a host where they are already set, such as
  one configured by [the performance testing environment setup](performance/environment/README.md), or where Docker
  disallows privileged containers; the run continues either way, with less accurate native stacks.
- Each profiled container writes a recording to `tests/integration/build/`, named
  `inttest_profile_<commit>_<time>_<container>_<pid>.jfr`. The commit id, from `git rev-parse --short HEAD` or
  `-Pgit.commit.id.abbrev=<id>`, tells apart the profiles of the revisions before and after a change.
- `-Pinttest.asyncprofiler.opts=<agent options>` (default `event=cpu,lock=1ms,alloc=2m,jfrsync=profile`),
  `-Pinttest.asyncprofiler.outputformat=<ext>` and `-Pinttest.asyncprofiler.dir=<dir>` change the recording.
- `-Pdocker.wolfi` builds the images from Wolfi instead of Alpine, which makes the `GLIBC_TUNABLES` that a test sets
  take effect.

Without `--tests`, the task runs `PulsarProfilingTest`, a `pulsar-perf` workload of the legacy TestNG profiling
runner. [The performance testing guide](performance/README.md) leads to rendering flame graphs from the recordings
and analyzing them, and to the legacy runner.

## Performance tests

The performance tests are for running performance test experiments. They run a Pulsar cluster and its workloads in
Docker containers on one host, as a scenario describes them, and write a report for every run: throughput, latency,
delivery and ordering checks, and the host's CPU state. Runs can be profiled with async-profiler, JDK Flight Recorder
and jonoffcpu's off-CPU recording at the same time, and two revisions can be compared. The experiments run from the
command line and write their results to files, so they can be automated, including tuning by AI agents.
[The performance testing guide](performance/README.md) is a tutorial for getting started.

## Directories

- [`integration`](integration): the integration tests and their test framework.
- [`docker-images`](docker-images): the Docker images that the integration tests use, and their contents:
  - [`latest-version-image`](docker-images/latest-version-image): the `pulsar-test-latest-version` image, the Pulsar
    image with the test functions, plugins, offloaders and Go and Python function examples.
  - [`java-test-image`](docker-images/java-test-image): the `java-test-image` image, a smaller image without the Go
    and Python function examples.
  - [`java-test-functions`](docker-images/java-test-functions) and
    [`java-test-plugins`](docker-images/java-test-plugins): the functions and plugins in the images.
- [`certificate-authority`](certificate-authority/README.md): the test certificate authority and the TLS certificates
  that the tests use.
- [`scripts`](scripts): scripts to run before and after the integration tests.
- [`performance`](performance/README.md): the performance test scenarios, their launcher, the configuration of a
  host for consistent results and the guidance for repeatable profiling runs.
- [`compose`](compose/README.md): Docker Compose files that start a Pulsar cluster.
- Tests of the packaged clients: [`pulsar-client-shade-test`](pulsar-client-shade-test),
  [`pulsar-client-admin-shade-test`](pulsar-client-admin-shade-test),
  [`pulsar-client-all-shade-test`](pulsar-client-all-shade-test),
  [`pulsar-client-v5-shade-test`](pulsar-client-v5-shade-test),
  [`pulsar-client-v5-all-test`](pulsar-client-v5-all-test),
  [`pulsar-client-admin-v5-test`](pulsar-client-admin-v5-test),
  [`pulsar-client-native-image`](pulsar-client-native-image) (GraalVM native image) and
  [`pulsar-client-test-bcfips`](pulsar-client-test-bcfips) (Bouncy Castle FIPS).
