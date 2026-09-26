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
[Profiling an integration-test cluster](../CONTRIBUTING.md#profiling-an-integration-test-cluster) describes
`profilingIntegrationTest`, which runs a test with async-profiler.

## Directories

- [`integration`](integration): the integration tests and their test framework.
- [`docker-images`](docker-images): the Docker images that the integration tests use, and their contents:
  - [`latest-version-image`](docker-images/latest-version-image): the `pulsar-test-latest-version` image, the Pulsar
    image with the test functions, plugins, offloaders and Go and Python function examples.
  - [`java-test-image`](docker-images/java-test-image): the `java-test-image` image, a smaller image without the Go and
    Python function examples.
  - [`java-test-functions`](docker-images/java-test-functions) and
    [`java-test-plugins`](docker-images/java-test-plugins): the functions and plugins in the images.
- [`certificate-authority`](certificate-authority/README.md): the test certificate authority and the TLS certificates
  that the tests use.
- [`scripts`](scripts): scripts to run before and after the integration tests.
- [`performance`](performance/README.md): the performance test scenarios, their launcher and the guidance for
  repeatable profiling runs. [Setting up the performance testing environment](performance/environment/README.md)
  describes configuring a host for consistent results.
- [`compose`](compose/README.md): Docker Compose files that start a Pulsar cluster.
- Tests of the packaged clients: [`pulsar-client-shade-test`](pulsar-client-shade-test),
  [`pulsar-client-admin-shade-test`](pulsar-client-admin-shade-test),
  [`pulsar-client-all-shade-test`](pulsar-client-all-shade-test),
  [`pulsar-client-v5-shade-test`](pulsar-client-v5-shade-test),
  [`pulsar-client-v5-all-test`](pulsar-client-v5-all-test),
  [`pulsar-client-admin-v5-test`](pulsar-client-admin-v5-test),
  [`pulsar-client-native-image`](pulsar-client-native-image) (GraalVM native image) and
  [`pulsar-client-test-bcfips`](pulsar-client-test-bcfips) (Bouncy Castle FIPS).
