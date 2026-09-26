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

# Comparing revisions

An A/B comparison runs the same scenario on a baseline revision and a candidate revision, and compares the runs. The
differences worth finding are often a few percent, about as large as the variation between two runs of the same
revision, so keep everything except the revision the same and repeat the runs.

## Prepare the host

Configure the host as [the performance testing environment setup](../environment/README.md) describes, so that the
CPU runs at a fixed frequency, and start it before the runs. Stop other builds, benchmarks and applications that
use the CPU, and keep the disk that holds Docker's data less than 90 % full.

## Check out both revisions

Use a worktree for each revision, so that both can be built and run without switching branches. Both revisions need
the same performance tests, and everything they build from, so that only the code under test differs.

When the candidate changes only the code under test, and the base revision already has the same performance tests,
check out the base revision as the baseline. For example, from a candidate checkout based on `origin/master`:

```bash
git worktree add --detach ../pulsar-baseline origin/master
```

Otherwise, for example when the candidate also changes the performance tests, or the base revision doesn't have them
yet, create the baseline from the candidate and restore only the code under test from the base revision. Take the
paths from `git diff --stat origin/master...HEAD`, such as the broker's and the managed ledger's sources, and commit
the result, so that the baseline's runs name their own commit:

```bash
git worktree add --detach ../pulsar-baseline HEAD
git -C ../pulsar-baseline checkout origin/master -- pulsar-broker/src/main managed-ledger/src/main
git -C ../pulsar-baseline commit -m "Baseline: the code under test from origin/master"
```

## Keep the runs and images of both revisions apart

Share one reports root between the checkouts and name the experiment with `--name`. The runs of both revisions then
sit under the same name, one directory per branch. A detached checkout's runs go under the closest branch that
contains its commit, as [Where runs are written](running-scenarios.md#where-runs-are-written) describes: a baseline
checked out from `origin/master` is `master`, and a baseline commit that no branch contains is `detached-<commit>`.
A baseline at an earlier commit of the candidate's branch can go under that branch, beside the candidate's runs: tell
them apart by the commit that each report names, or commit the baseline as the previous section describes, so that
its runs get a directory of their own.
Set the root in `~/.gradle/gradle.properties` with an absolute path:

```properties
performance.reportsDir=/data/pulsar-performance-reports
```

Both checkouts build the Docker images under the same tag by default, so each run would rebuild the images that the
other checkout's run replaced. Give each checkout its own tag with `-Pdocker.tag`.

## Run

Run the checkouts one at a time, since concurrent runs compete for the CPU. Alternate the revisions and run each
several times, so that a drift of the host, such as its temperature, affects both alike:

```bash
# In the baseline checkout
./gradlew :tests:performance:launcher:run -Pdocker.tag=baseline \
  --args='--config tests/performance/scenarios/iot-telemetry-high-rate.yaml --name my-change-ab'

# In the candidate checkout
./gradlew :tests:performance:launcher:run -Pdocker.tag=candidate \
  --args='--config tests/performance/scenarios/iot-telemetry-high-rate.yaml --name my-change-ab'
```

Keep the scenario, its settings and the profiler options identical for both revisions. Profiling has a measurement
cost, so compare profiled runs only with profiled runs.

## Compare

- Check each run's correctness first. Leave out runs with ordering violations, invalid messages, timeouts, broker
  restarts or out-of-memory errors, even if the clients recovered.
- Check the host in each report: a run in which the CPU throttled isn't comparable to one in which it didn't.
- Compare the same measures across the runs: throughput, the publish and end-to-end latency percentiles, and the
  backlog. Compare the medians of the repetitions, and look at their spread: a difference smaller than the spread
  between runs of the same revision isn't a result.
- Report percentage changes alongside the absolute values, and say which direction is better for each measure.
- Compare profiles with the correlator, see [Comparing two profiles](analyzing-profiles.md#comparing-two-profiles).

Each run's report records the commit it was made from, and whether the checkout had uncommitted changes. Keep the
scenario, any local changes, the runs and the conclusions together, so that a later run can reproduce the workload.
