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

# Performance scenarios

A scenario is a YAML file that describes a performance test: the Pulsar cluster, the workload that runs against it
and, optionally, what to profile. Scenarios build on each other with `extends`, so that a variation states only what
it changes. Run a scenario from the repository root with the launcher:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'
```

Use the `profile` task instead of `run` for a scenario with profiling options. [The performance testing
guide](../README.md) walks through running a scenario and reading its report.

## Scenarios for the standalone launcher

These scenarios run the IoT telemetry workload: keyed telemetry messages from gateways to Key_Shared applications,
with delivery and ordering checks. [The IoT telemetry scenario](docs/iot-telemetry.md) describes the workload in
detail.

| Scenario | What it runs | Memory configuration |
|---|---|---|
| [`iot-telemetry.yaml`](iot-telemetry.yaml) | **Start here.** The full topology: 100 gateways, 30 topics and 20 applications with 100 pods each, 1,000 messages per second for 120 s after a 20 s warmup | medium, about 11 GB |
| [`iot-telemetry-base.yaml`](iot-telemetry-base.yaml) | The defaults that the other IoT scenarios extend: a workload of one gateway and one application with one pod, and the medium-memory configuration | medium, about 11 GB |
| [`iot-telemetry-small.yaml`](iot-telemetry-small.yaml) | A smaller topology: 10 gateways, 30 topics and 20 applications with 10 pods each, 1,000 messages per second for 120 s after a 20 s warmup | low, about 3 GB |
| [`iot-telemetry-small-restarts.yaml`](iot-telemetry-small-restarts.yaml) | The smaller topology, restarting 10 % of each application's clients every 30 s | low, about 3 GB |
| [`iot-telemetry-restarts.yaml`](iot-telemetry-restarts.yaml) | The full topology, restarting 10 % of each application's clients every 30 s | medium, about 11 GB |
| [`iot-telemetry-high-rate.yaml`](iot-telemetry-high-rate.yaml) | A high rate: five million messages at 30,000 messages per second, from 500 preconnected producers to one topic, consumed by 5 applications with 10 pods each | high, about 14 GB |

## Configurations

The [`configs`](configs) directory has the configurations that a scenario combines with its workload: the cluster, the
memory, and profiling. They aren't scenarios of their own. A scenario extends a memory configuration, and profiling is
added on the command line with the launcher's `--extends` option.

### Memory configurations

A memory configuration sets the cluster, from [`cluster-base.yaml`](configs/cluster-base.yaml) with the settings that
every configuration shares, and the heap and direct memory of every container's JVM, through `PULSAR_MEM` in each
component's `env`. The memory it needs is what the host has to have available to Docker, which on macOS and Windows
is the memory of Docker's virtual machine:

| Configuration | Cluster | Recommended memory available to Docker |
|---|---|---|
| [`iot-telemetry-low-mem.yaml`](configs/iot-telemetry-low-mem.yaml) | [`cluster-low-mem.yaml`](configs/cluster-low-mem.yaml): one broker and one bookie, which holds every ledger with an ensemble, write quorum and ack quorum of 1, and heaps that the JVMs commit as they use them | about 3 GB. For checking that a scenario works on a small host; not meant for profiling. The applications' heap is sized for `iot-telemetry-small.yaml`, and the full topology runs out of it |
| [`iot-telemetry-medium-mem.yaml`](configs/iot-telemetry-medium-mem.yaml) | [`cluster-medium-mem.yaml`](configs/cluster-medium-mem.yaml): one broker and two bookies | about 11 GB. The default, which `iot-telemetry-base.yaml` extends |
| [`iot-telemetry-high-mem.yaml`](configs/iot-telemetry-high-mem.yaml) | [`cluster-high-mem.yaml`](configs/cluster-high-mem.yaml): one broker and three bookies, with a larger heap and direct memory for each | about 14 GB. For the high-rate scenarios |

A scenario that needs another memory configuration than the default extends it after the scenario it builds on, as
[`iot-telemetry-high-rate.yaml`](iot-telemetry-high-rate.yaml) does: `extends: [iot-telemetry.yaml,
configs/iot-telemetry-high-mem.yaml]`. To run a scenario with another memory configuration without a new file, add it
on the command line:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --extends configs/iot-telemetry-high-mem'
```

### Profiles

These files add profiling to any scenario with the launcher's `--extends` option, and the scenario then runs with the
`profile` task, see [Profiling](../docs/profiling.md). Profile a scenario with the medium- or the high-memory
configuration, not the low-memory one.

| File | What it adds |
|---|---|
| [`profile-broker.yaml`](configs/profile-broker.yaml) | Profiles the broker |
| [`profile-gateways.yaml`](configs/profile-gateways.yaml) | Profiles the gateways, the producer |
| [`profile-applications.yaml`](configs/profile-applications.yaml) | Profiles the applications, the consumers |

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/profile-broker'
```

### Heap dumps

[`heap-dumps-broker.yaml`](configs/heap-dumps-broker.yaml) adds heap dumps of the broker to any scenario: at its
highest heap usage, after every application has received every message, and when it runs out of memory. A heap dump
stops the JVM while it is written, so such a run is for finding what holds the memory, not for measuring. [Heap
dumps](../docs/heap-dumps.md) describes the other settings, such as dumps at given times and of the other components.

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --extends configs/heap-dumps-broker'
```

## Scenarios for the legacy TestNG runner

The scenarios of [the legacy TestNG profiling runner](../docs/legacy-testng-runner/README.md#scenario-files), which runs
`pulsar-perf` and is kept until its scenarios are migrated to the standalone launcher, are in
[its test resources][profiling-scenarios]:
`tests/integration/src/test/resources/org/apache/pulsar/tests/integration/profiling`.

## Writing a scenario

Start from the scenario closest to what you want to measure, and extend it with the settings you change. For
example, a higher rate and larger messages on the smaller topology, in
`tests/performance/scenarios/iot-telemetry-small-5k.yaml`:

```yaml
extends: iot-telemetry-small.yaml
workloads:
  iotTelemetry:
    rate: 5000
    payload:
      size: 1024
```

Run it like any other scenario. Its runs are named after the file, `iot-telemetry-small-5k`, unless it sets
`output.name`. It keeps the memory configuration of the scenario it extends, here the low-memory one; extend another
one after it, from [`configs`](#memory-configurations), when the workload needs more memory. To try a single value
without a new file, set it on the command line:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --set workloads.iotTelemetry.rate=5000'
```

[The scenario format](docs/scenario-format.md) describes the sections, inheritance,
[environment overrides](docs/scenario-format.md#environment-overrides) and the warmup, and
[the IoT telemetry scenario](docs/iot-telemetry.md#scenarios) the workload's settings. Add a scenario that others can
use to this directory and to the tables above, with a guide under [`docs`](docs) when it needs one, and a configuration
that scenarios combine, such as a memory configuration, to [`configs`](configs). `ScenarioFilesTest` in the launcher's
tests resolves every scenario in this directory, and fails when one no longer resolves to a valid cluster, or leaves a
container's memory unset.

[profiling-scenarios]: ../../integration/src/test/resources/org/apache/pulsar/tests/integration/profiling
