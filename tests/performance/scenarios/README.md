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
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml'
```

Use the `profile` task instead of `run` for a scenario with profiling options. [The performance testing
guide](../README.md) walks through running a scenario and reading its report.

## Scenarios for the standalone launcher

These scenarios run the IoT telemetry workload: keyed telemetry messages from gateways to Key_Shared applications,
with delivery and ordering checks. [The IoT telemetry scenario](docs/iot-telemetry.md) describes the workload in
detail.

| Scenario | What it runs |
|---|---|
| [`iot-telemetry-local.yaml`](iot-telemetry-local.yaml) | **Start here.** A topology sized for a workstation: 10 gateways, 30 topics and 20 applications with 10 clients each, 1,000 messages per second for 120 s after a 20 s warmup, restarting 10 % of each application's clients every 30 s |
| [`iot-telemetry-local-steady.yaml`](iot-telemetry-local-steady.yaml) | The same without client restarts |
| [`iot-telemetry.yaml`](iot-telemetry.yaml) | The full topology: 100 gateways and 20 applications with 100 clients each. The base of the other IoT scenarios |
| [`iot-telemetry-restarts.yaml`](iot-telemetry-restarts.yaml) | The full topology, restarting 10 % of each application's clients every 30 s |
| [`iot-telemetry-high-rate.yaml`](iot-telemetry-high-rate.yaml) | Saturation: five million messages without a rate limit, from 500 preconnected producers to one topic, consumed by 5 applications with 10 clients each |
| [`iot-telemetry-high-rate-profile.yaml`](iot-telemetry-high-rate-profile.yaml) | The saturation workload with the broker and the producer profiled; run it with the `profile` task |
| [`iot-telemetry-high-rate-dedup-snapshot-100k.yaml`](iot-telemetry-high-rate-dedup-snapshot-100k.yaml) | A diagnostic variation of the saturation workload with a deduplication snapshot every 100,000 entries |

## Scenarios for the legacy TestNG runner

These scenarios run `pulsar-perf` through [the legacy TestNG profiling runner](../docs/legacy-testng-runner.md),
which is kept until they are migrated to the standalone launcher.

| Scenario | What it runs |
|---|---|
| [`pulsar-profiling.yaml`](pulsar-profiling.yaml) | The runner's default settings, to start from |
| [`read-completion-isolation.yaml`](read-completion-isolation.yaml) | 500 producers on separate connections to one topic, with one Exclusive consumer. [Read-completion queue isolation](docs/read-completion-isolation.md) describes it and its variations: [Shared](read-completion-isolation-shared.yaml), [Failover](read-completion-isolation-failover.yaml), and [64/32 KiB](read-completion-isolation-64k-32k.yaml) and [256/128 KiB](read-completion-isolation-256k-128k.yaml) channel watermarks |
| [`key-shared-500x20.yaml`](key-shared-500x20.yaml) | 500 producers and 20 consumers on one Key_Shared subscription |

## Writing a scenario

Start from the scenario closest to what you want to measure, and extend it with the settings you change. For
example, a higher rate and larger messages on the workstation topology, in
`tests/performance/scenarios/iot-telemetry-local-5k.yaml`:

```yaml
extends: iot-telemetry-local.yaml
workloads:
  iotTelemetry:
    rate: 5000
    payloadBytes: 1024
```

Run it like any other scenario. Its runs are named after the file, `iot-telemetry-local-5k`, unless it sets
`output.name`. To try a single value without a new file, override it with an environment variable:

```bash
PULSAR_PERFORMANCE_WORKLOADS_IOTTELEMETRY_RATE=5000 \
./gradlew :tests:performance:launcher:run \
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml'
```

[The scenario format](docs/scenario-format.md) describes the sections, inheritance, environment overrides and the
warmup, and [the IoT telemetry scenario](docs/iot-telemetry.md#scenarios) the workload's settings. Add a scenario that
others can use to this directory and to the tables above, with a guide under [`docs`](docs) when it needs one.
