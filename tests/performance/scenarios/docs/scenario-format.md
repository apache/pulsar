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

# Scenario format

A scenario is a YAML configuration tree rather than a format tied to a test class. The shared loader in
[`common`](../../common) resolves the tree, and the launcher and the workload applications each read the subtree
they own. The launcher writes the resolved tree to `resolved-config.yaml` in the run directory and mounts that file
into the workload containers.

## Sections

- `cluster`: the Pulsar topology (`brokers`, `bookies`) and the environment variables of each kind of container:
  `brokerEnvs` and `bookkeeperEnvs` for the broker and the bookies, and `producerEnvs` and `consumerEnvs` for the
  producer and the consumer (application) containers of the workload, for example `GLIBC_TUNABLES`. A
  `JAVA_TOOL_OPTIONS` in `producerEnvs` or `consumerEnvs` is appended to the launcher's JVM options for those
  containers.
- `workloads`: named workload configurations, currently `iotTelemetry`, see
  [the IoT telemetry scenario](iot-telemetry.md). Workload-specific fields live below their workload name, so that
  another launcher or application can reuse the same file without interpreting unrelated sections. A workload command
  can select its subtree with `--config-path`.
- `profiling`: optional profiler options for the broker, producer and consumer processes, see
  [Profiling](../../docs/profiling.md#configuring-profiling).
- `output`: optional; `output.name` names the scenario's runs in the reports hierarchy instead of the file name, see
  [Where runs are written](../../docs/running-scenarios.md#where-runs-are-written).

## Inheritance

A top-level `extends` entry inherits one file or an ordered list of files:

```yaml
extends: [cluster.yaml, workloads/iot-base.yaml]
workloads:
  iotTelemetry:
    rate: 1000
    clientRestartFraction: 0.1
profiling:
  brokerOptions: event=cpu,interval=10ms,jfrsync=profile
  producerOptions: ~
output:
  name: iot-restart-profile
```

- Each inherited path is resolved relative to the file that declares it; absolute paths also work. Keep inherited
  files together.
- Parents can inherit other files recursively. Parents are applied in list order, and the current file last.
- Mappings merge recursively, while scalar values and lists replace earlier values. An explicit YAML `null` or `~`
  removes an inherited entry.
- Cycles, missing files, non-mapping roots and invalid `extends` entries are rejected.

## Environment overrides

For a one-off change, prefix an existing scalar path with `PULSAR_PERFORMANCE_`, uppercase it and separate the path
elements with underscores. The loader keeps the scalar's YAML type:

```bash
PULSAR_PERFORMANCE_WORKLOADS_IOTTELEMETRY_RATE=2000 \
./gradlew :tests:performance:launcher:run \
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml'
```

Environment overrides are applied after inheritance. They only update paths present in the resolved tree, which
keeps misspelled or inapplicable settings from creating new configuration. Keep maintained workloads in scenario
files, and use environment overrides for temporary measurements rather than as the only record of a workload.

## Warmup and the measurement window

The `iotTelemetry` workload can run traffic before the measurement begins:

- `warmupSeconds` with a positive `rate`, or `warmupMessages` when `rate: 0`. The two are mutually exclusive.
- `warmupRounds` repeats that traffic, and `warmupRoundDelaySeconds` adds an idle stabilization period after each
  fully drained round, including the final round. The default is one round with no delay.
- A round is fully drained only after every application has uniquely received its cumulative warmup message count;
  producer send completions alone don't release the barrier.

Warmup traffic remains part of the delivery and ordering validation. The producer throughput and the
epoch-millisecond measurement boundaries in `producer-summary.json` cover only the measured messages. Every consumer
summary records its first and last measured-message receipt. The measurement window runs from the producer's
measurement start through the latest last receipt across all applications; the launcher uses it to cut the
[measurement recording](../../docs/profiling.md#the-measurement-recording) of a profiled run.

The window assumes that the producer, consumer and broker clocks agree, as they do for containers on the same Docker
host. Multi-host experiments need synchronized clocks; the launcher doesn't estimate clock skew or correct the window.
