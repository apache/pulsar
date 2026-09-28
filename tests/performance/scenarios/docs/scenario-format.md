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

- `cluster`: the Pulsar topology. `brokers` and `bookies` each have `replicas`, the number of containers, and `env`,
  their environment variables, such as Pulsar settings and `PULSAR_MEM`. The IoT telemetry scenarios get it from a
  [memory configuration](../README.md#memory-configurations), which also sets the workload containers' `PULSAR_MEM`.
- `workloads`: named workload configurations, currently `iotTelemetry`, see [the IoT telemetry
  scenario](iot-telemetry.md#settings). Its `gateways.env` and `applications.env` are the environment variables of the
  gateways' and the applications' containers, such as `PULSAR_MEM`, which sets the JVM's heap and direct memory, or
  `GLIBC_TUNABLES`. Their JVMs get the options of Pulsar's client tools, from `conf/pulsar_env.sh` and as
  `bin/pulsar-perf` adds them; `PULSAR_GC` and `PULSAR_EXTRA_OPTS` apply when set, and a `JAVA_TOOL_OPTIONS` is appended
  to the launcher's JVM options, such as the profiler agent. Workload-specific fields live below their workload name, so
  that another launcher or application can reuse the same file without interpreting unrelated sections. A workload
  command can select its subtree with `--config-path`.
- `profiling`: optional profiler options for the broker, the gateways and the applications, see
  [Profiling](../../docs/profiling.md#configuring-profiling).
- `heapDumps`: optional heap dumps of the broker, the gateways and the applications, when they run out of memory, at
  the highest heap usage and at given times, see [Heap dumps](../../docs/heap-dumps.md).
- `metrics`: optional; `metrics.intervalSeconds`, 5 by default, is how often VictoriaMetrics scrapes the metrics of
  the brokers, the bookies and ZooKeeper, and the period of the broker's stats, see [Metrics](../../docs/metrics.md).
- `output`: optional; `output.name` names the scenario's runs in the reports hierarchy instead of the file name, see
  [Where runs are written](../../docs/running-scenarios.md#where-runs-are-written).

## Inheritance

A top-level `extends` entry inherits one file or an ordered list of files:

```yaml
extends: [iot-telemetry.yaml, configs/iot-telemetry-high-mem.yaml]
workloads:
  iotTelemetry:
    rate: 1000
    behaviors:
      podRestarts:
        intervalSeconds: 30
        fraction: 0.1
profiling:
  broker:
    asyncProfilerOptions: event=cpu,interval=10ms,jfrsync=profile
    offCpuOptions:
      admission:
        policy: none
  gateways: ~
output:
  name: iot-restart-profile
```

- Each inherited path is resolved relative to the file that declares it; absolute paths also work. Keep inherited
  files together: the scenarios in the scenarios directory, and the configurations that they combine in its
  [`configs`](../configs) directory.
- Parents can inherit other files recursively. Parents are applied in list order, and the current file last.
- Mappings merge recursively, while scalar values and lists replace earlier values. An explicit YAML `null` or `~`
  removes an inherited entry.
- Cycles, missing files, non-mapping roots and invalid `extends` entries are rejected.

## Adding to a scenario on the command line

The launcher's `--extends <scenario>` option merges a file on top of the scenario, as if the scenario's `extends`
listed it last, without writing a new scenario. It is how profiling, or another memory configuration, is added to a
scenario: the `profile-*` files in the scenarios' `configs` directory each profile one component, and the
`iot-telemetry-*-mem` files there set the cluster and the memory, see [Configurations](../README.md#configurations).

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/profile-broker --extends configs/profile-gateways'
```

- A relative path is looked for first in the directory of the `--scenario` file, then in the working directory, and
  `.yaml` may be left out. An absolute path is used as given. The files can have their own `extends`, which resolve
  relative to them.
- The option is repeatable, and the files are merged in order, after the scenario and its parents.
- The launcher copies the files into the run directory beside the scenario, and `resolved-config.yaml` has the
  result.

## Settings on the command line

The launcher's `--set <path>=<value>` option sets one value of the resolved scenario for a run. The path is the
value's keys separated by dots, in any case:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --set workloads.iotTelemetry.rate=5000'
```

- Every section on the path has to exist, so that a misspelled section fails the run instead of adding
  configuration. The last key may be new, such as a broker setting added with
  `--set cluster.brokers.env.dispatcherMaxReadBatchSize=500`.
- A value that replaces a scalar keeps the scalar's YAML type. Any other value is parsed as YAML, so that
  `--set 'profiling.broker.offCpuOptions.reasons=[blocked, runnable]'` sets a list.
- The option is repeatable, and the settings apply in order, after inheritance, `--extends` and the environment
  overrides.

## Environment overrides

An environment variable also sets a value, when a command line is inconvenient to change, such as in a script that runs
several scenarios. Name the variable with a prefix followed by an existing scalar's path, with the path's elements
separated by underscores. The prefix is either `PULSAR_PERFORMANCE_` or `pulsar_performance_`; a variable with the
prefix in any other case, such as `Pulsar_Performance_`, overrides nothing. The path is in any case, so that these set
the same value. The loader keeps the scalar's YAML type:

```bash
PULSAR_PERFORMANCE_WORKLOADS_IOTTELEMETRY_RATE=2000 \
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'

PULSAR_PERFORMANCE_workloads_iotTelemetry_rate=2000 \
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'

pulsar_performance_workloads_iottelemetry_rate=2000 \
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'
```

Environment overrides are applied after inheritance. They only update paths present in the resolved tree, which
keeps misspelled or inapplicable settings from creating new configuration. Keep maintained workloads in scenario
files, and use environment overrides for temporary measurements rather than as the only record of a workload.

## Warmup and the measurement window

The `iotTelemetry` workload can run traffic before the measurement begins:

- `warmup.seconds`, which needs a positive `rate`, or `warmup.messages`, with or without a rate. The two are mutually
  exclusive.
- `warmup.rounds` repeats that traffic, and `warmup.roundDelaySeconds` adds an idle stabilization period after each
  fully drained round, including the final round. The default is one round with no delay.
- A round is fully drained only after every application has uniquely received its cumulative warmup message count;
  the gateways' send completions alone don't release the barrier.

Warmup traffic remains part of the delivery and ordering validation. The gateways' throughput and the
epoch-millisecond measurement boundaries in `gateways-summary.json` cover only the measured messages. Every
application's summary records its first and last measured-message receipt. The measurement window runs from the
gateways' measurement start through the latest last receipt across all applications; the launcher uses it to cut the
[measurement recording](../../docs/profiling.md#the-measurement-recording) of a profiled run.

The window assumes that the gateways', the applications' and the broker's clocks agree, as they do for containers on the
same Docker host. Multi-host experiments need synchronized clocks; the launcher doesn't estimate clock skew or correct
the window.
