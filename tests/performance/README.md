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

# Performance testing

This directory documents repeatable performance experiments and their analysis. The container based
profiling harness lives under [`tests/integration`](../integration); its recordings normally land in
`tests/integration/build/pulsar-profiling`. Keep scenario files, commands, results and interpretation
here so that a later run can reproduce the same workload.

For micro-level questions about one class or method, use the JMH benchmarks in
[`microbench`](../../microbench). JMH is the benchmark harness; this directory is for documenting the
end-to-end profiling scenario, profile collection, analysis and conclusions. A useful experiment keeps
the workload definition, the revision under test, the profiler options, the raw recording and the
resulting analysis together.

## Profiling an integration-test cluster

Run the built-in scenarios with:

```bash
./gradlew :tests:integration:profilingIntegrationTest
./gradlew :tests:integration:profilingIntegrationTest --tests "*PulsarProfilingV4Test"
```

The first command profiles the v5 scalable-topic scenario. The second uses the v4 client against a
classic `persistent://` topic. Both variants profile a single broker and write recordings and command
output under `tests/integration/build/pulsar-profiling`.

The harness accepts a YAML scenario file through `PULSAR_PROFILING_CONFIG`. Start with
[`pulsar-profiling.yaml`](scenarios/pulsar-profiling.yaml); omitted values retain the existing defaults. The
sections correspond to the main components of a run: `cluster`, `load`, `profiling` and `output`. Individual scalar
values can still be overridden for a one-off run with the `PULSAR_PROFILING_` prefix and an upper-case
path, for example:

```bash
PULSAR_PROFILING_CONFIG="$PWD/tests/performance/scenarios/pulsar-profiling.yaml" \
PULSAR_PROFILING_LOAD_NUMBER_OF_MESSAGES=1000000 \
./gradlew :tests:integration:profilingIntegrationTest --tests "*PulsarProfilingV4Test"
```

For the v4 scenario, set `load.isolatedProducers` or `load.isolatedConsumers` to create that many
independent v4 client instances. The corresponding `pulsar-perf` command receives
`--isolated-clients`; v5 ignores these fields. The option is mutually exclusive with the regular
producer test-thread option and with consumer listener-thread expansion. Set `load.producerCount`
and `load.consumerCount` separately: creating clients does not create producers or consumers.
`load.subscriptionType` selects the subscription type; `producerIoThreads` and `consumerIoThreads`
size the shared client IO pools. `maxOutstanding` is per producer, not a global limit.
Set `load.batchingEnabled: true` to use pulsar-perf's default producer batching; the default is
`false`, preserving unbatched entry-by-entry measurements.

Client profiling is optional and independent of broker profiling. Set `profiling.producerOptions`
and/or `profiling.consumerOptions` to async-profiler options, for example
`event=cpu,interval=10ms,lock=0,alloc=2m,jfrsync=profile`. An empty or null option disables that
client's profiler. Client recordings are named `client-producer-*.jfr` and `client-consumer-*.jfr`
in the same output directory. The harness grants native CPU profiling access only to enabled clients.
The broker continues to use `-Pinttest.asyncprofiler.opts`. To inspect lock contention without
wall-clock sampling overhead, omit `wall` and use `lock=0` to record all supported lock events.
Use the same options for baseline and candidate; extra profiling has a measurement cost.

Broker-side variations from the contention investigations need no dedicated environment switches:
use `cluster.brokerEnvs` for write-buffer watermarks, dispatcher batch size, broker/BookKeeper IO
thread counts and `PULSAR_GC`; use `cluster.brokerMemory` for heap/direct-memory limits and JVM
properties such as allocator or transport selection. These maps let YAML scenarios retain the
complete configuration instead of relying on shell history.

The harness saves `resolved-config.yaml` with inheritance and environment overrides applied in the output directory.
For v4 production, `--num-producers` remains the producer count per topic and is distributed across
the isolated clients; when the counts differ, producers are assigned round-robin as evenly as possible.

### Inheriting scenario configurations

Use a top-level `extends` to inherit one file or a list of files:

```yaml
extends: [cluster.yaml, workloads/many-producers.yaml]
load:
  subscriptionType: Shared
cluster:
  brokerEnvs:
    preciseDispatcherFlowControl: ~
output:
  directory: build/pulsar-profiling/shared
```

Each path is relative to the file declaring it; absolute paths also work. Parents can themselves
inherit other files. Starting with the harness defaults, the loader visits each parent recursively
in the listed order, then applies the current file. Later values win, mappings merge recursively,
and scalar values and lists replace earlier values. Shared ancestors are applied on each visit;
inheritance cycles, missing files and invalid `extends` entries are rejected. Environment overrides
are applied last, and `resolved-config.yaml` contains the resulting values without `extends`.

An explicit YAML `null` or `~` removes an entry, including a harness default. For example, the
`preciseDispatcherFlowControl` removal above leaves that broker setting to the broker's own default.
Deleting a mapping removes all its entries; a later mapping starts fresh. Required workload fields
must still have valid values in the final configuration.

## Reproducible scenarios

- [Read-completion queue isolation](read-completion-isolation.md): 500 producers on separate
  connections to one persistent topic, with one Exclusive consumer. Includes the
  [scenario YAML](scenarios/read-completion-isolation.yaml), an inherited
  [Shared-subscription variant](scenarios/read-completion-isolation-shared.yaml), a
  [Failover variant](scenarios/read-completion-isolation-failover.yaml), explicit
  [64/32 KiB](scenarios/read-completion-isolation-64k-32k.yaml) and
  [256/128 KiB](scenarios/read-completion-isolation-256k-128k.yaml) channel-watermark variations,
  and baseline/comparison instructions.

## Inspecting recordings

Render the CPU, wall-clock, allocation and lock views with:

```bash
./gradlew jfrFlamegraphs -Pjfr=tests/integration/build/pulsar-profiling
```

The `.jfr` files can also be opened in [Eclipse Mission Control](https://adoptium.net/jmc) or IntelliJ
IDEA. Do not use `jfr summary` as a measure of profile completeness: recordings made with
`jfrsync=profile` contain profiler samples that the JDK summary does not show.

### Jafar MCP analysis

The [Jafar MCP server](https://github.com/btraceio/jafar/blob/main/jfr-mcp/README.md) lets an AI coding
agent query a recording. Register it once with [JBang](https://www.jbang.dev/) and JDK 25+:

```bash
claude mcp add jafar -- jbang jfr-mcp@btraceio --stdio
```

Use `jfr_diagnose` and `jfr_stackprofile` first, then query further with the other Jafar tools when
needed. Save the result beside the recording as `<recording>.analysis.md`, in addition to showing the
report in the console. A useful starting prompt is:

> use Jafar MCP's jfr_diagnose and jfr_stackprofile to analyze @filename.jfr. Besides showing the
> report on the console, write the analysis in a markdown file with the jfr file as prefix and the
> suffix as ".analysis.md"

Treat automated analysis as a lead. Confirm a performance claim with a controlled comparison, a JMH
benchmark where appropriate, or a second profile.

### Heap dumps and memory leaks with MAT MCP

For an `OutOfMemoryError` or suspected retention problem, analyze the resulting `.hprof` with a
headless Eclipse Memory Analyzer (MAT) MCP server such as
[`mcp-mat`](https://github.com/codelipenghui/mcp-mat). Use the leak suspects report and dominator tree first,
then query paths to GC roots or OQL for the retained objects. Keep the heap dump and the MCP result
outside the source tree when they contain sensitive workload data; record the commands, heap limits and
the resulting conclusions in the experiment notes.
