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

This directory contains standalone performance scenarios, workload applications, their launcher, and guidance
for repeatable profiling and analysis. Keep scenario files, commands, results and interpretation together so a
later run can reproduce the same workload.

For micro-level questions about one class or method, use the JMH benchmarks in
[`microbench`](../../microbench). JMH is the benchmark harness; this directory is for documenting the
end-to-end profiling scenario, profile collection, analysis and conclusions. A useful experiment keeps
the workload definition, the revision under test, the profiler options, the raw recording and the
resulting analysis together.

Performance scenarios belong in this directory. Build reusable, mountable workload applications in
[`tools`](tools) with `./gradlew :tests:performance:tools:installDist`, describe workloads in
[`scenarios`](scenarios), and run them through the standalone [`launcher`](launcher). The launcher owns the
Testcontainers cluster and workload lifecycle directly, consumes recursively merged YAML, persists the resolved
configuration and run artifacts, and does not use a unit-test framework as a process runner. Shared scenario
loading is implemented in [`common`](common).

The original profiling harness under `tests/integration` uses TestNG classes as wrappers around a manually run
performance workload. That runner is deprecated: TestNG discovery and test lifecycle add no useful test semantics
to these long-running profiling scenarios and make them harder to invoke and automate as standalone jobs. It is
retained temporarily for its existing v4 and v5 `pulsar-perf` scenarios while they are migrated. Add new scenarios,
workload applications and profiling support to `tests/performance` and the standalone launcher instead.

## Running standalone scenarios

The IoT scenarios exercise keyed telemetry fanout, ordering, client restart and saturation behavior. Run the
host-sized scenario with:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml'
```

Use the `profile` task when the selected scenario contains profiler options:

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--config tests/performance/scenarios/iot-telemetry-high-rate-profile.yaml'
```

The Gradle tasks build the Pulsar test image and the workload distribution before launching the scenario. See
[the IoT scenario reference](iot-telemetry.md) for topology, correctness checks and output details.

## Scenario configuration format

Scenario YAML is a reusable configuration tree rather than a format tied to a test class. The shared loader in
[`common`](common) resolves the tree; launchers and workload applications select the subtree they own. The
standalone launcher uses these top-level sections:

- `cluster`: the Pulsar topology and broker or BookKeeper environment settings;
- `workloads`: named workload configurations, currently including `iotTelemetry`;
- `profiling`: optional async-profiler options for the broker, producer and consumer processes, recorded through
  the [jonoffcpu](https://github.com/lhotari/jonoffcpu) agent, plus the shared `offCpu` sampling policy; and
- `output`: the run-artifact directory.

Workload-specific fields live below their workload name so another launcher or application can reuse the same
file without interpreting unrelated sections. The launcher writes the fully resolved tree to
`resolved-config.yaml` in the run directory and mounts that file into workload containers. A workload command can
select its subtree with `--config-path`.

The `iotTelemetry` workload can run traffic before measurements begin. Use `warmupSeconds` with a positive `rate`,
or use `warmupMessages` when `rate: 0`; the two settings are mutually exclusive. `warmupRounds` repeats that
traffic, and `warmupRoundDelaySeconds` adds an idle stabilization period after each fully drained round, including
the final round. A round is fully drained only after every backend application has uniquely received its cumulative
warmup message count; producer send completions alone do not release the barrier. The default is one round with no
delay. Warmup traffic remains part of delivery and ordering validation. Producer throughput and the
epoch-millisecond measurement boundaries in
`producer-summary.json` cover only the configured measurement messages. Every consumer summary records its first
and last measured-message receipt as metadata. The launcher cuts from the producer measurement start through the
latest last receipt across all backend applications.

Use a top-level `extends` entry to inherit one file or an ordered list of files:

```yaml
extends: [cluster.yaml, workloads/iot-base.yaml]
workloads:
  iotTelemetry:
    rate: 1000
    clientRestartFraction: 0.1
profiling:
  brokerOptions: event=cpu,interval=10ms,jfrsync=profile
  producerOptions: ~
  offCpu:
    reasons: [blocked]
    minOffCpuMicros: 100
    admission:
      policy: proportional
      recordAllAboveMicros: 10000
  retainOriginalRecording: true
  createMeasurementRecording: true
output:
  directory: build/performance/iot-restart-profile
```

Each inherited path is resolved relative to the file that declares it; absolute paths also work. Parents can
inherit other files recursively. Parents are applied in list order and the current file is applied last. Mappings
merge recursively, while scalar values and lists replace earlier values. An explicit YAML `null` or `~` removes
an inherited entry. Cycles, missing files, non-mapping roots and invalid `extends` entries are rejected.

Profiled standalone runs attach the [jonoffcpu](https://github.com/lhotari/jonoffcpu) agent, which embeds
async-profiler and adds kernel-measured off-CPU samples. Every profiled JVM writes a `.jfr` recording, a
`.jonoffcpu-capture.pb` stream with its `.manifest.json`, and the `.jonoffcpu.yaml` the agent was started with.
After the run, the launcher correlates each pair over the measurement window into a sibling
`<recording>-offcpu/` directory holding `jonoffcpu-offcpu-stacks.collapsed` (Java stacks weighted in
microseconds of off-CPU time), `jonoffcpu-offcpu-synthetic.jfr` for JFR viewers, `jonoffcpu-report.json` with
loss, delivery-delay and switch-out-reason accounting, `jonoffcpu-offcpu-profile.pb`, `jonoffcpu-complete.json`
written last once everything validates. From the stack profile the launcher then renders two slices with the
correlator's `stacks` subcommand and `--package-names abbreviate`, which shortens
`io.netty.channel.epoll.Native.epollWait0` to `i.n.c.e.Native.epollWait0`: `offcpu.collapsed` with every
interval, and `offcpu-no-idle.collapsed` without intervals in which a thread was waiting for work, such as
Netty's `epollWait`, `ThreadPoolExecutor.getTask` or HotSpot's idle GC workers
(`OffCpuFlamegraphs.IDLE_WAIT_FRAMES`). Each comes with a `.json` summary, which for the second accounts for the
time it removed, and an `.html` flame graph. The profile renders any other slice, such as kernel stacks, in
under a second without correlating again. The correlator runs with `--audit none`: its row-level audit files are about 2 KB per row,
so a broker capture would add hundreds of megabytes of them beside a few megabytes of stacks, and every aggregate
is already in `jonoffcpu-report.json`. Running the correlator again over the retained capture and recording with
`--audit full` reproduces them. The flame graphs are rendered in-process by the converter from async-profiler's
jonoffcpu fork, which comes as a dependency and labels the widths in microseconds, so nothing needs an
async-profiler installation. Profiled runs use the glibc-based Wolfi test image, on which native frames are
symbolized; `-Pinttest.testImageVariant=alpine` selects the Alpine image. `profiling.offCpu` is the agent's
[`sampling` block](https://github.com/lhotari/jonoffcpu#choosing-what-to-sample) shared by every profiled JVM:
the switch-out `reasons` to record (`[blocked]` by default), `minOffCpuMicros` and an `admission` policy, which
is required. The policy `none` records plain async-profiler
through the same agent and skips the correlation step.

Profiled standalone runs retain the complete JFR and also create a sibling whose name ends in
`.measurement.jfr`. The measurement recording contains events from the producer's recorded measurement start through
the latest measured-message receipt across all backend applications. This excludes startup, warmup, and shutdown
while retaining the broker and consumer work needed to deliver every measured message. One-time JVM, host, recording
setting and runtime configuration events are copied from the beginning of the complete recording so JDK Mission
Control can describe the source JVM. Set
`profiling.retainOriginalRecording: false` to remove the complete
recording after a successful cut, or `profiling.createMeasurementRecording: false` to keep only the complete
recording. Both options default to `true` and apply to broker, producer and consumer recordings.
Setting both to `false` intentionally discards all recordings produced by the current run. Retention options do
not remove recordings from earlier runs. Use a fresh output directory for each experiment to keep profiles,
summaries, and histograms together without mixing artifacts from different runs.

These timestamps assume that producer, consumer, and broker clocks agree, as they do for containers on the same
Docker host. Multi-host experiments need synchronized clocks; the launcher does not estimate clock skew or
correct the cut window. The broker-publish-to-listener latency uses the same clock assumption.

Every IoT run writes `producer/produce-latency.hdr` with successful measured-message send-completion latency and
one `consumer-*/consume-latency.hdr` per backend application with measured-message broker-publish-to-listener
latency. Both use microseconds internally and three significant digits. Warmup messages are tagged in the payload
and excluded. Consumer latency uses a timestamp captured on listener entry; the sample is recorded after payload
decoding and key validation, before sequence validation and acknowledgment. Decoding and validation time are
excluded from the latency value.

Render the producer distribution together with the count-weighted merge of all backend-application consumer
histograms as PNG and SVG:

```bash
./gradlew :tests:performance:launcher:renderHdrHistograms \
  --args='--run-directory tests/performance/build/iot-telemetry-high-rate-profile'
```

The default outputs are `latency-histograms.png` and `latency-histograms.svg` in the run directory. Pass
`--output-prefix /path/to/name` or `--title 'Comparison label'` to change them.

Use the same cutter independently to select a different interval from an existing recording. `--from` and `--to`
accept ISO-8601 instants, epoch milliseconds, or offsets from the recording start such as `500ms`, `5s`, `2m`, `1h`,
or `PT5S`. Omit `--from` to select from the beginning, or omit `--to` to select through the end. Use `--info`
without either boundary to display the actual recording start, end and total duration from the JFR chunk headers;
it can also accompany a cut. JFR cutting preserves those source chunk timestamps, so the original recording period
remains available in the cut file and in JDK Mission Control. The
task requires JDK 19 or newer because it uses the public JFR recording writer added in that release:

```bash
./gradlew :tests:performance:launcher:runJfrCut \
  --args='--input /tmp/full.jfr --from 5s --to 2m --output /tmp/measurement.jfr --info'
```

Java code can call `JfrCut.cut(Path input, Instant from, Instant to, Path output)` or
`JfrCut.cutFrom(Path input, Instant from, Path output)` directly without invoking the command-line entry point.
`JfrCut.cutUsingTimeExpressions(...)` provides the relative and omitted-boundary syntax,
and `JfrCut.recordingInfo(...)` returns the event range. Events overlapping the half-open interval `[from, to)`
are retained: duration events ending exactly at `from` are excluded, instantaneous events at `from` are included,
and events starting exactly at `to` are excluded.

For one-off standalone overrides, prefix an existing scalar path with `PULSAR_PERFORMANCE_`, uppercase it and
separate path elements with underscores. The loader preserves the scalar's YAML type. For example:

```bash
PULSAR_PERFORMANCE_WORKLOADS_IOTTELEMETRY_RATE=2000 \
./gradlew :tests:performance:launcher:run \
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml'
```

Environment overrides are applied after inheritance. They only update paths present in the resolved tree, which
keeps misspelled or workload-inapplicable settings from creating new configuration. Store maintained scenarios in
[`scenarios`](scenarios); use environment overrides for temporary measurements rather than as the only record of
a workload.

## Legacy TestNG profiling runner

The deprecated TestNG runner remains available for the existing scenarios:

```bash
./gradlew :tests:integration:profilingIntegrationTest
./gradlew :tests:integration:profilingIntegrationTest --tests "*PulsarProfilingV4Test"
```

The first command profiles the v5 scalable-topic scenario. The second uses the v4 client against a
classic `persistent://` topic. Both variants profile a single broker and write recordings and command
output under `tests/integration/build/pulsar-profiling`. Do not use this runner as the basis for new scenarios.

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
`--isolated-clients`, a v4-client option; the v5 scenario ignores these fields. The option is mutually exclusive with the regular
producer test-thread option and with consumer listener-thread expansion. Set `load.producerCount`
and `load.consumerCount` separately: creating clients does not create producers or consumers.
`load.subscriptionType` selects the subscription type; `producerIoThreads` and `consumerIoThreads`
size the shared client IO pools. `maxOutstanding` is per producer, not a global limit.
Set `load.batchingEnabled: true` to use pulsar-perf's default producer batching; the default is
`false`, preserving unbatched entry-by-entry measurements.

For a single Key_Shared subscription with 500 producers and 20 consumers, use
[`key-shared-500x20.yaml`](scenarios/key-shared-500x20.yaml). The `load.messageKeyGenerationMode`
option maps to pulsar-perf's `--message-key-generation-mode`: `random` uses random integer keys,
`autoIncrement` uses the sender's message counter, and an empty or null value omits keys.
Override it with `PULSAR_PROFILING_LOAD_MESSAGE_KEY_GENERATION_MODE`. This scenario disables batching
so every entry has one key, and uses isolated clients with shared resources on both sides.
All consumers use the same subscription; `receiverQueueSize` is per consumer.

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

## Reproducible scenarios

- [IoT telemetry fanout and ordering](iot-telemetry.md): keyed telemetry through interchangeable gateways
  to Key_Shared applications, including isolated shared-resource clients, restart validation, a 500-connection
  saturation workload, and standalone jonoffcpu profiler integration.
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

On macOS, add the JDK Mission Control application launcher to a directory on `PATH`:

```bash
mkdir -p ~/.local/bin
ln -s /Applications/JDK\ Mission\ Control.app/Contents/MacOS/jmc ~/.local/bin/jmc
```

JDK Mission Control requires an absolute recording path. From the directory containing a recording, open it with:

```bash
jmc -open "$PWD/<recording.jfr>"
```

The following shell function accepts a relative or absolute path and resolves it before launching JMC. Add it to
`~/.zshrc` or the corresponding shell startup file:

```bash
jmc-open() {
  if [ "$#" -ne 1 ]; then
    echo "usage: jmc-open <recording.jfr>" >&2
    return 2
  fi
  local recording directory
  recording=$1
  directory=$(cd "$(dirname "$recording")" && pwd -P) || return
  jmc -open "$directory/$(basename "$recording")"
}
```

With IntelliJ IDEA's command-line launcher installed, open a recording directly with:

```bash
idea <recording.jfr>
```

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
