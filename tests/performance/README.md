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

### Where runs are written

Every run gets a directory of its own, in a hierarchy by day, git branch and name:

```
<reports root>/<yyyy-MM-dd>/<branch>/<name>/<MM-dd-HH-mm-ss>/
```

- The reports root is `build/performance` in the repository; `-Pperformance.reportsDir=<dir>` (relative to the
  repository root, or absolute) puts the reports elsewhere, for example in a directory shared by several worktrees
  or a git repository of results. To make that permanent for every checkout and worktree on a machine, set it in
  `~/.gradle/gradle.properties` (use an absolute path there, since a relative one would resolve in each checkout):

  ```properties
  performance.reportsDir=/data/pulsar-performance-reports
  ```
- The branch is the checked-out branch, with `/` and other characters that do not belong in a directory name
  replaced by `-`; a detached HEAD is `detached-<commit>`.
- The name is the scenario file name without `.yaml`, or the scenario's `output.name` when it sets one. `--name`
  names an experiment instead, so that its runs stay together:

  ```bash
  ./gradlew :tests:performance:launcher:profile -Pperformance.reportsDir=/data/pulsar-reports \
    --args='--config tests/performance/scenarios/iot-key-shared-500x20-profile.yaml --name e232-ab'
  ```

- The run directory is named by the run's start in local time. Two runs of the same name started within the same
  second would share it.
- `--output <dir>` writes the run to exactly that directory instead, outside the hierarchy.

The launcher prints the run directory when it starts. In it, `index.html` and `README.md` are symbolic links to the
run report, so that a directory of runs served by an HTTP server, or pushed to a GitHub repository, opens each run
on its report; where the file system has no symbolic links, they are left out.

#### Browsing the reports over HTTP

The reports are static files, so any HTTP server can serve the reports root, and its directory listings lead
through days, branches and names to the runs. When the performance tests run on a separate machine, serve the
root there with Python's built-in server, bound to the loopback interface so that it is not reachable from the
network:

```bash
# On the performance testing machine
python3 -m http.server 8000 --bind 127.0.0.1 --directory build/performance
```

and reach it through an SSH tunnel, which forwards a local port to that loopback address over the encrypted SSH
connection:

```bash
# On your own machine
ssh -N -L 8000:127.0.0.1:8000 perf-host
```

Then open <http://localhost:8000/> and follow the listings to a run; its `index.html` opens the run report, from
which the profile reports, digests and flame graphs are linked. The server reads the files as they are requested,
so new runs appear without restarting it. With `-Pperformance.reportsDir=<dir>`, serve that directory instead.

Every run writes a report into its run directory; open `run-report.html` in a browser, where its links work:

| File | Contents |
|---|---|
| `run-report.md`, `run-report.html` | The scenario settings, where, by whom and from which commit the run was made, correctness per application, producer and delivered throughput, publish and end-to-end latency percentiles, the sampled backlog and per-second rates, and links to the profile reports of a profiled run and to the run's other files: the scenario as written and resolved, the summaries, container logs (`container.log.txt`, so that HTTP servers show them as text), HDR latency logs and topic stats |
| `<scenario>.yaml`, `resolved-config.yaml` | The scenario file as written, and the scenario with its inheritance and environment overrides applied, which the workloads read |
| `index.html`, `README.md` | Symbolic links to `run-report.html` and `run-report.md` |
| `run-info.json` | The run's start, host, user, project directory, git branch, commit and uncommitted changes, and Pulsar version, with the keys of `pulsar-version.properties` where they match; the launcher collects them itself, from git and `gradle.properties` in the checkout it runs from |
| `latency-histograms.svg`, `.png` | Publish and end-to-end latency distributions |
| `throughput.svg`, `.png` | Messages published and dispatched per second over the run, warmup included and the producers' finish marked |
| `backlog.svg`, `.png` | Each subscription's backlog over the run |
| `topic-stats.csv` | The broker's topic stats sampled once per second: backlog and message counters per subscription |

The backlog and the per-second rates come from the topic stats endpoint, polled once per second while the
producers run and the consumers drain; a sampled maximum is not the exact peak between samples. Every Markdown
report the launcher writes, including the profile reports and the off-CPU digests, has an HTML page beside it,
rendered with [commonmark-java](https://github.com/commonmark/commonmark-java), whose links lead to the other pages
and the flame graphs.

## Scenario configuration format

Scenario YAML is a reusable configuration tree rather than a format tied to a test class. The shared loader in
[`common`](common) resolves the tree; launchers and workload applications select the subtree they own. The
standalone launcher uses these top-level sections:

- `cluster`: the Pulsar topology and broker or BookKeeper environment settings;
- `workloads`: named workload configurations, currently including `iotTelemetry`;
- `profiling`: optional async-profiler options for the broker, producer and consumer processes, recorded through
  the [jonoffcpu](https://github.com/jonoffcpu/jonoffcpu) agent, plus the shared `offCpu` sampling policy; and
- `output`: optional; `output.name` names the scenario's runs in the reports hierarchy instead of the file name
  (see [Where runs are written](#where-runs-are-written)).

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
  name: iot-restart-profile
```

Each inherited path is resolved relative to the file that declares it; absolute paths also work. Parents can
inherit other files recursively. Parents are applied in list order and the current file is applied last. Mappings
merge recursively, while scalar values and lists replace earlier values. An explicit YAML `null` or `~` removes
an inherited entry. Cycles, missing files, non-mapping roots and invalid `extends` entries are rejected.

The `profiling` section is described in [Profiling with jonoffcpu](#profiling-with-jonoffcpu).

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
not remove recordings from earlier runs, which matters only when `--output` reuses a directory: each run in the
reports hierarchy has a directory of its own.

These timestamps assume that producer, consumer, and broker clocks agree, as they do for containers on the same
Docker host. Multi-host experiments need synchronized clocks; the launcher does not estimate clock skew or
correct the cut window. The broker-publish-to-listener latency uses the same clock assumption.

Every IoT run writes `producer/produce-latency.hdr` with successful measured-message send-completion latency and
one `consumer-*/consume-latency.hdr` per backend application with measured-message broker-publish-to-listener
latency. Both use microseconds internally and three significant digits. Warmup messages are tagged in the payload
and excluded. Consumer latency uses a timestamp captured on listener entry; the sample is recorded after payload
decoding and key validation, before sequence validation and acknowledgment. Decoding and validation time are
excluded from the latency value.

The run report includes these distributions. To render them again for any run directory, for example with a
different title, render the producer distribution together with the count-weighted merge of all
backend-application consumer histograms as PNG and SVG:

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

## Profiling with jonoffcpu

The `profile` task attaches the [jonoffcpu](https://github.com/jonoffcpu/jonoffcpu) agent to every JVM that has
profiler options. jonoffcpu bundles [async-profiler](https://github.com/async-profiler/async-profiler), so the
recording holds the usual CPU and allocation samples, and adds **off-CPU** samples: each interval in which a thread
blocked is measured by the kernel scheduler through eBPF and joined to the Java stack of the thread that waited. A
CPU profile shows where threads burn CPU; the off-CPU profile shows where they wait — on locks, monitors, queues,
I/O, safepoints or GC. The agent, the correlator that joins the two, and the flame graph converter are resolved by
Gradle (see `jonoffcpu` in `gradle/libs.versions.toml`); nothing needs installing on the host or in the image.

```yaml
profiling:
  brokerOptions: event=cpu,interval=10ms,alloc=2m,jfrsync=profile
  producerOptions: event=cpu,interval=10ms,alloc=2m,jfrsync=profile
  consumerOptions: ""
  offCpu:
    reasons: [blocked]
    minOffCpuMicros: 100
    admission:
      policy: proportional
      recordAllAboveMicros: 10000
```

The options are async-profiler options; an empty value leaves that component unprofiled. `profiling.offCpu` is
the agent's [`sampling` block](https://github.com/jonoffcpu/jonoffcpu#choosing-what-to-sample): which switch-out
reasons to record (`blocked` — the thread could not run — rather than `runnable` preemption), a minimum duration,
and an admission policy that records every long wait and samples short ones in proportion to their length. The
policy `none` records plain async-profiler through the same agent and skips the off-CPU steps.

Requirements:

- A Linux Docker engine whose kernel has BTF (`/sys/kernel/btf/vmlinux`), which recent distribution kernels have.
- The relaxed perf-event and BPF sysctls, which the `:tests:integration:tuneKernelPerfEvents` task that `profile`
  depends on writes from a throwaway privileged container; `-Pinttest.asyncprofiler.skipPerfEventTuning` skips it
  where they are already set.
- Profiled containers run privileged with the JVM as root: loading the eBPF programs needs `CAP_BPF` and
  `CAP_PERFMON`, which Docker grants to root in the container only. A tracefs is mounted read-only at
  `/sys/kernel/tracing` as a Docker volume.
- Profiled runs use the glibc-based `java-test-image:<tag>-wolfi` image, on which native frames (HotSpot,
  libc, JNI libraries) are symbolized; on the Alpine image every native frame reads as
  `/lib/ld-musl-x86_64.so.1`. `-Pinttest.testImageVariant=alpine` profiles on Alpine anyway.

### What a profiled run writes

For every recording `<recording>.jfr` (the broker's under `broker-profile/`, the producer's and consumers' in their
output directories):

| File | Contents |
|---|---|
| `profile-report.md`, `.html` | **Start here**, from `run-report.html`. One per profiled directory (`broker-profile/`, `producer/`): the run, and for each recording links to the off-CPU digest, the flame graphs with their totals, and the heatmaps |
| `<recording>.jfr` | The complete recording, unless `retainOriginalRecording: false` |
| `<recording>.measurement.jfr` | The same cut to the measurement window (see above) |
| `<recording>-flamegraphs/` | `cpu`, `wall`, `alloc` and `lock` views of the measurement recording, each only when its event is in the profiler options: `<view>.html`, `<view>-threads.html` (split by thread), `<view>-heatmap.html` (samples over time, for bursts and pauses) and `<view>.collapsed`. Pulsar and BookKeeper frames are highlighted |
| `<recording>.jonoffcpu-capture.pb`, `.manifest.json`, `<recording>.jonoffcpu.yaml` | The off-CPU capture stream, its manifest, and the agent configuration the JVM was started with |
| `<recording>-offcpu/jonoffcpu-summary.md`, `.json` | The off-CPU digest: the blocked time ranked by the application method that waited, by application root and by application method, where the time went and the capture coverage, leaving out the idle waits of `offcpu-idle-waits.txt` |
| `<recording>-offcpu/offcpu-no-idle.html` | Off-CPU flame graph of the measurement window without threads that were only waiting for work |
| `<recording>-offcpu/offcpu-no-idle-app-root.html` | The same with each stack starting at its first Pulsar or BookKeeper frame once executor and Netty dispatch frames are hidden, so the same code reached from different thread pools or event loops is one tree |
| `<recording>-offcpu/offcpu.html`, `offcpu-app-root.html` | Every blocked interval, idle waiting included, as is and from the first application frame |
| `<recording>-offcpu/*.collapsed`, `*.json` | The same slices as collapsed stacks (full names, microseconds) and the summary of each, including the time the idle filter removed |
| `<recording>-offcpu/offcpu-idle-waits.txt`, `<recording>.offcpu-idle-waits.txt` | The idle-wait patterns the run used; the copy beside the recording is the one the digest's reproduce commands name |
| `<recording>-offcpu/offcpu-dispatch-hide.txt`, `<recording>.offcpu-dispatch-hide.txt` | The BookKeeper and Pulsar frames that only dispatch work (executors running a task, Pulsar's inbound Netty handlers), hidden with jonoffcpu's `jvm-dispatch` preset in the digest and the app-root flame graphs |
| `<recording>-offcpu/jonoffcpu-offcpu-profile.pb` | The stack profile: every distinct stack with its counters, from which other slices are rendered without correlating again |
| `<recording>-offcpu/jonoffcpu-report.json` | Accounting: intervals recorded and matched, loss, switch-out reasons, sleeping versus run-queue time |

In a broker, over 99% of off-CPU time is threads waiting for work: Netty event loops in `epollWait`, executor
workers waiting for a task, JDK and HotSpot service threads. `offcpu-no-idle` leaves those out with the patterns in
the launcher resource `offcpu-idle-waits.txt`; each pattern names the wait itself rather than the thread's run loop,
so a lock taken while running a task stays in. What remains is lock and monitor contention, safepoints, GC phases
and I/O. The digest leaves out the same idle waits. The `-app-root` slices start each stack at its root-most frame
matching `^org\.apache\.`, once the frames that only dispatch work are hidden. Stacks without such a frame, such as
the JVM's own threads, are left out of them (`--root-at-unmatched hide`); the profile report shows how much time that
was, and the digest ranks it by thread pool.
The off-CPU flame graphs abbreviate package names (`o.a.p.b.s.p.PersistentDispatcherMultipleConsumers…`) and
highlight the `o.a.` frames. The correlator runs with `--audit none`, which skips its row-level audit files (about
2 KB per interval); run it again over the retained capture and recording with `--audit full` to reproduce them.

### Finding what to optimize

1. Open `run-report.html`, then the broker's profile report and the digest it links to, `offcpu-no-idle-app-root.html` and
   `cpu.html`. A single thread that is busy all the time — the `-threads` views show it — is a serial bottleneck that
   no amount of other headroom helps. The heatmaps show whether CPU or allocation comes in bursts or stalls.
2. Rank the blocked time by the deepest Pulsar or BookKeeper frame of each stack and the lock or wait below it. This
   needs no flame graph: stacks without an application frame collect by thread pool, and idle waits are listed
   separately. With the correlator JAR from the
   [jonoffcpu releases](https://github.com/jonoffcpu/jonoffcpu/releases):

   ```bash
   OFFCPU=<run directory>/broker-profile/<recording>-offcpu
   java -jar jonoffcpu-correlator.jar top --profile $OFFCPU/jonoffcpu-offcpu-profile.pb \
     --app '^org\.apache\.' --waiting-from $OFFCPU/offcpu-idle-waits.txt --package-names abbreviate
   ```

   `export --format jsonl` writes the profile one stack per row for SQL tools such as [DuckDB](https://duckdb.org/).

3. Render other slices from the stack profile in under a second. `--stack java+kernel` continues each stack into
   the kernel so the wait mechanism is visible; `--time split` ends each stack in `[sleeping]` or `[runqueue]`, which
   separates waiting for an event from waiting for a CPU after it arrived; `--include`/`--exclude` and their
   `-from FILE` forms select intervals by frame. Render the result with the converter JAR from the same release:

   ```bash
   java -jar jonoffcpu-correlator.jar stacks --profile $OFFCPU/jonoffcpu-offcpu-profile.pb \
     --exclude-from $OFFCPU/offcpu-idle-waits.txt --time split --package-names abbreviate \
     --output /tmp/blocked-split.collapsed --summary /tmp/blocked-split.json
   java -jar jfr-converter.jar --title "Blocked off-CPU time" --units µs --highlight '^o\.a\.' \
     /tmp/blocked-split.collapsed /tmp/blocked-split.html
   ```

   The transforms `--root-at`, `--trim-root`, `--hide` and `--collapse-leaf` change what each kept stack looks
   like without changing which intervals are kept or their totals.

4. Compare two runs per unit of work with `top --baseline` (baseline second), for example per million measured
   messages. Compare runs recorded with the same sampling policy; proportional admission under-represents short
   waits in the observed weights, so the comparison uses the estimated weights:

   ```bash
   java -jar jonoffcpu-correlator.jar top --profile candidate-offcpu/jonoffcpu-offcpu-profile.pb \
     --baseline baseline-offcpu/jonoffcpu-offcpu-profile.pb --units 4 --baseline-units 4 --weights estimated \
     --app '^org\.apache\.' --waiting-from candidate-offcpu/offcpu-idle-waits.txt --package-names abbreviate
   ```

Correlation holds each capture's distinct stacks in memory; the `profile` task runs with a 4 GB heap
(`-Pperformance.profile.maxHeapSize=...` changes it), several times what a few minutes of broker capture needs.

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

The standalone `profile` task already renders the configured views of every recording into
`<recording>-flamegraphs/` (see [What a profiled run writes](#what-a-profiled-run-writes)). For recordings from the
legacy runner, or to render all four views of any recording, use:

```bash
./gradlew jfrFlamegraphs -Pjfr=tests/integration/build/pulsar-profiling
```

The `.jfr` files can also be opened in [Eclipse Mission Control](https://adoptium.net/jmc) or IntelliJ
IDEA. Do not use `jfr summary` as a measure of profile completeness: async-profiler writes its CPU samples as
`jdk.ExecutionSample` and its allocation samples as `jdk.ObjectAllocationInNewTLAB` and
`jdk.ObjectAllocationOutsideTLAB` in its own chunks, which the JDK summary counts as zero even when the flame graphs
are full.

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
report in the console. For a standalone profiled run, analyze `<recording>.measurement.jfr`: CPU samples are
`jdk.ExecutionSample`, async-profiler's allocation samples are `jdk.ObjectAllocationInNewTLAB` (not
`jdk.ObjectAllocationSample`), and `jfrsync=profile` adds JDK events such as `jdk.JavaMonitorEnter` and
`jdk.ThreadPark`. Off-CPU time is not in a JFR file; use the digest `<recording>-offcpu/jonoffcpu-summary.md` and
the correlator's `top` and `stacks` subcommands (see [Finding what to optimize](#finding-what-to-optimize)). A
useful starting prompt is:

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
