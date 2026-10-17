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

# Profiling

The launcher's `profile` task attaches the [jonoffcpu](https://github.com/jonoffcpu/jonoffcpu) agent to every JVM
that has profiler options. In each of them, three recorders run at the same time:

- [async-profiler](https://github.com/async-profiler/async-profiler), which the agent bundles, samples CPU time and
  allocations into a JFR recording.
- JDK Flight Recorder (JFR), which async-profiler starts alongside with its `jfrsync` option when the component lists
  JFR configurations, as it does by default, or JFR events, records the JVM's own events into the same recording, such
  as monitor contention (`jdk.JavaMonitorEnter`), thread parking (`jdk.ThreadPark`) and garbage collection, with the
  JFR configurations that [The JFR configuration](#the-jfr-configuration) describes.
- jonoffcpu's eBPF collector records, from the kernel scheduler, every interval in which a thread blocked, into a
  capture stream beside the recording.

After the run, jonoffcpu's correlator joins each blocked interval to the Java stack of the thread that waited, which
gives the **off-CPU** profile. A CPU profile shows where threads burn CPU; the off-CPU profile shows where they wait —
on locks, monitors, queues, I/O, safepoints or GC. The agent, the correlator and the flame graph converter are
resolved by Gradle (see `jonoffcpu` in `gradle/libs.versions.toml`); nothing needs installing on the host or in the
image.

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/profile-broker --extends configs/profile-gateways'
```

The example profiles the broker and the gateways. To study the performance of Pulsar's Java client, profile the
gateways and the applications, which are its producers and consumers under the workload:

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --extends configs/profile-gateways --extends configs/profile-applications'
```

Every application runs in the applications' container, so `profile-applications` records one profile of all of them,
as `profile-gateways` records one of all the gateways.

Profile a scenario with the medium- or the high-memory configuration, not one with the low-memory configuration, such
as `iot-telemetry-small.yaml`, which isn't meant for profiling, see
[Memory configurations](../scenarios/README.md#memory-configurations). The `run` task rejects a scenario that has
profiler options, rather than silently running it without the agent.
[Analyzing profiles](analyzing-profiles.md) describes how to find what to optimize from the recordings.

## Configuring profiling

The scenario's `profiling` section configures it, with settings for each component: `broker`, `gateways`, the
producer, and `applications`, the consumers. The scenarios' `configs` directory has a file for each component, which
the launcher's `--extends` option adds to any scenario, as the example above does: `configs/profile-broker`,
`configs/profile-gateways` and `configs/profile-applications`.
[`profile-broker.yaml`](../scenarios/configs/profile-broker.yaml), an example of the settings, is:

```yaml
profiling:
  broker:
    asyncProfilerOptions: event=cpu,interval=10ms,alloc=2m
    offCpuOptions:
      reasons: [blocked]
      minOffCpuMicros: 100
      admission:
        policy: proportional
        recordAllAboveMicros: 10000
```

- `asyncProfilerOptions` is a comma-separated list of async-profiler's options, as the "Launch as agent" column of
  [its profiler options](https://github.com/async-profiler/async-profiler/blob/master/docs/ProfilerOptions.md) names
  them, such as `event=cpu,interval=10ms,alloc=2m`. A component without them isn't profiled. The launcher owns each
  recording's path, so that recordings stay inside the run directory, and rejects options that set `file=`.
- The launcher adds async-profiler's `jfrsync` option, which records JDK Flight Recorder's events alongside
  async-profiler's, when the component lists JFR configurations in `jfrConfigurations`, as it does by default, or JFR
  events in `jfrEventConfig`. The options don't set it; the launcher rejects options that do.
- `offCpuOptions` is the agent's [`sampling` block](https://github.com/jonoffcpu/jonoffcpu#choosing-what-to-sample):
  which switch-out reasons to record (`blocked` — the thread could not run — rather than `runnable` preemption), a
  minimum duration, and an admission policy that records every long wait and samples short ones in proportion to their
  length. A profiled component needs it; the policy `none` records plain async-profiler through the same agent and skips
  the off-CPU steps, for a Docker engine whose kernel can't run jonoffcpu's collector.
- `jfrConfigurations` lists the JFR configurations that the component records with, `[profile]` by default, and
  `[]` or `[none]` without `jfrEventConfig` turns JFR off, see [The JFR configuration](#the-jfr-configuration).
- `jfrEventConfig` lists JFR events, or settings of events, to add to the configurations, see
  [The JFR configuration](#the-jfr-configuration).
- `nettyAllocationsReport`, `false` by default, summarizes the measurement recording's Netty allocator events after
  the run, which the component records only when its `jfrConfigurations` list `netty-allocations.jfc`, see
  [Netty allocator events](#netty-allocator-events).
- The launcher keeps each complete recording and writes its measurement recording beside it, see
  [The measurement recording](#the-measurement-recording).

## The JFR configuration

A profiled JVM records JDK Flight Recorder's events with the JFR configurations that its component's
`jfrConfigurations` list, merged in order with the JDK's `jfr configure`. The default is `[profile]`:

- A name, such as `profile` or `default`, is one of the JDK's configurations, from `$JAVA_HOME/lib/jfr`. `profile` is
  the one that the JDK describes as a profiling configuration with about 2 % overhead.
- A name ending with `.jfc` is a file of [`tests/performance/jfr`](../jfr), such as
  [`netty-allocations.jfc`](../jfr/netty-allocations.jfc), see [Netty allocator events](#netty-allocator-events). For a
  purpose that needs other events or settings, add a `.jfc` file there and list it.
- `none` is an empty configuration, for a component that records only the events of its `jfrEventConfig`.
- An empty list, `jfrConfigurations: []`, lists no configurations.

Without `jfrEventConfig`, `jfrConfigurations: []` and `jfrConfigurations: [none]` turn JFR off: the launcher leaves
`jfrsync` out, and the component records only async-profiler's samples.

A component's `jfrEventConfig` lists events to add to its configurations, or settings of events that override
theirs. Each entry has an `event`, and optionally a `setting` and its `value`; an entry without a setting enables the
event. The launcher applies them after the configurations, `[profile]` unless the component lists others, or, with
`jfrConfigurations: []`, to the JDK's `default` configuration, as `jfr configure` does without `--input`;
`jfrConfigurations: [none]` starts from an empty one:

```yaml
profiling:
  broker:
    jfrEventConfig:
      - event: jdk.CPULoad
        setting: period
        value: 100 ms
      - event: io.netty.AllocateChunk
```

A single configuration of the JDK without `jfrEventConfig`, such as the default `profile`, goes to async-profiler's
`jfrsync` option as it is. Otherwise, before the cluster starts, the launcher merges the component's configurations,
and applies its `jfrEventConfig` after them, in a one-off container of the component's image, so that a configuration
of the JDK is the one of the JVM that records with it, into `jfr-configuration.jfc` beside the component's recordings,
and passes that file to `jfrsync`. The options don't set `jfrsync`; the launcher rejects options that do. When the
image's JDK can't merge them, such as a released Pulsar's image whose JDK has no `jfr` tool
(`-Pperformance.clusterPulsarImage`), the launcher says so and records that component with the JDK's `profile`
configuration.

### Netty allocator events

Netty's buffer allocators, `AdaptivePoolingAllocator` and `PooledByteBufAllocator`, emit JFR events for every buffer
that they allocate, grow and free, and for every chunk of memory that they allocate and free to hand out buffers from:
`io.netty.AllocateBuffer`, `ReallocateBuffer`, `FreeBuffer`, `AllocateChunk`, `FreeChunk` and `ReturnChunk`. Netty
4.2.4 and later have them, except `ReturnChunk`, which no Netty release has yet and which is ignored until one adds
it; Netty 4.1, which Pulsar 4.x uses, has none, so profiling a Pulsar 4.x image records none of them. Recording and
reporting them are two separate settings of a component:

- `jfrConfigurations: [profile, netty-allocations.jfc]` records them. The buffer events are recorded for every buffer,
  without stack traces, and JFR can't sample them, so they add a heavy overhead to the profiled JVM and make its
  recording much larger: in the IoT telemetry max-rate scenario, about 2.5 events of each kind per message in each
  profiled component, and a broker recording of 1 GB instead of 10 MB. Prefer a short measurement, and compare
  throughput and latency only between runs that record the same events.
- `nettyAllocationsReport: true` summarizes them after the run into `<recording>.measurement.netty-allocator.json`,
  which the profile report shows, see
  [Netty allocator events](analyzing-profiles.md#netty-allocator-events). A recording without them gives an empty
  summary.

`configs/profile-broker-netty-allocations`, `configs/profile-gateways-netty-allocations` and
`configs/profile-applications-netty-allocations` set both for a component, in place of its
`configs/profile-<component>`:

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--scenario tests/performance/scenarios/iot-telemetry-max-rate.yaml --extends configs/profile-broker-netty-allocations'
```

## Requirements

- A Docker engine whose kernel has BTF (`/sys/kernel/btf/vmlinux`), which recent Linux distribution kernels have.
  Profiling with async-profiler, jonoffcpu and JDK Flight Recorder works on Linux and on macOS, and was also tested on
  macOS arm64 with the [OrbStack](https://orbstack.dev/) Docker engine, whose Linux virtual machine has BTF. For
  measurements, Linux x86_64 is recommended: it is Pulsar's main target platform, and dedicated hardware configured
  with [the performance testing environment setup](../environment/README.md) has no noisy neighbours and less thermal
  and power throttling and CPU frequency variance than a virtual machine or a laptop's default power management.
- The relaxed perf event and BPF sysctls. The `profile` task depends on `:tests:integration:tuneKernelPerfEvents`,
  which writes them from a throwaway privileged container; `-Pinttest.asyncprofiler.skipPerfEventTuning` skips it
  where they are already set. `configure-perf-test-environment.sh start` from
  [the performance testing environment setup](../environment/README.md) sets them, and skips the task until `stop`.
- Profiled containers run privileged with the JVM as root: loading the eBPF programs needs `CAP_BPF` and
  `CAP_PERFMON`, which Docker grants to root in the container only. A tracefs is mounted read-only at
  `/sys/kernel/tracing` as a Docker volume.
- Profiled runs use the same Alpine image as the unprofiled ones, which is also Pulsar's default Docker image, since a
  profile of another image doesn't carry over to it: the libc's memory allocator, for one, behaves differently. Alpine
  strips its libc, musl, and the test image installs musl's debug symbols (`musl-dbg`), so that the profiles name
  musl's functions, such as the system call wrappers under the JNI methods, and walk native stacks through them;
  without them, every frame in musl reads as `/lib/ld-musl-x86_64.so.1`. `-Pinttest.testImageVariant=wolfi` profiles on
  the glibc-based `java-test-image:<tag>-wolfi` image instead.

Correlation holds each capture's distinct stacks in memory. The `profile` task runs with a 4 GB heap, several times
what a few minutes of broker capture needs; `-Pperformance.profile.maxHeapSize=...` changes it.

## What a profiled run writes

When the workloads have finished, the launcher processes every recording and writes the results into the run directory,
next to the recording: the broker's under `broker-profile/`, the gateways' under `gateways/`, and the applications'
under `applications/`. Nothing needs to be rendered by hand:

- **Flame graphs** of the measurement recording, in `<recording>-flamegraphs/`: a view for each event the profiler
  options record, CPU for `event=cpu` (or `itimer`, `ctimer`, `cpu-clock`), wall clock for `wall`, allocation for
  `alloc` and lock for `lock`. Options without an event record CPU.
- **Off-CPU flame graphs and a digest** of the blocked time, in `<recording>-offcpu/`, unless the off-CPU admission
  policy is `none`.
- **A profile report** in each profiled component's directory, `README.md` with its HTML page `index.html`, which
  links to both with their totals. Its names make an HTTP server or GitHub open the directory on the report. The run
  report's Profiles section links each profile's reports directly: its jonoffcpu report (off-CPU summary), which is
  the digest, its profile report, and its blocked time, CPU and allocation flame graphs, so the run's `index.html`
  leads to every flame graph of the run.

The launcher prints each of these directories and reports as it writes them. The recordings are named after the
component, such as `broker-profile/inttest_profile_<time>_<container>.jfr` and
`gateways/profile-gateways-<time>.jfr`. For every recording `<recording>.jfr`:

| File | Contents |
|---|---|
| `README.md`, `index.html` | The profile report. **Start here**, from the run report. One per profiled directory (`broker-profile/`, `gateways/`): the run, with a link back to the run report, and for each recording links to the off-CPU digest, the flame graphs with their totals, and the heatmaps, and how to open the JFR recordings in JDK Mission Control |
| `<recording>.jfr` | The complete recording |
| `<recording>.measurement.jfr` | The same cut to the measurement window, see [The measurement recording](#the-measurement-recording) |
| `jfr-configuration.jfc` | The JFR configuration that the component recorded with, merged from its `jfrConfigurations` when they aren't a single configuration of the JDK, see [The JFR configuration](#the-jfr-configuration) |
| `<recording>.measurement.netty-allocator.json` | With `nettyAllocationsReport: true`, the summary of the measurement recording's Netty allocator events, which the profile report shows, see [Netty allocator events](#netty-allocator-events) |
| `<recording>-flamegraphs/` | `cpu`, `wall`, `alloc` and `lock` views of the measurement recording, each only when its event is in the profiler options: `<view>.html`, `<view>-threads.html` (split by thread), `<view>-heatmap.html` (samples over time, for bursts and pauses) and `<view>.collapsed`. Pulsar and BookKeeper frames are highlighted |
| `<recording>.jonoffcpu-capture.pb`, `.manifest.json`, `<recording>.jonoffcpu.yaml` | The off-CPU capture stream, its manifest, and the agent configuration the JVM was started with |
| `<recording>-offcpu/jonoffcpu-summary.md`, `.json` | The off-CPU digest: the blocked time ranked by the application method that waited, by application root and by application method, where the time went and the capture coverage, leaving out the idle waits of `offcpu-idle-waits.txt` |
| `<recording>-offcpu/offcpu-no-idle.html` | The blocked time flame graph of the measurement window: where the profiled process, the broker or a Pulsar client, waited while it had work to do, such as on a lock, a monitor or I/O, without the threads that were only waiting for work |
| `<recording>-offcpu/offcpu-no-idle-app-root.html` | The blocked time from where threads entered Pulsar or BookKeeper code: each stack starts at its first Pulsar or BookKeeper frame once executor and Netty dispatch frames are hidden, so the same code reached from different thread pools or event loops is one tree |
| `<recording>-offcpu/offcpu.html`, `offcpu-app-root.html` | All off-CPU time, idle waits included, as is and from where threads entered Pulsar or BookKeeper code |
| `<recording>-offcpu/*.collapsed`, `*.json` | The same slices as collapsed stacks (full names, microseconds) and the summary of each, including the time the idle filter removed |
| `<recording>-offcpu/offcpu-idle-waits.txt`, `<recording>.offcpu-idle-waits.txt` | The idle-wait patterns the run used; the copy beside the recording is the one the digest's reproduce commands name |
| `<recording>-offcpu/offcpu-dispatch-hide.txt`, `<recording>.offcpu-dispatch-hide.txt` | The BookKeeper and Pulsar frames that only dispatch work (executors running a task, Pulsar's inbound Netty handlers), hidden with jonoffcpu's `jvm-dispatch` preset in the digest and the app-root flame graphs |
| `<recording>-offcpu/jonoffcpu-offcpu-profile.pb` | The stack profile: every distinct stack with its counters, from which other slices are rendered without correlating again |
| `<recording>-offcpu/jonoffcpu-report.json` | Accounting: intervals recorded and matched, loss, switch-out reasons, sleeping versus run-queue time |

### Idle waits

In a broker, over 99 % of off-CPU time is threads waiting for work: Netty event loops in `epollWait`, executor
workers waiting for a task, JDK and HotSpot service threads. The blocked time flame graphs, `offcpu-no-idle` and
`offcpu-no-idle-app-root`, leave those out with the patterns in
the report tool resource `offcpu-idle-waits.txt`. Each pattern names the wait itself rather than the thread's run
loop, so a lock taken while running a task stays in. What remains is lock and monitor contention, safepoints, GC
phases and I/O. The digest leaves out the same idle waits.

The `-app-root` slices start each stack at its root-most frame matching `^org\.apache\.(pulsar|bookkeeper)\.`, once
the frames that only dispatch work are hidden. Stacks without such a frame, such as the JVM's own threads, are left
out of them (`--root-at-unmatched hide`); the profile report shows how much time that was, and the digest ranks it
by thread pool. The off-CPU flame graphs abbreviate package names
(`o.a.p.b.s.p.PersistentDispatcherMultipleConsumers…`) and highlight the Pulsar and BookKeeper frames (`o.a.p.`,
`o.a.b.`). The correlator runs with `--audit none`, which skips its row-level audit files (about 2 KB per interval);
run it again over the retained capture and recording with `--audit full` to reproduce them.

## The measurement recording

After every profiled process exits, the launcher writes a sibling of each recording whose name ends in
`.measurement.jfr`. It contains the events from the gateways' recorded measurement start through the latest
measured-message receipt across all applications, including the full millisecond of that receipt. This leaves out
startup, warmup and shutdown, while keeping the broker and application work needed to deliver every measured message.
The one-time JVM, host, recording setting and runtime configuration events are copied from the beginning of the
complete recording, so that JDK Mission Control can describe the source JVM.

The window assumes that the gateways', the applications' and the broker's clocks agree, as they do for containers on the
same Docker host. Multi-host experiments need synchronized clocks; the launcher doesn't estimate clock skew or correct
the window.

A recording has a chunk of JDK Flight Recorder's events and, appended by `jfrsync`, a chunk of async-profiler's events.
Each chunk's header states how its clock ticks convert to time, and the launcher cuts each chunk by its own header.
JDK 22 and newer readers of a whole recording, such as `jfr print` and the jonoffcpu correlator, convert every chunk
with the first chunk's clock instead, so async-profiler has to count ticks from the same origin as the JVM. The
async-profiler that jonoffcpu 0.8.0 and newer bundles does, on x86 and on arm64. When a chunk's clock doesn't line up
with the first chunk's, `launcher.log` warns about the recording: its measurement recording and flame graphs keep the
events, but whole-recording readers, and the off-CPU correlation, place them elsewhere in time.

### Cutting a recording yourself

The same cutter selects another interval from an existing recording. The task needs JDK 19 or newer, for the public
JFR recording writer added in that release:

```bash
./gradlew :tests:performance:launcher:runJfrCut \
  --args='--input /tmp/full.jfr --from 5s --to 2m --output /tmp/measurement.jfr --info'
```

- `--from` and `--to` accept ISO-8601 instants, epoch milliseconds, or offsets from the recording start such as
  `500ms`, `5s`, `2m`, `1h` or `PT5S`. Omit `--from` to select from the beginning, or `--to` to select through the
  end.
- `--info` without either boundary displays the recording's start, end and total duration from the JFR chunk
  headers; it can also accompany a cut. Cutting preserves the source chunk timestamps, so the original recording
  period remains visible in the cut file and in JDK Mission Control.
- Events overlapping the half-open interval `[from, to)` are kept: duration events ending exactly at `from` are
  left out, instantaneous events at `from` are kept, and events starting exactly at `to` are left out.

Java code can call `JfrCut.cut(Path input, Instant from, Instant to, Path output)` or
`JfrCut.cutFrom(Path input, Instant from, Path output)` directly. `JfrCut.cutUsingTimeExpressions(...)` provides the
relative and omitted-boundary syntax, and `JfrCut.recordingInfo(...)` returns the event range.
