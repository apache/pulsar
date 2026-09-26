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
- JDK Flight Recorder (JFR), which async-profiler starts alongside with its `jfrsync` option, records the JVM's own
  events into the same recording, such as monitor contention (`jdk.JavaMonitorEnter`), thread parking
  (`jdk.ThreadPark`) and garbage collection.
- jonoffcpu's eBPF collector records, from the kernel scheduler, every interval in which a thread blocked, into a
  capture stream beside the recording.

After the run, jonoffcpu's correlator joins each blocked interval to the Java stack of the thread that waited, which
gives the **off-CPU** profile. A CPU profile shows where threads burn CPU; the off-CPU profile shows where they wait —
on locks, monitors, queues, I/O, safepoints or GC. The agent, the correlator and the flame graph converter are
resolved by Gradle (see `jonoffcpu` in `gradle/libs.versions.toml`); nothing needs installing on the host or in the
image.

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--config tests/performance/scenarios/iot-telemetry-high-rate-profile.yaml'
```

The `run` task rejects a scenario that has profiler options, rather than silently running it without the agent.
[Analyzing profiles](analyzing-profiles.md) describes how to find what to optimize from the recordings.

## Configuring profiling

The scenario's `profiling` section configures it:

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
  retainOriginalRecording: true
  createMeasurementRecording: true
```

- `brokerOptions`, `producerOptions` and `consumerOptions` are
  [async-profiler options](https://github.com/async-profiler/async-profiler/blob/master/docs/ProfilerOptions.md). An
  empty value leaves that component unprofiled. The launcher owns each recording's path, so that recordings stay
  inside the run directory, and rejects options that set `file=`.
- `jfrsync` chooses what JDK Flight Recorder records alongside async-profiler. `jfrsync=profile` uses the JFR
  configuration named `profile` that the JDK ships, `$JAVA_HOME/lib/jfr/profile.jfc`, which the JDK describes as a
  profiling configuration with about 2 % overhead; `jfrsync=default` uses `default.jfc`, meant for continuous use at
  less than 1 %. `jfrsync` also takes the path of a custom JFR configuration file (`.jfc`), or a list of events
  starting with `+`. A custom file's path is resolved inside the container that runs the JVM, so the file has to be
  readable there, for example built into the test image. Without `jfrsync`, the recording holds only
  async-profiler's samples.
- `offCpu` is the agent's [`sampling` block](https://github.com/jonoffcpu/jonoffcpu#choosing-what-to-sample): which
  switch-out reasons to record (`blocked` — the thread could not run — rather than `runnable` preemption), a minimum
  duration, and an admission policy that records every long wait and samples short ones in proportion to their
  length. The policy `none` records plain async-profiler through the same agent and skips the off-CPU steps.
- `retainOriginalRecording` and `createMeasurementRecording` choose which recordings to keep, see
  [The measurement recording](#the-measurement-recording).

## Requirements

- A Linux Docker engine whose kernel has BTF (`/sys/kernel/btf/vmlinux`), which recent distribution kernels have.
- The relaxed perf event and BPF sysctls. The `profile` task depends on `:tests:integration:tuneKernelPerfEvents`,
  which writes them from a throwaway privileged container; `-Pinttest.asyncprofiler.skipPerfEventTuning` skips it
  where they are already set. `configure-perf-test-environment.sh start` from
  [the performance testing environment setup](../environment/README.md) sets them, and skips the task until `stop`.
- Profiled containers run privileged with the JVM as root: loading the eBPF programs needs `CAP_BPF` and
  `CAP_PERFMON`, which Docker grants to root in the container only. A tracefs is mounted read-only at
  `/sys/kernel/tracing` as a Docker volume.
- Profiled runs use the glibc-based `java-test-image:<tag>-wolfi` image, on which native frames (HotSpot, libc, JNI
  libraries) are symbolized; on the Alpine image every native frame reads as `/lib/ld-musl-x86_64.so.1`.
  `-Pinttest.testImageVariant=alpine` profiles on Alpine anyway.

Correlation holds each capture's distinct stacks in memory. The `profile` task runs with a 4 GB heap, several times
what a few minutes of broker capture needs; `-Pperformance.profile.maxHeapSize=...` changes it.

## What a profiled run writes

When the workloads have finished, the launcher processes every recording and writes the results into the run
directory, next to the recording: the broker's under `broker-profile/`, and the producer's and consumers' in their own
directories (`producer/`, `<application>/`). Nothing needs to be rendered by hand:

- **Flame graphs** of the measurement recording, in `<recording>-flamegraphs/`: a view for each event the profiler
  options record, CPU for `event=cpu` (or `itimer`, `ctimer`, `cpu-clock`), wall clock for `wall`, allocation for
  `alloc` and lock for `lock`. Options without an event record CPU.
- **Off-CPU flame graphs and a digest** of the blocked time, in `<recording>-offcpu/`, unless the off-CPU admission
  policy is `none`.
- **A profile report** in each profiled component's directory, `README.md` with its HTML page `index.html`, which
  links to both with their totals. Its names make an HTTP server or GitHub open the directory on the report. The run
  report links to the profile reports, so the run's `index.html` leads to every flame graph of the run.

The launcher prints each of these directories and reports as it writes them. The recordings are named after the
component, such as `broker-profile/inttest_profile_<time>_<container>.jfr` and
`producer/profile-iot-produce-<time>.jfr`. For every recording `<recording>.jfr`:

| File | Contents |
|---|---|
| `README.md`, `index.html` | The profile report. **Start here**, from the run report. One per profiled directory (`broker-profile/`, `producer/`): the run, with a link back to the run report, and for each recording links to the off-CPU digest, the flame graphs with their totals, and the heatmaps |
| `<recording>.jfr` | The complete recording, unless `retainOriginalRecording: false` |
| `<recording>.measurement.jfr` | The same cut to the measurement window, see [The measurement recording](#the-measurement-recording) |
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

### Idle waits

In a broker, over 99 % of off-CPU time is threads waiting for work: Netty event loops in `epollWait`, executor
workers waiting for a task, JDK and HotSpot service threads. `offcpu-no-idle` leaves those out with the patterns in
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
`.measurement.jfr`. It contains the events from the producer's recorded measurement start through the latest
measured-message receipt across all applications, including the full millisecond of that receipt. This leaves out
startup, warmup and shutdown, while keeping the broker and consumer work needed to deliver every measured message.
The one-time JVM, host, recording setting and runtime configuration events are copied from the beginning of the
complete recording, so that JDK Mission Control can describe the source JVM.

Both `profiling.retainOriginalRecording` and `profiling.createMeasurementRecording` default to `true` and apply to
the broker, producer and consumer recordings:

- `retainOriginalRecording: false` removes the complete recording after a successful cut. If cutting fails, the
  complete recording is kept anyway.
- `createMeasurementRecording: false` keeps only the complete recording.
- Setting both to `false` discards all recordings of the run. Recordings of earlier runs are left alone, which
  matters only when `--output` reuses a directory: each run in the reports hierarchy has a directory of its own.

The window assumes that the producer, consumer and broker clocks agree, as they do for containers on the same
Docker host. Multi-host experiments need synchronized clocks; the launcher doesn't estimate clock skew or correct the
window.

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
