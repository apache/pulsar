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

# Analyzing profiles

A profiled run writes its flame graphs, off-CPU digest and profile reports into the run directory by itself, next
to the recordings, and the run report, the run's `index.html`, links to all of them; see
[What a profiled run writes](profiling.md#what-a-profiled-run-writes). This page describes how to go from them to what
to optimize, and the other tools that read the recordings.

## Finding what to optimize

1. Open the run report, `index.html`, then the broker's profile report and the digest it links to,
   `offcpu-no-idle-app-root.html` and `cpu.html`. A single thread that is busy all the time — the `-threads` views
   show it — is a serial bottleneck that no amount of other headroom helps. The heatmaps show whether CPU or
   allocation comes in bursts or stalls.
2. Rank the blocked time by the deepest Pulsar or BookKeeper frame of each stack and the lock or wait below it. This
   needs no flame graph: stacks without an application frame collect by thread pool, and idle waits are listed
   separately. With the correlator JAR from the [jonoffcpu releases](https://github.com/jonoffcpu/jonoffcpu/releases):

   ```bash
   OFFCPU=<run directory>/broker-profile/<recording>-offcpu
   java -jar jonoffcpu-correlator.jar top --profile $OFFCPU/jonoffcpu-offcpu-profile.pb \
     --app '^org\.apache\.(pulsar|bookkeeper)\.' --waiting-from $OFFCPU/offcpu-idle-waits.txt --package-names abbreviate
   ```

   `export --format jsonl` writes the profile one stack per row for SQL tools such as [DuckDB](https://duckdb.org/).
3. Render other slices from the stack profile in under a second. `--stack java+kernel` continues each stack into the
   kernel so the wait mechanism is visible; `--time split` ends each stack in `[sleeping]` or `[runqueue]`, which
   separates waiting for an event from waiting for a CPU after it arrived; `--include`/`--exclude` and their
   `-from FILE` forms select intervals by frame. Render the result with the converter JAR from the same release:

   ```bash
   java -jar jonoffcpu-correlator.jar stacks --profile $OFFCPU/jonoffcpu-offcpu-profile.pb \
     --exclude-from $OFFCPU/offcpu-idle-waits.txt --time split --package-names abbreviate \
     --output /tmp/blocked-split.collapsed --summary /tmp/blocked-split.json
   java -jar jfr-converter.jar --title "Blocked off-CPU time" --units µs --highlight '^o\.a\.(p|b)\.' \
     /tmp/blocked-split.collapsed /tmp/blocked-split.html
   ```

   The transforms `--root-at`, `--trim-root`, `--hide` and `--collapse-leaf` change what each kept stack looks like
   without changing which intervals are kept or their totals.
4. Compare two runs, see [Comparing two profiles](#comparing-two-profiles).

## Comparing two profiles

Compare the off-CPU time of two runs per unit of work with `top --baseline` (baseline second), for example per
million measured messages. Compare runs recorded with the same sampling policy. Proportional admission
under-represents short waits in the observed weights, so the comparison uses the estimated weights:

```bash
java -jar jonoffcpu-correlator.jar top --profile candidate-offcpu/jonoffcpu-offcpu-profile.pb \
  --baseline baseline-offcpu/jonoffcpu-offcpu-profile.pb --units 4 --baseline-units 4 --weights estimated \
  --app '^org\.apache\.(pulsar|bookkeeper)\.' --waiting-from candidate-offcpu/offcpu-idle-waits.txt --package-names abbreviate
```

Profile both revisions with the same profiler options: profiling has a cost, and different options change it.
[Comparing revisions](comparing-revisions.md) describes how to run the comparison.

## Flame graphs of other recordings

The flame graphs of a standalone run are already in its run directory. Recordings made elsewhere need the root
project's `jfrFlamegraphs` task: those of a module's tests profiled with `-PtestAsyncProfiler` (see
[Profiling tests with async-profiler](../../../CONTRIBUTING.md#profiling-tests-with-async-profiler)), of
[a profiled integration test](../../README.md#profiling-an-integration-test) or
[the legacy TestNG runner](legacy-testng-runner.md), and of the [JMH microbenchmarks](../../../microbench/README.md).
Without `-Pjfr`, it converts every recording in `build/test-profiles`, where `-PtestAsyncProfiler` writes them;
`-Pjfr` points it at a recording or at a directory, which it searches for recordings:

```bash
./gradlew jfrFlamegraphs
./gradlew jfrFlamegraphs -Pjfr=tests/integration/build/pulsar-profiling
```

It writes a directory beside each recording, named after the file without its extension plus `-flamegraphs`, with
the `cpu`, `wall`, `alloc` and `lock` views, each merged (`cpu.html`), split per thread (`cpu_threads.html`) and
grouped into async-profiler's categories (`cpu_classify.html`). A view whose event the recording doesn't contain is
skipped. It doesn't render off-CPU flame graphs, which need the jonoffcpu capture of a standalone profiled run.

## Opening recordings in JDK Mission Control or IntelliJ IDEA

The `.jfr` files open in [Eclipse Mission Control](https://adoptium.net/jmc) and IntelliJ IDEA. For a standalone
profiled run, open `<recording>.measurement.jfr`. Don't use `jfr summary` as a measure of profile completeness:
async-profiler writes its CPU samples as `jdk.ExecutionSample` and its allocation samples as
`jdk.ObjectAllocationInNewTLAB` and `jdk.ObjectAllocationOutsideTLAB` in its own chunks, which the JDK summary counts
as zero even when the flame graphs are full.

On macOS, add the JDK Mission Control launcher to a directory on `PATH`:

```bash
mkdir -p ~/.local/bin
ln -s /Applications/JDK\ Mission\ Control.app/Contents/MacOS/jmc ~/.local/bin/jmc
```

JDK Mission Control requires an absolute recording path:

```bash
jmc -open "$PWD/<recording.jfr>"
```

This shell function accepts a relative or absolute path and resolves it before launching JMC. Add it to `~/.zshrc`
or the corresponding shell startup file:

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

With IntelliJ IDEA's command-line launcher installed, open a recording with `idea <recording.jfr>`.

## AI agent analysis

The [Jafar MCP server](https://github.com/btraceio/jafar/blob/main/jfr-mcp/README.md) lets an AI coding agent query a
recording. Register it once with [JBang](https://www.jbang.dev/) and JDK 25+:

```bash
claude mcp add jafar -- jbang jfr-mcp@btraceio --stdio
```

For a broader toolset, use the `jafar-perf` Claude Code plugin from
[jafar-perf-box](https://github.com/btraceio/jafar-perf-box) instead. It adds skills and agents on top of the Jafar
MCP server that guide an analysis of a recording or a [heap dump](#heap-dumps-and-memory-leaks) from triage to a
report, including comparing recordings and investigating memory leaks, and it registers the Jafar MCP server itself.
[Its README](https://github.com/btraceio/jafar-perf-box/blob/main/plugins/jafar-perf/README.md) describes how to
install and use it.

Use `jfr_diagnose` and `jfr_stackprofile` first, then query further with the other Jafar tools when needed. Save the
result beside the recording as `<recording>.analysis.md`, in addition to showing the report in the console. For a
standalone profiled run, analyze `<recording>.measurement.jfr`: CPU samples are `jdk.ExecutionSample`,
async-profiler's allocation samples are `jdk.ObjectAllocationInNewTLAB` (not `jdk.ObjectAllocationSample`), and
`jfrsync=profile` adds JDK events such as `jdk.JavaMonitorEnter` and `jdk.ThreadPark` (see
[Configuring profiling](profiling.md#configuring-profiling)). Off-CPU time is not in a JFR file; use the digest
`<recording>-offcpu/jonoffcpu-summary.md` and the correlator's `top` and `stacks` subcommands (see
[Finding what to optimize](#finding-what-to-optimize)). A useful starting prompt is:

> use Jafar MCP's jfr_diagnose and jfr_stackprofile to analyze @filename.jfr. Besides showing the
> report on the console, write the analysis in a markdown file with the jfr file as prefix and the
> suffix as ".analysis.md"

Treat automated analysis as a lead. Confirm a performance claim with a controlled comparison, a JMH benchmark where
appropriate, or a second profile.

## Heap dumps and memory leaks

For an `OutOfMemoryError` or a suspected retention problem, analyze the resulting `.hprof` heap dump with an AI agent
through one of these tools:

- The [`jafar-perf` plugin](#ai-agent-analysis) from jafar-perf-box supports heap dump analysis, including
  investigating memory leaks and comparing heap dumps.
- [codelipenghui/mcp-mat](https://github.com/codelipenghui/mcp-mat), an MCP server that runs a headless Eclipse Memory
  Analyzer (MAT). Use the leak suspects report and the dominator tree first, then query paths to GC roots or OQL for
  the retained objects.

Keep the heap dump and the analysis outside the source tree when they contain sensitive workload data; record the
commands, heap limits and the resulting conclusions in the experiment notes.
