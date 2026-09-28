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

## Running the analysis CLIs

Run the jonoffcpu correlator and its jfr-converter through Gradle from the repository root. Gradle resolves the
same versions used by profiling, pinned by `jonoffcpu` in `gradle/libs.versions.toml`; neither tool needs a separate
installation. These tasks do not compile Pulsar, build images or start containers. Pass any of the tools' CLI
arguments through `--args`, including subcommands, filters and output paths:

```bash
./gradlew -q :tests:performance:report-tool:runJonoffcpuCorrelator --args='--help'
./gradlew -q :tests:performance:report-tool:runJonoffcpuCorrelator --args='top --help'
./gradlew -q :tests:performance:report-tool:runJfrConverter --args='--help'
```

Relative input and output paths resolve from the repository root. Quote paths containing spaces inside `--args`,
as in the examples below. `-q` suppresses Gradle's progress output. Both tasks use a `4g` maximum heap by default;
override it with `-Pperformance.profile.maxHeapSize=8g` for a larger capture. Use the help from the pinned tools
to discover their available options. The correlator prints rankings with `top`, writes collapsed stacks with
`stacks`, and exports stack rows with `export`; the converter writes flame graph HTML from collapsed stacks or JFR.
Choose fresh output paths for the correlator: it refuses to overwrite existing files. The converter can overwrite
its output, so choose a new filename when preserving an earlier rendering.

## Finding what to optimize

1. Open the run report, `index.html`, then the broker's profile report and the digest it links to,
   `offcpu-no-idle-app-root.html` and `cpu.html`. The digest ranks the blocked time by the method that waited; the
   blocked time flame graph, `offcpu-no-idle-app-root.html`, shows the same time as call trees, to inspect visually
   which code paths lead to the blocking methods. A single thread that is busy all the time — the `-threads` views
   show it — is a serial bottleneck that no amount of other headroom helps. The heatmaps show whether CPU or
   allocation comes in bursts or stalls.
2. Rank the blocked time by the deepest Pulsar or BookKeeper frame of each stack and the lock or wait below it. This
   needs no flame graph: stacks without an application frame collect by thread pool, and idle waits are listed
   separately. Replace the example directory with the off-CPU directory from your run:

   ```bash
   offcpu_dir=/absolute/path/to/run/broker-profile/recording-offcpu
   ./gradlew -q :tests:performance:report-tool:runJonoffcpuCorrelator \
     --args="top --profile '$offcpu_dir/jonoffcpu-offcpu-profile.pb' \
     --app '^org\.apache\.(pulsar|bookkeeper)\.' --waiting-from '$offcpu_dir/offcpu-idle-waits.txt' --package-names abbreviate"
   ```

   `export --format jsonl` writes the profile one stack per row for SQL tools such as [DuckDB](https://duckdb.org/).
3. Render other slices from the stack profile without correlating again. `--stack java+kernel` continues each stack
   into the kernel so the wait mechanism is visible; `--time split` ends each stack in `[sleeping]` or `[runqueue]`, which
   separates waiting for an event from waiting for a CPU after it arrived; `--include`/`--exclude` and their
   `-from FILE` forms select intervals by frame. Render the result with the converter:

   ```bash
   ./gradlew -q :tests:performance:report-tool:runJonoffcpuCorrelator \
     --args="stacks --profile '$offcpu_dir/jonoffcpu-offcpu-profile.pb' \
     --exclude-from '$offcpu_dir/offcpu-idle-waits.txt' --time split --package-names abbreviate \
     --output /tmp/blocked-split.collapsed --summary /tmp/blocked-split.json"
   ./gradlew -q :tests:performance:report-tool:runJfrConverter \
     --args="--title 'Blocked off-CPU time' --units µs --highlight '^o\.a\.(p|b)\.' \
     /tmp/blocked-split.collapsed /tmp/blocked-split.html"
   ```

   The transforms `--root-at`, `--trim-root`, `--hide` and `--collapse-leaf` change what each kept stack looks like
   without changing which intervals are kept or their totals.
4. Compare two runs, see [Comparing two profiles](#comparing-two-profiles).

## Comparing two profiles

Compare the off-CPU time of two runs per unit of work with `top --baseline` (candidate in `--profile`), for example per
million measured messages. Compare runs recorded with the same sampling policy. Proportional admission
under-represents short waits in the observed weights, so the comparison uses the estimated weights:

```bash
./gradlew -q :tests:performance:report-tool:runJonoffcpuCorrelator \
  --args="top --profile candidate-offcpu/jonoffcpu-offcpu-profile.pb \
  --baseline baseline-offcpu/jonoffcpu-offcpu-profile.pb --units 4 --baseline-units 4 --weights estimated \
  --app '^org\.apache\.(pulsar|bookkeeper)\.' --waiting-from candidate-offcpu/offcpu-idle-waits.txt --package-names abbreviate"
```

The example assumes four million measured messages in each run. Replace `--units 4` and `--baseline-units 4`
with each run's measured message count divided by one million, excluding warmup; do not assume the counts match.
Keep the same filters and profiler options on both revisions. Profiling has a cost, and different options change it.
[Comparing revisions](comparing-revisions.md) describes how to run the comparison.

## Analyzing collapsed stacks with DuckDB

For SQL analysis, DuckDB's [quack_flamegraph](https://github.com/kevintruong/quack-flamegraph) community extension
reads collapsed stacktrace files as tables. These are also called folded stacktrace files: `.collapsed` and
`.folded` are common extensions for the same format, with semicolon-separated frames and a weight at the end of
each line.

### Set up the profile and frame filter

Start DuckDB and set `profile` to the path of your collapsed stacks file, relative to DuckDB's working directory
or absolute. Set `pkg` to a regular expression for the frames you're interested in. Run the queries in the same
session; they read both variables, so you need to change the path and filter only once:

```sql
INSTALL quack_flamegraph FROM community;
LOAD quack_flamegraph;
```

```sql
SET VARIABLE profile = 'cpu.collapsed';
SET VARIABLE pkg = '^org[./]apache[./]';
```

The `^` anchors the match to the start of the frame name, and `[./]` matches either package separator, since frame
names use `/` or `.` depending on how the file was produced. To match several packages at once, list them as
alternatives:

```sql
SET VARIABLE pkg = '^org[./]apache[./](pulsar|bookkeeper)[./]';
```

Don't append `$` to these package-prefix patterns: frame names continue with the class and method name.
To match a substring anywhere in the frame name, leave out the `^`, as in `ManagedLedger`. Match the names actually
present in the file; an abbreviated frame such as `o.a.p.ManagedLedger.read` needs a different pattern.

The column named `samples` holds the weight recorded in the input, not necessarily a sample count. Depending on
the profile, weights can represent counts, durations or allocation sizes. Keep that unit when interpreting results;
do not compare weights from different profile types as if they measured the same thing.

### Rank stacks and methods

This query returns the 45 highest-weight stacks whose leaf frame matches `pkg`:

```sql
SELECT samples, leaf
FROM flamegraph_hot_stacks(getvariable('profile'))
WHERE regexp_matches(leaf, getvariable('pkg'))
ORDER BY samples DESC
LIMIT 45;
```

This ranks individual stacks, not totals grouped by leaf method, and filters only the leaf frame, not callers
elsewhere in the stack.

To rank matching methods by the total weight of the stacks they appear in, anywhere in the stack, use
`flamegraph_coverage`. Each stack is counted once per frame, so recursive calls don't inflate the total:

```sql
SELECT frame, coverage
FROM flamegraph_coverage(getvariable('profile'))
WHERE regexp_matches(frame, getvariable('pkg'))
ORDER BY coverage DESC
LIMIT 45;
```

### Find calls into other code

To find the highest-weight call edges from a matching parent frame to a child that doesn't match:

```sql
FROM flamegraph_edges(getvariable('profile'))
WHERE regexp_matches(parent, getvariable('pkg'))
  AND NOT regexp_matches(child, getvariable('pkg'))
ORDER BY samples DESC
LIMIT 45;
```

### Attribute samples to the deepest matching frame

To group the profile's weight by the last matching frame before execution enters other code, create `leaf_by_frame`.
For each stack, it finds the deepest frame matching `pkg` and pairs it with the frame it calls (`child`) and the
frame where the sample was taken (`leaf`). `child` is `NULL` when the matching frame is itself the leaf. Stacks
without a matching frame are left out, and `pct` is the share of the entire profile's weight, including unmatched
stacks. This associates work with a calling frame; it does not by itself prove that the caller is a bottleneck.

```sql
CREATE OR REPLACE VIEW leaf_by_frame AS
WITH total AS (SELECT sum(samples) AS t FROM read_folded(getvariable('profile'))),
m AS (
  SELECT frames, leaf, samples,
         list_last(list_filter(frames, lambda f: regexp_matches(f, getvariable('pkg')))) AS frame
  FROM flamegraph_hot_stacks(getvariable('profile'))
),
s AS (
  SELECT frame,
         frames[len(frames) - list_position(list_reverse(frames), frame) + 2] AS child,
         leaf, samples
  FROM m
  WHERE frame IS NOT NULL
)
SELECT frame, child, leaf,
       sum(samples)                                 AS samples,
       round(100.0 * sum(samples) / any_value(t), 2)  AS pct
FROM s CROSS JOIN total
GROUP BY ALL;
```

You can store this view in a file name `views.sql` for loading on command line.

The view reads `profile` and `pkg` each time it is queried, so changing either variable changes its results:

```sql
FROM leaf_by_frame
ORDER BY samples DESC
LIMIT 45;
```

### Analysing collapsed stacktrace files with quack_flamegraph

AI agents can automate analysis by combining quack_flamegraph's functions in DuckDB queries and views, as the
`leaf_by_frame` view above demonstrates. They can join results and add `WHERE` clauses to narrow the analysis,
exclude irrelevant matches, and investigate specific call paths.

For example, this command ranks stacks whose leaf frames match the filter and prints the results as JSON:

```sql
duckdb -json \
-cmd "SET VARIABLE profile = 'cpu.collapsed';SET VARIABLE pkg = '^org[./]apache[./]';" \
-cmd "INSTALL quack_flamegraph FROM community;LOAD quack_flamegraph;" \
-c "
SELECT samples, leaf
FROM flamegraph_hot_stacks(getvariable('profile'))
WHERE regexp_matches(leaf, getvariable('pkg'))
ORDER BY samples DESC
LIMIT 45;
"
```

To use the `leaf_by_frame` view from the command line, save its definition above in `quack_flamegraph_views.sql`,
then load it before running the query:

```sql
duckdb -json \
-cmd "SET VARIABLE profile = 'cpu.collapsed';SET VARIABLE pkg = '^org[./]apache[./]';" \
-cmd "INSTALL quack_flamegraph FROM community;LOAD quack_flamegraph;" \
-cmd ".read quack_flamegraph_views.sql" \
-c "
FROM leaf_by_frame ORDER BY samples DESC LIMIT 45;
"
```

### Export results for automation

For automated analysis, agents and scripts can export DuckDB query results as JSON or CSV. Save the setup statements
and one result query in `analysis.sql` (include the view definition if querying `leaf_by_frame`), then run:

```bash
duckdb -no-init -bail -json < analysis.sql > analysis.json
duckdb -no-init -bail -csv -header < analysis.sql > analysis.csv
```

Use one result query per output file so that JSON contains a single array and CSV contains one table with a header.
`-bail` stops on SQL errors; check the exit status before consuming the output.




## Flame graphs of other recordings

The flame graphs of a standalone run are already in its run directory. Recordings made elsewhere need the root
project's `jfrFlamegraphs` task: those of a module's tests profiled with `-PtestAsyncProfiler` (see
[Profiling tests with async-profiler](../../../CONTRIBUTING.md#profiling-tests-with-async-profiler)), of
[a profiled integration test](../../README.md#profiling-an-integration-test) or
[the legacy TestNG runner](legacy-testng-runner/README.md), and of the [JMH microbenchmarks](../../../microbench/README.md).
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

The `.jfr` files open in JDK Mission Control and IntelliJ IDEA. The OpenJDK distribution of JDK Mission Control is
[Eclipse Mission Control](https://adoptium.net/jmc), and the JDK's
[Troubleshoot Performance Issues Using Flight Recorder](https://docs.oracle.com/en/java/javase/25/troubleshoot/troubleshoot-performance-issues-using-jfr.html#GUID-0FE29092-18B5-4BEB-8D8D-0CBA7A4FEA1D)
guide describes finding performance issues in a recording with it. For a standalone profiled run, open
`<recording>.measurement.jfr`. Don't use `jfr summary` as a measure of profile completeness:
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

## Interactive analysis with jafar-shell

[jafar-shell](https://github.com/btraceio/jafar) is an interactive shell for JFR recordings and heap dumps, and also
pprof and OpenTelemetry profiles, with a query language for each: JfrPath for recordings and HdumpPath for heap dumps.
It is the newer version of jfr-shell, which reads only JFR recordings, and replaces it. Install it with
[JBang](https://www.jbang.dev/):

```bash
jbang app install jafar-shell@btraceio
```

Heap dump support is in Jafar's main branch and not yet in every release. When `jafar-shell` opens an `.hprof` as a
JFR recording and fails, build it from source and install the build instead:

```bash
git clone https://github.com/btraceio/jafar.git && cd jafar
./gradlew :jafar-shell:shadowJar
jbang app install --force --name jafar-shell jafar-shell/build/libs/jafar-shell-*-all.jar
```

Open a recording or a heap dump with `jafar-shell <file>`, and run queries with `show`, for example on a broker's
measurement recording:

```
jfr> show events/jdk.ExecutionSample | groupBy(sampledThread/javaName) | top(5, count)
```

The shell also reads its commands from standard input, so that a script or an agent can run queries without the
interactive prompt:

```bash
printf 'show events/jdk.ExecutionSample | count()\nexit\n' | jafar-shell -q <recording>.measurement.jfr
```

[The heap dump quick start](https://github.com/btraceio/jafar/blob/main/doc/hdump-shell-quickstart.md) and
[the JFR shell tutorial](https://github.com/btraceio/jafar/blob/main/doc/cli/Tutorial.md) describe the queries.

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

For an `OutOfMemoryError`, a suspected retention problem or a heap that fills up, analyze an `.hprof` heap dump. The
performance tests' launcher writes heap dumps of the broker, the gateways and the applications when they run out of
memory, at the highest heap usage and at given times, see [Heap dumps](heap-dumps.md). A compressed `.hprof.gz` dump
needs decompressing first for jafar-shell, which reads uncompressed dumps only: `gunzip -k <dump>.hprof.gz`. These
tools read them:

- [jafar-shell](#interactive-analysis-with-jafar-shell), interactively or from a script. [The heap dump quick
  start](https://github.com/btraceio/jafar/blob/main/doc/hdump-shell-quickstart.md) describes its HdumpPath queries,
  from the classes with the most instances to retained sizes, dominators, paths to GC roots and built-in leak
  detectors. Prefix each query with `show` in the shell:

  ```
  hdump> show classes | top(10, instanceCount)
  hdump> show objects | dominators(groupBy="class") | head(10)
  hdump> show checkLeaks(detector="growing-collections")
  ```

  For example, a dump of a broker with thousands of Key_Shared consumers shows that the subscriptions' consistent
  hash rings, the `ConsistentHashingStickyKeyConsumerSelector` instances and their `TreeMap` entries, retain most of
  its heap.
- The [`jafar-perf` plugin](#ai-agent-analysis) from jafar-perf-box lets an AI agent analyze heap dumps, including
  investigating memory leaks and comparing heap dumps.
- [codelipenghui/mcp-mat](https://github.com/codelipenghui/mcp-mat), an MCP server that runs a headless Eclipse Memory
  Analyzer (MAT). Use the leak suspects report and the dominator tree first, then query paths to GC roots or OQL for
  the retained objects.

Keep the heap dump and the analysis outside the source tree when they contain sensitive workload data; record the
commands, heap limits and the resulting conclusions in the experiment notes.
