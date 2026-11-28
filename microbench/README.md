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

# Microbenchmarks for Apache Pulsar

This module contains [JMH](https://github.com/openjdk/jmh) microbenchmarks for Apache Pulsar, for questions about
the performance of a single class or method, such as what a method, a data structure or a codec costs. JMH handles
warm-up, dead-code elimination and measurement, which hand-written timing code gets wrong, so a benchmark is the
strongest evidence for a small, self-contained optimization.
[The Pulsar Performance Testing Framework](../tests/performance/README.md) covers end-to-end scenarios that run a Pulsar cluster, and profiling and analysis.

> **Run benchmarks on Linux x86_64 when the numbers matter.** That is Pulsar's most common deployment
> target, and results from elsewhere do not carry over. `System.nanoTime()` is far more expensive on
> macOS than on Linux, which skews the results in some cases — JMH's own measurement loop pays that
> cost on every invocation. async-profiler also supports only some of its sampling engines on macOS,
> so `-prof async` is less reliable there. Benchmarking on macOS or arm64 is fine while iterating —
> just confirm the result on Linux x86_64 before drawing a conclusion from it.

## Getting consistent results

Results vary between runs when the CPU changes its frequency: turbo frequencies depend on the temperature and the
power budget of the CPU, and power management changes CPU and device settings while the benchmarks run. On Linux,
[the performance testing environment setup](../tests/performance/environment/README.md) configures the host for
consistent results, with turbo disabled so that the CPU runs at a fixed frequency, and restores power saving
afterwards.

## Running the benchmarks

The benchmarks are written using [JMH](http://openjdk.java.net/projects/code-tools/jmh/). To compile & run the benchmarks, use the following command:

```bash
# Compile everything including the shaded microbenchmarks jar
./gradlew :microbench:shadowJar

# run the benchmarks using the standalone shaded jar in any environment
java -jar microbench/build/libs/microbench-*-benchmarks.jar
```

### Running specific benchmarks

Display help:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar -h
```

Listing all benchmarks:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar -l
```

Running specific benchmarks:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar ".*BenchmarkName.*"
```

`KeyBasedBatchGroupingBenchmark` measures key construction, hashing and lookup in a populated batch
map for 1,024 keys, using ASCII text, UTF-8 text, binary partition keys and ordering keys of 16/64 raw
bytes. It compares legacy stored-string grouping, canonical Base64, canonical Latin-1, and Latin-1
with reuse of plain ASCII strings. The legacy approach is a cost reference with known correctness
issues when key representations are mixed. Setup and message creation are excluded; the reused input
strings can retain their cached hashes. These are grouping lookup costs, not producer throughput.
To compare Base64 normalization with the optimized container behavior and record allocation:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  'KeyBasedBatchGroupingBenchmark.(normalizedGroupingLookup|candidateGroupingLookup)' \
  -jvmArgsAppend '-Xms512m -Xmx512m -XX:+UseG1GC' -prof gc
```

`ManagedLedgerTrimScanBenchmark` compares the original full offload-deletion scan and `List.contains`
checks with a policy guard and a scan after the ordinary deletion prefix. It uses the managed-ledger
`ConcurrentSkipListMap` and real ledger metadata, including gaps in ledger IDs and a mix of complete,
incomplete, recent, and already-deleted offloads. Parameters cover 1,000/10,000 ledgers, 0%/50% ordinary
deletion candidates, and absent, null-lag, negative-lag, zero-lag, and positive-lag policies. Setup checks
that both algorithms select the same ordered candidates. This measures scan and membership costs with
result-list allocation; it excludes retention checks, locks, worker scheduling, metadata updates, and
BookKeeper deletion, so results are not full trim or broker latency. For a focused run with allocation data:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar ManagedLedgerTrimScanBenchmark \
  -p ledgerCount=10000 -p ordinaryDeletionPercent=0,50 -p policy=ABSENT,POSITIVE_LAG -prof gc
```

Running specific benchmarks with machine-readable output and saving the output to a file:

```shell
ts=$(date +%s)
java -jar microbench/build/libs/microbench-*-benchmarks.jar -rf json -rff jmh-result-$ts.json ".*BenchmarkName.*" | tee jmh-result-$ts.txt
```

The `jmh-result-*.json` file can be used to visualize the results using [JMH Visualizer](https://jmh.morethan.io/).

Checking what benchmarks match the pattern:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar ".*BenchmarkName.*" -lp
```

### Subscription backlog estimation

`ManagedLedgerBacklogSizeBenchmark` compares the production batch estimator with repeated calls to the production
per-position estimator. Each invocation estimates `subscriptionCount` subscriptions over `ledgerCount` closed-ledger
metadata records. `CLUSTERED_TAIL` places positions in the last four ledgers, with overlapping backlog suffixes;
`OPPOSITE_ENDS` alternates between the first and last ledger, including the two-subscription case. Setup checks that
the two estimators return identical results before measurement. Constructor collaborators are mocked; timed estimator
calls use a real `ManagedLedgerImpl` and do not invoke mocks or perform storage I/O.
Both paths consume every subscription's result. The per-position path consumes each estimate directly; the batch
path includes building the result map and looking up its estimates, as the stats caller does.

Measure latency and bytes allocated per complete batch of subscription estimates:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  '.*ManagedLedgerBacklogSizeBenchmark.estimate' \
  -p ledgerCount=10000 -p subscriptionCount=2,100 \
  -p placement=CLUSTERED_TAIL,OPPOSITE_ENDS -p estimator=PER_POSITION,BATCH \
  -prof gc -rf json -rff backlog-estimate.json
```

Probe monitor contention with one continuously polling stats thread and one thread acquiring the same ledger monitor:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  '.*ManagedLedgerBacklogSizeBenchmark.contended' \
  -p ledgerCount=10000 -p subscriptionCount=2,100 \
  -p placement=CLUSTERED_TAIL,OPPOSITE_ENDS -p estimator=PER_POSITION,BATCH \
  -rf json -rff backlog-contention.json
```

Inspect the `monitorProbe` sample-time percentiles separately from the `stats` scores. The probe's time includes
monitor acquisition and a counter increment; it measures a synthetic lock competitor, not `addEntry` latency or
throughput. The stats thread polls continuously, so faster estimators also issue more stats requests; these results
describe saturation rather than a fixed stats polling interval. Use lock profiling to examine blocked stacks and
cluster performance tests to assess effects on publishing. To compare revisions of the batch implementation, run
the same benchmark and parameters against each revision, preserving the JDK, JVM flags and host configuration.

Use the separate `periodic` group to compare monitor contention at the same requested stats polling rate:

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  '.*ManagedLedgerBacklogSizeBenchmark.periodic' \
  -p ledgerCount=10000 -p subscriptionCount=2,100 \
  -p placement=CLUSTERED_TAIL,OPPOSITE_ENDS -p estimator=PER_POSITION,BATCH \
  -p pollPeriodMicros=20000 -bm avgt -rf json -rff backlog-periodic.json
```

The default period requests 50 stats polls per second. Pacing uses a per-thread clock and parks outside the ledger
monitor. `periodicStats` measures the complete polling cycle, including its pacing delay; use the standalone
`estimate` benchmark for estimator latency. Average-time mode includes every measured monitor acquisition
in the `periodicMonitorProbe` mean. Sample-time mode can miss rare delayed acquisitions at this polling rate,
so its optional percentiles serve only as diagnostics.

The `polls` and `skippedPolls` auxiliary results count requests and dropped schedule slots, respectively.
Their JSON `rawData` contains per-iteration counts, which can include calls while JMH synchronizes workers
at the start and end of measurement. Use these counts as an approximate cadence check and compare monitor-probe
means when both estimators sustain similar polling rates with few missed slots. Use longer measurement iterations
to reduce the effect of calls at the boundaries. If estimates repeatedly overrun scheduled polls, increase the
poll period. As with the saturation group, the monitor probe is a synthetic competitor.

### Redelivery tracker

`InMemoryRedeliveryTrackerBenchmark` measures the tracker shared by consumers of one subscription.
The `readOnly` workload covers empty trackers, hits, and misses. The `mixed` workload updates existing
positions at a fixed operation ratio, without resizing either map. The `acknowledge` workload measures
normal empty-tracker reads interleaved with `removeBatch`, including acknowledgements when no entry
has been redelivered. Positions and acknowledgement batches are allocated during setup.

Build a separate benchmark JAR from each tracker implementation being compared, using the same
benchmark source, JDK, heap, GC, and CPU allocation. The benchmark calls the tracker from that build;
it does not contain copies of alternative production implementations. All worker threads share one
tracker. For example, on JDK 21:

```bash
./gradlew :microbench:shadowJar

redelivery_jmh_args=(-f 2 -wi 3 -w 1s -i 5 -r 1s -prof gc -rf json
  -jvmArgs "-Xms512m -Xmx512m -XX:+UseZGC -XX:+ZGenerational")

for redelivery_threads in 1 8; do
  java -jar microbench/build/libs/microbench-*-benchmarks.jar \
    'InMemoryRedeliveryTrackerBenchmark.readOnly$' "${redelivery_jmh_args[@]}" \
    -t "$redelivery_threads" -p lookup=empty,hit \
    -rff "redelivery-read-t${redelivery_threads}.json"
done

java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  'InMemoryRedeliveryTrackerBenchmark.mixed$' "${redelivery_jmh_args[@]}" \
  -t 8 -p writePercent=1,10 -rff redelivery-mixed.json

java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  'InMemoryRedeliveryTrackerBenchmark.acknowledge$' "${redelivery_jmh_args[@]}" \
  -t 8 -p batchSize=1,100 -rff redelivery-acknowledge.json
```

Save results separately for each implementation. `writePercent` counts write operations, rather
than writer threads. In `acknowledge`, each thread performs `batchSize` lookups followed by one batch
acknowledgement. One measured operation is a lookup or a batch acknowledgement, so compare results
at the same batch size. These are tracker microbenchmarks; use the concurrent regression tests for
map-resize correctness and a broker workload for end-to-end throughput and latency.

### Profiling with async-profiler

Profiling benchmarks with [async-profiler](https://github.com/async-profiler/async-profiler):

Set `LIBASYNCPROFILER_PATH` to the path of the async-profiler library.

Corretto JDK ships with async-profiler (asprof binary and libasyncProfiler dynamic library)

```shell
LIBASYNCPROFILER_PATH=$(ls $JAVA_HOME/lib/libasyncProfiler.*)
```

Alternatively, download async-profiler from https://github.com/async-profiler/async-profiler/releases and install to ~/async-profiler directory.

Mac OS example:

```shell
LIBASYNCPROFILER_PATH=$HOME/async-profiler/lib/libasyncProfiler.dylib
```

Linux example:

```shell
LIBASYNCPROFILER_PATH=$HOME/async-profiler/lib/libasyncProfiler.so
```

Then run the benchmarks with the `-prof` argument:
```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar -prof async:libPath=$LIBASYNCPROFILER_PATH\;output=flamegraph\;dir=profile-results ".*BenchmarkName.*"
```

The default value for `event` is `cpu`, which is a request for the best available [CPU sampling engine](https://github.com/async-profiler/async-profiler/blob/master/docs/CpuSamplingEngines.md#summary) rather than a specific one, so what it resolves to depends on the platform. If the profiler fails to start, add `\;event=itimer` to the `-prof` argument: `itimer` is available everywhere.

It's possible to add options to the async-profiler that aren't supported by the JMH async-profiler plugin. This can be done by adding `rawCommand` option to the `-prof` argument. This example shows how to add `all` (new in Async Profiler 4.1), `jfrsync` (record JFR events such as garbage collection) and `cstack=vmx` options.

```shell
java -jar microbench/build/libs/microbench-*-benchmarks.jar -prof async:libPath=$LIBASYNCPROFILER_PATH\;output=jfr\;dir=profile-results\;rawCommand=all,jfrsync,cstack=vmx ".*BenchmarkName.*"
```

Outside Linux this particular command needs `\;event=itimer` as well. `all` turns on wall clock
profiling, and where the `cpu` engine falls back to the wall clock engine the profiler refuses to
start with `Cannot start wall clock with the selected event`, which shows up as a `<failure>` on the
first warmup iteration and an empty result directory.

### Turning a JFR recording into flame graphs

`output=jfr` writes one recording per benchmark, into a directory named after the benchmark under
`dir=`. The `jfrFlamegraphs` Gradle task renders each recording into every view at once — point it at
the whole output directory and it finds the recordings inside:

```shell
./gradlew jfrFlamegraphs -Pjfr=profile-results
```

Each recording gets a directory beside it named after the file without its extension plus a
`-flamegraphs` suffix, holding `cpu`, `wall`, `alloc` and `lock`, each rendered merged
(`cpu.html`), split per thread (`cpu_threads.html`) and grouped into async-profiler's categories
(`cpu_classify.html`). A view whose event the recording does not contain is skipped.

The `.jfr` can also be opened in [Eclipse Mission Control](https://adoptium.net/jmc) or IntelliJ
IDEA, or analyzed by an AI agent.
[The Pulsar Performance Testing Framework](../tests/performance/README.md) leads to the analysis workflow and its tools.
