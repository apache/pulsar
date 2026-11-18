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

This module contains microbenchmarks for Apache Pulsar.

## Running the benchmarks

The benchmarks are written using [JMH](http://openjdk.java.net/projects/code-tools/jmh/). To compile & run the benchmarks, use the following command:

```bash
# Compile everything for creating the shaded microbenchmarks.jar file
mvn -Pcore-modules,microbench,-main -T 1C clean package

# run the benchmarks using the standalone shaded jar in any environment
java -jar microbench/target/microbenchmarks.jar
```

For fast recompiling of the benchmarks (without compiling Pulsar modules) and creating the shaded jar, you can use the following command:

```bash
mvn -Pmicrobench -pl microbench clean package
```

### Running specific benchmarks

Display help:

```shell
java -jar microbench/target/microbenchmarks.jar -h
```

Listing all benchmarks:

```shell
java -jar microbench/target/microbenchmarks.jar -l
```

Running specific benchmarks:

```shell
java -jar microbench/target/microbenchmarks.jar ".*BenchmarkName.*"
```

Running specific benchmarks with machine-readable output and saving the output to a file:

```shell
java -jar microbench/target/microbenchmarks.jar -rf json -rff jmh-result-$(date +%s).json ".*BenchmarkName.*" | tee jmh-result-$(date +%s).txt
```

The `jmh-result-*.json` file can be used to visualize the results using [JMH Visualizer](https://jmh.morethan.io/).

Checking what benchmarks match the pattern:

```shell
java -jar microbench/target/microbenchmarks.jar ".*BenchmarkName.*" -lp
```

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

```shell
# example of profiling with async-profiler
# download async-profiler from https://github.com/async-profiler/async-profiler/releases
LIBASYNCPROFILER_PATH=$HOME/async-profiler/lib/libasyncProfiler.dylib
java -jar microbench/target/microbenchmarks.jar -prof async:libPath=$LIBASYNCPROFILER_PATH\;output=flamegraph\;dir=profile-results ".*BenchmarkName.*"
```

When profiling on Mac OS, you might need to add `\;event=itimer` to the `-prof` argument since it's the only [async profiler CPU sampling engine that supports Mac OS](https://github.com/async-profiler/async-profiler/blob/master/docs/CpuSamplingEngines.md#summary). The default value for `event` is `cpu`.

It's possible to add options to the async-profiler that aren't supported by the JMH async-profiler plugin. This can be done by adding `rawCommand` option to the `-prof` argument. This example shows how to add `all` (new in Async Profiler 4.1), `jfrsync` (record JFR events such as garbage collection) and `cstack=vmx` options.

```shell
java -jar microbench/target/microbenchmarks.jar -prof async:libPath=$LIBASYNCPROFILER_PATH\;output=jfr\;dir=profile-results\;rawCommand=all,jfrsync,cstack=vmx ".*BenchmarkName.*"
```