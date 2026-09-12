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

> **Run benchmarks on Linux x86_64 when the numbers matter.** That is Pulsar's most common deployment
> target, and results from elsewhere do not carry over. `System.nanoTime()` is far more expensive on
> macOS than on Linux, which skews the results in some cases — JMH's own measurement loop pays that
> cost on every invocation. async-profiler also supports only some of its sampling engines on macOS,
> so `-prof async` is less reliable there. Benchmarking on macOS or arm64 is fine while iterating —
> just confirm the result on Linux x86_64 before drawing a conclusion from it.

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
(`cpu_classify.html`). A view whose event the recording does not contain is skipped. See
[Analyzing a JFR file](../CONTRIBUTING.md#analyzing-a-jfr-file) for the options it takes.

The `.jfr` can also be opened in [Eclipse Mission Control](https://adoptium.net/jmc) or IntelliJ
IDEA, or handed to an AI agent through the
[Jafar MCP server](https://github.com/btraceio/jafar/blob/main/jfr-mcp/README.md), which lets the
agent query the recording directly with tools such as `jfr_diagnose` and `jfr_stackprofile` — see
[Agent-assisted analysis](../CONTRIBUTING.md#agent-assisted-analysis-with-the-jafar-mcp-server).