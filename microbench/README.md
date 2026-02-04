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