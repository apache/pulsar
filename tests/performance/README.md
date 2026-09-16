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

This directory documents repeatable performance experiments and their analysis. The container based
profiling harness lives under [`tests/integration`](../integration); its recordings normally land in
`tests/integration/build/pulsar-profiling`. Keep scenario files, commands, results and interpretation
here so that a later run can reproduce the same workload.

For micro-level questions about one class or method, use the JMH benchmarks in
[`microbench`](../../microbench). JMH is the benchmark harness; this directory is for documenting the
end-to-end profiling scenario, profile collection, analysis and conclusions. A useful experiment keeps
the workload definition, the revision under test, the profiler options, the raw recording and the
resulting analysis together.

## Profiling an integration-test cluster

Run the built-in scenarios with:

```bash
./gradlew :tests:integration:profilingIntegrationTest
./gradlew :tests:integration:profilingIntegrationTest --tests "*PulsarProfilingV4Test"
```

The first command profiles the v5 scalable-topic scenario. The second uses the v4 client against a
classic `persistent://` topic. Both variants profile a single broker and write recordings and command
output under `tests/integration/build/pulsar-profiling`.

The harness accepts a YAML scenario file through `PULSAR_PROFILING_CONFIG`. Start with
[`pulsar-profiling.yaml`](pulsar-profiling.yaml); omitted values retain the existing defaults. The
sections correspond to the main components of a run: `cluster`, `load` and `output`. Individual scalar
values can still be overridden for a one-off run with the `PULSAR_PROFILING_` prefix and an upper-case
path, for example:

```bash
PULSAR_PROFILING_CONFIG=tests/performance/pulsar-profiling.yaml \
PULSAR_PROFILING_LOAD_NUMBER_OF_MESSAGES=1000000 \
./gradlew :tests:integration:profilingIntegrationTest --tests "*PulsarProfilingV4Test"
```

## Inspecting recordings

Render the CPU, wall-clock, allocation and lock views with:

```bash
./gradlew jfrFlamegraphs -Pjfr=tests/integration/build/pulsar-profiling
```

The `.jfr` files can also be opened in [Eclipse Mission Control](https://adoptium.net/jmc) or IntelliJ
IDEA. Do not use `jfr summary` as a measure of profile completeness: recordings made with
`jfrsync=profile` contain profiler samples that the JDK summary does not show.

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
