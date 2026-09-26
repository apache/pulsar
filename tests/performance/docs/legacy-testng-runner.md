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

# Legacy TestNG profiling runner

The original profiling harness under `tests/integration` uses TestNG classes as wrappers around a manually run
`pulsar-perf` workload. It is deprecated: TestNG discovery and test lifecycle add no useful test semantics to
long-running profiling scenarios, and make them harder to invoke and automate as standalone jobs. It is kept for its
existing v4 and v5 `pulsar-perf` scenarios while they are migrated. Add new scenarios, workload applications and
profiling support to the standalone launcher instead.

## Running it

```bash
./gradlew :tests:integration:profilingIntegrationTest
./gradlew :tests:integration:profilingIntegrationTest --tests "*PulsarProfilingV4Test"
```

`profilingIntegrationTest` builds the test image with async-profiler, relaxes the kernel's `perf_event` limits and
runs the test with retries off, as [Profiling an integration test](../../README.md#profiling-an-integration-test)
describes. Both variants drive `pulsar-perf` against a single broker, and share everything but the client generation
and the topic domain through `AbstractPulsarProfilingTest`:

- `PulsarProfilingTest`, which the task runs by default, drives a v5 scalable (`topic://`) topic with the `produce`
  and `consume` commands.
- `PulsarProfilingV4Test` drives a classic `persistent://` topic with the same commands, for which `pulsar-perf` picks
  the v4 client. The v4 client rejects the `topic://` domain, so the v4 client goes with the classic topic.

The runs aren't like-for-like: scalable topics split their segments under load (`scalableTopicAutoScaleEnabled`
defaults to true), so the v5 run profiles a topology that reshapes itself, while the v4 run's stays fixed. With
[`pulsar-profiling.yaml`](../scenarios/pulsar-profiling.yaml)'s defaults, a run sends 20 million messages, a bit over
a minute of load, and has to finish within three minutes, with both `pulsar-perf` commands exiting with zero, so that
a run that stalls or dies fails the test rather than passing as a finished profile.

Both variants write their recordings and command output under `tests/integration/build/pulsar-profiling`. The v4
run's `pulsar-perf` output, latency histograms, topic stats and metrics scrapes are suffixed `-v4`; the recordings
carry the container name, which embeds the test class name. The broker's profiler options come from
`-Pinttest.asyncprofiler.opts`. The runner doesn't render flame graphs;
[Flame graphs of other recordings](analyzing-profiles.md#flame-graphs-of-other-recordings) describes rendering them
from its recordings.

## Scenario files

The harness accepts a YAML scenario file through `PULSAR_PROFILING_CONFIG`. Start with
[`pulsar-profiling.yaml`](../scenarios/pulsar-profiling.yaml); omitted values keep the existing defaults. The
sections correspond to the main components of a run: `cluster`, `load`, `profiling` and `output`. Individual scalar
values can be overridden for a one-off run with the `PULSAR_PROFILING_` prefix and an upper-case path, for example:

```bash
PULSAR_PROFILING_CONFIG="$PWD/tests/performance/scenarios/pulsar-profiling.yaml" \
PULSAR_PROFILING_LOAD_NUMBER_OF_MESSAGES=1000000 \
./gradlew :tests:integration:profilingIntegrationTest --tests "*PulsarProfilingV4Test"
```

Use an absolute path in `PULSAR_PROFILING_CONFIG`, since Gradle runs the test in the integration module's directory.
The harness saves `resolved-config.yaml`, with inheritance and environment overrides applied, in the output
directory.

## Load settings

- For the v4 scenario, set `load.isolatedProducers` or `load.isolatedConsumers` to create that many independent v4
  client instances. The corresponding `pulsar-perf` command receives `--isolated-clients`, a v4-client option; the v5
  scenario ignores these fields. The option is mutually exclusive with the regular producer test-thread option and
  with consumer listener-thread expansion.
- Set `load.producerCount` and `load.consumerCount` separately: creating clients doesn't create producers or
  consumers. For v4 production, `--num-producers` remains the producer count per topic and is distributed across the
  isolated clients; when the counts differ, producers are assigned round-robin as evenly as possible.
- `load.subscriptionType` selects the subscription type; `producerIoThreads` and `consumerIoThreads` size the shared
  client IO pools. `maxOutstanding` is per producer, not a global limit.
- Set `load.batchingEnabled: true` to use pulsar-perf's default producer batching; the default is `false`, which
  keeps unbatched entry-by-entry measurements.
- `load.messageKeyGenerationMode` maps to pulsar-perf's `--message-key-generation-mode`: `random` uses random
  integer keys, `autoIncrement` uses the sender's message counter, and an empty or null value omits keys. Override it
  with `PULSAR_PROFILING_LOAD_MESSAGE_KEY_GENERATION_MODE`.

[`key-shared-500x20.yaml`](../scenarios/key-shared-500x20.yaml) runs a single Key_Shared subscription with 500
producers and 20 consumers. It disables batching so that every entry has one key, and uses isolated clients with
shared resources on both sides. All consumers use the same subscription; `receiverQueueSize` is per consumer.

## Client profiling

Client profiling is optional and independent of broker profiling. Set `profiling.producerOptions` and/or
`profiling.consumerOptions` to async-profiler options, for example
`event=cpu,interval=10ms,lock=0,alloc=2m,jfrsync=profile`. An empty or null option disables that client's profiler.
Client recordings are named `client-producer-*.jfr` and `client-consumer-*.jfr` in the same output directory. The
harness grants native CPU profiling access only to enabled clients. To inspect lock contention without wall-clock
sampling overhead, omit `wall` and use `lock=0` to record all supported lock events. Use the same options for the
baseline and the candidate; extra profiling has a measurement cost.

## Broker settings

Broker-side variations need no dedicated environment switches: use `cluster.brokerEnvs` for write-buffer watermarks,
dispatcher batch size, broker and BookKeeper IO thread counts and `PULSAR_GC`, and `cluster.brokerMemory` for heap
and direct-memory limits and JVM properties such as allocator or transport selection. These maps let YAML scenarios
keep the complete configuration instead of relying on shell history.

## Scenarios

[Read-completion queue isolation](../scenarios/docs/read-completion-isolation.md) runs on this runner: 500
producers on separate connections to one persistent topic, with one Exclusive consumer. It describes the scenario's
variations and how to compare a baseline and a candidate.
