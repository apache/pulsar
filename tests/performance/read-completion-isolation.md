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

# Read-completion profiling scenario

Use this scenario to measure whether consumer dispatch keeps up when many producers publish to
one persistent topic. Run the same workload and profiler settings on the base revision and the
candidate revision. See [README.md](README.md) for the profiling harness and analysis tools.

## Workload

[read-completion-isolation.yaml](scenarios/read-completion-isolation.yaml) runs the v4 `pulsar-perf` commands
with 500 producers, 500 isolated clients sharing PIP-234 resources, one connection per client,
and one Exclusive consumer on a non-partitioned `persistent://` topic. It sends 12 million
unbatched 128-byte messages at unrestricted rate. Each producer permits 40 outstanding sends,
for a total limit of 20,000. The harness creates the subscription before publishing.

Inherited variations change one aspect of this workload:

- [Shared subscription](scenarios/read-completion-isolation-shared.yaml)
- [Failover subscription](scenarios/read-completion-isolation-failover.yaml)
- [64/32 KiB channel high/low watermarks](scenarios/read-completion-isolation-64k-32k.yaml)
- [256/128 KiB channel high/low watermarks](scenarios/read-completion-isolation-256k-128k.yaml)

Keep inherited YAML files together: parent paths resolve relative to the file declaring them.
Always use the same variation for the baseline and candidate. A result for one subscription type
does not establish an improvement for another.

## Run a comparison

Use separate checkouts for the base and candidate revisions. Both must contain the same profiling
harness and scenario files. If the candidate changes the harness, apply just its harness changes
to the baseline before measuring. For example, from a candidate checkout based on `origin/master`:

```bash
git worktree add --detach ../pulsar-read-baseline origin/master
git diff origin/master...HEAD -- tests/integration tests/performance > /tmp/read-completion-harness.patch
git -C ../pulsar-read-baseline apply /tmp/read-completion-harness.patch
```

Run this command from each checkout's root. Replace `baseline` with `candidate` for the candidate
run and use a fresh output directory for every repetition. Use an absolute YAML path because
Gradle runs the test from the integration module directory.

```bash
PULSAR_PROFILING_CONFIG="$PWD/tests/performance/scenarios/read-completion-isolation.yaml" \
PULSAR_PROFILING_OUTPUT_DIRECTORY="$PWD/tests/integration/build/pulsar-profiling/baseline" \
./gradlew :tests:integration:profilingIntegrationTest --tests '*PulsarProfilingV4Test' \
  '-Pinttest.asyncprofiler.opts=event=cpu,interval=10ms,lock=0,alloc=2m,jfrsync=profile'
```

Build and run the checkouts sequentially because they share Docker image tags. Keep host power
settings, memory limits and profiler options identical. Avoid other builds or benchmarks during
measurement; record thermal throttling and available memory. Save the source revision and any local
diff alongside the output. The harness writes the effective configuration to `resolved-config.yaml`.

To select a variation, change `PULSAR_PROFILING_CONFIG` to its YAML file. For a rate-limited run,
add `PULSAR_PROFILING_LOAD_PRODUCE_RATE=100000`, which gives 200 messages/s per producer client.
The message count must be divisible by the number of isolated producer clients.

## Compare results

**Steady dispatch** measures broker delivery while all 500 producers are active. Omit the first
20 seconds after all producers connect, then use only complete snapshot intervals with 500
publishers at both endpoints. The window ends before producers start disconnecting. Divide the
sum of `msgOutCounter` increases by the total interval duration. Calculate **steady ingress**
the same way from `msgInCounter`. These measure broker activity, not client acknowledgement.
The cached `msgRateIn` and `msgRateOut` values can lag workload transitions.

**Whole-run throughput** is each `pulsar-perf` client's reported aggregate rate over its own timer.
Consumer throughput includes ramp-up and backlog drain; the producer timer is separate. A high
whole-run consumer rate can hide slow dispatch during publishing if the backlog drains quickly
after producers stop. Compare steady rates and backlog as well as aggregate completion speed.

**Sampled maximum backlog** is the largest `subscriptions.sub.msgBacklog` across all collected
`stats-v4.*.txt` snapshots. It is a sampled maximum, not an exact peak between snapshots. Report
the sampling interval alongside it.

Before comparing rates:

- Verify 500 publishers with distinct connection addresses and one consumer of the intended
  subscription type in the topic stats.
- Check final client counts and failed ACKs. The consumer must receive 12 million messages;
  completed-send accounting can let the producer exceed its target by sends already in flight.
- Reject runs with a timeout, broker restart, OOM or failed persistence, even if clients reconnect.
- Report percentage changes alongside absolute values and distinguish lower-is-better metrics
  such as backlog and CPU from higher-is-better throughput.

Analyze the broker recording with Jafar MCP `jfr_diagnose` and `jfr_stackprofile`, saving output
beside the recording as `<filename>.jfr.analysis.md`. Compare CPU, monitor contention and thread-park
views within the steady window. Executor queue delay need not appear as monitor contention.
The command above omits wall-clock sampling to avoid its overhead during lock profiling.
Compare allocations and actual GC pauses as well; concurrent GC duration is not pause time.

For a focused comparison of completion handoff overhead, run the JMH benchmark separately from the
integration test:

```bash
./gradlew :microbench:shadowJar
java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  '.*ReadCompletionHandoffBenchmark.*' -prof gc -rf json -rff read-handoff.json
```

The microbenchmark uses real executors without storage IO or publishing pressure. Its latency and
allocation results do not predict end-to-end throughput or establish concurrency correctness.
