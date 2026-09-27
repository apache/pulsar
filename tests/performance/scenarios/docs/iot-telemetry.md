# IoT telemetry scenario

The IoT scenario models small keyed telemetry messages entering Pulsar through interchangeable edge
gateways and fanning out to independent applications:

- 300,000 possible device IDs, 100 gateway clients and 30 persistent topics;
- one stable binary device ID key and a monotonic per-device counter in each 64-byte message;
- a 20-second warmup followed by 1,000 measured messages/second for 120 seconds;
- 20 applications, each using its own Key_Shared subscription across 100 isolated client instances;
- shared PIP-234 client resources within the gateways' process and within each application's; and
- broker-side producer deduplication with stable, unique producer names and explicit producer sequence IDs.

Producer names identify the gateway and topic. Consumer names identify the application and pod slot. These
identities are deterministic and are reused after a simulated pod restart, so deduplication and consumer
diagnostics continue to refer to the same logical endpoint.

Only one send per device is in flight. A later message may use another gateway, but it is submitted only
after the preceding send has been acknowledged. This prevents the load generator from manufacturing
cross-connection ordering failures.

Each application owns one sequence tracker shared by its simulated pods. The ordered path uses a bounded
array indexed by device ID. Sparse pending sets are created only when a gap is observed. Duplicate delivery
is counted and accepted; a first delivery above the expected device sequence is an ordering violation.
The gateways' and the applications' state arrays are persisted at the end, so the launcher also detects missing tail
messages that never expose a gap. The tracker remains alive while clients are restarted, which validates
application-visible order across Key_Shared hash-range reassignment.

## Scenarios

- [`iot-telemetry-base.yaml`](../iot-telemetry-base.yaml) has the defaults that the others extend, with one gateway
  and one application with one pod, and the medium-memory configuration.
- [`iot-telemetry.yaml`](../iot-telemetry.yaml) is the full topology without churn.
- [`iot-telemetry-restarts.yaml`](../iot-telemetry-restarts.yaml) restarts 10% of each application's
  pods every 30 seconds.
- [`iot-telemetry-small.yaml`](../iot-telemetry-small.yaml) keeps the 20-way fanout,
  30 topics and 1,000 msg/s rate, but uses 10 gateways and 10 pods per application, with the low-memory
  configuration.
- [`iot-telemetry-small-restarts.yaml`](../iot-telemetry-small-restarts.yaml) adds restart churn to that smaller
  topology.
- [`iot-telemetry-high-rate.yaml`](../iot-telemetry-high-rate.yaml) sends five million messages at 30,000 messages
  per second through 500 preconnected producers to one topic, with at most 10,000 in flight. Five applications each
  consume with ten pods on one Key_Shared subscription. The rate is one that every application keeps up with on a
  workstation whose CPU runs at a fixed base frequency: without a rate limit, the gateways publish faster than the
  applications receive, and an application that falls behind can stall for tens of seconds. It uses the high-memory
  configuration.

Each scenario runs with a memory configuration from the scenarios' `configs` directory, which sets the cluster and
the memory of every container: the low-memory one needs about 3 GB of memory available to Docker and isn't meant for
profiling, the medium-memory one, the default, about 11 GB, and the high-memory one about 14 GB. See
[Memory configurations](../README.md#memory-configurations).

Build the mountable workload distribution without running a cluster:

```bash
./gradlew :tests:performance:tools:installDist
```

The mount root is `tests/performance/tools/build/install/pulsar-performance-tools`. Run the scenario
through the standalone Testcontainers launcher with:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'
```

The Gradle task builds the server test image and the workload distribution before launching. The launcher
resolves YAML inheritance and `PULSAR_PERFORMANCE_` environment overrides, writes `resolved-config.yaml`
to the run directory, and mounts that resolved file and the application distribution into each container.
The `iot-produce` and `iot-consume` commands accept `--config-path` when a different subtree is desired.
Without warmup, direct tool invocations need only `--config` and `--output`. `iot-consume` runs every application,
each writing into a directory named after its subscription in `--output`. With warmup, pass the same fresh `--run-id`
to the `iot-produce` and the `iot-consume` process. The launcher generates
this correlation ID automatically and saves it in `run-id.txt`. Barrier markers include the ID so markers left
by an earlier run cannot release a new run's barrier.

`--coordination-directory` is optional and defaults to `<output>/coordination`. If the gateways' and the applications'
outputs are in different directories, pass a common shared coordination directory explicitly. For example, generate
`RUN_ID=$(uuidgen)` once and use `--run-id "$RUN_ID" --coordination-directory /tmp/iot-coordination` for every tool
process in that run. Reusing the directory is fine; use a new run ID for each invocation of the workload.

Set `gateways.producer.batchingEnabled` in the workload section to compare batched and unbatched keyed messages without
changing the tool implementation. Batched runs use `BatcherBuilder.KEY_BASED`, which keeps each batch to
one key as required for Key_Shared delivery.

Warmup messages exercise the same producer, client, connection, topic and consumer paths as measured messages. They
remain in the monotonic device sequences and end-to-end delivery checks, but are excluded from throughput. For a
rate-limited workload, set `warmup.seconds`; for an unrestricted workload, set `warmup.messages`. Do not set both. The
value applies to each of `warmup.rounds`. Every round drains its asynchronous sends and waits until every backend
application has uniquely received the cumulative warmup count before `warmup.roundDelaySeconds` begins. The delay after
the final round gives background JIT compilation and other startup work time to settle before the gateways record the
measurement boundary. This is a stabilization control, not a guarantee that the JVM has completed compilation.
`gateways-summary.json` records the warmup and measurement counts and the epoch-millisecond boundaries of the
measurement. Each `application-summary.json` records the first and last measured-message receipt as metadata. The
launcher uses the gateways' measurement start and the latest last receipt across all backend applications as the JFR
measurement interval.

The cluster, [`configs/cluster-base.yaml`](../configs/cluster-base.yaml), which every memory configuration extends,
also keeps incidental storage maintenance outside normal measurement windows. Its managed-ledger
entry, size and time limits allow the topic and cursor ledgers to remain open throughout ordinary runs. BookKeeper
ledger garbage collection waits for one day, entry-log compaction is disabled, and the journal size limit is raised.
The test containers are ephemeral, so delayed reclamation cannot accumulate between runs. These settings isolate
the broker messaging path; they are benchmark controls rather than production sizing recommendations. Use a
separate scenario with normal or deliberately short limits when measuring rollover, recovery, deletion, compaction,
or long-running storage behavior. BookKeeper entry-log flushing and disk-space checks remain enabled.

Set `rate: 0` together with a positive `measurement.messages` to remove producer pacing. Set
`gateways.producer.precreate: true` to open every gateway/topic producer before throughput timing begins. The gateways'
summary, `gateways-summary.json`, reports `messagesPerSecond` only for the post-warmup measurement phase and retains
`wholeRunMessagesPerSecond` as startup and warmup context.

## Settings

The workload's settings are under `workloads.iotTelemetry`. [`iot-telemetry-base.yaml`](../iot-telemetry-base.yaml)
has the defaults, with one gateway and one application with one pod, [`iot-telemetry.yaml`](../iot-telemetry.yaml)
sets the full topology's counts, and the memory configuration, here the default medium-memory one, sets the
containers' `env`:

```yaml
workloads:
  iotTelemetry:
    warmup:                    # traffic before the measurement, see above
      seconds: 20
      messages: 0
      rounds: 1
      roundDelaySeconds: 0
    measurement:               # seconds at the rate, unless messages sets the measured messages
      seconds: 120
      messages: 0
    timeoutSeconds: 240        # the longest the workload may run, at least the warmup and the measurement
    rate: 1000                 # messages per second; 0 sends as fast as possible
    payload:
      size: 64                 # bytes, the device's ID, sequence and send time included
    devices:
      count: 300000
    gateways:
      count: 100
      producer:                # the gateways' Pulsar clients and producers
        ioThreads: 8
        listenerThreads: 16
        maxOutstanding: 20000  # messages in flight across the gateways
        batchingEnabled: true
        precreate: false       # open every producer before the first message
      env:                     # the gateways' container, which runs every gateway; from the memory configuration
        PULSAR_MEM: -Xms512m -Xmx512m -XX:MaxDirectMemorySize=256m -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch
    topics:
      count: 30
      prefix: persistent://public/default/iot-telemetry-
    applications:              # each a Key_Shared subscription, consumed through its pods
      count: 20
      podsPerApplication: 100  # a Pulsar client with a consumer of the subscription each
      subscriptionPrefix: iot-application-
      client:                  # the client resources that every pod's client shares
        ioThreads: 8
        listenerThreads: 16
      env:                     # the applications' container, which runs every application; from the memory configuration
        PULSAR_MEM: -Xms1536m -Xmx1536m -XX:MaxDirectMemorySize=256m -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch
    behaviors:
      podRestarts:             # each application restarts this fraction of its pods every intervalSeconds
        intervalSeconds: 0
        fraction: 0.0
```

`timeoutSeconds` bounds the whole workload: the applications stop waiting for messages after it and the run fails,
the gateways give up waiting for a warmup round or the measurement's start, and the launcher waits for the containers
a minute longer. It has to be at least the time that the warmup rounds, their delays and the measurement need at the
rate, and should leave room for startup and for the applications to catch up. The launcher checks the workload's
settings before it starts a cluster, and rejects a shorter timeout with the time the workload needs. The launcher sets
the broker's service URL itself.

## Profiling with jonoffcpu

Use the `profile` task for a scenario that profiles a component: `profiling.broker`, `gateways` or `applications`
with `asyncProfilerOptions`. The launcher's `--extends` option adds them to a scenario from the `profile-*` files in
the scenarios' `configs` directory, here the broker's and the gateways' to the high-rate workload:

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/profile-broker --extends configs/profile-gateways'
```

The options are async-profiler options. The [jonoffcpu](https://github.com/jonoffcpu/jonoffcpu) agent runs
async-profiler with them, JDK Flight Recorder alongside with `jfrsync`, and its kernel-measured off-CPU recording, all
at the same time. [Profiling](../../docs/profiling.md) describes the requirements
and the files each recording produces, and [Analyzing profiles](../../docs/analyzing-profiles.md) how to find what to
optimize.

Broker recordings are written under `broker-profile/`, the gateways' under `gateways/` and the applications' under
`applications/`. The launcher owns each recording path so recordings remain inside the run
directory, and rejects options that set `file=`. A component without options isn't profiled. The ordinary `run` task
rejects profiling-enabled YAML rather than silently running without the agent.

The profile files sample CPU every 10 ms and allocations every 2 MB in their component, record the
JVM's own events with JFR's `profile` configuration (`jfrsync=profile`, see
[Configuring profiling](../../docs/profiling.md#configuring-profiling)), and record only intervals where a thread
blocked (`reasons: [blocked]`), not those where it was runnable but waiting for a CPU. It ignores waits under 100 µs
(`minOffCpuMicros: 100`) and records every wait of 10 ms or longer, sampling shorter ones in proportion to their
length (`admission: {policy: proportional, recordAllAboveMicros: 10000}`), which bounds the recording rate by off-CPU
time rather than by context-switch count: a broker run records about 400,000 intervals. For this workload, start with
the run report, `index.html`, the broker's profile report, the off-CPU digest it links to and `cpu-threads.html`: the
five-million-message run sends everything through one topic, so the topic's managed-ledger thread
(`BookKeeperClientWorker-OrderedExecutor-*`) is the serial stage to watch.

After every profiled process exits, the launcher writes a sibling `.measurement.jfr` spanning the gateways'
measurement start through the latest measured-message receipt across all backend applications. The upper boundary
includes the full millisecond containing that receipt. This removes startup, warmup, and shutdown while retaining
the broker and application work needed to deliver every measured message. The complete recording is kept beside it. The
cut recording also retains the one-time JVM, host, recording setting and runtime configuration events needed to
describe the source JVM in JDK Mission Control.

The JFR measurement window and broker-publish-to-listener latency assume synchronized gateway, application and
broker clocks. Containers on one Docker host share its clock. When adapting the tools to multiple hosts,
synchronize their clocks; no clock-skew correction is applied.

The gateways write `gateways-latency.hdr` containing send-to-completion latency for measured messages. Each backend
application writes `application-latency.hdr` containing broker-publish-to-listener latency for measured messages. Warmup
messages are excluded from both histograms. Each application captures its receipt timestamp on listener entry and
records the sample after payload decoding and key validation, before sequence validation and acknowledgment.
Decoding and validation time do not contribute to the latency value. Use the report tool's `renderHdrHistograms`
Gradle task to plot the publish latency and each application's end-to-end latency by percentile and over time
as SVG and PNG; see [Latency logs](../../docs/run-reports.md#latency-logs) for the command.

## Interpreting a run

A successful run sends the configured message count and reports the same number of unique messages for every
application. It must report zero invalid messages and zero ordering violations. Duplicate deliveries are valid
for Pulsar's at-least-once delivery model and are counted separately so comparisons can detect changes in their
frequency. The persisted gateway and application state checks for missing tail messages after all clients stop.

Key_Shared scenarios use the key-based producer batcher. It keeps every batch to one key so the broker can
route all messages for a device through the same Key_Shared hash range. Changing the batcher changes the
ordering contract exercised by the scenario and should not be mixed into a performance comparison.

The full topology opens 100 gateway clients and 2,000 application clients, all in the applications' container. Across
30 topics and 20 applications, this creates 60,000 internal topic consumers. The medium-memory configuration gives the
broker a 4 GiB heap and 1 GiB of direct memory to accommodate that topology: the consumers' Key_Shared hash rings hold
about 2 GB of live objects, and with a 2 GiB heap the broker spends its CPU in garbage collection. Keep the memory configuration and the
resolved scenario configuration constant when comparing broker revisions.
