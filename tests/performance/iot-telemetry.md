# IoT telemetry scenario

The IoT scenario models small keyed telemetry messages entering Pulsar through interchangeable edge
gateways and fanning out to independent applications:

- 300,000 possible device IDs, 100 gateway clients and 30 persistent topics;
- one stable binary device ID key and a monotonic per-device counter in each 64-byte message;
- a 20-second warmup followed by 1,000 measured messages/second for 120 seconds;
- 20 applications, each using its own Key_Shared subscription across 100 isolated client instances;
- shared PIP-234 client resources within each producer or application process; and
- broker-side producer deduplication with stable, unique producer names and explicit producer sequence IDs.

Producer names identify the gateway and topic. Consumer names identify the application and pod slot. These
identities are deterministic and are reused after a simulated client restart, so deduplication and consumer
diagnostics continue to refer to the same logical endpoint.

Only one send per device is in flight. A later message may use another gateway, but it is submitted only
after the preceding send has been acknowledged. This prevents the load generator from manufacturing
cross-connection ordering failures.

Each application owns one sequence tracker shared by its simulated pods. The ordered path uses a bounded
array indexed by device ID. Sparse pending sets are created only when a gap is observed. Duplicate delivery
is counted and accepted; a first delivery above the expected device sequence is an ordering violation.
Producer and consumer state arrays are persisted at the end, so the launcher also detects missing tail
messages that never expose a gap. The tracker remains alive while clients are restarted, which validates
application-visible order across Key_Shared hash-range reassignment.

## Scenarios

- [`iot-telemetry.yaml`](scenarios/iot-telemetry.yaml) is the full topology without churn.
- [`iot-telemetry-restarts.yaml`](scenarios/iot-telemetry-restarts.yaml) restarts 10% of each application's
  clients every 30 seconds.
- [`iot-telemetry-local-steady.yaml`](scenarios/iot-telemetry-local-steady.yaml) keeps the 20-way fanout,
  30 topics and 1,000 msg/s rate, but uses 10 gateways and 10 clients per application.
- [`iot-telemetry-local.yaml`](scenarios/iot-telemetry-local.yaml) adds restart churn to that host-sized
  topology.
- [`iot-telemetry-high-rate.yaml`](scenarios/iot-telemetry-high-rate.yaml) removes the producer rate limit
  and sends five million messages through 500 preconnected producers to one topic. Five applications each
  consume with ten isolated clients on one Key_Shared subscription.
- [`iot-telemetry-high-rate-profile.yaml`](scenarios/iot-telemetry-high-rate-profile.yaml) enables broker and
  producer async-profiler recordings for the same saturation workload.

Build the mountable workload distribution without running a cluster:

```bash
./gradlew :tests:performance:tools:installDist
```

The mount root is `tests/performance/tools/build/install/pulsar-performance-tools`. Run the local scenario
through the standalone Testcontainers launcher with:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml'
```

The Gradle task builds the server test image and the workload distribution before launching. The launcher
resolves YAML inheritance and `PULSAR_PERFORMANCE_` environment overrides, writes `resolved-config.yaml`
to the run directory, and mounts that resolved file and the application distribution into each container.
The `iot-produce` and `iot-consume` commands accept `--config-path` when a different subtree is desired.
Set `batchingEnabled` in the workload section to compare batched and unbatched keyed messages without
changing the tool implementation. Batched runs use `BatcherBuilder.KEY_BASED`, which keeps each batch to
one key as required for Key_Shared delivery.

Warmup messages exercise the same producer, client, connection, topic and consumer paths as measured messages. They
remain in the monotonic device sequences and end-to-end delivery checks, but are excluded from throughput. For a
rate-limited workload, set `warmupSeconds`; for an unrestricted workload, set `warmupMessages`. Do not set both.
`producer-summary.json` records the warmup and measurement counts and epoch-millisecond measurement boundaries so
the same window can be selected from broker and client JFRs.

Set `rate: 0` together with a positive `numberOfMessages` to remove producer pacing. Set
`precreateProducers: true` to open every gateway/topic producer before throughput timing begins. The producer
summary reports `messagesPerSecond` only for the post-warmup measurement phase and retains
`wholeRunMessagesPerSecond` as startup and warmup context.

## Async-profiler

Use the `profile` task for a scenario that has non-empty `profiling.brokerOptions`, `producerOptions` or
`consumerOptions`:

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--config tests/performance/scenarios/iot-telemetry-high-rate-profile.yaml'
```

The task builds the test image containing async-profiler, tunes Linux perf-event settings using the existing
integration-test task, and grants profiled containers the required capabilities. Broker recordings are written
under `broker-profile/`; producer and consumer recordings are written in their corresponding output directories.
The launcher owns each `file=` option so recordings remain inside the run directory. Empty options leave that
component unprofiled. The ordinary `run` task rejects profiling-enabled YAML rather than silently running with
an image that lacks the native agent.

## Interpreting a run

A successful run sends the configured message count and reports the same number of unique messages for every
application. It must report zero invalid messages and zero ordering violations. Duplicate deliveries are valid
for Pulsar's at-least-once delivery model and are counted separately so comparisons can detect changes in their
frequency. The persisted producer and consumer state checks for missing tail messages after all clients stop.

Key_Shared scenarios use the key-based producer batcher. It keeps every batch to one key so the broker can
route all messages for a device through the same Key_Shared hash range. Changing the batcher changes the
ordering contract exercised by the scenario and should not be mixed into a performance comparison.

The full topology opens 100 gateway clients and 2,000 isolated application clients. Across 30 topics and 20
applications, this creates 60,000 internal topic consumers. Scenario files specify a 2 GiB broker heap and
direct-memory limit to accommodate that topology. Keep those limits and the resolved scenario configuration
constant when comparing broker revisions.
