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

# Performance scenarios

A scenario is a YAML file that describes a performance test: the Pulsar cluster, the workload that runs against it
and, optionally, what to profile. Scenarios build on each other with `extends`, so that a variation states only what
it changes. Run a scenario from the repository root with the launcher:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'
```

Use the `profile` task instead of `run` for a scenario with profiling options. [The performance testing
guide](../README.md) walks through running a scenario and reading its report.

## Scenarios for the standalone launcher

These scenarios run the IoT telemetry workload: keyed telemetry messages from gateways to applications, with delivery
and ordering checks; the applications use Key_Shared subscriptions, and the alert scenarios Shared ones.
[The IoT telemetry scenario](docs/iot-telemetry.md) describes the workload in detail.

| Scenario | What it runs | Memory configuration |
|---|---|---|
| [`iot-telemetry.yaml`](iot-telemetry.yaml) | **Start here.** The full topology: 100 gateways, 30 topics and 20 applications with 100 pods each, 1,000 messages per second for 120 s after a 20 s warmup | medium, about 11 GB |
| [`iot-telemetry-base.yaml`](iot-telemetry-base.yaml) | The defaults that the other IoT scenarios extend: a workload of one gateway and one application with one pod, and the medium-memory configuration | medium, about 11 GB |
| [`iot-telemetry-small.yaml`](iot-telemetry-small.yaml) | A smaller topology: 10 gateways, 30 topics and 20 applications with 10 pods each, 1,000 messages per second for 120 s after a 20 s warmup | low, about 3 GB |
| [`iot-telemetry-small-restarts.yaml`](iot-telemetry-small-restarts.yaml) | The smaller topology, restarting 10 % of each application's clients every 30 s | low, about 3 GB |
| [`iot-telemetry-restarts.yaml`](iot-telemetry-restarts.yaml) | The full topology, restarting 10 % of each application's clients every 30 s | medium, about 11 GB |
| [`iot-telemetry-high-rate.yaml`](iot-telemetry-high-rate.yaml) | A high rate: five million messages at 30,000 messages per second, from 500 preconnected producers to one topic, consumed by 5 applications with 10 pods each | high, about 14 GB |
| [`iot-telemetry-catch-up.yaml`](iot-telemetry-catch-up.yaml) | Consumers joining a live stream: the high-rate scenario's 30,000 messages per second for 180 s, with 4 of its 5 applications joining 20, 22, 24 and 60 s after the measurement starts and reading their backlogs | high, about 14 GB |
| [`iot-telemetry-offload.yaml`](iot-telemetry-offload.yaml) | Catch-up reads from tiered storage: 100,000 1 KiB messages at 10,000 messages per second to one topic, offloaded to SeaweedFS as each ledger rolls over, and six applications that join afterwards and read their backlog from positions 10,240 messages apart, after an unload of the topic; see [Offloading](../docs/offloading.md) | medium, about 11 GB |
| [`iot-telemetry-offload-seek.yaml`](iot-telemetry-offload-seek.yaml) | Seeks by timestamp in offloaded ledgers: 110,000 1 KiB messages 1 ms apart to one topic, offloaded to SeaweedFS, and six applications whose subscriptions are reset to publish times spread over the offloaded ledgers, with the storage's reads counted; see [Offloading](../docs/offloading.md) | medium, about 11 GB |
| [`iot-telemetry-offload-random-seeks.yaml`](iot-telemetry-offload-random-seeks.yaml) | Random reads of offloaded ledgers by time: after 110,000 1 KiB messages 1 ms apart to one topic, offloaded to SeaweedFS, 500 seeks to random publish times, each reading the message there, with the storage's reads counted; see [Offloading](../docs/offloading.md#random-seeks) | medium, about 11 GB |
| [`iot-telemetry-max-rate.yaml`](iot-telemetry-max-rate.yaml) | The maximum rate: the 500 gateways of the high-rate scenario publish four million unbatched messages to one topic without a rate limit, consumed by one application with 20 pods, on single-copy ledgers | high, about 14 GB |
| [`iot-telemetry-failover-processors.yaml`](iot-telemetry-failover-processors.yaml) | Hot-standby stream processors on partitioned topics: 20 gateways publish 20,000 messages per second to 4 topics of 12 partitions, consumed by 8 applications, each a Failover subscription with 3 pods, restarting one pod of each every 20 s | medium, about 11 GB |
| [`iot-telemetry-partitioned.yaml`](iot-telemetry-partitioned.yaml) | Fleet telemetry on partitioned topics: 50 gateways publish 5,000 messages per second to 3 topics of 16 partitions, consumed by 10 applications with 20 pods each, each pod a multi-topics consumer of the 48 partitions | high, about 14 GB |
| [`iot-telemetry-partitioned-max-rate.yaml`](iot-telemetry-partitioned-max-rate.yaml) | The capacity of partitioned-topic consumers: the 500 gateways of the high-rate scenario publish six million messages to one topic of 16 partitions without a rate limit, consumed by 2 applications with 2 pods each | high, about 14 GB |
| [`iot-device-shadow.yaml`](iot-device-shadow.yaml) | Device shadows on compacted topics: 10 gateways publish the 512-byte states of 300,000 devices at 10,000 messages per second to 4 topics that the broker compacts every 4 MB, read by 2 applications with 3 pods each, each pod reading every topic compacted from the start and one pod of each restarted every 30 s | medium, about 11 GB |
| [`iot-alarm-retries.yaml`](iot-alarm-retries.yaml) | Alarm processing with retries and a dead letter topic: 10 gateways publish 5,000 alarms per second to 4 topics, processed by 4 applications, each a Shared subscription with 4 workers, which fail 5 % of the alarms twice and retry them through a retry topic, and dead-letter the 0.1 % that fail every attempt | medium, about 11 GB |
| [`iot-scheduled-commands.yaml`](iot-scheduled-commands.yaml) | Scheduled device commands with delayed delivery: 10 gateways publish 5,000 commands per second to 4 topics, each delivered 1 to 5 minutes later to 4 applications, each a Shared subscription with 4 pods: up to about 1.5 million delayed messages per subscription | medium, about 11 GB |
| [`iot-ota-firmware.yaml`](iot-ota-firmware.yaml) | OTA firmware distribution with chunked messages: 4 gateways publish 40 images of 1 MiB per second, in 128 KiB chunks, to 2 topics, consumed by 3 applications, each a Key_Shared subscription with 2 pods, restarting one of each every 30 s | medium, with larger gateway and application memory, about 13 GB |
| [`iot-fleet-onboarding.yaml`](iot-fleet-onboarding.yaml) | Fleet onboarding with a topic per site: 10 gateways publish 2,000 messages per second without a warmup to 2,000 topics that their first messages create, consumed by 3 applications with 2 pods each that subscribe by a pattern and discover the topics as they appear | high, about 14 GB |
| [`iot-disconnected-sites.yaml`](iot-disconnected-sites.yaml) | Disconnected sites with a message TTL of 60 s and a backlog quota of 20 MB with consumer_backlog_eviction: 10 gateways publish 5,000 messages per second to 4 topics, read by 4 sites with 2 Key_Shared pods each that go offline for 2 of every 3 minutes and keep each device's latest state | medium, about 11 GB |
| [`iot-alerts-fanout.yaml`](iot-alerts-fanout.yaml) | Alert fan-out with Shared subscriptions and broker deduplication: 20 gateways publish 5,000 alerts per second to one topic, consumed by 50 applications, each a Shared subscription with 4 worker pods: 250,000 messages per second of dispatch | high, about 14 GB |
| [`iot-alerts-fanout-max-rate.yaml`](iot-alerts-fanout-max-rate.yaml) | The alert fan-out at the maximum rate: two million unbatched alerts as fast as the cluster takes them | high, about 14 GB |
| [`iot-alerts-sites.yaml`](iot-alerts-sites.yaml) | The alert fan-out across 1,000 per-site topics: 5 gateways, 10 applications with 4 pods each, 5,000 producers, 10,000 Shared subscriptions and 40,000 consumers at 5,000 alerts per second in all | high, with a 4 GB application heap, about 17 GB |

## Configurations

The [`configs`](configs) directory has the configurations that a scenario combines with its workload: the cluster, the
memory, and profiling. They aren't scenarios of their own. A scenario extends a memory configuration, and profiling is
added on the command line with the launcher's `--extends` option.

### Memory configurations

A memory configuration sets the cluster, from [`cluster-base.yaml`](configs/cluster-base.yaml) with the settings that
every configuration shares, and the heap and direct memory of every container's JVM, through `PULSAR_MEM` in each
component's `env`. The memory it needs is what the host has to have available to Docker, which on macOS and Windows
is the memory of Docker's virtual machine:

| Configuration | Cluster | Recommended memory available to Docker |
|---|---|---|
| [`iot-telemetry-low-mem.yaml`](configs/iot-telemetry-low-mem.yaml) | [`cluster-low-mem.yaml`](configs/cluster-low-mem.yaml): one broker and one bookie, which holds every ledger with an ensemble, write quorum and ack quorum of 1, and heaps that the JVMs commit as they use them | about 3 GB. For checking that a scenario works on a small host; not meant for profiling. The applications' heap is sized for `iot-telemetry-small.yaml`, and the full topology runs out of it |
| [`iot-telemetry-medium-mem.yaml`](configs/iot-telemetry-medium-mem.yaml) | [`cluster-medium-mem.yaml`](configs/cluster-medium-mem.yaml): one broker and two bookies | about 11 GB. The default, which `iot-telemetry-base.yaml` extends |
| [`iot-telemetry-high-mem.yaml`](configs/iot-telemetry-high-mem.yaml) | [`cluster-high-mem.yaml`](configs/cluster-high-mem.yaml): one broker and three bookies, with a larger heap and direct memory for each | about 14 GB. For the high-rate scenarios |

A scenario that offloads, or prepares its late applications' join, also runs a Pulsar proxy for the launcher's admin
requests, which needs about 350 MB more; see [the scenario format](docs/scenario-format.md).

A scenario that needs another memory configuration than the default extends it after the scenario it builds on, as
[`iot-telemetry-high-rate.yaml`](iot-telemetry-high-rate.yaml) does: `extends: [iot-telemetry.yaml,
configs/iot-telemetry-high-mem.yaml]`. To run a scenario with another memory configuration without a new file, add it
on the command line:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --extends configs/iot-telemetry-high-mem'
```

### Profiles

These files add profiling to any scenario with the launcher's `--extends` option, and the scenario then runs with the
`profile` task, see [Profiling](../docs/profiling.md). Profile a scenario with the medium- or the high-memory
configuration, not the low-memory one.

| File | What it adds |
|---|---|
| [`profile-broker.yaml`](configs/profile-broker.yaml) | Profiles the broker |
| [`profile-gateways.yaml`](configs/profile-gateways.yaml) | Profiles the gateways, the producer |
| [`profile-applications.yaml`](configs/profile-applications.yaml) | Profiles the applications, the consumers |

```bash
./gradlew :tests:performance:launcher:profile \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/profile-broker'
```

### Heap dumps

[`heap-dumps-broker.yaml`](configs/heap-dumps-broker.yaml) adds heap dumps of the broker to any scenario: at its
highest heap usage, after every application has received every message, and when it runs out of memory. A heap dump
stops the JVM while it is written, so such a run is for finding what holds the memory, not for measuring. [Heap
dumps](../docs/heap-dumps.md) describes the other settings, such as dumps at given times and of the other components.

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --extends configs/heap-dumps-broker'
```

### The bookies' journal on a tmpfs

Every entry that a bookie stores is written to the disk twice: to its journal and to its entry log. All the bookies
share the host's disk, so with large unbatched entries the disk's write bandwidth becomes the limit of a run on many
developer machines, before Pulsar's: the max-rate scenario at 64 KB wrote 890–980 MB/s to a laptop's encrypted NVMe
drive. `host-io.csv` and the run report's host section show when it is: the disk's write MB/s stays flat across runs
while the host's CPU has headroom, as [the agent guide](../AGENTS.md#eliminating-bottlenecks) describes.

[`bookie-journal-tmpfs.yaml`](configs/bookie-journal-tmpfs.yaml) puts each bookie's journal on a tmpfs of 2 GB with
`cluster.bookies.journalTmpfs`, the tmpfs's size, so that only the ledger storage writes to the disk, which halves the
disk's writes. The test cluster doesn't sync the journal anyway (`journalSyncData=false`), so it changes no
durability guarantee of the test, only where the journal's writes go. In the max-rate scenario on that laptop it
halved the disk's writes at 8 KB and 64 KB, and at 64 KB the broker's CPU per message fell 19 %.

A bookie deletes its old journal files when it has flushed its write cache, but keeps the current file until it is
full, so the launcher sets the journal's files to 256 MB without backups (`journalMaxSizeMB` and `journalMaxBackups`,
in place of the scenario's), and each busy bookie's journal takes about 512 MB of memory. The bookies' `env` can't set
`journalDirectory` with it. Compare runs only with the same setting.

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry-max-rate.yaml --extends configs/bookie-journal-tmpfs'
```

The ledger storage still writes every entry to the disk, and syncs its files each time it flushes its write cache.
When those syncs are what's slow, [disabling write barriers](../environment/README.md#disabling-write-barriers) makes
them cheaper on a disk without power loss protection, at the risk of the host's file system.

### TLS

[`tls.yaml`](configs/tls.yaml) makes the gateways and the applications connect to the brokers with TLS, with
`cluster.tls.enabled`, so that a run measures what TLS costs: the handshakes when the clients connect, and the
encryption and decryption of every message, which the brokers' I/O threads (`pulsar-io`) and the clients' do.
Compare a scenario with and without it, on the same image.

The launcher creates a CA for each run and a certificate of the brokers that the CA signs, with the brokers' host and
container names, and writes them into the run directory's `tls` directory, which the brokers and the workloads mount.
The brokers open their TLS port, 6651, beside the plain one, and the workloads' service URL is
`pulsar+ssl://<broker>:6651`. The clients trust the CA and verify the brokers' names, as Pulsar's clients do by
default; they don't authenticate with certificates of their own. Only the workloads' connections use TLS: the proxy,
geo-replication between the clusters, and the launcher's admin requests keep the plain ports, and the brokers' own
connections to the bookies don't use TLS. With [a remote cluster](../docs/network-conditions.md#geo-replication), the
remote applications connect to its brokers with TLS too.

`cluster.tls.keyAlgorithm` sets the keys' algorithm: `RSA`, the default, 2048-bit RSA keys, or `EC`, ECDSA keys on the
P-256 curve, whose handshakes cost the brokers less CPU. The brokers' `env` can add other TLS settings, such as
`tlsProtocols` or `tlsCiphers`, or replace the launcher's.

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/tls'
```

Handshakes cost most when clients connect often: the pods' restarts
(`workloads.iotTelemetry.behaviors.podRestarts`), and network faults that reset connections, such as
[`network/connection-resets`](network/connection-resets.yaml) and
[`network/consumer-reconnect-storm`](network/consumer-reconnect-storm.yaml), make them reconnect during the
measurement.

### JWT authentication

[`jwt.yaml`](configs/jwt.yaml) makes the brokers authenticate every client with a JSON Web Token, with
`cluster.jwt.enabled`, so that a run measures what authentication costs, such as verifying the token of each
connection that a client opens. It works with or without [TLS](#tls): `--extends configs/tls --extends configs/jwt`
sends the tokens over TLS, as most deployments do.

The launcher creates the keys and the tokens for each run, in the formats of `pulsar tokens`, as the Pulsar Helm
chart's JWT secret job does, and writes them into the run directory's `jwt` directory, which the brokers and the
workloads mount. The gateways and the applications authenticate as `iot-workload`. The brokers' own clients, such as
geo-replication's, and the launcher's admin requests authenticate as `admin`, the brokers' super-user. The brokers
authenticate the clients but don't authorize them: any authenticated client may produce and consume. The brokers'
`/metrics` endpoint stays unauthenticated, as it is by default.

`cluster.jwt.signatureAlgorithm` sets how the tokens are signed: `RS256`, the default, with a 2048-bit RSA key pair
whose public key the brokers verify the tokens with, or `HS256`, with a secret key that the brokers share.

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/tls --extends configs/jwt'
```

## Scenarios for the legacy TestNG runner

The scenarios of [the legacy TestNG profiling runner](../docs/legacy-testng-runner/README.md#scenario-files), which runs
`pulsar-perf` and is kept until its scenarios are migrated to the standalone launcher, are in
[its test resources][profiling-scenarios]:
`tests/integration/src/test/resources/org/apache/pulsar/tests/integration/profiling`.

## Writing a scenario

Start from the scenario closest to what you want to measure, and extend it with the settings you change. For
example, a higher rate and larger messages on the smaller topology, in
`tests/performance/scenarios/iot-telemetry-small-5k.yaml`:

```yaml
extends: iot-telemetry-small.yaml
workloads:
  iotTelemetry:
    rate: 5000
    payload:
      size: 1024
```

Run it like any other scenario. Its runs are named after the file, `iot-telemetry-small-5k`, unless it sets
`output.name`. It keeps the memory configuration of the scenario it extends, here the low-memory one; extend another
one after it, from [`configs`](#memory-configurations), when the workload needs more memory. To try a single value
without a new file, set it on the command line:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --set workloads.iotTelemetry.rate=5000'
```

[The scenario format](docs/scenario-format.md) describes the sections, inheritance,
[environment overrides](docs/scenario-format.md#environment-overrides) and the warmup, and
[the IoT telemetry scenario](docs/iot-telemetry.md#scenarios) the workload's settings. Add a scenario that others can
use to this directory and to the tables above, with a guide under [`docs`](docs) when it needs one, and a configuration
that scenarios combine, such as a memory configuration, to [`configs`](configs). `ScenarioFilesTest` in the launcher's
tests resolves every scenario in this directory, and fails when one no longer resolves to a valid cluster, or leaves a
container's memory unset.

[profiling-scenarios]: ../../integration/src/test/resources/org/apache/pulsar/tests/integration/profiling
