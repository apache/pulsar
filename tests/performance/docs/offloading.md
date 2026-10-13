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

# Offloading to tiered storage

A scenario's `offload` section makes the brokers offload the topics' ledgers to tiered storage, so that a run measures
reads from it: subscriptions that fall behind read their backlog from the tiered storage once the brokers have
offloaded it and deleted it from BookKeeper. The storage is [SeaweedFS](https://github.com/seaweedfs/seaweedfs), an
S3-compatible object store, which the launcher runs in a container on the cluster's network, and the brokers use
Pulsar's S3 offloader. [`iot-telemetry-offload.yaml`](../scenarios/iot-telemetry-offload.yaml) is the scenario that
does it.

## The offload section

```yaml
offload:
  image: chrislusf/seaweedfs:4.47   # the SeaweedFS image, the default
  namespace: public/default         # the namespace whose offload policies the launcher sets, the default
  thresholdBytes: 0                 # how many bytes of a topic stay in BookKeeper, 0 by default
  thresholdSeconds: 0               # optional: the offload threshold in seconds, unset by default
  deletionLagMillis: 0              # how long an offloaded ledger stays in BookKeeper, 0 by default
```

With it, the launcher:

- starts SeaweedFS's single-process server (`weed mini`) with the bucket `pulsar` before the cluster, as
  `offload-storage` on the cluster's network, and stops it after the cluster;
- gives the brokers the S3 offloader's settings: `managedLedgerOffloadDriver=aws-s3`, the bucket, the endpoint
  `http://offload-storage:8333` and the credentials of the storage's one identity, with which SeaweedFS verifies the
  offloader's signed uploads. A setting in `cluster.brokers.env` overrides them;
- sets the namespace's offload policies once the cluster has started: the offload threshold, which with 0 offloads a
  ledger as soon as it rolls over, the threshold in seconds when the section sets it, and the deletion lag, which with 0
  deletes the ledger from BookKeeper once it is offloaded;
- records the topics' ledgers, and how many of them the brokers have offloaded, in `offload-status.csv`, when the late
  applications join and at the run's end, which the report's Offloading section sums.

The brokers offload a ledger only once it has rolled over, so a scenario sets the rollover in `cluster.brokers.env`:
`managedLedgerMaxEntriesPerLedger`, and `managedLedgerMinLedgerRolloverTimeMinutes: "0"`, without which a ledger
lives at least 10 minutes.

The storage is a network endpoint, `offload-storage`, so that the network impairments can put latency or a bandwidth
limit between it and the brokers, as an object store in another zone or region has:

```yaml
network:
  impairments:
    offload-storage-latency:
      between: [brokers, offload-storage]
      delay: 10ms
```

## Staggered positions and an unload before the join

Reading a backlog from tiered storage needs subscriptions that fall behind. In the IoT telemetry workload, an
application that joins after the measurement starts (`applications.joinSeconds`) has its subscription from the start,
so it keeps the backlog until it joins. Two settings of the workload's applications prepare that join; the launcher
carries them out at the earliest late application's join time, and the late applications wait for it before they open
their pods:

- `applications.skipMessages`: how many messages of each topic each late application skips, a value for each
  application. The launcher moves each subscription forward by that many entries with the admin API's skip, so that
  the applications read the backlog from staggered positions, in different ledgers. The entries are the messages
  because skipping needs `gateways.producer.batchingEnabled: false`. An application that skips messages expects every
  message but the skipped ones, as many as follow its position, and each device's from the first one that it
  receives; its summary has `skippedMessages`, and the report's Catch-up section shows them.
- `applications.unloadBeforeJoin: true`: after the skips, the launcher unloads every topic, so that the applications
  read from a topic loaded anew, without the broker's cached entries or open ledger handles.

## Comparing Pulsar releases

`-Pperformance.clusterPulsarImage`, `compareImages` and `compareImageWithCheckout` build the cluster's test image on a
released Pulsar image, see [Comparing revisions](comparing-revisions.md#comparing-with-a-released-pulsar). The Pulsar
images of 4.x releases don't include the offloaders, which a release publishes separately as
`apache-pulsar-offloaders-<version>-bin.tar.gz`: building the test image on such an image installs the release's
jcloud offloader from the Apache download CDN, or from the release archive for an older release. The images of
Pulsar 5.0 and later, and this checkout's, include it.

```bash
./gradlew :tests:performance:launcher:compareImages \
  -Pperformance.compare.baselineImage=apachepulsar/pulsar:4.0.14 \
  -Pperformance.compare.candidateImage=apachepulsar/pulsar:4.2.5 \
  --args='--scenario tests/performance/scenarios/iot-telemetry-offload.yaml --name offload-ab'
```

## What to look for

- The Offloading section: how many of the topic's ledgers had been offloaded when the applications joined. The
  ledgers that hadn't been, the open one among them, are still read from BookKeeper.
- The Catch-up section: how long each application took to catch up, and its catch-up rate, which reading from the
  tiered storage bounds.
- The backlog chart, each subscription's backlog falling as its application catches up, and the applications'
  end-to-end latency.
