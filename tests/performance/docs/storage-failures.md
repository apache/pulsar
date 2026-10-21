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

# Isolated storage-failure checks

`storageCheck` is an opt-in end-to-end correctness suite, not a throughput benchmark or a network-delay
approximation of disk latency. It reuses the launcher's Pulsar container topology and client libraries; ordinary
launcher unit tests do not start these containers.

```shell
./gradlew :tests:performance:launcher:storageCheck --tests StorageFailureCheck -PtestRetryCount=0
```

The task builds the current Alpine Java test image when needed, then derives a test-only image containing a small
journal FUSE filesystem. It does not modify the shared test image or change normal scenarios' durability settings.
Allow about 4 GB of memory for the small cluster and additional memory for image builds. Tests run serially with a
fresh cluster per fault. Docker must expose `/dev/fuse` and permit `SYS_ADMIN` and container-private mounts. Only
the faulted bookie receives this device/capability and an unconfined AppArmor profile; no container uses privileged
mode, host PID/network namespaces, or host filesystem binds. Unsupported FUSE/mount permissions fail setup rather
than silently passing/skipping a fault. Do not enable `PULSAR_CONTAINERS_LEAVE_RUNNING` for these checks.

## Faults and assertions

Each case uses two-copy ledgers (ensemble/write/ack quorum 2), journal writes with sync enabled on every bookie,
a durable unacknowledged subscription, and an initially stopped third bookie. Actual writable registrations—not
the list of known bookie cookies—prove that there is no spare capacity before the fault.

| Case | Actual storage operation |
|---|---|
| Disk full | Allocate at most 129 MiB into a dedicated 128 MiB ledger tmpfs, require kernel ENOSPC, and wait for BookKeeper's registered read-only transition. The shared Docker filesystem is never filled. |
| Read-only | The journal filesystem rejects mutations with EROFS, including writes/fsyncs through already-open descriptors. This is filesystem-level read-only refusal, not `chmod`, a forced BookKeeper state, or a claim to test an ext4 remount. |
| I/O error | The journal filesystem returns EIO on real journal `.txn` writes/fsyncs. Connections and nonjournal operations are not impaired. |
| Stalled journal | A real journal `.txn` write/fsync remains blocked until the independent control filesystem restores healthy mode. A 60-second watchdog records `EXPIRED` and fails the check rather than disguising a missed recovery. |

The suite requires bounded publish failures when the writable quorum is unavailable. With the fault still active,
it starts the spare, requires confirmed publishes to resume, and reads BookKeeper metadata at the confirmed
entry's ensemble segment to prove the spare replaced the faulted bookie (historical ensemble segments can still
contain it). It then clears the fault, restarts the broker and all bookie JVMs on the **same data**, and consumes
from the pinned subscription with no pre-restart consumer/prefetch cache. Every successfully confirmed message ID
and payload must be present. Timed-out sends may nevertheless have persisted, and legitimate redelivery is allowed;
neither is mislabeled as confirmed-message loss or a deduplication violation.

The disk-full case requires real ENOSPC plus read-only registration. The other cases require evidence of an
injected real journal write/fsync, not merely a disk-check probe. Small journal/entry-log sizes and explicit
minimum-free-space overrides keep the bounded volumes usable before the fault. No background producer can grow
without bound: the failure phase has only eight outstanding writes and finite client/test deadlines.

## Isolation, recovery and evidence

The wrapper mounts fixed, private, bounded tmpfs filesystems at `/storage/backing` and `/storage/ledgers` inside
only the target container; `/storage/journal` is a FUSE view of the backing filesystem. Mode/control and fault
records are on a separate 1 MiB mount at `/storage/control`. The target has a 2 GiB container memory limit. There
are no configurable host paths, loop devices, device-mapper targets, host-wide I/O throttles, or filesystem freezes.
Before filling, the test checks the exact ledger mount is tmpfs and no larger than 128 MiB. Cleanup restores
healthy mode and removes only the fixed test-owned filler before shutting down the cluster, even on failure.

Results and fault evidence are retained under `tests/performance/launcher/build/storage-check/<cluster>-<fault>/`,
including the confirmed-message manifest, initial/replacement ensemble evidence, filesystem fault events and FUSE
log. Standard TestNG results are under the module's `build/test-results/storageCheck/`; the existing container
harness also retains logs under the module's `build/container-logs/`. Keep these artifacts for failed runs.
A journal watchdog expiry is never a successful test result.

These are filesystem-error and process-recovery tests, **not physical disk, controller, power-loss, abrupt-crash,
or cross-container-recreation durability tests**. Tmpfs retains data across JVM restarts but disappears when the
container is removed. Sync is explicitly enabled to exercise the acknowledgement path; tmpfs cannot establish a
power-loss persistence guarantee. Testing block-layer/media failures requires a separately isolated disposable VM
and is not enabled by this task.
