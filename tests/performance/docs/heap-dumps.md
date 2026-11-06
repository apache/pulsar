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

# Heap dumps

The launcher can write heap dumps of the broker, the gateways and the applications during a run, to find what holds
their memory: when a JVM runs out of memory, at the highest heap usage, at given times, periodically, and at the end.
A scenario's `heapDumps` section asks for them, and a file in the scenarios' `configs` directory adds them to any
scenario with the launcher's `--extends` option:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --extends configs/heap-dumps-broker'
```

[`heap-dumps-broker.yaml`](../scenarios/configs/heap-dumps-broker.yaml) dumps the broker at its highest heap usage,
after every application has received every message, and when it runs out of memory.

A heap dump stops the JVM while it is written, which took 1–3 s for the 350 MB of live objects of a small broker,
and the launcher's dumps of the live objects run a full garbage collection first. A run with scheduled heap dumps is for
finding what holds the memory, not for measuring: its throughput and latency include the pauses. A dump on
`OutOfMemoryError` costs nothing until the JVM runs out of memory.

## Settings

`heapDumps` has a section for each component, `broker`, `gateways` and `applications`, with these settings, and
`gzipLevel` for every component's dumps:

```yaml
heapDumps:
  gzipLevel: 1                 # optional: compresses the dumps, from 1, the fastest, to 9; 0 or none doesn't
  broker:
    onOutOfMemoryError: true   # the JVM writes a dump when it runs out of memory
    atStart: true              # when the gateways start, which starts the traffic
    atSeconds: [60, 120]       # at these seconds after the gateways' start; one number works too
    everySeconds: 30           # periodically after the gateways' start
    atPeakUsage: true          # keeps a dump of the highest heap usage
    atEnd: true                # after every application has received every message; the broker only
```

- The times count from the start of the gateways. When the launcher waits for the CPU to cool down before the
  measurement, the measurement starts later than that; the run report's host section shows how long the cool-downs
  took.
- `atPeakUsage` samples the heap usage every 5 s with `jcmd GC.heap_info`, from the gateways' start to the end. It
  writes a dump when the heap is at least half full and its usage is at least 10 % above that of the previous peak
  dump, and replaces the previous peak dump with it, so that one peak dump remains. The heap usage includes garbage
  that the collector hasn't reclaimed yet, as the JVM reports it, while the dump holds the live objects.
- `atEnd` is only for the broker: the gateways and the applications have exited when every application has received
  every message.
- `onOutOfMemoryError` adds `-XX:+HeapDumpOnOutOfMemoryError` and the dump directory to the broker's
  `PULSAR_EXTRA_OPTS`, which come after the test image's own `-XX:HeapDumpPath=/var/log/pulsar` on its command line,
  and to the gateways' and the applications' `JAVA_TOOL_OPTIONS`, after any options that the component's `env` sets.
  The JVM writes the dump itself, named `java_pid<pid>.hprof`, also when the run then fails.
- The dumps are written one at a time; a dump that is due while another is being written waits for it.
- `gzipLevel` has the JVMs write the dumps gzip-compressed, as `.hprof.gz` files: the launcher's with
  `jcmd GC.heap_dump -gz=<level>`, and those on `OutOfMemoryError` with `-XX:HeapDumpGzipLevel=<level>`, which JDK 17
  and later have, so also the Java 21 of the Pulsar 4 images. Heap dumps compress well: at level 1, the dumps of a
  small broker came to about a quarter of their size, 354 MB to 89 MB, in the 1–2 s that its uncompressed dumps took.
  Higher levels take longer, and the JVM is stopped while it compresses. Without it, the dumps are uncompressed `.hprof` files, which the analysis tools open directly. Add
  it to a scenario that has a `heapDumps` section on the command line, such as
  `--extends configs/heap-dumps-broker --set heapDumps.gzipLevel=1`.

## Files

Each component's dumps are in `heap-dumps/<component>/` in the run directory, named after the JVM and what triggered
the dump, such as `heap-dumps/broker/broker-0-peak.hprof`, `broker-0-at-60s.hprof`, `broker-0-periodic-90s.hprof` and
`broker-0-end.hprof`, or `.hprof.gz` with `gzipLevel`. `heap-dumps/heap-dumps.csv` lists every dump that the launcher wrote: when, of which JVM, what
triggered it, the file, the heap usage and the maximum heap before the dump, in bytes, and how long the dump took.
The run report lists the dumps in its Heap dumps section, and the launcher prints each dump as it writes it, and where
the dumps are when the run ends, also when the run failed.

The JVMs in the containers write their dumps as other users than the launcher's, and readable only by them; the
launcher makes every dump readable when the run ends. Heap dumps are large, about the size of the live objects, so
keep an eye on the disk space when a run writes many of them, and remove them when they have been analyzed.

## Analyzing a heap dump

[Heap dumps and memory leaks](analyzing-profiles.md#heap-dumps-and-memory-leaks) describes the tools that read heap
dumps, including jafar-shell and AI agents. jafar-shell reads uncompressed dumps only: decompress a `.hprof.gz` dump
first, keeping the compressed one, with `gunzip -k broker-0-peak.hprof.gz`.
