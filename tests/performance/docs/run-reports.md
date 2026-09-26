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

# Run reports

Every run writes a report into its run directory: `README.md`, with an HTML page beside it, `index.html`, whose
links lead to the run's other files: the charts, the latency logs, the container logs and, in a profiled run,
the profile reports and flame graphs.

## Finding the results of a run

The launcher prints the run directory when the run starts, and the run report when it has finished:

```
Run directory: /home/user/pulsar/build/performance/2026-09-26/master/iot-telemetry-local/09-26-12-00-00
...
Run report: /home/user/pulsar/build/performance/2026-09-26/master/iot-telemetry-local/09-26-12-00-00/index.html
```

A profiled run also prints the directories of its off-CPU profiles and flame graphs, and its profile reports, before
the run report. Open `index.html` in a browser; its links work there, and lead to everything else the run wrote.
Because the report is the run directory's `index.html` and `README.md`, an HTTP server that serves the reports opens
each run's directory on its report, and so does GitHub for a repository of results.

To find a run later, look it up in the reports hierarchy, by day, git branch, name and start time:

```
<reports root>/<yyyy-MM-dd>/<branch>/<name>/<MM-dd-HH-mm-ss>/
```

The reports root is `build/performance` in the repository, or `performance.reportsDir` when it is set; the name is
the scenario file name without `.yaml`, `output.name` or `--name`.
[Where runs are written](running-scenarios.md#where-runs-are-written) describes each part. The newest run under the
default reports root is:

```bash
ls -dt build/performance/*/*/*/*/ | head -n 1
```

On a machine that runs the tests for others, serve the reports root over HTTP and browse it, see
[Browsing the reports over HTTP](#browsing-the-reports-over-http).

### When a run fails

A run that fails writes no report: the launcher stops with an error, such as `IoT consumer exited with status 1`, and
the Gradle task fails. The run directory that the launcher printed at the start still has what the run wrote before
it failed:

- `producer/container.log.txt` and `<application>/container.log.txt`, the logs of the workload containers
- `<application>/consumer-summary.json`, with the application's unique messages, duplicates, ordering violations and
  invalid messages, and `<application>/ordering-violations.txt`, with samples of the ordering violations, when the
  application got as far as its checks
- `topic-stats.csv` and `host-stats.csv`, sampled until the failure

A consumer exits with an error when it found ordering violations or invalid messages, or didn't receive every
message, and the launcher fails a run when an application's state shows that it missed messages ("did not receive
every device sequence"). A failed run isn't a valid measurement: find and fix the cause, and run it again.

## Layout of a run directory

```
<MM-dd-HH-mm-ss>/
├── index.html, README.md              the run report, as an HTML page and as Markdown
├── <scenario>.yaml, resolved-config.yaml
├── run-info.json, run-id.txt
├── throughput.svg, backlog.svg, latency-percentiles.png, latency-timeline.png,
│   host-temperature.svg, host-frequency.svg    the charts, the SVG charts also as PNG
├── topic-stats.csv, host-stats.csv    the sampled topic stats and host CPU
├── producer/                          the producer's outputs, and its recordings in a profiled run
├── <application>/                     one directory per consumer application, named after its subscription,
│                                      such as iot-application-0
├── broker-profile/                    the broker's recordings, flame graphs and profile report (README.md,
│                                      index.html), in a profiled run
└── coordination/                      the warmup barrier markers of the producer and the applications
```

A profiled run adds the recordings, their flame graphs and a profile report to each profiled component's directory,
see [What a profiled run writes](profiling.md#what-a-profiled-run-writes).

## Reading a run report

The report has these sections:

- **The settings table**: the scenario, the cluster, the workload, the host's CPU temperature and frequency during
  the measurement, and where, by whom and from which commit the run was made.
- **Correctness**: the unique messages, duplicates, ordering violations and invalid messages of each application.
  In a valid run, every application received every message the producer sent, warmup included, with no ordering
  violations or invalid messages. Duplicates are valid in Pulsar's at-least-once delivery, and are counted so that
  runs can be compared.
- **Throughput**: the producer throughput, the delivered throughput until the slowest application received the last
  message, the measurement's duration and how long the consumers were still draining after the producers finished.
- **Latency**: the publish latency (send to acknowledgment) and each application's end-to-end latency (publish to
  consume) at percentiles from p50 to the maximum, with charts by percentile and over time.
- **Backlog and rates**: each subscription's backlog and the per-second rates, sampled from the broker's topic
  stats once per second while the producers run and the consumers drain. A sampled maximum is not the exact peak
  between samples.
- **Host**: the CPU temperature, frequency and thermal throttling at the start and during the measurement, with
  their charts in a collapsed section; a chart whose values the host doesn't provide is left out. The report says so
  in bold when the CPU throttled during the measurement, and that throttling is unknown when the host has no thermal
  throttle counters. A host that isn't Linux isn't sampled.

A profiled run's report also links to the profile reports, see [Profiling](profiling.md#what-a-profiled-run-writes).
Every Markdown report the launcher writes, including the profile reports and the off-CPU digests, has an HTML page
beside it, rendered with [commonmark-java](https://github.com/commonmark/commonmark-java).

## Files of a run

| File | Contents |
|---|---|
| `README.md`, `index.html` | The run report, as Markdown and as its HTML page, with links to the run's other files. The names make HTTP servers and GitHub open a run's directory on its report |
| `<scenario>.yaml`, `resolved-config.yaml` | The scenario file as written, and the scenario with its inheritance and environment overrides applied, which the workloads read |
| `run-info.json` | The run's start, host, user, project directory, git branch and commit, whether the checkout had uncommitted changes, and the Pulsar version, with the keys of `pulsar-version.properties` where they match. The launcher collects them itself, from git and `gradle.properties` in the checkout it runs from |
| `run-id.txt` | The ID that correlates the producer and the consumers of the run |
| `throughput.svg`, `.png` | Messages published and dispatched per second over the run, warmup included and the producers' finish marked. A cool-down wait of 10 s or more before the measurement is cut out of the time axis |
| `backlog.svg`, `.png` | Each subscription's backlog over the run, on the same time axis |
| `latency-percentiles.png` | Latency by percentile, as HistogramLogAnalyzer plots it: publish and each application's end to end, on an axis that spreads the tail (90 %, 99 %, 99.9 %, …) |
| `latency-timeline.png` | The maximum latency of each logged interval over the run, publish and per application |
| `host-temperature.svg`, `.png`, `host-frequency.svg`, `.png` | The CPU package and hottest core temperature, and the mean and lowest core frequency, over the run |
| `topic-stats.csv` | The broker's topic stats sampled once per second: backlog and message counters per subscription |
| `host-stats.csv` | The host's CPU sampled once per second from Linux's sysfs files: package and hottest core temperature, mean and lowest core frequency, the kernel's thermal throttle counters and the fastest fan |
| `producer/producer-summary.json` | The producer's counts and throughput, and the epoch-millisecond boundaries of the measurement |
| `producer/produce-latency.hdr`, `.hgrm` | The publish latency log, and its percentile distribution in milliseconds, see [Latency logs](#latency-logs) |
| `producer/produced-state.bin` | The producer's next sequence number for each device. The launcher compares it with each application's `consumed-state.bin` and fails the run when they differ, which catches messages missing at the end, where no gap shows |
| `<application>/consumer-summary.json` | The application's unique messages, duplicates, ordering violations and invalid messages, and its first and last measured-message receipt |
| `<application>/consume-latency.hdr`, `.hgrm` | The application's end-to-end latency log, and its percentile distribution in milliseconds |
| `<application>/consumed-state.bin` | The application's next expected sequence number for each device |
| `<application>/ordering-violations.txt` | Samples of the ordering violations, with the message ID, topic and receiving thread; empty in a valid run |
| `producer/container.log.txt`, `<application>/container.log.txt` | The container's log, named `.txt` so that HTTP servers show it as text |
| `coordination/` | The markers with which the applications tell the producer that they received a warmup round |

## Latency logs

Every IoT run writes `producer/produce-latency.hdr` with the send-completion latency of the successfully sent
measured messages, and one `<application>/consume-latency.hdr` per application with the broker-publish-to-listener
latency of the measured messages. Both use microseconds internally and three significant digits. Warmup messages are
tagged in the payload and excluded. The consumer captures its timestamp on listener entry and records the sample
after payload decoding and key validation, before sequence validation and acknowledgment, so decoding and
validation time are excluded from the latency.

The run report plots these logs as HistogramLogAnalyzer does, with [XChart](https://knowm.org/open-source/xchart/):
the latency by percentile and the maximum latency of each logged interval, with the publish latency and each
application's end-to-end latency as separate lines. The applications consume independently, so their latencies are
never merged. To plot them again for a run directory:

```bash
./gradlew :tests:performance:report-tool:renderHdrHistograms \
  --args='--run-directory <run directory>'
```

The outputs are `latency-percentiles.png` and `latency-timeline.png` in the run directory; `--output-prefix
/path/to/name` changes them. The `.hdr` interval logs also open in
[HistogramLogAnalyzer](https://github.com/HdrHistogram/HistogramLogAnalyzer), and the `.hgrm` percentile
distributions in HdrHistogram's [plotFiles.html](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html), for
interactive comparisons across runs.

The broker-publish-to-listener latency assumes that the producer, consumer and broker clocks agree, as they do for
containers on the same Docker host.

## Browsing the reports over HTTP

The reports are static files, so any HTTP server can serve the reports root, and its directory listings lead
through days, branches and names to the runs. The `serveReports` task serves the reports root, which is
`performance.reportsDir` or else `build/performance`, and shows the YAML, CSV, HDR latency logs, collapsed stacks,
logs and Markdown of a run as text rather than as downloads:

```bash
./gradlew :tests:performance:report-tool:serveReports
```

Then open <http://127.0.0.1:8000/> and follow the listings to a run: a run's directory opens its report, from which
the profile reports, digests and flame graphs are linked. The server reads the files as they are requested, so new
runs appear without restarting it. Stop it with Ctrl+C.

It binds to the loopback interface on port 8000 by default, so that it isn't reachable from the network. These
Gradle properties change where it listens, on the command line with `-P` or in `~/.gradle/gradle.properties`:

| Property | Default |
|---|---|
| `performance.reportsServer.address` | `127.0.0.1` |
| `performance.reportsServer.port` | `8000` |

When the performance tests run on a separate machine, start the server there and reach it through an SSH tunnel,
which forwards a local port to that loopback address over the encrypted SSH connection:

```bash
# On your own machine
ssh -N -L 8000:127.0.0.1:8000 perf-host
```

[`serve-reports.py`](../serve-reports.py) does the same with Python's built-in server, without Gradle:
`tests/performance/serve-reports.py [directory] [--bind <address>] [--port <port>]`. It reads
`performance.reportsDir` from `~/.gradle/gradle.properties` when no directory is given.
