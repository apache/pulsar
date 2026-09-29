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
Run directory: /home/user/pulsar/build/performance/2026-09-26/master/iot-telemetry/09-26-12-00-00
...
Run report: /home/user/pulsar/build/performance/2026-09-26/master/iot-telemetry/09-26-12-00-00/index.html
```

In between, it prints the run's phases and, every 10 seconds, the workload's progress, see
[Progress on the console](running-scenarios.md#progress-on-the-console).

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

A run that fails writes no report. The launcher stops the run as soon as the gateways' or the applications' container
exits with an error, prints the failure in one line with its cause from the container's log, then shuts the cluster
down, and the Gradle task fails:

```
00:41:42 The run failed: The applications exited with status 1: IllegalStateException: Cannot restart IoT client, caused by ... (log: .../applications/container.log.txt)
00:41:42 Stack trace: .../launcher.log
```

The same holds while a container starts: it fails the run as soon as it exits, or when it has made no progress for
60 seconds, such as when the applications' JVM runs out of heap while their pods open. Its memory configuration's
`PULSAR_MEM` is then too small for the scenario:

```
06:43:34 The applications have opened 1,703 of 2,000 pods
06:43:36 The run failed: The applications exited with status 3: Terminating due to java.lang.OutOfMemoryError: Java heap space (log: .../applications/container.log.txt)
```

The stack trace is in `launcher.log`. A failure while shutting down, such as stopping a container, is only a warning.
The run directory that the launcher printed at the start still has what the run wrote before it failed:

- `console.log.txt`, what the launcher printed on the console, up to the failure
- `launcher.log`, the log of the launcher, with Testcontainers' log and the Pulsar containers' logs
- `gateways/container.log.txt` and `applications/container.log.txt`, the logs of the workload containers
- `applications/<application>/application-summary.json`, with the application's unique messages, duplicates, ordering
  violations and invalid messages, and `applications/<application>/ordering-violations.txt`, with samples of the
  ordering violations, when the application got as far as its checks
- `topic-stats.csv`, `host-stats.csv` and `host-io.csv`, sampled until the failure

The applications' container exits with an error when an application found ordering violations or invalid messages, or
didn't receive every message, and the launcher fails a run when an application's state shows that it missed messages
("did not receive every device sequence"). A failed run isn't a valid measurement: find and fix the cause, and run it again.

## Layout of a run directory

```
<MM-dd-HH-mm-ss>/
├── index.html, README.md              the run report, as an HTML page and as Markdown
├── <scenario>.yaml, resolved-config.yaml
├── run-info.json, run-id.txt
├── throughput.svg, backlog.svg, latency-percentiles.svg, latency-timeline.svg,
│   host-temperature.svg, host-frequency.svg    the charts, each also as PNG
├── topic-stats.csv, host-stats.csv    the sampled topic stats and host CPU
├── host-io.csv                        the sampled host CPU utilization and disk throughput
├── gateways/                          the gateways' outputs, and their recordings in a profiled run
├── applications/                      the applications' container log, and their recordings in a profiled run
│   └── <application>/                 one directory per application, named after its subscription,
│                                      such as iot-application-0
├── broker-profile/                    the broker's recordings, flame graphs and profile report (README.md,
│                                      index.html), in a profiled run
├── heap-dumps/                        the heap dumps, when the scenario asks for them, see heap-dumps.md
├── metrics.json                       the run's metrics in VictoriaMetrics and Grafana, see metrics.md
├── grafana-panels/                    panels of Grafana's dashboards over the run, as PNG images
└── coordination/                      the warmup barrier markers of the gateways and the applications
```

A profiled run adds the recordings, their flame graphs and a profile report to each profiled component's directory,
see [What a profiled run writes](profiling.md#what-a-profiled-run-writes).

## Reading a run report

The report's title names the run's start, the code that it used and the scenario, such as `Pulsar performance test
run 2026-09-26 12:00:00 master 0123456789ab iot-telemetry`: the branch, which a commit that no branch contains
doesn't have, and the commit, with `-dirty` when the checkout had uncommitted changes. The title tells the reports of
different runs and revisions apart, for example in browser tabs. The first paragraph links to the guide to the
performance tests, and a footer below a horizontal line at the end repeats the title, the run ID and that link. The
report has these sections:

- **The settings table**: the scenario, the cluster, the workload, the host's CPU temperature and frequency during
  the measurement, and where, by whom and from which commit the run was made. The host row names the host's CPU,
  cores, hardware threads, memory and operating system, and the Docker engine row the engine's version, CPUs and
  memory, which on macOS are those of Docker Desktop's virtual machine rather than the host's.
- **Correctness**: the unique messages, duplicates, ordering violations and invalid messages of each application.
  In a valid run, every application received every message the gateways sent, warmup included, with no ordering
  violations or invalid messages. Duplicates are valid in Pulsar's at-least-once delivery, and are counted so that
  runs can be compared.
- **Throughput**: the gateways' throughput, the delivered throughput until the slowest application received the last
  message, the measurement's duration and how long the applications were still receiving after the gateways finished.
- **Latency**: the publish latency (send to acknowledgment) and each application's end-to-end latency (publish to
  consume) at percentiles from p50 to the maximum, with charts by percentile and over time.
- **Backlog and rates**: each subscription's backlog and the per-second rates, sampled from the broker's topic
  stats once per second while the gateways publish and the applications receive. A sampled maximum is not the exact peak
  between samples.
- **Host**: the CPU temperature, frequency and thermal throttling at the start and during the measurement, with
  their charts in a collapsed section; a chart whose values the host doesn't provide is left out. The report says so
  in bold when the CPU throttled during the measurement, and that throttling is unknown when the host has no thermal
  throttle counters. A host that isn't Linux isn't sampled.
- **Metrics**, when the run collected them: the scrape interval, the run's `cluster` label in VictoriaMetrics, links to
  the run on Grafana's dashboards, and panels of the dashboards over the run with its events marked, each linking to
  the panel in Grafana, see [Metrics](metrics.md).
- **Heap dumps**, when the scenario asked for them: each dump, with the heap usage before it and its size, see
  [Heap dumps](heap-dumps.md).

A profiled run's report has a Profiles section, which links each profile's reports directly: its jonoffcpu report
(off-CPU summary), which is the off-CPU digest, its profile report, and its blocked time, CPU and allocation flame
graphs, see
[Profiling](profiling.md#what-a-profiled-run-writes).
Every Markdown report the launcher writes, including the profile reports and the off-CPU digests, has an HTML page
beside it, rendered with [commonmark-java](https://github.com/commonmark/commonmark-java).

## Files of a run

| File | Contents |
|---|---|
| `README.md`, `index.html` | The run report, as Markdown and as its HTML page, with links to the run's other files. The names make HTTP servers and GitHub open a run's directory on its report |
| `<scenario>.yaml`, `resolved-config.yaml` | The scenario file as written, and the scenario with its inheritance and environment overrides applied, which the workloads read |
| `run-info.json` | The run's start, host, user, project directory, git branch, whether the HEAD was detached, the commit, whether the checkout had uncommitted changes, and the Pulsar version, with the keys of `pulsar-version.properties` where they match. The launcher collects them itself, from git and `gradle.properties` in the checkout it runs from. The `host.*` keys have the host's CPU model, sockets, cores, hardware threads, memory and operating system, from a JDK Flight Recorder recording of the launcher's JVM that is stopped right away, so that they are there on every operating system, and the `docker.*` keys the Docker engine's version, CPUs, memory, operating system, kernel and architecture. When the cluster ran a released Pulsar, `cluster.pulsarImage` and `cluster.version` name its image and the version that the brokers reported, see [Comparing with a released Pulsar](comparing-revisions.md#comparing-with-a-released-pulsar) |
| `run-id.txt` | The ID that correlates the gateways and the applications of the run |
| `metrics.json` | The run's metrics in VictoriaMetrics and Grafana: its `cluster` label and selector, the scrape interval, the time range, the jobs, the events, and the URLs, credentials and data source to query them with, see [metrics.json](metrics.md#metricsjson) |
| `grafana-panels/*.png` | Panels of Grafana's dashboards over the run, which the report shows, see [Panels in the run report](metrics.md#panels-in-the-run-report) |
| `console.log.txt` | What the launcher printed on the console, from the run directory to the run report, or to the failure. Every run keeps it |
| `launcher.log` | The launcher's log: Testcontainers' log and the Pulsar containers' logs, which stay off the console. It is written during the run, and a successful run deletes it at the end, since the containers' logs make it large, unless `--keep-launcher-log` or `-Pperformance.keepLauncherLog` keeps it; a failed run keeps it |
| `throughput.svg`, `.png` | Messages published and dispatched per second over the run, warmup included and the gateways' finish marked. A cool-down wait of 10 s or more before the measurement is cut out of the time axis |
| `backlog.svg`, `.png` | Each subscription's backlog over the run, on the same time axis |
| `latency-percentiles.svg`, `.png` | Latency by percentile, as HistogramLogAnalyzer plots it: publish and each application's end to end, on an axis that spreads the tail (90 %, 99 %, 99.9 %, …) |
| `latency-timeline.svg`, `.png` | The maximum latency of each logged interval over the run, publish and per application |
| `host-temperature.svg`, `.png`, `host-frequency.svg`, `.png` | The CPU package and hottest core temperature, and the mean and lowest core frequency, over the run |
| `topic-stats.csv` | The broker's topic stats sampled once per second: backlog and message counters per subscription |
| `host-stats.csv` | The host's CPU sampled once per second from Linux's sysfs files: package and hottest core temperature, mean and lowest core frequency, the kernel's thermal throttle counters and the fastest fan |
| `host-io.csv` | The host's CPU utilization and disk throughput sampled once per second from Linux's `/proc/stat` and `/proc/diskstats`: the busy and I/O-wait share of all CPUs, and each physical disk's read and write MB/s and busy share. It shows whether a run is limited by the host's CPUs or its storage, which the bookies share |
| `gateways/gateways-summary.json` | The gateways' counts and throughput, and the epoch-millisecond boundaries of the measurement |
| `gateways/gateways-latency.hdr`, `.hgrm` | The publish latency log, and its percentile distribution in milliseconds, see [Latency logs](#latency-logs) |
| `gateways/gateways-state.bin` | The gateways' next sequence number for each device. The launcher compares it with each application's `application-state.bin` and fails the run when they differ, which catches messages missing at the end, where no gap shows |
| `applications/<application>/application-summary.json` | The application's unique messages, duplicates, ordering violations and invalid messages, and its first and last measured-message receipt |
| `applications/<application>/application-latency.hdr`, `.hgrm` | The application's end-to-end latency log, and its percentile distribution in milliseconds |
| `applications/<application>/application-state.bin` | The application's next expected sequence number for each device |
| `applications/<application>/ordering-violations.txt` | Samples of the ordering violations, with the message ID, topic and receiving thread; empty in a valid run |
| `gateways/container.log.txt`, `applications/container.log.txt` | The container's log, named `.txt` so that HTTP servers show it as text |
| `coordination/` | The markers with which the applications tell the gateways that they received a warmup round |

## Latency logs

Every IoT run writes `gateways/gateways-latency.hdr` with the send-completion latency of the successfully sent
measured messages, and one `applications/<application>/application-latency.hdr` per application with the
broker-publish-to-listener latency of the measured messages. Both use microseconds internally and three significant
digits. Warmup messages are tagged in the payload and excluded. Each application captures its timestamp on listener
entry and records the sample after payload decoding and key validation, before sequence validation and acknowledgment,
so decoding and validation time are excluded from the latency.

The run report plots these logs as HistogramLogAnalyzer does, with [XChart](https://knowm.org/open-source/xchart/):
the latency by percentile and the maximum latency of each logged interval, with the publish latency and each
application's end-to-end latency as separate lines. The applications consume independently, so their latencies are
never merged. To plot them again for a run directory:

```bash
./gradlew :tests:performance:report-tool:renderHdrHistograms \
  --args='--run-directory <run directory>'
```

The outputs are `latency-percentiles.svg` and `latency-timeline.svg` in the run directory, each with a PNG beside
it; `--output-prefix /path/to/name` changes them. The `.hdr` interval logs also open in
[HistogramLogAnalyzer](https://github.com/HdrHistogram/HistogramLogAnalyzer), and the `.hgrm` percentile
distributions in HdrHistogram's [plotFiles.html](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html), for
interactive comparisons across runs.

The broker-publish-to-listener latency assumes that the gateways', the applications' and the broker's clocks agree, as
they do for containers on the same Docker host.

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
| `performance.reportsServer.bindAddress` | `127.0.0.1` |
| `performance.reportsServer.port` | `8000` |
| `performance.reportsServer.baseUrl` | `http://<bind address>:<port>/` |

`performance.reportsServer.bindAddress=0.0.0.0` makes the server available on the network, at the machine's host name or
IP address. The server has no authentication, so do that only on a trusted network.

The launcher prints the URL of each report on the server, the run report's as `Run report URL:` and each profile
report's as `Profile report URL:`, so that a report on another machine opens with a click in the terminal. The URL is
the base URL with the report's path in the reports root appended, without a directory's `index.html`, which the server
opens for the directory. `performance.reportsServer.baseUrl` sets the base URL, such as `http://192.168.1.123:8000/`
for a server that you reach at that address; without it, it is `http://<bind address>:<port>/`, and with the bind
address `0.0.0.0` the address of the first network interface, in the order that the operating system numbers them,
that is up and has an IPv4 address other than a loopback or link-local one. Set `performance.reportsServer.baseUrl`
on a host with several interfaces, such as Docker's or a VPN's, when that address isn't the one that you reach it at,
or when you reach the server through an SSH tunnel or a proxy. `serveReports` prints the same base URL when it starts.
A run written with `--output` outside the reports root has no URL.

When the performance tests run on a separate machine, start the server there. It's reachable from your own machine
only when `performance.reportsServer.bindAddress` is set as above, in that machine's `~/.gradle/gradle.properties` or
with `-P`, or through an SSH tunnel with the default address. The tunnel forwards local ports to those loopback
addresses over the encrypted SSH connection. This one forwards the reports server, and Grafana and VictoriaMetrics of
the [metrics stack](metrics.md), which also listen on the loopback address by default:

```bash
# On your own machine
ssh -N -L 8000:127.0.0.1:8000 -L 3000:127.0.0.1:3000 -L 8428:127.0.0.1:8428 perf-host
```

Then open <http://127.0.0.1:8000/> for the reports and <http://127.0.0.1:3000/> for Grafana on your own machine, and
VictoriaMetrics' web UI, vmui, at <http://127.0.0.1:8428/vmui/?#/metrics> to browse the metrics or at
<http://127.0.0.1:8428/vmui> for PromQL queries. The URLs that the launcher prints, such as
`Run report URL:` and `Metrics in Grafana:`, open through the tunnel too, since they use the same addresses.

To open the tunnel with `ssh perf-host-pulsar-perf`, add a host to `~/.ssh/config` on your own machine:

```
Host perf-host-pulsar-perf
    HostName perf-host
    # Only forward the ports, without a remote shell, as -N does (OpenSSH 8.7 or later)
    SessionType none
    # Fail instead of running without a tunnel when a local port is already in use
    ExitOnForwardFailure yes
    LocalForward 8000 127.0.0.1:8000
    LocalForward 3000 127.0.0.1:3000
    LocalForward 8428 127.0.0.1:8428
```

`ssh perf-host-pulsar-perf` keeps the tunnel open until you stop it with Ctrl-C, and `ssh perf-host` still opens a
shell as before.

The tunnel prints nothing while it works. It prints a line only when something fails:

- When a service isn't running on the remote machine, such as Grafana while the metrics stack is down, the browser's
  request fails, and ssh prints a line such as `channel 3: open failed: connect failed: Connection refused` for each
  connection it couldn't forward.
- When a local port is already in use on your own machine, such as by another tunnel or a local Grafana, ssh prints
  `bind [127.0.0.1]:3000: Address already in use`. When it can't listen on the port at all, it also prints `Could not
  request local forwarding.`, and with `ExitOnForwardFailure yes` it exits instead of running without that port.

Add `-v` to see the forwarding itself: `ssh -v perf-host-pulsar-perf`, or `ssh -v -N -L …`, logs each forwarded port
when the tunnel starts, as `debug1: Local connections to LOCALHOST:8000 forwarded to remote address 127.0.0.1:8000`,
and each connection through it, as `debug1: Connection to port 8000 forwarding to 127.0.0.1 port 8000 requested.`,
together with ssh's other debug messages.

[`serve-reports.py`](../serve-reports.py) does the same with Python's built-in server, without Gradle:
`tests/performance/serve-reports.py [directory] [--bind <address>] [--port <port>]`. It reads
`performance.reportsDir` from `~/.gradle/gradle.properties` when no directory is given.
