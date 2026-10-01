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

# Running scenarios

The standalone launcher runs a scenario: it starts the Testcontainers cluster that the scenario describes, runs the
gateways and the applications in a container each, collects their outputs into a run directory, and calls the report
tool to write the run report. Run it through Gradle from the repository root, which builds the Pulsar test image and
the workload applications first when they are out of date.

## Gradle tasks

| Task | Use |
|---|---|
| `:tests:performance:launcher:run` | Runs a scenario without profiling. Rejects a scenario that has profiling options rather than silently running it without the profiler. |
| `:tests:performance:launcher:profile` | Runs a scenario with the jonoffcpu profiler attached to every JVM that has profiling options, see [Profiling](profiling.md). |
| `:tests:performance:tools:installDist` | Builds only the workload applications, into `tests/performance/tools/build/install/pulsar-performance-tools`, the directory the launcher mounts into the workload containers. |
| `:tests:performance:report-tool:runJonoffcpuCorrelator` | Runs the pinned jonoffcpu correlator on saved profiles or captures; pass any CLI options with `--args`, see [Running the analysis CLIs](analyzing-profiles.md#running-the-analysis-clis). |
| `:tests:performance:report-tool:runJfrConverter` | Runs jonoffcpu's pinned jfr-converter on collapsed stacks or JFR recordings; pass any CLI options with `--args`, see [Running the analysis CLIs](analyzing-profiles.md#running-the-analysis-clis). |

Pass the launcher's options with `--args`:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --name my-experiment'
```

## Launcher options

| Option | Description |
|---|---|
| `--scenario <file>` | The scenario file. Required. |
| `--extends <scenario>` | Merges a scenario file on top of the scenario, as if the scenario extended it last, such as `configs/profile-broker` to profile the broker or `configs/iot-telemetry-high-mem` to give the scenario more memory. A relative path is looked for first in the directory of the `--scenario` file, then in the working directory, and `.yaml` may be left out; an absolute path is used as given. Repeatable, applied in order. See [Adding to a scenario on the command line](../scenarios/docs/scenario-format.md#adding-to-a-scenario-on-the-command-line). |
| `--set <path>=<value>` | Sets a value of the resolved scenario, such as `workloads.iotTelemetry.rate=5000`. Repeatable, applied in order after the environment overrides. See [Settings on the command line](../scenarios/docs/scenario-format.md#settings-on-the-command-line). |
| `--name <name>` | The run's name in the reports hierarchy. Default: the scenario's `output.name`, or else the scenario file name without `.yaml`. |
| `--reports-dir <dir>` | The root of the reports hierarchy. Default: `performance.reportsDir`, or else `build/performance` in the repository. |
| `--output <dir>` | Writes the run to exactly this directory instead of one in the reports hierarchy. |
| `--cooldown-temperature <°C>` | Waits for the CPU package to cool down to this temperature before starting, see [Host temperature and cool-down](#host-temperature-and-cool-down). Default: `performance.cooldownTemperature`, or else no wait. |
| `--cooldown-timeout <seconds>` | The longest wait for `--cooldown-temperature`. Default: 600. |
| `--progress-interval <seconds>` | How often to print the workload's progress, see [Progress on the console](#progress-on-the-console). Default: 10. |
| `--no-metrics` | Collects no metrics of the brokers, the bookies and ZooKeeper, which a run collects by default, see [Metrics](metrics.md). Default: `performance.metrics`, or else collect them. |
| `--no-perf-stat` | Doesn't count the containers' CPU time, context switches, CPU migrations, cycles and instructions with `perf stat`, which a run does by default in a privileged sidecar container, see [Files of a run](run-reports.md#files-of-a-run). The containers' CPU use and voluntary and involuntary context switches are sampled from `/proc` either way when the Docker engine runs on the launcher's host; when it runs in a VM, such as on macOS, those and `host-io.csv` need the sidecar. Default: `performance.perfStat`, or else count them. |
| `--keep-launcher-log` | Keeps `launcher.log` when the run succeeds. Without it, a successful run deletes the log, since the containers' logs make it large; a failed run keeps it, and so does a run whose applications received duplicates, ordering violations or invalid messages. Default: `performance.keepLauncherLog`, or else a successful run deletes it. |
| `--tools-directory <dir>` | The installed workload applications. The Gradle tasks pass it. |

## Gradle properties

Pass these with `-P` on the command line, or set them in `~/.gradle/gradle.properties` for every run on a machine.

| Property | Description |
|---|---|
| `performance.reportsDir` | The root of the reports hierarchy, relative to the repository root or absolute. Use an absolute path in `~/.gradle/gradle.properties`, since a relative one resolves in each checkout. |
| `performance.cooldownTemperature` | The default of `--cooldown-temperature`. |
| `performance.metrics`, `performance.metrics.bindAddress`, `performance.metrics.grafanaUrl` | Whether runs collect metrics, `true` by default, where the metrics stack is published, and the URL of its Grafana, see [Metrics](metrics.md#settings). |
| `performance.perfStat` | Whether runs count the containers' CPU events with `perf stat` in a privileged sidecar container, `true` by default. |
| `performance.keepLauncherLog` | Keeps `launcher.log` of successful runs, as `--keep-launcher-log` does. The property alone, or with `true`, keeps it. |
| `performance.reportsServer.bindAddress`, `performance.reportsServer.port` | Where `:tests:performance:report-tool:serveReports` listens, `127.0.0.1` and `8000` by default, see [Browsing the reports over HTTP](run-reports.md#browsing-the-reports-over-http). |
| `performance.reportsServer.baseUrl` | The URL of the reports server, such as `http://192.168.1.123:8000/`, with which the launcher prints each report's URL. Default: `http://<bind address>:<port>/`, see [Browsing the reports over HTTP](run-reports.md#browsing-the-reports-over-http). |
| `performance.profile.maxHeapSize` | The heap of the `profile`, `runJonoffcpuCorrelator` and `runJfrConverter` tasks. Default: `4g`. |
| `performance.clusterPulsarImage` | A released Pulsar image for the cluster, such as `apachepulsar/pulsar:4.0.13` or `apachepulsar/pulsar:latest`, to test that release instead of the checkout. The tasks build the test image on it, which needs an Alpine-based Pulsar image, and use it for ZooKeeper, the bookies and the brokers; the workloads, and so the Pulsar client, stay on the checkout's image. See [Comparing with a released Pulsar](comparing-revisions.md#comparing-with-a-released-pulsar). |
| `docker.tag` | The tag of the Docker images the tasks build and run, `latest` by default. Separate tags keep the images of two revisions apart, see [Comparing revisions](comparing-revisions.md). |
| `docker.organization` | The organization of the Docker images, `apachepulsar` by default. |
| `inttest.testImageVariant` | `alpine` (the default, as the unprofiled runs and Pulsar's default image) or `wolfi`, the glibc-based image: the image that profiled runs use, see [Profiling](profiling.md#requirements). |
| `inttest.asyncprofiler.skipPerfEventTuning` | Skips the privileged container that relaxes the kernel's perf event and BPF limits before a profiled run, when they are already set. |

## Where runs are written

Every run gets a directory of its own, in a hierarchy by day, git branch and name:

```
<reports root>/<yyyy-MM-dd>/<branch>/<name>/<MM-dd-HH-mm-ss>/
```

- The reports root is `build/performance` in the repository. `-Pperformance.reportsDir=<dir>` puts the reports
  elsewhere, for example in a directory shared by several worktrees or in a git repository of results. To make that
  permanent for every checkout and worktree on a machine, set it in `~/.gradle/gradle.properties`:

  ```properties
  performance.reportsDir=/data/pulsar-performance-reports
  ```
- The branch is the checked-out branch, with `/` and other characters that don't belong in a directory name
  replaced by `-`. On a detached HEAD, it is the branch that contains the commit with the fewest commits after it,
  a local branch before a remote-tracking one at the same distance, and a remote-tracking branch is named without
  its remote: a detached checkout of `origin/master` is `master`. The report says that the HEAD was detached. A
  detached HEAD that no branch contains is `detached-<commit>`. When the cluster runs a released Pulsar with
  `performance.clusterPulsarImage`, the release takes the branch's place: `apachepulsar/pulsar:4.0.13` is
  `pulsar-4.0.13`.
- The name is the scenario file name without `.yaml`, or the scenario's `output.name` when it sets one. `--name`
  names an experiment instead, so that its runs stay together:

  ```bash
  ./gradlew :tests:performance:launcher:profile -Pperformance.reportsDir=/data/pulsar-reports \
    --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/profile-broker --name e232-ab'
  ```
- The run directory is named by the run's start in local time. Two runs of the same name started within the same
  second would share it.
- `--output <dir>` writes the run to exactly that directory instead, outside the hierarchy.

The launcher prints the run directory when it starts, and the run report when it has finished.
[Finding the results of a run](run-reports.md#finding-the-results-of-a-run) describes finding a run later, and
[Layout of a run directory](run-reports.md#layout-of-a-run-directory) what a run directory contains.

## Progress on the console

The launcher's console shows only the scenario's resolved configuration, with its inheritance and environment
overrides applied, and the run's phases and progress: starting the cluster, the applications and the
gateways, waiting for the applications, verifying, and the run report. The logs of Testcontainers and of the Pulsar
containers go to `launcher.log` in the run directory, which you can follow with `tail -f` during the run. A successful
run deletes it at the end, since the containers' logs make it large; a failed run keeps it, as does a run whose
applications received duplicates, ordering violations or invalid messages, and `--keep-launcher-log` keeps it also
after a successful run. What the launcher prints on the console goes also to `console.log.txt` in the
run directory, which every run keeps.

While the applications start, the launcher shows how many of their pods are open, every 5 seconds while the number
grows:

```
06:44:32 Starting 20 application(s) with 100 pod(s) each
06:44:38 The applications have opened 1,144 of 2,000 pods
06:44:43 The applications have opened 1,688 of 2,000 pods
06:44:48 The applications have opened 1,940 of 2,000 pods
06:44:49 Started the applications in 18 s
```

A workload container that exits while it starts fails the run at once, with the cause from its log, and one that
makes no progress for 60 seconds fails it then, see [When a run fails](run-reports.md#when-a-run-fails).

While the workload runs, the launcher prints two lines every `--progress-interval` seconds, as pulsar-perf does:
the gateways' messages, throughput, pending sends and publish latency, and the applications' messages, throughput,
backlog and end-to-end latency. The prefix has the time since the gateways started and the gateways' phase, such as
`warmup round 1/1` or `measurement 47 s`:

```
[01:21 measurement 47 s] Produced: 67,816 msg of 140,000 (48%) --- 1,020.0 msg/s --- 0.5 Mbit/s --- pending: 3 --- Latency: mean: 65.769 ms - med: 6.271 - 95pct: 342.783 - 99pct: 504.063 - 99.9pct: 744.447 - 99.99pct: 802.815 - Max: 814.591
[01:21 measurement 47 s] Received: 1,354,097 msg of 2,800,000 (48%) --- 20,048.9 msg/s --- 10.3 Mbit/s --- backlog: 1,081 msg (max per application: 65) --- Latency: mean: 80.588 ms - med: 13.007 - 95pct: 383.231 - 99pct: 550.399 - 99.9pct: 776.191 - 99.99pct: 874.495 - Max: 921.087
```

- The throughput and the latencies are those of the interval since the previous lines. The received messages and
  the throughput are summed over every application, and the latencies merged over every application; they include
  the warmup, which the latency logs and the run report leave out. The bit rates count the payload only.
- The backlog is the sum of every subscription's backlog, from the latest topic stats sample, and the largest
  application's, summed over its topics. It includes the messages that the applications have received but whose
  acknowledgments the broker hasn't processed yet, so it isn't zero while messages flow. When the latest sample is
  5 seconds old or older, because the broker answers the stats requests slowly, the line says how old it is. The lines
  also show duplicates and ordering violations as soon as an application has any.
- The end-to-end latency is measured from the broker's publish time, which has millisecond resolution.

The gateways' and the applications' containers stream their progress to the launcher from their control port,
`GET /progress` on port 8089, as newline-delimited JSON: a line per second with the phase, cumulative counters and the
second's latencies as a compressed HdrHistogram of microseconds, which the launcher merges. The applications' lines
sum their applications' counters, merge their latencies, and count the applications that have finished.

## Host temperature and cool-down

A host that heats up during a run lowers its clock speed or throttles, which lowers throughput and adds latency
stalls, and a run that starts on a CPU that the previous run left hot has less headroom than one that starts cool.
This matters most on laptops and small desktops, and when runs follow each other, as in an A/B comparison.

The launcher samples the host's CPU once per second from Linux's sysfs files (`/sys/class/hwmon`,
`/sys/class/thermal` and `/sys/devices/system/cpu`): the package and hottest core temperature, the core frequencies
and the kernel's thermal throttle counters. The run report summarizes them in the settings table and in its Host
section, and says so in bold when the CPU throttled during the measurement. A host without these files, such as one
that isn't Linux, is not sampled.

With turbo disabled, as [the performance testing environment setup](../environment/README.md) configures the host,
the CPU runs at a fixed base frequency, and cooling down between runs matters much less. Without it, let the
launcher wait for the CPU package to cool down to a temperature, before starting the cluster and again after the
warmup rounds, before the first measured message:

```bash
./gradlew :tests:performance:launcher:run -Pperformance.cooldownTemperature=50 \
  --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml'
```

Pick a temperature a few degrees above the host's idle temperature, which the first samples of `host-stats.csv`
show. `--cooldown-timeout <seconds>` bounds each wait; the run goes on after it, and the report says at which
temperature. The workloads' timeouts are extended by the cool-down timeout, so that a wait before the measurement
doesn't fail them. Both waits and their durations are in the run report.

For the wait before the measurement, the gateways serve two HTTP endpoints with the JDK's built-in server, which
the launcher reaches through the port Testcontainers maps on the host: `GET /measurement/ready?waitMillis=<ms>`
answers as soon as every warmup round has been received, and `POST /measurement/start` starts the measurement.
