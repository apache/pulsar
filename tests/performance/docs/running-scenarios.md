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
workload applications in containers of their own, collects their outputs into a run directory, and calls the report
tool to write the run report. Run it through Gradle from the repository root, which builds the Pulsar test image and
the workload applications first when they are out of date.

## Gradle tasks

| Task | Use |
|---|---|
| `:tests:performance:launcher:run` | Runs a scenario without profiling. Rejects a scenario that has profiling options rather than silently running it without the profiler. |
| `:tests:performance:launcher:profile` | Runs a scenario with the jonoffcpu profiler attached to every JVM that has profiling options, see [Profiling](profiling.md). |
| `:tests:performance:tools:installDist` | Builds only the workload applications, into `tests/performance/tools/build/install/pulsar-performance-tools`, the directory the launcher mounts into the workload containers. |

Pass the launcher's options with `--args`:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml --name my-experiment'
```

## Launcher options

| Option | Description |
|---|---|
| `--config <file>` | The scenario file. Required. |
| `--name <name>` | The run's name in the reports hierarchy. Default: the scenario's `output.name`, or else the scenario file name without `.yaml`. |
| `--reports-dir <dir>` | The root of the reports hierarchy. Default: `performance.reportsDir`, or else `build/performance` in the repository. |
| `--output <dir>` | Writes the run to exactly this directory instead of one in the reports hierarchy. |
| `--cooldown-temperature <°C>` | Waits for the CPU package to cool down to this temperature before starting, see [Host temperature and cool-down](#host-temperature-and-cool-down). Default: `performance.cooldownTemperature`, or else no wait. |
| `--cooldown-timeout <seconds>` | The longest wait for `--cooldown-temperature`. Default: 600. |
| `--tools-directory <dir>` | The installed workload applications. The Gradle tasks pass it. |

## Gradle properties

Pass these with `-P` on the command line, or set them in `~/.gradle/gradle.properties` for every run on a machine.

| Property | Description |
|---|---|
| `performance.reportsDir` | The root of the reports hierarchy, relative to the repository root or absolute. Use an absolute path in `~/.gradle/gradle.properties`, since a relative one resolves in each checkout. |
| `performance.cooldownTemperature` | The default of `--cooldown-temperature`. |
| `performance.reportsServer.address`, `performance.reportsServer.port` | Where `:tests:performance:report-tool:serveReports` listens, `127.0.0.1` and `8000` by default, see [Browsing the reports over HTTP](run-reports.md#browsing-the-reports-over-http). |
| `performance.profile.maxHeapSize` | The heap of the `profile` task, which correlates the off-CPU captures. Default: `4g`. |
| `docker.tag` | The tag of the Docker images the tasks build and run, `latest` by default. Separate tags keep the images of two revisions apart, see [Comparing revisions](comparing-revisions.md). |
| `docker.organization` | The organization of the Docker images, `apachepulsar` by default. |
| `inttest.testImageVariant` | `wolfi` (the default) or `alpine`: the image that profiled runs use, see [Profiling](profiling.md#requirements). |
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
  replaced by `-`. A detached HEAD is `detached-<commit>`.
- The name is the scenario file name without `.yaml`, or the scenario's `output.name` when it sets one. `--name`
  names an experiment instead, so that its runs stay together:

  ```bash
  ./gradlew :tests:performance:launcher:profile -Pperformance.reportsDir=/data/pulsar-reports \
    --args='--config tests/performance/scenarios/iot-telemetry-high-rate-profile.yaml --name e232-ab'
  ```
- The run directory is named by the run's start in local time. Two runs of the same name started within the same
  second would share it.
- `--output <dir>` writes the run to exactly that directory instead, outside the hierarchy.

The launcher prints the run directory when it starts, and the run report when it has finished.
[Finding the results of a run](run-reports.md#finding-the-results-of-a-run) describes finding a run later, and
[Layout of a run directory](run-reports.md#layout-of-a-run-directory) what a run directory contains.

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
  --args='--config tests/performance/scenarios/iot-telemetry-high-rate.yaml'
```

Pick a temperature a few degrees above the host's idle temperature, which the first samples of `host-stats.csv`
show. `--cooldown-timeout <seconds>` bounds each wait; the run goes on after it, and the report says at which
temperature. The workloads' timeouts are extended by the cool-down timeout, so that a wait before the measurement
doesn't fail them. Both waits and their durations are in the run report.

For the wait before the measurement, the producer serves two HTTP endpoints with the JDK's built-in server, which
the launcher reaches through the port Testcontainers maps on the host: `GET /measurement/ready?waitMillis=<ms>`
answers as soon as every warmup round has been received, and `POST /measurement/start` starts the measurement.
