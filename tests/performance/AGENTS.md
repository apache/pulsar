# Agent guide for the performance tests

Use this guide to turn an experiment into evidence: run a realistic workload, profile it, form a hypothesis,
change one thing, and compare it with its baseline over repeated runs. The framework runs a Pulsar cluster and
its workloads in Docker on a developer's machine; a run takes minutes once its images are built. The deliverable
is a reproducible conclusion with run reports, measurements and an explanation supported by profiles or metrics.
A faster run is useful only when the workload's delivery guarantees still hold.

This supplements the repository's [`AGENTS.md`](../../AGENTS.md). [`README.md`](README.md) is the tutorial;
read it before running or changing the tests. This guide is the agent's working reference, and the linked pages
describe the full interfaces. There is no automatic baseline comparison or performance regression verdict:
the agent must make and explain the comparison.

The testing strategy is to simulate real-world use cases of Pulsar: a domain models a use case, currently IoT
telemetry, and each scenario sets its scale so that it maps to a kind of real-world deployment. This keeps the tests
from becoming synthetic: unlike a `pulsar-perf` producer and consumer pair, a scenario exercises the features that
real deployments use together and checks their delivery guarantees, so its improvements are likely to carry over to
real-world use. Express experiments and new scenarios in the domain's terms, as
[How the tests work](README.md#how-the-tests-work) describes.

## Experiment loop

1. **Define the question.** Record the baseline and candidate revisions, the scenario and overrides, the expected
   effect, and the measure that would support or refute it. Express the workload in domain terms: devices,
   gateways, topics, applications and pods. Use the maintained [scenarios](scenarios/docs/iot-telemetry.md) before
   inventing a new one. For a rate-limited scenario, compare latency, backlog and resource use at the same offered
   load; throughput may stay at that limit even after an improvement. A capacity experiment needs an explicit
   rate sweep or unlimited rate, with the same settings on both revisions. For unlimited rate, set
   `workloads.iotTelemetry.rate=0`, a positive `workloads.iotTelemetry.measurement.messages`,
   `workloads.iotTelemetry.warmup.seconds=0` and a positive `workloads.iotTelemetry.warmup.messages`;
   the inherited time-based warmup needs a positive rate. See the [workload settings](scenarios/docs/iot-telemetry.md#settings).
2. **Prepare and establish a baseline.** Complete [Before running](#before-running), keep one shared reports root,
   and use separate worktrees and Docker tags as [Comparing revisions](docs/comparing-revisions.md) describes.
   Both revisions must use the same test harness, resolved scenario, warmup, measurement size, memory limits and
   metrics settings, except for the setting under test. Record dirty changes as a patch outside the source tree;
   a commit ID and a dirty flag alone cannot reproduce them.
3. **Measure without profiling.** Alternate baseline and candidate runs, one at a time. Start with at least three
   valid runs of each, then add repetitions if the spread leaves the question unresolved. Check correctness and
   the host state before using each run. Keep excluded runs and the reason for excluding them in the notes.
4. **Explain the result.** Make separate profiled runs of the relevant components with identical profiler options
   on both revisions. Use the off-CPU digest for waiting, CPU stacks for busy code, allocation stacks and JFR for
   allocation and GC, and metrics for the broker, bookies and ZooKeeper. Follow [Analysis tools](#analysis-tools).
   If the task is to find a bottleneck, do this before choosing a candidate change.
5. **Test one hypothesis.** Connect the observed bottleneck to a code path or setting, state why the proposed
   change should help, and make the change when implementation is in the user's task. Run the relevant correctness
   tests, then repeat the unprofiled comparison. Investigate a regression or an inconclusive result before trying
   another idea; do not stack unmeasured changes. A profile identifies a lead, not proof of a speedup.
6. **Report and stop at the experiment's scope.** Deliver the evidence described in
   [Experiment handoff](#experiment-handoff), including negative or inconclusive results. Keep run outputs outside
   source control. Restore host settings and stop services that this experiment started, within the user's
   authorization; leave a pre-existing metrics stack running.

## Eliminating bottlenecks

Use this mode when the task is to raise a scenario's capacity rather than to validate one change. A system has one
limiting stage at a time; removing it moves the limit somewhere else, so the fastest progress comes from short
iterations that each find the current limit, remove it and look again. Validate a change deeply only once it has
shown that it moves the limit.

1. **Find the limiting stage.** Profile the scenario (`profile` with `--extends configs/profile-broker`) and look
   for, in this order:
   - a single thread that is busy all the time, such as a topic's managed-ledger thread, which is a serial stage
     that no other headroom helps: split the CPU samples by thread as
     [Per-thread CPU](docs/analyzing-profiles.md#per-thread-cpu) describes and compare each thread's busy share
     with 100 %;
   - blocked time with an application frame in the off-CPU digest, such as a contended lock or monitor;
   - run-queue time in the off-CPU capture, which means that the host has run out of CPU;
   - a saturated resource outside the broker, in the bookies' and the host's metrics: storage flushes and throttled
     writes, the journal, the network.
2. **Measure per unit of work.** Compare CPU samples and blocked time per million messages, per thread and per
   thread pool, between runs; the throughput of a saturated scenario follows the cost per message of its serial
   stage. Prefer changes that take work off the limiting stage, or remove it, over micro-optimizations elsewhere.
   Reduce allocation only when garbage collection or allocation is a measurable part of the limiting stage's CPU.
3. **Make one change and screen it quickly.** One profiled run and one or two unprofiled runs per change are enough
   to see whether the limit moved; note which stage limits the new run. Keep the changes that help on an
   experiment branch and build on them; drop the ones that don't.
4. **Check the other entry sizes.** Entry size changes which stage limits the broker and the bookies: run the
   scenario with small entries (128 bytes) and with large unbatched ones, such as 8 KB and 128 KB. Scale the measured
   messages, the gateways' `maxOutstanding` and the workload containers' direct memory with the payload size.
   Batching is a client-side feature; for the broker and the bookies it only changes the entry size.
5. **Add observability when an assumption can't be checked.** When a profile suggests a cause that no existing
   metric shows, add a counter or a metric that does, in the experiment branch, and check the assumption with
   it in the next run.
6. **Record every iteration at a high level:** the limiting stage and its evidence, the change, the result, and
   where the limit moved. A stage outside Pulsar's control, such as the host's single disk under all the bookies,
   is a result too: record it and continue with the scenarios it does not limit.

Before proposing a change for review, validate it as the [Experiment loop](#experiment-loop) describes.

## Quick reference

Run commands from the repository root. Gradle properties (`-P...`) configure the task; launcher options go inside
`--args='...'`. The launcher tasks build their images and workload applications when needed, then run the experiment.

| Purpose | Command or input | Output / effect |
|---|---|---|
| Run an unprofiled measurement | `./gradlew :tests:performance:launcher:run --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'` | Prints the run directory, resolved settings, progress and report path; writes the [run artifacts](#the-run) |
| Profile selected components | `./gradlew :tests:performance:launcher:profile --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --extends configs/profile-broker'` | Adds JFR recordings, off-CPU captures, digests and flame graphs; use `profile`, since `run` rejects profiling options |
| Select more profiled components | Repeat `--extends configs/profile-gateways` and/or `--extends configs/profile-applications` inside `--args` of `profile` | Profiles producers and/or consumers; each workload container's recording covers all its gateways or applications |
| Change a setting for one run | Add `--set workloads.iotTelemetry.rate=5000` to `--args` | Applies after inheritance and environment overrides; inspect `resolved-config.yaml` to verify it |
| Group an experiment's runs | Add `--name <experiment>` to `--args` | Uses that name below the checkout's branch directory, which can be shared by both revisions; does not combine or compare reports |
| Keep revisions' images apart | Add `-Pdocker.tag=baseline` or `-Pdocker.tag=candidate` | Builds and uses a separate image tag in each checkout |
| Share a reports root | Add `-Pperformance.reportsDir=<absolute-directory>` to run and report-server commands | Keeps evidence outside worktrees; alternatively set `performance.reportsDir=/absolute/path` once in `~/.gradle/gradle.properties` |
| Keep logs for diagnosis | Add `-Pperformance.keepLauncherLog` | Retains `launcher.log` even on success; failures retain it by default |
| Start repetitions at a similar temperature | Add `-Pperformance.cooldownTemperature=<degrees-C>` | With Linux host sensors, waits before cluster startup and after warmup; keep the threshold identical across revisions and inspect timed-out waits in the report |
| Disable metrics | Add `-Pperformance.metrics=false` | Omits metrics collection; keep this choice identical across compared runs |
| Browse reports | `./gradlew :tests:performance:report-tool:serveReports` | Serves the configured reports root at <http://127.0.0.1:8000/> by default; stop the foreground server when finished |
| Query stored metrics after a run | `./gradlew :tests:performance:metrics:up` | Starts VictoriaMetrics and Grafana in the background, prints URLs; default ports are 8428 and 3000 |
| Stop that metrics stack | `./gradlew :tests:performance:metrics:down` | Stops containers while retaining metrics and dashboards in Docker volumes |
| Validate the host | `tests/performance/environment/scripts/configure-perf-test-environment.sh validate` | Checks Docker and disk space; on Linux also host tuning and AC power; errors go to stderr with a documented exit code |

[Running scenarios](docs/running-scenarios.md) lists every launcher option and Gradle property.
Relative `--extends` paths resolve against the scenario file's directory first, then the working directory;
`.yaml` can be omitted. Use an absolute path for a configuration outside those locations.
`./gradlew :tests:performance:launcher:run --args='--help'` prints the launcher's options, but its Gradle image-build
dependencies still run; it is not a side-effect-free preflight command.

For an A/B run, use this in the baseline checkout and repeat it in the candidate checkout with
`-Pdocker.tag=candidate`. Alternate them for the repetitions; do not run the commands concurrently:

```bash
./gradlew :tests:performance:launcher:run -Pdocker.tag=baseline \
  -Pperformance.reportsDir="$HOME/pulsar-performance-reports" \
  --args='--scenario tests/performance/scenarios/iot-telemetry.yaml --name my-change-ab'
```

Use the printed `Run directory:` as the exact run identity. By default the layout is
`<reports root>/<yyyy-MM-dd>/<branch>/<name>/<MM-dd-HH-mm-ss>/`, under `build/performance` if no root is configured.
`Run report:` names `index.html`; read the sibling `README.md` for text analysis. A report URL does not start the
HTTP server. Progress lines appear every 10 seconds and are copied to `console.log.txt`; they include warmup and
interval statistics, so use the report and summary files for measurement results. Avoid selecting a run merely
because it is the newest: associate each command, revision and resolved configuration with its printed path.
Check the commit and dirty state in `run-info.json`: a detached baseline can share the candidate's branch directory.
The report's delivered throughput divides measured messages by the time until the slowest application received
the last one; console receive rates and broker dispatch metrics sum deliveries across subscriptions. Do not
compare those as if they were the same measure.

## Before running

A run takes minutes and uses the whole host. Don't start runs, or run them alongside other runs or builds, unless the
user asked for them. Before a series of runs, check these:

| Check | How | When it fails |
|---|---|---|
| Memory available to Docker. Recommended headroom: 20 GB for Docker and 32 GB on the host | Scenario memory configurations: low about 3 GB; medium (default) about 11 GB; high about 14 GB; see [Memory configurations](scenarios/README.md#memory-configurations) | Tell the user if the scenario will not fit; the low-memory scenarios are for smaller runs, not profiling |
| Docker is available and has sufficient disk space; on Linux also fixed CPU frequency, stopped power-management daemons and AC power | [`configure-perf-test-environment.sh`](environment/scripts/configure-perf-test-environment.sh) `validate` checks Docker and disk on Linux and macOS, and host tuning only on Linux; failed checks print to stderr | Find the failed checks from its exit code in the table of [Exit codes](environment/README.md#exit-codes), and ask the user before doing what it says: for permission to run [`environment/scripts/docker-cleanup.sh`](environment/scripts/docker-cleanup.sh) when Docker's disk is too full, showing what it would remove with `--dry-run` first, and to configure the host, which needs `sudo` |
| Profiled scenario | Profile a scenario with the medium- or the high-memory configuration | Don't profile a scenario that uses the low-memory configuration |

Follow the repository's [build prerequisites](../../CONTRIBUTING.md#building) for Java and Docker.
`docker compose version` checks the Compose plugin needed by metrics. Collection is enabled by default: a run
reuses a running stack, or starts and stops one for itself. Missing metrics or failed panel rendering may only
produce a warning, so check `metrics.json` and the report before relying on that evidence.

Profiling also changes the environment: `profile` depends on `:tests:integration:tuneKernelPerfEvents`, which
runs a privileged container to change perf-event, BPF and transparent-huge-page settings in the Docker engine's
Linux kernel (the VM's kernel on macOS). Profiled containers run privileged with their JVM as root. Account for
this when preparing the host; these are not just image-build steps. When the kernel is already configured,
`-Pinttest.asyncprofiler.skipPerfEventTuning` skips the tuning task; the environment script's `start` sets that
property until `stop`. See [Profiling requirements](docs/profiling.md#requirements).

Variance sets the smallest change that a comparison can detect: when the spread between runs of the same revision is
larger than a change's effect, the effect can't be told apart from noise. When you'll be running experiments
repeatedly on Linux, you can suggest that the user sets up the sudoers rule that
[Running start and stop without a password](environment/README.md#running-start-and-stop-without-a-password)
describes. It lets you run `sudo /usr/local/sbin/configure-perf-test-environment.sh start` before the runs and `stop`
after them yourself; `install` still needs the user.
On a host with Linux temperature sensors, the optional cool-down gate can reduce thermal drift when fixed-frequency
tuning is unavailable; it does not replace checking throttling or run-to-run spread. It proceeds after
`--cooldown-timeout` (600 seconds by default), so inspect the report's Host section for whether the target was reached.

### Platforms

The cluster and the workloads run in Linux containers, so the tests run on any host with a Docker engine. The
README's [Before you start](README.md#before-you-start) lists the hosts that the tooling was tested on.

Linux x86_64 is Pulsar's main target platform, and on dedicated hardware a run has no noisy neighbours and less
thermal and power throttling and CPU frequency variance. On macOS and Windows, Docker runs in a virtual machine that
shares the host's CPUs, memory, disk and network with the host operating system, which schedules them, so the results
aren't representative of a Linux deployment and vary more between runs. Off-CPU profiling needs a kernel with BTF,
see [Profiling](docs/profiling.md#requirements); without it, profile with async-profiler and JDK Flight Recorder
only. Non-Linux hosts also lack the launcher's `host-stats.csv` evidence of temperature and throttling; a passing
Docker/disk validation does not fill that gap. Use non-Linux comparisons as exploratory results for that host:
name the host and Docker engine, repeat runs to establish their spread, and do not present the result as
representative of a Linux deployment. Confirm a deployment-performance claim on Linux x86_64. Never compare
runs made on different hosts.

## Using a run's results

- Find a run's results from the launcher's output, which prints the run directory and the run report.
- Use a run's measurement results only when its report shows a valid run, as [Read the report](README.md#2-read-the-report)
  describes: every application received every message, without ordering violations or invalid messages. Duplicates
  are counted and allowed by at-least-once delivery; the report flags them, but they do not alone fail the run.
  Report and investigate a change in duplicates rather than assuming the warning means a delivery failure.
- Don't claim a performance change from a single run: compare revisions as
  [Comparing revisions](docs/comparing-revisions.md) describes.
- A run that fails writes no report. Find the cause as [When a run fails](docs/run-reports.md#when-a-run-fails)
  describes, and tell the user rather than using the run.
- Don't use the measurement results of a run that wrote heap dumps: the dumps stop the JVM.
- Don't use a profiled run's measurement results for latency and throughput improvement claims; use comparisons
  between matching profiled runs to investigate the cause instead.
- Exclude runs with timeouts, out-of-memory errors, broker restarts or incomparable host throttling, even if clients
  recovered. Keep diagnostic evidence and record why the run was excluded. Missing host samples are not evidence
  that the host did not throttle.
- Don't add run output to the git repository.

## Inputs for analysis in a run directory

Prefer text artifacts — Markdown, JSON, CSV and collapsed stacks — to HTML pages, which need a browser to render.
Collapsed stacktrace files are also called folded stacktrace files. In the paths, `<component>` is a profiled
component's directory: `broker-profile`, `gateways` or `applications`. `<recording>` is the recording's basename
without `.jfr`, such as `inttest_profile_<commit>_<time>_<container>` or `profile-gateways-<time>` (the broker's
commit segment can be absent). Use the exact recording names linked from the profile report.
[Files of a run](docs/run-reports.md#files-of-a-run) and
[What a profiled run writes](docs/profiling.md#what-a-profiled-run-writes) describe every file.

### The run

| Input | What it has | Read it with |
|---|---|---|
| `README.md`, `index.html` | The run report: the settings, correctness, throughput, latency, backlog, host and metrics sections, and links to the profile reports | Read `README.md` |
| `run-info.json` | The run's start, host, Docker engine, git branch and commit, whether the checkout was dirty, and the Pulsar version; the cluster's image and version when it ran a released Pulsar | `jq` |
| `<scenario>.yaml`, `resolved-config.yaml` | The scenario as written, and with its inheritance and overrides applied | Read |
| `console.log.txt` | What the launcher printed, including the progress lines every 10 s | Read |
| `launcher.log` | Testcontainers' and the Pulsar containers' logs; kept when the run failed, or with `-Pperformance.keepLauncherLog` | `rg` |
| `gateways/gateways-summary.json` | The gateways' counts and throughput; `measurementMessages`, `measurementStartEpochMs` and `measurementEndEpochMs` identify the measured sends | `jq` |
| `applications/<application>/application-summary.json` | Each application's unique messages, duplicates, ordering violations and invalid messages; `lastMeasurementMessageReceivedEpochMs` records the last measured receipt | `jq` |
| `applications/<application>/ordering-violations.txt` | Samples of the ordering violations; empty in a valid run | Read |
| `gateways/container.log.txt`, `applications/container.log.txt` | The workload containers' logs | Read |
| `gateways/gateways-latency.hgrm`, `applications/<application>/application-latency.hgrm` | The publish and end-to-end latency percentile distributions, in milliseconds, as text | Read |
| `gateways/gateways-latency.hdr`, `applications/<application>/application-latency.hdr` | The latency interval logs | `renderHdrHistograms`, HistogramLogAnalyzer |
| `topic-stats.csv` | The broker's topic stats, sampled once per second: backlog and message counters per subscription | DuckDB, or any CSV reader |
| `host-stats.csv` | The host's CPU temperature, frequency and throttle counters, sampled once per second; on Linux only | DuckDB, or any CSV reader |
| `throughput.svg`, `backlog.svg`, `latency-percentiles.svg`, `latency-timeline.svg`, `host-temperature.svg`, `host-frequency.svg` | The report's charts, each with a PNG beside it | View the PNG |
| `metrics.json` | What querying the run's metrics needs: its label selector, time range, jobs and instances, and the URLs and credentials of VictoriaMetrics and Grafana, see [metrics.json](docs/metrics.md#metricsjson) | `jq`, then VictoriaMetrics' Prometheus API |
| `grafana-panels/*.png` | Panels of Grafana's dashboards over the run | View |

### Profiles

The following paths are relative to the profiled component's directory:

| Input | What it has | Read it with |
|---|---|---|
| `README.md`, `index.html` | The profile report: the measurement window, the recordings, and links to the digest and the flame graphs with their totals | Read `README.md` |
| `<recording>-offcpu/jonoffcpu-summary.md` | **Start here for waiting.** The off-CPU digest: the blocked time ranked by the Pulsar or BookKeeper method that waited, what it blocked on and for how long, and the capture's coverage; also as `.json` and `.html` | Read |
| `<recording>-offcpu/offcpu-no-idle.collapsed`, `offcpu-no-idle-app-root.collapsed` | The blocked call stacks without idle waits, the second from where threads entered Pulsar or BookKeeper code: one stack per line, frames separated by `;`, time in microseconds at the end | [DuckDB with the quack_flamegraph community extension](docs/analyzing-profiles.md#analyzing-collapsed-stacks-with-duckdb); also `rg`, `sort` |
| `<recording>-offcpu/offcpu.collapsed`, `offcpu-app-root.collapsed` | The same with every blocked interval, idle waits included | [DuckDB with the quack_flamegraph community extension](docs/analyzing-profiles.md#analyzing-collapsed-stacks-with-duckdb); also `rg`, `sort` |
| `<recording>-offcpu/offcpu*.json` | The totals of each slice, including the time that the idle filter removed | `jq` |
| `<recording>-offcpu/offcpu*.html` | The off-CPU flame graphs | A browser |
| `<recording>-offcpu/jonoffcpu-offcpu-profile.pb` | The stack profile, every distinct stack with its counters, from which other slices render without correlating again | The jonoffcpu correlator's `top`, `stacks` and `export` |
| `<recording>-offcpu/jonoffcpu-report.json` | The capture's accounting: the intervals recorded and matched, loss, switch-out reasons, and sleeping versus run-queue time | `jq` |
| `<recording>-offcpu/offcpu-idle-waits.txt`, `offcpu-dispatch-hide.txt` | The idle-wait patterns and the dispatch frames that the digest and the slices leave out or hide | Read |
| `<recording>-flamegraphs/<view>.collapsed` | The async-profiler views' stacks, `cpu`, `alloc`, and `wall` and `lock` when recorded, of the measurement window: one stack per line with its weight at the end | [DuckDB with the quack_flamegraph community extension](docs/analyzing-profiles.md#analyzing-collapsed-stacks-with-duckdb); also `rg`, `sort` |
| `<recording>-flamegraphs/<view>.html`, `<view>-threads.html`, `<view>-heatmap.html` | The flame graphs, split by thread, and over time for bursts and pauses | A browser |
| `<recording>.measurement.jfr` | The recording cut to the measurement window: async-profiler's CPU and allocation samples, and the JDK's events such as `jdk.JavaMonitorEnter`, `jdk.ThreadPark` and garbage collection | The Jafar MCP server, jafar-shell |
| `<recording>.jfr` | The complete recording, startup and shutdown included | The Jafar MCP server, jafar-shell; `runJfrCut` for another window |
| `<recording>.jonoffcpu-capture.pb`, `.jonoffcpu-capture.manifest.json`, `.jonoffcpu.yaml` | The off-CPU capture stream, its manifest, and the agent's configuration | The jonoffcpu correlator, to correlate again, such as with `--audit full` |

Off-CPU time isn't in the JFR recordings: read it from `<recording>-offcpu/`. Don't use `jfr summary` as a measure
of a recording's completeness: it counts async-profiler's samples, which are in their own chunks, as zero.

### Heap dumps

| Input | What it has | Read it with |
|---|---|---|
| `heap-dumps/heap-dumps.csv` | Every dump that the launcher wrote: when, of which JVM, what triggered it, the file, and the heap usage before it | DuckDB, or any CSV reader |
| `heap-dumps/<component>/*.hprof`, `*.hprof.gz` | The heap dumps, such as `heap-dumps/broker/broker-0-peak.hprof`, see [Heap dumps](docs/heap-dumps.md) | jafar-shell, after `gunzip -k` for a `.hprof.gz`; the `jafar-perf` plugin; mcp-mat |

## Analysis tools

| Tool | Reads | Use it for | Get it |
|---|---|---|---|
| [Jafar MCP server](https://github.com/btraceio/jafar/blob/main/jfr-mcp/README.md) | `.jfr` | Querying a recording from an agent: `jfr_diagnose` and `jfr_stackprofile` first, then the other Jafar tools | `claude mcp add jafar -- jbang jfr-mcp@btraceio --stdio`, with JBang and JDK 25+, see [AI agent analysis](docs/analyzing-profiles.md#ai-agent-analysis) |
| [`jafar-perf`](https://github.com/btraceio/jafar-perf-box/tree/main/plugins/jafar-perf) Claude Code plugin | `.jfr`, `.hprof` | Guided analysis from triage to a report, comparing recordings and heap dumps, and investigating memory leaks; it registers the Jafar MCP server itself | [The plugin's README](https://github.com/btraceio/jafar-perf-box/blob/main/plugins/jafar-perf/README.md); suggest it to the user when the Jafar tools aren't available |
| [jafar-shell](https://github.com/btraceio/jafar) | `.jfr`, `.hprof` | Queries in JfrPath and HdumpPath, read from standard input so that an agent can script them | `jbang app install jafar-shell@btraceio`, see [Interactive analysis with jafar-shell](docs/analyzing-profiles.md#interactive-analysis-with-jafar-shell) |
| jonoffcpu correlator | `jonoffcpu-offcpu-profile.pb`, the capture stream | Ranking the blocked time (`top`), comparing two profiles (`top --baseline`), rendering other slices (`stacks`), and exporting stacks for SQL (`export --format jsonl`) | `./gradlew :tests:performance:report-tool:runJonoffcpuCorrelator --args='--help'`; no separate installation |
| jfr-converter | Collapsed stacks, `.jfr` | Rendering a flame graph of a slice or recording | `./gradlew :tests:performance:report-tool:runJfrConverter --args='--help'`; no separate installation |
| [codelipenghui/mcp-mat](https://github.com/codelipenghui/mcp-mat) | `.hprof` | Eclipse Memory Analyzer's leak suspects report, dominator tree, paths to GC roots and OQL | See [Heap dumps and memory leaks](docs/analyzing-profiles.md#heap-dumps-and-memory-leaks) |
| `./gradlew :tests:performance:launcher:runJfrCut` | `.jfr` | Cutting a recording to another interval | See [Cutting a recording yourself](docs/profiling.md#cutting-a-recording-yourself) |
| `./gradlew :tests:performance:report-tool:renderHdrHistograms` | `.hdr` | Plotting a run's latency charts again | See [Latency logs](docs/run-reports.md#latency-logs) |
| `./gradlew jfrFlamegraphs` | `.jfr` made elsewhere | Flame graphs of recordings of profiled tests, integration tests and the JMH microbenchmarks | See [Flame graphs of other recordings](docs/analyzing-profiles.md#flame-graphs-of-other-recordings) |
| VictoriaMetrics' Prometheus API | `metrics.json` | Querying a run's broker, bookie and ZooKeeper metrics over time | The metrics stack, see [metrics.json](docs/metrics.md#metricsjson) |
| Grafana's image renderer | `metrics.json` | Rendering a dashboard panel over a run as a PNG image | The metrics stack, see [Rendering panels as images](docs/metrics.md#rendering-panels-as-images) |
| [DuckDB](https://duckdb.org/) with [quack_flamegraph](https://github.com/kevintruong/quack-flamegraph) | Collapsed/folded stacktrace files; also CSV and JSONL | Automated stack analysis: rank stacks, methods and call edges, and attribute samples to application frames; export JSON or CSV for agents and scripts | Install DuckDB, then follow the [extension setup and usage examples](docs/analyzing-profiles.md#analyzing-collapsed-stacks-with-duckdb) |
| JDK Mission Control, IntelliJ IDEA, [HistogramLogAnalyzer](https://github.com/HdrHistogram/HistogramLogAnalyzer) | `.jfr`, `.hdr` | Interactive analysis by the user | See [Opening recordings in JDK Mission Control or IntelliJ IDEA](docs/analyzing-profiles.md#opening-recordings-in-jdk-mission-control-or-intellij-idea) |

The VictoriaMetrics and Grafana rows need the metrics stack to run: `./gradlew :tests:performance:metrics:up` starts
it in the background. Stop only a stack that you started, and ask the user before stopping one that was running.

### Start with a question, then select the input

The run and profile reports are generated automatically; inspect their text before installing extra tools or
rendering anything again. Replace the example paths below with the exact files from the run. Optional tools
augment those reports; their absence need not block analysis of the saved summaries and collapsed stacks.

| Question | Start with | Follow up / output |
|---|---|---|
| Did the workload complete correctly, and what changed? | Run `README.md`, `resolved-config.yaml`, `run-info.json` and workload summaries | Compare measured throughput, latency percentiles and backlog; inspect logs for exclusions |
| Where does a busy process wait? | `jonoffcpu-summary.md` and `offcpu-no-idle-app-root.collapsed` | The digest already ranks blocking callsites; use the optional correlator CLI for comparisons or other slices, and check coverage and loss in `jonoffcpu-report.json` |
| Where does it use CPU or allocate? | `cpu.collapsed`, `alloc.collapsed`, then `.measurement.jfr` | Thread views find serial bottlenecks; heatmaps and JFR place CPU, allocation and GC activity in time |
| Which thread limits the throughput? | `.measurement.jfr`, converted with `--threads` | Rank threads and pools and break down the busiest thread with DuckDB, see [Per-thread CPU](docs/analyzing-profiles.md#per-thread-cpu) |
| How can an agent query stack profiles automatically? | A `.collapsed` or `.folded` file | Use DuckDB with quack_flamegraph; the [SQL examples](docs/analyzing-profiles.md#analyzing-collapsed-stacks-with-duckdb) rank stacks, methods and call edges and export machine-readable JSON or CSV |
| Which cluster component explains a stall? | `metrics.json`, `grafana-panels/*.png`, `topic-stats.csv` | Use Grafana's run annotations and VictoriaMetrics queries to relate broker, bookie and ZooKeeper behavior to the measurement |
| What retains the heap? | `heap-dumps.csv` and an `.hprof` dump | Use dominators, retained sizes and paths to GC roots; capture a separate diagnostic run with `--extends configs/heap-dumps-broker` as [Heap dumps](docs/heap-dumps.md) describes |

### Off-CPU: rank and compare blocking callsites

The Gradle tasks resolve the correlator and converter at the `jonoffcpu` version in `gradle/libs.versions.toml`;
no JAR lookup or installation is needed. Both accept arbitrary CLI options through `--args` and resolve relative
paths from the repository root, without compiling Pulsar or starting a cluster. Use `--args='top --help'` for the
correlator's ranking options. For an existing broker profile:

```bash
offcpu_dir=/absolute/path/to/run/broker-profile/recording-offcpu
./gradlew -q :tests:performance:report-tool:runJonoffcpuCorrelator \
  --args="top --profile '$offcpu_dir/jonoffcpu-offcpu-profile.pb' \
  --app '^org\.apache\.(pulsar|bookkeeper)\.' \
  --waiting-from '$offcpu_dir/offcpu-idle-waits.txt' --package-names abbreviate"
```

This prints a ranking of blocked time by the application method that waited and the wait beneath it.
For a comparison, pass the candidate with `--profile` and the baseline with `--baseline`, and normalize each
with `--units` and `--baseline-units`, for example measured messages divided by one million. Derive those counts
from the summaries, exclude warmup, and state the unit. Use `--weights estimated` for proportional admission and
identical sampling options and filters in both runs; see [Comparing two profiles](docs/analyzing-profiles.md#comparing-two-profiles).
Blocked time sums across threads and can exceed elapsed wall time; a large idle wait is not evidence of contention.
`stacks` writes collapsed stacks for a selected slice, and `export --format jsonl` writes rows for SQL analysis;
the [slice commands](docs/analyzing-profiles.md#finding-what-to-optimize) include their inputs and output paths.

### JFR: open a measurement recording, then query it

When the Jafar MCP tools are available, use this call sequence; tool prefixes depend on the agent host:

1. `jfr_open` with `{"path":"/absolute/path/to/recording.measurement.jfr"}` returns a session ID.
2. `jfr_diagnose` with `{"sessionId":"<returned ID>"}` returns an initial diagnosis and suggested investigations.
3. `jfr_stackprofile` with `{"sessionId":"<returned ID>","eventType":"jdk.ExecutionSample","limit":30}`
   returns CPU frames, sample shares, time buckets and per-thread counts.
4. `jfr_query` with `{"sessionId":"<returned ID>","query":"events/jdk.GCPhasePause | top(10)","limit":10}`
   queries individual events; adapt the query to the diagnosis and inspect the available schema for other events.
5. `jfr_close` with `{"sessionId":"<returned ID>"}` releases that session.

Use `jdk.ExecutionSample` for CPU and async-profiler's `jdk.ObjectAllocationInNewTLAB` and
`jdk.ObjectAllocationOutsideTLAB` for allocations. JDK events such as `jdk.JavaMonitorEnter` and `jdk.ThreadPark`
are available when recorded with `jfrsync=profile`. These are not the kernel off-CPU capture.
Without MCP, the shell can read commands from stdin:

```bash
printf 'show events/jdk.ExecutionSample | count()\nexit\n' | jafar-shell -q /absolute/path/to/recording.measurement.jfr
```

This prints an event count, not a performance verdict. Save the investigated events, queries, evidence and
conclusion beside the recording as `<recording>.analysis.md`.

### Metrics: query the exact run and time interval

Start the metrics stack if needed. Its volumes retain the time series after the test cluster exits; `metrics.json`
is connection and query metadata, not a copy of those time series. From the run directory, this queries the
broker publish rate and returns Prometheus JSON containing timestamp/value series:

```bash
curl --get "$(jq -r .victoriaMetrics.prometheusApi metrics.json)query_range" \
  --data-urlencode "query=sum(pulsar_rate_in$(jq -r .selector metrics.json))" \
  --data-urlencode "start=$(jq -r '.startEpochMs / 1000' metrics.json)" \
  --data-urlencode "end=$(jq -r '.endEpochMs / 1000' metrics.json)" \
  --data-urlencode "step=$(jq -r .intervalSeconds metrics.json)"
```

Always apply the run's selector; an unfiltered query can mix several experiments. The example covers the full
scrape period, including startup and warmup. For the producer's measurement window, take `measurementStartEpochMs`
and `measurementEndEpochMs` from `gateways/gateways-summary.json`. To include delivery and draining, extend the
end to the largest `lastMeasurementMessageReceivedEpochMs` across the applications' summaries, as the profile
window does. State which interval the query uses. Divide these epoch milliseconds by 1000 for the API's `start`
and `end`; Grafana `from` and `to` use epoch milliseconds. The `events[].epochMs` values in `metrics.json` and
Grafana annotations provide the same boundaries: `warmup-finished` is the measurement start, `gateways-finished`
the publish end, and `applications-finished` the last measured receipt across applications.

Open `grafanaDashboard` from `metrics.json` for the run's dashboard. To discover other metrics or try PromQL,
use the VictoriaMetrics `ui` URL in that file (default <http://127.0.0.1:8428/vmui>); keep the same selector and
time range. The existing `grafana-panels/*.png` need no live stack. To render another panel, take its dashboard
UID and panel ID from Grafana's dashboard JSON and use the saved cluster label, time range and connection
settings as [Rendering panels as images](docs/metrics.md#rendering-panels-as-images) describes; the output is PNG.

## Experiment handoff

Keep one experiment note under the shared reports root, for example `experiments/<name>.md`, with links to its
run directories and any per-recording analyses. Give the user:

- The question, hypothesis and verdict: improvement, regression or inconclusive, limited to the workload and host
  tested. Include the code or setting changed and what the profiles suggest caused the result.
- Reproduction commands, baseline and candidate commits, any local patches, resolved scenario, image tags, host
  and Docker details, tuning state, metrics and profiler settings, and the order of the runs.
- Correctness outcomes, duplicate counts, excluded runs and their reasons. Distinguish unprofiled measurements
  from diagnostic runs with profiling or heap dumps.
- A table of baseline and candidate medians, spread (for example min–max), absolute and percentage changes, and
  which direction is better. State the number of valid repetitions. Report a latency percentile's median across
  runs as such; do not present it as the percentile of pooled messages. A difference within run-to-run spread
  is inconclusive, and three runs alone do not establish statistical significance.
- Links to the reports, relevant charts, profile callsites and metric queries that support the explanation,
  alongside limitations, missing evidence and the next experiment if the question remains open.

## Pull requests for performance improvements

When the user asks you to create a pull request, or update one, for a performance improvement that was tested with
these tests, suggest adding the charts of the comparison to the pull request's description as SVG images. Each run
directory has its charts as SVG, such as `throughput.svg` and `latency-percentiles.svg`, see
[Files of a run](docs/run-reports.md#files-of-a-run). Use the run of each revision whose numbers are closest to that
revision's medians, and suggest the charts that show the improvement. Add the charts only when the user agrees.

These instructions are for the `apache/pulsar` repository, when a human has prepared the change and asks you to
create the pull request. Never open or update a pull request in `apache/pulsar` without the human's explicit
confirmation: as [Licensing and provenance](../../AGENTS.md#licensing-and-provenance-read-first) in the repository's
agent guide states, every pull request must be submitted by a human contributor who has reviewed and verified the
change and takes responsibility for it. Automated pull requests, such as those of a tuning experiment, may go to a
personal fork or another fork.

- Attach the charts with the `--attach` option of `gh pr create` and `gh pr edit`, which recent versions of the
  GitHub CLI have. Check its details with `gh pr create --help` or `gh pr edit --help`, and if the installed `gh`
  lacks the option, tell the user.
- The charts of every run have the same file names, so copy them to names that say the revision, such as
  `baseline-throughput.svg` and `candidate-throughput.svg`, before attaching them.
- Give each image an alt text that says what the chart shows and for which revision:
  `--attach './candidate-throughput.svg#Throughput with the change'`.
- `gh` appends an attachment to the end of the description, unless the description references the file with a
  Markdown image, such as `![Throughput with the change](candidate-throughput.svg)`, in which case it points that
  reference at the uploaded file. Put the references where the charts belong, such as in a table with a column for
  the baseline and one for the change.
- Then turn each Markdown image into an HTML image with a width, wrapped in a link to the uploaded file, by editing the
  description, for example with `gh pr view --json body` and `gh pr edit --body-file`:
  `<a href="<uploaded URL>"><img src="<uploaded URL>" alt="Throughput with the change" width="480"></a>`. Markdown
  images don't show properly in a table's columns, and the link lets the reader open a chart at its full size, which
  clicking an image doesn't do otherwise.
- The charts don't replace the numbers: state the medians of the measures and their changes in the text, as
  [Compare](docs/comparing-revisions.md#compare) describes.

## Changing the performance tests

- New scenarios, workload applications and profiling support belong to the standalone launcher, not to the
  deprecated TestNG runner under `tests/integration`.
- Keep the docs in sync with the code: when you change a launcher option, a Gradle property, a scenario key or the
  files a run writes, update the page that documents it and any corresponding examples in this guide.
- Run the unit tests with `./gradlew :tests:performance:common:test :tests:performance:tools:test
  :tests:performance:launcher:test :tests:performance:report-tool:test :tests:performance:metrics:test`. None of them
  starts a cluster or the metrics stack.
