# Agent guide for the performance tests

Supplemental guidance for AI coding assistants working on the performance tests, on top of the repository's
[`AGENTS.md`](../../AGENTS.md). The performance tests are for running performance test experiments, and for
automating them, including tuning Pulsar by AI agents. [`README.md`](README.md) is a tutorial for running a scenario,
reading its report, profiling a run and comparing revisions, and its Reference section lists the pages with the
details; read it before running or changing the tests.

The testing strategy is to simulate real-world use cases of Pulsar: a domain models a use case, currently IoT
telemetry, and each scenario sets its scale so that it maps to a kind of real-world deployment. This keeps the tests
from becoming synthetic: unlike a `pulsar-perf` producer and consumer pair, a scenario exercises the features that
real deployments use together and checks their delivery guarantees, so its improvements are likely to carry over to
real-world use. Express experiments and new scenarios in the domain's terms, as
[How the tests work](README.md#how-the-tests-work) describes.

## Quick reference

Run the commands in the repository's root directory:

| Task | Command |
|---|---|
| The launcher's options | `./gradlew :tests:performance:launcher:run --args='--help'` |
| Run a scenario | `./gradlew :tests:performance:launcher:run --args='--scenario tests/performance/scenarios/iot-telemetry.yaml'` |
| Change a setting for one run | add `--set workloads.iotTelemetry.rate=5000` to `--args` |
| Group the runs of an experiment | add `--name <experiment>` to `--args` |
| Profile a run | `./gradlew :tests:performance:launcher:profile --args='--scenario tests/performance/scenarios/iot-telemetry-high-rate.yaml --extends configs/profile-broker'` |
| Build a revision's images apart from another's | `-Pdocker.tag=<revision>` before `--args` |
| Set the reports root for every checkout | `mkdir -p ~/.gradle && echo "performance.reportsDir=$HOME/pulsar-performance-reports" >> ~/.gradle/gradle.properties` |
| Set the reports root for one command | `-Pperformance.reportsDir=<absolute directory>` |
| Find the newest run | `ls -dt <reports root>/*/*/*/*/ \| head -n 1`, with `build/performance` as the reports root when none is set |
| Serve the reports over HTTP | `./gradlew :tests:performance:report-tool:serveReports`, at <http://127.0.0.1:8000/> |
| Start and stop the metrics stack, VictoriaMetrics and Grafana | `./gradlew :tests:performance:metrics:up`, `./gradlew :tests:performance:metrics:down`, with Grafana at <http://127.0.0.1:3000/> and VictoriaMetrics at <http://127.0.0.1:8428/> |
| Keep `launcher.log` of a successful run | `-Pperformance.keepLauncherLog` |
| Run without collecting metrics | `-Pperformance.metrics=false` |
| Check the host's configuration to ensure minimal run-to-run variance (optional, only on Linux), without root | `tests/performance/environment/scripts/configure-perf-test-environment.sh validate` |

[Running scenarios](docs/running-scenarios.md) lists every launcher option and Gradle property.

## Before running

A run takes minutes and uses the whole host. Don't start runs, or run them alongside other runs or builds, unless the
user asked for them. Before a series of runs, check these:

| Check | How | When it fails |
|---|---|---|
| Memory available to Docker, which on macOS and Windows is the memory of Docker's virtual machine; 32 GB of RAM on the host is recommended | The scenario's memory configuration needs about 3 GB (low), 11 GB (medium, the default) or 14 GB (high), see [Memory configurations](scenarios/README.md#memory-configurations) | Tell the user rather than starting the run |
| Disk space: the disk that holds Docker's data less than 90 % full | `configure-perf-test-environment.sh validate`, which exits with 1 and prints the reasons to stderr | Ask the user for permission to run [`environment/scripts/docker-cleanup.sh`](environment/scripts/docker-cleanup.sh), which removes the Pulsar images and unused Docker data; show what it would remove with `--dry-run` first |
| On Linux, the host's configuration for low run-to-run variance: a fixed CPU frequency, and no daemons that change power settings during a run | `configure-perf-test-environment.sh validate`, without root | Ask the user to configure the host as [`environment/README.md`](environment/README.md) instructs, which needs `sudo` |
| Profiling | Profile a scenario with the medium- or the high-memory configuration | Don't profile a scenario that uses the low-memory configuration |

Variance sets the smallest change that a comparison can detect: when the spread between runs of the same revision is
larger than a change's effect, the effect can't be told apart from noise. When you'll be running experiments
repeatedly on Linux, you can suggest that the user sets up the sudoers rule that
[Running start and stop without a password](environment/README.md#running-start-and-stop-without-a-password)
describes. It lets you run `sudo /usr/local/sbin/configure-perf-test-environment.sh start` before the runs and `stop`
after them yourself; `install` still needs the user.

### Platforms

The cluster and the workloads run in Linux containers, so the tests run on any host with a Docker engine, see the
tested Docker engines in the README's [Before you start](README.md#before-you-start):

| Host | Runs and profiling | Use its results for |
|---|---|---|
| Linux; x86_64 on dedicated hardware configured for performance testing is recommended | Yes, including jonoffcpu's off-CPU profiling | Measurements and comparing revisions: Linux x86_64 is Pulsar's main target platform, and on dedicated hardware a run has no noisy neighbours and less thermal and power throttling and CPU frequency variance |
| macOS, with the [OrbStack](https://orbstack.dev/) Docker engine, tested on arm64 | Yes, including jonoffcpu's off-CPU profiling | Checking that a scenario works, and large effects |
| macOS with [Docker Desktop](https://www.docker.com/products/docker-desktop/) or [Podman Desktop](https://podman-desktop.io/), and Windows with WSL 2 | Should work, untested | Checking that a scenario works, and large effects |

On macOS and Windows, Docker runs in a virtual machine that shares the host's CPUs, memory, disk and network with the
host operating system, which schedules them, so the results aren't representative of a Linux deployment and vary more
between runs. Say that a result comes from a non-Linux host when reporting it, don't compare revisions on one, and
never compare runs made on different hosts.

## Using a run's results

- Find a run's results from the launcher's output, which prints the run directory and the run report.
- Use a run's numbers only when its report shows a valid run, as [Read the report](README.md#2-read-the-report)
  describes.
- Don't claim a performance change from a single run: compare revisions as
  [Comparing revisions](docs/comparing-revisions.md) describes.
- A run that fails writes no report. Find the cause as [When a run fails](docs/run-reports.md#when-a-run-fails)
  describes, and tell the user rather than using the run.
- Don't use the numbers of a run that wrote heap dumps: the dumps stop the JVM.
- Don't commit run output.

## Inputs for analysis in a run directory

Prefer the text files, the Markdown, JSON, CSV and collapsed stacks, to the HTML pages, which need a browser to
render. In the paths, `<component>` is the directory of a profiled component, `broker-profile`, `gateways` or
`applications`, and `<recording>` its recording's name without `.jfr`, such as
`broker-profile/inttest_profile_<time>_<container>` or `gateways/profile-gateways-<time>`.
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
| `gateways/gateways-summary.json` | The gateways' counts and throughput, and the boundaries of the measurement in epoch milliseconds | `jq` |
| `applications/<application>/application-summary.json` | Each application's unique messages, duplicates, ordering violations and invalid messages | `jq` |
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

A profiled component's directory has these for each recording:

| Input | What it has | Read it with |
|---|---|---|
| `<component>/README.md`, `index.html` | The profile report: the measurement window, the recordings, and links to the digest and the flame graphs with their totals | Read `README.md` |
| `<recording>-offcpu/jonoffcpu-summary.md` | **Start here for waiting.** The off-CPU digest: the blocked time ranked by the Pulsar or BookKeeper method that waited, what it blocked on and for how long, and the capture's coverage; also as `.json` and `.html` | Read |
| `<recording>-offcpu/offcpu-no-idle.collapsed`, `offcpu-no-idle-app-root.collapsed` | The blocked call stacks without idle waits, the second from where threads entered Pulsar or BookKeeper code: one stack per line, frames separated by `;`, time in microseconds at the end | Read, `rg`, `sort` |
| `<recording>-offcpu/offcpu.collapsed`, `offcpu-app-root.collapsed` | The same with every blocked interval, idle waits included | Read, `rg`, `sort` |
| `<recording>-offcpu/offcpu*.json` | The totals of each slice, including the time that the idle filter removed | `jq` |
| `<recording>-offcpu/offcpu*.html` | The off-CPU flame graphs | A browser |
| `<recording>-offcpu/jonoffcpu-offcpu-profile.pb` | The stack profile, every distinct stack with its counters, from which other slices render without correlating again | The jonoffcpu correlator's `top`, `stacks` and `export` |
| `<recording>-offcpu/jonoffcpu-report.json` | The capture's accounting: the intervals recorded and matched, loss, switch-out reasons, and sleeping versus run-queue time | `jq` |
| `<recording>-offcpu/offcpu-idle-waits.txt`, `offcpu-dispatch-hide.txt` | The idle-wait patterns and the dispatch frames that the digest and the slices leave out or hide | Read |
| `<recording>-flamegraphs/<view>.collapsed` | The async-profiler views' stacks, `cpu`, `alloc`, and `wall` and `lock` when recorded, of the measurement window: one stack per line with its weight at the end | Read, `rg`, `sort` |
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
| jonoffcpu correlator | `jonoffcpu-offcpu-profile.pb`, the capture stream | Ranking the blocked time (`top`), comparing two profiles (`top --baseline`), rendering other slices (`stacks`), and exporting stacks for SQL (`export --format jsonl`) | `jonoffcpu-correlator.jar` from the [jonoffcpu releases](https://github.com/jonoffcpu/jonoffcpu/releases) of the `jonoffcpu` version in `gradle/libs.versions.toml`, see [Finding what to optimize](docs/analyzing-profiles.md#finding-what-to-optimize) |
| jfr-converter | Collapsed stacks | Rendering a flame graph of a slice | `jfr-converter.jar` from the same release |
| [codelipenghui/mcp-mat](https://github.com/codelipenghui/mcp-mat) | `.hprof` | Eclipse Memory Analyzer's leak suspects report, dominator tree, paths to GC roots and OQL | See [Heap dumps and memory leaks](docs/analyzing-profiles.md#heap-dumps-and-memory-leaks) |
| `./gradlew :tests:performance:launcher:runJfrCut` | `.jfr` | Cutting a recording to another interval | See [Cutting a recording yourself](docs/profiling.md#cutting-a-recording-yourself) |
| `./gradlew :tests:performance:report-tool:renderHdrHistograms` | `.hdr` | Plotting a run's latency charts again | See [Latency logs](docs/run-reports.md#latency-logs) |
| `./gradlew jfrFlamegraphs` | `.jfr` made elsewhere | Flame graphs of recordings of profiled tests, integration tests and the JMH microbenchmarks | See [Flame graphs of other recordings](docs/analyzing-profiles.md#flame-graphs-of-other-recordings) |
| VictoriaMetrics' Prometheus API | `metrics.json` | Querying a run's broker, bookie and ZooKeeper metrics over time | The metrics stack, see [metrics.json](docs/metrics.md#metricsjson) |
| Grafana's image renderer | `metrics.json` | Rendering a dashboard panel over a run as a PNG image | The metrics stack, see [Rendering panels as images](docs/metrics.md#rendering-panels-as-images) |
| [DuckDB](https://duckdb.org/) | CSV, and the correlator's JSONL export | SQL over the sampled stats and the off-CPU stacks | Install it |
| JDK Mission Control, IntelliJ IDEA, [HistogramLogAnalyzer](https://github.com/HdrHistogram/HistogramLogAnalyzer) | `.jfr`, `.hdr` | Interactive analysis by the user | See [Opening recordings in JDK Mission Control or IntelliJ IDEA](docs/analyzing-profiles.md#opening-recordings-in-jdk-mission-control-or-intellij-idea) |

The VictoriaMetrics and Grafana rows need the metrics stack to run: `./gradlew :tests:performance:metrics:up` starts
it in the background. Stop only a stack that you started, and ask the user before stopping one that was running.

## Tuning experiments

- An experiment tests one change: a code change, or a setting in a scenario. State what it is expected to improve,
  and compare the change with its baseline on the same scenario, as
  [Comparing revisions](docs/comparing-revisions.md) describes.
- Find the cause of a difference in the profiles, as [Analyzing profiles](docs/analyzing-profiles.md) describes:
  start from the off-CPU digest for waiting, and from the `cpu` and `alloc` collapsed stacks for CPU and allocation.
  Compare two profiles' blocked time with the correlator's `top --baseline`, as
  [Comparing two profiles](docs/analyzing-profiles.md#comparing-two-profiles) describes.
- When a component's memory is in question, such as a heap that fills up, backlogs that grow at a modest rate, or an
  `OutOfMemoryError`, capture heap dumps with the scenario's `heapDumps` settings, such as
  `--extends configs/heap-dumps-broker`, as [Heap dumps](docs/heap-dumps.md) describes.
- Save an analysis beside its recording as `<recording>.analysis.md`, and keep the scenario, the revisions, the runs
  and the conclusion together.
- Treat automated analysis as a lead, and confirm a claim with a controlled comparison, a JMH benchmark or a second
  profile.

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
  files a run writes, update the page that documents it.
- Run the unit tests with `./gradlew :tests:performance:common:test :tests:performance:tools:test
  :tests:performance:launcher:test :tests:performance:report-tool:test :tests:performance:metrics:test`. None of them
  starts a cluster or the metrics stack.
