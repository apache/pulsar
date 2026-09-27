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
real-world use. Express experiments and new scenarios in the domain's terms, as "How the tests work" in the README
describes.

## Quick reference

Run the commands in the repository's root directory:

| Task | Command |
|---|---|
| Check the host, without root | `tests/performance/environment/scripts/configure-perf-test-environment.sh validate` |
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
| Start and stop the metrics stack, VictoriaMetrics and Grafana | `./gradlew :tests:performance:metrics:up`, `./gradlew :tests:performance:metrics:down` |
| Keep `launcher.log` of a successful run | `-Pperformance.keepLauncherLog` |
| Run without collecting metrics | `-Pperformance.metrics=false` |

A run's directory has its report, `README.md` and `index.html`, what the launcher printed, `console.log.txt`, and,
when it collected metrics, `metrics.json`. [Running scenarios](docs/running-scenarios.md) lists every launcher option
and Gradle property.

## Running experiments

- A run takes minutes and uses the whole host. Don't start runs, or run them alongside other runs or builds, unless
  the user asked for them.
- The performance tests run on Linux and on macOS, and should on Windows with WSL 2 too, since the cluster and the
  workloads run in Linux containers. Profiling with async-profiler, jonoffcpu's off-CPU profiling and JDK Flight
  Recorder works on both Linux and macOS, and was also tested on macOS arm64 with the
  [OrbStack](https://orbstack.dev/) Docker engine; [Docker Desktop](https://www.docker.com/products/docker-desktop/) and
  [Podman Desktop](https://podman-desktop.io/) are untested, see the tested Docker engines in the README's
  [Before you start](README.md#before-you-start). Linux
  x86_64 is recommended for measurements: it is Pulsar's main target platform, and on dedicated hardware configured
  for performance testing a run has no noisy neighbours and less thermal and power throttling and CPU frequency
  variance. On macOS and Windows, Docker runs in a virtual machine that shares the host's CPUs, memory, disk and
  network with the host operating system, which schedules them, so the results aren't representative of a Linux
  deployment and vary more between runs. Use such runs to check that a
  scenario works, or for large effects, and say that a result comes from a non-Linux host when reporting it; don't
  compare revisions on one, and never compare runs made on different hosts.
- On a Linux host, `configure-perf-test-environment.sh` reduces the run-to-run variance: it fixes the CPU frequency and
  stops the daemons that change power settings during a run, see
  [`environment/README.md`](environment/README.md). Variance sets the smallest change that a comparison can detect:
  when the spread between runs of the same revision is larger than a change's effect, the effect can't be told apart
  from noise, so minor improvements and regressions go undetected, or noise is mistaken for them.
- Before a series of runs, check the host; this needs no root:

  ```bash
  tests/performance/environment/scripts/configure-perf-test-environment.sh validate
  ```

  It exits with 1 when a check failed, and prints the reasons to stderr:
  - When the disk is too full, ask the user for permission to run
    [`environment/scripts/docker-cleanup.sh`](environment/scripts/docker-cleanup.sh), which removes the Pulsar
    images and unused Docker data. Show what it would remove with `--dry-run` first, and run it only once the user
    has allowed it.
  - When the host isn't configured, ask the user to configure it as
    [`environment/README.md`](environment/README.md) instructs, which needs `sudo`. When you'll be running
    experiments repeatedly, you can suggest that the user sets up the sudoers rule that
    [`environment/README.md`](environment/README.md#running-start-and-stop-without-a-password) describes. It lets
    you run `sudo /usr/local/sbin/configure-perf-test-environment.sh start` before the runs and `stop` after them
    yourself, without a password; `install` still needs the user.
- 32 GB of RAM on the host is recommended, although testing may be possible with less. Check that the host has the
  memory that the scenario's memory configuration needs available to Docker, which on macOS and Windows is the memory
  of Docker's virtual machine: about 3 GB for the low-memory configuration, 11 GB for the default medium-memory one
  and 14 GB for the high-memory one, see [Memory configurations](scenarios/README.md#memory-configurations). When it hasn't, tell the user rather than
  starting the run. Don't profile a scenario that uses the low-memory configuration.
- Use a run's numbers only when its report shows a valid run, as "Read the report" in the README describes, and don't
  claim a performance change from a single run: compare revisions as `docs/comparing-revisions.md` describes.
- Find a run's results from the launcher's output, which prints the run directory and the run report. A run that
  fails writes no report; find the cause as "When a run fails" in `docs/run-reports.md` describes, and tell the user
  rather than using the run. Don't commit run output.

## Tuning experiments

An experiment tests one change: a code change, or a setting in a scenario. State what it is expected to improve,
compare the change with its baseline on the same scenario, and find the cause of a difference in the profiles, as
`docs/analyzing-profiles.md` describes. Save an analysis beside its recording as `<recording>.analysis.md`, and keep
the scenario, the revisions, the runs and the conclusion together. Treat automated analysis as a lead, and confirm a
claim with a controlled comparison, a JMH benchmark or a second profile.

For the time that threads spent blocked, look first at a profile's jonoffcpu report,
`<recording>-offcpu/jonoffcpu-summary.md`. It is text, a digest that ranks the methods of the Pulsar broker's or the
Pulsar client's code where threads blocked, what they blocked on and for how long, see
[What a profiled run writes](docs/profiling.md#what-a-profiled-run-writes). For the full call trees behind it, read
the collapsed stacks beside it rather than the flame graphs, whose HTML pages need a browser to render:
`offcpu-no-idle.collapsed` has the blocked time and `offcpu-no-idle-app-root.collapsed` the same from where threads
entered Pulsar or BookKeeper code, one stack per line with its frames separated by `;` and its time in microseconds
at the end. The async-profiler views have collapsed stacks too, such as `<recording>-flamegraphs/cpu.collapsed`.

Analyze JFR recordings and `.hprof` heap dumps with the Jafar tools. When they aren't available, suggest that the user
installs the `jafar-perf` Claude Code plugin from [jafar-perf-box](https://github.com/btraceio/jafar-perf-box), which
adds analysis skills and agents and registers the Jafar MCP server;
[the plugin's README](https://github.com/btraceio/jafar-perf-box/blob/main/plugins/jafar-perf/README.md) describes how
to install and use it. Agents other than Claude Code can use the Jafar MCP server on its own, as
[AI agent analysis](docs/analyzing-profiles.md#ai-agent-analysis) describes. For heap dumps, see also
[Heap dumps and memory leaks](docs/analyzing-profiles.md#heap-dumps-and-memory-leaks). To run queries yourself, use
jafar-shell, which reads its commands from standard input, as
[Interactive analysis with jafar-shell](docs/analyzing-profiles.md#interactive-analysis-with-jafar-shell) describes.

When a component's memory is in question, such as a heap that fills up, backlogs that grow at a modest rate, or an
`OutOfMemoryError`, capture heap dumps with the scenario's `heapDumps` settings, such as
`--extends configs/heap-dumps-broker`, as [Heap dumps](docs/heap-dumps.md) describes, and don't use that run's numbers:
the dumps stop the JVM.

A run collects the metrics of its brokers, bookies and ZooKeeper into VictoriaMetrics, as [Metrics](docs/metrics.md)
describes. To look at them, or at a run's metrics over time beyond the run report's charts, the metrics stack has to
run: `./gradlew :tests:performance:metrics:up` starts it in the background, and `./gradlew
:tests:performance:metrics:down` stops it. Stop only a stack that you started, and ask the user before stopping one
that was running. `metrics.json` in the run directory has what you need
to drill down, as [metrics.json](docs/metrics.md#metricsjson) describes: VictoriaMetrics' Prometheus API, the run's
label selector, time range, jobs and instances, and Grafana's URL, API credentials and data source. Query
VictoriaMetrics with it, and render a Grafana panel as a PNG image through Grafana's API, as
[Rendering panels as images](docs/metrics.md#rendering-panels-as-images) describes.

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
