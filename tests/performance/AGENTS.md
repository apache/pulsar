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

## Running experiments

- A run takes minutes and uses the whole host. Don't start runs, or run them alongside other runs or builds, unless
  the user asked for them.
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

Analyze JFR recordings and `.hprof` heap dumps with the Jafar tools. When they aren't available, suggest that the user
installs the `jafar-perf` Claude Code plugin from [jafar-perf-box](https://github.com/btraceio/jafar-perf-box), which
adds analysis skills and agents and registers the Jafar MCP server;
[the plugin's README](https://github.com/btraceio/jafar-perf-box/blob/main/plugins/jafar-perf/README.md) describes how
to install and use it. Agents other than Claude Code can use the Jafar MCP server on its own, as
[AI agent analysis](docs/analyzing-profiles.md#ai-agent-analysis) describes. For heap dumps, see also
[Heap dumps and memory leaks](docs/analyzing-profiles.md#heap-dumps-and-memory-leaks).

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
- `gh` appends an attachment to the end of the description, unless the description references the file, such as
  `![Throughput with the change](./candidate-throughput.svg)`, in which case it points that reference at the uploaded
  file. Put the references where the charts belong, or, after attaching the files, move the image tags that `gh`
  added by editing the description, for example with `gh pr view --json body` and `gh pr edit --body-file`.
- The charts don't replace the numbers: state the medians of the measures and their changes in the text, as
  [Compare](docs/comparing-revisions.md#compare) describes.

## Changing the performance tests

- New scenarios, workload applications and profiling support belong to the standalone launcher, not to the
  deprecated TestNG runner under `tests/integration`.
- Keep the docs in sync with the code: when you change a launcher option, a Gradle property, a scenario key or the
  files a run writes, update the page that documents it.
- Run the unit tests with `./gradlew :tests:performance:common:test :tests:performance:tools:test
  :tests:performance:launcher:test :tests:performance:report-tool:test`. None of them starts a cluster.
