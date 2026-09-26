# Agent guide for the performance tests

Supplemental guidance for AI coding assistants working in `tests/performance`, on top of the repository's
[`AGENTS.md`](../../AGENTS.md). The detail lives in the human-facing docs; read the one that fits the task.

## Where to look

| Task | Read |
|---|---|
| Run a scenario, read its report, profile it, compare revisions | [`README.md`](README.md), the tutorial |
| Launcher options, Gradle properties, where runs are written | [`docs/running-scenarios.md`](docs/running-scenarios.md) |
| The files of a run and what they contain | [`docs/run-reports.md`](docs/run-reports.md) |
| Write or change a scenario | [`scenarios/README.md`](scenarios/README.md), [`scenarios/docs/scenario-format.md`](scenarios/docs/scenario-format.md) |
| Profiling options and the files of a profiled run | [`docs/profiling.md`](docs/profiling.md) |
| Find what to optimize from a profile | [`docs/analyzing-profiles.md`](docs/analyzing-profiles.md) |
| A/B comparison of two revisions | [`docs/comparing-revisions.md`](docs/comparing-revisions.md) |
| Configure the host, free Docker disk space | [`environment/README.md`](environment/README.md) |

## Running scenarios

- **A run takes minutes and uses the whole host.** Don't start runs, or run them concurrently with other runs or
  builds, unless the user asked for them. Before a series of runs, check that the disk that holds Docker's data is
  less than 90 % full (bookies go read-only at 95 %), and that the host is configured for consistent results:
  `thermald` isn't running and turbo is off, as `environment/scripts/configure-perf-test-environment.sh start` sets
  them. That script needs `sudo`; ask the user to run it rather than running it yourself.
- **Find the results from the launcher's output**: it prints `Run directory:` when a run starts and `Run report:`
  when it has finished, and a profiled run's flame graphs and profile reports are in that run directory. The layout
  is in [`docs/run-reports.md`](docs/run-reports.md#layout-of-a-run-directory).
- **Check a run's validity before using its numbers**: the report's Correctness section must show no ordering
  violations or invalid messages, and the Host section no thermal throttling.
- **Don't claim a performance change from a single run.** Compare the same scenario with the same settings on both
  revisions, alternate the runs and repeat them, and report medians with their spread, as
  [`docs/comparing-revisions.md`](docs/comparing-revisions.md) describes.
- **Don't commit run output.** Runs are written under `build/performance` or `performance.reportsDir`, outside the
  sources.

## Analyzing profiles

- Start from the run report, the broker's profile report (`broker-profile/index.html`) and the off-CPU digest it
  links to. Analyze `<recording>.measurement.jfr` rather than the complete recording, which includes startup and
  warmup.
- Off-CPU time isn't in the JFR recordings; it is in the digest and the correlator's stack profile, see
  [`docs/analyzing-profiles.md`](docs/analyzing-profiles.md).
- Save an analysis beside the recording as `<recording>.analysis.md`. Treat automated analysis as a lead, and confirm
  a claim with a controlled comparison, a JMH benchmark or a second profile.

## Changing the performance tests

- New scenarios, workload applications and profiling support belong to the standalone launcher, not to the
  deprecated TestNG runner under `tests/integration`.
- Keep the docs in sync with the code: when you change a launcher option, a Gradle property, a scenario key or the
  files a run writes, update the page that documents it, and the scenario tables in `scenarios/README.md` when you
  add a scenario.
- Run the unit tests with `./gradlew :tests:performance:common:test :tests:performance:tools:test
  :tests:performance:launcher:test :tests:performance:report-tool:test`. None of them starts a cluster.
