# Agent guide for the performance tests

Supplemental guidance for AI coding assistants working on the performance tests, on top of the repository's
[`AGENTS.md`](../../AGENTS.md). The performance tests are for running performance test experiments, and for
automating them, including tuning Pulsar by AI agents. [`README.md`](README.md) is a tutorial for running a scenario,
reading its report, profiling a run and comparing revisions, and its Reference section lists the pages with the
details; read it before running or changing the tests.

## Running experiments

- A run takes minutes and uses the whole host. Don't start runs, or run them alongside other runs or builds, unless
  the user asked for them.
- Before a series of runs, check the host; this needs no root:

  ```bash
  tests/performance/environment/scripts/configure-perf-test-environment.sh validate
  ```

  It exits with 1 when a check failed, and prints the reasons to stderr:
  - When the disk is too full, ask the user for permission to run `environment/scripts/docker-cleanup.sh`, which
    removes the Pulsar images and unused Docker data. Show what it would remove with `--dry-run` first, and run it
    only once the user has allowed it.
  - When the host isn't configured, ask the user to configure it as
    [`environment/README.md`](environment/README.md) instructs, which needs `sudo`. When you'll be running
    experiments repeatedly, you can suggest that the user sets up the sudoers rule that
    [`environment/README.md`](environment/README.md#running-start-and-stop-without-a-password) describes. It lets
    you run `sudo /usr/local/sbin/configure-perf-test-environment.sh start` before the runs and `stop` after them
    yourself, without a password; `install` still needs the user.
- Use a run's numbers only when its report shows a valid run, as "Read the report" in the README describes, and don't
  claim a performance change from a single run: compare revisions as `docs/comparing-revisions.md` describes.
- Find a run's results from the launcher's output, which prints the run directory and the run report. Don't commit
  run output.

## Tuning experiments

An experiment tests one change: a code change, or a setting in a scenario. State what it is expected to improve,
compare the change with its baseline on the same scenario, and find the cause of a difference in the profiles, as
`docs/analyzing-profiles.md` describes. Save an analysis beside its recording as `<recording>.analysis.md`, and keep
the scenario, the revisions, the runs and the conclusion together. Treat automated analysis as a lead, and confirm a
claim with a controlled comparison, a JMH benchmark or a second profile.

## Changing the performance tests

- New scenarios, workload applications and profiling support belong to the standalone launcher, not to the
  deprecated TestNG runner under `tests/integration`.
- Keep the docs in sync with the code: when you change a launcher option, a Gradle property, a scenario key or the
  files a run writes, update the page that documents it.
- Run the unit tests with `./gradlew :tests:performance:common:test :tests:performance:tools:test
  :tests:performance:launcher:test :tests:performance:report-tool:test`. None of them starts a cluster.
