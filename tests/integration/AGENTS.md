# Agent guide for the integration tests

Supplemental guidance for AI coding assistants working on the integration tests, on top of the repository's
[`AGENTS.md`](../../AGENTS.md). [`tests/README.md`](../README.md) describes running and profiling them; read it before
running or changing them.

- Run single test classes with `--tests`. The full suite is heavy and slow, and runs in CI.
- The tests need Docker. `integrationTest` builds the test image when it is out of date; don't build it separately.
- For performance optimizations, use the performance tests rather than a profiled integration test, as
  [`tests/performance/AGENTS.md`](../performance/AGENTS.md) guides.
