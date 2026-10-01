# Agent guide for the integration tests

Supplemental guidance for AI coding assistants working on the integration tests, on top of the repository's
[`AGENTS.md`](../../AGENTS.md). [`tests/README.md`](../README.md) describes running and profiling them; read it before
running or changing them.

- Run single test classes with `--tests`. The full suite is heavy and slow, and runs in CI.
- The tests need Docker. `integrationTest` builds the test image when it is out of date; don't build it separately.
- For performance optimizations, use the performance tests rather than a profiled integration test, as
  [`tests/performance/AGENTS.md`](../performance/AGENTS.md) guides.

## Analysis tools

Use [DuckDB](https://duckdb.org/) with the
[quack_flamegraph](https://github.com/kevintruong/quack-flamegraph) community extension to query collapsed
stacktrace files, also called folded stacktrace files, as SQL tables. See the
[setup and usage examples](../performance/docs/analyzing-profiles.md#analyzing-collapsed-stacks-with-duckdb)
for ranking stacks, methods and call edges. Agents and scripts can export query results as JSON with `duckdb -json`
or CSV with `duckdb -csv -header` for automated analysis, rather than parsing terminal tables or flame graph HTML.
