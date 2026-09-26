# Agent guide for the microbenchmarks

Supplemental guidance for AI coding assistants working on the JMH microbenchmarks, on top of the repository's
[`AGENTS.md`](../AGENTS.md). [`README.md`](README.md) describes building, running and profiling the benchmarks; read
it before running or writing one.

- Run the benchmarks a question needs, selected with a pattern, rather than all of them.
- Treat results from other platforms as provisional until they are confirmed on Linux x86_64, as the README explains.
- A benchmark measures a single class or method. The effect of a change on a running cluster is what
  [the performance tests](../tests/performance/AGENTS.md) measure.
