# Coding guidelines

Apache Pulsar follows the Sun Java Coding Conventions with additional project-specific rules. The
codebase is performance-critical, asynchronous, and concurrency-sensitive, so code review prioritizes
**correctness, thread safety, performance, maintainability, and backward compatibility**. This file is
the canonical coding reference for human contributors and AI coding agents; see [`AGENTS.md`](AGENTS.md)
for the agent-specific guardrails on top of it.

## Style

- **4 spaces** for indentation; **tabs must never be used**.
- Always use **curly braces**, even for single-line `if` statements.
- No `@author` tags in Javadoc.
- Every `TODO` must reference a GitHub issue, e.g. `// TODO: https://github.com/apache/pulsar/issues/XXXX`.
- Checkstyle config: `buildtools/src/main/resources/pulsar/checkstyle.xml`. Lombok is enabled.
  Spotless enforces license headers only (no code formatting); checkstyle covers style/whitespace.
  Both run on sources only (no compilation): `./gradlew quickCheck` runs the license-header and
  checkstyle checks across all modules, and `./gradlew sanityCheck` also compiles main + test — see
  [`CONTRIBUTING.md`](CONTRIBUTING.md#building).
- Prefer imports over fully qualified class names in code. Use a fully qualified class name only when
  needed to disambiguate a name collision that imports cannot resolve.

## Logging

- Prefer **[slog](https://github.com/merlimat/slog)** (`io.github.merlimat.slog`) via Lombok's
  **`@CustomLog`** (wired in `lombok.config` to `Logger.get(TYPE)`). **SLF4J is deprecated** for new
  code; never use `System.out` / `System.err`.
- **Default new logs to `TRACE`/`DEBUG`, not `INFO`** — Pulsar overuses `INFO` and floods production
  logs. Reserve `INFO` for low-frequency lifecycle/state-change events.
- Attach data as **structured attributes** — `log.info().attr("topic", topic).log("Published")` — not
  interpolated into the message string.
- For expensive `DEBUG`/`TRACE` values, don't guard with `isDebugEnabled()`/`isTraceEnabled()`; use
  slog's lazy form — `log.debug().attr("dump", () -> expensiveDump()).log("...")` or
  `log.debug(e -> e.attr("dump", expensiveDump()).log("..."))`.
- Avoid logging on hot paths, and stack traces at `INFO` or lower.
- Use `DEBUG` in a way where it could be enabled in production without causing too many log entries. Use `TRACE` for more detailed information.

## Asynchronous programming

Pulsar relies heavily on `CompletableFuture`; prefer it over `ListenableFuture` for new code.

- **A method returning `CompletableFuture` must not throw synchronously.** Propagate failures through
  the returned future — `return CompletableFuture.failedFuture(e);` — including for argument validation
  (`if (arg == null) return CompletableFuture.failedFuture(new IllegalArgumentException("arg"));`).
  Throwing *inside* a stage (`thenApply`, `thenCompose`, `handle`, `whenComplete`, …) is fine.

  Avoid (escapes synchronously; a caller chaining `.exceptionally(...)` never sees it):

  ```java
  CompletableFuture<T> process(String arg) {
      if (arg == null) {
          throw new IllegalArgumentException("arg");
      }
      return doProcessAsync(arg);
  }
  ```

  Prefer (report the validation failure through the returned future):

  ```java
  CompletableFuture<T> process(String arg) {
      if (arg == null) {
          return CompletableFuture.failedFuture(new IllegalArgumentException("arg"));
      }
      return doProcessAsync(arg);
  }
  ```
- **Never block on event-loop / async-execution threads** — no `Thread.sleep`, `Future.get()`,
  `CompletableFuture.join()`, or blocking IO. An operation that performs IO should return a future.
- **Avoid nested futures** (`CompletableFuture<CompletableFuture<T>>`); flatten with `thenCompose`.
  Prefer **`OrderedExecutor`** for ordered asynchronous work.

  Avoid (`thenApply` on a future-returning function yields `CompletableFuture<CompletableFuture<R>>`):

  ```java
  return firstAsync(arg).thenApply(v -> secondAsync(v));
  ```

  Prefer (`thenCompose` flattens it to `CompletableFuture<R>`):

  ```java
  return firstAsync(arg).thenCompose(v -> secondAsync(v));
  ```
- **Converting a synchronous-throwing method to a failed future is not mechanical** — some callers rely
  on the throw happening *before* the async work starts, so evaluate each call site. Use a shared
  `checkArgumentAsync` helper (in `FutureUtil`) to validate without duplicating try/catch.

- **Limit concurrency and handle backpressure.** Firing many async operations at once can overwhelm the
  system. Options:
  - **`com.spotify.futures.ConcurrencyReducer`** — caps in-flight futures at a configurable limit (used
    in the Admin client to bound concurrent requests per broker).
  - **`org.apache.pulsar.common.util.FutureUtil.Sequencer`** — runs async operations sequentially.
  - **`org.apache.pulsar.common.semaphore.AsyncSemaphoreImpl`** — a non-blocking semaphore with a
    per-operation cost that queues callers instead of failing when the limit is reached. Preferred over
    `ConcurrencyReducer` for request-driven cases that need a timeout on permit acquisition.

## Testing conventions

Most Pulsar **"unit tests"** (`src/test`, run with `./gradlew :<module>:test`) are actually
**integration-style** — they start a real in-JVM broker (`MockedPulsarServiceBaseTest` /
`pulsarTestContext`) rather than testing a class in isolation. The **container integration tests**
under `tests/` run against a Pulsar Docker image (see
[`CONTRIBUTING.md`](CONTRIBUTING.md#integration-tests)). Ideally code is factored so genuine units *can*
be unit-tested in isolation with light mocking — excessive mocking is a design smell, not the goal —
but much existing code isn't, so integration-style is the pragmatic default. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for how to *run* tests (groups, `--tests` scoping, retry count).

- **TestNG + Mockito.** Prefer **AssertJ** assertions (with descriptions) over TestNG asserts; use
  **Awaitility** for async conditions instead of `sleep` timing, with timeouts to prevent hangs.
  `untilAsserted(...)` retries assertions, `until(...)` waits for a boolean — don't swap them. Verify
  async interactions with Mockito `timeout(...)`, not fixed sleeps.
- Every feature or bug fix needs **deterministic** tests for edge and failure cases. A bug-fix test
  must **fail on the unpatched code for the real reason** — not because it forces internal state.
- For code not factored for isolation, prefer an integration-style test over mocking a web of
  collaborators: inject faults via the test infrastructure (e.g.
  `pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor(...)`) and assert on logs with
  `TestLogAppender`. It's fine to add a **clean new test class** rather than extend an awkward one.
- **No reflection into private state** (`WhiteboxImpl.getInternalState`/`setInternalState`,
  `setAccessible(true)`). Expose a **package-private `@VisibleForTesting`** accessor and put the test in
  the same package; flag new reflection in review ([dev@ rationale](https://lists.apache.org/thread/7gr04sqmzyttx4ln6ydtp3qv0xgo1o6m)).
- **New integration-style tests: extend `SharedPulsarBaseTest`.** It shares one `SharedPulsarCluster`
  for the test-JVM lifecycle (`admin` / `pulsarClient` are per test class); each method gets its own
  namespace. Use `getNamespace()` and `newTopicName()` — never hardcode namespace/topic names, since
  the runtime is shared.
- **Close/release what the test allocates.** A **`ByteBuf`/buffer leak** (pooled-allocator detection,
  `-Dpulsar.allocator.pooled=true`) is a **real bug** — fix the missing `release()`. A **thread leak
  from `ThreadLeakDetectorListener` is unreliable** (high false-positive rate, notably with
  `SharedPulsarBaseTest` and when `THREAD_LEAK_DETECTOR_WAIT_MILLIS` is too low — ≈`10000` recommended,
  only effective with the Gradle daemon disabled, `--no-daemon`); corroborate before treating it as
  real.
- **Validate performance optimizations with a JMH benchmark** under `microbench/`, simulating a
  realistic production usage pattern (see `microbench/README.md`).

## General recommendations

- **Use the narrowest interface type** for fields, parameters, variables, and returns (`Map`,
  `SequencedMap`, `SortedMap`, `Collection`, `List`) rather than a concrete type like `TreeMap`. Keep
  the concrete type only where its behaviour is required (e.g. a `TreeMap` for key-ordered iteration),
  still exposed through the interface.
- **Minimize method and constructor parameters.** For a constructor with many parameters,
  use a **builder** — the project uses Lombok `@Builder` for most internal classes, and it works on a
  `record` too. Consider refactoring by moving related methods to a separate class when it's a better fit.
- **Don't return generic tuples.** Instead of `org.apache.commons.lang3.tuple.Pair<L, R>` (or a similar
  tuple type), define a small, purpose-named **Java `record`** inline in the class that declares the
  method, with the **same visibility as the method** (`public`, package-private, or `private`).

  Avoid (positional and untyped; call sites read `getLeft()` / `getRight()`):

  ```java
  private Pair<Integer, Integer> minMax(Collection<Integer> values) { ... }
  ```

  Prefer (a purpose-named record with the same visibility as the method):

  ```java
  private record MinMax(int min, int max) {}
  private MinMax minMax(Collection<Integer> values) { ... }
  ```
- **Prefer record keys over concatenated strings.** For a composite `Map` key, use a small `record`
  instead of concatenating a `String` (e.g. `a + ":" + b`) — correct `equals`/`hashCode`, type-safe,
  no delimiter/escaping bugs.

  Avoid (delimiter collisions when a value contains `:`; no type safety):

  ```java
  Map<String, V> map = new HashMap<>();
  map.get(a + ":" + b);
  ```

  Prefer (a small record key with correct `equals`/`hashCode`):

  ```java
  record Key(String a, String b) {}
  Map<Key, V> map = new HashMap<>();
  map.get(new Key(a, b));
  ```
- **Don't use `@Builder` on public client-API classes** (harder to maintain backwards compatibility) — hand-write the builder.
- **Name methods for intent.** A method's name should reveal what it does. Query methods read like
  queries (`shouldSkipChunk`, not `skipChunk`); methods that mutate state or perform an action are
  named for that action. **Reserve the `get` prefix for pure queries** — using it for a method that
  mutates state, or otherwise does more than return a value is strongly discouraged.
 
## Dependencies

Prefer existing dependencies over new libraries. Pulsar commonly uses Apache Commons / Guava
(utilities), **FastUtil** (type-specific collections), **JCTools** (concurrent structures),
**RoaringBitmap** (compressed bitsets), **Caffeine** (caching), **Jackson** (JSON), Prometheus /
**OpenTelemetry** (metrics), and **Netty** (networking and buffers).

A new dependency must be justified (why existing ones are insufficient) and must update the
bundled-dependency `LICENSE`/`NOTICE` — verify with `./gradlew checkBinaryLicense`.

## Backward compatibility

Pulsar maintains strong compatibility guarantees. Changes must not break public APIs, client
compatibility, wire-protocol compatibility, or serialized/metadata formats — servers must work with
both older and newer clients. Flag any change that may break compatibility.

**Plugin / SPI extension points are public API.** Many interfaces are selected by a `*ClassName`
configuration setting — e.g. `LoadManager`, `LedgerOffloaderFactory`, `AuthorizationProvider` /
`AuthenticationProvider`, `EntryFilter`, `TopicFactory`, `BrokerInterceptor`, dispatcher /
delayed-delivery-tracker factories, `CustomCommand` — and third parties ship implementations. Changing
such an interface, or a `protected` member of an extensible class (`PulsarWebResource`,
`PersistentTopic`, `Producer`), breaks them: it generally needs a PIP and must not land in
maintenance-branch backports.

**Design interface changes for backward compatibility.** When you add a method to such an interface,
prefer a `default` implementation that delegates to an existing method, so older third-party
implementations keep working unchanged. If no sensible delegation exists, add a separate
capability-query method (e.g. `boolean supportsX()`) the broker checks at runtime, so it can support
older implementations gracefully instead of depending on the new method.

**Don't leak third-party types through public/plugin interfaces.** Exposing Netty or AsyncHttpClient
classes breaks consumers of the **shaded** client (shaded vs. unshaded classes differ) and couples
callers to the implementation — provide a Pulsar-owned abstraction. Changing a documented behaviour or
guarantee (e.g. PIP-68 exclusive-producer guarantees, default rate-limiter behaviour) needs a PIP and a
dev@ discussion, not just a code change.

**Introduce changes behind a backward-compatible default.** Make new/changed behaviour opt-in via
configuration rather than silently changing existing deployments. Behaviour that risks data loss (e.g.
skipping unrecoverable data) must be gated behind an explicit flag (such as `autoSkipNonRecoverableData`),
defaulting to the safe/old behaviour.

## Resource and memory management

- Always close resources (streams, connections, executors, buffers) — prefer try-with-resources.
- On internal networking/messaging paths, prefer **Netty `ByteBuf`** over `ByteBuffer` unless an
  external API requires it; release ref-counted buffers you allocate.
- **Don't hand-optimize allocation away.** Pulsar runs on **ZGC** (very low collection overhead), so
  the extra short-lived allocations from favouring immutable objects (see *Concurrency* below) are
  cheap. Older code pools objects with Netty's `Recycler`; this is **no longer recommended for new
  code** — under ZGC the `Recycler` often *costs* more CPU than it saves. Don't add new `Recycler`
  usage. See [PIP-443](pip/pip-443.md).

## Performance

- **Back optimizations with evidence** — a JMH benchmark (see *Testing conventions*) or a profile, not
  intuition — measured on JIT-warmed code (see *Reproducing concurrency / memory-visibility bugs*).
- **On hot paths** (dispatch, IO, per-message): avoid `String.format` (build strings directly),
  `Enum.values()` (match explicitly), and unnecessary allocation/locking; prefer lock-free or
  single-writer designs.
- **Don't add overhead to an already-overloaded system.** Avoid doing work then discarding it (e.g.
  reading entries only to drop them before dispatch) — extra work under load causes cascading failures;
  acquire/estimate up front and reconcile afterwards.
- **Bound in-memory caches** (size or byte limit + eviction) and de-duplicate repeated `String`s
  (cluster/tenant/namespace ids) with `org.apache.pulsar.common.util.StringInterner`.

## Configuration

When adding configuration options: use clear, descriptive names; provide sensible defaults; update the
default configuration files; and document the option.

## Code review checklist

When reviewing a PR, verify:

- Java coding conventions followed; logging follows the guidelines above (slog, levels, structured
  attributes).
- Thread-safety risks; no blocking in async paths; correct `CompletableFuture` usage.
- No unnecessary dependencies; LICENSE/NOTICE updated when dependencies change.
- Backward compatibility preserved.
- Tests exist and are appropriate; reflection into private state is flagged with a `@VisibleForTesting`
  accessor suggested instead.
- The **PR description explains the change** — at minimum **Motivation (why?)** and **Modifications
  (what/how?)**, matching `.github/PULL_REQUEST_TEMPLATE.md`; a title alone isn't sufficient.

Focus feedback on correctness, reliability, and maintainability.

## Concurrency

- Public classes should be **thread-safe**; annotate non-thread-safe ones with `@NotThreadSafe`.
- Protect shared mutable state; prefer fine-grained synchronization; mutate on the intended thread.
  Prefer the **single-writer principle** (a given piece of state mutated by only one thread) to avoid
  concurrent mutation entirely.
- **Minimize work while holding a lock.** Capture needed state into locals inside the synchronized
  block, then run callbacks, listeners, and IO *outside* it — never call out to listener/callback code
  while holding a lock (this has fixed real deadlocks and contention).
- Give threads **meaningful names**. When creating thread pools, prefer Netty's
  **`io.netty.util.concurrent.DefaultThreadFactory`** — it produces **`FastThreadLocalThread`**
  instances (lower overhead `FastThreadLocal` lookups, which matter on Netty paths like the pooled
  `ByteBuf` allocator) and assigns prefixed thread names.

Pulsar has no documented, project-wide concurrency model yet; see
[`ARCHITECTURE.md` → Concurrency model](ARCHITECTURE.md#concurrency-model-a-known-gap) for the
conventions that *should* govern threads, thread pools, and event loops.

### The Java Memory Model is what makes concurrent code correct

Several hard-to-investigate Pulsar bugs have come from misconceptions about Java synchronization:

- **A `synchronized` method or block is not, on its own, thread-safe.** It provides its
  visibility/ordering guarantees only when the **same monitor/lock guards both the reads and the
  writes** of the shared state.
- On 64-bit JVMs a field's value is **never corrupted** — a read returns some value that was actually
  written. What breaks is **visibility**: without a happens-before relationship, threads can observe
  different values, or never see an update. Establish happens-before with `synchronized`, `volatile`,
  `final`, or `java.util.concurrent` constructs.
- **A field accessed by more than one thread needs explicit visibility** — make it `volatile` (or
  guard every read *and* write with the same lock). `volatile` gives single-field visibility but does
  **not** make compound updates (read-modify-write, check-then-act) atomic — use `java.util.concurrent`
  atomics/locks for those.
- Visibility is per-field, so a mutable object can be observed **partially updated**.
- The only way to be reliably correct is to **conform to the Java Memory Model**. **Benign data races**
  are sometimes acceptable, and some Pulsar code relies on this by design — but only as a deliberate,
  documented choice.
- **Prefer immutable objects.** An object is **immutable** when all fields are `final` *and* every
  nested instance is itself immutable (a `record` is the common case; immutability must hold for the
  whole reachable graph). It is **effectively immutable** when never modified after construction but
  with non-`final` fields. Publication differs: an **immutable** object benefits from the JMM's
  final-field **safe initialization** (visible even when published via a data race) and needs **no**
  safe publication; an **effectively immutable** one must be shared via **safe publication** (a `final`
  or `volatile` field, or a `java.util.concurrent` construct such as `ConcurrentHashMap`). See
  [Safe initialization](https://shipilev.net/blog/2014/safe-public-construction/#_safe_initialization).

### Reproducing concurrency / memory-visibility bugs

These bugs are timing- and platform-dependent and easily masked, so a clean run is weak evidence a fix
is correct:

- Interpreted and JIT-compiled code behave differently. Reproductions often need several **warm-up
  rounds with a short pause** so the (tiered, asynchronous) JIT kicks in; a short test may never
  trigger compilation. JVM flags can force earlier compilation, and the exercised paths affect what
  gets compiled.
- Some races surface only on specific **hardware/OS** — classically **multi-socket / multi-NUMA**
  machines, whose weaker cross-socket memory ordering exposes races a single socket never shows.
