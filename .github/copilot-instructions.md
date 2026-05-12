The following instructions are only to be applied when performing a code review.

# Copilot Instructions for Apache Pulsar

## Project Context

Apache Pulsar is a distributed messaging and streaming platform designed for high throughput, low latency, and
horizontal scalability.

The codebase contains performance-critical, asynchronous, and concurrency-sensitive components such as brokers, storage
clients, and networking layers.

Code reviews should prioritize:

* correctness
* thread safety
* performance
* maintainability
* backward compatibility

---

# Java Coding Conventions

Apache Pulsar follows the Sun Java Coding Conventions with additional project-specific rules.

## Formatting

* Use **4 spaces** for indentation.
* **Tabs must never be used**.
* Always use **curly braces**, even for single-line `if` statements.

Example:

```java
if (condition) {
    doSomething();
}
```

## Javadoc

* Do **not include `@author` tags** in Javadoc.

## TODO Comments

All TODO comments must reference a GitHub issue.

Example:

```java
// TODO: https://github.com/apache/pulsar/issues/XXXX
```

---

# Dependencies

Prefer existing dependencies instead of introducing new libraries.

The Pulsar codebase commonly uses:

* **Apache Commons libraries or Guava** for utilities
* **FastUtil** for optimized type specific collections
* **JCTools** for high performance concurrent data structures
* **RoaringBitmap** for compressed bitmaps (bitsets)
* **Caffeine** for caching
* **Jackson** for JSON handling
* **Prometheus Java simpleclient** (or newer **Prometheus Java Metrics Library**) for metrics
* **OpenTelemetry API** for metrics
* **Netty** for networking and buffers

When introducing a new dependency:

* justify why existing dependencies are insufficient
* ensure required license files and notices are updated

---

# Logging Guidelines

* Use **SLF4J** for logging.
* Do **not use** `System.out` or `System.err`.
* Assume production commonly runs at **INFO** log level.
* Avoid excessive logging in hot paths.
* Guard expensive `DEBUG` and `TRACE` logging with `log.isDebugEnabled()` or `log.isTraceEnabled()`.
* Avoid logging stack traces at `INFO` and lower levels.

Log messages should be:

* clear and descriptive
* capitalized
* understandable without reading the code

---

# Resource and Memory Management

Ensure resources are always closed correctly.

Prefer:

```java
try (InputStream in = ...) {
    // use resource
}
```

Avoid leaks for:

* streams
* network connections
* executors
* buffers

For internal networking/messaging paths, prefer **Netty `ByteBuf`** over `ByteBuffer` unless an external API requires
`ByteBuffer`.

---

# Configuration Guidelines

When adding configuration options:

* use clear and descriptive names
* provide sensible default values
* update default configuration files
* document the configuration option

---

# Concurrency Guidelines

Pulsar is designed as a low-latency asynchronous system.

Verify:

* public classes are **thread-safe**
* shared mutable state is protected
* mutations occur on the intended thread
* fine-grained synchronization is preferred
* threads have meaningful names for diagnostics

If a class is not thread-safe, annotate it with:

```java
@NotThreadSafe
```

Prefer **OrderedExecutor** for ordered asynchronous execution.

---

# Asynchronous Programming Guidelines

Pulsar relies heavily on `CompletableFuture` and asynchronous pipelines.

Prefer `CompletableFuture` APIs over `ListenableFuture` for new code.

## Avoid Blocking in Async Paths

Do not introduce blocking operations in asynchronous execution paths.

Examples of blocking operations:

* `Thread.sleep`
* `Future.get()`
* `CompletableFuture.join()`
* blocking IO operations

Blocking calls must not run on event loop or async execution threads.

## Avoid Nested Futures

Avoid returning nested futures such as:

```java
CompletableFuture<CompletableFuture<T>>
```

Prefer flattening with `thenCompose`.

## Asynchronous Exception Handling

Methods returning `CompletableFuture` must not throw synchronous exceptions directly.

Incorrect:

```java
public CompletableFuture<Void> operation() {
    if (error) {
        throw new IllegalStateException("unexpected state");
    }
    return CompletableFuture.completedFuture(null);
}
```

Use returned futures to propagate failures:

```java
return CompletableFuture.failedFuture(exception);
```

This also applies to argument validation in async methods:

```java
if (arg == null) {
    return CompletableFuture.failedFuture(new IllegalArgumentException("arg"));
}
```

Throwing exceptions inside async stages such as `thenApply`, `thenCompose`, `thenRun`, `handle`, or `whenComplete`
is acceptable:

```java
return future.thenApply(v -> {
    if (error) {
        throw new IllegalStateException("unexpected state");
    }
    return result;
});
```

---

# Backward Compatibility

Apache Pulsar maintains strong compatibility guarantees.

Changes must not break:

* public APIs
* client compatibility
* wire protocol compatibility
* metadata or serialized formats

Servers must be compatible with both older and newer clients.

Flag any change that may break compatibility.

---

# Testing Guidelines

## Unit testing

* TestNG is used as the testing framework
* Mocking uses Mockito
* Assertions should prefer using AssertJ library with descriptions over using TestNG assertions
* Awaitility should be used to handle assertions with asynchronous conditions together with AssertJ

## Avoid Reflection in Tests

Do **not** use reflection to access private fields or methods from tests. This includes
`WhiteboxImpl.getInternalState`, `WhiteboxImpl.setInternalState`, `Field.setAccessible(true)`,
`Method.setAccessible(true)`, and similar reflection helpers.

Reflection-based test access is discouraged because it:

* breaks during refactoring without any compile-time signal — renaming or retyping a field
  silently invalidates the test
* produces verbose, brittle, and hard-to-read test code
* couples tests to implementation details that should be free to change

Instead, expose what tests legitimately need through **package-private** methods (or fields
where appropriate) and annotate them with `@VisibleForTesting`:

```java
@VisibleForTesting
Map<String, Subscription> getSubscriptions() {
    return subscriptions;
}
```

The test then accesses state through a normal, statically-typed call:

```java
var subscriptions = persistentTopic.getSubscriptions();
```

instead of:

```java
ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions =
        WhiteboxImpl.getInternalState(persistentTopic, "subscriptions");
```

Place the test in the same package as the class under test so package-private visibility is
sufficient — no need to widen visibility to `public` for testing.

When reviewing PRs, flag any new use of reflection in tests and suggest a `@VisibleForTesting`
package-private accessor instead. See the dev@ thread
[Stop using reflection to access private fields in tests](https://lists.apache.org/thread/7gr04sqmzyttx4ln6ydtp3qv0xgo1o6m)
for the full rationale.

# Testing Expectations

Every feature or bug fix should include tests.

Verify:

* unit tests exist
* edge cases are covered
* failure scenarios are tested
* tests are deterministic and stable
* tests avoid `sleep`-based timing assumptions
* tests include timeouts to prevent hangs

Integration tests may be required for distributed components.

---

# Pull Request Review Guidance

When reviewing a pull request, Copilot should:

* verify Java coding conventions are followed
* detect thread safety risks
* flag blocking operations in async paths
* detect improper `CompletableFuture` usage
* detect unnecessary dependencies and missing license updates
* ensure logging follows project guidelines
* verify backward compatibility
* suggest missing tests when appropriate
* flag reflection-based access to private fields or methods in tests (e.g. `WhiteboxImpl`,
  `setAccessible(true)`) and recommend a package-private `@VisibleForTesting` accessor instead

Focus feedback on correctness, reliability, and maintainability.