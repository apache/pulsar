* **Status**: Draft
* **Authors**: [Author]
* **Pull Request**: -
* **Mailing List discussion**: -
* **Release**: -

# Background knowledge

[PIP-89](https://github.com/apache/pulsar/blob/master/pip/pip-89.md) introduced the concept of structured event
logging for Pulsar and was approved in 2021. It proposed replacing SLF4J string-interpolated logging with structured
key-value logging, using a custom wrapper (`structured-event-log` module) built on top of SLF4J's MDC.

Since then, a standalone library called [slog](https://github.com/merlimat/slog) has been developed that provides a
more complete and performant implementation of structured logging. slog builds on the same principles as PIP-89 but
with a significantly improved API, better performance characteristics, and support for all standard log levels.

### Structured logging

Traditional logging uses string interpolation to embed values into log messages:

```java
log.info("[{}] Opening ledger {} with {} entries", name, ledgerId, entries);
```

Structured logging instead attaches context as typed key-value attributes:

```java
log.info().attr("ledgerId", ledgerId).attr("entries", entries).log("Opening ledger");
```

This makes log records machine-parseable, filterable, and queryable without complex regex patterns.

### slog API

slog provides a `Logger` interface with the following key features:

* **All standard levels**: `trace()`, `debug()`, `info()`, `warn()`, `error()` — each with four variants:
  simple message, printf-formatted, fluent builder, and lambda/consumer-based.
* **Fluent Event builder**: Chain `.attr(key, value)` calls to attach structured attributes, with primitive overloads
  (`long`, `int`, `double`, `float`, `boolean`) to avoid autoboxing.
* **Derived loggers with context**: `logger.with().attr("topic", topic).build()` creates a child logger where
  context attributes are automatically included in every log call.
* **Context composition**: `logger.with().ctx(otherLogger).build()` inherits context from another logger, enabling
  cross-component context propagation without coupling.
* **Zero overhead when disabled**: Level guards are checked before constructing any objects. The fluent builders
  return a no-op singleton `Event` when the level is disabled, making all chained calls effectively free.
* **Timed events**: `log.info().timed()` automatically records elapsed duration between the call and `.log()`.
* **Lazy evaluation**: `log.debug(e -> e.attr(...).log(...))` — the lambda only executes when the level is enabled.
* **Exception support**: `.exception(t)` for full stack traces, `.exceptionMessage(t)` for message-only.
* **Lombok integration**: `@CustomLog` annotation generates the static logger field automatically.

slog auto-discovers the backend at runtime: Log4j2 (preferred) or SLF4J 1.x/2.x as fallback.

# Motivation

While PIP-89 was approved and the `structured-event-log` module was added to the Pulsar codebase, adoption has been
very limited. Almost all Pulsar components still use traditional SLF4J logging with string interpolation. The
`structured-event-log` module's API is verbose and lacks support for standard log levels like `debug` and `trace`,
which has hindered adoption.

Meanwhile, the slog library has evolved to address these shortcomings with a cleaner, more complete API. This PIP
proposes to adopt slog as the standard logging approach across all Pulsar components, replacing both direct SLF4J
usage and the internal `structured-event-log` module.

### Problems with the current state

**Inconsistent log formatting**: Every developer formats log messages differently. The same information appears in
different positions, with different delimiters, and different key naming:
```java
// Various inconsistent patterns found across the codebase:
log.info("[{}] Opening managed ledger {}", name, ledgerId);
log.info("[{}] [{}] Cursor {} recovered to position {}", ledgerName, cursorName, cursor, position);
log.warn("Error opening ledger {} for cursor {}: {}", ledgerId, cursorName, errorMsg);
```

**Repeated boilerplate context**: The managed ledger name, cursor name, topic name, and other context identifiers
are repeated as positional arguments in nearly every log statement — often as the first one or two `{}` placeholders.
This is verbose, error-prone, and frequently inconsistent.

**isDebugEnabled guards**: Debug-level logging requires explicit level guards to avoid the cost of string
construction and argument evaluation:
```java
if (log.isDebugEnabled()) {
    log.debug("[{}] Reading entry ledger {}: {}", name, position.getLedgerId(), position.getEntryId());
}
```
This adds significant boilerplate — in `ManagedLedgerImpl` alone, there were 58 such guards.

**Poor queryability**: Log analysis tools (Splunk, Elasticsearch, Loki) cannot reliably extract values embedded in
interpolated strings. Regex-based parsing is fragile and varies by message format.

**Missing context in error scenarios**: When debugging production issues, engineers frequently find that critical
context was omitted from log messages because adding another positional argument was cumbersome.

### Why slog over `structured-event-log`

* **Complete level support**: slog supports all five standard levels (`trace`, `debug`, `info`, `warn`, `error`),
  while `structured-event-log` only supports `info`, `warn`, and `error`.
* **Simpler API**: slog's `log.info().attr("k", v).log("msg")` is more natural than
  `log.newRootEvent().resource("k", v).log("msg")`.
* **Better performance**: slog uses a cached generation-counter scheme for level checks with a single integer
  comparison and no volatile fence. Primitive `attr()` overloads avoid autoboxing.
* **Derived loggers**: `logger.with().attr(...)` creates an immutable child logger with persistent context — cleaner
  than creating `EventResources` objects.
* **Lazy evaluation**: Lambda-based logging (`log.debug(e -> ...)`) eliminates `isDebugEnabled` guards entirely.
* **Lombok integration**: `@CustomLog` works with slog, reducing logger declaration boilerplate.
* **Standalone library**: slog is maintained as an independent library, decoupled from Pulsar's release cycle.
  Being a separate library makes it easier to adopt across related projects (e.g., BookKeeper, Oxia Java client
  which already uses slog), enabling consistent structured logging and cross-project context propagation.

# Goals

## In Scope

* **Adopt slog** as the standard structured logging library for all Pulsar components.
* **Remove the internal `structured-event-log` module** from the Pulsar codebase, since slog supersedes it.
* **Migrate all components** from SLF4J to slog, one module at a time. Components can be migrated independently
  and there is no concern with a coexistence period where some components use SLF4J and others use slog.
* **Establish logging conventions**: standardized attribute names (`managedLedger`, `cursor`, `ledgerId`, `topic`,
  `position`, etc.) to ensure consistency across the codebase.
* **Use derived loggers for context propagation**: Classes that carry identity (e.g., a managed ledger's name, a
  cursor's name, a topic) should create derived loggers that automatically attach this context to every log call.

* **Cross-boundary context propagation with BookKeeper**: Since Pulsar embeds BookKeeper, slog's context
  composition (`ctx()`) enables sharing context across the Pulsar → BookKeeper boundary. For example, a BookKeeper
  write failure event log would automatically carry the Pulsar topic name as an attribute, without BookKeeper needing
  to know about Pulsar concepts. This dramatically improves debuggability of storage-layer issues.

## Out of Scope

* Defining centralized event enums (as discussed in PIP-89). slog uses string messages, and adopting an enum-based
  log event model would be highly impractical given the scale of the codebase.
* Modifying the log output format or Log4j2 configuration. slog works transparently with existing logging backends
  and configurations.
* Introducing distributed tracing or trace ID propagation. This is orthogonal and can be layered on top later.

### Pulsar client libraries

The `pulsar-client` module is a special case because its logs are emitted within users' applications rather than
on the server side. slog auto-discovers the logging backend at runtime (Log4j2 or SLF4J), so it will integrate
transparently with whatever logging framework the user's application already has configured. No action is required
by users — the client will simply emit structured log records through the existing backend, and users who configure
a JSON appender will automatically benefit from the structured attributes.

# High Level Design

The migration follows a straightforward, incremental approach:

1. **Add slog as a dependency** in the Pulsar build system (Gradle version catalog).
2. **Configure Lombok** with `@CustomLog` support for slog.
3. **Migrate modules one at a time**, starting with `managed-ledger` as the first module (already done as a
   proof of concept). Each module migration involves:
   - Replacing SLF4J imports and logger declarations with slog.
   - Converting log statements from parameterized format to structured format.
   - Introducing derived loggers for repeated context (e.g., ledger name, cursor name).
   - Removing `isDebugEnabled()` / `isTraceEnabled()` guards.
4. **Delete the `structured-event-log` module** — it has no existing usages and can be removed immediately.

Since slog auto-discovers the logging backend (Log4j2 or SLF4J), no changes to logging configuration files are
needed. The structured attributes are emitted via the backend's native mechanism (Log4j2 key-value pairs or SLF4J
MDC), so existing log appenders and formats continue to work. Users who want to leverage structured attributes in
their log output can configure a JSON appender.

# Detailed Design

## Design & Implementation Details

### Logger declaration

For simple classes, use Lombok's `@CustomLog` annotation:

```java
@CustomLog
public class OpAddEntry implements AddCallback {
    // Lombok generates: private static final Logger log = Logger.get(OpAddEntry.class);

    public void initiate() {
        log.info().attr("ledgerId", ledger.getId()).log("Initiating add entry");
    }
}
```

For classes with persistent identity context (e.g., a managed ledger, a cursor, a topic handler), create a
derived instance logger:

```java
public class ManagedLedgerImpl implements ManagedLedger {
    private static final Logger slog = Logger.get(ManagedLedgerImpl.class);
    protected final Logger log;

    public ManagedLedgerImpl(..., String name, ...) {
        this.log = slog.with().attr("managedLedger", name).build();
    }

    // All subsequent log calls automatically include managedLedger=<name>
    public void openLedger(long ledgerId) {
        log.info().attr("ledgerId", ledgerId).log("Opening ledger");
        // Emits: msg="Opening ledger" managedLedger=my-topic ledgerId=12345
    }
}
```

### Context composition across components

Classes that operate on behalf of another component can inherit its context:

```java
public class RangeEntryCacheImpl implements EntryCache {
    private static final Logger slog = Logger.get(RangeEntryCacheImpl.class);
    private final Logger log;

    RangeEntryCacheImpl(ManagedLedgerImpl ml, ...) {
        this.log = slog.with().ctx(ml.getLogger()).build();
        // Logger name is RangeEntryCacheImpl, but carries managedLedger context from ml
    }
}
```

### Cross-boundary context: Pulsar → BookKeeper

One of the most powerful benefits of slog's context model is the ability to propagate context across component
boundaries. Since Pulsar embeds BookKeeper, a BookKeeper ledger operation can inherit context from the Pulsar
component that initiated it:

```java
// In Pulsar's ManagedLedgerImpl (already has managedLedger context)
public void createLedger() {
    // Pass the Pulsar logger context to BookKeeper
    bookKeeper.asyncCreateLedger(..., log, ...);
}

// In BookKeeper's LedgerHandle
public class LedgerHandle {
    private final Logger log;

    LedgerHandle(Logger pulsarLogger, ...) {
        // BookKeeper's logger inherits Pulsar's context
        this.log = slog.with().ctx(pulsarLogger).attr("ledgerId", ledgerId).build();
    }

    void handleWriteError(int rc) {
        // This log entry automatically includes managedLedger=my-topic from Pulsar's context
        log.error().attr("rc", BKException.getMessage(rc)).log("Write failed");
        // Emits: msg="Write failed" managedLedger=my-topic ledgerId=12345 rc=NotEnoughBookies
    }
}
```

This eliminates the long-standing problem of BookKeeper errors being logged without any Pulsar-level context,
making it very difficult to correlate storage failures with the affected topics.

### Conversion patterns

| SLF4J pattern | slog equivalent |
|---|---|
| `log.info("msg")` | `log.info("msg")` |
| `log.info("[{}] msg {}", name, val)` | `log.info().attr("key", val).log("msg")` |
| `log.error("msg", exception)` | `log.error().exception(e).log("msg")` |
| `log.warn("msg: {}", e.getMessage())` | `log.warn().exceptionMessage(e).log("msg")` |
| `if (log.isDebugEnabled()) { log.debug(...) }` | `log.debug().attr(...).log(...)` |
| Manual timing with `System.nanoTime()` | `log.info().timed() ... event.log("done")` |

### Attribute naming conventions

To ensure consistency across the codebase, the following attribute names should be used:

| Attribute | Description | Example value |
|---|---|---|
| `managedLedger` | Managed ledger name | `my-tenant/my-ns/my-topic` |
| `cursor` | Cursor name | `my-subscription` |
| `topic` | Topic name | `persistent://tenant/ns/topic` |
| `ledgerId` | BookKeeper ledger ID | `12345` |
| `entryId` | Entry ID within a ledger | `42` |
| `position` | Ledger position (ledgerId:entryId) | `12345:42` |
| `readPosition` | Current read position | `12345:42` |
| `markDeletePosition` | Mark-delete position | `12345:41` |
| `state` | Component state | `Open`, `Closed` |
| `rc` | BookKeeper return code | `OK`, `NoSuchLedger` |
| `subscription` | Subscription name | `my-sub` |
| `consumer` | Consumer name | `consumer-0` |
| `producer` | Producer name | `producer-0` |

## Public-facing Changes

### Configuration

A new Lombok configuration is added to `lombok.config`:

```properties
lombok.log.custom.declaration = io.github.merlimat.slog.Logger io.github.merlimat.slog.Logger.get(TYPE)
```

### Log output

The log message format changes. With traditional SLF4J:
```
[ManagedLedgerImpl] [my-topic] Opening ledger 12345 with 100 entries
```

With slog (pattern appender):
```
[ManagedLedgerImpl] Opening ledger {managedLedger=my-topic, ledgerId=12345, entries=100}
```

With slog (Log4j2 JSON appender):
```json
{
  "instant": {"epochSecond": 1775174210, "nanoOfSecond": 277000000},
  "thread": "pulsar-ordered-executor-0-0",
  "level": "INFO",
  "loggerName": "org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl",
  "message": "Opening ledger",
  "contextMap": {
    "managedLedger": "my-topic",
    "ledgerId": "12345",
    "entries": "100"
  },
  "endOfBatch": false,
  "loggerFqcn": "io.github.merlimat.slog.impl.EventImpl",
  "threadId": 42,
  "threadPriority": 5
}
```

Users who rely on parsing specific log message patterns will need to update their parsers. However, the structured
format makes parsing significantly easier and more reliable.

# Monitoring

No changes to monitoring. Structured logging improves the ability to create log-based alerts and dashboards by
making attribute-based filtering possible (e.g., alert on all `ERROR` logs where `managedLedger=X`).

# Security Considerations

No security implications. This is an internal change to how log messages are formatted. No new endpoints, commands,
or permissions are introduced.

# Backward & Forward Compatibility

## Upgrade

* Log message formats will change as modules are migrated. Users who parse log messages with regex patterns will
  need to update their parsers. The structured format is strictly more parseable than the previous format.
* To get the most out of structured logging, users should consider switching to a JSON log appender if they haven't
  already. However, this is optional — traditional pattern-based appenders continue to work.
* No configuration changes are required for the upgrade.

## Downgrade / Rollback

No special steps required. Reverting to a previous version restores the original SLF4J logging.

## Pulsar Geo-Replication Upgrade & Downgrade/Rollback Considerations

No impact on geo-replication. Logging is a local concern and does not affect inter-cluster communication.

# Alternatives

### Continue using `structured-event-log`

The existing internal module could be extended. However, it lacks debug/trace levels, has a more verbose API, and
maintaining a custom logging library inside Pulsar adds unnecessary burden. slog is a superset of the
`structured-event-log` API with better performance and more features.

### Use SLF4J 2.0 Fluent API

SLF4J 2.0 introduced a fluent logging API (`log.atInfo().addKeyValue("k", v).log("msg")`). However:
* It does not support derived loggers with persistent context.
* It does not support context composition across components.
* No support for timed events or sampling.
* Attribute values are stored as strings only (no primitive overloads).
* Level guards still allocate objects in some implementations.

### Use Log4j2 directly

Log4j2 does not expose a public structured logging API for applications. While it is possible to pass structured
key-value data through low-level, non-public Log4j2 interfaces (which is what slog does internally as its
backend), this is not something easily or reliably usable directly from application code. It would also tightly
couple Pulsar to Log4j2 as the only backend, preventing users from using other SLF4J-compatible backends.
slog abstracts over these internals and supports both Log4j2 and SLF4J backends transparently.

### Use Flogger

[Flogger](https://github.com/google/flogger) is Google's fluent logging API for Java. While it offers a fluent API
and some structured logging features, it has significant performance drawbacks:
* Extremely slow on the enabled path (~0.7 ops/μs vs slog's 13–25 ops/μs — roughly **18–35× slower**).
* Very high allocation rate (1624–1904 B/op vs slog's 0–80 B/op — **20–24× more allocations**).
* No support for derived loggers with persistent context or context composition.
* No support for timed events.
* Tightly coupled to Google's internal infrastructure; limited backend flexibility.

### Performance comparison

Benchmarks from the [slog repository](https://github.com/merlimat/slog) (JMH, higher ops/μs is better, lower B/op
is better):

**Disabled path** (log level is off — measures the cost of checking the level):

| Library              | ops/μs  |
|----------------------|---------|
| slog                 | 1005.4  |
| Log4j2               | 413.1   |
| SLF4J                | 367.5   |
| Flogger              | 360.3   |

slog's disabled path is **2.4× faster** than Log4j2 and **2.8× faster** than SLF4J/Flogger. This matters because
the vast majority of debug/trace log statements are disabled in production.

**Enabled path** (log level is on — measures the cost of formatting and emitting a log message with 2 key-value
attributes):

| Library              | ops/μs |
|----------------------|--------|
| slog (simple)        | 24.6   |
| Log4j2 (simple)      | 14.6   |
| slog (fluent)        | 13.2   |
| slog (fluent+ctx)    | 12.0   |
| SLF4J (simple)       | 11.2   |
| SLF4J (fluent)       | 4.2    |
| Flogger              | 0.7    |

slog is the fastest in both simple and fluent modes. Flogger is **18–35× slower** than slog.

**Allocation rate** (bytes allocated per log operation):

| Library              | B/op |
|----------------------|------|
| slog (simple)        | 0    |
| Log4j2 (simple)      | 24   |
| SLF4J (simple)       | 24   |
| slog (fluent+ctx)    | 40   |
| slog (fluent)        | 80   |
| SLF4J (fluent)       | 1104 |
| Flogger              | 1624 |

slog's fluent API allocates **6× less** than Log4j2's fluent API, **14× less** than SLF4J's fluent API, and
**20× less** than Flogger. Since structured logging requires the fluent API, this is the most relevant comparison
for this PIP's use case.

# General Notes

The `managed-ledger` module has already been fully migrated as a proof of concept, converting all source and test files
(54 files, ~400 log statements). The migration reduced total lines of code (1481 additions vs 1392 deletions) while
making every log statement more informative and consistent.

# Links

* Original PIP-89: https://github.com/apache/pulsar/blob/master/pip/pip-89.md
* slog library: https://github.com/merlimat/slog
* PoC: managed-ledger migration: https://github.com/merlimat/pulsar/pull/20
* Mailing List discussion thread: -
* Mailing List voting thread: -
