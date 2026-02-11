# Adaptive Publish Throttling — PR Reference

## Config Keys

All keys live under the `CATEGORY_SERVER` section of `ServiceConfiguration`.
A **dynamic** key can be changed at runtime via the admin API without a restart.

| Key | Default | Dynamic | Purpose |
|-----|---------|:-------:|---------|
| `adaptivePublisherThrottlingEnabled` | `false` | No | Master switch. Requires broker restart to enable or disable. |
| `adaptivePublisherThrottlingObserveOnly` | `false` | **Yes** | When `true`, controller computes everything but never applies throttling. Flip at runtime as an emergency rollback. |
| `adaptivePublisherThrottlingIntervalMs` | `1000` | **Yes** | Milliseconds between controller evaluation cycles. |
| `adaptivePublisherThrottlingMemoryHighWatermarkPct` | `0.85` | **Yes** | JVM heap fraction where memory pressure reaches 1.0 (full throttle). |
| `adaptivePublisherThrottlingMemoryLowWatermarkPct` | `0.70` | **Yes** | JVM heap fraction below which memory pressure drops to 0 (hysteresis floor). Must be < high watermark. |
| `adaptivePublisherThrottlingBacklogHighWatermarkPct` | `0.90` | **Yes** | Backlog/quota fraction where backlog pressure reaches 1.0. Only topics with a backlog quota are affected. |
| `adaptivePublisherThrottlingBacklogLowWatermarkPct` | `0.75` | **Yes** | Backlog/quota fraction below which backlog pressure drops to 0 (hysteresis floor). Must be < high watermark. |
| `adaptivePublisherThrottlingMinRateFactor` | `0.10` | **Yes** | Hard floor: effective rate never drops below `naturalRate × minRateFactor`. |
| `adaptivePublisherThrottlingMaxRateChangeFactor` | `0.25` | **Yes** | Maximum rate change per cycle as a fraction of natural rate; prevents sudden jumps and oscillation. |
| `adaptivePublisherThrottlingPerTopicMetricsEnabled` | `false` | **Yes** | Emit per-topic OpenTelemetry metrics. Adds 6 time-series per topic; review cardinality before enabling in large clusters. |

---

## Files Changed and Added

### Modified (4 files)

| File | What changed |
|------|-------------|
| `pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java` | 10 new `@FieldContext`-annotated config fields with Javadoc |
| `conf/broker.conf` | 10 new commented-out config entries |
| `pulsar-broker/src/main/java/org/apache/pulsar/broker/service/AbstractTopic.java` | `AdaptivePublishRateLimiter` field; `handlePublishThrottling()` delegates to it when feature is enabled |
| `pulsar-broker/src/main/java/org/apache/pulsar/broker/service/BrokerService.java` | `AdaptivePublishThrottleController` lifecycle (`start()` on boot, `close()` on shutdown); `forEachPersistentTopic()` helper |

### Added (7 files)

| File | Purpose |
|------|---------|
| `pulsar-broker/src/main/java/org/apache/pulsar/broker/service/AdaptivePublishRateLimiter.java` | Per-topic `PublishRateLimiter` implementation. No-op when inactive. Asymmetric EWMA natural-rate tracking. Wraps `PublishRateLimiterImpl` delegate. |
| `pulsar-broker/src/main/java/org/apache/pulsar/broker/service/AdaptivePublishThrottleController.java` | Broker-level single-threaded scheduler. Computes memory + backlog pressure, applies bounded-step rate changes, enforces hysteresis, respects `observeOnly` flag. Never dies silently (try-catch-finally on every cycle). |
| `pulsar-broker/src/main/java/org/apache/pulsar/broker/stats/OpenTelemetryAdaptiveThrottleStats.java` | OpenTelemetry instrumentation: 3 broker-level gauges + 3 controller health metrics (always on) + 6 per-topic gauges (opt-in). |
| `pulsar-broker/src/test/java/org/apache/pulsar/broker/service/AdaptivePublishRateLimiterTest.java` | Unit tests for the limiter: no-op guarantee, activate/deactivate lifecycle, asymmetric EWMA correctness, `observeOnly` channel-autoread safety. |
| `pulsar-broker/src/test/java/org/apache/pulsar/broker/service/AdaptivePublishThrottleControllerTest.java` | Unit tests for controller math: `linearPressure()`, `computeTargetRate()` bounded-step arithmetic, hysteresis transitions, `observeOnly` guarantee, backlog pressure. |
| `pulsar-broker/src/test/java/org/apache/pulsar/broker/service/AdaptiveThrottleEndToEndTest.java` | End-to-end tests for the full control loop: no-pressure steady state, smooth rate reduction, monotonic decrease, deactivation on drain, `observeOnly` end-to-end (with IO-thread throttle-count assertion), null-limiter skip, first-cycle warmup. |
| `pulsar-broker/src/test/java/org/apache/pulsar/broker/service/ThrottleTypeEnumTest.java` | Ordinal stability guard for `ThrottleType` enum: asserts minimum constant count, that `AdaptivePublishRate` is present and last, and that it is declared reentrant. |

---

## Test Commands

Run these in a normal Pulsar developer environment from the repository root.
All commands require a full `mvn install -DskipTests` of the project first to
populate the local Maven cache with SNAPSHOT artifacts.

```bash
# Unit tests — controller math, limiter correctness, observeOnly guarantee
# Runs in ~15 seconds with no broker required.
mvn test -pl pulsar-broker -am \
  -Dtest="AdaptivePublishRateLimiterTest,AdaptivePublishThrottleControllerTest,AdaptiveThrottleEndToEndTest" \
  -DfailIfNoTests=false \
  --no-transfer-progress

# Regression — include existing publish rate limiter tests to catch regressions
mvn test -pl pulsar-broker -am \
  -Dtest="AdaptivePublish*,AdaptiveThrottle*,PublishRateLimiterTest" \
  -DfailIfNoTests=false \
  --no-transfer-progress

# Integration — broader broker service tests (slow; run before merging)
mvn test -pl pulsar-broker -am \
  -Dtest="PublishRateLimiterTest,RateLimiterTest,MessagePublishBufferThrottleTest" \
  -DfailIfNoTests=false \
  --no-transfer-progress
```

---

## Metrics Reference

### Always-on broker-level metrics (no cardinality cost)

| Metric | Type | Alert condition |
|--------|------|----------------|
| `pulsar.broker.adaptive.throttle.memory.pressure` | Gauge (0–1) | > 0.5 sustained |
| `pulsar.broker.adaptive.throttle.active.topic.count` | Gauge | > 0 unexpectedly |
| `pulsar.broker.adaptive.throttle.activation.count` | Counter | Rapid increase |
| `pulsar.broker.adaptive.throttle.controller.last.evaluation.timestamp` | Gauge (epoch ms) | `now − value > 3 × intervalMs` |
| `pulsar.broker.adaptive.throttle.controller.evaluation.duration` | Gauge (ms) | > `intervalMs` consistently |
| `pulsar.broker.adaptive.throttle.controller.evaluation.failure.count` | Counter | Any non-zero value |

### Per-topic metrics (opt-in via `adaptivePublisherThrottlingPerTopicMetricsEnabled=true`)

`pulsar.broker.topic.adaptive.throttle.{active, natural.publish.rate, effective.publish.rate, memory.pressure, backlog.pressure, rate.reduction.ratio}`

---

## Troubleshooting Guide

### Producers are being throttled — what is the cause?

**Step 1 — Confirm adaptive throttling is active, not static rate limits.**
Look for log lines: `[AdaptiveThrottle] ACTIVATED topic=...`
Static rate limit throttling logs differently (`PublishRateLimiterImpl`).

**Step 2 — Identify the pressure signal.**

| Signal | How to check |
|--------|-------------|
| **Memory pressure** | `pulsar.broker.adaptive.throttle.memory.pressure > 0`. Also check: `grep "memoryPressure=[^0]" broker.log`. The ACTIVATED log line shows both `memoryPressure=` and `backlogPressure=` fields. |
| **Backlog pressure** | Check ACTIVATED log: `backlogPressure=` field > 0. Or enable per-topic metrics and check `pulsar.broker.topic.adaptive.throttle.backlog.pressure`. |
| **Both** | `pressureFactor = max(memory, backlog)`. The higher signal dominates. |

**Step 3 — Remediation by signal type.**

*Memory pressure* (`memoryPressure > 0`):
- Identify topics with high message sizes or high throughput (`pulsar-admin topics stats`)
- Consider scaling out (add brokers, trigger topic offloading)
- Short-term: raise `adaptivePublisherThrottlingMemoryHighWatermarkPct` (dynamic, no restart)
- Emergency: set `adaptivePublisherThrottlingObserveOnly=true` to suspend all throttling instantly

*Backlog pressure* (`backlogPressure > 0`):
- Check consumer lag: `pulsar-admin topics stats --get-precise-backlog`
- Verify subscriptions are active and consumers are running
- Check if a consumer is stuck or restarting in a loop
- Review the topic's backlog quota: `pulsar-admin namespaces get-backlog-quota <namespace>`
- Short-term: raise `adaptivePublisherThrottlingBacklogHighWatermarkPct` (dynamic)

**Step 4 — Verify observe-only mode is not masking a real problem.**

If `adaptivePublisherThrottlingObserveOnly=true`:
- `pulsar.broker.adaptive.throttle.active.topic.count` will be **0** even under pressure
- Log lines say `OBSERVE-ONLY would-activate` — no actual throttling is applied
- Check: `pulsar-admin brokers get-runtime-configuration | grep observeOnly`

**Step 5 — Check controller health.**

If `pulsar.broker.adaptive.throttle.controller.evaluation.failure.count > 0`:
- Search broker logs: `grep "Evaluation cycle failed" broker.log`
- Common causes: NPE during topic iteration, unexpected `BrokerService` state at startup
- If the controller is stalled (`last.evaluation.timestamp` not advancing): broker restart required

**Step 6 — Confirm the throttle is releasing.**

When pressure drops below the low watermarks, the limiter deactivates and logs:
`[AdaptiveThrottle] DEACTIVATED topic=... naturalMsgRate=.../s`
If DEACTIVATED never appears after pressure clears, check that both
`memoryPressure` AND `backlogPressure` are below their respective low watermarks
(deactivation requires both, not just one).

---

## Commit Message

```
[feat][broker] Add adaptive publish throttle controller (disabled by default)

Introduces an opt-in, broker-level adaptive publish throttle that
dynamically reduces producer publish rates when JVM heap usage or
per-topic backlog size approaches configurable watermarks.

Key design points
-----------------
- AdaptivePublishRateLimiter: per-topic PublishRateLimiter that is a
  complete no-op (zero overhead) when inactive. Asymmetric EWMA
  (α_up=0.30, α_down=0.05) tracks the producer's peak natural rate.
- AdaptivePublishThrottleController: single-threaded broker-level
  scheduler; bounded-step rate changes (≤ 25 % of natural rate per
  cycle) prevent oscillation; hysteresis (activate on pressure > 0,
  deactivate only when pressure == 0) prevents rapid toggling.
- observeOnly mode: compute + log + emit metrics without applying
  any throttling; togglable via dynamic config as an emergency
  circuit-breaker (no restart required).
- Controller never dies silently: every evaluation cycle is wrapped
  in try-catch-finally; failures are counted and logged at ERROR.
- ThrottleType.AdaptivePublishRate: dedicated reentrant enum constant
  appended at the end of ThrottleType to preserve existing ordinals.
- OpenTelemetry: 3 always-on broker metrics + 3 controller health
  metrics (last-eval timestamp, duration, failure count) + 6 optional
  per-topic metrics (disabled by default).

Disabled by default: adaptivePublisherThrottlingEnabled=false.
A broker restart is required to enable it. All tuning parameters
(watermarks, rate factors, observeOnly flag) are dynamic.

Tests added
-----------
- AdaptivePublishRateLimiterTest  (13 unit tests)
- AdaptivePublishThrottleControllerTest  (30+ unit tests)
- AdaptiveThrottleEndToEndTest  (9 integration tests)
- ThrottleTypeEnumTest  (4 ordinal-stability guard tests)
```

## PR Description

> **Experimental feature** — disabled by default (`adaptivePublisherThrottlingEnabled=false`).
> A broker restart is required to enable it; all tuning parameters are dynamic config.
>
> **Focused review request** — the areas most likely to have subtle bugs are:
> 1. **Concurrency** — `AdaptivePublishRateLimiter`: the `volatile boolean active` fast path on IO threads (see `handlePublishThrottling`), and the single-writer contract on the controller scheduler thread.
> 2. **Lifecycle** — `BrokerService.startAdaptivePublishThrottleController()` and the `close()` path; `ThrottleType.AdaptivePublishRate` ordinal stability and the new `states[]` slot.
> 3. **Hysteresis** — deactivation requires **both** memory **and** backlog pressure to drop below their low watermarks simultaneously; verify this is correct for your use case.

---

### Summary

- Adds an adaptive publish throttle controller that dynamically reduces producer publish rates when JVM heap usage or per-topic backlog size approaches configurable watermarks.
- **Disabled by default.** Zero code path changes when `adaptivePublisherThrottlingEnabled=false`.
- **Observe-only mode** (`adaptivePublisherThrottlingObserveOnly=true`, toggleable at runtime) computes and logs decisions but never applies throttling — safe for validating in production and usable as an emergency circuit-breaker.
- Controller never dies silently: every cycle is wrapped in `try-catch-finally`; failures increment a dedicated OTel counter.
- `ThrottleType.AdaptivePublishRate` is appended as the last enum constant to preserve existing ordinals 0–6. A guard test (`ThrottleTypeEnumTest`) fails immediately if the enum is accidentally mutated.

---

### Diff navigation

#### Configuration (reviewers: verify defaults, dynamic flags, and conf entry comments)

| File | What changed |
|------|-------------|
| `ServiceConfiguration.java` | 10 new `@FieldContext` fields — `adaptivePublisherThrottling*` |
| `conf/broker.conf` | 10 new commented-out entries with inline docs and a recommended quick-start snippet |
| `README.md` | New "Experimental broker features" section with quick-start `broker.conf` snippet |

#### Broker wiring (reviewers: focus on concurrency, lifecycle, and enum ordinal safety)

| File | What changed |
|------|-------------|
| `ServerCnxThrottleTracker.java` | New `ThrottleType.AdaptivePublishRate` constant (appended last, reentrant); class-level Javadoc on ordinal stability |
| `AbstractTopic.java` | New `AdaptivePublishRateLimiter` field; `handlePublishThrottling()` delegation; uses `ThrottleType.AdaptivePublishRate` |
| `BrokerService.java` | `startAdaptivePublishThrottleController()` lifecycle; `forEachPersistentTopic()` helper; `close()` teardown |
| `AdaptivePublishRateLimiter.java` *(new)* | Per-topic limiter: `volatile boolean active` fast path, asymmetric EWMA, `activate()`/`deactivate()` |
| `AdaptivePublishThrottleController.java` *(new)* | Broker-level scheduler: pressure math, bounded-step rate changes, hysteresis, `observeOnly` guard, `try-catch-finally` safety |
| `OpenTelemetryAdaptiveThrottleStats.java` *(new)* | 6 broker-level OTel metrics (3 always-on + 3 health); 6 per-topic metrics (opt-in) |

#### Tests (reviewers: check the observeOnly safety tests and the enum guard tests)

| File | Key assertions |
|------|---------------|
| `AdaptivePublishRateLimiterTest.java` *(new)* | No-op guarantee, activate/deactivate, asymmetric EWMA, `observeOnly` never changes channel autoread |
| `AdaptivePublishThrottleControllerTest.java` *(new)* | `linearPressure()`, bounded-step `computeTargetRate()`, hysteresis, `observeOnly` never calls `activate()` |
| `AdaptiveThrottleEndToEndTest.java` *(new)* | Full control-loop integration: pressure → throttle → drain → deactivate; `observeOnly` IO-thread safety |
| `ThrottleTypeEnumTest.java` *(new)* | Minimum constant count, `AdaptivePublishRate` present and last, declared reentrant |

---

### Design FAQ

**Q: Why introduce a new `ThrottleType.AdaptivePublishRate` instead of reusing `TopicPublishRate`?**

`ServerCnxThrottleTracker` uses reference-counting via `states[ThrottleType.ordinal()]`.
If the adaptive limiter shared `TopicPublishRate` with the static rate limiter, a single
`unmarkThrottled()` call from the adaptive side could decrement the count that the static
limiter had incremented — leaving the connection unthrottled even though the static limit is
still exceeded. A dedicated constant keeps the two signals fully independent and eliminates an
entire class of ordering bugs.

**Q: Why does the controller not coordinate across brokers? Each broker throttles independently.**

Adaptive throttling reacts to local signals — JVM heap on this JVM, backlog on topics owned
by this broker. Cross-broker coordination would require a consensus protocol, introduce network
latency on the hot publish path, and create a single point of failure. Local-only decisions
are simpler, faster, and fail-safe: if the network partition occurs, each broker still protects
itself independently. Cluster-wide load imbalance (e.g. one hot broker) is better addressed
by Pulsar's topic-migration and load-balancer machinery than by throttle coordination.

**Q: What happens if the controller thread crashes?**

The evaluation loop is wrapped in `try-catch-finally`. A caught exception:

1. Increments `evaluationFailureCount` (surfaced as OTel counter
   `pulsar.broker.adaptive.throttle.controller.evaluation.failure.count`).
2. Logs the full stack trace at `ERROR` level so it appears in broker logs immediately.
3. Does **not** kill the `ScheduledExecutorService` — the scheduler reschedules the next
   cycle unconditionally (the `finally` block always completes before the next cycle fires).

If the failure is persistent (e.g. NPE on every cycle), the `last.evaluation.timestamp` metric
stops advancing while `evaluation.failure.count` climbs — a clear alert signal.
Active throttle limiters already applied to topics remain in place until the next successful
cycle; they do not release autonomously, so producers stay protected during transient failures.
A full controller stall (scheduler thread itself dies — only possible via an unchecked
`Error`) requires a broker restart; the OTel staleness alert on
`controller.last.evaluation.timestamp` will fire within `3 × intervalMs` of the stall.

---

### Test plan

- [ ] `mvn test -pl pulsar-broker -am -Dtest="AdaptivePublishRateLimiterTest,AdaptivePublishThrottleControllerTest,AdaptiveThrottleEndToEndTest,ThrottleTypeEnumTest" -DfailIfNoTests=false --no-transfer-progress` passes
- [ ] `mvn test -pl pulsar-broker -am -Dtest="PublishRateLimiterTest" -DfailIfNoTests=false --no-transfer-progress` (regression: static rate limiter unaffected)
- [ ] Broker starts with `adaptivePublisherThrottlingEnabled=false` (default): no controller thread, no `AdaptivePublishRateLimiter` allocated, no OTel instruments registered
- [ ] `observeOnly=true`: logs show `OBSERVE-ONLY would-activate`, zero `ACTIVATED` lines, `active.topic.count` metric stays at 0
- [ ] OTel scrape shows all 6 broker-level metrics when feature is enabled
