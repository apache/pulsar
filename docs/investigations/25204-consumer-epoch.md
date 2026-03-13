# Investigation: GitHub issue #25204 – MultiTopicsConsumer receives message with older ConsumerEpoch after redeliverUnacknowledgedMessages()

## Summary

MultiTopicsConsumer can deliver one or more messages from a batch with an older consumer epoch after `redeliverUnacknowledgedMessages()` has incremented the epoch, while other messages in the same batch are correctly filtered as stale. The root cause is a race in `MultiTopicsConsumerImpl.receiveMessageFromConsumer`: epoch is validated **per message** inside a `forEach` over the batch, without holding the same lock that `redeliverUnacknowledgedMessages()` uses. When the internal executor is multi-threaded, the redeliver runnable can run between two message iterations and increment `CONSUMER_EPOCH`, so the first message(s) pass the check and later ones fail, producing the observed inconsistency.

## Expected behavior (from PIP-84)

From **PIP-84: Pulsar client: Redeliver command add epoch** (GitHub wiki):

- **Motivation**: Pull and redeliver are not synchronous. Without synchronization, after a redeliver command the client could still receive previously pulled messages and cumulative-ack them, losing the redelivered messages.
- **Design**: Client and server maintain a **consumer epoch**. When the client calls `redeliverUnacknowledgedMessages()`, the epoch is incremented and the command is sent with that epoch; the server updates its epoch. Messages sent to the client carry the epoch at which the cursor read. The **client must filter out any message whose epoch is smaller than the current consumer epoch** (i.e. messages sent before the redeliver).
- **Semantics**: After a successful redeliver, the consumer must not receive a message with a messageId “greater” than what the user had previously received (in the sense of pre-rewind ordering); epoch filtering enforces that.
- **Notes**: Consumer reconnect resets epoch. **Epoch must be increased before clearing the consumer incoming queue.**

So: every message delivered to the application must have `message.getConsumerEpoch() >= CONSUMER_EPOCH.get(this)` at the time of the decision (or be `DEFAULT_CONSUMER_EPOCH` which is treated as “no epoch”, i.e. legacy broker). For a single batch, the decision must be consistent: either all messages in the batch are considered under the same current epoch, or none.

## Observed behavior (from issue PDF)

- **Environment**: Broker 4.0.8, Java client 4.1.1.
- **Setup**: Producer batching enabled, consumer Exclusive, `subscriptionInitialPosition` Earliest, batch index ack disabled.
- **Symptom**: After `redeliverUnacknowledgedMessages()` increments the epoch, a **random message** with `consumerEpoch=0` and `redeliveryCount=0` can still be delivered, while **other messages in the same batch** are filtered as old epoch (log: “Consumer filter old epoch message”).
- **Suspected root cause** (reporter): Race in `MultiTopicsConsumerImpl.receiveMessageFromConsumer` when iterating a batch and validating epoch while the epoch changes concurrently.

## Code reading notes (file paths + method names)

### Epoch storage and increment

| Location | What |
|----------|------|
| `pulsar-client/.../ConsumerBase.java` | `protected volatile long consumerEpoch` (line 128); `CONSUMER_EPOCH = AtomicLongFieldUpdater.newUpdater(ConsumerBase.class, "consumerEpoch")` (119–120). |
| `pulsar-client/.../MultiTopicsConsumerImpl.java` | `redeliverUnacknowledgedMessages()` (718–733): runs on `internalPinnedExecutor`, acquires `incomingQueueLock`, then `CONSUMER_EPOCH.incrementAndGet(this)`, then forwards to sub-consumers and `clearIncomingMessages()`. |
| `pulsar-client/.../ConsumerImpl.java` | `redeliverUnacknowledgedMessages()` (2196–2252): `synchronized(ConsumerImpl.this)`, then `incomingQueueLock.lock()`, then `CONSUMER_EPOCH.incrementAndGet(this)` (2226) for Failover/Exclusive, then clear queue and send redeliver command to broker. |

### Epoch on the wire and on messages

| Location | What |
|----------|------|
| `pulsar-common/.../PulsarApi.proto` | `CommandMessage` has `optional uint64 consumer_epoch = 5`; `CommandRedeliverUnacknowledgedMessages` has `optional uint64 epoch = 3`; `CommandSubscribe` has `optional uint64 consumer_epoch = 19`. |
| `pulsar-common/.../Commands.java` | `DEFAULT_CONSUMER_EPOCH = -1L` (127–129); `newRedeliverUnacknowledgedMessages(long consumerId, long consumerEpoch)` (1182–1186). |
| `pulsar-client/.../MessageImpl.java` | `private long consumerEpoch` (87), set in constructors/factories (e.g. 200). |
| `pulsar-client/.../ConsumerImpl.java` | `messageReceived(CommandMessage cmdMessage, ...)` (1428+): reads `consumerEpoch` from `cmdMessage.getConsumerEpoch()` (1438–1441) and passes it into message creation (1503, 1548–1549, 1568–1570). |

### Validation

| Location | What |
|----------|------|
| `pulsar-client/.../ConsumerBase.java` | `isValidConsumerEpoch(MessageImpl<T> message)` (1377–1387): for Failover/Exclusive, returns false if `message.getConsumerEpoch() != DEFAULT_CONSUMER_EPOCH && message.getConsumerEpoch() < CONSUMER_EPOCH.get(this)`; then logs “Consumer filter old epoch message” and `message.release()`. |
| `pulsar-client/.../ConsumerImpl.java` | Single-message path: `executeNotifyCallback(MessageImpl)` (1372–1392) runs on `internalPinnedExecutor` and calls `isValidConsumerEpoch(message)` before enqueueing. |
| `pulsar-client/.../MultiTopicsConsumerImpl.java` | `receiveMessageFromConsumer(ConsumerImpl, boolean)` (252–318): uses `consumer.batchReceiveAsync()` or `receiveAsync()`, then `thenAcceptAsync(messages -> { ... messages.forEach(msg -> { boolean isValidEpoch = isValidConsumerEpoch(msgImpl); ... }); }, internalPinnedExecutor)` (264–288). **Epoch is checked per message inside the forEach; no lock held.** |

### Call graph (epoch flow)

- **Storage**: `ConsumerBase.consumerEpoch` (volatile long), updated via `CONSUMER_EPOCH` (AtomicLongFieldUpdater).
- **Increment**: `MultiTopicsConsumerImpl.redeliverUnacknowledgedMessages()` (718–733) → `incomingQueueLock.lock()` → `CONSUMER_EPOCH.incrementAndGet(this)` → forward to sub-consumers, `clearIncomingMessages()`. `ConsumerImpl.redeliverUnacknowledgedMessages()` (2196+) → `synchronized` / `incomingQueueLock` → `CONSUMER_EPOCH.incrementAndGet(this)` → clear queue, send `Commands.newRedeliverUnacknowledgedMessages(consumerId, epoch)`.
- **Wire**: `PulsarApi.proto` `CommandMessage.consumer_epoch`, `CommandRedeliverUnacknowledgedMessages.epoch`; `ConsumerImpl.messageReceived(CommandMessage, ...)` reads `cmdMessage.getConsumerEpoch()` and passes into message creation.
- **Validation**: `ConsumerBase.isValidConsumerEpoch(MessageImpl)` (1377–1387): for Failover/Exclusive, reject if `message.getConsumerEpoch() != DEFAULT_CONSUMER_EPOCH && message.getConsumerEpoch() < CONSUMER_EPOCH.get(this)`. Single-topic path: `ConsumerImpl.executeNotifyCallback` calls `isValidConsumerEpoch` before enqueue. Multi-topic path: `MultiTopicsConsumerImpl.receiveMessageFromConsumer` → `thenAcceptAsync(messages -> messages.forEach(msg -> isValidConsumerEpoch(msgImpl)))` — **no lock**, so epoch can change mid-forEach.

### Redeliver call path

1. User or UnAckedMessageTracker/NegativeAcksTracker calls `Consumer.redeliverUnacknowledgedMessages()`.
2. **MultiTopicsConsumerImpl**: `redeliverUnacknowledgedMessages()` (718): `internalPinnedExecutor.execute(() -> { incomingQueueLock.lock(); CONSUMER_EPOCH.incrementAndGet(this); consumers.values().forEach(c -> c.redeliverUnacknowledgedMessages()); clearIncomingMessages(); ... unlock; })`.
3. **ConsumerImpl**: `redeliverUnacknowledgedMessages()` (2196): under `incomingQueueLock` increments epoch, clears queue, then sends `Commands.newRedeliverUnacknowledgedMessages(consumerId, CONSUMER_EPOCH.get(this))` on the connection.

### Concurrency

- **internalPinnedExecutor**: From `ConsumerBase` ctor: `this.internalPinnedExecutor = client.getInternalExecutorService()` which is `internalExecutorProvider.getExecutor()`. Default is `ExecutorProvider(conf.getNumIoThreads(), POOL_NAME_INTERNAL_EXECUTOR)` (PulsarClientResourcesConfigurer.createInternalExecutorProvider), i.e. **multi-threaded** (numIoThreads, typically > 1).
- **incomingQueueLock**: `ReentrantLock` for Failover/Exclusive (ConsumerBase 192); used by `redeliverUnacknowledgedMessages()` and by `enqueueMessageAndCheckBatchReceive()` (968–979). **Not** held during the batch iteration in `receiveMessageFromConsumer`.

## Hypotheses (ranked)

1. **Race in batch iteration (accepted)**
   In `receiveMessageFromConsumer`, the callback runs on `internalPinnedExecutor`. It does `messages.forEach(msg -> { isValidConsumerEpoch(msgImpl); ... })` without holding `incomingQueueLock`. Another runnable on the same (multi-threaded) executor can execute `redeliverUnacknowledgedMessages()`, which acquires `incomingQueueLock` and increments `CONSUMER_EPOCH` between two iterations. So message 1 sees epoch 0 (valid), message 2 sees epoch 1 (invalid, filtered). Evidence: code structure; multi-threaded internal executor; lock not held during forEach.

2. **Per-message vs per-batch epoch on wire**
   Each broker `CommandMessage` carries one epoch for the whole batch. So all messages from one batch have the same `consumerEpoch`. The inconsistency (one delivered, rest filtered) cannot be explained by different epochs on the wire; it requires the **current** consumer epoch to change between validations. (Supports hypothesis 1.)

3. **Single-threaded executor would hide the bug**
   If `internalPinnedExecutor` were single-threaded, the redeliver runnable would run only after the receive callback finished, so no mid-batch increment. Unit tests that use `ExecutorProvider(1, ...)` (e.g. MultiTopicsConsumerImplTest) would not see the race. (Explains why the bug is environment-dependent.)

4. **ConsumerImpl path is safe**
   In ConsumerImpl, each message is dispatched via `internalPinnedExecutor.execute(()->{ isValidConsumerEpoch(...); ... })`. That does not process a batch in one loop; each message is a separate runnable. Redeliver also runs on the same executor and holds `incomingQueueLock`; the lock is the same one used when enqueueing. So for a single ConsumerImpl, processing and redeliver are serialized by the lock. The bug is specific to MultiTopicsConsumerImpl’s batch loop without holding the lock.

## Experiments and results

### Step A – Call graph and epoch locations

- **Commands**: `grep -r consumerEpoch pulsar-client pulsar-common` (and `ConsumerEpoch` / `CONSUMER_EPOCH`); read `ConsumerBase`, `ConsumerImpl`, `MultiTopicsConsumerImpl`, `MessageImpl`, `Commands.java`, `PulsarApi.proto`.
- **Result**: Call graph and “Code reading notes” (including “Call graph (epoch flow)” subsection) documented. Confirmed: epoch stored in `ConsumerBase.consumerEpoch`, updated by `CONSUMER_EPOCH.incrementAndGet(this)` in MultiTopicsConsumerImpl (718–722) and ConsumerImpl under lock; validation in `ConsumerBase.isValidConsumerEpoch` (1377–1387); MultiTopicsConsumerImpl validates per message in forEach (277) without holding `incomingQueueLock`.

### Step B – Where validation runs and exact concurrency boundary

- **Per message vs per batch**: Validation is **per message** in `receiveMessageFromConsumer` (line 277: `isValidConsumerEpoch(msgImpl)` inside `messages.forEach`).
- **Exact concurrency boundary**: The boundary that allows mixed decisions inside one received batch is: (1) the `thenAcceptAsync(messages -> { ... messages.forEach(msg -> { ... isValidConsumerEpoch(msgImpl); ... }); }, internalPinnedExecutor)` callback runs on one thread of `internalPinnedExecutor`; (2) the forEach body does **not** hold `incomingQueueLock`; (3) `redeliverUnacknowledgedMessages()` is also run via `internalPinnedExecutor.execute(...)` and **does** hold `incomingQueueLock` and increments `CONSUMER_EPOCH`. So a second thread of the same pool can run the redeliver runnable **between** two iterations of the forEach (after the first `isValidConsumerEpoch` returns and before the second is called), changing the epoch so the second message is filtered while the first was accepted.
- **Race window**: Between the start and end of `messages.forEach`, any runnable that holds `incomingQueueLock` and increments `CONSUMER_EPOCH` (i.e. `redeliverUnacknowledgedMessages()`) can run on another thread of `internalPinnedExecutor`. So the window is the entire forEach body for each message.
- **Concurrency constructs**: `volatile long consumerEpoch` + `AtomicLongFieldUpdater` (visibility); `ReentrantLock incomingQueueLock` (not held during batch processing); `internalPinnedExecutor` (multi-threaded pool).

### Step C – Reproducible test strategy

- **Option 1 (preferred)**: Unit test with a **controlled multi-threaded executor** and a **single sub-consumer** that returns a batch of messages all with the same (old) epoch. After the receive callback starts and processes the first message, trigger `redeliverUnacknowledgedMessages()` from another thread so it runs mid-iteration (e.g. via a CountDownLatch so the test thread runs redeliver between message 1 and message 2). Assert: either all messages in the batch are filtered, or all are accepted; no mixed outcome.
- **Option 2**: Integration-style test with Embedded Pulsar: multi-topic consumer, producer batching, trigger redeliver while receiving; repeat to observe the flaky “one message delivered, rest filtered” (harder to make deterministic).

### Step D – Deterministic failing test (test subclass override)

- **Technique**: Test-only subclass of `MultiTopicsConsumerImpl` that overrides `isValidConsumerEpoch` (the epoch validation hook used inside the batch loop in `receiveMessageFromConsumer`). On the **first** validation call: (1) call `super.isValidConsumerEpoch(message)` and cache the result (epoch is still 0, so true for message 1); (2) count down latch1 so the test thread knows the receive thread is inside the loop; (3) await latch2 so the receive thread blocks; (4) return the cached result. The test thread: awaits latch1, calls `redeliverUnacknowledgedMessages()` (epoch increments to 1 on another executor thread), then countDown latch2. The receive thread unblocks and continues the forEach; the **second** message is validated with `CONSUMER_EPOCH.get(this) == 1`, so `isValidConsumerEpoch(msg2)` returns false. Outcome: exactly one message accepted by epoch (bug). Assertion: `acceptedByEpochCount != 1` fails.
- **Override point**: `ConsumerBase.isValidConsumerEpoch(MessageImpl)` (protected). The subclass overrides it so the first call blocks between validations; no change to production code.
- **Assertion**: The test counts how many messages pass epoch validation (return true from `isValidConsumerEpoch`) in an `AtomicInteger acceptedByEpochCount` updated inside the override. This avoids being masked by `clearIncomingMessages()` in redeliver (which can clear the queue after the first message is enqueued, making listener+queue count 0).
- **Command run**: From repo root, `cd pulsar-client; mvn -q test "-Dtest=org.apache.pulsar.client.impl.MultiTopicsConsumerEpochRaceTest#testEpochConsistencyWhenRedeliverRunsMidBatch" -DfailIfNoTests=false`
- **Result**: Test **fails** on current code (deterministic): `acceptedByEpoch == 1` (one message accepted by epoch, one filtered), assertion `assertNotEquals(acceptedByEpoch, 1)` fails with: `Bug 25204: batch must not have exactly one message accepted by epoch when epoch changes mid-batch (acceptedByEpoch=1; expected 0 or 2, not 1)`.

### Step E – Fix options (see below)

No code change until investigation doc is complete and a failing test exists or impossibility of a deterministic test is documented.

## Deterministic reproduction design (why it is deterministic)

- **Goal**: Force the epoch to change **between** the first and second `isValidConsumerEpoch` calls inside the same batch iteration in `receiveMessageFromConsumer`, so that message 1 is accepted (epoch 0) and message 2 is rejected (epoch 1), producing the inconsistent “exactly one delivered” outcome.
- **Means**: Block the receive callback thread **inside** the batch loop after the first validation and before the second. While blocked, run `redeliverUnacknowledgedMessages()` on another thread of the same multi-threaded `internalPinnedExecutor`, so the epoch increments. Then unblock so the loop continues and the second message is validated against the new epoch.
- **Why deterministic**: (1) The block is inside the overridden `isValidConsumerEpoch`, so the epoch change is guaranteed to occur between the two validation calls, not before or after the whole batch. (2) Latches enforce ordering: latch1 ensures the test does not call redeliver until the receive thread is past the first validation; latch2 ensures the receive thread does not continue to the second message until redeliver has been submitted and can run. (3) A 2-thread internal executor guarantees the redeliver runnable can run while the receive callback is blocked. No timing sleeps or flaky retries.

## Failing regression test (where it is and what it asserts)

- **Location**: `pulsar-client/src/test/java/org/apache/pulsar/client/impl/MultiTopicsConsumerEpochRaceTest.java`
- **Test method**: `testEpochConsistencyWhenRedeliverRunsMidBatch()`
- **What it asserts**: For a batch of two messages (both with `consumerEpoch == 0`), when the epoch is incremented **between** the validation of the first and second message (via the test subclass that blocks in `isValidConsumerEpoch` on the first call), the number of messages **accepted by epoch** (for which `isValidConsumerEpoch` returned true) must **not** be exactly 1. The test uses an `AtomicInteger acceptedByEpochCount` updated in the overridden `isValidConsumerEpoch` so the assertion is not masked by `clearIncomingMessages()` in redeliver. Assertion: `assertNotEquals(acceptedByEpochCount.get(), 1, "...")`. Under the bug, exactly one message is accepted and one is filtered, so `acceptedByEpoch == 1` and the assertion fails. After the fix (holding `incomingQueueLock` for the whole batch), redeliver cannot run mid-batch, so either both messages are accepted (epoch unchanged) or both filtered (epoch already incremented before the callback); `acceptedByEpoch` is 0 or 2 and the test passes.

## Root cause conclusion (evidence-based)

- **Root cause**: In `MultiTopicsConsumerImpl.receiveMessageFromConsumer`, epoch validation is done **per message** inside `messages.forEach`, **without** holding `incomingQueueLock`. The internal executor is a **multi-threaded** pool. So `redeliverUnacknowledgedMessages()` can run on another thread (after acquiring `incomingQueueLock` and incrementing `CONSUMER_EPOCH`) **between** two iterations of the forEach. The first message(s) are validated when `CONSUMER_EPOCH` is still the old value (e.g. 0) and are delivered; later messages are validated after the increment (e.g. 1) and are filtered. All messages in the batch share the same `consumerEpoch` from the broker; the only changing quantity is `CONSUMER_EPOCH.get(this)` read inside `isValidConsumerEpoch`.
- **Evidence**:
  - `MultiTopicsConsumerImpl.java` 264–288: `thenAcceptAsync(messages -> { ... messages.forEach(msg -> { ... boolean isValidEpoch = isValidConsumerEpoch(msgImpl); ... }); }, internalPinnedExecutor)` with no lock around the forEach.
  - `MultiTopicsConsumerImpl.java` 718–732: `redeliverUnacknowledgedMessages()` holds `incomingQueueLock` and increments `CONSUMER_EPOCH`.
  - `ConsumerBase.java` 1377–1381: `isValidConsumerEpoch` uses `CONSUMER_EPOCH.get(this)` (live read).
  - `PulsarClientResourcesConfigurer.java` 73–74: internal executor created with `conf.getNumIoThreads()` threads (multi-threaded).

## Fix options (at least 2, with tradeoffs)

1. **Hold `incomingQueueLock` for the whole batch processing**
   In `receiveMessageFromConsumer`, before the `messages.forEach`, acquire `incomingQueueLock`; release it after the forEach (and related logic that uses the same message list). Then `redeliverUnacknowledgedMessages()` cannot run in the middle of the batch, so the epoch is stable for the entire batch.
   - **Tradeoff**: Holds the lock for the duration of processing every message in the batch (including `messageReceived` and possibly listener/callbacks). If user code runs in the same thread and is slow, this could delay redeliver. PIP-84 says “increase epoch must before clear consumer incomingQueue”; it does not require the lock to be held during application processing, but holding it during the **epoch check and dispatch** (not necessarily during user callback) is enough. So we can release the lock after the forEach and before or after enqueueing, depending on whether we want to protect only the epoch check or also the enqueue step. Minimal approach: hold lock only for the forEach that does epoch check and calls `messageReceived` (or at least for the epoch-check + decision part).
   - **Risk**: Deadlock if `messageReceived` or anything it calls tries to acquire `incomingQueueLock`. Currently `messageReceived` → `enqueueMessageAndCheckBatchReceive` acquires `incomingQueueLock` (ConsumerBase 968). So we must not hold the lock across `messageReceived` if that path is taken, or we need to ensure the same thread and a reentrant lock (ReentrantLock is reentrant, so same thread can acquire again). So holding lock for the whole forEach is safe from deadlock (same thread re-enters when enqueueing).

2. **Snapshot epoch once at the start of the batch**
   Before the forEach, `long epochAtStart = CONSUMER_EPOCH.get(this);` and in the loop use a local `isValidConsumerEpoch(message, epochAtStart)` (or compare `message.getConsumerEpoch() < epochAtStart`) instead of reading `CONSUMER_EPOCH.get(this)` per message.
   - **Tradeoff**: Consistent view for the whole batch (no mid-batch change). But if a redeliver has already been requested before we started the callback, we want to filter the whole batch. Snapshotting at start means: if epoch was 0 when we started and redeliver runs after we snapshot but before we process any message, we still use 0 and might deliver the whole batch (wrong). So snapshot must be “current epoch at start of processing this batch”; if redeliver runs after we’ve taken the snapshot but before we’ve processed any message, we’d still deliver (epoch 0 batch vs snapshot 0). If redeliver runs before our callback runs, epoch is 1 and we’d snapshot 1 and filter all. So the only problematic case is: our callback runs, we snapshot 0, then before we process any message redeliver runs (epoch=1). Then we process all messages with snapshot 0 and deliver all (wrong). So a pure snapshot-without-lock does not fix the race; we need the snapshot to be taken under the same ordering as redeliver. So **option 2 alone is insufficient**. We need option 1 (lock) or snapshot **under the lock** (equivalent to option 1).

3. **Snapshot under lock**
   At the start of the callback, acquire `incomingQueueLock`, read `long epochSnapshot = CONSUMER_EPOCH.get(this)`, release lock, then run the forEach using `epochSnapshot` for validation.
   - **Tradeoff**: Redeliver can run after we release the lock; it will increment epoch and clear queue. We then deliver messages that might be “stale” relative to the new epoch (we’re using the old snapshot). So we’d deliver a batch that should have been filtered. So this does **not** implement PIP-84 correctly. So **option 3 is wrong** unless we hold the lock for the whole processing (back to option 1).

**Conclusion**: The correct minimal fix is **option 1**: hold `incomingQueueLock` for the entire batch iteration (epoch check + `messageReceived` for each message) so that `redeliverUnacknowledgedMessages()` cannot run in the middle. ReentrantLock allows the same thread to re-enter when `enqueueMessageAndCheckBatchReceive` takes the lock.

## Proposed minimal fix (no code yet)

- In `MultiTopicsConsumerImpl.receiveMessageFromConsumer`, in the `thenAcceptAsync` callback that receives `messages`:
  - Acquire `incomingQueueLock` before the `messages.forEach(...)` block.
  - Keep the existing forEach logic (epoch check, `messageReceived` or `increaseAvailablePermits`, etc.).
  - Release `incomingQueueLock` in a `finally` block after the forEach (and any logic that must see a consistent epoch for the batch).
- This ensures that for the duration of the batch processing, no other thread can run `redeliverUnacknowledgedMessages()` (which acquires the same lock), so `CONSUMER_EPOCH` does not change mid-batch and the outcome is consistent (all valid or all invalid for the batch).
- No change to ConsumerImpl or single-topic path; no change to wire protocol or broker.

## Test plan

1. **Unit test (deterministic regression)**
   - `MultiTopicsConsumerEpochRaceTest.testEpochConsistencyWhenRedeliverRunsMidBatch()` (in `pulsar-client`, group `flaky`): uses a test-only subclass of `MultiTopicsConsumerImpl` that overrides `isValidConsumerEpoch` to block after the first validation (latch1/latch2); 2-thread internal executor; mock sub-consumer returning a batch of 2 messages (epoch 0). The test triggers `redeliverUnacknowledgedMessages()` while the receive thread is blocked between the two validations, then asserts `acceptedByEpochCount.get() != 1` (messages that passed epoch validation). Without the fix the test fails (acceptedByEpoch == 1); with the fix it passes.

2. **Existing tests**
   - Run existing MultiTopicsConsumerImpl and TopicsConsumerImpl tests (including PerMessageUnAcknowledgedRedeliveryTest, TopicsConsumerImplTest) to ensure no regressions.

3. **Integration (optional)**
   - Manual or CI test: multi-topic consumer, Exclusive, batched producer, call redeliver while receiving; verify no “one old-epoch message delivered, rest filtered” behavior.

---

## PIP-84 summary (intended consumer epoch semantics)

From https://github.com/apache/pulsar/wiki/PIP-84-%3A-Pulsar-client%3A-Redeliver-command-add-epoch:

- **Problem**: Pull and redeliver are asynchronous; the client could receive previously pulled messages after sending redeliver and then cumulative-ack them, losing redelivered messages.
- **Solution**: Introduce a **consumer epoch** on client and server. Client increments epoch when calling `redeliverUnacknowledgedMessages()` and sends the new epoch with the command; server updates its epoch. Messages sent to the client carry the epoch at read time. **Client filters any message whose epoch is smaller than the current consumer epoch** (i.e. sent before the redeliver).
- **Protocol**: `CommandRedeliverUnacknowledgedMessages` has `optional uint64 epoch = 3`; `CommandMessage` has `optional uint64 epoch = 5`. Client sends epoch on redeliver; server includes epoch on each message.
- **API**: `redeliverUnacknowledgedMessages()` returns `CompletableFuture<Void>`. Only Exclusive/Failover increase epoch.
- **Ordering**: “Increase epoch must before clear consumer incomingQueue”; consumer reconnect resets epoch.

This investigation confirms that the bug violates the “filter messages with epoch smaller than current” rule for **part** of a batch when the epoch changes **during** the batch iteration in MultiTopicsConsumerImpl.
