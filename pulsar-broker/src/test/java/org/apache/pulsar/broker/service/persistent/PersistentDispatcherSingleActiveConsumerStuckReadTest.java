/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Regression test for a single-active-consumer (Failover) subscription that becomes permanently stuck
 * when a client redeliver command races the completion of an already-dispatched managed-ledger read.
 *
 * <p><b>Four coupled state variables.</b> A caught-up Failover subscription is described by four pieces
 * of state that must stay consistent:
 * <ol>
 *   <li>{@code cursor.waitingReadOp} &mdash; the armed tail-wait read op
 *       ({@link ManagedCursorImpl#hasPendingReadRequest()});</li>
 *   <li>the cursor's membership in {@link ManagedLedgerImpl}'s {@code waitingCursors} queue &mdash; the
 *       set a publish walks to wake parked readers;</li>
 *   <li>{@code cursor.pendingReadOps} &mdash; reads currently in flight
 *       ({@link ManagedCursorImpl#getPendingReadOpsCount()});</li>
 *   <li>{@code dispatcher.havePendingRead} &mdash; the dispatcher's belief that a read is outstanding.</li>
 * </ol>
 * In healthy operation an armed wait op implies {@code havePendingRead}, and implies the cursor is
 * either queued in {@code waitingCursors} or has an in-flight read.
 *
 * <p><b>The race.</b>
 * {@code PersistentDispatcherSingleActiveConsumer#internalRedeliverUnacknowledgedMessages} unconditionally
 * clears {@code havePendingRead} and arms a fresh read via {@code readMoreEntries} <i>without</i> draining
 * a read that {@code notifyEntriesAvailable} has already dispatched. The stale read's completion
 * ({@code readEntriesComplete} / {@code readEntriesFailed}) then clears {@code havePendingRead} again:
 * there is no read-generation guard on {@code havePendingRead}, and the only staleness check is the
 * {@code readConsumer != currentConsumer} identity comparison in {@code readEntriesComplete}, which runs
 * <i>after</i> that same method has already cleared the flag at its top. When the redeliver's re-arm and
 * the stale completion interleave in the order [redeliver, then stale completion], the result is the seed
 * state &mdash; an armed wait op with {@code havePendingRead == false}. This is still benign while the
 * cursor remains in {@code waitingCursors}.
 *
 * <p><b>The absorbing conversion.</b> When the last consumer disconnects, the durable-cursor path
 * ({@code AbstractDispatcherSingleActiveConsumer#removeConsumer} &rarr; {@code cancelPendingRead}, then
 * {@code ManagedLedger#removeWaitingCursor}) deliberately does not cancel the pending read for a durable
 * cursor &mdash; see the durable-cursor comment in {@code PersistentSubscription#removeConsumer}. Because
 * {@code havePendingRead} is already {@code false}, {@code cancelPendingRead} short-circuits on its
 * {@code havePendingRead} guard and leaves the wait op armed, while {@code removeWaitingCursor} strips the
 * cursor from the queue. The subscription is now in the absorbing orphan state: an armed wait op, absent
 * from {@code waitingCursors}, with no in-flight read and {@code havePendingRead == false}. Every
 * subsequent re-arm CAS-fails on the {@code WAITING_READ_OP_UPDATER} compare-and-set in
 * {@code ManagedCursorImpl#asyncReadEntriesWithSkipOrWait}, raising {@code ConcurrentWaitCallbackException}
 * &mdash; which {@code readEntriesFailed} returns early on instead of rescheduling &mdash; and every
 * publish's {@code notifyCursors} poll-misses. The subscription never reads again &mdash; which is exactly
 * what both tests assert behaviourally: after the disconnect a fresh consumer reconnects, and a message
 * published afterwards must actually reach that consumer's {@code sendMessages}.
 *
 * <p>The two racing events genuinely arrive on different threads: the read completion is posted to the
 * dispatcher's ordered executor by the managed-ledger/BookKeeper completion chain (the
 * {@code entriesFuture.whenCompleteAsync(..., executor)} hand-off at the end of {@code readMoreEntries}),
 * while the redeliver is posted by {@code redeliverUnacknowledgedMessages} from the client-command thread.
 * Their arrival order is a real race.
 *
 * <p><b>Fidelity notes / test seams.</b>
 * <ul>
 *   <li>The cursor, ledger, subscription, dispatcher, {@code waitingReadOp} compare-and-set,
 *       {@code checkForNewEntries}, {@code notifyEntriesAvailable}, {@code notifyCursors},
 *       {@code cancelPendingReadRequest}, {@code addWaitingCursor}/{@code removeWaitingCursor} and
 *       {@code havePendingRead} management are all real production code.</li>
 *   <li>The consumer lifecycle runs through the real {@code PersistentSubscription#addConsumer} and
 *       {@code PersistentSubscription#removeConsumer(Consumer, boolean)}, so dispatcher removal,
 *       {@code deactivateCursor()} and {@code removeWaitingCursor()} happen in the production order. The
 *       spied dispatcher is handed to the subscription through the existing production test seam
 *       {@code PersistentSubscription#reuseOrCreateDispatcher}.</li>
 *   <li>Only the metadata-parsing filter step ({@code filterEntriesForConsumer}) is stubbed on a Mockito
 *       spy, because the entries this test publishes are raw bytes rather than serialized Pulsar messages.
 *       The delivery tail itself is real: {@code dispatchEntriesToConsumer} runs and calls
 *       {@code Consumer#sendMessages}, which the mock consumer records before releasing the entries and
 *       completing immediately &mdash; so the production {@code readMoreEntries} continuation is posted by
 *       the real listener. Neither seam touches the four state variables above.</li>
 *   <li>The dispatcher's ordered executor is replaced with a {@link ManualExecutor} (injected through a
 *       spied {@code getTopicOrderedExecutor()} scoped to dispatcher construction) so every dispatcher
 *       async hop becomes an explicit task the test drains in a chosen order.</li>
 *   <li>The consumer's ack of {@code m1} is telescoped: it is applied before the dispatcher delivery of
 *       {@code m1}'s in-flight read completes. This is what keeps the schedule deterministic &mdash; after
 *       the ack the redeliver's {@code rewind} lands at the tail with no backlog, so the re-arm is a pure
 *       in-memory tail-wait and no BookKeeper read escapes the {@link ManualExecutor} to race the drain.
 *       Without the ack the {@code rewind} re-exposes the unacked entry and {@code readMoreEntries} takes
 *       the immediate-read branch of {@code ManagedCursorImpl#asyncReadEntriesWithSkipOrWait} (the
 *       {@code hasMoreEntries()} fast path that delegates straight to {@code asyncReadEntriesWithSkip}),
 *       issuing a real async read whose completion is off-board. The staged state (mark-delete at the
 *       tail plus an already-dispatched read whose completion is still pending) is production-reachable
 *       via a redeliver&rarr;ack&rarr;redeliver sequence.</li>
 *   <li>{@code newEntriesCheckDelayInMillis} is pinned to {@code 0} so {@code checkForNewEntries} runs
 *       inline at arm time rather than on the ledger scheduler. This is a determinism pin only; the bug
 *       itself is not an artifact of the zero delay. With the telescoped ack in place, every
 *       dispatcher-visible async hop is then an explicit task on the {@link ManualExecutor} board that the
 *       test drains in a chosen order.</li>
 * </ul>
 *
 * <p>Reproduces apache/pulsar#26164.
 */
@CustomLog
public class PersistentDispatcherSingleActiveConsumerStuckReadTest extends MockedBookKeeperTestCase {

    private static final String TOPIC = "persistent://prop/ns/sac-stuck-read";
    private static final int BOARD_WAIT_SECONDS = 5;
    private static final long QUIESCE_TIMEOUT_SECONDS = 10;
    private static final int QUIESCE_IDLE_ROUNDS = 4;
    private static final long DELIVERY_TIMEOUT_SECONDS = 5;

    private PulsarTestContext pulsarTestContext;
    private BrokerService brokerService;

    // Real managed-ledger objects under test.
    private ManagedLedgerImpl ledger;
    private ManagedCursorImpl cursor;

    // Real topic + subscription; the dispatcher is a Mockito spy so we can stub the entry filter step.
    private PersistentTopic topic;
    private PersistentSubscription subscription;
    private PersistentDispatcherSingleActiveConsumer dispatcher;
    private Consumer consumer;

    // The dispatcher's ordered executor: a manual task board we drain in a chosen order.
    private ManualExecutor dispatcherExecutor;

    // Everything the dispatcher handed to a Consumer#sendMessages tail, in delivery order.
    private final List<Delivery> deliveries = Collections.synchronizedList(new ArrayList<>());

    private final AtomicLong msgCounter = new AtomicLong();

    @Override
    protected ManagedLedgerConfig initManagedLedgerConfig(ManagedLedgerConfig config) {
        super.initManagedLedgerConfig(config);
        // Inline checkForNewEntries: the +10ms task runs synchronously at arm time -> deterministic.
        config.setNewEntriesCheckDelayInMillis(0);
        config.setMaxEntriesPerLedger(1_000_000);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setRetentionSizeInMB(-1);
        return config;
    }

    // -----------------------------------------------------------------------------------------------
    // Fixture
    // -----------------------------------------------------------------------------------------------

    /** Build a fresh topic/subscription/dispatcher on top of a real ledger + cursor. */
    private void buildFixture(String ledgerName) throws Exception {
        ServiceConfiguration svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("test");
        svcConfig.setActiveConsumerFailoverDelayTimeMillis(0);
        svcConfig.setSystemTopicEnabled(false);
        svcConfig.setTopicLevelPoliciesEnabled(false);

        // Real BrokerService via PulsarTestContext, but backed by the real MockedBookKeeper-based
        // ManagedLedgerFactory so we open a REAL ledger + cursor.
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .spyByDefault()
                .managedLedgerClients(bkc, factory)
                .build();
        brokerService = pulsarTestContext.getBrokerService();

        ledger = (ManagedLedgerImpl) factory.open(ledgerName, initManagedLedgerConfig(new ManagedLedgerConfig()));
        topic = new PersistentTopic(TOPIC, ledger, brokerService);
        // Open the cursor AFTER topic construction so the topic does not auto-create a second
        // subscription sharing this cursor.
        cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        // A real subscription that adopts the spied dispatcher through the production test seam, so
        // addConsumer/removeConsumer drive the real consumer lifecycle. The returned reference must be
        // qualified: the inherited PersistentSubscription.dispatcher field would otherwise shadow ours.
        subscription = new PersistentSubscription(topic, "sub", cursor, false) {
            @Override
            protected Dispatcher reuseOrCreateDispatcher(Dispatcher existingDispatcher, Consumer newConsumer) {
                return PersistentDispatcherSingleActiveConsumerStuckReadTest.this.dispatcher;
            }
        };

        // Inject the manual dispatcher executor via getTopicOrderedExecutor().chooseThread(), scoped to
        // just the dispatcher construction so unrelated broker work keeps using the real executor.
        OrderedExecutor realTopicOrdered = brokerService.getTopicOrderedExecutor();
        dispatcherExecutor = new ManualExecutor();
        OrderedExecutor topicOrdered = mock(OrderedExecutor.class);
        doReturn(dispatcherExecutor).when(topicOrdered).chooseThread();
        doReturn(dispatcherExecutor).when(topicOrdered).chooseThread(any());
        doReturn(topicOrdered).when(brokerService).getTopicOrderedExecutor();
        dispatcher = spy(new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Failover, -1, topic,
                subscription));
        doReturn(realTopicOrdered).when(brokerService).getTopicOrderedExecutor();

        // The published payloads are raw bytes, not serialized Pulsar messages, so the metadata-parsing
        // filter step is stubbed out. The rest of the delivery tail (dispatchEntriesToConsumer ->
        // Consumer#sendMessages) is real production code.
        doReturn(0).when(dispatcher).filterEntriesForConsumer(any(), any(), any(), any(), any(),
                anyBoolean(), any());

        deliveries.clear();
        consumer = newMockConsumer("c1");
    }

    /**
     * A mock consumer whose {@code sendMessages} tail records what the dispatcher delivered, releases the
     * entries and completes immediately. The succeeded future notifies its listener inline, so the
     * production {@code readMoreEntries} continuation lands on the manual board exactly as it does in
     * production.
     */
    private Consumer newMockConsumer(String consumerName) {
        Consumer mockConsumer = mock(Consumer.class);
        doReturn(1000).when(mockConsumer).getAvailablePermits();
        doReturn(1).when(mockConsumer).getAvgMessagesPerEntry();
        doReturn(true).when(mockConsumer).isWritable();
        doReturn(false).when(mockConsumer).readCompacted();
        doReturn(false).when(mockConsumer).isPreciseDispatcherFlowControl();
        doReturn(false).when(mockConsumer).isBlocked();
        doReturn(consumerName).when(mockConsumer).consumerName();
        doReturn(0).when(mockConsumer).getPriorityLevel();
        doReturn(0L).when(mockConsumer).getConsumerEpoch();
        doReturn(SubType.Failover).when(mockConsumer).subType();
        // PersistentSubscription.removeConsumer folds the removed consumer's counters into the subscription.
        doReturn(new ConsumerStatsImpl()).when(mockConsumer).getStats();
        doAnswer(inv -> {
            List<Entry> entries = inv.getArgument(0);
            for (Entry entry : entries) {
                deliveries.add(Delivery.of(consumerName, entry.getPosition()));
                entry.release();
            }
            EntryBatchSizes batchSizes = inv.getArgument(1);
            if (batchSizes != null) {
                batchSizes.recyle();
            }
            EntryBatchIndexesAcks batchIndexesAcks = inv.getArgument(2);
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
        }).when(mockConsumer).sendMessages(any(), any(), any(), anyInt(), anyLong(), anyLong(), any(), anyLong());
        return mockConsumer;
    }

    private void tearDownFixture() {
        try {
            if (cursor != null && !cursor.isClosed()) {
                cursor.close();
            }
        } catch (Exception ignore) {
            // best-effort cleanup
        }
        try {
            if (ledger != null) {
                ledger.close();
            }
        } catch (Exception ignore) {
            // best-effort cleanup
        }
        try {
            if (pulsarTestContext != null) {
                pulsarTestContext.close();
            }
        } catch (Exception ignore) {
            // best-effort cleanup
        }
        cursor = null;
        ledger = null;
        brokerService = null;
        pulsarTestContext = null;
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod() {
        tearDownFixture();
    }

    // -----------------------------------------------------------------------------------------------
    // Manual executor task board
    // -----------------------------------------------------------------------------------------------

    /** A deterministic executor: submitted Runnables queue up and run only when the test drains them. */
    static final class ManualExecutor extends AbstractExecutorService {
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();

        @Override
        public synchronized void execute(Runnable command) {
            queue.add(command);
        }

        synchronized int size() {
            return queue.size();
        }

        /** Remove and return all currently-queued tasks (tasks added later are NOT included). */
        synchronized List<Runnable> takeAll() {
            List<Runnable> snapshot = new ArrayList<>(queue);
            queue.clear();
            return snapshot;
        }

        /** Run one task FIFO; returns false if empty. */
        boolean runOne() {
            Runnable r;
            synchronized (this) {
                r = queue.poll();
            }
            if (r == null) {
                return false;
            }
            r.run();
            return true;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return takeAll();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    // -----------------------------------------------------------------------------------------------
    // Operation alphabet (each drives REAL code)
    // -----------------------------------------------------------------------------------------------

    /** Connect a consumer via the REAL PersistentSubscription.addConsumer path; for Failover with a zero
     *  failover delay this arms the tail-wait read synchronously through the dispatcher's
     *  scheduleReadOnActiveConsumer -> readMoreEntries. */
    private void connect(Consumer consumerToAdd) throws Exception {
        subscription.addConsumer(consumerToAdd).get();
    }

    /**
     * Disconnect a consumer via the REAL {@code PersistentSubscription.removeConsumer(consumer, false)} path,
     * so dispatcher removal (which calls {@code cancelPendingRead()}), {@code deactivateCursor()} and
     * {@code removeWaitingCursor()} all run in the production order. For a durable cursor the subscription
     * deliberately does not cancel the pending read &mdash; see the durable-cursor comment in
     * {@code PersistentSubscription#removeConsumer}.
     */
    private void disconnect(Consumer consumerToRemove) throws Exception {
        subscription.removeConsumer(consumerToRemove, false);
    }

    private Position publish() throws Exception {
        return ledger.addEntry(("m" + msgCounter.incrementAndGet()).getBytes());
    }

    private void ackUpTo(Position p) throws Exception {
        cursor.markDelete(p);
    }

    /** Block (with an explicit timeout) until the dispatcher board holds at least {@code n} tasks. */
    private void awaitBoardHasAtLeast(int n) {
        Awaitility.await("dispatcher board should hold at least " + n + " task(s)")
                .atMost(Duration.ofSeconds(BOARD_WAIT_SECONDS))
                .pollInterval(Duration.ofMillis(2))
                .until(() -> dispatcherExecutor.size() >= n);
    }

    /**
     * Drain the dispatcher board until nothing new is produced, with a hard deadline so a regression can
     * never hang CI. Async read completions posted by the (real) managed-ledger worker pool land on the
     * board between idle rounds.
     */
    private void quiesce() {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(QUIESCE_TIMEOUT_SECONDS);
        for (int idleRounds = 0; idleRounds < QUIESCE_IDLE_ROUNDS;) {
            if (System.nanoTime() > deadlineNanos) {
                throw new IllegalStateException("quiesce() did not settle within " + QUIESCE_TIMEOUT_SECONDS
                        + "s; board=" + dispatcherExecutor.size() + " pendingReadOps="
                        + cursor.getPendingReadOpsCount());
            }
            boolean did = false;
            while (dispatcherExecutor.runOne()) {
                did = true;
            }
            if (did) {
                idleRounds = 0;
                continue;
            }
            if (dispatcherExecutor.size() == 0 && cursor.getPendingReadOpsCount() == 0) {
                idleRounds++;
            }
            sleepQuietly();
        }
    }

    /**
     * Keep draining the dispatcher board until {@code position} has been handed to {@code target}'s
     * {@code sendMessages}, with a hard deadline so a regression fails instead of hanging CI.
     */
    private boolean pumpUntilDelivered(Consumer target, Position position) {
        Delivery expected = Delivery.of(target.consumerName(), position);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(DELIVERY_TIMEOUT_SECONDS);
        do {
            while (dispatcherExecutor.runOne()) {
                // A delivery posts follow-up work onto the board; keep draining before re-checking.
            }
            if (deliveries.contains(expected)) {
                return true;
            }
            sleepQuietly();
        } while (System.nanoTime() < deadlineNanos);
        return false;
    }

    private static void sleepQuietly() {
        try {
            Thread.sleep(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -----------------------------------------------------------------------------------------------
    // State snapshot + predicates
    // -----------------------------------------------------------------------------------------------

    /** One entry handed to a consumer by the dispatcher's real delivery tail. */
    private record Delivery(String consumerName, long ledgerId, long entryId) {

        static Delivery of(String consumerName, Position position) {
            return new Delivery(consumerName, position.getLedgerId(), position.getEntryId());
        }

        @Override
        public String toString() {
            return consumerName + "@" + ledgerId + ":" + entryId;
        }
    }

    /**
     * A snapshot of the four coupled state variables. {@code inQueue} is derived from
     * {@code ledger.getWaitingCursorsCount()}; the single-cursor fixture makes that ledger-wide count a
     * per-cursor membership proxy (0 &hArr; this cursor is not queued).
     */
    private record StuckState(boolean armed, boolean inQueue, int pendingReadOps, boolean havePendingRead) {

        /** The absorbing orphan state: armed, not queued, no in-flight read, and havePendingRead cleared. */
        boolean stranded() {
            return armed && !inQueue && pendingReadOps == 0 && !havePendingRead;
        }

        /** The benign seed: armed with havePendingRead cleared, but still queued (so not yet stranded). */
        boolean seed() {
            return armed && inQueue && !havePendingRead;
        }

        @Override
        public String toString() {
            return "armed=" + armed + " inQueue=" + inQueue + " pendingReadOps=" + pendingReadOps
                    + " havePendingRead=" + havePendingRead;
        }
    }

    private StuckState snapshot() {
        return new StuckState(cursor.hasPendingReadRequest(), ledger.getWaitingCursorsCount() >= 1,
                cursor.getPendingReadOpsCount(), dispatcher.havePendingRead);
    }

    /**
     * Tail shared by both schedules, asserting the externally observable contract of #26164: after the last
     * consumer disconnects and a fresh consumer reconnects to the same durable subscription, a message
     * published afterwards must actually be delivered to that consumer. On buggy master the primary
     * schedule leaves the cursor in the absorbing orphan state, the reconnect's re-arm CAS-fails and the
     * message never reaches {@code sendMessages}.
     */
    private void assertSubscriptionDeliversAfterReconnect(String schedule, StuckState preDisconnect)
            throws Exception {
        disconnect(consumer);
        quiesce();
        StuckState postDisconnect = snapshot();
        log.info().attr("state", postDisconnect).log("post-disconnect quiescent state");

        // A client reconnect: a brand new consumer takes over the (still durable) subscription.
        Consumer reconnected = newMockConsumer("c2");
        connect(reconnected);
        quiesce();
        StuckState postReconnect = snapshot();
        log.info().attr("state", postReconnect).log("post-reconnect quiescent state");

        Position m2 = publish();
        boolean deliveredM2 = pumpUntilDelivered(reconnected, m2);
        quiesce();
        StuckState afterWake = snapshot();
        log.info().attr("state", afterWake).attr("delivered", deliveredM2).log("post-wake-publish state");

        String states = "schedule=[" + schedule + "] pre-disconnect=[" + preDisconnect + "] post-disconnect=["
                + postDisconnect + "] post-reconnect=[" + postReconnect + "] after-wake-publish=[" + afterWake
                + "] deliveries=" + deliveries;

        // Primary, behavioural: the reconnected consumer must actually receive the new message.
        Assert.assertTrue(deliveredM2,
                "BUG REPRODUCED: after the last consumer disconnected and a new one reconnected, the message "
                        + "published next (" + m2 + ") never reached the reconnected consumer's sendMessages within "
                        + DELIVERY_TIMEOUT_SECONDS + "s; the Failover subscription is permanently stuck. " + states);
        Assert.assertTrue(cursor.getReadPosition().compareTo(m2) > 0,
                "the cursor read position must have advanced past the delivered message " + m2 + ", but it is "
                        + cursor.getReadPosition() + ". " + states);

        // Secondary, diagnostic: name the internal tuple that causes the stall, so a failure points at it.
        Assert.assertFalse(afterWake.stranded(),
                "the cursor must not be left in the absorbing orphan state (armed waitingReadOp, absent from "
                        + "ManagedLedger.waitingCursors, no in-flight read, havePendingRead=false). " + states);
    }

    /** Stage the shared precondition: an armed tail-wait read plus a read completion pending on the board. */
    private void stageInFlightReadCompletion() throws Exception {
        // Arm the tail-wait read: addConsumer -> scheduleReadOnActiveConsumer -> readMoreEntries.
        connect(consumer);
        Assert.assertTrue(cursor.hasPendingReadRequest(), "the tail-wait read op should be armed after addConsumer");
        Assert.assertEquals(ledger.getWaitingCursorsCount(), 1, "the cursor should be registered in waitingCursors");
        Assert.assertTrue(dispatcher.havePendingRead, "havePendingRead should be true while the tail read is armed");

        // Publish m1: notifyCursors -> notifyEntriesAvailable dispatches the armed op's read, advancing the
        // read position past m1 and posting readEntriesComplete onto the board. The wait op is now consumed,
        // so hasPendingReadRequest() is false, but havePendingRead is still true.
        Position m1 = publish();
        awaitBoardHasAtLeast(1);
        Assert.assertFalse(cursor.hasPendingReadRequest(),
                "the armed op should have been consumed by notifyEntriesAvailable");

        // Telescoped ack: mark-delete m1 before its already-dispatched read completes. This lands the
        // redeliver's subsequent rewind at the tail with no backlog, so the re-arm is a pure in-memory
        // tail-wait and no BookKeeper read escapes the ManualExecutor to race quiesce(). The staged state
        // (mark-delete at tail + an already-dispatched read whose completion is still pending) is
        // production-reachable via a redeliver->ack->redeliver sequence (see the class Javadoc).
        ackUpTo(m1);

        // Redeliver: posts internalRedeliver onto the board, behind the still-pending read completion.
        dispatcher.redeliverUnacknowledgedMessages(consumer, 1L);
        awaitBoardHasAtLeast(2);
    }

    /**
     * The board now holds exactly two tasks from two different producers:
     * <ol start="0">
     *   <li>{@code readEntriesComplete} &mdash; posted by the managed-ledger/BookKeeper completion chain
     *       ({@code whenCompleteAsync});</li>
     *   <li>{@code internalRedeliver} &mdash; posted by {@code redeliverUnacknowledgedMessages} (the
     *       client-command thread).</li>
     * </ol>
     */
    private List<Runnable> takeTwoRacingTasks() {
        List<Runnable> board = dispatcherExecutor.takeAll();
        Assert.assertEquals(board.size(), 2,
                "the board must hold exactly two tasks from two producers: the read completion "
                        + "(posted by the managed-ledger/BookKeeper completion chain) and internalRedeliver "
                        + "(posted by the client-command thread)");
        return board;
    }

    // ===============================================================================================
    // (a) PRIMARY reproduction: a redeliver races the completion of an already-dispatched read. m1's ack is
    // telescoped (applied before that read completes) so the redeliver's rewind lands at the tail and the
    // re-arm is a pure in-memory tail-wait -- fully deterministic. See the class Javadoc fidelity notes.
    // ===============================================================================================

    @Test(groups = "broker")
    public void testFailoverConsumerStuckWhenRedeliverRacesInFlightReadCompletion() throws Exception {
        buildFixture("sac-stuck-read-race");
        stageInFlightReadCompletion();
        List<Runnable> board = takeTwoRacingTasks();

        // Critical interleaving: run the redeliver's re-arm BEFORE the stale read completion. internalRedeliver
        // clears havePendingRead and arms a fresh read; the stale completion then clears havePendingRead again
        // while the fresh op stays armed -> the seed state.
        board.get(1).run();   // internalRedeliver
        board.get(0).run();   // stale readEntriesComplete
        quiesce();

        // Observation only (NOT asserted): the race mints the seed (armed op + havePendingRead=false, still
        // queued). The seed is deliberately not asserted -- a completion-side fix (a read-generation guard on
        // havePendingRead) would prevent the seed from ever forming, so binding on it would couple this test to
        // one fix strategy. The binding assertions are the behavioural ones after the disconnect + reconnect.
        StuckState seed = snapshot();
        log.info().attr("state", seed).attr("seedPreconditionHeld", seed.seed())
                .log("post-redeliver-race quiescent state (observation only)");

        assertSubscriptionDeliversAfterReconnect("redeliver, then stale completion", seed);
    }

    // ===============================================================================================
    // (b) NEGATIVE CONTROL: the same setup but the benign FIFO order [stale completion, then redeliver].
    // The invariant holds end to end; this PASSES on master.
    // ===============================================================================================

    @Test(groups = "broker")
    public void testRedeliverAfterReadCompletionDoesNotStrandCursor() throws Exception {
        buildFixture("sac-stuck-read-fifo");
        stageInFlightReadCompletion();
        List<Runnable> board = takeTwoRacingTasks();

        // Benign FIFO order: the stale completion runs FIRST (clearing havePendingRead), then the redeliver
        // re-arms and sets havePendingRead=true. The armed op and havePendingRead stay coupled, so no seed
        // forms and the later disconnect can cancel the op cleanly.
        board.get(0).run();   // stale readEntriesComplete
        board.get(1).run();   // internalRedeliver
        quiesce();

        StuckState healthy = snapshot();
        log.info().attr("state", healthy).log("post-FIFO-race quiescent state");
        Assert.assertFalse(healthy.stranded(), "the benign FIFO order must not strand the cursor: " + healthy);

        // The disconnect now finds havePendingRead=true, so cancelPendingRead actually cancels the armed op:
        // no orphan is created, the reconnect re-arms cleanly and the next publish is delivered.
        assertSubscriptionDeliversAfterReconnect("stale completion, then redeliver (negative control)", healthy);
    }
}
