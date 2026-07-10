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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
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
 * <p><b>The race.</b> {@code internalRedeliverUnacknowledgedMessages}
 * (PersistentDispatcherSingleActiveConsumer:275-306) unconditionally clears {@code havePendingRead}
 * (:300) and arms a fresh read via {@code readMoreEntries} (:305) <i>without</i> draining a read that
 * {@code notifyEntriesAvailable} has already dispatched. The stale read's completion
 * ({@code readEntriesComplete} :168 / {@code readEntriesFailed} :456) then clears {@code havePendingRead}
 * again: there is no read-generation guard on {@code havePendingRead}, and the only staleness check is
 * the consumer-identity comparison at :200, which runs <i>after</i> the flag was already cleared at :168.
 * When the redeliver's re-arm and the stale completion interleave in the order [redeliver, then stale
 * completion], the result is the seed state &mdash; an armed wait op with {@code havePendingRead == false}.
 * This is still benign while the cursor remains in {@code waitingCursors}.
 *
 * <p><b>The absorbing conversion.</b> When the last consumer disconnects, the durable-cursor path
 * ({@code AbstractDispatcherSingleActiveConsumer.removeConsumer} &rarr; {@code cancelPendingRead} :157,
 * then {@code ManagedLedger.removeWaitingCursor}) deliberately does not cancel the pending read for a
 * durable cursor &mdash; see the comment at PersistentSubscription:381-388. Because {@code havePendingRead}
 * is already {@code false}, {@code cancelPendingRead} short-circuits and leaves the wait op armed, while
 * {@code removeWaitingCursor} strips the cursor from the queue. The subscription is now in the absorbing
 * orphan state: an armed wait op, absent from {@code waitingCursors}, with no in-flight read and
 * {@code havePendingRead == false}. Every subsequent re-arm CAS-fails at ManagedCursorImpl:1110 with
 * {@code ConcurrentWaitCallbackException} (which {@code readEntriesFailed} :465-468 does not reschedule),
 * and every publish's {@code notifyCursors} poll-misses. The subscription never reads again.
 *
 * <p>The two racing events genuinely arrive on different threads: the read completion is posted to the
 * dispatcher's ordered executor by the managed-ledger/BookKeeper completion chain
 * ({@code whenCompleteAsync}, PersistentDispatcherSingleActiveConsumer:380-386), while the redeliver is
 * posted by the client-command thread (:272). Their arrival order is a real race.
 *
 * <p><b>Fidelity notes / test seams.</b>
 * <ul>
 *   <li>The cursor, ledger, dispatcher, {@code waitingReadOp} compare-and-set, {@code checkForNewEntries},
 *       {@code notifyEntriesAvailable}, {@code notifyCursors}, {@code cancelPendingReadRequest},
 *       {@code addWaitingCursor}/{@code removeWaitingCursor} and {@code havePendingRead} management are all
 *       real production code.</li>
 *   <li>The dispatcher's message-delivery tail ({@code filterEntriesForConsumer} +
 *       {@code dispatchEntriesToConsumer}) is stubbed on a Mockito spy to release the entries and post the
 *       same {@code readMoreEntries} continuation the real code posts at {@code dispatchEntriesToConsumer}
 *       :239. It touches none of the four state variables above.</li>
 *   <li>The dispatcher's ordered executor is replaced with a {@link ManualExecutor} (injected through a
 *       spied {@code getTopicOrderedExecutor()} scoped to dispatcher construction) so every dispatcher
 *       async hop becomes an explicit task the test drains in a chosen order.</li>
 *   <li>The consumer's ack of {@code m1} is telescoped: it is applied before the dispatcher delivery of
 *       {@code m1}'s in-flight read completes. This is what keeps the schedule deterministic &mdash; after
 *       the ack the redeliver's {@code rewind} lands at the tail with no backlog, so the re-arm is a pure
 *       in-memory tail-wait and no BookKeeper read escapes the {@link ManualExecutor} to race the drain.
 *       Without the ack the {@code rewind} re-exposes the unacked entry and {@code readMoreEntries} takes
 *       the immediate-read branch (ManagedCursorImpl:1099-1103), issuing a real async read whose completion
 *       is off-board. The staged state (mark-delete at the tail plus an already-dispatched read whose
 *       completion is still pending) is production-reachable via a redeliver&rarr;ack&rarr;redeliver
 *       sequence.</li>
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

    private PulsarTestContext pulsarTestContext;
    private BrokerService brokerService;

    // Real managed-ledger objects under test.
    private ManagedLedgerImpl ledger;
    private ManagedCursorImpl cursor;

    // Real dispatcher (Mockito spy so we can stub the delivery tail + pin the active consumer).
    private PersistentTopic topic;
    private PersistentSubscription subscription;
    private PersistentDispatcherSingleActiveConsumer dispatcher;
    private Consumer consumer;

    // The dispatcher's ordered executor: a manual task board we drain in a chosen order.
    private ManualExecutor dispatcherExecutor;

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
        subscription = new PersistentSubscription(topic, "sub", cursor, false);

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

        consumer = mock(Consumer.class);
        doReturn(1000).when(consumer).getAvailablePermits();
        doReturn(1).when(consumer).getAvgMessagesPerEntry();
        doReturn(true).when(consumer).isWritable();
        doReturn(false).when(consumer).readCompacted();
        doReturn(false).when(consumer).isPreciseDispatcherFlowControl();
        doReturn(false).when(consumer).isBlocked();
        doReturn("c1").when(consumer).consumerName();
        doReturn(0).when(consumer).getPriorityLevel();
        doReturn(0L).when(consumer).getConsumerEpoch();

        // Stub only the message-delivery tail; preserve the readEntriesComplete invariant logic + re-arm post.
        doAnswer(inv -> {
            List<Entry> entries = inv.getArgument(1);
            entries.forEach(Entry::release);
            dispatcherExecutor.execute(() -> dispatcher.readMoreEntries(dispatcher.getActiveConsumer()));
            return null;
        }).when(dispatcher).dispatchEntriesToConsumer(any(), any(), any(), any(), any(), anyLong());
        doReturn(0).when(dispatcher).filterEntriesForConsumer(any(), any(), any(), any(), any(),
                anyBoolean(), any());
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

    /** Connect the (only) consumer via the REAL addConsumer path; for Failover+delay=0 this arms the
     *  tail-wait read synchronously through scheduleReadOnActiveConsumer -> readMoreEntries. */
    private void arm() throws Exception {
        dispatcher.addConsumer(consumer).get();
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

    private static void sleepQuietly() {
        try {
            Thread.sleep(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * The last-consumer disconnect conversion for a durable cursor: the two operative production steps that
     * {@code PersistentSubscription.removeConsumer} performs. {@code dispatcher.removeConsumer} calls
     * {@code cancelPendingRead()} (which short-circuits when {@code havePendingRead} is already false,
     * leaving the op armed), then the subscription strips the cursor from the waiting queue &mdash; see the
     * comment at PersistentSubscription:381-388 explaining why the pending read is intentionally not
     * cancelled for a durable cursor.
     */
    private void applyLastConsumerDisconnect() throws Exception {
        dispatcher.removeConsumer(consumer);
        ledger.removeWaitingCursor(cursor);
    }

    // -----------------------------------------------------------------------------------------------
    // State snapshot + predicates
    // -----------------------------------------------------------------------------------------------

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
     * Tail for the reproduction schedule: disconnect the last consumer, publish once more, and assert the
     * five-leg failure at quiescence. FAILS deterministically on buggy master.
     */
    private void assertCursorStrandedAfterDisconnectAndWake(StuckState seed) throws Exception {
        applyLastConsumerDisconnect();
        quiesce();
        StuckState postDisconnect = snapshot();
        log.info().attr("state", postDisconnect).log("post-disconnect quiescent state");

        // A subsequent publish MUST wake the subscription.
        publish();
        quiesce();
        StuckState afterWake = snapshot();
        log.warn().attr("state", afterWake).log("post-wake-publish state");

        // Five legs at quiescence: (1) armed, (2) absent from waitingCursors (count==0), (3) no in-flight
        // read, (4) havePendingRead==false, and (5) the same tuple STILL holds after the wake publish.
        Assert.assertFalse(afterWake.stranded(),
                "BUG REPRODUCED: after the last consumer disconnected, the cursor holds an armed waitingReadOp "
                        + "(hasPendingReadRequest=true) but is absent from ManagedLedger.waitingCursors "
                        + "(waitingCursorsCount=0) with no in-flight read (pendingReadOps=0) while the dispatcher "
                        + "believes havePendingRead=false; a subsequent publish did not wake it (subscription "
                        + "permanently stuck). seed=[" + seed + "] post-disconnect=[" + postDisconnect
                        + "] after-wake-publish=[" + afterWake + "]");
    }

    // ===============================================================================================
    // (a) PRIMARY reproduction: a redeliver races the completion of an already-dispatched read. m1's ack is
    // telescoped (applied before that read completes) so the redeliver's rewind lands at the tail and the
    // re-arm is a pure in-memory tail-wait -- fully deterministic. See the class Javadoc fidelity notes.
    // ===============================================================================================

    @Test(groups = "broker")
    public void testFailoverConsumerStuckWhenRedeliverRacesInFlightReadCompletion() throws Exception {
        buildFixture("sac-stuck-read-race");

        // Arm the tail-wait read: addConsumer -> scheduleReadOnActiveConsumer -> readMoreEntries.
        arm();
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

        // The board now holds exactly two tasks from two different producers:
        //   [0] readEntriesComplete - posted by the managed-ledger/BookKeeper completion chain (whenCompleteAsync)
        //   [1] internalRedeliver   - posted by redeliverUnacknowledgedMessages (the client-command thread)
        List<Runnable> board = dispatcherExecutor.takeAll();
        Assert.assertEquals(board.size(), 2,
                "the board must hold exactly two tasks from two producers: the stale read completion "
                        + "(posted by the managed-ledger/BookKeeper completion chain) and internalRedeliver "
                        + "(posted by the client-command thread)");

        // Critical interleaving: run the redeliver's re-arm BEFORE the stale read completion. internalRedeliver
        // clears havePendingRead and arms a fresh read; the stale completion then clears havePendingRead again
        // while the fresh op stays armed -> the seed state.
        board.get(1).run();   // internalRedeliver
        board.get(0).run();   // stale readEntriesComplete
        quiesce();

        // Observation only (NOT asserted): the race mints the seed (armed op + havePendingRead=false, still
        // queued). The seed is deliberately not asserted -- a completion-side fix (a read-generation guard on
        // havePendingRead) would prevent the seed from ever forming, so binding on it would couple this test to
        // one fix strategy. The single binding assertion is assertFalse(stranded) after the disconnect + wake.
        StuckState seed = snapshot();
        log.info().attr("state", seed).attr("seedPreconditionHeld", seed.seed())
                .log("post-redeliver-race quiescent state (observation only)");

        assertCursorStrandedAfterDisconnectAndWake(seed);
    }

    // ===============================================================================================
    // (b) NEGATIVE CONTROL: the same setup but the benign FIFO order [stale completion, then redeliver].
    // The invariant holds end to end; this PASSES on master.
    // ===============================================================================================

    @Test(groups = "broker")
    public void testRedeliverAfterReadCompletionDoesNotStrandCursor() throws Exception {
        buildFixture("sac-stuck-read-fifo");

        arm();
        Assert.assertTrue(cursor.hasPendingReadRequest(), "the tail-wait read op should be armed after addConsumer");
        Assert.assertEquals(ledger.getWaitingCursorsCount(), 1, "the cursor should be registered in waitingCursors");
        Assert.assertTrue(dispatcher.havePendingRead, "havePendingRead should be true while the tail read is armed");

        Position m1 = publish();
        awaitBoardHasAtLeast(1);
        Assert.assertFalse(cursor.hasPendingReadRequest(),
                "the armed op should have been consumed by notifyEntriesAvailable");

        // Same telescoped-ack staging as the primary test, so the benign schedule is equally deterministic
        // (no off-board async read). The ONLY difference from the primary test is the board drain order below.
        ackUpTo(m1);

        dispatcher.redeliverUnacknowledgedMessages(consumer, 1L);
        awaitBoardHasAtLeast(2);

        List<Runnable> board = dispatcherExecutor.takeAll();
        Assert.assertEquals(board.size(), 2,
                "the board must hold exactly two tasks from two producers: the read completion "
                        + "(posted by the managed-ledger/BookKeeper completion chain) and internalRedeliver "
                        + "(posted by the client-command thread)");

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
        // no orphan is created, and the invariant holds end to end.
        applyLastConsumerDisconnect();
        quiesce();
        StuckState postDisconnect = snapshot();
        log.info().attr("state", postDisconnect).log("post-disconnect quiescent state");

        publish();
        quiesce();
        StuckState afterWake = snapshot();
        log.info().attr("state", afterWake).log("post-wake-publish state");

        Assert.assertFalse(afterWake.stranded(),
                "NEGATIVE CONTROL: with the benign FIFO order the cursor must never be stranded. "
                        + "healthy=[" + healthy + "] post-disconnect=[" + postDisconnect + "] after-wake-publish=["
                        + afterWake + "]");
    }
}
