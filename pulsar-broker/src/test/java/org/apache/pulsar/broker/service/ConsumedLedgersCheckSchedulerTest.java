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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.util.SingleThreadNonConcurrentFixedRateScheduler;
import org.apache.pulsar.utils.TestLogAppender;
import org.testng.annotations.Test;

/** Verifies maintenance submission timing without running timers or BookKeeper threads. */
@Test(groups = "broker", timeOut = 30000)
public class ConsumedLedgersCheckSchedulerTest {
    private static final Duration INTERVAL = Duration.ofSeconds(120);
    private static final Duration STEP = Duration.ofMillis(20);

    @Test
    public void testOneTopicIsCheckedEveryTwentyMillisecondsAndEveryRound() {
        Fixture fixture = new Fixture(3);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            assertThat(fixture.events).isEmpty();
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(INTERVAL.toNanos());

            for (int i = 0; i < 3; i++) {
                fixture.timer.runNext();
                assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, i + 1));
                assertThat(fixture.timer.nowNanos()).isEqualTo(INTERVAL.toNanos() + STEP.toNanos() * i);
                assertThat(fixture.timer.pendingCount()).isEqualTo(1);
                if (i == 0) {
                    PersistentTopic unvisited =
                            (PersistentTopic) fixture.topics.get("topic-2").join().orElseThrow();
                    verify(unvisited, never()).getManagedLedger();
                }
                if (i < 2) {
                    assertThat(fixture.timer.nextDelayNanos()).isEqualTo(STEP.toNanos());
                }
            }
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(Duration.ofMillis(119_960).toNanos());

            runTicks(fixture.timer, 3);
            List<String> twoRounds = new ArrayList<>(expectedEvents(0, 3));
            twoRounds.addAll(expectedEvents(0, 3));
            assertThat(fixture.events).containsExactlyElementsOf(twoRounds);
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofMillis(240_040).toNanos());
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
        }
        assertThat(fixture.timer.pendingCount()).isZero();
    }

    @Test
    public void testLateTickDoesNotCatchUpWithImmediateSubmissions() {
        Fixture fixture = new Fixture(3);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.advanceWithoutRunning(Duration.ofSeconds(500));
            fixture.timer.runNext();

            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofSeconds(500).toNanos());
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(STEP.toNanos());
            fixture.timer.runNext();
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofMillis(500_020).toNanos());
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 2));

            fixture.timer.advanceWithoutRunning(Duration.ofSeconds(200));
            fixture.timer.runNext();
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 3));
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(STEP.toNanos());
            fixture.timer.runNext();
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofMillis(700_040).toNanos());
        }
    }

    @Test
    public void testLongRoundDoesNotWaitAnotherFullIntervalAfterCompletion() {
        Fixture fixture = new Fixture(4);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), Duration.ofMillis(40));
            runTicks(fixture.timer, 4);
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 4));
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofMillis(100).toNanos());
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(STEP.toNanos());
            fixture.timer.runNext();
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofMillis(120).toNanos());
            verify(fixture.ledgers.get("topic-0"), times(2))
                    .trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        }
    }

    @Test
    public void testSlowSubmissionStillWaitsTwentyMillisecondsWithoutAnExtraRoundInterval() {
        Fixture fixture = new Fixture(2);
        doAnswer(invocation -> {
            fixture.events.add("trim:topic-0");
            fixture.timer.advanceWithoutRunning(Duration.ofSeconds(200));
            return null;
        }).when(fixture.ledgers.get("topic-0")).trimConsumedLedgersInBackground(any());
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.runNext();
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofSeconds(320).toNanos());
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(STEP.toNanos());

            fixture.timer.runNext();
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 2));
            assertThat(fixture.timer.nowNanos()).isEqualTo(Duration.ofMillis(320_020).toNanos());
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(STEP.toNanos());
        }
    }

    @Test
    public void testUnloadedTopicsAreSkippedAndReloadedNamesUseTheCurrentInstance() {
        Fixture fixture = new Fixture(3);
        ManagedLedger unloaded = fixture.ledgers.get("topic-1");
        ManagedLedger replaced = fixture.ledgers.get("topic-2");
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.runNext();

            fixture.topics.remove("topic-1");
            ManagedLedger replacement = fixture.addTopic("topic-2");
            ManagedLedger newlyLoaded = fixture.addTopic("topic-3");
            fixture.timer.runNext();

            verify(unloaded, never()).trimConsumedLedgersInBackground(any());
            verify(replaced, never()).trimConsumedLedgersInBackground(any());
            verify(replacement).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
            verify(newlyLoaded, never()).trimConsumedLedgersInBackground(any());
            assertThat(fixture.events).containsExactly("trim:topic-0", "rollover:topic-0",
                    "trim:topic-2", "rollover:topic-2");

            runTicks(fixture.timer, 3);
            verify(unloaded, never()).trimConsumedLedgersInBackground(any());
            verify(replaced, never()).trimConsumedLedgersInBackground(any());
            verify(replacement, times(2)).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
            verify(newlyLoaded).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        }
    }

    @Test
    public void testOnlyLoadedPersistentTopicsWithManagedLedgersAreChecked() {
        Fixture fixture = new Fixture(1);
        fixture.topics.put("non-persistent", CompletableFuture.completedFuture(Optional.of(mock(Topic.class))));
        CompletableFuture<Optional<Topic>> loading = new CompletableFuture<>();
        fixture.topics.put("loading", loading);
        fixture.topics.put("failed", CompletableFuture.failedFuture(new IllegalStateException("load failed")));
        CompletableFuture<Optional<Topic>> cancelled = new CompletableFuture<>();
        cancelled.cancel(false);
        fixture.topics.put("cancelled", cancelled);
        fixture.topics.put("absent", CompletableFuture.completedFuture(Optional.empty()));
        fixture.topics.put("no-ledger", CompletableFuture.completedFuture(Optional.of(mock(PersistentTopic.class))));
        ManagedLedger fencedLedger = fixture.addTopic("fenced");
        PersistentTopic fenced = (PersistentTopic) fixture.topics.get("fenced").join().orElseThrow();
        when(fenced.isFenced()).thenReturn(true);

        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            // Ineligible entries are skipped together rather than consuming individual pacing slots.
            runTicks(fixture.timer, 2);
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
            verify(fencedLedger, never()).trimConsumedLedgersInBackground(any());

            PersistentTopic loaded = mock(PersistentTopic.class);
            ManagedLedger ledger = mock(ManagedLedger.class);
            when(loaded.getManagedLedger()).thenReturn(ledger);
            loading.complete(Optional.of(loaded));
            runTicks(fixture.timer, 3);
            verify(ledger).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
            verify(ledger).rolloverCursorsInBackground();
        }
    }

    @Test
    public void testNullLedgerFencedAndClosingTopicsAfterSnapshotAreSkipped() {
        Fixture fixture = new Fixture(4);
        PersistentTopic noLedger = (PersistentTopic) fixture.topics.get("topic-1").join().orElseThrow();
        PersistentTopic fenced = (PersistentTopic) fixture.topics.get("topic-2").join().orElseThrow();
        PersistentTopic closing = (PersistentTopic) fixture.topics.get("topic-3").join().orElseThrow();
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.runNext();
            when(noLedger.getManagedLedger()).thenReturn(null);
            when(fenced.isFenced()).thenReturn(true);
            when(closing.isClosingOrDeleting()).thenReturn(true);
            fixture.timer.runNext();
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);

            when(noLedger.getManagedLedger()).thenReturn(fixture.ledgers.get("topic-1"));
            when(fenced.isFenced()).thenReturn(false);
            when(closing.isClosingOrDeleting()).thenReturn(false);
            runTicks(fixture.timer, 4);
            List<String> expected = new ArrayList<>(expectedEvents(0, 1));
            expected.addAll(expectedEvents(0, 4));
            assertThat(fixture.events).containsExactlyElementsOf(expected);
        }
    }

    @Test
    public void testSubmissionFailureDoesNotPreventOtherTopicsOrNextRound() {
        Fixture fixture = new Fixture(3);
        ManagedLedger first = fixture.ledgers.get("topic-0");
        doAnswer(invocation -> {
            throw new IllegalStateException("submission failed");
        }).when(first).trimConsumedLedgersInBackground(any());
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            runTicks(fixture.timer, 3);
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(1, 3));
            verify(first, never()).rolloverCursorsInBackground();

            runTicks(fixture.timer, 3);
            verify(first, times(2)).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
            List<String> expected = new ArrayList<>(expectedEvents(1, 3));
            expected.addAll(expectedEvents(1, 3));
            assertThat(fixture.events).containsExactlyElementsOf(expected);
        }
    }

    @Test
    public void testRolloverFailureDoesNotPreventOtherTopics() {
        Fixture fixture = new Fixture(2);
        ManagedLedger first = fixture.ledgers.get("topic-0");
        doAnswer(invocation -> {
            throw new IllegalStateException("rollover failed");
        }).when(first).rolloverCursorsInBackground();
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            runTicks(fixture.timer, 2);
            assertThat(fixture.events).containsExactly("trim:topic-0", "trim:topic-1", "rollover:topic-1");
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
        }
    }

    @Test
    public void testRoundLogsStartProgressAndCompletion() throws Exception {
        Fixture fixture = new Fixture(3);
        var logger = LogManager.getLogger(ConsumedLedgersCheckScheduler.class);
        Level previousLevel = logger.getLevel();
        Configurator.setLevel(logger.getName(), Level.DEBUG);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler();
                TestLogAppender appender = TestLogAppender.create(ConsumedLedgersCheckScheduler.class)) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.runNext();
            List<LogEvent> events = progressEvents(appender);
            assertThat(events).hasSize(1);
            assertProgress(events.get(0), Map.of("phase", "started", "totalTopics", "3", "remainingTopics", "3",
                    "elapsedMs", "0"));

            fixture.timer.advanceWithoutRunning(Duration.ofSeconds(30));
            fixture.timer.runNext();
            events = progressEvents(appender);
            assertThat(events).hasSize(2);
            assertProgress(events.get(1), Map.of("phase", "progress", "totalTopics", "3", "remainingTopics", "1",
                    "elapsedMs", "30000"));

            fixture.timer.runNext();
            events = progressEvents(appender);
            assertThat(events).hasSize(3);
            assertProgress(events.get(2), Map.of("phase", "completed", "totalTopics", "3", "remainingTopics", "0",
                    "elapsedMs", "30020"));
            assertThat(events).allSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.DEBUG));
        } finally {
            Configurator.setLevel(logger.getName(), previousLevel);
        }
    }

    @Test
    public void testSnapshotFailureIsRetriedAtTheNextInterval() {
        SnapshotMap topics = new SnapshotMap();
        Fixture fixture = new Fixture(1, topics);
        topics.snapshotAction = () -> {
            topics.snapshotAction = () -> { };
            throw new IllegalStateException("snapshot failed");
        };
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.runNext();
            assertThat(fixture.events).isEmpty();
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(INTERVAL.toNanos());
            fixture.timer.runNext();
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
        }
    }

    @Test
    public void testCloseDuringSnapshotPreventsPublicationAndSubmission() {
        SnapshotMap topics = new SnapshotMap();
        Fixture fixture = new Fixture(2, topics);
        ConsumedLedgersCheckScheduler scheduler = fixture.scheduler();
        topics.snapshotAction = scheduler::close;
        scheduler.start(fixture.timer.executor(), INTERVAL);
        fixture.timer.runNext();
        assertThat(fixture.events).isEmpty();
        assertThat(fixture.timer.pendingCount()).isZero();
    }

    @Test
    public void testEmptyRoundsKeepDiscoveringTopics() {
        Fixture fixture = new Fixture(0);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.runNext();
            assertThat(fixture.events).isEmpty();
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(INTERVAL.toNanos());
            fixture.addTopic("topic-0");
            fixture.timer.runNext();
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
        }
    }

    @Test
    public void testRepeatedStartDoesNotCreateAnotherTimer() {
        Fixture fixture = new Fixture(2);
        ManualScheduler anotherTimer = new ManualScheduler();
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            scheduler.start(anotherTimer.executor(), Duration.ofSeconds(1));
            assertThat(fixture.timer.pendingCount()).isEqualTo(1);
            assertThat(anotherTimer.pendingCount()).isZero();
            fixture.timer.runNext();
            assertThat(fixture.timer.nextDelayNanos()).isEqualTo(STEP.toNanos());
        }
    }

    @Test
    public void testCloseCancelsTimerAndPreventsAlreadyDequeuedTaskFromSubmitting() {
        Fixture fixture = new Fixture(2);
        ConsumedLedgersCheckScheduler scheduler = fixture.scheduler();
        scheduler.start(fixture.timer.executor(), INTERVAL);
        fixture.timer.runNext();
        Runnable dequeued = fixture.timer.takeNext();
        scheduler.close();
        scheduler.close();
        dequeued.run();
        assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
        assertThat(fixture.timer.pendingCount()).isZero();
        scheduler.start(fixture.timer.executor(), INTERVAL);
        assertThat(fixture.timer.pendingCount()).isZero();

        Fixture pending = new Fixture(1);
        ConsumedLedgersCheckScheduler pendingScheduler = pending.scheduler();
        pendingScheduler.start(pending.timer.executor(), INTERVAL);
        pendingScheduler.close();
        assertThat(pending.timer.pendingCount()).isZero();
        assertThat(pending.events).isEmpty();
    }

    @Test
    public void testCloseDoesNotWaitForAnInFlightSubmission() throws Exception {
        Fixture fixture = new Fixture(2);
        CountDownLatch submitting = new CountDownLatch(1);
        CountDownLatch releaseSubmission = new CountDownLatch(1);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            doAnswer(invocation -> {
                fixture.events.add("trim:topic-0");
                submitting.countDown();
                releaseSubmission.await();
                return null;
            }).when(fixture.ledgers.get("topic-0")).trimConsumedLedgersInBackground(any());
            scheduler.start(fixture.timer.executor(), INTERVAL);
            CompletableFuture<Void> running = CompletableFuture.runAsync(fixture.timer.takeNext());
            try {
                assertThat(submitting.await(10, TimeUnit.SECONDS)).isTrue();
                CompletableFuture.runAsync(scheduler::close).get(10, TimeUnit.SECONDS);
                assertThat(running).isNotDone();
            } finally {
                releaseSubmission.countDown();
                running.get(10, TimeUnit.SECONDS);
            }

            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
            verify(fixture.ledgers.get("topic-1"), never()).trimConsumedLedgersInBackground(any());
            assertThat(fixture.timer.pendingCount()).isZero();
        }
    }

    @Test
    public void testSchedulingRejectionStopsFurtherChecks() {
        Fixture fixture = new Fixture(2);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            scheduler.start(fixture.timer.executor(), INTERVAL);
            fixture.timer.rejectNewTasks();
            fixture.timer.runNext();
            assertThat(fixture.events).containsExactlyElementsOf(expectedEvents(0, 1));
            assertThat(fixture.timer.pendingCount()).isZero();
            ManualScheduler anotherTimer = new ManualScheduler();
            scheduler.start(anotherTimer.executor(), INTERVAL);
            assertThat(anotherTimer.pendingCount()).isZero();
        }

        Fixture rejectedInitially = new Fixture(1);
        rejectedInitially.timer.rejectNewTasks();
        try (ConsumedLedgersCheckScheduler scheduler = rejectedInitially.scheduler()) {
            scheduler.start(rejectedInitially.timer.executor(), INTERVAL);
            assertThat(rejectedInitially.timer.pendingCount()).isZero();
            assertThat(rejectedInitially.events).isEmpty();
        }
    }

    @Test
    public void testNonPositiveIntervalIsRejected() {
        Fixture fixture = new Fixture(1);
        try (ConsumedLedgersCheckScheduler scheduler = fixture.scheduler()) {
            assertThatThrownBy(() -> scheduler.start(fixture.timer.executor(), Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> scheduler.start(fixture.timer.executor(), Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(fixture.timer.pendingCount()).isZero();
        }
    }

    private static void runTicks(ManualScheduler timer, int count) {
        for (int i = 0; i < count; i++) {
            timer.runNext();
            assertThat(timer.pendingCount()).isEqualTo(1);
        }
    }

    private static List<LogEvent> progressEvents(TestLogAppender appender) {
        return appender.getEvents().stream()
                .filter(event -> event.getLoggerName().equals(ConsumedLedgersCheckScheduler.class.getName())
                        && event.getMessage().getFormattedMessage().equals("Consumed ledger check submission round"))
                .toList();
    }

    private static void assertProgress(LogEvent event, Map<String, String> expected) {
        expected.forEach((name, value) -> {
            Object actual = event.getContextData().getValue(name);
            assertThat(String.valueOf(actual)).as(name).isEqualTo(value);
        });
    }

    private static List<String> expectedEvents(int first, int end) {
        List<String> events = new ArrayList<>();
        for (int i = first; i < end; i++) {
            events.add("trim:topic-" + i);
            events.add("rollover:topic-" + i);
        }
        return events;
    }

    private static final class SnapshotMap extends LinkedHashMap<String, CompletableFuture<Optional<Topic>>> {
        private Runnable snapshotAction = () -> { };

        @Override
        public Set<String> keySet() {
            snapshotAction.run();
            return super.keySet();
        }
    }

    private static final class Fixture {
        private final Map<String, CompletableFuture<Optional<Topic>>> topics;
        private final Map<String, ManagedLedger> ledgers = new LinkedHashMap<>();
        private final List<String> events = new ArrayList<>();
        private final ManualScheduler timer = new ManualScheduler();

        private Fixture(int topicCount) {
            this(topicCount, new LinkedHashMap<>());
        }

        private Fixture(int topicCount, Map<String, CompletableFuture<Optional<Topic>>> topics) {
            this.topics = topics;
            for (int i = 0; i < topicCount; i++) {
                addTopic("topic-" + i);
            }
        }

        private ManagedLedger addTopic(String name) {
            PersistentTopic topic = mock(PersistentTopic.class);
            ManagedLedger ledger = mock(ManagedLedger.class);
            when(topic.getManagedLedger()).thenReturn(ledger);
            doAnswer(invocation -> {
                events.add("trim:" + name);
                return null;
            }).when(ledger).trimConsumedLedgersInBackground(any());
            doAnswer(invocation -> {
                events.add("rollover:" + name);
                return null;
            }).when(ledger).rolloverCursorsInBackground();
            topics.put(name, CompletableFuture.completedFuture(Optional.of(topic)));
            ledgers.put(name, ledger);
            return ledger;
        }

        private ConsumedLedgersCheckScheduler scheduler() {
            return new ConsumedLedgersCheckScheduler(topics, timer::nowNanos);
        }
    }

    /** Captures one-shot timers, including cancellation, and runs them only when explicitly advanced. */
    static final class ManualScheduler {
        private final SingleThreadNonConcurrentFixedRateScheduler executor =
                mock(SingleThreadNonConcurrentFixedRateScheduler.class);
        private final PriorityQueue<Task> tasks = new PriorityQueue<>(Comparator
                .comparingLong((Task task) -> task.deadlineNanos).thenComparingLong(task -> task.sequence));
        private long nowNanos;
        private long sequence;
        private boolean rejectNewTasks;

        ManualScheduler() {
            doAnswer(invocation -> {
                if (rejectNewTasks) {
                    throw new RejectedExecutionException("manual scheduler rejected timer");
                }
                Runnable command = invocation.getArgument(0);
                long delay = invocation.getArgument(1);
                TimeUnit unit = invocation.getArgument(2);
                Task task = new Task(command, nowNanos + unit.toNanos(delay), sequence++);
                tasks.add(task);
                doAnswer(cancel -> {
                    if (task.done || task.cancelled) {
                        return false;
                    }
                    task.cancelled = true;
                    tasks.remove(task);
                    return true;
                }).when(task.future).cancel(anyBoolean());
                when(task.future.isCancelled()).thenAnswer(ignored -> task.cancelled);
                when(task.future.isDone()).thenAnswer(ignored -> task.done || task.cancelled);
                return task.future;
            }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
            // Capture the previous broker monitor API too, so a wiring regression exercises its first full scan.
            doAnswer(invocation -> executor.schedule(invocation.<Runnable>getArgument(0),
                    invocation.<Long>getArgument(1), invocation.<TimeUnit>getArgument(3)))
                    .when(executor).scheduleAtFixedRateNonConcurrently(
                            any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        }

        SingleThreadNonConcurrentFixedRateScheduler executor() {
            return executor;
        }

        void runNext() {
            takeNext().run();
        }

        Runnable takeNext() {
            Task task = tasks.remove();
            nowNanos = Math.max(nowNanos, task.deadlineNanos);
            task.done = true;
            return task.command;
        }

        long nowNanos() {
            return nowNanos;
        }

        int pendingCount() {
            return tasks.size();
        }

        long nextDelayNanos() {
            return Math.max(0, tasks.element().deadlineNanos - nowNanos);
        }

        void advanceWithoutRunning(Duration duration) {
            nowNanos += duration.toNanos();
        }

        void rejectNewTasks() {
            rejectNewTasks = true;
        }

        private static final class Task {
            private final Runnable command;
            private final long deadlineNanos;
            private final long sequence;
            private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
            private boolean cancelled;
            private boolean done;

            private Task(Runnable command, long deadlineNanos, long sequence) {
                this.command = command;
                this.deadlineNanos = deadlineNanos;
                this.sequence = sequence;
            }
        }
    }
}
