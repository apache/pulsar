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
package org.apache.pulsar.broker.delayed;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryTopicDeliveryTrackerTest {

    private static class TestEnv {
        final Timer timer;
        final NavigableMap<Long, TimerTask> tasks;
        final Clock clock;
        final AtomicLong time;

        TestEnv() {
            this.tasks = new TreeMap<>();
            this.time = new AtomicLong(0L);
            this.clock = mock(Clock.class);
            when(clock.millis()).then((Answer<Long>) invocation -> time.get());

            this.timer = mock(Timer.class);
            when(timer.newTimeout(any(), anyLong(), any())).then(invocation -> {
                TimerTask task = invocation.getArgument(0, TimerTask.class);
                long timeout = invocation.getArgument(1, Long.class);
                TimeUnit unit = invocation.getArgument(2, TimeUnit.class);
                long scheduleAt = time.get() + unit.toMillis(timeout);
                tasks.put(scheduleAt, task);
                Timeout t = mock(Timeout.class);
                when(t.cancel()).then(i -> {
                    tasks.remove(scheduleAt, task);
                    return null;
                });
                when(t.isCancelled()).thenReturn(false);
                return t;
            });
        }
    }

    private static AbstractPersistentDispatcherMultipleConsumers newDispatcher(String subName, ManagedCursor cursor) {
        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Subscription subscription = mock(Subscription.class);
        when(subscription.getName()).thenReturn(subName);
        when(dispatcher.getSubscription()).thenReturn(subscription);
        when(dispatcher.getCursor()).thenReturn(cursor);
        return dispatcher;
    }

    @Test
    public void testSingleSubscriptionBasicFlow() throws Exception {
        TestEnv env = new TestEnv();
        long tickMs = 100;
        boolean strict = true;
        long lookahead = 10;
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, tickMs, env.clock, strict, lookahead);

        ManagedCursor cursor = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = newDispatcher("sub-a", cursor);
        DelayedDeliveryTracker tracker = manager.createOrGetTracker(dispatcher);

        assertFalse(tracker.hasMessageAvailable());

        // Add 3 messages in the future
        env.time.set(1000);
        assertTrue(tracker.addMessage(1, 1, 1200));
        assertTrue(tracker.addMessage(1, 2, 1300));
        assertTrue(tracker.addMessage(2, 1, 1400));

        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tracker.getNumberOfDelayedMessages(), 3);

        // Advance time so first 2 buckets are visible
        env.time.set(1350);
        assertTrue(tracker.hasMessageAvailable());
        NavigableSet<Position> scheduled = tracker.getScheduledMessages(10);
        // Should include both positions from first 2 buckets
        assertEquals(scheduled.size(), 2);

        // Global counter doesn't drop until mark-delete pruning
        assertEquals(tracker.getNumberOfDelayedMessages(), 3);

        // Mark-delete beyond the scheduled positions and prune
        ((InMemoryTopicDelayedDeliveryTracker) tracker)
                .updateMarkDeletePosition(PositionFactory.create(1L, 2L));
        // Trigger pruning by another get
        tracker.getScheduledMessages(10);
        // Now only one entry should remain in global index (prune is throttled -> wait up to 2s)
        long start = System.currentTimeMillis();
        while (tracker.getNumberOfDelayedMessages() != 1 && System.currentTimeMillis() - start < 2000) {
            try {
                Thread.sleep(30);
            } catch (InterruptedException ignored) {}
            tracker.getScheduledMessages(1);
        }
        assertEquals(tracker.getNumberOfDelayedMessages(), 1);

        // Cleanup
        tracker.close();
    }

    @Test
    public void testSharedIndexDedupAcrossSubscriptions() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);

        ManagedCursor c1 = mock(ManagedCursor.class);
        ManagedCursor c2 = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d1 = newDispatcher("sub-a", c1);
        AbstractPersistentDispatcherMultipleConsumers d2 = newDispatcher("sub-b", c2);

        DelayedDeliveryTracker v1 = manager.createOrGetTracker(d1);
        DelayedDeliveryTracker v2 = manager.createOrGetTracker(d2);

        env.time.set(1000);
        assertTrue(v1.addMessage(10, 20, 2000));
        // Add the same message from another subscription; should be de-duplicated in global index
        assertTrue(v2.addMessage(10, 20, 2000));

        assertEquals(v1.getNumberOfDelayedMessages(), 1);
        assertEquals(v2.getNumberOfDelayedMessages(), 1);

        v1.close();
        v2.close();
    }

    @Test
    public void testTimerRunTriggersOnlyAvailableSubscriptions() throws Exception {
        TestEnv env = new TestEnv();
        long tickMs = 100;
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, tickMs, env.clock, true, 0);

        ManagedCursor c1 = mock(ManagedCursor.class);
        ManagedCursor c2 = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d1 = newDispatcher("sub-a", c1);
        AbstractPersistentDispatcherMultipleConsumers d2 = newDispatcher("sub-b", c2);
        DelayedDeliveryTracker v1 = manager.createOrGetTracker(d1);
        DelayedDeliveryTracker v2 = manager.createOrGetTracker(d2);

        env.time.set(0);
        // Add two buckets. Only sub-a will have messages available based on mark-delete
        assertTrue(v1.addMessage(1, 1, 500));
        assertTrue(v2.addMessage(1, 2, 500));

        // Before cutoff
        assertFalse(v1.hasMessageAvailable());
        assertFalse(v2.hasMessageAvailable());

        // Set time after cutoff and set sub-a mark-delete behind entries, sub-b beyond entries
        env.time.set(600);
        ((InMemoryTopicDelayedDeliveryTracker) v1)
                .updateMarkDeletePosition(PositionFactory.create(0L, 0L)); // visible for sub-a
        ((InMemoryTopicDelayedDeliveryTracker) v2)
                .updateMarkDeletePosition(PositionFactory.create(1L, 5L)); // filtered for sub-b

        // Invoke manager timer task directly
        manager.run(mock(Timeout.class));

        // Only d1 should be triggered
        verify(d1, times(1)).readMoreEntriesAsync();
        verify(d2, times(0)).readMoreEntriesAsync();

        v1.close();
        v2.close();
    }

    @Test
    public void testPauseWithFixedDelays() throws Exception {
        TestEnv env = new TestEnv();
        long lookahead = 5;
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 10, env.clock, true, lookahead);

        ManagedCursor cursor = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = newDispatcher("sub-a", cursor);
        InMemoryTopicDelayedDeliveryTracker tracker =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(dispatcher);

        // Add strictly increasing deliverAt times (fixed delay scenario)
        env.time.set(0);
        for (int i = 1; i <= lookahead; i++) {
            assertTrue(tracker.addMessage(i, i, i * 100L));
        }
        assertTrue(tracker.shouldPauseAllDeliveries());

        // Move time forward to make messages available -> pause should be lifted
        env.time.set(lookahead * 100 + 1);
        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test
    public void testDynamicTickTimeUpdateAffectsCutoff() throws Exception {
        TestEnv env = new TestEnv();
        // non-strict mode: cutoff = now + tick
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 100, env.clock, false, 0);

        ManagedCursor cursor = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = newDispatcher("sub-a", cursor);
        DelayedDeliveryTracker tracker = manager.createOrGetTracker(dispatcher);

        env.time.set(1000);
        // deliverAt within current tick window -> rejected
        assertFalse(tracker.addMessage(1, 1, 1050)); // cutoff=1100
        assertEquals(tracker.getNumberOfDelayedMessages(), 0);

        // shrink tick: cutoff reduces -> same deliverAt becomes accepted
        tracker.resetTickTime(10);
        assertTrue(tracker.addMessage(1, 1, 1050)); // cutoff=1010
        assertEquals(tracker.getNumberOfDelayedMessages(), 1);

        tracker.close();
    }

    @Test
    public void testMinMarkDeleteAcrossSubscriptions() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);

        ManagedCursor c1 = mock(ManagedCursor.class);
        ManagedCursor c2 = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d1 = newDispatcher("sub-a", c1);
        AbstractPersistentDispatcherMultipleConsumers d2 = newDispatcher("sub-b", c2);
        InMemoryTopicDelayedDeliveryTracker v1 =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(d1);
        InMemoryTopicDelayedDeliveryTracker v2 =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(d2);

        env.time.set(0);
        assertTrue(v1.addMessage(1, 1, 100));
        assertTrue(v1.addMessage(1, 2, 100));
        assertTrue(v1.addMessage(2, 1, 100));
        assertEquals(v1.getNumberOfDelayedMessages(), 3);

        // c1 behind, c2 ahead (set via view so manager receives updates)
        v1.updateMarkDeletePosition(PositionFactory.create(0L, 0L));
        v2.updateMarkDeletePosition(PositionFactory.create(10L, 10L));

        env.time.set(200);
        // Trigger v2 read + prune attempt; min mark-delete still from c1 => no prune
        v2.getScheduledMessages(10);
        assertEquals(v1.getNumberOfDelayedMessages(), 3);

        // Advance c1 mark-delete beyond (1,2)
        v1.updateMarkDeletePosition(PositionFactory.create(1L, 2L));
        v1.getScheduledMessages(10);
        long start2 = System.currentTimeMillis();
        while (v1.getNumberOfDelayedMessages() != 1 && System.currentTimeMillis() - start2 < 2000) {
            try {
                Thread.sleep(30);
            } catch (InterruptedException ignored) {

            }
            v1.getScheduledMessages(1);
        }
        assertEquals(v1.getNumberOfDelayedMessages(), 1);

        v1.close();
        v2.close();
    }

    @Test
    public void testTimerSchedulingWindowAlignment() throws Exception {
        TestEnv env = new TestEnv();
        long tickMs = 1000;
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, tickMs, env.clock, true, 0);

        ManagedCursor cursor = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = newDispatcher("sub-a", cursor);
        DelayedDeliveryTracker tracker = manager.createOrGetTracker(dispatcher);

        // Establish lastTickRun via a manual run at t=10000
        env.time.set(10000);
        manager.run(mock(Timeout.class));

        // Add with deliverAt=10001, but tick window alignment should schedule at >= 11000
        assertTrue(tracker.addMessage(1, 1, 10001));
        long scheduledAt = env.tasks.firstKey();
        assertTrue(scheduledAt >= 11000, "scheduledAt=" + scheduledAt);

        // If no recent tick run, deliverAt should determine
        env.tasks.clear();
        env.time.set(20000);
        // No run -> lastTickRun remains 10000; bucketStart(20005)=20000; schedule aligns to bucket start immediately
        assertTrue(tracker.addMessage(1, 2, 20005));
        long scheduledAt2 = env.tasks.firstKey();
        assertEquals(scheduledAt2, 20000);

        tracker.close();
    }

    @Test
    public void testBufferMemoryUsageAndCleanup() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);

        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("sub-a", c);
        DelayedDeliveryTracker v = manager.createOrGetTracker(d);

        env.time.set(0);
        assertTrue(v.addMessage(1, 1, 10));
        assertTrue(v.getBufferMemoryUsage() > 0);

        v.close();
        // After last subscription closes, manager should clear index and memory
        assertEquals(manager.topicDelayedMessages(), 0);
        assertEquals(manager.topicBufferMemoryBytes(), 0);
    }

    @Test
    public void testGetScheduledMessagesLimit() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);
        ManagedCursor cursor = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = newDispatcher("sub", cursor);
        DelayedDeliveryTracker tracker = manager.createOrGetTracker(dispatcher);

        env.time.set(1000);
        for (int i = 0; i < 10; i++) {
            assertTrue(tracker.addMessage(1, i, 1001));
        }
        env.time.set(2000);
        NavigableSet<Position> positions = tracker.getScheduledMessages(3);
        assertEquals(positions.size(), 3);

        Position prev = null;
        for (Position p : positions) {
            if (prev != null) {
                assertTrue(prev.compareTo(p) < 0);
            }
            prev = p;
        }

        tracker.close();
    }

    @Test
    public void testHasMessageAvailableIgnoresMarkDelete() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 100, env.clock, true, 0);
        ManagedCursor cursor = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = newDispatcher("s", cursor);
        InMemoryTopicDelayedDeliveryTracker tracker =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(dispatcher);

        env.time.set(900);
        assertTrue(tracker.addMessage(1, 1, 1000));
        env.time.set(1000);
        tracker.updateMarkDeletePosition(PositionFactory.create(1, 1));
        assertTrue(tracker.hasMessageAvailable());
        assertTrue(tracker.getScheduledMessages(10).isEmpty());

        tracker.close();
    }

    @Test
    public void testCrossBucketDuplicatesDedupOnRead() throws Exception {
        TestEnv env = new TestEnv();
        long tick = 256;
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, tick, env.clock, true, 0);

        ManagedCursor c1 = mock(ManagedCursor.class);
        ManagedCursor c2 = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d1 = newDispatcher("s1", c1);
        AbstractPersistentDispatcherMultipleConsumers d2 = newDispatcher("s2", c2);
        DelayedDeliveryTracker v1 = manager.createOrGetTracker(d1);
        DelayedDeliveryTracker v2 = manager.createOrGetTracker(d2);

        env.time.set(1000);
        long deliverAt = 1023;
        assertTrue(v1.addMessage(9, 9, deliverAt));
        long before = manager.topicBufferMemoryBytes();

        v2.resetTickTime(32);
        assertTrue(v2.addMessage(9, 9, deliverAt));

        env.time.set(2000);
        NavigableSet<Position> scheduled = v1.getScheduledMessages(10);
        assertEquals(scheduled.size(), 1);
        assertTrue(manager.topicDelayedMessages() >= 1);
        assertTrue(manager.topicBufferMemoryBytes() > before);

        v1.close();
        v2.close();
    }

    @Test
    public void testClearIsNoOp() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        DelayedDeliveryTracker v = manager.createOrGetTracker(d);

        env.time.set(0);
        assertTrue(v.addMessage(1, 1, 10));
        long before = manager.topicDelayedMessages();
        v.clear().join();
        assertEquals(manager.topicDelayedMessages(), before);
        v.close();
    }

    @Test
    public void testMultiSubscriptionCloseDoesNotClear() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);

        ManagedCursor c1 = mock(ManagedCursor.class);
        ManagedCursor c2 = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d1 = newDispatcher("s1", c1);
        AbstractPersistentDispatcherMultipleConsumers d2 = newDispatcher("s2", c2);
        DelayedDeliveryTracker v1 = manager.createOrGetTracker(d1);
        DelayedDeliveryTracker v2 = manager.createOrGetTracker(d2);

        env.time.set(0);
        assertTrue(v1.addMessage(1, 1, 10));
        assertTrue(manager.topicDelayedMessages() > 0);

        v1.close();
        assertTrue(manager.topicDelayedMessages() > 0);
        // Move time forward so remaining view can read
        env.time.set(20);
        assertFalse(v2.getScheduledMessages(10).isEmpty());

        v2.close();
        assertEquals(manager.topicDelayedMessages(), 0);
    }

    @Test
    public void testBoundaryInputsRejected() throws Exception {
        TestEnv env = new TestEnv();
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);

        InMemoryTopicDelayedDeliveryTrackerManager mStrict =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 100, env.clock, true, 0);
        DelayedDeliveryTracker vStrict = mStrict.createOrGetTracker(d);
        env.time.set(1000);
        assertFalse(vStrict.addMessage(1, 1, -1));
        assertFalse(vStrict.addMessage(1, 2, 1000));
        vStrict.close();

        InMemoryTopicDelayedDeliveryTrackerManager mNonStrict =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 100, env.clock, false, 0);
        DelayedDeliveryTracker vNon = mNonStrict.createOrGetTracker(d);
        env.time.set(1000);
        assertFalse(vNon.addMessage(1, 3, 1100));
        vNon.close();
    }

    private static void expectIllegalState(Runnable r) {
        try {
            r.run();
            org.testng.Assert.fail("Expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // ok
        }
    }

    @Test
    public void testClosedViewThrowsOnOperations() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        InMemoryTopicDelayedDeliveryTracker v =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(d);
        v.close();

        expectIllegalState(() -> v.addMessage(1, 1, 10));
        expectIllegalState(v::hasMessageAvailable);
        expectIllegalState(() -> v.getScheduledMessages(1));
        expectIllegalState(v::clear);
    }

    @Test
    public void testRescheduleOnEarlierDeliverAt() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        DelayedDeliveryTracker v = manager.createOrGetTracker(d);

        env.time.set(0);
        assertTrue(v.addMessage(1, 1, 10));
        assertEquals(env.tasks.firstKey().longValue(), 10L);

        assertTrue(v.addMessage(1, 2, 5));
        assertEquals(env.tasks.size(), 1);
        assertEquals(env.tasks.firstKey().longValue(), 5L);

        v.close();
    }

    @Test
    public void testEmptyIndexCancelsTimerOnClose() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 100, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        DelayedDeliveryTracker v = manager.createOrGetTracker(d);

        env.time.set(0);
        assertTrue(v.addMessage(1, 1, 1000));
        assertFalse(env.tasks.isEmpty());
        v.close();
        assertTrue(env.tasks.isEmpty());
    }

    @Test
    public void testMemoryGrowthAndPruneShrink() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 10, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        InMemoryTopicDelayedDeliveryTracker v =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(d);

        env.time.set(0);
        for (int i = 0; i < 50; i++) {
            assertTrue(v.addMessage(1, i, 100));
        }
        long memBefore = manager.topicBufferMemoryBytes();
        assertTrue(memBefore > 0);

        env.time.set(200);
        v.updateMarkDeletePosition(PositionFactory.create(1, 25));
        v.getScheduledMessages(100);
        // Wait for prune-by-time throttling window and trigger reads to allow prune to occur
        long memAfter = manager.topicBufferMemoryBytes();
        long startWall = System.currentTimeMillis();
        while (memAfter >= memBefore && System.currentTimeMillis() - startWall < 2000) {
            try {
                Thread.sleep(60);
            } catch (InterruptedException ignored) {
            }
            v.getScheduledMessages(1);
            memAfter = manager.topicBufferMemoryBytes();
        }
        assertTrue(memAfter < memBefore, "Memory should shrink after prune");

        v.close();
    }

    @Test
    public void testTimerCancelAndReschedule() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 10, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        DelayedDeliveryTracker v = manager.createOrGetTracker(d);

        env.time.set(0);
        assertTrue(v.addMessage(1, 1, 100));
        long first = env.tasks.firstKey();
        assertTrue(first >= 10);

        assertTrue(v.addMessage(1, 2, 50));
        assertEquals(env.tasks.size(), 1);

        v.close();
    }

    @Test
    public void testSortedAndDedupScheduled() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        DelayedDeliveryTracker v = manager.createOrGetTracker(d);

        env.time.set(0);
        assertTrue(v.addMessage(2, 3, 10));
        assertTrue(v.addMessage(1, 5, 10));
        assertTrue(v.addMessage(1, 5, 10));
        env.time.set(100);

        NavigableSet<Position> scheduled = v.getScheduledMessages(10);
        assertEquals(scheduled.size(), 2);
        List<Position> list = new ArrayList<>(scheduled);
        assertTrue(list.get(0).compareTo(list.get(1)) < 0);

        v.close();
    }

    @Test
    public void testGlobalDelayedCountSemantics() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);
        ManagedCursor c1 = mock(ManagedCursor.class);
        ManagedCursor c2 = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d1 = newDispatcher("s1", c1);
        AbstractPersistentDispatcherMultipleConsumers d2 = newDispatcher("s2", c2);
        InMemoryTopicDelayedDeliveryTracker v1 =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(d1);
        InMemoryTopicDelayedDeliveryTracker v2 =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(d2);

        env.time.set(0);
        assertTrue(v1.addMessage(1, 1, 10));
        assertEquals(v1.getNumberOfDelayedMessages(), 1L);
        // Hide for sub-1 only; keep sub-2 without mark-delete so global min mark-delete doesn't prune
        v1.updateMarkDeletePosition(PositionFactory.create(1, 1));
        env.time.set(100);
        assertTrue(v1.getScheduledMessages(10).isEmpty());
        assertEquals(v1.getNumberOfDelayedMessages(), 1L);
        v1.close();
        v2.close();
    }

    @Test
    public void testConcurrentAdditionsSameBucket() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 1, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        DelayedDeliveryTracker v = manager.createOrGetTracker(d);

        env.time.set(0);
        int threads = 5;
        int perThread = 50;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService es = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            es.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        v.addMessage(1, base + i, 10);
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
        start.countDown();
        es.shutdown();
        es.awaitTermination(10, TimeUnit.SECONDS);

        env.time.set(100);
        NavigableSet<Position> scheduled = v.getScheduledMessages(threads * perThread);
        assertEquals(scheduled.size(), threads * perThread);
        assertEquals(manager.topicDelayedMessages(), threads * perThread);
        v.close();
    }

    @Test
    public void testConcurrentAdditionsMultipleBucketsAndReads() throws Exception {
        TestEnv env = new TestEnv();
        InMemoryTopicDelayedDeliveryTrackerManager manager =
                new InMemoryTopicDelayedDeliveryTrackerManager(env.timer, 5, env.clock, true, 0);
        ManagedCursor c = mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers d = newDispatcher("s", c);
        InMemoryTopicDelayedDeliveryTracker v =
                (InMemoryTopicDelayedDeliveryTracker) manager.createOrGetTracker(d);

        env.time.set(0);
        ExecutorService es = Executors.newFixedThreadPool(2);
        AtomicInteger added = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        es.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 200; i++) {
                    if (v.addMessage(1 + (i % 3), i, 10 + (i % 10))) {
                        added.incrementAndGet();
                    }
                }
            } catch (InterruptedException ignored) {
            }
        });
        es.submit(() -> {
            try {
                start.await();
                env.time.set(1000);
                for (int i = 0; i < 10; i++) {
                    v.getScheduledMessages(50);
                }
            } catch (InterruptedException ignored) {
            }
        });
        start.countDown();
        es.shutdown();
        es.awaitTermination(10, TimeUnit.SECONDS);

        assertTrue(manager.topicDelayedMessages() >= 0);
        v.close();
    }
}
