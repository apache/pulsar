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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryDeliveryTrackerTest extends AbstractDeliveryTrackerTest {

    @DataProvider(name = "delayedTracker")
    public Object[][] provider(Method method) throws Exception {
        dispatcher = mock(AbstractPersistentDispatcherMultipleConsumers.class);
        clock = mock(Clock.class);
        clockTime = new AtomicLong();
        when(clock.millis()).then(x -> clockTime.get());

        final String methodName = method.getName();
        return switch (methodName) {
            case "test" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            false, 0)
            }};
            case "testWithTimer" -> {
                Timer timer = mock(Timer.class);

                AtomicLong clockTime = new AtomicLong();
                Clock clock = mock(Clock.class);
                when(clock.millis()).then(x -> clockTime.get());

                NavigableMap<Long, TimerTask> tasks = new TreeMap<>();

                when(timer.newTimeout(any(), anyLong(), any())).then(invocation -> {
                    TimerTask task = invocation.getArgument(0, TimerTask.class);
                    long timeout = invocation.getArgument(1, Long.class);
                    TimeUnit unit = invocation.getArgument(2, TimeUnit.class);
                    long scheduleAt = clockTime.get() + unit.toMillis(timeout);
                    tasks.put(scheduleAt, task);

                    Timeout t = mock(Timeout.class);
                    when(t.cancel()).then(i -> {
                        tasks.remove(scheduleAt, task);
                        return null;
                    });
                    return t;
                });

                yield new Object[][]{{
                        new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                                false, 0),
                        tasks
                }};
            }
            case "testAddWithinTickTime" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 100, clock,
                            false, 0)
            }};
            case "testAddMessageWithStrictDelay" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowBeforeTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowAfterTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterFullTickTimeWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 500, clock,
                            true, 0)
            }};
            case "testWithFixedDelays", "testWithMixedDelays", "testWithNoDelays" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 8, clock,
                            true, 100)
            }};
            case "testStrictModeTimerStallsAfterEarlyPopAndReAdd" -> {
                // Mock timer that records the currently-armed timeouts so the test can observe whether a
                // delivery timer is live. Cancelling a timeout removes it from the map, mirroring the wheel.
                Timer mockTimer = mock(Timer.class);
                NavigableMap<Long, TimerTask> tasks = new TreeMap<>();
                when(mockTimer.newTimeout(any(), anyLong(), any())).then(invocation -> {
                    TimerTask task = invocation.getArgument(0, TimerTask.class);
                    long timeout = invocation.getArgument(1, Long.class);
                    TimeUnit unit = invocation.getArgument(2, TimeUnit.class);
                    long scheduleAt = clockTime.get() + unit.toMillis(timeout);
                    tasks.put(scheduleAt, task);
                    Timeout t = mock(Timeout.class);
                    when(t.cancel()).then(i -> tasks.remove(scheduleAt, task));
                    when(t.isCancelled()).then(i -> !tasks.containsValue(task));
                    return t;
                });
                // tickTimeMillis=1000 -> timestamps are trimmed to the lower 9 bits (multiples of 512ms),
                // which is what lets getScheduledMessages pop a message up to ~511ms before its deliverAt.
                yield new Object[][]{{
                        new InMemoryDelayedDeliveryTracker(dispatcher, mockTimer, 1000, clock,
                                true, 0),
                        tasks
                }};
            }
            default -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, 0)
            }};
        };
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithFixedDelays(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tracker.getNumberOfDelayedMessages(), 5);
        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= tracker.getFixedDelayDetectionLookahead(); i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        clockTime.set(tracker.getFixedDelayDetectionLookahead() * 10);

        tracker.getScheduledMessages(100);

        assertFalse(tracker.shouldPauseAllDeliveries());

        // Empty the tracker
        int removed = 0;
        do {
            removed = tracker.getScheduledMessages(100).size();
        } while (removed > 0);

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithMixedDelays(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= tracker.getFixedDelayDetectionLookahead(); i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        // Add message with earlier delivery time
        assertTrue(tracker.addMessage(5, 6, 5));

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithNoDelays(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= tracker.getFixedDelayDetectionLookahead(); i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        // Add message with no-delay
        assertFalse(tracker.addMessage(5, 6, -1L));

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test
    public void testClose() throws Exception {
        @Cleanup("stop")
        Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-in-memory-delayed-delivery-test"),
                1, TimeUnit.MILLISECONDS);

        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);

        AtomicLong clockTime = new AtomicLong();
        Clock clock = mock(Clock.class);
        when(clock.millis()).then(x -> clockTime.get());

        final Exception[] exceptions = new Exception[1];

        InMemoryDelayedDeliveryTracker tracker = new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                true, 0) {
            @Override
            public void run(Timeout timeout) throws Exception {
                super.timeout = timer.newTimeout(this, 1, TimeUnit.MILLISECONDS);
                if (timeout == null || timeout.isCancelled()) {
                    return;
                }
                try {
                    this.delayedMessageMap.firstKey();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptions[0] = e;
                }
            }
        };

        tracker.addMessage(1, 1, 10);
        clockTime.set(10);

        Thread.sleep(300);

        tracker.close();

        assertNull(exceptions[0]);
    }

    @Test(dataProvider = "delayedTracker")
    public void testDelaySequence(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        int messageCount = 5;
        for (int i = 1; i <= messageCount; i++) {
            assertTrue(tracker.addMessage(i, i, 1));
        }
        clockTime.set(10);
        assertTrue(tracker.hasMessageAvailable());
        assertEquals(tracker.getNumberOfDelayedMessages(), messageCount);

        for (int i = 1; i <= messageCount; i++) {
            Set<Position> scheduled = tracker.getScheduledMessages(1);
            assertEquals(scheduled.size(), 1);
            Position position = scheduled.iterator().next();
            assertEquals(position.getLedgerId(), i);
            assertEquals(position.getEntryId(), i);
        }
        tracker.close();
    }

    /**
     * Reproduces https://github.com/apache/pulsar/issues/25996.
     *
     * With isDelayedDeliveryDeliverAtTimeStrict=true and tickTimeMillis=1000, a message can be popped by
     * getScheduledMessages up to ~511ms before its real deliverAt (because of timestamp trimming). The
     * strict dispatcher re-adds the not-yet-due message, and {@code AbstractDelayedDeliveryTracker.updateTimer()}
     * cancels the timer that was armed for the next message while taking its {@code delayMillis < 0} early
     * return without clearing {@code currentTimeoutTarget}/{@code timeout}. When the early message is finally
     * delivered, updateTimer() short-circuits on the stale {@code currentTimeoutTarget} and never re-arms the
     * timer, so the remaining delayed messages stall indefinitely until an unrelated dispatch event occurs.
     *
     * The trimmed buckets (multiples of 512ms) used below:
     *   M1 deliverAt=60400 -> bucket 59904   (gets popped early then re-added)
     *   M2 deliverAt=61000 -> bucket 60928   (the message that must not be forgotten)
     */
    @Test(dataProvider = "delayedTracker")
    public void testStrictModeTimerStallsAfterEarlyPopAndReAdd(InMemoryDelayedDeliveryTracker tracker,
                                                               NavigableMap<Long, TimerTask> tasks) throws Exception {
        clockTime.set(0);

        // Two delayed messages in different trimmed buckets. A delivery timer is armed for the earliest.
        assertTrue(tracker.addMessage(1, 1, 60400));
        assertTrue(tracker.addMessage(2, 2, 61000));
        assertEquals(tasks.size(), 1, "a delivery timer should be armed for the earliest message");

        // The timer fires slightly late (wheel/scheduling granularity): now=60000 sits inside M1's trim
        // window (bucket 59904 <= 60000 < deliverAt 60400). Mimic the wheel popping and running the task.
        clockTime.set(60000);
        Timeout fired = mock(Timeout.class);
        when(fired.isCancelled()).thenReturn(false);
        tasks.pollFirstEntry().getValue().run(fired);

        // Dispatcher reads the scheduled messages: M1 is popped ~400ms early; the timer re-arms for M2.
        Set<Position> scheduled = tracker.getScheduledMessages(100);
        assertEquals(scheduled.size(), 1);
        assertEquals(scheduled.iterator().next(), PositionFactory.create(1, 1));
        assertEquals(tasks.size(), 1, "after popping M1 the timer should be re-armed for M2");

        // Strict mode: M1 is not actually due yet (60400 > now 60000), so the dispatcher puts it back.
        // This is the re-add that triggers the buggy updateTimer() early return.
        assertTrue(tracker.addMessage(1, 1, 60400));

        // M1's real deliverAt arrives; a dispatch round delivers it for real (it is no longer re-added).
        clockTime.set(60400);
        scheduled = tracker.getScheduledMessages(100);
        assertEquals(scheduled.size(), 1);
        assertEquals(scheduled.iterator().next(), PositionFactory.create(1, 1));

        // M2 is still pending and not yet available...
        assertEquals(tracker.getNumberOfDelayedMessages(), 1);
        assertFalse(tracker.hasMessageAvailable());

        // ...so a live delivery timer MUST exist to eventually deliver it. With the bug the timer was
        // cancelled and never re-armed (currentTimeoutTarget stale), leaving M2 to stall forever.
        assertFalse(tasks.isEmpty(),
                "a delivery timer must remain armed for the pending message M2; "
                        + "an empty timer set means it will never be delivered (issue #25996)");

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testAddMultipleMessagesSameWindow(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        tracker.addMessage(1, 1, 50);
        tracker.addMessage(1, 1, 50);
        tracker.addMessage(1, 1, 50);

        clockTime.set(60);

        tracker.getScheduledMessages(10);
    }
}
