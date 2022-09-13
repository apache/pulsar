/**
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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryDeliveryTrackerTest {

    // Create a single shared timer for the test.
    private final Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-in-memory-delayed-delivery-test"),
            500, TimeUnit.MILLISECONDS);
    protected PersistentDispatcherMultipleConsumers dispatcher;
    protected Clock clock;

    protected AtomicLong clockTime;

    @AfterClass(alwaysRun = true)
    public void cleanup() {
        timer.stop();
    }

    @DataProvider(name = "delayedTracker")
    public Object[][] provider(Method method) throws Exception {
        dispatcher = mock(PersistentDispatcherMultipleConsumers.class);
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
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 100, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowBeforeTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowAfterTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 100000, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterFullTickTimeWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 500, clock,
                            true, 0)
            }};
            case "testWithFixedDelays", "testWithMixedDelays","testWithNoDelays" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 500, clock,
                            true, 100)
            }};
            default -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, 0)
            }};
        };
    }

    @Test(dataProvider = "delayedTracker")
    public void test(DelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 2, 20));
        assertTrue(tracker.addMessage(2, 1, 10));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 5, 50));
        assertTrue(tracker.addMessage(5, 4, 40));

        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tracker.getNumberOfDelayedMessages(), 5);

        assertEquals(tracker.getScheduledMessages(10), Collections.emptySet());

        // Move time forward
        clockTime.set(15);

        // Message is rejected by tracker since it's already ready to send
        assertFalse(tracker.addMessage(6, 6, 10));

        assertEquals(tracker.getNumberOfDelayedMessages(), 5);
        assertTrue(tracker.hasMessageAvailable());
        Set<PositionImpl> scheduled = tracker.getScheduledMessages(10);
        assertEquals(scheduled.size(), 1);

        // Move time forward
        clockTime.set(60);

        assertEquals(tracker.getNumberOfDelayedMessages(), 4);
        assertTrue(tracker.hasMessageAvailable());
        scheduled = tracker.getScheduledMessages(1);
        assertEquals(scheduled.size(), 1);

        assertEquals(tracker.getNumberOfDelayedMessages(), 3);
        assertTrue(tracker.hasMessageAvailable());
        scheduled = tracker.getScheduledMessages(3);
        assertEquals(scheduled.size(), 3);

        assertEquals(tracker.getNumberOfDelayedMessages(), 0);
        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tracker.getScheduledMessages(10), Collections.emptySet());

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithTimer(DelayedDeliveryTracker tracker, NavigableMap<Long, TimerTask> tasks) throws Exception {
        assertTrue(tasks.isEmpty());
        assertTrue(tracker.addMessage(2, 2, 20));
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.firstKey().longValue(), 20);

        assertTrue(tracker.addMessage(1, 1, 10));
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.firstKey().longValue(), 10);

        assertTrue(tracker.addMessage(3, 3, 30));
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.firstKey().longValue(), 10);

        clockTime.set(15);

        TimerTask task = tasks.pollFirstEntry().getValue();
        Timeout cancelledTimeout = mock(Timeout.class);
        when(cancelledTimeout.isCancelled()).thenReturn(true);
        task.run(cancelledTimeout);
        verify(dispatcher, atMostOnce()).readMoreEntries();

        task.run(mock(Timeout.class));
        verify(dispatcher).readMoreEntries();

        tracker.close();
    }

    /**
     * Adding a message that is about to expire within the tick time should lead
     * to a rejection from the tracker when isDelayedDeliveryDeliverAtTimeStrict is false.
     */
    @Test(dataProvider = "delayedTracker")
    public void testAddWithinTickTime(DelayedDeliveryTracker tracker) {
        clockTime.set(0);

        assertFalse(tracker.addMessage(1, 1, 10));
        assertFalse(tracker.addMessage(2, 2, 99));
        assertFalse(tracker.addMessage(3, 3, 100));
        assertTrue(tracker.addMessage(4, 4, 101));
        assertTrue(tracker.addMessage(5, 5, 200));

        assertEquals(tracker.getNumberOfDelayedMessages(), 2);

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testAddMessageWithStrictDelay(DelayedDeliveryTracker tracker) {
        clockTime.set(10);

        // Verify behavior for the less than, equal to, and greater than deliverAt times.
        assertFalse(tracker.addMessage(1, 1, 9));
        assertFalse(tracker.addMessage(4, 4, 10));
        assertTrue(tracker.addMessage(1, 1, 11));

        assertEquals(tracker.getNumberOfDelayedMessages(), 1);
        assertFalse(tracker.hasMessageAvailable());

        tracker.close();
    }

    /**
     * In this test, the deliverAt time is after now, but the deliverAt time is too early to run another tick, so the
     * tickTimeMillis determines the delay.
     */
    @Test(dataProvider = "delayedTracker")
    public void testAddMessageWithDeliverAtTimeAfterNowBeforeTickTimeFrequencyWithStrict(DelayedDeliveryTracker tracker)
            throws Exception {
        // Set clock time, then run tracker to inherit clock time as the last tick time.
        clockTime.set(10000);
        Timeout timeout = mock(Timeout.class);
        when(timeout.isCancelled()).then(x -> false);
        ((InMemoryDelayedDeliveryTracker) tracker).run(timeout);
        verify(dispatcher, times(1)).readMoreEntries();

        // Add a message that has a delivery time just after the previous run. It will get delivered based on the
        // tick delay plus the last tick run.
        assertTrue(tracker.addMessage(1, 1, 10001));

        // Wait longer than the tick time plus the HashedWheelTimer's tick time to ensure that enough time has
        // passed where it would have been triggered if the tick time was doing the triggering.
        Thread.sleep(600);
        verify(dispatcher, times(1)).readMoreEntries();

        // Not wait for the message delivery to get triggered.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(dispatcher).readMoreEntries());

        tracker.close();
    }

    /**
     * In this test, the deliverAt time is after now, but before the (tickTimeMillis + now). Because there wasn't a
     * recent tick run, the deliverAt time determines the delay.
     */
    @Test(dataProvider = "delayedTracker")
    public void testAddMessageWithDeliverAtTimeAfterNowAfterTickTimeFrequencyWithStrict(
            DelayedDeliveryTracker tracker) {
        clockTime.set(500000);

        assertTrue(tracker.addMessage(1, 1, 500005));

        // Wait long enough for the runnable to run, but not longer than the tick time. The point is that the delivery
        // should get scheduled early when the tick duration has passed since the last tick.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(dispatcher).readMoreEntries());

        tracker.close();
    }

    /**
     * In this test, the deliverAt time is after now plus tickTimeMillis, so the tickTimeMillis determines the delay.
     */
    @Test(dataProvider = "delayedTracker")
    public void testAddMessageWithDeliverAtTimeAfterFullTickTimeWithStrict(DelayedDeliveryTracker tracker)
            throws Exception {
        clockTime.set(0);

        assertTrue(tracker.addMessage(1, 1, 2000));

        // Wait longer than the tick time plus the HashedWheelTimer's tick time to ensure that enough time has
        // passed where it would have been triggered if the tick time was doing the triggering.
        Thread.sleep(1000);
        verify(dispatcher).getCursor();

        // Not wait for the message delivery to get triggered.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(dispatcher).readMoreEntries());

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithFixedDelays(DelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tracker.getNumberOfDelayedMessages(), 5);
        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= fixedDelayLookahead; i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        clockTime.set(fixedDelayLookahead * 10);

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
    public void testWithMixedDelays(DelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= fixedDelayLookahead; i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        // Add message with earlier delivery time
        assertTrue(tracker.addMessage(5, 6, 5));

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithNoDelays(DelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= fixedDelayLookahead; i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        // Add message with no-delay
        assertFalse(tracker.addMessage(5, 6, -1L));

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

}
