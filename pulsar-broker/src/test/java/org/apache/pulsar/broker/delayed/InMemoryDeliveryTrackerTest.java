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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryDeliveryTrackerTest extends AbstractDeliveryTrackerTest {

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
        Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-in-memory-delayed-delivery-test"),
                1, TimeUnit.MILLISECONDS);

        PersistentDispatcherMultipleConsumers dispatcher = mock(PersistentDispatcherMultipleConsumers.class);

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
                    this.priorityQueue.peekN1();
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

        timer.stop();
    }
}
