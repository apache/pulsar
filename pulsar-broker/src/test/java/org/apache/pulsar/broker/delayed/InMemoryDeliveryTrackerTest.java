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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.time.Clock;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Cleanup;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryDeliveryTrackerTest {

    @Test
    public void test() throws Exception {
        PersistentDispatcherMultipleConsumers dispatcher = mock(PersistentDispatcherMultipleConsumers.class);

        Timer timer = mock(Timer.class);

        AtomicLong clockTime = new AtomicLong();
        Clock clock = mock(Clock.class);
        when(clock.millis()).then(x -> clockTime.get());

        @Cleanup
        InMemoryDelayedDeliveryTracker tracker = new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock);

        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(5, 5, 50));
        assertTrue(tracker.addMessage(4, 4, 40));

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
    }

    @Test
    public void testWithTimer() throws Exception {
        PersistentDispatcherMultipleConsumers dispatcher = mock(PersistentDispatcherMultipleConsumers.class);
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

        @Cleanup
        InMemoryDelayedDeliveryTracker tracker = new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock);

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
        verifyZeroInteractions(dispatcher);

        task.run(mock(Timeout.class));
        verify(dispatcher).readMoreEntries();
    }

    /**
     * Adding a message that is about to expire within the tick time should lead
     * to a rejection from the tracker.
     */
    @Test
    public void testAddWithinTickTime() {
        PersistentDispatcherMultipleConsumers dispatcher = mock(PersistentDispatcherMultipleConsumers.class);

        Timer timer = mock(Timer.class);

        AtomicLong clockTime = new AtomicLong();
        Clock clock = mock(Clock.class);
        when(clock.millis()).then(x -> clockTime.get());

        @Cleanup
        InMemoryDelayedDeliveryTracker tracker = new InMemoryDelayedDeliveryTracker(dispatcher, timer, 100, clock);

        clockTime.set(0);

        assertFalse(tracker.addMessage(1, 1, 10));
        assertFalse(tracker.addMessage(2, 2, 99));
        assertTrue(tracker.addMessage(3, 3, 100));
        assertTrue(tracker.addMessage(4, 4, 200));

        assertEquals(tracker.getNumberOfDelayedMessages(), 2);
    }

}
