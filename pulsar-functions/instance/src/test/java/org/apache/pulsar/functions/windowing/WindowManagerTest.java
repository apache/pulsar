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

package org.apache.pulsar.functions.windowing;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.windowing.evictors.CountEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.TimeEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.WatermarkCountEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.WatermarkTimeEvictionPolicy;
import org.apache.pulsar.functions.windowing.triggers.CountTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.TimeTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.WatermarkCountTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.WatermarkTimeTriggerPolicy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link WindowManager}
 */
@Slf4j
public class WindowManagerTest {
    private WindowManager<Integer> windowManager;
    private Listener listener;

    private static final long TIMESTAMP = 1516776194873L;
    private static final String TOPIC = "test-topic";

    private static class Listener implements WindowLifecycleListener<Event<Integer>> {
        private List<Event<Integer>> onExpiryEvents = Collections.emptyList();
        private List<Event<Integer>> onActivationEvents = Collections.emptyList();
        private List<Event<Integer>> onActivationNewEvents = Collections.emptyList();
        private List<Event<Integer>> onActivationExpiredEvents = Collections.emptyList();

        // all events since last clear
        private List<List<Event<Integer>>> allOnExpiryEvents = new ArrayList<>();
        private List<List<Event<Integer>>> allOnActivationEvents = new ArrayList<>();
        private List<List<Event<Integer>>> allOnActivationNewEvents = new ArrayList<>();
        private List<List<Event<Integer>>> allOnActivationExpiredEvents = new ArrayList<>();

        @Override
        public void onExpiry(List<Event<Integer>> events) {
            onExpiryEvents = events;
            allOnExpiryEvents.add(events);
        }

        @Override
        public void onActivation(List<Event<Integer>> events, List<Event<Integer>> newEvents, List<Event<Integer>>
                expired, Long timestamp) {
            onActivationEvents = events;
            allOnActivationEvents.add(events);
            onActivationNewEvents = newEvents;
            allOnActivationNewEvents.add(newEvents);
            onActivationExpiredEvents = expired;
            allOnActivationExpiredEvents.add(expired);
        }

        void clear() {
            onExpiryEvents = Collections.emptyList();
            onActivationEvents = Collections.emptyList();
            onActivationNewEvents = Collections.emptyList();
            onActivationExpiredEvents = Collections.emptyList();

            allOnExpiryEvents.clear();
            allOnActivationEvents.clear();
            allOnActivationNewEvents.clear();
            allOnActivationExpiredEvents.clear();
        }
    }

    @BeforeMethod
    public void setUp() {
        listener = new Listener();
        windowManager = new WindowManager<>(listener, new LinkedList<>());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        windowManager.shutdown();
    }

    @Test
    public void testCountBasedWindow() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new CountEvictionPolicy<Integer>(5);
        TriggerPolicy<Integer, ?> triggerPolicy = new CountTriggerPolicy<Integer>(2, windowManager, evictionPolicy);
        triggerPolicy.start();
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(triggerPolicy);
        windowManager.add(new EventImpl<>(1, TIMESTAMP, null));
        windowManager.add(new EventImpl<>(2, TIMESTAMP, null));
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(seq(1, 2), listener.onActivationEvents);
        assertEquals(seq(1, 2), listener.onActivationNewEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        windowManager.add(new EventImpl<>(3, TIMESTAMP, null));
        windowManager.add(new EventImpl<>(4, TIMESTAMP, null));
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(seq(1, 4), listener.onActivationEvents);
        assertEquals(seq(3, 4), listener.onActivationNewEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        windowManager.add(new EventImpl<>(5, TIMESTAMP, null));
        windowManager.add(new EventImpl<>(6, TIMESTAMP, null));
        // 1 expired
        assertEquals(seq(1), listener.onExpiryEvents);
        assertEquals(seq(2, 6), listener.onActivationEvents);
        assertEquals(seq(5, 6), listener.onActivationNewEvents);
        assertEquals(seq(1), listener.onActivationExpiredEvents);
        listener.clear();
        windowManager.add(new EventImpl<>(7, TIMESTAMP, null));
        // nothing expires until threshold is hit
        assertTrue(listener.onExpiryEvents.isEmpty());
        windowManager.add(new EventImpl<>(8, TIMESTAMP, null));
        // 1 expired
        assertEquals(seq(2, 3), listener.onExpiryEvents);
        assertEquals(seq(4, 8), listener.onActivationEvents);
        assertEquals(seq(7, 8), listener.onActivationNewEvents);
        assertEquals(seq(2, 3), listener.onActivationExpiredEvents);
    }

    @Test
    public void testExpireThreshold() throws Exception {
        int threshold = WindowManager.EXPIRE_EVENTS_THRESHOLD;
        int windowLength = 5;
        CountEvictionPolicy<Integer> countEvictionPolicy = new CountEvictionPolicy<Integer>(5);
        windowManager.setEvictionPolicy(countEvictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy = new TimeTriggerPolicy<Integer>(Duration.ofHours(1)
                .toMillis(), windowManager, countEvictionPolicy, null);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);
        for (Event<Integer> i : seq(1, 5)) {
            windowManager.add(i);
        }
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        for (Event<Integer> i : seq(6, 10)) {
            windowManager.add(i);
        }
        for (Event<Integer> i : seq(11, threshold)) {
            windowManager.add(i);
        }
        // window should be compacted and events should be expired.
        assertEquals(seq(1, threshold - windowLength), listener.onExpiryEvents);
    }

    private void testEvictBeforeWatermarkForWatermarkEvictionPolicy(EvictionPolicy
                                                                            watermarkEvictionPolicy,
                                                                    int windowLength) throws
            Exception {
        /**
         * The watermark eviction policy must not evict tuples until the first watermark has been
         * received.
         * The policies can't make a meaningful decision prior to the first watermark, so the safe
         * decision
         * is to postpone eviction.
         */
        int threshold = WindowManager.EXPIRE_EVENTS_THRESHOLD;
        windowManager.setEvictionPolicy(watermarkEvictionPolicy);
        WatermarkCountTriggerPolicy triggerPolicy = new WatermarkCountTriggerPolicy(windowLength, windowManager,
                watermarkEvictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);
        for (Event<Integer> i : seqThreshold(1, threshold)) {
            windowManager.add(i);
        }
        assertTrue(listener.onExpiryEvents.isEmpty(), "The watermark eviction policies should never evict events " +
                "before the first "
                + "watermark is received");
        windowManager.add(new WaterMarkEvent<>(threshold));
        // The events should be put in a window when the first watermark is received
        assertEquals(seqThreshold(1, threshold), listener.onActivationEvents);
        //Now add some more events and a new watermark, and check that the previous events are expired
        for (Event<Integer> i : seqThreshold(threshold + 1, threshold * 2)) {
            windowManager.add(i);
        }
        windowManager.add(new WaterMarkEvent<>(threshold + windowLength + 1));
        //All the events should be expired when the next watermark is received
        assertEquals(listener
                .onExpiryEvents, seqThreshold(1, threshold), "All the events should be expired after the second " +
                "watermark");
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testExpireThresholdWithWatermarkCountEvictionPolicy() throws Exception {
        int windowLength = WindowManager.EXPIRE_EVENTS_THRESHOLD;
        EvictionPolicy watermarkCountEvictionPolicy = new WatermarkCountEvictionPolicy(windowLength);
        testEvictBeforeWatermarkForWatermarkEvictionPolicy(watermarkCountEvictionPolicy, windowLength);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testExpireThresholdWithWatermarkTimeEvictionPolicy() throws Exception {
        int windowLength = WindowManager.EXPIRE_EVENTS_THRESHOLD;
        EvictionPolicy watermarkTimeEvictionPolicy = new WatermarkTimeEvictionPolicy(windowLength);
        testEvictBeforeWatermarkForWatermarkEvictionPolicy(watermarkTimeEvictionPolicy, windowLength);
    }

    @Test
    public void testTimeBasedWindow() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new TimeEvictionPolicy<Integer>(Duration
                .ofSeconds(1).toMillis());
        windowManager.setEvictionPolicy(evictionPolicy);
        /*
         * Don't wait for Timetrigger to fire since this could lead to timing issues in unit tests.
         * Set it to a large value and trigger manually.
          */
        TriggerPolicy<Integer, ?> triggerPolicy = new TimeTriggerPolicy<Integer>(Duration.ofDays(1)
                .toMillis(), windowManager, evictionPolicy, null);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);
        long now = System.currentTimeMillis();

        // add with past ts
        for (Event<Integer> i : seq(1, 50, now - 1000)) {
            windowManager.add(i);
        }

        // add with current ts
        for (Event<Integer> i : seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD, now)) {
            windowManager.add(i);
        }
        // first 50 should have expired due to expire events threshold
        assertEquals(50, listener.onExpiryEvents.size());

        // add more events with past ts
        for (Event<Integer> i : seq(
                WindowManager.EXPIRE_EVENTS_THRESHOLD + 1, WindowManager.EXPIRE_EVENTS_THRESHOLD + 100, now - 1000)) {
            windowManager.add(i);
        }
        // simulate the time trigger by setting the reference time and invoking onTrigger() manually
        evictionPolicy.setContext(new DefaultEvictionContext(now + 100));
        windowManager.onTrigger();

        // 100 events with past ts should expire
        assertEquals(100, listener.onExpiryEvents.size());
        assertEquals(seq(
                WindowManager.EXPIRE_EVENTS_THRESHOLD + 1,
                WindowManager.EXPIRE_EVENTS_THRESHOLD + 100, now - 1000), listener.onExpiryEvents);
        List<Event<Integer>> activationsEvents = seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD, now);
        assertEquals(seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD, now), listener.onActivationEvents);
        assertEquals(seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD, now), listener.onActivationNewEvents);
        // activation expired list should contain even the ones expired due to EXPIRE_EVENTS_THRESHOLD
        List<Event<Integer>> expiredList = seq(1, 50, now - 1000);
        expiredList.addAll(seq(
                WindowManager.EXPIRE_EVENTS_THRESHOLD + 1, WindowManager.EXPIRE_EVENTS_THRESHOLD + 100, now - 1000));
        assertEquals(expiredList, listener.onActivationExpiredEvents);

        listener.clear();
        // add more events with current ts
        List<Event<Integer>> newEvents = seq(
                WindowManager.EXPIRE_EVENTS_THRESHOLD + 101, WindowManager.EXPIRE_EVENTS_THRESHOLD + 200, now);
        for (Event<Integer> i : newEvents) {
            windowManager.add(i);
        }
        activationsEvents.addAll(newEvents);
        // simulate the time trigger by setting the reference time and invoking onTrigger() manually
        evictionPolicy.setContext(new DefaultEvictionContext(now + 200));
        windowManager.onTrigger();
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(activationsEvents, listener.onActivationEvents);
        assertEquals(newEvents, listener.onActivationNewEvents);

    }


    @Test
    public void testTimeBasedWindowExpiry() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy =
                new TimeEvictionPolicy<Integer>(Duration.ofMillis(100).toMillis());
        windowManager.setEvictionPolicy(evictionPolicy);
        /*
         * Don't wait for Timetrigger to fire since this could lead to timing issues in unit tests.
         * Set it to a large value and trigger manually.
          */
        TriggerPolicy<Integer, ?> triggerPolicy = new TimeTriggerPolicy<Integer>(Duration.ofDays(1)
                .toMillis(), windowManager, evictionPolicy, null);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);
        long now = TIMESTAMP;
        // add 10 events
        for (Event<Integer> i : seq(1, 10)) {
            windowManager.add(i);
        }
        // simulate the time trigger by setting the reference time and invoking onTrigger() manually
        evictionPolicy.setContext(new DefaultEvictionContext(now + 60));
        windowManager.onTrigger();

        assertEquals(seq(1, 10), listener.onActivationEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        listener.clear();
        // wait so all events expire
        evictionPolicy.setContext(new DefaultEvictionContext(now + 120));
        windowManager.onTrigger();

        assertEquals(seq(1, 10), listener.onExpiryEvents);
        assertTrue(listener.onActivationEvents.isEmpty());
        listener.clear();
        evictionPolicy.setContext(new DefaultEvictionContext(now + 180));
        windowManager.onTrigger();
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        assertTrue(listener.onActivationEvents.isEmpty());

    }

    @Test
    public void testTumblingWindow() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new CountEvictionPolicy<Integer>(3);
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy = new CountTriggerPolicy<Integer>(3, windowManager, evictionPolicy);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);
        windowManager.add(new EventImpl<>(1, TIMESTAMP, null));
        windowManager.add(new EventImpl<>(2, TIMESTAMP, null));
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        windowManager.add(new EventImpl<>(3, TIMESTAMP, null));
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(seq(1, 3), listener.onActivationEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        assertEquals(seq(1, 3), listener.onActivationNewEvents);

        listener.clear();
        windowManager.add(new EventImpl<>(4, TIMESTAMP, null));
        windowManager.add(new EventImpl<>(5, TIMESTAMP, null));
        windowManager.add(new EventImpl<>(6, TIMESTAMP, null));

        assertEquals(seq(1, 3), listener.onExpiryEvents);
        assertEquals(seq(4, 6), listener.onActivationEvents);
        assertEquals(seq(1, 3), listener.onActivationExpiredEvents);
        assertEquals(seq(4, 6), listener.onActivationNewEvents);

    }


    @Test
    public void testEventTimeBasedWindow() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new WatermarkTimeEvictionPolicy<>(20);
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy = new WatermarkTimeTriggerPolicy<Integer>(10,
                windowManager, evictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);

        windowManager.add(new EventImpl<>(1, 603, null));
        windowManager.add(new EventImpl<>(2, 605, null));
        windowManager.add(new EventImpl<>(3, 607, null));

        // This should trigger the scan to find
        // the next aligned window end ts, but not produce any activations
        windowManager.add(new WaterMarkEvent<Integer>(609));
        assertEquals(Collections.emptyList(), listener.allOnActivationEvents);

        windowManager.add(new EventImpl<>(4, 618, null));
        windowManager.add(new EventImpl<>(5, 626, null));
        windowManager.add(new EventImpl<>(6, 636, null));
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));

        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null),
                new EventImpl<>(4, 618, null)
        }), listener.allOnActivationEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null)
        }), listener.allOnActivationEvents.get(2));

        assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(0));
        assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null)
        }), listener.allOnActivationExpiredEvents.get(2));

        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null)
        }), listener.allOnActivationNewEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null)
        }), listener.allOnActivationNewEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(5, 626, null)
        }), listener.allOnActivationNewEvents.get(2));

        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null)
        }), listener.allOnExpiryEvents.get(0));

        // add more events with a gap in ts
        windowManager.add(new EventImpl<>(7, 825, null));
        windowManager.add(new EventImpl<>(8, 826, null));
        windowManager.add(new EventImpl<>(9, 827, null));
        windowManager.add(new EventImpl<>(10, 839, null));

        listener.clear();
        windowManager.add(new WaterMarkEvent<Integer>(834));

        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(5, 626, null),
                new EventImpl<>(6, 636, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(6, 636, null)
        }), listener.allOnActivationEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(7, 825, null),
                new EventImpl<>(8, 826, null),
                new EventImpl<>(9, 827, null)
        }), listener.allOnActivationEvents.get(2));

        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null)
        }), listener.allOnActivationExpiredEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(5, 626, null)
        }), listener.allOnActivationExpiredEvents.get(1));
        assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(2));

        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(6, 636, null)
        }), listener.allOnActivationNewEvents.get(0));
        assertEquals(Collections.emptyList(), listener.allOnActivationNewEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(7, 825, null),
                new EventImpl<>(8, 826, null),
                new EventImpl<>(9, 827, null)
        }), listener.allOnActivationNewEvents.get(2));

        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null)
        }), listener.allOnExpiryEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(5, 626, null)
        }), listener.allOnExpiryEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(6, 636, null)
        }), listener.allOnExpiryEvents.get(2));
    }

    @Test
    public void testCountBasedWindowWithEventTs() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new WatermarkCountEvictionPolicy<>(3);
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy
                = new WatermarkTimeTriggerPolicy<Integer>(10, windowManager, evictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);

        windowManager.add(new EventImpl<>(1, 603, null));
        windowManager.add(new EventImpl<>(2, 605, null));
        windowManager.add(new EventImpl<>(3, 607, null));
        windowManager.add(new EventImpl<>(4, 618, null));
        windowManager.add(new EventImpl<>(5, 626, null));
        windowManager.add(new EventImpl<>(6, 636, null));
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));

        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null),
                new EventImpl<>(4, 618, null)
        }), listener.allOnActivationEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(3, 607, null),
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null)
        }), listener.allOnActivationEvents.get(2));

        // add more events with a gap in ts
        windowManager.add(new EventImpl<>(7, 665, null));
        windowManager.add(new EventImpl<>(8, 666, null));
        windowManager.add(new EventImpl<>(9, 667, null));
        windowManager.add(new EventImpl<>(10, 679, null));

        listener.clear();
        windowManager.add(new WaterMarkEvent<Integer>(674));
        assertEquals(4, listener.allOnActivationEvents.size());
        // same set of events part of three windows
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null),
                new EventImpl<>(6, 636, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null),
                new EventImpl<>(6, 636, null)
        }), listener.allOnActivationEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null),
                new EventImpl<>(6, 636, null)
        }), listener.allOnActivationEvents.get(2));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(7, 665, null),
                new EventImpl<>(8, 666, null),
                new EventImpl<>(9, 667, null)
        }), listener.allOnActivationEvents.get(3));
    }

    @Test
    public void testCountBasedTriggerWithEventTs() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new WatermarkTimeEvictionPolicy<Integer>(20);
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy
                = new WatermarkCountTriggerPolicy<Integer>(3, windowManager, evictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);

        windowManager.add(new EventImpl<>(1, 603, null));
        windowManager.add(new EventImpl<>(2, 605, null));
        windowManager.add(new EventImpl<>(3, 607, null));
        windowManager.add(new EventImpl<>(4, 618, null));
        windowManager.add(new EventImpl<>(5, 625, null));
        windowManager.add(new EventImpl<>(6, 626, null));
        windowManager.add(new EventImpl<>(7, 629, null));
        windowManager.add(new EventImpl<>(8, 636, null));
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));

        assertEquals(2, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(3, 607, null),
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 625, null),
                new EventImpl<>(6, 626, null)

        }), listener.allOnActivationEvents.get(1));

        // add more events with a gap in ts
        windowManager.add(new EventImpl<>(9, 665, null));
        windowManager.add(new EventImpl<>(10, 666, null));
        windowManager.add(new EventImpl<>(11, 667, null));
        windowManager.add(new EventImpl<>(12, 669, null));
        windowManager.add(new EventImpl<>(12, 679, null));

        listener.clear();
        windowManager.add(new WaterMarkEvent<Integer>(674));
        assertEquals(2, listener.allOnActivationEvents.size());
        // same set of events part of three windows
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(9, 665, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(9, 665, null),
                new EventImpl<>(10, 666, null),
                new EventImpl<>(11, 667, null),
                new EventImpl<>(12, 669, null),
        }), listener.allOnActivationEvents.get(1));
    }

    @Test
    public void testCountBasedTumblingWithSameEventTs() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new WatermarkCountEvictionPolicy<>(2);
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy
                = new WatermarkCountTriggerPolicy<Integer>(2, windowManager, evictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);

        windowManager.add(new EventImpl<>(1, 10, null));
        windowManager.add(new EventImpl<>(2, 10, null));
        windowManager.add(new EventImpl<>(3, 11, null));
        windowManager.add(new EventImpl<>(4, 12, null));
        windowManager.add(new EventImpl<>(5, 12, null));
        windowManager.add(new EventImpl<>(6, 12, null));
        windowManager.add(new EventImpl<>(7, 12, null));
        windowManager.add(new EventImpl<>(8, 13, null));
        windowManager.add(new EventImpl<>(9, 14, null));
        windowManager.add(new EventImpl<>(10, 15, null));

        windowManager.add(new WaterMarkEvent<Integer>(20));
        assertEquals(5, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 10, null),
                new EventImpl<>(2, 10, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(3, 11, null),
                new EventImpl<>(4, 12, null)
        }), listener.allOnActivationEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(5, 12, null),
                new EventImpl<>(6, 12, null)
        }), listener.allOnActivationEvents.get(2));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(7, 12, null),
                new EventImpl<>(8, 13, null)
        }), listener.allOnActivationEvents.get(3));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(9, 14, null),
                new EventImpl<>(10, 15, null)
        }), listener.allOnActivationEvents.get(4));
    }

    @Test
    public void testCountBasedSlidingWithSameEventTs() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new WatermarkCountEvictionPolicy<>(5);
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy
                = new WatermarkCountTriggerPolicy<Integer>(2, windowManager, evictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);

        windowManager.add(new EventImpl<>(1, 10, null));
        windowManager.add(new EventImpl<>(2, 10, null));
        windowManager.add(new EventImpl<>(3, 11, null));
        windowManager.add(new EventImpl<>(4, 12, null));
        windowManager.add(new EventImpl<>(5, 12, null));
        windowManager.add(new EventImpl<>(6, 12, null));
        windowManager.add(new EventImpl<>(7, 12, null));
        windowManager.add(new EventImpl<>(8, 13, null));
        windowManager.add(new EventImpl<>(9, 14, null));
        windowManager.add(new EventImpl<>(10, 15, null));

        windowManager.add(new WaterMarkEvent<Integer>(20));
        assertEquals(5, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 10, null),
                new EventImpl<>(2, 10, null)
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 10, null),
                new EventImpl<>(2, 10, null),
                new EventImpl<>(3, 11, null),
                new EventImpl<>(4, 12, null)
        }), listener.allOnActivationEvents.get(1));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(2, 10, null),
                new EventImpl<>(3, 11, null),
                new EventImpl<>(4, 12, null),
                new EventImpl<>(5, 12, null),
                new EventImpl<>(6, 12, null)
        }), listener.allOnActivationEvents.get(2));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 12, null),
                new EventImpl<>(5, 12, null),
                new EventImpl<>(6, 12, null),
                new EventImpl<>(7, 12, null),
                new EventImpl<>(8, 13, null)
        }), listener.allOnActivationEvents.get(3));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(6, 12, null),
                new EventImpl<>(7, 12, null),
                new EventImpl<>(8, 13, null),
                new EventImpl<>(9, 14, null),
                new EventImpl<>(10, 15, null),
        }), listener.allOnActivationEvents.get(4));
    }

    @Test
    public void testEventTimeLag() throws Exception {
        EvictionPolicy<Integer, ?> evictionPolicy = new WatermarkTimeEvictionPolicy<>(20, 5);
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy
                = new WatermarkTimeTriggerPolicy<Integer>(10, windowManager, evictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);

        windowManager.add(new EventImpl<>(1, 603, null));
        windowManager.add(new EventImpl<>(2, 605, null));
        windowManager.add(new EventImpl<>(3, 607, null));
        windowManager.add(new EventImpl<>(4, 618, null));
        windowManager.add(new EventImpl<>(5, 626, null));
        windowManager.add(new EventImpl<>(6, 632, null));
        windowManager.add(new EventImpl<>(7, 629, null));
        windowManager.add(new EventImpl<>(8, 636, null));
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));
        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null),
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null),
                new EventImpl<>(4, 618, null),
        }), listener.allOnActivationEvents.get(1));
        // out of order events should be processed upto the lag
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null),
                new EventImpl<>(7, 629, null)
        }), listener.allOnActivationEvents.get(2));
    }

    @Test
    public void testScanStop() throws Exception {
        final Set<Event<Integer>> eventsScanned = new HashSet<>();
        EvictionPolicy<Integer, ?> evictionPolicy = new WatermarkTimeEvictionPolicy<Integer>(20, 5) {

            @Override
            public Action evict(Event<Integer> event) {
                eventsScanned.add(event);
                return super.evict(event);
            }

        };
        windowManager.setEvictionPolicy(evictionPolicy);
        TriggerPolicy<Integer, ?> triggerPolicy
                = new WatermarkTimeTriggerPolicy<Integer>(10, windowManager, evictionPolicy, windowManager);
        triggerPolicy.start();
        windowManager.setTriggerPolicy(triggerPolicy);

        windowManager.add(new EventImpl<>(1, 603, null));
        windowManager.add(new EventImpl<>(2, 605, null));
        windowManager.add(new EventImpl<>(3, 607, null));
        windowManager.add(new EventImpl<>(4, 618, null));
        windowManager.add(new EventImpl<>(5, 626, null));
        windowManager.add(new EventImpl<>(6, 629, null));
        windowManager.add(new EventImpl<>(7, 636, null));
        windowManager.add(new EventImpl<>(8, 637, null));
        windowManager.add(new EventImpl<>(9, 638, null));
        windowManager.add(new EventImpl<>(10, 639, null));

        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));

        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null),
        }), listener.allOnActivationEvents.get(0));
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null),
                new EventImpl<>(4, 618, null),
        }), listener.allOnActivationEvents.get(1));

        // out of order events should be processed upto the lag
        assertEquals(Arrays.asList(new Event[]{
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null),
                new EventImpl<>(6, 629, null)
        }), listener.allOnActivationEvents.get(2));

        // events 8, 9, 10 should not be scanned at all since TimeEvictionPolicy lag 5s should break
        // the WindowManager scan loop early.
        assertEquals(new HashSet<>(Arrays.asList(new Event[]{
                new EventImpl<>(1, 603, null),
                new EventImpl<>(2, 605, null),
                new EventImpl<>(3, 607, null),
                new EventImpl<>(4, 618, null),
                new EventImpl<>(5, 626, null),
                new EventImpl<>(6, 629, null),
                new EventImpl<>(7, 636, null)
        })), eventsScanned);
    }

    private List<Event<Integer>> seq(int start) {
        return seq(start, start);
    }

    private List<Event<Integer>> seq(int start, int stop) {
        return seq(start, stop, null);
    }

    private List<Event<Integer>> seq(int start, int stop, Long ts) {
        long timestamp = TIMESTAMP;
        if (ts != null) {
            timestamp = ts;
        }
        List<Event<Integer>> ints = new ArrayList<>();
        for (int i = start; i <= stop; i++) {
            ints.add(new EventImpl<>(i, timestamp, null));
        }
        return ints;
    }

    private List<Event<Integer>> seqThreshold(int start, int stop) {
        List<Event<Integer>> ints = new ArrayList<>();
        for (int i = start; i <= stop; i++) {
            ints.add(new EventImpl<>(i, i, null));
        }
        return ints;
    }
}
