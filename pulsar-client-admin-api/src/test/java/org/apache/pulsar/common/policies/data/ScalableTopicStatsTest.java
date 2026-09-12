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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class ScalableTopicStatsTest {

    // --- outer class: ScalableTopicStats ---

    @Test
    public void testNoArgsConstructorDefaults() {
        ScalableTopicStats stats = new ScalableTopicStats();
        assertEquals(stats.getEpoch(), 0L);
        assertEquals(stats.getTotalSegments(), 0);
        assertEquals(stats.getActiveSegments(), 0);
        assertEquals(stats.getSealedSegments(), 0);
        // Default maps are non-null — callers can put entries without a NullPointerException.
        assertNotNull(stats.getSegments());
        assertNotNull(stats.getSubscriptions());
        assertTrue(stats.getSegments().isEmpty());
        assertTrue(stats.getSubscriptions().isEmpty());
    }

    @Test
    public void testBuilderPopulatesFields() {
        Map<Long, ScalableTopicStats.SegmentStats> segMap = new LinkedHashMap<>();
        segMap.put(0L, new ScalableTopicStats.SegmentStats("segment://t/n/topic/0000-ffff-0", "ACTIVE"));

        Map<String, ScalableTopicStats.SubscriptionStats> subMap = new LinkedHashMap<>();
        subMap.put("sub-a", new ScalableTopicStats.SubscriptionStats(3));

        ScalableTopicStats stats = ScalableTopicStats.builder()
                .epoch(7L)
                .totalSegments(5)
                .activeSegments(3)
                .sealedSegments(2)
                .segments(segMap)
                .subscriptions(subMap)
                .build();

        assertEquals(stats.getEpoch(), 7L);
        assertEquals(stats.getTotalSegments(), 5);
        assertEquals(stats.getActiveSegments(), 3);
        assertEquals(stats.getSealedSegments(), 2);
        assertEquals(stats.getSegments().get(0L).name(), "segment://t/n/topic/0000-ffff-0");
        assertEquals(stats.getSegments().get(0L).state(), "ACTIVE");
        assertEquals(stats.getSubscriptions().get("sub-a").consumerCount(), 3);
    }

    @Test
    public void testBuilderWithoutMapsUsesEmptyDefaults() {
        ScalableTopicStats stats = ScalableTopicStats.builder().build();
        assertNotNull(stats.getSegments());
        assertNotNull(stats.getSubscriptions());
        assertTrue(stats.getSegments().isEmpty());
        assertTrue(stats.getSubscriptions().isEmpty());
    }

    @Test
    public void testBuilderDefaultMapIsFreshPerInstance() {
        // @Builder.Default should give each built instance its own map — otherwise two
        // instances would share state and mutations to one would leak into the other.
        ScalableTopicStats a = ScalableTopicStats.builder().build();
        ScalableTopicStats b = ScalableTopicStats.builder().build();
        assertNotSame(a.getSegments(), b.getSegments());
        assertNotSame(a.getSubscriptions(), b.getSubscriptions());
    }

    @Test
    public void testEqualsAndHashCode() {
        ScalableTopicStats a = ScalableTopicStats.builder()
                .epoch(1L).totalSegments(2).activeSegments(2).build();
        ScalableTopicStats b = ScalableTopicStats.builder()
                .epoch(1L).totalSegments(2).activeSegments(2).build();
        ScalableTopicStats c = ScalableTopicStats.builder()
                .epoch(2L).totalSegments(2).activeSegments(2).build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    @Test
    public void testAllArgsConstructor() {
        Map<Long, ScalableTopicStats.SegmentStats> segments = Map.of(
                0L, new ScalableTopicStats.SegmentStats("segment://t/n/topic/0000-7fff-0", "SEALED"));
        Map<String, ScalableTopicStats.SubscriptionStats> subs = Map.of(
                "sub-1", new ScalableTopicStats.SubscriptionStats(0));

        ScalableTopicStats stats = new ScalableTopicStats(3L, 4, 2, 2, segments, subs);

        assertEquals(stats.getEpoch(), 3L);
        assertEquals(stats.getTotalSegments(), 4);
        assertEquals(stats.getActiveSegments(), 2);
        assertEquals(stats.getSealedSegments(), 2);
        assertEquals(stats.getSegments(), segments);
        assertEquals(stats.getSubscriptions(), subs);
    }

    // --- nested record: SegmentStats ---

    @Test
    public void testSegmentStatsRecordAccessors() {
        ScalableTopicStats.SegmentStats seg = new ScalableTopicStats.SegmentStats(
                "segment://tenant/ns/my-topic/0000-3fff-2", "ACTIVE");
        assertEquals(seg.name(), "segment://tenant/ns/my-topic/0000-3fff-2");
        assertEquals(seg.state(), "ACTIVE");
    }

    @Test
    public void testSegmentStatsEqualsAndHashCode() {
        ScalableTopicStats.SegmentStats a = new ScalableTopicStats.SegmentStats("seg-a", "ACTIVE");
        ScalableTopicStats.SegmentStats b = new ScalableTopicStats.SegmentStats("seg-a", "ACTIVE");
        ScalableTopicStats.SegmentStats c = new ScalableTopicStats.SegmentStats("seg-a", "SEALED");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    // --- nested record: SubscriptionStats ---

    @Test
    public void testSubscriptionStatsRecordAccessor() {
        ScalableTopicStats.SubscriptionStats sub = new ScalableTopicStats.SubscriptionStats(42);
        assertEquals(sub.consumerCount(), 42);
    }

    @Test
    public void testSubscriptionStatsEqualsAndHashCode() {
        ScalableTopicStats.SubscriptionStats a = new ScalableTopicStats.SubscriptionStats(3);
        ScalableTopicStats.SubscriptionStats b = new ScalableTopicStats.SubscriptionStats(3);
        ScalableTopicStats.SubscriptionStats c = new ScalableTopicStats.SubscriptionStats(4);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }
}
