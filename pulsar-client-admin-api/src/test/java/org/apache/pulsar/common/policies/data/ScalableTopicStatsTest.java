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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.List;
import org.testng.annotations.Test;

public class ScalableTopicStatsTest {

    @Test
    public void testDefaults() {
        ScalableTopicStats stats = new ScalableTopicStats();
        assertEquals(stats.getEpoch(), 0L);
        assertEquals(stats.getTotalSegments(), 0);
        assertEquals(stats.getMsgRateIn(), 0.0);
        assertEquals(stats.getBacklogSize(), 0L);
        // Collections are non-null so the broker can accumulate into them directly.
        assertNotNull(stats.getSegments());
        assertNotNull(stats.getProducers());
        assertNotNull(stats.getSubscriptions());
        assertTrue(stats.getSegments().isEmpty());
        assertTrue(stats.getProducers().isEmpty());
        assertTrue(stats.getSubscriptions().isEmpty());
    }

    @Test
    public void testDefaultCollectionsAreFreshPerInstance() {
        ScalableTopicStats a = new ScalableTopicStats();
        ScalableTopicStats b = new ScalableTopicStats();
        assertNotSame(a.getSegments(), b.getSegments());
        assertNotSame(a.getProducers(), b.getProducers());
        assertNotSame(a.getSubscriptions(), b.getSubscriptions());
    }

    @Test
    public void testSegmentStatsState() {
        ScalableTopicStats.SegmentStats segment = new ScalableTopicStats.SegmentStats();
        assertNull(segment.getOwnerBroker());
        assertNotNull(segment.getParentIds());
        assertNotNull(segment.getChildIds());

        segment.setState("ACTIVE");
        assertTrue(segment.isActive());
        assertFalse(segment.isSealed());

        segment.setState("SEALED");
        assertFalse(segment.isActive());
        assertTrue(segment.isSealed());
    }

    @Test
    public void testNestedDefaults() {
        ScalableTopicStats.SubscriptionStats sub = new ScalableTopicStats.SubscriptionStats();
        assertNull(sub.getType());
        assertNotNull(sub.getSegments());
        assertNotNull(sub.getConsumers());

        ScalableTopicStats.ConsumerStats consumer = new ScalableTopicStats.ConsumerStats();
        assertFalse(consumer.isConnected());
        assertNotNull(consumer.getSegmentIds());

        ScalableTopicStats.ProducerStats producer = new ScalableTopicStats.ProducerStats();
        assertNotNull(producer.getSegmentIds());
    }

    @Test
    public void testEqualsAndHashCode() {
        ScalableTopicStats a = new ScalableTopicStats();
        a.setEpoch(1L);
        a.setTotalSegments(2);
        ScalableTopicStats.SegmentStats seg = new ScalableTopicStats.SegmentStats();
        seg.setSegmentId(0L);
        seg.setChildIds(List.of(1L, 2L));
        a.getSegments().put(0L, seg);

        ScalableTopicStats b = new ScalableTopicStats();
        b.setEpoch(1L);
        b.setTotalSegments(2);
        ScalableTopicStats.SegmentStats seg2 = new ScalableTopicStats.SegmentStats();
        seg2.setSegmentId(0L);
        seg2.setChildIds(List.of(1L, 2L));
        b.getSegments().put(0L, seg2);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.setEpoch(2L);
        assertFalse(a.equals(b));
    }
}
