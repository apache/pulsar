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
        assertEquals(stats.getMsgRateIn(), 0.0);
        assertEquals(stats.getBacklogSize(), 0L);
        // Nested objects and collections are non-null so the broker can fill them directly.
        assertNotNull(stats.getLayout());
        assertEquals(stats.getLayout().getEpoch(), 0L);
        assertNotNull(stats.getLayout().getSegments());
        assertNotNull(stats.getProducers());
        assertNotNull(stats.getSubscriptions());
        assertTrue(stats.getLayout().getSegments().isEmpty());
        assertTrue(stats.getProducers().isEmpty());
        assertTrue(stats.getSubscriptions().isEmpty());
    }

    @Test
    public void testDefaultCollectionsAreFreshPerInstance() {
        ScalableTopicStats a = new ScalableTopicStats();
        ScalableTopicStats b = new ScalableTopicStats();
        assertNotSame(a.getLayout(), b.getLayout());
        assertNotSame(a.getLayout().getSegments(), b.getLayout().getSegments());
        assertNotSame(a.getProducers(), b.getProducers());
        assertNotSame(a.getSubscriptions(), b.getSubscriptions());
    }

    @Test
    public void testSegmentStatsState() {
        ScalableTopicStats.LayoutSegment segment = new ScalableTopicStats.LayoutSegment();
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
    }

    @Test
    public void testEqualsAndHashCode() {
        ScalableTopicStats a = new ScalableTopicStats();
        a.getLayout().setEpoch(1L);
        ScalableTopicStats.LayoutSegment seg = new ScalableTopicStats.LayoutSegment();
        seg.setName("segment://t/ns/topic/0000-ffff-0");
        seg.setChildIds(List.of(1L, 2L));
        a.getLayout().getSegments().put(0L, seg);

        ScalableTopicStats b = new ScalableTopicStats();
        b.getLayout().setEpoch(1L);
        ScalableTopicStats.LayoutSegment seg2 = new ScalableTopicStats.LayoutSegment();
        seg2.setName("segment://t/ns/topic/0000-ffff-0");
        seg2.setChildIds(List.of(1L, 2L));
        b.getLayout().getSegments().put(0L, seg2);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.getLayout().setEpoch(2L);
        assertFalse(a.equals(b));
    }
}
