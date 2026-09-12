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
package org.apache.pulsar.common.scalable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class SegmentTopicNameTest {

    @Test
    public void testFromParent() {
        TopicName parent = TopicName.get("topic://tenant/ns/my-topic");
        HashRange range = HashRange.of(0x0000, 0x7FFF);
        TopicName segment = SegmentTopicName.fromParent(parent, range, 1);

        assertEquals(segment.toString(), "segment://tenant/ns/my-topic/0000-7fff-1");
        assertEquals(segment.getDomain(), TopicDomain.segment);
        assertEquals(segment.getTenant(), "tenant");
        assertEquals(segment.getNamespacePortion(), "ns");
        assertEquals(segment.getLocalName(), "my-topic");
        assertEquals(segment.getSegmentRange(), java.util.Optional.of(range));
        assertEquals(segment.getSegmentId(), 1);
    }

    @Test
    public void testFromParentFullRange() {
        TopicName parent = TopicName.get("topic://public/default/test");
        HashRange range = HashRange.full();
        TopicName segment = SegmentTopicName.fromParent(parent, range, 0);

        assertEquals(segment.toString(), "segment://public/default/test/0000-ffff-0");
    }

    @Test
    public void testGetParentTopicName() {
        TopicName segment = TopicName.get("segment://tenant/ns/my-topic/0000-7fff-1");
        TopicName parent = SegmentTopicName.getParentTopicName(segment);

        assertEquals(parent.toString(), "topic://tenant/ns/my-topic");
        assertEquals(parent.getDomain(), TopicDomain.topic);
    }

    @Test
    public void testGetHashRange() {
        TopicName segment = TopicName.get("segment://tenant/ns/my-topic/0000-7fff-1");
        HashRange range = SegmentTopicName.getHashRange(segment);

        assertEquals(range, HashRange.of(0x0000, 0x7FFF));
    }

    @Test
    public void testGetSegmentId() {
        TopicName segment = TopicName.get("segment://tenant/ns/my-topic/0000-7fff-42");
        long segmentId = SegmentTopicName.getSegmentId(segment);

        assertEquals(segmentId, 42);
    }

    @Test
    public void testRoundTrip() {
        TopicName parent = TopicName.get("topic://my-tenant/my-ns/orders");
        HashRange range = HashRange.of(0x4000, 0x7FFF);
        long segmentId = 5;

        TopicName segment = SegmentTopicName.fromParent(parent, range, segmentId);
        TopicName recoveredParent = SegmentTopicName.getParentTopicName(segment);
        HashRange recoveredRange = SegmentTopicName.getHashRange(segment);
        long recoveredId = SegmentTopicName.getSegmentId(segment);

        assertEquals(recoveredParent.toString(), parent.toString());
        assertEquals(recoveredRange, range);
        assertEquals(recoveredId, segmentId);
    }

    @Test
    public void testFromParentRejectsPersistentDomain() {
        TopicName persistent = TopicName.get("persistent://tenant/ns/my-topic");
        assertThrows(IllegalArgumentException.class,
                () -> SegmentTopicName.fromParent(persistent, HashRange.full(), 0));
    }

    @Test
    public void testGetParentRejectsPersistentDomain() {
        TopicName persistent = TopicName.get("persistent://tenant/ns/my-topic");
        assertThrows(IllegalArgumentException.class,
                () -> SegmentTopicName.getParentTopicName(persistent));
    }

    @Test
    public void testTopicDomainParsing() {
        TopicName topic = TopicName.get("topic://t/ns/foo");
        assertEquals(topic.getDomain(), TopicDomain.topic);
        assertEquals(topic.getTenant(), "t");
        assertEquals(topic.getNamespacePortion(), "ns");
        assertEquals(topic.getLocalName(), "foo");
    }

    @Test
    public void testSegmentDomainParsing() {
        TopicName segment = TopicName.get("segment://t/ns/foo/0000-ffff-0");
        assertEquals(segment.getDomain(), TopicDomain.segment);
        assertEquals(segment.getTenant(), "t");
        assertEquals(segment.getNamespacePortion(), "ns");
        assertEquals(segment.getLocalName(), "foo");
        assertEquals(segment.getSegmentRange(), java.util.Optional.of(HashRange.full()));
        assertEquals(segment.getSegmentId(), 0);
        assertTrue(segment.isSegment());
    }

    @Test
    public void testFormatDescriptor() {
        String desc = SegmentTopicName.formatDescriptor(HashRange.of(0x0000, 0x7FFF), 3);
        assertEquals(desc, "0000-7fff-3");
    }
}
