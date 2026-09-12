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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class ClientSegmentLayoutTest {

    private static final TopicName PARENT = TopicName.get("topic://tenant/ns/my-topic");

    private static ScalableTopicDAG buildDag(long epoch) {
        return new ScalableTopicDAG().setEpoch(epoch);
    }

    private static void addSegment(ScalableTopicDAG dag, long id, int start, int end,
                                   SegmentState state) {
        dag.addSegment()
                .setSegmentId(id)
                .setHashStart(start)
                .setHashEnd(end)
                .setState(state)
                .setCreatedAtEpoch(0L);
    }

    // --- epoch / simple accessors ---

    @Test
    public void testEpochAndEmptyOptionalsFromEmptyDag() {
        ScalableTopicDAG dag = buildDag(42L);

        ClientSegmentLayout layout = ClientSegmentLayout.fromProto(dag, PARENT);

        assertEquals(layout.epoch(), 42L);
        assertTrue(layout.activeSegments().isEmpty());
        assertTrue(layout.segmentBrokerUrls().isEmpty());
        assertNull(layout.controllerBrokerUrl());
        assertNull(layout.controllerBrokerUrlTls());
    }

    // --- active segments ---

    @Test
    public void testOnlyActiveSegmentsAreIncluded() {
        ScalableTopicDAG dag = buildDag(1L);
        addSegment(dag, 0L, 0x0000, 0x7FFF, SegmentState.SEALED);
        addSegment(dag, 1L, 0x8000, 0xFFFF, SegmentState.ACTIVE);
        addSegment(dag, 2L, 0x0000, 0x3FFF, SegmentState.ACTIVE);

        ClientSegmentLayout layout = ClientSegmentLayout.fromProto(dag, PARENT);

        assertEquals(layout.activeSegments().size(), 2);
        for (ActiveSegment seg : layout.activeSegments()) {
            assertFalse(seg.segmentId() == 0L, "sealed segment must be excluded");
        }
    }

    @Test
    public void testActiveSegmentsSortedByHashRangeStart() {
        // Intentionally add segments in a non-sorted order.
        ScalableTopicDAG dag = buildDag(1L);
        addSegment(dag, 0L, 0x8000, 0xBFFF, SegmentState.ACTIVE);
        addSegment(dag, 1L, 0x0000, 0x3FFF, SegmentState.ACTIVE);
        addSegment(dag, 2L, 0xC000, 0xFFFF, SegmentState.ACTIVE);
        addSegment(dag, 3L, 0x4000, 0x7FFF, SegmentState.ACTIVE);

        ClientSegmentLayout layout = ClientSegmentLayout.fromProto(dag, PARENT);

        // Sorted by hashRange.start ascending.
        var segs = layout.activeSegments();
        assertEquals(segs.size(), 4);
        assertEquals(segs.get(0).hashRange().start(), 0x0000);
        assertEquals(segs.get(1).hashRange().start(), 0x4000);
        assertEquals(segs.get(2).hashRange().start(), 0x8000);
        assertEquals(segs.get(3).hashRange().start(), 0xC000);
    }

    @Test
    public void testActiveSegmentTopicNameBuiltFromParent() {
        ScalableTopicDAG dag = buildDag(3L);
        addSegment(dag, 5L, 0x0000, 0x7FFF, SegmentState.ACTIVE);

        ClientSegmentLayout layout = ClientSegmentLayout.fromProto(dag, PARENT);

        assertEquals(layout.activeSegments().size(), 1);
        ActiveSegment seg = layout.activeSegments().get(0);
        assertNotNull(seg.segmentTopicName());
        // Segment topic names embed tenant/namespace/parent/descriptor — verify those pieces.
        assertTrue(seg.segmentTopicName().contains("tenant"));
        assertTrue(seg.segmentTopicName().contains("ns"));
        assertTrue(seg.segmentTopicName().contains("my-topic"));
    }

    // --- immutability ---

    @Test
    public void testReturnedCollectionsAreImmutable() {
        ScalableTopicDAG dag = buildDag(0L);
        addSegment(dag, 0L, 0x0000, 0xFFFF, SegmentState.ACTIVE);
        dag.addSegmentBroker().setSegmentId(0L).setBrokerUrl("pulsar://broker:6650");

        ClientSegmentLayout layout = ClientSegmentLayout.fromProto(dag, PARENT);

        assertThrows(UnsupportedOperationException.class,
                () -> layout.activeSegments().add(null));
        assertThrows(UnsupportedOperationException.class,
                () -> layout.segmentBrokerUrls().put(99L, "x"));
    }

    // --- broker URLs ---

    @Test
    public void testSegmentBrokerUrlsCollected() {
        ScalableTopicDAG dag = buildDag(0L);
        addSegment(dag, 0L, 0x0000, 0x7FFF, SegmentState.ACTIVE);
        addSegment(dag, 1L, 0x8000, 0xFFFF, SegmentState.ACTIVE);
        dag.addSegmentBroker().setSegmentId(0L).setBrokerUrl("pulsar://broker-a:6650");
        dag.addSegmentBroker().setSegmentId(1L).setBrokerUrl("pulsar://broker-b:6650");

        ClientSegmentLayout layout = ClientSegmentLayout.fromProto(dag, PARENT);

        assertEquals(layout.segmentBrokerUrls().size(), 2);
        assertEquals(layout.segmentBrokerUrls().get(0L), "pulsar://broker-a:6650");
        assertEquals(layout.segmentBrokerUrls().get(1L), "pulsar://broker-b:6650");
    }

    // --- controller URL ---

    @Test
    public void testControllerBrokerUrlPropagated() {
        ScalableTopicDAG dag = buildDag(0L);
        dag.setControllerBrokerUrl("pulsar://controller:6650");
        dag.setControllerBrokerUrlTls("pulsar+ssl://controller:6651");

        ClientSegmentLayout layout = ClientSegmentLayout.fromProto(dag, PARENT);

        assertEquals(layout.controllerBrokerUrl(), "pulsar://controller:6650");
        assertEquals(layout.controllerBrokerUrlTls(), "pulsar+ssl://controller:6651");
    }
}
