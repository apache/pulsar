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
package org.apache.pulsar.broker.service.scalable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.testng.annotations.Test;

public class SegmentLayoutTest {

    @Test
    public void testInitialLayout() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        assertEquals(layout.getEpoch(), 0);
        assertEquals(layout.getActiveSegments().size(), 2);
        assertEquals(layout.getAllSegments().size(), 2);

        SegmentInfo seg0 = layout.getAllSegments().get(0L);
        SegmentInfo seg1 = layout.getAllSegments().get(1L);
        assertNotNull(seg0);
        assertNotNull(seg1);

        assertEquals(seg0.hashRange(), HashRange.of(0x0000, 0x7FFF));
        assertEquals(seg1.hashRange(), HashRange.of(0x8000, 0xFFFF));
        assertTrue(seg0.isActive());
        assertTrue(seg1.isActive());
    }

    @Test
    public void testSingleSegmentInitialLayout() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(1, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        assertEquals(layout.getActiveSegments().size(), 1);
        SegmentInfo seg = layout.getAllSegments().get(0L);
        assertEquals(seg.hashRange(), HashRange.full());
    }

    @Test
    public void testFourSegmentInitialLayout() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(4, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        assertEquals(layout.getActiveSegments().size(), 4);
        // Verify ranges cover the full space without gaps
        int expectedStart = 0;
        for (int i = 0; i < 4; i++) {
            SegmentInfo seg = layout.getAllSegments().get((long) i);
            assertEquals(seg.hashRange().start(), expectedStart);
            expectedStart = seg.hashRange().end() + 1;
        }
        // Last segment should end at 0xFFFF
        assertEquals(layout.getAllSegments().get(3L).hashRange().end(), 0xFFFF);
    }

    @Test
    public void testFindActiveSegment() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        SegmentInfo found = layout.findActiveSegment(0x1000);
        assertEquals(found.segmentId(), 0);

        found = layout.findActiveSegment(0x9000);
        assertEquals(found.segmentId(), 1);
    }

    @Test
    public void testSplitSegment() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        SegmentLayout afterSplit = layout.splitSegment(0);

        assertEquals(afterSplit.getEpoch(), 1);
        assertEquals(afterSplit.getActiveSegments().size(), 3); // 1 original + 2 new
        assertEquals(afterSplit.getAllSegments().size(), 4); // 2 original + 2 new

        // Original segment should be sealed
        SegmentInfo sealed = afterSplit.getAllSegments().get(0L);
        assertTrue(sealed.isSealed());
        assertEquals(sealed.childIds(), List.of(2L, 3L));

        // New segments should be active
        SegmentInfo child1 = afterSplit.getAllSegments().get(2L);
        SegmentInfo child2 = afterSplit.getAllSegments().get(3L);
        assertTrue(child1.isActive());
        assertTrue(child2.isActive());
        assertEquals(child1.parentIds(), List.of(0L));
        assertEquals(child2.parentIds(), List.of(0L));

        // Ranges should cover the original range
        assertEquals(child1.hashRange(), HashRange.of(0x0000, 0x3FFF));
        assertEquals(child2.hashRange(), HashRange.of(0x4000, 0x7FFF));
    }

    @Test
    public void testSplitNonActiveSegment() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        SegmentLayout afterSplit = layout.splitSegment(0);

        // Segment 0 is now sealed, cannot split again
        assertThrows(IllegalArgumentException.class, () -> afterSplit.splitSegment(0));
    }

    @Test
    public void testMergeSegments() {
        // Start with 2 segments, split seg-0, then merge the children back
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        SegmentLayout afterSplit = layout.splitSegment(0); // seg-2 [0000-3fff], seg-3 [4000-7fff]

        SegmentLayout afterMerge = afterSplit.mergeSegments(2, 3);

        assertEquals(afterMerge.getEpoch(), 2);
        assertEquals(afterMerge.getActiveSegments().size(), 2); // merged + seg-1

        // Children should be sealed
        assertTrue(afterMerge.getAllSegments().get(2L).isSealed());
        assertTrue(afterMerge.getAllSegments().get(3L).isSealed());

        // Merged segment should be active with original range
        SegmentInfo merged = afterMerge.getAllSegments().get(4L);
        assertTrue(merged.isActive());
        assertEquals(merged.hashRange(), HashRange.of(0x0000, 0x7FFF));
        assertEquals(merged.parentIds(), List.of(2L, 3L));
    }

    @Test
    public void testMergeNonAdjacentSegments() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(4, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        // Segments 0 and 2 are not adjacent
        assertThrows(IllegalArgumentException.class, () -> layout.mergeSegments(0, 2));
    }

    @Test
    public void testPruneSegment() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        SegmentLayout afterSplit = layout.splitSegment(0);

        // Prune sealed segment 0
        SegmentLayout afterPrune = afterSplit.pruneSegment(0);

        assertFalse(afterPrune.getAllSegments().containsKey(0L));
        // Children should have empty parent lists now
        assertEquals(afterPrune.getAllSegments().get(2L).parentIds(), List.of());
        assertEquals(afterPrune.getAllSegments().get(3L).parentIds(), List.of());
    }

    @Test
    public void testCannotPruneActiveSegment() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        assertThrows(IllegalArgumentException.class, () -> layout.pruneSegment(0));
    }

    @Test
    public void testGetChildren() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(1, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        SegmentLayout afterSplit = layout.splitSegment(0);

        List<SegmentInfo> children = afterSplit.getChildren(0);
        assertEquals(children.size(), 2);
        assertEquals(children.get(0).segmentId(), 1);
        assertEquals(children.get(1).segmentId(), 2);
    }

    @Test
    public void testGetParents() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(1, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        SegmentLayout afterSplit = layout.splitSegment(0);

        List<SegmentInfo> parents = afterSplit.getParents(1);
        assertEquals(parents.size(), 1);
        assertEquals(parents.get(0).segmentId(), 0);

        // Root has no parents
        assertEquals(afterSplit.getParents(0).size(), 0);
    }

    @Test
    public void testGetLineage() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(1, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        SegmentLayout afterSplit = layout.splitSegment(0);

        List<SegmentInfo> lineage = afterSplit.getLineage(0);
        // Lineage of seg-0: itself + its two children
        assertEquals(lineage.size(), 3);
    }

    @Test
    public void testToMetadata() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of("key", "value"));
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        SegmentLayout afterSplit = layout.splitSegment(0);

        ScalableTopicMetadata restored = afterSplit.toMetadata(Map.of("key", "value"));
        assertEquals(restored.getEpoch(), 1);
        assertEquals(restored.getNextSegmentId(), 4);
        assertEquals(restored.getSegments().size(), 4);
        assertEquals(restored.getProperties().get("key"), "value");
    }

    @Test
    public void testNextSegmentIdAdvances() {
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(2, Map.of());
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);
        assertEquals(layout.getNextSegmentId(), 2);

        SegmentLayout split1 = layout.splitSegment(0);
        assertEquals(split1.getNextSegmentId(), 4);

        SegmentLayout split2 = split1.splitSegment(1);
        assertEquals(split2.getNextSegmentId(), 6);
    }
}
