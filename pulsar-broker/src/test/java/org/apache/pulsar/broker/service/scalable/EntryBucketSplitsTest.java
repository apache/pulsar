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
import java.util.List;
import java.util.Map;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.testng.annotations.Test;

/**
 * PIP-486: the controller's entry-bucket budget — equal-width split computation and how it is shared
 * across a topic's segments on create / migrate / split / merge.
 */
public class EntryBucketSplitsTest {

    // --- equalWidth ---

    @Test
    public void testEqualWidthSingleBucketHasNoSplits() {
        assertEquals(EntryBucketSplits.equalWidth(1), List.of());
        assertEquals(EntryBucketSplits.equalWidth(0), List.of());
    }

    @Test
    public void testEqualWidthPowersOfTwoAreExact() {
        assertEquals(EntryBucketSplits.equalWidth(2), List.of(0x8000));
        assertEquals(EntryBucketSplits.equalWidth(4), List.of(0x4000, 0x8000, 0xC000));
    }

    @Test
    public void testEqualWidthNonPowerOfTwoRoundsDown() {
        // 65536/3 = 21845.33, 2*65536/3 = 43690.67
        assertEquals(EntryBucketSplits.equalWidth(3), List.of(21845, 43690));
    }

    // --- bucketsForBudget ---

    @Test
    public void testBucketsForBudgetFloorsAtLeastOne() {
        assertEquals(EntryBucketSplits.bucketsForBudget(4, 1), 4);
        assertEquals(EntryBucketSplits.bucketsForBudget(4, 2), 2);
        assertEquals(EntryBucketSplits.bucketsForBudget(4, 4), 1);
        assertEquals(EntryBucketSplits.bucketsForBudget(4, 8), 1); // floor never drops below 1
        assertEquals(EntryBucketSplits.bucketsForBudget(4, 3), 1); // 4/3 -> 1
    }

    // --- createInitialMetadata shares the budget across segments ---

    @Test
    public void testInitialSingleSegmentTakesWholeBudget() {
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(1, 4, Map.of());
        assertEquals(md.getSegments().get(0L).bucketCount(), 4);
    }

    @Test
    public void testInitialBudgetSplitAcrossSegments() {
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(2, 4, Map.of());
        assertEquals(md.getSegments().get(0L).bucketCount(), 2);
        assertEquals(md.getSegments().get(1L).bucketCount(), 2);
    }

    @Test
    public void testInitialSegmentsAtOrAboveBudgetSettleAtOneBucket() {
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(4, 4, Map.of());
        for (long id = 0; id < 4; id++) {
            assertEquals(md.getSegments().get(id).bucketCount(), 1);
        }
    }

    // --- createMigratedMetadata: children share the budget, sealed legacy parents keep one bucket ---

    @Test
    public void testMigratedChildrenShareBudgetParentsHaveOneBucket() {
        TopicName base = TopicName.get("persistent://tenant/ns/topic");
        ScalableTopicMetadata md = ScalableTopicController.createMigratedMetadata(base, 2, 4);
        // parents 0..1 (sealed legacy), children 2..3 (active).
        assertEquals(md.getSegments().get(0L).bucketCount(), 1);
        assertEquals(md.getSegments().get(1L).bucketCount(), 1);
        assertEquals(md.getSegments().get(2L).bucketCount(), 2);
        assertEquals(md.getSegments().get(3L).bucketCount(), 2);
    }

    // --- split halves buckets, merge sums them ---

    @Test
    public void testSplitHalvesParentBuckets() {
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(1, 4, Map.of());
        SegmentLayout afterSplit = SegmentLayout.fromMetadata(md).splitSegment(0, 0L);
        // parent 0 had N=4; the two children (ids 1, 2) get N=2 each.
        assertEquals(afterSplit.getAllSegments().get(1L).bucketCount(), 2);
        assertEquals(afterSplit.getAllSegments().get(2L).bucketCount(), 2);
    }

    @Test
    public void testSplitOfSingleBucketStaysSingleBucket() {
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(4, 4, Map.of());
        SegmentLayout afterSplit = SegmentLayout.fromMetadata(md).splitSegment(0, 0L);
        long firstChild = afterSplit.getAllSegments().get(0L).childIds().get(0);
        assertEquals(afterSplit.getAllSegments().get(firstChild).bucketCount(), 1);
    }

    @Test
    public void testMergeSumsChildBuckets() {
        // 4 segments, each N=1; merging two adjacent ones recovers N = 1 + 1 = 2.
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(4, 4, Map.of());
        SegmentLayout merged = SegmentLayout.fromMetadata(md).mergeSegments(0, 1, 0L);
        long mergedId = merged.getAllSegments().get(0L).childIds().get(0);
        assertEquals(merged.getAllSegments().get(mergedId).bucketCount(), 2);
    }

    // --- ranges: split points -> per-bucket hash ranges ---

    @Test
    public void testRangesSingleBucketSpansWholeRing() {
        assertEquals(EntryBucketSplits.ranges(List.of()), List.of(HashRange.of(0, 0xFFFF)));
    }

    @Test
    public void testRangesFromSplits() {
        assertEquals(EntryBucketSplits.ranges(List.of(0x8000)),
                List.of(HashRange.of(0, 0x7FFF), HashRange.of(0x8000, 0xFFFF)));
        assertEquals(EntryBucketSplits.ranges(List.of(0x4000, 0x8000, 0xC000)),
                List.of(HashRange.of(0, 0x3FFF), HashRange.of(0x4000, 0x7FFF),
                        HashRange.of(0x8000, 0xBFFF), HashRange.of(0xC000, 0xFFFF)));
    }
}
