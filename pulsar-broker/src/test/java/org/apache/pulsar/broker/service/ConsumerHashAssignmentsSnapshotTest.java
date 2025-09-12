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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.BrokerTestUtil.createMockConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Range;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumerHashAssignmentsSnapshotTest {
    @Test
    public void testMergeOverlappingRanges() {
        SortedSet<Range> ranges = new TreeSet<>();
        ranges.add(Range.of(1, 5));
        ranges.add(Range.of(6, 10));
        ranges.add(Range.of(8, 12));
        ranges.add(Range.of(15, 20));
        ranges.add(Range.of(21, 25));

        SortedSet<Range> expectedMergedRanges = new TreeSet<>();
        expectedMergedRanges.add(Range.of(1, 12));
        expectedMergedRanges.add(Range.of(15, 25));

        List<Range> mergedRanges = ConsumerHashAssignmentsSnapshot.mergeOverlappingRanges(ranges);

        assertThat(mergedRanges).containsExactlyElementsOf(expectedMergedRanges);
    }

    @Test
    public void testDiffRanges_NoChanges() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));
        mappingAfter.add(new HashRangeAssignment(Range.of(1, 5), consumer1));

        Map<Range, Pair<Consumer, Consumer>> diff =
                ConsumerHashAssignmentsSnapshot.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).isEmpty();
    }

    @Test
    public void testDiffRanges_ConsumerChanged() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));
        mappingAfter.add(new HashRangeAssignment(Range.of(1, 5), consumer2));

        Map<Range, Pair<Consumer, Consumer>> diff =
                ConsumerHashAssignmentsSnapshot.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(1, 5), Pair.of(consumer1, consumer2));
    }

    @Test
    public void testDiffRanges_RangeAdded() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();
        Consumer consumer1 = createMockConsumer("consumer1");

        mappingAfter.add(new HashRangeAssignment(Range.of(1, 5), consumer1));

        Map<Range, Pair<Consumer, Consumer>> diff =
                ConsumerHashAssignmentsSnapshot.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(1, 5), Pair.of(null, consumer1));
    }

    @Test
    public void testDiffRanges_RangeRemoved() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));

        Map<Range, Pair<Consumer, Consumer>> diff =
                ConsumerHashAssignmentsSnapshot.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(1, 5), Pair.of(consumer1, null));
    }

    @Test
    public void testDiffRanges_OverlappingRanges() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));
        mappingAfter.add(new HashRangeAssignment(Range.of(3, 7), consumer2));

        Map<Range, Pair<Consumer, Consumer>> diff =
                ConsumerHashAssignmentsSnapshot.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(3, 5), Pair.of(consumer1, consumer2));
    }

    @Test
    public void testResolveConsumerRemovedHashRanges_NoChanges() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));
        mappingAfter.add(new HashRangeAssignment(Range.of(1, 5), consumer1));

        ImpactedConsumersResult impactedConsumers =
                ConsumerHashAssignmentsSnapshot.resolveConsumerRemovedHashRanges(mappingBefore, mappingAfter);

        assertThat(impactedConsumers.getRemovedHashRanges()).isEmpty();
    }

    @Test
    public void testResolveConsumerRemovedHashRanges_ConsumerChanged() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));
        mappingAfter.add(new HashRangeAssignment(Range.of(1, 5), consumer2));

        ImpactedConsumersResult impactedConsumers =
                ConsumerHashAssignmentsSnapshot.resolveConsumerRemovedHashRanges(mappingBefore, mappingAfter);

        assertThat(impactedConsumers.getRemovedHashRanges()).containsExactlyInAnyOrderEntriesOf(
                Map.of(consumer1, RemovedHashRanges.of(List.of(Range.of(1, 5)))));
    }

    @Test
    public void testResolveConsumerRemovedHashRanges_RangeAdded() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingAfter.add(new HashRangeAssignment(Range.of(1, 5), consumer1));

        ImpactedConsumersResult impactedConsumers =
                ConsumerHashAssignmentsSnapshot.resolveConsumerRemovedHashRanges(mappingBefore, mappingAfter);

        assertThat(impactedConsumers.getRemovedHashRanges()).isEmpty();
    }

    @Test
    public void testResolveConsumerRemovedHashRanges_RangeRemoved() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));

        ImpactedConsumersResult impactedConsumers =
                ConsumerHashAssignmentsSnapshot.resolveConsumerRemovedHashRanges(mappingBefore, mappingAfter);

        assertThat(impactedConsumers.getRemovedHashRanges()).containsExactlyInAnyOrderEntriesOf(
                Map.of(consumer1, RemovedHashRanges.of(List.of(Range.of(1, 5)))));
    }

    @Test
    public void testResolveConsumerRemovedHashRanges_OverlappingRanges() {
        List<HashRangeAssignment> mappingBefore = new ArrayList<>();
        List<HashRangeAssignment> mappingAfter = new ArrayList<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.add(new HashRangeAssignment(Range.of(1, 5), consumer1));
        mappingAfter.add(new HashRangeAssignment(Range.of(3, 7), consumer2));

        ImpactedConsumersResult impactedConsumers =
                ConsumerHashAssignmentsSnapshot.resolveConsumerRemovedHashRanges(mappingBefore, mappingAfter);

        assertThat(impactedConsumers.getRemovedHashRanges()).containsExactlyInAnyOrderEntriesOf(
                Map.of(consumer1, RemovedHashRanges.of(List.of(Range.of(3, 5)))));
    }
}