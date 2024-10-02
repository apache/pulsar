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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Range;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class HashRangesTest {
    @Test
    public void testMergeOverlappingRanges() {
        NavigableSet<Range> ranges = new TreeSet<>();
        ranges.add(Range.of(1, 5));
        ranges.add(Range.of(6, 10));
        ranges.add(Range.of(8, 12));
        ranges.add(Range.of(15, 20));
        ranges.add(Range.of(21, 25));

        NavigableSet<Range> expectedMergedRanges = new TreeSet<>();
        expectedMergedRanges.add(Range.of(1, 12));
        expectedMergedRanges.add(Range.of(15, 25));

        NavigableSet<Range> mergedRanges = HashRanges.mergeOverlappingRanges(ranges);

        assertThat(mergedRanges).containsExactlyElementsOf(expectedMergedRanges);
    }


    @Test
    public void testDiffRanges_NoChanges() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.put(Range.of(1, 5), consumer1);
        mappingAfter.put(Range.of(1, 5), consumer1);

        Map<Range, Pair<Consumer, Consumer>> diff = HashRanges.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).isEmpty();
    }

    @Test
    public void testDiffRanges_ConsumerChanged() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.put(Range.of(1, 5), consumer1);
        mappingAfter.put(Range.of(1, 5), consumer2);

        Map<Range, Pair<Consumer, Consumer>> diff = HashRanges.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(1, 5), Pair.of(consumer1, consumer2));
    }

    @Test
    public void testDiffRanges_RangeAdded() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingAfter.put(Range.of(1, 5), consumer1);

        Map<Range, Pair<Consumer, Consumer>> diff = HashRanges.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(1, 5), Pair.of(null, consumer1));
    }

    @Test
    public void testDiffRanges_RangeRemoved() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.put(Range.of(1, 5), consumer1);

        Map<Range, Pair<Consumer, Consumer>> diff = HashRanges.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(1, 5), Pair.of(consumer1, null));
    }

    @Test
    public void testDiffRanges_OverlappingRanges() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.put(Range.of(1, 5), consumer1);
        mappingAfter.put(Range.of(3, 7), consumer2);

        Map<Range, Pair<Consumer, Consumer>> diff = HashRanges.diffRanges(mappingBefore, mappingAfter);

        assertThat(diff).containsEntry(Range.of(3, 5), Pair.of(consumer1, consumer2));
    }

    @Test
    public void testResolveImpactedExistingConsumers_NoChanges() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.put(Range.of(1, 5), consumer1);
        mappingAfter.put(Range.of(1, 5), consumer1);

        Map<Consumer, NavigableSet<Range>> impactedConsumers =
                HashRanges.resolveImpactedExistingConsumers(mappingBefore, mappingAfter);

        assertThat(impactedConsumers).isEmpty();
    }

    @Test
    public void testResolveImpactedExistingConsumers_ConsumerChanged() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.put(Range.of(1, 5), consumer1);
        mappingAfter.put(Range.of(1, 5), consumer2);

        Map<Consumer, NavigableSet<Range>> impactedConsumers =
                HashRanges.resolveImpactedExistingConsumers(mappingBefore, mappingAfter);

        assertThat(impactedConsumers).containsExactlyInAnyOrderEntriesOf(
                Map.of(consumer1, new TreeSet<>(List.of(Range.of(1, 5))),
                        consumer2, new TreeSet<>(List.of(Range.of(1, 5)))));
    }

    @Test
    public void testResolveImpactedExistingConsumers_RangeAdded() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingAfter.put(Range.of(1, 5), consumer1);

        Map<Consumer, NavigableSet<Range>> impactedConsumers =
                HashRanges.resolveImpactedExistingConsumers(mappingBefore, mappingAfter);

        assertThat(impactedConsumers).containsExactlyInAnyOrderEntriesOf(
                Map.of(consumer1, new TreeSet<>(List.of(Range.of(1, 5)))));
    }

    @Test
    public void testResolveImpactedExistingConsumers_RangeRemoved() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        mappingBefore.put(Range.of(1, 5), consumer1);

        Map<Consumer, NavigableSet<Range>> impactedConsumers =
                HashRanges.resolveImpactedExistingConsumers(mappingBefore, mappingAfter);

        assertThat(impactedConsumers).containsExactlyInAnyOrderEntriesOf(
                Map.of(consumer1, new TreeSet<>(List.of(Range.of(1, 5)))));
    }

    @Test
    public void testResolveImpactedExistingConsumers_OverlappingRanges() {
        Map<Range, Consumer> mappingBefore = new HashMap<>();
        Map<Range, Consumer> mappingAfter = new HashMap<>();

        Consumer consumer1 = createMockConsumer("consumer1");
        Consumer consumer2 = createMockConsumer("consumer2");
        mappingBefore.put(Range.of(1, 5), consumer1);
        mappingAfter.put(Range.of(3, 7), consumer2);

        Map<Consumer, NavigableSet<Range>> impactedConsumers =
                HashRanges.resolveImpactedExistingConsumers(mappingBefore, mappingAfter);

        assertThat(impactedConsumers).containsExactlyInAnyOrderEntriesOf(
                Map.of(consumer1, new TreeSet<>(List.of(Range.of(3, 5))),
                        consumer2, new TreeSet<>(List.of(Range.of(3, 7)))));
    }
}