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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;
import org.assertj.core.data.Offset;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsistentHashingStickyKeyConsumerSelectorTest {

    @Test
    public void testConsumerSelect() throws ConsumerAssignException {

        ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(200);
        String key1 = "anyKey";
        Assert.assertNull(selector.select(key1.getBytes()));

        Consumer consumer1 = mock(Consumer.class);
        when(consumer1.consumerName()).thenReturn("c1");
        selector.addConsumer(consumer1);
        Assert.assertEquals(selector.select(key1.getBytes()), consumer1);

        Consumer consumer2 = mock(Consumer.class);
        when(consumer2.consumerName()).thenReturn("c2");
        selector.addConsumer(consumer2);

        final int N = 1000;
        final double PERCENT_ERROR = 0.20; // 20 %

        Map<String, Integer> selectionMap = new HashMap<>();
        for (int i = 0; i < N; i++) {
            String key = UUID.randomUUID().toString();
            Consumer selectedConsumer = selector.select(key.getBytes());
            int count = selectionMap.computeIfAbsent(selectedConsumer.consumerName(), c -> 0);
            selectionMap.put(selectedConsumer.consumerName(), count + 1);
        }

        // Check that keys got assigned uniformely to consumers
        Assert.assertEquals(selectionMap.get("c1"), N/2, N/2 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c2"), N/2, N/2 * PERCENT_ERROR);
        selectionMap.clear();

        Consumer consumer3 = mock(Consumer.class);
        when(consumer3.consumerName()).thenReturn("c3");
        selector.addConsumer(consumer3);

        for (int i = 0; i < N; i++) {
            String key = UUID.randomUUID().toString();
            Consumer selectedConsumer = selector.select(key.getBytes());
            int count = selectionMap.computeIfAbsent(selectedConsumer.consumerName(), c -> 0);
            selectionMap.put(selectedConsumer.consumerName(), count + 1);
        }

        Assert.assertEquals(selectionMap.get("c1"), N/3, N/3 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c2"), N/3, N/3 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c3"), N/3, N/3 * PERCENT_ERROR);
        selectionMap.clear();

        Consumer consumer4 = mock(Consumer.class);
        when(consumer4.consumerName()).thenReturn("c4");
        selector.addConsumer(consumer4);

        for (int i = 0; i < N; i++) {
            String key = UUID.randomUUID().toString();
            Consumer selectedConsumer = selector.select(key.getBytes());
            int count = selectionMap.computeIfAbsent(selectedConsumer.consumerName(), c -> 0);
            selectionMap.put(selectedConsumer.consumerName(), count + 1);
        }

        Assert.assertEquals(selectionMap.get("c1"), N/4, N/4 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c2"), N/4, N/4 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c3"), N/4, N/4 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c4"), N/4, N/4 * PERCENT_ERROR);
        selectionMap.clear();

        selector.removeConsumer(consumer1);

        for (int i = 0; i < N; i++) {
            String key = UUID.randomUUID().toString();
            Consumer selectedConsumer = selector.select(key.getBytes());
            int count = selectionMap.computeIfAbsent(selectedConsumer.consumerName(), c -> 0);
            selectionMap.put(selectedConsumer.consumerName(), count + 1);
        }

        Assert.assertEquals(selectionMap.get("c2"), N/3, N/3 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c3"), N/3, N/3 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c4"), N/3, N/3 * PERCENT_ERROR);
        selectionMap.clear();

        selector.removeConsumer(consumer2);
        for (int i = 0; i < N; i++) {
            String key = UUID.randomUUID().toString();
            Consumer selectedConsumer = selector.select(key.getBytes());
            int count = selectionMap.computeIfAbsent(selectedConsumer.consumerName(), c -> 0);
            selectionMap.put(selectedConsumer.consumerName(), count + 1);
        }

        System.err.println(selectionMap);
        Assert.assertEquals(selectionMap.get("c3"), N/2, N/2 * PERCENT_ERROR);
        Assert.assertEquals(selectionMap.get("c4"), N/2, N/2 * PERCENT_ERROR);
        selectionMap.clear();

        selector.removeConsumer(consumer3);
        for (int i = 0; i < N; i++) {
            String key = UUID.randomUUID().toString();
            Consumer selectedConsumer = selector.select(key.getBytes());
            int count = selectionMap.computeIfAbsent(selectedConsumer.consumerName(), c -> 0);
            selectionMap.put(selectedConsumer.consumerName(), count + 1);
        }

        Assert.assertEquals(selectionMap.get("c4").intValue(), N);
    }


    @Test
    public void testGetConsumerKeyHashRanges() throws BrokerServiceException.ConsumerAssignException {
        ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(3);
        List<String> consumerName = Arrays.asList("consumer1", "consumer2", "consumer3");
        List<Consumer> consumers = new ArrayList<>();
        long id=0;
        for (String s : consumerName) {
            Consumer consumer = createMockConsumer(s, s, id++);
            selector.addConsumer(consumer);
            consumers.add(consumer);
        }

        // check that results are the same when called multiple times
        assertThat(selector.getConsumerKeyHashRanges())
                .containsExactlyEntriesOf(selector.getConsumerKeyHashRanges());

        Map<Consumer, List<Range>> expectedResult = new HashMap<>();
        assertThat(consumers.get(0).consumerName()).isEqualTo("consumer1");
        expectedResult.put(consumers.get(0), Arrays.asList(
                Range.of(14359, 18366),
                Range.of(29991, 39817),
                Range.of(52980, 60442)));
        assertThat(consumers.get(1).consumerName()).isEqualTo("consumer2");
        expectedResult.put(consumers.get(1), Arrays.asList(
                Range.of(1, 6668),
                Range.of(39818, 52979),
                Range.of(60443, 63679),
                Range.of(65184, 65535)));
        assertThat(consumers.get(2).consumerName()).isEqualTo("consumer3");
        expectedResult.put(consumers.get(2), Arrays.asList(
                Range.of(6669, 14358),
                Range.of(18367, 29990),
                Range.of(63680, 65183)));
        Map<Consumer, List<Range>> consumerKeyHashRanges = selector.getConsumerKeyHashRanges();
        assertThat(consumerKeyHashRanges).containsExactlyInAnyOrderEntriesOf(expectedResult);

        // check that ranges are continuous and cover the whole range
        List<Range> allRanges =
                consumerKeyHashRanges.values().stream().flatMap(List::stream).sorted().collect(Collectors.toList());
        Range previousRange = null;
        for (Range range : allRanges) {
            if (previousRange != null) {
                assertThat(range.getStart()).isEqualTo(previousRange.getEnd() + 1);
            }
            previousRange = range;
        }
        Range totalRange = selector.getKeyHashRange();
        assertThat(allRanges.stream().mapToInt(Range::size).sum()).isEqualTo(totalRange.size());
    }

    @Test
    public void testConsumersGetSufficientlyAccuratelyEvenlyMapped()
            throws BrokerServiceException.ConsumerAssignException {
        ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(200);
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            // use the same name for all consumers, use toString to distinguish them
            Consumer consumer = createMockConsumer("consumer", String.format("index %02d", i), i);
            selector.addConsumer(consumer);
            consumers.add(consumer);
        }
        printConsumerRangesStats(selector);

        int totalSelections = 10000;

        Map<Consumer, MutableInt> consumerSelectionCount = new HashMap<>();
        for (int i = 0; i < totalSelections; i++) {
            Consumer selectedConsumer = selector.select(("key " + i).getBytes(StandardCharsets.UTF_8));
            consumerSelectionCount.computeIfAbsent(selectedConsumer, c -> new MutableInt()).increment();
        }

        printSelectionCountStats(consumerSelectionCount);

        int averageCount = totalSelections / consumers.size();
        int allowedVariance = (int) (0.2d * averageCount);
        System.out.println("averageCount: " + averageCount + " allowedVariance: " + allowedVariance);

        for (Map.Entry<Consumer, MutableInt> entry : consumerSelectionCount.entrySet()) {
            assertThat(entry.getValue().intValue()).describedAs("consumer: %s", entry.getKey())
                    .isCloseTo(averageCount, Offset.offset(allowedVariance));
        }

        consumers.forEach(selector::removeConsumer);
        assertThat(selector.getConsumerKeyHashRanges()).isEmpty();
    }

    private static void printSelectionCountStats(Map<Consumer, MutableInt> consumerSelectionCount) {
        int totalSelections = consumerSelectionCount.values().stream().mapToInt(MutableInt::intValue).sum();
        consumerSelectionCount.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(Consumer::toString)))
                .forEach(entry -> System.out.println(
                        String.format("consumer: %s got selected %d times. ratio: %.2f%%", entry.getKey(),
                                entry.getValue().intValue(),
                                ((double) entry.getValue().intValue() / totalSelections) * 100.0d)));
    }

    private static void printConsumerRangesStats(ConsistentHashingStickyKeyConsumerSelector selector) {
        selector.getConsumerKeyHashRanges().entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(),
                        entry.getValue().stream().mapToInt(Range::size).sum()))
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(Consumer::toString)))
                .forEach(entry -> System.out.println(
                        String.format("consumer: %s total ranges size: %d ratio: %.2f%%", entry.getKey(),
                                entry.getValue(),
                                ((double) entry.getValue() / selector.getKeyHashRange().size()) * 100.0d)));
    }

    private static Consumer createMockConsumer(String consumerName, String toString, long id) {
        // without stubOnly, the mock will record method invocations and run into OOME
        Consumer consumer =  mock(Consumer.class, Mockito.withSettings().stubOnly());
        when(consumer.consumerName()).thenReturn(consumerName);
        when(consumer.getPriorityLevel()).thenReturn(0);
        when(consumer.toString()).thenReturn(toString);
        when(consumer.consumerId()).thenReturn(id);
        return consumer;
    }

    // reproduces https://github.com/apache/pulsar/issues/22050
    @Test
    public void shouldNotCollideWithConsumerNameEndsWithNumber() {
        ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(12);
        List<String> consumerName = Arrays.asList("consumer1", "consumer11");
        List<Consumer> consumers = new ArrayList<>();
        for (String s : consumerName) {
            Consumer consumer = mock(Consumer.class);
            when(consumer.consumerName()).thenReturn(s);
            selector.addConsumer(consumer);
            consumers.add(consumer);
        }
        Map<Range, Consumer> rangeToConsumer = new HashMap<>();
        for (Map.Entry<Consumer, List<Range>> entry : selector.getConsumerKeyHashRanges().entrySet()) {
            for (Range range : entry.getValue()) {
                Consumer previous = rangeToConsumer.put(range, entry.getKey());
                if (previous != null) {
                    Assert.fail("Ranges are colliding between " + previous.consumerName() + " and " + entry.getKey()
                            .consumerName());
                }
            }
        }
    }

    @Test
    public void shouldRemoveConsumersFromConsumerKeyHashRanges() {
        ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(12);
        List<Consumer> consumers = IntStream.range(1, 100).mapToObj(i -> "consumer" + i)
                .map(consumerName -> {
                    Consumer consumer = mock(Consumer.class);
                    when(consumer.consumerName()).thenReturn(consumerName);
                    return consumer;
                }).collect(Collectors.toList());

        // when consumers are added
        consumers.forEach(selector::addConsumer);
        // then each consumer should have a range
        Assert.assertEquals(selector.getConsumerKeyHashRanges().size(), consumers.size());
        // when consumers are removed
        consumers.forEach(selector::removeConsumer);
        // then there should be no mapping remaining
        Assert.assertEquals(selector.getConsumerKeyHashRanges().size(), 0);
        // when consumers are removed again, should not fail
        consumers.forEach(selector::removeConsumer);
    }

    @Test
    public void testShouldNotChangeSelectedConsumerWhenConsumerIsRemoved() {
        final ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(100);
        final String consumerName = "consumer";
        final int numOfInitialConsumers = 100;
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numOfInitialConsumers; i++) {
            final Consumer consumer = createMockConsumer(consumerName, "index " + i, i);
            consumers.add(consumer);
            selector.addConsumer(consumer);
        }

        int hashRangeSize = selector.getKeyHashRange().size();
        int validationPointCount = 200;
        int increment = hashRangeSize / (validationPointCount + 1);
        List<Consumer> selectedConsumerBeforeRemoval = new ArrayList<>();

        for (int i = 0; i < validationPointCount; i++) {
            selectedConsumerBeforeRemoval.add(selector.select(i * increment));
        }

        for (int i = 0; i < validationPointCount; i++) {
            Consumer selected = selector.select(i * increment);
            Consumer expected = selectedConsumerBeforeRemoval.get(i);
            assertThat(selected.consumerId()).as("validationPoint %d", i).isEqualTo(expected.consumerId());
        }

        Set<Consumer> removedConsumers = new HashSet<>();
        for (Consumer removedConsumer : consumers) {
            selector.removeConsumer(removedConsumer);
            removedConsumers.add(removedConsumer);
            Map<Consumer, List<Range>> consumerKeyHashRanges = selector.getConsumerKeyHashRanges();
            for (int i = 0; i < validationPointCount; i++) {
                int hash = i * increment;
                Consumer selected = selector.select(hash);
                Consumer expected = selectedConsumerBeforeRemoval.get(i);
                if (!removedConsumers.contains(expected)) {
                    assertThat(selected.consumerId()).as("validationPoint %d, removed %s, hash %d ranges %s", i,
                            removedConsumer.toString(), hash, consumerKeyHashRanges).isEqualTo(expected.consumerId());
                }
            }
        }
    }

    @Test
    public void testShouldNotChangeSelectedConsumerWhenConsumerIsRemovedCheckHashRanges() {
        final ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(100);
        final String consumerName = "consumer";
        final int numOfInitialConsumers = 25;
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numOfInitialConsumers; i++) {
            final Consumer consumer = createMockConsumer(consumerName, "index " + i, i);
            consumers.add(consumer);
            selector.addConsumer(consumer);
        }

        Map<Consumer, List<Range>> expected = selector.getConsumerKeyHashRanges();
        assertThat(selector.getConsumerKeyHashRanges()).as("sanity check").containsExactlyInAnyOrderEntriesOf(expected);
        System.out.println(expected);

        for (Consumer removedConsumer : consumers) {
            selector.removeConsumer(removedConsumer);
            for (Map.Entry<Consumer, List<Range>> entry : expected.entrySet()) {
                if (entry.getKey() == removedConsumer) {
                    continue;
                }
                for (Range range : entry.getValue()) {
                    Consumer rangeStartConsumer = selector.select(range.getStart());
                    assertThat(rangeStartConsumer).as("removed %s, range %s", removedConsumer, range)
                            .isEqualTo(entry.getKey());
                    Consumer rangeEndConsumer = selector.select(range.getEnd());
                    assertThat(rangeEndConsumer).as("removed %s, range %s", removedConsumer, range)
                            .isEqualTo(entry.getKey());
                    assertThat(rangeStartConsumer).isSameAs(rangeEndConsumer);
                }
            }
            expected = selector.getConsumerKeyHashRanges();
        }
    }

    @Test
    public void testShouldNotChangeSelectedConsumerUnnecessarilyWhenConsumerIsAddedCheckHashRanges() {
        final ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(100);
        final String consumerName = "consumer";
        final int numOfInitialConsumers = 25;
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numOfInitialConsumers; i++) {
            final Consumer consumer = createMockConsumer(consumerName, "index " + i, i);
            consumers.add(consumer);
            selector.addConsumer(consumer);
        }

        Map<Consumer, List<Range>> expected = selector.getConsumerKeyHashRanges();
        assertThat(selector.getConsumerKeyHashRanges()).as("sanity check").containsExactlyInAnyOrderEntriesOf(expected);

        for (int i = numOfInitialConsumers; i < numOfInitialConsumers * 2; i++) {
            final Consumer addedConsumer = createMockConsumer(consumerName, "index " + i, i);
            selector.addConsumer(addedConsumer);
            for (Map.Entry<Consumer, List<Range>> entry : expected.entrySet()) {
                if (entry.getKey() == addedConsumer) {
                    continue;
                }
                for (Range range : entry.getValue()) {
                    Consumer rangeStartConsumer = selector.select(range.getStart());
                    if (rangeStartConsumer != addedConsumer) {
                        assertThat(rangeStartConsumer).as("added %s, range start %s", addedConsumer, range)
                                .isEqualTo(entry.getKey());
                    }
                    Consumer rangeEndConsumer = selector.select(range.getStart());
                    if (rangeEndConsumer != addedConsumer) {
                        assertThat(rangeEndConsumer).as("added %s, range end %s", addedConsumer, range)
                                .isEqualTo(entry.getKey());
                    }
                }
            }
            expected = selector.getConsumerKeyHashRanges();
        }
    }

    @Test
    public void testShouldNotChangeSelectedConsumerWhenConsumerIsAdded() {
        final ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(100);
        final String consumerName = "consumer";
        final int numOfInitialConsumers = 50;
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numOfInitialConsumers; i++) {
            final Consumer consumer = createMockConsumer(consumerName, "index " + i, i);
            consumers.add(consumer);
            selector.addConsumer(consumer);
        }

        int hashRangeSize = selector.getKeyHashRange().size();
        int validationPointCount = 200;
        int increment = hashRangeSize / (validationPointCount + 1);
        List<Consumer> selectedConsumerBeforeRemoval = new ArrayList<>();

        for (int i = 0; i < validationPointCount; i++) {
            selectedConsumerBeforeRemoval.add(selector.select(i * increment));
        }

        for (int i = 0; i < validationPointCount; i++) {
            Consumer selected = selector.select(i * increment);
            Consumer expected = selectedConsumerBeforeRemoval.get(i);
            assertThat(selected.consumerId()).as("validationPoint %d", i).isEqualTo(expected.consumerId());
        }

        Set<Consumer> addedConsumers = new HashSet<>();
        for (int i = numOfInitialConsumers; i < numOfInitialConsumers * 2; i++) {
            final Consumer addedConsumer = createMockConsumer(consumerName, "index " + i, i);
            selector.addConsumer(addedConsumer);
            addedConsumers.add(addedConsumer);
            for (int j = 0; j < validationPointCount; j++) {
                int hash = j * increment;
                Consumer selected = selector.select(hash);
                Consumer expected = selectedConsumerBeforeRemoval.get(j);
                if (!addedConsumers.contains(addedConsumer)) {
                    assertThat(selected.consumerId()).as("validationPoint %d, hash %d", j, hash).isEqualTo(expected.consumerId());
                }
            }
        }
    }

    @Test
    public void testShouldContainMinimalMappingChangesWhenConsumerLeavesAndRejoins() {
        final ConsistentHashingStickyKeyConsumerSelector selector =
                new ConsistentHashingStickyKeyConsumerSelector(100, true);
        final String consumerName = "consumer";
        final int numOfInitialConsumers = 10;
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numOfInitialConsumers; i++) {
            final Consumer consumer = createMockConsumer(consumerName, "index " + i, i);
            consumers.add(consumer);
            selector.addConsumer(consumer);
        }

        ConsumerHashAssignmentsSnapshot assignmentsBefore = selector.getConsumerHashAssignmentsSnapshot();

        Map<Consumer, List<Range>> expected = selector.getConsumerKeyHashRanges();
        assertThat(selector.getConsumerKeyHashRanges()).as("sanity check").containsExactlyInAnyOrderEntriesOf(expected);

        selector.removeConsumer(consumers.get(0));
        selector.removeConsumer(consumers.get(numOfInitialConsumers / 2));
        selector.addConsumer(consumers.get(0));
        selector.addConsumer(consumers.get(numOfInitialConsumers / 2));

        ConsumerHashAssignmentsSnapshot assignmentsAfter = selector.getConsumerHashAssignmentsSnapshot();
        int removedRangesSize = assignmentsBefore.diffRanges(assignmentsAfter).keySet().stream()
                .mapToInt(Range::size)
                .sum();
        double allowedremovedRangesPercentage = 1; // 1%
        int hashRangeSize = selector.getKeyHashRange().size();
        int allowedremovedRanges = (int) (hashRangeSize * (allowedremovedRangesPercentage / 100.0d));
        assertThat(removedRangesSize).describedAs("Allow up to %d%% of total hash range size to be impacted",
                allowedremovedRangesPercentage).isLessThan(allowedremovedRanges);
    }

    @Test
    public void testShouldNotSwapExistingConsumers() {
        final ConsistentHashingStickyKeyConsumerSelector selector =
                new ConsistentHashingStickyKeyConsumerSelector(200, true);
        final String consumerName = "consumer";
        final int consumerCount = 100;
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            final Consumer consumer = createMockConsumer(consumerName + i, "index " + i, i);
            consumers.add(consumer);
            selector.addConsumer(consumer);
        }
        ConsumerHashAssignmentsSnapshot assignmentsBefore = selector.getConsumerHashAssignmentsSnapshot();
        for (int i = 0; i < consumerCount; i++) {
            Consumer consumer = consumers.get(i);

            // remove consumer
            selector.removeConsumer(consumer);

            ConsumerHashAssignmentsSnapshot assignmentsAfter = selector.getConsumerHashAssignmentsSnapshot();
            assertThat(assignmentsBefore.resolveImpactedConsumers(assignmentsAfter).getRemovedHashRanges())
                    .describedAs(
                            "when a consumer is removed, the removed hash ranges should only be from "
                                    + "the removed consumer")
                    .containsOnlyKeys(consumer);
            assignmentsBefore = assignmentsAfter;

            // add consumer back
            selector.addConsumer(consumer);

            assignmentsAfter = selector.getConsumerHashAssignmentsSnapshot();
            List<Range> addedConsumerRanges = assignmentsAfter.getRangesByConsumer().get(consumer);

            Map<Consumer, RemovedHashRanges> removedHashRanges =
                    assignmentsBefore.resolveImpactedConsumers(assignmentsAfter).getRemovedHashRanges();
            ConsumerHashAssignmentsSnapshot finalAssignmentsBefore = assignmentsBefore;
            assertThat(removedHashRanges).allSatisfy((c, removedHashRange) -> {
                assertThat(removedHashRange
                        .isFullyContainedInRanges(finalAssignmentsBefore.getRangesByConsumer().get(c)))
                        .isTrue();
                assertThat(removedHashRange
                        .isFullyContainedInRanges(addedConsumerRanges))
                        .isTrue();
            }).describedAs("when a consumer is added back, all removed hash ranges should be ones "
                    + "that are moved from existing consumers to the new consumer.");

            List<Range> allRemovedRanges =
                    ConsumerHashAssignmentsSnapshot.mergeOverlappingRanges(
                            removedHashRanges.entrySet().stream().map(Map.Entry::getValue)
                                    .map(RemovedHashRanges::asRanges)
                                    .flatMap(List::stream).collect(Collectors.toCollection(TreeSet::new)));
            assertThat(allRemovedRanges)
                    .describedAs("all removed ranges should be the same as the ranges of the added consumer")
                    .containsExactlyElementsOf(addedConsumerRanges);

            assignmentsBefore = assignmentsAfter;
        }
    }

    @Test
    public void testConsumersReconnect() {
        final ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(100);
        final String consumerName = "consumer";
        final int numOfInitialConsumers = 50;
        final int validationPointCount = 200;
        final List<Integer> pointsToTest = pointsToTest(validationPointCount, selector.getKeyHashRange().size());
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numOfInitialConsumers; i++) {
            final Consumer consumer = createMockConsumer(consumerName, "index " + i, i);
            consumers.add(consumer);
            selector.addConsumer(consumer);
        }

        // Mark original results.
        List<Consumer> selectedConsumersBeforeRemove = new ArrayList<>();
        for (int i = 0; i < validationPointCount; i++) {
            int point = pointsToTest.get(i);
            selectedConsumersBeforeRemove.add(selector.select(point));
        }

        // All consumers leave (in any order)
        List<Consumer> randomOrderConsumers = new ArrayList<>(consumers);
        Collections.shuffle(randomOrderConsumers);
        for (Consumer c : randomOrderConsumers) {
            selector.removeConsumer(c);
        }

        // All consumers reconnect in the same order as originally
        for (Consumer c : consumers) {
            selector.addConsumer(c);
        }

        // Check that the same consumers are selected as before
        for (int j = 0; j < validationPointCount; j++) {
            int point = pointsToTest.get(j);
            Consumer selected = selector.select(point);
            Consumer expected = selectedConsumersBeforeRemove.get(j);
            assertThat(selected.consumerId()).as("validationPoint %d, hash %d", j, point).isEqualTo(expected.consumerId());
        }
    }

    private List<Integer> pointsToTest(int validationPointCount, int hashRangeSize) {
        List<Integer> res = new ArrayList<>();
        final int increment = hashRangeSize / (validationPointCount + 1);
        for (int i = 0; i < validationPointCount; i++) {
            res.add(Math.max(i * increment, hashRangeSize - 1));
        }
        return res;
    }

    @Test(enabled = false)
    public void testPerformanceOfAdding1000ConsumersWith100Points() {
        // test that adding 1000 consumers with 100 points runs in a reasonable time.
        // This takes about 1 second on Apple M3
        // this unit test can be used for basic profiling
        final ConsistentHashingStickyKeyConsumerSelector selector =
                new ConsistentHashingStickyKeyConsumerSelector(100, true);
        for (int i = 0; i < 1000; i++) {
            // use real class to avoid Mockito over head
            final Consumer consumer = new Consumer("consumer" + i, 0) {
                @Override
                public int hashCode() {
                    return consumerName().hashCode();
                }

                @Override
                public boolean equals(Object obj) {
                    if (obj instanceof Consumer) {
                        return consumerName().equals(((Consumer) obj).consumerName());
                    }
                    return false;
                }
            };
            selector.addConsumer(consumer);
        }
    }
}