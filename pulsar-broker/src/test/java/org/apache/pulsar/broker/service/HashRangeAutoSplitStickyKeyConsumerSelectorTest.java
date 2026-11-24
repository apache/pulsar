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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Range;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class HashRangeAutoSplitStickyKeyConsumerSelectorTest {

    @Test
    public void testGetConsumerKeyHashRanges() throws BrokerServiceException.ConsumerAssignException {
        HashRangeAutoSplitStickyKeyConsumerSelector selector =
                new HashRangeAutoSplitStickyKeyConsumerSelector(2 << 5, false);
        List<String> consumerName = Arrays.asList("consumer1", "consumer2", "consumer3", "consumer4");
        List<Consumer> consumers = new ArrayList<>();
        for (String s : consumerName) {
            Consumer consumer = mock(Consumer.class);
            when(consumer.consumerName()).thenReturn(s);
            selector.addConsumer(consumer);
            consumers.add(consumer);
        }

        Map<Consumer, List<Range>> expectedResult = new HashMap<>();
        expectedResult.put(consumers.get(0), Collections.singletonList(Range.of(49, 64)));
        expectedResult.put(consumers.get(3), Collections.singletonList(Range.of(33, 48)));
        expectedResult.put(consumers.get(1), Collections.singletonList(Range.of(17, 32)));
        expectedResult.put(consumers.get(2), Collections.singletonList(Range.of(0, 16)));
        for (Map.Entry<Consumer, List<Range>> entry : selector.getConsumerKeyHashRanges().entrySet()) {
            Assert.assertEquals(entry.getValue(), expectedResult.get(entry.getKey()));
            expectedResult.remove(entry.getKey());
        }
        Assert.assertEquals(expectedResult.size(), 0);
    }

    @Test
    public void testGetConsumerKeyHashRangesWithSameConsumerName() throws Exception {
        HashRangeAutoSplitStickyKeyConsumerSelector selector =
                new HashRangeAutoSplitStickyKeyConsumerSelector(2 << 5, false);
        final String consumerName = "My-consumer";
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Consumer consumer = mock(Consumer.class);
            when(consumer.consumerName()).thenReturn(consumerName);
            selector.addConsumer(consumer);
            consumers.add(consumer);
        }

        List<Range> prev = null;
        for (Consumer consumer : consumers) {
            List<Range> ranges = selector.getConsumerKeyHashRanges().get(consumer);
            Assert.assertEquals(ranges.size(), 1);
            if (prev != null) {
                Assert.assertNotEquals(prev, ranges);
            }
            prev = ranges;
        }
    }

    @Test
    public void testSelectionMatchesPublishedRangesAcrossMembershipChanges() throws Exception {
        HashRangeAutoSplitStickyKeyConsumerSelector selector =
                new HashRangeAutoSplitStickyKeyConsumerSelector(2 << 5, false);
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Consumer consumer = mock(Consumer.class);
            selector.addConsumer(consumer).join();
            consumers.add(consumer);
            assertSelectionMatchesRanges(selector, 64);
        }
        for (Consumer consumer : consumers) {
            selector.removeConsumer(consumer);
            assertSelectionMatchesRanges(selector, 64);
        }
        Assert.assertNull(selector.select(0));
    }

    @Test(timeOut = 30000)
    public void testConcurrentSelectionDuringMembershipChanges() throws Exception {
        HashRangeAutoSplitStickyKeyConsumerSelector selector =
                new HashRangeAutoSplitStickyKeyConsumerSelector(2 << 10, false);
        Consumer stableConsumer = mock(Consumer.class);
        Consumer transientConsumer = mock(Consumer.class);
        selector.addConsumer(stableConsumer).join();
        Range hashRange = selector.getKeyHashRange();
        int readerCount = 3;
        int rounds = 100;
        CyclicBarrier phase = new CyclicBarrier(readerCount + 1);
        ExecutorService executor = Executors.newFixedThreadPool(readerCount);
        List<Future<?>> readers = new ArrayList<>();
        try {
            for (int reader = 0; reader < readerCount; reader++) {
                readers.add(executor.submit(() -> {
                    for (int round = 0; round < rounds; round++) {
                        phase.await(5, TimeUnit.SECONDS);
                        for (int hash = hashRange.getStart(); hash <= hashRange.getEnd(); hash++) {
                            Consumer selected = selector.select(hash);
                            Assert.assertTrue(selected == stableConsumer || selected == transientConsumer);
                        }
                        phase.await(5, TimeUnit.SECONDS);
                    }
                    for (int hash = hashRange.getStart(); hash <= hashRange.getEnd(); hash++) {
                        Assert.assertSame(selector.select(hash), stableConsumer, "hash " + hash);
                    }
                    return null;
                }));
            }
            for (int round = 0; round < rounds; round++) {
                // Readers and membership changes participate in every round before advancing together.
                phase.await(5, TimeUnit.SECONDS);
                selector.addConsumer(transientConsumer).join();
                selector.removeConsumer(transientConsumer);
                phase.await(5, TimeUnit.SECONDS);
            }
            for (Future<?> reader : readers) {
                reader.get(10, TimeUnit.SECONDS);
            }
            selector.removeConsumer(stableConsumer);
            for (int hash = hashRange.getStart(); hash <= hashRange.getEnd(); hash++) {
                Assert.assertNull(selector.select(hash), "hash " + hash);
            }
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void assertSelectionMatchesRanges(HashRangeAutoSplitStickyKeyConsumerSelector selector,
                                                     int rangeSize) {
        Map<Consumer, List<Range>> ranges = selector.getConsumerKeyHashRanges();
        for (int hash = 0; hash < rangeSize; hash++) {
            Consumer selected = selector.select(hash);
            int currentHash = hash;
            Consumer expected = ranges.entrySet().stream()
                    .filter(entry -> entry.getValue().stream().anyMatch(range -> range.contains(currentHash)))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            Assert.assertSame(selected, expected, "hash " + hash);
        }
    }
}
