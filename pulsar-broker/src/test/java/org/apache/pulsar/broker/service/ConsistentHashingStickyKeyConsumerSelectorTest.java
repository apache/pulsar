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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsistentHashingStickyKeyConsumerSelectorTest {

    @Test
    public void testConsumerSelect() throws ConsumerAssignException {

        ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(100);
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
        for (String s : consumerName) {
            Consumer consumer = mock(Consumer.class);
            when(consumer.consumerName()).thenReturn(s);
            selector.addConsumer(consumer);
            consumers.add(consumer);
        }
        Map<Consumer, List<Range>> expectedResult = new HashMap<>();
        expectedResult.put(consumers.get(0), Arrays.asList(
                Range.of(119056335, 242013991),
                Range.of(722195657, 1656011842),
                Range.of(1707482098, 1914695766)));
        expectedResult.put(consumers.get(1), Arrays.asList(
                Range.of(0, 90164503),
                Range.of(90164504, 119056334),
                Range.of(382436668, 722195656)));
        expectedResult.put(consumers.get(2), Arrays.asList(
                Range.of(242013992, 242377547),
                Range.of(242377548, 382436667),
                Range.of(1656011843, 1707482097)));
        for (Map.Entry<Consumer, List<Range>> entry : selector.getConsumerKeyHashRanges().entrySet()) {
            System.out.println(entry.getValue());
            Assert.assertEquals(entry.getValue(), expectedResult.get(entry.getKey()));
            expectedResult.remove(entry.getKey());
        }
        Assert.assertEquals(expectedResult.size(), 0);
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
    }
}
