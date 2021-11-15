/**
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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate.Predicate4ConsistentHashingStickyKeyConsumerSelector;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Test(groups = "broker")
public class ConsistentHashingStickyKeyConsumerSelectorTest {

    @Test
    public void testEventListener() throws Exception{
        final ConsistentHashingStickyKeyConsumerSelector selector =
                new ConsistentHashingStickyKeyConsumerSelector(100);
        // consumer count: 0 --> 1
        Consumer consumer1 = mock(Consumer.class);
        when(consumer1.consumerName()).thenReturn("c1");
        AtomicInteger eventCount1 = new AtomicInteger();
        doAnswer(invocation -> {
            String props = invocation.getArgument(0);
            StickyKeyConsumerPredicate predicate = StickyKeyConsumerPredicate.decode(props);
            Assert.assertTrue(predicate instanceof Predicate4ConsistentHashingStickyKeyConsumerSelector);
            eventCount1.incrementAndGet();
            return null;
        }).when(consumer1).notifyActiveConsumerChange(anyString());
        selector.addConsumer(consumer1);
        Assert.assertEquals(1, eventCount1.get());
        // consumer count: 1 --> 2
        Consumer consumer2 = mock(Consumer.class);
        when(consumer2.consumerName()).thenReturn("c2");
        AtomicInteger eventCount2 = new AtomicInteger();
        doAnswer(invocation -> {
            String props = invocation.getArgument(0);
            StickyKeyConsumerPredicate predicate = StickyKeyConsumerPredicate.decode(props);
            Assert.assertTrue(predicate instanceof Predicate4ConsistentHashingStickyKeyConsumerSelector);
            eventCount2.incrementAndGet();
            return null;
        }).when(consumer2).notifyActiveConsumerChange(anyString());
        selector.addConsumer(consumer2);
        Assert.assertEquals(1, eventCount2.get());
        Assert.assertEquals(2, eventCount1.get());
        // consumer count: 2 --> 3
        Consumer consumer3 = mock(Consumer.class);
        when(consumer3.consumerName()).thenReturn("c3");
        AtomicInteger eventCount3 = new AtomicInteger();
        doAnswer(invocation -> {
            String props = invocation.getArgument(0);
            StickyKeyConsumerPredicate predicate = StickyKeyConsumerPredicate.decode(props);
            Assert.assertTrue(predicate instanceof Predicate4ConsistentHashingStickyKeyConsumerSelector);
            eventCount3.incrementAndGet();
            return null;
        }).when(consumer3).notifyActiveConsumerChange(anyString());
        selector.addConsumer(consumer3);
        Assert.assertEquals(eventCount1.get(), 3);
        Assert.assertEquals(eventCount2.get(), 2);
        Assert.assertEquals(eventCount3.get(), 1);
        // consumer count: 3 --> 2
        selector.removeConsumer(consumer1);
        Assert.assertEquals(eventCount1.get(), 3);
        Assert.assertEquals(eventCount2.get(), 3);
        Assert.assertEquals(eventCount3.get(), 2);
        // consumer count: 2 --> 1
        selector.removeConsumer(consumer2);
        Assert.assertEquals(eventCount1.get(), 3);
        Assert.assertEquals(eventCount2.get(), 3);
        Assert.assertEquals(eventCount3.get(), 3);
        // consumer count: 1 --> 0
        selector.removeConsumer(consumer3);
        Assert.assertEquals(eventCount1.get(), 3);
        Assert.assertEquals(eventCount2.get(), 3);
        Assert.assertEquals(eventCount3.get(), 3);
    }

    @Test(dependsOnMethods = {"testConsumerSelect"})
    public void testGenerateSpecialPredicate() throws Exception{
        final ConsistentHashingStickyKeyConsumerSelector selector = new ConsistentHashingStickyKeyConsumerSelector(100);
        String key1 = "anyKey";
        // one consumer
        Consumer consumer1 = mock(Consumer.class);
        when(consumer1.consumerName()).thenReturn("c1");
        selector.addConsumer(consumer1);
        Assert.assertTrue(selector.generateSpecialPredicate(consumer1).test(key1));
        // more consumer
        Consumer consumer2 = mock(Consumer.class);
        when(consumer2.consumerName()).thenReturn("c2");
        selector.addConsumer(consumer2);
        Consumer consumer3 = mock(Consumer.class);
        when(consumer3.consumerName()).thenReturn("c3");
        selector.addConsumer(consumer3);
        Consumer consumer4 = mock(Consumer.class);
        when(consumer4.consumerName()).thenReturn("c4");
        selector.addConsumer(consumer4);
        Consumer consumer5 = mock(Consumer.class);
        when(consumer5.consumerName()).thenReturn("c5");
        selector.addConsumer(consumer5);
        // do test
        final Map<Consumer, StickyKeyConsumerPredicate> predicateMapping = new HashMap<>();
        predicateMapping.put(consumer1,
                StickyKeyConsumerPredicate.decode(selector.generateSpecialPredicate(consumer1).encode()));
        predicateMapping.put(consumer2,
                StickyKeyConsumerPredicate.decode(selector.generateSpecialPredicate(consumer2).encode()));
        predicateMapping.put(consumer3,
                StickyKeyConsumerPredicate.decode(selector.generateSpecialPredicate(consumer3).encode()));
        predicateMapping.put(consumer4,
                StickyKeyConsumerPredicate.decode(selector.generateSpecialPredicate(consumer4).encode()));
        predicateMapping.put(consumer5,
                StickyKeyConsumerPredicate.decode(selector.generateSpecialPredicate(consumer5).encode()));
        for (int i = 0; i < 100; i++){
            String randomKey = UUID.randomUUID().toString();
            Consumer selectedConsumer = selector.select(randomKey.getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<Consumer, StickyKeyConsumerPredicate> entry : predicateMapping.entrySet()){
                if (selectedConsumer == entry.getKey()){
                    Assert.assertTrue(entry.getValue().test(randomKey));
                } else {
                    Assert.assertFalse(entry.getValue().test(randomKey));
                }
            }
        }
    }

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
                Range.of(0, 330121749),
                Range.of(330121750, 618146114),
                Range.of(1797637922, 1976098885)));
        expectedResult.put(consumers.get(1), Arrays.asList(
                Range.of(938427576, 1094135919),
                Range.of(1138613629, 1342907082),
                Range.of(1342907083, 1797637921)));
        expectedResult.put(consumers.get(2), Arrays.asList(
                Range.of(618146115, 772640562),
                Range.of(772640563, 938427575),
                Range.of(1094135920, 1138613628)));
        for (Map.Entry<Consumer, List<Range>> entry : selector.getConsumerKeyHashRanges().entrySet()) {
            System.out.println(entry.getValue());
            Assert.assertEquals(entry.getValue(), expectedResult.get(entry.getKey()));
            expectedResult.remove(entry.getKey());
        }
        Assert.assertEquals(expectedResult.size(), 0);
    }
}
