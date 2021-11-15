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

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate.Predicate4HashRangeAutoSplitStickyKeyConsumerSelector;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(groups = "broker")
public class HashRangeAutoSplitStickyKeyConsumerSelectorTest {

    @Test
    public void testEventListener() throws Exception{
        final HashRangeAutoSplitStickyKeyConsumerSelector selector =
                new HashRangeAutoSplitStickyKeyConsumerSelector();
        // consumer count: 0 --> 1
        Consumer consumer1 = mock(Consumer.class);
        when(consumer1.consumerName()).thenReturn("c1");
        AtomicInteger eventCount1 = new AtomicInteger();
        doAnswer(invocation -> {
            String props = invocation.getArgument(0);
            StickyKeyConsumerPredicate predicate = StickyKeyConsumerPredicate.decode(props);
            Assert.assertTrue(predicate instanceof Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
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
            Assert.assertTrue(predicate instanceof Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
            eventCount2.incrementAndGet();
            return null;
        }).when(consumer2).notifyActiveConsumerChange(anyString());
        selector.addConsumer(consumer2);
        Assert.assertEquals(2, eventCount1.get());
        Assert.assertEquals(1, eventCount2.get());
        // consumer count: 2 --> 3
        Consumer consumer3 = mock(Consumer.class);
        when(consumer3.consumerName()).thenReturn("c3");
        AtomicInteger eventCount3 = new AtomicInteger();
        doAnswer(invocation -> {
            String props = invocation.getArgument(0);
            StickyKeyConsumerPredicate predicate = StickyKeyConsumerPredicate.decode(props);
            Assert.assertTrue(predicate instanceof Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
            eventCount3.incrementAndGet();
            return null;
        }).when(consumer3).notifyActiveConsumerChange(anyString());
        selector.addConsumer(consumer3);
        Assert.assertEquals(eventCount1.get(), 2);
        Assert.assertEquals(eventCount2.get(), 2);
        Assert.assertEquals(eventCount3.get(), 1);
        // consumer count: 3 --> 2
        selector.removeConsumer(consumer1);
        Assert.assertEquals(eventCount1.get(), 2);
        Assert.assertEquals(eventCount2.get(), 3);
        Assert.assertEquals(eventCount3.get(), 1);
        // consumer count: 2 --> 1
        selector.removeConsumer(consumer2);
        Assert.assertEquals(eventCount1.get(), 2);
        Assert.assertEquals(eventCount2.get(), 3);
        Assert.assertEquals(eventCount3.get(), 2);
        // consumer count: 1 --> 0
        selector.removeConsumer(consumer3);
        Assert.assertEquals(eventCount1.get(), 2);
        Assert.assertEquals(eventCount2.get(), 3);
        Assert.assertEquals(eventCount3.get(), 2);
    }

    @Test(dependsOnMethods = {"testGetConsumerKeyHashRanges", "testGetConsumerKeyHashRangesWithSameConsumerName"})
    public void testGenerateSpecialPredicate() throws Exception{
        HashRangeAutoSplitStickyKeyConsumerSelector selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
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
        Map<Consumer, StickyKeyConsumerPredicate> predicateMapping = new HashMap<>();
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
        // weave listener
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
    public void testGetConsumerKeyHashRanges() throws BrokerServiceException.ConsumerAssignException {
        HashRangeAutoSplitStickyKeyConsumerSelector selector = new HashRangeAutoSplitStickyKeyConsumerSelector(2 << 5);
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
        HashRangeAutoSplitStickyKeyConsumerSelector selector = new HashRangeAutoSplitStickyKeyConsumerSelector(2 << 5);
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
}
