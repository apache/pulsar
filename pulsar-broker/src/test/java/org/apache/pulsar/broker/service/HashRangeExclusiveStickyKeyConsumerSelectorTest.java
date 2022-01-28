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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.IntRange;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class HashRangeExclusiveStickyKeyConsumerSelectorTest {

    @Test
    public void testConsumerSelect() throws BrokerServiceException.ConsumerAssignException {

        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        Consumer consumer1 = mock(Consumer.class);
        KeySharedMeta keySharedMeta1 = new KeySharedMeta()
                .setKeySharedMode(KeySharedMode.STICKY);
        keySharedMeta1.addHashRange().setStart(0).setEnd(2);
        when(consumer1.getKeySharedMeta()).thenReturn(keySharedMeta1);
        Assert.assertEquals(consumer1.getKeySharedMeta(), keySharedMeta1);
        selector.addConsumer(consumer1);
        Assert.assertEquals(selector.getRangeConsumer().size(),2);
        Consumer selectedConsumer;
        for (int i = 0; i < 3; i++) {
            selectedConsumer = selector.select(i);
            Assert.assertEquals(selectedConsumer, consumer1);
        }
        selectedConsumer = selector.select(4);
        Assert.assertNull(selectedConsumer);

        Consumer consumer2 = mock(Consumer.class);
        KeySharedMeta keySharedMeta2 = new KeySharedMeta()
                .setKeySharedMode(KeySharedMode.STICKY);
        keySharedMeta2.addHashRange().setStart(3).setEnd(9);
        when(consumer2.getKeySharedMeta()).thenReturn(keySharedMeta2);
        Assert.assertEquals(consumer2.getKeySharedMeta(), keySharedMeta2);
        selector.addConsumer(consumer2);
        Assert.assertEquals(selector.getRangeConsumer().size(),4);

        for (int i = 3; i < 10; i++) {
            selectedConsumer = selector.select(i);
            Assert.assertEquals(selectedConsumer, consumer2);
        }

        for (int i = 0; i < 3; i++) {
            selectedConsumer = selector.select(i);
            Assert.assertEquals(selectedConsumer, consumer1);
        }

        selector.removeConsumer(consumer1);
        Assert.assertEquals(selector.getRangeConsumer().size(),2);
        selectedConsumer = selector.select(1);
        Assert.assertNull(selectedConsumer);

        selector.removeConsumer(consumer2);
        Assert.assertEquals(selector.getRangeConsumer().size(),0);
        selectedConsumer = selector.select(5);
        Assert.assertNull(selectedConsumer);
    }

    @Test(expectedExceptions = BrokerServiceException.ConsumerAssignException.class)
    public void testEmptyRanges() throws BrokerServiceException.ConsumerAssignException {
        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        Consumer consumer = mock(Consumer.class);
        KeySharedMeta keySharedMeta = new KeySharedMeta()
                .setKeySharedMode(KeySharedMode.STICKY);
        when(consumer.getKeySharedMeta()).thenReturn(keySharedMeta);
        selector.addConsumer(consumer);
    }

    @Test(expectedExceptions = BrokerServiceException.ConsumerAssignException.class)
    public void testNullKeySharedMeta() throws BrokerServiceException.ConsumerAssignException {
        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        Consumer consumer = mock(Consumer.class);
        when(consumer.getKeySharedMeta()).thenReturn(null);
        selector.addConsumer(consumer);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidRangeTotal() {
        new HashRangeExclusiveStickyKeyConsumerSelector(0);
    }

    @Test
    public void testGetConsumerKeyHashRanges() throws BrokerServiceException.ConsumerAssignException {
        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        List<String> consumerName = Arrays.asList("consumer1", "consumer2", "consumer3", "consumer4");
        List<int[]> range = Arrays.asList(new int[] {0, 2}, new int[] {3, 7}, new int[] {9, 12}, new int[] {15, 20});
        List<Consumer> consumers = new ArrayList<>();
        for (int index = 0; index < consumerName.size(); index++) {
            Consumer consumer = mock(Consumer.class);
            KeySharedMeta keySharedMeta = new KeySharedMeta()
                    .setKeySharedMode(KeySharedMode.STICKY);
            keySharedMeta.addHashRange()
                    .setStart(range.get(index)[0])
                    .setEnd(range.get(index)[1]);
            when(consumer.getKeySharedMeta()).thenReturn(keySharedMeta);
            when(consumer.consumerName()).thenReturn(consumerName.get(index));
            Assert.assertEquals(consumer.getKeySharedMeta(), keySharedMeta);
            selector.addConsumer(consumer);
            consumers.add(consumer);
        }

        Map<Consumer, List<Range>> expectedResult = new HashMap<>();
        expectedResult.put(consumers.get(0), Collections.singletonList(Range.of(0, 2)));
        expectedResult.put(consumers.get(1), Collections.singletonList(Range.of(3, 7)));
        expectedResult.put(consumers.get(2), Collections.singletonList(Range.of(9, 12)));
        expectedResult.put(consumers.get(3), Collections.singletonList(Range.of(15, 20)));
        for (Map.Entry<Consumer, List<Range>> entry : selector.getConsumerKeyHashRanges().entrySet()) {
            Assert.assertEquals(entry.getValue(), expectedResult.get(entry.getKey()));
            expectedResult.remove(entry.getKey());
        }
        Assert.assertEquals(expectedResult.size(), 0);
    }

    @Test
    public void testGetConsumerKeyHashRangesWithSameConsumerName() throws Exception {
        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        final String consumerName = "My-consumer";
        List<int[]> range = Arrays.asList(new int[] {0, 2}, new int[] {3, 7}, new int[] {9, 12});
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Consumer consumer = mock(Consumer.class);
            KeySharedMeta keySharedMeta = new KeySharedMeta()
                    .setKeySharedMode(KeySharedMode.STICKY);
            keySharedMeta.addHashRange()
                    .setStart(range.get(i)[0])
                    .setEnd(range.get(i)[1]);
            when(consumer.getKeySharedMeta()).thenReturn(keySharedMeta);
            when(consumer.consumerName()).thenReturn(consumerName);
            Assert.assertEquals(consumer.getKeySharedMeta(), keySharedMeta);
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
    public void testSingleRangeConflict() throws BrokerServiceException.ConsumerAssignException {
        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        Consumer consumer1 = mock(Consumer.class);
        KeySharedMeta keySharedMeta1 = new KeySharedMeta()
                .setKeySharedMode(KeySharedMode.STICKY);
        keySharedMeta1.addHashRange().setStart(2).setEnd(5);
        when(consumer1.getKeySharedMeta()).thenReturn(keySharedMeta1);
        Assert.assertEquals(consumer1.getKeySharedMeta(), keySharedMeta1);
        selector.addConsumer(consumer1);
        Assert.assertEquals(selector.getRangeConsumer().size(),2);

        final List<IntRange> testRanges = new ArrayList<>();
        testRanges.add(new IntRange().setStart(4).setEnd(6));
        testRanges.add(new IntRange().setStart(1).setEnd(3));
        testRanges.add(new IntRange().setStart(2).setEnd(2));
        testRanges.add(new IntRange().setStart(5).setEnd(5));
        testRanges.add(new IntRange().setStart(1).setEnd(5));
        testRanges.add(new IntRange().setStart(2).setEnd(6));
        testRanges.add(new IntRange().setStart(2).setEnd(5));
        testRanges.add(new IntRange().setStart(1).setEnd(6));
        testRanges.add(new IntRange().setStart(8).setEnd(6));

        for (IntRange testRange : testRanges) {
            Consumer consumer = mock(Consumer.class);
            KeySharedMeta keySharedMeta = new KeySharedMeta()
                    .setKeySharedMode(KeySharedMode.STICKY);
            keySharedMeta.addHashRange().copyFrom(testRange);
            when(consumer.getKeySharedMeta()).thenReturn(keySharedMeta);
            Assert.assertEquals(consumer.getKeySharedMeta(), keySharedMeta);
            try {
                selector.addConsumer(consumer);
                Assert.fail("should be failed");
            } catch (BrokerServiceException.ConsumerAssignException ignore) {
            }
            Assert.assertEquals(selector.getRangeConsumer().size(),2);
        }
    }

    @Test
    public void testMultipleRangeConflict() throws BrokerServiceException.ConsumerAssignException {
        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        Consumer consumer1 = mock(Consumer.class);
        KeySharedMeta keySharedMeta1 = new KeySharedMeta()
                .setKeySharedMode(KeySharedMode.STICKY);
        keySharedMeta1.addHashRange().setStart(2).setEnd(5);
        when(consumer1.getKeySharedMeta()).thenReturn(keySharedMeta1);
        Assert.assertEquals(consumer1.getKeySharedMeta(), keySharedMeta1);
        selector.addConsumer(consumer1);
        Assert.assertEquals(selector.getRangeConsumer().size(),2);

        final List<List<IntRange>> testRanges = new ArrayList<>();
        testRanges.add(Lists.newArrayList(
                new IntRange().setStart(2).setEnd(2),
                new IntRange().setStart(3).setEnd(3),
                new IntRange().setStart(4).setEnd(5))
        );
        testRanges.add(Lists.newArrayList(
                new IntRange().setStart(0).setEnd(0),
                new IntRange().setStart(1).setEnd(2))
        );

        for (List<IntRange> testRange : testRanges) {
            Consumer consumer = mock(Consumer.class);
            KeySharedMeta keySharedMeta = new KeySharedMeta()
                    .setKeySharedMode(KeySharedMode.STICKY)
                    .addAllHashRanges(testRange);
            when(consumer.getKeySharedMeta()).thenReturn(keySharedMeta);
            Assert.assertEquals(consumer.getKeySharedMeta(), keySharedMeta);
            try {
                selector.addConsumer(consumer);
                Assert.fail("should be failed");
            } catch (BrokerServiceException.ConsumerAssignException ignore) {
            }
            Assert.assertEquals(selector.getRangeConsumer().size(),2);
        }
    }
}
