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

import com.google.common.collect.Lists;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HashRangeExclusiveStickyKeyConsumerSelectorTest {

    @Test
    public void testConsumerSelect() throws BrokerServiceException.ConsumerAssignException {

        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        Consumer consumer1 = mock(Consumer.class);
        PulsarApi.KeySharedMeta keySharedMeta1 = PulsarApi.KeySharedMeta.newBuilder()
                .setKeySharedMode(PulsarApi.KeySharedMode.STICKY)
                .addHashRanges(PulsarApi.IntRange.newBuilder().setStart(0).setEnd(2).build())
                .build();
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
        PulsarApi.KeySharedMeta keySharedMeta2 = PulsarApi.KeySharedMeta.newBuilder()
                .setKeySharedMode(PulsarApi.KeySharedMode.STICKY)
                .addHashRanges(PulsarApi.IntRange.newBuilder().setStart(3).setEnd(9).build())
                .build();
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
        PulsarApi.KeySharedMeta keySharedMeta = PulsarApi.KeySharedMeta.newBuilder()
                .setKeySharedMode(PulsarApi.KeySharedMode.STICKY)
                .build();
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
    public void testSingleRangeConflict() throws BrokerServiceException.ConsumerAssignException {
        HashRangeExclusiveStickyKeyConsumerSelector selector = new HashRangeExclusiveStickyKeyConsumerSelector(10);
        Consumer consumer1 = mock(Consumer.class);
        PulsarApi.KeySharedMeta keySharedMeta1 = PulsarApi.KeySharedMeta.newBuilder()
                .setKeySharedMode(PulsarApi.KeySharedMode.STICKY)
                .addHashRanges(PulsarApi.IntRange.newBuilder().setStart(2).setEnd(5).build())
                .build();
        when(consumer1.getKeySharedMeta()).thenReturn(keySharedMeta1);
        Assert.assertEquals(consumer1.getKeySharedMeta(), keySharedMeta1);
        selector.addConsumer(consumer1);
        Assert.assertEquals(selector.getRangeConsumer().size(),2);

        final List<PulsarApi.IntRange> testRanges = new ArrayList<>();
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(4).setEnd(6).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(1).setEnd(3).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(2).setEnd(2).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(5).setEnd(5).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(1).setEnd(5).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(2).setEnd(6).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(2).setEnd(5).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(1).setEnd(6).build());
        testRanges.add(PulsarApi.IntRange.newBuilder().setStart(8).setEnd(6).build());

        for (PulsarApi.IntRange testRange : testRanges) {
            Consumer consumer = mock(Consumer.class);
            PulsarApi.KeySharedMeta keySharedMeta = PulsarApi.KeySharedMeta.newBuilder()
                    .setKeySharedMode(PulsarApi.KeySharedMode.STICKY)
                    .addHashRanges(testRange)
                    .build();
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
        PulsarApi.KeySharedMeta keySharedMeta1 = PulsarApi.KeySharedMeta.newBuilder()
                .setKeySharedMode(PulsarApi.KeySharedMode.STICKY)
                .addHashRanges(PulsarApi.IntRange.newBuilder().setStart(2).setEnd(5).build())
                .build();
        when(consumer1.getKeySharedMeta()).thenReturn(keySharedMeta1);
        Assert.assertEquals(consumer1.getKeySharedMeta(), keySharedMeta1);
        selector.addConsumer(consumer1);
        Assert.assertEquals(selector.getRangeConsumer().size(),2);

        final List<List<PulsarApi.IntRange>> testRanges = new ArrayList<>();
        testRanges.add(Lists.newArrayList(
                PulsarApi.IntRange.newBuilder().setStart(2).setEnd(2).build(),
                PulsarApi.IntRange.newBuilder().setStart(3).setEnd(3).build(),
                PulsarApi.IntRange.newBuilder().setStart(4).setEnd(5).build())
        );
        testRanges.add(Lists.newArrayList(
                PulsarApi.IntRange.newBuilder().setStart(0).setEnd(0).build(),
                PulsarApi.IntRange.newBuilder().setStart(1).setEnd(2).build())
        );

        for (List<PulsarApi.IntRange> testRange : testRanges) {
            Consumer consumer = mock(Consumer.class);
            PulsarApi.KeySharedMeta keySharedMeta = PulsarApi.KeySharedMeta.newBuilder()
                    .setKeySharedMode(PulsarApi.KeySharedMode.STICKY)
                    .addAllHashRanges(testRange)
                    .build();
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
