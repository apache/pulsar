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

import static org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
import static org.mockito.Mockito.mock;

import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class HashRangeAutoSplitStickyKeyConsumerSelectorTest {

    @Test
    public void testConsumerSelect() throws ConsumerAssignException {

        HashRangeAutoSplitStickyKeyConsumerSelector selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
        String key1 = "anyKey";
        Assert.assertNull(selector.select(key1.getBytes()));

        Consumer consumer1 = mock(Consumer.class);
        selector.addConsumer(consumer1);
        int consumer1Slot = DEFAULT_RANGE_SIZE;
        Assert.assertEquals(selector.select(key1.getBytes()), consumer1);
        Assert.assertEquals(selector.getConsumerRange().size(), 1);
        Assert.assertEquals(selector.getRangeConsumer().size(), 1);

        Consumer consumer2 = mock(Consumer.class);
        selector.addConsumer(consumer2);
        Assert.assertEquals(selector.getConsumerRange().size(), 2);
        Assert.assertEquals(selector.getRangeConsumer().size(), 2);
        int consumer2Slot = consumer1Slot >> 1;

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes()) % DEFAULT_RANGE_SIZE;
            if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer2);
            } else {
                Assert.assertEquals(selector.select(key.getBytes()), consumer1);
            }
        }

        Consumer consumer3 = mock(Consumer.class);
        selector.addConsumer(consumer3);
        Assert.assertEquals(selector.getConsumerRange().size(), 3);
        Assert.assertEquals(selector.getRangeConsumer().size(), 3);
        int consumer3Slot = consumer2Slot >> 1;

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes()) % DEFAULT_RANGE_SIZE;
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer3);
            } else if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer2);
            } else {
                Assert.assertEquals(selector.select(key.getBytes()), consumer1);
            }
        }

        Consumer consumer4 = mock(Consumer.class);
        selector.addConsumer(consumer4);
        Assert.assertEquals(selector.getConsumerRange().size(), 4);
        Assert.assertEquals(selector.getRangeConsumer().size(), 4);
        int consumer4Slot = consumer1Slot - ((consumer1Slot - consumer2Slot) >> 1);

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes()) % DEFAULT_RANGE_SIZE;
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer3);
            } else if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer2);
            } else if (slot < consumer4Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer4);
            } else {
                Assert.assertEquals(selector.select(key.getBytes()), consumer1);
            }
        }

        selector.removeConsumer(consumer1);
        Assert.assertEquals(selector.getConsumerRange().size(), 3);
        Assert.assertEquals(selector.getRangeConsumer().size(), 3);
        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes()) % DEFAULT_RANGE_SIZE;
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer3);
            } else if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer2);
            } else {
                Assert.assertEquals(selector.select(key.getBytes()), consumer4);
            }
        }

        selector.removeConsumer(consumer2);
        Assert.assertEquals(selector.getConsumerRange().size(), 2);
        Assert.assertEquals(selector.getRangeConsumer().size(), 2);
        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes()) % DEFAULT_RANGE_SIZE;
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key.getBytes()), consumer3);
            } else {
                Assert.assertEquals(selector.select(key.getBytes()), consumer4);
            }
        }

        selector.removeConsumer(consumer3);
        Assert.assertEquals(selector.getConsumerRange().size(), 1);
        Assert.assertEquals(selector.getRangeConsumer().size(), 1);
        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            Assert.assertEquals(selector.select(key.getBytes()), consumer4);
        }
    }

    @Test(expectedExceptions = ConsumerAssignException.class)
    public void testSplitExceed() throws ConsumerAssignException {
        StickyKeyConsumerSelector selector = new HashRangeAutoSplitStickyKeyConsumerSelector(16);
        for (int i = 0; i <= 16; i++) {
            selector.addConsumer(mock(Consumer.class));
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRangeSizeLessThan2() {
        new HashRangeAutoSplitStickyKeyConsumerSelector(1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRangeSizePower2() {
        new HashRangeAutoSplitStickyKeyConsumerSelector(6);
    }
}
