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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class HashRangeStickyKeyConsumerSelectorTest {

    @Test
    public void testConsumerSelect() {

        StickyKeyConsumerSelector selector = new HashRangeStickyKeyConsumerSelector();
        String key1 = "anyKey";
        Assert.assertNull(selector.select(key1));

        Consumer consumer1 = mock(Consumer.class);
        selector.addConsumer(consumer1);
        int consumer1Slot = HashRangeStickyKeyConsumerSelector.RANGE_SIZE;
        Assert.assertEquals(selector.select(key1), consumer1);

        Consumer consumer2 = mock(Consumer.class);
        selector.addConsumer(consumer2);
        int consumer2Slot = consumer1Slot >> 1;

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Math.abs(key.hashCode() % HashRangeStickyKeyConsumerSelector.RANGE_SIZE);
            if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key), consumer2);
            } else {
                Assert.assertEquals(selector.select(key), consumer1);
            }
        }

        Consumer consumer3 = mock(Consumer.class);
        selector.addConsumer(consumer3);
        int consumer3Slot = consumer2Slot >> 1;

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Math.abs(key.hashCode() % HashRangeStickyKeyConsumerSelector.RANGE_SIZE);
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key), consumer3);
            } else if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key), consumer2);
            } else {
                Assert.assertEquals(selector.select(key), consumer1);
            }
        }

        Consumer consumer4 = mock(Consumer.class);
        selector.addConsumer(consumer4);
        int consumer4Slot = consumer1Slot - ((consumer1Slot - consumer2Slot) >> 1);

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Math.abs(key.hashCode() % HashRangeStickyKeyConsumerSelector.RANGE_SIZE);
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key), consumer3);
            } else if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key), consumer2);
            } else if (slot < consumer4Slot) {
                Assert.assertEquals(selector.select(key), consumer4);
            } else {
                Assert.assertEquals(selector.select(key), consumer1);
            }
        }

        selector.removeConsumer(consumer1);
        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Math.abs(key.hashCode() % HashRangeStickyKeyConsumerSelector.RANGE_SIZE);
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key), consumer3);
            } else if (slot < consumer2Slot) {
                Assert.assertEquals(selector.select(key), consumer2);
            } else {
                Assert.assertEquals(selector.select(key), consumer4);
            }
        }

        selector.removeConsumer(consumer2);
        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            int slot = Math.abs(key.hashCode() % HashRangeStickyKeyConsumerSelector.RANGE_SIZE);
            if (slot < consumer3Slot) {
                Assert.assertEquals(selector.select(key), consumer3);
            } else {
                Assert.assertEquals(selector.select(key), consumer4);
            }
        }

        selector.removeConsumer(consumer3);
        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            Assert.assertEquals(selector.select(key), consumer4);
        }
    }
}
