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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.testng.Assert;
import org.testng.annotations.Test;

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

}
