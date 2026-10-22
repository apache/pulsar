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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsumerNameIndexTrackerTest {
    private ConsumerNameIndexTracker tracker;

    @BeforeMethod
    public void setUp() {
        tracker = new ConsumerNameIndexTracker();
    }

    private static Consumer mockConsumer() {
        return mockConsumer("consumer");
    }


    private static Consumer mockConsumer(String consumerName) {
        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn(consumerName);
        return consumer;
    }

    @Test
    public void testIncreaseConsumerRefCountAndReturnIndex() {
        Consumer consumer1 = mockConsumer();
        Consumer consumer2 = mockConsumer();
        ConsumerIdentityWrapper wrapper1 = new ConsumerIdentityWrapper(consumer1);
        ConsumerIdentityWrapper wrapper2 = new ConsumerIdentityWrapper(consumer2);
        int index1 = tracker.increaseConsumerRefCountAndReturnIndex(wrapper1);
        int index2 = tracker.increaseConsumerRefCountAndReturnIndex(wrapper2);
        assertNotEquals(index1, index2);
        assertEquals(index1, tracker.getTrackedIndex(wrapper1));
        assertEquals(index2, tracker.getTrackedIndex(wrapper2));
    }

    @Test
    public void testTrackingReturnsStableIndexWhenRemovedAndAddedInSameOrder() {
        List<ConsumerIdentityWrapper> consumerIdentityWrappers =
                IntStream.range(0, 100).mapToObj(i -> mockConsumer()).map(ConsumerIdentityWrapper::new).toList();
        Map<ConsumerIdentityWrapper, Integer> trackedIndexes =
                consumerIdentityWrappers.stream().collect(Collectors.toMap(
                        wrapper -> wrapper, wrapper -> tracker.increaseConsumerRefCountAndReturnIndex(wrapper)));
        // stop tracking every other consumer
        for (int i = 0; i < consumerIdentityWrappers.size(); i++) {
            if (i % 2 == 0) {
                tracker.decreaseConsumerRefCount(consumerIdentityWrappers.get(i));
            }
        }
        // check that others are tracked
        for (int i = 0; i < consumerIdentityWrappers.size(); i++) {
            ConsumerIdentityWrapper wrapper = consumerIdentityWrappers.get(i);
            int trackedIndex = tracker.getTrackedIndex(wrapper);
            assertEquals(trackedIndex, i % 2 == 0 ? -1 : trackedIndexes.get(wrapper));
        }
        // check that new consumers are tracked with the same index
        for (int i = 0; i < consumerIdentityWrappers.size(); i++) {
            ConsumerIdentityWrapper wrapper = consumerIdentityWrappers.get(i);
            if (i % 2 == 0) {
                int trackedIndex = tracker.increaseConsumerRefCountAndReturnIndex(wrapper);
                assertEquals(trackedIndex, trackedIndexes.get(wrapper));
            }
        }
        // check that all consumers are tracked with the original indexes
        for (int i = 0; i < consumerIdentityWrappers.size(); i++) {
            ConsumerIdentityWrapper wrapper = consumerIdentityWrappers.get(i);
            int trackedIndex = tracker.getTrackedIndex(wrapper);
            assertEquals(trackedIndex, trackedIndexes.get(wrapper));
        }
    }

    @Test
    public void testTrackingMultipleTimes() {
        List<ConsumerIdentityWrapper> consumerIdentityWrappers =
                IntStream.range(0, 100).mapToObj(i -> mockConsumer()).map(ConsumerIdentityWrapper::new).toList();
        Map<ConsumerIdentityWrapper, Integer> trackedIndexes =
                consumerIdentityWrappers.stream().collect(Collectors.toMap(
                        wrapper -> wrapper, wrapper -> tracker.increaseConsumerRefCountAndReturnIndex(wrapper)));
        Map<ConsumerIdentityWrapper, Integer> trackedIndexes2 =
                consumerIdentityWrappers.stream().collect(Collectors.toMap(
                        wrapper -> wrapper, wrapper -> tracker.increaseConsumerRefCountAndReturnIndex(wrapper)));
        assertThat(tracker.getTrackedConsumerNamesCount()).isEqualTo(1);
        assertThat(trackedIndexes).containsExactlyInAnyOrderEntriesOf(trackedIndexes2);
        consumerIdentityWrappers.forEach(wrapper -> tracker.decreaseConsumerRefCount(wrapper));
        for (ConsumerIdentityWrapper wrapper : consumerIdentityWrappers) {
            int trackedIndex = tracker.getTrackedIndex(wrapper);
            assertEquals(trackedIndex, trackedIndexes.get(wrapper));
        }
        consumerIdentityWrappers.forEach(wrapper -> tracker.decreaseConsumerRefCount(wrapper));
        assertThat(tracker.getTrackedConsumersCount()).isEqualTo(0);
        assertThat(tracker.getTrackedConsumerNamesCount()).isEqualTo(0);
    }

    @Test
    public void testDecreaseConsumerRefCount() {
        Consumer consumer1 = mockConsumer();
        ConsumerIdentityWrapper wrapper1 = new ConsumerIdentityWrapper(consumer1);
        int index1 = tracker.increaseConsumerRefCountAndReturnIndex(wrapper1);
        assertNotEquals(index1, -1);
        tracker.decreaseConsumerRefCount(wrapper1);
        assertEquals(tracker.getTrackedIndex(wrapper1), -1);
    }

    @Test
    public void testGetTrackedIndex() {
        Consumer consumer1 = mockConsumer();
        Consumer consumer2 = mockConsumer();
        ConsumerIdentityWrapper wrapper1 = new ConsumerIdentityWrapper(consumer1);
        ConsumerIdentityWrapper wrapper2 = new ConsumerIdentityWrapper(consumer2);
        int index1 = tracker.increaseConsumerRefCountAndReturnIndex(wrapper1);
        int index2 = tracker.increaseConsumerRefCountAndReturnIndex(wrapper2);
        assertEquals(index1, tracker.getTrackedIndex(wrapper1));
        assertEquals(index2, tracker.getTrackedIndex(wrapper2));
    }

    @Test
    public void testTrackingMultipleNames() {
        List<ConsumerIdentityWrapper> consumerIdentityWrappers =
                IntStream.range(0, 100).mapToObj(i -> mockConsumer("consumer" + i)).map(ConsumerIdentityWrapper::new)
                        .toList();
        consumerIdentityWrappers.forEach(wrapper -> tracker.increaseConsumerRefCountAndReturnIndex(wrapper));
        assertThat(tracker.getTrackedConsumerNamesCount()).isEqualTo(100);
        assertThat(tracker.getTrackedConsumersCount()).isEqualTo(100);
        consumerIdentityWrappers.forEach(wrapper -> tracker.decreaseConsumerRefCount(wrapper));
        assertThat(tracker.getTrackedConsumersCount()).isEqualTo(0);
        assertThat(tracker.getTrackedConsumerNamesCount()).isEqualTo(0);
    }
}