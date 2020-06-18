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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.testng.annotations.Test;

/**
 * Unit test of {@link RoundRobinPartitionMessageRouterImpl}.
 */
public class RoundRobinPartitionMessageRouterImplTest {

    @Test
    public void testChoosePartitionWithoutKey() {
        Message<?> msg = mock(Message.class);
        when(msg.getKey()).thenReturn(null);

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(
                HashingScheme.JavaStringHash, 0, false, 0);
        for (int i = 0; i < 10; i++) {
            assertEquals(i % 5, router.choosePartition(msg, new TopicMetadataImpl(5)));
        }
    }

    @Test
    public void testChoosePartitionWithoutKeyWithBatching() {
        Message<?> msg = mock(Message.class);
        when(msg.getKey()).thenReturn(null);

        // Fake clock, simulate 1 millisecond passes for each invocation
        Clock clock = new Clock() {
            private long current = 0;

            @Override
            public Clock withZone(ZoneId zone) {
                return null;
            }

            @Override
            public long millis() {
                return current++;
            }

            @Override
            public Instant instant() {
                return Instant.ofEpochMilli(millis());
            }

            @Override
            public ZoneId getZone() {
                return ZoneId.systemDefault();
            }
        };

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(
                HashingScheme.JavaStringHash, 0, true, 5, clock);

        // Since the batching time is 5millis, first 5 messages will go on partition 0 and next five would go on
        // partition 1
        for (int i = 0; i < 5; i++) {
            assertEquals(0, router.choosePartition(msg, new TopicMetadataImpl(5)));
        }

        for (int i = 5; i < 10; i++) {
            assertEquals(1, router.choosePartition(msg, new TopicMetadataImpl(5)));
        }
    }

    @Test
    public void testChoosePartitionWithNegativeTime() {
        Message<?> msg = mock(Message.class);
        when(msg.getKey()).thenReturn(null);

        // Fake clock, simulate timestamp that resolves into a negative Integer value
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn((long) Integer.MAX_VALUE);

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(
                HashingScheme.JavaStringHash, 3, true, 5, clock);

        int idx = router.choosePartition(msg, new TopicMetadataImpl(5));
        assertTrue(idx >= 0);
        assertTrue(idx < 5);
    }

    @Test
    public void testChoosePartitionWithKey() {
        String key1 = "key1";
        String key2 = "key2";
        Message<?> msg1 = mock(Message.class);
        when(msg1.hasKey()).thenReturn(true);
        when(msg1.getKey()).thenReturn(key1);
        Message<?> msg2 = mock(Message.class);
        when(msg2.hasKey()).thenReturn(true);
        when(msg2.getKey()).thenReturn(key2);

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(
                HashingScheme.JavaStringHash, 0, false, 0);
        TopicMetadataImpl metadata = new TopicMetadataImpl(100);

        assertEquals(key1.hashCode() % 100, router.choosePartition(msg1, metadata));
        assertEquals(key2.hashCode() % 100, router.choosePartition(msg2, metadata));
    }

    @Test
    public void testBatchingAwareness() throws Exception {
        Message<?> msg = mock(Message.class);
        when(msg.getKey()).thenReturn(null);

        Clock clock = mock(Clock.class);

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(
                HashingScheme.JavaStringHash, 0, true, 10, clock);
        TopicMetadataImpl metadata = new TopicMetadataImpl(100);

        // time at `12345*` milliseconds
        for (int i = 0; i < 10; i++) {
            when(clock.millis()).thenReturn(123450L + i);

            assertEquals(45, router.choosePartition(msg, metadata));
        }

        // time at `12346*` milliseconds
        for (int i = 0; i < 10; i++) {
            when(clock.millis()).thenReturn(123460L + i);

            assertEquals(46, router.choosePartition(msg, metadata));
        }
    }
}
