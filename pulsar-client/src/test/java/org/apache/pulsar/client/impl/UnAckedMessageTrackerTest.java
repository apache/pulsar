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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class UnAckedMessageTrackerTest  {

    @Test
    public void testAddAndRemove() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-timer", Thread.currentThread().isDaemon()),
                1, TimeUnit.MILLISECONDS);
        when(client.timer()).thenReturn(timer);

        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        doNothing().when(consumer).onAckTimeoutSend(any());
        doNothing().when(consumer).redeliverUnacknowledgedMessages(any());
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(1000000);
        conf.setTickDurationMillis(100000);
        UnAckedMessageTracker tracker = new UnAckedMessageTracker(client, consumer, conf);
        tracker.close();

        assertTrue(tracker.isEmpty());
        assertEquals(tracker.size(), 0);

        MessageIdImpl mid = new MessageIdImpl(1L, 1L, -1);
        assertTrue(tracker.add(mid));
        assertFalse(tracker.add(mid));
        assertEquals(tracker.size(), 1);

        HashSet<MessageId> headPartition = tracker.timePartitions.removeFirst();
        headPartition.clear();
        tracker.timePartitions.addLast(headPartition);

        assertFalse(tracker.add(mid));
        assertEquals(tracker.size(), 1);

        assertTrue(tracker.remove(mid));
        assertTrue(tracker.isEmpty());
        assertEquals(tracker.size(), 0);

        timer.stop();
    }

    @Test
    public void testTrackChunkedMessageId() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-timer", Thread.currentThread().isDaemon()),
                1, TimeUnit.MILLISECONDS);
        when(client.timer()).thenReturn(timer);

        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        doNothing().when(consumer).onAckTimeoutSend(any());
        doNothing().when(consumer).redeliverUnacknowledgedMessages(any());
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(1000);
        conf.setTickDurationMillis(1000);
        UnAckedMessageTracker tracker = new UnAckedMessageTracker(client, consumer, conf);

        assertTrue(tracker.isEmpty());
        assertEquals(tracker.size(), 0);

        // Build chunked message ID
        MessageIdImpl[] chunkMsgIds = new MessageIdImpl[5];
        for (int i = 0; i < 5; i++) {
            chunkMsgIds[i] = new MessageIdImpl(1L, i, -1);
        }
        ChunkMessageIdImpl chunkedMessageId =
                new ChunkMessageIdImpl(chunkMsgIds[0], chunkMsgIds[chunkMsgIds.length - 1]);

        consumer.unAckedChunkedMessageIdSequenceMap =
                ConcurrentOpenHashMap.<MessageIdAdv, MessageIdImpl[]>newBuilder().build();
        consumer.unAckedChunkedMessageIdSequenceMap.put(chunkedMessageId, chunkMsgIds);

        // Redeliver chunked message
        tracker.add(chunkedMessageId);

        Awaitility.await()
                .pollInterval(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertEquals(tracker.size(), 0));

        // Assert that all chunk message ID are removed from unAckedChunkedMessageIdSequenceMap
        assertEquals(consumer.unAckedChunkedMessageIdSequenceMap.size(), 0);

        timer.stop();
    }

}
