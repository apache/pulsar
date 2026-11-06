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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UnAckedMessageTrackerTest  {

    @Test
    @SuppressWarnings("unchecked")
    public void testAddAndRemove() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        when(client.getCnxPool()).thenReturn(connectionPool);
        @Cleanup("stop")
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
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTrackChunkedMessageId() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        when(client.getCnxPool()).thenReturn(connectionPool);
        @Cleanup("stop")
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

        consumer.unAckedChunkedMessageIdSequenceMap = new ConcurrentHashMap<>();
        consumer.unAckedChunkedMessageIdSequenceMap.put(chunkedMessageId, chunkMsgIds);

        // Redeliver chunked message
        tracker.add(chunkedMessageId);

        Awaitility.await()
                .pollInterval(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertEquals(tracker.size(), 0));

        // Assert that all chunk message ID are removed from unAckedChunkedMessageIdSequenceMap
        assertEquals(consumer.unAckedChunkedMessageIdSequenceMap.size(), 0);
    }

    @DataProvider(name = "subscriptionTypes")
    public Object[][] subscriptionTypes() {
        return new Object[][] {
                {SubscriptionType.Exclusive, true},
                {SubscriptionType.Failover, true},
                {SubscriptionType.Shared, false},
                {SubscriptionType.Key_Shared, false},
        };
    }

    /**
     * An Exclusive or Failover consumer cannot redeliver single messages, so when some of its messages time out it
     * redelivers all of its unacked messages and clears the tracker. onAckTimeoutSend must then report all of them,
     * also the ones received in a later tick, or they are redelivered without ever being reported.
     */
    @Test(dataProvider = "subscriptionTypes")
    @SuppressWarnings("unchecked")
    public void testAckTimeoutReportsAllRedeliveredMessages(SubscriptionType subscriptionType,
                                                            boolean redeliversAll) throws Exception {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        // Run the tick task by hand
        Timer timer = mock(Timer.class);
        Timeout timeout = mock(Timeout.class);
        ArgumentCaptor<TimerTask> tickTask = ArgumentCaptor.forClass(TimerTask.class);
        when(timer.newTimeout(tickTask.capture(), anyLong(), any())).thenReturn(timeout);
        when(client.timer()).thenReturn(timer);

        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        List<Set<MessageId>> ackTimeoutSends = new ArrayList<>();
        // Copy the set, since the tracker reuses it
        doAnswer(invocation -> ackTimeoutSends.add(new HashSet<>(invocation.<Set<MessageId>>getArgument(0))))
                .when(consumer).onAckTimeoutSend(any());
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionType(subscriptionType);
        conf.setAckTimeoutMillis(300);
        conf.setTickDurationMillis(100);
        // ceil(300 / 100) + 1 time partitions: a message times out at the 4th tick after it is tracked
        UnAckedMessageTracker tracker = new UnAckedMessageTracker(client, consumer, conf);

        MessageIdImpl first = new MessageIdImpl(1L, 1L, -1);
        MessageIdImpl second = new MessageIdImpl(1L, 2L, -1);
        tracker.add(first);
        tickTask.getValue().run(timeout);
        tracker.add(second);
        tickTask.getValue().run(timeout);
        tickTask.getValue().run(timeout);
        assertTrue(ackTimeoutSends.isEmpty());

        // first times out
        tickTask.getValue().run(timeout);
        assertEquals(ackTimeoutSends, List.of(redeliversAll ? Set.of(first, second) : Set.of(first)));
        assertEquals(tracker.size(), redeliversAll ? 0 : 1);
    }

    /**
     * When an Exclusive or Failover consumer redelivers a chunked message along with the timed-out ones, before it
     * timed out itself, the tracker must keep the mapping to its chunks: the application can still acknowledge the
     * message it received, and the acknowledgment must cover all of its chunks, not only the last one.
     */
    @Test(dataProvider = "subscriptionTypes")
    @SuppressWarnings("unchecked")
    public void testAckTimeoutKeepsChunkMappingsOfMessagesNotTimedOut(SubscriptionType subscriptionType,
                                                                       boolean redeliversAll) throws Exception {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        // Run the tick task by hand
        Timer timer = mock(Timer.class);
        Timeout timeout = mock(Timeout.class);
        ArgumentCaptor<TimerTask> tickTask = ArgumentCaptor.forClass(TimerTask.class);
        when(timer.newTimeout(tickTask.capture(), anyLong(), any())).thenReturn(timeout);
        when(client.timer()).thenReturn(timer);

        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        consumer.unAckedChunkedMessageIdSequenceMap = new ConcurrentHashMap<>();
        List<Set<MessageId>> ackTimeoutSends = new ArrayList<>();
        // Copy the set, since the tracker reuses it
        doAnswer(invocation -> ackTimeoutSends.add(new HashSet<>(invocation.<Set<MessageId>>getArgument(0))))
                .when(consumer).onAckTimeoutSend(any());
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionType(subscriptionType);
        conf.setAckTimeoutMillis(300);
        conf.setTickDurationMillis(100);
        UnAckedMessageTracker tracker = new UnAckedMessageTracker(client, consumer, conf);

        MessageIdImpl timedOut = new MessageIdImpl(1L, 1L, -1);
        MessageIdImpl[] chunks = {new MessageIdImpl(1L, 2L, -1), new MessageIdImpl(1L, 3L, -1)};
        ChunkMessageIdImpl chunked = new ChunkMessageIdImpl(chunks[0], chunks[1]);
        consumer.unAckedChunkedMessageIdSequenceMap.put(chunked, chunks);
        tracker.add(timedOut);
        tickTask.getValue().run(timeout);
        tracker.add(chunked);
        tickTask.getValue().run(timeout);
        tickTask.getValue().run(timeout);

        // timedOut times out
        tickTask.getValue().run(timeout);
        assertEquals(ackTimeoutSends,
                List.of(redeliversAll ? Set.of(timedOut, chunks[0], chunks[1]) : Set.of(timedOut)));
        // PersistentAcknowledgmentsGroupingTracker acknowledges all the chunks of a message only through this mapping
        assertEquals(consumer.unAckedChunkedMessageIdSequenceMap.get(chunked), chunks);
    }

}
