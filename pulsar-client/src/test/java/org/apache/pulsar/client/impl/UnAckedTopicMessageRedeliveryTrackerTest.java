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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.testng.annotations.Test;

public class UnAckedTopicMessageRedeliveryTrackerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveTopicMessages() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        when(client.getCnxPool()).thenReturn(connectionPool);
        @Cleanup("stop")
        Timer timer = new HashedWheelTimer(
                new DefaultThreadFactory("pulsar-timer", Thread.currentThread().isDaemon()),
                1, TimeUnit.MILLISECONDS);
        when(client.timer()).thenReturn(timer);

        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        doNothing().when(consumer).onAckTimeoutSend(any());
        doNothing().when(consumer).redeliverUnacknowledgedMessages(any());

        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(1_000_000);
        conf.setTickDurationMillis(100_000);
        conf.setAckTimeoutRedeliveryBackoff(MultiplierRedeliveryBackoff.builder().build());

        UnAckedTopicMessageRedeliveryTracker tracker =
                new UnAckedTopicMessageRedeliveryTracker(client, consumer, conf);

        String ownerTopic = "persistent://public/default/my-topic-partition-0";
        TopicMessageIdImpl msgInPartition =
                new TopicMessageIdImpl(ownerTopic, new MessageIdImpl(1L, 0L, -1));
        TopicMessageIdImpl msgInAckTimeout =
                new TopicMessageIdImpl(ownerTopic, new MessageIdImpl(2L, 0L, -1));

        assertTrue(tracker.add(msgInPartition));
        tracker.ackTimeoutMessages.put(msgInAckTimeout, System.currentTimeMillis() + 1_000_000L);
        assertEquals(tracker.size(), 2);

        assertEquals(tracker.removeTopicMessages("persistent://public/default/my-topic-partition-0"), 2);
        assertTrue(tracker.isEmpty());

        tracker.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveTopicMessagesDoesNotMatchPrefixTopic() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        when(client.getCnxPool()).thenReturn(connectionPool);
        @Cleanup("stop")
        Timer timer = new HashedWheelTimer(
                new DefaultThreadFactory("pulsar-timer", Thread.currentThread().isDaemon()),
                1, TimeUnit.MILLISECONDS);
        when(client.timer()).thenReturn(timer);

        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        doNothing().when(consumer).onAckTimeoutSend(any());
        doNothing().when(consumer).redeliverUnacknowledgedMessages(any());

        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(1_000_000);
        conf.setTickDurationMillis(100_000);
        conf.setAckTimeoutRedeliveryBackoff(MultiplierRedeliveryBackoff.builder().build());

        UnAckedTopicMessageRedeliveryTracker tracker =
                new UnAckedTopicMessageRedeliveryTracker(client, consumer, conf);

        String ownerTopic = "persistent://public/default/my-topic-v2-partition-0";
        TopicMessageIdImpl msgInPartition =
                new TopicMessageIdImpl(ownerTopic, new MessageIdImpl(1L, 0L, -1));

        assertTrue(tracker.add(msgInPartition));
        assertEquals(tracker.size(), 1);

        assertEquals(tracker.removeTopicMessages("persistent://public/default/my-topic"), 0);
        assertEquals(tracker.size(), 1);

        tracker.close();
    }

}
