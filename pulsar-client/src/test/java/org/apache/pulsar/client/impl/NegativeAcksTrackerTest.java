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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class NegativeAcksTrackerTest {

    private static final RedeliveryBackoff ONE_HOUR_FOR_FIRST_REDELIVERY_ELSE_ONE_SECOND =
            redeliveryCount -> redeliveryCount == 1 ? TimeUnit.HOURS.toMillis(1) : TimeUnit.SECONDS.toMillis(1);

    @Test
    @SuppressWarnings("unchecked")
    public void testShorterBackoffAfterLongerBackoffIsRedeliveredOnTime() {
        @Cleanup("stop")
        Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-timer", true), 1, TimeUnit.MILLISECONDS);
        List<Set<MessageId>> redeliveries = new CopyOnWriteArrayList<>();
        ConsumerBase<byte[]> consumer = mockConsumer(timer);
        doAnswer(invocation -> {
            redeliveries.add(Set.copyOf((Set<MessageId>) invocation.getArgument(0)));
            return null;
        }).when(consumer).redeliverUnacknowledgedMessages(any(Set.class));

        @Cleanup
        NegativeAcksTracker tracker = new NegativeAcksTracker(consumer, newConf());

        tracker.add(mockMessage(new MessageIdImpl(1L, 1L, -1), 1)); // 1 hour backoff
        tracker.add(mockMessage(new MessageIdImpl(1L, 2L, -1), 0)); // 1 second backoff

        // The message with the short backoff must not wait for the timer of the message with the long backoff
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(redeliveries).as("redeliveries within 5 seconds").hasSize(1));
        assertThat(redeliveries.get(0)).containsExactly(new MessageIdImpl(1L, 2L, -2));
        assertThat(tracker.getNackedMessagesCount()).as("messages still waiting for their backoff").isEqualTo(1);
    }

    @Test
    public void testEarlierNackReplacesPendingTimeout() {
        Timer timer = mock(Timer.class);
        Timeout longTimeout = mock(Timeout.class);
        Timeout shortTimeout = mock(Timeout.class);
        when(timer.newTimeout(any(TimerTask.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(longTimeout, shortTimeout);

        @Cleanup
        NegativeAcksTracker tracker = new NegativeAcksTracker(mockConsumer(timer), newConf());

        tracker.add(mockMessage(new MessageIdImpl(1L, 1L, -1), 1)); // 1 hour backoff
        verify(timer, times(1)).newTimeout(any(TimerTask.class), eq(TimeUnit.HOURS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));
        verify(longTimeout, never()).cancel();

        tracker.add(mockMessage(new MessageIdImpl(1L, 2L, -1), 0)); // 1 second backoff
        verify(timer, times(1)).newTimeout(any(TimerTask.class), eq(TimeUnit.SECONDS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));
        verify(longTimeout, times(1)).cancel();
        verify(shortTimeout, never()).cancel();
    }

    @Test
    public void testSameOrLaterNackKeepsPendingTimeout() {
        Timer timer = mock(Timer.class);
        Timeout timeout = mock(Timeout.class);
        when(timer.newTimeout(any(TimerTask.class), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(timeout);

        @Cleanup
        NegativeAcksTracker tracker = new NegativeAcksTracker(mockConsumer(timer), newConf());

        tracker.add(mockMessage(new MessageIdImpl(1L, 1L, -1), 0)); // 1 second backoff
        tracker.add(mockMessage(new MessageIdImpl(1L, 2L, -1), 0)); // 1 second backoff, due slightly later
        tracker.add(mockMessage(new MessageIdImpl(1L, 3L, -1), 1)); // 1 hour backoff

        // Nacks that are not due before the pending timeout share it
        verify(timer, times(1)).newTimeout(any(TimerTask.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        verify(timeout, never()).cancel();
        assertThat(tracker.getNackedMessagesCount()).isEqualTo(3);
    }

    @Test
    public void testReplacedTimeoutDoesNotRedeliver() throws Exception {
        Timer timer = mock(Timer.class);
        Timeout longTimeout = mock(Timeout.class);
        Timeout shortTimeout = mock(Timeout.class);
        when(timer.newTimeout(any(TimerTask.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(longTimeout, shortTimeout);
        ConsumerBase<byte[]> consumer = mockConsumer(timer);

        @Cleanup
        NegativeAcksTracker tracker = new NegativeAcksTracker(consumer, newConf());

        tracker.add(mockMessage(new MessageIdImpl(1L, 1L, -1), 1)); // 1 hour backoff
        tracker.add(mockMessage(new MessageIdImpl(1L, 2L, -1), 0)); // 1 second backoff
        ArgumentCaptor<TimerTask> tasks = ArgumentCaptor.forClass(TimerTask.class);
        verify(timer, times(2)).newTimeout(tasks.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));

        // A task that was already running when its timeout got replaced has to leave the nacked messages alone
        tasks.getAllValues().get(0).run(longTimeout);

        verify(consumer, never()).redeliverUnacknowledgedMessages(any(Set.class));
        assertThat(tracker.getNackedMessagesCount()).isEqualTo(2);
    }

    private static ConsumerConfigurationData<?> newConf() {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setNegativeAckRedeliveryBackoff(ONE_HOUR_FOR_FIRST_REDELIVERY_ELSE_ONE_SECOND);
        return conf;
    }

    @SuppressWarnings("unchecked")
    private static ConsumerBase<byte[]> mockConsumer(Timer timer) {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.timer()).thenReturn(timer);
        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        when(consumer.getClient()).thenReturn(client);
        consumer.unAckedChunkedMessageIdSequenceMap = new ConcurrentHashMap<>();
        return consumer;
    }

    private static Message<?> mockMessage(MessageId messageId, int redeliveryCount) {
        Message<?> message = mock(Message.class);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.getRedeliveryCount()).thenReturn(redeliveryCount);
        return message;
    }
}
