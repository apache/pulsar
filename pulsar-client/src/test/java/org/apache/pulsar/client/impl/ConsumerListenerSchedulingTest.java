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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.testng.annotations.Test;

// Control arrival, drain and timer execution to verify scheduling order deterministically.
// MessageListenerExecutorTest additionally covers rejection recovery with a real broker and bounded executor.
public class ConsumerListenerSchedulingTest {
    @Test
    public void consecutiveRejectionsDoNotStrandCoalescedMessages() throws Exception {
        Fixture fixture = new Fixture(2);
        Message<byte[]> first = newMessage();
        Message<byte[]> second = newMessage();
        Message<byte[]> third = newMessage();
        fixture.addAndTrigger(first);
        fixture.addAndTrigger(second);
        fixture.addAndTrigger(third);

        assertEquals(fixture.tasks.size(), 1);
        fixture.runRemaining();
        assertEquals(fixture.attempted, List.of(first));
        assertTrue(fixture.accepted.isEmpty());
        assertEquals(new ArrayList<>(fixture.consumer.incomingMessages), List.of(second, third));

        fixture.fireRetry();
        fixture.runRemaining();
        assertEquals(fixture.attempted, List.of(first, first));
        assertTrue(fixture.accepted.isEmpty());
        assertEquals(new ArrayList<>(fixture.consumer.incomingMessages), List.of(second, third));

        fixture.fireRetry();
        fixture.runRemaining();
        assertEquals(fixture.attempted, List.of(first, first, first, second, third));
        assertEquals(fixture.accepted, List.of(first, second, third));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
        assertTrue(fixture.retries.isEmpty());
    }

    @Test
    public void rejectingEveryMessageFinishesAndAllowsLaterNotifications() throws Exception {
        Fixture fixture = new Fixture(Integer.MAX_VALUE);
        Message<byte[]> first = newMessage();
        Message<byte[]> second = newMessage();
        Message<byte[]> third = newMessage();
        fixture.addAndTrigger(first);
        fixture.runRemaining();
        assertEquals(fixture.attempted, List.of(first));

        // New notifications must finish without spinning or bypassing the rejected head message.
        fixture.addAndTrigger(second);
        fixture.addAndTrigger(third);
        fixture.runRemaining();
        assertEquals(fixture.attempted, List.of(first));
        assertTrue(fixture.accepted.isEmpty());
        assertEquals(fixture.retries.size(), 1);

        fixture.fireRetry();
        fixture.runRemaining();
        assertEquals(fixture.attempted, List.of(first, first));
        assertTrue(fixture.accepted.isEmpty());
        assertEquals(new ArrayList<>(fixture.consumer.incomingMessages), List.of(second, third));
        assertEquals(fixture.retries.size(), 1);

        // Recover without a new arrival; the timer must restart delivery of all retained messages.
        fixture.failedSubmissions = 0;
        fixture.fireRetry();
        fixture.runRemaining();
        assertEquals(fixture.accepted, List.of(first, second, third));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
        assertTrue(fixture.retries.isEmpty());

        Message<byte[]> next = newMessage();
        fixture.addAndTrigger(next);
        fixture.runRemaining();
        assertEquals(fixture.accepted, List.of(first, second, third, next));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
        assertTrue(fixture.retries.isEmpty());
    }

    @Test
    public void rejectionRetryRunsAfterPendingArrivals() throws Exception {
        Fixture fixture = new Fixture(2);
        Message<byte[]> first = newMessage();
        Message<byte[]> second = newMessage();
        Message<byte[]> third = newMessage();
        for (Message<byte[]> message : List.of(first, second, third)) {
            fixture.tasks.add(() -> fixture.consumer.incomingMessages.add(message));
            fixture.consumer.tryTriggerListener();
        }

        fixture.runNext(); // First arrival.
        fixture.runNext(); // Drain rejects the first message and schedules a timer.
        assertEquals(fixture.attempted, List.of(first));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
        fixture.fireRetry(); // Enqueue the retry behind the pending arrivals and coalesced drain.
        assertEquals(fixture.attempted, List.of(first));
        assertEquals(fixture.tasks.size(), 4);
        fixture.runNext(); // Second arrival, ahead of the retry.
        fixture.runNext(); // Third arrival, ahead of the retry.
        assertEquals(new ArrayList<>(fixture.consumer.incomingMessages), List.of(second, third));
        fixture.runNext(); // Coalesced drain must not bypass the rejected message.
        assertEquals(fixture.attempted, List.of(first));
        fixture.runNext(); // Retry rejects the first message again.
        assertEquals(fixture.attempted, List.of(first, first));
        assertTrue(fixture.tasks.isEmpty());

        fixture.fireRetry();
        fixture.runRemaining();
        assertEquals(fixture.attempted, List.of(first, first, first, second, third));
        assertEquals(fixture.accepted, List.of(first, second, third));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
        assertTrue(fixture.retries.isEmpty());
    }

    @Test
    public void consecutiveSubmissionFailuresDoNotStrandCoalescedMessages() {
        Fixture fixture = new Fixture(2, false);
        Message<byte[]> first = newMessage();
        Message<byte[]> second = newMessage();
        Message<byte[]> third = newMessage();
        fixture.addAndTrigger(first);
        fixture.addAndTrigger(second);
        fixture.addAndTrigger(third);

        assertEquals(fixture.tasks.size(), 1);
        expectThrows(IllegalStateException.class, fixture::runNext);
        expectThrows(IllegalStateException.class, fixture::runNext);
        fixture.runRemaining();

        assertEquals(fixture.attempted, List.of(first, second, third));
        assertEquals(fixture.accepted, List.of(third));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
    }

    @Test
    public void failingEverySubmissionFinishesAndAllowsLaterNotifications() {
        Fixture fixture = new Fixture(3, false);
        for (int i = 0; i < 3; i++) {
            fixture.addAndTrigger(newMessage());
        }
        for (int i = 0; i < 3; i++) {
            expectThrows(IllegalStateException.class, fixture::runNext);
        }
        fixture.runRemaining();
        assertEquals(fixture.attempted.size(), 3);
        assertTrue(fixture.accepted.isEmpty());
        assertTrue(fixture.consumer.incomingMessages.isEmpty());

        Message<byte[]> next = newMessage();
        fixture.addAndTrigger(next);
        fixture.runRemaining();
        assertEquals(fixture.accepted, List.of(next));
    }

    @Test
    public void failedSubmissionFollowUpRunsAfterPendingArrivals() {
        Fixture fixture = new Fixture(2, false);
        Message<byte[]> first = newMessage();
        Message<byte[]> second = newMessage();
        Message<byte[]> third = newMessage();
        for (Message<byte[]> message : List.of(first, second, third)) {
            fixture.tasks.add(() -> fixture.consumer.incomingMessages.add(message));
            fixture.consumer.tryTriggerListener();
        }

        fixture.runNext(); // First arrival.
        expectThrows(IllegalStateException.class, fixture::runNext);
        assertEquals(fixture.attempted, List.of(first));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
        fixture.runNext(); // Second arrival, ahead of the retry.
        fixture.runNext(); // Third arrival, ahead of the retry.
        assertEquals(fixture.consumer.incomingMessages.size(), 2);
        expectThrows(IllegalStateException.class, fixture::runNext);
        fixture.runRemaining();

        assertEquals(fixture.attempted, List.of(first, second, third));
        assertEquals(fixture.accepted, List.of(third));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> newMessage() {
        return mock(Message.class);
    }

    private static final class Fixture {
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private final Queue<PendingTimeout> retries = new ArrayDeque<>();
        private final List<Message<?>> attempted = new ArrayList<>();
        private final List<Message<?>> accepted = new ArrayList<>();
        private final ConsumerImpl<byte[]> consumer;
        private int failedSubmissions;

        private Fixture(int failedSubmissions) {
            this(failedSubmissions, true);
        }

        private Fixture(int failedSubmissions, boolean reject) {
            this.failedSubmissions = failedSubmissions;
            ExecutorService executor = mock(ExecutorService.class);
            doAnswer(invocation -> {
                tasks.add(invocation.getArgument(0));
                return null;
            }).when(executor).execute(any(Runnable.class));
            ExecutorProvider executorProvider = mock(ExecutorProvider.class);
            when(executorProvider.getExecutor()).thenReturn(executor);
            PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, executor);
            client.getConfiguration().setStatsIntervalSeconds(0);
            when(client.timer().newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(invocation -> {
                        Timeout timeout = mock(Timeout.class);
                        long delay = invocation.getArgument(1);
                        TimeUnit unit = invocation.getArgument(2);
                        retries.add(new PendingTimeout(invocation.getArgument(0), timeout, unit.toMillis(delay)));
                        return timeout;
                    });
            ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
            conf.setSubscriptionName("test-sub");
            conf.setMessageListener((ignored, message) -> { });
            conf.setMessageListenerExecutor((message, runnable) -> {
                attempted.add(message);
                if (attempted.size() <= this.failedSubmissions) {
                    if (reject) {
                        throw new RejectedExecutionException("listener submission rejected");
                    }
                    throw new IllegalStateException("listener submission failed");
                }
                accepted.add(message);
            });
            consumer = new ConsumerImpl<>(client, "non-persistent://tenant/ns/listener-scheduling", conf,
                    executorProvider, -1, false, false, new CompletableFuture<>(), null, 0, Schema.BYTES,
                    null, true) {
                @Override
                protected Message<byte[]> internalReceive(long timeout, TimeUnit unit) {
                    // Keep the real listener drain and scheduler, without message decoding or broker state.
                    return incomingMessages.poll();
                }
            };
            consumer.setState(HandlerState.State.Ready);
            assertTrue(tasks.isEmpty());
        }

        private void addAndTrigger(Message<byte[]> message) {
            consumer.incomingMessages.add(message);
            consumer.tryTriggerListener();
        }

        private void runNext() {
            tasks.remove().run();
        }

        private void fireRetry() throws Exception {
            assertEquals(retries.size(), 1, "Only one retry should be pending while listener submission is blocked");
            PendingTimeout retry = retries.remove();
            assertTrue(retry.delayMillis() > 0, "Rejected submissions must be retried with a delay");
            retry.task().run(retry.timeout());
        }

        private void runRemaining() {
            int runs = 0;
            while (!tasks.isEmpty()) {
                assertTrue(++runs <= 10, "Listener drain must not retry indefinitely");
                runNext();
            }
        }
    }

    private record PendingTimeout(TimerTask task, Timeout timeout, long delayMillis) { }
}
