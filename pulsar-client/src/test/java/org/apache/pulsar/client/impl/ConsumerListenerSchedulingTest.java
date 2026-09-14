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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
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

public class ConsumerListenerSchedulingTest {
    @Test
    public void consecutiveRejectionsDoNotStrandCoalescedMessages() {
        Fixture fixture = new Fixture(2);
        Message<byte[]> first = newMessage();
        Message<byte[]> second = newMessage();
        Message<byte[]> third = newMessage();
        fixture.addAndTrigger(first);
        fixture.addAndTrigger(second);
        fixture.addAndTrigger(third);

        assertEquals(fixture.tasks.size(), 1);
        expectThrows(RejectedExecutionException.class, fixture::runNext);
        expectThrows(RejectedExecutionException.class, fixture::runNext);
        fixture.runRemaining();

        assertEquals(fixture.attempted, List.of(first, second, third));
        assertEquals(fixture.accepted, List.of(third));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
    }

    @Test
    public void rejectingEveryMessageFinishesAndAllowsLaterNotifications() {
        Fixture fixture = new Fixture(3);
        for (int i = 0; i < 3; i++) {
            fixture.addAndTrigger(newMessage());
        }
        for (int i = 0; i < 3; i++) {
            expectThrows(RejectedExecutionException.class, fixture::runNext);
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
    public void rejectionRetryRunsAfterPendingArrivals() {
        Fixture fixture = new Fixture(2);
        Message<byte[]> first = newMessage();
        Message<byte[]> second = newMessage();
        Message<byte[]> third = newMessage();
        for (Message<byte[]> message : List.of(first, second, third)) {
            fixture.tasks.add(() -> fixture.consumer.incomingMessages.add(message));
            fixture.consumer.tryTriggerListener();
        }

        fixture.runNext(); // First arrival.
        expectThrows(RejectedExecutionException.class, fixture::runNext);
        assertEquals(fixture.attempted, List.of(first));
        assertTrue(fixture.consumer.incomingMessages.isEmpty());
        fixture.runNext(); // Second arrival, ahead of the retry.
        fixture.runNext(); // Third arrival, ahead of the retry.
        assertEquals(fixture.consumer.incomingMessages.size(), 2);
        expectThrows(RejectedExecutionException.class, fixture::runNext);
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
        private final List<Message<?>> attempted = new ArrayList<>();
        private final List<Message<?>> accepted = new ArrayList<>();
        private final ConsumerImpl<byte[]> consumer;

        private Fixture(int rejectedSubmissions) {
            ExecutorService executor = mock(ExecutorService.class);
            doAnswer(invocation -> {
                tasks.add(invocation.getArgument(0));
                return null;
            }).when(executor).execute(any(Runnable.class));
            ExecutorProvider executorProvider = mock(ExecutorProvider.class);
            when(executorProvider.getExecutor()).thenReturn(executor);
            PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, executor);
            client.getConfiguration().setStatsIntervalSeconds(0);
            ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
            conf.setSubscriptionName("test-sub");
            conf.setMessageListener((ignored, message) -> { });
            conf.setMessageListenerExecutor((message, runnable) -> {
                attempted.add(message);
                if (attempted.size() <= rejectedSubmissions) {
                    throw new RejectedExecutionException("listener submission rejected");
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

        private void runRemaining() {
            int runs = 0;
            while (!tasks.isEmpty()) {
                assertTrue(++runs <= 10, "Listener drain must not retry indefinitely");
                runNext();
            }
        }
    }
}
