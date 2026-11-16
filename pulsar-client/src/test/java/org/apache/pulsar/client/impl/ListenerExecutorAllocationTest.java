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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ListenerExecutorAllocationTest {
    private static final String TOPIC = "persistent://public/default/listener-executors";
    private final List<ConsumerBase<byte[]>> consumers = new ArrayList<>();
    private ExecutorProvider external;
    private ExecutorService internal;
    private PulsarClientImpl client;

    private void createClient(int listenerThreads) {
        external = spy(new ExecutorProvider(listenerThreads, "listener-allocation"));
        internal = Executors.newSingleThreadExecutor(new DefaultThreadFactory("listener-allocation-internal"));
        client = ClientTestFixtures.createPulsarClientMock(external, internal);
        client.getConfiguration().setStatsIntervalSeconds(0);
        // Exercise real consumer construction and dispatch without opening a broker connection.
        when(client.getConnection(anyString(), anyInt())).thenReturn(new CompletableFuture<>());
    }

    private ConsumerConfigurationData<byte[]> configuration() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.getTopicNames().add(TOPIC);
        conf.setSubscriptionName("shared-subscription");
        conf.setSubscriptionType(SubscriptionType.Shared);
        conf.setAutoUpdatePartitions(false);
        conf.setAcknowledgementsGroupTimeMicros(0);
        conf.setMessageListener((consumer, message) -> { });
        return conf;
    }

    private ConsumerImpl<byte[]> createConsumer(ConsumerConfigurationData<byte[]> conf) {
        ConsumerImpl<byte[]> consumer = ConsumerImpl.newConsumerImpl(client, TOPIC, conf, external,
                -1, false, new CompletableFuture<>(), null, Schema.BYTES, null, true);
        consumers.add(consumer);
        return consumer;
    }

    private MultiTopicsConsumerImpl<byte[]> createPartitionedConsumer(int partitions) {
        MultiTopicsConsumerImpl<byte[]> consumer = MultiTopicsConsumerImpl.createPartitionedConsumer(
                client, configuration(), external, new CompletableFuture<>(), partitions, Schema.BYTES, null);
        consumers.add(consumer);
        assertThat(consumer.getConsumers()).hasSize(partitions);
        return consumer;
    }

    @SuppressWarnings("unchecked")
    private static MessageImpl<byte[]> newMessage() {
        MessageImpl<byte[]> message = mock(MessageImpl.class);
        when(message.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));
        return message;
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        try {
            List<CompletableFuture<Void>> closes = new ArrayList<>();
            for (ConsumerBase<byte[]> consumer : consumers) {
                closes.add(consumer.closeAsync());
            }
            FutureUtil.waitForAll(closes).get(5, TimeUnit.SECONDS);
        } finally {
            consumers.clear();
            if (internal != null) {
                internal.shutdownNow();
            }
            if (external != null) {
                external.shutdownNow();
            }
        }
    }

    @DataProvider
    public static Object[][] consumerCountsAndPartitions() {
        return new Object[][] {{2, 1}, {3, 1}, {4, 1}, {5, 1}, {9, 1}, {10, 1}, {10, 2}, {10, 3}, {10, 0}};
    }

    @Test(dataProvider = "consumerCountsAndPartitions", timeOut = 30000)
    public void testActiveConsumersUseAllListenerThreads(int count, int partitions) throws Exception {
        createClient(count);
        List<Message<byte[]>> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MessageImpl<byte[]> message = newMessage();
            if (partitions == 0) {
                createConsumer(configuration());
                messages.add(message);
            } else {
                MultiTopicsConsumerImpl<byte[]> consumer = createPartitionedConsumer(partitions);
                ConsumerImpl<byte[]> partition = consumer.getConsumers().stream()
                        .filter(c -> c.getPartitionIndex() == 0).findFirst().orElseThrow();
                messages.add(new TopicMessageImpl<>(partition.getTopic(), message, partition));
            }
        }

        verify(external, never()).getExecutor();
        assertConcurrentDispatch(consumers, messages);
    }

    @DataProvider
    public static Object[][] consumerCountsWithMultipleActivePartitions() {
        return new Object[][] {{2, 2}, {2, 3}, {3, 2}, {3, 3}};
    }

    @Test(dataProvider = "consumerCountsWithMultipleActivePartitions", timeOut = 30000)
    public void testActivePartitionsUseSeparateListenerExecutors(int count, int partitions) throws Exception {
        // Size the pool for all active partition consumers, including odd and even pool sizes.
        createClient(count * partitions);
        List<ConsumerBase<byte[]>> dispatchConsumers = new ArrayList<>();
        List<Message<byte[]>> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MultiTopicsConsumerImpl<byte[]> consumer = createPartitionedConsumer(partitions);
            for (ConsumerImpl<byte[]> partition : consumer.getConsumers()) {
                dispatchConsumers.add(consumer);
                messages.add(new TopicMessageImpl<>(partition.getTopic(), newMessage(), partition));
            }
        }

        verify(external, never()).getExecutor();
        assertConcurrentDispatch(dispatchConsumers, messages);
    }

    private void assertConcurrentDispatch(List<ConsumerBase<byte[]>> dispatchConsumers,
                                          List<Message<byte[]>> messages) throws Exception {
        int count = messages.size();
        assertThat(dispatchConsumers).hasSize(count);
        CountDownLatch entered = new CountDownLatch(count);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(count);
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        Map<Integer, Thread> assignedThreads = new ConcurrentHashMap<>();
        try {
            for (int i = 0; i < count; i++) {
                int messageIndex = i;
                ConsumerBase<byte[]> consumer = dispatchConsumers.get(i);
                consumer.messageListenerExecutor.execute(messages.get(i), () -> {
                    threads.add(Thread.currentThread());
                    assignedThreads.put(messageIndex, Thread.currentThread());
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        completed.countDown();
                    }
                });
            }
            assertThat(entered.await(5, TimeUnit.SECONDS))
                    .as("All %s listener tasks should start concurrently; only %s threads are active", count,
                            threads.size())
                    .isTrue();
            assertThat(threads).hasSize(count);
        } finally {
            release.countDown();
        }
        assertThat(completed.await(5, TimeUnit.SECONDS)).as("Listener tasks completed").isTrue();
        // A second dispatch from each partition consumer must stay on its original executor.
        for (int i = 0; i < count; i++) {
            CompletableFuture<Thread> repeated = new CompletableFuture<>();
            dispatchConsumers.get(i).messageListenerExecutor.execute(messages.get(i),
                    () -> repeated.complete(Thread.currentThread()));
            assertThat(repeated.get(5, TimeUnit.SECONDS)).isSameAs(assignedThreads.get(i));
        }
        verify(external, times(count)).getExecutor();
    }

    @Test(timeOut = 30000)
    public void testConcurrentFirstDispatchRemainsPinned() throws Exception {
        createClient(10);
        ConsumerImpl<byte[]> consumer = createConsumer(configuration());
        verify(external, never()).getExecutor();
        int callers = 8;
        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(callers);
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        ExecutorService dispatchers = Executors.newFixedThreadPool(callers,
                new DefaultThreadFactory("listener-allocation-dispatch"));
        Message<byte[]> message = newMessage();
        try {
            for (int i = 0; i < callers; i++) {
                dispatchers.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        consumer.messageListenerExecutor.execute(message, () -> {
                            threads.add(Thread.currentThread());
                            completed.countDown();
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).as("Dispatchers are ready").isTrue();
            start.countDown();
            assertThat(completed.await(5, TimeUnit.SECONDS)).as("Concurrent dispatches completed").isTrue();
            assertThat(threads).hasSize(1);
            verify(external, times(1)).getExecutor();
        } finally {
            start.countDown();
            dispatchers.shutdownNow();
        }
    }

    @Test(timeOut = 30000)
    public void testConsumerEventsAndMessagesShareExecutor() throws Exception {
        createClient(10);
        ConsumerConfigurationData<byte[]> conf = configuration();
        ConsumerEventListener eventListener = mock(ConsumerEventListener.class);
        conf.setConsumerEventListener(eventListener);
        ConsumerImpl<byte[]> consumer = createConsumer(conf);
        CompletableFuture<Thread> activeThread = new CompletableFuture<>();
        CompletableFuture<Thread> inactiveThread = new CompletableFuture<>();
        doAnswer(invocation -> {
            activeThread.complete(Thread.currentThread());
            return null;
        }).when(eventListener).becameActive(consumer, -1);
        doAnswer(invocation -> {
            inactiveThread.complete(Thread.currentThread());
            return null;
        }).when(eventListener).becameInactive(consumer, -1);
        verify(external, never()).getExecutor();
        consumer.activeConsumerChanged(true);
        consumer.activeConsumerChanged(false);
        CompletableFuture<Thread> dispatched = new CompletableFuture<>();
        consumer.messageListenerExecutor.execute(newMessage(), () -> dispatched.complete(Thread.currentThread()));
        Thread messageThread = dispatched.get(5, TimeUnit.SECONDS);
        assertThat(activeThread.get(5, TimeUnit.SECONDS)).isSameAs(messageThread);
        assertThat(inactiveThread.get(5, TimeUnit.SECONDS)).isSameAs(messageThread);
        verify(eventListener).becameActive(consumer, -1);
        verify(eventListener).becameInactive(consumer, -1);
        verify(external, times(1)).getExecutor();
    }

    @Test(timeOut = 10000)
    public void testZeroQueueListenerSelectsExecutorOnFirstMessage() throws Exception {
        createClient(10);
        ConsumerConfigurationData<byte[]> conf = configuration();
        conf.setReceiverQueueSize(0);
        CompletableFuture<Message<byte[]>> received = new CompletableFuture<>();
        conf.setMessageListener((consumer, message) -> received.complete(message));
        ConsumerImpl<byte[]> consumer = createConsumer(conf);
        verify(external, never()).getExecutor();
        MessageImpl<byte[]> message = newMessage();
        assertThat(consumer.canEnqueueMessage(message)).isFalse();
        assertThat(received.get(5, TimeUnit.SECONDS)).isSameAs(message);
        verify(external, times(1)).getExecutor();
    }

    @Test(timeOut = 10000)
    public void testKeySharedUsesKeyExecutor() throws Exception {
        createClient(10);
        ConsumerConfigurationData<byte[]> conf = configuration();
        conf.setSubscriptionType(SubscriptionType.Key_Shared);
        ConsumerImpl<byte[]> consumer = createConsumer(conf);
        byte[] key = new byte[] {1, 2, 3};
        Message<byte[]> message = newMessage();
        when(message.hasOrderingKey()).thenReturn(true);
        when(message.getOrderingKey()).thenReturn(key);
        CompletableFuture<Thread> actual = new CompletableFuture<>();
        consumer.messageListenerExecutor.execute(message, () -> actual.complete(Thread.currentThread()));
        assertThat(actual.get(5, TimeUnit.SECONDS))
                .isSameAs(external.getExecutor(key).submit(Thread::currentThread).get(5, TimeUnit.SECONDS));
        verify(external, never()).getExecutor();
    }

    @Test
    public void testCustomExecutorDoesNotSelectPinnedExecutor() {
        createClient(10);
        ConsumerConfigurationData<byte[]> conf = configuration();
        conf.setMessageListenerExecutor((message, runnable) -> runnable.run());
        ConsumerImpl<byte[]> consumer = createConsumer(conf);
        Runnable listener = mock(Runnable.class);
        consumer.messageListenerExecutor.execute(newMessage(), listener);
        verify(listener).run();
        verify(external, never()).getExecutor();
    }
}
