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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UnAckedMessageRedeliveryTrackerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testConsumerCallbacksRunWithoutTrackerLock() throws Exception {
        ManualTimer timer = new ManualTimer();
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.timer()).thenReturn(timer.timer);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        consumer.unAckedChunkedMessageIdSequenceMap = new ConcurrentHashMap<>();
        ExecutorService lockProbe = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("ack-timeout-lock-probe"));
        try (UnAckedMessageRedeliveryTracker tracker =
                     new UnAckedMessageRedeliveryTracker(client, consumer, configuration())) {
            List<Boolean> callbackLockAvailability = new ArrayList<>();
            List<Set<MessageId>> callbackMessageIds = new ArrayList<>();
            doAnswer(invocation -> {
                callbackMessageIds.add(new HashSet<>(invocation.<Set<MessageId>>getArgument(0)));
                callbackLockAvailability.add(canAcquireWriteLock(tracker, lockProbe));
                return null;
            }).when(consumer).onAckTimeoutSend(any());
            doAnswer(invocation -> {
                callbackMessageIds.add(new HashSet<>(invocation.<Set<MessageId>>getArgument(0)));
                callbackLockAvailability.add(canAcquireWriteLock(tracker, lockProbe));
                return null;
            }).when(consumer).redeliverUnacknowledgedMessages(any());

            MessageId messageId = new MessageIdImpl(1, 1, -1);
            assertThat(tracker.add(messageId, 3)).isTrue();
            timer.runTick();
            assertThat(callbackMessageIds).isEmpty();
            timer.runTick();

            assertThat(callbackLockAvailability)
                    .as("Both consumer callbacks must allow another thread to acquire the tracker write lock")
                    .containsExactly(true, true);
            assertThat(callbackMessageIds).containsExactly(Set.of(messageId), Set.of(messageId));
            assertThat(tracker.isEmpty()).isTrue();
            timer.runTick();
            verify(consumer, times(1)).onAckTimeoutSend(any());
            verify(consumer, times(1)).redeliverUnacknowledgedMessages(any());
        } finally {
            lockProbe.shutdownNow();
        }
    }

    @DataProvider(name = "subscriptionTypes")
    public Object[][] subscriptionTypes() {
        return new Object[][] {{SubscriptionType.Exclusive}, {SubscriptionType.Failover}};
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testConcurrentExplicitRedelivery(SubscriptionType subscriptionType) throws Exception {
        ManualTimer timer = new ManualTimer();
        ExecutorProvider executorProvider = new ExecutorProvider(1, "ack-timeout-redelivery-test");
        ExecutorService internalExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("ack-timeout-internal"));
        ExecutorService timeoutExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("ack-timeout-redelivery"));
        AtomicReference<Thread> timeoutThread = new AtomicReference<>();
        ConsumerImpl<byte[]> consumerToClose = null;
        Future<?> timeoutRedelivery = null;
        try {
            PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
            when(client.timer()).thenReturn(timer.timer);
            when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
            client.getConfiguration().setStatsIntervalSeconds(0);
            ConsumerConfigurationData<byte[]> conf = configuration();
            conf.setSubscriptionType(subscriptionType);
            ConsumerImpl<byte[]> consumer = ConsumerImpl.newConsumerImpl(client,
                    "non-persistent://tenant/ns/ack-timeout-redelivery", conf, executorProvider,
                    -1, false, new CompletableFuture<Consumer<byte[]>>(), null, null, null, true);
            consumerToClose = consumer;
            consumer.setState(HandlerState.State.Ready);
            UnAckedMessageTracker tracker = consumer.getUnAckedMessageTracker();
            tracker.add(new MessageIdImpl(1, 1, -1));
            timer.runTick();

            synchronized (consumer) {
                long applicationThreadId = Thread.currentThread().getId();
                timeoutRedelivery = timeoutExecutor.submit(() -> {
                    timeoutThread.set(Thread.currentThread());
                    timer.runTick();
                    return null;
                });
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertThat(timeoutThread.get()).isNotNull();
                    ThreadInfo threadInfo = ManagementFactory.getThreadMXBean()
                            .getThreadInfo(timeoutThread.get().getId(), 30);
                    assertThat(threadInfo.getThreadState()).isEqualTo(Thread.State.BLOCKED);
                    assertThat(threadInfo.getLockOwnerId()).isEqualTo(applicationThreadId);
                    assertThat(threadInfo.getLockInfo().getIdentityHashCode())
                            .isEqualTo(System.identityHashCode(consumer));
                    assertThat(List.of(threadInfo.getStackTrace())).anyMatch(frame ->
                            frame.getClassName().equals(ConsumerImpl.class.getName())
                                    && frame.getMethodName().equals("redeliverUnacknowledgedMessages"));
                });
                // The timeout is actually waiting for the consumer monitor. Probe the other edge of the
                // original lock cycle without blocking forever when testing the unfixed implementation.
                boolean trackerLockAvailable = tracker.writeLock.tryLock();
                if (trackerLockAvailable) {
                    tracker.writeLock.unlock();
                }
                assertThat(trackerLockAvailable)
                        .as("Timeout redelivery must release the tracker lock before waiting for the consumer monitor")
                        .isTrue();
                consumer.redeliverUnacknowledgedMessages();
            }
            timeoutRedelivery.get(10, TimeUnit.SECONDS);
            assertThat(tracker.isEmpty()).isTrue();

            tracker.add(new MessageIdImpl(1, 2, -1));
            timer.runTick();
            timer.runTick();
            assertThat(tracker.isEmpty()).isTrue();
        } finally {
            try {
                // The consumer monitor has been released even when the lock probe failed.
                if (timeoutRedelivery != null) {
                    timeoutRedelivery.get(10, TimeUnit.SECONDS);
                }
                if (consumerToClose != null) {
                    consumerToClose.closeAsync().get(10, TimeUnit.SECONDS);
                }
            } finally {
                timeoutExecutor.shutdownNow();
                executorProvider.shutdownNow();
                internalExecutor.shutdownNow();
            }
        }
    }

    private static ConsumerConfigurationData<byte[]> configuration() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("test-subscription");
        conf.setAckTimeoutMillis(1000);
        conf.setTickDurationMillis(1000);
        conf.setAckTimeoutRedeliveryBackoff(redeliveryCount -> 0);
        return conf;
    }

    private static boolean canAcquireWriteLock(UnAckedMessageTracker tracker, ExecutorService executor)
            throws Exception {
        return executor.submit(() -> {
            if (!tracker.writeLock.tryLock()) {
                return false;
            }
            tracker.writeLock.unlock();
            return true;
        }).get(10, TimeUnit.SECONDS);
    }

    private static final class ManualTimer {
        private final Timer timer = mock(Timer.class);
        private Timeout nextTimeout;

        private ManualTimer() {
            when(timer.newTimeout(any(TimerTask.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                    .thenAnswer(invocation -> {
                        Timeout timeout = mock(Timeout.class);
                        when(timeout.task()).thenReturn(invocation.getArgument(0));
                        AtomicBoolean cancelled = new AtomicBoolean();
                        when(timeout.isCancelled()).thenAnswer(ignored -> cancelled.get());
                        when(timeout.cancel()).thenAnswer(ignored -> !cancelled.getAndSet(true));
                        nextTimeout = timeout;
                        return timeout;
                    });
        }

        private void runTick() throws Exception {
            Timeout timeout = nextTimeout;
            timeout.task().run(timeout);
        }
    }
}
