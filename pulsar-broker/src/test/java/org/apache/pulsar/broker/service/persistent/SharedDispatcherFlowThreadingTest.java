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
package org.apache.pulsar.broker.service.persistent;

import static java.util.Collections.emptyMap;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Key_Shared;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.api.proto.KeySharedMode.AUTO_SPLIT;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.EventExecutor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class SharedDispatcherFlowThreadingTest extends SharedPulsarBaseTest {

    @DataProvider(name = "dispatcherVariants")
    public Object[][] dispatcherVariants() {
        return new Object[][] {
                {false, Shared},
                {true, Shared},
                {false, Key_Shared},
                {true, Key_Shared}
        };
    }

    @DataProvider(name = "dispatcherImplementations")
    public Object[][] dispatcherImplementations() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "dispatcherVariants", timeOut = 30_000)
    public void testFlowDoesNotBlockIoEventLoopWhileDispatchThreadHoldsMonitor(
            boolean classic, SubType subType) throws Exception {
        TestContext context = createTestContext(classic, subType);
        CountDownLatch dispatchThreadHasMonitor = new CountDownLatch(1);
        CountDownLatch releaseDispatchThread = new CountDownLatch(1);
        Future<?> dispatchTask = dispatchMessagesThread(context.dispatcher()).submit(() -> {
            synchronized (context.dispatcher()) {
                dispatchThreadHasMonitor.countDown();
                assertThat(releaseDispatchThread.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return null;
        });

        Future<?> flowFuture = null;
        Future<?> heartbeatFuture = null;
        try {
            assertThat(dispatchThreadHasMonitor.await(5, TimeUnit.SECONDS)).isTrue();
            EventExecutor ioEventLoop = context.topic().getBrokerService().executor().next();
            flowFuture = ioEventLoop.submit(() -> context.consumer().flowPermits(100));
            heartbeatFuture = ioEventLoop.submit(() -> { });

            // The Flow callback must only enqueue dispatcher work, allowing the next I/O event to run immediately.
            heartbeatFuture.get(2, TimeUnit.SECONDS);
            assertThat(context.consumer().getAvailablePermits()).isEqualTo(100);
            assertThat(totalAvailablePermits(context.dispatcher())).isZero();
        } finally {
            releaseDispatchThread.countDown();
            dispatchTask.get(5, TimeUnit.SECONDS);
            if (flowFuture != null) {
                flowFuture.get(5, TimeUnit.SECONDS);
            }
            if (heartbeatFuture != null) {
                heartbeatFuture.get(5, TimeUnit.SECONDS);
            }
        }

        drainDispatchMessagesThread(context.dispatcher());
        assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(100);
        assertThat(context.consumer().getAvailablePermitsForDispatcherRemoval()).isEqualTo(100);
    }

    @Test(dataProvider = "dispatcherVariants", timeOut = 30_000)
    public void testFlowAccountingAndReadRunOnDispatchMessagesThread(
            boolean classic, SubType subType) throws Exception {
        TestContext context = createTestContext(classic, subType);
        CountDownLatch dispatchThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseDispatchThread = new CountDownLatch(1);
        AtomicReference<Thread> dispatchThread = new AtomicReference<>();
        AtomicReference<Thread> readThread = new AtomicReference<>();
        stubReadMoreEntries(context.dispatcher(), () -> readThread.set(Thread.currentThread()));
        Future<?> dispatchTask = dispatchMessagesThread(context.dispatcher()).submit(() -> {
            dispatchThread.set(Thread.currentThread());
            dispatchThreadBlocked.countDown();
            assertThat(releaseDispatchThread.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
        });

        try {
            assertThat(dispatchThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            EventExecutor ioEventLoop = context.topic().getBrokerService().executor().next();
            ioEventLoop.submit(() -> context.consumer().flowPermits(100)).get(5, TimeUnit.SECONDS);

            assertThat(context.consumer().getAvailablePermits()).isEqualTo(100);
            assertThat(totalAvailablePermits(context.dispatcher())).isZero();
        } finally {
            releaseDispatchThread.countDown();
            dispatchTask.get(5, TimeUnit.SECONDS);
        }

        drainDispatchMessagesThread(context.dispatcher());
        assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(100);
        assertThat(context.consumer().getAvailablePermitsForDispatcherRemoval()).isEqualTo(100);
        assertThat(readThread.get()).isSameAs(dispatchThread.get());
    }

    @Test(dataProvider = "dispatcherVariants", timeOut = 30_000)
    public void testQueuedFlowCompletesPendingAfterConsumerRemoval(boolean classic, SubType subType)
            throws Exception {
        TestContext context = createTestContext(classic, subType);
        CountDownLatch dispatchThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseDispatchThread = new CountDownLatch(1);
        AtomicInteger readTriggers = new AtomicInteger();
        stubReadMoreEntries(context.dispatcher(), readTriggers::incrementAndGet);
        Future<?> dispatchTask = dispatchMessagesThread(context.dispatcher()).submit(() -> {
            dispatchThreadBlocked.countDown();
            assertThat(releaseDispatchThread.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
        });

        try {
            assertThat(dispatchThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            EventExecutor ioEventLoop = context.topic().getBrokerService().executor().next();
            ioEventLoop.submit(() -> context.consumer().flowPermits(100)).get(5, TimeUnit.SECONDS);

            assertThat(context.consumer().getAvailablePermits()).isEqualTo(100);
            assertThat(context.consumer().getAvailablePermitsForDispatcherRemoval()).isZero();
            context.dispatcher().removeConsumer(context.consumer());
            assertThat(context.dispatcher().getConsumers()).isEmpty();
            assertThat(totalAvailablePermits(context.dispatcher())).isZero();
        } finally {
            releaseDispatchThread.countDown();
            dispatchTask.get(5, TimeUnit.SECONDS);
        }

        drainDispatchMessagesThread(context.dispatcher());
        assertThat(context.consumer().getAvailablePermitsForDispatcherRemoval()).isEqualTo(100);
        assertThat(totalAvailablePermits(context.dispatcher())).isZero();
        assertThat(readTriggers).hasValue(0);
    }

    @Test(dataProvider = "dispatcherVariants", timeOut = 30_000)
    public void testQueuedFlowDoesNotApplyToEqualReplacementConsumer(boolean classic, SubType subType)
            throws Exception {
        TestContext context = createTestContext(classic, subType);
        CountDownLatch dispatchThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseDispatchThread = new CountDownLatch(1);
        AtomicInteger readTriggers = new AtomicInteger();
        stubReadMoreEntries(context.dispatcher(), readTriggers::incrementAndGet);
        Future<?> dispatchTask = dispatchMessagesThread(context.dispatcher()).submit(() -> {
            dispatchThreadBlocked.countDown();
            assertThat(releaseDispatchThread.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
        });
        Consumer original = context.consumer();
        Consumer replacement = new Consumer(original.getSubscription(), original.subType(), context.topic().getName(),
                original.consumerId(), 0, original.consumerName(), true, original.cnx(), "role", emptyMap(), false,
                new KeySharedMeta().setKeySharedMode(AUTO_SPLIT), MessageId.latest, DEFAULT_CONSUMER_EPOCH);

        try {
            assertThat(dispatchThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            EventExecutor ioEventLoop = context.topic().getBrokerService().executor().next();
            ioEventLoop.submit(() -> original.flowPermits(100)).get(5, TimeUnit.SECONDS);

            assertThat(original.getAvailablePermits()).isEqualTo(100);
            assertThat(original.getAvailablePermitsForDispatcherRemoval()).isZero();
            context.dispatcher().removeConsumer(original);
            context.dispatcher().addConsumer(replacement).join();
            assertThat(replacement).isNotSameAs(original).isEqualTo(original);
            assertThat(replacement.hashCode()).isEqualTo(original.hashCode());
        } finally {
            releaseDispatchThread.countDown();
            try {
                dispatchTask.get(5, TimeUnit.SECONDS);
            } finally {
                drainDispatchMessagesThread(context.dispatcher());
            }
        }

        assertThat(context.dispatcher().getConsumers()).containsExactly(replacement);
        assertThat(context.dispatcher().getConsumers().get(0)).isSameAs(replacement);
        assertThat(original.getAvailablePermitsForDispatcherRemoval()).isEqualTo(100);
        assertThat(replacement.getAvailablePermits()).isZero();
        assertThat(totalAvailablePermits(context.dispatcher())).isZero();
        assertThat(readTriggers).hasValue(0);
    }

    @Test(dataProvider = "dispatcherImplementations", timeOut = 30_000)
    public void testRejectedFlowStaysPendingAndIsExcludedFromRemoval(boolean classic) throws Exception {
        String topicName = newTopicName();
        String subscriptionName = "shared-sub";
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic realTopic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        BrokerService isolatedBroker = spy(realTopic.getBrokerService());
        PersistentTopic isolatedTopic = spy(realTopic);
        doReturn(isolatedBroker).when(isolatedTopic).getBrokerService();

        ExecutorService rejectingLane = mock(ExecutorService.class);
        doThrow(new RejectedExecutionException("test rejection")).when(rejectingLane).execute(any());
        when(rejectingLane.isShutdown()).thenReturn(true);
        OrderedExecutor isolatedOrderedExecutor = mock(OrderedExecutor.class);
        doReturn(rejectingLane).when(isolatedOrderedExecutor).chooseThread();
        doReturn(isolatedOrderedExecutor).when(isolatedBroker).getTopicOrderedExecutor();

        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        when(cursor.getName()).thenReturn(subscriptionName);
        when(cursor.isClosed()).thenReturn(true);
        Subscription subscription = mock(PersistentSubscription.class);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscription.getTopic()).thenReturn(isolatedTopic);
        Dispatcher dispatcher = classic
                ? new PersistentDispatcherMultipleConsumersClassic(isolatedTopic, cursor, subscription)
                : new PersistentDispatcherMultipleConsumers(isolatedTopic, cursor, subscription);
        doAnswer(invocation -> {
            dispatcher.consumerFlow(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(subscription).consumerFlow(any(), anyInt());

        TransportCnx cnx = mock(TransportCnx.class);
        when(cnx.isActive()).thenReturn(true);
        when(cnx.isWritable()).thenReturn(true);
        Consumer remainingConsumer = new Consumer(subscription, Shared, topicName, 1, 0, "remaining", true,
                cnx, "role", emptyMap(), false, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        Consumer removedConsumer = new Consumer(subscription, Shared, topicName, 2, 0, "removed", true,
                cnx, "role", emptyMap(), false, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        dispatcher.addConsumer(remainingConsumer).join();
        dispatcher.addConsumer(removedConsumer).join();

        removedConsumer.flowPermits(100);

        assertThat(removedConsumer.getAvailablePermits()).isEqualTo(100);
        assertThat(removedConsumer.getAvailablePermitsForDispatcherRemoval()).isZero();
        assertThat(totalAvailablePermits(dispatcher)).isZero();
        dispatcher.removeConsumer(removedConsumer);
        assertThat(dispatcher.getConsumers()).containsExactly(remainingConsumer);
        assertThat(removedConsumer.getAvailablePermitsForDispatcherRemoval()).isZero();
        assertThat(totalAvailablePermits(dispatcher)).isZero();
        verify(rejectingLane).execute(any());
    }

    private TestContext createTestContext(boolean classic, SubType subType) throws Exception {
        String topicName = newTopicName();
        String subscriptionName = "shared-sub";
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        when(cursor.getName()).thenReturn(subscriptionName);
        when(cursor.isClosed()).thenReturn(true);
        Subscription subscription = mock(PersistentSubscription.class);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscription.getTopic()).thenReturn(topic);

        Dispatcher dispatcher;
        if (subType == Key_Shared) {
            dispatcher = classic
                    ? spy(new PersistentStickyKeyDispatcherMultipleConsumersClassic(
                            topic, cursor, subscription, getConfig(),
                            new KeySharedMeta().setKeySharedMode(AUTO_SPLIT)))
                    : spy(new PersistentStickyKeyDispatcherMultipleConsumers(
                            topic, cursor, subscription, getConfig(),
                            new KeySharedMeta().setKeySharedMode(AUTO_SPLIT)));
        } else {
            dispatcher = classic
                    ? spy(new PersistentDispatcherMultipleConsumersClassic(topic, cursor, subscription))
                    : spy(new PersistentDispatcherMultipleConsumers(topic, cursor, subscription));
        }
        doAnswer(invocation -> {
            dispatcher.consumerFlow(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(subscription).consumerFlow(any(), anyInt());

        TransportCnx cnx = mock(TransportCnx.class);
        when(cnx.isActive()).thenReturn(true);
        when(cnx.isWritable()).thenReturn(true);
        Consumer consumer = new Consumer(subscription, subType, topicName, 1, 0, "consumer", true, cnx, "role",
                emptyMap(), false, new KeySharedMeta().setKeySharedMode(AUTO_SPLIT), MessageId.latest,
                DEFAULT_CONSUMER_EPOCH);
        dispatcher.addConsumer(consumer).join();
        return new TestContext(topic, dispatcher, consumer);
    }

    private static void stubReadMoreEntries(Dispatcher dispatcher, Runnable action) {
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers pip379Dispatcher) {
            doAnswer(invocation -> {
                action.run();
                return null;
            }).when(pip379Dispatcher).readMoreEntries();
        } else {
            PersistentDispatcherMultipleConsumersClassic classicDispatcher =
                    (PersistentDispatcherMultipleConsumersClassic) dispatcher;
            doAnswer(invocation -> {
                action.run();
                return null;
            }).when(classicDispatcher).readMoreEntries();
        }
    }

    private static void drainDispatchMessagesThread(Dispatcher dispatcher) throws Exception {
        dispatchMessagesThread(dispatcher).submit(() -> { }).get(5, TimeUnit.SECONDS);
    }

    private static ExecutorService dispatchMessagesThread(Dispatcher dispatcher) {
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers pip379Dispatcher) {
            return pip379Dispatcher.dispatchMessagesThread;
        }
        return ((PersistentDispatcherMultipleConsumersClassic) dispatcher).dispatchMessagesThread;
    }

    private static int totalAvailablePermits(Dispatcher dispatcher) {
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers pip379Dispatcher) {
            return pip379Dispatcher.totalAvailablePermits;
        }
        return ((PersistentDispatcherMultipleConsumersClassic) dispatcher).totalAvailablePermits;
    }

    private record TestContext(PersistentTopic topic, Dispatcher dispatcher, Consumer consumer) {
    }
}
