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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.carrotsearch.hppc.ObjectSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersTest extends SharedPulsarBaseTest {
    @DataProvider(name = "readCallbackListModes")
    public Object[][] readCallbackListModes() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "readCallbackListModes", timeOut = 30_000)
    public void testReadCallbackAcceptsOtherListImplementations(boolean subscriptionThread, boolean immutable)
            throws Exception {
        boolean oldSubscriptionThread = getConfig().isDispatcherDispatchMessagesInSubscriptionThread();
        getConfig().setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        try {
            PersistentSubscription subscription = createSubscriptionWithBacklog();
            List<Entry> entries = subscription.getCursor().readEntries(3);
            List<ByteBuf> buffers = entries.stream().map(Entry::getDataBuffer).toList();
            List<Entry> callbackEntries = immutable ? List.copyOf(entries) : new LinkedList<>(entries);
            PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(
                    (PersistentTopic) subscription.getTopic(), subscription.getCursor(), subscription);
            try {
                // With no consumers, normal dispatch releases the entries and schedules replay.
                // An unsupported list mutation would instead schedule a dispatch-failure retry.
                dispatcher.readEntriesComplete(callbackEntries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
                Awaitility.await().until(() -> !dispatcher.isSendInProgress());
                assertThat(dispatcher.isDispatchRetryPending()).isFalse();
                assertThat(dispatcher.getNumberOfMessagesInReplay()).isEqualTo(3);
                assertThat(callbackEntries).containsExactlyElementsOf(entries);
            } finally {
                dispatcher.close(false, Optional.empty()).get(5, TimeUnit.SECONDS);
                admin.topics().unload(subscription.getTopic().getName());
            }
            buffers.forEach(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            getConfig().setDispatcherDispatchMessagesInSubscriptionThread(oldSubscriptionThread);
        }
    }

    @Test(dataProvider = "routingFailureModes", timeOut = 30_000)
    public void testMetadataFailureDuringWrappingReplaysWholeRead(boolean subscriptionThread) throws Exception {
        boolean oldSubscriptionThread = getConfig().isDispatcherDispatchMessagesInSubscriptionThread();
        getConfig().setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        Double sample = CollectorRegistry.defaultRegistry.getSampleValue("pulsar_broker_pending_bytes_to_dispatch");
        double pendingBytes = sample == null ? 0 : sample;
        try {
            PersistentSubscription subscription = createSubscriptionWithBacklog();
            List<Entry> entries = subscription.getCursor().readEntries(3);
            List<ByteBuf> buffers = entries.stream().map(Entry::getDataBuffer).toList();
            Entry failingEntry = Mockito.spy(entries.get(1));
            ownedMocks.add(failingEntry);
            Mockito.doThrow(new IllegalStateException("metadata lookup failed"))
                    .when(failingEntry).getMessageMetadata();
            entries.set(1, failingEntry);
            PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(
                    (PersistentTopic) subscription.getTopic(), subscription.getCursor(), subscription);
            try {
                // Fail between the wrapped prefix and raw suffix of a real cursor read.
                dispatcher.readEntriesComplete(entries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
                Awaitility.await().untilAsserted(() -> {
                    assertThat(dispatcher.getNumberOfMessagesInReplay()).isEqualTo(3);
                    assertThat(dispatcher.isSendInProgress()).isFalse();
                    assertThat(CollectorRegistry.defaultRegistry
                            .getSampleValue("pulsar_broker_pending_bytes_to_dispatch")).isEqualTo(pendingBytes);
                });
                Mockito.verify(failingEntry).release();
            } finally {
                dispatcher.close(false, Optional.empty()).get(5, TimeUnit.SECONDS);
                admin.topics().unload(subscription.getTopic().getName());
            }
            buffers.forEach(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            getConfig().setDispatcherDispatchMessagesInSubscriptionThread(oldSubscriptionThread);
        }
    }

    private PersistentSubscription createSubscriptionWithBacklog() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "sub", MessageId.earliest);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(false).create()) {
            for (int i = 0; i < 3; i++) {
                producer.newMessage().key("key").value("m" + i).send();
            }
        }
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        return topic.getSubscription("sub");
    }

    @DataProvider(name = "routingFailureModes")
    public Object[][] routingFailureModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "routingFailureModes", timeOut = 30_000)
    public void testRoutingFailureRetriesWithBackoff(boolean subscriptionThread) throws Exception {
        boolean oldSubscriptionThread = getConfig().isDispatcherDispatchMessagesInSubscriptionThread();
        int oldInitialBackoff = getConfig().getDispatcherReadFailureBackoffInitialTimeInMs();
        int oldMaxBackoff = getConfig().getDispatcherReadFailureBackoffMaxTimeInMs();
        getConfig().setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(2000);
        getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(2000);
        Double sample = CollectorRegistry.defaultRegistry.getSampleValue("pulsar_broker_pending_bytes_to_dispatch");
        double pendingBytes = sample == null ? 0 : sample;
        try {
            String topicName = newTopicName();
            admin.topics().createNonPartitionedTopic(topicName);
            admin.topics().createSubscription(topicName, "sub", MessageId.earliest);
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
            // The stored metadata is structurally valid, but its base64 key is invalid. The real
            // Key_Shared routing path fails in Commands.resolveStickyKey after the cursor completes its read.
            MessageMetadata metadata = new MessageMetadata().setProducerName("bad-key-producer")
                    .setSequenceId(0).setPublishTime(System.currentTimeMillis())
                    .setPartitionKey("%").setPartitionKeyB64Encoded(true);
            ByteBuf entry = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                    metadata, Unpooled.EMPTY_BUFFER);
            try {
                topic.getManagedLedger().addEntry(ByteBufUtil.getBytes(entry));
            } finally {
                entry.release();
            }
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                    .enableBatching(false).create()) {
                producer.newMessage().key("good-key").value("good").send();
            }
            PersistentSubscription subscription = topic.getSubscription("sub");
            try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionName("sub").subscriptionType(SubscriptionType.Key_Shared).subscribe()) {
                PersistentDispatcherMultipleConsumers dispatcher =
                        (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
                Awaitility.await().untilAsserted(() -> {
                    assertThat(dispatcher.isDispatchRetryPending()).isTrue();
                    assertThat(dispatcher.getNumberOfMessagesInReplay()).isEqualTo(2);
                    assertThat(CollectorRegistry.defaultRegistry
                            .getSampleValue("pulsar_broker_pending_bytes_to_dispatch")).isEqualTo(pendingBytes);
                });
                synchronized (dispatcher) {
                    assertThat(dispatcher.isDispatchRetryPending()).isTrue();
                    long readCount = dispatcher.getReadMoreEntriesCallCount();
                    // The retry timer cannot acquire the dispatcher monitor during these notifications.
                    for (int i = 0; i < 100; i++) {
                        dispatcher.readMoreEntries();
                    }
                    assertThat(dispatcher.getReadMoreEntriesCallCount()).isEqualTo(readCount);
                    assertThat(dispatcher.isHavePendingReplayRead()).isFalse();
                }
                // An operator removes the poison entry; recovery must resume without reconnecting the client.
                admin.topics().skipMessages(topicName, "sub", 1);
                Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(message.getValue()).isEqualTo("good");
                consumer.acknowledge(message);
                Awaitility.await().untilAsserted(() -> {
                    assertThat(subscription.getCursor().getNumberOfEntriesInBacklog(false)).isZero();
                    assertThat(CollectorRegistry.defaultRegistry
                            .getSampleValue("pulsar_broker_pending_bytes_to_dispatch")).isEqualTo(pendingBytes);
                });
            }
        } finally {
            getConfig().setDispatcherDispatchMessagesInSubscriptionThread(oldSubscriptionThread);
            getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(oldInitialBackoff);
            getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(oldMaxBackoff);
        }
    }

    // Concurrent mock calls only run on executors that the test shuts down and joins.
    private final List<Object> ownedMocks = new ArrayList<>();

    private <T> T ownMock(Class<T> type) {
        T mock = Mockito.mock(type);
        ownedMocks.add(mock);
        return mock;
    }

    @AfterMethod(alwaysRun = true)
    public void releaseOwnedMocks() {
        try {
            Mockito.reset(ownedMocks.toArray());
        } finally {
            ownedMocks.forEach(mock -> Mockito.framework().clearInlineMock(mock));
            ownedMocks.clear();
        }
    }

    @Test(timeOut = 30_000)
    public void testReadMoreEntriesConflatesConcurrentRequests() throws Exception {
        ManagedCursor cursor = ownMock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor);
        CountDownLatch[] passStarted = {new CountDownLatch(1), new CountDownLatch(1)};
        CountDownLatch[] releasePass = {new CountDownLatch(1), new CountDownLatch(1)};
        AtomicInteger passes = new AtomicInteger();
        Mockito.doAnswer(inv -> {
            assertThat(Thread.holdsLock(dispatcher)).isTrue();
            int pass = passes.getAndIncrement();
            if (pass < passStarted.length) {
                passStarted[pass].countDown();
                assertThat(releasePass[pass].await(10, TimeUnit.SECONDS)).isTrue();
            }
            return true;
        }).when(cursor).isClosed();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> owner = executor.submit(dispatcher::readMoreEntries);
            for (int pass = 0; pass < passStarted.length; pass++) {
                assertThat(passStarted[pass].await(10, TimeUnit.SECONDS)).isTrue();
                // All callers must return while the owner still holds the dispatcher monitor.
                executor.submit(() -> {
                    for (int request = 0; request < 100; request++) {
                        dispatcher.readMoreEntries();
                    }
                }).get(10, TimeUnit.SECONDS);
                assertThat(passes.get()).isEqualTo(pass + 1);
                releasePass[pass].countDown();
            }
            owner.get(10, TimeUnit.SECONDS);
            assertThat(passes.get()).isEqualTo(3);
            dispatcher.readMoreEntries();
            assertThat(passes.get()).isEqualTo(4);
        } finally {
            for (CountDownLatch latch : releasePass) {
                latch.countDown();
            }
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(timeOut = 30_000)
    public void testReadMoreEntriesConflatesReentrantRequests() throws Exception {
        ManagedCursor cursor = ownMock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor);
        AtomicInteger passes = new AtomicInteger();
        AtomicInteger depth = new AtomicInteger();
        Mockito.doAnswer(inv -> {
            assertThat(depth.incrementAndGet()).isEqualTo(1);
            try {
                if (passes.incrementAndGet() < 1000) {
                    dispatcher.readMoreEntries();
                    dispatcher.readMoreEntries();
                }
                return true;
            } finally {
                depth.decrementAndGet();
            }
        }).when(cursor).isClosed();

        dispatcher.readMoreEntries();
        assertThat(passes.get()).isEqualTo(1000);
    }

    @Test(timeOut = 30_000)
    public void testReadMoreEntriesRecoversAfterFailure() throws Exception {
        ManagedCursor cursor = ownMock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor);
        IllegalStateException failure = new IllegalStateException("read failed");
        Mockito.doThrow(failure).when(cursor).isClosed();
        assertThatThrownBy(dispatcher::readMoreEntries).isSameAs(failure);

        Mockito.doReturn(true).when(cursor).isClosed();
        Mockito.clearInvocations(cursor);
        dispatcher.readMoreEntries();
        Mockito.verify(cursor).isClosed();
    }

    @Test(timeOut = 30_000)
    public void testReadMoreEntriesRunsReentrantRequestsAfterFailure() throws Exception {
        ManagedCursor cursor = ownMock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor);
        IllegalStateException failure = new IllegalStateException("read failed");
        IllegalStateException followUpFailure = new IllegalStateException("follow-up read failed");
        AtomicInteger passes = new AtomicInteger();
        Mockito.doAnswer(inv -> {
            int pass = passes.incrementAndGet();
            if (pass <= 3) {
                dispatcher.readMoreEntries();
                // Reusing the first exception must not cause self-suppression.
                throw pass == 2 ? followUpFailure : failure;
            }
            return true;
        }).when(cursor).isClosed();

        assertThatThrownBy(dispatcher::readMoreEntries).isSameAs(failure);
        assertThat(passes.get()).as("Registered follow-ups run before propagating the failure").isEqualTo(4);
        assertThat(failure.getSuppressed()).containsExactly(followUpFailure);
        dispatcher.readMoreEntries();
        assertThat(passes.get()).isEqualTo(5);
    }

    @Test(timeOut = 30_000)
    public void testReadMoreEntriesRunsConcurrentRequestAfterFailure() throws Exception {
        ManagedCursor cursor = ownMock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor);
        IllegalStateException failure = new IllegalStateException("read failed");
        CountDownLatch passStarted = new CountDownLatch(1);
        CountDownLatch releasePass = new CountDownLatch(1);
        AtomicInteger passes = new AtomicInteger();
        Mockito.doAnswer(inv -> {
            if (passes.incrementAndGet() == 1) {
                passStarted.countDown();
                assertThat(releasePass.await(10, TimeUnit.SECONDS)).isTrue();
                throw failure;
            }
            return true;
        }).when(cursor).isClosed();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> owner = executor.submit(dispatcher::readMoreEntries);
            assertThat(passStarted.await(10, TimeUnit.SECONDS)).isTrue();
            executor.submit(dispatcher::readMoreEntries).get(10, TimeUnit.SECONDS);
            releasePass.countDown();
            assertThatThrownBy(() -> owner.get(10, TimeUnit.SECONDS)).hasCause(failure);
            assertThat(passes.get()).as("The registered follow-up runs without a new caller").isEqualTo(2);
            dispatcher.readMoreEntries();
            assertThat(passes.get()).isEqualTo(3);
        } finally {
            releasePass.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private AbstractPersistentDispatcherMultipleConsumers createReadConflationDispatcher(ManagedCursor cursor)
            throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "s1", MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        Mockito.doReturn("s1").when(cursor).getName();
        Subscription subscription = ownMock(PersistentSubscription.class);
        Mockito.doReturn(topic).when(subscription).getTopic();
        return new PersistentDispatcherMultipleConsumers(topic, cursor, subscription);
    }

    @Test(timeOut = 30 * 1000)
    public void testTopicDeleteIfConsumerSetMismatchConsumerList() throws Exception {
        final String topicName = newTopicName();
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        // Make an error that "consumerSet" is mismatch with "consumerList".
        Dispatcher dispatcher = getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();
        ObjectSet<org.apache.pulsar.broker.service.Consumer> consumerSet =
                WhiteboxImpl.getInternalState(dispatcher, "consumerSet");
        List<org.apache.pulsar.broker.service.Consumer> consumerList =
                WhiteboxImpl.getInternalState(dispatcher, "consumerList");

        org.apache.pulsar.broker.service.Consumer serviceConsumer = consumerList.get(0);
        consumerSet.add(serviceConsumer);
        consumerList.add(serviceConsumer);

        // Verify: the topic can be deleted successfully.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(timeOut = 30 * 1000)
    public void testTopicDeleteIfConsumerSetMismatchConsumerList2() throws Exception {
        final String topicName = newTopicName();
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        // Make an error that "consumerSet" is mismatch with "consumerList".
        Dispatcher dispatcher = getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();
        ObjectSet<org.apache.pulsar.broker.service.Consumer> consumerSet =
                WhiteboxImpl.getInternalState(dispatcher, "consumerSet");
        consumerSet.clear();

        // Verify: the topic can be deleted successfully.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @Test
    public void testSkipReadEntriesFromCloseCursor() throws Exception {
        String topicName = newTopicName();
        String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        PersistentSubscription sub = topic.getSubscription(subscription);
        AtomicInteger scheduledReads = new AtomicInteger();
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topic, sub.getCursor(), sub) {
                    @Override
                    void scheduleReadEntriesWithDelay(Exception exception, ReadType readType, long delay) {
                        scheduledReads.incrementAndGet();
                        super.scheduleReadEntriesWithDelay(exception, readType, delay);
                    }
                };

        dispatcher.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("cursor closed"),
                null);

        Assert.assertEquals(scheduledReads.get(), 0, "Closed cursor failures must not schedule another read");
        admin.topics().delete(topicName, false);
    }

    @Test
    public void testRaceConditionInTrackDelayedDelivery() throws Exception {
        final int numThreads = 16;
        final int operationsPerThread = 2000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();

        final String topicName = newTopicName();
        final String subscription = "s1";

        // Needed to create the topic
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().get();

        PersistentSubscription sub = topic.getSubscription(subscription);
        ManagedCursor cursor = sub.getCursor();

        PersistentDispatcherMultipleConsumers dispatcher =
            new PersistentDispatcherMultipleConsumers(topic, cursor, sub);

        // Align all writes to the same bucket
        // This is the key which triggers the race condition
        long deliverAt = System.currentTimeMillis() + 5000;

        MessageMetadata messageMetadata = new MessageMetadata()
            .setSequenceId(1)
            .setProducerName("testProducer")
            .setPartitionKeyB64Encoded(false)
            .setPublishTime(System.currentTimeMillis())
            .setDeliverAtTime(deliverAt);

        @Cleanup("shutdown")
        ExecutorService executorService = Executors.newFixedThreadPool(32);

        // Start clear message thread
        for (int i = 0; i < numThreads / 2; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        dispatcher.clearDelayedMessages();
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start track delayed delivery thread
        for (int i = numThreads / 2; i < numThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        dispatcher.trackDelayedDelivery(1, 1, messageMetadata);
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Assert.assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");

        if (errors.get() > 0) {
            Exception exception = firstException.get();
            if (exception != null) {
                System.err.println("First exception caught: " + exception.getMessage());
                exception.printStackTrace();
            }
        }
        Assert.assertEquals(errors.get(), 0, "No exceptions should occur during concurrent operations");
    }

    @Test
    public void testCloseReleasesDelayedDeliveryTrackerCreatedAsync() throws Exception {
        // Verify close prevents a late async tracker from being installed.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcherWithDelayedTrackerFuture(trackerFuture);

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1,
                newDelayedMessageMetadata(System.currentTimeMillis() + 5_000)));
        Assert.assertTrue(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertTrue(dispatcher.shouldPauseDeliveryForDelayTracker());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 1);
        Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1);

        CompletableFuture<Void> clearFuture = dispatcher.clearDelayedMessages();
        Assert.assertFalse(clearFuture.isDone());

        dispatcher.close(false, Optional.empty()).get(1, TimeUnit.MINUTES);
        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        clearFuture.get(1, TimeUnit.MINUTES);

        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        trackerFuture.complete(tracker);

        Mockito.verify(tracker).closeAsync();
        Mockito.verify(tracker, Mockito.never()).addMessage(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(dispatcher, Mockito.never()).readMoreEntriesAsync();
    }

    @Test
    public void testClosedDispatcherDoesNotCreateDelayedDeliveryTracker() throws Exception {
        // Verify a delayed message read after close doesn't start a tracker creation that is never cleaned up.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcherWithDelayedTrackerFuture(trackerFuture);
        dispatcher.close(false, Optional.empty()).get(1, TimeUnit.MINUTES);

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1,
                newDelayedMessageMetadata(System.currentTimeMillis() + 5_000)));
        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertFalse(dispatcher.shouldPauseDeliveryForDelayTracker());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        dispatcher.clearDelayedMessages().get(1, TimeUnit.MINUTES);
    }

    @Test
    public void testDelayedDeliveryTrackerCreatedAsyncTracksPendingMessages() throws Exception {
        // Verify pending delayed messages are drained into the created tracker.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcherWithDelayedTrackerFuture(trackerFuture);
        long deliverAtTime = System.currentTimeMillis() + 5_000;

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, newDelayedMessageMetadata(deliverAtTime)));
        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 2, newDelayedMessageMetadata(deliverAtTime)));
        // A message without a delivery time is dispatched normally while the tracker is being created
        Assert.assertFalse(dispatcher.trackDelayedDelivery(1, 3, new MessageMetadata()));
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 2);

        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        Mockito.doReturn(true).when(tracker).addMessage(Mockito.eq(1L), Mockito.anyLong(), Mockito.eq(deliverAtTime));
        trackerFuture.complete(tracker);

        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        Assert.assertEquals(dispatcher.getNumberOfMessagesInReplay(), 0);
        Mockito.verify(tracker).resetTickTime(1000L);
        Mockito.verify(tracker).addMessage(1, 1, deliverAtTime);
        Mockito.verify(tracker).addMessage(1, 2, deliverAtTime);
        Mockito.verify(dispatcher).readMoreEntriesAsync();
    }

    @Test
    public void testDelayedDeliveryTrackerCreatedInline() throws Exception {
        // Verify a factory that completes inline, like the in-memory one, installs the tracker right away and
        // doesn't schedule an extra read.
        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        long deliverAtTime = System.currentTimeMillis() + 5_000;
        Mockito.doReturn(true).when(tracker).addMessage(1, 1, deliverAtTime);
        PersistentDispatcherMultipleConsumers dispatcher =
                newDispatcherWithDelayedTrackerFuture(CompletableFuture.completedFuture(tracker));

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, newDelayedMessageMetadata(deliverAtTime)));
        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertFalse(dispatcher.shouldPauseDeliveryForDelayTracker());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        Mockito.verify(tracker).addMessage(1, 1, deliverAtTime);
        Mockito.verify(dispatcher, Mockito.never()).readMoreEntriesAsync();
    }

    @Test
    public void testDelayedDeliveryTrackerCreatedAsyncReplaysElapsedMessages() throws Exception {
        // Verify a pending message whose delivery time passed during the creation is replayed.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcherWithDelayedTrackerFuture(trackerFuture);
        long deliverAtTime = System.currentTimeMillis() + 5_000;

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, newDelayedMessageMetadata(deliverAtTime)));
        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        Mockito.doReturn(false).when(tracker).addMessage(1, 1, deliverAtTime);
        trackerFuture.complete(tracker);

        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getNumberOfMessagesInReplay(), 1);
        Mockito.verify(dispatcher).readMoreEntriesAsync();
    }

    @Test
    public void testDelayedDeliveryTrackerAddMessageFailureReplaysPendingMessages() throws Exception {
        // Verify a tracker that throws while taking the pending messages doesn't leave them or the dispatch stuck.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcherWithDelayedTrackerFuture(trackerFuture);
        long deliverAtTime = System.currentTimeMillis() + 5_000;

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, newDelayedMessageMetadata(deliverAtTime)));
        CompletableFuture<Void> clearFuture = dispatcher.clearDelayedMessages();
        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        Mockito.doThrow(new IllegalStateException("addMessage failed")).when(tracker)
                .addMessage(1, 1, deliverAtTime);
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(tracker).clear();
        trackerFuture.complete(tracker);

        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertFalse(dispatcher.shouldPauseDeliveryForDelayTracker());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        Assert.assertEquals(dispatcher.getNumberOfMessagesInReplay(), 1);
        clearFuture.get(1, TimeUnit.MINUTES);
        Mockito.verify(dispatcher).readMoreEntriesAsync();
    }

    @Test
    public void testClearDelayedMessagesWaitsForDelayedDeliveryTrackerCreatedAsync() throws Exception {
        // Verify clear waits for async tracker creation before clearing tracker state.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcherWithDelayedTrackerFuture(trackerFuture);
        long deliverAtTime = System.currentTimeMillis() + 5_000;

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, newDelayedMessageMetadata(deliverAtTime)));
        CompletableFuture<Void> clearFuture = dispatcher.clearDelayedMessages();
        Assert.assertFalse(clearFuture.isDone());

        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        Mockito.doReturn(true).when(tracker).addMessage(1, 1, deliverAtTime);
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(tracker).clear();
        trackerFuture.complete(tracker);

        clearFuture.get(1, TimeUnit.MINUTES);
        Mockito.verify(tracker).clear();
        Mockito.verify(dispatcher).readMoreEntriesAsync();
    }

    @Test
    public void testDelayedDeliveryTrackerCreateFailureKeepsMessagesPendingAndRetries() throws Exception {
        // Verify a failed creation neither delivers the pending messages early nor disables delayed delivery.
        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(tracker).clear();
        DelayedDeliveryTrackerFactory factory = Mockito.mock(DelayedDeliveryTrackerFactory.class);
        Mockito.doReturn(FutureUtil.failedFuture(new RuntimeException("failed to create tracker")))
                .doReturn(CompletableFuture.completedFuture(tracker))
                .when(factory).newTrackerAsync(Mockito.any());
        PersistentDispatcherMultipleConsumers dispatcher = newDispatcherWithDelayedTrackerFactory(factory);
        long deliverAtTime = System.currentTimeMillis() + 5_000;
        Mockito.doReturn(true).when(tracker).addMessage(1, 1, deliverAtTime);

        try {
            Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, newDelayedMessageMetadata(deliverAtTime)));
            Assert.assertTrue(dispatcher.isDelayedDeliveryTrackerCreatePending());
            Assert.assertTrue(dispatcher.shouldPauseDeliveryForDelayTracker());
            Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 1);
            Assert.assertEquals(dispatcher.getNumberOfMessagesInReplay(), 0);
            CompletableFuture<Void> clearFuture = dispatcher.clearDelayedMessages();
            Assert.assertFalse(clearFuture.isDone());

            // The retry is scheduled after the initial backoff delay
            Awaitility.await().untilAsserted(() -> {
                Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
                Assert.assertTrue(clearFuture.isDone());
            });
            Mockito.verify(factory, Mockito.times(2)).newTrackerAsync(dispatcher);
            Mockito.verify(tracker).addMessage(1, 1, deliverAtTime);
            Mockito.verify(tracker).clear();
            Assert.assertEquals(dispatcher.getNumberOfMessagesInReplay(), 0);
            Mockito.verify(dispatcher).readMoreEntriesAsync();
        } finally {
            // Stop any further retry
            dispatcher.close(false, Optional.empty()).get(1, TimeUnit.MINUTES);
        }
    }

    private PersistentDispatcherMultipleConsumers newDispatcherWithDelayedTrackerFuture(
            CompletableFuture<DelayedDeliveryTracker> trackerFuture) {
        DelayedDeliveryTrackerFactory factory = Mockito.mock(DelayedDeliveryTrackerFactory.class);
        Mockito.doReturn(trackerFuture).when(factory).newTrackerAsync(Mockito.any());
        return newDispatcherWithDelayedTrackerFactory(factory);
    }

    private PersistentDispatcherMultipleConsumers newDispatcherWithDelayedTrackerFactory(
            DelayedDeliveryTrackerFactory factory) {
        BrokerService brokerService = Mockito.spy(getPulsar().getBrokerService());
        Mockito.doReturn(factory).when(brokerService).getDelayedDeliveryTrackerFactory();

        PersistentTopic topic = Mockito.mock(PersistentTopic.class);
        Mockito.doReturn(brokerService).when(topic).getBrokerService();
        Mockito.doReturn("persistent://public/default/test").when(topic).getName();
        Mockito.doReturn(true).when(topic).isDelayedDeliveryEnabled();
        Mockito.doReturn(1000L).when(topic).getDelayedDeliveryTickTimeMillis();

        ManagedCursor cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn("sub").when(cursor).getName();

        Subscription sub = Mockito.mock(PersistentSubscription.class);
        Mockito.doReturn("sub").when(sub).getName();
        Mockito.doReturn(topic).when(sub).getTopic();

        PersistentDispatcherMultipleConsumers dispatcher =
                Mockito.spy(new PersistentDispatcherMultipleConsumers(topic, cursor, sub));
        Mockito.doNothing().when(dispatcher).readMoreEntriesAsync();
        return dispatcher;
    }

    private MessageMetadata newDelayedMessageMetadata(long deliverAtTime) {
        return new MessageMetadata()
                .setSequenceId(1)
                .setProducerName("testProducer")
                .setPublishTime(System.currentTimeMillis())
                .setDeliverAtTime(deliverAtTime);
    }
}
