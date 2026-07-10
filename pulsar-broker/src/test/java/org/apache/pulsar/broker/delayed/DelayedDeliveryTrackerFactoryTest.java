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
package org.apache.pulsar.broker.delayed;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.bucket.BucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.bucket.RecoverDelayedDeliveryTrackerException;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DelayedDeliveryTrackerFactoryTest extends ProducerConsumerBase {
    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setDelayedDeliveryTrackerFactoryClassName(BucketDelayedDeliveryTrackerFactory.class.getName());
        conf.setDelayedDeliveryMaxNumBuckets(10);
        conf.setDelayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds(1);
        conf.setDelayedDeliveryMaxIndexesPerBucketSnapshotSegment(10);
        conf.setDelayedDeliveryMinIndexCountPerBucket(50);
        conf.setDelayedDeliveryTickTimeMillis(1024);
        conf.setDispatcherReadFailureBackoffInitialTimeInMs(1000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testFallbackToInMemoryTracker() throws Exception {
        Pair<BrokerService, AbstractPersistentDispatcherMultipleConsumers> pair =
                mockDelayedDeliveryTrackerFactoryAndDispatcher();
        BrokerService brokerService = pair.getLeft();
        AbstractPersistentDispatcherMultipleConsumers dispatcher = pair.getRight();

        // Since Mocked BucketDelayedDeliveryTrackerFactory.newTracker0() throws RecoverDelayedDeliveryTrackerException,
        // the factory should be fallback to InMemoryDelayedDeliveryTrackerFactory
        @Cleanup
        DelayedDeliveryTracker tracker = brokerService.getDelayedDeliveryTrackerFactory().newTracker(dispatcher);
        Assert.assertTrue(tracker instanceof InMemoryDelayedDeliveryTracker);

        DelayedDeliveryTrackerFactory fallbackFactory = brokerService.getFallbackDelayedDeliveryTrackerFactory();
        Assert.assertTrue(fallbackFactory instanceof InMemoryDelayedDeliveryTrackerFactory);
    }


    private Pair<BrokerService, AbstractPersistentDispatcherMultipleConsumers>
    mockDelayedDeliveryTrackerFactoryAndDispatcher() throws Exception {
        BrokerService brokerService = Mockito.spy(pulsar.getBrokerService());

        // Mock dispatcher
        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                Mockito.mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Mockito.doReturn("test").when(dispatcher).getName();

        @Cleanup
        DelayedDeliveryTrackerFactory originalDelayedDeliveryTrackerFactory =
                brokerService.getDelayedDeliveryTrackerFactory();

        // Mock BucketDelayedDeliveryTrackerFactory
        @Cleanup
        BucketDelayedDeliveryTrackerFactory factory = new BucketDelayedDeliveryTrackerFactory();
        factory = Mockito.spy(factory);
        factory.initialize(pulsar);
        Mockito.doThrow(new RecoverDelayedDeliveryTrackerException(new RuntimeException()))
                .when(factory).newTracker0(Mockito.eq(dispatcher));
        // Mock brokerService
        Mockito.doReturn(factory).when(brokerService).getDelayedDeliveryTrackerFactory();
        // Mock topic and subscription
        PersistentTopic topic = Mockito.mock(PersistentTopic.class);
        Mockito.doReturn(brokerService).when(topic).getBrokerService();
        Subscription subscription = Mockito.mock(Subscription.class);
        Mockito.doReturn("topic").when(topic).getName();
        Mockito.doReturn("sub").when(subscription).getName();
        Mockito.doReturn(topic).when(dispatcher).getTopic();
        Mockito.doReturn(subscription).when(dispatcher).getSubscription();

        return Pair.of(brokerService, dispatcher);
    }

    @Test
    public void testFallbackToInMemoryTrackerFactoryFailed() throws Exception {
        Pair<BrokerService, AbstractPersistentDispatcherMultipleConsumers> pair =
                mockDelayedDeliveryTrackerFactoryAndDispatcher();
        BrokerService brokerService = pair.getLeft();
        AbstractPersistentDispatcherMultipleConsumers dispatcher = pair.getRight();

        @Cleanup
        DelayedDeliveryTrackerFactory originalDelayedDeliveryTrackerFactory =
                brokerService.getDelayedDeliveryTrackerFactory();

        // Mock InMemoryDelayedDeliveryTrackerFactory
        @Cleanup
        InMemoryDelayedDeliveryTrackerFactory factory = new InMemoryDelayedDeliveryTrackerFactory();
        factory = Mockito.spy(factory);
        factory.initialize(pulsar);
        // Mock InMemoryDelayedDeliveryTrackerFactory.newTracker0() throws RuntimeException
        Mockito.doThrow(new RuntimeException()).when(factory).newTracker0(Mockito.eq(dispatcher));

        // Mock brokerService to return mocked InMemoryDelayedDeliveryTrackerFactory
        Mockito.doAnswer(inv -> null).when(brokerService).initializeFallbackDelayedDeliveryTrackerFactory();
        Mockito.doReturn(factory).when(brokerService).getFallbackDelayedDeliveryTrackerFactory();

        // Since Mocked BucketDelayedDeliveryTrackerFactory.newTracker0() throws RecoverDelayedDeliveryTrackerException,
        // and Mocked InMemoryDelayedDeliveryTrackerFactory.newTracker0() throws RuntimeException,
        // the tracker instance should be DelayedDeliveryTracker.DISABLE
        @Cleanup
        DelayedDeliveryTracker tracker = brokerService.getDelayedDeliveryTrackerFactory().newTracker(dispatcher);
        Assert.assertEquals(tracker, DelayedDeliveryTracker.DISABLE);
    }

    @Test
    public void testCreateBucketTrackerAsync() throws Exception {
        // Verify async creation waits for bucket snapshot recovery to complete.
        BrokerService brokerService = Mockito.spy(pulsar.getBrokerService());
        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                Mockito.mock(AbstractPersistentDispatcherMultipleConsumers.class);
        ManagedCursor cursor = Mockito.mock(ManagedCursor.class);
        Mockito.doReturn("sub").when(cursor).getName();
        Mockito.doReturn(Map.of(BucketDelayedDeliveryTracker.DELAYED_BUCKET_KEY_PREFIX + "_1_2", "1"))
                .when(cursor).getCursorProperties();
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(cursor).removeCursorProperty(Mockito.any());
        Mockito.doReturn(cursor).when(dispatcher).getCursor();
        Mockito.doReturn("persistent://public/default/test / sub").when(dispatcher).getName();

        PersistentTopic topic = Mockito.mock(PersistentTopic.class);
        Mockito.doReturn(brokerService).when(topic).getBrokerService();
        Mockito.doReturn("topic").when(topic).getName();
        Subscription subscription = Mockito.mock(Subscription.class);
        Mockito.doReturn("sub").when(subscription).getName();
        Mockito.doReturn(topic).when(dispatcher).getTopic();
        Mockito.doReturn(subscription).when(dispatcher).getSubscription();

        BucketSnapshotStorage storage = Mockito.mock(BucketSnapshotStorage.class);
        CompletableFuture<SnapshotMetadata> metadataFuture = new CompletableFuture<>();
        Mockito.doReturn(metadataFuture).when(storage).getBucketSnapshotMetadata(Mockito.anyLong());
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(storage).deleteBucketSnapshot(Mockito.anyLong());

        @Cleanup
        BucketDelayedDeliveryTrackerFactory factory = new BucketDelayedDeliveryTrackerFactory();
        factory.initialize(pulsar);
        factory.bucketSnapshotStorage.close();
        factory.bucketSnapshotStorage = storage;

        CompletableFuture<DelayedDeliveryTracker> trackerFuture = factory.newTrackerAsync(dispatcher);
        Assert.assertFalse(trackerFuture.isDone());

        metadataFuture.complete(new SnapshotMetadata());
        @Cleanup
        DelayedDeliveryTracker tracker = trackerFuture.get(1, TimeUnit.MINUTES);
        Assert.assertTrue(tracker instanceof BucketDelayedDeliveryTracker);
    }

    @Test
    public void testCreateBucketTrackerAsyncRecoveryTimeoutFallbackToInMemoryTracker() throws Exception {
        // Verify bucket recovery timeout falls back without waiting for the real timeout.
        BrokerService brokerService = Mockito.spy(pulsar.getBrokerService());
        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                Mockito.mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Mockito.doReturn("persistent://public/default/test / sub").when(dispatcher).getName();

        PersistentTopic topic = Mockito.mock(PersistentTopic.class);
        Mockito.doReturn(brokerService).when(topic).getBrokerService();
        Mockito.doReturn("topic").when(topic).getName();
        Subscription subscription = Mockito.mock(Subscription.class);
        Mockito.doReturn("sub").when(subscription).getName();
        Mockito.doReturn(topic).when(dispatcher).getTopic();
        Mockito.doReturn(subscription).when(dispatcher).getSubscription();

        @Cleanup
        BucketDelayedDeliveryTrackerFactory factory = Mockito.spy(new BucketDelayedDeliveryTrackerFactory());
        factory.initialize(pulsar);
        BucketDelayedDeliveryTracker bucketTracker = Mockito.mock(BucketDelayedDeliveryTracker.class);
        TimeoutException timeoutException = new TimeoutException("recover timeout");
        var timeoutExecutor = brokerService.executor();
        Mockito.doReturn(FutureUtil.failedFuture(timeoutException))
                .when(bucketTracker).recoverBucketSnapshotAsync(Mockito.eq(timeoutExecutor));
        Mockito.doReturn(bucketTracker).when(factory).newTracker0(Mockito.eq(dispatcher));

        @Cleanup
        DelayedDeliveryTracker tracker = factory.newTrackerAsync(dispatcher).get(1, TimeUnit.MINUTES);
        Assert.assertTrue(tracker instanceof InMemoryDelayedDeliveryTracker);
        Mockito.verify(bucketTracker).recoverBucketSnapshotAsync(Mockito.eq(timeoutExecutor));
    }

    @Test(timeOut = 60_000)
    public void testDelayedMessageWaitsForAsyncTrackerCreationInBrokerPath() throws Exception {
        // Verify broker dispatch waits for bucket snapshot recovery before resuming delayed delivery.
        String topicName = "persistent://public/default/" + UUID.randomUUID();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        topic = Mockito.spy(topic);
        BrokerService brokerService = Mockito.spy(pulsar.getBrokerService());

        BucketSnapshotStorage storage = Mockito.mock(BucketSnapshotStorage.class);
        CompletableFuture<SnapshotMetadata> metadataFuture = new CompletableFuture<>();
        Mockito.doReturn(metadataFuture).when(storage).getBucketSnapshotMetadata(1L);
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(storage).deleteBucketSnapshot(1L);

        @Cleanup
        BucketDelayedDeliveryTrackerFactory factory = new BucketDelayedDeliveryTrackerFactory();
        factory.initialize(pulsar);
        factory.bucketSnapshotStorage.close();
        factory.bucketSnapshotStorage = storage;

        Mockito.doReturn(factory).when(brokerService).getDelayedDeliveryTrackerFactory();
        Mockito.doReturn(brokerService).when(topic).getBrokerService();
        brokerService.getTopics().put(topicName, CompletableFuture.completedFuture(Optional.of(topic)));

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        PersistentSubscription subscription = topic.getSubscription("sub");
        Dispatcher dispatcher = subscription.getDispatcher();
        Assert.assertTrue(dispatcher instanceof PersistentDispatcherMultipleConsumers);
        PersistentDispatcherMultipleConsumers dispatcher0 = (PersistentDispatcherMultipleConsumers) dispatcher;

        // Force bucket recovery to wait on snapshot metadata.
        dispatcher0.getCursor().putCursorProperty(
                BucketDelayedDeliveryTracker.DELAYED_BUCKET_KEY_PREFIX + "_1_2", "1")
                .get(1, TimeUnit.MINUTES);

        producer.newMessage()
                .value("delayed")
                .deliverAfter(100, TimeUnit.MILLISECONDS)
                .send();

        Mockito.verify(storage, Mockito.timeout(10_000)).getBucketSnapshotMetadata(1L);

        // The delivery time has passed, but bucket recovery is still pending.
        Assert.assertNull(consumer.receive(1, TimeUnit.SECONDS));

        // Finish bucket recovery; dispatcher should replay the elapsed delayed message.
        metadataFuture.complete(new SnapshotMetadata());
        Mockito.verify(storage, Mockito.timeout(10_000)).deleteBucketSnapshot(1L);

        var message = consumer.receive(10, TimeUnit.SECONDS);
        Assert.assertNotNull(message);
        Assert.assertEquals(message.getValue(), "delayed");
        consumer.acknowledge(message);
    }

    // 1. Create BucketDelayedDeliveryTracker failed, fallback to InMemoryDelayedDeliveryTracker,
    // 2. Publish delay messages
    @Test(timeOut = 60_000)
    public void testPublishDelayMessagesAndCreateBucketDelayDeliveryTrackerFailed() throws Exception {
        String topicName = "persistent://public/default/" + UUID.randomUUID();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        // Mock BucketDelayedDeliveryTrackerFactory.newTracker0() throws RecoverDelayedDeliveryTrackerException
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        topic = Mockito.spy(topic);
        BrokerService brokerService = Mockito.spy(pulsar.getBrokerService());
        BucketDelayedDeliveryTrackerFactory factory =
                (BucketDelayedDeliveryTrackerFactory) Mockito.spy(brokerService.getDelayedDeliveryTrackerFactory());
        Mockito.doThrow(new RecoverDelayedDeliveryTrackerException(new RuntimeException()))
                .when(factory).newTracker0(Mockito.any());
        Mockito.doReturn(factory).when(brokerService).getDelayedDeliveryTrackerFactory();

        // Return mocked BrokerService
        Mockito.doReturn(brokerService).when(topic).getBrokerService();

        // Set Mocked topic to BrokerService
        final var topicMap = brokerService.getTopics();
        topicMap.put(topicName, CompletableFuture.completedFuture(Optional.of(topic)));

        // Create consumer
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .messageListener((c, msg) -> {
                    try {
                        c.acknowledge(msg);
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(e);
                    }
                })
                .subscribe();

        PersistentSubscription subscription = topic.getSubscription("sub");
        Dispatcher dispatcher = subscription.getDispatcher();
        Assert.assertTrue(dispatcher instanceof PersistentDispatcherMultipleConsumers);

        // Publish a delay message to initialize DelayedDeliveryTracker
        producer.newMessage().value("test").deliverAfter(10_000, TimeUnit.MILLISECONDS).send();

        // Get DelayedDeliveryTracker from Dispatcher
        PersistentDispatcherMultipleConsumers dispatcher0 = (PersistentDispatcherMultipleConsumers) dispatcher;
        Field trackerField =
                PersistentDispatcherMultipleConsumers.class.getDeclaredField("delayedDeliveryTracker");
        trackerField.setAccessible(true);

        AtomicReference<Optional<DelayedDeliveryTracker>> reference = new AtomicReference<>();
        // Wait until DelayedDeliveryTracker is initialized
        Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
            @SuppressWarnings("unchecked")
            Optional<DelayedDeliveryTracker> optional =
                    (Optional<DelayedDeliveryTracker>) trackerField.get(dispatcher0);
            if (optional.isPresent()) {
                reference.set(optional);
                return true;
            }
            return false;
        });

        Optional<DelayedDeliveryTracker> optional = reference.get();
        Assert.assertTrue(optional.get() instanceof InMemoryDelayedDeliveryTracker);

        // Mock DelayedDeliveryTracker and Count the number of addMessage() calls
        AtomicInteger counter = new AtomicInteger(0);
        InMemoryDelayedDeliveryTracker tracker = (InMemoryDelayedDeliveryTracker) optional.get();
        tracker =  Mockito.spy(tracker);
        Mockito.doAnswer(inv -> {
            counter.incrementAndGet();
            return inv.callRealMethod();
        }).when(tracker).addMessage(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
        // Set Mocked InMemoryDelayedDeliveryTracker back to Dispatcher
        trackerField.set(dispatcher0, Optional.of(tracker));

        // Publish 10 delay messages, so the counter should be 10
        for (int i = 0; i < 10; i++) {
            producer.newMessage().value("test")
                    .deliverAfter(10_000, TimeUnit.MILLISECONDS).send();
        }

        try {
            Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> counter.get() == 10);
        } finally {
            consumer.close();
        }
    }
}
