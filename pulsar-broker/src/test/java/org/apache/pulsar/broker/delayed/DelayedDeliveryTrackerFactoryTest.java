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

import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.bucket.RecoverDelayedDeliveryTrackerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.*;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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


    private Pair<BrokerService, AbstractPersistentDispatcherMultipleConsumers> mockDelayedDeliveryTrackerFactoryAndDispatcher()
            throws Exception {
        BrokerService brokerService = Mockito.spy(pulsar.getBrokerService());

        // Mock dispatcher
        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                Mockito.mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Mockito.doReturn("test").when(dispatcher).getName();
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
