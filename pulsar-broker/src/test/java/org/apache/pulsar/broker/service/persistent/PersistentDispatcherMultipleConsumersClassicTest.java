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

import com.carrotsearch.hppc.ObjectSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersClassicTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30 * 1000)
    public void testTopicDeleteIfConsumerSetMismatchConsumerList() throws Exception {
        final String topicName = newTopicName();
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

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
        final String topicName = newTopicName();
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        for (int i = 0; i < 10; i++) {
            producer.send("message-" + i);
        }
        producer.close();

        // Get the dispatcher of the topic.
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().get();

        ManagedCursor cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(subscription).when(cursor).getName();
        Subscription sub = Mockito.mock(PersistentSubscription.class);
        Mockito.doReturn(topic).when(sub).getTopic();
        // Mock the dispatcher.
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                Mockito.spy(new PersistentDispatcherMultipleConsumersClassic(topic, cursor, sub));
        // Return 10 permits to make the dispatcher can read more entries.
        Mockito.doReturn(10).when(dispatcher).getFirstAvailableConsumerPermits();

        // Make the count + 1 when call the scheduleReadEntriesWithDelay(...).
        AtomicInteger callScheduleReadEntriesWithDelayCnt = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callScheduleReadEntriesWithDelayCnt.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).scheduleReadEntriesWithDelay(Mockito.any(), Mockito.any(), Mockito.anyLong());

        // Make the count + 1 when call the readEntriesFailed(...).
        AtomicInteger callReadEntriesFailed = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callReadEntriesFailed.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).readEntriesFailed(Mockito.any(), Mockito.any());

        Mockito.doReturn(false).when(cursor).isClosed();

        // Mock the readEntriesOrWait(...) to simulate the cursor is closed.
        Mockito.doAnswer(inv -> {
            PersistentDispatcherMultipleConsumersClassic dispatcher1 = inv.getArgument(2);
            dispatcher1.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("cursor closed"),
                    null);
            return null;
        }).when(cursor).asyncReadEntriesOrWait(Mockito.anyInt(), Mockito.anyLong(), Mockito.eq(dispatcher),
                Mockito.any(), Mockito.any());

        dispatcher.readMoreEntries();

        // Verify: the readEntriesFailed should be called once and
        // the scheduleReadEntriesWithDelay should not be called.
        Assert.assertTrue(callReadEntriesFailed.get() == 1 && callScheduleReadEntriesWithDelayCnt.get() == 0);

        // Verify: the topic can be deleted successfully.
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
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().get();

        ManagedCursor cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(subscription).when(cursor).getName();

        Subscription sub = Mockito.mock(PersistentSubscription.class);
        Mockito.doReturn(topic).when(sub).getTopic();

        PersistentDispatcherMultipleConsumersClassic dispatcher =
            new PersistentDispatcherMultipleConsumersClassic(topic, cursor, sub);

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
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                newDispatcherWithDelayedTrackerFuture(trackerFuture);
        MessageMetadata messageMetadata = newDelayedMessageMetadata(System.currentTimeMillis() + 5_000);

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, messageMetadata));
        Assert.assertTrue(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 1);

        CompletableFuture<Void> clearFuture = dispatcher.clearDelayedMessages();
        Assert.assertFalse(clearFuture.isDone());

        dispatcher.close(false, Optional.empty()).get(1, TimeUnit.MINUTES);
        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        clearFuture.get(1, TimeUnit.MINUTES);

        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        trackerFuture.complete(tracker);

        Mockito.verify(tracker, Mockito.timeout(5_000)).close();
        Mockito.verify(dispatcher, Mockito.never()).readMoreEntriesAsync();
    }

    @Test
    public void testDelayedDeliveryTrackerCreatedAsyncTracksPendingMessages() throws Exception {
        // Verify pending delayed messages are drained into the created tracker.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                newDispatcherWithDelayedTrackerFuture(trackerFuture);
        long deliverAtTime = System.currentTimeMillis() + 5_000;
        MessageMetadata messageMetadata = newDelayedMessageMetadata(deliverAtTime);

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1, messageMetadata));
        DelayedDeliveryTracker tracker = Mockito.mock(DelayedDeliveryTracker.class);
        Mockito.doReturn(true).when(tracker).addMessage(1, 1, deliverAtTime);

        trackerFuture.complete(tracker);

        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        Mockito.verify(tracker).resetTickTime(1000L);
        Mockito.verify(tracker).addMessage(1, 1, deliverAtTime);
        Mockito.verify(dispatcher).readMoreEntriesAsync();
    }

    @Test
    public void testClearDelayedMessagesWaitsForDelayedDeliveryTrackerCreatedAsync() throws Exception {
        // Verify clear waits for async tracker creation before clearing tracker state.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                newDispatcherWithDelayedTrackerFuture(trackerFuture);
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
    public void testDelayedDeliveryTrackerCreatedAsyncFailureReplaysPendingMessages() throws Exception {
        // Verify pending delayed messages are replayed when tracker creation fails.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                newDispatcherWithDelayedTrackerFuture(trackerFuture);

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1,
                newDelayedMessageMetadata(System.currentTimeMillis() + 5_000)));
        trackerFuture.completeExceptionally(new RuntimeException("failed to create tracker"));

        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        Assert.assertEquals(dispatcher.getNumberOfMessagesInReplay(), 1);
        Mockito.verify(dispatcher).readMoreEntriesAsync();
    }

    @Test
    public void testClearDelayedMessagesCompletesWhenDelayedDeliveryTrackerCreateFailsAsync() throws Exception {
        // Verify clear completes even when async tracker creation fails.
        CompletableFuture<DelayedDeliveryTracker> trackerFuture = new CompletableFuture<>();
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                newDispatcherWithDelayedTrackerFuture(trackerFuture);

        Assert.assertTrue(dispatcher.trackDelayedDelivery(1, 1,
                newDelayedMessageMetadata(System.currentTimeMillis() + 5_000)));
        CompletableFuture<Void> clearFuture = dispatcher.clearDelayedMessages();

        trackerFuture.completeExceptionally(new RuntimeException("failed to create tracker"));

        clearFuture.get(1, TimeUnit.MINUTES);
        Assert.assertFalse(dispatcher.isDelayedDeliveryTrackerCreatePending());
        Assert.assertEquals(dispatcher.getDelayedDeliveryTrackerCreatePendingMessages(), 0);
        Mockito.verify(dispatcher).readMoreEntriesAsync();
    }

    private PersistentDispatcherMultipleConsumersClassic newDispatcherWithDelayedTrackerFuture(
            CompletableFuture<DelayedDeliveryTracker> trackerFuture) {
        BrokerService brokerService = Mockito.spy(getPulsar().getBrokerService());
        DelayedDeliveryTrackerFactory factory = Mockito.mock(DelayedDeliveryTrackerFactory.class);
        Mockito.doReturn(trackerFuture).when(factory).newTrackerAsync(Mockito.any());
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

        PersistentDispatcherMultipleConsumersClassic dispatcher =
                Mockito.spy(new PersistentDispatcherMultipleConsumersClassic(topic, cursor, sub));
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
