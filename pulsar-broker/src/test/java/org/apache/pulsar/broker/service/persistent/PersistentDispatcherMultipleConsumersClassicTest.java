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
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.awaitility.reflect.WhiteboxImpl;
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
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                new PersistentDispatcherMultipleConsumersClassic(topic, sub.getCursor(), sub) {
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
}
