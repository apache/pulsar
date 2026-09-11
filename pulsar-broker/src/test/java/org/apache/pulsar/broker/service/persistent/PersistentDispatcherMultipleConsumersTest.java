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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30_000, dataProvider = "readConflationDispatcherTypes")
    public void testReadMoreEntriesConflatesConcurrentRequests(boolean classic) throws Exception {
        ManagedCursor cursor = Mockito.mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor, classic);
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

    @Test(timeOut = 30_000, dataProvider = "readConflationDispatcherTypes")
    public void testReadMoreEntriesConflatesReentrantRequests(boolean classic) throws Exception {
        ManagedCursor cursor = Mockito.mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor, classic);
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

    @Test(timeOut = 30_000, dataProvider = "readConflationDispatcherTypes")
    public void testReadMoreEntriesRecoversAfterFailure(boolean classic) throws Exception {
        ManagedCursor cursor = Mockito.mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor, classic);
        IllegalStateException failure = new IllegalStateException("read failed");
        Mockito.doThrow(failure).when(cursor).isClosed();
        assertThatThrownBy(dispatcher::readMoreEntries).isSameAs(failure);

        Mockito.doReturn(true).when(cursor).isClosed();
        Mockito.clearInvocations(cursor);
        dispatcher.readMoreEntries();
        Mockito.verify(cursor).isClosed();
    }

    @Test(timeOut = 30_000, dataProvider = "readConflationDispatcherTypes")
    public void testReadMoreEntriesRunsReentrantRequestsAfterFailure(boolean classic) throws Exception {
        ManagedCursor cursor = Mockito.mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor, classic);
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

    @Test(timeOut = 30_000, dataProvider = "readConflationDispatcherTypes")
    public void testReadMoreEntriesRunsConcurrentRequestAfterFailure(boolean classic) throws Exception {
        ManagedCursor cursor = Mockito.mock(ManagedCursor.class);
        AbstractPersistentDispatcherMultipleConsumers dispatcher = createReadConflationDispatcher(cursor, classic);
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

    @DataProvider
    public Object[][] readConflationDispatcherTypes() {
        return new Object[][] {{false}, {true}};
    }

    private AbstractPersistentDispatcherMultipleConsumers createReadConflationDispatcher(ManagedCursor cursor,
                                                                                       boolean classic)
            throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "s1", MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        Mockito.doReturn("s1").when(cursor).getName();
        Subscription subscription = Mockito.mock(PersistentSubscription.class);
        Mockito.doReturn(topic).when(subscription).getTopic();
        return classic ? new PersistentDispatcherMultipleConsumersClassic(topic, cursor, subscription)
                : new PersistentDispatcherMultipleConsumers(topic, cursor, subscription);
    }

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
        PersistentDispatcherMultipleConsumers dispatcher =
                Mockito.spy(new PersistentDispatcherMultipleConsumers(topic, cursor, sub));
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
            AbstractPersistentDispatcherMultipleConsumers dispatcher1 = inv.getArgument(2);
            dispatcher1.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("cursor closed"),
                    null);
            return null;
        }).when(cursor).asyncReadEntriesWithSkipOrWait(Mockito.anyInt(), Mockito.anyLong(), Mockito.eq(dispatcher),
                Mockito.any(), Mockito.any(), Mockito.any());

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
}
