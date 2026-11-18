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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import io.netty.util.concurrent.EventExecutor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.customroute.PartialRoundRobinMessageRouterImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-impl")
public class PartialPartitionedProducerTest extends SharedPulsarBaseTest {

    @Test
    public void testPtWithSinglePartition() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        for (int i = 0; i < 10; i++) {
            producerImpl.newMessage().value("msg".getBytes()).send();
        }
        assertEquals(producerImpl.getProducers().size(), 1);
    }

    @Test(timeOut = 60000)
    public void testLazyStartSendFromSendCallbackDoesNotBlock() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();

        // The first message waits in its batch for the flush below, so it is acknowledged after its callback is
        // installed, and the callback runs on the IO thread that reads the acknowledgement
        final AtomicBoolean sentOnIoThread = new AtomicBoolean();
        final CompletableFuture<MessageId> sends = producer.newMessage().key("0").value("first".getBytes())
                .sendAsync()
                .thenCompose(id -> {
                    sentOnIoThread.set(isOnIoThread(client));
                    final CompletableFuture<MessageId> second =
                            producer.newMessage().key("1").value("second".getBytes()).sendAsync();
                    producer.flushAsync();
                    return second;
                });
        producer.flushAsync();

        assertNotNull(sends.get(10, TimeUnit.SECONDS));
        assertTrue(sentOnIoThread.get());
    }

    @Test(timeOut = 60000)
    public void testIoThreadSendsKeepTheirOrderWhileStarting() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TopicName.get(topic).getPartition(1).toString())
                .subscriptionName("sub")
                .subscribe();
        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic).enableBatching(false).create();

        final List<CompletableFuture<MessageId>> sends = onIoThread(client, () -> {
            final List<CompletableFuture<MessageId>> started = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                started.add(producer.newMessage().key("1").value(("msg-" + i).getBytes()).sendAsync());
            }
            return started;
        });
        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);

        for (int i = 0; i < 20; i++) {
            assertEquals(new String(consumer.receive(10, TimeUnit.SECONDS).getValue()), "msg-" + i);
        }
    }

    @Test(timeOut = 60000)
    public void testFlushIncludesAnIoThreadSendWaitingForItsProducer() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();

        final CompletableFuture<MessageId> send = onIoThread(client,
                () -> producer.newMessage().key("1").value("msg".getBytes()).sendAsync());
        producer.flush();

        // Only a flush sends the batch before its delay
        assertNotNull(send.get(10, TimeUnit.SECONDS));
    }

    @Test(timeOut = 60000)
    public void testCloseCompletesAnIoThreadSendWaitingForItsProducer() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        // A producer closed while it is created never completes its creation, like this one
        final ProducerImpl<byte[]> neverCreated = mockProducer(new CompletableFuture<>());
        final PulsarClientImpl client = spy((PulsarClientImpl) pulsarClient);
        doReturn(neverCreated).when(client).newProducerImpl(any(), eq(1), any(), any(), any(), any(), any());
        final Producer<byte[]> producer = newLazyStartProducer(client).topic(topic).enableBatching(false).create();

        final CompletableFuture<MessageId> send = onIoThread(client,
                () -> producer.newMessage().key("1").value("msg".getBytes()).sendAsync());
        producer.close();

        final Throwable failure = send.handle((id, e) -> e).get(10, TimeUnit.SECONDS);
        assertTrue(FutureUtil.unwrapCompletionException(failure)
                instanceof PulsarClientException.AlreadyClosedException, String.valueOf(failure));
    }

    @DataProvider
    public static Object[][] blockIfQueueFull() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 60000, dataProvider = "blockIfQueueFull")
    public void testIoThreadSendsWhileStartingRespectMaxPendingMessages(boolean blockIfQueueFull) throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .maxPendingMessages(2)
                .blockIfQueueFull(blockIfQueueFull)
                .create();

        final List<CompletableFuture<MessageId>> sends = onIoThread(client, () -> {
            final List<CompletableFuture<MessageId>> started = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                started.add(producer.newMessage().key("1").value(("msg-" + i).getBytes()).sendAsync());
            }
            // Rejected at once, even with blockIfQueueFull: the producer cannot be created before this IO thread
            // is free, and the thread must not wait for room
            assertTrue(started.get(2).isCompletedExceptionally());
            return started;
        });

        FutureUtil.waitForAll(sends.subList(0, 2)).get(10, TimeUnit.SECONDS);
        final Throwable failure = sends.get(2).handle((id, e) -> e).get(10, TimeUnit.SECONDS);
        assertTrue(FutureUtil.unwrapCompletionException(failure)
                instanceof PulsarClientException.ProducerQueueIsFullError, String.valueOf(failure));
    }

    @Test(timeOut = 60000, dataProvider = "blockIfQueueFull")
    public void testIoThreadSendsWhileStartingRespectTheMemoryLimit(boolean blockIfQueueFull) throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = singleIoThreadClientBuilder().memoryLimit(1, SizeUnit.KILO_BYTES).build();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .blockIfQueueFull(blockIfQueueFull)
                .create();

        final List<CompletableFuture<MessageId>> sends = onIoThread(client, () -> {
            final List<CompletableFuture<MessageId>> started = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                started.add(producer.newMessage().key("1").value(new byte[1024]).sendAsync());
            }
            // Rejected while queued: the producer cannot be created before this IO thread is free
            assertTrue(started.get(2).isCompletedExceptionally());
            return started;
        });

        assertNotNull(sends.get(0).get(10, TimeUnit.SECONDS));
        final Throwable failure = sends.get(2).handle((id, e) -> e).get(10, TimeUnit.SECONDS);
        assertTrue(FutureUtil.unwrapCompletionException(failure)
                instanceof PulsarClientException.MemoryBufferIsFullError, String.valueOf(failure));
    }

    @Test(timeOut = 60000)
    public void testQueuedSendsDoNotWaitForEachOthersMemory() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = singleIoThreadClientBuilder().memoryLimit(1, SizeUnit.KILO_BYTES).build();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        // The limit admits one more message once it is reached, so the second is queued with its memory reserved
        final List<CompletableFuture<MessageId>> sends = onIoThread(client, () -> List.of(
                producer.newMessage().key("1").value(new byte[1]).sendAsync(),
                producer.newMessage().key("1").value(new byte[2048]).sendAsync()));

        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
    }

    @Test(timeOut = 60000)
    public void testSendFromTheFailureCallbackOfAQueuedSend() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        // The producer rejects the oversized message as it takes it from the queue, so the callback runs on the
        // thread that hands the queued sends over
        final byte[] oversized = new byte[getConfig().getMaxMessageSize() + 1];
        final CompletableFuture<MessageId> retried = onIoThread(client,
                () -> producer.newMessage().key("1").value(oversized).sendAsync().exceptionallyCompose(failure -> {
                    assertTrue(FutureUtil.unwrapCompletionException(failure)
                            instanceof PulsarClientException.InvalidMessageException, String.valueOf(failure));
                    return producer.newMessage().key("1").value("retry".getBytes()).sendAsync();
                }));

        assertNotNull(retried.get(10, TimeUnit.SECONDS));
    }

    @Test(timeOut = 60000)
    public void testFlushRequestedWhileStartingCoversTheQueuedSends() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = singleIoThreadClientBuilder().memoryLimit(1, SizeUnit.KILO_BYTES).build();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .maxPendingMessages(2)
                .blockIfQueueFull(true)
                .create();

        // The batch buffer of the first send takes more than the memory left, so the second may only use what it
        // reserved when it was queued, and only the flush sends the batches before their delay
        final List<CompletableFuture<MessageId>> sends = onIoThread(client, () -> {
            final List<CompletableFuture<MessageId>> started = List.of(
                    producer.newMessage().key("1").value(new byte[1]).sendAsync(),
                    producer.newMessage().key("1").value(new byte[1]).sendAsync());
            producer.flushAsync();
            return started;
        });

        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
    }

    @Test(timeOut = 60000)
    public void testFlushRequestedWhileStartingCoversASendMadeDuringTheDrain() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final AtomicReference<Producer<byte[]>> producer = new AtomicReference<>();
        final AtomicReference<CompletableFuture<MessageId>> sentDuringDrain = new AtomicReference<>();
        // The third send is queued behind the second, and only a flush sends their batch before its delay
        producer.set(newLazyStartProducer(client, topic)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .maxPendingMessages(3)
                .blockIfQueueFull(true)
                .intercept(sendWhileHandedOver(producer, sentDuringDrain))
                .create());

        final List<CompletableFuture<MessageId>> sends = onIoThread(client, () -> {
            final List<CompletableFuture<MessageId>> started = List.of(
                    producer.get().newMessage().key("1").value("first".getBytes()).sendAsync(),
                    producer.get().newMessage().key("1").value("second".getBytes()).sendAsync());
            producer.get().flushAsync();
            return started;
        });

        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
        assertNotNull(sentDuringDrain.get().get(10, TimeUnit.SECONDS));
    }

    @Test(timeOut = 60000, dataProvider = "blockIfQueueFull")
    public void testSendMadeDuringTheDrainRespectsMaxPendingMessages(boolean blockIfQueueFull) throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final AtomicReference<Producer<byte[]>> producer = new AtomicReference<>();
        final AtomicReference<CompletableFuture<MessageId>> sentDuringDrain = new AtomicReference<>();
        producer.set(newLazyStartProducer(client, topic)
                .enableBatching(false)
                .maxPendingMessages(2)
                .blockIfQueueFull(blockIfQueueFull)
                .intercept(sendWhileHandedOver(producer, sentDuringDrain))
                .create());

        final List<CompletableFuture<MessageId>> sends = onIoThread(client, () -> List.of(
                producer.get().newMessage().key("1").value("first".getBytes()).sendAsync(),
                producer.get().newMessage().key("1").value("second".getBytes()).sendAsync()));

        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
        // The first send is with the producer and the second is still queued: both hold their permit
        final Throwable failure = sentDuringDrain.get().handle((id, e) -> e).get(10, TimeUnit.SECONDS);
        assertTrue(FutureUtil.unwrapCompletionException(failure)
                instanceof PulsarClientException.ProducerQueueIsFullError, String.valueOf(failure));
    }

    // An interceptor runs on the thread that hands a queued send to its producer. This one sends a third message to
    // partition 1 from there, while the first send to it is handed over.
    private static ProducerInterceptor sendWhileHandedOver(AtomicReference<Producer<byte[]>> producer,
                                                           AtomicReference<CompletableFuture<MessageId>> sent) {
        final AtomicBoolean sentOnce = new AtomicBoolean();
        return new PassThroughInterceptor() {
            @Override
            public Message<?> beforeSend(Producer<?> partitionProducer, Message<?> message) {
                if ("1".equals(message.getKey()) && sentOnce.compareAndSet(false, true)) {
                    sent.set(producer.get().newMessage().key("1").value("third".getBytes()).sendAsync());
                }
                return message;
            }
        };
    }

    private abstract static class PassThroughInterceptor implements ProducerInterceptor {
        @Override
        public boolean eligible(Message<?> message) {
            return true;
        }

        @Override
        public Message<?> beforeSend(Producer<?> partitionProducer, Message<?> message) {
            return message;
        }

        @Override
        public void onSendAcknowledgement(Producer<?> partitionProducer, Message<?> message, MessageId msgId,
                                          Throwable exception) {
        }

        @Override
        public void close() {
        }
    }

    @Test(timeOut = 60000)
    public void testSendFromAnInterceptorOfAFailedQueuedSend() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final AtomicReference<Producer<byte[]>> producer = new AtomicReference<>();
        final AtomicReference<CompletableFuture<MessageId>> retried = new AtomicReference<>();
        // The producer rejects the oversized message as it takes it from the queue, and tells the interceptor on the
        // thread that hands the queued sends over, before it fails the send
        final ProducerInterceptor retryOnFailure = new PassThroughInterceptor() {
            @Override
            public void onSendAcknowledgement(Producer<?> partitionProducer, Message<?> message, MessageId msgId,
                                              Throwable exception) {
                if (exception != null && retried.get() == null) {
                    retried.set(producer.get().newMessage().key("1").value("retry".getBytes()).sendAsync());
                }
            }
        };
        producer.set(newLazyStartProducer(client, topic)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .intercept(retryOnFailure)
                .create());

        final byte[] oversized = new byte[getConfig().getMaxMessageSize() + 1];
        final CompletableFuture<MessageId> rejected = onIoThread(client,
                () -> producer.get().newMessage().key("1").value(oversized).sendAsync());

        final Throwable failure = rejected.handle((id, e) -> e).get(10, TimeUnit.SECONDS);
        assertTrue(FutureUtil.unwrapCompletionException(failure)
                instanceof PulsarClientException.InvalidMessageException, String.valueOf(failure));
        assertNotNull(retried.get().get(10, TimeUnit.SECONDS));
    }

    @Test(timeOut = 60000)
    public void testQueuedEmptyMessageKeepsItsPermit() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>)
                newLazyStartProducer(client, topic)
                        .enableBatching(false)
                        .maxPendingMessages(1)
                        .create();

        // An empty message reserves no memory. It still brings its permit along instead of taking a second one.
        final CompletableFuture<MessageId> send = onIoThread(client,
                () -> producer.newMessage().key("1").value(new byte[0]).sendAsync());

        assertNotNull(send.get(10, TimeUnit.SECONDS));
        Awaitility.await().untilAsserted(() -> assertEquals(
                producer.getProducers().get(1).getSemaphore().orElseThrow().availablePermits(), 1));
    }

    @Test(timeOut = 60000)
    public void testQueuedChunkedSendDoesNotWaitForPermits() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient client = newSingleIoThreadClient();
        final PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>)
                newLazyStartProducer(client, topic)
                        .enableBatching(false)
                        .enableChunking(true)
                        .chunkMaxMessageSize(1024)
                        .maxPendingMessages(1)
                        .blockIfQueueFull(true)
                        .create();

        // Each chunk needs a permit. The queued send brings one along, and the thread that hands it over must not
        // wait for the others, which only the acknowledgements that this thread may have to read give back.
        final CompletableFuture<MessageId> send = onIoThread(client,
                () -> producer.newMessage().key("1").value(new byte[3000]).sendAsync());

        final Throwable failure = send.handle((id, e) -> e).get(10, TimeUnit.SECONDS);
        assertTrue(FutureUtil.unwrapCompletionException(failure)
                instanceof PulsarClientException.ProducerQueueIsFullError, String.valueOf(failure));
        Awaitility.await().untilAsserted(() -> assertEquals(
                producer.getProducers().get(1).getSemaphore().orElseThrow().availablePermits(), 1));
    }

    @Test(timeOut = 60000)
    public void testQueuedSendsDoNotNeedTheInternalExecutor() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) newSingleIoThreadClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();
        // Consumers and the transaction coordinator client share the internal executor, whose only thread is busy
        final CountDownLatch release = holdInternalExecutor(client);
        try {
            final CompletableFuture<MessageId> send = onIoThread(client,
                    () -> producer.newMessage().key("1").value(new byte[1]).sendAsync());

            assertNotNull(send.get(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
    }

    @DataProvider
    public static Object[][] transactional() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 60000, dataProvider = "transactional")
    public void testSendFromTheInternalExecutorWhileSendsAreQueued(boolean transactional) throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) newSingleIoThreadTransactionClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        final Transaction txn = transactional ? newTransaction(client) : null;

        // The producer cannot be created before the IO thread is free, so the first send is still queued when the
        // internal executor sends. Its only thread also completes the registration of the topic with the
        // transaction, which the queued send waits for, so it must not wait for the queued send.
        final CompletableFuture<CompletableFuture<MessageId>> fromInternalExecutor = new CompletableFuture<>();
        final CompletableFuture<MessageId> queued = onIoThread(client, () -> {
            final CompletableFuture<MessageId> first = (transactional ? producer.newMessage(txn)
                    : producer.newMessage()).key("1").value("first".getBytes()).sendAsync();
            client.getInternalExecutorService().execute(() -> fromInternalExecutor.complete(
                    producer.newMessage().key("1").value("second".getBytes()).sendAsync()));
            return first;
        });

        assertNotNull(queued.get(10, TimeUnit.SECONDS));
        assertNotNull(fromInternalExecutor.get(10, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS));
    }

    @Test(timeOut = 60000)
    public void testNoThreadWaitsForTheTransactionOfAQueuedSend() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) newSingleIoThreadTransactionClient();
        final PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>)
                newLazyStartProducer(client, topic)
                        .enableBatching(false)
                        .blockIfQueueFull(true)
                        .sendTimeout(0, TimeUnit.SECONDS)
                        .create();
        final Transaction txn = newTransaction(client);
        // The only thread of the internal executor sends the request that registers the topic with the transaction
        // and handles the response, which the only IO thread writes and reads. The topic is registered once the
        // producer is created, so a thread that waited for it to hand the queued send over would wait forever.
        final CountDownLatch release = holdInternalExecutor(client);
        try {
            final CompletableFuture<MessageId> send = onIoThread(client,
                    () -> producer.newMessage(txn).key("1").value("msg".getBytes()).sendAsync());
            Awaitility.await().until(() -> producer.getProducers().get(1).producerCreatedFuture().isDone());
            release.countDown();

            assertNotNull(send.get(10, TimeUnit.SECONDS));
            txn.commit().get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test(timeOut = 60000)
    public void testCommitRightAfterAQueuedTransactionalSend() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TopicName.get(topic).getPartition(1).toString())
                .subscriptionName("sub")
                .subscribe();
        @Cleanup
        final PulsarClient client = newSingleIoThreadTransactionClient();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        final Transaction txn = newTransaction(client);

        // The transaction is open when the send is made, so the commit waits for it, as for a created producer
        final List<CompletableFuture<?>> sendThenCommit = onIoThread(client, () -> List.of(
                producer.newMessage(txn).key("1").value("msg".getBytes()).sendAsync(),
                txn.commit()));

        FutureUtil.waitForAll(sendThenCommit).get(10, TimeUnit.SECONDS);
        assertEquals(new String(consumer.receive(10, TimeUnit.SECONDS).getValue()), "msg");
    }

    @Test(timeOut = 60000)
    public void testCloseSettlesAQueuedSendWaitingForItsTransaction() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) newSingleIoThreadTransactionClient();
        final PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>)
                newLazyStartProducer(client, topic)
                        .enableBatching(false)
                        .maxPendingMessages(1)
                        .sendTimeout(0, TimeUnit.SECONDS)
                        .create();
        final Transaction txn = newTransaction(client);
        // The topic cannot be registered with the transaction while the only thread of the internal executor is busy
        final CountDownLatch release = holdInternalExecutor(client);
        try {
            final CompletableFuture<MessageId> send = onIoThread(client,
                    () -> producer.newMessage(txn).key("1").value(new byte[1]).sendAsync());
            Awaitility.await().until(() -> producer.getProducers().get(1).producerCreatedFuture().isDone());
            assertFalse(send.isDone());
            final ProducerImpl<byte[]> partitionProducer = producer.getProducers().get(1);
            producer.close();

            final Throwable failure = send.handle((id, e) -> e).get(10, TimeUnit.SECONDS);
            assertTrue(FutureUtil.unwrapCompletionException(failure)
                    instanceof PulsarClientException.AlreadyClosedException, String.valueOf(failure));
            assertEquals(partitionProducer.getSemaphore().orElseThrow().availablePermits(), 1);
            assertEquals(client.getMemoryLimitController().currentUsage(), 0);
        } finally {
            release.countDown();
        }
    }

    @Test(timeOut = 60000)
    public void testReplacementProducerDoesNotTakeOverTheQueueOfAFailedOne() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarClient realClient = newSingleIoThreadClient();
        final PulsarClientImpl client = spy((PulsarClientImpl) realClient);
        // The first producer of partition 1 fails its creation when the test says so, its replacement is a real one
        final CompletableFuture<Producer<byte[]>> failedCreation = new CompletableFuture<>();
        final ProducerImpl<byte[]> failed = mockProducer(failedCreation);
        final AtomicBoolean firstProducer = new AtomicBoolean(true);
        doAnswer(invocation -> firstProducer.compareAndSet(true, false) ? failed : invocation.callRealMethod())
                .when(client).newProducerImpl(any(), eq(1), any(), any(), any(), any(), any());
        final Producer<byte[]> producer = newLazyStartProducer(client).topic(topic)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();
        final CountDownLatch release = holdInternalExecutor(client);
        try {
            final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            final CompletableFuture<MessageId> retried = onIoThread(client,
                    () -> producer.newMessage().key("1").value("first".getBytes()).sendAsync()
                            .exceptionallyCompose(failure -> {
                                firstFailure.set(failure);
                                return producer.newMessage().key("1").value("retry".getBytes()).sendAsync();
                            }));
            failedCreation.completeExceptionally(new PulsarClientException("creation failed"));
            final CompletableFuture<MessageId> toReplacement = onIoThread(client,
                    () -> producer.newMessage().key("1").value("second".getBytes()).sendAsync());
            release.countDown();

            // The send queued for the failed producer fails, the sends made after that go to the replacement
            assertNotNull(retried.get(10, TimeUnit.SECONDS));
            assertTrue(FutureUtil.unwrapCompletionException(firstFailure.get())
                    instanceof PulsarClientException.NotConnectedException, String.valueOf(firstFailure.get()));
            assertNotNull(toReplacement.get(10, TimeUnit.SECONDS));
            // The replacement completed its creation, so a thread that waits for it can send
            assertNotNull(CompletableFuture.supplyAsync(
                            () -> producer.newMessage().key("1").value("third".getBytes()).sendAsync().join())
                    .get(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
    }

    @Test(timeOut = 60000)
    public void testFailureCallbackOfAQueuedSendDoesNotWaitForTheReplacementProducer() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        // The first producer of partition 1 fails its creation when the test says so, its replacement is never created
        final CompletableFuture<Producer<byte[]>> failedCreation = new CompletableFuture<>();
        final ProducerImpl<byte[]> failed = mockProducer(failedCreation);
        final ProducerImpl<byte[]> neverCreated = mockProducer(new CompletableFuture<>());
        final PulsarClientImpl client = spy((PulsarClientImpl) pulsarClient);
        doReturn(failed, neverCreated).when(client).newProducerImpl(any(), eq(1), any(), any(), any(), any(), any());
        final Producer<byte[]> producer = newLazyStartProducer(client).topic(topic).enableBatching(false).create();
        try {
            final CompletableFuture<MessageId> retried = onIoThread(client,
                    () -> producer.newMessage().key("1").value("first".getBytes()).sendAsync()
                            .exceptionallyCompose(failure ->
                                    producer.newMessage().key("1").value("retry".getBytes()).sendAsync()));
            // The thread that fails the creation hands the queued send over and runs its callback. It is not an IO
            // thread, and the retry must not make it wait for the replacement either.
            CompletableFuture.runAsync(
                            () -> failedCreation.completeExceptionally(new PulsarClientException("creation failed")))
                    .get(10, TimeUnit.SECONDS);
            assertFalse(retried.isDone());
            producer.close();

            final Throwable failure = retried.handle((id, e) -> e).get(10, TimeUnit.SECONDS);
            assertTrue(FutureUtil.unwrapCompletionException(failure)
                    instanceof PulsarClientException.AlreadyClosedException, String.valueOf(failure));
        } finally {
            producer.closeAsync();
        }
    }

    // A partition producer whose creation the test controls
    @SuppressWarnings("unchecked")
    private static ProducerImpl<byte[]> mockProducer(CompletableFuture<Producer<byte[]>> creation) {
        final ProducerImpl<byte[]> producer = mock(ProducerImpl.class);
        doReturn(creation).when(producer).producerCreatedFuture();
        doReturn(CompletableFuture.completedFuture(null)).when(producer).closeAsync();
        return producer;
    }

    @Test(timeOut = 30000)
    public void testSyncSendToAStartingPartitionIsFlushed() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final Producer<byte[]> producer = newLazyStartProducer(pulsarClient, topic)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();

        assertNotNull(producer.newMessage().key("1").value("msg".getBytes()).send());
    }

    @Test(timeOut = 30000)
    public void testFlushWithFullQueuesWhileStarting() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 3);

        @Cleanup
        final Producer<byte[]> producer = newLazyStartProducer(pulsarClient, topic)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .maxPendingMessages(1)
                .blockIfQueueFull(true)
                .create();

        final CompletableFuture<MessageId> first = producer.newMessage().key("1").value("msg".getBytes()).sendAsync();
        final CompletableFuture<MessageId> second = producer.newMessage().key("2").value("msg".getBytes()).sendAsync();
        producer.flush();

        assertTrue(first.isDone() && second.isDone());
    }

    private PulsarClient newSingleIoThreadClient() throws PulsarClientException {
        return singleIoThreadClientBuilder().build();
    }

    private PulsarClient newSingleIoThreadTransactionClient() throws PulsarClientException {
        return singleIoThreadClientBuilder().enableTransaction(true).build();
    }

    private static Transaction newTransaction(PulsarClient client) throws Exception {
        return client.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get(10, TimeUnit.SECONDS);
    }

    // With one IO thread, a send made on it runs before that thread can read the creation response. The internal
    // executor has as many threads as there are IO threads.
    private ClientBuilder singleIoThreadClientBuilder() {
        return PulsarClient.builder().serviceUrl(getBrokerServiceUrl()).ioThreads(1);
    }

    private static ProducerBuilder<byte[]> newLazyStartProducer(PulsarClient client, String topic) {
        return newLazyStartProducer(client).topic(topic);
    }

    private static ProducerBuilder<byte[]> newLazyStartProducer(PulsarClient client) {
        return client.newProducer()
                .enableLazyStartPartitionedProducers(true)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(routeByKey());
    }

    // Makes the sends on an IO thread of the client
    private static <R> R onIoThread(PulsarClient client, Supplier<R> sends) throws Exception {
        final EventExecutor ioThread = ((PulsarClientImpl) client).eventLoopGroup().next();
        return ioThread.submit(() -> {
            assertTrue(isOnIoThread(client));
            return sends.get();
        }).get(10, TimeUnit.SECONDS);
    }

    private static boolean isOnIoThread(PulsarClient client) {
        for (EventExecutor ioThread : ((PulsarClientImpl) client).eventLoopGroup()) {
            if (ioThread.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    // Keeps a thread of the internal executor busy, its only one for a client with one IO thread, until the
    // returned latch is released
    private static CountDownLatch holdInternalExecutor(PulsarClientImpl client) {
        final CountDownLatch release = new CountDownLatch(1);
        client.getInternalExecutorService().execute(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return release;
    }

    private static MessageRouter routeByKey() {
        return new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                return msg.hasKey() ? Integer.parseInt(msg.getKey()) : 0;
            }
        };
    }

    @Test
    public void testSendAfterCloseDoesNotStartPartitionProducer() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        final PartitionedProducerImpl<byte[]> producerImpl =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return msg.hasKey() ? Integer.parseInt(msg.getKey()) : 0;
                    }
                })
                .create();
        producerImpl.newMessage().key("0").value("msg".getBytes()).send();
        assertEquals(producerImpl.getProducers().size(), 1);
        producerImpl.close();

        assertThrows(PulsarClientException.AlreadyClosedException.class,
                () -> producerImpl.newMessage().key("1").value("msg".getBytes()).send());
        // No producer may be started for partition 1 on a closed producer
        assertEquals(producerImpl.getProducers().size(), 1);
        assertTrue(admin.topics().getStats(TopicName.get(topic).getPartition(1).toString())
                .getPublishers().isEmpty());
    }

    @Test
    public void testCloseRacingLazyStartClosesPartitionProducer() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        final PulsarClientImpl client = spy((PulsarClientImpl) pulsarClient);
        final AtomicReference<Producer<byte[]>> producerRef = new AtomicReference<>();
        // close() runs after the send passed the state check and before the new producer is added
        doAnswer(invocation -> {
            producerRef.get().close();
            return invocation.callRealMethod();
        }).when(client).newProducerImpl(any(), eq(1), any(), any(), any(), any(), any());
        final PartitionedProducerImpl<byte[]> producerImpl =
                (PartitionedProducerImpl<byte[]>) new ProducerBuilderImpl<>(client, Schema.BYTES)
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return msg.hasKey() ? Integer.parseInt(msg.getKey()) : 0;
                    }
                })
                .create();
        producerRef.set(producerImpl);

        assertThrows(PulsarClientException.AlreadyClosedException.class,
                () -> producerImpl.newMessage().key("1").value("msg".getBytes()).send());
        // The send fails when the close starts; the partition producer is closed once its creation completes
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producerImpl.getProducers().size(), 1);
            assertTrue(admin.topics().getStats(TopicName.get(topic).getPartition(1).toString())
                    .getPublishers().isEmpty());
        });
    }

    @Test
    public void testPtWithPartialPartition() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new PartialRoundRobinMessageRouterImpl(3))
                .create();

        for (int i = 0; i < 10; i++) {
            producerImpl.newMessage().value("msg".getBytes()).send();
        }
        assertEquals(producerImpl.getProducers().size(), 3);
    }

    // AddPartitionTest
    @Test
    public void testPtLazyLoading() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        final Supplier<Boolean> send = () -> {
            for (int i = 0; i < 10; i++) {
                try {
                    producerImpl.newMessage().value("msg".getBytes()).send();
                } catch (Throwable e) {
                    return false;
                }
            }
            return true;
        };

        // create first producer at initialization step
        assertEquals(producerImpl.getProducers().size(), 1);

        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 10);
    }

    @Test
    public void testPtLoadingNotSharedMode() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplExclusive =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .accessMode(ProducerAccessMode.Exclusive)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        // create first producer at initialization step
        assertEquals(producerImplExclusive.getProducers().size(), 10);

        producerImplExclusive.close();

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplWaitForExclusive =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        assertEquals(producerImplWaitForExclusive.getProducers().size(), 10);
    }

    // AddPartitionAndLimitTest
    @Test
    public void testPtUpdateWithPartialPartition() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        final Field field = PartitionedProducerImpl.class.getDeclaredField("topicMetadata");
        field.setAccessible(true);
        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new PartialRoundRobinMessageRouterImpl(3))
                .accessMode(ProducerAccessMode.Shared)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .create();

        final Supplier<Boolean> send = ()-> {
            for (int i = 0; i < 10; i++) {
                try {
                    producerImpl.newMessage().value("msg".getBytes()).send();
                } catch (Throwable e) {
                    return false;
                }
            }
            return true;
        };

        // create first producer at initialization step
        assertEquals(producerImpl.getProducers().size(), 1);

        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 2);

        admin.topics().updatePartitionedTopic(topic, 3);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImpl)).numPartitions(), 3));
        assertEquals(producerImpl.getProducers().size(), 2);

        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 3);

        admin.topics().updatePartitionedTopic(topic, 4);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImpl)).numPartitions(), 4));
        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 3);
    }

    @Test
    public void testPtUpdateNotSharedMode() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        final Field field = PartitionedProducerImpl.class.getDeclaredField("topicMetadata");
        field.setAccessible(true);
        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplExclusive =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .accessMode(ProducerAccessMode.Exclusive)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .create();

        assertEquals(producerImplExclusive.getProducers().size(), 2);

        admin.topics().updatePartitionedTopic(topic, 3);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImplExclusive)).numPartitions(), 3));
        assertEquals(producerImplExclusive.getProducers().size(), 3);

        producerImplExclusive.close();

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplWaitForExclusive =
                (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .create();

        assertEquals(producerImplWaitForExclusive.getProducers().size(), 3);

        admin.topics().updatePartitionedTopic(topic, 4);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImplWaitForExclusive)).numPartitions(), 4));
        assertEquals(producerImplWaitForExclusive.getProducers().size(), 4);
    }
}
