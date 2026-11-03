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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
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
        final Producer<byte[]> producer = newLazyStartProducer(client, topic).enableBatching(false).create();

        final CompletableFuture<MessageId> sends = producer.newMessage().key("0").value("first".getBytes())
                .sendAsync()
                .thenCompose(id -> producer.newMessage().key("1").value("second".getBytes()).sendAsync());

        assertNotNull(sends.get(10, TimeUnit.SECONDS));
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

        final List<CompletableFuture<MessageId>> sends = fromSendCallback(producer, p -> {
            final List<CompletableFuture<MessageId>> started = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                started.add(p.newMessage().key("1").value(("msg-" + i).getBytes()).sendAsync());
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

        final CompletableFuture<MessageId> send = fromSendCallback(producer,
                p -> p.newMessage().key("1").value("msg".getBytes()).sendAsync());
        producer.flush();

        // Only a flush sends the batch before its delay
        assertNotNull(send.get(10, TimeUnit.SECONDS));
    }

    @Test(timeOut = 60000)
    public void testCloseCompletesAnIoThreadSendWaitingForItsProducer() throws Throwable {
        final String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 2);

        // A producer closed while it is created never completes its creation, like this one
        @SuppressWarnings("unchecked")
        final ProducerImpl<byte[]> neverCreated = mock(ProducerImpl.class);
        doReturn(new CompletableFuture<>()).when(neverCreated).producerCreatedFuture();
        doReturn(CompletableFuture.completedFuture(null)).when(neverCreated).closeAsync();
        final PulsarClientImpl client = spy((PulsarClientImpl) pulsarClient);
        doReturn(neverCreated).when(client).newProducerImpl(any(), eq(1), any(), any(), any(), any(), any());
        final Producer<byte[]> producer = new ProducerBuilderImpl<>(client, Schema.BYTES)
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(routeByKey())
                .create();

        final CompletableFuture<MessageId> send = fromSendCallback(producer,
                p -> p.newMessage().key("1").value("msg".getBytes()).sendAsync());
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

        final List<CompletableFuture<MessageId>> sends = fromSendCallback(producer, p -> {
            final List<CompletableFuture<MessageId>> started = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                started.add(p.newMessage().key("1").value(("msg-" + i).getBytes()).sendAsync());
            }
            // Rejected at once, even with blockIfQueueFull: the producer cannot be created before this callback
            // returns, and this IO thread must not wait for room
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
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .ioThreads(1)
                .memoryLimit(1, SizeUnit.KILO_BYTES)
                .build();
        final Producer<byte[]> producer = newLazyStartProducer(client, topic)
                .enableBatching(false)
                .blockIfQueueFull(blockIfQueueFull)
                .create();

        final List<CompletableFuture<MessageId>> sends = fromSendCallback(producer, p -> {
            final List<CompletableFuture<MessageId>> started = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                started.add(p.newMessage().key("1").value(new byte[1024]).sendAsync());
            }
            // Rejected while queued: the producer cannot be created before this callback returns
            assertTrue(started.get(2).isCompletedExceptionally());
            return started;
        });

        assertNotNull(sends.get(0).get(10, TimeUnit.SECONDS));
        final Throwable failure = sends.get(2).handle((id, e) -> e).get(10, TimeUnit.SECONDS);
        assertTrue(FutureUtil.unwrapCompletionException(failure)
                instanceof PulsarClientException.MemoryBufferIsFullError, String.valueOf(failure));
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
        // With one IO thread, a send made on it runs before that thread can read the creation response
        return PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .ioThreads(1)
                .build();
    }

    private static ProducerBuilder<byte[]> newLazyStartProducer(PulsarClient client, String topic) {
        return client.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(routeByKey());
    }

    // Makes the sends in the callback of a send to partition 0, which runs on an IO thread
    private static <R> R fromSendCallback(Producer<byte[]> producer, Function<Producer<byte[]>, R> sends)
            throws Exception {
        final CompletableFuture<MessageId> first = producer.newMessage().key("0").value("first".getBytes())
                .sendAsync();
        // Sends the first message even if it waits in a batch
        producer.flushAsync();
        return first.thenApply(id -> sends.apply(producer)).get(10, TimeUnit.SECONDS);
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
