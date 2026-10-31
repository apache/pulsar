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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.CustomLog;
import lombok.Data;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Different with {@link org.apache.pulsar.client.api.SimpleProducerConsumerTest}, this class can visit the variables
 * of {@link ConsumerImpl} or {@link ProducerImpl} which have protected or default access modifiers.
 */
@CustomLog
@Test(groups = "broker-impl")
public class ProducerConsumerInternalTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] failedFlushTransactions() {
        List<Object[]> cases = new ArrayList<>();
        for (BatcherBuilder builder : List.of(BatcherBuilder.DEFAULT, BatcherBuilder.KEY_BASED,
                new EntryBucketBatcherBuilder(List.of(0x4000, 0x8000, 0xC000)))) {
            cases.add(new Object[] {builder, true});
            cases.add(new Object[] {builder, false});
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "failedFlushTransactions", timeOut = 60000)
    public void testTransactionBoundaryAfterFailedFlush(BatcherBuilder builder, boolean abort) throws Exception {
        String topic = newTopicName();
        AtomicInteger failures = new AtomicInteger();
        try (PulsarClient client = PulsarClient.builder().serviceUrl(getBrokerServiceUrl())
                .enableTransaction(true).memoryLimit(2700, SizeUnit.BYTES).build();
             Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub").subscribe();
             Producer<byte[]> producer = client.newProducer().topic(topic).batcherBuilder(builder)
                     .batchingMaxPublishDelay(1, TimeUnit.DAYS).create()) {
            PulsarClientImpl clientImpl = (PulsarClientImpl) client;
            // Use the real allocation accounting and its pressure callback, without replacing the producer,
            // batch containers, crypto, transport or transaction coordinator with mocks.
            clientImpl.getMemoryLimitController().registerTrigger(() -> {
                if (failures.getAndIncrement() == 0) {
                    throw new OutOfMemoryError("injected batch growth failure");
                }
            });
            Transaction txn = client.newTransaction().withTransactionTimeout(30, TimeUnit.SECONDS).build()
                    .get(10, TimeUnit.SECONDS);
            CompletableFuture<MessageId> first = producer.newMessage(txn).key("key-1").value(new byte[500])
                    .sendAsync();
            CompletableFuture<MessageId> second = producer.newMessage(txn).key("key-1").value(new byte[500])
                    .sendAsync();
            ProducerImpl<byte[]> producerImpl = (ProducerImpl<byte[]>) producer;
            Awaitility.await().untilAsserted(() -> assertThat(producerImpl.getPendingQueueSize()).isEqualTo(2));
            CompletableFuture<MessageId> plain = producer.newMessage().key("key-1").value(new byte[32]).sendAsync();
            assertThat(failures).hasValue(1);
            producer.flushAsync().get(10, TimeUnit.SECONDS);
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            if (abort) {
                txn.abort().get(10, TimeUnit.SECONDS);
            } else {
                txn.commit().get(10, TimeUnit.SECONDS);
            }
            assertThat(plain).as("the incompatible message must fail instead of joining the retained transaction")
                    .isCompletedExceptionally();

            CompletableFuture<MessageId> retry = producer.newMessage().key("key-1").value(new byte[32]).sendAsync();
            producer.flushAsync().get(10, TimeUnit.SECONDS);
            retry.get(10, TimeUnit.SECONDS);
            if (!abort) {
                assertThat(consumer.receive(10, TimeUnit.SECONDS).getData()).hasSize(500);
                assertThat(consumer.receive(10, TimeUnit.SECONDS).getData()).hasSize(500);
            }
            Message<byte[]> delivered = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(delivered).as("the retried plain message must be delivered after transaction completion")
                    .isNotNull();
            assertThat(delivered.getData()).hasSize(32);
            assertThat(consumer.receive(100, TimeUnit.MILLISECONDS)).isNull();
            assertThat(clientImpl.getMemoryLimitController().currentUsage()).isZero();
        }
    }

    @DataProvider
    public Object[][] failedAddBuilders() {
        return new Object[][] {
                {BatcherBuilder.KEY_BASED},
                {new EntryBucketBatcherBuilder(List.of(0x4000, 0x8000, 0xC000))}
        };
    }

    @Test(dataProvider = "failedAddBuilders", timeOut = 30000)
    public void testSendToAnotherBucketAfterFailedAdd(BatcherBuilder builder) throws Exception {
        String topic = newTopicName();
        AtomicInteger failures = new AtomicInteger();
        try (PulsarClient client = PulsarClient.builder().serviceUrl(getBrokerServiceUrl())
                .memoryLimit(1000, SizeUnit.BYTES).build();
             Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub").subscribe();
             Producer<byte[]> producer = client.newProducer().topic(topic).batcherBuilder(builder)
                     .batchingMaxPublishDelay(20, TimeUnit.MILLISECONDS).create()) {
            PulsarClientImpl clientImpl = (PulsarClientImpl) client;
            clientImpl.getMemoryLimitController().registerTrigger(() -> {
                if (failures.getAndIncrement() == 0) {
                    throw new OutOfMemoryError("injected first batch allocation failure");
                }
            });
            CompletableFuture<MessageId> recovered;
            // Keep the real flush timer out until both adds have finished, then let only the timer send.
            synchronized (producer) {
                CompletableFuture<MessageId> failed = producer.newMessage().key("key-1").value(new byte[16])
                        .sendAsync();
                assertThat(failed).isCompletedExceptionally();
                assertThat(clientImpl.getMemoryLimitController().currentUsage()).isZero();
                recovered = producer.newMessage().key("key-2").value(new byte[16]).sendAsync();
            }
            recovered.get(10, TimeUnit.SECONDS);
            Message<byte[]> delivered = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(delivered).isNotNull();
            assertThat(delivered.getKey()).isEqualTo("key-2");
            assertThat(consumer.receive(100, TimeUnit.MILLISECONDS)).isNull();
            assertThat(clientImpl.getMemoryLimitController().currentUsage()).isZero();
        }
    }

    @Test
    public void testSameProducerRegisterTwice() throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);

        // Create producer using default producerName.
        ProducerImpl producer = (ProducerImpl) pulsarClient.newProducer().topic(topicName).create();
        ServiceProducer serviceProducer = getServiceProducer(producer, topicName);

        // Remove producer maintained by server cnx. To make it can register the second time.
        removeServiceProducerMaintainedByServerCnx(serviceProducer);

        // Trigger the client producer reconnect.
        CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        commandCloseProducer.setProducerId(producer.producerId);
        producer.getClientCnx().handleCloseProducer(commandCloseProducer);

        // Verify the reconnection will be success.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getState().toString(), "Ready");
        });
    }

    @Test
    public void testSameProducerRegisterTwiceWithSpecifiedProducerName() throws Exception {
        final String topicName = newTopicName();
        final String pName = "p1";
        admin.topics().createNonPartitionedTopic(topicName);

        // Create producer using default producerName.
        ProducerImpl producer = (ProducerImpl) pulsarClient.newProducer().producerName(pName).topic(topicName).create();
        ServiceProducer serviceProducer = getServiceProducer(producer, topicName);

        // Remove producer maintained by server cnx. To make it can register the second time.
        removeServiceProducerMaintainedByServerCnx(serviceProducer);

        // Trigger the client producer reconnect.
        CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        commandCloseProducer.setProducerId(producer.producerId);
        producer.getClientCnx().handleCloseProducer(commandCloseProducer);

        // Verify the reconnection will be success.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getState().toString(), "Ready", "The producer registration failed");
        });
    }

    private ServiceProducer getServiceProducer(ProducerImpl clientProducer, String topicName) {
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).join().get();
        org.apache.pulsar.broker.service.Producer serviceProducer =
                persistentTopic.getProducers().get(clientProducer.getProducerName());
        long clientProducerId = WhiteboxImpl.getInternalState(clientProducer, "producerId");
        assertEquals(serviceProducer.getProducerId(), clientProducerId);
        assertEquals(serviceProducer.getEpoch(), clientProducer.getConnectionHandler().getEpoch());
        return new ServiceProducer(serviceProducer, persistentTopic);
    }

    private void removeServiceProducerMaintainedByServerCnx(ServiceProducer serviceProducer) {
        ServerCnx serverCnx = (ServerCnx) serviceProducer.getServiceProducer().getCnx();
        serverCnx.removedProducer(serviceProducer.getServiceProducer());
        Awaitility.await().untilAsserted(() -> {
            assertFalse(serverCnx.getProducers().containsKey(serviceProducer.getServiceProducer().getProducerId()));
        });
    }

    @Data
    @AllArgsConstructor
    private static class ServiceProducer {
        private org.apache.pulsar.broker.service.Producer serviceProducer;
        private PersistentTopic persistentTopic;
    }

    @Test(groups = "flaky")
    public void testExclusiveConsumerWillAlwaysRetryEvenIfReceivedConsumerBusyError() throws Exception {
        final String topicName = newTopicName();
        final String subscriptionName = "subscription1";
        admin.topics().createNonPartitionedTopic(topicName);

        final ConsumerImpl consumer = (ConsumerImpl) pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionType(SubscriptionType.Exclusive).subscriptionName(subscriptionName).subscribe();

        ClientCnx clientCnx = consumer.getClientCnx();
        ServerCnx serverCnx = (ServerCnx) getTopic(topicName, false).join().get()
                .getSubscription(subscriptionName)
                .getDispatcher().getConsumers().get(0).cnx();

        // Make a disconnect to trigger broker remove the consumer which related this connection.
        // Make the second subscribe runs after the broker removing the old consumer, then it will receive
        // an error: "Exclusive consumer is already connected"
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        serverCnx.execute(() -> {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        clientCnx.close();
        Thread.sleep(1000);
        countDownLatch.countDown();

        // Verify the consumer will always retry subscribe event received ConsumerBusy error.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(consumer.getState(), HandlerState.State.Ready);
        });

        // cleanup.
        consumer.close();
    }

    @Test(timeOut = 60000)
    public void testSameConsumerSubscribeTwiceRedeliversDispatchedMessages() throws Exception {
        final String topicName = newTopicName();
        final int numMessages = 5;
        admin.topics().createNonPartitionedTopic(topicName);

        @Cleanup
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName("sub").receiverQueueSize(10)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes());
        }
        Awaitility.await().untilAsserted(() -> assertEquals(consumer.numMessagesInQueue(), numMessages));

        // A superseded subscribe attempt that fails schedules a reconnect while the consumer is still live on the
        // broker: the client clears its queue and sends a second subscribe for the same consumer id on the same
        // connection.
        consumer.getConnectionHandler().reconnectLater(new PulsarClientException("injected"));
        assertEquals(consumer.getState(), HandlerState.State.Connecting);
        Awaitility.await().untilAsserted(() -> assertEquals(consumer.getState(), HandlerState.State.Ready));

        // The messages dropped from the client queue must be redelivered, not left unacked on the broker.
        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(msg, "Message " + i + " was not redelivered");
            consumer.acknowledge(msg);
        }
    }

    @DataProvider(name = "containerBuilder")
    public Object[][] containerBuilderProvider() {
        return new Object[][] {
                { BatcherBuilder.DEFAULT },
                { BatcherBuilder.KEY_BASED }
        };
    }

    @Test(timeOut = 30000, dataProvider = "containerBuilder")
    public void testSendTimerCheckForBatchContainer(BatcherBuilder batcherBuilder) throws Exception {
        final String topicName = newTopicName();
        @Cleanup Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batcherBuilder(batcherBuilder)
                .sendTimeout(1, TimeUnit.SECONDS)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(1000)
                .create();

        log.info().attr("msg", System.nanoTime()).log("Before sendAsync msg-0");
        CompletableFuture<MessageId> future = producer.sendAsync("msg-0".getBytes());
        future.thenAccept(msgId -> log.info().attr("time", System.nanoTime()).attr("msgId", msgId)
                .log("msg-0 done"));
        future.get(); // t: the current time point

        ((ProducerImpl<byte[]>) producer).triggerSendTimer(); // t+1000ms && t+2000ms: run() will be called again

        Thread.sleep(1950); // t+2050ms: the batch timer is expired, which happens after run() is called
        log.info().attr("msg", System.nanoTime()).log("Before sendAsync msg-1");
        future = producer.sendAsync("msg-1".getBytes());
        future.thenAccept(msgId -> log.info().attr("time", System.nanoTime()).attr("msgId", msgId)
                .log("msg-1 done"));
        future.get();
    }


    @Test
    public void testRetentionPolicyByProducingMessages() throws Exception {
        // Disable dedup so the pulsar.dedup cursor doesn't block ledger trimming
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);
        // Setup: configure the entries per ledger and retention polices.
        final int maxEntriesPerLedger = 10, messagesCount = 10;
        final String topicName = newTopicName();
        SharedPulsarCluster.get().getPulsarService().getConfiguration()
                .setManagedLedgerMaxEntriesPerLedger(maxEntriesPerLedger);
        SharedPulsarCluster.get().getPulsarService().getConfiguration()
                .setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        SharedPulsarCluster.get().getPulsarService().getConfiguration()
                .setDefaultRetentionTimeInMinutes(0);
        SharedPulsarCluster.get().getPulsarService().getConfiguration()
                .setDefaultRetentionSizeInMB(0);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .sendTimeout(1, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub")
                .subscribe();
        // Act: prepare a full ledger data and ack them.
        for (int i = 0; i < messagesCount; i++) {
            producer.newMessage().sendAsync();
        }
        for (int i = 0; i < messagesCount; i++) {
            Message<byte[]> message = consumer.receive();
            assertNotNull(message);
            consumer.acknowledge(message);
        }
        // Verify: a new empty ledger will be created after the current ledger is fulled.
        // And the previous consumed ledgers will be deleted
        Awaitility.await().untilAsserted(() -> {
            admin.topics().trimTopic(topicName);
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStatsAsync(topicName).get();
            assertEquals(internalStats.currentLedgerEntries, 0);
            assertEquals(internalStats.ledgers.size(), 1);
        });
    }


    @Test
    public void testProducerCompressionMinMsgBodySize() throws PulsarClientException {
        byte[] msg1024 = new byte[1024];
        byte[] msg1025 = new byte[1025];
        final String topicName = newTopicName();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topicName)
                .producerName("producer")
                .compressionType(CompressionType.LZ4)
                .compressionMinMsgBodySize(1024)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("sub")
                .subscribe();

        // disable batch
        producer.conf.setBatchingEnabled(false);
        producer.newMessage().value(msg1024).send();
        MessageImpl<byte[]> message = (MessageImpl<byte[]>) consumer.receive();
        CompressionType compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.NONE);
        producer.newMessage().value(msg1025).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.LZ4);

        // enable batch
        producer.conf.setBatchingEnabled(true);
        producer.newMessage().value(msg1024).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.NONE);
        producer.newMessage().value(msg1025).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.LZ4);

        // Verify data integrity
        String data = "compression test message";
        producer.conf.setBatchingEnabled(true);
        producer.getConfiguration().setCompressMinMsgBodySize(1);
        producer.newMessage().value(data.getBytes()).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        assertEquals(new String(message.getData()), data);

        producer.conf.setBatchingEnabled(false);
        producer.getConfiguration().setCompressMinMsgBodySize(1);
        producer.newMessage().value(data.getBytes()).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        assertEquals(new String(message.getData()), data);

    }
}
