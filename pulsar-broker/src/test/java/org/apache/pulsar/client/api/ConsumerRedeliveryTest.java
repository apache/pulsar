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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@CustomLog
public class ConsumerRedeliveryTest extends SharedPulsarBaseTest {

    @DataProvider(name = "individualRedeliverySubscriptionTypes")
    public Object[][] individualRedeliverySubscriptionTypes() {
        return new Object[][] { { SubscriptionType.Shared }, { SubscriptionType.Key_Shared } };
    }

    @DataProvider(name = "conflictingBatchAckRedeliveryVariants")
    public Object[][] conflictingBatchAckRedeliveryVariants() {
        return new Object[][] {
                { SubscriptionType.Shared, false }, { SubscriptionType.Shared, true },
                { SubscriptionType.Key_Shared, false }, { SubscriptionType.Key_Shared, true }
        };
    }

    @Test(dataProvider = "conflictingBatchAckRedeliveryVariants", timeOut = 60_000)
    public void testSelectiveRedeliveryAfterConflictingTransactionalBatchAck(SubscriptionType subscriptionType,
                                                                            boolean includePendingPosition)
            throws Exception {
        String topic = newTopicName();
        @Cleanup
        PulsarClient transactionClient = PulsarClient.builder().serviceUrl(getBrokerServiceUrl())
                .enableTransaction(true).build();
        @Cleanup
        Producer<Integer> producer = transactionClient.newProducer(Schema.INT32).topic(topic)
                .batcherBuilder(BatcherBuilder.KEY_BASED).batchingMaxMessages(2)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create();
        @Cleanup
        ConsumerImpl<Integer> consumer = (ConsumerImpl<Integer>) transactionClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("sub").subscriptionType(subscriptionType)
                .enableBatchIndexAcknowledgment(true).isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            sends.add(producer.newMessage().key("key").value(i).sendAsync());
        }
        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
        Message<Integer> first = consumer.receive(10, TimeUnit.SECONDS);
        Message<Integer> last = consumer.receive(10, TimeUnit.SECONDS);
        assertThat(first).isNotNull();
        assertThat(last).isNotNull();
        List<Message<Integer>> unacked = new ArrayList<>();
        unacked.add(last);
        if (includePendingPosition) {
            sends.clear();
            for (int i = 2; i < 4; i++) {
                sends.add(producer.newMessage().key("key").value(i).sendAsync());
            }
            FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
            for (int i = 0; i < 2; i++) {
                Message<Integer> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                unacked.add(message);
            }
        }
        Transaction transaction = transactionClient.newTransaction().withTransactionTimeout(2, TimeUnit.MINUTES)
                .build().get(10, TimeUnit.SECONDS);
        Transaction conflictingTransaction = transactionClient.newTransaction()
                .withTransactionTimeout(2, TimeUnit.MINUTES).build().get(10, TimeUnit.SECONDS);
        try {
            consumer.acknowledgeAsync(first.getMessageId(), transaction).get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> consumer.acknowledgeAsync(first.getMessageId(), conflictingTransaction)
                    .get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(PulsarClientException.TransactionConflictException.class);
            transaction.commit().get(10, TimeUnit.SECONDS);

            Set<MessageId> requested = unacked.stream().map(Message::getMessageId).collect(Collectors.toSet());
            consumer.redeliverUnacknowledgedMessages(requested);
            Set<MessageId> redelivered = new HashSet<>();
            for (int i = 0; i < unacked.size(); i++) {
                Message<Integer> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).as("untouched batch indexes must remain selectively redeliverable").isNotNull();
                redelivered.add(message.getMessageId());
                consumer.acknowledgeAsync(message).get(10, TimeUnit.SECONDS);
            }
            assertThat(redelivered).containsExactlyInAnyOrderElementsOf(requested);
        } finally {
            if (transaction.getState() == Transaction.State.OPEN) {
                transaction.abort().get(10, TimeUnit.SECONDS);
            }
            conflictingTransaction.abort().get(10, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "individualRedeliverySubscriptionTypes", timeOut = 90_000)
    public void testZeroCountSelectiveRedeliveryPreservesBackpressure(SubscriptionType subscriptionType)
            throws Exception {
        String topic = newTopicName();
        int maxUnackedMessages = 10;
        int batchSize = 2;
        admin.namespaces().setMaxUnackedMessagesPerConsumer(getNamespace(), maxUnackedMessages);
        @Cleanup
        PulsarClient transactionClient = PulsarClient.builder().serviceUrl(getBrokerServiceUrl())
                .enableTransaction(true).build();
        @Cleanup
        Producer<Integer> producer = transactionClient.newProducer(Schema.INT32).topic(topic)
                .batcherBuilder(BatcherBuilder.KEY_BASED).batchingMaxMessages(batchSize)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create();
        @Cleanup
        ConsumerImpl<Integer> consumer = (ConsumerImpl<Integer>) transactionClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("sub").subscriptionType(subscriptionType)
                .receiverQueueSize(maxUnackedMessages).enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
        PersistentSubscription subscription = (PersistentSubscription) getTopic(topic, false)
                .get(10, TimeUnit.SECONDS).orElseThrow().getSubscription("sub");
        var brokerConsumer = subscription.getConsumers().get(0);
        Awaitility.await().untilAsserted(() ->
                assertThat(brokerConsumer.getMaxUnackedMessages()).isEqualTo(maxUnackedMessages));
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            sends.add(producer.newMessage().key("key").value(i).sendAsync());
        }
        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
        Message<Integer> first = consumer.receive(10, TimeUnit.SECONDS);
        Message<Integer> last = consumer.receive(10, TimeUnit.SECONDS);
        assertThat(first).isNotNull();
        assertThat(last).isNotNull();
        MessageIdImpl zeroCountPosition = (MessageIdImpl) last.getMessageId();
        Transaction transaction = transactionClient.newTransaction().withTransactionTimeout(2, TimeUnit.MINUTES)
                .build().get(10, TimeUnit.SECONDS);
        CountDownLatch executorPaused = new CountDownLatch(1);
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        Future<?> pauseTask = null;
        CompletableFuture<Void> finalAck = null;
        try {
            consumer.acknowledgeAsync(first.getMessageId(), transaction).get(10, TimeUnit.SECONDS);
            PendingAckHandleImpl handle = (PendingAckHandleImpl) subscription.getPendingAckHandle();
            // Hold the transaction completion path while the real ACK command updates consumer accounting.
            pauseTask = handle.getInternalPinnedExecutor().submit(() -> {
                executorPaused.countDown();
                assertThat(releaseExecutor.await(60, TimeUnit.SECONDS)).isTrue();
                return null;
            });
            assertThat(executorPaused.await(10, TimeUnit.SECONDS)).isTrue();
            finalAck = consumer.acknowledgeAsync(last.getMessageId(), transaction);
            Awaitility.await().untilAsserted(() -> {
                AtomicInteger remainingUnacked = new AtomicInteger(-1);
                brokerConsumer.getPendingAcks().forEach((ledgerId, entryId, count, stickyKeyHash) -> {
                    if (ledgerId == zeroCountPosition.getLedgerId() && entryId == zeroCountPosition.getEntryId()) {
                        remainingUnacked.set(count);
                    }
                });
                assertThat(remainingUnacked).hasValue(0);
            });
            assertThat(finalAck).isNotDone();

            sends.clear();
            for (int i = 0; i < maxUnackedMessages; i++) {
                sends.add(producer.newMessage().key("key").value(i + batchSize).sendAsync());
            }
            FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
            for (int i = 0; i < maxUnackedMessages; i++) {
                Message<Integer> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
            }
            consumer.getLastMessageIdsAsync().get(10, TimeUnit.SECONDS);
            assertThat(brokerConsumer.isBlocked()).isTrue();
            assertThat(brokerConsumer.getUnackedMessages()).isEqualTo(maxUnackedMessages);
            consumer.increaseAvailablePermits(maxUnackedMessages);
            consumer.getLastMessageIdsAsync().get(10, TimeUnit.SECONDS);
            int availablePermits = brokerConsumer.getAvailablePermits();
            consumer.redeliverUnacknowledgedMessages(Set.of(last.getMessageId()));
            consumer.getLastMessageIdsAsync().get(10, TimeUnit.SECONDS);
            assertThat(brokerConsumer.isBlocked()).as("zero-count positions must not release backpressure").isTrue();
            assertThat(brokerConsumer.getUnackedMessages()).isEqualTo(maxUnackedMessages);
            assertThat(brokerConsumer.getAvailablePermits()).isEqualTo(availablePermits);
        } finally {
            releaseExecutor.countDown();
            if (pauseTask != null) {
                pauseTask.get(10, TimeUnit.SECONDS);
            }
            if (finalAck != null) {
                finalAck.get(10, TimeUnit.SECONDS);
            }
            transaction.commit().get(10, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "individualRedeliverySubscriptionTypes", timeOut = 30_000)
    public void testStaleSelectiveRedeliveryDoesNotUnblockConsumer(SubscriptionType subscriptionType) throws Exception {
        String topic = newTopicName();
        int maxUnackedMessages = 10;
        admin.namespaces().setMaxUnackedMessagesPerConsumer(getNamespace(), maxUnackedMessages);
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic).enableBatching(false).create();
        @Cleanup
        ConsumerImpl<Integer> consumer = (ConsumerImpl<Integer>) pulsarClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("sub").subscriptionType(subscriptionType)
                .receiverQueueSize(maxUnackedMessages).isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
        PersistentSubscription subscription = (PersistentSubscription) getTopic(topic, false)
                .get(10, TimeUnit.SECONDS).orElseThrow().getSubscription("sub");
        var brokerConsumer = subscription.getConsumers().get(0);
        Awaitility.await().untilAsserted(() ->
                assertThat(brokerConsumer.getMaxUnackedMessages()).isEqualTo(maxUnackedMessages));

        producer.send(-1);
        Message<Integer> ackedMessage = consumer.receive(10, TimeUnit.SECONDS);
        assertThat(ackedMessage).isNotNull();
        consumer.acknowledgeAsync(ackedMessage).get(10, TimeUnit.SECONDS);
        Awaitility.await().untilAsserted(() -> assertThat(brokerConsumer.getUnackedMessages()).isZero());

        for (int i = 0; i < maxUnackedMessages; i++) {
            producer.send(i);
        }
        List<Message<Integer>> unackedMessages = new ArrayList<>();
        for (int i = 0; i < maxUnackedMessages; i++) {
            Message<Integer> message = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            unackedMessages.add(message);
        }
        // The response on the same connection also fences the FLOW commands sent while receiving.
        consumer.getLastMessageIdAsync().get(10, TimeUnit.SECONDS);
        assertThat(brokerConsumer.isBlocked()).isTrue();
        assertThat(brokerConsumer.getUnackedMessages()).isEqualTo(maxUnackedMessages);
        int availablePermits = brokerConsumer.getAvailablePermits();

        // An ACK timeout or negative ACK can arrive after the original ACK has already removed the position.
        consumer.redeliverUnacknowledgedMessages(Set.of(ackedMessage.getMessageId()));
        consumer.getLastMessageIdAsync().get(10, TimeUnit.SECONDS);
        assertThat(brokerConsumer.isBlocked()).as("stale redelivery must preserve unacked backpressure").isTrue();
        assertThat(brokerConsumer.getUnackedMessages()).isEqualTo(maxUnackedMessages);
        assertThat(brokerConsumer.getAvailablePermits()).isEqualTo(availablePermits);

        // A mixed request still redelivers its valid position, even above the ACK resume threshold.
        MessageId redeliveryId = unackedMessages.get(0).getMessageId();
        consumer.redeliverUnacknowledgedMessages(Set.of(ackedMessage.getMessageId(), redeliveryId));
        Message<Integer> redelivered = consumer.receive(10, TimeUnit.SECONDS);
        assertThat(redelivered).isNotNull();
        assertThat(redelivered.getMessageId()).isEqualTo(redeliveryId);
        consumer.getLastMessageIdAsync().get(10, TimeUnit.SECONDS);
        assertThat(brokerConsumer.getUnackedMessages()).isEqualTo(maxUnackedMessages);
        assertThat(brokerConsumer.isBlocked()).isTrue();
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @DataProvider(name = "batchedMessageAck")
    public Object[][] batchedMessageAck() {
        // When batch index ack is disabled (by default), only after all single messages were sent would the pending
        // ACK be added into the ACK tracker.
        return new Object[][] {
                // numAcked, batchSize, ack type
                { 3, 5, CommandAck.AckType.Individual },
                { 5, 5, CommandAck.AckType.Individual },
                { 3, 5, CommandAck.AckType.Cumulative },
                { 5, 5, CommandAck.AckType.Cumulative }
        };
    }

    /**
     * It verifies that redelivered messages are sorted based on the ledger-ids.
     * <pre>
     * 1. client publishes 100 messages across 50 ledgers
     * 2. broker delivers 100 messages to consumer
     * 3. consumer ack every alternative message and doesn't ack 50 messages
     * 4. broker sorts replay messages based on ledger and redelivers messages ledger by ledger
     * </pre>
     * @throws Exception
     */
    @Test(dataProvider = "ackReceiptEnabled")
    public void testOrderedRedelivery(boolean ackReceiptEnabled) throws Exception {
        String topic = newTopicName();

        int origMaxEntries = getConfig().getManagedLedgerMaxEntriesPerLedger();
        int origMinRollover = getConfig().getManagedLedgerMinLedgerRolloverTimeMinutes();
        getConfig().setManagedLedgerMaxEntriesPerLedger(2);
        getConfig().setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        try {
            @Cleanup
            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(topic)
                    .producerName("my-producer-name")
                    .create();
            ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topic).subscriptionName("s1")
                    .subscriptionType(SubscriptionType.Shared)
                    .isAckReceiptEnabled(ackReceiptEnabled);
            ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();

            final int totalMsgs = 100;

            for (int i = 0; i < totalMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            int consumedCount = 0;
            Set<MessageId> messageIds = new HashSet<>();
            for (int i = 0; i < totalMsgs; i++) {
                Message<byte[]> message = consumer1.receive(5, TimeUnit.SECONDS);
                if (message != null && (consumedCount % 2) == 0) {
                    consumer1.acknowledge(message);
                } else {
                    messageIds.add(message.getMessageId());
                }
                consumedCount += 1;
            }
            assertEquals(totalMsgs, consumedCount);

            // redeliver all unack messages
            consumer1.redeliverUnacknowledgedMessages(messageIds);

            MessageIdImpl lastMsgId = null;
            for (int i = 0; i < totalMsgs / 2; i++) {
                Message<byte[]> message = consumer1.receive(5, TimeUnit.SECONDS);
                MessageIdImpl msgId = (MessageIdImpl) message.getMessageId();
                if (lastMsgId != null) {
                    assertTrue(lastMsgId.getLedgerId() <= msgId.getLedgerId(), "lastMsgId: "
                            + lastMsgId + " -- msgId: " + msgId);
                }
                lastMsgId = msgId;
            }

            // close consumer so, this consumer's unack messages will be redelivered to new consumer
            consumer1.close();

            @Cleanup
            Consumer<byte[]> consumer2 = consumerBuilder.subscribe();
            lastMsgId = null;
            for (int i = 0; i < totalMsgs / 2; i++) {
                Message<byte[]> message = consumer2.receive(5, TimeUnit.SECONDS);
                MessageIdImpl msgId = (MessageIdImpl) message.getMessageId();
                if (lastMsgId != null) {
                    assertTrue(lastMsgId.getLedgerId() <= msgId.getLedgerId());
                }
                lastMsgId = msgId;
            }
        } finally {
            getConfig().setManagedLedgerMaxEntriesPerLedger(origMaxEntries);
            getConfig().setManagedLedgerMinLedgerRolloverTimeMinutes(origMinRollover);
        }
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testUnAckMessageRedeliveryWithReceiveAsync(boolean ackReceiptEnabled)
            throws PulsarClientException, ExecutionException, InterruptedException {
        String topic = newTopicName();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .isAckReceiptEnabled(ackReceiptEnabled)
                .enableBatchIndexAcknowledgment(ackReceiptEnabled)
                .ackTimeout(3, TimeUnit.SECONDS)
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        final int messages = 10;
        List<CompletableFuture<Message<String>>> futures = new ArrayList<>(10);
        for (int i = 0; i < messages; i++) {
            futures.add(consumer.receiveAsync());
        }

        for (int i = 0; i < messages; i++) {
            producer.sendAsync("my-message-" + i);
        }

        int messageReceived = 0;
        for (CompletableFuture<Message<String>> future : futures) {
            Message<String> message = future.get();
            assertNotNull(message);
            messageReceived++;
            // Don't ack message, wait for ack timeout.
        }

        assertEquals(10, messageReceived);
        for (int i = 0; i < messages; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            messageReceived++;
            consumer.acknowledge(message);
        }

        assertEquals(20, messageReceived);

        producer.close();
        consumer.close();
    }

    /**
    * Validates broker should dispatch messages to consumer which still has the permit to consume more messages.
    *
    * @throws Exception
    */
    @Test
    public void testConsumerWithPermitReceiveBatchMessages() throws Exception {

        final int queueSize = 10;
        int batchSize = 100;
        String subName = "my-subscriber-name";
        String topicName = newTopicName();
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .receiverQueueSize(queueSize).subscriptionType(SubscriptionType.Shared).subscriptionName(subName)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        producerBuilder.enableBatching(true);
        producerBuilder.batchingMaxPublishDelay(2000, TimeUnit.MILLISECONDS);
        producerBuilder.batchingMaxMessages(100);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < batchSize; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        for (int i = 0; i < queueSize; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer1.getTotalIncomingMessages(), batchSize));

        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .receiverQueueSize(queueSize).subscriptionType(SubscriptionType.Shared).subscriptionName(subName)
                .subscribe();

        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer2.getTotalIncomingMessages(), queueSize));
    }

    @Test(timeOut = 30000)
    public void testMessageRedeliveryWhenTimeoutInListener() throws Exception {

        final String subName = "sub_testMessageRedeliveryWhenTimeoutInListener";
        final String topicName = newTopicName();

        final int messages = 10;

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        final String redeliveryMsg = "Hello-0";
        final AtomicInteger index = new AtomicInteger(0);
        BlockingQueue<Message<byte[]>> receivedMsgs = new LinkedBlockingQueue<>();
        MessageListener<byte[]> listener = (consumer, msg) -> {
            try {
                // the first "Hello-0" will wait until timeout
                if (index.getAndDecrement() == 0 && new String(msg.getData()).equals(redeliveryMsg)) {
                    Thread.sleep(3000);
                }
                receivedMsgs.add(msg);
                consumer.acknowledge(msg);
            } catch (Exception e) {
            }
        };

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .messageListener(listener)
                .subscribe();

        for (int i = 0; i < messages; i++) {
            producer.sendAsync("Hello-" + i);
        }
        producer.flush();

        // assert only redelivery the "Hello-0" msg
        Awaitility.await().untilAsserted(() -> assertEquals(receivedMsgs.size(), messages + 1));
        List<Message<byte[]>> redeliveryMsgList = receivedMsgs.stream()
                .filter(msg -> new String(msg.getData()).equals(redeliveryMsg))
                .collect(Collectors.toList());
        assertEquals(redeliveryMsgList.size(), 2);
        for (int i = 0; i < redeliveryMsgList.size(); i++) {
            assertEquals(i, redeliveryMsgList.get(i).getRedeliveryCount());
        }
    }

    @Test(timeOut = 30000)
    public void testMessageRedeliveryAfterUnloadedWithEarliestPosition() throws Exception {

        final String subName = "my-subscriber-name";
        final String topicName = newTopicName();
        final int messages = 100;

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        List<CompletableFuture<MessageId>> sendResults = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            sendResults.add(producer.sendAsync("Hello - " + i));
        }
        producer.flush();

        FutureUtil.waitForAll(sendResults).get();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        List<Message<String>> received = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            received.add(consumer.receive());
        }

        assertEquals(received.size(), messages);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        admin.topics().unload(topicName);

        // The consumer does not ack any messages, so after unloading the topic,
        // the consumer should get the unacked messages again

        received.clear();
        for (int i = 0; i < messages; i++) {
            received.add(consumer.receive());
        }

        assertEquals(received.size(), messages);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 30000, dataProvider = "batchedMessageAck")
    public void testAckNotSent(int numAcked, int batchSize, CommandAck.AckType ackType) throws Exception {
        String topic = newTopicName();
        @Cleanup Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .enableBatchIndexAcknowledgment(false)
                .acknowledgmentGroupTime(1, TimeUnit.HOURS) // ACK won't be sent
                .subscribe();
        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(batchSize)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        for (int i = 0; i < batchSize; i++) {
            String value = "msg-" + i;
            producer.sendAsync(value).thenAccept(
                    id -> log.info().attr("value", value).attr("id", id).log("message sent"));
        }
        List<Message<String>> msgs = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            msgs.add(consumer.receive());
        }
        if (ackType == CommandAck.AckType.Individual) {
            for (int i = 0; i < numAcked; i++) {
                consumer.acknowledge(msgs.get(i));
            }
        } else {
            consumer.acknowledgeCumulative(msgs.get(numAcked - 1));
        }

        consumer.redeliverUnacknowledgedMessages();

        msgs.clear();
        for (int i = 0; i < batchSize; i++) {
            Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            log.info().attr("received", msg.getValue()).attr("from", msg.getMessageId()).log("Received from");
            msgs.add(msg);
        }
        List<String> values = msgs.stream().map(Message::getValue).collect(Collectors.toList());
        // All messages are redelivered because only if the whole batch are acknowledged would the message ID be
        // added into the ACK tracker.
        if (numAcked < batchSize) {
            assertEquals(values, IntStream.range(0, batchSize).mapToObj(i -> "msg-" + i).collect(Collectors.toList()));
        } else {
            assertTrue(values.isEmpty());
        }
    }

    @Test
    public void testRedeliverMessagesWithoutValue() throws Exception {
        String topic = newTopicName();
        @Cleanup Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .enableRetry(true)
                .subscribe();
        @Cleanup Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(true)
                .create();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().key("messages without value").send();
        }

        Message<Integer> message = consumer.receive();
        consumer.reconsumeLater(message, 2, TimeUnit.SECONDS);
        for (int i = 0; i < 9; i++) {
            assertNotNull(consumer.receive(5, TimeUnit.SECONDS));
        }
        assertTrue(consumer.receive(5, TimeUnit.SECONDS).getTopicName().contains("sub-RETRY"));
    }
}
