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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Selective redelivery tests of {@link ConsumerRedeliveryTest} that need a broker with the transaction coordinator
 * enabled.
 */
@Test(groups = "broker-api")
public class ConsumerRedeliveryTransactionTest extends TransactionTestBase {

    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        setUpBase(1, 1, null, 0);
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    private String getNamespace() {
        return NAMESPACE1;
    }

    private String newTopicName() {
        return "persistent://" + NAMESPACE1 + "/topic-" + UUID.randomUUID();
    }

    private String getBrokerServiceUrl() {
        return getPulsarServiceList().get(0).getBrokerServiceUrl();
    }

    private CompletableFuture<Optional<Topic>> getTopic(String topic, boolean createIfMissing) {
        return getPulsarServiceList().get(0).getBrokerService().getTopic(topic, createIfMissing);
    }

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
}
