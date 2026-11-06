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
package org.apache.pulsar.broker.service;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", singleThreaded = true)
public class BatchIndexAckPendingAcksTest extends SharedPulsarBaseTest {
    private boolean transactionCoordinatorEnabled;

    @BeforeMethod(alwaysRun = true)
    public void disableTransactions() {
        // The shared cluster enables transactions, whose separate cleanup path masks this regression.
        transactionCoordinatorEnabled = getConfig().isTransactionCoordinatorEnabled();
        getConfig().setTransactionCoordinatorEnabled(false);
    }

    @AfterMethod(alwaysRun = true)
    public void restoreTransactions() {
        getConfig().setTransactionCoordinatorEnabled(transactionCoordinatorEnabled);
    }

    @DataProvider
    public Object[][] completedBatchScenarios() {
        List<Object[]> scenarios = new ArrayList<>();
        for (SubscriptionType type : List.of(SubscriptionType.Shared, SubscriptionType.Key_Shared)) {
            for (int batchSize : new int[]{4, 130}) {
                for (boolean ackHole : new boolean[]{false, true}) {
                    for (boolean receipt : new boolean[]{false, true}) {
                        scenarios.add(new Object[]{type, batchSize, ackHole, receipt});
                    }
                }
            }
        }
        return scenarios.toArray(Object[][]::new);
    }

    @DataProvider
    public Object[][] subscriptionTypes() {
        return new Object[][]{{SubscriptionType.Shared}, {SubscriptionType.Key_Shared}};
    }

    @DataProvider
    public Object[][] mergedAckScenarios() {
        return new Object[][]{
                {SubscriptionType.Shared, false}, {SubscriptionType.Shared, true},
                {SubscriptionType.Key_Shared, false}, {SubscriptionType.Key_Shared, true}
        };
    }

    @Test(dataProvider = "completedBatchScenarios", timeOut = 60000)
    public void testCompletedBatchReleasesPendingAck(SubscriptionType type, int batchSize,
                                                    boolean ackHole, boolean receipt) throws Exception {
        String topic = newTopicName();
        try (ConsumerImpl<String> consumer = newConsumer(topic, type, "owner");
             Producer<String> producer = newProducer(topic, batchSize)) {
            PersistentSubscription subscription = getSubscription(topic);
            Consumer owner = subscription.getConsumers().get(0);
            MessageIdAdv hole = ackHole ? publishAndReceiveHole(producer, consumer) : null;
            MessageIdAdv batch = publishAndReceiveBatch(producer, consumer, batchSize);
            int otherUnacked = ackHole ? 1 : 0;
            assertUnacked(subscription, owner, batchSize + otherUnacked);

            long[] partial = remainingIndexes(batchSize - 1, batchSize);
            sendAck(consumer, owner, batch, partial, true);
            assertPartialBatch(subscription, owner, batch, 1, otherUnacked);
            // Repeated and stale bitmaps must neither decrement twice nor resurrect acknowledged indexes.
            sendAck(consumer, owner, batch, partial, true);
            sendAck(consumer, owner, batch, remainingIndexes(0, batchSize), true);
            assertPartialBatch(subscription, owner, batch, 1, otherUnacked);

            sendAck(consumer, owner, batch, new long[(batchSize + Long.SIZE - 1) / Long.SIZE], receipt);
            assertCompletedBatch(subscription, owner, batch, hole);

            sendAck(consumer, owner, batch, new long[]{0}, true);
            sendAck(consumer, owner, batch, partial, true);
            assertCompletedBatch(subscription, owner, batch, hole);
        }
    }

    @Test(dataProvider = "mergedAckScenarios", timeOut = 60000)
    public void testMergedBatchIndexAcksReleasePendingAck(SubscriptionType type, boolean ackHole) throws Exception {
        String topic = newTopicName();
        try (ConsumerImpl<String> consumer = newConsumer(topic, type, "owner");
             Producer<String> producer = newProducer(topic, 4)) {
            PersistentSubscription subscription = getSubscription(topic);
            Consumer owner = subscription.getConsumers().get(0);
            MessageIdAdv hole = ackHole ? publishAndReceiveHole(producer, consumer) : null;
            MessageIdAdv batch = publishAndReceiveBatch(producer, consumer, 4);
            sendAck(consumer, owner, batch, new long[]{0b1100}, true);
            assertPartialBatch(subscription, owner, batch, 2, ackHole ? 1 : 0);
            // Neither incoming bitmap is empty: only merging with the cursor proves completion.
            sendAck(consumer, owner, batch, new long[]{0b0011}, true);
            assertCompletedBatch(subscription, owner, batch, hole);
        }
    }

    @Test(dataProvider = "subscriptionTypes", timeOut = 60000)
    public void testWholeEntryAckReleasesPendingAck(SubscriptionType type) throws Exception {
        String topic = newTopicName();
        try (ConsumerImpl<String> consumer = newConsumer(topic, type, "owner");
             Producer<String> producer = newProducer(topic, 4)) {
            PersistentSubscription subscription = getSubscription(topic);
            Consumer owner = subscription.getConsumers().get(0);
            MessageIdAdv batch = publishAndReceiveBatch(producer, consumer, 4);
            sendAck(consumer, owner, batch, new long[]{0b1100}, true);
            assertPartialBatch(subscription, owner, batch, 2, 0);
            // Control: an ordinary whole-entry ACK omits ack_set and already releases the pending entry.
            consumer.acknowledgeAsync(new MessageIdImpl(batch.getLedgerId(), batch.getEntryId(), -1))
                    .get(10, SECONDS);
            assertCompletedBatch(subscription, owner, batch, null);
        }
    }

    @Test(dataProvider = "subscriptionTypes", timeOut = 60000)
    public void testPartialBatchRemainsRedeliverableAfterReconnect(SubscriptionType type) throws Exception {
        String topic = newTopicName();
        try (Producer<String> producer = newProducer(topic, 4)) {
            MessageIdAdv batch;
            PersistentSubscription subscription;
            try (ConsumerImpl<String> consumer = newConsumer(topic, type, "owner")) {
                subscription = getSubscription(topic);
                Consumer owner = subscription.getConsumers().get(0);
                batch = publishAndReceiveBatch(producer, consumer, 4);
                sendAck(consumer, owner, batch, new long[]{0b1100}, true);
                assertPartialBatch(subscription, owner, batch, 2, 0);
            }
            try (ConsumerImpl<String> reconnected = newConsumer(topic, type, "reconnected")) {
                List<MessageId> redelivered = new ArrayList<>();
                for (int index = 2; index < 4; index++) {
                    var message = reconnected.receive(10, SECONDS);
                    assertThat(message).as("unacknowledged index %s is redelivered after reconnect", index).isNotNull();
                    MessageIdAdv id = (MessageIdAdv) message.getMessageId();
                    assertThat(position(id)).as("redelivery comes from the original batch").isEqualTo(position(batch));
                    assertThat(id.getBatchIndex()).as("only unacknowledged indexes are redelivered").isEqualTo(index);
                    redelivered.add(id);
                }
                Consumer owner = subscription.getConsumers().stream()
                        .filter(candidate -> candidate.consumerName().equals("reconnected")).findFirst().orElseThrow();
                assertPartialBatch(subscription, owner, batch, 2, 0);
                for (MessageId id : redelivered) {
                    reconnected.acknowledgeAsync(id).get(10, SECONDS);
                }
                assertCompletedBatch(subscription, owner, batch, null);
            }
        }
    }

    @Test(timeOut = 60000)
    public void testAckFromAnotherConsumerReleasesOwnersPendingAck() throws Exception {
        String topic = newTopicName();
        try (ConsumerImpl<String> consumer = newConsumer(topic, SubscriptionType.Shared, "owner");
             Producer<String> producer = newProducer(topic, 4)) {
            PersistentSubscription subscription = getSubscription(topic);
            Consumer owner = subscription.getConsumers().get(0);
            MessageIdAdv batch = publishAndReceiveBatch(producer, consumer, 4);
            // Join after delivery so the ACK sender is deterministically different from the pending-ack owner.
            try (ConsumerImpl<String> other = newConsumer(topic, SubscriptionType.Shared, "other")) {
                Consumer sender = subscription.getConsumers().stream()
                        .filter(candidate -> candidate.consumerName().equals("other")).findFirst().orElseThrow();
                sendAck(other, sender, batch, new long[]{0b1100}, true);
                assertPartialBatch(subscription, owner, batch, 2, 0);
                assertThat(sender.getUnackedMessages()).as("ACK sender does not own the batch").isZero();
                sendAck(other, sender, batch, new long[]{0}, true);
                assertCompletedBatch(subscription, owner, batch, null);
                assertThat(sender.getPendingAcks().size()).as("ACK sender has no pending entries").isZero();
                assertThat(sender.getUnackedMessages()).as("ACK sender's count must not become negative").isZero();
            }
        }
    }

    private ConsumerImpl<String> newConsumer(String topic, SubscriptionType type, String name) throws Exception {
        return (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(type)
                .consumerName(name)
                .enableBatchIndexAcknowledgment(true)
                .isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, MILLISECONDS)
                .subscribe();
    }

    private Producer<String> newProducer(String topic, int batchSize) throws Exception {
        return pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(batchSize)
                .batchingMaxPublishDelay(1, HOURS)
                .create();
    }

    private PersistentSubscription getSubscription(String topic) {
        return ((PersistentTopic) getTopicReference(topic).orElseThrow()).getSubscription("sub");
    }

    private MessageIdAdv publishAndReceiveHole(Producer<String> producer, ConsumerImpl<String> consumer)
            throws Exception {
        CompletableFuture<MessageId> sent = producer.newMessage().key("batch-key").value("ack-hole").sendAsync();
        producer.flushAsync().get(10, SECONDS);
        MessageId id = sent.get(10, SECONDS);
        var message = consumer.receive(10, SECONDS);
        assertThat(message).as("earlier unacknowledged message").isNotNull();
        assertThat(message.getMessageId()).as("ACK hole was delivered before the target batch").isEqualTo(id);
        return (MessageIdAdv) message.getMessageId();
    }

    private MessageIdAdv publishAndReceiveBatch(Producer<String> producer, ConsumerImpl<String> consumer,
                                              int batchSize) throws Exception {
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            // All messages have the same key, including for Key_Shared subscriptions.
            sends.add(producer.newMessage().key("batch-key").value("message-" + i).sendAsync());
        }
        producer.flushAsync().get(10, SECONDS);
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(10, SECONDS);
        MessageIdAdv first = null;
        for (int i = 0; i < batchSize; i++) {
            var message = consumer.receive(10, SECONDS);
            assertThat(message).as("delivered message %s of %s", i, batchSize).isNotNull();
            MessageIdAdv id = (MessageIdAdv) message.getMessageId();
            assertThat(id.getBatchIndex()).as("batch index").isEqualTo(i);
            assertThat(id.getBatchSize()).as("messages must be stored in one real batch").isEqualTo(batchSize);
            if (first == null) {
                first = id;
            }
            assertThat(position(id)).as("all messages belong to one ledger entry").isEqualTo(position(first));
        }
        return first;
    }

    private void sendAck(ConsumerImpl<String> consumer, Consumer sender, MessageIdAdv id,
                         long[] ackSet, boolean receipt) throws Exception {
        MessageIdData messageId = new MessageIdData().setLedgerId(id.getLedgerId())
                .setEntryId(id.getEntryId()).setBatchSize(id.getBatchSize());
        for (long word : ackSet) {
            messageId.addAckSet(word);
        }
        long requestId = receipt ? ((PulsarClientImpl) pulsarClient).newRequestId() : -1;
        // Send a real protocol frame through the client's connection. The ordinary Java ACK builder
        // strips trailing zero words, so it cannot reproduce clients sending ack_set=[0] (or [0,0,0]).
        var command = Commands.newAck(sender.consumerId(), List.of(messageId), AckType.Individual,
                null, Map.of(), -1, -1, requestId);
        if (receipt) {
            consumer.getClientCnx().newAckForReceipt(command, requestId).get(10, SECONDS);
        } else {
            consumer.getClientCnx().ctx().writeAndFlush(command).get(10, SECONDS);
        }
    }

    private static long[] remainingIndexes(int from, int to) {
        BitSet remaining = new BitSet(to);
        remaining.set(from, to);
        return remaining.toLongArray();
    }

    private static Position position(MessageIdAdv id) {
        return PositionFactory.create(id.getLedgerId(), id.getEntryId());
    }

    private static void assertPartialBatch(PersistentSubscription subscription, Consumer owner,
                                           MessageIdAdv batch, int remaining, int otherUnacked) {
        assertThat(subscription.getCursor().isMessageDeleted(position(batch)))
                .as("partially acknowledged entry must remain available for redelivery").isFalse();
        assertThat(owner.getPendingAcks().getRemainingUnacked(batch.getLedgerId(), batch.getEntryId()))
                .as("partially acknowledged batch remains tracked").isEqualTo(remaining);
        assertUnacked(subscription, owner, remaining + otherUnacked);
    }

    private static void assertCompletedBatch(PersistentSubscription subscription, Consumer owner,
                                             MessageIdAdv batch, MessageIdAdv hole) {
        Awaitility.await().atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(subscription.getCursor().isMessageDeleted(position(batch)))
                    .as("cursor has acknowledged every index of the batch").isTrue();
            assertUnacked(subscription, owner, hole == null ? 0 : 1);
        });
        if (hole == null) {
            assertThat(subscription.getCursor().getMarkDeletedPosition().compareTo(position(batch)))
                    .as("ordinary ACK advances mark-delete").isGreaterThanOrEqualTo(0);
        } else {
            assertThat(subscription.getCursor().getMarkDeletedPosition().compareTo(position(hole)))
                    .as("an earlier ACK hole prevents mark-delete from advancing").isNegative();
            assertThat(owner.getPendingAcks().contains(hole.getLedgerId(), hole.getEntryId()))
                    .as("the earlier unacknowledged entry must stay tracked").isTrue();
        }
        Awaitility.await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(owner.getPendingAcks().contains(batch.getLedgerId(), batch.getEntryId()))
                    .as("completed batch must release its pending entry (remainingUnacked=%s)",
                            owner.getPendingAcks().getRemainingUnacked(batch.getLedgerId(), batch.getEntryId()))
                    .isFalse();
            assertThat(owner.getPendingAcks().size()).as("only the ACK hole may remain tracked")
                    .isEqualTo(hole == null ? 0 : 1);
        });
    }

    private static void assertUnacked(PersistentSubscription subscription, Consumer owner, int expected) {
        assertThat(owner.getUnackedMessages()).as("owning consumer's unacknowledged count").isEqualTo(expected);
        assertThat(((PersistentDispatcherMultipleConsumers) subscription.getDispatcher()).getTotalUnackedMessages())
                .as("subscription's unacknowledged count").isEqualTo(expected);
    }
}
