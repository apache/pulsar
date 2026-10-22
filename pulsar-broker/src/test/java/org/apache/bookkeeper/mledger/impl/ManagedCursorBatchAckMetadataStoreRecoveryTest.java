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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ManagedCursorBatchAckMetadataStoreRecoveryTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30000)
    public void testSharedConsumerSkipsPartiallyAcknowledgedBatchAfterTopicUnload() throws Exception {
        int batchSize = 4;
        String topicName = newTopicName();
        String subscriptionName = "shared-sub";
        List<MessageId> sentMessageIds = new ArrayList<>(batchSize);

        try (Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxMessages(batchSize)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
             Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                     .topic(topicName)
                     .subscriptionName(subscriptionName)
                     .subscriptionType(SubscriptionType.Shared)
                     .receiverQueueSize(batchSize)
                     .enableBatchIndexAcknowledgment(true)
                     .isAckReceiptEnabled(true)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                     .subscribe()) {
            List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                sendFutures.add(producer.sendAsync(i));
            }
            producer.flush();
            for (CompletableFuture<MessageId> sendFuture : sendFutures) {
                sentMessageIds.add(sendFuture.get(5, TimeUnit.SECONDS));
            }
            assertSingleBatch(sentMessageIds, batchSize);

            List<Message<Integer>> messages = new ArrayList<>(batchSize);
            try {
                for (int i = 0; i < batchSize; i++) {
                    Message<Integer> message = consumer.receive(5, TimeUnit.SECONDS);
                    assertThat(message).as("message %s from the original batch", i).isNotNull();
                    assertThat(message.getValue()).isEqualTo(i);
                    messages.add(message);
                }
                consumer.acknowledge(messages.get(0));
                consumer.acknowledge(messages.get(1));
            } finally {
                messages.forEach(Message::release);
            }
        }

        PersistentTopic originalTopic = (PersistentTopic) getTopicReference(topicName).orElseThrow();
        ManagedLedgerImpl originalLedger = (ManagedLedgerImpl) originalTopic.getManagedLedger();
        ManagedCursorImpl originalCursor = cursor(originalTopic, subscriptionName);
        MessageIdAdv batchMessageId = (MessageIdAdv) sentMessageIds.get(0);
        Position batchPosition = PositionFactory.create(batchMessageId.getLedgerId(), batchMessageId.getEntryId());
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(originalCursor.getBatchPositionAckSet(batchPosition)).isNotNull());
        long[] ackSetBeforeUnload = originalCursor.getBatchPositionAckSet(batchPosition);

        admin.topics().unload(topicName);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> getTopicReference(topicName).isEmpty());

        ManagedCursorInfo persisted = storedCursorInfo(originalLedger, subscriptionName);
        assertThat(persisted.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                .as("the partial batch ACK was written to MetadataStore before recovery")
                .isEqualTo(1);
        assertThat(persisted.getBatchedEntryDeletionIndexInfoAt(0).getPosition().getLedgerId())
                .isEqualTo(batchPosition.getLedgerId());
        assertThat(persisted.getBatchedEntryDeletionIndexInfoAt(0).getPosition().getEntryId())
                .isEqualTo(batchPosition.getEntryId());

        try (PulsarClient newClient = newPulsarClient();
             Consumer<Integer> recoveredConsumer = newClient.newConsumer(Schema.INT32)
                     .topic(topicName)
                     .subscriptionName(subscriptionName)
                     .subscriptionType(SubscriptionType.Shared)
                     .receiverQueueSize(batchSize)
                     .enableBatchIndexAcknowledgment(true)
                     .isAckReceiptEnabled(true)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                     .subscribe()) {
            PersistentTopic recoveredTopic = (PersistentTopic) getTopicReference(topicName).orElseThrow();
            ManagedCursorImpl recoveredCursor = cursor(recoveredTopic, subscriptionName);
            assertThat(recoveredCursor).as("topic unload must rebuild the cursor").isNotSameAs(originalCursor);
            assertThat(recoveredCursor.getBatchPositionAckSet(batchPosition)).containsExactly(ackSetBeforeUnload);

            Set<Integer> expectedValues = new HashSet<>(Set.of(2, 3));
            for (int i = 0; i < 2; i++) {
                Message<Integer> message = recoveredConsumer.receive(5, TimeUnit.SECONDS);
                assertThat(message).as("unacknowledged message %s", i).isNotNull();
                try {
                    assertThat(expectedValues.remove(message.getValue()))
                            .as("only unacknowledged messages should be delivered")
                            .isTrue();
                    recoveredConsumer.acknowledge(message);
                } finally {
                    message.release();
                }
            }
            assertThat(expectedValues).isEmpty();

            Message<Integer> unexpected = recoveredConsumer.receive(1, TimeUnit.SECONDS);
            try {
                assertThat(unexpected).as("acknowledged batch indexes must not be redelivered").isNull();
            } finally {
                if (unexpected != null) {
                    unexpected.release();
                }
            }
        }
    }

    private static void assertSingleBatch(List<MessageId> messageIds, int batchSize) {
        assertThat(messageIds).hasSize(batchSize);
        MessageIdAdv first = (MessageIdAdv) messageIds.get(0);
        for (int i = 0; i < messageIds.size(); i++) {
            MessageIdAdv current = (MessageIdAdv) messageIds.get(i);
            assertThat(current.getLedgerId()).isEqualTo(first.getLedgerId());
            assertThat(current.getEntryId()).isEqualTo(first.getEntryId());
            assertThat(current.getBatchIndex()).isEqualTo(i);
        }
    }

    private static ManagedCursorImpl cursor(PersistentTopic topic, String subscriptionName) {
        PersistentSubscription subscription = topic.getSubscription(subscriptionName);
        return (ManagedCursorImpl) subscription.getCursor();
    }

    private static ManagedCursorInfo storedCursorInfo(ManagedLedgerImpl ledger, String subscriptionName)
            throws Exception {
        CompletableFuture<ManagedCursorInfo> result = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), subscriptionName, new MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {
                result.complete(info);
            }

            @Override
            public void operationFailed(MetaStoreException exception) {
                result.completeExceptionally(exception);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }
}
