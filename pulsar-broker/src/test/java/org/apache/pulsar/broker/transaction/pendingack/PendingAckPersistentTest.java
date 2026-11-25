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
package org.apache.pulsar.broker.transaction.pendingack;

import static org.apache.bookkeeper.mledger.impl.AckSetStateUtil.getAckSetArrayOrNull;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import com.google.common.collect.Multimap;
import io.opentelemetry.api.common.Attributes;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorResetInProgressException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.OpenTelemetryTransactionPendingAckStoreStats;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@CustomLog
@Test(groups = "broker")
public class PendingAckPersistentTest extends TransactionTestBase {

    private static final String PENDING_ACK_REPLAY_TOPIC = NAMESPACE1 + "/pending-ack-replay";

    private static final int NUM_PARTITIONS = 16;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        setUpBase(1, NUM_PARTITIONS, PENDING_ACK_REPLAY_TOPIC, 0);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test(timeOut = 60_000)
    public void testIndividualTransactionCommitWaitsForCursorReset() throws Exception {
        verifyIndividualTransactionCommitDuringCursorReset(CursorCloseTiming.NONE);
    }

    @Test(timeOut = 60_000)
    public void testIndividualTransactionCommitCanRetryWhenCursorClosesDuringReset() throws Exception {
        verifyIndividualTransactionCommitDuringCursorReset(CursorCloseTiming.DURING_RESET);
    }

    @Test(timeOut = 60_000)
    public void testIndividualTransactionCommitCanRetryWhenCursorClosesBeforeAckRetry() throws Exception {
        verifyIndividualTransactionCommitDuringCursorReset(CursorCloseTiming.BEFORE_ACK_RETRY);
    }

    private enum CursorCloseTiming {
        NONE, DURING_RESET, BEFORE_ACK_RETRY
    }

    private void verifyIndividualTransactionCommitDuringCursorReset(CursorCloseTiming closeTiming) throws Exception {
        String subscriptionName = "txn-cursor-reset";
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(45, TimeUnit.SECONDS).build().get(10, TimeUnit.SECONDS);
        List<Position> positions = new ArrayList<>();
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(PENDING_ACK_REPLAY_TOPIC).subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared).subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(PENDING_ACK_REPLAY_TOPIC).enableBatching(false).create()) {
            int messageCount = 5;
            for (int i = 0; i < messageCount; i++) {
                producer.send("message-" + i);
            }
            for (int i = 0; i < messageCount; i++) {
                Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                MessageIdAdv id = (MessageIdAdv) message.getMessageId();
                positions.add(PositionFactory.create(id.getLedgerId(), id.getEntryId()));
                if (i == messageCount - 1) {
                    consumer.acknowledgeAsync(id, transaction).get(10, TimeUnit.SECONDS);
                }
            }
        }

        PersistentTopic topic = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                .getTopic(TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString(), false).get(10, TimeUnit.SECONDS)
                .orElseThrow();
        PersistentSubscription subscription = topic.getSubscription(subscriptionName);
        ManagedCursorImpl cursor = (ManagedCursorImpl) subscription.getCursor();
        PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) subscription.getPendingAckHandle();
        ManagedLedgerImpl pendingAckLedger = (ManagedLedgerImpl) subscription.getPendingAckManageLedger()
                .get(10, TimeUnit.SECONDS);
        Position ackPosition = positions.getLast();
        assertThat(subscription.getPositionInPendingAck(ackPosition)).isNotNull();
        cursor.setThrottleMarkDelete(0);
        cursor.markDelete(positions.get(2));
        long lastCursorLedgerEntry = cursor.getCursorLedgerLastEntry();
        long pendingAckEntries = pendingAckLedger.getEntriesAddedCounter();

        CompletableFuture<Void> releaseReset = pulsarTestContexts.get(0).getMockBookKeeper()
                .addEntryResponseGate(cursor.getCursorLedger());
        try {
            CompletableFuture<Void> reset = subscription.resetCursor(positions.get(1));
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                    cursor.getCursorLedgerLastEntry() > lastCursorLedgerEntry
                            && releaseReset.getNumberOfDependents() > 0);
            assertThat(reset).isNotDone();

            TxnID txnID = transaction.getTxnID();
            // The transaction coordinator retries ServiceNotReady. Observe the broker's first response on close.
            CompletableFuture<Void> commit = closeTiming == CursorCloseTiming.NONE
                    ? transaction.commit()
                    : subscription.endTxn(txnID.getMostSigBits(), txnID.getLeastSigBits(), TxnAction.COMMIT_VALUE, -1L);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> pendingAckLedger.getEntriesAddedCounter() > pendingAckEntries);
            // The commit marker's completion hands off the cursor ACK to the pinned executor.
            pendingAckLedger.getExecutor().submit(() -> { }).get(10, TimeUnit.SECONDS);
            pendingAckHandle.getInternalPinnedExecutor().submit(() -> { }).get(10, TimeUnit.SECONDS);
            assertThat(commit).isNotDone();
            assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();

            if (closeTiming != CursorCloseTiming.NONE) {
                CloseFuture close = null;
                if (closeTiming == CursorCloseTiming.BEFORE_ACK_RETRY) {
                    closeCursorAfterResetBeforeAckRetry(pendingAckHandle, cursor, releaseReset, reset, commit);
                } else {
                    close = new CloseFuture();
                    cursor.asyncClose(close, null);
                }
                assertThatThrownBy(() -> commit.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(BrokerServiceException.ServiceUnitNotReadyException.class)
                        .hasRootCauseInstanceOf(CursorAlreadyClosedException.class);
                assertThat(releaseReset.isDone()).isEqualTo(closeTiming == CursorCloseTiming.BEFORE_ACK_RETRY);
                if (close != null) {
                    assertThat(close).isNotDone();
                    assertThat(reset).isNotDone();
                    releaseReset.complete(null);
                    reset.get(10, TimeUnit.SECONDS);
                    close.get(10, TimeUnit.SECONDS);
                    assertThat(cursor.getMarkDeletedPosition()).isEqualTo(positions.getFirst());
                }
                assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();
                assertThat(subscription.getPositionInPendingAck(ackPosition)).isNotNull();
                assertThat(pendingAckLedger.getEntriesAddedCounter()).isEqualTo(pendingAckEntries + 1);
                return;
            }

            releaseReset.complete(null);
            reset.get(10, TimeUnit.SECONDS);
            commit.get(10, TimeUnit.SECONDS);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(positions.getFirst());
            assertThat(cursor.isMessageDeleted(ackPosition)).isTrue();
            assertThat(subscription.getPositionInPendingAck(ackPosition)).isNull();
            assertThat(pendingAckLedger.getEntriesAddedCounter()).isEqualTo(pendingAckEntries + 1);
        } finally {
            releaseReset.complete(null);
        }
    }

    private static void closeCursorAfterResetBeforeAckRetry(PendingAckHandleImpl pendingAckHandle,
            ManagedCursorImpl cursor, CompletableFuture<Void> releaseReset, CompletableFuture<Void> reset,
            CompletableFuture<Void> commit) throws Exception {
        CountDownLatch pinnedExecutorPaused = new CountDownLatch(1);
        CountDownLatch resumePinnedExecutor = new CountDownLatch(1);
        // The managed ledger can finish the reset and close while the pinned ACK retry is paused.
        var pause = pendingAckHandle.getInternalPinnedExecutor().submit(() -> {
            pinnedExecutorPaused.countDown();
            return resumePinnedExecutor.await(20, TimeUnit.SECONDS);
        });
        try {
            assertThat(pinnedExecutorPaused.await(10, TimeUnit.SECONDS)).isTrue();
            releaseReset.complete(null);
            reset.get(10, TimeUnit.SECONDS);
            assertThat(commit).isNotDone();
            cursor.close();
        } finally {
            resumePinnedExecutor.countDown();
        }
        assertThat(pause.get(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test(timeOut = 60_000)
    public void testSeekReplaysPreviouslyAcknowledgedBatchIndex() throws Exception {
        String subscriptionName = "seek-ordinary-partial-batch-ack";
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(45, TimeUnit.SECONDS).build().get(10, TimeUnit.SECONDS);
        try {
            PersistentSubscription subscription;
            Position batchPosition;
            Position reservedPosition;
            try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                        .topic(PENDING_ACK_REPLAY_TOPIC).subscriptionName(subscriptionName)
                        .subscriptionType(SubscriptionType.Shared).enableBatchIndexAcknowledgment(true)
                        .isAckReceiptEnabled(true).acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
                 Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                        .topic(PENDING_ACK_REPLAY_TOPIC).enableBatching(true).batchingMaxMessages(2)
                        .batchingMaxPublishDelay(10, TimeUnit.SECONDS).create()) {
                CompletableFuture<Void> sent = CompletableFuture.allOf(
                        producer.sendAsync("first"), producer.sendAsync("second"), producer.sendAsync("reserved"));
                producer.flush();
                sent.get(10, TimeUnit.SECONDS);
                List<MessageIdAdv> delivered = new ArrayList<>();
                for (String value : List.of("first", "second", "reserved")) {
                    Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
                    assertThat(message).isNotNull();
                    assertThat(message.getValue()).isEqualTo(value);
                    delivered.add((MessageIdAdv) message.getMessageId());
                }

                MessageIdAdv firstId = delivered.get(0);
                MessageIdAdv secondId = delivered.get(1);
                MessageIdAdv reservedId = delivered.get(2);
                batchPosition = PositionFactory.create(firstId.getLedgerId(), firstId.getEntryId());
                reservedPosition = PositionFactory.create(reservedId.getLedgerId(), reservedId.getEntryId());
                assertThat(PositionFactory.create(secondId.getLedgerId(), secondId.getEntryId()))
                        .isEqualTo(batchPosition);
                assertThat(firstId.getBatchIndex()).isZero();
                assertThat(secondId.getBatchIndex()).isEqualTo(1);
                assertThat(reservedPosition).isGreaterThan(batchPosition);

                PersistentTopic topic = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                        .getTopic(TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString(), false).get(10, TimeUnit.SECONDS)
                        .orElseThrow();
                subscription = topic.getSubscription(subscriptionName);
                consumer.acknowledgeAsync(firstId).get(10, TimeUnit.SECONDS);
                assertOrdinaryBatchAckState(subscription, batchPosition);
                consumer.acknowledgeAsync(reservedId, transaction).get(10, TimeUnit.SECONDS);
                assertThat(subscription.getPositionInPendingAck(reservedPosition)).isNotNull();
            }

            subscription.resetCursor(batchPosition).get(10, TimeUnit.SECONDS);
            assertThat(subscription.getCursor().getBatchPositionAckSet(batchPosition)).isNull();
            assertThat(subscription.getPositionInPendingAck(batchPosition)).isNull();
            assertThat(subscription.getPositionInPendingAck(reservedPosition)).isNotNull();
            try (Consumer<String> replay = pulsarClient.newConsumer(Schema.STRING)
                    .topic(PENDING_ACK_REPLAY_TOPIC).subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Shared).enableBatchIndexAcknowledgment(true).subscribe()) {
                for (String value : List.of("first", "second")) {
                    Message<String> message = replay.receive(5, TimeUnit.SECONDS);
                    assertThat(message).isNotNull();
                    assertThat(message.getValue()).isEqualTo(value);
                }
            }
        } finally {
            transaction.abort().get(10, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] ordinaryBatchAckBeforeReset() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 60_000, dataProvider = "ordinaryBatchAckBeforeReset")
    public void testRejectedOrdinaryBatchAckDoesNotHideMessagesAfterReset(boolean previouslyAcknowledged)
            throws Exception {
        verifyOrdinaryBatchAckDuringReset(previouslyAcknowledged, true);
    }

    @Test(timeOut = 60_000)
    public void testRejectedOrdinaryBatchAckDoesNotHideMessagesAfterFailedReset() throws Exception {
        verifyOrdinaryBatchAckDuringReset(false, false);
    }

    private void verifyOrdinaryBatchAckDuringReset(boolean previouslyAcknowledged, boolean resetSucceeds)
            throws Exception {
        String subscriptionName = "ordinary-batch-ack-cursor-reset";
        PersistentSubscription subscription;
        ManagedCursorImpl cursor;
        Position beforeBatch;
        Position batchPosition;
        org.apache.pulsar.broker.service.Consumer brokerConsumer;
        CommandAck ack;
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(PENDING_ACK_REPLAY_TOPIC).subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Shared).enableBatchIndexAcknowledgment(true).subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic(PENDING_ACK_REPLAY_TOPIC).enableBatching(true).batchingMaxMessages(2)
                    .batchingMaxPublishDelay(10, TimeUnit.SECONDS).create()) {
            CompletableFuture<MessageId> leading = producer.sendAsync("before batch");
            producer.flush();
            MessageIdAdv leadingId = (MessageIdAdv) leading.get(10, TimeUnit.SECONDS);
            beforeBatch = PositionFactory.create(leadingId.getLedgerId(), leadingId.getEntryId());
            CompletableFuture<MessageId> first = producer.sendAsync("first");
            CompletableFuture<MessageId> second = producer.sendAsync("second");
            MessageIdAdv firstId = (MessageIdAdv) first.get(10, TimeUnit.SECONDS);
            MessageIdAdv secondId = (MessageIdAdv) second.get(10, TimeUnit.SECONDS);
            assertThat(firstId.getLedgerId()).isEqualTo(secondId.getLedgerId());
            assertThat(firstId.getEntryId()).isEqualTo(secondId.getEntryId());
            assertThat(firstId.getBatchIndex()).isZero();
            assertThat(secondId.getBatchIndex()).isEqualTo(1);
            assertThat(consumer.receive(5, TimeUnit.SECONDS)).isNotNull();
            assertThat(consumer.receive(5, TimeUnit.SECONDS)).isNotNull();
            assertThat(consumer.receive(5, TimeUnit.SECONDS)).isNotNull();
            batchPosition = PositionFactory.create(firstId.getLedgerId(), firstId.getEntryId());

            PersistentTopic topic = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                    .getTopic(TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString(), false).get(10, TimeUnit.SECONDS)
                    .orElseThrow();
            subscription = topic.getSubscription(subscriptionName);
            cursor = (ManagedCursorImpl) subscription.getCursor();
            cursor.setThrottleMarkDelete(0);
            brokerConsumer = subscription.getConsumers().getFirst();
            ack = new CommandAck().setConsumerId(brokerConsumer.consumerId()).setAckType(CommandAck.AckType.Individual);
            ack.addMessageId().setLedgerId(batchPosition.getLedgerId()).setEntryId(batchPosition.getEntryId());
            if (previouslyAcknowledged) {
                brokerConsumer.messageAcked(ack, true).get(10, TimeUnit.SECONDS);
            } else {
                cursor.markDelete(beforeBatch);
            }
            ack.getMessageIdAt(0).addAckSet(0b10L);
        }
        assertThat(cursor.isMessageDeleted(batchPosition)).isEqualTo(previouslyAcknowledged);

        long cursorLedgerId = cursor.getCursorLedger();
        long lastCursorLedgerEntry = cursor.getCursorLedgerLastEntry();
        var testContext = pulsarTestContexts.get(0);
        var bookKeeper = testContext.getMockBookKeeper();
        CompletableFuture<Void> resetWrite;
        Runnable releaseResetWrite;
        if (resetSucceeds) {
            resetWrite = bookKeeper.addEntryResponseGate(cursorLedgerId);
            releaseResetWrite = () -> resetWrite.complete(null);
        } else {
            // A failed BookKeeper write falls back to the existing cursor's metadata node.
            String cursorPath = "/managed-ledgers/" + cursor.getManagedLedger().getName() + "/" + cursor.getName();
            testContext.getMockZooKeeper().failConditional(KeeperException.Code.SYSTEMERROR,
                    (operation, path) -> operation == MockZooKeeper.Op.SET && path.equals(cursorPath));
            resetWrite = bookKeeper.addEntryWriteGate(cursorLedgerId);
            var failure = org.apache.bookkeeper.client.BKException.create(BKException.Code.WriteException);
            releaseResetWrite = () -> resetWrite.completeExceptionally(failure);
        }
        try {
            CompletableFuture<Void> reset = subscription.resetCursor(resetSucceeds ? batchPosition : beforeBatch);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                    resetWrite.getNumberOfDependents() > 0 && (resetSucceeds
                            ? cursor.getCursorLedgerLastEntry() > lastCursorLedgerEntry
                            : cursor.getPersistentMarkDeletedPosition() == null));
            assertThat(reset).isNotDone();

            // Model an ACK already routed to the old consumer before disconnect, with cursor admission now racing seek.
            CompletableFuture<Void> receipt = brokerConsumer.messageAcked(ack, true);
            assertThat(receipt).isCompletedExceptionally();
            assertThatThrownBy(() -> receipt.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorResetInProgressException.class);
            assertThat(reset).isNotDone();
            assertThat(subscription.getPositionInPendingAck(batchPosition)).isNull();
            assertThat(cursor.getBatchPositionAckSet(batchPosition)).isNull();

            releaseResetWrite.run();
            if (resetSucceeds) {
                reset.get(10, TimeUnit.SECONDS);
            } else {
                assertThatThrownBy(() -> reset.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(BrokerServiceException.SubscriptionInvalidCursorPosition.class);
                assertThat(cursor.getMarkDeletedPosition()).isEqualTo(beforeBatch);
            }
        } finally {
            resetWrite.complete(null);
        }
        assertThat(cursor.isMessageDeleted(batchPosition)).isFalse();
        assertThat(cursor.getBatchPositionAckSet(batchPosition)).isNull();
        assertThat(subscription.getPositionInPendingAck(batchPosition)).isNull();

        try (Consumer<String> replay = pulsarClient.newConsumer(Schema.STRING)
                .topic(PENDING_ACK_REPLAY_TOPIC).subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared).enableBatchIndexAcknowledgment(true).subscribe()) {
            Message<String> first = replay.receive(5, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            assertThat(first.getValue()).isEqualTo("first");
            Message<String> second = replay.receive(5, TimeUnit.SECONDS);
            assertThat(second).isNotNull();
            assertThat(second.getValue()).isEqualTo("second");

            var replayConsumer = subscription.getConsumers().getFirst();
            ack.setConsumerId(replayConsumer.consumerId());
            replayConsumer.messageAcked(ack, true).get(10, TimeUnit.SECONDS);
            assertOrdinaryBatchAckState(subscription, batchPosition);
        }
    }

    private static void assertOrdinaryBatchAckState(PersistentSubscription subscription, Position batchPosition) {
        assertThat(subscription.getCursor().getBatchPositionAckSet(batchPosition)).containsExactly(0b10L);
        Position pending = subscription.getPositionInPendingAck(batchPosition);
        assertThat(pending).isNotNull();
        assertThat(getAckSetArrayOrNull(pending)).containsExactly(0b10L);
    }

    @DataProvider(name = "retryableErrors")
    public Object[][] retryableErrors() {
        return new Object[][] {
            {new ManagedLedgerException("mock retryable error")},
            {new MetadataStoreException("mock retryable error")},
            {new BKException(-1)},
        };
    }

    /**
     * Test consumer can be built successfully with retryable exception
     * and get correct error with no-retryable exception.
     * @throws Exception
     */
    @Test(timeOut = 60000, dataProvider = "retryableErrors")
    public void testBuildConsumerEncounterPendingAckInitFailure(Exception retryableError) throws Exception {
        // 1. Prepare and make sure the consumer can be built successfully.
        String topic = BrokerTestUtil.newUniqueName(NAMESPACE1 + "/tp");
        admin.topics().createNonPartitionedTopic(topic);
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .subscriptionName("subName1")
                .topic(topic)
                .subscribe();
        // 2. Mock a transactionPendingAckStoreProvider to test building consumer
        // failing at transactionPendingAckStoreProvider::checkInitializedBefore.
        Field transactionPendingAckStoreProviderField = PulsarService.class
                .getDeclaredField("transactionPendingAckStoreProvider");
        transactionPendingAckStoreProviderField.setAccessible(true);
        TransactionPendingAckStoreProvider pendingAckStoreProvider =
                (TransactionPendingAckStoreProvider) transactionPendingAckStoreProviderField
                        .get(pulsarServiceList.get(0));
        TransactionPendingAckStoreProvider mockProvider = mock(pendingAckStoreProvider.getClass());
        // 3. Test retryable exception when checkInitializedBefore:
        // The consumer will be built successfully after one time retry.
        when(mockProvider.checkInitializedBefore(any()))
                // First, the method checkInitializedBefore will fail with a retryable exception.
                .thenReturn(FutureUtil.failedFuture(retryableError))
                // Then, the method will be executed successfully.
                .thenReturn(CompletableFuture.completedFuture(false));
        transactionPendingAckStoreProviderField.set(pulsarServiceList.get(0), mockProvider);
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .subscriptionName("subName2")
                .topic(topic)
                .subscribe();

        // 4. Test retryable exception when newPendingAckStore:
        // The consumer will be built successfully after one time retry.
        when(mockProvider.checkInitializedBefore(any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        when(mockProvider.newPendingAckStore(any()))
                // First, the method newPendingAckStore will fail with a retryable exception.
                .thenReturn(FutureUtil.failedFuture(new ManagedLedgerException("mock fail new store")))
                // Then, the method will be executed successfully. Delegate to the real provider
                // rather than thenCallRealMethod(): the configured provider is now the dispatching
                // provider, and a Mockito mock of it has null delegate fields, so calling its real
                // method would NPE. The original real provider behaves identically for this topic.
                .thenAnswer(invocation -> pendingAckStoreProvider.newPendingAckStore(invocation.getArgument(0)));
        transactionPendingAckStoreProviderField.set(pulsarServiceList.get(0), mockProvider);
        Consumer<byte[]> consumer3 = pulsarClient.newConsumer()
                .subscriptionName("subName3")
                .topic(topic)
                .subscribe();

        // 5. Test no-retryable exception:
        // The consumer building will be failed without retrying.
        when(mockProvider.checkInitializedBefore(any()))
                // The method checkInitializedBefore will fail with a no-retryable exception without retrying.
                .thenReturn(FutureUtil.failedFuture(new ManagedLedgerException
                        .NonRecoverableLedgerException("mock fail")))
                .thenReturn(CompletableFuture.completedFuture(false));
        // The broker fails every subscribe attempt the same way, and the client retries until its operation timeout
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .operationTimeout(1, TimeUnit.SECONDS)
                .build();
        try {
            @Cleanup
            Consumer<byte[]> consumer4 = pulsarClient.newConsumer()
                    .subscriptionName("subName4")
                    .topic(topic)
                    .subscribe();
            fail();
        } catch (Exception exception) {
            assertTrue(exception.getMessage().contains("Failed to init transaction pending ack."));
        }

        // cleanup.
        consumer1.close();
        consumer2.close();
        consumer3.close();
        pulsarClient.close();
        admin.topics().delete(topic, false);
    }

    @Test
    public void individualPendingAckReplayTest() throws Exception {
        int messageCount = 1000;
        String subName = "individual-test";
        // Each transactional ack is an add to the pending ack log: don't make every add sleep 1 ms
        pulsarTestContexts.get(0).getMockBookKeeper().setDefaultAddEntryDelayMillis(0);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .enableBatching(true)
                .batchingMaxMessages(200)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Transaction abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        // Send every message on its own (one entry each), then receive them: receiving each one right after sending
        // it waits for the broker's new entries check delay (10 ms) every time
        for (int i = 0; i < messageCount; i++) {
            producer.send("Hello Pulsar!".getBytes());
        }
        List<MessageId> pendingAckMessageIds = new ArrayList<>();
        List<MessageId> normalAckMessageIds = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            Message<byte[]> message = consumer.receive();
            if (i % 2 == 0) {
                consumer.acknowledgeAsync(message.getMessageId(), abortTxn).get();
                pendingAckMessageIds.add(message.getMessageId());
            } else {
                normalAckMessageIds.add(message.getMessageId());
            }
        }

        //in order to test pending ack replay
        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Awaitility.await().until(consumer::isConnected);
        Transaction commitTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        // this messageIds are ack by transaction
        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeAsync(pendingAckMessageIds.get(i), txn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }
        // this messageIds are not ack by transaction
        for (int i = 0; i < normalAckMessageIds.size(); i++) {
            consumer.acknowledgeAsync(normalAckMessageIds.get(i), commitTxn).get();
        }

        txn.abort().get();
        // commit this txn, normalAckMessageIds are in pending ack state
        commitTxn.commit().get();
        // abort this txn, pendingAckMessageIds are delete from pending ack state
        abortTxn.abort().get();

        // replay this pending ack
        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Awaitility.await().until(consumer::isConnected);

        abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        commitTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
        // normalAckMessageIds are ack and then commit, so ack fail
        for (int i = 0; i < normalAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeAsync(normalAckMessageIds.get(i), abortTxn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }

        // pendingAckMessageIds are all abort, so can ack again
        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            consumer.acknowledgeAsync(pendingAckMessageIds.get(i), commitTxn).get();
        }

        abortTxn.abort().get();
        commitTxn.commit().get();

        PersistentTopic topic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString(), false).get().get();
        Field field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field.setAccessible(true);
        PendingAckHandleImpl pendingAckHandle =
                (PendingAckHandleImpl) field.get(topic.getSubscription(subName));
        field = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        CompletableFuture<PendingAckStore> pendingAckStoreCompletableFuture =
                (CompletableFuture<PendingAckStore>) field.get(pendingAckHandle);
        pendingAckStoreCompletableFuture.get();

        field = MLPendingAckStore.class.getDeclaredField("cursor");
        field.setAccessible(true);

        ManagedCursor managedCursor = (ManagedCursor) field.get(pendingAckStoreCompletableFuture.get());

        // in order to check out the pending ack cursor is clear whether or not.
        Awaitility.await()
                .until(() -> (managedCursor.getMarkDeletedPosition())
                        .compareTo(managedCursor.getManagedLedger().getLastConfirmedEntry()) == -1);
    }

    @Test
    public void testPendingAckMetrics() throws Exception {
        final int messageCount = 100;
        String subName = "testMetric" + UUID.randomUUID();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        for (int a = 0; a < messageCount; a++) {
            producer.send(UUID.randomUUID().toString());
        }

        for (int a = 0; a < messageCount; a++) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (null == message) {
                break;
            }

            Transaction txn = pulsarClient.newTransaction()
                    .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            consumer.acknowledgeCumulativeAsync(message.getMessageId(), txn).get();
            if (a % 2 == 0) {
                txn.abort().get();
            } else {
                txn.commit().get();
            }
        }

        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsarServiceList.get(0), true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metricsMap = parseMetrics(metricsStr);

        Collection<Metric> abortedCount = metricsMap.get("pulsar_txn_tp_aborted_count_total");
        Collection<Metric> committedCount = metricsMap.get("pulsar_txn_tp_committed_count_total");
        Collection<Metric> commitLatency = metricsMap.get("pulsar_txn_tp_commit_latency");
        Assert.assertTrue(commitLatency.size() > 0);

        int count = 0;
        for (Metric metric : commitLatency) {
            if (metric.tags.get("topic").endsWith(PENDING_ACK_REPLAY_TOPIC) && metric.value > 0) {
                count++;
            }
        }
        Assert.assertTrue(count > 0);

        // Half of the transactions were aborted and half committed; each outcome must show up as exactly one
        // succeeded series for this subscription with that count.
        List<Metric> aborted = abortedCount.stream()
                .filter(m -> subName.equals(m.tags.get("subscription")) && "succeed".equals(m.tags.get("status")))
                .toList();
        Assert.assertEquals(aborted.size(), 1);
        assertTrue(aborted.get(0).tags.get("topic").endsWith(PENDING_ACK_REPLAY_TOPIC));
        Assert.assertEquals(aborted.get(0).value, messageCount / 2.0);
        List<Metric> committed = committedCount.stream()
                .filter(m -> subName.equals(m.tags.get("subscription")) && "succeed".equals(m.tags.get("status")))
                .toList();
        Assert.assertEquals(committed.size(), 1);
        assertTrue(committed.get(0).tags.get("topic").endsWith(PENDING_ACK_REPLAY_TOPIC));
        Assert.assertEquals(committed.get(0).value, messageCount / 2.0);

        var otelMetrics = pulsarTestContexts.get(0).getOpenTelemetryMetricReader().collectAllMetrics();
        var commonAttributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "tnx")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "tnx/ns1")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString())
                .put(OpenTelemetryAttributes.PULSAR_SUBSCRIPTION_NAME, subName)
                .build();
        assertMetricLongSumValue(otelMetrics, OpenTelemetryTransactionPendingAckStoreStats.ACK_COUNTER,
                Attributes.builder()
                        .putAll(commonAttributes)
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "committed")
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_ACK_STORE_OPERATION_STATUS, "success")
                        .build(),
                50);
        assertMetricLongSumValue(otelMetrics, OpenTelemetryTransactionPendingAckStoreStats.ACK_COUNTER,
                Attributes.builder()
                        .putAll(commonAttributes)
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "committed")
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_ACK_STORE_OPERATION_STATUS, "failure")
                        .build(),
                0);
        assertMetricLongSumValue(otelMetrics, OpenTelemetryTransactionPendingAckStoreStats.ACK_COUNTER,
                Attributes.builder()
                        .putAll(commonAttributes)
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "aborted")
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_ACK_STORE_OPERATION_STATUS, "success")
                        .build(),
                50);
        assertMetricLongSumValue(otelMetrics, OpenTelemetryTransactionPendingAckStoreStats.ACK_COUNTER,
                Attributes.builder()
                        .putAll(commonAttributes)
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "aborted")
                        .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_ACK_STORE_OPERATION_STATUS, "failure")
                        .build(),
                0);
    }

    @Test
    public void cumulativePendingAckReplayTest() throws Exception {
        int messageCount = 1000;
        getPulsarServiceList().get(0).getConfig().setTransactionPendingAckLogIndexMinLag(4 * messageCount + 2);
        getPulsarServiceList().get(0).getConfiguration().setManagedLedgerDefaultMarkDeleteRateLimit(10);
        String subName = "cumulative-test";
        // Each transactional ack is an add to the pending ack log: don't make every add sleep 1 ms
        pulsarTestContexts.get(0).getMockBookKeeper().setDefaultAddEntryDelayMillis(0);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .enableBatching(true)
                .batchingMaxMessages(200)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        Transaction abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        List<MessageId> pendingAckMessageIds = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            producer.send("Hello Pulsar!".getBytes());
        }

        for (int i = 0; i < messageCount; i++) {
            Message<byte[]> message = consumer.receive();
            pendingAckMessageIds.add(message.getMessageId());
            consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
        }

        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        Awaitility.await().until(consumer::isConnected);

        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeCumulativeAsync(pendingAckMessageIds.get(i), txn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }
        Transaction commitTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
        abortTxn.abort().get();

        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            consumer.acknowledgeCumulativeAsync(pendingAckMessageIds.get(i), commitTxn).get();
        }
        commitTxn.commit().get();

        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Awaitility.await().until(consumer::isConnected);

        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeCumulativeAsync(pendingAckMessageIds.get(i), txn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }

        PersistentTopic topic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString(), false).get().get();
        Field field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field.setAccessible(true);
        PendingAckHandleImpl pendingAckHandle =
                (PendingAckHandleImpl) field.get(topic.getSubscription(subName));
        field = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        CompletableFuture<PendingAckStore> pendingAckStoreCompletableFuture =
                (CompletableFuture<PendingAckStore>) field.get(pendingAckHandle);
        pendingAckStoreCompletableFuture.get();

        field = MLPendingAckStore.class.getDeclaredField("cursor");
        field.setAccessible(true);

        ManagedCursor managedCursor = (ManagedCursor) field.get(pendingAckStoreCompletableFuture.get());

        // in order to check out the pending ack cursor is clear whether or not.
        Awaitility.await()
                .until(() -> (managedCursor.getMarkDeletedPosition())
                        .compareTo(managedCursor.getManagedLedger().getLastConfirmedEntry()) == 0);
    }

    @Test
    private void testDeleteSubThenDeletePendingAckManagedLedger() throws Exception {

        String subName = "test-delete";

        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test-delete").toString();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        consumer.close();

        admin.topics().deleteSubscription(topic, subName);

        List<String> topics = admin.namespaces().getTopics(NAMESPACE1);

        TopicStats topicStats = admin.topics().getStats(topic, false);

        assertFalse(topics.contains(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subName)));

        assertTrue(topics.contains(topic));
    }

    @Test
    private void testDeleteTopicThenDeletePendingAckManagedLedger() throws Exception {

        String subName1 = "test-delete";
        String subName2 = "test-delete";

        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test-delete").toString();
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName1)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        consumer1.close();

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName2)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        consumer2.close();

        admin.topics().delete(topic);

        List<String> topics = admin.namespaces().getTopics(NAMESPACE1);

        assertFalse(topics.contains(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subName1)));
        assertFalse(topics.contains(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subName2)));
        assertFalse(topics.contains(topic));
    }

    @Test(groups = "quarantine")
    public void testDeleteUselessLogDataWhenSubCursorMoved() throws Exception {
        getPulsarServiceList().get(0).getConfig().setTransactionPendingAckLogIndexMinLag(5);
        getPulsarServiceList().get(0).getConfiguration().setManagedLedgerDefaultMarkDeleteRateLimit(5);
        String subName = "test-log-delete";
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test-log-delete").toString();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        for (int i = 0; i < 20; i++) {
            producer.newMessage().send();
        }
        // init
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        consumer.acknowledgeAsync(message.getMessageId(), transaction).get();

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(topic, false).get().get();

        PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subName);
        Field field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field.setAccessible(true);
        PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) field.get(persistentSubscription);
        Field field1 = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field1.setAccessible(true);
        @SuppressWarnings("unchecked")
        CompletableFuture<PendingAckStore> storeFuture =
                (CompletableFuture<PendingAckStore>) field1.get(pendingAckHandle);
        PendingAckStore pendingAckStore = storeFuture.get();

        Field field3 = MLPendingAckStore.class.getDeclaredField("pendingAckLogIndex");
        Field field4 = MLPendingAckStore.class.getDeclaredField("maxIndexLag");

        field3.setAccessible(true);
        field4.setAccessible(true);

        @SuppressWarnings("unchecked")
        ConcurrentSkipListMap<Position, Position> pendingAckLogIndex =
                (ConcurrentSkipListMap<Position, Position>) field3.get(pendingAckStore);
        long maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(pendingAckLogIndex.size(), 0);
        Assert.assertEquals(maxIndexLag, 5);
        transaction.commit().get();

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(persistentSubscription.getCursor().getPersistentMarkDeletedPosition().getEntryId(),
                        ((MessageIdImpl) message.getMessageId()).getEntryId()));
        // 7 more acks. Will find that there are still only two records in the map.
        Transaction transaction1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        Message<byte[]> message0 = null;
        //remove previous index
        for (int i = 0; i < 4; i++) {
            message0 = consumer.receive(5, TimeUnit.SECONDS);
            consumer.acknowledgeAsync(message0.getMessageId(), transaction1).get();
        }
        Assert.assertEquals(pendingAckLogIndex.size(), 1);
        maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(maxIndexLag, 5);
        //add new index
        for (int i = 0; i < 9; i++) {
            message0 = consumer.receive(5, TimeUnit.SECONDS);
            consumer.acknowledgeAsync(message0.getMessageId(), transaction1).get();
        }

        Assert.assertEquals(pendingAckLogIndex.size(), 2);
        maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(maxIndexLag, 10);

        transaction1.commit().get();
        Message<byte[]> message1 = message0;
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(persistentSubscription.getCursor().getPersistentMarkDeletedPosition().getEntryId(),
                        ((MessageIdImpl) message1.getMessageId()).getEntryId()));

        Transaction transaction2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        Message<byte[]> message2 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message2.getMessageId(), transaction2).get();

        Assert.assertEquals(pendingAckLogIndex.size(), 0);
        maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(maxIndexLag, 5);
    }

    @Test
    public void testPendingAckLowWaterMarkRemoveFirstTxn() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test").toString();

        String subName = "subName";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        for (int i = 0; i < 5; i++) {
            producer.newMessage().send();
        }

        Transaction transaction1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();

        Message<byte[]> message1 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message1.getMessageId(), transaction1);
        transaction1.commit().get();

        Transaction transaction2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        while (transaction1.getTxnID().getMostSigBits() != transaction2.getTxnID().getMostSigBits()) {
            transaction2 = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build()
                    .get();
        }

        Transaction transaction3 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        while (transaction1.getTxnID().getMostSigBits() != transaction3.getTxnID().getMostSigBits()) {
            transaction3 = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build()
                    .get();
        }

        Message<byte[]> message3 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message3.getMessageId(), transaction2);
        transaction2.commit().get();

        Message<byte[]> message2 = consumer.receive(5, TimeUnit.SECONDS);

        Field field = TransactionImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(transaction1, TransactionImpl.State.OPEN);

        consumer.acknowledgeAsync(message2.getMessageId(), transaction1).get();
        Message<byte[]> message4 = consumer.receive(5, TimeUnit.SECONDS);
        field.set(transaction2, TransactionImpl.State.OPEN);
        consumer.acknowledgeAsync(message4.getMessageId(), transaction2).get();

        Message<byte[]> message5 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message5.getMessageId(), transaction3);
        transaction3.commit().get();

        PersistentTopic persistentTopic =
                (PersistentTopic) getPulsarServiceList()
                        .get(0)
                        .getBrokerService()
                        .getTopic(topic, false)
                        .get()
                        .get();

        PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subName);
        Field field1 = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field1.setAccessible(true);
        PendingAckHandleImpl oldPendingAckHandle = (PendingAckHandleImpl) field1.get(persistentSubscription);
        Field field2 = PendingAckHandleImpl.class.getDeclaredField("individualAckOfTransaction");
        field2.setAccessible(true);
        @SuppressWarnings("unchecked")
        LinkedMap<TxnID, HashMap<Position, Position>> oldIndividualAckOfTransaction =
                (LinkedMap<TxnID, HashMap<Position, Position>>) field2.get(oldPendingAckHandle);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(oldIndividualAckOfTransaction.size(), 0));

        PendingAckHandleImpl pendingAckHandle = new PendingAckHandleImpl(persistentSubscription);

        Method method = PendingAckHandleImpl.class.getDeclaredMethod("initPendingAckStore");
        method.setAccessible(true);
        method.invoke(pendingAckHandle);

        Field field3 = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field3.setAccessible(true);

        Awaitility.await().until(() -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<PendingAckStore> completableFuture =
                    (CompletableFuture<PendingAckStore>) field3.get(pendingAckHandle);
            completableFuture.get();
            return true;
        });

        @SuppressWarnings("unchecked")
        LinkedMap<TxnID, HashMap<Position, Position>> individualAckOfTransaction =
                (LinkedMap<TxnID, HashMap<Position, Position>>) field2.get(pendingAckHandle);

        assertFalse(individualAckOfTransaction.containsKey(transaction1.getTxnID()));
        assertFalse(individualAckOfTransaction.containsKey(transaction2.getTxnID()));
    }

    @Test
    public void testTransactionConflictExceptionWhenAckBatchMessage() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test").toString();

        String subscriptionName = "my-subscription-batch";
        pulsarServiceList.get(0).getBrokerService()
                .getManagedLedgerConfig(TopicName.get(topic)).get()
                .setDeletionAtBatchIndexLevelEnabled(true);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true)
                .batchingMaxMessages(3)
                // set batch max publish delay big enough to make sure entry has 3 messages
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .topic(topic).create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .isAckReceiptEnabled(true)
                .topic(topic)
                .subscribe();

        List<MessageId> messageIds = new ArrayList<>();
        List<CompletableFuture<MessageId>> futureMessageIds = new ArrayList<>();

        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String message = "my-message-" + i;
            messages.add(message);
            CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(message);
            futureMessageIds.add(messageIdCompletableFuture);
        }

        for (CompletableFuture<MessageId> futureMessageId : futureMessageIds) {
            MessageId messageId = futureMessageId.get();
            messageIds.add(messageId);
        }

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.DAYS)
                .build()
                .get();

        Message<String> message1 = consumer.receive();
        Message<String> message2 = consumer.receive();

        BatchMessageIdImpl messageId = (BatchMessageIdImpl) message2.getMessageId();
        consumer.acknowledgeAsync(messageId, transaction).get();

        Transaction transaction2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.DAYS)
                .build()
                .get();
        transaction.commit().get();

        try {
            consumer.acknowledgeAsync(messageId, transaction2).get();
            fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
        }
    }

    @Test
    public void testGetSubPatternTopicFilterTxnInternalTopic() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "testGetSubPatternTopicFilterTxnInternalTopic").toString();

        int partition = 3;
        admin.topics().createPartitionedTopic(topic, partition);

        String subscriptionName = "sub";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic).create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic)
                .subscribe();

        for (int i = 0; i < partition; i++) {
            producer.send("test");
        }

        // creat pending ack managedLedger
        for (int i = 0; i < partition; i++) {
            Transaction transaction = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build()
                    .get();
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction);
            transaction.commit().get();
        }

        consumer.close();
        admin.namespaces().getTopics(NAMESPACE1).forEach(name ->
                assertFalse(SystemTopicNames.isTransactionInternalName(TopicName.get(name))));

        @Cleanup
        Consumer<String> patternConsumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("patternSub")
                .subscriptionType(SubscriptionType.Shared)
                .topicsPattern("persistent://" + NAMESPACE1 + "/.*")
                .subscribe();

        for (int i = 0; i < partition; i++) {
            producer.send("test" + i);
        }

        // can use pattern sub consume
        for (int i = 0; i < partition; i++) {
            patternConsumer.acknowledgeAsync(patternConsumer.receive().getMessageId());
        }
        patternConsumer.close();
        producer.close();
    }

    @Test
    public void testGetManagedLegerConfigFailThenUnload() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "testGetManagedLegerConfigFailThenUnload").toString();

        String subscriptionName = "sub";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic).create();

        PersistentTopic persistentTopic =
                (PersistentTopic) getPulsarServiceList()
                        .get(0)
                        .getBrokerService()
                        .getTopic(topic, false)
                        .get().orElse(null);

        assertNotNull(persistentTopic);
        BrokerService brokerService = spy(persistentTopic.getBrokerService());
        AtomicBoolean isGetManagedLedgerConfigFail = new AtomicBoolean(false);
        doAnswer(invocation -> {
            if (isGetManagedLedgerConfigFail.get()) {
                return FutureUtil.failedFuture(new BrokerServiceException.ServiceUnitNotReadyException("test"));
            } else {
                return invocation.callRealMethod();
            }
        }).when(brokerService).getManagedLedgerConfig(any());
        Field field = AbstractTopic.class.getDeclaredField("brokerService");
        field.setAccessible(true);
        field.set(persistentTopic, brokerService);

        // init pending ack store
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic)
                .subscribe();

        producer.send("test");
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();

        isGetManagedLedgerConfigFail.set(true);
        // pending ack init fail, so the ack will throw exception
        try {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction).get();
            fail("ack should fail");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.LookupException);
        }

        // can unload success
        admin.topics().unload(topic);
    }
}
