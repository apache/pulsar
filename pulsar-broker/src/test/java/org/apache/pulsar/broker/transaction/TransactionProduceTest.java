/**
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
package org.apache.pulsar.broker.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionProduceTest extends TransactionTestBase {

    private static final int TOPIC_PARTITION = 3;
    private static final String PRODUCE_COMMIT_TOPIC = NAMESPACE1 + "/produce-commit";
    private static final String PRODUCE_ABORT_TOPIC = NAMESPACE1 + "/produce-abort";
    private static final String ACK_COMMIT_TOPIC = NAMESPACE1 + "/ack-commit";
    private static final String ACK_ABORT_TOPIC = NAMESPACE1 + "/ack-abort";
    private static final int NUM_PARTITIONS = 16;
    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(1, NUM_PARTITIONS, PRODUCE_COMMIT_TOPIC, TOPIC_PARTITION);
        admin.topics().createPartitionedTopic(PRODUCE_ABORT_TOPIC, TOPIC_PARTITION);
        admin.topics().createPartitionedTopic(ACK_COMMIT_TOPIC, TOPIC_PARTITION);
        admin.topics().createPartitionedTopic(ACK_ABORT_TOPIC, TOPIC_PARTITION);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void produceAndCommitTest() throws Exception {
        produceTest(true);
    }

    @Test
    public void produceAndAbortTest() throws Exception {
        produceTest(false);
    }

    // endAction - commit: true, endAction - abort: false
    private void produceTest(boolean endAction) throws Exception {
        final String topic = endAction ? PRODUCE_COMMIT_TOPIC : PRODUCE_ABORT_TOPIC;
        PulsarClient pulsarClient = this.pulsarClient;
        Transaction tnx = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        long txnIdMostBits = ((TransactionImpl) tnx).getTxnIdMostBits();
        long txnIdLeastBits = ((TransactionImpl) tnx).getTxnIdLeastBits();
        Assert.assertTrue(txnIdMostBits > -1);
        Assert.assertTrue(txnIdLeastBits > -1);

        @Cleanup
        Producer<byte[]> outProducer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCntPerPartition = 3;
        int messageCnt = TOPIC_PARTITION * messageCntPerPartition;
        String content = "Hello Txn - ";
        Set<String> messageSet = new HashSet<>();
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();
        for (int i = 0; i < messageCnt; i++) {
            String msg = content + i;
            messageSet.add(msg);
            CompletableFuture<MessageId> produceFuture = outProducer
                    .newMessage(tnx).value(msg.getBytes(UTF_8)).sendAsync();
            futureList.add(produceFuture);
        }

        checkMessageId(futureList, true);

        // the target topic hasn't the commit marker before commit
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(topic, i);
            Assert.assertNotNull(originTopicCursor);
            log.info("entries count: {}", originTopicCursor.getNumberOfEntries());
            Assert.assertEquals(messageCntPerPartition, originTopicCursor.getNumberOfEntries());

            List<Entry> entries = originTopicCursor.readEntries(messageCnt);
            // check the messages
            for (int j = 0; j < messageCntPerPartition; j++) {
                MessageMetadata messageMetadata = Commands.parseMessageMetadata(entries.get(j).getDataBuffer());
                Assert.assertEquals(messageMetadata.getTxnidMostBits(), txnIdMostBits);
                Assert.assertEquals(messageMetadata.getTxnidLeastBits(), txnIdLeastBits);

                byte[] bytes = new byte[entries.get(j).getDataBuffer().readableBytes()];
                entries.get(j).getDataBuffer().readBytes(bytes);
                System.out.println(new String(bytes));
                Assert.assertTrue(messageSet.remove(new String(bytes)));
            }
            originTopicCursor.close();
        }

        if (endAction) {
            tnx.commit().get();
        } else {
            tnx.abort().get();
        }

        for (int i = 0; i < TOPIC_PARTITION; i++) {
            // the target topic partition received the commit marker
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(topic, i);
            List<Entry> entries = originTopicCursor.readEntries((int) originTopicCursor.getNumberOfEntries());
            Assert.assertEquals(messageCntPerPartition + 1, entries.size());

            MessageMetadata messageMetadata = Commands.parseMessageMetadata(entries.get(messageCntPerPartition).getDataBuffer());
            if (endAction) {
                Assert.assertEquals(MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
            } else {
                Assert.assertEquals(MarkerType.TXN_ABORT_VALUE, messageMetadata.getMarkerType());
            }
        }

        Assert.assertEquals(0, messageSet.size());
        log.info("produce and {} test finished.", endAction ? "commit" : "abort");
    }

    private void checkMessageId(List<CompletableFuture<MessageId>> futureList, boolean isFinished) {
        futureList.forEach(messageIdFuture -> {
            try {
                MessageId messageId = messageIdFuture.get(1, TimeUnit.SECONDS);
                if (isFinished) {
                    Assert.assertNotNull(messageId);
                    log.info("Tnx finished success! messageId: {}", messageId);
                } else {
                    Assert.fail("MessageId shouldn't be get before txn abort.");
                }
            } catch (Exception e) {
                if (!isFinished) {
                    if (e instanceof TimeoutException) {
                        log.info("This is a expected exception.");
                    } else {
                        log.error("This exception is not expected.", e);
                        Assert.fail("This exception is not expected.");
                    }
                } else {
                    log.error("Tnx commit failed!", e);
                    Assert.fail("Tnx commit failed!");
                }
            }
        });
    }

    private ReadOnlyCursor getOriginTopicCursor(String topic, int partition) {
        try {
            if (partition >= 0) {
                topic = TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            }
            return getPulsarServiceList().get(0).getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(topic).getPersistenceNamingEncoding(),
                    PositionImpl.EARLIEST, new ManagedLedgerConfig());
        } catch (Exception e) {
            log.error("Failed to get origin topic readonly cursor.", e);
            Assert.fail("Failed to get origin topic readonly cursor.");
            return null;
        }
    }

    @Test
    public void ackCommitTest() throws Exception {
        final String subscriptionName = "ackCommitTest";
        Transaction txn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        log.info("init transaction {}.", txn);

        Producer<byte[]> incomingProducer = pulsarClient.newProducer()
                .topic(ACK_COMMIT_TOPIC)
                .batchingMaxMessages(1)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();
        int incomingMessageCnt = 10;
        for (int i = 0; i < incomingMessageCnt; i++) {
            incomingProducer.newMessage().value("Hello Txn.".getBytes()).sendAsync();
        }
        log.info("prepare incoming messages finished.");

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(ACK_COMMIT_TOPIC)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Awaitility.await().until(consumer::isConnected);

        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            log.info("receive messageId: {}", message.getMessageId());
            consumer.acknowledgeAsync(message.getMessageId(), txn);
        }

        // The pending messages count should be the incomingMessageCnt
        Awaitility.await().untilAsserted(
                () -> Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), incomingMessageCnt));

        consumer.redeliverUnacknowledgedMessages();
        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), incomingMessageCnt);

        txn.commit().get();

        // After commit, the pending messages count should be 0
        Awaitility.await().untilAsserted(
                () -> Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), 0));

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }

        log.info("finish test ackCommitTest");
    }

    @Test
    public void ackAbortTest() throws Exception {
        final String subscriptionName = "ackAbortTest";
        Transaction txn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build().get();
        log.info("init transaction {}.", txn);

        Producer<byte[]> incomingProducer = pulsarClient.newProducer()
                .topic(ACK_ABORT_TOPIC)
                .batchingMaxMessages(1)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();
        int incomingMessageCnt = 10;
        for (int i = 0; i < incomingMessageCnt; i++) {
            incomingProducer.newMessage().value("Hello Txn.".getBytes()).send();
        }
        log.info("prepare incoming messages finished.");

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(ACK_ABORT_TOPIC)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        Awaitility.await().until(consumer::isConnected);

        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            log.info("receive messageId: {}", message.getMessageId());
            consumer.acknowledgeAsync(message.getMessageId(), txn);
        }

        // The pending messages count should be the incomingMessageCnt
        Awaitility.await().untilAsserted(
                () -> Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), incomingMessageCnt));

        consumer.redeliverUnacknowledgedMessages();
        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), incomingMessageCnt);

        txn.abort().get();

        // After commit, the pending messages count should be 0
        Awaitility.await().untilAsserted(
                () -> Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), 0));

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            log.info("second receive messageId: {}", message.getMessageId());
        }

        log.info("finish test ackAbortTest");
    }

    private int getPendingAckCount(String topic, String subscriptionName) throws Exception {
        Class<PersistentSubscription> clazz = PersistentSubscription.class;

        int pendingAckCount = 0;
        for (PulsarService pulsarService : getPulsarServiceList()) {
            for (String key : pulsarService.getBrokerService().getTopics().keys()) {
                if (key.contains(topic)) {
                    Field field = clazz.getDeclaredField("pendingAckHandle");
                    field.setAccessible(true);
                    PersistentSubscription subscription =
                            (PersistentSubscription) pulsarService.getBrokerService()
                                    .getTopics().get(key).get().get().getSubscription(subscriptionName);
                    PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) field.get(subscription);
                    field = PendingAckHandleImpl.class.getDeclaredField("individualAckPositions");
                    field.setAccessible(true);

                    Map<PositionImpl, MutablePair<PositionImpl, Long>> map =
                            (Map<PositionImpl, MutablePair<PositionImpl, Long>>) field.get(pendingAckHandle);
                    if (map != null) {
                        pendingAckCount += map.size();
                    }
                }
            }
        }
        log.info("subscriptionName: {}, pendingAckCount: {}", subscriptionName, pendingAckCount);
        return pendingAckCount;
    }


    @Test
    public void testCommitFailure() throws Exception {
        Transaction txn = pulsarClient.newTransaction().build().get();
        final String topic = NAMESPACE1 + "/test-commit-failure";
        @Cleanup
        final Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        producer.newMessage(txn).value(new byte[1024 * 1024 * 10]).sendAsync();
        try {
            txn.commit().get();
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof PulsarClientException.InvalidMessageException);
            Assert.assertEquals(txn.getState(), Transaction.State.ABORTED);
        }
        try {
            getPulsarServiceList().get(0).getTransactionMetadataStoreService().getTxnMeta(txn.getTxnID())
                    .getNow(null);
            Assert.fail();
        } catch (CompletionException e) {
            Assert.assertTrue(e.getCause() instanceof CoordinatorException.TransactionNotFoundException);
        }
    }
}
