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

import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.buffer.impl.PersistentTransactionBuffer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
public class TransactionProduceTest extends TransactionTestBase {

    private final static int TOPIC_PARTITION = 3;

    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String PRODUCE_COMMIT_TOPIC = NAMESPACE1 + "/produce-commit";
    private final static String PRODUCE_ABORT_TOPIC = NAMESPACE1 + "/produce-abort";
    private final static String ACK_COMMIT_TOPIC = NAMESPACE1 + "/ack-commit";
    private final static String ACK_ABORT_TOPIC = NAMESPACE1 + "/ack-abort";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(PRODUCE_COMMIT_TOPIC, 3);
        admin.topics().createPartitionedTopic(PRODUCE_ABORT_TOPIC, 3);
        admin.topics().createPartitionedTopic(ACK_COMMIT_TOPIC, 3);
        admin.topics().createPartitionedTopic(ACK_ABORT_TOPIC, 3);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void produceAndCommitTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Transaction tnx = pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        long txnIdMostBits = ((TransactionImpl) tnx).getTxnIdMostBits();
        long txnIdLeastBits = ((TransactionImpl) tnx).getTxnIdLeastBits();
        Assert.assertTrue(txnIdMostBits > -1);
        Assert.assertTrue(txnIdLeastBits > -1);

        @Cleanup
        PartitionedProducerImpl<byte[]> outProducer = (PartitionedProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(PRODUCE_COMMIT_TOPIC)
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

        // the target topic hasn't the commit marker before commit
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(PRODUCE_COMMIT_TOPIC, i);
            Assert.assertNotNull(originTopicCursor);
            Assert.assertFalse(originTopicCursor.hasMoreEntries());
            originTopicCursor.close();
        }

        // the messageId callback can't be called before commit
        checkMessageId(futureList, false);

        tnx.commit().get();

        // the messageId callback should be called after commit
        checkMessageId(futureList, true);

        Thread.sleep(1000);

        for (int i = 0; i < TOPIC_PARTITION; i++) {
            // the target topic partition received the commit marker
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(PRODUCE_COMMIT_TOPIC, i);
            Assert.assertNotNull(originTopicCursor);
            Assert.assertTrue(originTopicCursor.hasMoreEntries());
            List<Entry> entries = originTopicCursor.readEntries((int) originTopicCursor.getNumberOfEntries());
            Assert.assertEquals(1, entries.size());
            PulsarApi.MessageMetadata messageMetadata = Commands.parseMessageMetadata(entries.get(0).getDataBuffer());
            Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
            long commitMarkerLedgerId = entries.get(0).getLedgerId();
            long commitMarkerEntryId = entries.get(0).getEntryId();

            // the target topic transactionBuffer should receive the transaction messages,
            // committing marker and commit marker
            ReadOnlyCursor tbTopicCursor = getTBTopicCursor(PRODUCE_COMMIT_TOPIC, i);
            Assert.assertNotNull(tbTopicCursor);
            Assert.assertTrue(tbTopicCursor.hasMoreEntries());
            long tbEntriesCnt = tbTopicCursor.getNumberOfEntries();
            log.info("transaction buffer entries count: {}", tbEntriesCnt);
            Assert.assertEquals(tbEntriesCnt, messageCntPerPartition + 2);

            entries = tbTopicCursor.readEntries((int) tbEntriesCnt);
            // check the messages
            for (int j = 0; j < messageCntPerPartition; j++) {
                messageMetadata = Commands.parseMessageMetadata(entries.get(j).getDataBuffer());
                Assert.assertEquals(messageMetadata.getTxnidMostBits(), txnIdMostBits);
                Assert.assertEquals(messageMetadata.getTxnidLeastBits(), txnIdLeastBits);

                byte[] bytes = new byte[entries.get(j).getDataBuffer().readableBytes()];
                entries.get(j).getDataBuffer().readBytes(bytes);
                System.out.println(new String(bytes));
                Assert.assertTrue(messageSet.remove(new String(bytes)));
            }

            // check committing marker
            messageMetadata = Commands.parseMessageMetadata(entries.get(messageCntPerPartition).getDataBuffer());
            Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMITTING_VALUE, messageMetadata.getMarkerType());

            // check commit marker, committedAtLedgerId and committedAtEntryId
            messageMetadata = Commands.parseMessageMetadata(entries.get(messageCntPerPartition + 1).getDataBuffer());
            Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
            PulsarMarkers.TxnCommitMarker commitMarker = Markers.parseCommitMarker(entries.get(messageCntPerPartition + 1).getDataBuffer());
            Assert.assertEquals(commitMarkerLedgerId, commitMarker.getMessageId().getLedgerId());
            Assert.assertEquals(commitMarkerEntryId, commitMarker.getMessageId().getEntryId());
        }

        Assert.assertEquals(0, messageSet.size());
        System.out.println("finish test");
    }

    @Test
    public void produceAndAbortTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        TransactionImpl txn = (TransactionImpl) pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        @Cleanup
        PartitionedProducerImpl<byte[]> outProducer = (PartitionedProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(PRODUCE_ABORT_TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCntPerPartition = 3;
        int messageCnt = TOPIC_PARTITION * messageCntPerPartition;
        Set<String> messageSet = new HashSet<>();
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();
        for (int i = 0; i < messageCnt; i++) {
            String msg = "Hello Txn - " + i;
            messageSet.add(msg);
            CompletableFuture<MessageId> produceFuture = outProducer
                    .newMessage(txn).value(msg.getBytes(UTF_8)).sendAsync();
            futureList.add(produceFuture);
        }

        // the target topic hasn't the abort marker before commit
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(PRODUCE_ABORT_TOPIC, i);
            Assert.assertNotNull(originTopicCursor);
            Assert.assertFalse(originTopicCursor.hasMoreEntries());
            originTopicCursor.close();
        }

        // the messageId callback can't be called before commit
        checkMessageId(futureList, false);

        txn.abort().get();

        // the messageId callback should be called after commit
        checkMessageId(futureList, true);

        // the target topic partition doesn't have any entries
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(PRODUCE_ABORT_TOPIC, i);
            Assert.assertNotNull(originTopicCursor);
            Assert.assertFalse(originTopicCursor.hasMoreEntries());
        }

        // the target topic transactionBuffer should receive the transaction messages,
        // committing marker and commit marker
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            ReadOnlyCursor tbTopicCursor = getTBTopicCursor(PRODUCE_ABORT_TOPIC, i);
            Assert.assertNotNull(tbTopicCursor);
            Assert.assertTrue(tbTopicCursor.hasMoreEntries());
            long tbEntriesCnt = tbTopicCursor.getNumberOfEntries();
            log.info("transaction buffer entries count: {}", tbEntriesCnt);
            Assert.assertEquals(tbEntriesCnt, messageCntPerPartition + 1);

            PulsarApi.MessageMetadata messageMetadata;
            List<Entry> entries = tbTopicCursor.readEntries((int) tbEntriesCnt);
            // check the messages
            for (int j = 0; j < messageCntPerPartition; j++) {
                messageMetadata = Commands.parseMessageMetadata(entries.get(j).getDataBuffer());
                Assert.assertEquals(messageMetadata.getTxnidMostBits(), txn.getTxnIdMostBits());
                Assert.assertEquals(messageMetadata.getTxnidLeastBits(), txn.getTxnIdLeastBits());

                byte[] bytes = new byte[entries.get(j).getDataBuffer().readableBytes()];
                entries.get(j).getDataBuffer().readBytes(bytes);
                Assert.assertTrue(messageSet.remove(new String(bytes)));
            }

            // check abort marker
            messageMetadata = Commands.parseMessageMetadata(entries.get(messageCntPerPartition).getDataBuffer());
            Assert.assertEquals(PulsarMarkers.MarkerType.TXN_ABORT_VALUE, messageMetadata.getMarkerType());
        }

        Assert.assertEquals(0, messageSet.size());
        log.info("finish test produceAndAbortTest.");
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

    private ReadOnlyCursor getTBTopicCursor(String topic, int partition) {
        try {
            String topicSuffix = partition >= 0 ? TopicName.PARTITIONED_TOPIC_SUFFIX + partition : "";
            topic = PersistentTransactionBuffer.getTransactionBufferTopicName(
                    TopicName.get(topic).toString() + topicSuffix);

            return getPulsarServiceList().get(0).getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(topic).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
        } catch (Exception e) {
            log.error("Failed to get transaction buffer topic readonly cursor.", e);
            Assert.fail("Failed to get transaction buffer topic readonly cursor.");
            return null;
        }
    }

    private ReadOnlyCursor getOriginTopicCursor(String topic, int partition) {
        try {
            if (partition >= 0) {
                topic = TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            }
            return getPulsarServiceList().get(0).getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(topic).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
        } catch (Exception e) {
            log.error("Failed to get origin topic readonly cursor.", e);
            Assert.fail("Failed to get origin topic readonly cursor.");
            return null;
        }
    }

    @Test
    public void ackCommitTest() throws Exception {
        final String subscriptionName = "ackCommitTest";
        Transaction txn = ((PulsarClientImpl) pulsarClient)
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

        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(ACK_COMMIT_TOPIC)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            log.info("receive messageId: {}", message.getMessageId());
            consumer.acknowledgeAsync(message.getMessageId(), txn);
        }

        Thread.sleep(1000);

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), incomingMessageCnt);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), incomingMessageCnt);

        txn.commit().get();

        Thread.sleep(1000);

        // After commit, the pending messages count should be 0
        Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), 0);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }

        log.info("finish test ackCommitTest");
    }

    @Test
    public void ackAbortTest() throws Exception {
        final String subscriptionName = "ackAbortTest";
        Transaction txn = ((PulsarClientImpl) pulsarClient)
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        log.info("init transaction {}.", txn);

        Producer<byte[]> incomingProducer = pulsarClient.newProducer()
                .topic(ACK_ABORT_TOPIC)
                .batchingMaxMessages(1)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();
        int incomingMessageCnt = 10;
        for (int i = 0; i < incomingMessageCnt; i++) {
            incomingProducer.newMessage().value("Hello Txn.".getBytes()).sendAsync();
        }
        log.info("prepare incoming messages finished.");

        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(ACK_ABORT_TOPIC)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            log.info("receive messageId: {}", message.getMessageId());
            consumer.acknowledgeAsync(message.getMessageId(), txn);
        }

        Thread.sleep(1000);

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), incomingMessageCnt);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), incomingMessageCnt);

        txn.abort().get();

        Thread.sleep(1000);

        // After commit, the pending messages count should be 0
        Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), 0);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            log.info("second receive messageId: {}", message.getMessageId());
        }

        log.info("finish test ackAbortTest");
    }

    private int getPendingAckCount(String topic, String subscriptionName) throws Exception {
        Class<PersistentSubscription> clazz = PersistentSubscription.class;
        Field field = clazz.getDeclaredField("pendingAckMessages");
        field.setAccessible(true);

        int pendingAckCount = 0;
        for (PulsarService pulsarService : getPulsarServiceList()) {
            for (String key : pulsarService.getBrokerService().getTopics().keys()) {
                if (key.contains(topic)) {
                    PersistentSubscription subscription =
                            (PersistentSubscription) pulsarService.getBrokerService()
                                    .getTopics().get(key).get().get().getSubscription(subscriptionName);
                    ConcurrentOpenHashSet<Position> set = (ConcurrentOpenHashSet<Position>) field.get(subscription);
                    if (set != null) {
                        pendingAckCount += set.size();
                    }
                }
            }
        }
        log.info("subscriptionName: {}, pendingAckCount: {}", subscriptionName, pendingAckCount);
        return pendingAckCount;
    }


}
