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
package org.apache.pulsar.broker.admin.v3;

import com.google.common.collect.Sets;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionMetadata;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AdminApiTransactionTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setEnablePackagesManagement(true);
        conf.setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        conf.setTransactionCoordinatorEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setTransactionBufferSnapshotMaxTransactionCount(1);
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Sets.newHashSet("test"));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testGetTransactionCoordinatorStats() throws Exception {
        initTransaction(2);
        getTransaction().commit().get();
        getTransaction().abort().get();
        TransactionCoordinatorStats transactionCoordinatorstats =
                admin.transactions().getCoordinatorStatsById(1).get();
        verifyCoordinatorStats(transactionCoordinatorstats.state,
                transactionCoordinatorstats.leastSigBits, transactionCoordinatorstats.lowWaterMark);

        transactionCoordinatorstats = admin.transactions().getCoordinatorStatsById(0).get();
        verifyCoordinatorStats(transactionCoordinatorstats.state,
                transactionCoordinatorstats.leastSigBits, transactionCoordinatorstats.lowWaterMark);
        Map<Integer, TransactionCoordinatorStats> stats = admin.transactions().getCoordinatorStats().get();

        assertEquals(stats.size(), 2);

        transactionCoordinatorstats = stats.get(0);
        verifyCoordinatorStats(transactionCoordinatorstats.state,
                transactionCoordinatorstats.leastSigBits, transactionCoordinatorstats.lowWaterMark);

        transactionCoordinatorstats = stats.get(1);
        verifyCoordinatorStats(transactionCoordinatorstats.state,
                transactionCoordinatorstats.leastSigBits, transactionCoordinatorstats.lowWaterMark);
    }

    @Test(timeOut = 20000)
    public void testGetTransactionInBufferStats() throws Exception {
        initTransaction(2);
        TransactionImpl transaction = (TransactionImpl) getTransaction();
        final String topic = "persistent://public/default/testGetTransactionInBufferStats";
        admin.topics().createNonPartitionedTopic(topic);
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topic).sendTimeout(0, TimeUnit.SECONDS).create();
        MessageId messageId = producer.newMessage(transaction).value("Hello pulsar!".getBytes()).send();
        TransactionInBufferStats transactionInBufferStats = admin.transactions()
                .getTransactionInBufferStats(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits()), topic).get();
        PositionImpl position =
                PositionImpl.get(((MessageIdImpl) messageId).getLedgerId(), ((MessageIdImpl) messageId).getEntryId());
        assertEquals(transactionInBufferStats.startPosition, position.toString());
        assertFalse(transactionInBufferStats.aborted);

        transaction.abort().get();

        transactionInBufferStats = admin.transactions()
                .getTransactionInBufferStats(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits()), topic).get();
        assertNull(transactionInBufferStats.startPosition);
        assertTrue(transactionInBufferStats.aborted);
    }

    @Test(timeOut = 20000)
    public void testGetTransactionPendingAckStats() throws Exception {
        initTransaction(2);
        final String topic = "persistent://public/default/testGetTransactionInBufferStats";
        final String subName = "test";
        admin.topics().createNonPartitionedTopic(topic);
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topic).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName(subName).subscribe();
        producer.sendAsync("Hello pulsar!".getBytes());
        producer.sendAsync("Hello pulsar!".getBytes());
        producer.sendAsync("Hello pulsar!".getBytes());
        producer.sendAsync("Hello pulsar!".getBytes());
        TransactionImpl transaction = (TransactionImpl) getTransaction();
        TransactionInPendingAckStats transactionInPendingAckStats = admin.transactions()
                .getTransactionInPendingAckStats(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits()), topic, subName).get();
        assertNull(transactionInPendingAckStats.cumulativeAckPosition);

        consumer.receive();
        consumer.receive();
        Message<byte[]> message = consumer.receive();
        BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) message.getMessageId();
        consumer.acknowledgeCumulativeAsync(batchMessageId, transaction).get();

        transactionInPendingAckStats = admin.transactions()
                .getTransactionInPendingAckStats(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits()), topic, subName).get();

        assertEquals(transactionInPendingAckStats.cumulativeAckPosition,
                String.valueOf(batchMessageId.getLedgerId()) +
                        ':' +
                        batchMessageId.getEntryId() +
                        ':' +
                        batchMessageId.getBatchIndex());
    }

    @Test(timeOut = 20000)
    public void testGetTransactionMetadata() throws Exception {
        initTransaction(2);
        long currentTime = System.currentTimeMillis();
        TransactionImpl transaction = (TransactionImpl) getTransaction();
        final String topic1 = "persistent://public/default/testGetTransactionMetadata-1";
        final String subName1 = "test1";
        final String topic2 = "persistent://public/default/testGetTransactionMetadata-2";
        final String subName2 = "test2";
        final String subName3 = "test3";
        admin.topics().createNonPartitionedTopic(topic1);
        admin.topics().createNonPartitionedTopic(topic2);

        Producer<byte[]> producer1 = pulsarClient.newProducer(Schema.BYTES)
                .sendTimeout(0, TimeUnit.SECONDS).topic(topic1).create();

        Producer<byte[]> producer2 = pulsarClient.newProducer(Schema.BYTES)
                .sendTimeout(0, TimeUnit.SECONDS).topic(topic2).create();

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer(Schema.BYTES).topic(topic1)
                .subscriptionName(subName1).subscribe();

        Consumer<byte[]> consumer2 = pulsarClient.newConsumer(Schema.BYTES).topic(topic2)
                .subscriptionName(subName2).subscribe();

        Consumer<byte[]> consumer3 = pulsarClient.newConsumer(Schema.BYTES).topic(topic2)
                .subscriptionName(subName3).subscribe();

        MessageId messageId1 = producer1.send("Hello pulsar!".getBytes());
        MessageId messageId2 = producer2.send("Hello pulsar!".getBytes());
        MessageId messageId3 = producer1.newMessage(transaction).value("Hello pulsar!".getBytes()).send();
        MessageId messageId4 = producer2.newMessage(transaction).value("Hello pulsar!".getBytes()).send();

        consumer1.acknowledgeCumulativeAsync(messageId1, transaction).get();
        consumer2.acknowledgeCumulativeAsync(messageId2, transaction).get();
        consumer3.acknowledgeCumulativeAsync(messageId2, transaction).get();
        TxnID txnID = new TxnID(transaction.getTxnIdMostBits(), transaction.getTxnIdLeastBits());
        TransactionMetadata transactionMetadata = admin.transactions()
                .getTransactionMetadata(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits())).get();

        assertEquals(transactionMetadata.txnId, txnID.toString());
        assertTrue(transactionMetadata.openTimestamp > currentTime);
        assertEquals(transactionMetadata.timeoutAt, 5000L);
        assertEquals(transactionMetadata.status, "OPEN");

        Map<String, TransactionInBufferStats> producedPartitions = transactionMetadata.producedPartitions;
        Map<String, Map<String, TransactionInPendingAckStats>> ackedPartitions = transactionMetadata.ackedPartitions;

        PositionImpl position1 = getPositionByMessageId(messageId1);
        PositionImpl position2 = getPositionByMessageId(messageId2);
        PositionImpl position3 = getPositionByMessageId(messageId3);
        PositionImpl position4 = getPositionByMessageId(messageId4);

        assertFalse(producedPartitions.get(topic1).aborted);
        assertFalse(producedPartitions.get(topic2).aborted);
        assertEquals(producedPartitions.get(topic1).startPosition, position3.toString());
        assertEquals(producedPartitions.get(topic2).startPosition, position4.toString());

        assertEquals(ackedPartitions.get(topic1).size(), 1);
        assertEquals(ackedPartitions.get(topic2).size(), 2);
        assertEquals(ackedPartitions.get(topic1).get(subName1).cumulativeAckPosition, position1.toString());
        assertEquals(ackedPartitions.get(topic2).get(subName2).cumulativeAckPosition, position2.toString());
        assertEquals(ackedPartitions.get(topic2).get(subName3).cumulativeAckPosition, position2.toString());

    }

    @Test(timeOut = 20000)
    public void testGetTransactionBufferStats() throws Exception {
        initTransaction(2);
        TransactionImpl transaction = (TransactionImpl) getTransaction();
        final String topic = "persistent://public/default/testGetTransactionBufferStats";
        final String subName1 = "test1";
        final String subName2 = "test2";
        admin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .sendTimeout(0, TimeUnit.SECONDS).topic(topic).create();
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName(subName1).subscribe();

        Consumer<byte[]> consumer2 = pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName(subName2).subscribe();
        long currentTime = System.currentTimeMillis();
        MessageId messageId = producer.newMessage(transaction).value("Hello pulsar!".getBytes()).send();
        transaction.commit().get();

        transaction = (TransactionImpl) getTransaction();
        consumer1.acknowledgeAsync(messageId, transaction).get();
        consumer2.acknowledgeAsync(messageId, transaction).get();

        TransactionBufferStats transactionBufferStats = admin.transactions().
                getTransactionBufferStats(topic).get();

        assertEquals(transactionBufferStats.state, "Ready");
        assertEquals(transactionBufferStats.maxReadPosition,
                PositionImpl.get(((MessageIdImpl) messageId).getLedgerId(),
                        ((MessageIdImpl) messageId).getEntryId() + 1).toString());
        assertTrue(transactionBufferStats.lastSnapshotTimestamps > currentTime);
    }

    @Test(timeOut = 20000)
    public void testGetPendingAckStats() throws Exception {
        initTransaction(2);
        final String topic = "persistent://public/default/testGetPendingAckStats";
        final String subName = "test1";
        admin.topics().createNonPartitionedTopic(topic);

        pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName(subName).subscribe();

        TransactionPendingAckStats transactionPendingAckStats = admin.transactions().
                getPendingAckStats(topic, subName).get();

        assertEquals(transactionPendingAckStats.state, "Ready");
    }

    private static PositionImpl getPositionByMessageId(MessageId messageId) {
        return PositionImpl.get(((MessageIdImpl) messageId).getLedgerId(), ((MessageIdImpl) messageId).getEntryId());
    }

    private static void verifyCoordinatorStats(String state,
                                                long sequenceId, long lowWaterMark) {
        assertEquals(state, "Ready");
        assertEquals(sequenceId, 0);
        assertEquals(lowWaterMark, 0);
    }

    private void initTransaction(int coordinatorSize) throws Exception {
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), coordinatorSize);
        admin.lookups().lookupTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        Awaitility.await().until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == coordinatorSize);
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true).build();
    }

    private Transaction getTransaction() throws Exception {
        return pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS).build().get();
    }
}
