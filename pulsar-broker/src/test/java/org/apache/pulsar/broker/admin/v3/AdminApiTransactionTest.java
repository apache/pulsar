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
package org.apache.pulsar.broker.admin.v3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.http.HttpStatus;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorInfo;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorInternalStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionMetadata;
import org.apache.pulsar.common.policies.data.TransactionPendingAckInternalStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.stats.PositionInPendingAckStats;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class AdminApiTransactionTest extends MockedPulsarServiceBaseTest {

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        conf.setEnablePackagesManagement(true);
        conf.setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        conf.setTransactionCoordinatorEnabled(true);
        conf.setTransactionBufferSnapshotMaxTransactionCount(1);
        return conf;
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Set.of("test"));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default", Set.of("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testListTransactionCoordinators() throws Exception {
        initTransaction(4);
        final List<TransactionCoordinatorInfo> result = admin
                .transactions().listTransactionCoordinatorsAsync().get();
        assertEquals(result.size(), 4);
        final String expectedUrl = pulsar.getBrokerServiceUrl();
        for (int i = 0; i < 4; i++) {
            assertEquals(result.get(i).getBrokerServiceUrl(), expectedUrl);
        }
    }

    @Test(timeOut = 20000)
    public void testGetTransactionCoordinatorStats() throws Exception {
        initTransaction(2);
        getTransaction().commit().get();
        getTransaction().abort().get();
        TransactionCoordinatorStats transactionCoordinatorstats =
                admin.transactions().getCoordinatorStatsByIdAsync(1).get();
        verifyCoordinatorStats(transactionCoordinatorstats.state,
                transactionCoordinatorstats.leastSigBits, transactionCoordinatorstats.lowWaterMark);

        transactionCoordinatorstats = admin.transactions().getCoordinatorStatsByIdAsync(0).get();
        verifyCoordinatorStats(transactionCoordinatorstats.state,
                transactionCoordinatorstats.leastSigBits, transactionCoordinatorstats.lowWaterMark);
        Map<Integer, TransactionCoordinatorStats> stats = admin.transactions().getCoordinatorStatsAsync().get();

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
        try {
            admin.transactions()
                    .getTransactionInBufferStatsAsync(new TxnID(1, 1), topic).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        try {
            pulsar.getBrokerService().getTopic(topic, false);
            admin.transactions()
                    .getTransactionInBufferStatsAsync(new TxnID(1, 1), topic).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        admin.topics().createNonPartitionedTopic(topic);
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topic).sendTimeout(0, TimeUnit.SECONDS).create();
        MessageId messageId = producer.newMessage(transaction).value("Hello pulsar!".getBytes()).send();
        TransactionInBufferStats transactionInBufferStats = admin.transactions()
                .getTransactionInBufferStatsAsync(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits()), topic).get();
        PositionImpl position =
                PositionImpl.get(((MessageIdImpl) messageId).getLedgerId(), ((MessageIdImpl) messageId).getEntryId());
        assertEquals(transactionInBufferStats.startPosition, position.toString());
        assertFalse(transactionInBufferStats.aborted);

        transaction.abort().get();

        transactionInBufferStats = admin.transactions()
                .getTransactionInBufferStatsAsync(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits()), topic).get();
        assertNull(transactionInBufferStats.startPosition);
        assertTrue(transactionInBufferStats.aborted);
    }

    @Test(timeOut = 20000)
    public void testGetTransactionInPendingAckStats() throws Exception {
        initTransaction(2);
        final String topic = "persistent://public/default/testGetTransactionInBufferStats";
        final String subName = "test";
        try {
            admin.transactions()
                    .getTransactionInPendingAckStatsAsync(new TxnID(1,
                            2), topic, subName).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        try {
            pulsar.getBrokerService().getTopic(topic, false);
            admin.transactions()
                    .getTransactionInPendingAckStatsAsync(new TxnID(1,
                            2), topic, subName).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
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
                .getTransactionInPendingAckStatsAsync(new TxnID(transaction.getTxnIdMostBits(),
                        transaction.getTxnIdLeastBits()), topic, subName).get();
        assertNull(transactionInPendingAckStats.cumulativeAckPosition);

        consumer.receive();
        consumer.receive();
        Message<byte[]> message = consumer.receive();
        BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) message.getMessageId();
        consumer.acknowledgeCumulativeAsync(batchMessageId, transaction).get();

        transactionInPendingAckStats = admin.transactions()
                .getTransactionInPendingAckStatsAsync(new TxnID(transaction.getTxnIdMostBits(),
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
        final String topic1 = "persistent://public/default/testGetTransactionMetadata-1";
        final String subName1 = "test1";
        final String topic2 = "persistent://public/default/testGetTransactionMetadata-2";
        final String subName2 = "test2";
        final String subName3 = "test3";
        admin.topics().createNonPartitionedTopic(topic1);
        admin.topics().createNonPartitionedTopic(topic2);
        TransactionImpl transaction = (TransactionImpl) getTransaction();

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
                .getTransactionMetadataAsync(new TxnID(transaction.getTxnIdMostBits(),
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
        try {
            admin.transactions()
                    .getTransactionBufferStatsAsync(topic).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        try {
            pulsar.getBrokerService().getTopic(topic, false);
            admin.transactions()
                    .getTransactionBufferStatsAsync(topic).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
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
                getTransactionBufferStatsAsync(topic).get();

        assertEquals(transactionBufferStats.state, "Ready");
        assertEquals(transactionBufferStats.maxReadPosition,
                PositionImpl.get(((MessageIdImpl) messageId).getLedgerId(),
                        ((MessageIdImpl) messageId).getEntryId() + 1).toString());
        assertTrue(transactionBufferStats.lastSnapshotTimestamps > currentTime);
        assertNull(transactionBufferStats.lowWaterMarks);
        transactionBufferStats = admin.transactions().getTransactionBufferStats(topic, true);
        assertNotNull(transactionBufferStats.lowWaterMarks);
    }

    @DataProvider(name = "ackType")
    public static Object[] ackType() {
        return new Object[] { "cumulative", "individual"};
    }

    @Test(timeOut = 20000, dataProvider = "ackType")
    public void testGetPendingAckStats(String ackType) throws Exception {
        initTransaction(2);
        final String topic = "persistent://public/default/testGetPendingAckStats";
        final String subName = "test1";
        try {
            admin.transactions()
                    .getPendingAckStatsAsync(topic, subName).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        try {
            pulsar.getBrokerService().getTopic(topic, false);
            admin.transactions()
                    .getPendingAckStatsAsync(topic, subName).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        admin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .sendTimeout(0, TimeUnit.SECONDS).topic(topic).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName(subName).subscribe();
        TransactionPendingAckStats transactionPendingAckStats = admin.transactions().
                getPendingAckStatsAsync(topic, subName).get();
        assertEquals(transactionPendingAckStats.state, "None");

        producer.newMessage().value("Hello pulsar!".getBytes()).send();

        TransactionImpl transaction  = (TransactionImpl) getTransaction();
        if (ackType.equals("individual")) {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction).get();
        } else {
            consumer.acknowledgeCumulativeAsync(consumer.receive().getMessageId(), transaction).get();
        }

        transactionPendingAckStats = admin.transactions().
                getPendingAckStatsAsync(topic, subName).get();
        assertNull(transactionPendingAckStats.lowWaterMarks);
        assertEquals(transactionPendingAckStats.state, "Ready");
    }

    @Test
    public void testTransactionGetStats() throws Exception {
        initTransaction(1);
        final String topic = "persistent://public/default/testGetPendingAckStats";
        final String subName = "test1";

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .sendTimeout(0, TimeUnit.SECONDS).topic(topic).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName(subName).subscribe();

        Transaction transaction1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        Transaction transaction2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        producer.newMessage().send();
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message.getMessageId(), transaction1).get();
        for (int i = 0; i < 5; i++) {
            producer.newMessage(transaction1).send();
        }
        transaction1.commit().get();
        message = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message.getMessageId(), transaction2).get();
        producer.newMessage(transaction2).send();

        TransactionBufferStats transactionBufferStats =
                admin.transactions().getTransactionBufferStats(topic, true);
        assertEquals(transactionBufferStats.ongoingTxnSize, 1);
        assertNotNull(transactionBufferStats.lowWaterMarks);

        TransactionPendingAckStats transactionPendingAckStats =
                admin.transactions().getPendingAckStats(topic, subName, true);

        assertEquals(transactionPendingAckStats.ongoingTxnSize, 1);
        assertNotNull(transactionPendingAckStats.lowWaterMarks);
        assertEquals(admin.transactions().getCoordinatorStatsById(0).ongoingTxnSize, 1);
    }

    @Test(timeOut = 20000)
    public void testGetSlowTransactions() throws Exception {
        initTransaction(2);
        TransactionImpl transaction1 = (TransactionImpl) pulsarClient.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();
        TransactionImpl transaction2 = (TransactionImpl) pulsarClient.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();
        pulsarClient.newTransaction().withTransactionTimeout(20, TimeUnit.SECONDS).build();
        pulsarClient.newTransaction().withTransactionTimeout(20, TimeUnit.SECONDS).build();

        Map<String, TransactionMetadata> transactionMetadataMap =  admin.transactions()
                .getSlowTransactionsAsync(30, TimeUnit.SECONDS).get();

        assertEquals(transactionMetadataMap.size(), 2);

        TxnID txnID1 = new TxnID(transaction1.getTxnIdMostBits(), transaction1.getTxnIdLeastBits());
        TxnID txnID2 = new TxnID(transaction2.getTxnIdMostBits(), transaction2.getTxnIdLeastBits());

        TransactionMetadata transactionMetadata = transactionMetadataMap.get(txnID1.toString());
        assertNotNull(transactionMetadata);
        assertEquals(transactionMetadata.timeoutAt, 60000);

        transactionMetadata = transactionMetadataMap.get(txnID2.toString());
        assertNotNull(transactionMetadata);
        assertEquals(transactionMetadata.timeoutAt, 60000);
    }

    private static PositionImpl getPositionByMessageId(MessageId messageId) {
        return PositionImpl.get(((MessageIdImpl) messageId).getLedgerId(), ((MessageIdImpl) messageId).getEntryId());
    }

    @Test(timeOut = 20000)
    public void testGetCoordinatorInternalStats() throws Exception {
        initTransaction(1);
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        TransactionCoordinatorInternalStats stats = admin.transactions()
                .getCoordinatorInternalStatsAsync(0, true).get();
        verifyManagedLegerInternalStats(stats.transactionLogStats.managedLedgerInternalStats, 26);
        assertEquals(TopicName.get(TopicDomain.persistent.toString(), NamespaceName.SYSTEM_NAMESPACE,
                MLTransactionLogImpl.TRANSACTION_LOG_PREFIX + "0").getPersistenceNamingEncoding(),
                stats.transactionLogStats.managedLedgerName);

        transaction.commit().get();

        stats = admin.transactions()
                .getCoordinatorInternalStatsAsync(0, false).get();
        assertNull(stats.transactionLogStats.managedLedgerInternalStats.ledgers.get(0).metadata);
        assertEquals(TopicName.get(TopicDomain.persistent.toString(), NamespaceName.SYSTEM_NAMESPACE,
                MLTransactionLogImpl.TRANSACTION_LOG_PREFIX + "0").getPersistenceNamingEncoding(),
                stats.transactionLogStats.managedLedgerName);
    }

    @Test(timeOut = 20000)
    public void testGetPendingAckInternalStats() throws Exception {
        initTransaction(1);
        TransactionImpl transaction = (TransactionImpl) getTransaction();
        final String topic = "persistent://public/default/testGetPendingAckInternalStats";
        final String subName = "test";
        try {
            admin.transactions()
                    .getPendingAckInternalStatsAsync(topic, subName, true).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        try {
            pulsar.getBrokerService().getTopic(topic, false);
            admin.transactions()
                    .getPendingAckInternalStatsAsync(topic, subName, true).get();
            fail("Should failed here");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PulsarAdminException.NotFoundException);
            PulsarAdminException.NotFoundException cause = (PulsarAdminException.NotFoundException)ex.getCause();
            assertEquals(cause.getMessage(), "Topic not found");
        }
        admin.topics().createNonPartitionedTopic(topic);
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topic).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName(subName).subscribe();
        MessageId messageId = producer.send("Hello pulsar!".getBytes());
        consumer.acknowledgeAsync(messageId, transaction).get();

        TransactionPendingAckInternalStats stats = admin.transactions()
                .getPendingAckInternalStatsAsync(topic, subName, true).get();
        ManagedLedgerInternalStats managedLedgerInternalStats = stats.pendingAckLogStats.managedLedgerInternalStats;
        assertEquals(TopicName.get(TopicDomain.persistent.toString(), "public", "default",
                "testGetPendingAckInternalStats" + "-"
                        + subName + SystemTopicNames.PENDING_ACK_STORE_SUFFIX).getPersistenceNamingEncoding(),
                stats.pendingAckLogStats.managedLedgerName);

        verifyManagedLegerInternalStats(managedLedgerInternalStats, 16);

        ManagedLedgerInternalStats finalManagedLedgerInternalStats = managedLedgerInternalStats;
        managedLedgerInternalStats.cursors.forEach((s, cursorStats) -> {
            assertEquals(s, SystemTopicNames.PENDING_ACK_STORE_CURSOR_NAME);
            assertEquals(cursorStats.readPosition, finalManagedLedgerInternalStats.lastConfirmedEntry);
        });

        stats = admin.transactions()
                .getPendingAckInternalStatsAsync(topic, subName, false).get();
        managedLedgerInternalStats = stats.pendingAckLogStats.managedLedgerInternalStats;

        assertEquals(TopicName.get(TopicDomain.persistent.toString(), "public", "default",
                "testGetPendingAckInternalStats" + "-"
                        + subName + SystemTopicNames.PENDING_ACK_STORE_SUFFIX).getPersistenceNamingEncoding(),
                stats.pendingAckLogStats.managedLedgerName);
        assertNull(managedLedgerInternalStats.ledgers.get(0).metadata);
    }

    @Test(timeOut = 20000)
    public void testTransactionNotEnabled() throws Exception {
        cleanup();
        conf.setTransactionCoordinatorEnabled(false);
        setup();
        try {
            admin.transactions().getCoordinatorInternalStats(1, false);
        } catch (PulsarAdminException ex) {
            assertEquals(ex.getStatusCode(), HttpStatus.SC_SERVICE_UNAVAILABLE);
        }
        try {
            admin.transactions().scaleTransactionCoordinators(1);
        } catch (PulsarAdminException ex) {
            assertEquals(ex.getStatusCode(), HttpStatus.SC_SERVICE_UNAVAILABLE);
        }
    }

    @Test
    public void testUpdateTransactionCoordinatorNumber() throws Exception {
        int coordinatorSize = 3;
        pulsar.getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(coordinatorSize));
        try {
            admin.transactions().scaleTransactionCoordinators(coordinatorSize - 1);
            fail();
        } catch (PulsarAdminException pulsarAdminException) {
            assertEquals(pulsarAdminException.getStatusCode(), HttpStatus.SC_NOT_ACCEPTABLE);
        }
        try {
            admin.transactions().scaleTransactionCoordinators(-1);
            fail();
        } catch (PulsarAdminException pulsarAdminException) {
            assertEquals(pulsarAdminException.getCause().getMessage(),
                    "Number of transaction coordinators must be more than 0");
        }

        admin.transactions().scaleTransactionCoordinators(coordinatorSize * 2);
        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true));
        pulsarClient.close();
        pulsarClient = null;
        Awaitility.await().until(() -> pulsar.getTransactionMetadataStoreService().getStores().size() ==
                        coordinatorSize * 2);
        pulsar.getConfiguration().setAuthenticationEnabled(true);
        Set<String> proxyRoles = spy(Set.class);
        doReturn(true).when(proxyRoles).contains(any());
        pulsar.getConfiguration().setProxyRoles(proxyRoles);
        try {
            admin.transactions().scaleTransactionCoordinators(coordinatorSize * 2 + 1);
            fail();
        } catch (PulsarAdminException.NotAuthorizedException ignored) {
        }
    }

    @Test
    public void testGetRecoveryTime() throws Exception {
        initTransaction(1);
        final String topic = "persistent://public/default/testGetRecoveryTime";
        final String subName = "test";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subName)
                .topic(topic)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic)
                .create();

        Awaitility.await().untilAsserted(() -> {
            Map<Integer, TransactionCoordinatorStats> transactionCoordinatorStatsMap =
                    admin.transactions().getCoordinatorStats();
            assertNotEquals(transactionCoordinatorStatsMap.get(0).recoverStartTime, 0L);
            assertNotEquals(transactionCoordinatorStatsMap.get(0).recoverEndTime, 0L);
            assertNotEquals(transactionCoordinatorStatsMap.get(0).recoverEndTime, -1L);
        });
        Awaitility.await().untilAsserted(() -> {
            TransactionBufferStats transactionBufferStats = admin.transactions().getTransactionBufferStats(topic);
            assertNotEquals(transactionBufferStats.recoverStartTime, 0L);
            assertNotEquals(transactionBufferStats.recoverEndTime, 0L);
            assertNotEquals(transactionBufferStats.recoverEndTime, -1L);
        });

        TransactionPendingAckStats transactionPendingAckStats =
                admin.transactions().getPendingAckStats(topic, subName);
        assertEquals(transactionPendingAckStats.recoverStartTime, 0L);
        assertEquals(transactionPendingAckStats.recoverEndTime, 0L);

        Transaction transaction1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.MINUTES)
                .build()
                .get();

        producer.newMessage().send();
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);

        consumer.acknowledgeAsync(message.getMessageId(), transaction1);
        transaction1.commit().get();

        transactionPendingAckStats =
                admin.transactions().getPendingAckStats(topic, subName);
        assertNotEquals(transactionPendingAckStats.recoverStartTime, 0L);
        assertNotEquals(transactionPendingAckStats.recoverEndTime, 0L);
        assertNotEquals(transactionPendingAckStats.recoverEndTime, -1L);
    }


    @Test
    public void testCheckPositionInPendingAckState() throws Exception {
         String topic = "persistent://public/default/test";
         String subName = "sub";
         initTransaction(1);
         Transaction transaction = pulsarClient.newTransaction()
                 .withTransactionTimeout(5, TimeUnit.SECONDS)
                 .build()
                 .get();

         @Cleanup
         Producer<byte[]> producer = pulsarClient.newProducer()
                 .sendTimeout(5, TimeUnit.SECONDS)
                 .enableBatching(false)
                 .topic(topic)
                 .create();
         @Cleanup
         Consumer<byte[]> consumer = pulsarClient.newConsumer()
                 .topic(topic)
                 .subscriptionName(subName)
                 .subscribe();

         producer.newMessage().send();

         Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
         MessageIdImpl messageId = (MessageIdImpl) message.getMessageId();

         PositionInPendingAckStats result = admin.transactions().getPositionStatsInPendingAck(topic, subName,
                 messageId.getLedgerId(), messageId.getEntryId(), null);
         assertEquals(result.state, PositionInPendingAckStats.State.PendingAckNotReady);

         consumer.acknowledgeAsync(messageId, transaction).get();
         result = admin.transactions().getPositionStatsInPendingAck(topic, subName,
                messageId.getLedgerId(), messageId.getEntryId(), null);
         assertEquals(result.state, PositionInPendingAckStats.State.PendingAck);
         transaction.commit().get();
         Awaitility.await().untilAsserted(() -> {
             PositionInPendingAckStats r = admin.transactions().getPositionStatsInPendingAck(topic, subName,
                     messageId.getLedgerId(), messageId.getEntryId(), null);
             assertEquals(r.state, PositionInPendingAckStats.State.MarkDelete);
         });
    }

    @Test
    public void testGetPositionStatsInPendingAckStatsFroBatch() throws Exception {
        String topic = "persistent://public/default/test";
        String subscriptionName = "my-subscription-batch";
        initTransaction(1);
        pulsar.getBrokerService()
                .getManagedLedgerConfig(TopicName.get(topic)).get()
                .setDeletionAtBatchIndexLevelEnabled(true);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true)
                .batchingMaxMessages(3)
                // set batch max publish delay big enough to make sure entry has 3 messages
                .batchingMaxPublishDelay(3, TimeUnit.SECONDS)
                .topic(topic).create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscriptionName)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
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

        PositionInPendingAckStats positionStatsInPendingAckStats =
                admin.transactions().getPositionStatsInPendingAck(topic, subscriptionName,
                messageId.getLedgerId(), messageId.getEntryId(), 1);
        assertEquals(positionStatsInPendingAckStats.state, PositionInPendingAckStats.State.PendingAck);

        positionStatsInPendingAckStats =
                admin.transactions().getPositionStatsInPendingAck(topic, subscriptionName,
                        messageId.getLedgerId(), messageId.getEntryId(), 2);
        assertEquals(positionStatsInPendingAckStats.state, PositionInPendingAckStats.State.NotInPendingAck);
        positionStatsInPendingAckStats =
                admin.transactions().getPositionStatsInPendingAck(topic, subscriptionName,
                        messageId.getLedgerId(), messageId.getEntryId(), 10);
        assertEquals(positionStatsInPendingAckStats.state, PositionInPendingAckStats.State.InvalidPosition);

    }

    private static void verifyCoordinatorStats(String state,
                                               long sequenceId, long lowWaterMark) {
        assertEquals(state, "Ready");
        assertEquals(sequenceId, 0);
        assertEquals(lowWaterMark, 0);
    }

    private void initTransaction(int coordinatorSize) throws Exception {
        pulsar.getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(coordinatorSize));
        admin.lookups().lookupTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString());
        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true));
        pulsarClient.close();
        pulsarClient = null;
        Awaitility.await().until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == coordinatorSize);
        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true));
    }

    private Transaction getTransaction() throws Exception {
        return pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS).build().get();
    }

    private static void verifyManagedLegerInternalStats(ManagedLedgerInternalStats managedLedgerInternalStats,
                                                        long totalSize) {
        assertEquals(managedLedgerInternalStats.entriesAddedCounter, 1);
        assertEquals(managedLedgerInternalStats.numberOfEntries, 1);
        assertEquals(managedLedgerInternalStats.totalSize, totalSize);
        assertEquals(managedLedgerInternalStats.currentLedgerEntries, 1);
        assertEquals(managedLedgerInternalStats.currentLedgerSize, totalSize);
        assertNull(managedLedgerInternalStats.lastLedgerCreationFailureTimestamp);
        assertEquals(managedLedgerInternalStats.waitingCursorsCount, 0);
        assertEquals(managedLedgerInternalStats.pendingAddEntriesCount, 0);
        assertNotNull(managedLedgerInternalStats.lastConfirmedEntry);
        assertEquals(managedLedgerInternalStats.ledgers.size(), 1);
        assertNotNull(managedLedgerInternalStats.ledgers.get(0).metadata);
        assertEquals(managedLedgerInternalStats.cursors.size(), 1);
    }
}
