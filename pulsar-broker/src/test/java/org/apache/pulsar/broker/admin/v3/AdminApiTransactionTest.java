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
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStatus;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
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
    public void testGetTransactionCoordinatorStatus() throws Exception {
        initTransaction(2);
        getTransaction().commit().get();
        getTransaction().abort().get();
        TransactionCoordinatorStatus transactionCoordinatorStatus =
                admin.transactions().getCoordinatorStatusById(1).get();
        verifyCoordinatorStatus(transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.leastSigBits, transactionCoordinatorStatus.lowWaterMark);

        transactionCoordinatorStatus = admin.transactions().getCoordinatorStatusById(0).get();
        verifyCoordinatorStatus(transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.leastSigBits, transactionCoordinatorStatus.lowWaterMark);
        Map<Integer, TransactionCoordinatorStatus> status = admin.transactions().getCoordinatorStatus().get();

        assertEquals(status.size(), 2);

        transactionCoordinatorStatus = status.get(0);
        verifyCoordinatorStatus(transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.leastSigBits, transactionCoordinatorStatus.lowWaterMark);

        transactionCoordinatorStatus = status.get(1);
        verifyCoordinatorStatus(transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.leastSigBits, transactionCoordinatorStatus.lowWaterMark);
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

    private static void verifyCoordinatorStatus(String state,
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
