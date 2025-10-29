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

import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl.TRANSACTION_LOG_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.GeoPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-replication")
public class ReplicationTxnTest extends OneWayReplicatorTestBase {

    private boolean transactionBufferSegmentedSnapshotEnabled = false;
    private int txnLogPartitions = 4;

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Override
    protected PulsarClient initClient(ClientBuilder clientBuilder) throws Exception {
        return clientBuilder.enableTransaction(true).build();
    }

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setSystemTopicEnabled(true);
        config.setTopicLevelPoliciesEnabled(true);
        config.setTransactionCoordinatorEnabled(true);
        config.setTransactionLogBatchedWriteEnabled(true);
        config.setTransactionPendingAckBatchedWriteEnabled(true);
        config.setTransactionBufferSegmentedSnapshotEnabled(transactionBufferSegmentedSnapshotEnabled);
    }

    @Override
    protected void createDefaultTenantsAndClustersAndNamespace() throws Exception {
        super.createDefaultTenantsAndClustersAndNamespace();

        // Create resource that transaction function relies on.
        admin1.tenants().createTenant(SYSTEM_NAMESPACE.getTenant(), new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1, cluster2)));
        admin1.namespaces().createNamespace(SYSTEM_NAMESPACE.toString(), 4);
        pulsar1.getPulsarResources().getNamespaceResources().getPartitionedTopicResources().createPartitionedTopic(
                SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN, new PartitionedTopicMetadata(txnLogPartitions));
        //admin1.topics().createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString(), 4);

        admin2.tenants().createTenant(SYSTEM_NAMESPACE.getTenant(), new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1, cluster2)));
        admin2.namespaces().createNamespace(SYSTEM_NAMESPACE.toString(), 4);
        pulsar2.getPulsarResources().getNamespaceResources().getPartitionedTopicResources().createPartitionedTopic(
                SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN, new PartitionedTopicMetadata(txnLogPartitions));
    }

    private void pubAndSubOneMsg(String topic, String subscription) throws Exception {
        Consumer consumer1 = client1.newConsumer(Schema.STRING).topic(topic).subscriptionName(subscription)
                .isAckReceiptEnabled(true).subscribe();
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topic).create();
        producer1.newMessage().value("msg1").send();
        // start txn.
        Transaction txn = client1.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        // consume.
        Message<String> c1Msg1 = consumer1.receive(5, TimeUnit.SECONDS);
        assertNotNull(c1Msg1);
        assertEquals(c1Msg1.getValue(), "msg1");
        consumer1.acknowledgeAsync(c1Msg1.getMessageId(), txn).join();
        // send.
        producer1.newMessage(txn).value("msg2").send();
        // commit.
        txn.commit().get();

        // Consume the msg with TXN.
        Message<String> c1Msg2 = consumer1.receive(5, TimeUnit.SECONDS);
        assertNotNull(c1Msg2);
        assertEquals(c1Msg2.getValue(), "msg2");
        consumer1.acknowledgeAsync(c1Msg2.getMessageId()).join();

        // Consume messages on the remote cluster.
        Consumer consumer2 = client2.newConsumer(Schema.STRING).topic(topic).subscriptionName(subscription).subscribe();
        Message<String> c2Msg1 = consumer2.receive(15, TimeUnit.SECONDS);
        assertNotNull(c2Msg1);
        MessageMetadata msgMetadata1 = WhiteboxImpl.getInternalState(c2Msg1, "msgMetadata");
        // Verify: the messages replicated has no TXN id.
        assertFalse(msgMetadata1.hasTxnidMostBits());
        assertFalse(msgMetadata1.hasTxnidLeastBits());
        consumer2.acknowledge(c2Msg1);
        Message<String> c2Msg2 = consumer2.receive(15, TimeUnit.SECONDS);
        assertNotNull(c2Msg2);
        MessageMetadata msgMetadata2 = WhiteboxImpl.getInternalState(c2Msg2, "msgMetadata");
        // Verify: the messages replicated has no TXN id.
        assertFalse(msgMetadata2.hasTxnidMostBits());
        assertFalse(msgMetadata2.hasTxnidLeastBits());
        consumer2.acknowledge(c2Msg2);

        // cleanup.
        producer1.close();
        consumer1.close();
        consumer2.close();
    }

    private void verifyNoReplicator(BrokerService broker, TopicName topicName) throws Exception {
        String tpStr = topicName.toString();
        CompletableFuture<Optional<Topic>> future = broker.getTopic(tpStr, true);
        if (future == null) {
            return;
        }
        PersistentTopic persistentTopic = (PersistentTopic) future.join().get();
        assertTrue(persistentTopic.getReplicators().isEmpty());
    }

    @Test
    public void testTxnLogNotBeReplicated() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp");
        final String subscription = "s1";
        admin1.topics().createNonPartitionedTopic(topic);
        waitReplicatorStarted(topic);
        admin1.topics().createSubscription(topic, subscription, MessageId.earliest);
        admin2.topics().createSubscription(topic, subscription, MessageId.earliest);
        // Pub & Sub.
        pubAndSubOneMsg(topic, subscription);
        // To cover more cases, sleep 3s.
        Thread.sleep(3000);

        // Verify: messages on the TXN system topic did not been replicated.
        // __transaction_log_: it only uses ML, will not create topic.
        for (int i = 0; i < txnLogPartitions; i++) {
            TopicName txnLog = TopicName.get(TopicDomain.persistent.value(),
                    NamespaceName.SYSTEM_NAMESPACE, TRANSACTION_LOG_PREFIX + i);
            assertNotNull(pulsar1.getManagedLedgerFactory()
                    .getManagedLedgerInfo(txnLog.getPersistenceNamingEncoding()));
            assertFalse(broker1.getTopics().containsKey(txnLog.toString()));
        }
        // __transaction_pending_ack: it only uses ML, will not create topic.
        TopicName pendingAck = TopicName.get(
                MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subscription));
        assertNotNull(pulsar1.getManagedLedgerFactory()
                .getManagedLedgerInfo(pendingAck.getPersistenceNamingEncoding()));
        assertFalse(broker1.getTopics().containsKey(pendingAck.toString()));
        // __transaction_buffer_snapshot.
        verifyNoReplicator(broker1, TopicName.get(TopicDomain.persistent.value(),
                TopicName.get(topic).getNamespaceObject(),
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT));
        verifyNoReplicator(broker1, TopicName.get(TopicDomain.persistent.value(),
                TopicName.get(topic).getNamespaceObject(),
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS));
        verifyNoReplicator(broker1, TopicName.get(TopicDomain.persistent.value(),
                TopicName.get(topic).getNamespaceObject(),
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_INDEXES));

        // cleanup.
        cleanupTopics(() -> {
            admin1.topics().delete(topic);
            admin2.topics().delete(topic);
            try {
                admin1.topics().delete(pendingAck.toString());
            } catch (Exception ex) {}
            try {
                admin2.topics().delete(pendingAck.toString());
            } catch (Exception ex) {}
        });
    }

    @Test
    public void testOngoingMessagesWillNotBeReplicated() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp");
        final String subscription = "s1";
        admin1.topics().createNonPartitionedTopic(topic);
        waitReplicatorStarted(topic);
        admin1.topics().createSubscription(topic, subscription, MessageId.earliest);
        admin2.topics().createSubscription(topic, subscription, MessageId.earliest);
        // Pub without commit.
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topic).create();
        Transaction txn = client1.newTransaction().withTransactionTimeout(1, TimeUnit.HOURS).build().get();
        producer1.newMessage(txn).value("msg1").send();
        // Verify: receive nothing on the remote cluster.
        Consumer consumer2 = client2.newConsumer(Schema.STRING).topic(topic).subscriptionName(subscription).subscribe();
        Message<String> msg = consumer2.receive(15, TimeUnit.SECONDS);
        assertNull(msg);
        // Verify: the repl cursor is not end of the topic.
        PersistentTopic persistentTopic = (PersistentTopic) broker1.getTopic(topic, false).join().get();
        GeoPersistentReplicator replicator =
                (GeoPersistentReplicator) persistentTopic.getReplicators().values().iterator().next();
        assertTrue(replicator.getCursor().hasMoreEntries());

        // cleanup.
        producer1.close();
        consumer2.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topic);
            admin2.topics().delete(topic);
            TopicName pendingAck = TopicName.get(
                    MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subscription));
            try {
                admin1.topics().delete(pendingAck.toString());
            } catch (Exception ex) {}
            try {
                admin2.topics().delete(pendingAck.toString());
            } catch (Exception ex) {}
        });
    }
}
