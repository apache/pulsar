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

import static org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl.TRANSACTION_LOG_PREFIX;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import com.google.common.collect.Sets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.rocksdb.Snapshot;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionTest extends TransactionTestBase {

    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";
    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 1;

    @BeforeMethod
    protected void setup() throws Exception {
        this.setBrokerCount(NUM_BROKERS);
        this.internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length - 1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder()
                .serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), NUM_PARTITIONS);
        pulsarClient.close();
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
    }

    @Test
    public void brokerNotInitTxnManagedLedgerTopic() throws Exception {
        String subName = "test";

        String topicName = TopicName.get(NAMESPACE1 + "/test").toString();


        @Cleanup
        Consumer<byte[]> consumer = getConsumer(topicName, subName);

        consumer.close();

        Awaitility.await().until(() -> {
            try {
                pulsarClient.newTransaction()
                        .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
            } catch (Exception e) {
                return false;
            }
            return true;
        });

        admin.namespaces().unload(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.namespaces().unload(NAMESPACE1);

        @Cleanup
        Consumer<byte[]> consumer1 = getConsumer(topicName, subName);

        Awaitility.await().until(() -> {
            try {
                pulsarClient.newTransaction()
                        .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
            } catch (Exception e) {
                return false;
            }
            return true;
        });

        ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                getPulsarServiceList().get(0).getBrokerService().getTopics();

        Assert.assertNull(topics.get(TopicName.get(TopicDomain.persistent.value(),
                NamespaceName.SYSTEM_NAMESPACE, TRANSACTION_LOG_PREFIX).toString() + 0));
        Assert.assertNull(topics.get(TopicName.TRANSACTION_COORDINATOR_ASSIGN.getPartition(0).toString()));
        Assert.assertNull(topics.get(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topicName, subName)));
    }


    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public Consumer<byte[]> getConsumer(String topicName, String subName) throws PulsarClientException {
        return pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();
    }

    @Test
    public void testGetTxnID() throws Exception {
        // wait tc init success to ready state
        Assert.assertTrue(waitForCoordinatorToBeAvailable(NUM_BROKERS, NUM_PARTITIONS));
        Transaction transaction = pulsarClient.newTransaction()
                .build().get();
        TxnID txnID = transaction.getTxnID();
        Assert.assertEquals(txnID.getLeastSigBits(), 0);
        Assert.assertEquals(txnID.getMostSigBits(), 0);
        transaction.abort();
        transaction = pulsarClient.newTransaction()
                .build().get();
        txnID = transaction.getTxnID();
        Assert.assertEquals(txnID.getLeastSigBits(), 1);
        Assert.assertEquals(txnID.getMostSigBits(), 0);
    }

    public void testChangeSnapshotAfterCommonMessageSend() throws Exception{
        String topic = "persistent://" + NAMESPACE1 + "/testSnapShot";
        admin.topics().createNonPartitionedTopic(topic);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .producerName("testSnapshot").sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic).enableBatching(true)
                .create();
        ReaderBuilder<TransactionBufferSnapshot> readerBuilder = pulsarClient
                .newReader(Schema.AVRO(TransactionBufferSnapshot.class))
                .startMessageId(MessageId.latest)
                .topic(NAMESPACE1 + "/" + EventsTopicNames.TRANSACTION_BUFFER_SNAPSHOT);
        Reader<TransactionBufferSnapshot> reader= readerBuilder.create();
        long waitSnapShotTime = getPulsarServiceList().get(0).getConfiguration()
                .getTransactionBufferSnapshotMinTimeInMillis();

        producer.newMessage(Schema.STRING).value("common message send").send();

        Awaitility.await().atMost(waitSnapShotTime * 2, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try{
                        Message<TransactionBufferSnapshot> message = reader.readNext();
                        TransactionBufferSnapshot snapshot = message.getValue();
                        Assert.assertEquals(snapshot.getMaxReadPositionEntryId(),0);
                        return true;
                    }catch (java.lang.AssertionError e){
                        return false;
                    }
                });

        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        for (int i = 0; i < 2; i++) {
            producer.newMessage(transaction).value("transaction message : " + i).send();
        }

        try {
            Awaitility.await().atMost(waitSnapShotTime * 2, TimeUnit.MILLISECONDS)
                    .until(() -> {
                        try{
                            Message<TransactionBufferSnapshot> message = reader.readNext();
                            TransactionBufferSnapshot snapshot = message.getValue();
                            Assert.assertEquals(snapshot.getMaxReadPositionEntryId(),2);
                            return true;
                        }catch (java.lang.AssertionError e){
                            return false;
                        }
                    });
            Assert.fail("Snapshot should not change before transaction end");
        }catch (ConditionTimeoutException e){
            log.info("Snapshot will not change to uncommitted messages");
        }
        transaction.commit();

        Awaitility.await().atMost(waitSnapShotTime * 2, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try{
                        Message<TransactionBufferSnapshot> message = reader.readNext();
                        TransactionBufferSnapshot snapshot = message.getValue();
                        Assert.assertEquals(snapshot.getMaxReadPositionEntryId(),3);
                        return true;
                    }catch (java.lang.AssertionError e){
                        return false;
                    }
                });
    }
}