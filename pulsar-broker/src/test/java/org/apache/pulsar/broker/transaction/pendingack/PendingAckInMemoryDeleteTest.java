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
package org.apache.pulsar.broker.transaction.pendingack;

import com.google.common.collect.Sets;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Slf4j
@Test(groups = "broker")
public class PendingAckInMemoryDeleteTest extends TransactionTestBase {

    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";

    @BeforeMethod
    protected void setup() throws Exception {
        setBrokerCount(1);
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Map<TransactionCoordinatorID, TransactionMetadataStore> stores =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService().getStores();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            if (stores.size() == 16) {
                for (TransactionCoordinatorID transactionCoordinatorID : stores.keySet()) {
                    if (((MLTransactionMetadataStore) stores.get(transactionCoordinatorID)).getState()
                            != TransactionMetadataStoreState.State.Ready) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        });
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void txnAckTestNoBatchAndSharedSubMemoryDeleteTest() throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";
        String subscriptionName = "test";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(normalTopic)
                .isAckReceiptEnabled(true)
                .subscriptionName(subscriptionName)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(2, TimeUnit.SECONDS)
                .acknowledgmentGroupTime(0, TimeUnit.MICROSECONDS)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(false)
                .create();

        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {

            int messageCnt = 1000;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }
            Message<byte[]> message;

            Transaction commitTxn = getTxn();
            //send 1000 and ack 999, and test the consumer pending ack has already clear 999 messages
            for (int i = 0; i < messageCnt - 1; i++) {
                message = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                if (i % 2 == 0) {
                    consumer.acknowledgeAsync(message.getMessageId(), commitTxn).get();
                    log.info("txn receive msgId: {}, count: {}", message.getMessageId(), i);
                } else {
                    consumer.acknowledge(message.getMessageId());
                    log.info("normal receive msgId: {}, count: {}", message.getMessageId(), i);
                }
            }

            commitTxn.commit().get();

            int count = 0;
            for (int i = 0; i < getPulsarServiceList().size(); i++) {
                Field field = BrokerService.class.getDeclaredField("topics");
                field.setAccessible(true);
                ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                        (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                                .get(getPulsarServiceList().get(i).getBrokerService());
                CompletableFuture<Optional<Topic>> completableFuture = topics.get("persistent://" + normalTopic);
                if (completableFuture != null) {
                    Optional<Topic> topic = completableFuture.get();
                    if (topic.isPresent()) {
                        PersistentSubscription persistentSubscription = (PersistentSubscription) topic.get()
                                .getSubscription(subscriptionName);
                        field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
                        field.setAccessible(true);
                        PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) field.get(persistentSubscription);
                        field = PendingAckHandleImpl.class.getDeclaredField("individualAckOfTransaction");
                        field.setAccessible(true);
                        LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>> individualAckOfTransaction =
                                (LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>>) field.get(pendingAckHandle);
                        assertTrue(individualAckOfTransaction.isEmpty());
                        if (retryCnt == 0) {
                            //one message are not ack
                            assertEquals(persistentSubscription.getConsumers().get(0).getPendingAcks().size(), 1);
                        } else {
                            //two message are not ack
                            assertEquals(persistentSubscription.getConsumers().get(0).getPendingAcks().size(), 2);
                        }
                        count++;
                    }
                }
            }
            //make sure the consumer is ownership for a broker server
            assertEquals(count, 1);
        }
    }

    @Test
    public void txnAckTestBatchAndSharedSubMemoryDeleteTest() throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";
        String subscriptionName = "test";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(normalTopic)
                .subscriptionName(subscriptionName)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(true)
                .ackTimeout(2, TimeUnit.SECONDS)
                .acknowledgmentGroupTime(0, TimeUnit.MICROSECONDS)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(true)
                .batchingMaxMessages(200)
                .create();

        PersistentSubscription persistentSubscription = null;
        PendingAckHandleImpl pendingAckHandle = null;

        LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>> individualAckOfTransaction = null;
        ManagedCursorImpl managedCursor = null;

        ConcurrentSkipListMap<PositionImpl, BitSetRecyclable> batchDeletedIndexes = null;

        Message<byte[]>[] messages = new Message[2];
        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {

            int messageCnt = 1000;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }
            Message<byte[]> message;

            // after transaction abort, the messages could be received
            Transaction commitTxn = getTxn();

            //send 1000 and ack 999, and test the consumer pending ack has already clear 999 messages
            for (int i = 0; i < messageCnt - 1; i++) {
                message = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                if (i % 2 == 0) {
                    consumer.acknowledgeAsync(message.getMessageId(), commitTxn).get();
                    log.info("txn receive msgId: {}, count: {}", message.getMessageId(), i);
                } else {
                    consumer.acknowledge(message.getMessageId());
                    log.info("normal receive msgId: {}, count: {}", message.getMessageId(), i);
                }
            }

            commitTxn.commit().get();
            int count = 0;
            for (int i = 0; i < getPulsarServiceList().size(); i++) {
                Field field = BrokerService.class.getDeclaredField("topics");
                field.setAccessible(true);
                ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                        (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                                .get(getPulsarServiceList().get(i).getBrokerService());
                CompletableFuture<Optional<Topic>> completableFuture = topics.get("persistent://" + normalTopic);
                if (completableFuture != null) {
                    Optional<Topic> topic = completableFuture.get();
                    if (topic.isPresent()) {
                        persistentSubscription = (PersistentSubscription) topic.get().getSubscription(subscriptionName);
                        field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
                        field.setAccessible(true);
                        pendingAckHandle = (PendingAckHandleImpl) field.get(persistentSubscription);
                        field = PendingAckHandleImpl.class.getDeclaredField("individualAckOfTransaction");
                        field.setAccessible(true);
                        individualAckOfTransaction =
                                (LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>>) field.get(pendingAckHandle);
                        assertTrue(individualAckOfTransaction.isEmpty());
                        managedCursor = (ManagedCursorImpl) persistentSubscription.getCursor();
                        field = ManagedCursorImpl.class.getDeclaredField("batchDeletedIndexes");
                        field.setAccessible(true);
                        batchDeletedIndexes =
                                (ConcurrentSkipListMap<PositionImpl, BitSetRecyclable>) field.get(managedCursor);
                        if (retryCnt == 0) {
                            //one message are not ack
                            assertEquals(batchDeletedIndexes.size(), 1);
                            assertEquals(persistentSubscription.getConsumers().get(0).getPendingAcks().size(), 1);
                            messages[0] = consumer.receive();
                        } else {
                            //two message are not ack
                            assertEquals(batchDeletedIndexes.size(), 2);
                            assertEquals(persistentSubscription.getConsumers().get(0).getPendingAcks().size(), 2);
                            messages[1] = consumer.receive();
                        }
                        count++;
                    }
                }
            }
            assertEquals(count, 1);
        }

        Transaction commitTwice = getTxn();

        //this message is in one batch point
        consumer.acknowledge(messages[0].getMessageId());
        assertEquals(batchDeletedIndexes.size(), 1);
        assertEquals(persistentSubscription.getConsumers().get(0).getPendingAcks().size(), 1);

        // this test is for the last message has been cleared in this consumer pending acks
        // and it won't clear the last message in cursor batch index ack set
        consumer.acknowledgeAsync(messages[1].getMessageId(), commitTwice).get();
        assertEquals(batchDeletedIndexes.size(), 1);
        assertEquals(persistentSubscription.getConsumers().get(0).getPendingAcks().size(), 0);

        // the messages has been produced were all acked, the memory in broker for the messages has been cleared.
        commitTwice.commit().get();
        assertEquals(batchDeletedIndexes.size(), 0);
        assertEquals(persistentSubscription.getConsumers().get(0).getPendingAcks().size(), 0);
    }

    private Transaction getTxn() throws Exception {
        return pulsarClient
                .newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();
    }
}
