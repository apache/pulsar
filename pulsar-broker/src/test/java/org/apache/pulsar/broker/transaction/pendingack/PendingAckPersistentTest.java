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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
public class PendingAckPersistentTest extends TransactionTestBase {

    private static final String PENDING_ACK_REPLAY_TOPIC = "persistent://public/txn/pending-ack-replay";

    @BeforeMethod
    public void setup() throws Exception {
        setBrokerCount(1);
        super.internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterDataImpl.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace("public/txn", 10);
        admin.topics().createNonPartitionedTopic(PENDING_ACK_REPLAY_TOPIC);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void individualPendingAckReplayTest() throws Exception {
        int messageCount = 1000;
        String subName = "individual-test";

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
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        Transaction abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        List<MessageId> pendingAckMessageIds = new ArrayList<>();
        List<MessageId> normalAckMessageIds = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            producer.send("Hello Pulsar!".getBytes());
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
        // commit this txn , normalAckMessageIds are in pending ack state
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
        CompletableFuture<PendingAckStore> pendingAckStoreCompletableFuture =
                (CompletableFuture<PendingAckStore>) field.get(pendingAckHandle);
        pendingAckStoreCompletableFuture.get();

        field = MLPendingAckStore.class.getDeclaredField("cursor");
        field.setAccessible(true);

        ManagedCursor managedCursor = (ManagedCursor) field.get(pendingAckStoreCompletableFuture.get());

        // in order to check out the pending ack cursor is clear whether or not.
        Awaitility.await()
                .until(() -> ((PositionImpl) managedCursor.getMarkDeletedPosition())
                        .compareTo((PositionImpl) managedCursor.getManagedLedger().getLastConfirmedEntry()) == -1);
    }

    @Test
    public void cumulativePendingAckReplayTest() throws Exception {
        int messageCount = 1000;
        String subName = "cumulative-test";

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
                .enableBatchIndexAcknowledgment(true)
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
        CompletableFuture<PendingAckStore> pendingAckStoreCompletableFuture =
                (CompletableFuture<PendingAckStore>) field.get(pendingAckHandle);
        pendingAckStoreCompletableFuture.get();

        field = MLPendingAckStore.class.getDeclaredField("cursor");
        field.setAccessible(true);

        ManagedCursor managedCursor = (ManagedCursor) field.get(pendingAckStoreCompletableFuture.get());

        // in order to check out the pending ack cursor is clear whether or not.
        Awaitility.await()
                .until(() -> ((PositionImpl) managedCursor.getMarkDeletedPosition())
                        .compareTo((PositionImpl) managedCursor.getManagedLedger().getLastConfirmedEntry()) == -1);
    }
}
