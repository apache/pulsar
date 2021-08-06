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

import com.google.common.collect.Sets;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Slf4j
public class TopicTransactionBufferRecoverTest extends TransactionTestBase {

    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";
    private static final String RECOVER_COMMIT = NAMESPACE1 + "/recover-commit";
    private static final String RECOVER_ABORT = NAMESPACE1 + "/recover-abort";
    private static final String SUBSCRIPTION_NAME = "test-recover";
    private static final String TAKE_SNAPSHOT = NAMESPACE1 + "/take-snapshot";
    private static final String ABORT_DELETE = NAMESPACE1 + "/abort-delete";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createNonPartitionedTopic(RECOVER_COMMIT);
        admin.topics().createNonPartitionedTopic(RECOVER_ABORT);
        admin.topics().createNonPartitionedTopic(TAKE_SNAPSHOT);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        if (pulsarClient != null) {
            pulsarClient.shutdown();
            pulsarClient = null;
        }
        super.internalCleanup();
    }

    @DataProvider(name = "testTopic")
    public Object[] testTopic() {
        return new Object[] {
                RECOVER_ABORT,
                RECOVER_COMMIT
        };
    }

    @Test(dataProvider = "testTopic")
    private void recoverTest(String testTopic) throws Exception {
        PulsarClient pulsarClient = this.pulsarClient;
        Transaction tnx1 = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build().get();

        Transaction tnx2 = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build().get();

        @Cleanup
        Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .topic(testTopic)
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(testTopic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCnt = 10;
        String content = "Hello Txn - ";
        for (int i = 0; i < messageCnt; i++) {
            String msg = content + i;
            if (i % 2 == 0) {
                MessageId messageId = producer.newMessage(tnx1).value(msg).send();
                log.info("Txn1 send message : {}, messageId : {}", msg, messageId);
            } else {
                MessageId messageId = producer.newMessage(tnx2).value(msg).send();
                log.info("Txn2 send message : {}, messageId : {}", msg, messageId);
            }
        }
        Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);

        tnx1.commit();

        // only can receive message 1
        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(message);
        log.info("Txn1 commit receive message : {}, messageId : {}", message.getValue(), message.getMessageId());
        consumer.acknowledge(message);

        // can't receive message
        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);
        admin.topics().unload(RECOVER_COMMIT);

        Awaitility.await().until(() -> {
            for (int i = 0; i < getPulsarServiceList().size(); i++) {
                Field field = BrokerService.class.getDeclaredField("topics");
                field.setAccessible(true);
                ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                        (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                                .get(getPulsarServiceList().get(i).getBrokerService());
                CompletableFuture<Optional<Topic>> completableFuture = topics.get("persistent://" + testTopic);
                if (completableFuture != null) {
                    Optional<Topic> topic = completableFuture.get();
                    if (topic.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) topic.get();
                        field = PersistentTopic.class.getDeclaredField("transactionBuffer");
                        field.setAccessible(true);
                        TopicTransactionBuffer topicTransactionBuffer =
                                (TopicTransactionBuffer) field.get(persistentTopic);
                        if (topicTransactionBuffer.checkIfReady()) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
            }
            return false;
        });

        if (testTopic.equals(RECOVER_COMMIT)) {
            tnx2.commit().get();

            for (int i = messageCnt; i > 1; i --) {
                message = consumer.receive();
                log.info("Txn2 commit receive message : {}, messageId : {}",
                        message.getValue(), message.getMessageId());
                consumer.acknowledge(message);
            }

            // can't receive message
            message = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(message);
        } else {
            tnx2.abort().get();

            for (int i = messageCnt / 2; i > 1; i --) {
                message = consumer.receive();
                log.info("Txn2 commit receive message : {}, messageId : {}",
                        message.getValue(), message.getMessageId());
                consumer.acknowledge(message);
            }

            // can't receive message
            message = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(message);
        }

        consumer.close();
        producer.close();

    }

    @Test
    private void testTakeSnapshot() throws IOException, ExecutionException, InterruptedException {

        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(TAKE_SNAPSHOT)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Transaction tnx1 = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build().get();
        Transaction tnx2 = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build().get();
        Transaction tnx3 = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build().get();
        Transaction abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build().get();

        ReaderBuilder<TransactionBufferSnapshot> readerBuilder = pulsarClient
                .newReader(Schema.AVRO(TransactionBufferSnapshot.class))
                .startMessageId(MessageId.earliest)
                .topic(NAMESPACE1 + "/" + EventsTopicNames.TRANSACTION_BUFFER_SNAPSHOT);
        Reader<TransactionBufferSnapshot> reader = readerBuilder.create();

        MessageId messageId1 = producer.newMessage(tnx1).value("test").send();
        tnx1.commit().get();
        // wait timeout take snapshot

        TransactionBufferSnapshot transactionBufferSnapshot = reader.readNext().getValue();
        assertEquals(transactionBufferSnapshot.getMaxReadPositionEntryId(), ((MessageIdImpl) messageId1).getEntryId() + 1);
        assertEquals(transactionBufferSnapshot.getMaxReadPositionLedgerId(), ((MessageIdImpl) messageId1).getLedgerId());
        assertFalse(reader.hasMessageAvailable());

        // take snapshot by change times
        MessageId messageId2 = producer.newMessage(tnx2).value("test").send();
        tnx2.commit().get();

        MessageId messageId3 = producer.newMessage(tnx3).value("test").send();
        tnx3.commit().get();

        TransactionBufferSnapshot snapshot = reader.readNext().getValue();
        assertEquals(snapshot.getMaxReadPositionEntryId(), ((MessageIdImpl) messageId3).getEntryId() + 1);
        assertEquals(snapshot.getMaxReadPositionLedgerId(), ((MessageIdImpl) messageId3).getLedgerId());
        assertEquals(snapshot.getAborts().size(), 0);
        assertFalse(reader.hasMessageAvailable());

        MessageId messageId4 = producer.newMessage(abortTxn).value("test").send();
        abortTxn.abort().get();

        transactionBufferSnapshot = reader.readNext().getValue();
        assertEquals(transactionBufferSnapshot.getMaxReadPositionEntryId(), ((MessageIdImpl) messageId4).getEntryId() + 1);
        assertEquals(transactionBufferSnapshot.getMaxReadPositionLedgerId(), ((MessageIdImpl) messageId4).getLedgerId());
        assertEquals(transactionBufferSnapshot.getAborts().size(), 1);
        assertEquals(transactionBufferSnapshot.getAborts().get(0).getTxnIdLeastBits(),
                ((TransactionImpl) abortTxn).getTxnIdLeastBits());
        assertEquals(transactionBufferSnapshot.getAborts().get(0).getTxnIdMostBits(),
                ((TransactionImpl) abortTxn).getTxnIdMostBits());
        assertFalse(reader.hasMessageAvailable());
        reader.close();
        producer.close();

    }

    @Test
    private void testTopicTransactionBufferDeleteAbort() throws Exception {
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(ABORT_DELETE)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .topic(ABORT_DELETE)
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscribe();

        Transaction tnx = pulsarClient.newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS)
                .build().get();
        String value = "Hello Pulsar!";

        MessageId messageId1 = producer.newMessage(tnx).value(value).send();
        tnx.abort().get();

        admin.topics().unload(ABORT_DELETE);

        tnx = pulsarClient.newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS)
                .build().get();

        value = "Hello";
        producer.newMessage(tnx).value(value).send();
        tnx.commit().get();

        Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
        System.out.println("consumer receive message" + message.getMessageId());
        assertNotNull(message.getValue(), value);
        consumer.acknowledge(message);

        tnx = pulsarClient.newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS)
                .build().get();

        MessageId messageId2 = producer.newMessage(tnx).value(value).send();
        tnx.abort().get();

        assertTrue(((MessageIdImpl) messageId2).getLedgerId() != ((MessageIdImpl) messageId1).getLedgerId());
        boolean exist = false;
        for (int i = 0; i < getPulsarServiceList().size(); i++) {
            Field field = BrokerService.class.getDeclaredField("topics");
            field.setAccessible(true);
            ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                    (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                            .get(getPulsarServiceList().get(i).getBrokerService());
            CompletableFuture<Optional<Topic>> completableFuture = topics.get("persistent://" + ABORT_DELETE);
            if (completableFuture != null) {
                Optional<Topic> topic = completableFuture.get();
                if (topic.isPresent()) {
                    PersistentTopic persistentTopic = (PersistentTopic) topic.get();
                    field = ManagedLedgerImpl.class.getDeclaredField("ledgers");
                    field.setAccessible(true);
                    NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers
                            = (NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo>) field.get(persistentTopic.getManagedLedger());

                    ledgers.remove(((MessageIdImpl) messageId1).getLedgerId());
                    tnx = pulsarClient.newTransaction()
                            .withTransactionTimeout(2, TimeUnit.SECONDS)
                            .build().get();

                    producer.newMessage(tnx).value(value).send();
                    tnx.commit().get();
                    field = PersistentTopic.class.getDeclaredField("transactionBuffer");
                    field.setAccessible(true);
                    TopicTransactionBuffer topicTransactionBuffer =
                            (TopicTransactionBuffer) field.get(persistentTopic);
                    field = TopicTransactionBuffer.class.getDeclaredField("aborts");
                    field.setAccessible(true);
                    LinkedMap<TxnID, PositionImpl> linkedMap =
                            (LinkedMap<TxnID, PositionImpl>) field.get(topicTransactionBuffer);
                    assertEquals(linkedMap.size(), 1);
                    assertEquals(linkedMap.get(linkedMap.firstKey()).getLedgerId(),
                            ((MessageIdImpl) message.getMessageId()).getLedgerId());
                    exist = true;
                }
            }
        }
        assertTrue(exist);
    }
}
