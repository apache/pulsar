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
import static org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl.TRANSACTION_LOG_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Sets;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStoreProvider;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
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
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
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
public class TransactionTest extends TransactionTestBase {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 1;

    @BeforeMethod
    protected void setup() throws Exception {
       setUpBase(NUM_BROKERS, NUM_PARTITIONS, NAMESPACE1 + "/test", 0);
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

    @Test
    public void testSubscriptionRecreateTopic()
            throws PulsarAdminException, NoSuchFieldException, IllegalAccessException, PulsarClientException {
        String topic = "persistent://pulsar/system/testReCreateTopic";
        String subName = "sub_testReCreateTopic";
        int retentionSizeInMbSetTo = 5;
        int retentionSizeInMbSetTopic = 6;
        int retentionSizeInMinutesSetTo = 5;
        int retentionSizeInMinutesSetTopic = 6;
        admin.topics().createNonPartitionedTopic(topic);
        PulsarService pulsarService = super.getPulsarServiceList().get(0);
        pulsarService.getBrokerService().getTopics().clear();
        ManagedLedgerFactory managedLedgerFactory = pulsarService.getBrokerService().getManagedLedgerFactory();
        Field field = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        field.setAccessible(true);
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers =
                (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) field.get(managedLedgerFactory);
        ledgers.remove(TopicName.get(topic).getPersistenceNamingEncoding());
        try {
            admin.topics().createNonPartitionedTopic(topic);
            Assert.fail();
        } catch (PulsarAdminException.ConflictException e) {
            log.info("Cann`t create topic again");
        }
        admin.topics().setRetention(topic,
                new RetentionPolicies(retentionSizeInMinutesSetTopic, retentionSizeInMbSetTopic));
        pulsarClient.newConsumer().topic(topic)
                .subscriptionName(subName)
                .subscribe();
        pulsarService.getBrokerService().getTopicIfExists(topic).thenAccept(option -> {
            if (!option.isPresent()) {
                log.error("Failed o get Topic named: {}", topic);
                Assert.fail();
            }
            PersistentTopic originPersistentTopic = (PersistentTopic) option.get();
            String pendingAckTopicName = MLPendingAckStore
                    .getTransactionPendingAckStoreSuffix(originPersistentTopic.getName(), subName);

            try {
                admin.topics().setRetention(pendingAckTopicName,
                        new RetentionPolicies(retentionSizeInMinutesSetTo, retentionSizeInMbSetTo));
            } catch (PulsarAdminException e) {
                log.error("Failed to get./setRetention of topic with Exception:" + e);
                Assert.fail();
            }
            PersistentSubscription subscription = originPersistentTopic
                    .getSubscription(subName);
            subscription.getPendingAckManageLedger().thenAccept(managedLedger -> {
                long retentionSize = managedLedger.getConfig().getRetentionSizeInMB();
                if (!originPersistentTopic.getTopicPolicies().isPresent()) {
                    log.error("Failed to getTopicPolicies of :" + originPersistentTopic);
                    Assert.fail();
                }
                TopicPolicies topicPolicies = originPersistentTopic.getTopicPolicies().get();
                Assert.assertEquals(retentionSizeInMbSetTopic, retentionSize);
                MLPendingAckStoreProvider mlPendingAckStoreProvider = new MLPendingAckStoreProvider();
                CompletableFuture<PendingAckStore> future = mlPendingAckStoreProvider.newPendingAckStore(subscription);
                future.thenAccept(pendingAckStore -> {
                            ((MLPendingAckStore) pendingAckStore).getManagedLedger().thenAccept(managedLedger1 -> {
                                Assert.assertEquals(managedLedger1.getConfig().getRetentionSizeInMB(),
                                        retentionSizeInMbSetTo);
                            });
                        }
                );
            });


        });


    }

    @Test
    public void testTakeSnapshotBeforeBuildTxnProducer() throws Exception {
        String topic = "persistent://" + NAMESPACE1 + "/testSnapShot";
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(topic, false)
                .get().get();

        ReaderBuilder<TransactionBufferSnapshot> readerBuilder = pulsarClient
                .newReader(Schema.AVRO(TransactionBufferSnapshot.class))
                .startMessageId(MessageId.earliest)
                .topic(NAMESPACE1 + "/" + EventsTopicNames.TRANSACTION_BUFFER_SNAPSHOT);
        Reader<TransactionBufferSnapshot> reader = readerBuilder.create();

        long waitSnapShotTime = getPulsarServiceList().get(0).getConfiguration()
                .getTransactionBufferSnapshotMinTimeInMillis();
        Awaitility.await().atMost(waitSnapShotTime * 2, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assert.assertFalse(reader.hasMessageAvailable()));

        //test take snapshot by build producer by the transactionEnable client
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .producerName("testSnapshot").sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic).enableBatching(true)
                .create();

        Awaitility.await().untilAsserted(() -> {
            Message<TransactionBufferSnapshot> message1 = reader.readNext();
            TransactionBufferSnapshot snapshot1 = message1.getValue();
            Assert.assertEquals(snapshot1.getMaxReadPositionEntryId(), -1);
        });

        // test snapshot by publish  normal messages.
        producer.newMessage(Schema.STRING).value("common message send").send();
        producer.newMessage(Schema.STRING).value("common message send").send();

        Awaitility.await().untilAsserted(() -> {
            Message<TransactionBufferSnapshot> message1 = reader.readNext();
            TransactionBufferSnapshot snapshot1 = message1.getValue();
            Assert.assertEquals(snapshot1.getMaxReadPositionEntryId(), 1);
        });
    }


    @Test
    public void testAppendBufferWithNotManageLedgerExceptionCanCastToMLE()
            throws Exception {
        String topic = "persistent://pulsar/system/testReCreateTopic";
        admin.topics().createNonPartitionedTopic(topic);

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                        .getTopic(topic, false)
                        .get().get();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Topic.PublishContext publishContext = new Topic.PublishContext() {

            @Override
            public String getProducerName() {
                return "test";
            }

            public long getSequenceId() {
                return  30;
            }
            /**
             * Return the producer name for the original producer.
             *
             * For messages published locally, this will return the same local producer name, though in case of
             * replicated messages, the original producer name will differ
             */
            public String getOriginalProducerName() {
                return "test";
            }

            public long getOriginalSequenceId() {
                return  30;
            }

            public long getHighestSequenceId() {
                return  30;
            }

            public long getOriginalHighestSequenceId() {
                return  30;
            }

            public long getNumberOfMessages() {
                return  30;
            }

            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                Assert.assertTrue(e.getCause() instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException);
                countDownLatch.countDown();
            }
        };

        //Close topic manageLedger.
        persistentTopic.getManagedLedger().close();

        //Publish to a closed managerLedger to test ManagerLedgerException.
        persistentTopic.publishTxnMessage(new TxnID(123L, 321L),
                Unpooled.copiedBuffer("message", UTF_8), publishContext);

        //If it times out, it means that the assertTrue in publishContext.completed is failed.
        Awaitility.await().until(() -> {
            countDownLatch.await();
            return true;
        });
    }

    @Test
    public void testMaxReadPositionForNormalPublish() throws Exception{
        String topic = "persistent://" + NAMESPACE1 + "/NormalPublish";
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                  .getTopic(topic, false).get().get();

        TopicTransactionBuffer topicTransactionBuffer = (TopicTransactionBuffer) persistentTopic.getTransactionBuffer();
        PulsarClient noTxnClient = PulsarClient.builder().enableTransaction(false)
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl()).build();

        //test the state of TransactionBuffer is NoSnapshot
        //before build Producer by pulsarClient that enables transaction.
        Producer<String> normalProducer = noTxnClient.newProducer(Schema.STRING)
                .producerName("testNormalPublish")
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(topicTransactionBuffer.checkIfNoSnapshot()));

        //test publishing normal messages will change maxReadPosition in the state of NoSnapshot.
        MessageIdImpl messageId = (MessageIdImpl) normalProducer.newMessage().value("normal message").send();
        PositionImpl position = topicTransactionBuffer.getMaxReadPosition();
        Assert.assertEquals(position.getLedgerId(), messageId.getLedgerId());
        Assert.assertEquals(position.getEntryId(), messageId.getEntryId());

        //test the state of TransactionBuffer is Ready after build Producer by pulsarClient that enables transaction.
        Producer<String> txnProducer = pulsarClient.newProducer(Schema.STRING)
                .producerName("testTransactionPublish")
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Awaitility.await().untilAsserted(() ->Assert.assertTrue(topicTransactionBuffer.checkIfReady()));
        //test publishing txn messages will not change maxReadPosition if don`t commit or abort.
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS).build().get();
        MessageIdImpl messageId1 = (MessageIdImpl) txnProducer.newMessage(transaction).value("txn message").send();
        PositionImpl position1 = topicTransactionBuffer.getMaxReadPosition();
        Assert.assertEquals(position1.getLedgerId(), messageId.getLedgerId());
        Assert.assertEquals(position1.getEntryId(), messageId.getEntryId());

        MessageIdImpl messageId2 = (MessageIdImpl) normalProducer.newMessage().value("normal message").send();
        PositionImpl position2 = topicTransactionBuffer.getMaxReadPosition();
        Assert.assertEquals(position2.getLedgerId(), messageId.getLedgerId());
        Assert.assertEquals(position2.getEntryId(), messageId.getEntryId());
        transaction.commit().get();
        PositionImpl position3 = topicTransactionBuffer.getMaxReadPosition();

        Assert.assertEquals(position3.getLedgerId(), messageId2.getLedgerId());
        Assert.assertEquals(position3.getEntryId(), messageId2.getEntryId() + 1);

        //test publishing normal messages will change maxReadPosition if the state of TB
        //is Ready and ongoingTxns is empty.
        MessageIdImpl messageId4 = (MessageIdImpl) normalProducer.newMessage().value("normal message").send();
        PositionImpl position4 = topicTransactionBuffer.getMaxReadPosition();
        Assert.assertEquals(position4.getLedgerId(), messageId4.getLedgerId());
        Assert.assertEquals(position4.getEntryId(), messageId4.getEntryId());

        //test publishing normal messages will not change maxReadPosition if the state o TB is Initializing.
        Class<TopicTransactionBufferState> transactionBufferStateClass =
                (Class<TopicTransactionBufferState>) topicTransactionBuffer.getClass().getSuperclass();
        Field field = transactionBufferStateClass.getDeclaredField("state");
        field.setAccessible(true);
        Class<TopicTransactionBuffer> topicTransactionBufferClass = TopicTransactionBuffer.class;
        Field maxReadPositionField = topicTransactionBufferClass.getDeclaredField("maxReadPosition");
        maxReadPositionField.setAccessible(true);
        field.set(topicTransactionBuffer, TopicTransactionBufferState.State.Initializing);
        MessageIdImpl messageId5 = (MessageIdImpl) normalProducer.newMessage().value("normal message").send();
        PositionImpl position5 = (PositionImpl) maxReadPositionField.get(topicTransactionBuffer);
        Assert.assertEquals(position5.getLedgerId(), messageId4.getLedgerId());
        Assert.assertEquals(position5.getEntryId(), messageId4.getEntryId());

        }

    @Test
    public void testEndTBRecoveringWhenManagerLedgerDisReadable() throws Exception{
        String topic = NAMESPACE1 + "/testEndTBRecoveringWhenManagerLedgerDisReadable";
        admin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .producerName("test")
                .enableBatching(false)
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic)
                .create();
        producer.newMessage().send();
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(topic, false).get().get();
        persistentTopic.getManagedLedger().getConfig().setAutoSkipNonRecoverableData(true);

        ManagedCursor managedCursor = mock(ManagedCursor.class);
        doReturn("transaction-buffer-sub").when(managedCursor).getName();
        doReturn(true).when(managedCursor).hasMoreEntries();
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.NonRecoverableLedgerException("No ledger exist"),
                    null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());
        Class<ManagedLedgerImpl> managedLedgerClass = ManagedLedgerImpl.class;
        Field field = managedLedgerClass.getDeclaredField("cursors");
        field.setAccessible(true);
        ManagedCursorContainer managedCursors = (ManagedCursorContainer) field.get(persistentTopic.getManagedLedger());
        managedCursors.removeCursor("transaction-buffer-sub");
        managedCursors.add(managedCursor);

        TransactionBuffer buffer1 = new TopicTransactionBuffer(persistentTopic);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(buffer1.getStats().state, "Ready"));

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.ManagedLedgerFencedException(), null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        TransactionBuffer buffer2 = new TopicTransactionBuffer(persistentTopic);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(buffer2.getStats().state, "Ready"));
    }
}