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
import static org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore.PENDING_ACK_STORE_SUFFIX;
import static org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl.TRANSACTION_LOG_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.buffer.Unpooled;
import io.netty.util.Timeout;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.Bytes;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotService;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferProvider;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferRecoverCallBack;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStoreProvider;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
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
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionSequenceIdGenerator;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;
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
    public void testCreateTransactionSystemTopic() throws Exception {
        String subName = "test";
        String topicName = TopicName.get(NAMESPACE1 + "/" + "testCreateTransactionSystemTopic").toString();

        try {
            // init pending ack
            @Cleanup
            Consumer<byte[]> consumer = getConsumer(topicName, subName);
            Transaction transaction = pulsarClient.newTransaction()
                    .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();

            consumer.acknowledgeAsync(new MessageIdImpl(10, 10, 10), transaction).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
        }
        topicName = MLPendingAckStore.getTransactionPendingAckStoreSuffix(topicName, subName);

        // getList does not include transaction system topic
        List<String> list = admin.topics().getList(NAMESPACE1);
        assertEquals(list.size(), 4);
        list.forEach(topic -> assertFalse(topic.contains(PENDING_ACK_STORE_SUFFIX)));

        try {
            // can't create transaction system topic
            @Cleanup
            Consumer<byte[]> consumer = getConsumer(topicName, subName);
            fail();
        } catch (PulsarClientException.NotAllowedException e) {
            assertTrue(e.getMessage().contains("Can not create transaction system topic"));
        }

        // can't create transaction system topic
        try {
            admin.topics().getSubscriptions(topicName);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(e.getMessage(), "Can not create transaction system topic " + topicName);
        }

        // can't create transaction system topic
        try {
            admin.topics().createPartitionedTopic(topicName, 3);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(e.getMessage(), "Cannot create topic in system topic format!");
        }

        // can't create transaction system topic
        try {
            admin.topics().createNonPartitionedTopic(topicName);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(e.getMessage(), "Cannot create topic in system topic format!");
        }
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
    public void testMaxReadPositionForNormalPublish() throws Exception {
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

        Awaitility.await().untilAsserted(() -> Assert.assertTrue(topicTransactionBuffer.checkIfReady()));
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
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();

        producer.newMessage(txn).value("test").send();

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic("persistent://" + topic, false).get().get();
        persistentTopic.getManagedLedger().getConfig().setAutoSkipNonRecoverableData(true);

        ManagedCursorImpl managedCursor = mock(ManagedCursorImpl.class);
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
        managedCursors.add(managedCursor, managedCursor.getMarkDeletedPosition());

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.ManagedLedgerFencedException(), null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        TransactionBuffer buffer2 = new TopicTransactionBuffer(persistentTopic);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(buffer2.getStats().state, "Ready"));
        managedCursors.removeCursor("transaction-buffer-sub");

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("test"), null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        managedCursors.add(managedCursor, managedCursor.getMarkDeletedPosition());
        TransactionBuffer buffer3 = new TopicTransactionBuffer(persistentTopic);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(buffer3.getStats().state, "Ready"));
        persistentTopic.getInternalStats(false).thenAccept(internalStats -> {
            assertTrue(internalStats.cursors.isEmpty());
        });
        managedCursors.removeCursor("transaction-buffer-sub");
    }

    @Test
    public void testEndTPRecoveringWhenManagerLedgerDisReadable() throws Exception{
        String topic = NAMESPACE1 + "/testEndTPRecoveringWhenManagerLedgerDisReadable";
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
        PersistentSubscription persistentSubscription = (PersistentSubscription) persistentTopic
                .createSubscription("test",
                CommandSubscribe.InitialPosition.Earliest, false, null).get();

        ManagedCursorImpl managedCursor = mock(ManagedCursorImpl.class);
        doReturn(true).when(managedCursor).hasMoreEntries();
        doReturn(false).when(managedCursor).isClosed();
        doReturn(new PositionImpl(-1, -1)).when(managedCursor).getMarkDeletedPosition();
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.NonRecoverableLedgerException("No ledger exist"),
                    null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        TransactionPendingAckStoreProvider pendingAckStoreProvider = mock(TransactionPendingAckStoreProvider.class);
        doReturn(CompletableFuture.completedFuture(
                new MLPendingAckStore(persistentTopic.getManagedLedger(), managedCursor, null, 500)))
                .when(pendingAckStoreProvider).newPendingAckStore(any());
        doReturn(CompletableFuture.completedFuture(true)).when(pendingAckStoreProvider).checkInitializedBefore(any());

        Class<PulsarService> pulsarServiceClass = PulsarService.class;
        Field field = pulsarServiceClass.getDeclaredField("transactionPendingAckStoreProvider");
        field.setAccessible(true);
        field.set(getPulsarServiceList().get(0), pendingAckStoreProvider);

        PendingAckHandleImpl pendingAckHandle1 = new PendingAckHandleImpl(persistentSubscription);
        Awaitility.await().untilAsserted(() ->
                assertEquals(pendingAckHandle1.getStats().state, "Ready"));

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.ManagedLedgerFencedException(), null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        PendingAckHandleImpl pendingAckHandle2 = new PendingAckHandleImpl(persistentSubscription);
        Awaitility.await().untilAsserted(() ->
                assertEquals(pendingAckHandle2.getStats().state, "Ready"));

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("test"), null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        PendingAckHandleImpl pendingAckHandle3 = new PendingAckHandleImpl(persistentSubscription);

        Awaitility.await().untilAsserted(() ->
                assertEquals(pendingAckHandle3.getStats().state, "Ready"));
    }

    @Test
    public void testEndTCRecoveringWhenManagerLedgerDisReadable() throws Exception{
        String topic = NAMESPACE1 + "/testEndTBRecoveringWhenManagerLedgerDisReadable";
        admin.topics().createNonPartitionedTopic(topic);

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(topic, false).get().get();
        persistentTopic.getManagedLedger().getConfig().setAutoSkipNonRecoverableData(true);
        Map<String, String> map = new HashMap<>();
        map.put(MLTransactionSequenceIdGenerator.MAX_LOCAL_TXN_ID, "1");
        persistentTopic.getManagedLedger().setProperties(map);

        ManagedCursor managedCursor = mock(ManagedCursor.class);
        doReturn(true).when(managedCursor).hasMoreEntries();
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.NonRecoverableLedgerException("No ledger exist"),
                    null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        persistentTopic.getManagedLedger().getConfig().setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        MLTransactionLogImpl mlTransactionLog =
                new MLTransactionLogImpl(new TransactionCoordinatorID(1), null,
                        persistentTopic.getManagedLedger().getConfig());
        Class<MLTransactionLogImpl> mlTransactionLogClass = MLTransactionLogImpl.class;
        Field field = mlTransactionLogClass.getDeclaredField("cursor");
        field.setAccessible(true);
        field.set(mlTransactionLog, managedCursor);
        field = mlTransactionLogClass.getDeclaredField("managedLedger");
        field.setAccessible(true);
        field.set(mlTransactionLog, persistentTopic.getManagedLedger());

        TransactionRecoverTracker transactionRecoverTracker = mock(TransactionRecoverTracker.class);
        doNothing().when(transactionRecoverTracker).appendOpenTransactionToTimeoutTracker();
        doNothing().when(transactionRecoverTracker).handleCommittingAndAbortingTransaction();
        TransactionTimeoutTracker timeoutTracker = mock(TransactionTimeoutTracker.class);
        doNothing().when(timeoutTracker).start();
        MLTransactionMetadataStore metadataStore1 =
                new MLTransactionMetadataStore(new TransactionCoordinatorID(1),
                        mlTransactionLog, timeoutTracker, mlTransactionSequenceIdGenerator);
        metadataStore1.init(transactionRecoverTracker).get();
        Awaitility.await().untilAsserted(() ->
                assertEquals(metadataStore1.getCoordinatorStats().state, "Ready"));

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.ManagedLedgerFencedException(), null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        MLTransactionMetadataStore metadataStore2 =
                new MLTransactionMetadataStore(new TransactionCoordinatorID(1),
                        mlTransactionLog, timeoutTracker, mlTransactionSequenceIdGenerator);
        metadataStore2.init(transactionRecoverTracker).get();
        Awaitility.await().untilAsserted(() ->
                assertEquals(metadataStore2.getCoordinatorStats().state, "Ready"));

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("test"), null);
            return null;
        }).when(managedCursor).asyncReadEntries(anyInt(), any(), any(), any());

        MLTransactionMetadataStore metadataStore3 =
                new MLTransactionMetadataStore(new TransactionCoordinatorID(1),
                        mlTransactionLog, timeoutTracker, mlTransactionSequenceIdGenerator);
        metadataStore3.init(transactionRecoverTracker).get();
        Awaitility.await().untilAsserted(() ->
                assertEquals(metadataStore3.getCoordinatorStats().state, "Ready"));
    }

    @Test
    public void testEndTxnWhenCommittingOrAborting() throws Exception {
        Transaction commitTxn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        Transaction abortTxn = pulsarClient
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();

        Class<TransactionImpl> transactionClass = TransactionImpl.class;
        Field field = transactionClass.getDeclaredField("state");
        field.setAccessible(true);

        field.set(commitTxn, TransactionImpl.State.COMMITTING);
        field.set(abortTxn, TransactionImpl.State.ABORTING);

        abortTxn.abort();
        commitTxn.commit();
    }

    @Test
    public void testNoEntryCanBeReadWhenRecovery() throws Exception {
        String topic = NAMESPACE1 + "/test";
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                        .getTopic(TopicName.get(topic).toString(), true)
                        .get()
                        .get();

        Class<PersistentTopic> persistentTopicClass = PersistentTopic.class;
        Field filed1 = persistentTopicClass.getDeclaredField("ledger");
        Field field2 = persistentTopicClass.getDeclaredField("transactionBuffer");
        filed1.setAccessible(true);
        field2.setAccessible(true);
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) spy(filed1.get(persistentTopic));
        filed1.set(persistentTopic, managedLedger);

        TopicTransactionBuffer topicTransactionBuffer = (TopicTransactionBuffer) field2.get(persistentTopic);
        Method method = TopicTransactionBuffer.class.getDeclaredMethod("takeSnapshot");
        method.setAccessible(true);
        CompletableFuture<Void> completableFuture = (CompletableFuture<Void>) method.invoke(topicTransactionBuffer);
        completableFuture.get();

        doReturn(PositionImpl.LATEST).when(managedLedger).getLastConfirmedEntry();
        ManagedCursorImpl managedCursor = mock(ManagedCursorImpl.class);
        doReturn(false).when(managedCursor).hasMoreEntries();
        doReturn(managedCursor).when(managedLedger).newNonDurableCursor(any(), any());

        TopicTransactionBuffer transactionBuffer = new TopicTransactionBuffer(persistentTopic);
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(transactionBuffer.checkIfReady()));
    }

    @Test
    public void testRetryExceptionOfEndTxn() throws Exception{
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();
        Class<TransactionMetadataStoreState> transactionMetadataStoreStateClass = TransactionMetadataStoreState.class;
        getPulsarServiceList().get(0).getTransactionMetadataStoreService().getStores()
                .values()
                .forEach((transactionMetadataStore -> {
                    try {
                        Field field = transactionMetadataStoreStateClass.getDeclaredField("state");
                        field.setAccessible(true);
                        field.set(transactionMetadataStore, TransactionMetadataStoreState.State.Initializing);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }));
        CompletableFuture<Void> completableFuture =  transaction.commit();
        try {
            completableFuture.get(5, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }
        getPulsarServiceList().get(0).getTransactionMetadataStoreService().getStores()
                .values()
                .stream()
                .forEach((transactionMetadataStore -> {
                    try {
                        Field field = transactionMetadataStoreStateClass.getDeclaredField("state");
                        field.setAccessible(true);
                        field.set(transactionMetadataStore, TransactionMetadataStoreState.State.Ready);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }));
        completableFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCancelTxnTimeout() throws Exception{
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();

        transaction.commit().get();

        Field field = TransactionImpl.class.getDeclaredField("timeout");
        field.setAccessible(true);
        Timeout timeout = (Timeout) field.get(transaction);
        Assert.assertTrue(timeout.isCancelled());

        transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();

        transaction.abort().get();
        timeout = (Timeout) field.get(transaction);
        Assert.assertTrue(timeout.isCancelled());
    }

    @Test
    public void testNotChangeMaxReadPositionAndAddAbortTimesWhenCheckIfNoSnapshot() throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService()
                .getTopic(NAMESPACE1 + "/changeMaxReadPositionAndAddAbortTimes" + UUID.randomUUID(), true)
                .get().get();
        TransactionBuffer buffer = persistentTopic.getTransactionBuffer();
        Field field = TopicTransactionBuffer.class.getDeclaredField("changeMaxReadPositionAndAddAbortTimes");
        field.setAccessible(true);
        AtomicLong changeMaxReadPositionAndAddAbortTimes = (AtomicLong) field.get(buffer);
        Field field1 = TopicTransactionBufferState.class.getDeclaredField("state");
        field1.setAccessible(true);

        Awaitility.await().untilAsserted(() -> {
                    TopicTransactionBufferState.State state = (TopicTransactionBufferState.State) field1.get(buffer);
                    Assert.assertEquals(state, TopicTransactionBufferState.State.NoSnapshot);
        });
        Assert.assertEquals(changeMaxReadPositionAndAddAbortTimes.get(), 0L);

        buffer.syncMaxReadPositionForNormalPublish(new PositionImpl(1, 1));
        Assert.assertEquals(changeMaxReadPositionAndAddAbortTimes.get(), 0L);

    }

    @Test
    public void testAutoCreateSchemaForTransactionSnapshot() throws Exception {
        String namespace = TENANT + "/ns2";
        String topic = namespace + "/test";
        pulsarServiceList.forEach((pulsarService ->
                pulsarService.getConfiguration().setAllowAutoUpdateSchemaEnabled(false)));
        admin.namespaces().createNamespace(namespace);
        admin.topics().createNonPartitionedTopic(topic);
        TopicName transactionBufferTopicName =
                NamespaceEventsSystemTopicFactory.getSystemTopicName(
                        TopicName.get(topic).getNamespaceObject(), EventType.TRANSACTION_BUFFER_SNAPSHOT);
        TopicName transactionBufferTopicName1 =
                NamespaceEventsSystemTopicFactory.getSystemTopicName(
                        TopicName.get(topic).getNamespaceObject(), EventType.TOPIC_POLICY);
        Awaitility.await().untilAsserted(() -> {
            SchemaInfo schemaInfo = admin
                    .schemas()
                    .getSchemaInfo(transactionBufferTopicName.toString());
            Assert.assertNotNull(schemaInfo);
            SchemaInfo schemaInfo1 = admin
                    .schemas()
                    .getSchemaInfo(transactionBufferTopicName1.toString());
            Assert.assertNotNull(schemaInfo1);
        });
        pulsarServiceList.forEach((pulsarService ->
                pulsarService.getConfiguration().setAllowAutoUpdateSchemaEnabled(true)));
    }

    @Test
    public void testPendingAckMarkDeletePosition() throws Exception {
        getPulsarServiceList().get(0).getConfig().setTransactionPendingAckLogIndexMinLag(1);
        getPulsarServiceList().get(0).getConfiguration().setManagedLedgerDefaultMarkDeleteRateLimit(5);
        String topic = NAMESPACE1 + "/test1";

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer(Schema.BYTES)
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        consumer.getSubscription();

        PersistentSubscription persistentSubscription = (PersistentSubscription) getPulsarServiceList()
                .get(0)
                .getBrokerService()
                .getTopic(topic, false)
                .get()
                .get()
                .getSubscription("sub");

        ManagedCursor subscriptionCursor = persistentSubscription.getCursor();

        subscriptionCursor.getMarkDeletedPosition();
        //pendingAck add message1 and commit mark, metadata add message1
        //PersistentMarkDeletedPosition have not updated
        producer.newMessage()
                .value("test".getBytes(UTF_8))
                .send();
        Transaction transaction = pulsarClient
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.MINUTES)
                .build().get();

        Message<byte[]> message1 = consumer.receive(10, TimeUnit.SECONDS);

        consumer.acknowledgeAsync(message1.getMessageId(), transaction);
        transaction.commit().get();
        //PersistentMarkDeletedPosition of subscription have updated to message1,
        //check whether delete markDeletedPosition of pendingAck after append entry to pendingAck
        transaction = pulsarClient
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.MINUTES)
                .build().get();

        producer.newMessage()
                .value("test".getBytes(UTF_8))
                .send();
        Message<byte[]> message2 = consumer.receive(10, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message2.getMessageId(), transaction);

        Awaitility.await().untilAsserted(() -> {
            ManagedLedgerInternalStats managedLedgerInternalStats = admin
                    .transactions()
                    .getPendingAckInternalStats(topic, "sub", false)
                    .pendingAckLogStats
                    .managedLedgerInternalStats;
            String [] markDeletePosition = managedLedgerInternalStats.cursors.get("__pending_ack_state")
                    .markDeletePosition.split(":");
            String [] lastConfirmedEntry = managedLedgerInternalStats.lastConfirmedEntry.split(":");
            Assert.assertEquals(markDeletePosition[0], lastConfirmedEntry[0]);
            //don`t contain commit mark and unCommitted message2
            Assert.assertEquals(Integer.parseInt(markDeletePosition[1]),
                    Integer.parseInt(lastConfirmedEntry[1]) - 2);
        });
    }

    @Test
    public void testConsistencyOfTransactionStatsAtEndTxn() throws Exception {
        TransactionMetadataStore transactionMetadataStore = getPulsarServiceList().get(0)
                .getTransactionMetadataStoreService()
                .getStores()
                .get(new TransactionCoordinatorID(0));

        Field field = MLTransactionMetadataStore.class.getDeclaredField("transactionLog");
        field.setAccessible(true);
        MLTransactionLogImpl transactionLog = (MLTransactionLogImpl) field.get(transactionMetadataStore);
        Field field1 = MLTransactionLogImpl.class.getDeclaredField("cursor");
        field1.setAccessible(true);
        ManagedCursorImpl managedCursor = (ManagedCursorImpl) field1.get(transactionLog);
        managedCursor.close();

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();

        transaction.commit().get();
    }

    @Test
    public void testGetConnectExceptionForAckMsgWhenCnxIsNull() throws Exception {
        String topic = NAMESPACE1 + "/testGetConnectExceptionForAckMsgWhenCnxIsNull";
        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer(Schema.BYTES)
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.newMessage().value(Bytes.toBytes(i)).send();
        }
        ClientCnx cnx = Whitebox.invokeMethod(consumer, "cnx");
        Whitebox.invokeMethod(consumer, "connectionClosed", cnx);

        Message<byte[]> message = consumer.receive();
        Transaction transaction = pulsarClient
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        try {
            consumer.acknowledgeAsync(message.getMessageId(), transaction).get();
            fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof PulsarClientException.ConnectException);
        }
    }


    @Test
    public void testPendingAckBatchMessageCommit() throws Exception {
        String topic = NAMESPACE1 + "/testPendingAckBatchMessageCommit";

        // enable batch index ack
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer(Schema.BYTES)
                .topic(topic)
                .enableBatching(true)
                // ensure that batch message is sent
                .batchingMaxPublishDelay(3, TimeUnit.SECONDS)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();

        // send batch message, the size is 5
        for (int i = 0; i < 5; i++) {
            producer.sendAsync(("test" + i).getBytes());
        }

        producer.flush();

        Transaction txn1 = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.MINUTES).build().get();
        // ack the first message with transaction
        consumer.acknowledgeAsync(consumer.receive().getMessageId(), txn1).get();
        Transaction txn2 = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.MINUTES).build().get();
        // ack the second message with transaction
        MessageId messageId = consumer.receive().getMessageId();
        consumer.acknowledgeAsync(messageId, txn2).get();

        // commit the txn1
        txn1.commit().get();
        // abort the txn2
        txn2.abort().get();

        Transaction txn3 = pulsarClient.newTransaction()
                .withTransactionTimeout(10, TimeUnit.MINUTES).build().get();
        // repeat ack the second message, can ack successful
        consumer.acknowledgeAsync(messageId, txn3).get();
    }

    /**
     * When change pending ack handle state failure, exceptionally complete cmd-subscribe.
     * see: https://github.com/apache/pulsar/pull/16248.
     */
    @Test
    public void testPendingAckReplayChangeStateError() throws InterruptedException, TimeoutException {
        AtomicInteger atomicInteger = new AtomicInteger(1);
        // Create Executor
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        // Mock serviceConfiguration.
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
        when(serviceConfiguration.isTransactionCoordinatorEnabled()).thenReturn(true);
        // Mock executorProvider.
        ExecutorProvider executorProvider = mock(ExecutorProvider.class);
        when(executorProvider.getExecutor()).thenReturn(executorService);
        when(executorProvider.getExecutor(any(Object.class))).thenReturn(executorService);
        // Mock pendingAckStore.
        PendingAckStore pendingAckStore = mock(PendingAckStore.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                executorService.execute(()->{
                    PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) invocation.getArguments()[0];
                    pendingAckHandle.closeAsync();
                    MLPendingAckReplyCallBack mlPendingAckReplyCallBack
                            = new MLPendingAckReplyCallBack(pendingAckHandle);
                    mlPendingAckReplyCallBack.replayComplete();
                });
                return null;
            }
        }).when(pendingAckStore).replayAsync(any(), any());
        // Mock executorProvider.
        TransactionPendingAckStoreProvider pendingAckStoreProvider = mock(TransactionPendingAckStoreProvider.class);
        when(pendingAckStoreProvider.checkInitializedBefore(any()))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(pendingAckStoreProvider.newPendingAckStore(any()))
                .thenReturn(CompletableFuture.completedFuture(pendingAckStore));
        // Mock pulsar.
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfig()).thenReturn(serviceConfiguration);
        when(pulsar.getTransactionExecutorProvider()).thenReturn(executorProvider);
        when(pulsar.getTransactionPendingAckStoreProvider()).thenReturn(pendingAckStoreProvider);
        // Mock brokerService.
        BrokerService brokerService = mock(BrokerService.class);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        // Mock topic.
        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(topic.getName()).thenReturn("topic-a");
        // Mock cursor for subscription.
        ManagedCursor cursor_subscription = mock(ManagedCursor.class);
        doThrow(new RuntimeException("1")).when(cursor_subscription).updateLastActive();
        // Create subscription.
        String subscriptionName = "sub-a";
        boolean replicated = false;
        Map<String, String> subscriptionProperties = Collections.emptyMap();
        PersistentSubscription persistentSubscription = new PersistentSubscription(topic, subscriptionName,
                cursor_subscription, replicated, subscriptionProperties);
        org.apache.pulsar.broker.service.Consumer consumer = mock(org.apache.pulsar.broker.service.Consumer.class);
        try {
            CompletableFuture<Void> addConsumerFuture = persistentSubscription.addConsumer(consumer);
            addConsumerFuture.get(5, TimeUnit.SECONDS);
            fail("Expect failure by PendingAckHandle closed, but success");
        } catch (ExecutionException executionException){
            Throwable t = executionException.getCause();
            Assert.assertTrue(t instanceof BrokerServiceException.ServiceUnitNotReadyException);
        }
    }

    /**
     * When change TB state failure, exceptionally complete cmd-producer.
     * see: https://github.com/apache/pulsar/pull/16248.
     */
    @Test
    public void testTBRecoverChangeStateError() throws InterruptedException, TimeoutException {
        final AtomicReference<PersistentTopic> persistentTopic = new AtomicReference<PersistentTopic>();
        AtomicInteger atomicInteger = new AtomicInteger(1);
        // Create Executor
        ScheduledExecutorService executorService_recover = mock(ScheduledExecutorService.class);
        // Mock serviceConfiguration.
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
        when(serviceConfiguration.isEnableReplicatedSubscriptions()).thenReturn(false);
        when(serviceConfiguration.isTransactionCoordinatorEnabled()).thenReturn(true);
        // Mock executorProvider.
        ExecutorProvider executorProvider = mock(ExecutorProvider.class);
        when(executorProvider.getExecutor(any(Object.class))).thenReturn(executorService_recover);
        // Mock pendingAckStore.
        PendingAckStore pendingAckStore = mock(PendingAckStore.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                new Thread(() -> {
                    TopicTransactionBuffer.TopicTransactionBufferRecover recover
                            = (TopicTransactionBuffer.TopicTransactionBufferRecover)invocation.getArguments()[0];
                    TopicTransactionBufferRecoverCallBack callBack
                            = Whitebox.getInternalState(recover, "callBack");;
                    try {
                        persistentTopic.get().getTransactionBuffer().closeAsync().get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    callBack.recoverComplete();
                }).start();
                return null;
            }
        }).when(executorService_recover).execute(any());
        // Mock executorProvider.
        TransactionPendingAckStoreProvider pendingAckStoreProvider = mock(TransactionPendingAckStoreProvider.class);
        when(pendingAckStoreProvider.checkInitializedBefore(any()))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(pendingAckStoreProvider.newPendingAckStore(any()))
                .thenReturn(CompletableFuture.completedFuture(pendingAckStore));
        // Mock TransactionBufferSnapshotService
        TransactionBufferSnapshotService transactionBufferSnapshotService
                = mock(TransactionBufferSnapshotService.class);
        SystemTopicClient.Writer writer = mock(SystemTopicClient.Writer.class);
        when(writer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(transactionBufferSnapshotService.createWriter(any()))
                .thenReturn(CompletableFuture.completedFuture(writer));
        // Mock pulsar.
        PulsarService pulsar = mock(PulsarService.class);
        PulsarResources pulsarResources = mock(PulsarResources.class);
        NamespaceResources namespaceResources = mock(NamespaceResources.class);
        when(pulsarResources.getNamespaceResources()).thenReturn(namespaceResources);
        when(pulsar.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsar.getConfiguration()).thenReturn(serviceConfiguration);
        when(pulsar.getConfig()).thenReturn(serviceConfiguration);
        when(pulsar.getTransactionExecutorProvider()).thenReturn(executorProvider);
        when(pulsar.getTransactionBufferSnapshotService()).thenReturn(transactionBufferSnapshotService);
        TopicTransactionBufferProvider topicTransactionBufferProvider = new TopicTransactionBufferProvider();
        when(pulsar.getTransactionBufferProvider()).thenReturn(topicTransactionBufferProvider);
        // Mock BacklogQuotaManager
        BacklogQuotaManager backlogQuotaManager = mock(BacklogQuotaManager.class);
        // Mock brokerService.
        BrokerService brokerService = mock(BrokerService.class);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getBacklogQuotaManager()).thenReturn(backlogQuotaManager);
        // Mock managedLedger.
        ManagedLedgerImpl managedLedger = mock(ManagedLedgerImpl.class);
        ManagedCursorContainer managedCursors = new ManagedCursorContainer();
        when(managedLedger.getCursors()).thenReturn(managedCursors);
        PositionImpl position = PositionImpl.EARLIEST;
        when(managedLedger.getLastConfirmedEntry()).thenReturn(position);
        // Create topic.
        persistentTopic.set(new PersistentTopic("topic-a", managedLedger, brokerService));
        try {
            // Do check.
            persistentTopic.get().checkIfTransactionBufferRecoverCompletely(true).get(5, TimeUnit.SECONDS);
            fail("Expect failure by TB closed, but it is finished.");
        } catch (ExecutionException executionException){
            Throwable t = executionException.getCause();
            Assert.assertTrue(t instanceof BrokerServiceException.ServiceUnitNotReadyException);
        }
    }

    @Test
    public void testGetTxnState() throws Exception {
        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.SECONDS)
                .build().get();

        // test OPEN and TIMEOUT
        assertEquals(transaction.getState(), Transaction.State.OPEN);
        Transaction timeoutTxn = transaction;
        Awaitility.await().until(() -> timeoutTxn.getState() == Transaction.State.TIME_OUT);

        // test abort
        transaction = pulsarClient.newTransaction().withTransactionTimeout(3, TimeUnit.SECONDS)
                .build().get();
        transaction.abort().get();
        assertEquals(transaction.getState(), Transaction.State.ABORTED);

        // test commit
        transaction = pulsarClient.newTransaction().withTransactionTimeout(3, TimeUnit.SECONDS)
                .build().get();
        transaction.commit().get();
        assertEquals(transaction.getState(), Transaction.State.COMMITTED);

        // test error
        transaction = pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.SECONDS)
                .build().get();
        pulsarServiceList.get(0).getTransactionMetadataStoreService()
                .endTransaction(transaction.getTxnID(), 0, false);
        transaction.commit();
        Transaction errorTxn = transaction;
        Awaitility.await().until(() -> errorTxn.getState() == Transaction.State.ERROR);

        // test committing
        transaction = pulsarClient.newTransaction().withTransactionTimeout(3, TimeUnit.SECONDS)
                .build().get();
        ((TransactionImpl) transaction).registerSendOp(new CompletableFuture<>());
        transaction.commit();
        Transaction committingTxn = transaction;
        Awaitility.await().until(() -> committingTxn.getState() == Transaction.State.COMMITTING);

        // test aborting
        transaction = pulsarClient.newTransaction().withTransactionTimeout(3, TimeUnit.SECONDS)
                .build().get();
        ((TransactionImpl) transaction).registerSendOp(new CompletableFuture<>());
        transaction.abort();
        Transaction abortingTxn = transaction;
        Awaitility.await().until(() -> abortingTxn.getState() == Transaction.State.ABORTING);
    }


    @Test
    public void testEncryptionRequired() throws Exception {
        final String namespace = "tnx/ns-prechecks";
        final String topic = "persistent://" + namespace + "/test_transaction_topic";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setEncryptionRequiredStatus(namespace, true);
        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        Producer<byte[]> producer = this.pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(5, TimeUnit.SECONDS)
                .addEncryptionKey("my-app-key")
                .defaultCryptoKeyReader("file:./src/test/resources/certificate/public-key.client-rsa.pem")
                .create();

        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS).build().get();
        producer.newMessage(txn)
                    .value(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
                    .send();
        txn.commit();
    }
}
