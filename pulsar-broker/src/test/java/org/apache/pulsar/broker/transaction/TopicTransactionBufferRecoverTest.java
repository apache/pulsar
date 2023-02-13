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
package org.apache.pulsar.broker.transaction;

import static org.apache.pulsar.common.naming.SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotServiceFactory;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.impl.SingleSnapshotAbortedTxnProcessorImpl;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndex;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TxnIDData;
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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class TopicTransactionBufferRecoverTest extends TransactionTestBase {

    private static final String RECOVER_COMMIT = NAMESPACE1 + "/recover-commit";
    private static final String RECOVER_ABORT = NAMESPACE1 + "/recover-abort";
    private static final String SNAPSHOT_INDEX = NAMESPACE1 + "/snapshot-index";
    private static final String SNAPSHOT_SEGMENT = NAMESPACE1 + "/snapshot-segment";
    private static final String SUBSCRIPTION_NAME = "test-recover";
    private static final String TAKE_SNAPSHOT = NAMESPACE1 + "/take-snapshot";
    private static final String ABORT_DELETE = NAMESPACE1 + "/abort-delete";
    private static final int NUM_PARTITIONS = 16;
    @BeforeMethod
    protected void setup() throws Exception {
        conf.getProperties().setProperty("brokerClient_operationTimeoutMs", Integer.valueOf(10 * 1000).toString());
        setUpBase(1, NUM_PARTITIONS, RECOVER_COMMIT, 0);
        admin.topics().createNonPartitionedTopic(RECOVER_ABORT);
        admin.topics().createNonPartitionedTopic(TAKE_SNAPSHOT);
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

    @DataProvider(name = "enableSnapshotSegment")
    public Object[] testSnapshot() {
        return new Boolean[] {
                true,
                false
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

        tnx1.commit().get();

        // only can receive message 1
        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(message);
        log.info("Txn1 commit receive message : {}, messageId : {}", message.getValue(), message.getMessageId());
        consumer.acknowledge(message);

        // can't receive message
        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);
        admin.topics().unload(testTopic);

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

    private void makeTBSnapshotReaderTimeoutIfFirstRead(TopicName topicName) throws Exception {
        SystemTopicClient.Reader mockReader = mock(SystemTopicClient.Reader.class);
        AtomicBoolean isFirstCallOfMethodHasMoreEvents = new AtomicBoolean();
        AtomicBoolean isFirstCallOfMethodHasReadNext = new AtomicBoolean();
        AtomicBoolean isFirstCallOfMethodHasReadNextAsync = new AtomicBoolean();

        doAnswer(invocation -> {
            if (isFirstCallOfMethodHasMoreEvents.compareAndSet(false,true)){
                return true;
            } else {
                return false;
            }
        }).when(mockReader).hasMoreEvents();

        doAnswer(invocation -> {
            if (isFirstCallOfMethodHasReadNext.compareAndSet(false, true)){
                // Just stuck the thread.
                Thread.sleep(3600 * 1000);
            }
            return null;
        }).when(mockReader).readNext();

        doAnswer(invocation -> {
            CompletableFuture<Message> future = new CompletableFuture<>();
            new Thread(() -> {
                if (isFirstCallOfMethodHasReadNextAsync.compareAndSet(false, true)){
                    // Just stuck the thread.
                    try {
                        Thread.sleep(3600 * 1000);
                    } catch (InterruptedException e) {
                    }
                    future.complete(null);
                } else {
                    future.complete(null);
                }
            }).start();
            return future;
        }).when(mockReader).readNextAsync();

        when(mockReader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        for (PulsarService pulsarService : pulsarServiceList){
            // Init prop: lastMessageIdInBroker.
            final SystemTopicTxnBufferSnapshotService tbSnapshotService =
                    pulsarService.getTransactionBufferSnapshotServiceFactory().getTxnBufferSnapshotService();
            SystemTopicTxnBufferSnapshotService spyTbSnapshotService = spy(tbSnapshotService);
            doAnswer(invocation -> CompletableFuture.completedFuture(mockReader))
                    .when(spyTbSnapshotService).createReader(topicName);
            Field field =
                    TransactionBufferSnapshotServiceFactory.class.getDeclaredField("txnBufferSnapshotService");
            field.setAccessible(true);
            field.set(pulsarService.getTransactionBufferSnapshotServiceFactory(), spyTbSnapshotService);
        }
    }

    @Test(timeOut = 60 * 1000)
    public void testTBRecoverCanRetryIfTimeoutRead() throws Exception {
        String topicName = String.format("persistent://%s/%s", NAMESPACE1,
                "tx_recover_" + UUID.randomUUID().toString().replaceAll("-", "_"));

        // Make race condition of "getLastMessageId" and "compaction" to make recover can't complete.
        makeTBSnapshotReaderTimeoutIfFirstRead(TopicName.get(topicName));
        // Verify( Cmd-PRODUCER will wait for TB recover finished )
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .batchingMaxMessages(2)
                .create();

        // cleanup.
        producer.close();
        admin.topics().delete(topicName, false);
    }

    private void testTakeSnapshot() throws Exception {
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
                .topic(NAMESPACE1 + "/" + TRANSACTION_BUFFER_SNAPSHOT);
        Reader<TransactionBufferSnapshot> reader = readerBuilder.create();

        MessageId messageId1 = producer.newMessage(tnx1).value("test").send();
        tnx1.commit().get();
        // wait timeout take snapshot

        Awaitility.await().untilAsserted(() -> {
            TransactionBufferSnapshot transactionBufferSnapshot = reader.readNext().getValue();
            assertEquals(transactionBufferSnapshot.getMaxReadPositionEntryId(), -1);
            assertEquals(transactionBufferSnapshot.getMaxReadPositionLedgerId(), ((MessageIdImpl) messageId1).getLedgerId());
            transactionBufferSnapshot = reader.readNext().getValue();
            assertEquals(transactionBufferSnapshot.getMaxReadPositionEntryId(), ((MessageIdImpl) messageId1).getEntryId() + 1);
            assertEquals(transactionBufferSnapshot.getMaxReadPositionLedgerId(), ((MessageIdImpl) messageId1).getLedgerId());
            assertFalse(reader.hasMessageAvailable());
        });

        // take snapshot by change times
        MessageId messageId2 = producer.newMessage(tnx2).value("test").send();
        tnx2.commit().get();


        TransactionBufferSnapshot snapshot = reader.readNext().getValue();
        assertEquals(snapshot.getMaxReadPositionEntryId(), ((MessageIdImpl) messageId2).getEntryId() + 1);
        assertEquals(snapshot.getMaxReadPositionLedgerId(), ((MessageIdImpl) messageId2).getLedgerId());
        assertEquals(snapshot.getAborts().size(), 0);
        assertFalse(reader.hasMessageAvailable());

        MessageId messageId3 = producer.newMessage(abortTxn).value("test").send();
        abortTxn.abort().get();

        TransactionBufferSnapshot transactionBufferSnapshot = reader.readNext().getValue();
        assertEquals(transactionBufferSnapshot.getMaxReadPositionEntryId(), ((MessageIdImpl) messageId3).getEntryId() + 1);
        assertEquals(transactionBufferSnapshot.getMaxReadPositionLedgerId(), ((MessageIdImpl) messageId3).getLedgerId());
        assertEquals(transactionBufferSnapshot.getAborts().size(), 1);
        assertEquals(transactionBufferSnapshot.getAborts().get(0).getTxnIdLeastBits(),
                ((TransactionImpl) abortTxn).getTxnIdLeastBits());
        assertEquals(transactionBufferSnapshot.getAborts().get(0).getTxnIdMostBits(),
                ((TransactionImpl) abortTxn).getTxnIdMostBits());
        assertFalse(reader.hasMessageAvailable());
        reader.close();
        producer.close();

    }

    @Test(dataProvider = "enableSnapshotSegment")
    private void testTopicTransactionBufferDeleteAbort(Boolean enableSnapshotSegment) throws Exception {
        getPulsarServiceList().get(0).getConfig().setTransactionBufferSegmentedSnapshotEnabled(enableSnapshotSegment);
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
                    field = TopicTransactionBuffer.class.getDeclaredField("snapshotAbortedTxnProcessor");
                    field.setAccessible(true);
                    AbortedTxnProcessor abortedTxnProcessor = (AbortedTxnProcessor) field.get(topicTransactionBuffer);

                    if (enableSnapshotSegment) {
                        //TODO
                        exist = true;
                    } else {
                        Field abortsField = SingleSnapshotAbortedTxnProcessorImpl.class.getDeclaredField("aborts");
                        abortsField.setAccessible(true);

                        LinkedMap<TxnID, PositionImpl> linkedMap =
                                (LinkedMap<TxnID, PositionImpl>) abortsField.get(abortedTxnProcessor);
                        assertEquals(linkedMap.size(), 1);
                        assertEquals(linkedMap.get(linkedMap.firstKey()).getLedgerId(),
                                ((MessageIdImpl) message.getMessageId()).getLedgerId());
                        exist = true;
                    }

                }
            }
        }
        assertTrue(exist);
    }

    @Test(dataProvider = "enableSnapshotSegment")
    public void clearTransactionBufferSnapshotTest(Boolean enableSnapshotSegment) throws Exception {
        getPulsarServiceList().get(0).getConfig().setTransactionBufferSegmentedSnapshotEnabled(enableSnapshotSegment);
        String topic = NAMESPACE1 + "/tb-snapshot-delete-" + RandomUtils.nextInt();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        producer.newMessage(txn).value("test".getBytes()).sendAsync();
        producer.newMessage(txn).value("test".getBytes()).sendAsync();
        txn.commit().get();
        producer.close();

        // take snapshot
        PersistentTopic originalTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(topic).toString(), false).get().get();
        TopicTransactionBuffer topicTransactionBuffer = (TopicTransactionBuffer) originalTopic.getTransactionBuffer();
        Field abortedTxnProcessorField = TopicTransactionBuffer.class.getDeclaredField("snapshotAbortedTxnProcessor");
        abortedTxnProcessorField.setAccessible(true);
        AbortedTxnProcessor abortedTxnProcessor =
                (AbortedTxnProcessor) abortedTxnProcessorField.get(topicTransactionBuffer);
        abortedTxnProcessor.takeAbortedTxnsSnapshot(topicTransactionBuffer.getMaxReadPosition());

        TopicName transactionBufferTopicName;
        if (!enableSnapshotSegment) {
            transactionBufferTopicName  = NamespaceEventsSystemTopicFactory.getSystemTopicName(
                            TopicName.get(topic).getNamespaceObject(), EventType.TRANSACTION_BUFFER_SNAPSHOT);
        }  else {
            transactionBufferTopicName  = NamespaceEventsSystemTopicFactory.getSystemTopicName(
                    TopicName.get(topic).getNamespaceObject(), EventType.TRANSACTION_BUFFER_SNAPSHOT_INDEXES);
        }
        PersistentTopic snapshotTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(transactionBufferTopicName.toString(), false).get().get();
        Field field = PersistentTopic.class.getDeclaredField("currentCompaction");
        field.setAccessible(true);

        // Trigger compaction and make sure it is finished.
        checkSnapshotCount(transactionBufferTopicName, true, snapshotTopic, field);
        admin.topics().delete(topic, true);
        checkSnapshotCount(transactionBufferTopicName, false, snapshotTopic, field);
    }

    private void checkSnapshotCount(TopicName topicName, boolean hasSnapshot,
                                    PersistentTopic persistentTopic, Field field) throws Exception {
        persistentTopic.triggerCompaction();
        CompletableFuture<Long> compactionFuture = (CompletableFuture<Long>) field.get(persistentTopic);
        Awaitility.await().untilAsserted(() -> assertTrue(compactionFuture.isDone()));

        Reader<GenericRecord> reader = pulsarClient.newReader(Schema.AUTO_CONSUME())
                .readCompacted(true)
                .startMessageId(MessageId.earliest)
                .startMessageIdInclusive()
                .topic(topicName.toString())
                .create();

        int count = 0;
        while (true) {
            Message<GenericRecord> snapshotMsg = reader.readNext(2, TimeUnit.SECONDS);
            if (snapshotMsg != null) {
                count++;
            } else {
                break;
            }
        }
        assertTrue(hasSnapshot ? count > 0 : count == 0);
        reader.close();
    }

    @Test(timeOut=30000)
    public void testTransactionBufferRecoverThrowException() throws Exception {
        String topic = NAMESPACE1 + "/testTransactionBufferRecoverThrowPulsarClientException";
        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        producer.newMessage(txn).value("test".getBytes()).sendAsync();
        producer.newMessage(txn).value("test".getBytes()).sendAsync();
        txn.commit().get();

        PersistentTopic originalTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(topic).toString(), false).get().get();
        SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshot> systemTopicTxnBufferSnapshotService =
                mock(SystemTopicTxnBufferSnapshotService.class);
        SystemTopicClient.Reader<TransactionBufferSnapshot> reader = mock(SystemTopicClient.Reader.class);
        SystemTopicClient.Writer<TransactionBufferSnapshot> writer = mock(SystemTopicClient.Writer.class);

        doReturn(CompletableFuture.completedFuture(reader))
                .when(systemTopicTxnBufferSnapshotService).createReader(any());
        doReturn(CompletableFuture.completedFuture(writer))
                .when(systemTopicTxnBufferSnapshotService).createWriter(any());
        TransactionBufferSnapshotServiceFactory transactionBufferSnapshotServiceFactory =
                mock(TransactionBufferSnapshotServiceFactory.class);
        doReturn(systemTopicTxnBufferSnapshotService)
                .when(transactionBufferSnapshotServiceFactory).getTxnBufferSnapshotService();
        doReturn(CompletableFuture.completedFuture(null)).when(reader).closeAsync();
        doReturn(CompletableFuture.completedFuture(null)).when(writer).closeAsync();
        Field field = PulsarService.class.getDeclaredField("transactionBufferSnapshotServiceFactory");
        field.setAccessible(true);
        TransactionBufferSnapshotServiceFactory transactionBufferSnapshotServiceFactoryOriginal =
                ((TransactionBufferSnapshotServiceFactory)field.get(getPulsarServiceList().get(0)));
        // mock reader can't read snapshot fail throw RuntimeException
        doThrow(new RuntimeException("test")).when(reader).hasMoreEvents();
        // check reader close topic
        checkCloseTopic(pulsarClient, transactionBufferSnapshotServiceFactoryOriginal,
                transactionBufferSnapshotServiceFactory, originalTopic, field, producer);
        doReturn(true).when(reader).hasMoreEvents();

        // mock reader can't read snapshot fail throw PulsarClientException
        doThrow(new PulsarClientException("test")).when(reader).hasMoreEvents();
        // check reader close topic
        checkCloseTopic(pulsarClient, transactionBufferSnapshotServiceFactoryOriginal,
                transactionBufferSnapshotServiceFactory, originalTopic, field, producer);
        doReturn(true).when(reader).hasMoreEvents();

        // mock create reader fail
        doReturn(FutureUtil.failedFuture(new PulsarClientException("test")))
                .when(systemTopicTxnBufferSnapshotService).createReader(any());
        // check create reader fail close topic
        originalTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(topic).toString(), false).get().get();
        checkCloseTopic(pulsarClient, transactionBufferSnapshotServiceFactoryOriginal,
                transactionBufferSnapshotServiceFactory, originalTopic, field, producer);
        doReturn(CompletableFuture.completedFuture(reader)).when(systemTopicTxnBufferSnapshotService).createReader(any());

        // check create writer fail close topic
        originalTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(topic).toString(), false).get().get();
        // mock create writer fail
        doReturn(FutureUtil.failedFuture(new PulsarClientException("test")))
                .when(systemTopicTxnBufferSnapshotService).createWriter(any());
        checkCloseTopic(pulsarClient, transactionBufferSnapshotServiceFactoryOriginal,
                transactionBufferSnapshotServiceFactory, originalTopic, field, producer);
    }

    private void checkCloseTopic(PulsarClient pulsarClient,
                                 TransactionBufferSnapshotServiceFactory transactionBufferSnapshotServiceFactoryOriginal,
                                 TransactionBufferSnapshotServiceFactory transactionBufferSnapshotServiceFactory,
                                 PersistentTopic originalTopic,
                                 Field field,
                                 Producer<byte[]> producer) throws Exception {
        field.set(getPulsarServiceList().get(0), transactionBufferSnapshotServiceFactory);

        // recover again will throw then close topic
        new TopicTransactionBuffer(originalTopic);
        Awaitility.await().untilAsserted(() -> {
            // isFenced means closed
            Field close = AbstractTopic.class.getDeclaredField("isFenced");
            close.setAccessible(true);
            assertTrue((boolean) close.get(originalTopic));
        });

        field.set(getPulsarServiceList().get(0), transactionBufferSnapshotServiceFactoryOriginal);

        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        producer.newMessage(txn).value("test".getBytes()).send();
        txn.commit().get();
    }


    @Test
    public void testTransactionBufferNoSnapshotCloseReader() throws Exception{
        String topic = NAMESPACE1 + "/test";
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).producerName("testTxnTimeOut_producer")
                .topic(topic).sendTimeout(0, TimeUnit.SECONDS).enableBatching(false).create();

        admin.topics().unload(topic);

        // unload success, all readers have been closed except for the compaction sub
        producer.send("test");
        TopicStats stats = admin.topics().getStats(NAMESPACE1 + "/" + TRANSACTION_BUFFER_SNAPSHOT);

        // except for the compaction sub
        assertEquals(stats.getSubscriptions().size(), 1);
        assertTrue(stats.getSubscriptions().keySet().contains("__compaction"));
    }

    @Test
    public void testTransactionBufferIndexSystemTopic() throws Exception {
        SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotIndexes> transactionBufferSnapshotIndexService =
                new TransactionBufferSnapshotServiceFactory(pulsarClient).getTxnBufferSnapshotIndexService();

        SystemTopicClient.Writer<TransactionBufferSnapshotIndexes> indexesWriter =
                transactionBufferSnapshotIndexService.createWriter(TopicName.get(SNAPSHOT_INDEX)).get();

        SystemTopicClient.Reader<TransactionBufferSnapshotIndexes> indexesReader =
                transactionBufferSnapshotIndexService.createReader(TopicName.get(SNAPSHOT_INDEX)).get();


        List<TransactionBufferSnapshotIndex> indexList = new LinkedList<>();

        for (long i = 0; i < 5; i++) {
            indexList.add(new TransactionBufferSnapshotIndex(i, i, i, i, i));
        }

        TransactionBufferSnapshotIndexes transactionBufferTransactionBufferSnapshotIndexes =
                new TransactionBufferSnapshotIndexes(SNAPSHOT_INDEX,
                        indexList, null);

        indexesWriter.write(SNAPSHOT_INDEX, transactionBufferTransactionBufferSnapshotIndexes);

        assertTrue(indexesReader.hasMoreEvents());
        transactionBufferTransactionBufferSnapshotIndexes = indexesReader.readNext().getValue();
        assertEquals(transactionBufferTransactionBufferSnapshotIndexes.getTopicName(), SNAPSHOT_INDEX);
        assertEquals(transactionBufferTransactionBufferSnapshotIndexes.getIndexList().size(), 5);
        assertNull(transactionBufferTransactionBufferSnapshotIndexes.getSnapshot());

        TransactionBufferSnapshotIndex transactionBufferSnapshotIndex =
                transactionBufferTransactionBufferSnapshotIndexes.getIndexList().get(1);
        assertEquals(transactionBufferSnapshotIndex.getAbortedMarkLedgerID(), 1L);
        assertEquals(transactionBufferSnapshotIndex.getAbortedMarkEntryID(), 1L);
        assertEquals(transactionBufferSnapshotIndex.getSegmentLedgerID(), 1L);
        assertEquals(transactionBufferSnapshotIndex.getSegmentEntryID(), 1L);
        assertEquals(transactionBufferSnapshotIndex.getSequenceID(), 1L);
    }

    public static String buildKey(
            TransactionBufferSnapshotSegment snapshot) {
        return  "multiple-" + snapshot.getSequenceId() + "-" + snapshot.getTopicName();
    }

    @Test
    public void testTransactionBufferSegmentSystemTopic() throws Exception {
        // init topic and topicName
        String snapshotTopic = NAMESPACE1 + "/" + EventType.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS;
        TopicName snapshotSegmentTopicName = TopicName.getPartitionedTopicName(snapshotTopic);

        //send message to create manager ledger
        Producer<TransactionBufferSnapshotSegment> producer =
                pulsarClient.newProducer(Schema.AVRO(
                                TransactionBufferSnapshotSegment.class))
                .topic(snapshotTopic)
                .create();

        // get brokerService and pulsarService
        PulsarService pulsarService = getPulsarServiceList().get(0);
        BrokerService brokerService = pulsarService.getBrokerService();

        // create snapshot segment writer
        SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotSegment>
                transactionBufferSnapshotSegmentService =
                new TransactionBufferSnapshotServiceFactory(pulsarClient).getTxnBufferSnapshotSegmentService();

        SystemTopicClient.Writer<TransactionBufferSnapshotSegment>
                segmentWriter = transactionBufferSnapshotSegmentService.createWriter(snapshotSegmentTopicName).get();

        // write two snapshot to snapshot segment topic
        TransactionBufferSnapshotSegment snapshot =
                new TransactionBufferSnapshotSegment();

        //build and send snapshot
        snapshot.setTopicName(snapshotTopic);
        snapshot.setSequenceId(1L);
        snapshot.setPersistentPositionLedgerId(2L);
        snapshot.setPersistentPositionEntryId(3L);
        LinkedList<TxnIDData> txnIDSet = new LinkedList<>();
        txnIDSet.add(new TxnIDData(1, 1));
        snapshot.setAborts(txnIDSet );

        segmentWriter.write(buildKey(snapshot), snapshot);
        snapshot.setSequenceId(2L);

        MessageIdImpl messageId = (MessageIdImpl) segmentWriter.write(buildKey(snapshot), snapshot);

        //Create read-only managed ledger
        //And read the entry and decode entry to snapshot
        CompletableFuture<Entry> entryCompletableFuture = new CompletableFuture<>();
        AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = new AsyncCallbacks
                .OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedgerImpl readOnlyManagedLedger, Object ctx) {
                readOnlyManagedLedger.asyncReadEntry(
                        new PositionImpl(messageId.getLedgerId(), messageId.getEntryId()),
                        new AsyncCallbacks.ReadEntryCallback() {
                            @Override
                            public void readEntryComplete(Entry entry, Object ctx) {
                                entryCompletableFuture.complete(entry);
                            }

                            @Override
                            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                entryCompletableFuture.completeExceptionally(exception);
                            }
                        }, null);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                //
            }
        };
        pulsarService.getManagedLedgerFactory()
                .asyncOpenReadOnlyManagedLedger(snapshotSegmentTopicName.getPersistenceNamingEncoding(), callback,
                        brokerService.getManagedLedgerConfig(snapshotSegmentTopicName).get(),null);

        Entry entry = entryCompletableFuture.get();
        //decode snapshot from entry
        ByteBuf headersAndPayload = entry.getDataBuffer();
        //skip metadata
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        snapshot = Schema.AVRO(TransactionBufferSnapshotSegment.class)
                .decode(Unpooled.wrappedBuffer(headersAndPayload).nioBuffer());

        //verify snapshot
        assertEquals(snapshot.getTopicName(), snapshotTopic);
        assertEquals(snapshot.getSequenceId(), 2L);
        assertEquals(snapshot.getPersistentPositionLedgerId(), 2L);
        assertEquals(snapshot.getPersistentPositionEntryId(), 3L);
        assertEquals(snapshot.getAborts().toArray()[0], new TxnIDData(1, 1));
    }

    //Verify the snapshotSegmentProcessor end to end
    @Test
    public void testSnapshotSegment() throws Exception {
        String topic ="persistent://" + NAMESPACE1 + "/testSnapshotSegment";
        String subName = "testSnapshotSegment";

        LinkedMap<Transaction, MessageId> ongoingTxns = new LinkedMap<>();
        LinkedList<MessageId> abortedTxns = new LinkedList<>();
        // 0. Modify the configurations, enabling the segment snapshot and set the size of the snapshot segment.
        int theSizeOfSegment = 10;
        int theCountOfSnapshotMaxTxnCount = 3;
        this.getPulsarServiceList().get(0).getConfig().setTransactionBufferSegmentedSnapshotEnabled(true);
        this.getPulsarServiceList().get(0).getConfig()
                .setTransactionBufferSnapshotSegmentSize(8 + topic.length() + theSizeOfSegment * 3);
        this.getPulsarServiceList().get(0).getConfig()
                .setTransactionBufferSnapshotMaxTransactionCount(theCountOfSnapshotMaxTxnCount);
        // 1. Build producer and consumer
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(false)
                .create();

        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        // 2. Check the AbortedTxnProcessor workflow 10 times
        int messageSize = theSizeOfSegment * 4;
        for (int i = 0; i < 10; i++) {
            MessageId maxReadMessage = null;
            int abortedTxnSize = 0;
            for (int j = 0; j < messageSize; j++) {
                Transaction transaction = pulsarClient.newTransaction()
                        .withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
                //Half common message and half transaction message.
                if (j % 2 == 0) {
                    MessageId messageId = producer.newMessage(transaction).value(i * 10 + j).send();
                    //And the transaction message have a half which are aborted.
                    if (RandomUtils.nextInt() % 2 == 0) {
                        transaction.abort().get();
                        abortedTxns.add(messageId);
                        abortedTxnSize++;
                    } else {
                        ongoingTxns.put(transaction, messageId);
                        if (maxReadMessage == null) {
                            //The except number of the messages that can be read
                            maxReadMessage = messageId;
                        }
                    }
                } else {
                    producer.newMessage().value(i * 10 + j).send();
                    transaction.commit().get();
                }
            }
            // 2.1 Receive all message before the maxReadPosition to verify the correctness of the max read position.
            int hasReceived = 0;
            while (true) {
                Message<Integer> message = consumer.receive(2, TimeUnit.SECONDS);
                if (message != null) {
                    Assert.assertTrue(message.getMessageId().compareTo(maxReadMessage) < 0);
                    hasReceived ++;
                } else {
                    break;
                }
            }
            //2.2 Commit all ongoing transaction and verify that the consumer can receive all rest message
            // expect for aborted txn message.
            for (Transaction ongoingTxn: ongoingTxns.keySet()) {
                ongoingTxn.commit().get();
            }
            ongoingTxns.clear();
            for (int k = hasReceived; k < messageSize - abortedTxnSize; k++) {
                Message<Integer> message = consumer.receive(2, TimeUnit.SECONDS);
                assertNotNull(message);
                assertFalse(abortedTxns.contains(message.getMessageId()));
            }
        }
        // 3. After the topic unload, the consumer can receive all the messages in the 10 tests
        // expect for the aborted transaction messages.
        admin.topics().unload(topic);
        for (int i = 0; i < messageSize * 10 - abortedTxns.size(); i++) {
            Message<Integer> message = consumer.receive(2, TimeUnit.SECONDS);
            assertNotNull(message);
            assertFalse(abortedTxns.contains(message.getMessageId()));
        }
        assertNull(consumer.receive(2, TimeUnit.SECONDS));
    }

}
