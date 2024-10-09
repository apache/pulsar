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

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService.ReferenceCountedWriter;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.impl.SingleSnapshotAbortedTxnProcessorImpl;
import org.apache.pulsar.broker.transaction.buffer.impl.SnapshotSegmentAbortedTxnProcessorImpl;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class SegmentAbortedTxnProcessorTest extends TransactionTestBase {

    private static final String PROCESSOR_TOPIC = "persistent://" + NAMESPACE1 + "/abortedTxnProcessor";
    private static final int SEGMENT_SIZE = 5;
    private PulsarService pulsarService = null;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        setUpBase(1, 1, null, 0);
        this.pulsarService = getPulsarServiceList().get(0);
        this.pulsarService.getConfig().setTransactionBufferSegmentedSnapshotEnabled(true);
        this.pulsarService.getConfig().setTransactionBufferSnapshotSegmentSize(8 + PROCESSOR_TOPIC.length() +
                SEGMENT_SIZE * 3);
        admin.topics().createNonPartitionedTopic(PROCESSOR_TOPIC);
        assertTrue(getSnapshotAbortedTxnProcessor(PROCESSOR_TOPIC) instanceof SnapshotSegmentAbortedTxnProcessorImpl);
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Test api:
     *   1. putAbortedTxnAndPosition
     *   2. checkAbortedTransaction
     *   3. takeAbortedTxnsSnapshot
     *   4. recoverFromSnapshot
     *   5. trimExpiredAbortedTxns
     * @throws Exception
     */
    @Test
    public void testPutAbortedTxnIntoProcessor() throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) pulsarService.getBrokerService()
                .getTopic(PROCESSOR_TOPIC, false).get().get();
        AbortedTxnProcessor processor = new SnapshotSegmentAbortedTxnProcessorImpl(persistentTopic);
        //1. prepare test data.
        //1.1 Put 10 aborted txn IDs to persistent two sealed segments.
        for (int i = 0; i < 10; i++) {
            TxnID txnID = new TxnID(0, i);
            Position position = PositionFactory.create(0, i);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        //1.2 Put 4 aborted txn IDs into the unsealed segment.
        for (int i = 10; i < 14; i++) {
            TxnID txnID = new TxnID(0, i);
            Position position = PositionFactory.create(0, i);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        //1.3 Verify the common data flow
        verifyAbortedTxnIDAndSegmentIndex(processor, 0, 14);
        //2. Take the latest snapshot and verify recover from snapshot
        AbortedTxnProcessor newProcessor = new SnapshotSegmentAbortedTxnProcessorImpl(persistentTopic);
        Position maxReadPosition = PositionFactory.create(0, 14);
        //2.1 Avoid update operation being canceled.
        waitTaskExecuteCompletely(processor);
        //2.2 take the latest snapshot
        processor.takeAbortedTxnsSnapshot(maxReadPosition).get();
        newProcessor.recoverFromSnapshot().get();
        //Verify the recovery data flow
        verifyAbortedTxnIDAndSegmentIndex(newProcessor, 0, 14);
        //3. Delete the ledgers and then verify the date.
        Field ledgersField = ManagedLedgerImpl.class.getDeclaredField("ledgers");
        ledgersField.setAccessible(true);
        NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers =
                (NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo>)
                        ledgersField.get(persistentTopic.getManagedLedger());
        ledgers.forEach((k, v) -> {
            ledgers.remove(k);
        });
        newProcessor.trimExpiredAbortedTxns();
        //4. Verify the two sealed segment will be deleted.
        Awaitility.await().untilAsserted(() -> verifyAbortedTxnIDAndSegmentIndex(newProcessor, 11, 4));
        processor.closeAsync().get(5, TimeUnit.SECONDS);
    }

    private void waitTaskExecuteCompletely(AbortedTxnProcessor processor) throws Exception {
        Field workerField = SnapshotSegmentAbortedTxnProcessorImpl.class.getDeclaredField("persistentWorker");
        workerField.setAccessible(true);
        SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker persistentWorker =
                (SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker) workerField.get(processor);
        Field taskQueueField = SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker.class
                .getDeclaredField("taskQueue");
        taskQueueField.setAccessible(true);
        Queue queue = (Queue) taskQueueField.get(persistentWorker);
        Awaitility.await().untilAsserted(() -> assertEquals(queue.size(), 0));
    }

    private void verifyAbortedTxnIDAndSegmentIndex(AbortedTxnProcessor processor, int begin, int txnIdSize)
            throws Exception {
        //Verify the checking of the aborted txn IDs
        for (int i = begin; i < txnIdSize; i++) {
            Assert.assertTrue(processor.checkAbortedTransaction(new TxnID(0, i)));
        }
        //Verify there are 2 sealed segment and the unsealed segment size is 4.
        Field unsealedSegmentField = SnapshotSegmentAbortedTxnProcessorImpl.class
                .getDeclaredField("unsealedTxnIds");
        Field indexField = SnapshotSegmentAbortedTxnProcessorImpl.class
                .getDeclaredField("segmentIndex");
        unsealedSegmentField.setAccessible(true);
        indexField.setAccessible(true);
        LinkedList<TxnID> unsealedSegment = (LinkedList<TxnID>) unsealedSegmentField.get(processor);
        LinkedMap<Position, TxnID> indexes = (LinkedMap<Position, TxnID>) indexField.get(processor);
        assertEquals(unsealedSegment.size(), txnIdSize % SEGMENT_SIZE);
        assertEquals(indexes.size(), txnIdSize / SEGMENT_SIZE);
    }

    // Verify the update index future can be completed when the queue has other tasks.
    @Test
    public void testFuturesCanCompleteWhenItIsCanceled() throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) pulsarService.getBrokerService()
                .getTopic(PROCESSOR_TOPIC, false).get().get();
        AbortedTxnProcessor processor = new SnapshotSegmentAbortedTxnProcessorImpl(persistentTopic);
        Field workerField = SnapshotSegmentAbortedTxnProcessorImpl.class.getDeclaredField("persistentWorker");
        workerField.setAccessible(true);
        SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker persistentWorker =
                (SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker) workerField.get(processor);
        Field taskQueueField = SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker.class
                .getDeclaredField("taskQueue");
        taskQueueField.setAccessible(true);
        Supplier task = CompletableFuture::new;
        Queue queue = (Queue) taskQueueField.get(persistentWorker);
        queue.add(new MutablePair<>(SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker.OperationType.WriteSegment,
                new MutablePair<>(new CompletableFuture<>(), task)));
        try {
            processor.takeAbortedTxnsSnapshot(PositionFactory.create(1, 10)).get(2, TimeUnit.SECONDS);
            fail("The update index operation should fail.");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException);
        } finally {
            processor.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testClearSnapshotSegments() throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) pulsarService.getBrokerService()
                .getTopic(PROCESSOR_TOPIC, false).get().get();
        AbortedTxnProcessor processor = new SnapshotSegmentAbortedTxnProcessorImpl(persistentTopic);
        //1. Write two snapshot segment.
        for (int j = 0; j < SEGMENT_SIZE * 2; j++) {
            TxnID txnID = new TxnID(0, j);
            Position position = PositionFactory.create(0, j);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        Awaitility.await().untilAsserted(() -> verifySnapshotSegmentsSize(PROCESSOR_TOPIC, 2));
        //2. Close index writer, making the index can not be updated.
        Field field = SnapshotSegmentAbortedTxnProcessorImpl.class.getDeclaredField("persistentWorker");
        field.setAccessible(true);
        SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker worker =
                (SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker) field.get(processor);
        Field indexWriteFutureField = SnapshotSegmentAbortedTxnProcessorImpl
                .PersistentWorker.class.getDeclaredField("snapshotIndexWriter");
        indexWriteFutureField.setAccessible(true);
        ReferenceCountedWriter<TransactionBufferSnapshotIndexes> snapshotIndexWriter =
                (ReferenceCountedWriter<TransactionBufferSnapshotIndexes>) indexWriteFutureField.get(worker);
        snapshotIndexWriter.release();
        // After release, the writer should be closed, call close method again to make sure the writer was closed.
        snapshotIndexWriter.getFuture().get().close();
        //3. Try to write a snapshot segment that will fail to update indexes.
        for (int j = 0; j < SEGMENT_SIZE; j++) {
            TxnID txnID = new TxnID(0, j);
            Position position = PositionFactory.create(0, j);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        //4. Wait writing segment completed.
        Awaitility.await().untilAsserted(() -> verifySnapshotSegmentsSize(PROCESSOR_TOPIC, 3));
        //5. Clear all the snapshot segments and indexes.
        try {
            processor.clearAbortedTxnSnapshot().get();
            //Failed to clear index due to the index writer is closed.
            Assert.fail();
        } catch (Exception ignored) {
        }
        //6. Do compaction and wait it completed.
        TopicName segmentTopicName  = NamespaceEventsSystemTopicFactory.getSystemTopicName(
                TopicName.get(PROCESSOR_TOPIC).getNamespaceObject(),
                EventType.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS);
        TopicName indexTopicName  = NamespaceEventsSystemTopicFactory.getSystemTopicName(
                TopicName.get(PROCESSOR_TOPIC).getNamespaceObject(),
                EventType.TRANSACTION_BUFFER_SNAPSHOT_INDEXES);
        doCompaction(segmentTopicName);
        doCompaction(indexTopicName);
        //7. Verify the snapshot segments and index after clearing.
        verifySnapshotSegmentsSize(PROCESSOR_TOPIC, 0);
        verifySnapshotSegmentsIndexSize(PROCESSOR_TOPIC, 1);
        processor.closeAsync().get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTxnSegmentStats() throws Exception {
        // Set up test environment
        String namespace = TENANT + "/testTxnSegmentStats";
        String topic = "persistent://" + namespace + "/testTxnSegmentStats";
        this.pulsarService.getConfig().setTransactionBufferSnapshotSegmentSize(8 + topic.length() + SEGMENT_SIZE * 3);

        // Create necessary resources
        Transactions transactions = admin.transactions();
        admin.namespaces().createNamespace(namespace);
        admin.topics().createNonPartitionedTopic(topic);

        // Prepare topic, producer, and consumer
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("my-sub").subscribe();

        // Record the start time of the test
        long testStartTime = System.currentTimeMillis();

        Transaction transaction = null;
        // Send messages with transactions and abort them
        for (int i = 0; i < SEGMENT_SIZE; i++) {
            transaction = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.HOURS).build().get();
            producer.newMessage(transaction).send();
            transaction.abort().get();
        }

        Transaction txn = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.HOURS).build().get();
        producer.newMessage(txn).send();
        txn.abort().get();

        // Get the transaction buffer stats without segment stats
        TransactionBufferStats statsWithoutSegmentStats = transactions
                .getTransactionBufferStats(topic, false, false);
        assertNotNull(statsWithoutSegmentStats);
        assertNotNull(statsWithoutSegmentStats.segmentsStats);
        assertNull(statsWithoutSegmentStats.segmentsStats.segmentStats);
        assertEquals(statsWithoutSegmentStats.snapshotType, AbortedTxnProcessor.SnapshotType.Segment.toString());

        // Verify the segment stats
        assertEquals(statsWithoutSegmentStats.segmentsStats.segmentsSize, 1L);
        assertEquals(statsWithoutSegmentStats.segmentsStats.unsealedAbortTxnIDSize, 1L);
        assertEquals(statsWithoutSegmentStats.segmentsStats.currentSegmentCapacity, SEGMENT_SIZE);
        assertEquals(statsWithoutSegmentStats.totalAbortedTransactions, SEGMENT_SIZE + 1);
        assertTrue(statsWithoutSegmentStats.segmentsStats.lastTookSnapshotSegmentTimestamp >= testStartTime);

        // Get the transaction buffer stats with segment stats
        TransactionBufferStats statsWithSegmentStats = transactions
                .getTransactionBufferStats(topic, false, true);
        assertNotNull(statsWithSegmentStats);
        assertNotNull(statsWithSegmentStats.segmentsStats.segmentStats);

        // Verify if the segment stats are present when requested
        assertEquals(statsWithSegmentStats.segmentsStats.segmentStats.size(), 1);
        assertEquals(statsWithSegmentStats.segmentsStats.segmentStats.get(0).lastTxnID,
                transaction.getTxnID().toString());

        // Verify multiple segments
        for (int i = 0; i < SEGMENT_SIZE * 3; i++) {
            transaction = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.HOURS).build().get();
            producer.newMessage(transaction).send();
            transaction.abort().get();
        }
        statsWithSegmentStats = transactions.getTransactionBufferStats(topic, false, true);

        // Verify the segment stats
        assertEquals(statsWithSegmentStats.segmentsStats.segmentsSize, 4L);
        assertEquals(statsWithSegmentStats.segmentsStats.unsealedAbortTxnIDSize, 1L);
        assertEquals(statsWithSegmentStats.totalAbortedTransactions, SEGMENT_SIZE * 4 + 1);

        // Reset the configuration
        this.pulsarService.getConfig()
                .setTransactionBufferSnapshotSegmentSize(8 + PROCESSOR_TOPIC.length() + SEGMENT_SIZE * 3);
    }

    private void verifySnapshotSegmentsSize(String topic, int size) throws Exception {
        SystemTopicClient.Reader<TransactionBufferSnapshotSegment> reader =
                pulsarService.getTransactionBufferSnapshotServiceFactory()
                        .getTxnBufferSnapshotSegmentService()
                        .createReader(TopicName.get(topic)).get();
        int segmentCount = 0;
        while (reader.hasMoreEvents()) {
            Message<TransactionBufferSnapshotSegment> message = reader.readNextAsync()
                    .get(5, TimeUnit.SECONDS);
            if (topic.equals(message.getValue().getTopicName())) {
                segmentCount++;
            }
        }
        assertEquals(segmentCount, size);
    }

    private void verifySnapshotSegmentsIndexSize(String topic, int size) throws Exception {
        SystemTopicClient.Reader<TransactionBufferSnapshotIndexes> reader =
                pulsarService.getTransactionBufferSnapshotServiceFactory()
                        .getTxnBufferSnapshotIndexService()
                        .createReader(TopicName.get(topic)).get();
        int indexCount = 0;
        while (reader.hasMoreEvents()) {
            Message<TransactionBufferSnapshotIndexes> message = reader.readNextAsync()
                    .get(5, TimeUnit.SECONDS);
            if (topic.equals(message.getValue().getTopicName())) {
                indexCount++;
            }
            System.out.printf("message.getValue().getTopicName() :" + message.getValue().getTopicName());
        }
        assertEquals(indexCount, size);
    }

    private void doCompaction(TopicName topic) throws Exception {
        PersistentTopic snapshotTopic = (PersistentTopic) pulsarService.getBrokerService()
                .getTopic(topic.toString(), false).get().get();
        Field field = PersistentTopic.class.getDeclaredField("currentCompaction");
        field.setAccessible(true);
        snapshotTopic.triggerCompaction();
        CompletableFuture<Long> compactionFuture = (CompletableFuture<Long>) field.get(snapshotTopic);
        org.awaitility.Awaitility.await().untilAsserted(() -> assertTrue(compactionFuture.isDone()));
    }

    /**
     * This test verifies the compatibility of the transaction buffer segmented snapshot feature
     * when enabled on an existing topic.
     * It performs the following steps:
     * 1. Creates a topic with segmented snapshot disabled.
     * 2. Sends 10 messages without using transactions.
     * 3. Sends 10 messages using transactions and aborts them.
     * 4. Sends 10 messages without using transactions.
     * 5. Verifies that only the non-transactional messages are received.
     * 6. Enables the segmented snapshot feature and sets the snapshot segment size.
     * 7. Unloads the topic.
     * 8. Sends a new message using a transaction and aborts it.
     * 9. Verifies that the topic has exactly one segment.
     * 10. Re-subscribes the consumer and re-verifies that only the non-transactional messages are received.
     */
    @Test
    public void testSnapshotProcessorUpgrade() throws Exception {
        String NAMESPACE2 = TENANT + "/ns2";
        admin.namespaces().createNamespace(NAMESPACE2);
        this.pulsarService = getPulsarServiceList().get(0);
        this.pulsarService.getConfig().setTransactionBufferSegmentedSnapshotEnabled(false);

        // Create a topic, send 10 messages without using transactions, and send 10 messages using transactions.
        // Abort these transactions and verify the data.
        final String topicName = "persistent://" + NAMESPACE2 + "/testSnapshotProcessorUpgrade";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("test-sub").subscribe();

        assertTrue(getSnapshotAbortedTxnProcessor(topicName) instanceof SingleSnapshotAbortedTxnProcessorImpl);
        // Send 10 messages without using transactions
        for (int i = 0; i < 10; i++) {
            producer.send(("test-message-" + i).getBytes());
        }

        // Send 10 messages using transactions and abort them
        for (int i = 0; i < 10; i++) {
            Transaction txn = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build().get();
            producer.newMessage(txn).value(("test-txn-message-" + i).getBytes()).sendAsync();
            txn.abort().get();
        }

        // Send 10 messages without using transactions
        for (int i = 10; i < 20; i++) {
            producer.send(("test-message-" + i).getBytes());
        }

        // Verify the data
        for (int i = 0; i < 20; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertEquals("test-message-" + i, new String(msg.getData()));
        }

        // Enable segmented snapshot
        this.pulsarService.getConfig().setTransactionBufferSegmentedSnapshotEnabled(true);
        this.pulsarService.getConfig().setTransactionBufferSnapshotSegmentSize(8 + PROCESSOR_TOPIC.length() +
                SEGMENT_SIZE * 3);

        // Unload the topic
        admin.topics().unload(topicName);
        assertTrue(getSnapshotAbortedTxnProcessor(topicName) instanceof SnapshotSegmentAbortedTxnProcessorImpl);

        // Sends a new message using a transaction and aborts it.
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        producer.newMessage(txn).value("test-message-new".getBytes()).send();
        txn.abort().get();

        // Verifies that the topic has exactly one segment.
        Awaitility.await().untilAsserted(() -> {
            String segmentTopic = "persistent://" + NAMESPACE2 + "/" +
                    SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS;
            TopicStats topicStats = admin.topics().getStats(segmentTopic);
            assertEquals(1, topicStats.getMsgInCounter());
        });

        // Re-subscribes the consumer and re-verifies that only the non-transactional messages are received.
        consumer.close();
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("test-sub").subscribe();
        for (int i = 0; i < 20; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertEquals("test-message-" + i, new String(msg.getData()));
        }
    }

    /**
     * This test verifies that when the segmented snapshot feature is enabled, creating a new topic
     * does not create a __transaction_buffer_snapshot topic in the same namespace.
     * The test performs the following steps:
     * 1. Enable the segmented snapshot feature.
     * 2. Create a new namespace.
     * 3. Create a new topic in the namespace.
     * 4. Check that the __transaction_buffer_snapshot topic is not created in the same namespace.
     * 5. Destroy the namespace after the test.
     */
    @Test
    public void testSegmentedSnapshotWithoutCreatingOldSnapshotTopic() throws Exception {
        // Enable the segmented snapshot feature
        pulsarService = getPulsarServiceList().get(0);
        pulsarService.getConfig().setTransactionBufferSegmentedSnapshotEnabled(true);

        // Create a new namespace
        String namespaceName = "tnx/testSegmentedSnapshotWithoutCreatingOldSnapshotTopic";
        admin.namespaces().createNamespace(namespaceName);

        // Create a new topic in the namespace
        String topicName = "persistent://" + namespaceName + "/newTopic";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();
        assertTrue(getSnapshotAbortedTxnProcessor(topicName) instanceof SnapshotSegmentAbortedTxnProcessorImpl);
        // Check that the __transaction_buffer_snapshot topic is not created in the same namespace
        String transactionBufferSnapshotTopic = "persistent://" + namespaceName + "/" +
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT;
        try {
            admin.topics().getStats(transactionBufferSnapshotTopic);
            fail("The __transaction_buffer_snapshot topic should not exist");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 404);
        }

        // Destroy the namespace after the test
        admin.namespaces().deleteNamespace(namespaceName, true);
    }

    private AbortedTxnProcessor getSnapshotAbortedTxnProcessor(String topicName) {
        PersistentTopic persistentTopic = getPersistentTopic(topicName);
        return WhiteboxImpl.getInternalState(persistentTopic.getTransactionBuffer(), "snapshotAbortedTxnProcessor");
    }

    private PersistentTopic getPersistentTopic(String topicName) {
        for (PulsarService pulsar : getPulsarServiceList()) {
            CompletableFuture<Optional<Topic>> future =
                    pulsar.getBrokerService().getTopic(topicName, false);
            if (future == null) {
                continue;
            }
            return (PersistentTopic) future.join().get();
        }
        throw new NullPointerException("topic[" + topicName +  "] not found");
    }
}
