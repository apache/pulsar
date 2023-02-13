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

import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.impl.SnapshotSegmentAbortedTxnProcessorImpl;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicName;
import org.testcontainers.shaded.org.awaitility.Awaitility;
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
        setUpBase(1, 1, PROCESSOR_TOPIC, 0);
        this.pulsarService = getPulsarServiceList().get(0);
        this.pulsarService.getConfig().setTransactionBufferSegmentedSnapshotEnabled(true);
        this.pulsarService.getConfig().setTransactionBufferSnapshotSegmentSize(8 + PROCESSOR_TOPIC.length() +
                SEGMENT_SIZE * 3);
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
            PositionImpl position = new PositionImpl(0, i);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        //1.2 Put 4 aborted txn IDs into the unsealed segment.
        for (int i = 10; i < 14; i++) {
            TxnID txnID = new TxnID(0, i);
            PositionImpl position = new PositionImpl(0, i);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        //1.3 Verify the common data flow
        verifyAbortedTxnIDAndSegmentIndex(processor, 0, 14);
        //2. Take the latest snapshot and verify recover from snapshot
        AbortedTxnProcessor newProcessor = new SnapshotSegmentAbortedTxnProcessorImpl(persistentTopic);
        PositionImpl maxReadPosition = new PositionImpl(0, 14);
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
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(queue.size(), 0));
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
        LinkedMap<PositionImpl, TxnID> indexes = (LinkedMap<PositionImpl, TxnID>) indexField.get(processor);
        Assert.assertEquals(unsealedSegment.size(), txnIdSize % SEGMENT_SIZE);
        Assert.assertEquals(indexes.size(), txnIdSize / SEGMENT_SIZE);
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
            processor.takeAbortedTxnsSnapshot(new PositionImpl(1, 10)).get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException);
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
            PositionImpl position = new PositionImpl(0, j);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        Awaitility.await().untilAsserted(() -> verifySnapshotSegmentsSize(PROCESSOR_TOPIC, 2));
        //2. Close index writer, making the index can not be updated.
        Field field = SnapshotSegmentAbortedTxnProcessorImpl.class.getDeclaredField("persistentWorker");
        field.setAccessible(true);
        SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker worker =
                (SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker) field.get(processor);
        Field indexWriteFutureField = SnapshotSegmentAbortedTxnProcessorImpl
                .PersistentWorker.class.getDeclaredField("snapshotIndexWriterFuture");
        indexWriteFutureField.setAccessible(true);
        CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotIndexes>> snapshotIndexWriterFuture =
                (CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotIndexes>>)
                        indexWriteFutureField.get(worker);
        snapshotIndexWriterFuture.get().close();
        //3. Try to write a snapshot segment that will fail to update indexes.
        for (int j = 0; j < SEGMENT_SIZE; j++) {
            TxnID txnID = new TxnID(0, j);
            PositionImpl position = new PositionImpl(0, j);
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
        Assert.assertEquals(segmentCount, size);
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
        Assert.assertEquals(indexCount, size);
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
}
