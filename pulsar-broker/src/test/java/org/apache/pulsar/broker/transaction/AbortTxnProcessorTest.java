package org.apache.pulsar.broker.transaction;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.impl.SnapshotSegmentAbortedTxnProcessorImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class AbortTxnProcessorTest extends TransactionTestBase {

    private static final String PROCESSOR_TOPIC = "persistent://" + NAMESPACE1 + "/abortedTxnProcessor";
    private static final int SEGMENT_SIZE = 5;
    private PulsarService pulsarService = null;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        setUpBase(1, 1, PROCESSOR_TOPIC, 0);
        this.pulsarService = getPulsarServiceList().get(0);
        this.pulsarService.getConfig().setTransactionBufferSegmentedSnapshotEnabled(true);
        this.pulsarService.getConfig().setTransactionBufferSnapshotSegmentSize(8 + PROCESSOR_TOPIC.length() + 5 * 3);
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
            PositionImpl position = new PositionImpl(0,i);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        //1.2 Put 4 aborted txn IDs into the unsealed segment.
        for (int i = 10; i < 14; i++) {
            TxnID txnID = new TxnID(0, i);
            PositionImpl position = new PositionImpl(0,i);
            processor.putAbortedTxnAndPosition(txnID, position);
        }
        //1.3 Verify the common data flow
        verifyAbortedTxnIDAndSegmentIndex(processor,0,14);
        //2. Take the latest snapshot and verify recover from snapshot
        AbortedTxnProcessor newProcessor = new SnapshotSegmentAbortedTxnProcessorImpl(persistentTopic);
        PositionImpl maxReadPosition = new PositionImpl(0, 14);
        //2.1 Avoid update operation being canceled.
        waitTaskExecuteCompletely(processor);
        //2.2 take the latest snapshot
        processor.takeAbortedTxnsSnapshot(maxReadPosition).get();
        newProcessor.recoverFromSnapshot().get();
        //Verify the recovery data flow
        verifyAbortedTxnIDAndSegmentIndex(newProcessor,0,14);
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
    public void testFuturesCanCompleteWithException() throws Exception {
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
        Queue queue = (Queue) taskQueueField.get(persistentWorker);
        queue.add(new Object());
        processor.takeAbortedTxnsSnapshot(new PositionImpl(1, 10)).get(2, TimeUnit.SECONDS);
    }
}
