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
package org.apache.pulsar.transaction.coordinator.impl;

import com.google.common.collect.ComparisonChain;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.apache.pulsar.transaction.coordinator.test.MockedBookKeeperTestCase;
import static org.apache.pulsar.transaction.coordinator.impl.DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;
import static org.mockito.Mockito.*;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MLTransactionLogImplTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "variedBufferedWriteConfigProvider")
    private Object[][] variedBufferedWriteConfigProvider(){
        return new Object[][]{
                {true, true},
                {false, false},
                {true, false},
                {false, true}
        };
    }
    /**
     * 1. Add some transaction logs.
     * 2. Create a new transaction meta store and execute recover, assert that the txn-mapping built by Recover is as
     *    expected. This validates the read and write correct.
     * 3. Commit some transaction and add another transaction logs.
     * 4. Create another transaction meta store and execute recover, assert that the cursor-mark-deleted-position and
     *    the cursor-batch-indexes is expected. This validates delete correct, assert that
     */
    @Test(dataProvider = "variedBufferedWriteConfigProvider")
    public void testMainProcess(boolean writeWithBatch, boolean readWithBatch) throws Exception {
        HashedWheelTimer transactionTimer = new HashedWheelTimer(new DefaultThreadFactory("transaction-timer"),
                1, TimeUnit.MILLISECONDS);
        TxnLogBufferedWriterConfig bufferedWriterConfigForWrite = new TxnLogBufferedWriterConfig();
        bufferedWriterConfigForWrite.setBatchedWriteMaxDelayInMillis(1000 * 3600);
        bufferedWriterConfigForWrite.setBatchedWriteMaxRecords(3);
        bufferedWriterConfigForWrite.setBatchEnabled(writeWithBatch);
        TransactionCoordinatorID transactionCoordinatorID = TransactionCoordinatorID.get(0);
        MLTransactionLogImpl mlTransactionLogForWrite = new MLTransactionLogImpl(TransactionCoordinatorID.get(0), factory,
                new ManagedLedgerConfig(), bufferedWriterConfigForWrite, transactionTimer, DISABLED_BUFFERED_WRITER_METRICS);
        mlTransactionLogForWrite.initialize().get(3, TimeUnit.SECONDS);
        Map<Integer, List<CompletableFuture<Position>>> expectedMapping = new HashMap<>();
        /**
         * 1. Start 20 transactions, these will eventually be committed.
         *    1-1. new transactions.
         *    1-2. add partition.
         *    1-3. add subscribe.
         * 2. Start 30 transactions, these transactions will never commit, to validate the delete-logic.
         */
        // Add logs: start transaction.
        for (int i = 1; i <= 20; i++){
            TransactionMetadataEntry transactionLog = new TransactionMetadataEntry();
            transactionLog.setTxnidMostBits(i);
            transactionLog.setTxnidLeastBits(i);
            transactionLog.setMaxLocalTxnId(i);
            transactionLog.setStartTime(i);
            transactionLog.setTimeoutMs(i);
            transactionLog.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.NEW);
            CompletableFuture<Position> future = mlTransactionLogForWrite.append(transactionLog);
            expectedMapping.computeIfAbsent(i, k -> new ArrayList<>());
            expectedMapping.get(i).add(future);
        }
        // Add logs: add partition.
        for (int i = 1; i <= 20; i++){
            TransactionMetadataEntry transactionLog = new TransactionMetadataEntry();
            transactionLog.setTxnidLeastBits(i);
            transactionLog.setMaxLocalTxnId(i);
            transactionLog.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.ADD_PARTITION);
            transactionLog.addAllPartitions(Arrays.asList(String.valueOf(i)));
            CompletableFuture<Position> future = mlTransactionLogForWrite.append(transactionLog);
            expectedMapping.computeIfAbsent(i, k -> new ArrayList<>());
            expectedMapping.get(i).add(future);
        }
        // Add logs: add subscription.
        for (int i = 1; i <= 20; i++){
            TransactionMetadataEntry transactionLog = new TransactionMetadataEntry();
            transactionLog.setTxnidLeastBits(i);
            transactionLog.setMaxLocalTxnId(i);
            transactionLog.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.ADD_SUBSCRIPTION);
            Subscription subscription = new Subscription();
            subscription.setSubscription(String.valueOf(i));
            subscription.setTopic(String.valueOf(i));
            transactionLog.addAllSubscriptions(Arrays.asList(subscription));
            CompletableFuture<Position> future = mlTransactionLogForWrite.append(transactionLog);
            expectedMapping.computeIfAbsent(i, k -> new ArrayList<>());
            expectedMapping.get(i).add(future);
        }
        // Add logs: new transactions. These transactions will never commit, to validate the logic of transaction log.
        for (int i = 21; i <= 50; i++){
            TransactionMetadataEntry transactionLog = new TransactionMetadataEntry();
            transactionLog.setTxnidMostBits(i);
            transactionLog.setTxnidLeastBits(i);
            transactionLog.setMaxLocalTxnId(i);
            transactionLog.setStartTime(i);
            transactionLog.setTimeoutMs(i);
            transactionLog.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.NEW);
            CompletableFuture<Position> future = mlTransactionLogForWrite.append(transactionLog);
            expectedMapping.computeIfAbsent(i, k -> new ArrayList<>());
            expectedMapping.get(i).add(future);
        }
        // Waiting all future completed.
        FutureUtil.waitForAll(expectedMapping.values().stream()
                .flatMap(l -> l.stream()).collect(Collectors.toList()))
                .get(2, TimeUnit.SECONDS);
        /**
         * Create a new transaction meta store and execute recover to verify that the txn-mapping built by Recover is as
         * expected. This validates the read and write correct.
         */
        // Create another transaction log for recover.
        TxnLogBufferedWriterConfig bufferedWriterConfigForRecover = new TxnLogBufferedWriterConfig();
        bufferedWriterConfigForRecover.setBatchedWriteMaxDelayInMillis(1000 * 3600);
        bufferedWriterConfigForRecover.setBatchedWriteMaxRecords(3);
        bufferedWriterConfigForRecover.setBatchEnabled(readWithBatch);
        MLTransactionLogImpl mlTransactionLogForRecover = new MLTransactionLogImpl(TransactionCoordinatorID.get(0),
                factory, new ManagedLedgerConfig(), bufferedWriterConfigForRecover, transactionTimer,
                DISABLED_BUFFERED_WRITER_METRICS);
        mlTransactionLogForRecover.initialize().get(3, TimeUnit.SECONDS);
        // Recover and verify the txnID and position mappings.
        TransactionTimeoutTracker timeoutTracker = mock(TransactionTimeoutTracker.class);
        MLTransactionSequenceIdGenerator sequenceIdGenerator = mock(MLTransactionSequenceIdGenerator.class);
        TransactionRecoverTracker recoverTracker = mock(TransactionRecoverTracker.class);
        MLTransactionMetadataStore transactionMetadataStoreForRecover = new MLTransactionMetadataStore(transactionCoordinatorID,
                mlTransactionLogForRecover, timeoutTracker, sequenceIdGenerator, Integer.MAX_VALUE);
        transactionMetadataStoreForRecover.init(recoverTracker).get(2000, TimeUnit.SECONDS);
        Assert.assertEquals(transactionMetadataStoreForRecover.txnMetaMap.size(), expectedMapping.size());
        Iterator<Integer> txnIdSet = expectedMapping.keySet().iterator();
        while (txnIdSet.hasNext()){
            int txnId = txnIdSet.next();
            List<CompletableFuture<Position>> expectedPositions = expectedMapping.get(txnId);
            List<Position> actualPositions = transactionMetadataStoreForRecover.txnMetaMap.get(Long.valueOf(txnId)).getRight();
            Assert.assertEquals(actualPositions.size(), expectedPositions.size());
            for (int i = 0; i< expectedPositions.size(); i++){
                Position expectedPosition = expectedPositions.get(i).get(1, TimeUnit.SECONDS);
                Position actualPosition = actualPositions.get(i);
                Assert.assertEquals(actualPosition, expectedPosition);
            }
        }
        /**
         * 1. Commit transactions that create at step-1.
         * 2. Start another 20 transactions, these transactions will never commit, to validate the delete-logic.
         */
        // Add logs: committing.
        for (int i = 1; i <= 20; i++){
            TransactionMetadataEntry transactionLog = new TransactionMetadataEntry();
            transactionLog.setTxnidLeastBits(i);
            transactionLog.setMaxLocalTxnId(i);
            transactionLog.setExpectedStatus(TxnStatus.OPEN);
            transactionLog.setNewStatus(TxnStatus.COMMITTING);
            transactionLog.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.UPDATE);
            CompletableFuture<Position> future = mlTransactionLogForWrite.append(transactionLog);
            expectedMapping.computeIfAbsent(i, k -> new ArrayList<>());
            expectedMapping.get(i).add(future);
        }
        // Add logs: committed.
        for (int i = 1; i <= 20; i++){
            TransactionMetadataEntry transactionLog = new TransactionMetadataEntry();
            transactionLog.setTxnidLeastBits(i);
            transactionLog.setMaxLocalTxnId(i);
            transactionLog.setExpectedStatus(TxnStatus.COMMITTING);
            transactionLog.setNewStatus(TxnStatus.COMMITTED);
            transactionLog.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.UPDATE);
            CompletableFuture<Position> future = mlTransactionLogForWrite.append(transactionLog);
            expectedMapping.computeIfAbsent(i, k -> new ArrayList<>());
            expectedMapping.get(i).add(future);
        }
        // Add logs: new transaction.
        for (int i = 51; i <= 70; i++){
            TransactionMetadataEntry transactionLog = new TransactionMetadataEntry();
            transactionLog.setTxnidMostBits(i);
            transactionLog.setTxnidLeastBits(i);
            transactionLog.setMaxLocalTxnId(i);
            transactionLog.setStartTime(i);
            transactionLog.setTimeoutMs(i);
            transactionLog.setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.NEW);
            CompletableFuture<Position> future = mlTransactionLogForWrite.append(transactionLog);
            expectedMapping.computeIfAbsent(i, k -> new ArrayList<>());
            expectedMapping.get(i).add(future);
        }
        // Waiting all callback.
        FutureUtil.waitForAll(expectedMapping.values().stream()
                        .flatMap(l -> l.stream()).collect(Collectors.toList()))
                .get(2, TimeUnit.SECONDS);
        // rewind the cursor
        ManagedCursorImpl managedCursorForRecover =
                (ManagedCursorImpl)transactionMetadataStoreForRecover.getManagedLedger().getCursors().iterator().next();
        /** Rewind the cursor for next-step. **/
        managedCursorForRecover.rewind();
        /**
         * Create another transaction meta store and execute recover, assert that the cursor-mark-deleted-position and
         * the cursor-batch-indexes is expected.
         */
        // Create another transaction log for recover.
        MLTransactionLogImpl mlTransactionLogForDelete = new MLTransactionLogImpl(TransactionCoordinatorID.get(0),
                factory, new ManagedLedgerConfig(), bufferedWriterConfigForRecover, transactionTimer,
                DISABLED_BUFFERED_WRITER_METRICS);
        mlTransactionLogForDelete.initialize().get(3, TimeUnit.SECONDS);
        MLTransactionMetadataStore transactionMetadataStoreForDelete = new MLTransactionMetadataStore(transactionCoordinatorID,
                mlTransactionLogForDelete, timeoutTracker, sequenceIdGenerator, Integer.MAX_VALUE);
        transactionMetadataStoreForDelete.init(recoverTracker).get(2000, TimeUnit.SECONDS);
        ManagedCursorImpl managedCursor =
                (ManagedCursorImpl)mlTransactionLogForDelete.getManagedLedger().getCursors().iterator().next();
        // Calculate expected deleted positions.
        List<Position> expectedDeletedPositions = new ArrayList<>();
        for (int i = 1; i <= 20; i++){
            expectedDeletedPositions.addAll(
                    expectedMapping.remove(i).stream()
                            .map(f -> f.join())
                            .collect(Collectors.toList()));
        }
        expectedDeletedPositions = expectedDeletedPositions.stream().sorted((o1,o2) -> {
            if (o1 instanceof TxnBatchedPositionImpl){
                TxnBatchedPositionImpl t1 = (TxnBatchedPositionImpl) o1;
                TxnBatchedPositionImpl t2 = (TxnBatchedPositionImpl) o2;
                return ComparisonChain.start()
                        .compare(o1.getLedgerId(), o2.getLedgerId())
                        .compare(o1.getEntryId(), o2.getEntryId())
                        .compare(t1.getBatchIndex(), t2.getBatchIndex())
                        .result();
            }else {
                return ComparisonChain.start()
                        .compare(o1.getLedgerId(), o2.getLedgerId())
                        .compare(o1.getEntryId(), o2.getEntryId())
                        .result();
            }
        }).collect(Collectors.toList());
        PositionImpl markDeletedPosition = null;
        LinkedHashMap<PositionImpl, BitSetRecyclable> batchIndexes = null;
        if (expectedDeletedPositions.get(0) instanceof TxnBatchedPositionImpl){
            Pair<PositionImpl, LinkedHashMap<PositionImpl, BitSetRecyclable>> pair =
                    calculateBatchIndexes(
                            expectedDeletedPositions.stream()
                                    .map(p -> (TxnBatchedPositionImpl)p)
                                    .collect(Collectors.toList())
                    );
            markDeletedPosition = pair.getLeft();
            batchIndexes = pair.getRight();
        } else {
            markDeletedPosition = calculateMarkDeletedPosition(expectedDeletedPositions);
        }
        final PositionImpl markDeletedPosition_final = markDeletedPosition;
        // Assert mark deleted position correct.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
            Position actualMarkDeletedPosition = managedCursor.getMarkDeletedPosition();
            return markDeletedPosition_final.getLedgerId() == actualMarkDeletedPosition.getLedgerId() &&
                    markDeletedPosition_final.getEntryId() == actualMarkDeletedPosition.getEntryId();
        });
        // Assert batchIndexes correct.
        if (batchIndexes != null){
            // calculate last deleted position.
            Map.Entry<PositionImpl, BitSetRecyclable>
                    lastOne = batchIndexes.entrySet().stream().reduce((a, b) -> b).get();
            // Wait last one has been deleted from cursor.
            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
                long[] ls = managedCursor.getBatchPositionAckSet(lastOne.getKey());
                return Arrays.equals(lastOne.getValue().toLongArray(), ls);
            });
            // Verify batch indexes.
            for (Map.Entry<PositionImpl, BitSetRecyclable> entry : batchIndexes.entrySet()){
                PositionImpl p = entry.getKey();
                long[] actualAckSet = managedCursor.getBatchPositionAckSet(p);
                Assert.assertEquals(actualAckSet, entry.getValue().toLongArray());
                entry.getValue().recycle();
            }
        }
        /** cleanup. **/
        mlTransactionLogForWrite.closeAsync().get(2, TimeUnit.SECONDS);
        mlTransactionLogForRecover.closeAsync().get(2, TimeUnit.SECONDS);
        mlTransactionLogForDelete.closeAsync().get(2, TimeUnit.SECONDS);
        transactionMetadataStoreForRecover.closeAsync().get(2, TimeUnit.SECONDS);
        transactionMetadataStoreForDelete.closeAsync().get(2, TimeUnit.SECONDS);
        transactionTimer.stop();
    }

    /***
     * Calculate markDeletedPosition by {@param sortedDeletedPositions}.
     */
    private PositionImpl calculateMarkDeletedPosition(Collection<Position> sortedDeletedPositions){
        Position markDeletedPosition = null;
        for (Position position : sortedDeletedPositions){
            if (markDeletedPosition == null){
                markDeletedPosition = position;
                continue;
            }
            // Ledger are never closed, so all positions has same ledger-id.
            if (markDeletedPosition.getEntryId() == position.getEntryId() - 1){
                markDeletedPosition = position;
                continue;
            } else {
                break;
            }
        }
        if (markDeletedPosition == null) {
            return null;
        }
        return PositionImpl.get(markDeletedPosition.getLedgerId(), markDeletedPosition.getEntryId());
    }

    /***
     * Calculate markDeletedPosition and batchIndexes by {@param sortedDeletedPositions}.
     */
    private Pair<PositionImpl, LinkedHashMap<PositionImpl, BitSetRecyclable>> calculateBatchIndexes(
            List<TxnBatchedPositionImpl> sortedDeletedPositions){
        // build batchIndexes.
        LinkedHashMap<PositionImpl, BitSetRecyclable> batchIndexes = new LinkedHashMap<>();
        for (TxnBatchedPositionImpl batchedPosition : sortedDeletedPositions){
            batchedPosition.setAckSetByIndex();
            PositionImpl k = PositionImpl.get(batchedPosition.getLedgerId(), batchedPosition.getEntryId());
            BitSetRecyclable bitSetRecyclable = batchIndexes.get(k);
            if (bitSetRecyclable == null){
                bitSetRecyclable = BitSetRecyclable.valueOf(batchedPosition.getAckSet());
                batchIndexes.put(k, bitSetRecyclable);
            }
            bitSetRecyclable.clear(batchedPosition.getBatchIndex());
        }
        // calculate markDeletedPosition.
        Position markDeletedPosition = null;
        for (Map.Entry<PositionImpl, BitSetRecyclable> entry : batchIndexes.entrySet()){
            PositionImpl position = entry.getKey();
            BitSetRecyclable bitSetRecyclable = entry.getValue();
            if (!bitSetRecyclable.isEmpty()){
                break;
            }
            if (markDeletedPosition == null){
                markDeletedPosition = position;
                continue;
            }
            // Ledger are never closed, so all positions has same ledger-id.
            if (markDeletedPosition.getEntryId() == position.getEntryId() - 1){
                markDeletedPosition = position;
                continue;
            } else {
                break;
            }
        }
        // remove empty bitSet.
        List<Position> shouldRemoveFromMap = new ArrayList<>();
        for (Map.Entry<PositionImpl, BitSetRecyclable> entry : batchIndexes.entrySet()) {
            BitSetRecyclable bitSetRecyclable = entry.getValue();
            if (bitSetRecyclable.isEmpty()) {
                shouldRemoveFromMap.add(entry.getKey());
            }
        }
        for (Position position : shouldRemoveFromMap){
            BitSetRecyclable bitSetRecyclable = batchIndexes.remove(position);
            bitSetRecyclable.recycle();
        }
        return Pair.of(PositionImpl.get(markDeletedPosition.getLedgerId(), markDeletedPosition.getEntryId()),
                batchIndexes);
    }
}