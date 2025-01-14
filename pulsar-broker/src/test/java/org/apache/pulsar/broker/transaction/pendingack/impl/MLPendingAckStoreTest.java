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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.util.LogIndexLagBackoff;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterConfig;
import static org.mockito.Mockito.*;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MLPendingAckStoreTest extends TransactionTestBase {

    private PersistentSubscription persistentSubscriptionMock;

    private ManagedCursor managedCursorMock;

    private ExecutorService internalPinnedExecutor;

    private int pendingAckLogIndexMinLag = 1;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        setUpBase(1, 1, NAMESPACE1 + "/test", 0);
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        String topic = NAMESPACE1 + "/test-txn-topic";
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(topic, false).get().get();
        getPulsarServiceList().get(0).getConfig().setTransactionPendingAckLogIndexMinLag(pendingAckLogIndexMinLag);
        CompletableFuture<Subscription> subscriptionFuture = persistentTopic .createSubscription("test",
                CommandSubscribe.InitialPosition.Earliest, false, null);
        PersistentSubscription subscription = (PersistentSubscription) subscriptionFuture.get();
        ManagedCursor managedCursor = subscription.getCursor();
        this.managedCursorMock = spy(managedCursor);
        this.persistentSubscriptionMock = spy(subscription);
        when(this.persistentSubscriptionMock.getCursor()).thenReturn(managedCursorMock);
        this.internalPinnedExecutor = this.persistentSubscriptionMock
                .getTopic()
                .getBrokerService()
                .getPulsar()
                .getTransactionExecutorProvider()
                .getExecutor(this);
    }

    @AfterMethod(alwaysRun = true)
    private void afterMethod() throws Exception {
        ServiceConfiguration defaultConfig = new ServiceConfiguration();
        ServiceConfiguration serviceConfiguration =
                persistentSubscriptionMock.getTopic().getBrokerService().getPulsar().getConfiguration();
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxRecords(
                defaultConfig.getTransactionPendingAckBatchedWriteMaxRecords()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxSize(
                defaultConfig.getTransactionPendingAckBatchedWriteMaxSize()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxDelayInMillis(
                defaultConfig.getTransactionPendingAckBatchedWriteMaxDelayInMillis()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteEnabled(defaultConfig.isTransactionPendingAckBatchedWriteEnabled());
        admin.topics().delete("persistent://" + NAMESPACE1 + "/test-txn-topic", true);
    }

    @AfterClass
    public void cleanup(){
        super.internalCleanup();
    }

    private MLPendingAckStore createPendingAckStore(TxnLogBufferedWriterConfig txnLogBufferedWriterConfig)
            throws Exception {
        MLPendingAckStoreProvider mlPendingAckStoreProvider = new MLPendingAckStoreProvider();
        ServiceConfiguration serviceConfiguration =
                persistentSubscriptionMock.getTopic().getBrokerService().getPulsar().getConfiguration();
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxRecords(
                txnLogBufferedWriterConfig.getBatchedWriteMaxRecords()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxSize(
                txnLogBufferedWriterConfig.getBatchedWriteMaxSize()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxDelayInMillis(
                txnLogBufferedWriterConfig.getBatchedWriteMaxDelayInMillis()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteEnabled(txnLogBufferedWriterConfig.isBatchEnabled());
        return (MLPendingAckStore) mlPendingAckStoreProvider.newPendingAckStore(persistentSubscriptionMock).get();
    }

    /**
     * Overridden cases:
     *   1. Batched write and replay with batched feature.
     *   1. Non-batched write and replay without batched feature
     *   1. Batched write and replay without batched feature.
     *   1. Non-batched write and replay with batched feature.
     */
    @DataProvider(name = "mainProcessArgs")
    public Object[][] mainProcessArgsProvider(){
        Object[][] args = new Object[4][];
        args[0] = new Object[]{true, true};
        args[1] = new Object[]{false, false};
        args[2] = new Object[]{true, false};
        args[3] = new Object[]{false, true};
        return args;
    }

    /**
     * This method executed the following steps of validation:
     *   1. Write some data, verify indexes build correct after write.
     *   2. Replay data that has been written, verify indexes build correct after replay.
     *   3. Verify that position deletion is in sync with {@link PersistentSubscription}.
     * @param writeWithBatch Whether to enable batch feature when writing data.
     * @param readWithBatch Whether to enable batch feature when replay.
     */
    @Test(dataProvider = "mainProcessArgs")
    public void testMainProcess(boolean writeWithBatch, boolean readWithBatch) throws Exception {
        // Write some data.
        TxnLogBufferedWriterConfig configForWrite = new TxnLogBufferedWriterConfig();
        configForWrite.setBatchEnabled(writeWithBatch);
        configForWrite.setBatchedWriteMaxRecords(2);
        // Denied scheduled flush.
        configForWrite.setBatchedWriteMaxDelayInMillis(1000 * 3600);
        MLPendingAckStore mlPendingAckStoreForWrite = createPendingAckStore(configForWrite);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (int i = 0; i < 20; i++){
            TxnID txnID = new TxnID(i, i);
            PositionImpl position = PositionImpl.get(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendCumulativeAck(txnID, position));
        }
        for (int i = 0; i < 10; i++){
            TxnID txnID = new TxnID(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendCommitMark(txnID, CommandAck.AckType.Cumulative));
        }
        for (int i = 10; i < 20; i++){
            TxnID txnID = new TxnID(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendAbortMark(txnID, CommandAck.AckType.Cumulative));
        }
        for (int i = 40; i < 50; i++){
            TxnID txnID = new TxnID(i, i);
            PositionImpl position = PositionImpl.get(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendCumulativeAck(txnID, position));
        }
        FutureUtil.waitForAll(futureList).get();
        // Verify build sparse indexes correct after add many cmd-ack.
        ArrayList<Long> positionList = new ArrayList<>();
        for (long i = 0; i < 50; i++){
            positionList.add(i);
        }
        // The indexes not contains the data which is commit or abort.
        LinkedHashSet<Long> skipSet = new LinkedHashSet<>();
        for (long i = 20; i < 40; i++){
            skipSet.add(i);
        }
        if (writeWithBatch) {
            for (long i = 0; i < 50; i++){
                if (i % 2 == 0){
                    // The indexes contains only the last position in the batch.
                    skipSet.add(i);
                }
            }
        }
        LinkedHashSet<Long> expectedPositions = calculatePendingAckIndexes(positionList, skipSet);
        Assert.assertEquals(
                mlPendingAckStoreForWrite.pendingAckLogIndex.keySet().stream()
                        .map(PositionImpl::getEntryId).collect(Collectors.toList()),
                new ArrayList<>(expectedPositions)
        );
        // Replay.
        TxnLogBufferedWriterConfig configForReplay = new TxnLogBufferedWriterConfig();
        configForReplay.setBatchEnabled(readWithBatch);
        configForReplay.setBatchedWriteMaxRecords(2);
        // Denied scheduled flush.
        configForReplay.setBatchedWriteMaxDelayInMillis(1000 * 3600);
        MLPendingAckStore mlPendingAckStoreForRead = createPendingAckStore(configForReplay);
        PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
        when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(internalPinnedExecutor);
        when(pendingAckHandle.changeToReadyState()).thenReturn(true);
        // Process controller, mark the replay task already finish.
        final AtomicInteger processController = new AtomicInteger();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                processController.incrementAndGet();
                return null;
            }
        }).when(pendingAckHandle).completeHandleFuture();
        mlPendingAckStoreForRead.replayAsync(pendingAckHandle, internalPinnedExecutor);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> processController.get() == 1);
        // Verify build sparse indexes correct after replay.
        Assert.assertEquals(mlPendingAckStoreForRead.pendingAckLogIndex.size(),
                mlPendingAckStoreForWrite.pendingAckLogIndex.size());
        Iterator<Map.Entry<PositionImpl, PositionImpl>> iteratorReplay =
                mlPendingAckStoreForRead.pendingAckLogIndex.entrySet().iterator();
        Iterator<Map.Entry<PositionImpl, PositionImpl>> iteratorWrite =
                mlPendingAckStoreForWrite.pendingAckLogIndex.entrySet().iterator();
        while (iteratorReplay.hasNext()){
            Map.Entry<PositionImpl, PositionImpl> replayEntry = iteratorReplay.next();
            Map.Entry<PositionImpl, PositionImpl> writeEntry =  iteratorWrite.next();
            Assert.assertEquals(replayEntry.getKey(), writeEntry.getKey());
            Assert.assertEquals(replayEntry.getValue().getLedgerId(), writeEntry.getValue().getLedgerId());
            Assert.assertEquals(replayEntry.getValue().getEntryId(), writeEntry.getValue().getEntryId());
        }
        // Verify delete correct.
        when(managedCursorMock.getPersistentMarkDeletedPosition()).thenReturn(PositionImpl.get(19, 19));
        mlPendingAckStoreForWrite.clearUselessLogData();
        mlPendingAckStoreForRead.clearUselessLogData();
        Assert.assertTrue(mlPendingAckStoreForWrite.pendingAckLogIndex.keySet().iterator().next().getEntryId() > 19);
        Assert.assertTrue(mlPendingAckStoreForRead.pendingAckLogIndex.keySet().iterator().next().getEntryId() > 19);

        // cleanup.
        closePendingAckStoreWithRetry(mlPendingAckStoreForWrite);
        closePendingAckStoreWithRetry(mlPendingAckStoreForRead);
    }

    /**
     * Why should retry?
     * Because when the cursor close and cursor switch ledger are concurrent executing, the bad version exception is
     * thrown.
     */
    private void closePendingAckStoreWithRetry(MLPendingAckStore pendingAckStore){
        Awaitility.await().until(() -> {
            try {
                pendingAckStore.closeAsync().get();
                return true;
            } catch (Exception ex){
                return false;
            }
        });
    }

    /**
     * Build a sparse index from the {@param positionList}, the logic same as {@link MLPendingAckStore}.
     * @param positionList the position add to pending ack log/
     * @param skipSet the position which should increment the count but not marked to indexes. aka: commit & abort.
     */
    private LinkedHashSet<Long> calculatePendingAckIndexes(List<Long> positionList, LinkedHashSet<Long> skipSet){
        LogIndexLagBackoff logIndexLagBackoff = new LogIndexLagBackoff(pendingAckLogIndexMinLag, Long.MAX_VALUE, 1);
        long nextCount = logIndexLagBackoff.next(0);
        long recordCountInCurrentLoop = 0;
        LinkedHashSet<Long> indexes = new LinkedHashSet<>();
        for (int i = 0; i < positionList.size(); i++){
            recordCountInCurrentLoop ++;
            long value = positionList.get(i);
            if (skipSet.contains(value)){
                continue;
            }
            if (recordCountInCurrentLoop >= nextCount){
                indexes.add(value);
                nextCount = logIndexLagBackoff.next(indexes.size());
                recordCountInCurrentLoop = 0;
            }
        }
        return indexes;
    }
}
