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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.CollectorRegistry;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.transaction.coordinator.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.apache.pulsar.transaction.coordinator.impl.DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class TxnLogBufferedWriterTest extends MockedBookKeeperTestCase {

    /** Default metrics definitions. **/
    private String[] metricsLabelNames = new String[]{"coordinatorId"};
    private String[] metricsLabelValues = new String[]{"1"};
    private String metricsPrefix = "pulsar_txn_tc";
    private String mlName = "-";

    /**
     * Overridden cases:
     *   1. Enabled the batch feature.
     *     1-1. Normal.
     *     1-2. The number of data writes is few (Small data block).
     *     1-3. The number of data writes is large (Small data block).
     *     1-4. Large data writes.
     *     1-5. A batch has only one data
     *     1-6. A batch has only two data
     *   2. Disabled the batch feature.
     *   3. Bookie always error.
     *      3-1. Enabled the batch feature.
     *      3-1. Disabled the batch feature.
     *   4. Bookie sometimes error.
     *      4-1. Enabled the batch feature.
     *      4-1. Disabled the batch feature.
     *   5. {@link TxnLogBufferedWriter} sometimes close.
     *      5-1. Enabled the batch feature.
     *      5-1. Disabled the batch feature.
     */
    @DataProvider(name= "mainProcessCasesProvider")
    public Object[][] mainProcessCasesProvider(){
        return new Object [][]{
                // Normal.
                {512, 1024 * 1024, 1, true, 2000, 4, BookieErrorType.NO_ERROR, false},
                // The number of data writes is few.
                {512, 1024 * 1024, 1, true, 100, 4, BookieErrorType.NO_ERROR, false},
                // The number of data writes is large.
                {512, 1024 * 1024, 100, true, 20000, 4, BookieErrorType.NO_ERROR, false},
                // Big data writes.
                {512, 1024, 100, true, 3000, 1024, BookieErrorType.NO_ERROR, false},
                // A batch has only one data
                {1, 1024 * 1024, 100, true, 512, 4, BookieErrorType.NO_ERROR, false},
                // A batch has only two data
                {2, 1024 * 1024, 100, true, 1999, 4, BookieErrorType.NO_ERROR, false},
                // Disabled the batch feature
                {128, 1024 * 1024, 1, false, 500, 4, BookieErrorType.NO_ERROR, false},
                // Bookie always error.
                {128, 1024 * 1024, 1, true, 512, 4, BookieErrorType.ALWAYS_ERROR, false},
                {128, 1024 * 1024, 1, false, 512, 4, BookieErrorType.ALWAYS_ERROR, false},
                // Bookie sometimes error.
                {128, 1024 * 1024, 1, true, 512, 4, BookieErrorType.SOMETIMES_ERROR, false},
                {128, 1024 * 1024, 1, false, 512, 4, BookieErrorType.SOMETIMES_ERROR, false},
                // TxnLogBufferedWriter sometimes close.
                {128, 1024 * 1024, 1, true, 512, 4, BookieErrorType.NO_ERROR, true},
                {128, 1024 * 1024, 1, false, 512, 4, BookieErrorType.NO_ERROR, true}
        };
    }

    @Override
    protected ManagedLedgerFactoryConfig createManagedLedgerFactoryConfig() {
        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = super.createManagedLedgerFactoryConfig();
        // disable the broker cache so that assertAllByteBufHasBeenReleased can work correctly.
        managedLedgerFactoryConfig.setMaxCacheSize(0);
        return managedLedgerFactoryConfig;
    }

    /**
     * Tests all operations from write to callback, including these step:
     *   1. Write many data.
     *   2. Verify callback correct.
     *   3. Read from bookie, verify all data correct.
     *   4. Verify byte buffer that generated by DataSerializer has been released after process finish.
     *   5. Cleanup.
     * Overridden cases: see {@link #mainProcessCasesProvider}.
     *
     * TODO: Additional validation is required:
     *   1. Recycle is handled correctly.
     *   2. ByteBuf generated by data merge before Bookie writes is released correctly, including prefix-ByteBuf and
     *      composite-ByteBuf.
     *   3. Even if executed "bkc.failAfter", also need to verify the data which has already written to bookie.
     */
    @Test(dataProvider = "mainProcessCasesProvider", timeOut = 1000 * 10)
    public void testMainProcess(int batchedWriteMaxRecords, int batchedWriteMaxSize, int batchedWriteMaxDelayInMillis,
                                boolean batchEnabled, final int writeCmdExecuteCount,
                                int eachDataBytesLen, BookieErrorType bookieErrorType,
                                boolean closeBufferedWriter) throws Exception {
        // Assert args.
        if (BookieErrorType.SOMETIMES_ERROR == bookieErrorType || closeBufferedWriter){
            if (writeCmdExecuteCount < batchedWriteMaxRecords * 2){
                throw new IllegalArgumentException("if bookieErrorType is BookieErrorType.SOMETIMES_ERROR or"
                        + " closeBufferedWriter is ture, param-writeCmdExecuteCount max large than"
                        + " param-batchedWriteMaxRecords * 2");
            }
        }
        // Calculate the exactly batch-enabled for assert.
        boolean exactlyBatched = batchEnabled;
        if (batchedWriteMaxSize <= eachDataBytesLen || batchedWriteMaxRecords <= 1){
            exactlyBatched = false;
        }
        /**
         * Create components for tests.
         *  - managedLedger
         *  - managedCursor
         *  - orderedExecutor
         *  - orderedExecutor
         *  - scheduledExecutorService
         *  - dataSerializer
         */
        ManagedLedger managedLedger = factory.open("txn_test_ledger");
        ManagedCursor managedCursor = managedLedger.openCursor("txn_test_cursor");
        if (BookieErrorType.ALWAYS_ERROR == bookieErrorType){
            managedLedger = Mockito.spy(managedLedger);
            managedCursor = Mockito.spy(managedCursor);
            failureManagedLedger(managedLedger);
        } else if (BookieErrorType.SOMETIMES_ERROR == bookieErrorType){
            bkc.failAfter(1, BKException.Code.NotEnoughBookiesException);
            metadataStore.setAlwaysFail(new MetadataStoreException.BadVersionException(""));
        }
        HashedWheelTimer transactionTimer = new HashedWheelTimer(new DefaultThreadFactory("transaction-timer"),
                1, TimeUnit.MILLISECONDS);
        JsonDataSerializer dataSerializer = new JsonDataSerializer(eachDataBytesLen);
        /**
         * Execute test task.
         *   1. Write many times.
         *   2. Store the param-context and param-position of callback function for verify.
         */
        // Create TxLogBufferedWriter.
        TxnLogBufferedWriter txnLogBufferedWriter =  new TxnLogBufferedWriter<Integer>(
                    managedLedger, ((ManagedLedgerImpl) managedLedger).getExecutor(), transactionTimer,
                    dataSerializer, batchedWriteMaxRecords, batchedWriteMaxSize,
                    batchedWriteMaxDelayInMillis, batchEnabled, DISABLED_BUFFERED_WRITER_METRICS);
        // Store the param-context, param-position, param-exception of callback function and complete-count for verify.
        List<Integer> contextArrayOfCallback = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, ManagedLedgerException> exceptionArrayOfCallback = new ConcurrentHashMap<>();
        Map<PositionImpl, List<Position>> positionsOfCallback = Collections.synchronizedMap(new LinkedHashMap<>());
        AtomicBoolean anyFlushCompleted = new AtomicBoolean();
        TxnLogBufferedWriter.AddDataCallback callback = new TxnLogBufferedWriter.AddDataCallback(){
            @Override
            public void addComplete(Position position, Object ctx) {
                anyFlushCompleted.set(true);
                if (contextArrayOfCallback.contains(Integer.valueOf(String.valueOf(ctx)))){
                    return;
                }
                contextArrayOfCallback.add((int)ctx);
                PositionImpl lightPosition = PositionImpl.get(position.getLedgerId(), position.getEntryId());
                positionsOfCallback.computeIfAbsent(lightPosition,
                        p -> Collections.synchronizedList(new ArrayList<>()));
                positionsOfCallback.get(lightPosition).add(position);
            }
            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                if (contextArrayOfCallback.contains(Integer.valueOf(String.valueOf(ctx)))){
                    return;
                }
                contextArrayOfCallback.add((int)ctx);
                exceptionArrayOfCallback.put((int)ctx, exception);
            }
        };
        // Write many times.
        int bufferedWriteCloseAtIndex = writeCmdExecuteCount/2
                + new Random().nextInt(writeCmdExecuteCount / 4 + 1) - 1;
        for (int i = 0; i < writeCmdExecuteCount; i++){
            txnLogBufferedWriter.asyncAddData(i, callback, i);
            // Ensure flush at least once before close buffered writer.
            if (closeBufferedWriter && i == 0){
                // TODO Clean up this method in next PR, make more easier to read.
            }
            if (closeBufferedWriter && bufferedWriteCloseAtIndex == i){
                // Wait for any complete callback, avoid unstable.
                Awaitility.await().until(() -> anyFlushCompleted.get());
                txnLogBufferedWriter.close();
            }
        }
        /**
         * Assert callback correct.
         *   1. callback count.
         *   2. callback param-context.
         *   2. if {@param bookieError} is true. verify the ex count.
         *   3. if {@param bookieError} is false. verify the param-position count.
         *   4. if enabled batch-feature, will verify the attributes (batchSize, batchIndex) of callback param-position.
         * Note: {@link TxnLogBufferedWriter#close()} will make callback that fail by state check execute earlier, so if
         *   {@param closeBufferedWriter} is true, we should sort callback-context-array. Other callback-param should
         *   not sort.
         */
        // Assert callback count.
        Awaitility.await().until(() -> contextArrayOfCallback.size() == writeCmdExecuteCount);
        // Assert callback param-context, verify that all callbacks are executed in strict order.
        // If exception occurs, the failure callback be executed earlier. So sorted contextArrayOfCallback.
        if (closeBufferedWriter || bookieErrorType == BookieErrorType.SOMETIMES_ERROR){
            Collections.sort(contextArrayOfCallback);
        }
        assertEquals(contextArrayOfCallback.size(), writeCmdExecuteCount);
        for (int ctxIndex = 0, successIndex = 0; ctxIndex < writeCmdExecuteCount; ctxIndex++){
            // When calling `txnLogBufferedWriter.close`, all tasks in the queue will fail immediately, this makes the
            // callback of failure task earlier.
            if (exceptionArrayOfCallback.containsKey(ctxIndex)){
                continue;
            }
            assertEquals(contextArrayOfCallback.get(successIndex).intValue(), ctxIndex);
            successIndex++;
        }
        // if {@param bookieError} is true. verify the ex count.
        // if {@param bookieError} is false. verify the param-position count.
        int exceptionCallbackCount = exceptionArrayOfCallback.size();
        int positionCallbackCount = (int) positionsOfCallback.values().stream().flatMap(l -> l.stream()).count();
        if (BookieErrorType.SOMETIMES_ERROR == bookieErrorType ||  closeBufferedWriter){
            assertEquals(exceptionCallbackCount + positionCallbackCount, writeCmdExecuteCount);
        } else if (BookieErrorType.NO_ERROR == bookieErrorType){
            assertEquals(positionCallbackCount, writeCmdExecuteCount);
        } else {
            assertEquals(exceptionCallbackCount, writeCmdExecuteCount);
        }
        // if enabled batch-feature, will verify the attributes (batchSize, batchIndex) of callback param-position.
        if (exactlyBatched && BookieErrorType.ALWAYS_ERROR != bookieErrorType){
            Iterator<List<Position>> callbackPositionIterator = positionsOfCallback.values().iterator();
            List<String> exactlyFlushedDataArray = dataSerializer.getGeneratedJsonArray();
            for (int batchedEntryIndex = 0; batchedEntryIndex < exactlyFlushedDataArray.size() - exceptionCallbackCount;
                 batchedEntryIndex++) {
                String json = exactlyFlushedDataArray.get(batchedEntryIndex);
                List<Integer> batchedData = JsonDataSerializer.deserializeMergedData(json);
                List<Position> innerPositions = callbackPositionIterator.next();
                for (int i = 0; i < batchedData.size(); i++) {
                    TxnBatchedPositionImpl innerPosition =
                            (TxnBatchedPositionImpl) innerPositions.get(i);
                    assertEquals(innerPosition.getBatchSize(), batchedData.size());
                    assertEquals(innerPosition.getBatchIndex(), i);
                }
            }
        }
        /**
         * Read entries from Bookie, Assert all data and position correct.
         *   1. Assert the data of the read matches the data write.
         *   2. Assert the position of the read matches the position of the callback.
         *   3. Assert callback count equals entry count.
         * Note: after call {@link PulsarMockBookKeeper#failAfter}, the managed ledger could not execute read entries
         *       anymore, so when {@param bookieErrorType} equals {@link BookieErrorType.SOMETIMES_ERROR}, ski verify.
         * Note2: Verify that all entry was written in strict order.
         */
        if (BookieErrorType.NO_ERROR == bookieErrorType) {
            Iterator<PositionImpl> callbackPositionIterator = positionsOfCallback.keySet().iterator();
            List<String> dataArrayWrite = dataSerializer.getGeneratedJsonArray();
            int entryCounter = 0;
            while (managedCursor.hasMoreEntries()) {
                List<Entry> entries = managedCursor.readEntries(1);
                if (entries == null || entries.isEmpty()) {
                    continue;
                }
                for (int m = 0; m < entries.size(); m++) {
                    // Get data read.
                    Entry entry = entries.get(m);
                    // Assert the position of the read matches the position of the callback.
                    PositionImpl callbackPosition = callbackPositionIterator.next();
                    assertEquals(entry.getLedgerId(), callbackPosition.getLedgerId());
                    assertEquals(entry.getEntryId(), callbackPosition.getEntryId());
                    if (exactlyBatched) {
                        // Get expected entry data from cache of DataSerializer.
                        String expectEntryData = dataArrayWrite.get(entryCounter);
                        ByteBuf entryByteBuf = entry.getDataBuffer();
                        entryByteBuf.skipBytes(4);
                        byte[] entryContentBytes = new byte[entryByteBuf.readableBytes()];
                        entryByteBuf.readBytes(entryContentBytes);
                        String actEntryData = new String(entryContentBytes, Charset.defaultCharset());
                        // Assert the data of the read matches the data write.
                        assertEquals(actEntryData, expectEntryData);
                    } else {
                        int entryValue = entry.getDataBuffer().readInt();
                        // Assert the data of the read matches the data write.
                        assertEquals(entryValue, entryCounter);
                    }
                    entry.release();
                    entryCounter++;
                }
            }
            // Assert callback count equals entry count.
            assertEquals(entryCounter, positionsOfCallback.size());
        }
        /** cleanup. **/
        txnLogBufferedWriter.close().get();
        // If we already call {@link PulsarMockBookKeeper#failAfter}, the managed ledger could not close anymore.
        if (BookieErrorType.SOMETIMES_ERROR != bookieErrorType){
            managedLedger.close();
        }
        transactionTimer.stop();
        /**
         * Assert all Byte Buf generated by DataSerializer has been released.
         *   1. Because ManagedLedger holds write cache, some data is not actually released until ManagedLedger is
         *      closed.
         *   2. If we already call {@link PulsarMockBookKeeper#failAfter(int, int)}, the managed ledger could not close
         *      anymore, so when {@param bookieErrorType} equals {@link BookieErrorType.SOMETIMES_ERROR}, skip verify.
         */
        if (BookieErrorType.SOMETIMES_ERROR != bookieErrorType) {
            dataSerializer.assertAllByteBufHasBeenReleased();
        }
        dataSerializer.cleanup();
    }

    private void failureManagedLedger(ManagedLedger managedLedger){
        Mockito.doAnswer(invocation -> {
            AsyncCallbacks.AddEntryCallback callback =
                    (AsyncCallbacks.AddEntryCallback) invocation.getArguments()[1];
            ManagedLedgerException managedLedgerException =
                    new ManagedLedgerException(new Exception("Fail by mock."));
            callback.addFailed(managedLedgerException, invocation.getArguments()[2]);
            return null;
        }).when(managedLedger).asyncAddEntry(Mockito.any(ByteBuf.class), Mockito.any(), Mockito.any());
    }

    /**
     * Adjustable thresholds: trigger BookKeeper-write when reaching any one of the following conditions
     *     Max size (bytes)
     *     Max records count
     *     Max delay time
     * Tests these three thresholds.
     */
    @Test
    public void testFlushThresholds() throws Exception{
        // Create components.
        ManagedLedger managedLedger = Mockito.mock(ManagedLedger.class);
        Mockito.when(managedLedger.getName()).thenReturn(mlName);
        OrderedExecutor orderedExecutor =  OrderedExecutor.newBuilder().numThreads(5).name("txn-topic-threads").build();
        Executor topicExecutor = orderedExecutor.chooseThread(managedLedger.getName());
        HashedWheelTimer transactionTimer = new HashedWheelTimer(new DefaultThreadFactory("transaction-timer"),
                1, TimeUnit.MILLISECONDS);
        SumStrDataSerializer dataSerializer = new SumStrDataSerializer();
        // Cache the data flush to Bookie for Asserts.
        List<Integer> dataArrayFlushedToBookie = Collections.synchronizedList(new ArrayList<>());
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ByteBuf byteBuf = (ByteBuf)invocation.getArguments()[0];
                byteBuf.skipBytes(4);
                dataArrayFlushedToBookie.add(byteBuf.readInt());
                AsyncCallbacks.AddEntryCallback callback =
                        (AsyncCallbacks.AddEntryCallback) invocation.getArguments()[1];
                callback.addComplete(PositionImpl.get(1,1), byteBuf,
                        invocation.getArguments()[2]);
                return null;
            }
        }).when(managedLedger).asyncAddEntry(Mockito.any(ByteBuf.class), Mockito.any(), Mockito.any());
        // Test threshold: writeMaxDelayInMillis (use timer).
        TxnLogBufferedWriter txnLogBufferedWriter1 = new TxnLogBufferedWriter<>(managedLedger, topicExecutor,
                transactionTimer, dataSerializer, 32, 1024 * 4,
                100, true, DISABLED_BUFFERED_WRITER_METRICS);
        TxnLogBufferedWriter.AddDataCallback callback = Mockito.mock(TxnLogBufferedWriter.AddDataCallback.class);
        txnLogBufferedWriter1.asyncAddData(100, callback, 100);
        Thread.sleep(90);
        // Verify does not refresh ahead of time.
        assertEquals(dataArrayFlushedToBookie.size(), 0);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> dataArrayFlushedToBookie.size() == 1);
        assertEquals(dataArrayFlushedToBookie.get(0).intValue(), 100);
        txnLogBufferedWriter1.close().get();

        // Test threshold: batchedWriteMaxRecords.
        TxnLogBufferedWriter txnLogBufferedWriter2 = new TxnLogBufferedWriter<>(managedLedger, topicExecutor,
                transactionTimer, dataSerializer, 32, 1024 * 4,
                10000, true, DISABLED_BUFFERED_WRITER_METRICS);
        for (int i = 0; i < 32; i++){
            txnLogBufferedWriter2.asyncAddData(1, callback, 1);
        }
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> dataArrayFlushedToBookie.size() == 2);
        assertEquals(dataArrayFlushedToBookie.get(1).intValue(), 32);
        txnLogBufferedWriter2.close();

        // Test threshold: batchedWriteMaxSize.
        TxnLogBufferedWriter txnLogBufferedWriter3 = new TxnLogBufferedWriter<>(managedLedger, topicExecutor,
                transactionTimer, dataSerializer, 1024, 64 * 4,
                10000, true, DISABLED_BUFFERED_WRITER_METRICS);
        for (int i = 0; i < 64; i++){
            txnLogBufferedWriter3.asyncAddData(1, callback, 1);
        }
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> dataArrayFlushedToBookie.size() == 3);
        assertEquals(dataArrayFlushedToBookie.get(2).intValue(), 64);
        txnLogBufferedWriter3.close();

        // Assert all resources released
        dataSerializer.assertAllByteBufHasBeenReleased();
        // clean up.
        dataSerializer.cleanup();
        transactionTimer.stop();
        orderedExecutor.shutdown();
    }

    /**
     * The use of {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)} for timed
     * tasks in the original implementation caused this problem:
     *   When the writer thread processes slowly, the scheduleAtFixedRate task will continue to append tasks to the
     *   ledger thread, this burdens the ledger thread and leads to an avalanche.
     * This method is used to verify the fix for the above problem. see: https://github.com/apache/pulsar/pull/16679.
     */
    @Test
    public void testPendingScheduleTriggerTaskCount() throws Exception {
        // Create components.
        ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(65536 * 2);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 5,
                TimeUnit.SECONDS, workQueue);
        HashedWheelTimer transactionTimer = new HashedWheelTimer(new DefaultThreadFactory("transaction-timer"),
                1, TimeUnit.MILLISECONDS);
        SumStrDataSerializer dataSerializer = new SumStrDataSerializer();
        // Mock managed ledger and write counter.
        MockedManagedLedger mockedManagedLedger = mockManagedLedgerWithWriteCounter(mlName);
        // Start tests.
        TxnLogBufferedWriter txnLogBufferedWriter = new TxnLogBufferedWriter<>(mockedManagedLedger.managedLedger,
                threadPoolExecutor, transactionTimer, dataSerializer, 2, 1024 * 4,
                1, true, DISABLED_BUFFERED_WRITER_METRICS);
        TxnLogBufferedWriter.AddDataCallback callback = Mockito.mock(TxnLogBufferedWriter.AddDataCallback.class);
        // Append heavier tasks to the Ledger thread.
        AtomicInteger heavierTaskCounter = new AtomicInteger();
        Thread heavierTask = new Thread(() -> {
            while (true) {
                threadPoolExecutor.execute(() -> {
                    try {
                        heavierTaskCounter.incrementAndGet();
                        Thread.sleep(19);
                    } catch (InterruptedException e) {
                    }
                    heavierTaskCounter.decrementAndGet();
                });
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        // Append normal tasks to ledger thread.
        AtomicInteger addAsyncDataTaskCounter = new AtomicInteger();
        AtomicInteger normalFlushCounter = new AtomicInteger();
        Thread normalWriteTask = new Thread(() -> {
            while (true) {
                for (int i = 0; i < 2; i++) {
                    addAsyncDataTaskCounter.incrementAndGet();
                    txnLogBufferedWriter.asyncAddData(1, callback, 1);
                }
                normalFlushCounter.incrementAndGet();
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heavierTask.start();
        normalWriteTask.start();
        // Running 100 millis.
        Thread.sleep(100);
        heavierTask.interrupt();
        normalWriteTask.interrupt();
        /**
         * Calculates the expected maximum number of remaining tasks.
         * 1. add the task async add.
         * 2. add the task flush by records count limit.
         * 3. sub the task already complete.
         * 4. add the heavier task count.
         */
        int maxCountOfRemainingTasks = 0;
        maxCountOfRemainingTasks += normalFlushCounter.get();
        maxCountOfRemainingTasks += addAsyncDataTaskCounter.get();
        maxCountOfRemainingTasks -= mockedManagedLedger.writeCounter.get() * 3;
        maxCountOfRemainingTasks += heavierTaskCounter.get();
        // In addition to the above tasks, is the timing tasks.
        // Assert the timing task count. The above calculation is not accurate, so leave a margin.
        assertTrue(workQueue.size() - maxCountOfRemainingTasks < 10);
        // clean up.
        txnLogBufferedWriter.close().get();
        dataSerializer.cleanup();
        threadPoolExecutor.shutdown();
        transactionTimer.stop();
        threadPoolExecutor.shutdown();
    }

    private static class JsonDataSerializer implements TxnLogBufferedWriter.DataSerializer<Integer>{

        private static ObjectMapper objectMapper = new ObjectMapper();

        private List<ByteBuf> generatedByteBufArray = Collections.synchronizedList(new ArrayList<>());

        @Getter
        private List<String> generatedJsonArray = Collections.synchronizedList(new ArrayList<>());

        private int eachDataBytesLen = 4;

        public JsonDataSerializer(){

        }

        public JsonDataSerializer(int eachDataBytesLen){
            this.eachDataBytesLen = eachDataBytesLen;
        }

        @Override
        public int getSerializedSize(Integer data) {
            return eachDataBytesLen;
        }
        @Override
        public ByteBuf serialize(Integer data) {
            ByteBuf byteBuf = Unpooled.buffer(4);
            byteBuf.writeInt(data == null ? 0 : data.intValue());
            holdsByteBuf(byteBuf);
            return byteBuf;
        }
        @Override
        public ByteBuf serialize(ArrayList<Integer> dataArray) {
            try {
                String str = objectMapper.writeValueAsString(dataArray);
                generatedJsonArray.add(str);
                ByteBuf byteBuf = Unpooled.copiedBuffer(str.getBytes(Charset.defaultCharset()));
                holdsByteBuf(byteBuf);
                return byteBuf;
            } catch (Exception e){
                throw new RuntimeException(e);
            }
        }

        public static List<Integer> deserializeMergedData(ByteBuf byteBuf){
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(bytes);
            return deserializeMergedData(new String(bytes, Charset.defaultCharset()));
        }

        public static List<Integer> deserializeMergedData(String json){
            try {
                return objectMapper.readValue(json, ArrayList.class);
            } catch (Exception e){
                throw new RuntimeException(e);
            }
        }

        protected void holdsByteBuf(ByteBuf byteBuf){
            generatedByteBufArray.add(byteBuf);
        }

        protected void cleanup(){
            // Just for GC.
            generatedByteBufArray = Collections.synchronizedList(new ArrayList<>());
            generatedJsonArray = Collections.synchronizedList(new ArrayList<>());
        }

        protected void assertAllByteBufHasBeenReleased(){
            for (ByteBuf byteBuf : generatedByteBufArray){
                assertEquals(byteBuf.refCnt(), 0);
            }
        }

        protected int getSizePerData(){
            return eachDataBytesLen;
        }
    }

    private static class SumStrDataSerializer extends JsonDataSerializer {

        @Override
        public ByteBuf serialize(ArrayList<Integer> dataArray) {
            int sum = CollectionUtils.isEmpty(dataArray) ? 0 : dataArray.stream().reduce((a, b) -> a+b).get();
            ByteBuf byteBuf = Unpooled.buffer(4);
            byteBuf.writeInt(sum);
            holdsByteBuf(byteBuf);
            return byteBuf;
        }
    }

    private static class RandomLenSumStrDataSerializer extends SumStrDataSerializer {

        @Getter
        private int totalSize;

        /**
         * After the test, when {@link TxnLogBufferedWriterConfig#getBatchedWriteMaxRecords()} = 256
         *   and {@link TxnLogBufferedWriterConfig#getBatchedWriteMaxSize()} = 1024,
         *   and {@link TxnLogBufferedWriterConfig#getBatchedWriteMaxDelayInMillis()} = 1, the random-size baseline
         *   was set as 9, and there was maximum probability that all three thresholds could be hit.
         */
        @Override
        public int getSerializedSize(Integer data) {
            int size = new Random().nextInt(9);
            totalSize += size;
            return size;
        }
    }

    private static class TwoLenSumDataSerializer extends JsonDataSerializer {

        private final int len1;

        private final int len2;

        private AtomicBoolean useLen2 = new AtomicBoolean();

        public TwoLenSumDataSerializer(int len1, int len2){
            this.len1 = len1;
            this.len2 = len2;
        }

        /**
         * After the test, when {@link TxnLogBufferedWriterConfig#getBatchedWriteMaxRecords()} = 256
         *   and {@link TxnLogBufferedWriterConfig#getBatchedWriteMaxSize()} = 1024,
         *   and {@link TxnLogBufferedWriterConfig#getBatchedWriteMaxDelayInMillis()} = 1, the random-size baseline
         *   was set as 9, and there was maximum probability that all three thresholds could be hit.
         */
        @Override
        public int getSerializedSize(Integer data) {
            boolean b = useLen2.get();
            useLen2.set(!b);
            return b ? len2 : len1;
        }
    }

    private static class WrongDataSerializer extends SumStrDataSerializer {

        private final AtomicInteger counter = new AtomicInteger();

        private final int dataSize;

        private final boolean failOnGetSerializedSize;

        private final boolean failOnSerializeSingleData;

        private final boolean failOnSerializeDataArray;

        public WrongDataSerializer(int dataSize, boolean failOnGetSerializedSize, boolean failOnSerializeSingleData,
                                   boolean failOnSerializeDataArray) {
            this.dataSize = dataSize;
            this.failOnGetSerializedSize = failOnGetSerializedSize;
            this.failOnSerializeSingleData = failOnSerializeSingleData;
            this.failOnSerializeDataArray = failOnSerializeDataArray;
        }

        @Override
        public int getSerializedSize(Integer data) {
            if (counter.incrementAndGet() % 2 == 0 && failOnGetSerializedSize){
                // sometimes error.
                throw new IllegalArgumentException("serialize wrong");
            }
            return dataSize;
        }

        @Override
        public ByteBuf serialize(Integer data) {
            if (failOnSerializeSingleData) {
                // always error.
                throw new IllegalArgumentException("serialize wrong");
            }
            return super.serialize(data);
        }

        @Override
        public ByteBuf serialize(ArrayList<Integer> dataArray) {
            if (failOnSerializeDataArray){
                // always error.
                throw new IllegalArgumentException("serialize wrong");
            }
            return super.serialize(dataArray);
        }
    }

    public enum BookieErrorType{
        NO_ERROR,
        ALWAYS_ERROR,
        SOMETIMES_ERROR;
    }

    /**
     * Test Transaction buffered writer stats when disabled batch feature.
     */
    @Test
    public void testMetricsStatsWhenDisabledBatchFeature() throws Exception {
        TxnLogBufferedWriterMetricsStats metricsStats = new TxnLogBufferedWriterMetricsStats(
                metricsPrefix, metricsLabelNames, metricsLabelValues, CollectorRegistry.defaultRegistry
        );
        ManagedLedger managedLedger = factory.open("tx_test_ledger");
        // Create callback with counter.
        var callbackWithCounter = createCallBackWithCounter();
        OrderedExecutor orderedExecutor =  OrderedExecutor.newBuilder().numThreads(5).name("txn-threads").build();
        // Create TxnLogBufferedWriter.
        HashedWheelTimer transactionTimer = new HashedWheelTimer(new DefaultThreadFactory("transaction-timer"),
                1, TimeUnit.MILLISECONDS);
        var dataSerializer = new RandomLenSumStrDataSerializer();
        var txnLogBufferedWriter = new TxnLogBufferedWriter<Integer>(
                managedLedger, orderedExecutor, transactionTimer,
                dataSerializer, Integer.MAX_VALUE, Integer.MAX_VALUE,
                Integer.MAX_VALUE, false, metricsStats);
        // Add some data.
        int writeCount = 1000;
        for (int i = 0; i < writeCount; i++){
            txnLogBufferedWriter.asyncAddData(1, callbackWithCounter.callback, "");
        }
        // Wait for all data write finish.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(
                () -> callbackWithCounter.finishCounter.get() + callbackWithCounter.failureCounter.get() == writeCount
        );
        assertEquals(callbackWithCounter.failureCounter.get(), 0);
        // Assert metrics stat.
        verifyTheHistogramMetrics(0, 0, 0);
        // cleanup.
        txnLogBufferedWriter.close().get();
        metricsStats.close();
        transactionTimer.stop();
        orderedExecutor.shutdown();
        CollectorRegistry.defaultRegistry.clear();
    }

    @Test
    public void testMetricsStatsThatTriggeredByMaxRecordCount() throws Exception {
        SumStrDataSerializer dataSerializer = new SumStrDataSerializer();
        int batchedWriteMaxRecords = 2;
        int writeCount = 100;
        int expectedBatchFlushCount = writeCount / batchedWriteMaxRecords;
        int expectedTotalBytesSize = writeCount * dataSerializer.getSizePerData();
        // Create callback with counter.
        var callbackWithCounter = createCallBackWithCounter();
        // Create TxnLogBufferedWriter.
        var txnLogBufferedWriterContext = createTxnBufferedWriterContextWithMetrics(
                dataSerializer, batchedWriteMaxRecords, Integer.MAX_VALUE, Integer.MAX_VALUE);
        var txnLogBufferedWriter = txnLogBufferedWriterContext.txnLogBufferedWriter;
        // Add some data.
        for (int i = 0; i < writeCount; i++){
            txnLogBufferedWriter.asyncAddData(1, callbackWithCounter.callback, "");
        }
        // Wait for all write finish.
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(
                () -> callbackWithCounter.finishCounter.get() + callbackWithCounter.failureCounter.get() == writeCount
        );
        int actualBatchFlushCount = txnLogBufferedWriterContext.mockedManagedLedger.writeCounter.get();
        assertEquals(callbackWithCounter.failureCounter.get(), 0);
        assertEquals(expectedBatchFlushCount, actualBatchFlushCount);
        verifyTheCounterMetrics(expectedBatchFlushCount,0,0,0);
        verifyTheHistogramMetrics(expectedBatchFlushCount, writeCount, expectedTotalBytesSize);
        // cleanup.
        releaseTxnLogBufferedWriterContext(txnLogBufferedWriterContext);
        // after close, verify the metrics change to 0.
        verifyTheCounterMetrics(0,0,0,0);
        verifyTheHistogramMetrics(0,0,0);
    }

    @Test
    public void testMetricsStatsThatTriggeredByMaxSize() throws Exception {
        SumStrDataSerializer dataSerializer = new SumStrDataSerializer();
        int batchedWriteMaxSize = 16;
        int writeCount = 100;
        int expectedBatchFlushCount = writeCount / (batchedWriteMaxSize / dataSerializer.getSizePerData());
        int expectedTotalBytesSize = expectedBatchFlushCount * batchedWriteMaxSize;
        var callbackWithCounter = createCallBackWithCounter();
        // Create TxnLogBufferedWriter.
        var txnLogBufferedWriterContext = createTxnBufferedWriterContextWithMetrics(
                dataSerializer, Integer.MAX_VALUE, batchedWriteMaxSize, Integer.MAX_VALUE);
        var txnLogBufferedWriter = txnLogBufferedWriterContext.txnLogBufferedWriter;
        // Add some data.
        for (int i = 0; i < writeCount; i++){
            txnLogBufferedWriter.asyncAddData(1, callbackWithCounter.callback, "");
        }
        // Wait for all write finish.
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(
                () -> callbackWithCounter.finishCounter.get() + callbackWithCounter.failureCounter.get() == writeCount
        );
        int actualBatchFlushCount = txnLogBufferedWriterContext.mockedManagedLedger.writeCounter.get();
        assertEquals(callbackWithCounter.failureCounter.get(), 0);
        assertEquals(expectedBatchFlushCount, actualBatchFlushCount);
        verifyTheCounterMetrics(0, expectedBatchFlushCount,0,0);
        verifyTheHistogramMetrics(expectedBatchFlushCount, writeCount, expectedTotalBytesSize);
        // cleanup.
        releaseTxnLogBufferedWriterContext(txnLogBufferedWriterContext);
        // after close, verify the metrics change to 0.
        verifyTheCounterMetrics(0,0,0,0);
        verifyTheHistogramMetrics(0,0,0);
    }

    @Test
    public void testMetricsStatsThatTriggeredByMaxDelayTime() throws Exception {
        SumStrDataSerializer dataSerializer = new SumStrDataSerializer();
        int writeCount = 1;
        int batchedWriteMaxDelayInMillis = 1000;
        int expectFlushCount = 1;
        int expectedTotalBytesSize = writeCount * dataSerializer.getSizePerData();
        var callbackWithCounter = createCallBackWithCounter();
        // Create TxnLogBufferedWriter.
        var txnLogBufferedWriterContext =
                createTxnBufferedWriterContextWithMetrics(dataSerializer, Integer.MAX_VALUE,
                        Integer.MAX_VALUE, batchedWriteMaxDelayInMillis);
        var txnLogBufferedWriter = txnLogBufferedWriterContext.txnLogBufferedWriter;
        // Add one data.
        txnLogBufferedWriter.asyncAddData(1, callbackWithCounter.callback, "");
        // Wait for all write finish.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(
                () -> callbackWithCounter.finishCounter.get() + callbackWithCounter.failureCounter.get() == writeCount
        );
        assertEquals(callbackWithCounter.failureCounter.get(), 0);
        int actualBatchFlushCount = txnLogBufferedWriterContext.mockedManagedLedger.writeCounter.get();
        assertEquals(actualBatchFlushCount, expectFlushCount);
        verifyTheCounterMetrics(0,0, actualBatchFlushCount,0);
        verifyTheHistogramMetrics(actualBatchFlushCount, writeCount, expectedTotalBytesSize);
        // cleanup.
        releaseTxnLogBufferedWriterContext(txnLogBufferedWriterContext);
        // after close, verify the metrics change to 0.
        verifyTheCounterMetrics(0,0,0,0);
        verifyTheHistogramMetrics(0,0,0);
    }

    @Test
    public void testMetricsStatsThatTriggeredByLargeSingleData() throws Exception {
        // Use TwoLenSumDataSerializer for: write a little data once, then write a large data once.
        int bytesSizePerRecordWhichInBatch = 4;
        int batchedWriteMaxSize = 1024;
        TwoLenSumDataSerializer dataSerializer =
                new TwoLenSumDataSerializer(bytesSizePerRecordWhichInBatch, batchedWriteMaxSize);
        int writeCount = 100;
        // Both "directly write BK because the data too large" and "flush batch data because next data too large" will
        // write BK, so expectWriteBKCount equals writeCount.
        int expectWriteBKCount = writeCount;
        int singleLargeDataRequestCount = expectWriteBKCount / 2;
        int expectedBatchFlushTriggeredByLargeData = singleLargeDataRequestCount;
        int expectedTotalBytesSize = expectedBatchFlushTriggeredByLargeData * bytesSizePerRecordWhichInBatch;
        var callbackWithCounter = createCallBackWithCounter();
        // Create TxnLogBufferedWriter.
        var txnLogBufferedWriterContext = createTxnBufferedWriterContextWithMetrics(
                dataSerializer, Integer.MAX_VALUE, batchedWriteMaxSize, Integer.MAX_VALUE);
        var txnLogBufferedWriter = txnLogBufferedWriterContext.txnLogBufferedWriter;
        // Add some data.
        for (int i = 0; i < writeCount; i++){
            txnLogBufferedWriter.asyncAddData(1, callbackWithCounter.callback, i);
        }
        // Wait for all data write finish.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(
                () -> callbackWithCounter.finishCounter.get() + callbackWithCounter.failureCounter.get() == writeCount
        );
        assertEquals(callbackWithCounter.failureCounter.get(), 0);
        assertEquals(txnLogBufferedWriterContext.mockedManagedLedger.writeCounter.get(), expectWriteBKCount);
        verifyTheCounterMetrics(0,0,0,expectedBatchFlushTriggeredByLargeData);
        verifyTheHistogramMetrics(expectedBatchFlushTriggeredByLargeData,
                writeCount - singleLargeDataRequestCount, expectedTotalBytesSize);
        // cleanup.
        releaseTxnLogBufferedWriterContext(txnLogBufferedWriterContext);
        // after close, verify the metrics change to 0.
        verifyTheCounterMetrics(0,0,0,0);
        verifyTheHistogramMetrics(0,0,0);
    }

    @Test
    public void testFailWhenAddData() throws Exception {
        int batchedWriteMaxSize = 1024;
        TxnLogBufferedWriter.DataSerializer dataSerializer =
                new WrongDataSerializer(batchedWriteMaxSize, true, true, true);
        int writeCount = 100;
        var callbackWithCounter = createCallBackWithCounter();
        // Create TxnLogBufferedWriter.
        var txnLogBufferedWriterContext = createTxnBufferedWriterContextWithMetrics(
                dataSerializer, Integer.MAX_VALUE, batchedWriteMaxSize, Integer.MAX_VALUE);
        var txnLogBufferedWriter = txnLogBufferedWriterContext.txnLogBufferedWriter;
        // Add some data.
        for (int i = 0; i < writeCount; i++){
            txnLogBufferedWriter.asyncAddData(i, callbackWithCounter.callback, i);
        }

        // Wait for all data write finish.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(
                () -> {
                    return callbackWithCounter.failureCounter.get() == writeCount;
                }
        );
        assertEquals(txnLogBufferedWriterContext.mockedManagedLedger.writeCounter.get(), 0);
        // cleanup.
        releaseTxnLogBufferedWriterContext(txnLogBufferedWriterContext);
    }

    private void releaseTxnLogBufferedWriterContext(TxnLogBufferedWriterContext context)
            throws ExecutionException, InterruptedException {
        context.txnLogBufferedWriter.close().get();
        context.metrics.close();
        context.timer.stop();
        CollectorRegistry.defaultRegistry.clear();
    }

    @AllArgsConstructor
    private static class TxnLogBufferedWriterContext{
        TxnLogBufferedWriter<Integer> txnLogBufferedWriter;
        MockedManagedLedger mockedManagedLedger;
        Timer timer;
        TxnLogBufferedWriterMetricsStats metrics;
    }

    @AllArgsConstructor
    private static class MockedManagedLedger{
        ManagedLedger managedLedger;
        AtomicInteger writeCounter;
    }

    @AllArgsConstructor
    public static class AddDataCallbackAndCounter{
        TxnLogBufferedWriter.AddDataCallback callback;
        AtomicInteger finishCounter;
        AtomicInteger failureCounter;
    }

    private TxnLogBufferedWriterContext createTxnBufferedWriterContextWithMetrics(
            TxnLogBufferedWriter.DataSerializer<Integer> dataSerializer, int batchedWriteMaxRecords,
            int batchedWriteMaxSize, int batchedWriteMaxDelayInMillis){
        OrderedExecutor orderedExecutor =  OrderedExecutor.newBuilder()
                .numThreads(5).name("txn-threads").build();
        HashedWheelTimer transactionTimer = new HashedWheelTimer(new DefaultThreadFactory("transaction-timer"),
                1, TimeUnit.MILLISECONDS);
        TxnLogBufferedWriterMetricsStats metricsStats = new TxnLogBufferedWriterMetricsStats(
                metricsPrefix, metricsLabelNames, metricsLabelValues, CollectorRegistry.defaultRegistry
        );
        // Mock managed ledger and write counter.
        MockedManagedLedger mockedManagedLedger = mockManagedLedgerWithWriteCounter(mlName);
        // Create Txn Buffered Writer.
        TxnLogBufferedWriter<Integer> txnLogBufferedWriter = new TxnLogBufferedWriter<Integer>(
                mockedManagedLedger.managedLedger,
                orderedExecutor.chooseThread(mockedManagedLedger.managedLedger.getName()), transactionTimer,
                dataSerializer, batchedWriteMaxRecords, batchedWriteMaxSize,
                batchedWriteMaxDelayInMillis, true, metricsStats);
        return new TxnLogBufferedWriterContext(txnLogBufferedWriter, mockedManagedLedger, transactionTimer,
                metricsStats);
    }

    private void verifyTheCounterMetrics(int triggeredByRecordCount, int triggeredByMaxSize, int triggeredByMaxDelay,
                                         int triggeredByLargeData){
        assertEquals(
                getCounterValue(String.format("%s_bufferedwriter_flush_trigger_max_records", metricsPrefix)),
                triggeredByRecordCount);
        assertEquals(
                getCounterValue(String.format("%s_bufferedwriter_flush_trigger_max_size", metricsPrefix)),
                triggeredByMaxSize);
        assertEquals(
                getCounterValue(String.format("%s_bufferedwriter_flush_trigger_max_delay", metricsPrefix)),
                triggeredByMaxDelay);
        assertEquals(
                getCounterValue(String.format("%s_bufferedwriter_flush_trigger_large_data", metricsPrefix)),
                triggeredByLargeData);
    }

    private void verifyTheHistogramMetrics(int batchFlushCount, int totalRecordsCount, int totalSize){
        // Total flush count.
        assertEquals(
                getHistogramCount(String.format("%s_bufferedwriter_batch_records", metricsPrefix)),
                batchFlushCount);
        assertEquals(
                getHistogramCount(String.format("%s_bufferedwriter_batch_size_bytes", metricsPrefix)),
                batchFlushCount);
        assertEquals(
                getHistogramCount(String.format("%s_bufferedwriter_batch_oldest_record_delay_seconds", metricsPrefix)),
                batchFlushCount);
        // Total records count.
        assertEquals(
                getHistogramSum(String.format("%s_bufferedwriter_batch_records", metricsPrefix)),
                totalRecordsCount);
        // Total data size.
        assertEquals(
                getHistogramSum(String.format("%s_bufferedwriter_batch_size_bytes", metricsPrefix)),
                totalSize);
    }

    private MockedManagedLedger mockManagedLedgerWithWriteCounter(String mlName){
        AtomicInteger writeCounter = new AtomicInteger();
        ManagedLedger managedLedger = Mockito.mock(ManagedLedger.class);
        Mockito.when(managedLedger.getName()).thenReturn(mlName);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                writeCounter.incrementAndGet();
                AsyncCallbacks.AddEntryCallback callback =
                        (AsyncCallbacks.AddEntryCallback) invocation.getArguments()[1];
                callback.addComplete(PositionImpl.get(1,1), (ByteBuf)invocation.getArguments()[0],
                        invocation.getArguments()[2]);
                return null;
            }
        }).when(managedLedger).asyncAddEntry(Mockito.any(ByteBuf.class), Mockito.any(), Mockito.any());
        return new MockedManagedLedger(managedLedger, writeCounter);
    }

    private AddDataCallbackAndCounter createCallBackWithCounter(){
        AtomicInteger finishCounter = new AtomicInteger();
        AtomicInteger failureCounter = new AtomicInteger();
        TxnLogBufferedWriter.AddDataCallback callback = new TxnLogBufferedWriter.AddDataCallback(){
            @Override
            public void addComplete(Position position, Object context) {
                finishCounter.incrementAndGet();
            }
            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                failureCounter.incrementAndGet();
            }
        };
        return new AddDataCallbackAndCounter(callback, finishCounter, failureCounter);
    }

    private double getCounterValue(String name) {
        Double d = CollectorRegistry.defaultRegistry.getSampleValue(
                name + "_total",
                metricsLabelNames,
                metricsLabelValues);
        return d == null ? 0: d.doubleValue();
    }

    private double getHistogramCount(String name) {
        Double d = CollectorRegistry.defaultRegistry.getSampleValue(
                name + "_count",
                metricsLabelNames,
                metricsLabelValues);
        return d == null ? 0: d.doubleValue();
    }

    private double getHistogramSum(String name) {
        Double d = CollectorRegistry.defaultRegistry.getSampleValue(
                name + "_sum",
                metricsLabelNames,
                metricsLabelValues);
        return d == null ? 0: d.doubleValue();
    }

}
