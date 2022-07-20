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
package org.apache.pulsar.transaction.coordinator.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.transaction.coordinator.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class TxnLogBufferedWriterTest extends MockedBookKeeperTestCase {

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
        Object [][] provider = new Object [13][];
        // Normal.
        provider[0] = new Object[]{512, 1024 * 1024, 1, true, 2000, 2, 4, BookieErrorType.NO_ERROR, false};
        // The number of data writes is few.
        provider[1] = new Object[]{512, 1024 * 1024, 1, true, 100, 2, 4, BookieErrorType.NO_ERROR, false};
        // The number of data writes is large.
        provider[2] = new Object[]{512, 1024 * 1024, 100, true, 20000, 5, 4, BookieErrorType.NO_ERROR, false};
        // Big data writes.
        provider[3] = new Object[]{512, 1024, 100, true, 3000, 4, 1024, BookieErrorType.NO_ERROR, false};
        // A batch has only one data
        provider[4] = new Object[]{1, 1024 * 1024, 100, true, 2000, 4, 4, BookieErrorType.NO_ERROR, false};
        // A batch has only two data
        provider[5] = new Object[]{2, 1024 * 1024, 100, true, 1999, 4, 4, BookieErrorType.NO_ERROR, false};
        // Disabled the batch feature
        provider[6] = new Object[]{512, 1024 * 1024, 1, false, 2000, 4, 4, BookieErrorType.NO_ERROR, false};
        // Bookie always error.
        provider[7] = new Object[]{512, 1024 * 1024, 1, true, 2000, 2, 4, BookieErrorType.ALWAYS_ERROR, false};
        provider[8] = new Object[]{512, 1024 * 1024, 1, false, 2000, 4, 4, BookieErrorType.ALWAYS_ERROR, false};
        // Bookie sometimes error.
        provider[9] = new Object[]{512, 1024 * 1024, 1, true, 2000, 4, 4, BookieErrorType.SOMETIMES_ERROR, false};
        provider[10] = new Object[]{512, 1024 * 1024, 1, false, 2000, 4, 4, BookieErrorType.SOMETIMES_ERROR, false};
        // TxnLogBufferedWriter sometimes close.
        provider[11] = new Object[]{512, 1024 * 1024, 1, true, 2000, 4, 4, BookieErrorType.NO_ERROR, true};
        provider[12] = new Object[]{512, 1024 * 1024, 1, false, 2000, 4, 4, BookieErrorType.NO_ERROR, true};
        return provider;
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
    @Test(dataProvider = "mainProcessCasesProvider")
    public void testMainProcess(int batchedWriteMaxRecords, int batchedWriteMaxSize, int batchedWriteMaxDelayInMillis,
                                boolean batchEnabled, final int writeCmdExecuteCount, int maxWaitSeconds,
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
        if (batchedWriteMaxSize <= eachDataBytesLen){
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
        ManagedLedger managedLedger = factory.open("tx_test_ledger");
        ManagedCursor managedCursor = managedLedger.openCursor("tx_test_cursor");
        if (BookieErrorType.ALWAYS_ERROR == bookieErrorType){
            managedLedger = Mockito.spy(managedLedger);
            managedCursor = Mockito.spy(managedCursor);
            failureManagedLedger(managedLedger);
        } else if (BookieErrorType.SOMETIMES_ERROR == bookieErrorType){
            bkc.failAfter(1, BKException.Code.NotEnoughBookiesException);
            metadataStore.setAlwaysFail(new MetadataStoreException.BadVersionException(""));
        }
        OrderedExecutor orderedExecutor =  OrderedExecutor.newBuilder()
                .numThreads(5).name("tx-threads").build();
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("tx-scheduler-threads"));
        JsonDataSerializer dataSerializer = new JsonDataSerializer(eachDataBytesLen);
        /**
         * Execute test task.
         *   1. Write many times.
         *   2. Store the param-context and param-position of callback function for verify.
         */
        // Create TxLogBufferedWriter.
        TxnLogBufferedWriter txnLogBufferedWriter = new TxnLogBufferedWriter<Integer>(
                        managedLedger, orderedExecutor, scheduledExecutorService,
                        dataSerializer, batchedWriteMaxRecords, batchedWriteMaxSize,
                        batchedWriteMaxDelayInMillis, batchEnabled);
        // Store the param-context, param-position, param-exception of callback function and complete-count for verify.
        ArrayList<Integer> contextArrayOfCallback = new ArrayList<>();
        ArrayList<ManagedLedgerException> exceptionArrayOfCallback = new ArrayList<>();
        LinkedHashMap<PositionImpl, ArrayList<Position>> positionsOfCallback = new LinkedHashMap<>();
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
                positionsOfCallback.computeIfAbsent(lightPosition, p -> new ArrayList<>());
                positionsOfCallback.get(lightPosition).add(position);
            }
            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                if (contextArrayOfCallback.contains(Integer.valueOf(String.valueOf(ctx)))){
                    return;
                }
                contextArrayOfCallback.add((int)ctx);
                exceptionArrayOfCallback.add(exception);
            }
        };
        // Write many times.
        int bufferedWriteCloseAtIndex = writeCmdExecuteCount/2
                + new Random().nextInt(writeCmdExecuteCount / 4 + 1) - 1;
        for (int i = 0; i < writeCmdExecuteCount; i++){
            txnLogBufferedWriter.asyncAddData(i, callback, i);
            // Ensure flush at least once before close buffered writer.
            if (closeBufferedWriter && i == 0){
                txnLogBufferedWriter.trigFlush(true);
            }
            if (closeBufferedWriter && bufferedWriteCloseAtIndex == i){
                // Wait for any complete callback, avoid unstable.
                Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> anyFlushCompleted.get());
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
        Awaitility.await().atMost(maxWaitSeconds, TimeUnit.SECONDS)
                .until(() -> contextArrayOfCallback.size() == writeCmdExecuteCount);
        // Assert callback param-context, verify that all callbacks are executed in strict order.
        if (closeBufferedWriter){
            Collections.sort(contextArrayOfCallback);
        }
        Assert.assertEquals(contextArrayOfCallback.size(), writeCmdExecuteCount);
        for (int ctxIndex = 0; ctxIndex < writeCmdExecuteCount; ctxIndex++){
            Assert.assertEquals(contextArrayOfCallback.get(ctxIndex).intValue(), ctxIndex);
        }
        // if {@param bookieError} is true. verify the ex count.
        // if {@param bookieError} is false. verify the param-position count.
        int exceptionCallbackCount = exceptionArrayOfCallback.size();
        int positionCallbackCount = (int) positionsOfCallback.values().stream().flatMap(l -> l.stream()).count();
        if (BookieErrorType.SOMETIMES_ERROR == bookieErrorType ||  closeBufferedWriter){
            Assert.assertTrue(exceptionCallbackCount > 0);
            Assert.assertTrue(positionCallbackCount > 0);
            Assert.assertEquals(exceptionCallbackCount + positionCallbackCount, writeCmdExecuteCount);
        } else if (BookieErrorType.NO_ERROR == bookieErrorType){
            Assert.assertEquals(positionCallbackCount, writeCmdExecuteCount);
        } else {
            Assert.assertEquals(exceptionCallbackCount, writeCmdExecuteCount);
        }
        // if enabled batch-feature, will verify the attributes (batchSize, batchIndex) of callback param-position.
        if (exactlyBatched && BookieErrorType.ALWAYS_ERROR != bookieErrorType){
            Iterator<ArrayList<Position>> callbackPositionIterator = positionsOfCallback.values().iterator();
            List<String> exactlyFlushedDataArray = dataSerializer.getGeneratedJsonArray();
            for (int batchedEntryIndex = 0; batchedEntryIndex < exactlyFlushedDataArray.size() - exceptionCallbackCount;
                 batchedEntryIndex++) {
                String json = exactlyFlushedDataArray.get(batchedEntryIndex);
                List<Integer> batchedData = JsonDataSerializer.deserializeMergedData(json);
                ArrayList<Position> innerPositions = callbackPositionIterator.next();
                for (int i = 0; i < batchedData.size(); i++) {
                    TxnBatchedPositionImpl innerPosition =
                            (TxnBatchedPositionImpl) innerPositions.get(i);
                    Assert.assertEquals(innerPosition.getBatchSize(), batchedData.size());
                    Assert.assertEquals(innerPosition.getBatchIndex(), i);
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
                    Assert.assertEquals(entry.getLedgerId(), callbackPosition.getLedgerId());
                    Assert.assertEquals(entry.getEntryId(), callbackPosition.getEntryId());
                    if (exactlyBatched) {
                        // Get expected entry data from cache of DataSerializer.
                        String expectEntryData = dataArrayWrite.get(entryCounter);
                        ByteBuf entryByteBuf = entry.getDataBuffer();
                        entryByteBuf.skipBytes(4);
                        byte[] entryContentBytes = new byte[entryByteBuf.readableBytes()];
                        entryByteBuf.readBytes(entryContentBytes);
                        String actEntryData = new String(entryContentBytes, Charset.defaultCharset());
                        // Assert the data of the read matches the data write.
                        Assert.assertEquals(actEntryData, expectEntryData);
                    } else {
                        int entryValue = entry.getDataBuffer().readInt();
                        // Assert the data of the read matches the data write.
                        Assert.assertEquals(entryValue, entryCounter);
                    }
                    entry.release();
                    entryCounter++;
                }
            }
            // Assert callback count equals entry count.
            Assert.assertEquals(entryCounter, positionsOfCallback.size());
        }
        /** cleanup. **/
        txnLogBufferedWriter.close();
        // If we already call {@link PulsarMockBookKeeper#failAfter}, the managed ledger could not close anymore.
        if (BookieErrorType.SOMETIMES_ERROR != bookieErrorType){
            managedLedger.close();
        }
        scheduledExecutorService.shutdown();
        orderedExecutor.shutdown();
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
        String managedLedgerName = "-";
        ManagedLedger managedLedger = Mockito.mock(ManagedLedger.class);
        Mockito.when(managedLedger.getName()).thenReturn(managedLedgerName);
        OrderedExecutor orderedExecutor =  OrderedExecutor.newBuilder().numThreads(5).name("tx-topic-threads").build();
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("tx-scheduler-threads"));
        SumStrDataSerializer dataSerializer = new SumStrDataSerializer();
        // Cache the data flush to Bookie for Asserts.
        List<Integer> dataArrayFlushedToBookie = new ArrayList<>();
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
        // Start tests.
        TxnLogBufferedWriter txnLogBufferedWriter = new TxnLogBufferedWriter<>(managedLedger, orderedExecutor,
                scheduledExecutorService, dataSerializer, 32, 1024 * 4, 100, true);
        TxnLogBufferedWriter.AddDataCallback callback = Mockito.mock(TxnLogBufferedWriter.AddDataCallback.class);
        // Test threshold: writeMaxDelayInMillis.
        txnLogBufferedWriter.asyncAddData(100, callback, 100);
        Thread.sleep(101);
        // Test threshold: batchedWriteMaxRecords.
        for (int i = 0; i < 32; i++){
            txnLogBufferedWriter.asyncAddData(1, callback, 1);
        }
        // Test threshold: batchedWriteMaxSize.
        TxnLogBufferedWriter txnLogBufferedWriter2 = new TxnLogBufferedWriter<>(managedLedger, orderedExecutor,
                scheduledExecutorService, dataSerializer, 1024, 64 * 4, 100, true);
        for (int i = 0; i < 64; i++){
            txnLogBufferedWriter2.asyncAddData(1, callback, 1);
        }
        // Assert 4 flush.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> dataArrayFlushedToBookie.size() == 3);
        Assert.assertEquals(dataArrayFlushedToBookie.get(0).intValue(), 100);
        Assert.assertEquals(dataArrayFlushedToBookie.get(1).intValue(), 32);
        Assert.assertEquals(dataArrayFlushedToBookie.get(2).intValue(), 64);
        // Assert all resources released
        dataSerializer.assertAllByteBufHasBeenReleased();
        // clean up.
        dataSerializer.cleanup();
        scheduledExecutorService.shutdown();
        orderedExecutor.shutdown();
    }

    private static class JsonDataSerializer implements TxnLogBufferedWriter.DataSerializer<Integer>{

        private static ObjectMapper objectMapper = new ObjectMapper();

        private ArrayList<ByteBuf> generatedByteBufArray = new ArrayList<>();

        @Getter
        private ArrayList<String> generatedJsonArray = new ArrayList<>();

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
            generatedByteBufArray = new ArrayList<>();
            generatedJsonArray = new ArrayList<>();
        }

        protected void assertAllByteBufHasBeenReleased(){
            for (ByteBuf byteBuf : generatedByteBufArray){
                Assert.assertEquals(byteBuf.refCnt(), 0);
            }
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

    public enum BookieErrorType{
        NO_ERROR,
        ALWAYS_ERROR,
        SOMETIMES_ERROR;
    }
}
