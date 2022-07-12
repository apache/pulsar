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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.PreferHeapByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.transaction.coordinator.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class TxnLogBufferedWriterTest extends MockedBookKeeperTestCase {

    /**
     * Tests all operations from write to callback, including
     * {@link TxnLogBufferedWriter#asyncAddData(Object, AsyncCallbacks.AddEntryCallback, Object)}
     * {@link TxnLogBufferedWriter#trigFlush()}
     * and so on.
     */
    @Test
    public void testMainProcess() throws Exception {
        // Create components.
        ManagedLedger managedLedger = factory.open("tx_test_ledger");
        ManagedCursor managedCursor = managedLedger.openCursor("tx_test_cursor");
        OrderedExecutor orderedExecutor =  OrderedExecutor.newBuilder()
                .numThreads(5).name("tx-brokers-topic-workers").build();
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-stats-updater"));
        // Create TxLogBufferedWriter.
        ArrayList<String> stringBatchedEntryDataList = new ArrayList<>();
        // Holds variable byteBufBatchedEntryDataList just for release.
        ArrayList<ByteBuf> byteBufBatchedEntryDataList = new ArrayList<>();
        TxnLogBufferedWriter txnLogBufferedWriter =
                new TxnLogBufferedWriter<ByteBuf>(managedLedger, orderedExecutor, scheduledExecutorService,
                        new TxnLogBufferedWriter.DataSerializer<ByteBuf>(){

                            @Override
                            public int getSerializedSize(ByteBuf byteBuf) {
                                return byteBuf.readableBytes();
                            }

                            @Override
                            public ByteBuf serialize(ByteBuf byteBuf) {
                                return byteBuf;
                            }

                            @Override
                            public ByteBuf serialize(ArrayList<ByteBuf> dataArray) {
                                StringBuilder stringBuilder = new StringBuilder();
                                for (int i = 0; i < dataArray.size(); i++){
                                    ByteBuf byteBuf = dataArray.get(i);
                                    byteBuf.markReaderIndex();
                                    stringBuilder.append(byteBuf.readInt());
                                    if (i != dataArray.size() - 1){
                                        stringBuilder.append(",");
                                    }
                                }
                                String contentStr = stringBuilder.toString();
                                stringBatchedEntryDataList.add(contentStr);
                                byte[] bs = contentStr.getBytes(Charset.defaultCharset());
                                ByteBuf content = PreferHeapByteBufAllocator.DEFAULT.buffer(bs.length);
                                content.writeBytes(bs);
                                byteBufBatchedEntryDataList.add(content);
                                return content;
                            }
                        }, 512, 1024 * 1024 * 4, 1, true);
        // Create callback.
        ArrayList<Integer> callbackCtxList = new ArrayList<>();
        LinkedHashMap<PositionImpl, ArrayList<Position>> callbackPositions =
                new LinkedHashMap<PositionImpl, ArrayList<Position>>();
        AsyncCallbacks.AddEntryCallback callback = new AsyncCallbacks.AddEntryCallback(){
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                if (callbackCtxList.contains(Integer.valueOf(String.valueOf(ctx)))){
                    return;
                }
                callbackCtxList.add((int)ctx);
                PositionImpl lightPosition = PositionImpl.get(position.getLedgerId(), position.getEntryId());
                callbackPositions.computeIfAbsent(lightPosition, p -> new ArrayList<>());
                callbackPositions.get(lightPosition).add(position);
            }
            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
            }
        };
        // Loop write data.  Holds variable dataArrayProvided just for release.
        List<ByteBuf> dataArrayProvided = new ArrayList<>();
        int cmdAddExecutedCount = 5000;
        for (int i = 0; i < cmdAddExecutedCount; i++){
            ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(8);
            byteBuf.writeInt(i);
            dataArrayProvided.add(byteBuf);
            txnLogBufferedWriter.asyncAddData(byteBuf, callback, i);
        }
        // Wait for all cmd-write finish.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> callbackCtxList.size() == cmdAddExecutedCount);
        // Release data provided.
        for (ByteBuf byteBuf : dataArrayProvided){
            byteBuf.release();
        }
        // Assert callback ctx correct.
        Assert.assertEquals(callbackCtxList.size(), cmdAddExecutedCount);
        for (int ctxIndex = 0; ctxIndex < cmdAddExecutedCount; ctxIndex++){
            Assert.assertEquals(callbackCtxList.get(ctxIndex).intValue(), ctxIndex);
        }
        // Assert callback positions correct.
        Assert.assertEquals(callbackPositions.values().stream().flatMap(l -> l.stream()).count(), cmdAddExecutedCount);
        Iterator<ArrayList<Position>> callbackPositionIterator = callbackPositions.values().iterator();
        for (int batchedEntryIndex = 0; batchedEntryIndex < stringBatchedEntryDataList.size(); batchedEntryIndex++){
            String stringBatchedEntryData = stringBatchedEntryDataList.get(batchedEntryIndex);
            String[] entryDataArray = stringBatchedEntryData.split(",");
            ArrayList<Position> innerPositions = callbackPositionIterator.next();
            int batchSize = entryDataArray.length;
            for(int i = 0; i < entryDataArray.length; i++){
                TxnLogBufferedWriter.TxnBatchedPositionImpl innerPosition =
                        (TxnLogBufferedWriter.TxnBatchedPositionImpl) innerPositions.get(i);
                Assert.assertEquals(innerPosition.getBatchSize(), batchSize);
                Assert.assertEquals(innerPosition.getBatchIndex(), i);
            }
        }
        // Assert content correct.
        int batchedEntryIndex = 0;
        Iterator<PositionImpl> expectedBatchedPositionIterator = callbackPositions.keySet().iterator();
        while (managedCursor.hasMoreEntries()) {
            List<Entry> entries = managedCursor.readEntries(1);
            if (entries == null || entries.isEmpty()) {
                continue;
            }
            for (int m = 0; m < entries.size(); m++) {
                String stringBatchedEntryContent = stringBatchedEntryDataList.get(batchedEntryIndex);
                Entry entry = entries.get(m);
                ByteBuf entryByteBuf = entry.getDataBuffer();
                entryByteBuf.skipBytes(4);
                // Assert entry content correct.
                byte[] entryContentBytes = new byte[entryByteBuf.readableBytes()];
                entryByteBuf.readBytes(entryContentBytes);
                String entryContentString = new String(entryContentBytes, Charset.defaultCharset());
                Assert.assertEquals(entryContentString, stringBatchedEntryContent);
                // Assert position correct.
                PositionImpl expectPosition = expectedBatchedPositionIterator.next();
                Assert.assertEquals(entry.getLedgerId(), expectPosition.getLedgerId());
                Assert.assertEquals(entry.getEntryId(), expectPosition.getEntryId());
                entry.release();
                batchedEntryIndex++;
            }
        }
        Assert.assertEquals(batchedEntryIndex, stringBatchedEntryDataList.size());
        // cleanup.
        txnLogBufferedWriter.close();
        managedLedger.close();
        scheduledExecutorService.shutdown();
        orderedExecutor.shutdown();
    }

    /**
     * Test main process when disabled batch feature.
     */
    @Test
    public void testDisabled() throws Exception {
        // Create components.
        ManagedLedger managedLedger = factory.open("tx_test_ledger");
        ManagedCursor managedCursor = managedLedger.openCursor("tx_test_cursor");
        // Create TxLogBufferedWriter.
        TxnLogBufferedWriter txnLogBufferedWriter =
                new TxnLogBufferedWriter<ByteBuf>(managedLedger, null, null,
                        new TxnLogBufferedWriter.DataSerializer<ByteBuf>() {
                            @Override
                            public int getSerializedSize(ByteBuf byteBuf) {
                                return 0;
                            }

                            @Override
                            public ByteBuf serialize(ByteBuf byteBuf) {
                                return byteBuf;
                            }

                            @Override
                            public ByteBuf serialize(ArrayList<ByteBuf> dataArray) {
                                return null;
                            }
                        }, 512, 1024 * 1024 * 4, 1, false);
        // Create callback.
        CompletableFuture<Triple<Position, ByteBuf, Object>> future = new CompletableFuture<>();
        AsyncCallbacks.AddEntryCallback callback = new AsyncCallbacks.AddEntryCallback(){
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                future.complete(Triple.of(position, entryData, ctx));
            }
            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        };
        // Async add data
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(8);
        byteBuf.writeInt(1);
        txnLogBufferedWriter.asyncAddData(byteBuf, callback, 1);
        // Wait add finish.
        Triple<Position, ByteBuf, Object> triple = future.get(2, TimeUnit.SECONDS);
        // Assert callback ctx correct.
        Assert.assertEquals(triple.getMiddle(), byteBuf);
        Assert.assertEquals(triple.getRight(), 1);
        // Assert read entries correct.
        List<Entry> entries = managedCursor.readEntriesOrWait(1);
        Assert.assertEquals(entries.size(), 1);
        Entry entry = entries.get(0);
        Assert.assertEquals(entry.getLedgerId(), triple.getLeft().getLedgerId());
        Assert.assertEquals(entry.getEntryId(), triple.getLeft().getEntryId());
        Assert.assertEquals(entry.getDataBuffer().readInt(), 1);
        entry.release();
        // cleanup.
        txnLogBufferedWriter.close();
        managedLedger.close();
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
        Mockito.when(managedLedger.getName()).thenReturn("-");
        OrderedExecutor orderedExecutor =  OrderedExecutor.newBuilder()
                .numThreads(5).name("tx-brokers-topic-workers").build();
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-stats-updater"));
        TxnLogBufferedWriter.DataSerializer<Integer> serializer = new TxnLogBufferedWriter.DataSerializer<Integer>(){
            @Override
            public int getSerializedSize(Integer data) {
                return 4;
            }
            @Override
            public ByteBuf serialize(Integer data) {
                return null;
            }
            @Override
            public ByteBuf serialize(ArrayList<Integer> dataArray) {
                int sum = CollectionUtils.isEmpty(dataArray) ? 0 : dataArray.stream().reduce((a, b) -> a+b).get();
                ByteBuf byteBuf = Unpooled.buffer(4);
                byteBuf.writeInt(sum);
                return byteBuf;
            }
        };
        List<Integer> flushedDataList = new ArrayList<>();
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ByteBuf byteBuf = (ByteBuf)invocation.getArguments()[0];
                byteBuf.skipBytes(4);
                flushedDataList.add(byteBuf.readInt());
                AsyncCallbacks.AddEntryCallback callback =
                        (AsyncCallbacks.AddEntryCallback) invocation.getArguments()[1];
                callback.addComplete(PositionImpl.get(1,1), byteBuf,
                        invocation.getArguments()[2]);
                return null;
            }
        }).when(managedLedger).asyncAddEntry(Mockito.any(ByteBuf.class), Mockito.any(), Mockito.any());

        TxnLogBufferedWriter txnLogBufferedWriter = new TxnLogBufferedWriter<>(managedLedger, orderedExecutor,
                scheduledExecutorService, serializer, 32, 1024 * 4, 100, true);
        AsyncCallbacks.AddEntryCallback callback = Mockito.mock(AsyncCallbacks.AddEntryCallback.class);
        // Test threshold: writeMaxDelayInMillis.
        txnLogBufferedWriter.asyncAddData(100, callback, 100);
        Thread.sleep(101);
        // Test threshold: batchedWriteMaxRecords.
        for (int i = 0; i < 32; i++){
            txnLogBufferedWriter.asyncAddData(1, callback, 1);
        }
        // Test threshold: batchedWriteMaxSize.
        TxnLogBufferedWriter txnLogBufferedWriter2 = new TxnLogBufferedWriter<>(managedLedger, orderedExecutor,
                scheduledExecutorService, serializer, 1024, 64 * 4, 100, true);
        for (int i = 0; i < 64; i++){
            txnLogBufferedWriter2.asyncAddData(1, callback, 1);
        }
        // Assert 3 flush.
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> flushedDataList.size() == 3);
        Assert.assertEquals(flushedDataList.get(0).intValue(), 100);
        Assert.assertEquals(flushedDataList.get(1).intValue(), 32);
        Assert.assertEquals(flushedDataList.get(2).intValue(), 64);
        // clean up.
        scheduledExecutorService.shutdown();
        orderedExecutor.shutdown();
    }
}
