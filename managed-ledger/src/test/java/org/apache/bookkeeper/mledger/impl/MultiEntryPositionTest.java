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
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class MultiEntryPositionTest extends MockedBookKeeperTestCase {

    @Test
    public void testGetRangeGroupByLedgerId() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnableLruCacheMaxUnackedRanges(true);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(0, 0, 0, 1);
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(1, 0, 1, 1);
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(2, 0, 2, 1);

        Map<Long, List<MLDataFormats.MessageRange>> rangeGroupByLedgerId = c1.getRangeGroupByLedgerId(null);

        assertEquals(rangeGroupByLedgerId.size(), 3);
        AtomicLong count = new AtomicLong(0);
        rangeGroupByLedgerId.forEach((key, value) -> {
            assertEquals(key.longValue(), count.get());
            assertEquals(value.size(), 1);
            assertEquals(value.get(0).getLowerEndpoint().getLedgerId(), count.get());
            assertEquals(value.get(0).getLowerEndpoint().getEntryId(), 0);
            assertEquals(value.get(0).getUpperEndpoint().getLedgerId(), count.getAndIncrement());
            assertEquals(value.get(0).getUpperEndpoint().getEntryId(), 1);
        });

        Set<Long> filter = new HashSet<>();
        filter.add(2L);
        rangeGroupByLedgerId = c1.getRangeGroupByLedgerId(filter);
        assertEquals(rangeGroupByLedgerId.size(), 1);
        assertTrue(rangeGroupByLedgerId.containsKey(2L));
        c1.close();
        ledger.close();
    }

    @Test
    public void testGetDeletionIndexInfosGroupByLedgerId() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnableLruCacheMaxUnackedRanges(true);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        List<BitSetRecyclable> list = Arrays.asList(BitSetRecyclable.create(),
                BitSetRecyclable.create(),
                BitSetRecyclable.create());
        c1.getBatchDeletedIndexes().put(new PositionImpl(0,0), list.get(0));
        c1.getBatchDeletedIndexes().put(new PositionImpl(1,0), list.get(1));
        c1.getBatchDeletedIndexes().put(new PositionImpl(2,0), list.get(2));

        Map<Long, List<MLDataFormats.BatchedEntryDeletionIndexInfo>> map = c1.
                getDeletionIndexInfosGroupByLedgerId(null);

        assertEquals(map.size(), 3);
        AtomicLong count = new AtomicLong(0);
        map.forEach((key, value) -> {
            assertEquals(key.longValue(), count.get());
            assertEquals(value.size(), 1);
            assertEquals(value.get(0).getPosition().getLedgerId(), count.getAndIncrement());
            assertEquals(value.get(0).getPosition().getEntryId(), 0);
        });

        Set<Long> filter = new HashSet<>();
        filter.add(2L);
        map = c1.getDeletionIndexInfosGroupByLedgerId(filter);
        assertEquals(map.size(), 1);
        assertTrue(map.containsKey(2L));

        c1.close();
        ledger.close();
        list.forEach(BitSetRecyclable::recycle);
    }

    /**
     * Covered chain: internalAsyncMarkDelete & NoLedger -> internalAsyncMarkDelete + NoLedger ->
     * startCreatingNewMetadataLedger -> createNewMetadataLedgerAndSwitch -> doCreateNewMetadataLedger ->
     * persistPositionToLedger -> switchToNewLedger -> flushPendingMarkDeletes -> internalMarkDelete ->
     * persistPositionToLedger -> remove individualDeletedMessages
     * @throws Exception
     */
    @Test
    public void testCopyLruEntriesToNewLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnableLruCacheMaxUnackedRanges(true);
        ManagedLedger ledger = factory.open("my_test_ledger" + UUID.randomUUID(), config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        //init new ledger
        c1.startCreatingNewMetadataLedger();
        Awaitility.await().untilAsserted(() -> assertEquals(c1.getState(),"Open"));
        LedgerHandle ledgerHandle = c1.getCursorLedgerHandle();
        long ledgerId = ledgerHandle.getId();
        // init IndividuallyDeletedMessages
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(0, 0, 0, 1);
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(1, 0, 1, 1);
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(2, 0, 2, 1);
        Map<Long, List<MLDataFormats.MessageRange>> rangeGroupByLedgerId = c1.getRangeGroupByLedgerId(null);
        MLDataFormats.PositionInfo.Builder builder =
                MLDataFormats.PositionInfo.newBuilder().setLedgerId(1).setEntryId(0);
        Map<Long, MLDataFormats.NestedPositionInfo> rangeMarker = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            if (builder.getIndividualDeletedMessagesList().size() > 0) {
                builder.removeIndividualDeletedMessages(0);
            }
            builder.addIndividualDeletedMessages(rangeGroupByLedgerId.get((long) i).get(0));
            long entryId = ledgerHandle.addEntry(builder.build().toByteArray());
            rangeMarker.put((long) i, MLDataFormats.NestedPositionInfo
                    .newBuilder().setLedgerId(ledgerId).setEntryId(entryId).build());
        }
        // init marker
        c1.getRangeMarker().putAll(rangeMarker);
        c1.setLastMarkDeleteEntry(new ManagedCursorImpl.MarkDeleteEntry(
                new PositionImpl(0, 0), new HashMap<>(), null, null));
        // init PendingMarkDeletes
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(3, 0, 3, 1);
        CompletableFuture<Void> future = new CompletableFuture<>();
        c1.pendingMarkDeleteOps.add(new ManagedCursorImpl.MarkDeleteEntry(new PositionImpl(0, 0),
                new HashMap<>(), new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null));
        // trigger switch and make sure it is finished
        c1.startCreatingNewMetadataLedger();
        Awaitility.await().untilAsserted(() -> assertEquals(c1.getState(),"Open"));
        future.get();
        // validate marker
        assertEquals(c1.getRangeMarker().size(), 4);
        // After copying, flushPendingMarkDeletes will be triggered.
        // Copying will occupy entryId 0-3, and flushPendingMarkDeletes will occupy 4-7.
        AtomicLong entryIdCounter = new AtomicLong(4);
        AtomicLong ledgerIdCounter = new AtomicLong(0);
        long newLedgerId = ledgerId + 1;
        c1.getRangeMarker().forEach((key, value) -> {
            // in IndividuallyDeletedMessages ledgerId is from 0 to 3
            assertEquals(key.longValue(), ledgerIdCounter.getAndIncrement());
            assertEquals(value.getLedgerId(), newLedgerId);
            assertEquals(value.getEntryId(), entryIdCounter.getAndIncrement());
        });

        // Verify the copied entry and marker
        Enumeration<LedgerEntry> entries = c1.getCursorLedgerHandle().readEntries(0, 3);
        int counter = 0;
        MLDataFormats.NestedPositionInfo.Builder positionBuilder = MLDataFormats.NestedPositionInfo.newBuilder();
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            MLDataFormats.PositionInfo positionInfo = MLDataFormats.PositionInfo.parseFrom(entry.getEntryInputStream());
            if (counter == 3) {
                //the last entry is marker
                assertEquals(positionInfo.getMarkerIndexInfoCount(), 3);
                AtomicLong subCounter = new AtomicLong(0);
                positionInfo.getMarkerIndexInfoList().forEach(markerIndexInfo -> {
                    assertEquals(markerIndexInfo.getEntryPosition().getLedgerId(), newLedgerId);
                    assertEquals(markerIndexInfo.getEntryPosition().getEntryId(), subCounter.getAndIncrement());
                });
                assertEquals(subCounter.get(), 3);
                continue;
            } else {
                assertEquals(positionInfo.getMarkerIndexInfoCount(), 0);
            }
            MLDataFormats.MessageRange range = positionInfo.getIndividualDeletedMessagesList().get(0);
            assertEquals(range.getLowerEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(0).build());
            assertEquals(range.getUpperEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(1).build());
            counter++;
        }
        assertEquals(counter, 3);

        // Verify entries and marker created by flushPendingMarkDeletes
        entries = c1.getCursorLedgerHandle().readEntries(4, 7);
        counter = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            MLDataFormats.PositionInfo positionInfo = MLDataFormats.PositionInfo.parseFrom(entry.getEntryInputStream());
            if (counter == 4) {
                // the last one is marker
                assertEquals(positionInfo.getMarkerIndexInfoCount(), 3);
                AtomicLong subCounter = new AtomicLong(0);
                positionInfo.getMarkerIndexInfoList().forEach(markerIndexInfo -> {
                    assertEquals(markerIndexInfo.getEntryPosition().getLedgerId(), newLedgerId);
                    assertEquals(markerIndexInfo.getEntryPosition().getEntryId(), subCounter.getAndIncrement());
                });
                assertEquals(subCounter.get(), 4);
                continue;
            } else {
                assertEquals(positionInfo.getMarkerIndexInfoCount(), 0);
            }
            MLDataFormats.MessageRange range = positionInfo.getIndividualDeletedMessagesList().get(0);
            assertEquals(range.getLowerEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(0).build());
            assertEquals(range.getUpperEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(1).build());
            counter++;
        }
        assertEquals(counter, 4);

        // Verify cache
        assertEquals(c1.getIndividuallyDeletedMessages(), "[(0:0..0:1],(1:0..1:1],(2:0..2:1],(3:0..3:1]]");
        assertEquals(c1.getRangeMarker().size(), 4);
        RangeSetWrapper<PositionImpl> setWrapper = (RangeSetWrapper<PositionImpl>) c1.getIndividuallyDeletedMessagesSet();
        Awaitility.await().untilAsserted(() -> assertEquals(setWrapper.getLruCounter().size(), 4));
        c1.close();
        ledger.close();
    }

    /**
     * Covered chain: internalAsyncMarkDelete & Ledger is Open -> internalMarkDelete -> persistPositionToLedger
     * -> remove individualDeletedMessages
     * @throws Exception
     */
    @Test
    public void testPersistPositionToLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnableLruCacheMaxUnackedRanges(true);
        ManagedLedger ledger = factory.open("my_test_ledger" + UUID.randomUUID(), config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        //init new ledger
        c1.startCreatingNewMetadataLedger();
        Awaitility.await().untilAsserted(() -> assertEquals(c1.getState(),"Open"));
        LedgerHandle ledgerHandle = c1.getCursorLedgerHandle();
        long ledgerId = ledgerHandle.getId();
        // init IndividuallyDeletedMessages
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(0, 0, 0, 1);
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(1, 0, 1, 1);
        c1.getIndividuallyDeletedMessagesSet().addOpenClosed(2, 0, 2, 1);
        CompletableFuture<Void> future = new CompletableFuture<>();
        c1.internalMarkDelete(new ManagedCursorImpl.MarkDeleteEntry(new PositionImpl(1, 10), new HashMap<>(),
                new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null));

        future.get();
        Map<Long, MLDataFormats.NestedPositionInfo> marker = c1.getRangeMarker();
        // markDelete position is (1, 10] , so only [(2:0..2:1]] is left
        assertEquals(marker.size(), 1);
        MLDataFormats.NestedPositionInfo positionInfo = marker.get((long) 2);
        assertEquals(positionInfo.getLedgerId(), ledgerId);
        assertEquals(positionInfo.getEntryId(), 3);
        assertEquals(c1.getIndividuallyDeletedMessages(), "[(2:0..2:1]]");
        Enumeration<LedgerEntry> entries = c1.getCursorLedgerHandle().readEntries(positionInfo.getEntryId() - 2,
                positionInfo.getEntryId() + 1);
        int counter = 0;
        MLDataFormats.NestedPositionInfo.Builder positionBuilder = MLDataFormats.NestedPositionInfo.newBuilder();
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            MLDataFormats.PositionInfo position = MLDataFormats.PositionInfo.parseFrom(entry.getEntryInputStream());
            if (counter == 3) {
                // the last one is marker
                assertEquals(position.getMarkerIndexInfoCount(), 3);
                AtomicLong subCounter = new AtomicLong(positionInfo.getEntryId() - 2);
                position.getMarkerIndexInfoList().forEach(markerIndexInfo -> {
                    assertEquals(markerIndexInfo.getEntryPosition().getLedgerId(), ledgerId);
                    assertEquals(markerIndexInfo.getEntryPosition().getEntryId(), subCounter.getAndIncrement());
                });
                assertEquals(subCounter.get(), 4);
                continue;
            } else {
                assertEquals(position.getMarkerIndexInfoCount(), 0);
            }
            MLDataFormats.MessageRange range = position.getIndividualDeletedMessagesList().get(0);
            assertEquals(range.getLowerEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(0).build());
            assertEquals(range.getUpperEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(1).build());
            counter++;
        }
    }
}
