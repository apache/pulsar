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
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import static org.testng.AssertJUnit.assertEquals;

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

        Map<Long, List<MLDataFormats.MessageRange>> rangeGroupByLedgerId = c1.getRangeGroupByLedgerId();

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

        Map<Long, List<MLDataFormats.BatchedEntryDeletionIndexInfo>> map = c1.getDeletionIndexInfosGroupByLedgerId();

        assertEquals(map.size(), 3);
        AtomicLong count = new AtomicLong(0);
        map.forEach((key, value) -> {
            assertEquals(key.longValue(), count.get());
            assertEquals(value.size(), 1);
            assertEquals(value.get(0).getPosition().getLedgerId(), count.getAndIncrement());
            assertEquals(value.get(0).getPosition().getEntryId(), 0);
        });

        c1.close();
        ledger.close();
        list.forEach(BitSetRecyclable::recycle);
    }

    @Test
    public void testCopyLruEntriesToNewLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnableLruCacheMaxUnackedRanges(true);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
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
        Map<Long, List<MLDataFormats.MessageRange>> rangeGroupByLedgerId = c1.getRangeGroupByLedgerId();
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
                new PositionImpl(1, 2), new HashMap<>(), null, null));
        // trigger switch and make sure it is finished
        c1.startCreatingNewMetadataLedger();
        Awaitility.await().untilAsserted(() -> assertEquals(c1.getState(),"Open"));

        // validate marker
        assertEquals(c1.getRangeMarker().size(), 3);
        AtomicLong count = new AtomicLong(0);
        long newLedgerId = ledgerId + 1;
        c1.getRangeMarker().forEach((key, value) -> {
            assertEquals(key.longValue(), count.get());
            assertEquals(value.getLedgerId(), newLedgerId);
            assertEquals(value.getEntryId(), count.getAndIncrement());
        });

        // validate entry
        Enumeration<LedgerEntry> entries = c1.getCursorLedgerHandle().readEntries(0, 3);
        int counter = 0;
        MLDataFormats.NestedPositionInfo.Builder positionBuilder = MLDataFormats.NestedPositionInfo.newBuilder();
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            MLDataFormats.PositionInfo positionInfo = MLDataFormats.PositionInfo.parseFrom(entry.getEntryInputStream());
            System.out.println(positionInfo.getIndividualDeletedMessagesList());
            if (counter == 3) {
                //marker
                assertEquals(positionInfo.getMarkerIndexInfoCount(), 3);
                AtomicLong subCounter = new AtomicLong(0);
                positionInfo.getMarkerIndexInfoList().forEach(markerIndexInfo -> {
                    assertEquals(markerIndexInfo.getEntryPosition().getLedgerId(), newLedgerId);
                    assertEquals(markerIndexInfo.getEntryPosition().getEntryId(), subCounter.getAndIncrement());
                });
                continue;
            } else {
                assertEquals(positionInfo.getMarkerIndexInfoCount(), 0);
            }
            MLDataFormats.MessageRange range = positionInfo.getIndividualDeletedMessagesList().get(0);
            assertEquals(range.getLowerEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(0).build());
            assertEquals(range.getUpperEndpoint(), positionBuilder.setLedgerId(counter).setEntryId(1).build());
            counter++;
        }

        c1.close();
        ledger.close();
    }
}
