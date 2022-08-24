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

import static org.testng.Assert.*;
import com.google.common.collect.Range;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.GetResult;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedCursorRecoverTest extends MockedBookKeeperTestCase {

    private String mlName = "t_cs_recover_ledger";

    private String cursorName = "t_cs_recover_cursor";

    @DataProvider(name = "MlConfig")
    public Object[][] MlConfigProvider() {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxUnackedRangesToPersistInMetadataStore(0);
        managedLedgerConfig.setThrottleMarkDelete(0);
        return new Object[][]{
                {managedLedgerConfig}
        };
    }

    @Test(dataProvider = "MlConfig")
    public void testRecoverByLedger(ManagedLedgerConfig config) throws Exception {
        CursorContext context = createCursorContext(config);
        long ledgerId = context.cursor.getReadPosition().getLedgerId();
        long firstEntryId = context.cursor.getReadPosition().getEntryId();
        if (context.cursor.getMarkDeletedPosition().getLedgerId() == ledgerId) {
            firstEntryId = context.cursor.getMarkDeletedPosition().getEntryId() + 1;
        }
        writeSomeEntries(100, context.ml);
        // persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor,
                createPositionList(ledgerId, firstEntryId, firstEntryId + 1, firstEntryId + 3));
        // recover and verify.
        CursorContext recoverContext = releaseAndCreateNewCursorContext(context, false);
        assertEquals(recoverContext.cursor.getMarkDeletedPosition().getEntryId(), firstEntryId + 1);
        assertEquals(recoverContext.cursor.getIndividuallyDeletedMessagesSet().size(), 1);
        Range<PositionImpl> range = recoverContext.cursor.getIndividuallyDeletedMessagesSet().firstRange();
        assertEquals(range.lowerEndpoint().getEntryId(), firstEntryId + 2);
        assertEquals(range.upperEndpoint().getEntryId(), firstEntryId + 3);
        //cleanup.
        cleanup(recoverContext);
    }

    @Test(dataProvider = "MlConfig")
    public void testRecoverByZk(ManagedLedgerConfig config) throws Exception {
        CursorContext context = createCursorContext(config);
        long ledgerId = context.cursor.getReadPosition().getLedgerId();
        long firstEntryId = context.cursor.getReadPosition().getEntryId();
        writeSomeEntries(100, context.ml);
        // persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor, PositionImpl.get(ledgerId, firstEntryId));
        // persist to ZK.
        closeCursorLedger(context.cursor);
        deleteAndWaitCursorPersistentToZk(context.cursor,
                createPositionList(ledgerId, firstEntryId + 1, firstEntryId + 3));
        // recover and verify.
        CursorContext recoverContext = releaseAndCreateNewCursorContext(context, false);
        assertEquals(recoverContext.cursor.getMarkDeletedPosition().getEntryId(), firstEntryId + 1);
        assertEquals(recoverContext.cursor.getIndividuallyDeletedMessagesSet().size(), 1);
        Range<PositionImpl> range = recoverContext.cursor.getIndividuallyDeletedMessagesSet().firstRange();
        assertEquals(range.lowerEndpoint().getEntryId(), firstEntryId + 2);
        assertEquals(range.upperEndpoint().getEntryId(), firstEntryId + 3);
        // cleanup.
        cleanup(recoverContext);
    }

    @Test(dataProvider = "MlConfig")
    public void testRecoverSkipBrokenLedger(ManagedLedgerConfig config) throws Exception {
        CursorContext context = createCursorContext(config);
        long ledgerId = context.cursor.getReadPosition().getLedgerId();
        long firstEntryId = context.cursor.getReadPosition().getEntryId();
        writeSomeEntries(100, context.ml);
        // persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor, PositionImpl.get(ledgerId, firstEntryId));
        // persist to ZK.
        closeCursorLedger(context.cursor);
        deleteAndWaitCursorPersistentToZk(context.cursor,
                createPositionList(ledgerId, firstEntryId + 1, firstEntryId + 2, firstEntryId + 4));
        // new ledger, and persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor,
                createPositionList(ledgerId, firstEntryId + 5, firstEntryId + 6, firstEntryId + 8));
        // make broken ledger, recover and verify.
        CursorContext recoverContext = releaseAndCreateNewCursorContext(context, true);
        assertEquals(recoverContext.cursor.getMarkDeletedPosition().getEntryId(), firstEntryId + 2);
        assertEquals(recoverContext.cursor.getIndividuallyDeletedMessagesSet().size(), 1);
        Range<PositionImpl> range = recoverContext.cursor.getIndividuallyDeletedMessagesSet().firstRange();
        assertEquals(range.lowerEndpoint().getEntryId(), firstEntryId + 3);
        assertEquals(range.upperEndpoint().getEntryId(), firstEntryId + 4);
        // cleanup.
        cleanup(recoverContext);
    }

    private ArrayList<Position> createPositionList(long ledgerId, long...entryIds){
        ArrayList<Position> list = new ArrayList<>();
        for (long entryId : entryIds){
            list.add(PositionImpl.get(ledgerId, entryId));
        }
        return list;
    }

    private void deleteAndWaitCursorPersistentToLedger(ManagedCursorImpl cursor, PositionImpl position)
            throws Exception {
        deleteAndWaitCursorPersistentToLedger(cursor, Collections.singletonList(position));
    }

    private void deleteAndWaitCursorPersistentToLedger(ManagedCursorImpl cursor, Iterable<Position> positions)
            throws Exception {
        AtomicInteger expectIncrementCount = new AtomicInteger(1);
        Set<String> stateSupports = new HashSet<>(Arrays.asList("Open", "NoLedger"));
        if (!stateSupports.contains(cursor.getState())) {
            throw new RuntimeException("no support for: " + cursor.getState());
        }
        // The Ledger creation will trigger a copy Info write.
        if ("NoLedger".equals(cursor.getState())) {
            expectIncrementCount.incrementAndGet();
        }
        long originalCount = cursor.getStats().getPersistLedgerSucceed();
        cursor.delete(positions);
        Awaitility.await().until(() ->
                cursor.getStats().getPersistLedgerSucceed() == originalCount + expectIncrementCount.get());
    }

    private void deleteAndWaitCursorPersistentToZk(ManagedCursorImpl cursor, PositionImpl position) throws Exception {
        deleteAndWaitCursorPersistentToZk(cursor, Collections.singletonList(position));
    }

    private void deleteAndWaitCursorPersistentToZk(ManagedCursorImpl cursor, Iterable<Position> positions)
            throws Exception {
        long originalCount = cursor.getStats().getPersistZookeeperSucceed();
        cursor.delete(positions);
        Awaitility.await().until(() -> cursor.getStats().getPersistZookeeperSucceed() == originalCount + 1);
    }

    private void cleanup(CursorContext context) throws Exception {
        Set<Long> createdLedgerIdSet = new HashSet<>();
        createdLedgerIdSet.addAll(context.ml.getLedgersInfo().keySet());
        createdLedgerIdSet.add(context.cursor.getCursorLedger());
        // release.
        context.release();
        // delete ledgers.
        for (long ledgerId : createdLedgerIdSet) {
            try {
                bkc.deleteLedger(ledgerId);
            }catch (BKException.BKNoSuchLedgerExistsException e){
                // ignore.
            }
        }
        // delete bk nodes.
        GetResult cursorResult = metadataStore.get(String.format("/managed-ledgers/%s/%s", mlName, cursorName))
                .join().get();
        metadataStore.delete(cursorResult.getStat().getPath(), Optional.of(cursorResult.getStat().getVersion()))
                .join();
        GetResult mlResult = metadataStore.get(String.format("/managed-ledgers/%s", mlName)).join().get();
        metadataStore.delete(mlResult.getStat().getPath(), Optional.of(mlResult.getStat().getVersion())).join();

    }

    private void writeSomeEntries(int entryCount, ManagedLedgerImpl managedLedger) throws Exception {
        for (int i = 0; i < entryCount; i++) {
            managedLedger.addEntry(String.valueOf(i).toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    private CursorContext createCursorContext(ManagedLedgerConfig managedLedgerConfig) throws Exception {
        ManagedLedgerImpl managedLedger =
                (ManagedLedgerImpl) factory.open(mlName, managedLedgerConfig);
        ManagedCursorImpl managedCursor =
                (ManagedCursorImpl) managedLedger.openCursor(cursorName);
        return new CursorContext(managedLedger, managedCursor);
    }

    private CursorContext releaseAndCreateNewCursorContext(CursorContext cursorContext, boolean makeBrokenLedger)
            throws Exception {
        long lastCursorLedger = cursorContext.cursor.getCursorLedger();
        // release old.
        ManagedLedgerConfig config = cursorContext.ml.getConfig();
        cursorContext.release();
        // delete last cursor-ledger.
        if (makeBrokenLedger){
            bkc.deleteLedger(lastCursorLedger);
        }
        // create new.
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open(mlName, config);
        Iterator<ManagedCursor> cursorIterators = managedLedger.getCursors().iterator();
        assertTrue(cursorIterators.hasNext());
        ManagedCursorImpl managedCursor = (ManagedCursorImpl) cursorIterators.next();
        assertEquals(managedCursor.getName(), cursorName);
        return new CursorContext(managedLedger, managedCursor);
    }

    private static void closeCursorLedger(ManagedCursorImpl managedCursor) {
        Awaitility.await().until(() -> {
            LedgerHandle ledgerHandle = Whitebox.getInternalState(managedCursor, "cursorLedger");
            if (ledgerHandle == null) {
                return false;
            }
            ledgerHandle.close();
            return true;
        });
    }

    @AllArgsConstructor
    private static class CursorContext {

        private ManagedLedgerImpl ml;

        private ManagedCursorImpl cursor;

        private void release() throws Exception {
            ml.close();
        }
    }
}
