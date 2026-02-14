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

import static org.apache.bookkeeper.client.api.BKException.Code.*;
import static org.testng.Assert.*;
import static org.mockito.Mockito.*;
import com.google.common.collect.Range;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedCursorRecoverTest extends MockedBookKeeperTestCase {

    private String mlName = "t_cs_recover_ledger";

    private String cursorName = "t_cs_recover_cursor";

    private ManagedLedgerConfig config = new ManagedLedgerConfig();

    {
        config.setMaxUnackedRangesToPersistInMetadataStore(0);
        config.setThrottleMarkDelete(0);
    }

    @Test
    public void testRecoverByLedger() throws Exception {
        CursorContext context = createCursorContext(config);
        long ledgerId = context.cursor.getReadPosition().getLedgerId();
        long firstEntryId = context.cursor.getReadPosition().getEntryId();
        if (context.cursor.getMarkDeletedPosition().getLedgerId() == ledgerId) {
            firstEntryId = context.cursor.getMarkDeletedPosition().getEntryId() + 1;
        }
        writeSomeEntries(100, context.ml);
        // persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor, ledgerId,
                firstEntryId, firstEntryId + 1, firstEntryId + 3);
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

    @Test
    public void testRecoverByLedgerWithLedgerReadTimeoutException() throws Exception {
        CursorContext context = createCursorContext(config);
        writeSomeEntries(100, context.ml);
        long ledgerId = context.cursor.getReadPosition().getLedgerId();
        long firstEntryId = context.cursor.getReadPosition().getEntryId();
        // make individual ack.
        deleteAndWaitCursorPersistentToLedger(context.cursor, ledgerId, firstEntryId);
        deleteAndWaitCursorPersistentToLedger(context.cursor, ledgerId, firstEntryId + 4);

        // Mock LedgerRecoveryException 1 times.
        BookKeeper mockBookkeeper = mockBookKeeperForLedgerReadTimeoutException(context.cursor, 1);
        final ManagedCursorImpl recoverCursor =
                new ManagedCursorImpl(mockBookkeeper, config, context.cursor.ledger, cursorName);

        // The first recover fail by timeout ex.
        try {
            recoverAndWait(recoverCursor);
            fail("Expect timeout ex");
        } catch (Exception ex){
            assertTrue(ex.getCause() instanceof ManagedLedgerException);
            assertTrue(BKException.getMessage(BKException.Code.TimeoutException).equals(ex.getCause().getMessage()));
        }

        // The second recover will success and no data lost.
        recoverAndWait(recoverCursor);
        // verify.
        assertEquals(recoverCursor.markDeletePosition.getEntryId(), firstEntryId);
        assertEquals(recoverCursor.getIndividuallyDeletedMessagesSet().size(), 1);
        Range<PositionImpl> range = recoverCursor.getIndividuallyDeletedMessagesSet().firstRange();
        assertEquals(range.lowerEndpoint().getEntryId(), firstEntryId + 3);
        assertEquals(range.upperEndpoint().getEntryId(), firstEntryId + 4);

        // cleanup.
        cleanup(context);
    }

    @Test
    public void testRecoverByZk() throws Exception {
        CursorContext context = createCursorContext(config);
        long ledgerId = context.cursor.getReadPosition().getLedgerId();
        long firstEntryId = context.cursor.getReadPosition().getEntryId();
        writeSomeEntries(100, context.ml);
        // persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor, ledgerId, firstEntryId);
        // persist to ZK.
        closeCursorLedger(context.cursor);
        deleteAndWaitCursorPersistentToZk(context.cursor, ledgerId, firstEntryId + 1);
        // recover and verify.
        CursorContext recoverContext = releaseAndCreateNewCursorContext(context, false);
        assertEquals(recoverContext.cursor.getMarkDeletedPosition().getEntryId(), firstEntryId + 1);
        assertEquals(recoverContext.cursor.getIndividuallyDeletedMessagesSet().size(), 0);
        // cleanup.
        cleanup(recoverContext);
    }

    @Test
    public void testRecoverSkipBrokenLedger() throws Exception {
        CursorContext context = createCursorContext(config);
        long ledgerId = context.cursor.getReadPosition().getLedgerId();
        long firstEntryId = context.cursor.getReadPosition().getEntryId();
        writeSomeEntries(100, context.ml);
        // persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor, ledgerId, firstEntryId);
        // persist to ZK.
        closeCursorLedger(context.cursor);
        deleteAndWaitCursorPersistentToZk(context.cursor, ledgerId,
                firstEntryId + 1, firstEntryId + 2, firstEntryId + 4);
        // new ledger, and persist to ledger.
        deleteAndWaitCursorPersistentToLedger(context.cursor, ledgerId,
                firstEntryId + 5, firstEntryId + 6, firstEntryId + 8);
        // make broken ledger, recover and verify.
        CursorContext recoverContext = releaseAndCreateNewCursorContext(context, true);
        assertEquals(recoverContext.cursor.getMarkDeletedPosition().getEntryId(), firstEntryId + 2);
        // Why "individuallyDeletedMessagesSet().size() == 0"? Because it will rewrite to empty when switch ledger.
        assertEquals(recoverContext.cursor.getIndividuallyDeletedMessagesSet().size(), 0);
        // cleanup.
        cleanup(recoverContext);
    }

    private LedgerHandle mockLedgerHandle(ArrayList<LedgerEntry> entryList, long lastEntryId) {

        final LedgerHandle mockedLedgerHandle = mock(LedgerHandle.class);
        when(mockedLedgerHandle.getLastAddConfirmed()).thenReturn(lastEntryId);
        AtomicInteger atomicInteger = new AtomicInteger();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallback.ReadCallback cb = (AsyncCallback.ReadCallback) invocation.getArguments()[2];
                Object ctx = invocation.getArguments()[3];
                int i = atomicInteger.incrementAndGet();
                Enumeration<LedgerEntry> entryEnumeration = IteratorUtils.asEnumeration(
                        Collections.singletonList(entryList.get(entryList.size() - i)).iterator());
                cb.readComplete(OK, mockedLedgerHandle, entryEnumeration, ctx);
                return null;
            }
        }).when(mockedLedgerHandle).asyncReadEntries(anyLong(), anyLong(), any(), any());
        return mockedLedgerHandle;
    }

    private void recoverAndWait(ManagedCursorImpl cursor) {
        CompletableFuture<Void> completableFuture = new CompletableFuture();
        cursor.recover(new ManagedCursorImpl.VoidCallback() {
            @Override
            public void operationComplete() {
                completableFuture.complete(null);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                completableFuture.completeExceptionally(exception);
            }
        });
        completableFuture.join();
    }

    private BookKeeper mockBookKeeperForLedgerReadTimeoutException(ManagedCursorImpl originCursor, int exCount)
            throws Exception {
        BookKeeper mockBookkeeper = mock(BookKeeper.class);
        Enumeration<LedgerEntry> ledgerEntryEnumeration =
                originCursor.cursorLedger.readEntries(0, originCursor.cursorLedger.getLastAddConfirmed());
        ArrayList<LedgerEntry> originalCursorData = new ArrayList<>();
        Iterator<LedgerEntry> iterator = ledgerEntryEnumeration.asIterator();
        while (iterator.hasNext()) {
            originalCursorData.add(iterator.next());
        }
        LedgerHandle mockLedgerHandle =
                mockLedgerHandle(originalCursorData, originCursor.cursorLedger.getLastAddConfirmed());

        AtomicInteger exCounter = new AtomicInteger(exCount);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                AsyncCallback.OpenCallback openCallback = (AsyncCallback.OpenCallback) invocation.getArguments()[3];
                Object ctx = invocation.getArguments()[4];
                if (exCounter.decrementAndGet() >= 0){
                    openCallback.openComplete(BKException.Code.TimeoutException, mockLedgerHandle, ctx);
                } else {
                    openCallback.openComplete(BKException.Code.OK, mockLedgerHandle, ctx);
                }

                return null;
            }
        }).when(mockBookkeeper).asyncOpenLedger(anyLong(), any(BookKeeper.DigestType.class),
                any(), any(), any());
        return mockBookkeeper;
    }

    private ArrayList<Position> createPositionList(long ledgerId, long... entryIds) {
        ArrayList<Position> list = new ArrayList<>();
        for (long entryId : entryIds) {
            list.add(PositionImpl.get(ledgerId, entryId));
        }
        return list;
    }

    private void deleteAndWaitCursorPersistentToLedger(ManagedCursorImpl cursor, long ledgerId, long... entryIds)
            throws Exception {
        Iterable<Position> positions = createPositionList(ledgerId, entryIds);
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

    private void deleteAndWaitCursorPersistentToZk(ManagedCursorImpl cursor, long ledgerId, long... entryIds)
            throws Exception {
        Iterable<Position> positions = createPositionList(ledgerId, entryIds);
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
            } catch (BKException.BKNoSuchLedgerExistsException e) {
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

    private CursorContext releaseAndCreateNewCursorContext(CursorContext cursorContext, boolean deleteLastValidLedger)
            throws Exception {
        long lastCursorLedger = cursorContext.cursor.getCursorLedger();
        // release old.
        ManagedLedgerConfig config = cursorContext.ml.getConfig();
        cursorContext.release();
        // delete last cursor-ledger.
        if (deleteLastValidLedger) {
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
            LedgerHandle ledgerHandle = managedCursor.cursorLedger;
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
