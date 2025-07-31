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
package org.apache.bookkeeper.mledger.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Iterables;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NonDurableCursorTest extends MockedBookKeeperTestCase {

    private static final Charset Encoding = StandardCharsets.UTF_8;

    @Test(timeOut = 20000)
    void readFromEmptyLedger() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 0);
        entries.forEach(Entry::release);

        ledger.addEntry("test".getBytes(Encoding));
        entries = c1.readEntries(10);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        entries = c1.readEntries(10);
        assertEquals(entries.size(), 0);
        entries.forEach(Entry::release);

        // Test string representation
        assertEquals(c1.toString(), "NonDurableCursorImpl{ledger=my_test_ledger, cursor="
                + c1.getName() + ", ackPos=3:-1, readPos=3:1}");
    }

    @Test(timeOut = 20000)
    void testOpenNonDurableCursorAtNonExistentMessageId() throws Exception {
        ManagedLedger ledger = factory.open("non_durable_cursor_at_non_existent_msgid");
        ManagedLedgerImpl mlImpl = (ManagedLedgerImpl) ledger;

        PositionImpl position = mlImpl.getLastPosition();

        ManagedCursor c1 = ledger.newNonDurableCursor(new PositionImpl(
            position.getLedgerId(),
            position.getEntryId() - 1
        ));

        assertEquals(c1.getReadPosition(), new PositionImpl(
            position.getLedgerId(),
            0
        ));

        c1.close();
        ledger.close();
    }

    @Test(timeOut = 20000)
    void testZNodeBypassed() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        assertTrue(ledger.getCursors().iterator().hasNext());

        c1.close();
        ledger.close();

        // Re-open
        ManagedLedger ledger2 = factory.open("my_test_ledger");
        assertTrue(!ledger2.getCursors().iterator().hasNext());
    }

    @Test(timeOut = 20000)
    void readTwice() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ManagedCursor c2 = ledger.newNonDurableCursor(PositionImpl.LATEST);

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(Entry::release);

        entries = c1.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(Entry::release);

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(Entry::release);

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    void readWithCacheDisabled() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ManagedCursor c2 = ledger.newNonDurableCursor(PositionImpl.LATEST);

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        assertEquals(new String(entries.get(0).getData(), Encoding), "entry-1");
        assertEquals(new String(entries.get(1).getData(), Encoding), "entry-2");
        entries.forEach(Entry::release);

        entries = c1.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(Entry::release);

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(Entry::release);

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    void readFromClosedLedger() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.LATEST);

        ledger.close();

        try {
            c1.readEntries(2);
            fail("ledger is closed, should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void testNumberOfEntries() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ManagedCursor c4 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ManagedCursor c5 = ledger.newNonDurableCursor(PositionImpl.LATEST);

        assertEquals(c1.getNumberOfEntries(), 4);
        assertTrue(c1.hasMoreEntries());

        assertEquals(c2.getNumberOfEntries(), 3);
        assertTrue(c2.hasMoreEntries());

        assertEquals(c3.getNumberOfEntries(), 2);
        assertTrue(c3.hasMoreEntries());

        assertEquals(c4.getNumberOfEntries(), 1);
        assertTrue(c4.hasMoreEntries());

        assertEquals(c5.getNumberOfEntries(), 0);
        assertFalse(c5.hasMoreEntries());

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        c1.markDelete(entries.get(1).getPosition());
        assertEquals(c1.getNumberOfEntries(), 2);
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    void testNumberOfEntriesInBacklog() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ManagedCursor c4 = ledger.newNonDurableCursor(PositionImpl.LATEST);
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ManagedCursor c5 = ledger.newNonDurableCursor(PositionImpl.LATEST);

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);
        assertEquals(c2.getNumberOfEntriesInBacklog(false), 3);
        assertEquals(c3.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c4.getNumberOfEntriesInBacklog(false), 1);
        assertEquals(c5.getNumberOfEntriesInBacklog(false), 0);

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(Entry::release);

        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);

        c1.markDelete(p1);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);

        c1.delete(p3);

        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 2);

        c1.markDelete(p4);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);
    }

    @Test(timeOut = 20000)
    void markDeleteWithErrors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 2);
        cursor.markDelete(entries.get(0).getPosition());

        stopBookKeeper();

        // Mark-delete should succeed if BK is down
        cursor.markDelete(entries.get(1).getPosition());

        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    void markDeleteAcrossLedgers() throws Exception {
        ManagedLedger ml1 = factory.open("my_test_ledger");
        ManagedCursor mc1 = ml1.openCursor("c1");

        // open ledger id 3 for ml1
        // markDeletePosition for mc1 is 3:-1
        // readPosition is 3:0

        ml1.close();
        mc1.close();

        // force removal of this ledger from the cache
        factory.close(ml1);

        ManagedLedger ml2 = factory.open("my_test_ledger");
        ManagedCursor mc2 = ml2.openCursor("c1");

        // open ledger id 5 for ml2
        // this entry is written at 5:0
        Position pos = ml2.addEntry("dummy-entry-1".getBytes(Encoding));

        List<Entry> entries = mc2.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-1");
        entries.forEach(Entry::release);

        mc2.delete(pos);

        // verify if the markDeletePosition moves from 3:-1 to 5:0
        assertEquals(mc2.getMarkDeletedPosition(), pos);
        assertEquals(mc2.getMarkDeletedPosition().getNext(), mc2.getReadPosition());
    }

    @Test(timeOut = 20000)
    void markDeleteGreaterThanLastConfirmedEntry() throws Exception {
        ManagedLedger ml1 = factory.open("my_test_ledger");
        ManagedCursor mc1 = ml1.newNonDurableCursor(PositionImpl.get(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1));
        assertEquals(mc1.getMarkDeletedPosition(), ml1.getLastConfirmedEntry());
    }

    @Test(timeOut = 20000)
    void testResetCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl lastPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        final AtomicBoolean moveStatus = new AtomicBoolean(false);
        PositionImpl resetPosition = new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() - 2);
        try {
            cursor.resetCursor(resetPosition);
            moveStatus.set(true);
        } catch (Exception e) {
            log.warn("error in reset cursor", e.getCause());
        }

        assertTrue(moveStatus.get());
        assertEquals(resetPosition, cursor.getReadPosition());
        cursor.close();
        ledger.close();
    }

    @Test(timeOut = 20000)
    void testasyncResetCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.LATEST);
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl lastPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        final AtomicBoolean moveStatus = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        PositionImpl resetPosition = new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() - 2);

        cursor.asyncResetCursor(resetPosition, false, new AsyncCallbacks.ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                moveStatus.set(true);
                countDownLatch.countDown();
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                moveStatus.set(false);
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        assertTrue(moveStatus.get());
        assertEquals(resetPosition, cursor.getReadPosition());
        cursor.close();
        ledger.close();
    }

    @Test(timeOut = 20000)
    void rewind() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));
        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        log.debug("p1: {}", p1);
        log.debug("p2: {}", p2);
        log.debug("p3: {}", p3);
        log.debug("p4: {}", p4);

        assertEquals(c1.getNumberOfEntries(), 4);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);
        c1.markDelete(p1);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 3);
        entries.forEach(Entry::release);

        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        c1.markDelete(p2);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 2);

        entries = c1.readEntries(10);
        assertEquals(entries.size(), 2);
        entries.forEach(Entry::release);

        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 2);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 2);
        c1.markDelete(p4);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 0);
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        assertEquals(c1.getNumberOfEntries(), 1);
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));
        assertEquals(c1.getNumberOfEntries(), 2);
    }

    @Test(timeOut = 20000)
    void markDeleteSkippingMessage() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl p4 = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        assertEquals(cursor.getNumberOfEntries(), 4);

        cursor.markDelete(p1);
        assertTrue(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 3);

        assertEquals(cursor.getReadPosition(), p2);

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-2");
        entries.forEach(Entry::release);

        cursor.markDelete(p4);
        assertFalse(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 0);

        assertEquals(cursor.getReadPosition(), new PositionImpl(p4.getLedgerId(), p4.getEntryId() + 1));
    }

    @Test(timeOut = 20000)
    public void asyncMarkDeleteBlocking() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMetadataMaxEntriesPerLedger(5);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        final ManagedCursor c1 = ledger.openCursor("c1");
        final AtomicReference<Position> lastPosition = new AtomicReference<Position>();

        final int N = 100;
        final CountDownLatch latch = new CountDownLatch(N);
        for (int i = 0; i < N; i++) {
            ledger.asyncAddEntry("entry".getBytes(Encoding), new AddEntryCallback() {
                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                }

                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    lastPosition.set(position);
                    c1.asyncMarkDelete(position, new MarkDeleteCallback() {
                        @Override
                        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                        }

                        @Override
                        public void markDeleteComplete(Object ctx) {
                            latch.countDown();
                        }
                    }, null);
                }
            }, null);
        }

        latch.await();

        assertEquals(c1.getNumberOfEntries(), 0);

        // Reopen
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger");
        ManagedCursor c2 = ledger.openCursor("c1");

        assertEquals(c2.getMarkDeletedPosition(), lastPosition.get());
    }

    @Test(timeOut = 20000)
    void unorderedMarkDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        final ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));

        c1.markDelete(p2);
        try {
            c1.markDelete(p1);
            fail("Should have thrown exception");
        } catch (ManagedLedgerException e) {
            // ok
        }

        assertEquals(c1.getMarkDeletedPosition(), p2);
    }

    @Test(timeOut = 20000)
    void testSingleDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(3)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.LATEST);

        Position p1 = ledger.addEntry("entry1".getBytes());
        Position p2 = ledger.addEntry("entry2".getBytes());
        Position p3 = ledger.addEntry("entry3".getBytes());
        Position p4 = ledger.addEntry("entry4".getBytes());
        Position p5 = ledger.addEntry("entry5".getBytes());
        Position p6 = ledger.addEntry("entry6".getBytes());

        Position p0 = cursor.getMarkDeletedPosition();

        cursor.delete(p4);
        assertEquals(cursor.getMarkDeletedPosition(), p0);

        cursor.delete(p1);
        assertEquals(cursor.getMarkDeletedPosition(), p1);

        cursor.delete(p3);

        // Delete will silently succeed
        cursor.delete(p3);
        assertEquals(cursor.getMarkDeletedPosition(), p1);

        cursor.delete(p2);
        assertEquals(cursor.getMarkDeletedPosition(), p4);

        cursor.delete(p5);
        assertEquals(cursor.getMarkDeletedPosition(), p5);

        cursor.close();
        try {
            cursor.delete(p6);
        } catch (ManagedLedgerException e) {
            // Ok
        }
    }

    @Test(timeOut = 20000)
    void subscribeToEarliestPositionWithImmediateDeletion() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        /* Position p1 = */ ledger.addEntry("entry-1".getBytes());
        /* Position p2 = */ ledger.addEntry("entry-2".getBytes());
        /* Position p3 = */ ledger.addEntry("entry-3".getBytes());

        Thread.sleep(300);
        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        assertEquals(c1.getReadPosition(), new PositionImpl(6, 0));
        assertEquals(c1.getMarkDeletedPosition(), new PositionImpl(6, -1));
    }

    @Test // (timeOut = 20000)
    void subscribeToEarliestPositionWithDeferredDeletion() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));

        Position p1 = ledger.addEntry("entry-1".getBytes());
        Position p2 = ledger.addEntry("entry-2".getBytes());
        /* Position p3 = */ ledger.addEntry("entry-3".getBytes());
        /* Position p4 = */ ledger.addEntry("entry-4".getBytes());
        /* Position p5 = */ ledger.addEntry("entry-5".getBytes());
        /* Position p6 = */ ledger.addEntry("entry-6".getBytes());

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        assertEquals(c1.getReadPosition(), p1);
        assertEquals(c1.getMarkDeletedPosition(), new PositionImpl(3, -1));
        assertEquals(c1.getNumberOfEntries(), 6);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 6);

        ManagedCursor c2 = ledger.newNonDurableCursor(p1);
        assertEquals(c2.getReadPosition(), p2);
        assertEquals(c2.getMarkDeletedPosition(), p1);
        assertEquals(c2.getNumberOfEntries(), 5);
        assertEquals(c2.getNumberOfEntriesInBacklog(false), 5);
    }

    @Test
    void testCursorWithNameIsCachable() throws Exception {
        final String p1CursorName = "entry-1";
        final String p2CursorName = "entry-2";
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        Position p1 = ledger.addEntry(p1CursorName.getBytes());
        Position p2 = ledger.addEntry(p2CursorName.getBytes());

        ManagedCursor c1 = ledger.newNonDurableCursor(p1, p1CursorName);
        ManagedCursor c2 = ledger.newNonDurableCursor(p1, p1CursorName);
        ManagedCursor c3 = ledger.newNonDurableCursor(p2, p2CursorName);
        ManagedCursor c4 = ledger.newNonDurableCursor(p2, p2CursorName);

        assertEquals(c1, c2);
        assertEquals(c3, c4);

        assertNotEquals(c1, c3);
        assertNotEquals(c2, c3);
        assertNotEquals(c1, c4);
        assertNotEquals(c2, c4);

        assertNotNull(c1.getName());
        assertNotNull(c2.getName());
        assertNotNull(c3.getName());
        assertNotNull(c4.getName());
        ledger.close();
    }

    @Test
    public void testGetSlowestConsumer() throws Exception {
        final String mlName = "test-get-slowest-consumer-ml";
        final String c1 = "cursor1";
        final String nc1 = "non-durable-cursor1";
        final String ncEarliest = "non-durable-cursor-earliest";

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(mlName, new ManagedLedgerConfig());
        Position p1 = ledger.addEntry(c1.getBytes(UTF_8));
        log.info("write entry 1 : pos = {}", p1);
        Position p2 = ledger.addEntry(nc1.getBytes(UTF_8));
        log.info("write entry 2 : pos = {}", p2);
        Position p3 = ledger.addEntry(nc1.getBytes(UTF_8));
        log.info("write entry 3 : pos = {}", p3);

        ManagedCursor cursor1 = ledger.openCursor(c1);
        cursor1.seek(p3);
        assertEquals(p3, ledger.getCursors().getSlowestReaderPosition());

        ManagedCursor nonCursor1 = ledger.newNonDurableCursor(p2, nc1);
        // The slowest reader should still be the durable cursor since non-durable readers are not taken into account
        assertEquals(p3, ledger.getCursors().getSlowestReaderPosition());

        PositionImpl earliestPos = new PositionImpl(-1, -2);

        ManagedCursor nonCursorEarliest = ledger.newNonDurableCursor(earliestPos, ncEarliest);

        // The slowest reader should still be the durable cursor since non-durable readers are not taken into account
        assertEquals(p3, ledger.getCursors().getSlowestReaderPosition());

        // move non-durable cursor should NOT update the slowest reader position
        nonCursorEarliest.markDelete(p1);
        assertEquals(p3, ledger.getCursors().getSlowestReaderPosition());

        nonCursorEarliest.markDelete(p2);
        assertEquals(p3, ledger.getCursors().getSlowestReaderPosition());

        nonCursorEarliest.markDelete(p3);
        assertEquals(p3, ledger.getCursors().getSlowestReaderPosition());

        nonCursor1.markDelete(p3);
        assertEquals(p3, ledger.getCursors().getSlowestReaderPosition());

        ledger.close();
    }

    @Test
    public void testBacklogStatsWhenDroppingData() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testBacklogStatsWhenDroppingData",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor nonDurableCursor = ledger.newNonDurableCursor(PositionImpl.EARLIEST);

        assertEquals(nonDurableCursor.getNumberOfEntries(), 0);
        assertEquals(nonDurableCursor.getNumberOfEntriesInBacklog(true), 0);

        List<Position> positions = new ArrayList();
        for (int i = 0; i < 10; i++) {
            positions.add(ledger.addEntry(("entry-" + i).getBytes(UTF_8)));
        }

        assertEquals(nonDurableCursor.getNumberOfEntries(), 10);
        assertEquals(nonDurableCursor.getNumberOfEntriesInBacklog(true), 10);

        c1.markDelete(positions.get(4));
        assertEquals(c1.getNumberOfEntries(), 5);
        assertEquals(c1.getNumberOfEntriesInBacklog(true), 5);

        // Since the durable cursor has moved, the data will be trimmed
        CompletableFuture<List<LedgerInfo>> promise = ledger.asyncTrimConsumedLedgers();
        promise.join();
        // The mark delete position has moved to position 4:1, and the ledger 4 only has one entry,
        // so the ledger 4 can be deleted. nonDurableCursor should has the same backlog with durable cursor.
        assertEquals(nonDurableCursor.getNumberOfEntries(), 5);
        assertEquals(nonDurableCursor.getNumberOfEntriesInBacklog(true), 5);

        c1.close();
        ledger.deleteCursor(c1.getName());
        promise = ledger.asyncTrimConsumedLedgers();
        promise.join();

        assertEquals(nonDurableCursor.getNumberOfEntries(), 0);
        assertEquals(nonDurableCursor.getNumberOfEntriesInBacklog(true), 0);

        ledger.close();
    }

    @Test
    public void testInvalidateReadHandleWithSlowNonDurableCursor() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testInvalidateReadHandleWithSlowNonDurableCursor",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(1).setRetentionTime(-1, TimeUnit.SECONDS)
                        .setRetentionSizeInMB(-1));
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor nonDurableCursor = ledger.newNonDurableCursor(PositionImpl.EARLIEST);

        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            positions.add(ledger.addEntry(("entry-" + i).getBytes(UTF_8)));
        }

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            ledger.asyncReadEntry((PositionImpl) positions.get(i), new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                }
            }, null);
        }

        latch.await();

        c1.markDelete(positions.get(4));

        CompletableFuture<List<LedgerInfo>> promise = ledger.asyncTrimConsumedLedgers();
        promise.join();

        Assert.assertTrue(ledger.ledgerCache.containsKey(positions.get(0).getLedgerId()));
        Assert.assertTrue(ledger.ledgerCache.containsKey(positions.get(1).getLedgerId()));
        Assert.assertTrue(ledger.ledgerCache.containsKey(positions.get(2).getLedgerId()));
        Assert.assertTrue(ledger.ledgerCache.containsKey(positions.get(3).getLedgerId()));
        Assert.assertTrue(ledger.ledgerCache.containsKey(positions.get(4).getLedgerId()));

        promise = new CompletableFuture<>();

        nonDurableCursor.markDelete(positions.get(3));

        ledger.internalTrimConsumedLedgers(promise);
        promise.join();

        Assert.assertFalse(ledger.ledgerCache.containsKey(positions.get(0).getLedgerId()));
        Assert.assertFalse(ledger.ledgerCache.containsKey(positions.get(1).getLedgerId()));
        Assert.assertFalse(ledger.ledgerCache.containsKey(positions.get(2).getLedgerId()));
        Assert.assertFalse(ledger.ledgerCache.containsKey(positions.get(3).getLedgerId()));
        Assert.assertTrue(ledger.ledgerCache.containsKey(positions.get(4).getLedgerId()));

        ledger.close();
    }

    @Test(expectedExceptions = NullPointerException.class)
    void testCursorWithNameIsNotNull() throws Exception {
        final String p1CursorName = "entry-1";
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        Position p1 = ledger.addEntry(p1CursorName.getBytes());

        try {
            ledger.newNonDurableCursor(p1, null);
        } catch (NullPointerException npe) {
            assertEquals(npe.getMessage(), "cursor name can't be null");
            throw npe;
        } finally {
            ledger.close();
        }
    }

    @Test
    void deleteNonDurableCursorWithName() throws Exception {
        ManagedLedger ledger = factory.open("deleteManagedLedgerWithNonDurableCursor");

        ManagedCursor c = ledger.newNonDurableCursor(PositionImpl.EARLIEST, "custom-name");
        assertEquals(Iterables.size(ledger.getCursors()), 1);

        ledger.deleteCursor(c.getName());
        assertEquals(Iterables.size(ledger.getCursors()), 0);
    }

    @Test
    public void testMessagesConsumedCounterInitializedCorrect() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testMessagesConsumedCounterInitializedCorrect",
                        new ManagedLedgerConfig().setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(1));
        Position position = ledger.addEntry("1".getBytes(Encoding));
        NonDurableCursorImpl cursor = (NonDurableCursorImpl) ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        cursor.delete(position);
        assertEquals(cursor.getMessagesConsumedCounter(), 1);
        assertTrue(cursor.getMessagesConsumedCounter() <= ledger.getEntriesAddedCounter());
        // cleanup.
        cursor.close();
        ledger.close();
    }


    private static final Logger log = LoggerFactory.getLogger(NonDurableCursorTest.class);
}
