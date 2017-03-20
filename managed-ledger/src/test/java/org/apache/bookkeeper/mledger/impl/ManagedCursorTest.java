/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.impl.MetaStoreImplZookeeper.ZNodeProtobufFormat;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ManagedCursorTest extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;
    
    @Factory(dataProvider = "protobufFormat")
    public ManagedCursorTest(ZNodeProtobufFormat protobufFormat) {
        super();
        this.protobufFormat = protobufFormat;
    }

    @Test(timeOut = 20000)
    void readFromEmptyLedger() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor c1 = ledger.openCursor("c1");
        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());

        ledger.addEntry("test".getBytes(Encoding));
        entries = c1.readEntries(10);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        entries = c1.readEntries(10);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());

        // Test string representation
        assertEquals(c1.toString(), "ManagedCursorImpl{ledger=my_test_ledger, name=c1, ackPos=3:-1, readPos=3:1}");
    }

    @Test(timeOut = 20000)
    void readTwice() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

        entries = c1.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());
    }

    @Test(timeOut = 20000)
    void readWithCacheDisabled() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle(), config);
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        assertEquals(new String(entries.get(0).getData(), Encoding), "entry-1");
        assertEquals(new String(entries.get(1).getData(), Encoding), "entry-2");
        entries.forEach(e -> e.release());

        entries = c1.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

        entries = c2.readEntries(2);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());
    }

    @Test(timeOut = 20000)
    void getEntryDataTwice() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-1".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 1);

        Entry entry = entries.get(0);
        assertEquals(entry.getLength(), "entry-1".length());
        byte[] data1 = entry.getData();
        byte[] data2 = entry.getData();
        assertEquals(data1, data2);
        entry.release();
    }

    @Test(timeOut = 20000)
    void readFromClosedLedger() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");

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
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ManagedCursor c4 = ledger.openCursor("c4");
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ManagedCursor c5 = ledger.openCursor("c5");

        assertEquals(c1.getNumberOfEntries(), 4);
        assertEquals(c1.hasMoreEntries(), true);

        assertEquals(c2.getNumberOfEntries(), 3);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntries(), 2);
        assertEquals(c3.hasMoreEntries(), true);

        assertEquals(c4.getNumberOfEntries(), 1);
        assertEquals(c4.hasMoreEntries(), true);

        assertEquals(c5.getNumberOfEntries(), 0);
        assertEquals(c5.hasMoreEntries(), false);

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        c1.markDelete(entries.get(1).getPosition());
        assertEquals(c1.getNumberOfEntries(), 2);
        entries.forEach(e -> e.release());
    }

    @Test(timeOut = 20000)
    void testNumberOfEntriesInBacklog() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        ManagedCursor c1 = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ManagedCursor c4 = ledger.openCursor("c4");
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ManagedCursor c5 = ledger.openCursor("c5");

        assertEquals(c1.getNumberOfEntriesInBacklog(), 4);
        assertEquals(c2.getNumberOfEntriesInBacklog(), 3);
        assertEquals(c3.getNumberOfEntriesInBacklog(), 2);
        assertEquals(c4.getNumberOfEntriesInBacklog(), 1);
        assertEquals(c5.getNumberOfEntriesInBacklog(), 0);

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 4);

        c1.markDelete(p1);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);

        c1.delete(p3);

        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);

        c1.markDelete(p4);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
    }

    @Test(timeOut = 20000)
    void testNumberOfEntriesWithReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");

        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.hasMoreEntries(), true);

        assertEquals(c2.getNumberOfEntries(), 1);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntries(), 0);
        assertEquals(c3.hasMoreEntries(), false);

        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void asyncReadWithoutErrors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertNull(ctx);
                assertEquals(entries.size(), 1);
                entries.forEach(e -> e.release());
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    void asyncReadWithErrors() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        stopBookKeeper();

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                entries.forEach(e -> e.release());
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                fail("async-call should not have failed");
            }

        }, null);

        counter.await();

        cursor.rewind();

        // Clear the cache to force reading from BK
        ledger.entryCache.clear();

        final CountDownLatch counter2 = new CountDownLatch(1);

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("async-call should have failed");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter2.countDown();
            }

        }, null);

        counter2.await();
    }

    @Test(timeOut = 20000, expectedExceptions = IllegalArgumentException.class)
    void asyncReadWithInvalidParameter() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        stopBookKeeper();

        cursor.asyncReadEntries(0, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("async-call should have failed");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    void markDeleteWithErrors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        List<Entry> entries = cursor.readEntries(100);

        stopBookKeeper();
        assertEquals(entries.size(), 1);

        try {
            cursor.markDelete(entries.get(0).getPosition());
            fail("call should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        entries.forEach(e -> e.release());
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
        entries.forEach(e -> e.release());

        mc2.delete(pos);

        // verify if the markDeletePosition moves from 3:-1 to 5:0
        assertEquals(mc2.getMarkDeletedPosition(), pos);
        assertEquals(mc2.getMarkDeletedPosition().getNext(), mc2.getReadPosition());
    }

    @Test(timeOut = 20000)
    void testResetCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.openCursor("trc1");
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
        assertTrue(cursor.getReadPosition().equals(resetPosition));
        cursor.close();
        ledger.close();
    }

    @Test(timeOut = 20000)
    void testasyncResetCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.openCursor("tarc1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl lastPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        final AtomicBoolean moveStatus = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        PositionImpl resetPosition = new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() - 2);

        cursor.asyncResetCursor(resetPosition, new AsyncCallbacks.ResetCursorCallback() {
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
        assertTrue(cursor.getReadPosition().equals(resetPosition));
        cursor.close();
        ledger.close();
    }

    @Test(timeOut = 20000)
    void testConcurrentResetCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_concurrent_move_ledger");

        final int Messages = 100;
        final int Consumers = 5;

        List<Future<AtomicBoolean>> futures = Lists.newArrayList();
        ExecutorService executor = Executors.newCachedThreadPool();
        final CyclicBarrier barrier = new CyclicBarrier(Consumers + 1);

        for (int i = 0; i < Messages; i++) {
            ledger.addEntry("test".getBytes());
        }
        final PositionImpl lastPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        for (int i = 0; i < Consumers; i++) {
            final ManagedCursor cursor = ledger.openCursor("tcrc" + i);
            final int idx = i;

            futures.add(executor.submit(new Callable<AtomicBoolean>() {
                public AtomicBoolean call() throws Exception {
                    barrier.await();

                    final AtomicBoolean moveStatus = new AtomicBoolean(false);
                    CountDownLatch countDownLatch = new CountDownLatch(1);
                    final PositionImpl resetPosition = new PositionImpl(lastPosition.getLedgerId(),
                            lastPosition.getEntryId() - (5 * idx));

                    cursor.asyncResetCursor(resetPosition, new AsyncCallbacks.ResetCursorCallback() {
                        @Override
                        public void resetComplete(Object ctx) {
                            moveStatus.set(true);
                            PositionImpl pos = (PositionImpl) ctx;
                            log.info("move to [{}] completed for consumer [{}]", pos.toString(), idx);
                            countDownLatch.countDown();
                        }

                        @Override
                        public void resetFailed(ManagedLedgerException exception, Object ctx) {
                            moveStatus.set(false);
                            PositionImpl pos = (PositionImpl) ctx;
                            log.warn("move to [{}] failed for consumer [{}]", pos.toString(), idx);
                            countDownLatch.countDown();
                        }
                    });
                    countDownLatch.await();
                    assertTrue(cursor.getReadPosition().equals(resetPosition));
                    cursor.close();

                    return moveStatus;
                }
            }));
        }

        barrier.await();

        for (Future<AtomicBoolean> f : futures) {
            assertTrue(f.get().get());
        }
        ledger.close();
    }

    @Test(timeOut = 20000)
    void seekPosition() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl lastPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        cursor.seek(new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() - 1));
    }

    @Test(timeOut = 20000)
    void seekPosition2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        PositionImpl seekPosition = (PositionImpl) ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.seek(new PositionImpl(seekPosition.getLedgerId(), seekPosition.getEntryId()));
    }

    @Test(timeOut = 20000)
    void seekPosition3() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl seekPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        Position entry5 = ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        Position entry6 = ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.seek(new PositionImpl(seekPosition.getLedgerId(), seekPosition.getEntryId()));

        assertEquals(cursor.getReadPosition(), seekPosition);
        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-4");
        entries.forEach(e -> e.release());

        cursor.seek(entry5.getNext());
        assertEquals(cursor.getReadPosition(), entry6);
        entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-6");
        entries.forEach(e -> e.release());
    }

    @Test(timeOut = 20000)
    void seekPosition4() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.markDelete(p1);
        assertEquals(cursor.getMarkDeletedPosition(), p1);
        assertEquals(cursor.getReadPosition(), p2);

        List<Entry> entries = cursor.readEntries(2);
        entries.forEach(e -> e.release());

        cursor.seek(p2);
        assertEquals(cursor.getMarkDeletedPosition(), p1);
        assertEquals(cursor.getReadPosition(), p2);
    }

    @Test(timeOut = 20000)
    void rewind() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor c1 = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        log.debug("p1: {}", p1);
        log.debug("p2: {}", p2);
        log.debug("p3: {}", p3);
        log.debug("p4: {}", p4);

        assertEquals(c1.getNumberOfEntries(), 4);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 4);
        c1.markDelete(p1);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);
        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 3);
        entries.forEach(e -> e.release());

        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);
        c1.markDelete(p2);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);

        entries = c1.readEntries(10);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 2);
        c1.markDelete(p4);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
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
        ManagedCursor cursor = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl p4 = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        assertEquals(cursor.getNumberOfEntries(), 4);

        cursor.markDelete(p1);
        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 3);

        assertEquals(cursor.getReadPosition(), p2);

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-2");
        entries.forEach(e -> e.release());

        cursor.markDelete(p4);
        assertEquals(cursor.hasMoreEntries(), false);
        assertEquals(cursor.getNumberOfEntries(), 0);

        assertEquals(cursor.getReadPosition(), new PositionImpl(p4.getLedgerId(), p4.getEntryId() + 1));
    }

    @Test(timeOut = 20000)
    void removingCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        assertEquals(cursor.getNumberOfEntries(), 6);
        assertEquals(ledger.getNumberOfEntries(), 6);
        ledger.deleteCursor("c1");

        // Verify that it's a new empty cursor
        cursor = ledger.openCursor("c1");
        assertEquals(cursor.getNumberOfEntries(), 0);
        ledger.addEntry("dummy-entry-7".getBytes(Encoding));

        // Verify that GC trimming kicks in
        while (ledger.getNumberOfEntries() > 2) {
            Thread.sleep(10);
        }
    }

    @Test(timeOut = 20000)
    void cursorPersistence() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(3);
        Position p1 = entries.get(2).getPosition();
        c1.markDelete(p1);
        entries.forEach(e -> e.release());

        entries = c1.readEntries(4);
        Position p2 = entries.get(2).getPosition();
        c2.markDelete(p2);
        entries.forEach(e -> e.release());

        // Reopen

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");

        assertEquals(c1.getMarkDeletedPosition(), p1);
        assertEquals(c2.getMarkDeletedPosition(), p2);
        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void cursorPersistence2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMetadataMaxEntriesPerLedger(1));
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");
        ManagedCursor c3 = ledger.openCursor("c3");
        Position p0 = c3.getMarkDeletedPosition();
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c4 = ledger.openCursor("c4");
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        Position p5 = ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        c1.markDelete(p1);
        c1.markDelete(p2);
        c1.markDelete(p3);
        c1.markDelete(p4);
        c1.markDelete(p5);

        c2.markDelete(p1);

        // Reopen

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");
        c4 = ledger.openCursor("c4");

        assertEquals(c1.getMarkDeletedPosition(), p5);
        assertEquals(c2.getMarkDeletedPosition(), p1);
        assertEquals(c3.getMarkDeletedPosition(), p0);
        assertEquals(c4.getMarkDeletedPosition(), p1);
        factory2.shutdown();
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
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                }

                public void addComplete(Position position, Object ctx) {
                    lastPosition.set(position);
                    c1.asyncMarkDelete(position, new MarkDeleteCallback() {
                        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                        }

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
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger");
        ManagedCursor c2 = ledger.openCursor("c1");

        assertEquals(c2.getMarkDeletedPosition(), lastPosition.get());
        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void cursorPersistenceAsyncMarkDeleteSameThread() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMetadataMaxEntriesPerLedger(5));
        final ManagedCursor c1 = ledger.openCursor("c1");

        final int N = 100;
        List<Position> positions = Lists.newArrayList();
        for (int i = 0; i < N; i++) {
            Position p = ledger.addEntry("dummy-entry".getBytes(Encoding));
            positions.add(p);
        }

        Position lastPosition = positions.get(N - 1);

        final CountDownLatch latch = new CountDownLatch(N);
        for (final Position p : positions) {
            c1.asyncMarkDelete(p, new MarkDeleteCallback() {
                public void markDeleteComplete(Object ctx) {
                    latch.countDown();
                }

                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Failed to markdelete", exception);
                    latch.countDown();
                }
            }, null);
        }

        latch.await();

        // Reopen
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger");
        ManagedCursor c2 = ledger.openCursor("c1");

        assertEquals(c2.getMarkDeletedPosition(), lastPosition);
        factory2.shutdown();
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
    void unorderedAsyncMarkDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        final ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));

        final CountDownLatch latch = new CountDownLatch(2);
        c1.asyncMarkDelete(p2, new MarkDeleteCallback() {
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                fail();
            }

            public void markDeleteComplete(Object ctx) {
                latch.countDown();
            }
        }, null);

        c1.asyncMarkDelete(p1, new MarkDeleteCallback() {
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }

            public void markDeleteComplete(Object ctx) {
                fail();
            }
        }, null);

        latch.await();

        assertEquals(c1.getMarkDeletedPosition(), p2);
    }

    @Test(timeOut = 20000)
    void deleteCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntries(), 2);

        // Remove and recreate the same cursor
        ledger.deleteCursor("c1");

        try {
            c1.readEntries(10);
            fail("must fail, the cursor should be closed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            c1.markDelete(p2);
            fail("must fail, the cursor should be closed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        c1 = ledger.openCursor("c1");
        assertEquals(c1.getNumberOfEntries(), 0);

        c1.close();
        try {
            c1.readEntries(10);
            fail("must fail, the cursor should be closed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        c1.close();
    }

    @Test(timeOut = 20000)
    void errorCreatingCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        bkc.failAfter(1, BKException.Code.NotEnoughBookiesException);
        try {
            ledger.openCursor("c1");
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        Position p1 = ledger.addEntry("entry".getBytes());
        ledger.addEntry("entry".getBytes());
        ManagedCursor c1 = ledger.openCursor("c1");
        Position p3 = ledger.addEntry("entry".getBytes());

        assertEquals(c1.getReadPosition(), p3);

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        bkc.failAfter(3, BKException.Code.LedgerRecoveryException);

        ledger = factory2.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        // Verify the ManagedCursor was rewind back to the snapshotted position
        assertEquals(c1.getReadPosition(), p3);
        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        bkc.failAfter(4, BKException.Code.MetadataVersionException);

        try {
            ledger = factory2.open("my_test_ledger");
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor3() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        Position p1 = ledger.addEntry("entry".getBytes());
        ledger.addEntry("entry".getBytes());
        ManagedCursor c1 = ledger.openCursor("c1");
        Position p3 = ledger.addEntry("entry".getBytes());

        assertEquals(c1.getReadPosition(), p3);

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        bkc.failAfter(4, BKException.Code.ReadException);

        ledger = factory2.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        // Verify the ManagedCursor was rewind back to the snapshotted position
        assertEquals(c1.getReadPosition(), p3);
        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void testSingleDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(3));
        ManagedCursor cursor = ledger.openCursor("c1");

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
    void testFilteringReadEntries() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(3));
        ManagedCursor cursor = ledger.openCursor("c1");

        /* Position p1 = */ledger.addEntry("entry1".getBytes());
        /* Position p2 = */ledger.addEntry("entry2".getBytes());
        /* Position p3 = */ledger.addEntry("entry3".getBytes());
        /* Position p4 = */ledger.addEntry("entry4".getBytes());
        Position p5 = ledger.addEntry("entry5".getBytes());
        /* Position p6 = */ledger.addEntry("entry6".getBytes());

        assertEquals(cursor.getNumberOfEntries(), 6);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 6);

        List<Entry> entries = cursor.readEntries(3);
        assertEquals(entries.size(), 3);
        entries.forEach(e -> e.release());

        assertEquals(cursor.getNumberOfEntries(), 3);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 6);

        log.info("Deleting {}", p5);
        cursor.delete(p5);

        assertEquals(cursor.getNumberOfEntries(), 2);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 5);

        entries = cursor.readEntries(3);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());
        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 5);
    }

    @Test(timeOut = 20000)
    void testReadingAllFilteredEntries() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(3));
        ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");

        ledger.addEntry("entry1".getBytes());
        Position p2 = ledger.addEntry("entry2".getBytes());
        Position p3 = ledger.addEntry("entry3".getBytes());
        Position p4 = ledger.addEntry("entry4".getBytes());
        Position p5 = ledger.addEntry("entry5".getBytes());

        c2.readEntries(1).get(0).release();
        c2.delete(p2);
        c2.delete(p3);

        List<Entry> entries = c2.readEntries(2);
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getPosition(), p4);
        assertEquals(entries.get(1).getPosition(), p5);
        entries.forEach(e -> e.release());
    }

    @Test(timeOut = 20000)
    void testCountingWithDeletedEntries() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        /* Position p2 = */ledger.addEntry("entry2".getBytes());
        /* Position p3 = */ledger.addEntry("entry3".getBytes());
        /* Position p4 = */ledger.addEntry("entry4".getBytes());
        Position p5 = ledger.addEntry("entry5".getBytes());
        Position p6 = ledger.addEntry("entry6".getBytes());
        Position p7 = ledger.addEntry("entry7".getBytes());
        Position p8 = ledger.addEntry("entry8".getBytes());

        assertEquals(cursor.getNumberOfEntries(), 8);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 8);

        cursor.delete(p8);
        assertEquals(cursor.getNumberOfEntries(), 7);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 7);

        cursor.delete(p1);
        assertEquals(cursor.getNumberOfEntries(), 6);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 6);

        cursor.delete(p7);
        cursor.delete(p6);
        cursor.delete(p5);
        assertEquals(cursor.getNumberOfEntries(), 3);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 3);
    }

    @Test(timeOut = 20000)
    void testMarkDeleteTwice() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        cursor.markDelete(p1);
        cursor.markDelete(p1);

        assertEquals(cursor.getMarkDeletedPosition(), p1);
    }

    @Test(timeOut = 20000)
    void testSkipEntries() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        Position pos;

        ManagedCursor c1 = ledger.openCursor("c1");

        // test skip on empty ledger
        pos = c1.getReadPosition();
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        assertEquals(c1.getReadPosition(), pos);

        pos = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        pos = ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        // skip entries in same ledger
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        assertEquals(c1.getNumberOfEntries(), 1);

        // skip entries until end of ledger
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getReadPosition(), pos.getNext());
        assertEquals(c1.getMarkDeletedPosition(), pos);

        // skip entries across ledgers
        for (int i = 0; i < 6; i++) {
            pos = ledger.addEntry("dummy-entry".getBytes(Encoding));
        }

        c1.skipEntries(5, IndividualDeletedEntries.Exclude);
        assertEquals(c1.getNumberOfEntries(), 1);

        // skip more than the current set of entries
        c1.skipEntries(10, IndividualDeletedEntries.Exclude);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.hasMoreEntries(), false);
        assertEquals(c1.getReadPosition(), pos.getNext());
        assertEquals(c1.getMarkDeletedPosition(), pos);
    }

    @Test(timeOut = 20000)
    void testSkipEntriesWithIndividualDeletedMessages() throws Exception {
        ManagedLedger ledger = factory.open("testSkipEntriesWithIndividualDeletedMessages",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(5));
        ManagedCursor c1 = ledger.openCursor("c1");

        Position pos1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position pos2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position pos3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        Position pos4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        Position pos5 = ledger.addEntry("dummy-entry-5".getBytes(Encoding));

        // delete individual messages
        c1.delete(pos2);
        c1.delete(pos4);

        c1.skipEntries(3, IndividualDeletedEntries.Exclude);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getReadPosition(), pos5.getNext());
        assertEquals(c1.getMarkDeletedPosition(), pos5);

        pos1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        pos2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        pos3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        pos4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        pos5 = ledger.addEntry("dummy-entry-5".getBytes(Encoding));

        c1.delete(pos2);
        c1.delete(pos4);

        c1.skipEntries(4, IndividualDeletedEntries.Include);
        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.getReadPosition(), pos5);
        assertEquals(c1.getMarkDeletedPosition(), pos4);
    }

    @Test(timeOut = 20000)
    void testClearBacklog() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.hasMoreEntries(), true);

        c1.clearBacklog();
        c3.clearBacklog();

        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.hasMoreEntries(), false);

        assertEquals(c2.getNumberOfEntriesInBacklog(), 2);
        assertEquals(c2.getNumberOfEntries(), 2);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c3.getNumberOfEntries(), 0);
        assertEquals(c3.hasMoreEntries(), false);

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");

        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.hasMoreEntries(), false);

        assertEquals(c2.getNumberOfEntriesInBacklog(), 2);
        assertEquals(c2.getNumberOfEntries(), 2);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c3.getNumberOfEntries(), 0);
        assertEquals(c3.hasMoreEntries(), false);
        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void testRateLimitMarkDelete() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1); // Throttle to 1/s
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        ManagedCursor c1 = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);
        c1.markDelete(p1);
        c1.markDelete(p2);
        c1.markDelete(p3);

        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);

        // Re-open to recover from storage
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig());

        c1 = ledger.openCursor("c1");

        // Only the 1st mark-delete was persisted
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);
        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void deleteSingleMessageTwice() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("entry-4".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntriesInBacklog(), 4);
        assertEquals(c1.getNumberOfEntries(), 4);

        c1.delete(p1);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getMarkDeletedPosition(), p1);
        assertEquals(c1.getReadPosition(), p2);

        // Should have not effect since p1 is already deleted
        c1.delete(p1);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 3);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getMarkDeletedPosition(), p1);
        assertEquals(c1.getReadPosition(), p2);

        c1.delete(p2);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getMarkDeletedPosition(), p2);
        assertEquals(c1.getReadPosition(), p3);

        // Should have not effect since p2 is already deleted
        c1.delete(p2);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getMarkDeletedPosition(), p2);
        assertEquals(c1.getReadPosition(), p3);

        c1.delete(p3);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 1);
        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.getMarkDeletedPosition(), p3);
        assertEquals(c1.getReadPosition(), p4);

        // Should have not effect since p3 is already deleted
        c1.delete(p3);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 1);
        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.getMarkDeletedPosition(), p3);
        assertEquals(c1.getReadPosition(), p4);

        c1.delete(p4);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getMarkDeletedPosition(), p4);
        assertEquals(c1.getReadPosition(), p4.getNext());

        // Should have not effect since p4 is already deleted
        c1.delete(p4);
        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getMarkDeletedPosition(), p4);
        assertEquals(c1.getReadPosition(), p4.getNext());
    }

    @Test(timeOut = 10000)
    void testReadEntriesOrWait() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        final int Consumers = 10;
        final CountDownLatch counter = new CountDownLatch(Consumers);

        for (int i = 0; i < Consumers; i++) {
            ManagedCursor c = ledger.openCursor("c" + i);

            c.asyncReadEntriesOrWait(1, new ReadEntriesCallback() {
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    assertEquals(entries.size(), 1);
                    entries.forEach(e -> e.release());
                    counter.countDown();
                }

                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Error reading", exception);
                }
            }, null);
        }

        ledger.addEntry("test".getBytes());
        counter.await();
    }

    @Test(timeOut = 20000)
    void testReadEntriesOrWaitBlocking() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        final int Messages = 100;
        final int Consumers = 10;

        List<Future<Void>> futures = Lists.newArrayList();
        ExecutorService executor = Executors.newCachedThreadPool();
        final CyclicBarrier barrier = new CyclicBarrier(Consumers + 1);

        for (int i = 0; i < Consumers; i++) {
            final ManagedCursor cursor = ledger.openCursor("c" + i);

            futures.add(executor.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    barrier.await();

                    int toRead = Messages;
                    while (toRead > 0) {
                        List<Entry> entries = cursor.readEntriesOrWait(10);
                        assertTrue(entries.size() <= 10);
                        toRead -= entries.size();
                        entries.forEach(e -> e.release());
                    }

                    return null;
                }
            }));
        }

        barrier.await();
        for (int i = 0; i < Messages; i++) {
            ledger.addEntry("test".getBytes());
        }

        for (Future<Void> f : futures) {
            f.get();
        }
    }

    @Test(timeOut = 20000)
    void testFindNewestMatching() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertNull(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))));
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingOdd1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        Position p1 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p1);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingOdd2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("expired".getBytes(Encoding));
        Position p2 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p2);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingOdd3() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position p3 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p3);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingOdd4() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position p4 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p4);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingOdd5() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position p5 = ledger.addEntry("expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p5);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEven1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        Position p1 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p1);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEven2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("expired".getBytes(Encoding));
        Position p2 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p2);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEven3() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position p3 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p3);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEven4() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");

        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position p4 = ledger.addEntry("expired".getBytes(Encoding));

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p4);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                null);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        Position p1 = ledger.addEntry("expired".getBytes(Encoding));
        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p1);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase3() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        Position p1 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p1);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase4() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        Position p1 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p1);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase5() throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase5");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("expired".getBytes(Encoding));
        Position p2 = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                p2);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase6() throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase6",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(3));

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position newPosition = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));
        List<Entry> entries = c1.readEntries(3);
        c1.markDelete(entries.get(2).getPosition());
        entries.forEach(e -> e.release());
        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                newPosition);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase7() throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase7");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position lastPosition = ledger.addEntry("expired".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(4);
        c1.markDelete(entries.get(0).getPosition());
        c1.delete(entries.get(2).getPosition());
        entries.forEach(e -> e.release());

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                lastPosition);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase8() throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase8");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position lastPosition = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(4);
        c1.delete(entries.get(1).getPosition());
        c1.delete(entries.get(2).getPosition());
        entries.forEach(e -> e.release());

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                lastPosition);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase9() throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase9");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position lastPosition = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(5);
        c1.delete(entries.get(1).getPosition());
        c1.delete(entries.get(3).getPosition());
        entries.forEach(e -> e.release());

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                lastPosition);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingEdgeCase10() throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase10");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("expired".getBytes(Encoding));
        Position lastPosition = ledger.addEntry("expired".getBytes(Encoding));
        ledger.addEntry("not-expired".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(7);
        c1.delete(entries.get(1).getPosition());
        c1.delete(entries.get(3).getPosition());
        c1.delete(entries.get(6).getPosition());
        entries.forEach(e -> e.release());

        assertEquals(
                c1.findNewestMatching(entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))),
                lastPosition);
    }

    @Test(timeOut = 20000)
    void testIndividuallyDeletedMessages() throws Exception {
        ManagedLedger ledger = factory.open("testIndividuallyDeletedMessages");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("entry-0".getBytes(Encoding));
        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));
        ledger.addEntry("entry-4".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(4);
        c1.delete(entries.get(1).getPosition());
        c1.delete(entries.get(2).getPosition());
        c1.markDelete(entries.get(3).getPosition());
        entries.forEach(e -> e.release());

        assertTrue(c1.isIndividuallyDeletedEntriesEmpty());
    }

    @Test(timeOut = 20000)
    void testIndividuallyDeletedMessages1() throws Exception {
        ManagedLedger ledger = factory.open("testIndividuallyDeletedMessages1");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("entry-0".getBytes(Encoding));
        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));
        ledger.addEntry("entry-4".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(4);
        c1.delete(entries.get(1).getPosition());
        c1.markDelete(entries.get(3).getPosition());
        entries.forEach(e -> e.release());

        assertTrue(c1.isIndividuallyDeletedEntriesEmpty());
    }

    @Test(timeOut = 20000)
    void testIndividuallyDeletedMessages2() throws Exception {
        ManagedLedger ledger = factory.open("testIndividuallyDeletedMessages2");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("entry-0".getBytes(Encoding));
        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));
        ledger.addEntry("entry-4".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(4);
        c1.delete(entries.get(1).getPosition());
        c1.delete(entries.get(2).getPosition());
        c1.delete(entries.get(0).getPosition());
        entries.forEach(e -> e.release());

        assertTrue(c1.isIndividuallyDeletedEntriesEmpty());
    }

    @Test(timeOut = 20000)
    void testIndividuallyDeletedMessages3() throws Exception {
        ManagedLedger ledger = factory.open("testIndividuallyDeletedMessages3");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("entry-0".getBytes(Encoding));
        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));
        ledger.addEntry("entry-4".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(4);
        c1.delete(entries.get(1).getPosition());
        c1.delete(entries.get(2).getPosition());
        c1.markDelete(entries.get(0).getPosition());
        entries.forEach(e -> e.release());

        assertTrue(c1.isIndividuallyDeletedEntriesEmpty());
    }

    public static byte[] getEntryPublishTime(String msg) throws Exception {
        return Long.toString(System.currentTimeMillis()).getBytes();
    }

    public Position findPositionFromAllEntries(ManagedCursor c1, final long timestamp) throws Exception {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            Position position = null;
        }

        final Result result = new Result();
        AsyncCallbacks.FindEntryCallback findEntryCallback = new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        };

        c1.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {

            try {
                long publishTime = Long.valueOf(new String(entry.getData()));
                return publishTime <= timestamp;
            } catch (Exception e) {
                log.error("Error de-serializing message for message position find", e);
            } finally {
                entry.release();
            }
            return false;
        }, findEntryCallback, ManagedCursorImpl.FindPositionConstraint.SearchAllAvailableEntries);
        counter.await();
        if (result.exception != null) {
            throw result.exception;
        }
        return result.position;
    }

    void internalTestFindNewestMatchingAllEntries(final String name, final int entriesPerLedger,
            final int expectedEntryId) throws Exception {
        final String ledgerAndCursorName = name;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        ledger.addEntry(getEntryPublishTime("retained1"));
        // space apart message publish times
        Thread.sleep(100);
        ledger.addEntry(getEntryPublishTime("retained2"));
        Thread.sleep(100);
        ledger.addEntry(getEntryPublishTime("retained3"));
        Thread.sleep(100);
        Position newPosition = ledger.addEntry(getEntryPublishTime("expectedresetposition"));
        long timestamp = System.currentTimeMillis();
        long ledgerId = ((PositionImpl) newPosition).getLedgerId();
        Thread.sleep(2);

        ledger.addEntry(getEntryPublishTime("not-read"));
        List<Entry> entries = c1.readEntries(3);
        c1.markDelete(entries.get(2).getPosition());
        c1.close();
        ledger.close();
        entries.forEach(e -> e.release());
        // give timed ledger trimming a chance to run
        Thread.sleep(100);

        ledger = factory.open(ledgerAndCursorName, config);
        c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        PositionImpl found = (PositionImpl) findPositionFromAllEntries(c1, timestamp);
        assertEquals(found.getLedgerId(), ledgerId);
        assertEquals(found.getEntryId(), expectedEntryId);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingAllEntries() throws Exception {
        final String ledgerAndCursorName = "testFindNewestMatchingAllEntries";
        // condition below assumes entries per ledger is 2
        // needs to be changed if entries per ledger is changed
        int expectedEntryId = 1;
        int entriesPerLedger = 2;
        internalTestFindNewestMatchingAllEntries(ledgerAndCursorName, entriesPerLedger, expectedEntryId);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingAllEntries2() throws Exception {
        final String ledgerAndCursorName = "testFindNewestMatchingAllEntries2";
        // condition below assumes entries per ledger is 1
        // needs to be changed if entries per ledger is changed
        int expectedEntryId = 0;
        int entriesPerLedger = 1;
        internalTestFindNewestMatchingAllEntries(ledgerAndCursorName, entriesPerLedger, expectedEntryId);
    }

    @Test(timeOut = 20000)
    void testFindNewestMatchingAllEntriesSingleLedger() throws Exception {
        final String ledgerAndCursorName = "testFindNewestMatchingAllEntriesSingleLedger";
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        // needs to be changed if entries per ledger is changed
        int expectedEntryId = 3;
        int entriesPerLedger = config.getMaxEntriesPerLedger();
        internalTestFindNewestMatchingAllEntries(ledgerAndCursorName, entriesPerLedger, expectedEntryId);
    }

    @Test(timeOut = 20000)
    void testReplayEntries() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        PositionImpl p1 = (PositionImpl) ledger.addEntry("entry1".getBytes(Encoding));
        PositionImpl p2 = (PositionImpl) ledger.addEntry("entry2".getBytes(Encoding));
        PositionImpl p3 = (PositionImpl) ledger.addEntry("entry3".getBytes(Encoding));
        ledger.addEntry("entry4".getBytes(Encoding));

        // 1. Replay empty position set should return empty entry set
        Set<PositionImpl> positions = Sets.newHashSet();
        assertTrue(c1.replayEntries(positions).isEmpty());

        positions.add(p1);
        positions.add(p3);

        // 2. entries 1 and 3 should be returned, but they can be in any order
        List<Entry> entries = c1.replayEntries(positions);
        assertEquals(entries.size(), 2);
        assertTrue((Arrays.equals(entries.get(0).getData(), "entry1".getBytes(Encoding))
                && Arrays.equals(entries.get(1).getData(), "entry3".getBytes(Encoding)))
                || (Arrays.equals(entries.get(0).getData(), "entry3".getBytes(Encoding))
                        && Arrays.equals(entries.get(1).getData(), "entry1".getBytes(Encoding))));
        entries.forEach(Entry::release);

        // 3. Fail on reading non-existing position
        PositionImpl invalidPosition = new PositionImpl(100, 100);
        positions.add(invalidPosition);

        try {
            c1.replayEntries(positions);
            fail("Should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }
        positions.remove(invalidPosition);

        // 4. Fail to attempt to read mark-deleted position (p1)
        c1.markDelete(p2);

        try {
            // as mark-delete is at position: p2 it should read entry : p3
            assertEquals(1, c1.replayEntries(positions).size());
        } catch (ManagedLedgerException e) {
            fail("Should have not failed");
        }
    }

    @Test(timeOut = 20000)
    void outOfOrderAcks() throws Exception {
        ManagedLedger ledger = factory.open("outOfOrderAcks");
        ManagedCursor c1 = ledger.openCursor("c1");

        int N = 10;

        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            positions.add(ledger.addEntry("entry".getBytes()));
        }

        assertEquals(c1.getNumberOfEntriesInBacklog(), N);

        c1.delete(positions.get(3));
        assertEquals(c1.getNumberOfEntriesInBacklog(), N - 1);

        c1.delete(positions.get(2));
        assertEquals(c1.getNumberOfEntriesInBacklog(), N - 2);

        c1.delete(positions.get(1));
        assertEquals(c1.getNumberOfEntriesInBacklog(), N - 3);

        c1.delete(positions.get(0));
        assertEquals(c1.getNumberOfEntriesInBacklog(), N - 4);
    }

    @Test(timeOut = 20000)
    void randomOrderAcks() throws Exception {
        ManagedLedger ledger = factory.open("outOfOrderAcks");
        ManagedCursor c1 = ledger.openCursor("c1");

        int N = 10;

        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            positions.add(ledger.addEntry("entry".getBytes()));
        }

        assertEquals(c1.getNumberOfEntriesInBacklog(), N);

        // Randomize the ack sequence
        Collections.shuffle(positions);

        int toDelete = N;
        for (Position p : positions) {
            assertEquals(c1.getNumberOfEntriesInBacklog(), toDelete);
            c1.delete(p);
            --toDelete;
            assertEquals(c1.getNumberOfEntriesInBacklog(), toDelete);
        }
    }

    @Test(timeOut = 20000)
    void testGetEntryAfterN() throws Exception {
        ManagedLedger ledger = factory.open("testGetEntryAfterN");

        ManagedCursor c1 = ledger.openCursor("c1");

        Position pos1 = ledger.addEntry("msg1".getBytes());
        Position pos2 = ledger.addEntry("msg2".getBytes());
        Position pos3 = ledger.addEntry("msg3".getBytes());
        Position pos4 = ledger.addEntry("msg4".getBytes());
        Position pos5 = ledger.addEntry("msg5".getBytes());

        List<Entry> entries = c1.readEntries(4);
        entries.forEach(e -> e.release());
        long currentLedger = ((PositionImpl) c1.getMarkDeletedPosition()).getLedgerId();

        // check if the first message is returned for '0'
        Entry e = c1.getNthEntry(1, IndividualDeletedEntries.Exclude);
        assertEquals(e.getDataAndRelease(), "msg1".getBytes());

        // check that if we call get entry for the same position twice, it returns the same entry
        e = c1.getNthEntry(1, IndividualDeletedEntries.Exclude);
        assertEquals(e.getDataAndRelease(), "msg1".getBytes());

        // check for a position 'n' after md position
        e = c1.getNthEntry(3, IndividualDeletedEntries.Exclude);
        assertEquals(e.getDataAndRelease(), "msg3".getBytes());

        // check for the last position
        e = c1.getNthEntry(5, IndividualDeletedEntries.Exclude);
        assertEquals(e.getDataAndRelease(), "msg5".getBytes());

        // check for a position outside the limits of the number of entries that exists, it should return null
        e = c1.getNthEntry(10, IndividualDeletedEntries.Exclude);
        assertNull(e);

        // check that the mark delete and read positions have not been updated after all the previous operations
        assertEquals((PositionImpl) c1.getMarkDeletedPosition(), new PositionImpl(currentLedger, -1));
        assertEquals((PositionImpl) c1.getReadPosition(), new PositionImpl(currentLedger, 4));

        c1.markDelete(pos4);
        assertEquals(c1.getMarkDeletedPosition(), pos4);
        e = c1.getNthEntry(1, IndividualDeletedEntries.Exclude);
        assertEquals(e.getDataAndRelease(), "msg5".getBytes());

        c1.readEntries(1);
        c1.markDelete(pos5);

        e = c1.getNthEntry(1, IndividualDeletedEntries.Exclude);
        assertNull(e);
    }

    @Test(timeOut = 20000)
    void testGetEntryAfterNWithIndividualDeletedMessages() throws Exception {
        ManagedLedger ledger = factory.open("testGetEnteryAfterNWithIndividualDeletedMessages");

        ManagedCursor c1 = ledger.openCursor("c1");

        Position pos1 = ledger.addEntry("msg1".getBytes());
        Position pos2 = ledger.addEntry("msg2".getBytes());
        Position pos3 = ledger.addEntry("msg3".getBytes());
        Position pos4 = ledger.addEntry("msg4".getBytes());
        Position pos5 = ledger.addEntry("msg5".getBytes());

        c1.delete(pos3);
        c1.delete(pos4);

        Entry e = c1.getNthEntry(3, IndividualDeletedEntries.Exclude);
        assertEquals(e.getDataAndRelease(), "msg5".getBytes());

        e = c1.getNthEntry(3, IndividualDeletedEntries.Include);
        assertEquals(e.getDataAndRelease(), "msg3".getBytes());
    }

    @Test(timeOut = 20000)
    void cancelReadOperation() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");

        // No read request so far
        assertEquals(c1.cancelPendingReadRequest(), false);

        CountDownLatch counter = new CountDownLatch(1);

        c1.asyncReadEntriesOrWait(1, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }
        }, null);

        assertEquals(c1.cancelPendingReadRequest(), true);

        CountDownLatch counter2 = new CountDownLatch(1);

        c1.asyncReadEntriesOrWait(1, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                counter2.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter2.countDown();
            }
        }, null);

        ledger.addEntry("entry-1".getBytes(Encoding));

        Thread.sleep(100);

        // Read operation should have already been completed
        assertEquals(c1.cancelPendingReadRequest(), false);

        counter2.await();
    }

    @Test(timeOut = 20000)
    public void testReopenMultipleTimes() throws Exception {
        ManagedLedger ledger = factory.open("testReopenMultipleTimes");
        ManagedCursor c1 = ledger.openCursor("c1");

        Position mdPosition = c1.getMarkDeletedPosition();

        c1.close();
        ledger.close();

        ledger = factory.open("testReopenMultipleTimes");
        c1 = ledger.openCursor("c1");

        // since the empty data ledger will be deleted, the cursor position should also be updated
        assertNotEquals(c1.getMarkDeletedPosition(), mdPosition);

        c1.close();
        ledger.close();

        ledger = factory.open("testReopenMultipleTimes");
        c1 = ledger.openCursor("c1");
    }

    @Test(timeOut = 20000)
    public void testOutOfOrderDeletePersistenceWithClose() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());

        ManagedCursor c1 = ledger.openCursor("c1");
        List<Position> addedPositions = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Position p = ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));
            addedPositions.add(p);
        }

        // Acknowledge few messages leaving holes
        c1.delete(addedPositions.get(2));
        c1.delete(addedPositions.get(5));
        c1.delete(addedPositions.get(7));
        c1.delete(addedPositions.get(8));
        c1.delete(addedPositions.get(9));

        assertEquals(c1.getNumberOfEntriesInBacklog(), 20 - 5);

        ledger.close();
        factory.shutdown();

        // Re-Open
        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");
        assertEquals(c1.getNumberOfEntriesInBacklog(), 20 - 5);

        List<Entry> entries = c1.readEntries(20);
        assertEquals(entries.size(), 20 - 5);

        List<String> entriesStr = entries.stream().map(e -> new String(e.getDataAndRelease(), Encoding))
                .collect(Collectors.toList());
        assertEquals(entriesStr.get(0), "dummy-entry-0");
        assertEquals(entriesStr.get(1), "dummy-entry-1");
        // Entry-2 was deleted
        assertEquals(entriesStr.get(2), "dummy-entry-3");
        assertEquals(entriesStr.get(3), "dummy-entry-4");
        // Entry-6 was deleted
        assertEquals(entriesStr.get(4), "dummy-entry-6");

        assertFalse(c1.hasMoreEntries());
    }

    @Test(timeOut = 20000)
    public void testOutOfOrderDeletePersistenceAfterCrash() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());

        ManagedCursor c1 = ledger.openCursor("c1");
        List<Position> addedPositions = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Position p = ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));
            addedPositions.add(p);
        }

        // Acknowledge few messages leaving holes
        c1.delete(addedPositions.get(2));
        c1.delete(addedPositions.get(5));
        c1.delete(addedPositions.get(7));
        c1.delete(addedPositions.get(8));
        c1.delete(addedPositions.get(9));

        assertEquals(c1.getNumberOfEntriesInBacklog(), 20 - 5);

        // Re-Open
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");
        assertEquals(c1.getNumberOfEntriesInBacklog(), 20 - 5);

        List<Entry> entries = c1.readEntries(20);
        assertEquals(entries.size(), 20 - 5);

        List<String> entriesStr = entries.stream().map(e -> new String(e.getDataAndRelease(), Encoding))
                .collect(Collectors.toList());
        assertEquals(entriesStr.get(0), "dummy-entry-0");
        assertEquals(entriesStr.get(1), "dummy-entry-1");
        // Entry-2 was deleted
        assertEquals(entriesStr.get(2), "dummy-entry-3");
        assertEquals(entriesStr.get(3), "dummy-entry-4");
        // Entry-6 was deleted
        assertEquals(entriesStr.get(4), "dummy-entry-6");

        assertFalse(c1.hasMoreEntries());
        factory2.shutdown();
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorTest.class);
}
