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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.common.api.proto.IntRange;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedCursorTest extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    @DataProvider(name = "useOpenRangeSet")
    public static Object[][] useOpenRangeSet() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
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

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
        ManagedLedger ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

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

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);
        assertEquals(c2.getNumberOfEntriesInBacklog(false), 3);
        assertEquals(c3.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c4.getNumberOfEntriesInBacklog(false), 1);
        assertEquals(c5.getNumberOfEntriesInBacklog(false), 0);

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

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
    void testNumberOfEntriesInBacklogWithFallback() throws Exception {
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

        Field field = ManagedCursorImpl.class.getDeclaredField("messagesConsumedCounter");
        field.setAccessible(true);
        long counter = ((ManagedLedgerImpl) ledger).getEntriesAddedCounter() + 1;
        field.setLong(c1, counter);
        field.setLong(c2, counter);
        field.setLong(c3, counter);
        field.setLong(c4, counter);
        field.setLong(c5, counter);

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);
        assertEquals(c2.getNumberOfEntriesInBacklog(false), 3);
        assertEquals(c3.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c4.getNumberOfEntriesInBacklog(false), 1);
        assertEquals(c5.getNumberOfEntriesInBacklog(false), 0);
    }

    @Test(timeOut = 20000)
    void testNumberOfEntriesWithReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");

        assertEquals(c1.getNumberOfEntries(), 2);
        assertTrue(c1.hasMoreEntries());

        assertEquals(c2.getNumberOfEntries(), 1);
        assertTrue(c2.hasMoreEntries());

        assertEquals(c3.getNumberOfEntries(), 0);
        assertFalse(c3.hasMoreEntries());
    }

    @Test(timeOut = 20000)
    void asyncReadWithoutErrors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertNull(ctx);
                assertEquals(entries.size(), 1);
                entries.forEach(e -> e.release());
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null, PositionImpl.latest);

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
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                entries.forEach(e -> e.release());
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                fail("async-call should not have failed");
            }

        }, null, PositionImpl.latest);

        counter.await();

        cursor.rewind();

        // Clear the cache to force reading from BK
        ledger.entryCache.clear();

        final CountDownLatch counter2 = new CountDownLatch(1);

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("async-call should have failed");
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter2.countDown();
            }

        }, null, PositionImpl.latest);

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
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("async-call should have failed");
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null, PositionImpl.latest);

        counter.await();
    }

    @Test(timeOut = 20000)
    void testAsyncReadWithMaxSizeByte() throws Exception {
        ManagedLedger ledger = factory.open("testAsyncReadWithMaxSizeByte");
        ManagedCursor cursor = ledger.openCursor("c1");

        for (int i = 0; i < 100; i++) {
            ledger.addEntry(new byte[1024]);
        }

        // First time, since we don't have info, we'll get 1 single entry
        readAndCheck(cursor, 10, 3 * 1024, 1);
        // We should only return 3 entries, based on the max size
        readAndCheck(cursor, 20, 3 * 1024, 3);
        // If maxSize is < avg, we should get 1 entry
        readAndCheck(cursor, 10, 500, 1);
    }

    private void readAndCheck(ManagedCursor cursor, int numEntriesToRead,
                              long maxSizeBytes, int expectedNumRead) throws InterruptedException {
        CountDownLatch counter = new CountDownLatch(1);
        cursor.asyncReadEntries(numEntriesToRead, maxSizeBytes, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                Assert.assertEquals(entries.size(), expectedNumRead);
                entries.forEach(e -> e.release());
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }
        }, null, null);
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

        // Mark-delete should succeed if BK is down
        cursor.markDelete(entries.get(0).getPosition());

        entries.forEach(e -> e.release());
    }

    @Test(timeOut = 20000)
    void markDeleteWithZKErrors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        List<Entry> entries = cursor.readEntries(100);

        assertEquals(entries.size(), 1);

        stopBookKeeper();
        metadataStore.setAlwaysFail(new MetadataStoreException("error"));

        try {
            cursor.markDelete(entries.get(0).getPosition());
            fail("Should have failed");
        } catch (Exception e) {
            // Expected
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
        assertEquals(resetPosition, cursor.getReadPosition());
        cursor.close();
        ledger.close();
    }

    @Test(timeOut = 20000)
    void testResetCursor1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger",
            new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("trc1");
        PositionImpl actualEarliest = (PositionImpl) ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl lastInPrev = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        PositionImpl firstInNext = (PositionImpl) ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));
        ledger.addEntry("dummy-entry-7".getBytes(Encoding));
        ledger.addEntry("dummy-entry-8".getBytes(Encoding));
        ledger.addEntry("dummy-entry-9".getBytes(Encoding));
        PositionImpl last = (PositionImpl) ledger.addEntry("dummy-entry-10".getBytes(Encoding));

        final AtomicBoolean moveStatus = new AtomicBoolean(false);

        // reset to earliest
        PositionImpl earliest = PositionImpl.earliest;
        try {
            cursor.resetCursor(earliest);
            moveStatus.set(true);
        } catch (Exception e) {
            log.warn("error in reset cursor", e.getCause());
        }
        assertTrue(moveStatus.get());
        PositionImpl earliestPos = new PositionImpl(actualEarliest.getLedgerId(), -1);
        assertEquals(earliestPos, cursor.getReadPosition());
        moveStatus.set(false);

        // reset to one after last entry in a ledger should point to the first entry in the next ledger
        PositionImpl resetPosition = new PositionImpl(lastInPrev.getLedgerId(), lastInPrev.getEntryId() + 1);
        try {
            cursor.resetCursor(resetPosition);
            moveStatus.set(true);
        } catch (Exception e) {
            log.warn("error in reset cursor", e.getCause());
        }
        assertTrue(moveStatus.get());
        assertEquals(firstInNext, cursor.getReadPosition());
        moveStatus.set(false);

        // reset to a non exist larger ledger should point to the first non-exist entry in the last ledger
        PositionImpl latest = new PositionImpl(last.getLedgerId() + 2, 0);
        try {
            cursor.resetCursor(latest);
            moveStatus.set(true);
        } catch (Exception e) {
            log.warn("error in reset cursor", e.getCause());
        }
        assertTrue(moveStatus.get());
        PositionImpl lastPos = new PositionImpl(last.getLedgerId(), last.getEntryId() + 1);
        assertEquals(lastPos, cursor.getReadPosition());
        moveStatus.set(false);

        // reset to latest should point to the first non-exist entry in the last ledger
        PositionImpl anotherLast = PositionImpl.latest;
        try {
            cursor.resetCursor(anotherLast);
            moveStatus.set(true);
        } catch (Exception e) {
            log.warn("error in reset cursor", e.getCause());
        }
        assertTrue(moveStatus.get());
        assertEquals(lastPos, cursor.getReadPosition());

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
        assertEquals(resetPosition, cursor.getReadPosition());
        cursor.close();
        ledger.close();
    }

    @Test(timeOut = 20000)
    void testConcurrentResetCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_concurrent_move_ledger");

        final int Messages = 100;
        final int Consumers = 5;

        List<Future<AtomicBoolean>> futures = Lists.newArrayList();
        @Cleanup("shutdownNow")
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
                @Override
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
                    assertEquals(resetPosition, cursor.getReadPosition());
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
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);
        c1.markDelete(p1);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 3);
        entries.forEach(e -> e.release());

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
        entries.forEach(e -> e.release());

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
        ManagedCursor cursor = ledger.openCursor("c1");
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
        entries.forEach(e -> e.release());

        cursor.markDelete(p4);
        assertFalse(cursor.hasMoreEntries());
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
        Awaitility.await().until(() -> ledger.getNumberOfEntries() <= 2);
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

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");

        assertEquals(c1.getMarkDeletedPosition(), p1);
        assertEquals(c2.getMarkDeletedPosition(), p2);
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

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");
        c4 = ledger.openCursor("c4");

        assertEquals(c1.getMarkDeletedPosition(), p5);
        assertEquals(c2.getMarkDeletedPosition(), p1);
        assertEquals(c3.getMarkDeletedPosition(), p0);
        assertEquals(c4.getMarkDeletedPosition(), p1);
    }

    @Test
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
                @Override
                public void markDeleteComplete(Object ctx) {
                    latch.countDown();
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Failed to markdelete", exception);
                    latch.countDown();
                }
            }, null);
        }

        latch.await();

        // Reopen
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger");
        ManagedCursor c2 = ledger.openCursor("c1");

        assertEquals(c2.getMarkDeletedPosition(), lastPosition);
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
            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                fail();
            }

            @Override
            public void markDeleteComplete(Object ctx) {
                latch.countDown();
            }
        }, null);

        c1.asyncMarkDelete(p1, new MarkDeleteCallback() {
            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }

            @Override
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
        metadataStore.failConditional(new MetadataStoreException("error"), (op, path) ->
                path.equals("/managed-ledgers/my_test_ledger/c1")
                        && op == FaultInjectionMetadataStore.OperationType.PUT
        );

        try {
            ledger.openCursor("c1");
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test
    void failDuringRecoveryWithEmptyLedger() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("cursor");

        ledger.addEntry("entry-1".getBytes());
        Position p2 = ledger.addEntry("entry-2".getBytes());
        Position p3 = ledger.addEntry("entry-3".getBytes());

        cursor.markDelete(p2);
        // Do graceful close so snapshot is forced
        ledger.close();

        // Re-open
        ledger = factory.open("my_test_ledger");
        cursor = ledger.openCursor("cursor");
        cursor.markDelete(p3);

        // Force-reopen so the recovery will be forced to read from ledger
        bkc.returnEmptyLedgerAfter(1);
        ManagedLedgerFactoryConfig conf = new ManagedLedgerFactoryConfig();

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, conf);
        ledger = factory2.open("my_test_ledger");
        cursor = ledger.openCursor("cursor");

        // Cursor was rolled back to p2 because of the ledger recovery failure
        assertEquals(cursor.getMarkDeletedPosition(), p2);
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        Position p1 = ledger.addEntry("entry".getBytes());
        ledger.addEntry("entry".getBytes());
        ManagedCursor c1 = ledger.openCursor("c1");
        Position p3 = ledger.addEntry("entry".getBytes());

        assertEquals(c1.getReadPosition(), p3);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        bkc.failAfter(3, BKException.Code.LedgerRecoveryException);

        ledger = factory2.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        // Verify the ManagedCursor was rewind back to the snapshotted position
        assertEquals(c1.getReadPosition(), p3);
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        bkc.failAfter(4, BKException.Code.MetadataVersionException);

        try {
            ledger = factory2.open("my_test_ledger");
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor3() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        Position p1 = ledger.addEntry("entry".getBytes());
        ledger.addEntry("entry".getBytes());
        ManagedCursor c1 = ledger.openCursor("c1");
        Position p3 = ledger.addEntry("entry".getBytes());

        assertEquals(c1.getReadPosition(), p3);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        bkc.failAfter(4, BKException.Code.ReadException);

        ledger = factory2.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        // Verify the ManagedCursor was rewind back to the snapshotted position
        assertEquals(c1.getReadPosition(), p3);
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
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 6);

        List<Entry> entries = cursor.readEntries(3);
        assertEquals(entries.size(), 3);
        entries.forEach(e -> e.release());

        assertEquals(cursor.getNumberOfEntries(), 3);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 6);

        log.info("Deleting {}", p5);
        cursor.delete(p5);

        assertEquals(cursor.getNumberOfEntries(), 2);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 5);

        entries = cursor.readEntries(3);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());
        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 5);
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
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 8);

        cursor.delete(p8);
        assertEquals(cursor.getNumberOfEntries(), 7);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 7);

        cursor.delete(p1);
        assertEquals(cursor.getNumberOfEntries(), 6);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 6);

        cursor.delete(p7);
        cursor.delete(p6);
        cursor.delete(p5);
        assertEquals(cursor.getNumberOfEntries(), 3);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 3);
    }

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testMarkDeleteTwice(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig()
                .setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet).setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        cursor.markDelete(p1);
        cursor.markDelete(p1);

        assertEquals(cursor.getMarkDeletedPosition(), p1);
    }

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testSkipEntries(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig()
                .setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet).setMaxEntriesPerLedger(2));
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
        assertFalse(c1.hasMoreEntries());
        assertEquals(c1.getReadPosition(), pos.getNext());
        assertEquals(c1.getMarkDeletedPosition(), pos);
    }

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testSkipEntriesWithIndividualDeletedMessages(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("testSkipEntriesWithIndividualDeletedMessages", new ManagedLedgerConfig()
                .setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet).setMaxEntriesPerLedger(5));
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

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testClearBacklog(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig()
                .setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet).setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertTrue(c1.hasMoreEntries());

        c1.clearBacklog();
        c3.clearBacklog();

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertFalse(c1.hasMoreEntries());

        assertEquals(c2.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c2.getNumberOfEntries(), 2);
        assertTrue(c2.hasMoreEntries());

        assertEquals(c3.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(c3.getNumberOfEntries(), 0);
        assertFalse(c3.hasMoreEntries());

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertFalse(c1.hasMoreEntries());

        assertEquals(c2.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c2.getNumberOfEntries(), 2);
        assertTrue(c2.hasMoreEntries());

        assertEquals(c3.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(c3.getNumberOfEntries(), 0);
        assertFalse(c3.hasMoreEntries());
    }

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testRateLimitMarkDelete(boolean useOpenRangeSet) throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1).setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet); // Throttle to 1/s
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        ManagedCursor c1 = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        c1.markDelete(p1);
        c1.markDelete(p2);
        c1.markDelete(p3);

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);

        // Re-open to recover from storage
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig());

        c1 = ledger.openCursor("c1");

        // Only the 1st mark-delete was persisted
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 2);
    }

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void deleteSingleMessageTwice(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet));

        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("entry-4".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);
        assertEquals(c1.getNumberOfEntries(), 4);

        c1.delete(p1);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getMarkDeletedPosition(), p1);
        assertEquals(c1.getReadPosition(), p2);

        // Should have not effect since p1 is already deleted
        c1.delete(p1);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getMarkDeletedPosition(), p1);
        assertEquals(c1.getReadPosition(), p2);

        c1.delete(p2);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getMarkDeletedPosition(), p2);
        assertEquals(c1.getReadPosition(), p3);

        // Should have not effect since p2 is already deleted
        c1.delete(p2);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getMarkDeletedPosition(), p2);
        assertEquals(c1.getReadPosition(), p3);

        c1.delete(p3);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 1);
        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.getMarkDeletedPosition(), p3);
        assertEquals(c1.getReadPosition(), p4);

        // Should have not effect since p3 is already deleted
        c1.delete(p3);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 1);
        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.getMarkDeletedPosition(), p3);
        assertEquals(c1.getReadPosition(), p4);

        c1.delete(p4);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getMarkDeletedPosition(), p4);
        assertEquals(c1.getReadPosition(), p4.getNext());

        // Should have not effect since p4 is already deleted
        c1.delete(p4);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getMarkDeletedPosition(), p4);
        assertEquals(c1.getReadPosition(), p4.getNext());
    }

    @Test(timeOut = 10000, dataProvider = "useOpenRangeSet")
    void testReadEntriesOrWait(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet));

        final int Consumers = 10;
        final CountDownLatch counter = new CountDownLatch(Consumers);

        for (int i = 0; i < Consumers; i++) {
            ManagedCursor c = ledger.openCursor("c" + i);

            c.asyncReadEntriesOrWait(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    assertEquals(entries.size(), 1);
                    entries.forEach(e -> e.release());
                    counter.countDown();
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Error reading", exception);
                }
            }, null, PositionImpl.latest);
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
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();
        final CyclicBarrier barrier = new CyclicBarrier(Consumers + 1);

        for (int i = 0; i < Consumers; i++) {
            final ManagedCursor cursor = ledger.openCursor("c" + i);

            futures.add(executor.submit(new Callable<Void>() {
                @Override
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
        assertNull(c1.findNewestMatching(
            entry -> Arrays.equals(entry.getDataAndRelease(), "expired".getBytes(Encoding))));
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

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testFindNewestMatchingEdgeCase6(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase6", new ManagedLedgerConfig()
                .setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet).setMaxEntriesPerLedger(3));

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

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testFindNewestMatchingEdgeCase7(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("testFindNewestMatchingEdgeCase7",
                new ManagedLedgerConfig().setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet));

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

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testIndividuallyDeletedMessages(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("testIndividuallyDeletedMessages",
                new ManagedLedgerConfig().setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet));

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

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testIndividuallyDeletedMessages2(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("testIndividuallyDeletedMessages2",
                new ManagedLedgerConfig().setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet));

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

    @Test(timeOut = 20000, dataProvider = "useOpenRangeSet")
    void testIndividuallyDeletedMessages3(boolean useOpenRangeSet) throws Exception {
        ManagedLedger ledger = factory.open("testIndividuallyDeletedMessages3",
                new ManagedLedgerConfig().setUnackedRangesOpenCacheSetEnabled(useOpenRangeSet));

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

    @Test(timeOut = 20000)
    void testFindNewestMatchingAfterLedgerRollover() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ledger.addEntry("first-expired".getBytes(Encoding));
        ledger.addEntry("second".getBytes(Encoding));
        ledger.addEntry("third".getBytes(Encoding));
        ledger.addEntry("fourth".getBytes(Encoding));
        Position last = ledger.addEntry("last-expired".getBytes(Encoding));

        // roll a new ledger
        int numLedgersBefore = ledger.getLedgersInfo().size();
        ledger.getConfig().setMaxEntriesPerLedger(1);
        ledger.rollCurrentLedgerIfFull();
        Awaitility.await().atMost(20, TimeUnit.SECONDS)
                .until(() -> ledger.getLedgersInfo().size() > numLedgersBefore);

        // the algorithm looks for "expired" messages
        // starting from the first, then it moves to the last message
        // if the condition evaluates to true on the last message
        // then we are done
        // there was a bug (https://github.com/apache/pulsar/issues/9082)
        // in which if the last message was in a different ledger
        // the jump from the first message to the last message went
        // to an invalid position and so the search stopped at the first message

        // we want to assert here that the algorithm returns the position of the
        // last message
        assertEquals(last,
                c1.findNewestMatching(entry -> {
                    byte[] data = entry.getDataAndRelease();
                    return Arrays.equals(data, "first-expired".getBytes(Encoding))
                            || Arrays.equals(data, "last-expired".getBytes(Encoding));
                }));

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
            public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                    Object ctx) {
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

        found = (PositionImpl) findPositionFromAllEntries(c1, 0);
        assertNull(found);
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
    void testGetLastIndividualDeletedRange() throws Exception {
        ManagedLedger ledger = factory.open("test_last_individual_deleted");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        PositionImpl markDeletedPosition = (PositionImpl) c1.getMarkDeletedPosition();
        for(int i = 0; i < 10; i++) {
            ledger.addEntry(("entry" + i).getBytes(Encoding));
        }
        PositionImpl p1 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 1);
        PositionImpl p2 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 2);
        PositionImpl p3 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 5);
        PositionImpl p4 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 6);

        c1.delete(Lists.newArrayList(p1, p2, p3, p4));

        assertEquals(c1.getLastIndividualDeletedRange(), Range.openClosed(PositionImpl.get(p3.getLedgerId(),
                p3.getEntryId() - 1), p4));

        PositionImpl p5 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 8);
        c1.delete(p5);

        assertEquals(c1.getLastIndividualDeletedRange(), Range.openClosed(PositionImpl.get(p5.getLedgerId(),
                p5.getEntryId() - 1), p5));

    }

    @Test(timeOut = 20000)
    void testTrimDeletedEntries() throws ManagedLedgerException, InterruptedException {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        PositionImpl markDeletedPosition = (PositionImpl) c1.getMarkDeletedPosition();
        for(int i = 0; i < 10; i++) {
            ledger.addEntry(("entry" + i).getBytes(Encoding));
        }
        PositionImpl p1 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 1);
        PositionImpl p2 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 2);
        PositionImpl p3 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 5);
        PositionImpl p4 = PositionImpl.get(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 6);

        c1.delete(Lists.newArrayList(p1, p2, p3, p4));

        EntryImpl entry1 = EntryImpl.create(p1, ByteBufAllocator.DEFAULT.buffer(0));
        EntryImpl entry2 = EntryImpl.create(p2, ByteBufAllocator.DEFAULT.buffer(0));
        EntryImpl entry3 = EntryImpl.create(p3, ByteBufAllocator.DEFAULT.buffer(0));
        EntryImpl entry4 = EntryImpl.create(p4, ByteBufAllocator.DEFAULT.buffer(0));
        EntryImpl entry5 = EntryImpl.create(markDeletedPosition.getLedgerId() , markDeletedPosition.getEntryId() + 7,
                ByteBufAllocator.DEFAULT.buffer(0));
        List<Entry> entries = Lists.newArrayList(entry1, entry2, entry3, entry4, entry5);
        c1.trimDeletedEntries(entries);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), PositionImpl.get(markDeletedPosition.getLedgerId() ,
                markDeletedPosition.getEntryId() + 7));
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

        assertEquals(c1.getNumberOfEntriesInBacklog(false), N);

        c1.delete(positions.get(3));
        assertEquals(c1.getNumberOfEntriesInBacklog(false), N - 1);

        c1.delete(positions.get(2));
        assertEquals(c1.getNumberOfEntriesInBacklog(false), N - 2);

        c1.delete(positions.get(1));
        assertEquals(c1.getNumberOfEntriesInBacklog(false), N - 3);

        c1.delete(positions.get(0));
        assertEquals(c1.getNumberOfEntriesInBacklog(false), N - 4);
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

        assertEquals(c1.getNumberOfEntriesInBacklog(false), N);

        // Randomize the ack sequence
        Collections.shuffle(positions);

        int toDelete = N;
        for (Position p : positions) {
            assertEquals(c1.getNumberOfEntriesInBacklog(false), toDelete);
            c1.delete(p);
            --toDelete;
            assertEquals(c1.getNumberOfEntriesInBacklog(false), toDelete);
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
        assertEquals(c1.getMarkDeletedPosition(), new PositionImpl(currentLedger, -1));
        assertEquals(c1.getReadPosition(), new PositionImpl(currentLedger, 4));

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
        assertFalse(c1.cancelPendingReadRequest());

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
        }, null, PositionImpl.latest);

        assertTrue(c1.cancelPendingReadRequest());

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
        }, null, PositionImpl.latest);

        ledger.addEntry("entry-1".getBytes(Encoding));

        Thread.sleep(100);

        // Read operation should have already been completed
        assertFalse(c1.cancelPendingReadRequest());

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

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 20 - 5);

        ledger.close();
        factory.shutdown();

        // Re-Open
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 20 - 5);

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

        assertEquals(c1.getNumberOfEntriesInBacklog(false), 20 - 5);

        // Re-Open
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 20 - 5);

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

    /**
     * <pre>
     * Verifies that {@link ManagedCursorImpl#createNewMetadataLedger()} cleans up orphan ledgers if fails to switch new
     * ledger
     * </pre>
     * @throws Exception
     */
    @Test(timeOut=5000)
    public void testLeakFailedLedgerOfManageCursor() throws Exception {

        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig();
        ManagedLedger ledger = factory.open("my_test_ledger", mlConfig);

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        CountDownLatch latch = new CountDownLatch(1);
        c1.createNewMetadataLedger(new VoidCallback() {
            @Override
            public void operationComplete() {
                latch.countDown();
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                latch.countDown();
            }
        });

        // update cursor-info with data which makes bad-version for existing managed-cursor
        String path = "/managed-ledgers/my_test_ledger/c1";
        metadataStore.put(path, "".getBytes(), Optional.empty()).join();

        // try to create ledger again which will fail because managedCursorInfo znode is already updated with different
        // version so, this call will fail with BadVersionException
        CountDownLatch latch2 = new CountDownLatch(1);
        // create ledger will create ledgerId = 6
        long ledgerId = 6;
        c1.createNewMetadataLedger(new VoidCallback() {
            @Override
            public void operationComplete() {
                latch2.countDown();
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                latch2.countDown();
            }
        });

        // Wait until operation is completed and the failed ledger should have been deleted
        latch2.await();

        try {
            bkc.openLedgerNoRecovery(ledgerId, DigestType.fromApiDigestType(mlConfig.getDigestType()),
                                     mlConfig.getPassword());
            fail("ledger should have deleted due to update-cursor failure");
        } catch (BKException e) {
            // ok
        }
    }

    /**
     * Verifies cursor persists individually unack range into cursor-ledger if range count is higher than
     * MaxUnackedRangesToPersistInZk
     *
     * @throws Exception
     */
    @Test(timeOut = 20000)
    public void testOutOfOrderDeletePersistenceIntoLedgerWithClose() throws Exception {

        final int totalAddEntries = 100;
        String ledgerName = "my_test_ledger";
        String cursorName = "c1";
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        // metaStore is allowed to store only up to 10 deleted entries range
        managedLedgerConfig.setMaxUnackedRangesToPersistInZk(10);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, managedLedgerConfig);

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(cursorName);

        List<Position> addedPositions = new ArrayList<>();
        for (int i = 0; i < totalAddEntries; i++) {
            Position p = ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));
            addedPositions.add(p);
            if (i % 2 == 0) {
                // Acknowledge alternative message to create totalEntries/2 holes
                c1.delete(addedPositions.get(i));
            }
        }

        assertEquals(c1.getNumberOfEntriesInBacklog(false), totalAddEntries / 2);

        // Close ledger to persist individual-deleted positions into cursor-ledger
        ledger.close();

        // verify cursor-ledgerId is updated properly into cursor-metaStore
        CountDownLatch cursorLedgerLatch = new CountDownLatch(1);
        AtomicLong cursorLedgerId = new AtomicLong(0);
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), cursorName, new MetaStoreCallback<ManagedCursorInfo>() {
            @Override
            public void operationComplete(ManagedCursorInfo result, Stat stat) {
                cursorLedgerId.set(result.getCursorsLedgerId());
                cursorLedgerLatch.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                cursorLedgerLatch.countDown();
            }
        });
        cursorLedgerLatch.await();
        assertEquals(cursorLedgerId.get(), c1.getCursorLedger());

        // verify cursor-ledger's last entry has individual-deleted positions
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger individualDeletedMessagesCount = new AtomicInteger(0);
        bkc.asyncOpenLedger(c1.getCursorLedger(), DigestType.CRC32C, "".getBytes(), (rc, lh, ctx) -> {
            if (rc == BKException.Code.OK) {
                long lastEntry = lh.getLastAddConfirmed();
                lh.asyncReadEntries(lastEntry, lastEntry, (rc1, lh1, seq, ctx1) -> {
                    try {
                        LedgerEntry entry = seq.nextElement();
                        PositionInfo positionInfo;
                        positionInfo = PositionInfo.parseFrom(entry.getEntry());
                        individualDeletedMessagesCount.set(positionInfo.getIndividualDeletedMessagesCount());
                    } catch (Exception e) {
                    }
                    latch.countDown();
                }, null);
            } else {
                latch.countDown();
            }
        }, null);

        latch.await();
        assertEquals(individualDeletedMessagesCount.get(), totalAddEntries / 2 - 1);

        // Re-Open
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = (ManagedLedgerImpl) factory2.open(ledgerName, managedLedgerConfig);
        c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        // verify cursor has been recovered
        assertEquals(c1.getNumberOfEntriesInBacklog(false), totalAddEntries / 2);

        // try to read entries which should only read non-deleted positions
        List<Entry> entries = c1.readEntries(totalAddEntries);
        assertEquals(entries.size(), totalAddEntries / 2);
    }

    /**
     * Close Cursor without MaxUnackedRangesToPersistInZK: It should store individually unack range into Zk
     *
     * @throws Exception
     */
    @Test(timeOut = 20000)
    public void testOutOfOrderDeletePersistenceIntoZkWithClose() throws Exception {
        final int totalAddEntries = 100;
        String ledgerName = "my_test_ledger_zk";
        String cursorName = "c1";
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, managedLedgerConfig);

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(cursorName);

        List<Position> addedPositions = new ArrayList<>();
        for (int i = 0; i < totalAddEntries; i++) {
            Position p = ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));
            addedPositions.add(p);
            if (i % 2 == 0) {
                // Acknowledge alternative message to create totalEntries/2 holes
                c1.delete(addedPositions.get(i));
            }
        }

        assertEquals(c1.getNumberOfEntriesInBacklog(false), totalAddEntries / 2);

        // Close ledger to persist individual-deleted positions into cursor-ledger
        ledger.close();

        // verify cursor-ledgerId is updated as -1 into cursor-metaStore
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger individualDeletedMessagesCount = new AtomicInteger(0);
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), cursorName, new MetaStoreCallback<ManagedCursorInfo>() {
            @Override
            public void operationComplete(ManagedCursorInfo result, Stat stat) {
                individualDeletedMessagesCount.set(result.getIndividualDeletedMessagesCount());
                latch.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                latch.countDown();
            }
        });
        latch.await();
        assertEquals(individualDeletedMessagesCount.get(), totalAddEntries / 2 - 1);

        // Re-Open
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = (ManagedLedgerImpl) factory2.open(ledgerName, managedLedgerConfig);
        c1 = (ManagedCursorImpl) ledger.openCursor(cursorName);
        // verify cursor has been recovered
        assertEquals(c1.getNumberOfEntriesInBacklog(false), totalAddEntries / 2);

        // try to read entries which should only read non-deleted positions
        List<Entry> entries = c1.readEntries(totalAddEntries);
        assertEquals(entries.size(), totalAddEntries / 2);
    }

    @Test
    public void testInvalidMarkDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());

        ManagedCursor cursor = ledger.openCursor("c1");
        Position readPosition = cursor.getReadPosition();
        Position markDeletePosition = cursor.getMarkDeletedPosition();

        List<Position> addedPositions = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Position p = ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));
            addedPositions.add(p);
        }

        // validate: cursor.asyncMarkDelete(..)
        CountDownLatch markDeleteCallbackLatch = new CountDownLatch(1);
        Position position = PositionImpl.get(100, 100);
        AtomicBoolean markDeleteCallFailed = new AtomicBoolean(false);
        cursor.asyncMarkDelete(position, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                markDeleteCallbackLatch.countDown();
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                markDeleteCallFailed.set(true);
                markDeleteCallbackLatch.countDown();
            }
        }, null);
        markDeleteCallbackLatch.await();
        assertEquals(readPosition, cursor.getReadPosition());
        assertEquals(markDeletePosition, cursor.getMarkDeletedPosition());

        // validate : cursor.asyncDelete(..)
        CountDownLatch deleteCallbackLatch = new CountDownLatch(1);
        markDeleteCallFailed.set(false);
        cursor.asyncDelete(position, new DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                deleteCallbackLatch.countDown();
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                markDeleteCallFailed.set(true);
                deleteCallbackLatch.countDown();
            }
        }, null);

        deleteCallbackLatch.await();
        assertEquals(readPosition, cursor.getReadPosition());
        assertEquals(markDeletePosition, cursor.getMarkDeletedPosition());
    }

    @Test
    public void testEstimatedUnackedSize() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());

        ManagedCursor cursor = ledger.openCursor("c1");

        byte[] entryData = new byte[5];

        // write 15 entries, saving position of 5th
        for (int i = 0; i < 4; i++) { ledger.addEntry(entryData); }
        Position deleteAt = ledger.addEntry(entryData);
        for (int i = 0; i < 10; i++) { ledger.addEntry(entryData); }

        assertEquals(cursor.getEstimatedSizeSinceMarkDeletePosition(), 15 * entryData.length);

        cursor.markDelete(deleteAt);

        // it's not an estimate if all entries are the same size
        assertEquals(cursor.getEstimatedSizeSinceMarkDeletePosition(), 10 * entryData.length);
    }

    @Test(timeOut = 20000)
    public void testRecoverCursorAheadOfLastPosition() throws Exception {
        final String mlName = "my_test_ledger";
        final PositionImpl lastPosition = new PositionImpl(1L, 10L);
        final PositionImpl nextPosition = new PositionImpl(3L, -1L);

        final String cursorName = "my_test_cursor";
        final long cursorsLedgerId = -1L;
        final long markDeleteLedgerId = 2L;
        final long markDeleteEntryId = -1L;

        MetaStore mockMetaStore = mock(MetaStore.class);
        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) {
                ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(cursorsLedgerId)
                        .setMarkDeleteLedgerId(markDeleteLedgerId).setMarkDeleteEntryId(markDeleteEntryId)
                        .setLastActive(0L).build();
                Stat stat = mock(Stat.class);
                MetaStoreCallback<ManagedCursorInfo> callback = (MetaStoreCallback<ManagedCursorInfo>) invocation
                        .getArguments()[2];
                callback.operationComplete(info, stat);
                return null;
            }
        }).when(mockMetaStore).asyncGetCursorInfo(eq(mlName), eq(cursorName), any(MetaStoreCallback.class));

        ManagedLedgerImpl ml = mock(ManagedLedgerImpl.class);
        when(ml.getName()).thenReturn(mlName);
        when(ml.getStore()).thenReturn(mockMetaStore);
        when(ml.getLastPosition()).thenReturn(lastPosition);
        when(ml.getNextValidLedger(markDeleteLedgerId)).thenReturn(3L);
        when(ml.getNextValidPosition(lastPosition)).thenReturn(nextPosition);
        when(ml.ledgerExists(markDeleteLedgerId)).thenReturn(false);

        BookKeeper mockBookKeeper = mock(BookKeeper.class);
        final ManagedCursorImpl cursor = new ManagedCursorImpl(mockBookKeeper, new ManagedLedgerConfig(), ml,
                cursorName);

        cursor.recover(new VoidCallback() {
            @Override
            public void operationComplete() {
                assertEquals(cursor.getMarkDeletedPosition(), lastPosition);
                assertEquals(cursor.getReadPosition(), nextPosition);
                assertEquals(cursor.getNumberOfEntries(), 0L);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                fail("Cursor recovery should not fail");
            }
        });
    }

    @Test
    void testAlwaysInactive() throws Exception {
        ManagedLedger ml = factory.open("testAlwaysInactive");
        ManagedCursor cursor = ml.openCursor("c1");

        assertTrue(cursor.isActive());

        cursor.setAlwaysInactive();

        assertFalse(cursor.isActive());

        cursor.setActive();
        assertFalse(cursor.isActive());
    }

    @Test
    void testNonDurableCursorActive() throws Exception {
        ManagedLedger ml = factory.open("testInactive");
        ManagedCursor cursor = ml.newNonDurableCursor(PositionImpl.latest, "c1");

        assertTrue(cursor.isActive());

        cursor.setInactive();
        assertFalse(cursor.isActive());
    }

    @Test
    public void deleteMessagesCheckhMarkDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        final int totalEntries = 1000;
        final Position[] positions = new Position[totalEntries];
        for (int i = 0; i < totalEntries; i++) {
            // add entry
            positions[i] = ledger.addEntry(("entry-" + i).getBytes(Encoding));
        }
        assertEquals(c1.getNumberOfEntries(), totalEntries);
        int totalDeletedMessages = 0;
        for (int i = 0; i < totalEntries; i++) {
            // delete entry
            if ((i % 3) == 0) {
                c1.delete(positions[i]);
                totalDeletedMessages += 1;
            }
        }
        assertEquals(c1.getNumberOfEntriesInBacklog(false), totalEntries - totalDeletedMessages);
        assertEquals(c1.getNumberOfEntries(), totalEntries - totalDeletedMessages);
        assertEquals(c1.getMarkDeletedPosition(), positions[0]);
        assertEquals(c1.getReadPosition(), positions[1]);

        // delete 1/2 of the messags
        for (int i = 0; i < totalEntries / 2; i++) {
            // delete entry
            if ((i % 3) != 0) {
                c1.delete(positions[i]);
                totalDeletedMessages += 1;
            }
        }
        int markDelete = totalEntries / 2 - 1;
        assertEquals(c1.getNumberOfEntriesInBacklog(false), totalEntries - totalDeletedMessages);
        assertEquals(c1.getNumberOfEntries(), totalEntries - totalDeletedMessages);
        assertEquals(c1.getMarkDeletedPosition(), positions[markDelete]);
        assertEquals(c1.getReadPosition(), positions[markDelete + 1]);
    }

    @Test
    public void testBatchIndexDelete() throws ManagedLedgerException, InterruptedException {
        ManagedLedger ledger = factory.open("test_batch_index_delete");
        ManagedCursor cursor = ledger.openCursor("c1");

        final int totalEntries = 100;
        final Position[] positions = new Position[totalEntries];
        for (int i = 0; i < totalEntries; i++) {
            // add entry
            positions[i] = ledger.addEntry(("entry-" + i).getBytes(Encoding));
        }
        assertEquals(cursor.getNumberOfEntries(), totalEntries);
        deleteBatchIndex(cursor, positions[0], 10, Lists.newArrayList(new IntRange().setStart(2).setEnd(4)));
        List<IntRange> deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[0]), 10);
        Assert.assertEquals(1, deletedIndexes.size());
        Assert.assertEquals(2, deletedIndexes.get(0).getStart());
        Assert.assertEquals(4, deletedIndexes.get(0).getEnd());

        deleteBatchIndex(cursor, positions[0], 10, Lists.newArrayList(new IntRange().setStart(3).setEnd(8)));
        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[0]), 10);
        Assert.assertEquals(1, deletedIndexes.size());
        Assert.assertEquals(2, deletedIndexes.get(0).getStart());
        Assert.assertEquals(8, deletedIndexes.get(0).getEnd());

        deleteBatchIndex(cursor, positions[0], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(0)));
        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[0]), 10);
        Assert.assertEquals(2, deletedIndexes.size());
        Assert.assertEquals(0, deletedIndexes.get(0).getStart());
        Assert.assertEquals(0, deletedIndexes.get(0).getEnd());
        Assert.assertEquals(2, deletedIndexes.get(1).getStart());
        Assert.assertEquals(8, deletedIndexes.get(1).getEnd());

        deleteBatchIndex(cursor, positions[0], 10, Lists.newArrayList(new IntRange().setStart(1).setEnd(1)));
        deleteBatchIndex(cursor, positions[0], 10, Lists.newArrayList(new IntRange().setStart(9).setEnd(9)));
        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[0]), 10);
        Assert.assertNull(deletedIndexes);
        Assert.assertEquals(positions[0], cursor.getMarkDeletedPosition());

        deleteBatchIndex(cursor, positions[1], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(5)));
        cursor.delete(positions[1]);
        deleteBatchIndex(cursor, positions[1], 10, Lists.newArrayList(new IntRange().setStart(6).setEnd(8)));
        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[1]), 10);
        Assert.assertNull(deletedIndexes);

        deleteBatchIndex(cursor, positions[2], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(5)));
        cursor.markDelete(positions[3]);
        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[2]), 10);
        Assert.assertNull(deletedIndexes);

        deleteBatchIndex(cursor, positions[3], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(5)));
        cursor.resetCursor(positions[0]);
        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[3]), 10);
        Assert.assertNull(deletedIndexes);
    }

    @Test
    public void testBatchIndexesDeletionPersistAndRecover() throws ManagedLedgerException, InterruptedException {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        // Make sure the cursor metadata updated by the cursor ledger ID.
        managedLedgerConfig.setMaxUnackedRangesToPersistInZk(-1);
        ManagedLedger ledger = factory.open("test_batch_indexes_deletion_persistent", managedLedgerConfig);
        ManagedCursor cursor = ledger.openCursor("c1");

        final int totalEntries = 100;
        final Position[] positions = new Position[totalEntries];
        for (int i = 0; i < totalEntries; i++) {
            // add entry
            positions[i] = ledger.addEntry(("entry-" + i).getBytes(Encoding));
        }
        assertEquals(cursor.getNumberOfEntries(), totalEntries);
        deleteBatchIndex(cursor, positions[6], 10, Lists.newArrayList(new IntRange().setStart(1).setEnd(3)));
        deleteBatchIndex(cursor, positions[5], 10, Lists.newArrayList(new IntRange().setStart(3).setEnd(6)));
        deleteBatchIndex(cursor, positions[0], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(9)));
        deleteBatchIndex(cursor, positions[1], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(9)));
        deleteBatchIndex(cursor, positions[2], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(9)));
        deleteBatchIndex(cursor, positions[3], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(9)));
        deleteBatchIndex(cursor, positions[4], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(9)));

        cursor.close();
        ledger.close();
        ledger = factory.open("test_batch_indexes_deletion_persistent", managedLedgerConfig);
        cursor = ledger.openCursor("c1");

        List<IntRange> deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[5]), 10);
        Assert.assertEquals(deletedIndexes.size(), 1);
        Assert.assertEquals(deletedIndexes.get(0).getStart(), 3);
        Assert.assertEquals(deletedIndexes.get(0).getEnd(), 6);
        Assert.assertEquals(cursor.getMarkDeletedPosition(), positions[4]);
        deleteBatchIndex(cursor, positions[5], 10, Lists.newArrayList(new IntRange().setStart(0).setEnd(9)));
        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[5]), 10);
        Assert.assertNull(deletedIndexes);
        Assert.assertEquals(cursor.getMarkDeletedPosition(), positions[5]);

        deletedIndexes = getAckedIndexRange(cursor.getDeletedBatchIndexesAsLongArray((PositionImpl) positions[6]), 10);
        Assert.assertEquals(deletedIndexes.size(), 1);
        Assert.assertEquals(deletedIndexes.get(0).getStart(), 1);
        Assert.assertEquals(deletedIndexes.get(0).getEnd(), 3);
    }

    private void deleteBatchIndex(ManagedCursor cursor, Position position, int batchSize,
                                  List<IntRange> deleteIndexes) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        PositionImpl pos = (PositionImpl) position;
        BitSet bitSet = new BitSet(batchSize);
        bitSet.set(0, batchSize);
        deleteIndexes.forEach(intRange -> {
            bitSet.clear(intRange.getStart(), intRange.getEnd() + 1);
        });
        pos.ackSet = bitSet.toLongArray();

        cursor.asyncDelete(pos,
            new DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    latch.countDown();
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                }
            }, null);
        latch.await();
        pos.ackSet = null;
    }

    private List<IntRange> getAckedIndexRange(long[] bitSetLongArray, int batchSize) {
        if (bitSetLongArray == null) {
            return null;
        }
        List<IntRange> result = new ArrayList<>();
        BitSet bitSet = BitSet.valueOf(bitSetLongArray);
        int nextClearBit = bitSet.nextClearBit(0);
        while (nextClearBit != -1 && nextClearBit <= batchSize) {
            int nextSetBit = bitSet.nextSetBit(nextClearBit);
            if (nextSetBit == -1) {
                break;
            }
            result.add(new IntRange().setStart(nextClearBit).setEnd(nextSetBit - 1));
            nextClearBit = bitSet.nextClearBit(nextSetBit);
        }
        return result;
    }

    @Test
    public void testReadEntriesOrWaitWithMaxSize() throws Exception {
        ManagedLedger ledger = factory.open("testReadEntriesOrWaitWithMaxSize");
        ManagedCursor c = ledger.openCursor("c");

        for (int i = 0; i < 20; i++) {
            ledger.addEntry(new byte[1024]);
        }

        // First time, since we don't have info, we'll get 1 single entry
        List<Entry> entries = c.readEntriesOrWait(10, 3 * 1024);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        // We should only return 3 entries, based on the max size
        entries = c.readEntriesOrWait(10, 3 * 1024);
        assertEquals(entries.size(), 3);
        entries.forEach(e -> e.release());

        // If maxSize is < avg, we should get 1 entry
        entries = c.readEntriesOrWait(10, 5);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());
    }

    @Test
    public void testReadEntriesOrWaitWithMaxPosition() throws Exception {
        int readMaxNumber = 10;
        int sendNumber = 20;
        ManagedLedger ledger = factory.open("testReadEntriesOrWaitWithMaxPosition");
        ManagedCursor c = ledger.openCursor("c");
        Position position = PositionImpl.earliest;
        Position maxCanReadPosition = PositionImpl.earliest;
        for (int i = 0; i < sendNumber; i++) {
            if (i == readMaxNumber - 1) {
                position = ledger.addEntry(new byte[1024]);
            } else if (i == sendNumber - 1) {
                maxCanReadPosition = ledger.addEntry(new byte[1024]);
            } else {
                ledger.addEntry(new byte[1024]);
            }

        }
        CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
        c.asyncReadEntriesOrWait(sendNumber, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                completableFuture.complete(entries.size());
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                completableFuture.completeExceptionally(exception);
            }
        }, null, (PositionImpl) position);

        int number = completableFuture.get();
        assertEquals(number, readMaxNumber);

        c.asyncReadEntriesOrWait(sendNumber, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                completableFuture.complete(entries.size());
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                completableFuture.completeExceptionally(exception);
            }
        }, null, (PositionImpl) maxCanReadPosition);

        assertEquals(number, sendNumber - readMaxNumber);

    }

    @Test
    public void testFlushCursorAfterInactivity() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1.0);

        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setCursorPositionFlushSeconds(1);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        ManagedLedger ledger1 = factory1.open("testFlushCursorAfterInactivity", config);
        ManagedCursor c1 = ledger1.openCursor("c");
        List<Position> positions = new ArrayList<Position>();

        for (int i = 0; i < 20; i++) {
            positions.add(ledger1.addEntry(new byte[1024]));
        }

        CountDownLatch latch = new CountDownLatch(positions.size());

        positions.forEach(p -> c1.asyncMarkDelete(p, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                latch.countDown();
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                throw new RuntimeException(exception);
            }
        }, null));

        latch.await();

        assertEquals(c1.getMarkDeletedPosition(), positions.get(positions.size() - 1));

        Awaitility.await()
                // Give chance to the flush to be automatically triggered.
                // NOTE: this can't be set too low, or it causes issues with ZK thread pool rejecting
                .pollDelay(Duration.ofMillis(2000))
                .untilAsserted(() -> {
                    // Abruptly re-open the managed ledger without graceful close
                    @Cleanup("shutdown")
                    ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
                    ManagedLedger ledger2 = factory2.open("testFlushCursorAfterInactivity", config);
                    ManagedCursor c2 = ledger2.openCursor("c");

                    assertEquals(c2.getMarkDeletedPosition(), positions.get(positions.size() - 1));
                });
    }

    @Test
    public void testFlushCursorAfterIndividualDeleteInactivity() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1.0);

        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setCursorPositionFlushSeconds(1);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        ManagedLedger ledger1 = factory1.open("testFlushCursorAfterIndDelInactivity", config);
        ManagedCursor c1 = ledger1.openCursor("c");
        List<Position> positions = new ArrayList<Position>();

        for (int i = 0; i < 20; i++) {
            positions.add(ledger1.addEntry(new byte[1024]));
        }

        CountDownLatch latch = new CountDownLatch(positions.size());

        positions.forEach(p -> c1.asyncDelete(p, new DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                latch.countDown();
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                throw new RuntimeException(exception);
            }
        }, null));

        latch.await();

        assertEquals(c1.getMarkDeletedPosition(), positions.get(positions.size() - 1));

        // reopen the cursor and we should see entries not be flushed
        @Cleanup("shutdown")
        ManagedLedgerFactory dirtyFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedger ledgerDirty = dirtyFactory.open("testFlushCursorAfterIndDelInactivity", config);
        ManagedCursor dirtyCursor = ledgerDirty.openCursor("c");

        assertNotEquals(dirtyCursor.getMarkDeletedPosition(), positions.get(positions.size() - 1));

        Awaitility.await()
                // Give chance to the flush to be automatically triggered.
                // NOTE: this can't be set too low, or it causes issues with ZK thread pool rejecting
                .pollDelay(Duration.ofMillis(2000))
                .untilAsserted(() -> {
                    // Abruptly re-open the managed ledger without graceful close
                    @Cleanup("shutdown")
                    ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
                    ManagedLedger ledger2 = factory2.open("testFlushCursorAfterIndDelInactivity", config);
                    ManagedCursor c2 = ledger2.openCursor("c");

                    assertEquals(c2.getMarkDeletedPosition(), positions.get(positions.size() - 1));
                });
    }

    @Test
    public void testCursorCheckReadPositionChanged() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        ManagedCursor c1 = ledger.openCursor("c1");

        // check empty ledger
        assertTrue(c1.checkAndUpdateReadPositionChanged());
        assertTrue(c1.checkAndUpdateReadPositionChanged());

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        // read-position has not been moved
        assertFalse(c1.checkAndUpdateReadPositionChanged());

        List<Entry> entries = c1.readEntries(2);
        entries.forEach(e -> {
            try {
                c1.markDelete(e.getPosition());
                e.release();
            } catch (Exception e1) {
                // Ok
            }
        });

        // read-position is moved
        assertTrue(c1.checkAndUpdateReadPositionChanged());
        // read-position has not been moved since last read
        assertFalse(c1.checkAndUpdateReadPositionChanged());

        c1.close();
        ledger.close();

        ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        // recover cursor
        ManagedCursor c2 = ledger.openCursor("c1");
        assertTrue(c2.checkAndUpdateReadPositionChanged());
        assertFalse(c2.checkAndUpdateReadPositionChanged());

        entries = c2.readEntries(2);
        entries.forEach(e -> {
            try {
                c2.markDelete(e.getPosition());
                e.release();
            } catch (Exception e1) {
                // Ok
            }
        });

        assertTrue(c2.checkAndUpdateReadPositionChanged());
        // returns true because read-position is on tail
        assertTrue(c2.checkAndUpdateReadPositionChanged());
        assertTrue(c2.checkAndUpdateReadPositionChanged());

        ledger.close();
    }


    @Test
    public void testCursorGetBacklog() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        managedLedgerConfig.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("get-backlog", managedLedgerConfig);
        ManagedCursor managedCursor = ledger.openCursor("test");

        Position position = ledger.addEntry("test".getBytes(Encoding));
        ledger.addEntry("test".getBytes(Encoding));
        Position position1 = ledger.addEntry("test".getBytes(Encoding));
        ledger.addEntry("test".getBytes(Encoding));

        Assert.assertEquals(managedCursor.getNumberOfEntriesInBacklog(true), 4);
        Assert.assertEquals(managedCursor.getNumberOfEntriesInBacklog(false), 4);
        Field field = ManagedLedgerImpl.class.getDeclaredField("ledgers");
        field.setAccessible(true);

        ((ConcurrentSkipListMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo>) field.get(ledger)).remove(position.getLedgerId());
        field = ManagedCursorImpl.class.getDeclaredField("markDeletePosition");
        field.setAccessible(true);
        field.set(managedCursor, PositionImpl.get(position1.getLedgerId(), -1));


        Assert.assertEquals(managedCursor.getNumberOfEntriesInBacklog(true), 2);
        Assert.assertEquals(managedCursor.getNumberOfEntriesInBacklog(false), 4);
    }

    @Test
    public void testCursorNoRolloverIfNoMetadataSession() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        managedLedgerConfig.setMetadataMaxEntriesPerLedger(2);
        managedLedgerConfig.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        managedLedgerConfig.setThrottleMarkDelete(0);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testCursorNoRolloverIfNoMetadataSession", managedLedgerConfig);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("test");

        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            positions.add(ledger.addEntry("test".getBytes(Encoding)));
        }

        cursor.delete(positions.get(0));

        long initialLedgerId = cursor.getCursorLedger();

        metadataStore.triggerSessionEvent(SessionEvent.SessionLost);

        for (int i = 1; i < 10; i++) {
            cursor.delete(positions.get(i));
        }

        assertEquals(cursor.getCursorLedger(), initialLedgerId);

        // After the session gets reestablished, the rollover should restart
        metadataStore.triggerSessionEvent(SessionEvent.SessionReestablished);

        for (int i = 0; i < 10; i++) {
            Position p = ledger.addEntry("test".getBytes(Encoding));
            cursor.delete(p);
        }

        assertNotEquals(cursor.getCursorLedger(), initialLedgerId);
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorTest.class);
}
