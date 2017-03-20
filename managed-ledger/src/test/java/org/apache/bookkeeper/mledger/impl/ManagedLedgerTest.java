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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.Stat;
import org.apache.bookkeeper.mledger.impl.MetaStoreImplZookeeper.ZNodeProtobufFormat;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.yahoo.pulsar.common.api.DoubleByteBuf;
import com.yahoo.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import com.yahoo.pulsar.common.util.protobuf.ByteBufCodedOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class ManagedLedgerTest extends MockedBookKeeperTestCase {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTest.class);

    private static final Charset Encoding = Charsets.UTF_8;

    @Factory(dataProvider = "protobufFormat")
    public ManagedLedgerTest(ZNodeProtobufFormat protobufFormat) {
        super();
        this.protobufFormat = protobufFormat;
    }

    @Test
    public void managedLedgerApi() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor cursor = ledger.openCursor("c1");

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        // Reads all the entries in batches of 20
        while (cursor.hasMoreEntries()) {

            List<Entry> entries = cursor.readEntries(20);
            log.debug("Read {} entries", entries.size());

            for (Entry entry : entries) {
                log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
                entry.release();
            }

            // Acknowledge only on last entry
            Entry lastEntry = entries.get(entries.size() - 1);
            cursor.markDelete(lastEntry.getPosition());

            log.info("-----------------------");
        }

        log.info("Finished reading entries");

        ledger.close();
    }

    @Test(timeOut = 20000)
    public void simple() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getNumberOfActiveEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getNumberOfActiveEntries(), 0);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        ManagedCursor cursor = ledger.openCursor("c1");

        assertEquals(cursor.hasMoreEntries(), false);
        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 0);
        assertEquals(cursor.readEntries(100), new ArrayList<Entry>());

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 1);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 1);
        assertEquals(ledger.getNumberOfActiveEntries(), 1);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);

        ledger.close();
        factory.shutdown();
    }

    @Test(timeOut = 20000)
    public void closeAndReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory2.open("my_test_ledger");

        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        ledger.close();
        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    public void acknowledge1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);

        List<Entry> entries = cursor.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 2);
        assertEquals(cursor.hasMoreEntries(), false);

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getNumberOfActiveEntries(), 2);
        cursor.markDelete(entries.get(0).getPosition());

        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 1);
        assertEquals(cursor.hasMoreEntries(), false);
        assertEquals(ledger.getNumberOfActiveEntries(), 1);

        ledger.close();

        // / Reopen the same managed-ledger

        ledger = factory.open("my_test_ledger");
        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        assertEquals(cursor.getNumberOfEntries(), 1);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 1);
        assertEquals(cursor.hasMoreEntries(), true);

        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        ledger.close();
    }

    @Test(timeOut = 20000)
    public void asyncAPI() throws Throwable {
        final CountDownLatch counter = new CountDownLatch(1);

        factory.asyncOpen("my_test_ledger", new ManagedLedgerConfig(), new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        ManagedLedger ledger = (ManagedLedger) ctx;

                        ledger.asyncAddEntry("test".getBytes(Encoding), new AddEntryCallback() {
                            @Override
                            public void addComplete(Position position, Object ctx) {
                                @SuppressWarnings("unchecked")
                                Pair<ManagedLedger, ManagedCursor> pair = (Pair<ManagedLedger, ManagedCursor>) ctx;
                                ManagedLedger ledger = pair.first;
                                ManagedCursor cursor = pair.second;

                                assertEquals(ledger.getNumberOfEntries(), 1);
                                assertEquals(ledger.getTotalSize(), "test".getBytes(Encoding).length);

                                cursor.asyncReadEntries(2, new ReadEntriesCallback() {
                                    @Override
                                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                                        ManagedCursor cursor = (ManagedCursor) ctx;

                                        assertEquals(entries.size(), 1);
                                        Entry entry = entries.get(0);
                                        assertEquals(new String(entry.getDataAndRelease(), Encoding), "test");

                                        log.debug("Mark-Deleting to position {}", entry.getPosition());
                                        cursor.asyncMarkDelete(entry.getPosition(), new MarkDeleteCallback() {
                                            @Override
                                            public void markDeleteComplete(Object ctx) {
                                                log.debug("Mark delete complete");
                                                ManagedCursor cursor = (ManagedCursor) ctx;
                                                assertEquals(cursor.hasMoreEntries(), false);

                                                counter.countDown();
                                            }

                                            @Override
                                            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                                                fail(exception.getMessage());
                                            }

                                        }, cursor);
                                    }

                                    @Override
                                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                                        fail(exception.getMessage());
                                    }
                                }, cursor);
                            }

                            @Override
                            public void addFailed(ManagedLedgerException exception, Object ctx) {
                                fail(exception.getMessage());
                            }
                        }, new Pair<ManagedLedger, ManagedCursor>(ledger, cursor));
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        fail(exception.getMessage());
                    }

                }, ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }
        }, null);

        counter.await();

        log.info("Test completed");
    }

    @Test(timeOut = 20000)
    public void spanningMultipleLedgers() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        for (int i = 0; i < 11; i++)
            ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 11);
        assertEquals(cursor.hasMoreEntries(), false);

        PositionImpl first = (PositionImpl) entries.get(0).getPosition();
        PositionImpl last = (PositionImpl) entries.get(entries.size() - 1).getPosition();
        entries.forEach(e -> e.release());

        log.info("First={} Last={}", first, last);
        assertTrue(first.getLedgerId() < last.getLedgerId());
        assertEquals(first.getEntryId(), 0);
        assertEquals(last.getEntryId(), 0);

        // Read again, from next ledger id
        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);
        assertEquals(cursor.hasMoreEntries(), false);

        ledger.close();
    }

    @Test(timeOut = 20000)
    public void spanningMultipleLedgersWithSize() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1000000);
        config.setMaxSizePerLedgerMb(1);
        config.setEnsembleSize(1);
        config.setWriteQuorumSize(1).setAckQuorumSize(1);
        config.setMetadataWriteQuorumSize(1).setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        byte[] content = new byte[1023 * 1024];

        for (int i = 0; i < 3; i++)
            ledger.addEntry(content);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 3);
        assertEquals(cursor.hasMoreEntries(), false);

        PositionImpl first = (PositionImpl) entries.get(0).getPosition();
        PositionImpl last = (PositionImpl) entries.get(entries.size() - 1).getPosition();
        entries.forEach(e -> e.release());

        // Read again, from next ledger id
        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);
        assertEquals(cursor.hasMoreEntries(), false);
        entries.forEach(e -> e.release());

        log.info("First={} Last={}", first, last);
        assertTrue(first.getLedgerId() < last.getLedgerId());
        assertEquals(first.getEntryId(), 0);
        assertEquals(last.getEntryId(), 0);
        ledger.close();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidReadEntriesArg1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());
        cursor.readEntries(-1);

        fail("Should have thrown an exception in the above line");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidReadEntriesArg2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());
        cursor.readEntries(0);

        fail("Should have thrown an exception in the above line");
    }

    @Test(timeOut = 20000)
    public void deleteAndReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Delete and reopen
        ledger.delete();
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 0);
        ledger.close();
    }

    @Test(timeOut = 20000)
    public void deleteAndReopenWithCursors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Delete and reopen
        ledger.delete();
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 0);
        ManagedCursor cursor = ledger.openCursor("test-cursor");
        assertEquals(cursor.hasMoreEntries(), false);
        ledger.close();
    }

    @Test(timeOut = 20000)
    public void asyncDeleteWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);

        final CountDownLatch counter = new CountDownLatch(1);
        stopBookKeeper();
        stopZooKeeper();

        // Delete and reopen
        factory.open("my_test_ledger", new ManagedLedgerConfig()).asyncDelete(new DeleteLedgerCallback() {

            @Override
            public void deleteLedgerComplete(Object ctx) {
                assertNull(ctx);
                fail("The async-call should have failed");
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void asyncAddEntryWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CountDownLatch counter = new CountDownLatch(1);

        ledger.asyncAddEntry("dummy-entry-1".getBytes(Encoding), new AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                assertNull(ctx);

                counter.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null);

        counter.await();
        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);
    }

    @Test(timeOut = 20000)
    public void doubleAsyncAddEntryWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CountDownLatch done = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final String content = "dummy-entry-" + i;
            ledger.asyncAddEntry(content.getBytes(Encoding), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    assertNotNull(ctx);

                    log.info("Successfully added {}", content);
                    done.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    fail(exception.getMessage());
                }

            }, this);
        }

        done.await();
        assertEquals(ledger.getNumberOfEntries(), 10);
    }

    @Test(timeOut = 20000)
    public void asyncAddEntryWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CountDownLatch counter = new CountDownLatch(1);
        stopBookKeeper();
        stopZooKeeper();

        ledger.asyncAddEntry("dummy-entry-1".getBytes(Encoding), new AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                fail("Should have failed");
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void asyncCloseWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        ledger.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                assertNull(ctx);
                counter.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void asyncOpenCursorWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        final CountDownLatch counter = new CountDownLatch(1);

        ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                assertNull(ctx);
                assertNotNull(cursor);

                counter.countDown();
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void asyncOpenCursorWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        final CountDownLatch counter = new CountDownLatch(1);

        stopBookKeeper();
        stopZooKeeper();

        ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                fail("The async-call should have failed");
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }
        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void readFromOlderLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
    }

    @Test(timeOut = 20000)
    public void readFromOlderLedgers() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
        cursor.readEntries(1).forEach(e -> e.release());

        assertEquals(cursor.hasMoreEntries(), true);
        cursor.readEntries(1).forEach(e -> e.release());
        assertEquals(cursor.hasMoreEntries(), true);
        cursor.readEntries(1).forEach(e -> e.release());
        assertEquals(cursor.hasMoreEntries(), false);
    }

    @Test(timeOut = 20000)
    public void triggerLedgerDeletion() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(ledger.getNumberOfEntries(), 3);
        entries.forEach(e -> e.release());

        assertEquals(cursor.hasMoreEntries(), true);
        entries = cursor.readEntries(1);
        assertEquals(cursor.hasMoreEntries(), true);

        cursor.markDelete(entries.get(0).getPosition());
        entries.forEach(e -> e.release());
    }

    @Test(timeOut = 20000)
    public void testEmptyManagedLedgerContent() throws Exception {
        ZooKeeper zk = bkc.getZkHandle();
        zk.create("/managed-ledger", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/managed-ledger/my_test_ledger", " ".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
    }

    @Test(timeOut = 20000)
    public void testProducerAndNoConsumer() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);

        ledger.addEntry("entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Since there are no consumers, older ledger will be deleted
        // in a short time (in a background thread)
        ledger.addEntry("entry-2".getBytes(Encoding));
        while (ledger.getNumberOfEntries() > 1) {
            log.debug("entries={}", ledger.getNumberOfEntries());
            Thread.sleep(100);
        }

        ledger.addEntry("entry-3".getBytes(Encoding));
        while (ledger.getNumberOfEntries() > 1) {
            log.debug("entries={}", ledger.getNumberOfEntries());
            Thread.sleep(100);
        }
    }

    @Test(timeOut = 20000)
    public void testTrimmer() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 0);

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));
        ledger.addEntry("entry-4".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 4);

        cursor.readEntries(1).forEach(e -> e.release());
        cursor.readEntries(1).forEach(e -> e.release());
        List<Entry> entries = cursor.readEntries(1);
        Position lastPosition = entries.get(0).getPosition();
        entries.forEach(e -> e.release());

        assertEquals(ledger.getNumberOfEntries(), 4);

        cursor.markDelete(lastPosition);

        while (ledger.getNumberOfEntries() != 2) {
            Thread.sleep(10);
        }
    }

    @Test(timeOut = 20000)
    public void testAsyncAddEntryAndSyncClose() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 0);

        final CountDownLatch counter = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.asyncAddEntry(content.getBytes(Encoding), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    counter.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    fail(exception.getMessage());
                }

            }, null);
        }

        counter.await();

        assertEquals(ledger.getNumberOfEntries(), 100);
    }

    @Test(timeOut = 20000)
    public void moveCursorToNextLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        log.debug("Added 1st message");
        List<Entry> entries = cursor.readEntries(1);
        log.debug("read message ok");
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        ledger.addEntry("entry-2".getBytes(Encoding));
        log.debug("Added 2nd message");
        ledger.addEntry("entry-3".getBytes(Encoding));
        log.debug("Added 3nd message");

        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 2);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(e -> e.release());

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 0);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 0);
    }

    @Test(timeOut = 20000)
    public void differentSessions() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 1);

        ledger.close();

        // Create a new factory and re-open the same managed ledger
        factory = new ManagedLedgerFactoryImpl(bkc, zkc);

        ledger = factory.open("my_test_ledger");

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        cursor = ledger.openCursor("c1");

        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 1);

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 2);

        ledger.close();
    }

    @Test(enabled = false)
    public void fenceManagedLedger() throws Exception {
        ManagedLedgerFactory factory1 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger1 = factory1.open("my_test_ledger");
        ManagedCursor cursor1 = ledger1.openCursor("c1");
        ledger1.addEntry("entry-1".getBytes(Encoding));

        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger2 = factory2.open("my_test_ledger");
        ManagedCursor cursor2 = ledger2.openCursor("c1");

        // At this point ledger1 must have been fenced
        try {
            ledger1.addEntry("entry-1".getBytes(Encoding));
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        try {
            ledger1.addEntry("entry-2".getBytes(Encoding));
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        try {
            cursor1.readEntries(10);
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        try {
            ledger1.openCursor("new cursor");
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        ledger2.addEntry("entry-2".getBytes(Encoding));

        assertEquals(cursor2.getNumberOfEntries(), 2);
        factory1.shutdown();
        factory2.shutdown();
    }

    @Test
    public void forceCloseLedgers() throws Exception {
        ManagedLedger ledger1 = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ledger1.openCursor("c1");
        ManagedCursor c2 = ledger1.openCursor("c2");
        ledger1.addEntry("entry-1".getBytes(Encoding));
        ledger1.addEntry("entry-2".getBytes(Encoding));
        ledger1.addEntry("entry-3".getBytes(Encoding));

        c2.readEntries(1).forEach(e -> e.release());
        c2.readEntries(1).forEach(e -> e.release());
        c2.readEntries(1).forEach(e -> e.release());

        ledger1.close();

        try {
            ledger1.addEntry("entry-3".getBytes(Encoding));
            fail("should not have reached this point");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            ledger1.openCursor("new-cursor");
            fail("should not have reached this point");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test
    public void closeLedgerWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("entry-1".getBytes(Encoding));

        stopZooKeeper();
        stopBookKeeper();

        try {
            ledger.close();
            // fail("should have thrown exception");
        } catch (ManagedLedgerException e) {
            // Ok
        }
    }

    @Test(timeOut = 20000)
    public void deleteWithErrors1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        PositionImpl position = (PositionImpl) ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Force delete a ledger and test that deleting the ML still happens
        // without errors
        bkc.deleteLedger(position.getLedgerId());
        ledger.delete();
    }

    @Test(timeOut = 20000)
    public void deleteWithErrors2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        stopZooKeeper();

        try {
            ledger.delete();
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        } catch (RejectedExecutionException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    public void readWithErrors1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        stopZooKeeper();
        stopBookKeeper();

        try {
            cursor.readEntries(10);
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            ledger.addEntry("dummy-entry-3".getBytes(Encoding));
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000, enabled = false)
    void concurrentAsyncOpen() throws Exception {
        final CountDownLatch counter = new CountDownLatch(2);

        class Result {
            ManagedLedger instance1 = null;
            ManagedLedger instance2 = null;
        }

        final Result result = new Result();
        factory.asyncOpen("my-test-ledger", new OpenLedgerCallback() {

            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.instance1 = ledger;
                counter.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);

        factory.asyncOpen("my-test-ledger", new OpenLedgerCallback() {

            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.instance2 = ledger;
                counter.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);

        counter.await();
        assertEquals(result.instance1, result.instance2);
        assertNotNull(result.instance1);
    }

    @Test // (timeOut = 20000)
    public void asyncOpenClosedLedger() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-closed-ledger");

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        c1.close();

        assertEquals(ledger.getNumberOfEntries(), 1);

        ledger.setFenced();

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedger instance1 = null;
        }

        final Result result = new Result();
        factory.asyncOpen("my-closed-ledger", new OpenLedgerCallback() {

            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.instance1 = ledger;
                counter.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);
        counter.await();
        assertNotNull(result.instance1);

        ManagedCursor c2 = result.instance1.openCursor("c1");
        List<Entry> entries = c2.readEntries(1);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

    }

    @Test
    public void getCursors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");

        assertEquals(Sets.newHashSet(ledger.getCursors()), Sets.newHashSet(c1, c2));

        c1.close();
        ledger.deleteCursor("c1");
        assertEquals(Sets.newHashSet(ledger.getCursors()), Sets.newHashSet(c2));

        c2.close();
        ledger.deleteCursor("c2");
        assertEquals(Sets.newHashSet(ledger.getCursors()), Sets.newHashSet());
    }

    @Test
    public void ledgersList() throws Exception {
        MetaStore store = factory.getMetaStore();

        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet());
        ManagedLedger ledger1 = factory.open("ledger1");
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet("ledger1"));
        ManagedLedger ledger2 = factory.open("ledger2");
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet("ledger1", "ledger2"));
        ledger1.delete();
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet("ledger2"));
        ledger2.delete();
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet());
    }

    @Test
    public void testCleanup() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes(Encoding));
        assertEquals(bkc.getLedgers().size(), 2);

        ledger.delete();
        assertEquals(bkc.getLedgers().size(), 0);
    }

    @Test(timeOut = 20000)
    public void testAsyncCleanup() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes(Encoding));
        assertEquals(bkc.getLedgers().size(), 2);

        final CountDownLatch latch = new CountDownLatch(1);

        ledger.asyncDelete(new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                fail("should have succeeded");
            }

            @Override
            public void deleteLedgerComplete(Object ctx) {
                latch.countDown();
            }
        }, null);

        latch.await();
        assertEquals(bkc.getLedgers().size(), 0);
    }

    @Test(timeOut = 20000)
    public void testReopenAndCleanup() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes(Encoding));
        ledger.close();
        Thread.sleep(100);
        assertEquals(bkc.getLedgers().size(), 1);

        factory.shutdown();

        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");
        Thread.sleep(100);
        assertEquals(bkc.getLedgers().size(), 2);

        ledger.close();
        factory.open("my_test_ledger", new ManagedLedgerConfig()).delete();
        Thread.sleep(100);
        assertEquals(bkc.getLedgers().size(), 0);

        factory.shutdown();
    }

    @Test(timeOut = 20000)
    public void doubleOpen() throws Exception {
        ManagedLedger ledger1 = factory.open("my_test_ledger");
        ManagedLedger ledger2 = factory.open("my_test_ledger");

        assertTrue(ledger1 == ledger2);
    }

    @Test
    public void compositeNames() throws Exception {
        // Should not throw exception
        factory.open("my/test/ledger");
    }

    @Test
    public void previousPosition() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("my_cursor");

        Position p0 = cursor.getMarkDeletedPosition();
        // This is expected because p0 is already an "invalid" position (since no entry has been mark-deleted yet)
        assertEquals(ledger.getPreviousPosition((PositionImpl) p0), p0);

        // Force to close an empty ledger
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        // again
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        PositionImpl pBeforeWriting = ledger.getLastPosition();
        PositionImpl p1 = (PositionImpl) ledger.addEntry("entry".getBytes());
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        Position p2 = ledger.addEntry("entry".getBytes());
        Position p3 = ledger.addEntry("entry".getBytes());
        Position p4 = ledger.addEntry("entry".getBytes());

        assertEquals(ledger.getPreviousPosition(p1), pBeforeWriting);
        assertEquals(ledger.getPreviousPosition((PositionImpl) p2), p1);
        assertEquals(ledger.getPreviousPosition((PositionImpl) p3), p2);
        assertEquals(ledger.getPreviousPosition((PositionImpl) p4), p3);
    }

    /**
     * Reproduce a race condition between opening cursors and concurrent mark delete operations
     */
    @Test(timeOut = 20000)
    public void testOpenRaceCondition() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        final ManagedLedger ledger = factory.open("my-ledger", config);
        final ManagedCursor c1 = ledger.openCursor("c1");

        final int N = 1000;
        final Position position = ledger.addEntry("entry-0".getBytes());
        Executor executor = Executors.newCachedThreadPool();
        final CountDownLatch counter = new CountDownLatch(2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < N; i++) {
                        c1.markDelete(position);
                    }
                    counter.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < N; i++) {
                        ledger.openCursor("cursor-" + i);
                    }
                    counter.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // If there is the race condition, this method will not complete triggering the test timeout
        counter.await();
    }

    @Test
    public void invalidateConsumedEntriesFromCache() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache entryCache = ledger.entryCache;

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ManagedCursorImpl c2 = (ManagedCursorImpl) ledger.openCursor("c2");

        PositionImpl p1 = (PositionImpl) ledger.addEntry("entry-1".getBytes());
        PositionImpl p2 = (PositionImpl) ledger.addEntry("entry-2".getBytes());
        PositionImpl p3 = (PositionImpl) ledger.addEntry("entry-3".getBytes());
        PositionImpl p4 = (PositionImpl) ledger.addEntry("entry-4".getBytes());

        assertEquals(entryCache.getSize(), 7 * 4);
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        c2.setReadPosition(p3);
        ledger.discardEntriesFromCache(c2, p2);

        assertEquals(entryCache.getSize(), 7 * 4);
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        c1.setReadPosition(p2);
        ledger.discardEntriesFromCache(c1, p1);
        assertEquals(entryCache.getSize(), 7 * 3);
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        c1.setReadPosition(p3);
        ledger.discardEntriesFromCache(c1, p2);
        assertEquals(entryCache.getSize(), 7 * 2);
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        ledger.deactivateCursor(c1);
        assertEquals(entryCache.getSize(), 7 * 2); // as c2.readPosition=p3 => Cache contains p3,p4
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        c2.setReadPosition(p4);
        ledger.discardEntriesFromCache(c2, p3);
        assertEquals(entryCache.getSize(), 7);
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        ledger.deactivateCursor(c2);
        assertEquals(entryCache.getSize(), 0);
        assertEquals(cacheManager.getSize(), entryCache.getSize());
    }

    @Test
    public void discardEmptyLedgersOnClose() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        c1.close();
        ledger.close();

        // re-open
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        assertEquals(ledger.getLedgersInfoAsList().size(), 2); // 1 ledger with 1 entry and the current writing ledger

        c1.close();
        ledger.close();

        // re-open, now the previous empty ledger should have been discarded
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        assertEquals(ledger.getLedgersInfoAsList().size(), 2); // 1 ledger with 1 entry, and the current
        // writing ledger
    }

    @Test
    public void discardEmptyLedgersOnError() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        bkc.failNow(BKException.Code.NoBookieAvailableException);
        zkc.failNow(Code.CONNECTIONLOSS);
        try {
            ledger.addEntry("entry".getBytes());
            fail("Should have received exception");
        } catch (ManagedLedgerException e) {
            // Ok
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 0);

        // Next write should fail as well
        try {
            ledger.addEntry("entry".getBytes());
            fail("Should have received exception");
        } catch (ManagedLedgerException e) {
            // Ok
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 0);
        assertEquals(ledger.getNumberOfEntries(), 0);
    }

    @Test
    public void cursorReadsWithDiscardedEmptyLedgers() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = c1.getReadPosition();

        c1.close();
        ledger.close();

        // re-open
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.hasMoreEntries(), false);

        ledger.addEntry("entry".getBytes());

        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.hasMoreEntries(), true);

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        List<Entry> entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        assertEquals(c1.hasMoreEntries(), false);
        assertEquals(c1.readEntries(1).size(), 0);

        c1.seek(p1);
        assertEquals(c1.hasMoreEntries(), true);
        assertEquals(c1.getNumberOfEntries(), 1);

        entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());
        assertEquals(c1.readEntries(1).size(), 0);
    }

    @Test
    public void cursorReadsWithDiscardedEmptyLedgersStillListed() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-1".getBytes());
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        ledger.addEntry("entry-2".getBytes());

        final LedgerInfo l1info = ledger.getLedgersInfoAsList().get(0);
        final LedgerInfo l2info = ledger.getLedgersInfoAsList().get(1);

        ledger.close();

        // Add the deleted ledger back in the meta-data to simulate an empty ledger that was deleted but not removed
        // from the list of ledgers
        final CountDownLatch counter = new CountDownLatch(1);
        final MetaStore store = factory.getMetaStore();
        store.getManagedLedgerInfo("my_test_ledger", new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                // Update the list
                ManagedLedgerInfo.Builder info = ManagedLedgerInfo.newBuilder(result);
                info.clearLedgerInfo();
                info.addLedgerInfo(LedgerInfo.newBuilder().setLedgerId(l1info.getLedgerId()).build());
                info.addLedgerInfo(l2info);

                store.asyncUpdateLedgerIds("my_test_ledger", info.build(), version, new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat version) {
                        counter.countDown();
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        counter.countDown();
                    }
                });
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                counter.countDown();
            }
        });

        // Wait for the change to be effective
        counter.await();

        // Delete the ledger and mantain it in the ledgers list
        bkc.deleteLedger(l1info.getLedgerId());

        // re-open
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getNumberOfEntries(), 1);
        assertEquals(c1.hasMoreEntries(), true);
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        assertEquals(c1.hasMoreEntries(), false);
        entries = c1.readEntries(1);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());
    }

    @Test
    public void addEntryWithOffset() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("012345678".getBytes(), 2, 3);

        List<Entry> entries = c1.readEntries(1);
        assertEquals(entries.get(0).getLength(), 3);
        Entry entry = entries.get(0);
        assertEquals(new String(entry.getData()), "234");
        entry.release();
    }

    @Test
    public void totalSizeTest() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", conf);
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry(new byte[10], 1, 8);

        assertEquals(ledger.getTotalSize(), 8);

        PositionImpl p2 = (PositionImpl) ledger.addEntry(new byte[12], 2, 5);

        assertEquals(ledger.getTotalSize(), 13);
        c1.markDelete(new PositionImpl(p2.getLedgerId(), -1));

        // Wait for background trimming
        Thread.sleep(400);
        assertEquals(ledger.getTotalSize(), 5);
    }

    @Test
    public void testMinimumRolloverTime() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setMaxEntriesPerLedger(1);
        conf.setMinimumRolloverTime(1, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", conf);
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        Thread.sleep(1000);

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
    }

    @Test
    public void testMaximumRolloverTime() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setMaxEntriesPerLedger(5);
        conf.setMinimumRolloverTime(1, TimeUnit.SECONDS);
        conf.setMaximumRolloverTime(1, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_maxtime_ledger", conf);
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        Thread.sleep(2000);

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
    }

    @Test
    public void testRetention() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(1);
        config.setRetentionTime(1, TimeUnit.HOURS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1");
        ml.addEntry("iamaverylongmessagethatshouldberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("retention_test_ledger", config);
        c1 = ml.openCursor("c1");
        ml.addEntry("shortmessage".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();
        assertTrue(ml.getLedgersInfoAsList().size() > 1);
        assertTrue(ml.getTotalSize() > "shortmessage".getBytes().length);
    }

    @Test(enabled = true)
    public void testNoRetention() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(0);
        config.setMaxEntriesPerLedger(1);
        // Default is no-retention

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("noretention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1noretention");
        ml.addEntry("iamaverylongmessagethatshouldnotberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("noretention_test_ledger", config);
        c1 = ml.openCursor("c1noretention");
        ml.addEntry("shortmessage".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        // sleep for trim
        Thread.sleep(1000);
        ml.close();

        assertTrue(ml.getLedgersInfoAsList().size() <= 1);
        assertTrue(ml.getTotalSize() <= "shortmessage".getBytes().length);
    }

    @Test
    public void testDeletionAfterRetention() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(0);
        config.setMaxEntriesPerLedger(1);
        config.setRetentionTime(1, TimeUnit.SECONDS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1noretention");
        ml.addEntry("iamaverylongmessagethatshouldnotberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        c1 = ml.openCursor("c1noretention");
        ml.addEntry("shortmessage".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        // let retention expire
        Thread.sleep(1000);
        ml.close();
        // sleep for trim
        Thread.sleep(100);
        assertTrue(ml.getLedgersInfoAsList().size() <= 1);
        assertTrue(ml.getTotalSize() <= "shortmessage".getBytes().length);
    }

    @Test
    public void testTimestampOnWorkingLedger() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setMaxEntriesPerLedger(1);
        conf.setRetentionSizeInMB(10);
        conf.setRetentionTime(1, TimeUnit.HOURS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("my_test_ledger", conf);
        ml.openCursor("c1");
        ml.addEntry("msg1".getBytes());
        Iterator<LedgerInfo> iter = ml.getLedgersInfoAsList().iterator();
        long ts = -1;
        while (iter.hasNext()) {
            LedgerInfo i = iter.next();
            if (iter.hasNext()) {
                assertTrue(ts <= i.getTimestamp(), i.toString());
                ts = i.getTimestamp();
            } else {
                // the last timestamp can be
                // 0 if it is still opened
                // >0 if it is closed after the addEntry see OpAddEntry#addComplete()
                assertTrue(i.getTimestamp() == 0 || ts <= i.getTimestamp(), i.toString());
            }
        }

        ml.addEntry("msg02".getBytes());

        ml.close();
        // Thread.sleep(1000);
        iter = ml.getLedgersInfoAsList().iterator();
        ts = -1;
        while (iter.hasNext()) {
            LedgerInfo i = iter.next();
            if (iter.hasNext()) {
                assertTrue(ts <= i.getTimestamp(), i.toString());
                ts = i.getTimestamp();
            } else {
                assertTrue(i.getTimestamp() > 0, "well closed LedgerInfo should set a timestamp > 0");
            }
        }
    }

    @Test
    public void testBackwardCompatiblityForMeta() throws Exception {
        final ManagedLedgerInfo[] storedMLInfo = new ManagedLedgerInfo[3];
        final Stat[] versions = new Stat[1];

        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setMaxEntriesPerLedger(1);
        conf.setRetentionSizeInMB(10);
        conf.setRetentionTime(1, TimeUnit.HOURS);

        ManagedLedger ml = factory.open("backward_test_ledger", conf);
        ml.openCursor("c1");
        ml.addEntry("msg1".getBytes());
        ml.addEntry("msg2".getBytes());
        ml.close();

        MetaStore store = new MetaStoreImplZookeeper(zkc, executor);
        CountDownLatch l1 = new CountDownLatch(1);

        // obtain the ledger info
        store.getManagedLedgerInfo("backward_test_ledger", new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                storedMLInfo[0] = result;
                versions[0] = version;
                l1.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                fail("on get ManagedLedgerInfo backward_test_ledger");
            }
        });

        l1.await();
        ManagedLedgerInfo.Builder builder1 = ManagedLedgerInfo.newBuilder();

        // simulate test for old ledger with no timestampl
        for (LedgerInfo info : storedMLInfo[0].getLedgerInfoList()) {
            LedgerInfo noTimestamp = ManagedLedgerInfo.LedgerInfo.newBuilder().mergeFrom(info).clearTimestamp().build();
            assertFalse(noTimestamp.hasTimestamp(), "expected old version info with no timestamp");
            builder1.addLedgerInfo(noTimestamp);

        }
        storedMLInfo[1] = builder1.build();

        // test timestamp on new ledger

        CountDownLatch l2 = new CountDownLatch(1);
        store.asyncUpdateLedgerIds("backward_test_ledger", storedMLInfo[1], versions[0], new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat version) {
                l2.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                fail("on asyncUpdateLedgerIds");
            }
        });

        // verify that after update ledgers have timestamp

        ManagedLedgerImpl newVersionLedger = (ManagedLedgerImpl) factory.open("backward_test_ledger", conf);
        List<LedgerInfo> mlInfo = newVersionLedger.getLedgersInfoAsList();

        assertTrue(mlInfo.stream().allMatch(new Predicate<LedgerInfo>() {
            @Override
            public boolean test(LedgerInfo ledgerInfo) {
                return ledgerInfo.hasTimestamp();
            }
        }));
    }

    @Test
    public void testEstimatedBacklogSize() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testEstimatedBacklogSize");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry(new byte[1024]);
        Position position2 = ledger.addEntry(new byte[1024]);
        ledger.addEntry(new byte[1024]);
        ledger.addEntry(new byte[1024]);
        Position lastPosition = ledger.addEntry(new byte[1024]);

        long backlog = ledger.getEstimatedBacklogSize();
        assertEquals(backlog, 1024 * 5);

        List<Entry> entries = c1.readEntries(2);
        entries.forEach(Entry::release);
        c1.markDelete(position2);

        backlog = ledger.getEstimatedBacklogSize();
        assertEquals(backlog, 1024 * 3);

        entries = c1.readEntries(3);
        entries.forEach(Entry::release);
        c1.markDelete(lastPosition);

        backlog = ledger.getEstimatedBacklogSize();
        assertEquals(backlog, 0);
    }

    @Test
    public void testGetNextValidPosition() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testGetNextValidPosition", conf);
        ManagedCursor c1 = ledger.openCursor("c1");

        PositionImpl p1 = (PositionImpl) ledger.addEntry("entry1".getBytes());
        PositionImpl p2 = (PositionImpl) ledger.addEntry("entry2".getBytes());
        PositionImpl p3 = (PositionImpl) ledger.addEntry("entry3".getBytes());

        assertEquals(ledger.getNextValidPosition((PositionImpl) c1.getMarkDeletedPosition()), p1);
        assertEquals(ledger.getNextValidPosition(p1), p2);
        assertEquals(ledger.getNextValidPosition(p3), PositionImpl.get(p3.getLedgerId(), p3.getEntryId() + 1));
    }

    /**
     * Validations:
     *
     * 1. openCursor : activates cursor 2. EntryCache keeps entries: till entry will be read by all active cursors a.
     * active cursor1 reads entry b. EntryCache keeps entry till cursor2 reads c. active cursor2 reads entry d.
     * EntryCache deletes all read entries by cursor1 and cursor2 3. EntryCache discard entries: deactivate slower
     * cursor a. active cursor1 read all entries b. EntryCache keeps entry till cursor2 reads c. deactivate cursor2 d.
     * EntryCache deletes all read entries by cursor1
     *
     * @throws Exception
     */
    @Test
    public void testActiveDeactiveCursorWithDiscardEntriesFromCache() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cache_eviction_ledger");

        // Open Cursor also adds cursor into activeCursor-container
        ManagedCursor cursor1 = ledger.openCursor("c1");
        ManagedCursor cursor2 = ledger.openCursor("c2");
        Set<ManagedCursor> activeCursors = Sets.newHashSet();
        activeCursors.add(cursor1);
        activeCursors.add(cursor2);
        Field cacheField = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        cacheField.setAccessible(true);
        EntryCacheImpl entryCache = (EntryCacheImpl) cacheField.get(ledger);

        Iterator<ManagedCursor> activeCursor = ledger.getActiveCursors().iterator();

        // (1) validate cursors are part of activeCursorContainer
        activeCursors.remove(activeCursor.next());
        activeCursors.remove(activeCursor.next());
        assertTrue(activeCursors.isEmpty());
        assertFalse(activeCursor.hasNext());

        final int totalInsertedEntries = 50;
        for (int i = 0; i < totalInsertedEntries; i++) {
            String content = "entry"; // 5 bytes
            ledger.addEntry(content.getBytes());
        }

        // (2) Validate: as ledger has active cursors: all entries have been cached
        assertEquals((5 * totalInsertedEntries), entryCache.getSize());

        // read 20 entries
        final int readEntries = 20;
        List<Entry> entries1 = cursor1.readEntries(readEntries);
        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }
        // Acknowledge only on last entry
        cursor1.markDelete(entries1.get(entries1.size() - 1).getPosition());

        // read after a second: as RateLimiter limits triggering of removing cache
        Thread.sleep(1000);

        List<Entry> entries2 = cursor2.readEntries(readEntries);
        for (Entry entry : entries2) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }
        // Acknowledge only on last entry
        cursor2.markDelete((entries2.get(entries2.size() - 1)).getPosition());

        // (3) Validate: cache should remove all entries read by both active cursors
		log.info("expected, found : {}, {}", (5 * (totalInsertedEntries - readEntries)), entryCache.getSize());
        assertEquals((5 * (totalInsertedEntries - readEntries)), entryCache.getSize());

        final int remainingEntries = totalInsertedEntries - readEntries;
        entries1 = cursor1.readEntries(remainingEntries);
        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }
        // Acknowledge only on last entry
        cursor1.markDelete(entries1.get(entries1.size() - 1).getPosition());

        // (4) Validate: cursor2 is active cursor and has not read these entries yet: so, cache should not remove these
        // entries
        assertEquals((5 * (remainingEntries)), entryCache.getSize());

        ledger.deactivateCursor(cursor2);

        // (5) Validate: cursor2 is not active cursor now: cache should have removed all entries read by active cursor1
        assertEquals(0, entryCache.getSize());

        log.info("Finished reading entries");

        ledger.close();
    }

    @Test
    public void testActiveDeactiveCursor() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cache_eviction_ledger");

        Field cacheField = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        cacheField.setAccessible(true);
        EntryCacheImpl entryCache = (EntryCacheImpl) cacheField.get(ledger);

        final int totalInsertedEntries = 20;
        for (int i = 0; i < totalInsertedEntries; i++) {
            String content = "entry"; // 5 bytes
            ledger.addEntry(content.getBytes());
        }

        // (1) Validate: cache not stores entries as no active cursor
        assertEquals(0, entryCache.getSize());

        // Open Cursor also adds cursor into activeCursor-container
        ManagedCursor cursor1 = ledger.openCursor("c1");
        ManagedCursor cursor2 = ledger.openCursor("c2");
        ledger.deactivateCursor(cursor2);

        for (int i = 0; i < totalInsertedEntries; i++) {
            String content = "entry"; // 5 bytes
            ledger.addEntry(content.getBytes());
        }

        // (2) Validate: cache stores entries as active cursor has not read message
        assertEquals((5 * totalInsertedEntries), entryCache.getSize());

        // read 20 entries
        List<Entry> entries1 = cursor1.readEntries(totalInsertedEntries);
        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        // (3) Validate: cache discards all entries as read by active cursor
        assertEquals(0, entryCache.getSize());

        ledger.close();
    }

    @Test
    public void testCursorRecoveryForEmptyLedgers() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testCursorRecoveryForEmptyLedgers");
        ManagedCursor c1 = ledger.openCursor("c1");

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEquals(c1.getMarkDeletedPosition(), ledger.lastConfirmedEntry);

        c1.close();
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("testCursorRecoveryForEmptyLedgers");
        c1 = ledger.openCursor("c1");

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEquals(c1.getMarkDeletedPosition(), ledger.lastConfirmedEntry);
    }

    @Test
    public void testBacklogCursor() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cache_backlog_ledger");

        final long maxMessageCacheRetentionTimeMillis = 100;
        Field field = ManagedLedgerImpl.class.getDeclaredField("maxMessageCacheRetentionTimeMillis");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(ledger, maxMessageCacheRetentionTimeMillis);
        Field backlogThresholdField = ManagedLedgerImpl.class.getDeclaredField("maxActiveCursorBacklogEntries");
        backlogThresholdField.setAccessible(true);
        final long maxActiveCursorBacklogEntries = (long) backlogThresholdField.get(ledger);

        // Open Cursor also adds cursor into activeCursor-container
        ManagedCursor cursor1 = ledger.openCursor("c1");
        ManagedCursor cursor2 = ledger.openCursor("c2");

        final int totalBacklogSizeEntries = (int) maxActiveCursorBacklogEntries;
        CountDownLatch latch = new CountDownLatch(totalBacklogSizeEntries);
        for (int i = 0; i < totalBacklogSizeEntries + 1; i++) {
            String content = "entry"; // 5 bytes
            ByteBuf entry = getMessageWithMetadata(content.getBytes());
            ledger.asyncAddEntry(entry, new AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    latch.countDown();
                    entry.release();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                    entry.release();
                }

            }, null);
        }
        latch.await();

        // Verify: cursors are active as :haven't started deactivateBacklogCursor scan
        assertTrue(cursor1.isActive());
        assertTrue(cursor2.isActive());

        // it allows message to be older enough to be considered in backlog
        Thread.sleep(maxMessageCacheRetentionTimeMillis * 2);

        // deactivate backlog cursors
        ledger.checkBackloggedCursors();
        Thread.sleep(100);

        // both cursors have to be inactive
        assertFalse(cursor1.isActive());
        assertFalse(cursor2.isActive());

        // read entries so, cursor1 reaches maxBacklog threshold again to be active again
        List<Entry> entries1 = cursor1.readEntries(50);
        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        // activate cursors which caught up maxbacklog threshold
        ledger.checkBackloggedCursors();

        // verify: cursor1 has consumed messages so, under maxBacklog threshold => active
        assertTrue(cursor1.isActive());

        // verify: cursor2 has not consumed messages so, above maxBacklog threshold => inactive
        assertFalse(cursor2.isActive());

        ledger.close();
    }

    @Test
    public void testConcurrentOpenCursor() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testConcurrentOpenCursor");

        final AtomicReference<ManagedCursor> cursor1 = new AtomicReference<>(null);
        final AtomicReference<ManagedCursor> cursor2 = new AtomicReference<>(null);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch latch = new CountDownLatch(2);

        cachedExecutor.execute(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
            }
            ledger.asyncOpenCursor("c1", new OpenCursorCallback() {

                @Override
                public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                    cursor1.set(cursor);
                    latch.countDown();
                }
            }, null);
        });

        cachedExecutor.execute(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
            }
            ledger.asyncOpenCursor("c1", new OpenCursorCallback() {

                @Override
                public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                    cursor2.set(cursor);
                    latch.countDown();
                }
            }, null);
        });

        latch.await();
        assertNotNull(cursor1.get());
        assertNotNull(cursor2.get());
        assertEquals(cursor1.get(), cursor2.get());

        ledger.close();
    }

    public ByteBuf getMessageWithMetadata(byte[] data) throws IOException {
        MessageMetadata messageData = MessageMetadata.newBuilder().setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name").setSequenceId(0).build();
        ByteBuf payload = Unpooled.wrappedBuffer(data, 0, data.length);

        int msgMetadataSize = messageData.getSerializedSize();
        int headersSize = 4 + msgMetadataSize;
        ByteBuf headers = PooledByteBufAllocator.DEFAULT.buffer(headersSize, headersSize);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(headers);
        headers.writeInt(msgMetadataSize);
        messageData.writeTo(outStream);
        outStream.recycle();
        return DoubleByteBuf.get(headers, payload);
    }

}
