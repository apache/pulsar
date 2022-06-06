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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.testng.annotations.Test;

public class ManagedLedgerBkTest extends BookKeeperClusterTestCase {

    public ManagedLedgerBkTest() {
        super(2);
    }

    @Test
    public void testSimpleRead() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my-ledger", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int N = 1;

        for (int i = 0; i < N; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(N);
        assertEquals(N, entries.size());
        entries.forEach(e -> e.release());
    }

    @Test
    public void testBookieFailure() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedger ledger = factory.open("my-ledger", config);
        ManagedCursor cursor = ledger.openCursor("my-cursor");
        ledger.addEntry("entry-0".getBytes());

        killBookie(1);

        // Now we want to simulate that:
        // 1. The write operation fails because we only have 1 bookie available
        // 2. The bk client cannot properly close the ledger (finalizing the number of entries) because ZK is also
        // not available
        // 3. When we re-establish the service one, the ledger recovery will be triggered and the half-committed entry
        // is restored

        // Force to close the ZK client object so that BK will fail to close the ledger
        bkc.getZkHandle().close();

        try {
            ledger.addEntry("entry-1".getBytes());
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        bkc.close();
        metadataStore.unsetAlwaysFail();

        bkc = new BookKeeperTestClient(baseClientConf);
        startNewBookie();

        // Reconnect a new bk client
        factory.shutdown();

        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory.open("my-ledger", config);
        cursor = ledger.openCursor("my-cursor");

        // Next add should succeed
        ledger.addEntry("entry-2".getBytes());

        assertEquals(3, cursor.getNumberOfEntriesInBacklog(false));

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-0", new String(entries.get(0).getData()));
        entries.forEach(e -> e.release());

        // entry-1 which was half-committed will get fully committed during the recovery phase
        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-1", new String(entries.get(0).getData()));
        entries.forEach(e -> e.release());

        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-2", new String(entries.get(0).getData()));
        entries.forEach(e -> e.release());
        factory.shutdown();
    }

    @Test
    public void verifyConcurrentUsage() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();

        config.setMaxCacheSize(100 * 1024 * 1024);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-ledger", conf);

        int NumProducers = 1;
        int NumConsumers = 1;

        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(NumProducers + NumConsumers + 1);

        List<Future<?>> futures = Lists.newArrayList();

        for (int i = 0; i < NumProducers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();

                    while (!done.get()) {
                        ledger.addEntry("entry".getBytes());
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }

        for (int i = 0; i < NumConsumers; i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();

                    ManagedCursor cursor = ledger.openCursor("my-cursor-" + idx);

                    while (!done.get()) {
                        List<Entry> entries = cursor.readEntries(1);
                        if (!entries.isEmpty()) {
                            cursor.markDelete(entries.get(0).getPosition());
                        }

                        entries.forEach(e -> e.release());
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }

        barrier.await();

        Thread.sleep(1 * 1000);

        done.set(true);
        for (Future<?> future : futures) {
            future.get();
        }

        factory.getMbean().refreshStats(1, TimeUnit.SECONDS);

        assertTrue(factory.getMbean().getCacheHitsRate() > 0.0);
        assertEquals(factory.getMbean().getCacheMissesRate(), 0.0);
        assertTrue(factory.getMbean().getCacheHitsThroughput() > 0.0);
        assertEquals(factory.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void testSimple() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig();
        mlConfig.setEnsembleSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1).setWriteQuorumSize(1);
        // set the data ledger size
        mlConfig.setMaxEntriesPerLedger(100);
        // set the metadata ledger size to 1 to kick off many ledger switching cases
        mlConfig.setMetadataMaxEntriesPerLedger(2);
        ManagedLedger ledger = factory.open("ml-simple-ledger", mlConfig);

        ledger.addEntry("test".getBytes());
    }

    @Test
    public void testConcurrentMarkDelete() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig();
        mlConfig.setEnsembleSize(1).setWriteQuorumSize(1)
            .setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
            .setMetadataAckQuorumSize(1);
        // set the data ledger size
        mlConfig.setMaxEntriesPerLedger(100);
        // set the metadata ledger size to 1 to kick off many ledger switching cases
        mlConfig.setMetadataMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("ml-markdelete-ledger", mlConfig);

        final List<Position> addedEntries = Lists.newArrayList();

        int numCursors = 10;
        final CyclicBarrier barrier = new CyclicBarrier(numCursors);

        List<ManagedCursor> cursors = Lists.newArrayList();
        for (int i = 0; i < numCursors; i++) {
            cursors.add(ledger.openCursor(String.format("c%d", i)));
        }

        for (int i = 0; i < 50; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        List<Future<?>> futures = Lists.newArrayList();

        for (ManagedCursor cursor : cursors) {
            futures.add(executor.submit(() -> {
                barrier.await();

                for (Position position : addedEntries) {
                    cursor.markDelete(position);
                }

                return null;
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        // Since in this test we roll-over the cursor ledger every 10 entries acknowledged, the background roll back
        // might still be happening when the futures are completed.
        Thread.sleep(1000);
    }

    @Test
    public void asyncMarkDeleteAndClose() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        ManagedLedgerConfig config = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        List<Position> positions = Lists.newArrayList();

        for (int i = 0; i < 10; i++) {
            Position p = ledger.addEntry("entry".getBytes());
            positions.add(p);
        }

        final CountDownLatch counter = new CountDownLatch(positions.size());
        final AtomicBoolean gotException = new AtomicBoolean(false);

        for (Position p : positions) {
            cursor.asyncDelete(p, new DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    // Ok
                    counter.countDown();
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    exception.printStackTrace();
                    gotException.set(true);
                    counter.countDown();
                }
            }, null);
        }

        counter.await();

        cursor.close();
        ledger.close();

        assertFalse(gotException.get());
    }

    /**
     * When auto-replication is triggered, if there were no writes on the ML during the grace period, auto-replication
     * will close the ledger an re-replicate it. After that, the next write will get a FencedException. We should
     * recover from this condition by creating a new ledger and retrying the write.
     */
    @Test
    public void ledgerFencedByAutoReplication() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        ManagedCursor c1 = ledger.openCursor("c1");

        PositionImpl p1 = (PositionImpl) ledger.addEntry("entry-1".getBytes());

        // Trigger the closure of the data ledger
        bkc.openLedger(p1.getLedgerId(), BookKeeper.DigestType.CRC32C, new byte[] {});

        ledger.addEntry("entry-2".getBytes());

        assertEquals(2, c1.getNumberOfEntries());
        assertEquals(2, c1.getNumberOfEntriesInBacklog(false));

        PositionImpl p3 = (PositionImpl) ledger.addEntry("entry-3".getBytes());

        // Now entry-2 should have been written before entry-3
        assertEquals(3, c1.getNumberOfEntries());
        assertEquals(3, c1.getNumberOfEntriesInBacklog(false));
        assertTrue(p1.getLedgerId() != p3.getLedgerId());
    }

    /**
     * When another process steals the ML, the old instance should not succeed in any operation
     */
    @Test
    public void ledgerFencedByFailover() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory1.open("my_test_ledger", config);
        ledger1.openCursor("c");

        ledger1.addEntry("entry-1".getBytes());

        // Open the ML from another factory
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerImpl ledger2 = (ManagedLedgerImpl) factory2.open("my_test_ledger", config);
        ManagedCursor c2 = ledger2.openCursor("c");

        try {
            ledger1.addEntry("entry-2".getBytes());
            fail("Should have failed");
        } catch (ManagedLedgerException e) {
            // Ok
        }

        ledger2.addEntry("entry-2".getBytes());

        try {
            ledger1.addEntry("entry-2".getBytes());
            fail("Should have failed");
        } catch (ManagedLedgerException bve) {
            // Ok
        }

        assertEquals(2, c2.getNumberOfEntriesInBacklog(false));
    }

    @Test
    public void testOfflineTopicBacklog() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("property/cluster/namespace/my-ledger", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int N = 1;

        for (int i = 0; i < N; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(N);
        assertEquals(N, entries.size());
        entries.forEach(e -> e.release());
        ledger.close();

        ManagedLedgerOfflineBacklog offlineTopicBacklog = new ManagedLedgerOfflineBacklog(
                DigestType.CRC32, "".getBytes(Charsets.UTF_8), "", false);
        PersistentOfflineTopicStats offlineTopicStats = offlineTopicBacklog.getEstimatedUnloadedTopicBacklog(
                (ManagedLedgerFactoryImpl) factory, "property/cluster/namespace/my-ledger");
        assertNotNull(offlineTopicStats);
    }

    @Test(timeOut = 20000)
    void testResetCursorAfterRecovery() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig conf = new ManagedLedgerConfig().setMaxEntriesPerLedger(10).setEnsembleSize(1)
                .setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger", conf);
        ManagedCursor cursor = ledger.openCursor("trc1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes());
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes());
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes());
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes());

        cursor.markDelete(p3);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_move_cursor_ledger", conf);
        cursor = ledger.openCursor("trc1");

        assertEquals(cursor.getMarkDeletedPosition(), p3);
        assertEquals(cursor.getReadPosition(), p4);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 1);

        cursor.resetCursor(p2);
        assertEquals(cursor.getMarkDeletedPosition(), p1);
        assertEquals(cursor.getReadPosition(), p2);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 3);
    }

    @Test(timeOut = 30000)
    public void managedLedgerClosed() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        int N = 100;

        AtomicReference<ManagedLedgerException> res = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(N);

        for (int i = 0; i < N; i++) {
            ledger1.asyncAddEntry(("entry-" + i).getBytes(), new AddEntryCallback() {

                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    res.compareAndSet(null, exception);
                    latch.countDown();
                }
            }, null);

            if (i == 1) {
                ledger1.close();
            }
        }

        // Ensures all the callback must have been invoked
        latch.await();
        assertNotNull(res.get());
        assertEquals(res.get().getClass(), ManagedLedgerAlreadyClosedException.class);
    }

    @Test
    public void testChangeCrcType() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        config.setDigestType(DigestType.CRC32);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-0".getBytes());
        ledger.addEntry("entry-1".getBytes());
        ledger.addEntry("entry-2".getBytes());

        ledger.close();

        config.setDigestType(DigestType.CRC32C);
        ledger = factory.open("my_test_ledger", config);
        c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-3".getBytes());

        assertEquals(c1.getNumberOfEntries(), 4);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);

        List<Entry> entries = c1.readEntries(4);
        assertEquals(entries.size(), 4);
        for (int i = 0; i < 4; i++) {
            assertEquals(new String(entries.get(i).getData()), "entry-" + i);
        }
    }



}
