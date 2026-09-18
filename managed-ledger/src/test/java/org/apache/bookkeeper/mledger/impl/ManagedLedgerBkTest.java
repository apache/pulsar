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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.rawEntryConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarBookKeeperTestClient;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
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
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.mledger.util.ThrowableToStringUtil;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class ManagedLedgerBkTest extends BookKeeperClusterTestCase {

    private final ObjectMapper jackson = new ObjectMapper();

    public ManagedLedgerBkTest() {
        super(2);
        // Use the v2 wire protocol so that reads exercise the BookKeeper batch read API against real bookies
        baseClientConf.setUseV2WireProtocol(true);
    }

    @Test
    public void testBatchReadNotUsedWithV3WireProtocolClient() throws Exception {
        // The suite's client uses the v2 wire protocol; a v3 client cannot batch read, so reads must not use it
        ClientConfiguration v3ClientConf = new ClientConfiguration(baseClientConf);
        v3ClientConf.setUseV2WireProtocol(false);
        @Cleanup
        BookKeeper v3Client = new PulsarBookKeeperTestClient(v3ClientConf);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, v3Client);
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);
        assertFalse(ledger.isBatchReadEnabled());
        ManagedCursor cursor = ledger.openCursor("c1");
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }
        List<Entry> entries = cursor.readEntries(10);
        assertEquals(entries.size(), 10);
        entries.forEach(Entry::release);
    }

    @Test
    public void testSimpleRead() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int num = 1;

        for (int i = 0; i < num; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(num);
        assertEquals(num, entries.size());
        entries.forEach(Entry::release);
    }

    @Test
    public void testSeekRetracksCursorAfterUntracking() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = rawEntryConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1);
        config.setCacheEvictionByExpectedReadCount(true);
        config.setCacheEvictionByMarkDeletedPosition(false);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-untracking-" + testName, config);
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            positions.add(ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        List<ManagedCursor> cursors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ManagedCursor cursor = ledger.openCursor("cursor" + i, InitialPosition.Earliest);
            cursor.seek(positions.get(i + 1));
            cursor.setActive();
            cursors.add(cursor);
        }
        ActiveManagedCursorContainer container = ledger.getActiveCursors();
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
        ManagedCursor cursor = cursors.get(2);
        // Null untracking is a container API operation; normal broker deactivation removes the cursor.
        container.updateCursor(cursor, null);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));

        // Exercise ManagedCursorImpl -> ManagedLedgerImpl -> the active cursor container.
        // One update among four tracked nodes forces incremental reinsertion.
        cursor.seek(positions.get(6));
        assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursor)).isEqualTo(5);
        assertThat(container.getSlowestCursorPosition()).isEqualTo(positions.get(1));
        assertThat(container.size()).isEqualTo(5);
        for (int i = 0; i < cursors.size(); i++) {
            if (i != 2) {
                assertThat(ledger.getNumberOfCursorsAtSamePositionOrBefore(cursors.get(i)))
                        .as("rank of cursor%s", i).isEqualTo(i < 2 ? i + 1 : i);
            }
        }
    }

    @Test
    public void testBookieFailure() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
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

        bkc = new PulsarBookKeeperTestClient(baseClientConf);
        startNewBookie();

        // Reconnect a new bk client
        factory.shutdown();

        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory.open("my-ledger" + testName, config);
        cursor = ledger.openCursor("my-cursor");

        // Next add should succeed
        ledger.addEntry("entry-2".getBytes());

        assertEquals(3, cursor.getNumberOfEntriesInBacklog(false));

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-0", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);

        // entry-1 which was half-committed will get fully committed during the recovery phase
        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-1", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);

        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-2", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);
        factory.shutdown();
    }

    @Test
    public void verifyConcurrentUsage() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();

        config.setMaxCacheSize(100 * 1024 * 1024);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        ManagedLedgerConfig conf = rawEntryConfig();
        conf.setEnsembleSize(2).setAckQuorumSize(2);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-ledger-" + UUID.randomUUID(), conf);

        int numProducers = 1;
        int numConsumers = 1;

        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(numProducers + numConsumers + 1);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < numProducers; i++) {
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

        for (int i = 0; i < numConsumers; i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                try {
                    // Open the cursor before any entry is added: an entry appended while the cursor is being created
                    // can be skipped by the cache and still land after the cursor's starting position
                    ManagedCursor cursor = ledger.openCursor("my-cursor-" + idx);

                    barrier.await();

                    while (!done.get()) {
                        List<Entry> entries = cursor.readEntries(1);
                        if (!entries.isEmpty()) {
                            cursor.markDelete(entries.get(0).getPosition());
                        }

                        entries.forEach(Entry::release);
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
    public void verifyAsyncReadEntryUsingCache() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();

        config.setMaxCacheSize(100 * 1024 * 1024);
        config.setCacheEvictionTimeThresholdMillis(10000);
        config.setCacheEvictionIntervalMs(10000);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        ManagedLedgerConfig conf = rawEntryConfig();
        conf.setEnsembleSize(2).setAckQuorumSize(2)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-ledger" + testName, conf);

        int numProducers = 5;
        int numConsumers = 10;

        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(numProducers + numConsumers + 1);

        List<Future<?>> futures = new ArrayList<>();
        List<Position> positions = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numProducers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    // wait for all threads to be ready to start at once
                    barrier.await();
                    while (!done.get()) {
                        Position position = ledger.addEntry("entry".getBytes());
                        positions.add(position);
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw FutureUtil.wrapToCompletionException(e);
                }
            }));
        }

        // create a dummy cursor since caching happens only when there are active consumers
        ManagedCursor cursor = ledger.openCursor("dummy");

        for (int i = 0; i < numConsumers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    // wait for all threads to be ready to start at once
                    barrier.await();
                    while (!done.get()) {
                        if (positions.isEmpty()) {
                            Thread.sleep(1);
                            continue;
                        }
                        // Simulate a replay queue read pattern where individual entries are read
                        Position randomPosition = positions.get(ThreadLocalRandom.current().nextInt(positions.size()));
                        // Clone the original instance so that another instance is used in the asyncReadEntry call
                        // This is to test that keys are compared by .equals and not by reference under the covers
                        randomPosition = PositionFactory.create(randomPosition);
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        ledger.asyncReadEntry(randomPosition, new AsyncCallbacks.ReadEntryCallback() {
                            @Override
                            public void readEntryComplete(Entry entry, Object ctx) {
                                entry.release();
                                future.complete(null);
                            }

                            @Override
                            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                future.completeExceptionally(exception);
                            }
                        }, null);
                        future.get();
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw FutureUtil.wrapToCompletionException(e);
                }
            }));
        }

        // trigger all worker threads at once to continue from the barrier
        barrier.await();

        int testDurationSeconds = 3;
        Thread.sleep(testDurationSeconds * 1000);

        done.set(true);
        for (Future<?> future : futures) {
            future.get();
        }

        factory.getMbean().refreshStats(testDurationSeconds, TimeUnit.SECONDS);

        assertTrue(factory.getMbean().getCacheHitsRate() > 0.0);
        assertEquals(factory.getMbean().getCacheMissesRate(), 0.0);
        assertTrue(factory.getMbean().getCacheHitsThroughput() > 0.0);
        assertEquals(factory.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void testSimple() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig mlConfig = rawEntryConfig();
        mlConfig.setEnsembleSize(1).setAckQuorumSize(1).setWriteQuorumSize(1);
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
        ManagedLedgerConfig mlConfig = rawEntryConfig();
        mlConfig.setEnsembleSize(1).setWriteQuorumSize(1)
            .setAckQuorumSize(1);
        // set the data ledger size
        mlConfig.setMaxEntriesPerLedger(100);
        // set the metadata ledger size to 1 to kick off many ledger switching cases
        mlConfig.setMetadataMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("ml-markdelete-ledger", mlConfig);

        final List<Position> addedEntries = new ArrayList<>();

        int numCursors = 10;
        final CyclicBarrier barrier = new CyclicBarrier(numCursors);

        List<ManagedCursor> cursors = new ArrayList<>();
        for (int i = 0; i < numCursors; i++) {
            cursors.add(ledger.openCursor(String.format("c%d", i)));
        }

        for (int i = 0; i < 50; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        List<Future<?>> futures = new ArrayList<>();

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

        ManagedLedgerConfig config = rawEntryConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        List<Position> positions = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Position p = ledger.addEntry("entry".getBytes());
            positions.add(p);
        }

        final CountDownLatch counter = new CountDownLatch(positions.size());
        final AtomicReference<Exception> gotException = new AtomicReference<>();

        for (Position p : positions) {
            cursor.asyncDelete(p, new DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    // Ok
                    counter.countDown();
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    gotException.set(exception);
                    counter.countDown();
                }
            }, null);
        }

        counter.await();

        // cleanup.
        closeManagedLedgerWithRetry(ledger);

        // Add information to determine the problem.
        if (gotException.get() != null){
            fail(ThrowableToStringUtil.toString(gotException.get()));
        }
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
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes());

        // Trigger the closure of the data ledger
        bkc.openLedger(p1.getLedgerId(), BookKeeper.DigestType.CRC32C, new byte[] {});

        ledger.addEntry("entry-2".getBytes());

        assertEquals(2, c1.getNumberOfEntries());
        assertEquals(2, c1.getNumberOfEntriesInBacklog(false));

        Position p3 = ledger.addEntry("entry-3".getBytes());

        // Now entry-2 should have been written before entry-3
        assertEquals(3, c1.getNumberOfEntries());
        assertEquals(3, c1.getNumberOfEntriesInBacklog(false));
        assertTrue(p1.getLedgerId() != p3.getLedgerId());
    }

    @Test
    public void testLedgerCallbacksRunOnManagedLedgerThread() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(2)
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ml-thread-callbacks-" + UUID.randomUUID(), config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c1");
        Thread mlThread = CompletableFuture.supplyAsync(Thread::currentThread, ledger.getExecutor()).get();

        // Data ledger created with the managed ledger name as ordering key
        Position p1 = ledger.addEntry("entry-1".getBytes(StandardCharsets.UTF_8));
        assertEquals(readCallbackThread(ledger.currentLedger, p1.getEntryId()), mlThread);

        // Closed ledger re-opened through getLedgerHandle with the same key
        ledger.addEntry("entry-2".getBytes(StandardCharsets.UTF_8));
        Position p3 = ledger.addEntry("entry-3".getBytes(StandardCharsets.UTF_8));
        assertNotEquals(p3.getLedgerId(), p1.getLedgerId());
        LedgerHandle reopened = (LedgerHandle) ledger.getLedgerHandle(p1.getLedgerId()).get(10, TimeUnit.SECONDS);
        assertEquals(readCallbackThread(reopened, p1.getEntryId()), mlThread);

        // Cursor ledger, created through the same create path
        cursor.markDelete(p1);
        Awaitility.await().until(() -> cursor.cursorLedger != null && cursor.cursorLedger.getLastAddConfirmed() >= 0);
        assertEquals(readCallbackThread(cursor.cursorLedger, cursor.cursorLedger.getLastAddConfirmed()), mlThread);
    }

    @Test
    public void testInlineAddCompletionsKeepOrder() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("inline-add-order-" + UUID.randomUUID(), config);

        int entries = 200;
        List<Position> completed = new CopyOnWriteArrayList<>();
        AtomicReference<ManagedLedgerException> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(entries);
        AddEntryCallback callback = new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                completed.add(position);
                latch.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                failure.compareAndSet(null, exception);
                latch.countDown();
            }
        };
        // Submit from the managed ledger thread, so the BookKeeper completions and their processing all run
        // inline on that thread, and check they still complete the adds in submission order
        ledger.getExecutor().execute(() -> {
            for (int i = 0; i < entries; i++) {
                ledger.asyncAddEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8), callback, null);
            }
        });
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(completed.size(), entries);
        for (int i = 1; i < entries; i++) {
            assertTrue(completed.get(i - 1).compareTo(completed.get(i)) < 0, "completed out of order at " + i);
        }
    }

    private static Thread readCallbackThread(LedgerHandle lh, long entryId) throws Exception {
        CompletableFuture<Thread> callbackThread = new CompletableFuture<>();
        lh.asyncReadEntries(entryId, entryId,
                (rc, handle, seq, ctx) -> callbackThread.complete(Thread.currentThread()), null);
        return callbackThread.get(10, TimeUnit.SECONDS);
    }

    /**
     * When another process steals the ML, the old instance should not succeed in any operation.
     */
    @Test
    public void ledgerFencedByFailover() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory1.open("my_test_ledger" + testName, config);
        ledger1.openCursor("c");

        ledger1.addEntry("entry-1".getBytes());

        // Open the ML from another factory
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerImpl ledger2 = (ManagedLedgerImpl) factory2.open("my_test_ledger" + testName, config);
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
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1);
        ManagedLedger ledger = factory.open("property/namespace/my-ledger", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int num = 1;

        for (int i = 0; i < num; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(num);
        assertEquals(num, entries.size());
        entries.forEach(Entry::release);
        ledger.close();

        ManagedLedgerOfflineBacklog offlineTopicBacklog = new ManagedLedgerOfflineBacklog(
                DigestType.CRC32, "".getBytes(StandardCharsets.UTF_8), "", false);
        PersistentOfflineTopicStats offlineTopicStats = offlineTopicBacklog.getEstimatedUnloadedTopicBacklog(
                (ManagedLedgerFactoryImpl) factory, "property/namespace/my-ledger");
        assertNotNull(offlineTopicStats);
    }

    @Test(timeOut = 20000)
    void testResetCursorAfterRecovery() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig conf = rawEntryConfig().setMaxEntriesPerLedger(10).setEnsembleSize(1)
                .setWriteQuorumSize(1).setAckQuorumSize(1);
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
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);

        int num = 100;

        AtomicReference<ManagedLedgerException> res = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(num);

        for (int i = 0; i < num; i++) {
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
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2);
        config.setDigestType(DigestType.CRC32);
        ManagedLedger ledger = factory.open("my_test_ledger" + testName, config);
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-0".getBytes());
        ledger.addEntry("entry-1".getBytes());
        ledger.addEntry("entry-2".getBytes());

        ledger.close();

        config.setDigestType(DigestType.CRC32C);
        ledger = factory.open("my_test_ledger" + testName, config);
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

    @Test
    public void testPeriodicRollover() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        int rolloverTimeForCursorInSeconds = 5;

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = rawEntryConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setLedgerRolloverTimeout(rolloverTimeForCursorInSeconds);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        Position pos = ledger.addEntry("entry-0".getBytes());
        ledger.addEntry("entry-1".getBytes());

        List<Entry> entries = cursor.readEntries(2);
        assertEquals(2, entries.size());
        entries.forEach(Entry::release);

        ManagedCursorImpl cursorImpl = (ManagedCursorImpl) cursor;
        assertEquals(ManagedCursorImpl.State.NoLedger, cursorImpl.state);

        // this creates the ledger
        cursor.delete(pos);

        Awaitility.await().until(() -> cursorImpl.state == ManagedCursorImpl.State.Open);

        Thread.sleep(rolloverTimeForCursorInSeconds * 1000 + 1000);

        long currentLedgerId = cursorImpl.getCursorLedger();
        assertTrue(cursor.periodicRollover());
        Awaitility.await().until(() -> cursorImpl.getCursorLedger() != currentLedgerId);
    }

    @DataProvider(name = "booleans")
    public Object[][] booleans() {
        return new Object[][] {
                {true},
                {false},
        };
    }

    @Test(dataProvider = "booleans")
    public void testConfigPersistIndividualAckAsLongArray(boolean enable) throws Exception {
        final String mlName = "ml" + UUID.randomUUID().toString().replaceAll("-", "");
        final String cursorName = "c1";
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        final ManagedLedgerConfig config = rawEntryConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMaxUnackedRangesToPersistInMetadataStore(1)
                .setPersistIndividualAckAsLongArray(enable);

        ManagedLedger ledger1 = factory.open(mlName, config);
        ManagedCursorImpl cursor1 = (ManagedCursorImpl) ledger1.openCursor(cursorName);

        // Write entries.
        int totalEntries = 100;
        List<Position> entries = new ArrayList<>();
        for (int i = 0; i < totalEntries; i++) {
            Position p = ledger1.addEntry("entry".getBytes());
            entries.add(p);
        }
        // Make ack holes and trigger a mark deletion.
        for (int i = totalEntries - 1; i >= 0; i--) {
            if (i % 2 == 0) {
                cursor1.delete(entries.get(i));
            }
        }
        cursor1.markDelete(entries.get(9));
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cursor1.pendingMarkDeleteOps.size(), 0);
        });

        // Verify: the config affects.
        long cursorLedgerLac = cursor1.cursorLedger.getLastAddConfirmed();
        LedgerEntry ledgerEntry = cursor1.cursorLedger.readEntries(cursorLedgerLac, cursorLedgerLac).nextElement();
        PositionInfo positionInfo = new PositionInfo();
        positionInfo.parseFrom(ledgerEntry.getEntry());
        if (enable) {
            assertNotEquals(positionInfo.getIndividualDeletedMessageRangesCount(), 0);
        } else {
            assertEquals(positionInfo.getIndividualDeletedMessageRangesCount(), 0);
        }

        // cleanup
        ledger1.close();
        factory.shutdown();
    }
}
