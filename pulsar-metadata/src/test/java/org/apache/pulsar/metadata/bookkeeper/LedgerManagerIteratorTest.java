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
package org.apache.pulsar.metadata.bookkeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;


/**
 * Test the ledger manager iterator.
 */
@Slf4j
public class LedgerManagerIteratorTest extends BaseMetadataStoreTest {

    private String newLedgersRoot() {
        return "/ledgers-" + UUID.randomUUID();
    }

    /**
     * Remove ledger using lm synchronously.
     *
     * @param lm
     * @param ledgerId
     * @throws InterruptedException
     */
    void removeLedger(LedgerManager lm, Long ledgerId) throws Exception {
        lm.removeLedgerMetadata(ledgerId, Version.ANY).get();
    }

    /**
     * Create ledger using lm synchronously.
     *
     * @param lm
     * @param ledgerId
     * @throws InterruptedException
     */
    void createLedger(LedgerManager lm, Long ledgerId) throws Exception {
        createLedgerAsync(lm, ledgerId).get();
    }

    CompletableFuture<Versioned<LedgerMetadata>> createLedgerAsync(LedgerManager lm, long ledgerId) {
        List<BookieId> ensemble = Lists.newArrayList(new BookieSocketAddress("192.0.2.1", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.2", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.3", 1234).toBookieId());
        LedgerMetadata meta = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                .withPassword("passwd".getBytes())
                .withDigestType(BookKeeper.DigestType.CRC32.toApiDigestType())
                .newEnsembleEntry(0L, ensemble)
                .build();
        return lm.createLedgerMetadata(ledgerId, meta);
    }

    static Set<Long> ledgerRangeToSet(LedgerRangeIterator lri) throws IOException {
        Set<Long> ret = new TreeSet<>();
        long last = -1;
        while (lri.hasNext()) {
            LedgerManager.LedgerRange lr = lri.next();
            assertFalse("ledger range must not be empty", lr.getLedgers().isEmpty());
            assertTrue("ledger ranges must not overlap", last < lr.start());
            ret.addAll(lr.getLedgers());
            last = lr.end();
        }
        return ret;
    }

    static Set<Long> getLedgerIdsByUsingAsyncProcessLedgers(LedgerManager lm) throws InterruptedException {
        Set<Long> ledgersReadAsync = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger finalRC = new AtomicInteger();

        lm.asyncProcessLedgers((ledgerId, callback) -> {
            ledgersReadAsync.add(ledgerId);
            callback.processResult(BKException.Code.OK, null, null);
        }, (rc, s, obj) -> {
            finalRC.set(rc);
            latch.countDown();
        }, null, BKException.Code.OK, BKException.Code.ReadException);

        latch.await();
        assertEquals("Final RC of asyncProcessLedgers", BKException.Code.OK, finalRC.get());
        return ledgersReadAsync;
    }

    @Test(dataProvider = "impl")
    public void testIterateNoLedgers(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, newLedgersRoot());
        LedgerRangeIterator lri = lm.getLedgerRanges(0);
        assertNotNull(lri);
        if (lri.hasNext()) {
            lri.next();
        }

        assertEquals(false, lri.hasNext());
    }

    @Test(dataProvider = "impl")
    public void testSingleLedger(String provider, Supplier<String> urlSupplier) throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, newLedgersRoot());

        long id = 2020202;
        createLedger(lm, id);

        LedgerRangeIterator lri = lm.getLedgerRanges(0);
        assertNotNull(lri);
        Set<Long> lids = ledgerRangeToSet(lri);
        assertEquals(lids.size(), 1);
        assertEquals(lids.iterator().next().longValue(), id);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", lids, ledgersReadAsync);
    }

    @Test(dataProvider = "impl")
    public void testTwoLedgers(String provider, Supplier<String> urlSupplier) throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, newLedgersRoot());

        Set<Long> ids = new TreeSet<>(Arrays.asList(101010101L, 2020340302L));
        for (Long id : ids) {
            createLedger(lm, id);
        }

        LedgerRangeIterator lri = lm.getLedgerRanges(0);
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);
    }

    @Test(dataProvider = "impl")
    public void testSeveralContiguousLedgers(String provider, Supplier<String> urlSupplier) throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, newLedgersRoot());

        Set<Long> ids = new TreeSet<>();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (long i = 0; i < 2000; ++i) {
            futures.add(createLedgerAsync(lm, i));
            ids.add(i);
        }

        FutureUtil.waitForAll(futures).get();

        LedgerRangeIterator lri = lm.getLedgerRanges(0);
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);
    }

    @Test(dataProvider = "impl")
    public void testRemovalOfNodeJustTraversed(String provider, Supplier<String> urlSupplier) throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, newLedgersRoot());

        /* For LHLM, first two should be leaves on the same node, second should be on adjacent level 4 node
         * Removing all 3 once the iterator hits the first should result in the whole tree path ending
         * at that node disappearing.  If this happens after the iterator stops at that leaf, it should
         * result in a few NodeExists errors (handled silently) as the iterator fails back up the tree
         * to the next path.
         */
        Set<Long> toRemove = new TreeSet<>(
                Arrays.asList(
                        3394498498348983841L,
                        3394498498348983842L,
                        3394498498348993841L));

        long first = 2345678901234567890L;
        // Nodes which should be listed anyway
        Set<Long> mustHave = new TreeSet<>(
                Arrays.asList(
                        first,
                        6334994393848474732L));

        Set<Long> ids = new TreeSet<>();
        ids.addAll(toRemove);
        ids.addAll(mustHave);
        for (Long id : ids) {
            createLedger(lm, id);
        }

        Set<Long> found = new TreeSet<>();
        LedgerRangeIterator lri = lm.getLedgerRanges(0);
        while (lri.hasNext()) {
            LedgerManager.LedgerRange lr = lri.next();
            found.addAll(lr.getLedgers());

            if (lr.getLedgers().contains(first)) {
                for (long id : toRemove) {
                    removeLedger(lm, id);
                }
                toRemove.clear();
            }
        }

        for (long id : mustHave) {
            assertTrue(found.contains(id));
        }
    }

    @Test(dataProvider = "impl")
    public void validateEmptyL4PathSkipped(String provider, Supplier<String> urlSupplier) throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        String ledgersRoot = newLedgersRoot();

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, ledgersRoot);

        Set<Long> ids = new TreeSet<>(
                Arrays.asList(
                        2345678901234567890L,
                        3394498498348983841L,
                        6334994393848474732L,
                        7349370101927398483L));
        for (Long id : ids) {
            createLedger(lm, id);
        }

        String[] paths = {
                ledgersRoot + "/633/4994/3938/4948", // Empty L4 path, must be skipped

        };

        for (String path : paths) {
            store.put(path, "data".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        }

        LedgerRangeIterator lri = lm.getLedgerRanges(0);
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);

        lri = lm.getLedgerRanges(0);
        int emptyRanges = 0;
        while (lri.hasNext()) {
            if (lri.next().getLedgers().isEmpty()) {
                emptyRanges++;
            }
        }
        assertEquals(0, emptyRanges);
    }

    @Test(dataProvider = "impl")
    public void testWithSeveralIncompletePaths(String provider, Supplier<String> urlSupplier) throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        String ledgersRoot = newLedgersRoot();

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, ledgersRoot);

        Set<Long> ids = new TreeSet<>(
                Arrays.asList(
                        2345678901234567890L,
                        3394498498348983841L,
                        6334994393848474732L,
                        7349370101927398483L));
        for (Long id : ids) {
            createLedger(lm, id);
        }

        String[] paths = {
                ledgersRoot + "000/0000/0000", // top level, W-4292762
                ledgersRoot + "/234/5678/9999", // shares two path segments with the first one, comes after
                ledgersRoot + "/339/0000/0000", // shares one path segment with the second one, comes first
                ledgersRoot + "/633/4994/3938/0000", // shares three path segments with the third one, comes first
                ledgersRoot + "/922/3372/0000/0000", // close to max long, at end

        };
        for (String path : paths) {
            store.put(path, "data".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        }

        LedgerRangeIterator lri = lm.getLedgerRanges(0);
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);
    }

    @Test(dataProvider = "impl")
    public void checkConcurrentModifications(String provider, Supplier<String> urlSupplier) throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        String ledgersRoot = newLedgersRoot();

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, ledgersRoot);
        final int numWriters = 10;
        final int numCheckers = 10;
        final int numLedgers = 100;
        final long runtime = TimeUnit.NANOSECONDS.convert(2, TimeUnit.SECONDS);
        final boolean longRange = true;

        final Set<Long> mustExist = new TreeSet<>();
        Random rng = new Random();
        for (int i = 0; i < numLedgers; ++i) {
            long lid = Math.abs(rng.nextLong());
            if (!longRange) {
                lid %= 1000000;
            }
            createLedger(lm, lid);
            mustExist.add(lid);
        }

        final long start = MathUtils.nowInNano();
        final CountDownLatch latch = new CountDownLatch(1);
        ArrayList<Future<?>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newCachedThreadPool();
        final ConcurrentSkipListSet<Long> createdLedgers = new ConcurrentSkipListSet<>();
        for (int i = 0; i < numWriters; ++i) {
            Future<?> f = executor.submit(() -> {
                @Cleanup
                LedgerManager writerLM = new PulsarLedgerManager(store, ledgersRoot);
                Random writerRNG = new Random(rng.nextLong());

                latch.await();

                while (MathUtils.elapsedNanos(start) < runtime) {
                    long candidate = 0;
                    do {
                        candidate = Math.abs(writerRNG.nextLong());
                        if (!longRange) {
                            candidate %= 1000000;
                        }
                    } while (mustExist.contains(candidate) || !createdLedgers.add(candidate));

                    createLedger(writerLM, candidate);
                    removeLedger(writerLM, candidate);
                }
                return null;
            });
            futures.add(f);
        }

        for (int i = 0; i < numCheckers; ++i) {
            Future<?> f = executor.submit(() -> {
                @Cleanup
                LedgerManager checkerLM = new PulsarLedgerManager(store, ledgersRoot);
                latch.await();

                while (MathUtils.elapsedNanos(start) < runtime) {
                    LedgerRangeIterator lri = checkerLM.getLedgerRanges(0);
                    Set<Long> returnedIds = ledgerRangeToSet(lri);
                    for (long id : mustExist) {
                        assertTrue(returnedIds.contains(id));
                    }

                    Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(checkerLM);
                    for (long id : mustExist) {
                        assertTrue(ledgersReadAsync.contains(id));
                    }
                }
                return null;
            });
            futures.add(f);
        }

        latch.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdownNow();
    }

    @Test(dataProvider = "impl")
    public void hierarchicalLedgerManagerAsyncProcessLedgersTest(String provider, Supplier<String> urlSupplier)
            throws Throwable {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        LedgerManager lm = new PulsarLedgerManager(store, newLedgersRoot());
        LedgerRangeIterator lri = lm.getLedgerRanges(0);

        Set<Long> ledgerIds = new TreeSet<>(Arrays.asList(1234L, 123456789123456789L));
        for (Long ledgerId : ledgerIds) {
            createLedger(lm, ledgerId);
        }
        Set<Long> ledgersReadThroughIterator = ledgerRangeToSet(lri);
        assertEquals("Comparing LedgersIds read through Iterator", ledgerIds, ledgersReadThroughIterator);
        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ledgerIds, ledgersReadAsync);
    }
}
