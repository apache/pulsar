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

import static org.apache.bookkeeper.mledger.impl.OffloadPrefixTest.assertEventuallyTrue;
import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.OffloadedLedgerHandle;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.MockClock;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OffloadPrefixReadTest extends MockedBookKeeperTestCase {

    private final String offloadTypeAppendable = "NonAppendable";

    @Override
    protected void initManagedLedgerFactoryConfig(ManagedLedgerFactoryConfig config) {
        super.initManagedLedgerFactoryConfig(config);
        // disable cache.
        config.setMaxCacheSize(0);
    }

    @DataProvider(name = "offloadAndDeleteTypes")
    public Object[][] offloadAndDeleteTypes() {
        return new Object[][]{
                {"normal", true},
                {"normal", false},
                {offloadTypeAppendable, true},
                {offloadTypeAppendable, false},
        };
    }

    @Test(dataProvider = "offloadAndDeleteTypes")
    public void testOffloadRead(String offloadType, boolean deleteMl) throws Exception {
        MockLedgerOffloader offloader = spy(MockLedgerOffloader.class);
        ManagedLedgerConfig config = defaultConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        for (int i = 0; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().isComplete());
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(1).getOffloadContext().isComplete());
        Assert.assertFalse(ledger.getLedgersInfoAsList().get(2).getOffloadContext().isComplete());

        if (offloadTypeAppendable.equals(offloadType)) {
            config.setLedgerOffloader(new NonAppendableLedgerOffloader(offloader));
        }

        UUID firstLedgerUUID = new UUID(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidMsb(),
                ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidLsb());
        UUID secondLedgerUUID = new UUID(ledger.getLedgersInfoAsList().get(1).getOffloadContext().getUidMsb(),
                ledger.getLedgersInfoAsList().get(1).getOffloadContext().getUidLsb());

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        int i = 0;
        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
        verify(offloader, times(1))
                .readOffloaded(anyLong(), (UUID) any(), anyMap());
        verify(offloader).readOffloaded(anyLong(), eq(firstLedgerUUID), anyMap());

        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
        verify(offloader, times(2))
                .readOffloaded(anyLong(), (UUID) any(), anyMap());
        verify(offloader).readOffloaded(anyLong(), eq(secondLedgerUUID), anyMap());

        for (Entry e : cursor.readEntries(5)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
        verify(offloader, times(2))
                .readOffloaded(anyLong(), (UUID) any(), anyMap());

        if (!deleteMl) {
            ledger.close();
            // Ensure that all the read handles had been closed
            assertEquals(offloader.openedReadHandles.get(), 0);
        } else {
            // Verify: the ledger offloaded will be deleted after managed ledger is deleted.
            ledger.delete();
            Awaitility.await().untilAsserted(() -> {
                assertTrue(offloader.offloads.size() <= 1);
                assertTrue(ledger.ledgers.size() <= 1);
            });
        }
    }

    @DataProvider(name = "offloadTypes")
    public Object[][] offloadTypes() {
        return new Object[][]{
                {"normal"},
                {offloadTypeAppendable},
        };
    }

    @Test(dataProvider = "offloadTypes")
    public void testBookkeeperFirstOffloadRead(String offloadType) throws Exception {
        MockLedgerOffloader offloader = spy(MockLedgerOffloader.class);
        MockClock clock = new MockClock();
        offloader.getOffloadPolicies()
                .setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.BOOKKEEPER_FIRST);
        //delete after 5 minutes
        offloader.getOffloadPolicies()
                .setManagedLedgerOffloadDeletionLagInMillis(300000L);
        ManagedLedgerConfig config = defaultConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);


        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_bookkeeper_first_test_ledger", config);

        for (int i = 0; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                .filter(e -> e.getOffloadContext().isComplete()).count(), 2);

        LedgerInfo firstLedger = ledger.getLedgersInfoAsList().get(0);
        Assert.assertTrue(firstLedger.getOffloadContext().isComplete());
        LedgerInfo secondLedger;
        secondLedger = ledger.getLedgersInfoAsList().get(1);
        Assert.assertTrue(secondLedger.getOffloadContext().isComplete());

        UUID firstLedgerUUID = new UUID(firstLedger.getOffloadContext().getUidMsb(),
                firstLedger.getOffloadContext().getUidLsb());
        UUID secondLedgerUUID = new UUID(secondLedger.getOffloadContext().getUidMsb(),
                secondLedger.getOffloadContext().getUidLsb());

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        int i = 0;
        for (Entry e : cursor.readEntries(10)) {
            Assert.assertEquals(new String(e.getData()), "entry-" + i++);
        }
        // For offloaded first and not deleted ledgers, they should be read from bookkeeper.
        verify(offloader, never())
                .readOffloaded(anyLong(), (UUID) any(), anyMap());

        // Delete offladed message from bookkeeper
        assertEventuallyTrue(() -> bkc.getLedgers().contains(firstLedger.getLedgerId()));
        assertEventuallyTrue(() -> bkc.getLedgers().contains(secondLedger.getLedgerId()));
        clock.advance(6, TimeUnit.MINUTES);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedger.getLedgerId()));
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(secondLedger.getLedgerId()));
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().isBookkeeperDeleted());
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(1).getOffloadContext().isBookkeeperDeleted());

        if (offloadTypeAppendable.equals(offloadType)) {
            config.setLedgerOffloader(new NonAppendableLedgerOffloader(offloader));
        }

        for (Entry e : cursor.readEntries(10)) {
            Assert.assertEquals(new String(e.getData()), "entry-" + i++);
        }

        // Ledgers deleted from bookkeeper, now should read from offloader
        verify(offloader, atLeastOnce())
                .readOffloaded(anyLong(), (UUID) any(), anyMap());
        verify(offloader).readOffloaded(anyLong(), eq(secondLedgerUUID), anyMap());

        // Verify: the ledger offloaded will be trimmed after if no backlog.
        while (cursor.hasMoreEntries()) {
            cursor.readEntries(1);
        }
        config.setRetentionTime(0, TimeUnit.MILLISECONDS);
        config.setRetentionSizeInMB(0);
        CompletableFuture<Void> trimFuture = new CompletableFuture<>();
        ledger.trimConsumedLedgersInBackground(trimFuture);
        trimFuture.join();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(offloader.offloads.size() <= 1);
            assertTrue(ledger.ledgers.size() <= 1);
        });

        // cleanup.
        ledger.delete();
    }



    @Test
    public void testSkipOffloadIfReadOnly() throws Exception {
        LedgerOffloader ol = new NonAppendableLedgerOffloader(spy(MockLedgerOffloader.class));
        ManagedLedgerConfig config = defaultConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(ol);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        for (int i = 0; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (ManagedLedgerException mle) {
            assertTrue(mle.getMessage().contains("does not support offload"));
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertFalse(ledger.getLedgersInfoAsList().get(0).getOffloadContext().isComplete());
        Assert.assertFalse(ledger.getLedgersInfoAsList().get(1).getOffloadContext().isComplete());
        Assert.assertFalse(ledger.getLedgersInfoAsList().get(2).getOffloadContext().isComplete());

        // cleanup.
        ledger.delete();
    }


    @Test
    public void testFindPositionProbesIndexedEntriesOfOffloadedLedgers() throws Exception {
        final int indexInterval = 10;
        List<Long> offloadedReads = Collections.synchronizedList(new ArrayList<>());
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
            @SneakyThrows
            @Override
            public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid,
                                                               Map<String, String> offloadDriverMetadata) {
                return CompletableFuture.completedFuture(new MockOffloadReadHandle(offloads.get(uuid)) {
                    @Override
                    public long getIndexedEntryIdFloor(long entryId) {
                        return entryId - entryId % indexInterval;
                    }

                    @Override
                    public long getIndexedEntryIdCeiling(long entryId) {
                        long ceiling = entryId + (indexInterval - entryId % indexInterval) % indexInterval;
                        return ceiling <= getLastAddConfirmed() ? ceiling : -1;
                    }

                    @Override
                    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
                        offloadedReads.add(firstEntry);
                        return super.readAsync(firstEntry, lastEntry);
                    }
                });
            }
        };
        ManagedLedgerConfig config = defaultConfig();
        config.setMaxEntriesPerLedger(100);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        int numEntries = 250;
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            positions.add(ledger.addEntry(Integer.toString(i).getBytes()));
        }
        ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        assertThat(ledger.getLedgersInfoAsList()).hasSize(3);
        assertThat(ledger.getLedgersInfoAsList().get(0).getOffloadContext().isComplete()).isTrue();
        assertThat(ledger.getLedgersInfoAsList().get(1).getOffloadContext().isComplete()).isTrue();

        for (int target = 1; target < numEntries; target++) {
            final int value = target;
            offloadedReads.clear();
            Position found = ledger.asyncFindPosition(
                    entry -> Integer.parseInt(new String(entry.getDataAndRelease())) < value).get();
            // The last entry lower than the target is found, the next position is returned
            assertThat(found).as("position found for target %d", target).isEqualTo(positions.get(target));
            // Only the last steps of the search, around a single data block, read entries that are not indexed:
            // at most ceil(log2(indexInterval)) + 2 probes. Without probing indexed entries, it would be up to 9.
            long unindexedReads = offloadedReads.stream().filter(entryId -> entryId % indexInterval != 0).count();
            assertThat(unindexedReads).as("unindexed offloaded entries read for target %d: %s", target, offloadedReads)
                    .isLessThanOrEqualTo(6);
        }
        ledger.close();
    }

    @Test
    public void testFindNewestMatchingFailsOnceWhenOffloadedLedgerCannotBeOpened() throws Exception {
        AtomicLong failingLedgerId = new AtomicLong(-1);
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
            @Override
            public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid,
                                                               Map<String, String> offloadDriverMetadata) {
                if (ledgerId == failingLedgerId.get()) {
                    // e.g. the index of the offloaded ledger is missing from the tiered storage
                    return CompletableFuture.failedFuture(new BKException.BKNoSuchLedgerExistsException());
                }
                return super.readOffloaded(ledgerId, uuid, offloadDriverMetadata);
            }
        };
        ManagedLedgerImpl ledger = openLedgerWithOffloadedEntries(offloader, 250);
        failingLedgerId.set(ledger.getLedgersInfoAsList().get(1).getLedgerId());

        // The search opens the second offloaded ledger to choose a probe in it
        CountingFindEntryCallback callback = new CountingFindEntryCallback();
        ledger.newNonDurableCursor(PositionFactory.EARLIEST).asyncFindNewestMatching(
                ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries,
                entry -> Integer.parseInt(new String(entry.getDataAndRelease())) < 150, callback, null, true);

        callback.awaitCalled();
        assertThat(callback.failures.get()).as("reported failures").isEqualTo(1);
        assertThat(callback.completions.get()).as("reported completions").isZero();
        ledger.close();
    }

    @Test
    public void testFindNewestMatchingFailsWhenProbeFailsAfterHandleOpenedAsynchronously() throws Exception {
        ManagedLedgerImpl ledger = spy(openLedgerWithOffloadedEntries(new MockLedgerOffloader(), 250));
        CompletableFuture<long[]> indexedEntryIds = new CompletableFuture<>();
        doReturn(indexedEntryIds).when(ledger).getIndexedEntryIdsAround(any());
        AtomicInteger reads = new AtomicInteger();
        doAnswer(invocation -> {
            // The first and last entries are read before the search chooses its first probe
            if (reads.incrementAndGet() <= 2) {
                return invocation.callRealMethod();
            }
            throw new IllegalStateException("read failure");
        }).when(ledger).asyncReadEntry(any(Position.class), any(ReadEntryCallback.class), any());

        CountingFindEntryCallback callback = startFindNewestMatching(ledger);
        // The read handle opens after the probe was chosen, so the search continues in a callback of its future
        verify(ledger, timeout(10_000)).getIndexedEntryIdsAround(any());
        indexedEntryIds.complete(null);

        callback.awaitCalled();
        assertThat(callback.failures.get()).as("reported failures").isEqualTo(1);
        assertThat(callback.completions.get()).as("reported completions").isZero();
        ledger.close();
    }

    @Test
    public void testFindNewestMatchingReportsOnlyTheFirstOutcome() throws Exception {
        ManagedLedgerImpl ledger = spy(openLedgerWithOffloadedEntries(new MockLedgerOffloader(), 250));
        CompletableFuture<long[]> indexedEntryIds = new CompletableFuture<>();
        doReturn(indexedEntryIds).when(ledger).getIndexedEntryIdsAround(any());
        AtomicInteger reads = new AtomicInteger();
        doAnswer(invocation -> {
            if (reads.incrementAndGet() <= 2) {
                return invocation.callRealMethod();
            }
            // The read reports its failure, which completes the search, and then throws
            ReadEntryCallback readCallback = invocation.getArgument(1);
            readCallback.readEntryFailed(new ManagedLedgerException("read failure"), invocation.getArgument(2));
            throw new IllegalStateException("failure after the search completed");
        }).when(ledger).asyncReadEntry(any(Position.class), any(ReadEntryCallback.class), any());

        CountingFindEntryCallback callback = startFindNewestMatching(ledger);
        verify(ledger, timeout(10_000)).getIndexedEntryIdsAround(any());
        indexedEntryIds.complete(null);

        callback.awaitCalled();
        assertThat(callback.failures.get()).as("reported failures").isEqualTo(1);
        assertThat(callback.completions.get()).as("reported completions").isZero();
        ledger.close();
    }

    private static CountingFindEntryCallback startFindNewestMatching(ManagedLedgerImpl ledger) {
        CountingFindEntryCallback callback = new CountingFindEntryCallback();
        Position firstPosition = PositionFactory.create(ledger.getLedgersInfoAsList().get(0).getLedgerId(), 0);
        new OpFindNewest(ledger, firstPosition, entry -> Integer.parseInt(new String(entry.getDataAndRelease())) < 150,
                ledger.getNumberOfEntries() - 1, callback, null).find();
        return callback;
    }

    private ManagedLedgerImpl openLedgerWithOffloadedEntries(LedgerOffloader offloader, int numEntries)
            throws Exception {
        ManagedLedgerConfig config = defaultConfig();
        config.setMaxEntriesPerLedger(100);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        for (int i = 0; i < numEntries; i++) {
            ledger.addEntry(Integer.toString(i).getBytes());
        }
        ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        assertThat(ledger.getLedgersInfoAsList()).hasSize(3);
        assertThat(ledger.getLedgersInfoAsList().get(1).getOffloadContext().isComplete()).isTrue();
        return ledger;
    }

    /**
     * Counts the outcomes reported to a search callback, and keeps checking for a while after the first one, so that
     * a second outcome reported later is counted too.
     */
    private static class CountingFindEntryCallback implements AsyncCallbacks.FindEntryCallback {
        private final AtomicInteger completions = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();

        @Override
        public void findEntryComplete(Position position, Object ctx) {
            completions.incrementAndGet();
        }

        @Override
        public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                                    Object ctx) {
            failures.incrementAndGet();
        }

        void awaitCalled() {
            Awaitility.await().until(() -> completions.get() + failures.get() > 0);
            // A second outcome would be reported shortly after the first one
            Awaitility.await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                    .until(() -> completions.get() + failures.get() == 1);
        }
    }

    static class MockLedgerOffloader implements LedgerOffloader {
        ConcurrentHashMap<UUID, ReadHandle> offloads = new ConcurrentHashMap<UUID, ReadHandle>();


        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create("S3", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_SECONDS,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS,
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY);

        Set<Long> offloadedLedgers() {
            return offloads.values().stream().map(ReadHandle::getId).collect(Collectors.toSet());
        }


        @Override
        public String getOffloadDriverName() {
            return "mock";
        }

        @Override
        public CompletableFuture<Void> offload(ReadHandle ledger,
                                               UUID uuid,
                                               Map<String, String> extraMetadata) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            try {
                offloads.put(uuid, new MockOffloadReadHandle(ledger));
                promise.complete(null);
            } catch (Exception e) {
                promise.completeExceptionally(e);
            }
            return promise;
        }

        @SneakyThrows
        @Override
        public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid,
                                                           Map<String, String> offloadDriverMetadata) {
            return CompletableFuture.completedFuture(new VerifyClosingReadHandle(offloads.get(uuid)));
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uuid,
                                                       Map<String, String> offloadDriverMetadata) {
            offloads.remove(uuid);
            return CompletableFuture.completedFuture(null);
        };

        @Override
        public OffloadPoliciesImpl getOffloadPolicies() {
            return offloadPolicies;
        }

        @Override
        public void close() {

        }

        private final AtomicInteger openedReadHandles = new AtomicInteger(0);

        @SuppressWarnings("try")
        class VerifyClosingReadHandle extends MockOffloadReadHandle {
            VerifyClosingReadHandle(ReadHandle toCopy) throws Exception {
                super(toCopy);
                openedReadHandles.incrementAndGet();
            }

            @Override
            public CompletableFuture<Void> closeAsync() {
                openedReadHandles.decrementAndGet();
                return super.closeAsync();
            }
        }
    }

    @SuppressWarnings("try")
    static class MockOffloadReadHandle implements ReadHandle, OffloadedLedgerHandle {
        final long id;
        final List<ByteBuf> entries = new ArrayList<>();
        final LedgerMetadata metadata;
        long lastAccessTimestamp = System.currentTimeMillis();

        MockOffloadReadHandle(ReadHandle toCopy) throws Exception {
            id = toCopy.getId();
            long lac = toCopy.getLastAddConfirmed();
            try (LedgerEntries entries = toCopy.read(0, lac)) {
                for (LedgerEntry e : entries) {
                    this.entries.add(e.getEntryBuffer().retainedSlice());
                }
            }
            metadata = new MockMetadata(toCopy.getLedgerMetadata());
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public LedgerMetadata getLedgerMetadata() {
            return metadata;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
            List<LedgerEntry> readEntries = new ArrayList<>();
            for (long eid = firstEntry; eid <= lastEntry; eid++) {
                ByteBuf buf = entries.get((int) eid).retainedSlice();
                readEntries.add(LedgerEntryImpl.create(id, eid, buf.readableBytes(), buf));
            }
            return CompletableFuture.completedFuture(LedgerEntriesImpl.create(readEntries));
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
            return readAsync(firstEntry, lastEntry);
        }

        @Override
        public CompletableFuture<Long> readLastAddConfirmedAsync() {
            return unsupported();
        }

        @Override
        public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
            return unsupported();
        }

        @Override
        public long getLastAddConfirmed() {
            return entries.size() - 1;
        }

        @Override
        public long getLength() {
            return metadata.getLength();
        }

        @Override
        public boolean isClosed() {
            return metadata.isClosed();
        }

        @Override
        public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                          long timeOutInMillis,
                                                                                          boolean parallel) {
            return unsupported();
        }

        private <T> CompletableFuture<T> unsupported() {
            CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(new UnsupportedOperationException());
            return future;
        }

        @Override
        public long lastAccessTimestamp() {
            return lastAccessTimestamp;
        }

        public void setLastAccessTimestamp(long lastAccessTimestamp) {
            this.lastAccessTimestamp = lastAccessTimestamp;
        }
    }

    static class MockMetadata implements LedgerMetadata {
        private final int ensembleSize;
        private final int writeQuorumSize;
        private final int ackQuorumSize;
        private final long lastEntryId;
        private final long length;
        private final DigestType digestType;
        private final long ctime;
        private final boolean isClosed;
        private final int metadataFormatVersion;
        private final State state;
        private final byte[] password;
        private final Map<String, byte[]> customMetadata;
        private final long ledgerId;
        MockMetadata(LedgerMetadata toCopy) {
            ledgerId = toCopy.getLedgerId();
            ensembleSize = toCopy.getEnsembleSize();
            writeQuorumSize = toCopy.getWriteQuorumSize();
            ackQuorumSize = toCopy.getAckQuorumSize();
            lastEntryId = toCopy.getLastEntryId();
            length = toCopy.getLength();
            digestType = toCopy.getDigestType();
            ctime = toCopy.getCtime();
            isClosed = toCopy.isClosed();
            metadataFormatVersion = toCopy.getMetadataFormatVersion();
            state = toCopy.getState();
            password = Arrays.copyOf(toCopy.getPassword(), toCopy.getPassword().length);
            customMetadata = Map.copyOf(toCopy.getCustomMetadata());
        }

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public boolean hasPassword() {
            return true;
        }

        @Override
        public State getState() {
            return state;
        }

        @Override
        public int getMetadataFormatVersion() {
            return metadataFormatVersion;
        }

        @Override
        public long getCToken() {
            return 0;
        }

        @Override
        public int getEnsembleSize() {
            return ensembleSize;
        }

        @Override
        public int getWriteQuorumSize() {
            return writeQuorumSize;
        }

        @Override
        public int getAckQuorumSize() {
            return ackQuorumSize;
        }

        @Override
        public long getLastEntryId() {
            return lastEntryId;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public DigestType getDigestType() {
            return digestType;
        }

        @Override
        public byte[] getPassword() {
            return password;
        }

        @Override
        public long getCtime() {
            return ctime;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public Map<String, byte[]> getCustomMetadata() {
            return customMetadata;
        }

        @Override
        public List<BookieId> getEnsembleAt(long entryId) {
            throw new UnsupportedOperationException("Pulsar shouldn't look at this");
        }

        @Override
        public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
            throw new UnsupportedOperationException("Pulsar shouldn't look at this");
        }

        @Override
        public String toSafeString() {
            return toString();
        }
    }
}
