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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Measures the reads from the blob store done by a search by timestamp over offloaded ledgers, with a real managed
 * ledger, the real binary search and the real blob store read handle over an in-memory blob store.
 *
 * <p>The cost of such a search is dominated by the number of ranged reads (GETs) sent to the blob store, each one
 * costing tens of milliseconds on a real object storage, so this test counts them rather than measuring time.
 */
@CustomLog
public class OffloadedLedgerFindPositionTest extends BlobStoreManagedLedgerOffloaderBase {

    // Entries of 2 to 6 KiB, so that entries are not aligned with data blocks, in an offloaded ledger of about
    // 60 MiB made of 16 MiB data blocks, read with 1 MiB ranged reads
    private static final int MIN_ENTRY_SIZE = 2 * 1024;
    private static final int ENTRY_SIZE_RANGE = 4 * 1024;
    private static final int ENTRIES_PER_LEDGER = 15_013;
    private static final int OFFLOADED_LEDGERS = 1;
    private static final int BLOCK_SIZE = 16 * 1024 * 1024;
    private static final int READ_BUFFER_SIZE = 1024 * 1024;
    private static final int SEARCHES = 16;
    private static final long BASE_TIMESTAMP = 1_000_000L;

    private final CountingOffloaderStats offloaderStats = new CountingOffloaderStats();
    private MetadataStoreExtended metadataStore;
    private ManagedLedgerFactoryImpl factory;
    private BlobStoreManagedLedgerOffloader offloader;

    public OffloadedLedgerFindPositionTest() throws Exception {
        super();
        config = getConfiguration(BUCKET, Map.of(
                TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + "MaxBlockSizeInBytes",
                Integer.toString(BLOCK_SIZE),
                TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + "ReadBufferSizeInBytes",
                Integer.toString(READ_BUFFER_SIZE)));
        provider.validate(config);
        blobStore = provider.getBlobStore(config);
    }

    @AfterClass(alwaysRun = true)
    public void cleanupFactory() throws Exception {
        // Also closes the managed ledger, e.g. when an assertion failed while it was open
        if (factory != null) {
            factory.shutdown();
        }
        if (offloader != null) {
            offloader.close();
        }
        if (metadataStore != null) {
            metadataStore.close();
        }
    }

    @Test(timeOut = 300_000)
    public void testFindPositionReadsFromOffloadedLedgers() throws Exception {
        TieredStorageConfiguration offloaderConfig = mock(TieredStorageConfiguration.class, delegatesTo(config));
        // Offload and read back from the same in-memory blob store
        doReturn(blobStore).when(offloaderConfig).getBlobStore();
        offloader = BlobStoreManagedLedgerOffloader.create(offloaderConfig,
                Map.of(), scheduler, scheduler, offloaderStats, entryOffsetsCache);

        metadataStore = MetadataStoreExtended.create("memory:local",
                MetadataStoreConfig.builder().metadataStoreName("find-position").build());
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        // Measure the reads from the blob store, not from the broker entry cache
        factoryConfig.setMaxCacheSize(0);
        factory = new ManagedLedgerFactoryImpl(metadataStore, bk, factoryConfig);
        ManagedLedgerConfig ledgerConfig = new ManagedLedgerConfig();
        ledgerConfig.setMaxEntriesPerLedger(ENTRIES_PER_LEDGER);
        ledgerConfig.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ledgerConfig.setRetentionTime(-1, TimeUnit.SECONDS);
        ledgerConfig.setRetentionSizeInMB(-1);
        ledgerConfig.setLedgerOffloader(offloader);

        // The offloader decodes the topic name from the managed ledger name
        String ledgerName = "public/default/persistent/find-position";
        long startNanos = System.nanoTime();
        ManagedLedger ledger = factory.open(ledgerName, ledgerConfig);
        int numEntries = OFFLOADED_LEDGERS * ENTRIES_PER_LEDGER + 1;
        List<CompletableFuture<Position>> addFutures = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            int entrySize = MIN_ENTRY_SIZE + (int) ((i * 7919L) % ENTRY_SIZE_RANGE);
            ByteBuffer payload = ByteBuffer.allocate(entrySize).putLong(0, BASE_TIMESTAMP + i);
            CompletableFuture<Position> addFuture = new CompletableFuture<>();
            ledger.asyncAddEntry(payload.array(), new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    addFuture.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    addFuture.completeExceptionally(exception);
                }
            }, null);
            addFutures.add(addFuture);
        }
        List<Position> positions = new ArrayList<>(numEntries);
        for (CompletableFuture<Position> addFuture : addFutures) {
            positions.add(addFuture.get(1, TimeUnit.MINUTES));
        }
        long addedNanos = System.nanoTime();
        ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        long offloadedNanos = System.nanoTime();
        List<LedgerInfo> ledgers = new ArrayList<>(ledger.getLedgersInfo().values());
        for (int i = 0; i < OFFLOADED_LEDGERS; i++) {
            assertThat(ledgers.get(i).getOffloadContext().isComplete()).as("ledger %d offloaded", i).isTrue();
        }

        long coldGets = 0;
        long coldBytes = 0;
        long maxColdGets = 0;
        long warmGets = 0;
        long warmBytes = 0;
        long maxWarmGets = 0;
        for (int search = 0; search < SEARCHES; search++) {
            // Spread the searched entries over the offloaded ledgers and over the positions within their blocks
            int target = (int) ((long) (2 * search + 1) * OFFLOADED_LEDGERS * ENTRIES_PER_LEDGER / (2 * SEARCHES))
                    + search * 997 % 400 - 200;

            // Cold: the offloaded ledgers are opened again and no entry offset is cached
            ledger.close();
            entryOffsetsCache.clear();
            ledger = factory.open(ledgerName, ledgerConfig);
            long gets = offloaderStats.gets.get();
            long bytes = offloaderStats.bytes.get();
            assertThat(findPosition(ledger, target)).as("cold search of entry %d", target)
                    .isEqualTo(positions.get(target));
            coldGets += offloaderStats.gets.get() - gets;
            coldBytes += offloaderStats.bytes.get() - bytes;
            maxColdGets = Math.max(maxColdGets, offloaderStats.gets.get() - gets);

            // Warm: a second search nearby, e.g. a consumer seeking again, reuses the opened ledgers
            int nearbyTarget = target - 200;
            gets = offloaderStats.gets.get();
            bytes = offloaderStats.bytes.get();
            assertThat(findPosition(ledger, nearbyTarget)).as("warm search of entry %d", nearbyTarget)
                    .isEqualTo(positions.get(nearbyTarget));
            warmGets += offloaderStats.gets.get() - gets;
            warmBytes += offloaderStats.bytes.get() - bytes;
            maxWarmGets = Math.max(maxWarmGets, offloaderStats.gets.get() - gets);
        }
        ledger.close();
        long searchedNanos = System.nanoTime();

        log.debug().attr("addMs", TimeUnit.NANOSECONDS.toMillis(addedNanos - startNanos))
                .attr("offloadMs", TimeUnit.NANOSECONDS.toMillis(offloadedNanos - addedNanos))
                .attr("searchMs", TimeUnit.NANOSECONDS.toMillis(searchedNanos - offloadedNanos))
                .attr("searches", SEARCHES)
                .attr("coldAvgGets", coldGets / SEARCHES)
                .attr("coldMaxGets", maxColdGets)
                .attr("coldAvgMiB", coldBytes / SEARCHES / (1024 * 1024))
                .attr("warmAvgGets", warmGets / SEARCHES)
                .attr("warmMaxGets", maxWarmGets)
                .attr("warmAvgMiB", warmBytes / SEARCHES / (1024 * 1024))
                .log("Blob store reads of searches by timestamp over offloaded ledgers");

        // Probing the first entries of the data blocks, whose offsets are indexed, instead of scanning the blocks
        // brings a cold search from 34 to 15 reads on average with this data, and does not make a warm search more
        // expensive (7 reads on average).
        assertThat(coldGets).as("reads of cold searches").isLessThanOrEqualTo(20L * SEARCHES);
        assertThat(warmGets).as("reads of warm searches").isLessThanOrEqualTo(8L * SEARCHES);
    }

    private static Position findPosition(ManagedLedger ledger, int targetEntry) throws Exception {
        long targetTimestamp = BASE_TIMESTAMP + targetEntry;
        // The position after the last entry older than the target timestamp, i.e. the target entry
        return ledger.asyncFindPosition(entry -> {
            try {
                return entry.getDataBuffer().getLong(entry.getDataBuffer().readerIndex()) < targetTimestamp;
            } finally {
                entry.release();
            }
        }).get();
    }

    /**
     * Counts the ranged reads of offloaded data, recorded once per read sent to the blob store.
     */
    private static class CountingOffloaderStats implements LedgerOffloaderStats {
        private final AtomicLong gets = new AtomicLong();
        private final AtomicLong bytes = new AtomicLong();

        @Override
        public void recordReadOffloadDataLatency(String topic, long latency, TimeUnit unit) {
            gets.incrementAndGet();
        }

        @Override
        public void recordReadOffloadBytes(String topic, long size) {
            bytes.addAndGet(size);
        }

        @Override
        public void recordOffloadError(String topic) {
        }

        @Override
        public void recordOffloadBytes(String topic, long size) {
        }

        @Override
        public void recordReadLedgerLatency(String topic, long latency, TimeUnit unit) {
        }

        @Override
        public void recordWriteToStorageError(String topic) {
        }

        @Override
        public void recordReadOffloadError(String topic) {
        }

        @Override
        public void recordReadOffloadIndexLatency(String topic, long latency, TimeUnit unit) {
        }

        @Override
        public void recordDeleteOffloadOps(String topic, boolean succeed) {
        }

        @Override
        public void close() {
        }
    }
}
