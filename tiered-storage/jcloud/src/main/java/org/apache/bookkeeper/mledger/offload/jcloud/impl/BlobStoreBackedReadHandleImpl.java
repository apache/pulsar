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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.OffloadedLedgerHandle;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockUtils.VersionCheck;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;

@CustomLog
public class BlobStoreBackedReadHandleImpl implements ReadHandle, OffloadedLedgerHandle {

    protected static final AtomicIntegerFieldUpdater<BlobStoreBackedReadHandleImpl> PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BlobStoreBackedReadHandleImpl.class, "pendingRead");

    // Bound on how far seekToEntryOffset() probes backwards through entryOffsetsCache for an anchor
    // before falling back to re-walking from the sparse index marker.
    private static final long MAX_OFFSET_PROBE =
            Long.getLong("pulsar.jclouds.readhandleimpl.offsetprobe.max", 1024);

    // Minimum distance, in bytes of offloaded data, between two entry offsets remembered while skipping entries.
    // A later read of an entry in a part of the data block that was skipped before scans about this many bytes at
    // most, from the nearest remembered offset, instead of scanning from the start of the block. A value lower than
    // or equal to 0 disables remembering offsets.
    private static final long MIN_LEARNED_OFFSET_INTERVAL_BYTES = 64 * 1024;
    private static final long DEFAULT_LEARNED_OFFSET_INTERVAL_BYTES = learnedOffsetIntervalBytes(
            Long.getLong("pulsar.jclouds.readhandleimpl.learnedoffset.interval.bytes", 1024 * 1024));

    private final long ledgerId;
    private final OffloadIndexBlock index;
    private final BackedInputStream inputStream;
    private final DataInputStream dataStream;
    private final ExecutorService executor;
    private final OffsetsCache entryOffsetsCache;
    // Entry ids that have an exact offset in the index, i.e. the first entry of each data block, in ascending order.
    // Copied at open time since the index is recycled on close.
    private final long[] indexedEntryIds;
    // Entry offsets remembered while skipping more than one entry to reach an entry whose offset is not cached (see
    // skipPreviousEntry), at least "learnedOffsetIntervalBytes" apart. Sequential reads add none: the next read after
    // the entries [a..b] only skips entry b, whose offset is cached, to reach b + 1. At most the size of the data
    // object divided by the interval, plus one, e.g. 2049 offsets for a fully skipped 2 GiB object with the default
    // interval of 1 MiB. Only accessed by the read tasks, which run on the ordered read thread of the ledger; the
    // concurrent map is a safety margin.
    private final ConcurrentSkipListMap<Long, Long> learnedOffsets = new ConcurrentSkipListMap<>();
    private final long learnedOffsetIntervalBytes;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    enum State {
        Opened,
        Closed
    }

    private volatile State state = null;

    private volatile int pendingRead;

    private volatile long lastAccessTimestamp = System.currentTimeMillis();

    @VisibleForTesting
    BlobStoreBackedReadHandleImpl(long ledgerId, OffloadIndexBlock index,
                                          BackedInputStream inputStream, ExecutorService executor,
                                          OffsetsCache entryOffsetsCache) {
        this(ledgerId, index, inputStream, executor, entryOffsetsCache, DEFAULT_LEARNED_OFFSET_INTERVAL_BYTES);
    }

    @VisibleForTesting
    BlobStoreBackedReadHandleImpl(long ledgerId, OffloadIndexBlock index,
                                  BackedInputStream inputStream, ExecutorService executor,
                                  OffsetsCache entryOffsetsCache, long learnedOffsetIntervalBytes) {
        this.ledgerId = ledgerId;
        this.index = index;
        this.inputStream = inputStream;
        this.dataStream = new DataInputStream(inputStream);
        this.executor = executor;
        this.entryOffsetsCache = entryOffsetsCache;
        this.indexedEntryIds = collectIndexedEntryIds(ledgerId, index);
        this.learnedOffsetIntervalBytes = learnedOffsetIntervalBytes;
        state = State.Opened;
    }

    @VisibleForTesting
    static long learnedOffsetIntervalBytes(long configuredIntervalBytes) {
        return configuredIntervalBytes <= 0 ? 0 : Math.max(MIN_LEARNED_OFFSET_INTERVAL_BYTES, configuredIntervalBytes);
    }

    @VisibleForTesting
    NavigableSet<Long> getLearnedEntryIds() {
        return new TreeSet<>(learnedOffsets.keySet());
    }

    private static long[] collectIndexedEntryIds(long ledgerId, OffloadIndexBlock index) {
        List<Long> entryIds = new ArrayList<>();
        try {
            long entryId = index.getLedgerMetadata().getLastEntryId();
            while (entryId >= 0) {
                OffloadIndexEntry indexEntry = index.getIndexEntryForEntry(entryId);
                if (indexEntry == null || indexEntry.getEntryId() > entryId) {
                    break;
                }
                entryIds.add(indexEntry.getEntryId());
                entryId = indexEntry.getEntryId() - 1;
            }
        } catch (Exception e) {
            // Reads still work, but searches can no longer prefer the entries that are cheap to read
            log.warn().attr("ledgerId", ledgerId).exception(e)
                    .log("Failed to collect the indexed entry ids of the offloaded ledger");
            return new long[0];
        }
        long[] result = new long[entryIds.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = entryIds.get(result.length - 1 - i);
        }
        return result;
    }

    @Override
    public long getIndexedEntryIdFloor(long entryId) {
        // Only the entries of the index: the offsets learned while skipping entries depend on the previous reads, and
        // a search that probes them would take a different path, and not benefit from the offsets it already cached
        int i = Arrays.binarySearch(indexedEntryIds, entryId);
        if (i >= 0) {
            return entryId;
        }
        // No indexed entry lower than entryId when the insertion point is 0
        return i == -1 ? -1 : indexedEntryIds[-i - 2];
    }

    @Override
    public long getIndexedEntryIdCeiling(long entryId) {
        int i = Arrays.binarySearch(indexedEntryIds, entryId);
        if (i >= 0) {
            return entryId;
        }
        // No indexed entry greater than entryId when the insertion point is the end of the array
        int insertionPoint = -i - 1;
        return insertionPoint == indexedEntryIds.length ? -1 : indexedEntryIds[insertionPoint];
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return index.getLedgerMetadata();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null || !closeFuture.compareAndSet(null, new CompletableFuture<>())) {
            return closeFuture.get();
        }

        CompletableFuture<Void> promise = closeFuture.get();
        executor.execute(() -> {
            try {
                index.close();
                inputStream.close();
                state = State.Closed;
                promise.complete(null);
            } catch (IOException t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    private class ReadTask implements Runnable {
        private final long firstEntry;
        private final long lastEntry;
        private final CompletableFuture<LedgerEntries> promise;
        private int seekedAndTryTimes = 0;
        // Offset of the learned offset that the next learned offset must be at least the interval away from: the
        // nearest one at or before the skip position, or the next one when it is closer than the interval. Valid until
        // the next seek. Tracked here so that skipping entries does not look up "learnedOffsets" for each of them.
        private long learnedOffsetFloor;
        private boolean learnedOffsetFloorKnown = false;

        public ReadTask(long firstEntry, long lastEntry, CompletableFuture<LedgerEntries> promise) {
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.promise = promise;
        }

        @Override
        public void run() {
            if (state == State.Closed) {
                log.warn().attr("ledgerId", ledgerId).attr("firstEntry", firstEntry)
                        .attr("lastEntry", lastEntry).log("Reading a closed read handler");
                promise.completeExceptionally(new ManagedLedgerException.OffloadReadHandleClosedException());
                return;
            }

            List<LedgerEntry> entryCollector = new ArrayList<LedgerEntry>();
            try {
                if (firstEntry > lastEntry
                        || firstEntry < 0
                        || lastEntry > getLastAddConfirmed()) {
                    promise.completeExceptionally(new BKException.BKIncorrectParameterException());
                    return;
                }
                long entriesToRead = (lastEntry - firstEntry) + 1;
                long expectedEntryId = firstEntry;
                seekToEntryOffset(firstEntry);
                seekedAndTryTimes++;

                while (entriesToRead > 0) {
                    long currentPosition = inputStream.getCurrentPosition();
                    int length = dataStream.readInt();
                    if (length < 0) { // hit padding or new block
                        seekToEntryOffset(expectedEntryId);
                        continue;
                    }
                    long entryId = dataStream.readLong();
                    if (entryId == expectedEntryId) {
                        entryOffsetsCache.put(ledgerId, entryId, currentPosition);
                        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(length, length);
                        entryCollector.add(LedgerEntryImpl.create(ledgerId, entryId, length, buf));
                        int toWrite = length;
                        while (toWrite > 0) {
                            toWrite -= buf.writeBytes(dataStream, toWrite);
                        }
                        entriesToRead--;
                        expectedEntryId++;
                    } else {
                        handleUnexpectedEntryId(expectedEntryId, entryId);
                    }
                }
                promise.complete(LedgerEntriesImpl.create(entryCollector));
            } catch (Throwable t) {
                log.error().attr("firstEntry", firstEntry).attr("lastEntry", lastEntry)
                        .attr("ledgerId", ledgerId)
                        .attr("position", inputStream.getCurrentPosition()).exception(t)
                        .log("Failed to read entries from the offloader");
                if (t instanceof KeyNotFoundException) {
                    promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                } else {
                    promise.completeExceptionally(t);
                }
                entryCollector.forEach(LedgerEntry::close);
            }
        }

        // in the normal case, the entry id should increment in order. But if there has random access in
        // the read method, we should allow to seek to the right position and the entry id should
        // never over to the last entry again.
        private void handleUnexpectedEntryId(long expectedId, long actEntryId) throws Exception {
            LedgerMetadata ledgerMetadata = getLedgerMetadata();
            OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedId);
            OffloadIndexEntry offsetOfActId = actEntryId <= getLedgerMetadata().getLastEntryId() && actEntryId >= 0
                    ? index.getIndexEntryForEntry(actEntryId) : null;
            // If it still fails after tried entries count times, throw the exception.
            long maxTryTimes = Math.max(3, (lastEntry - firstEntry + 1) >> 2);
            if (seekedAndTryTimes > maxTryTimes) {
                log.error().attr("firstEntry", firstEntry).attr("lastEntry", lastEntry)
                        .attr("ledgerId", ledgerId).attr("actualEntryId", actEntryId)
                        .attr("actualOffset", offsetOfActId != null ? String.valueOf(offsetOfActId) : "null")
                        .attr("expectedEntryId", expectedId)
                        .attr("expectedOffset", String.valueOf(offsetOfExpectedId))
                        .attr("retries", seekedAndTryTimes)
                        .attr("lac", ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : -1)
                        .log("Got incorrect entry id, exhausted retries");
                throw new BKException.BKUnexpectedConditionException();
            } else {
                log.warn().attr("firstEntry", firstEntry).attr("lastEntry", lastEntry)
                        .attr("ledgerId", ledgerId).attr("actualEntryId", actEntryId)
                        .attr("actualOffset", offsetOfActId != null ? String.valueOf(offsetOfActId) : "null")
                        .attr("expectedEntryId", expectedId)
                        .attr("expectedOffset", String.valueOf(offsetOfExpectedId))
                        .attr("retries", seekedAndTryTimes)
                        .attr("lac", ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : -1)
                        .log("Got incorrect entry id, retrying");
            }
            seekToEntryOffset(expectedId);
            seekedAndTryTimes++;
        }

        private void skipPreviousEntry(long startEntryId, long expectedEntryId) throws IOException, BKException {
            // Skipping a single entry is how sequential reads continue: nothing to learn there
            boolean learnOffsets = learnedOffsetIntervalBytes > 0 && expectedEntryId - startEntryId > 1;
            long nextExpectedEntryId = startEntryId;
            while (nextExpectedEntryId < expectedEntryId) {
                long offset = inputStream.getCurrentPosition();
                int len = dataStream.readInt();
                if (len < 0) {
                    LedgerMetadata ledgerMetadata = getLedgerMetadata();
                    OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                    log.error().attr("firstEntry", firstEntry).attr("lastEntry", lastEntry)
                            .attr("ledgerId", ledgerId).attr("entryId", nextExpectedEntryId)
                            .attr("len", len)
                            .attr("expectedEntryId", expectedEntryId)
                            .attr("expectedOffset", String.valueOf(offsetOfExpectedId))
                            .attr("retries", seekedAndTryTimes)
                            .attr("lac", ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : -1)
                            .log("Failed to skip previous entry, got negative len");
                    throw new BKException.BKUnexpectedConditionException();
                }
                long entryId = dataStream.readLong();
                if (entryId == nextExpectedEntryId) {
                    entryOffsetsCache.put(ledgerId, entryId, offset);
                    if (learnOffsets) {
                        learnOffset(entryId, offset);
                    }
                    long skipped = inputStream.skip(len);
                    if (skipped != len) {
                        LedgerMetadata ledgerMetadata = getLedgerMetadata();
                        OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                        log.error().attr("firstEntry", firstEntry).attr("lastEntry", lastEntry)
                                .attr("ledgerId", ledgerId).attr("entryId", entryId)
                                .attr("offset", offset).attr("len", len)
                                .attr("expectedEntryId", expectedEntryId)
                                .attr("expectedOffset", String.valueOf(offsetOfExpectedId))
                                .attr("retries", seekedAndTryTimes)
                                .attr("lac", ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : -1)
                                .log("Failed to skip previous entry, no more data");
                        throw new BKException.BKUnexpectedConditionException();
                    }
                    nextExpectedEntryId++;
                } else {
                    LedgerMetadata ledgerMetadata = getLedgerMetadata();
                    OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                    log.error().attr("firstEntry", firstEntry).attr("lastEntry", lastEntry)
                            .attr("ledgerId", ledgerId).attr("actualEntryId", entryId)
                            .attr("expectedEntryId", expectedEntryId)
                            .attr("expectedOffset", String.valueOf(offsetOfExpectedId))
                            .attr("retries", seekedAndTryTimes)
                            .attr("lac", ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : -1)
                            .log("Got incorrect entry id while skipping previous entry");
                    throw new BKException.BKUnexpectedConditionException();
                }
            }
        }

        private void learnOffset(long entryId, long offset) {
            if (!learnedOffsetFloorKnown) {
                Map.Entry<Long, Long> floor = learnedOffsets.floorEntry(entryId);
                learnedOffsetFloor = floor != null ? floor.getValue() : Long.MIN_VALUE;
                learnedOffsetFloorKnown = true;
            }
            if (learnedOffsetFloor != Long.MIN_VALUE && offset - learnedOffsetFloor < learnedOffsetIntervalBytes) {
                return;
            }
            // Keep the offsets at least the interval apart from the next one too, e.g. when an earlier scan remembered
            // an offset just after the entries that this scan reaches
            Map.Entry<Long, Long> next = learnedOffsets.higherEntry(entryId);
            if (next != null && next.getValue() - offset < learnedOffsetIntervalBytes) {
                learnedOffsetFloor = next.getValue();
                return;
            }
            learnedOffsets.put(entryId, offset);
            learnedOffsetFloor = offset;
        }

        private void seekToEntryOffset(long expectedEntryId) throws IOException, BKException {
            learnedOffsetFloorKnown = false;
            // 1. Try to find the precise index.
            // 1-1. Precise cached indexes.
            Long cachedPreciseIndex = entryOffsetsCache.getIfPresent(ledgerId, expectedEntryId);
            if (cachedPreciseIndex != null) {
                inputStream.seek(cachedPreciseIndex);
                return;
            }
            // 1-2. Precise persistent indexes.
            OffloadIndexEntry indexOfNearestEntry = index.getIndexEntryForEntry(expectedEntryId);
            if (indexOfNearestEntry.getEntryId() == expectedEntryId) {
                inputStream.seek(indexOfNearestEntry.getDataOffset());
                return;
            }
            // 1-3. Offsets remembered by previous scans of the same block, see step 3.
            Map.Entry<Long, Long> learned = learnedOffsets.floorEntry(expectedEntryId);
            if (learned != null && learned.getKey() < indexOfNearestEntry.getEntryId()) {
                learned = null;
            }
            if (learned != null && learned.getKey() == expectedEntryId) {
                inputStream.seek(learned.getValue());
                return;
            }
            // 2. Probe backwards for the nearest cached offset within a bounded window. Since entry-0
            //    must have a precise index, we can skip checking whether "expectedEntryId" is larger
            //    than 0. skipPreviousEntry() below caches every entry it walks past, so once a block
            //    has been walked once, a later jump into the same block lands on that cached run
            //    within "gap" probes instead of re-walking from the sparse index marker in step 4.
            //    Probing stops at the nearest learned offset, which is used when nothing closer is cached.
            long probeFloor = Math.max(indexOfNearestEntry.getEntryId(), expectedEntryId - MAX_OFFSET_PROBE);
            if (learned != null) {
                probeFloor = Math.max(probeFloor, learned.getKey() + 1);
            }
            for (long probe = expectedEntryId - 1; probe >= probeFloor; probe--) {
                Long cachedOffset = entryOffsetsCache.getIfPresent(ledgerId, probe);
                if (cachedOffset != null) {
                    inputStream.seek(cachedOffset);
                    skipPreviousEntry(probe, expectedEntryId);
                    return;
                }
            }
            // 3. Use the nearest offset remembered by a previous scan of the same block, so that only the entries
            //    after it are scanned instead of the block from its start: about "learnedOffsetIntervalBytes" at most
            //    in a part of the block that was scanned before. A binary search by timestamp reads entries spread
            //    over the block, which would otherwise rescan it from its start on each read.
            if (learned != null) {
                inputStream.seek(learned.getValue());
                skipPreviousEntry(learned.getKey(), expectedEntryId);
                return;
            }
            // 4. Use the persistent index of the nearest entry that is smaller than "expectedEntryId".
            //    Because it is a sparse index, some entries need to be skipped.
            if (indexOfNearestEntry.getEntryId() < expectedEntryId) {
                inputStream.seek(indexOfNearestEntry.getDataOffset());
                skipPreviousEntry(indexOfNearestEntry.getEntryId(), expectedEntryId);
            } else {
                LedgerMetadata ledgerMetadata = getLedgerMetadata();
                log.error().attr("firstEntry", firstEntry).attr("lastEntry", lastEntry)
                        .attr("ledgerId", ledgerId)
                        .attr("index", String.valueOf(indexOfNearestEntry))
                        .attr("expectedEntryId", expectedEntryId)
                        .attr("retries", seekedAndTryTimes)
                        .attr("lac", ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : -1)
                        .log("Got incorrect index greater than expected entry");
                throw new BKException.BKUnexpectedConditionException();
            }
        }
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        log.debug().attr("ledgerId", getId()).attr("firstEntry", firstEntry)
                .attr("lastEntry", lastEntry).attr("entries", 1 + lastEntry - firstEntry)
                .log("Reading entries");
        CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();

        // Ledger handles will be only marked idle when "pendingRead" is "0", it is not needed to update
        // "lastAccessTimestamp" if "pendingRead" is larger than "0".
        // Rather than update "lastAccessTimestamp" when starts a reading, updating it when a reading task is finished
        // is better.
        PENDING_READ_UPDATER.incrementAndGet(this);
        promise.whenComplete((__, ex) -> {
            lastAccessTimestamp = System.currentTimeMillis();
            PENDING_READ_UPDATER.decrementAndGet(BlobStoreBackedReadHandleImpl.this);
        });
        executor.execute(new ReadTask(firstEntry, lastEntry, promise));
        return promise;
    }

    private void seekToEntry(long nextExpectedId) throws IOException {
        Long knownOffset = entryOffsetsCache.getIfPresent(ledgerId, nextExpectedId);
        if (knownOffset != null) {
            inputStream.seek(knownOffset);
        } else {
            // we don't know the exact position
            // we seek to somewhere before the entry
            long dataOffset = index.getIndexEntryForEntry(nextExpectedId).getDataOffset();
            inputStream.seek(dataOffset);
        }
    }

    private void seekToEntry(OffloadIndexEntry offloadIndexEntry) throws IOException {
        long dataOffset = offloadIndexEntry.getDataOffset();
        inputStream.seek(dataOffset);
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public long getLastAddConfirmed() {
        return getLedgerMetadata().getLastEntryId();
    }

    @Override
    public long getLength() {
        return getLedgerMetadata().getLength();
    }

    @Override
    public boolean isClosed() {
        return getLedgerMetadata().isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    public static ReadHandle open(ScheduledExecutorService executor,
                                  BlobStore blobStore, String bucket, String key, String indexKey,
                                  VersionCheck versionCheck,
                                  long ledgerId, int readBufferSize,
                                  LedgerOffloaderStats offloaderStats, String managedLedgerName,
                                  OffsetsCache entryOffsetsCache)
            throws IOException, BKException.BKNoSuchLedgerExistsException {
        int retryCount = 3;
        OffloadIndexBlock index = null;
        IOException lastException = null;
        String topicName = TopicName.fromPersistenceNamingEncoding(managedLedgerName);
        // The following retry is used to avoid to some network issue cause read index file failure.
        // If it can not recovery in the retry, we will throw the exception and the dispatcher will schedule to
        // next read.
        // If we use a backoff to control the retry, it will introduce a concurrent operation.
        // We don't want to make it complicated, because in the most of case it shouldn't in the retry loop.
        while (retryCount-- > 0) {
            long readIndexStartTime = System.nanoTime();
            Blob blob = blobStore.getBlob(bucket, indexKey);
            if (blob == null) {
                log.error().attr("indexKey", indexKey).attr("bucket", bucket)
                    .log("Index key not found in container");
                throw new BKException.BKNoSuchLedgerExistsException();
            }
            offloaderStats.recordReadOffloadIndexLatency(topicName,
                    System.nanoTime() - readIndexStartTime, TimeUnit.NANOSECONDS);
            versionCheck.check(indexKey, blob);
            OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create();
            try (InputStream payLoadStream = blob.getPayload().openStream()) {
                index = (OffloadIndexBlock) indexBuilder.fromStream(payLoadStream);
            } catch (IOException e) {
                // retry to avoid the network issue caused read failure
                log.warn().attr("indexKey", indexKey).attr("retriesLeft", retryCount)
                        .exception(e).log("Failed to get index block from offloaded index file");
                lastException = e;
                continue;
            }
            lastException = null;
            break;
        }
        if (lastException != null) {
            throw lastException;
        }

        BackedInputStream inputStream = new BlobStoreBackedInputStreamImpl(blobStore, bucket, key,
                versionCheck, index.getDataObjectLength(), readBufferSize, offloaderStats, managedLedgerName);

        return new BlobStoreBackedReadHandleImpl(ledgerId, index, inputStream, executor, entryOffsetsCache);
    }

    // for testing
    @VisibleForTesting
    State getState() {
        return this.state;
    }

    @Override
    public long lastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    @Override
    public int getPendingRead() {
        return PENDING_READ_UPDATER.get(this);
    }
}
