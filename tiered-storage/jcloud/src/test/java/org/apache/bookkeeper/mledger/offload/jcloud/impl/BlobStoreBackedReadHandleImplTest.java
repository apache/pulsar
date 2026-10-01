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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BlobStoreBackedReadHandleImplTest {

    private OffsetsCache offsetsCache = new OffsetsCache();

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    @AfterClass
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (offsetsCache != null) {
            offsetsCache.close();
        }
    }

    @AfterClass
    public void clearCache() throws Exception {
        offsetsCache.clear();
    }

    private String getExpectedEntryContent(int entryId) {
        return "Entry " + entryId;
    }

    private Pair<BlobStoreBackedReadHandleImpl, ByteBuf> createReadHandle(
            long ledgerId, int entries, boolean hasDirtyData) throws Exception {
        // Build data.
        List<Pair<Integer, Integer>> offsets = new ArrayList<>();
        int totalLen = 0;
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(1024);
        data.writeInt(0);
        data.writerIndex(128);
        //data.readerIndex(128);
        for (int i = 0; i < entries; i++) {
            if (hasDirtyData && i == 1) {
                data.writeBytes("dirty data".getBytes(UTF_8));
            }
            offsets.add(Pair.of(i, data.writerIndex()));
            offsetsCache.put(ledgerId, i, data.writerIndex());
            byte[] entryContent = getExpectedEntryContent(i).getBytes(UTF_8);
            totalLen += entryContent.length;
            data.writeInt(entryContent.length);
            data.writeLong(i);
            data.writeBytes(entryContent);
        }
        // Build metadata.
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entries)
                .withLength(totalLen)
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();
        BackedInputStreamImpl inputStream = new BackedInputStreamImpl(data);
        // Since we have written data to "offsetsCache", the index will never be used.
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        for (Pair<Integer, Integer> pair : offsets) {
            when(mockIndex.getIndexEntryForEntry(pair.getLeft())).thenReturn(
                    OffloadIndexEntryImpl.of(pair.getLeft(), 0, pair.getRight(), 0));
        }
        // Build obj.
        return Pair.of(new BlobStoreBackedReadHandleImpl(ledgerId, mockIndex, inputStream, executor, offsetsCache),
                data);
    }

    private static class BackedInputStreamImpl extends BackedInputStream {

        private ByteBuf data;
        private long bytesRead;

        private BackedInputStreamImpl(ByteBuf data){
            this.data = data;
        }

        @Override
        public void seek(long position) {
            data.readerIndex((int) position);
        }

        @Override
        public void seekForward(long position) throws IOException {
            data.readerIndex((int) position);
        }

        @Override
        public long getCurrentPosition() {
            return data.readerIndex();
        }

        @Override
        public int read() throws IOException {
            if (data.readableBytes() == 0) {
                throw new EOFException("The input-stream has no bytes to read");
            }
            bytesRead++;
            // An unsigned byte, as specified by InputStream.read()
            return data.readByte() & 0xFF;
        }

        @Override
        public int available() throws IOException {
            return data.readableBytes();
        }
    }

    @DataProvider
    public Object[][] streamStartAt() {
        return new Object[][] {
            // It gives a 0 value of the entry length.
            { 0, false },
            // It gives a 0 value of the entry length.
            { 1, false },
            // The first entry starts at 128.
            { 128, false },
            // It gives a 0 value of the entry length.
            { 0, true },
            // It gives a 0 value of the entry length.
            { 1, true },
            // The first entry starts at 128.
            { 128, true }
        };
    }

    @Test(dataProvider = "streamStartAt")
    public void testRead(int streamStartAt, boolean hasDirtyData) throws Exception {
        int entryCount = 5;
        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> ledgerDataPair =
                createReadHandle(1, entryCount, hasDirtyData);
        BlobStoreBackedReadHandleImpl ledger = ledgerDataPair.getLeft();
        ByteBuf data = ledgerDataPair.getRight();
        data.readerIndex(streamStartAt);
        // Teat read each entry.
        for (int i = 0; i < 5; i++) {
            LedgerEntries entries = ledger.read(i, i);
            assertEquals(new String(entries.iterator().next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // Test read all entries.
        LedgerEntries entries1 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator1 = entries1.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator1.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // Test a special case.
        // 1. Read from 0 to "lac - 1".
        // 2. Any reading.
        LedgerEntries entries2 = ledger.read(0, entryCount - 2);
        Iterator<LedgerEntry> iterator2 = entries2.iterator();
        for (int i = 0; i < entryCount - 1; i++) {
            assertEquals(new String(iterator2.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        LedgerEntries entries3 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator3 = entries3.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator3.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // cleanup.
        ledger.close();
    }

    @Test
    public void testGetIndexedEntryIdFloor() throws Exception {
        int entries = 5000;
        long[] indexedEntryIds = {0, 2500};
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(3)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entries - 1)
                .withLength(entries * 100L)
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();
        // A sparse index with one index entry per data block, like the index of an offloaded ledger
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        when(mockIndex.getIndexEntryForEntry(anyLong())).thenAnswer(invocation -> {
            long entryId = invocation.getArgument(0);
            long floor = entryId >= indexedEntryIds[1] ? indexedEntryIds[1] : indexedEntryIds[0];
            return OffloadIndexEntryImpl.of(floor, 0, 128 + floor * 100, 0);
        });
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(0);
        BlobStoreBackedReadHandleImpl ledger = new BlobStoreBackedReadHandleImpl(3, mockIndex,
                new BackedInputStreamImpl(data), executor, offsetsCache);
        try {
            assertThat(ledger.getIndexedEntryIdFloor(0)).isEqualTo(0);
            assertThat(ledger.getIndexedEntryIdFloor(2000)).as("floor in the first block").isEqualTo(0);
            assertThat(ledger.getIndexedEntryIdFloor(2500)).as("first entry of the second block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdFloor(4999)).as("floor in the second block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdCeiling(0)).isEqualTo(0);
            assertThat(ledger.getIndexedEntryIdCeiling(2000)).as("ceiling in the first block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdCeiling(2500)).as("first entry of the second block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdCeiling(4999)).as("no ceiling in the last block").isEqualTo(-1);
        } finally {
            ledger.close();
            data.release();
        }
    }

    private static final int ENTRY_SIZE = 20;
    private static final int HEADER_SIZE = 128;

    /**
     * A read handle over entries of {@link #ENTRY_SIZE} bytes, with 12 bytes of entry header, whose sparse index only
     * knows the offsets of the first entries of its data blocks, like the index of an offloaded ledger.
     */
    private final class SparselyIndexedLedger implements AutoCloseable {
        // A dedicated offsets cache whose offsets do not expire during the test: the TTL of the tests of this module
        // is 1 second, and the assertions count exact bytes depending on the cached offsets
        private final OffsetsCache offsetsCache = new OffsetsCache(3600, 1_000_000);
        private final ByteBuf data;
        private final BackedInputStreamImpl inputStream;
        private final BlobStoreBackedReadHandleImpl handle;

        SparselyIndexedLedger(long ledgerId, int entryCount, long[] blockFirstEntryIds, long learnedOffsetIntervalBytes)
                throws Exception {
            data = ByteBufAllocator.DEFAULT.heapBuffer(HEADER_SIZE + entryCount * ENTRY_SIZE);
            data.writerIndex(HEADER_SIZE);
            for (int i = 0; i < entryCount; i++) {
                data.writeInt(ENTRY_SIZE - 12);
                data.writeLong(i);
                data.writeZero(ENTRY_SIZE - 12);
            }
            LedgerMetadata metadata = LedgerMetadataBuilder.create()
                    .withId(ledgerId)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withAckQuorumSize(1)
                    .withDigestType(DigestType.CRC32C)
                    .withPassword("pwd".getBytes(UTF_8))
                    .withClosedState()
                    .withLastEntryId(entryCount - 1)
                    .withLength((long) entryCount * (ENTRY_SIZE - 12))
                    .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                    .build();
            OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
            when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
            when(mockIndex.getIndexEntryForEntry(anyLong())).thenAnswer(invocation -> {
                long entryId = invocation.getArgument(0);
                long floor = 0;
                for (long blockFirstEntryId : blockFirstEntryIds) {
                    if (blockFirstEntryId <= entryId) {
                        floor = blockFirstEntryId;
                    }
                }
                return OffloadIndexEntryImpl.of(floor, 0, offsetOf(floor), 0);
            });
            inputStream = new BackedInputStreamImpl(data);
            handle = new BlobStoreBackedReadHandleImpl(ledgerId, mockIndex, inputStream, executor, offsetsCache,
                    learnedOffsetIntervalBytes);
        }

        long offsetOf(long entryId) {
            return HEADER_SIZE + entryId * ENTRY_SIZE;
        }

        /**
         * Reads the given entries and returns the number of bytes read from the data object to read them.
         */
        long read(long firstEntry, long lastEntry) throws Exception {
            inputStream.bytesRead = 0;
            try (LedgerEntries entries = handle.read(firstEntry, lastEntry)) {
                long expectedEntryId = firstEntry;
                for (LedgerEntry entry : entries) {
                    assertThat(entry.getEntryId()).isEqualTo(expectedEntryId++);
                }
                assertThat(expectedEntryId).as("entries read").isEqualTo(lastEntry + 1);
            }
            return inputStream.bytesRead;
        }

        @Override
        public void close() throws Exception {
            handle.close();
            data.release();
            offsetsCache.close();
        }
    }

    /**
     * The learned entry ids from {@code first} to {@code last}, {@code step} apart.
     */
    private static Set<Long> entryIds(long first, long last, long step) {
        return LongStream.iterate(first, id -> id <= last, id -> id + step).boxed().collect(Collectors.toSet());
    }

    @Test
    public void testReadScansFromLearnedOffset() throws Exception {
        // A single data block: only the first entry has an indexed offset. With 20 byte entries, learned offsets
        // at least 4096 bytes apart are 205 entries apart.
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(2, 5000, new long[] {0}, 4096)) {
            // The first read scans the block from its start, and learns offsets along the way
            assertThat(ledger.read(2000, 2000)).as("bytes scanned by the first read").isEqualTo(2001L * ENTRY_SIZE);
            assertThat(ledger.handle.getLearnedEntryIds()).isEqualTo(entryIds(0, 1845, 205));

            // A read further than the probe window of the offsets cache resumes from the nearest learned offset
            // instead of scanning the block from its start again
            assertThat(ledger.read(4999, 4999)).as("bytes scanned by the second read")
                    .isEqualTo((4999L - 1845 + 1) * ENTRY_SIZE);

            // Learned offsets are not reported as indexed entries, so that searches do not depend on previous reads
            assertThat(ledger.handle.getIndexedEntryIdFloor(4999)).isEqualTo(0);
        }
    }

    @Test
    public void testReadUsesCachedOffsetsCloserThanLearnedOffsets() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(5, 5000, new long[] {0}, 4096)) {
            // Learns the offsets of entries 0, 205, ..., 1845, and caches the offsets of entries 0 to 2000
            ledger.read(2000, 2000);

            // A cached offset between the nearest learned offset and the entry is closer, so it is used
            assertThat(ledger.read(2100, 2100)).as("bytes scanned from the cached offset of entry 2000")
                    .isEqualTo((2100L - 2000 + 1) * ENTRY_SIZE);

            // A cached offset before the nearest learned offset is farther, so the learned offset is used
            ledger.offsetsCache.clear();
            ledger.offsetsCache.put(5, 1800, ledger.offsetOf(1800));
            assertThat(ledger.read(1900, 1900)).as("bytes scanned from the learned offset of entry 1845")
                    .isEqualTo((1900L - 1845 + 1) * ENTRY_SIZE);
        }
    }

    @Test
    public void testLearnedOffsetsOfAnotherBlockAreNotUsed() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(4, 5000, new long[] {0, 2500}, 4096)) {
            // Learns the offsets of entries 0, 205, ..., 1845 in the first block
            ledger.read(2000, 2000);

            // Without the offsets cache, a learned entry is read directly
            ledger.offsetsCache.clear();
            assertThat(ledger.read(1845, 1845)).as("bytes read for a learned entry").isEqualTo(ENTRY_SIZE);

            // An entry of the second block is scanned from the start of that block, not from an offset learned in
            // the first block
            assertThat(ledger.read(3000, 3000)).as("bytes scanned in the second block")
                    .isEqualTo((3000L - 2500 + 1) * ENTRY_SIZE);
        }
    }

    @Test
    public void testLearnedOffsetsStayApartFromTheNextOne() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(6, 5000, new long[] {0, 2500}, 4096)) {
            // Learns the offsets of entries 2500, 2705 and 2910 in the second block
            ledger.read(3000, 3000);
            // Then scans the first block up to its end: entry 2460 is 205 entries after entry 2255 but less than the
            // interval before the learned entry 2500, so it is not learned
            ledger.read(2499, 2499);

            Set<Long> expected = entryIds(0, 2255, 205);
            expected.addAll(entryIds(2500, 2910, 205));
            assertThat(ledger.handle.getLearnedEntryIds()).isEqualTo(expected);
        }
    }

    @Test
    public void testSequentialReadsDoNotLearnOffsets() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(7, 5000, new long[] {0}, 4096)) {
            // Each read after the first one only skips the last entry of the previous read, whose offset is cached
            for (long firstEntry = 0; firstEntry < 5000; firstEntry += 100) {
                ledger.read(firstEntry, firstEntry + 99);
            }
            assertThat(ledger.handle.getLearnedEntryIds()).isEmpty();
        }
    }

    @Test
    public void testLearnedOffsetIntervalBytes() {
        assertThat(BlobStoreBackedReadHandleImpl.learnedOffsetIntervalBytes(0)).as("disabled").isZero();
        assertThat(BlobStoreBackedReadHandleImpl.learnedOffsetIntervalBytes(-1)).as("disabled").isZero();
        assertThat(BlobStoreBackedReadHandleImpl.learnedOffsetIntervalBytes(1)).as("minimum")
                .isEqualTo(64 * 1024);
        assertThat(BlobStoreBackedReadHandleImpl.learnedOffsetIntervalBytes(2 * 1024 * 1024))
                .isEqualTo(2 * 1024 * 1024);
    }

    @Test
    public void testDisabledLearnedOffsets() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(8, 5000, new long[] {0}, 0)) {
            ledger.read(2000, 2000);
            assertThat(ledger.handle.getLearnedEntryIds()).isEmpty();
            // Without learned offsets, the read scans the block from its start again
            assertThat(ledger.read(4999, 4999)).isEqualTo(5000L * ENTRY_SIZE);
        }
    }
}
