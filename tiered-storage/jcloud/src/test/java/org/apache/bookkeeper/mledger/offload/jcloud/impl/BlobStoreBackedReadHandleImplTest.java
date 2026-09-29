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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    @Test
    public void testReadScansFromLearnedOffset() throws Exception {
        int entrySize = 20;
        int entryCount = 5000;
        long learnedOffsetIntervalBytes = 4096;
        int headerSize = 128;
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(headerSize + entryCount * entrySize);
        data.writerIndex(headerSize);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(entrySize - 12);
            data.writeLong(i);
            data.writeZero(entrySize - 12);
        }
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(2)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entryCount - 1)
                .withLength((long) entryCount * (entrySize - 12))
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();
        // A single data block: only the first entry has an indexed offset
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        when(mockIndex.getIndexEntryForEntry(anyLong())).thenReturn(OffloadIndexEntryImpl.of(0, 0, headerSize, 0));
        BackedInputStreamImpl inputStream = new BackedInputStreamImpl(data);
        // Start without cached offsets, e.g. from a previous run of the same test
        offsetsCache.clear();
        BlobStoreBackedReadHandleImpl ledger = new BlobStoreBackedReadHandleImpl(2, mockIndex, inputStream,
                executor, offsetsCache, learnedOffsetIntervalBytes);
        try {
            // The first read scans the block from its start
            long firstEntry = 2000;
            try (LedgerEntries entries = ledger.read(firstEntry, firstEntry)) {
                assertThat(entries.iterator().next().getEntryId()).isEqualTo(firstEntry);
            }
            assertThat(inputStream.bytesRead).as("bytes scanned by the first read")
                    .isGreaterThanOrEqualTo(firstEntry * entrySize);

            // A read further than the probe window of the offsets cache resumes from the nearest learned offset
            // instead of scanning the block from its start again
            long secondEntry = 4999;
            inputStream.bytesRead = 0;
            try (LedgerEntries entries = ledger.read(secondEntry, secondEntry)) {
                assertThat(entries.iterator().next().getEntryId()).isEqualTo(secondEntry);
            }
            // Offsets are learned every 205 entries (the first ones at least 4096 bytes apart), so the nearest one
            // below the first read is the one of entry 9 * 205 = 1845
            assertThat(inputStream.bytesRead).as("bytes scanned by the second read")
                    .isEqualTo((secondEntry - 1845 + 1) * entrySize);

            // Learned offsets are not reported as indexed entries, so that searches do not depend on previous reads
            assertThat(ledger.getIndexedEntryIdFloor(secondEntry)).isEqualTo(0);
        } finally {
            ledger.close();
            data.release();
        }
    }

    @Test
    public void testLearnedOffsetsOfAnotherBlockAreNotUsed() throws Exception {
        int entrySize = 20;
        int entryCount = 5000;
        long secondBlockFirstEntry = 2500;
        int headerSize = 128;
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(headerSize + entryCount * entrySize);
        data.writerIndex(headerSize);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(entrySize - 12);
            data.writeLong(i);
            data.writeZero(entrySize - 12);
        }
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(4)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entryCount - 1)
                .withLength((long) entryCount * (entrySize - 12))
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();
        // Two data blocks, whose first entries have indexed offsets
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        when(mockIndex.getIndexEntryForEntry(anyLong())).thenAnswer(invocation -> {
            long entryId = invocation.getArgument(0);
            long floor = entryId >= secondBlockFirstEntry ? secondBlockFirstEntry : 0;
            return OffloadIndexEntryImpl.of(floor, 0, headerSize + floor * entrySize, 0);
        });
        BackedInputStreamImpl inputStream = new BackedInputStreamImpl(data);
        offsetsCache.clear();
        BlobStoreBackedReadHandleImpl ledger = new BlobStoreBackedReadHandleImpl(4, mockIndex, inputStream,
                executor, offsetsCache, 4096);
        try {
            // Learns the offsets of entries 0, 205, ..., 1845 in the first block
            try (LedgerEntries entries = ledger.read(2000, 2000)) {
                assertThat(entries.iterator().next().getEntryId()).isEqualTo(2000);
            }
            // Without the offsets cache, a learned entry is read directly
            offsetsCache.clear();
            inputStream.bytesRead = 0;
            try (LedgerEntries entries = ledger.read(1845, 1845)) {
                assertThat(entries.iterator().next().getEntryId()).isEqualTo(1845);
            }
            assertThat(inputStream.bytesRead).as("bytes read for a learned entry").isEqualTo(entrySize);

            // An entry of the second block is scanned from the start of that block, not from an offset learned in
            // the first block
            inputStream.bytesRead = 0;
            try (LedgerEntries entries = ledger.read(3000, 3000)) {
                assertThat(entries.iterator().next().getEntryId()).isEqualTo(3000);
            }
            assertThat(inputStream.bytesRead).as("bytes scanned in the second block")
                    .isEqualTo((3000 - secondBlockFirstEntry + 1) * entrySize);
        } finally {
            ledger.close();
            data.release();
        }
    }
}
