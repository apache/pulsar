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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.offload.jcloud.DataBlockHeader;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

@Slf4j
public class BlockAwareSegmentInputStreamTest {
    private static final byte DEFAULT_ENTRY_BYTE = 0xB;

    @Data
    class MockLedgerEntry implements LedgerEntry {
        long ledgerId;
        long entryId;
        long length;
        byte entryBytes[];
        ByteBuf entryBuffer;

        MockLedgerEntry(long ledgerId, long entryId, long length,
                        Supplier<Byte> dataSupplier) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.length = length;
            this.entryBytes = new byte[(int)length];
            entryBuffer = Unpooled.wrappedBuffer(entryBytes);
            entryBuffer.writerIndex(0);
            IntStream.range(0, (int)length).forEach(i -> entryBuffer.writeByte(dataSupplier.get()));
        }

        @Override
        public ByteBuffer getEntryNioBuffer() {
            return null;
        }

        @Override
        public LedgerEntry duplicate() {
            return null;
        }

        @Override
        public void close() {
            entryBuffer.release();
        }
    }

    @Data
    class MockLedgerEntries implements LedgerEntries {
        int ledgerId;
        int startEntryId;
        int count;
        int entrySize;
        List<LedgerEntry> entries;

        MockLedgerEntries(int ledgerId, int startEntryId, int count, int entrySize, Supplier<Byte> dataSupplier) {
            this.ledgerId = ledgerId;
            this.startEntryId = startEntryId;
            this.count = count;
            this.entrySize = entrySize;
            this.entries = Lists.newArrayList(count);

            IntStream.range(startEntryId, startEntryId + count).forEach(i ->
                    entries.add(new MockLedgerEntry(ledgerId, i, entrySize, dataSupplier)));
        }

        @Override
        public void close() {
            entries.clear();
        }

        @Override
        public LedgerEntry getEntry(long entryId) {
            if (entryId < startEntryId || entryId >= startEntryId + count) {
                return null;
            }

            return entries.get(((int)entryId - startEntryId));
        }

        @Override
        public Iterator<LedgerEntry> iterator() {
            return entries.iterator();
        }
    }

    class MockReadHandle implements ReadHandle {
        int ledgerId;
        int entrySize;
        int lac;
        Supplier<Byte> dataSupplier;

        MockReadHandle(int ledgerId, int entrySize, int lac, Supplier<Byte> dataSupplier) {
            this.ledgerId = ledgerId;
            this.entrySize = entrySize;
            this.lac = lac;
            this.dataSupplier = dataSupplier;
        }

        MockReadHandle(int ledgerId, int entrySize, int lac) {
            this(ledgerId, entrySize, lac, () -> DEFAULT_ENTRY_BYTE);
        }

        @Override
        public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
            CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
            LedgerEntries entries = new MockLedgerEntries(ledgerId,
                (int)firstEntry,
                (int)(lastEntry - firstEntry + 1),
                    entrySize, dataSupplier);

            future.complete(entries);
            return future;
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
            return readAsync(firstEntry, lastEntry);
        }

        @Override
        public CompletableFuture<Long> readLastAddConfirmedAsync() {
            return null;
        }

        @Override
        public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
            return null;
        }

        @Override
        public long getLastAddConfirmed() {
            return lac;
        }

        @Override
        public long getLength() {
            return (lac + 1) * entrySize;
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public CompletableFuture<LastConfirmedAndEntry>
        readLastAddConfirmedAndEntryAsync(long entryId, long timeOutInMillis, boolean parallel) {
            return null;
        }

        @Override
        public LedgerMetadata getLedgerMetadata() {
            return null;
        }

        @Override
        public long getId() {
            return ledgerId;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return null;
        }
    }

    @Test
    public void testHaveEndPadding() throws Exception {
        int ledgerId = 1;
        int entrySize = 8;
        int lac = 160;
        ReadHandle readHandle = new MockReadHandle(ledgerId, entrySize, lac);

        // set block size bigger than to (header + entry) size.
        int blockSize = 3148 + 5;
        BlockAwareSegmentInputStreamImpl inputStream = new BlockAwareSegmentInputStreamImpl(readHandle, 0, blockSize);
        int expectedEntryCount = (blockSize - DataBlockHeaderImpl.getDataStartOffset()) / (entrySize + 4 + 8);

        // verify get methods
        assertEquals(inputStream.getLedger(), readHandle);
        assertEquals(inputStream.getStartEntryId(), 0);
        assertEquals(inputStream.getBlockSize(), blockSize);

        // verify read inputStream
        // 1. read header. 128
        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        DataBlockHeader headerRead = DataBlockHeaderImpl.fromStream(new ByteArrayInputStream(headerB));
        assertEquals(headerRead.getBlockLength(), blockSize);
        assertEquals(headerRead.getFirstEntryId(), 0);

        byte[] entryData = new byte[entrySize];
        Arrays.fill(entryData, (byte)0xB); // 0xB is MockLedgerEntry.blockPadding

        // 2. read Ledger entries. 201 * 20
        IntStream.range(0, expectedEntryCount).forEach(i -> {
            try {
                byte lengthBuf[] = new byte[4];
                byte entryIdBuf[] = new byte[8];
                byte content[] = new byte[entrySize];
                inputStream.read(lengthBuf);
                inputStream.read(entryIdBuf);
                inputStream.read(content);

                assertEquals(entrySize, Ints.fromByteArray(lengthBuf));
                assertEquals(i, Longs.fromByteArray(entryIdBuf));
                assertArrayEquals(entryData, content);
            } catch (Exception e) {
                fail("meet exception", e);
            }
        });

        // 3. read padding
        int left = blockSize - DataBlockHeaderImpl.getDataStartOffset() -  expectedEntryCount * (entrySize + 4 + 8);
        assertEquals(left, 5);
        byte padding[] = new byte[left];
        inputStream.read(padding);
        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        IntStream.range(0, paddingBuf.capacity()/4).forEach(i ->
            assertEquals(Integer.toHexString(paddingBuf.readInt()),
                         Integer.toHexString(0xFEDCDEAD)));

        // 4. reach end.
        assertEquals(inputStream.read(), -1);

        assertEquals(inputStream.getBlockEntryCount(), expectedEntryCount);
        assertEquals(inputStream.getBlockEntryBytesCount(), entrySize * expectedEntryCount);
        assertEquals(inputStream.getEndEntryId(), expectedEntryCount - 1);

        inputStream.close();
    }

    @Test
    public void testNoEndPadding() throws Exception {
        int ledgerId = 1;
        int entrySize = 8;
        int lac = 120;
        ReadHandle readHandle = new MockReadHandle(ledgerId, entrySize, lac);

        // set block size equals to (header + entry) size.
        int blockSize = 2148;
        BlockAwareSegmentInputStreamImpl inputStream = new BlockAwareSegmentInputStreamImpl(readHandle, 0, blockSize);
        int expectedEntryCount = (blockSize - DataBlockHeaderImpl.getDataStartOffset())
            / (entrySize + BlockAwareSegmentInputStreamImpl.ENTRY_HEADER_SIZE);

        // verify get methods
        assertEquals(inputStream.getLedger(), readHandle);
        assertEquals(inputStream.getStartEntryId(), 0);
        assertEquals(inputStream.getBlockSize(), blockSize);

        // verify read inputStream
        // 1. read header. 128
        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        DataBlockHeader headerRead = DataBlockHeaderImpl.fromStream(new ByteArrayInputStream(headerB));
        assertEquals(headerRead.getBlockLength(), blockSize);
        assertEquals(headerRead.getFirstEntryId(), 0);

        byte[] entryData = new byte[entrySize];
        Arrays.fill(entryData, (byte)0xB); // 0xB is MockLedgerEntry.blockPadding

        // 2. read Ledger entries. 201 * 20
        IntStream.range(0, expectedEntryCount).forEach(i -> {
            try {
                byte lengthBuf[] = new byte[4];
                byte entryIdBuf[] = new byte[8];
                byte content[] = new byte[entrySize];
                inputStream.read(lengthBuf);
                inputStream.read(entryIdBuf);
                inputStream.read(content);

                assertEquals(entrySize, Ints.fromByteArray(lengthBuf));
                assertEquals(i, Longs.fromByteArray(entryIdBuf));
                assertArrayEquals(entryData, content);
            } catch (Exception e) {
                fail("meet exception", e);
            }
        });

        // 3. should be no padding
        int left = blockSize - DataBlockHeaderImpl.getDataStartOffset() -  expectedEntryCount * (entrySize + 4 + 8);
        assertEquals(left, 0);

        // 4. reach end.
        assertEquals(inputStream.read(), -1);

        assertEquals(inputStream.getBlockEntryCount(), expectedEntryCount);
        assertEquals(inputStream.getBlockEntryBytesCount(), entrySize * expectedEntryCount);
        assertEquals(inputStream.getEndEntryId(), expectedEntryCount - 1);

        inputStream.close();
    }

    @Test
    public void testReadTillLac() throws Exception {
        // simulate last data block read.
        int ledgerId = 1;
        int entrySize = 8;
        int lac = 89;
        ReadHandle readHandle = new MockReadHandle(ledgerId, entrySize, lac);

        // set block size equals to (header + lac_entry) size.
        int blockSize = DataBlockHeaderImpl.getDataStartOffset() + (1 + lac) * (entrySize + 4 + 8);
        BlockAwareSegmentInputStreamImpl inputStream = new BlockAwareSegmentInputStreamImpl(readHandle, 0, blockSize);
        int expectedEntryCount = (blockSize - DataBlockHeaderImpl.getDataStartOffset()) / (entrySize + 4 + 8);

        // verify get methods
        assertEquals(inputStream.getLedger(), readHandle);
        assertEquals(inputStream.getStartEntryId(), 0);
        assertEquals(inputStream.getBlockSize(), blockSize);

        // verify read inputStream
        // 1. read header. 128
        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        DataBlockHeader headerRead = DataBlockHeaderImpl.fromStream(new ByteArrayInputStream(headerB));
        assertEquals(headerRead.getBlockLength(), blockSize);
        assertEquals(headerRead.getFirstEntryId(), 0);

        byte[] entryData = new byte[entrySize];
        Arrays.fill(entryData, (byte)0xB); // 0xB is MockLedgerEntry.blockPadding

        // 2. read Ledger entries. 96 * 20
        IntStream.range(0, expectedEntryCount).forEach(i -> {
            try {
                byte lengthBuf[] = new byte[4];
                byte entryIdBuf[] = new byte[8];
                byte content[] = new byte[entrySize];
                inputStream.read(lengthBuf);
                inputStream.read(entryIdBuf);
                inputStream.read(content);

                assertEquals(entrySize, Ints.fromByteArray(lengthBuf));
                assertEquals(i, Longs.fromByteArray(entryIdBuf));
                assertArrayEquals(entryData, content);
            } catch (Exception e) {
                fail("meet exception", e);
            }
        });

        // 3. should have no padding
        int left = blockSize - DataBlockHeaderImpl.getDataStartOffset() -  expectedEntryCount * (entrySize + 4 + 8);
        assertEquals(left, 0);

        // 4. reach end.
        assertEquals(inputStream.read(), -1);

        assertEquals(inputStream.getBlockEntryCount(), expectedEntryCount);
        assertEquals(inputStream.getBlockEntryBytesCount(), entrySize * expectedEntryCount);
        assertEquals(inputStream.getEndEntryId(), expectedEntryCount - 1);

        inputStream.close();
    }

    @Test
    public void testNoEntryPutIn() throws Exception {
        // simulate first entry size over the block size budget, it shouldn't be added.
        // 2 entries, each with bigger size than block size, so there should no entry added into block.
        int ledgerId = 1;
        int entrySize = 1000;
        int lac = 1;
        ReadHandle readHandle = new MockReadHandle(ledgerId, entrySize, lac);

        // set block size not able to hold one entry
        int blockSize = DataBlockHeaderImpl.getDataStartOffset() + entrySize;
        BlockAwareSegmentInputStreamImpl inputStream = new BlockAwareSegmentInputStreamImpl(readHandle, 0, blockSize);
        int expectedEntryCount = 0;

        // verify get methods
        assertEquals(inputStream.getLedger(), readHandle);
        assertEquals(inputStream.getStartEntryId(), 0);
        assertEquals(inputStream.getBlockSize(), blockSize);

        // verify read inputStream
        // 1. read header. 128
        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        DataBlockHeader headerRead = DataBlockHeaderImpl.fromStream(new ByteArrayInputStream(headerB));
        assertEquals(headerRead.getBlockLength(), blockSize);
        assertEquals(headerRead.getFirstEntryId(), 0);


        // 2. since no entry put in, it should only get padding after header.
        byte padding[] = new byte[blockSize - DataBlockHeaderImpl.getDataStartOffset()];
        inputStream.read(padding);
        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        IntStream.range(0, paddingBuf.capacity()/4).forEach(i ->
            assertEquals(Integer.toHexString(paddingBuf.readInt()),
                         Integer.toHexString(0xFEDCDEAD)));

        // 3. reach end.
        assertEquals(inputStream.read(), -1);

        assertEquals(inputStream.getBlockEntryCount(), 0);
        assertEquals(inputStream.getBlockEntryBytesCount(), 0);
        assertEquals(inputStream.getEndEntryId(), -1);

        inputStream.close();
    }

    @Test
    public void testPaddingOnLastBlock() throws Exception {
        int ledgerId = 1;
        int entrySize = 1000;
        int lac = 0;
        ReadHandle readHandle = new MockReadHandle(ledgerId, entrySize, lac);

        // set block size not able to hold one entry
        int blockSize = DataBlockHeaderImpl.getDataStartOffset() + entrySize * 2;
        BlockAwareSegmentInputStreamImpl inputStream = new BlockAwareSegmentInputStreamImpl(readHandle, 0, blockSize);
        int expectedEntryCount = 1;

        // verify get methods
        assertEquals(inputStream.getLedger(), readHandle);
        assertEquals(inputStream.getStartEntryId(), 0);
        assertEquals(inputStream.getBlockSize(), blockSize);

        // verify read inputStream
        // 1. read header. 128
        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        DataBlockHeader headerRead = DataBlockHeaderImpl.fromStream(new ByteArrayInputStream(headerB));
        assertEquals(headerRead.getBlockLength(), blockSize);
        assertEquals(headerRead.getFirstEntryId(), 0);

        // 2. There should be a single entry
        byte[] entryData = new byte[entrySize];
        Arrays.fill(entryData, (byte)0xB); // 0xB is MockLedgerEntry.blockPadding

        IntStream.range(0, expectedEntryCount).forEach(i -> {
            try {
                byte lengthBuf[] = new byte[4];
                byte entryIdBuf[] = new byte[8];
                byte content[] = new byte[entrySize];
                inputStream.read(lengthBuf);
                inputStream.read(entryIdBuf);
                inputStream.read(content);

                assertEquals(entrySize, Ints.fromByteArray(lengthBuf));
                assertEquals(i, Longs.fromByteArray(entryIdBuf));
                assertArrayEquals(entryData, content);
            } catch (Exception e) {
                fail("meet exception", e);
            }
        });

        // 3. Then padding
        int consumedBytes = DataBlockHeaderImpl.getDataStartOffset()
            + expectedEntryCount * (entrySize + BlockAwareSegmentInputStreamImpl.ENTRY_HEADER_SIZE);
        byte padding[] = new byte[blockSize - consumedBytes];
        inputStream.read(padding);
        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        IntStream.range(0, paddingBuf.capacity()/4).forEach(i ->
                assertEquals(Integer.toHexString(paddingBuf.readInt()),
                             Integer.toHexString(0xFEDCDEAD)));

        // 3. reach end.
        assertEquals(inputStream.read(), -1);

        assertEquals(inputStream.getBlockEntryCount(), 1);
        assertEquals(inputStream.getBlockEntryBytesCount(), entrySize);
        assertEquals(inputStream.getEndEntryId(), 0);

        inputStream.close();
    }

    @Test
    public void testOnlyNegativeOnEOF() throws Exception {
        int ledgerId = 1;
        int entrySize = 10000;
        int lac = 0;

        Random r = new Random(0);
        ReadHandle readHandle = new MockReadHandle(ledgerId, entrySize, lac, () -> (byte)r.nextInt());

        int blockSize = DataBlockHeaderImpl.getDataStartOffset() + entrySize * 2;
        BlockAwareSegmentInputStreamImpl inputStream = new BlockAwareSegmentInputStreamImpl(readHandle, 0, blockSize);

        int bytesRead = 0;
        for (int i = 0; i < blockSize*2; i++) {
            int ret = inputStream.read();
            if (ret < 0) { // should only be EOF
                assertEquals(bytesRead, blockSize);
                break;
            } else {
                bytesRead++;
            }
        }
    }

}
