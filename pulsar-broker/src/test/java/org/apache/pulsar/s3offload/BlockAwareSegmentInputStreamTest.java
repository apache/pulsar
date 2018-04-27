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
package org.apache.pulsar.s3offload;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.pulsar.broker.s3offload.DataBlockHeader;
import org.apache.pulsar.broker.s3offload.impl.BlockAwareSegmentInputStream;
import org.apache.pulsar.broker.s3offload.impl.DataBlockHeaderImpl;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

@Slf4j
public class BlockAwareSegmentInputStreamTest {
    @Data
    class MockLedgerEntry implements LedgerEntry {
        byte blockPadding = 0xB;
        long ledgerId;
        long entryId;
        long length;
        byte entryBytes[];
        ByteBuf entryBuffer;

        MockLedgerEntry(long ledgerId, long entryId, long length) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.length = length;
            this.entryBytes = new byte[(int)length];
            entryBuffer = Unpooled.wrappedBuffer(entryBytes);
            entryBuffer.writerIndex(0);
            IntStream.range(0, (int)length).forEach(i -> entryBuffer.writeByte(blockPadding));
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

        MockLedgerEntries(int ledgerId, int startEntryId, int count, int entrySize) {
            this.ledgerId = ledgerId;
            this.startEntryId = startEntryId;
            this.count = count;
            this.entrySize = entrySize;
            this.entries = Lists.newArrayList(count);

            IntStream.range(startEntryId, startEntryId + count).forEach(i ->
                entries.add(new MockLedgerEntry(ledgerId, i, entrySize)));
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
        MockReadHandle(int ledgerId, int entrySize, int lac) {
            this.ledgerId = ledgerId;
            this.entrySize = entrySize;
            this.lac = lac;
        }

        @Override
        public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
            CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
            LedgerEntries entries = new MockLedgerEntries(ledgerId,
                (int)firstEntry,
                (int)(lastEntry - firstEntry + 1),
                entrySize);

            Executors.newCachedThreadPool(new DefaultThreadFactory("test"))
                .submit(() -> future.complete(entries));
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
    public void blockAwareSegmentInputStreamTest() throws Exception {
        int ledgerId = 1;
        int entrySize = 8;
        int lac = 200;
        ReadHandle readHandle = new MockReadHandle(ledgerId, entrySize, lac);

        int blockSize = 2 * 1024; // first 1024 for blockHeader, so it should only able to have entry: 1024/(8+4+8) = 51
        BlockAwareSegmentInputStream inputStream = new BlockAwareSegmentInputStream(readHandle, 0, blockSize);
        CompletableFuture<Void> initFuture = inputStream.initialize();
        initFuture.get();

        int expectedEntryCount = 1024 / (entrySize + 4 + 8);
        // verify get methods
        assertTrue(inputStream.getLedger() == readHandle);
        assertTrue(inputStream.getStartEntryId() == 0);
        assertTrue(inputStream.getBlockSize() == blockSize);
        assertTrue(inputStream.getBlockEntryCount() == expectedEntryCount);
        assertTrue(inputStream.getPayloadBytesWritten() == entrySize * expectedEntryCount);
        assertTrue(inputStream.getLastEntryIdWritten() == expectedEntryCount - 1);

        // verify header
        DataBlockHeader header = inputStream.getDataBlockHeader();
        assertTrue(header.getBlockMagicWord() == 0xDBDBDBDB);
        assertTrue(header.getBlockLength() == blockSize);
        assertTrue(header.getBlockEntryCount() == expectedEntryCount);
        assertTrue(header.getFirstEntryId() == 0);

        // verify read inputStream
        // 1. read header
        byte headerB[] = new byte[DataBlockHeader.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        DataBlockHeader headerRead = DataBlockHeaderImpl.fromStream(new ByteArrayInputStream(headerB));
        assertTrue(headerRead.getBlockMagicWord() == 0xDBDBDBDB);
        assertTrue(headerRead.getBlockLength() == blockSize);
        assertTrue(headerRead.getBlockEntryCount() == expectedEntryCount);
        assertTrue(headerRead.getFirstEntryId() == 0);

        byte[] entryData = new byte[entrySize];
        IntStream.range(0, entrySize).forEach(i -> {
            entryData[i] = 0xB;
        });

        // 2. read Ledger entries
        IntStream.range(0, expectedEntryCount).forEach(i -> {
            try {
                byte lengthBuf[] = new byte[4];
                byte entryIdBuf[] = new byte[8];
                byte content[] = new byte[entrySize];
                inputStream.read(lengthBuf);
                inputStream.read(entryIdBuf);
                inputStream.read(content);

                long entryId = Unpooled.wrappedBuffer(entryIdBuf).getLong(0);

                assertEquals(entrySize, Unpooled.wrappedBuffer(lengthBuf).getInt(0));
                assertEquals(i, entryId);
                assertArrayEquals(entryData, content);
            } catch (Exception e) {
                fail("meet exception", e);
            }
        });

        // 3. read padding
        int left = blockSize - DataBlockHeader.getDataStartOffset() -  expectedEntryCount * (entrySize + 4 + 8);
        byte padding[] = new byte[left];
        inputStream.read(padding);
        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        IntStream.range(0, paddingBuf.capacity()).forEach(i ->
            assertEquals(paddingBuf.readByte(), inputStream.getBlockEndPadding())
        );

        // 4. reach end.
        assertEquals(-1, inputStream.read());

        inputStream.close();
    }

}
