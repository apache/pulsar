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

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The BlockAwareSegmentInputStreamImpl for each cold storage data block.
 * It gets data from ledger, and will be read out the content for a data block.
 * DataBlockHeader + entries(each with format[[entry_size -- int][entry_id -- long][entry_data]]) + padding
 */
public class BlockAwareSegmentInputStreamImpl extends BlockAwareSegmentInputStream {
    private static final Logger log = LoggerFactory.getLogger(BlockAwareSegmentInputStreamImpl.class);

    static final int[] BLOCK_END_PADDING = new int[]{ 0xFE, 0xDC, 0xDE, 0xAD };
    static final byte[] BLOCK_END_PADDING_BYTES =  Ints.toByteArray(0xFEDCDEAD);

    private final ByteBuf paddingBuf = PulsarByteBufAllocator.DEFAULT.buffer(128, 128);

    private final ReadHandle ledger;
    private final long startEntryId;
    private final int blockSize;

    // Number of Message entries that read from ledger and been readout from this InputStream.
    private int blockEntryCount;

    // tracking read status for both header and entries.
    // Bytes that already been read from this InputStream
    private int bytesReadOffset = 0;
    // Byte from this index is all padding byte
    private int dataBlockFullOffset;
    private final InputStream dataBlockHeaderStream;

    // how many entries want to read from ReadHandle each time.
    private static final int ENTRIES_PER_READ = 100;
    // buf the entry size and entry id.
    static final int ENTRY_HEADER_SIZE = 4 /* entry size */ + 8 /* entry id */;
    // Keep a list of all entries ByteBuf, each ByteBuf contains 2 buf: entry header and entry content.
    private List<ByteBuf> entriesByteBuf = null;
    private int currentOffset = 0;
    private final AtomicBoolean close = new AtomicBoolean(false);

    public BlockAwareSegmentInputStreamImpl(ReadHandle ledger, long startEntryId, int blockSize) {
        this.ledger = ledger;
        this.startEntryId = startEntryId;
        this.blockSize = blockSize;
        this.dataBlockHeaderStream = DataBlockHeaderImpl.of(blockSize, startEntryId).toStream();
        this.blockEntryCount = 0;
        this.dataBlockFullOffset = blockSize;
        this.entriesByteBuf = Lists.newLinkedList();
    }

    private ByteBuf readEntries(int len) throws IOException {
        checkState(bytesReadOffset >= DataBlockHeaderImpl.getDataStartOffset());
        checkState(bytesReadOffset < blockSize);

        // once reach the end of entry buffer, read more, if there is more
        if (bytesReadOffset < dataBlockFullOffset
            && entriesByteBuf.isEmpty()
            && startEntryId + blockEntryCount <= ledger.getLastAddConfirmed()) {
            entriesByteBuf = readNextEntriesFromLedger(startEntryId + blockEntryCount, ENTRIES_PER_READ);
        }

        if (!entriesByteBuf.isEmpty()
            && bytesReadOffset + entriesByteBuf.get(0).readableBytes() <= blockSize) {
            // always read from the first ByteBuf in the list, once read all of its content remove it.
            ByteBuf entryByteBuf = entriesByteBuf.get(0);
            int readableBytes = entryByteBuf.readableBytes();
            int read = Math.min(readableBytes, len);
            ByteBuf buf = entryByteBuf.slice(currentOffset, read);
            buf.retain();
            currentOffset += read;
            entryByteBuf.readerIndex(currentOffset);
            bytesReadOffset += read;

            if (entryByteBuf.readableBytes() == 0) {
                entryByteBuf.release();
                entriesByteBuf.remove(0);
                blockEntryCount++;
                currentOffset = 0;
            }

            return buf;
        } else {
            // no space for a new entry or there are no more entries
            // set data block full, return end padding
            if (dataBlockFullOffset == blockSize) {
                dataBlockFullOffset = bytesReadOffset;
            }
            paddingBuf.clear();
            for (int i = 0; i < Math.min(len, paddingBuf.capacity()); i++) {
                paddingBuf.writeByte(BLOCK_END_PADDING_BYTES[(bytesReadOffset++ - dataBlockFullOffset)
                    % BLOCK_END_PADDING_BYTES.length]);
            }
            return paddingBuf.retain();
        }
    }

    // read ledger entries.
    private int readEntries() throws IOException {
        checkState(bytesReadOffset >= DataBlockHeaderImpl.getDataStartOffset());
        checkState(bytesReadOffset < blockSize);

        // once reach the end of entry buffer, read more, if there is more
        if (bytesReadOffset < dataBlockFullOffset
            && entriesByteBuf.isEmpty()
            && startEntryId + blockEntryCount <= ledger.getLastAddConfirmed()) {
            entriesByteBuf = readNextEntriesFromLedger(startEntryId + blockEntryCount, ENTRIES_PER_READ);
        }

        if (!entriesByteBuf.isEmpty() && bytesReadOffset + entriesByteBuf.get(0).readableBytes() <= blockSize) {
            // always read from the first ByteBuf in the list, once read all of its content remove it.
            ByteBuf entryByteBuf = entriesByteBuf.get(0);
            int ret = entryByteBuf.readUnsignedByte();
            bytesReadOffset++;

            if (entryByteBuf.readableBytes() == 0) {
                entryByteBuf.release();
                entriesByteBuf.remove(0);
                blockEntryCount++;
            }

            return ret;
        } else {
            // no space for a new entry or there are no more entries
            // set data block full, return end padding
            if (dataBlockFullOffset == blockSize) {
                dataBlockFullOffset = bytesReadOffset;
            }
            return BLOCK_END_PADDING[(bytesReadOffset++ - dataBlockFullOffset) % BLOCK_END_PADDING.length];
        }
    }

    private List<ByteBuf> readNextEntriesFromLedger(long start, long maxNumberEntries) throws IOException {
        long end = Math.min(start + maxNumberEntries - 1, ledger.getLastAddConfirmed());
        try (LedgerEntries ledgerEntriesOnce = ledger.readAsync(start, end).get()) {
            log.debug("read ledger entries. start: {}, end: {}", start, end);

            List<ByteBuf> entries = Lists.newLinkedList();

            Iterator<LedgerEntry> iterator = ledgerEntriesOnce.iterator();
            while (iterator.hasNext()) {
                LedgerEntry entry = iterator.next();
                ByteBuf buf = entry.getEntryBuffer().retain();
                int entryLength = buf.readableBytes();
                long entryId = entry.getEntryId();

                CompositeByteBuf entryBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer(2);
                ByteBuf entryHeaderBuf = PulsarByteBufAllocator.DEFAULT.buffer(ENTRY_HEADER_SIZE, ENTRY_HEADER_SIZE);

                entryHeaderBuf.writeInt(entryLength).writeLong(entryId);
                entryBuf.addComponents(true, entryHeaderBuf, buf);

                entries.add(entryBuf);
            }
            return entries;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception when get CompletableFuture<LedgerEntries>. ", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IOException(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("The given bytes are null");
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException("off=" + off + ", len=" + len + ", b.length=" + b.length);
        } else if (len == 0) {
            return 0;
        }

        int offset = off;
        int readLen = len;
        int readBytes = 0;
        // reading header
        if (dataBlockHeaderStream.available() > 0) {
            int read = dataBlockHeaderStream.read(b, off, len);
            offset += read;
            readLen -= read;
            readBytes += read;
            bytesReadOffset += read;
        }
        if (readLen == 0) {
            return readBytes;
        }

        // reading ledger entries
        if (bytesReadOffset < blockSize) {
            readLen = Math.min(readLen, blockSize - bytesReadOffset);
            ByteBuf readEntries = readEntries(readLen);
            int read = readEntries.readableBytes();
            readEntries.readBytes(b, offset, read);
            readEntries.release();
            readBytes += read;
            return readBytes;
        }

        // reached end
        return -1;
    }

    @Override
    public int read() throws IOException {
        // reading header
        if (dataBlockHeaderStream.available() > 0) {
            bytesReadOffset++;
            return dataBlockHeaderStream.read();
        }

        // reading Ledger entries.
        if (bytesReadOffset < blockSize) {
            return readEntries();
        }

        // reached end
        return -1;
    }

    @Override
    public void close() throws IOException {
        // The close method will be triggered twice in the BlobStoreManagedLedgerOffloader#offload method.
        // The stream resource used by the try-with block which will called the close
        // And through debug, writeBlobStore.uploadMultipartPart in the offload method also will trigger
        // the close method.
        // So we add the close variable to avoid release paddingBuf twice.
        if (close.compareAndSet(false, true)) {
            super.close();
            dataBlockHeaderStream.close();
            if (!entriesByteBuf.isEmpty()) {
                entriesByteBuf.forEach(buf -> buf.release());
                entriesByteBuf.clear();
            }
            paddingBuf.clear();
            paddingBuf.release();
        }
    }

    @Override
    public ReadHandle getLedger() {
        return ledger;
    }

    @Override
    public long getStartEntryId() {
        return startEntryId;
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    public int getDataBlockFullOffset() {
        return dataBlockFullOffset;
    }

    @Override
    public int getBlockEntryCount() {
        return blockEntryCount;
    }

    @Override
    public long getEndEntryId() {
        // return -1 when no entry contained
        if (blockEntryCount == 0) {
            return -1;
        }
        return startEntryId + blockEntryCount - 1;
    }

    @Override
    public int getBlockEntryBytesCount() {
        return dataBlockFullOffset - DataBlockHeaderImpl.getDataStartOffset() - ENTRY_HEADER_SIZE * blockEntryCount;
    }

    public static long getHeaderSize() {
        return DataBlockHeaderImpl.getDataStartOffset();
    }

    // Calculate the block size after uploaded `entryBytesAlreadyWritten` bytes
    public static int calculateBlockSize(int maxBlockSize, ReadHandle readHandle,
                                         long firstEntryToWrite, long entryBytesAlreadyWritten) {
        return (int) Math.min(
            maxBlockSize,
            (readHandle.getLastAddConfirmed() - firstEntryToWrite + 1) * ENTRY_HEADER_SIZE
                + (readHandle.getLength() - entryBytesAlreadyWritten)
                + DataBlockHeaderImpl.getDataStartOffset());
    }

}

