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
package org.apache.pulsar.broker.s3offload.impl;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.pulsar.broker.s3offload.DataBlockHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * The BlockAwareSegmentInputStream for each cold storage data block.
 * It contains a byte buffer, which contains all the content for this data block.
 *      DataBlockHeader + entries(each with format[[entry_size -- int][entry_id -- long][entry_data]])
 *
 */
public class BlockAwareSegmentInputStream extends InputStream {
    private static final Logger log = LoggerFactory.getLogger(BlockAwareSegmentInputStream.class);

    private static final int blockEndPaddingInt = 0xFEDCDEAD;
    private static final byte[] blockEndPadding = Ints.toByteArray(blockEndPaddingInt);

    private final ReadHandle ledger;
    private final long startEntryId;
    private final int blockSize;

    // Number of Message entries that read from ledger and been readout from this InputStream.
    private int blockEntryCount;
    // Number of payload Bytes read from ledger, and has been has been kept in this InputStream.
    private int payloadBytesHave;
    // Number of bytes that has been kept in this InputStream.
    private int blockBytesHave;

    // tracking read status for both header and entries.
    // Bytes that already been read from this InputStream
    private int bytesReadOffset = 0;
    // Byte from this index is all padding byte
    private int dataBlockFullOffset;

    private DataBlockHeader dataBlockHeader;
    private static int dataBlockHeaderLength = DataBlockHeaderImpl.getDataStartOffset();
    private InputStream dataBlockHeaderBuf;

    // how many entries want to read from ReadHandle each time.
    private static final int entriesNumberEachRead = 100;
    // buf the entry size and entry id.
    private static final int entryHeaderSize = 4 /* entry size*/ + 8 /* entry id */;
    private static final int entryContentSize = 1000;
    // the start offset in data block for this time read from ReadHandle.
    private int entryStartBlockOffset;
    // the end offset in data block for this time read from ReadHandle.
    private int entryEndBlockOffset;
    private byte[] entriesHead = new byte[entriesNumberEachRead * (entryHeaderSize + entryContentSize)];
    private ByteBuf entriesHeadBuf = null;

    // read ledger entries.
    private int readEntries() throws IOException {
        checkState(bytesReadOffset >= dataBlockHeaderLength);
        checkState(bytesReadOffset < dataBlockFullOffset);
        checkState(ledger != null);

        try {
            // once reach the end of entry buffer, start a new read.
            if (bytesReadOffset == blockBytesHave) {
                readLedgerEntriesOnce();
                if (log.isDebugEnabled()) {
                    log.debug("After readLedgerEntriesOnce: bytesReadOffset: {}, blockBytesHave: {}",
                        bytesReadOffset, blockBytesHave);
                }
            }

            int index = bytesReadOffset - entryStartBlockOffset;
            int ret = entriesHead[index];
            bytesReadOffset++;

            return ret;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception when get CompletableFuture<LedgerEntries>. ", e);
            throw new IOException(e);
        }
    }

    // read entries from ledger, and pre-handle the entry headers.
    private void readLedgerEntriesOnce() throws InterruptedException, ExecutionException {
        checkState(bytesReadOffset == blockBytesHave);

        long start = startEntryId + blockEntryCount;
        long end = Math.min(start + entriesNumberEachRead - 1, ledger.getLastAddConfirmed());
        LedgerEntries ledgerEntriesOnce = ledger.readAsync(start, end).get();
        if (entriesHeadBuf != null) {
            entriesHeadBuf.release();
            entriesHeadBuf = null;
        }
        entriesHeadBuf = Unpooled.wrappedBuffer(entriesHead);
        entriesHeadBuf.writerIndex(0);

        if (log.isDebugEnabled()) {
            log.debug("read ledger entries. start: {}, end: {}", start, end);
        }

        Iterator<LedgerEntry> iterator = ledgerEntriesOnce.iterator();
        long entryId = start;
        while (iterator.hasNext()) {
            LedgerEntry entry = iterator.next();
            int entryLength = (int)entry.getLength();
            entryId = entry.getEntryId();

            if (blockSize - blockBytesHave >= entryLength + entryHeaderSize) {
                // data block has space for this entry, keep this entry
                entriesHeadBuf.writeInt(entryLength)
                    .writeLong(entryId)
                    .writeBytes(entry.getEntryBuffer());

                // set counters
                blockEntryCount ++;
                payloadBytesHave += entryLength;
                blockBytesHave += entryLength + entryHeaderSize;
            } else {
                // data block has no space left for a whole message entry
                dataBlockFullOffset = blockBytesHave;
                break;
            }
        }

        ledgerEntriesOnce.close();

        if (entryId == ledger.getLastAddConfirmed()) {
            // last data block, and have read all the entries, the blockSize should equals to the size written now.
            checkState(blockSize == blockBytesHave);
        }

        entryStartBlockOffset = bytesReadOffset;
        entryEndBlockOffset = blockBytesHave;
    }


    public BlockAwareSegmentInputStream(ReadHandle ledger, long startEntryId, int blockSize) {
        this.ledger = ledger;
        this.startEntryId = startEntryId;
        this.blockSize = blockSize;
        this.dataBlockHeader = DataBlockHeaderImpl.of(blockSize, startEntryId);
        this.payloadBytesHave = 0;
        this.blockEntryCount = 0;
        this.dataBlockFullOffset = blockSize;
        this.entryStartBlockOffset = DataBlockHeaderImpl.getDataStartOffset();
        this.entryEndBlockOffset = entryStartBlockOffset;
    }
    
    // read DataBlockHeader.
    private int readDataBlockHeader() throws IOException {
        checkState(bytesReadOffset < dataBlockHeaderLength);

        blockBytesHave = dataBlockHeaderLength;
        if (bytesReadOffset < dataBlockHeader.getHeaderSize()) {
            // reading header content.
            if (dataBlockHeaderBuf == null) {
                checkState(bytesReadOffset == 0);
                dataBlockHeaderBuf = dataBlockHeader.toStream();
            }
            int ret =  dataBlockHeaderBuf.read();
            if (bytesReadOffset == dataBlockHeader.getHeaderSize()) {
                dataBlockHeaderBuf.close();
            }
            bytesReadOffset++;
            return ret;
        } else {
            // reading header padding part, return 0.
            bytesReadOffset++;
            return 0;
        }
    }

    @Override
    public int read() throws IOException {
        // reading header
        if (bytesReadOffset < dataBlockHeaderLength) {
            return readDataBlockHeader();
        }

        // reading Ledger entries.
        if (bytesReadOffset < dataBlockFullOffset) {
            return readEntries();
        }

        // read padding
        if (bytesReadOffset < blockSize) {
            return blockEndPadding[(bytesReadOffset++ - dataBlockFullOffset) % 4];
        }

        // reached end
        return -1;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    public ReadHandle getLedger() {
        return ledger;
    }

    public long getStartEntryId() {
        return startEntryId;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public DataBlockHeader getDataBlockHeader() {
        return dataBlockHeader;
    }

    public int getBlockEntryCount() {
        return blockEntryCount;
    }

    public long getLastEntryIdWritten() {
        return startEntryId + blockEntryCount - 1;
    }

    public int getPayloadBytesHave() {
        return payloadBytesHave;
    }

    public byte[] getBlockEndPadding() {
        return blockEndPadding;
    }

}

