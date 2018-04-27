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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
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
public class BlockAwareSegmentInputStream extends ByteArrayInputStream {
    private static final Logger log = LoggerFactory.getLogger(BlockAwareSegmentInputStream.class);

    private static final byte blockEndPadding = 0xA;

    private final ReadHandle ledger;
    private final long startEntryId;
    private final int blockSize;

    private DataBlockHeader dataBlockHeader;
    // Number of Message entries that read from ledger and written to this block.
    private int blockEntryCount;
    // Number of Payload Bytes from ledger that has been written to this buffer.
    private int payloadBytesWritten;

    // ByteBuf that wrapped the buffer and use to write content into the buffer.
    private final ByteBuf writer;

    public BlockAwareSegmentInputStream(ReadHandle ledger, long startEntryId, int blockSize) {
        super(new byte[blockSize]);
        this.ledger = ledger;
        this.startEntryId = startEntryId;
        this.blockSize = blockSize;

        this.payloadBytesWritten = 0;
        this.blockEntryCount = 0;
        this.writer = Unpooled.wrappedBuffer(this.buf);
        // set writer index to the payload start offset
        writer.writerIndex(DataBlockHeaderImpl.getDataStartOffset());
    }

    @Override
    public void close() throws IOException {
        writer.release();
        super.close();
    }

    // Read entry complete, no space for one more entry, do padding and construct header, complete future
    private void entryReadComplete(CompletableFuture<Void> future) throws IOException {
        // padding at end
        IntStream.range(0, writer.writableBytes()).forEach(i -> writer.writeByte(blockEndPadding));

        // construct and write block header
        dataBlockHeader = DataBlockHeaderImpl.of(blockSize, blockEntryCount, startEntryId);
        writer.writerIndex(0);
        writer.writeBytes(dataBlockHeader.toStream(), dataBlockHeader.getHeaderSize());
        writer.writerIndex(blockSize);
        future.complete(null);
    }

    // roughly get how may entries we would read this time.
    private int readEntryCount() {
        // read some more entries than roughly computed each time.
        final int readAheadNum = 100;
        long ledgerLength = ledger.getLength();
        long lac = ledger.getLastAddConfirmed();
        int averageEntrySize = (int)(ledgerLength / (lac + 1)) + 4 + 8;
        int numEntriesToRead = writer.writableBytes() / averageEntrySize;
        return numEntriesToRead + readAheadNum;
    }

    // put the entries that read from ledger into buffer, and update counters. If buffer is full, complete the future.
    private void bufReadEntries(CompletableFuture<Void> future, LedgerEntries entries) throws IOException {
        Iterator<LedgerEntry> iterator = entries.iterator();
        int entryLength = 0;
        while (iterator.hasNext()) {
            LedgerEntry entry = iterator.next();
            long entryId = entry.getEntryId();
            entryLength = (int)entry.getLength();

            if (writer.writableBytes() >= entryLength + 4 + 8) {
                // has space for this entry, write it into buf
                writer
                    .writeInt(entryLength)
                    .writeLong(entryId)
                    .writeBytes(entry.getEntryBuffer());
                // set counters
                blockEntryCount ++;
                payloadBytesWritten += entryLength;
                entry.close();
            } else {
                // buf has no space left for a whole message entry
                entry.close();
                if (iterator.hasNext()) {
                    iterator.forEachRemaining(LedgerEntry::close);
                }
                // padding and write header
                entryReadComplete(future);
                return;
            }
        }

        // not have read enough entries, read more.
        checkState(!iterator.hasNext());
        if (writer.writableBytes() > 4 + 8) {
            readLedgerEntries(future, startEntryId + blockEntryCount, readEntryCount());
        } else {
            entryReadComplete(future);
        }
    }

    // read `number` entries start from `start`, if buffer is full, bufReadEntries will complete the future.
    private void readLedgerEntries(CompletableFuture<Void> future, long start, int number) {
        ledger.readAsync(start, Math.min(start + number - 1, ledger.getLastAddConfirmed()))
            .whenComplete((ledgerEntries, exception) -> {
            try {
                if (exception != null) {
                    log.error("Meet exception in readAsync.", exception);
                    this.close();
                    future.completeExceptionally(exception);
                    return;
                }

                bufReadEntries(future, ledgerEntries);
            } catch (Exception ioe) {
                log.error("Meet exception while read entries.", ioe);
                future.completeExceptionally(ioe);
            }
        });
    }

    // initialize and fill data from ReadHandle
    public CompletableFuture<Void> initialize() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        readLedgerEntries(future, startEntryId, readEntryCount());
        return future;
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

    public int getPayloadBytesWritten() {
        return payloadBytesWritten;
    }

    public byte getBlockEndPadding() {
        return blockEndPadding;
    }

}

