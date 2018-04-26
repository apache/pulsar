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
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.apache.pulsar.broker.s3offload.DataBlockHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * The BlockAwareSegmentInputStream for each cold storage data block.
 * It contains a byte buffer, which contains all the content for this data block.
 * DataBlockHeader + entries(each with format[[entry_size -- int][entry_id -- long][entry_data]])
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

    // initialize and fill data from ReadHandle
    public CompletableFuture<Void> initialize() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        ledger.readAsync(startEntryId, ledger.getLastAddConfirmed()).whenComplete((ledgerEntries, exception) -> {
            try {
                if (exception != null) {
                    this.close();
                    future.completeExceptionally(exception);
                    return;
                }

                Iterator<LedgerEntry> iterator = ledgerEntries.iterator();
                while (iterator.hasNext()) {
                    LedgerEntry entry = iterator.next();
                    int entryLength = (int)entry.getLength();
                    long entryId = entry.getEntryId();

                    if (writer.writableBytes() >= entryLength + 4 + 8) {
                        writer
                            .writeInt(entryLength)
                            .writeLong(entryId)
                            .writeBytes(entry.getEntryBuffer());
                        // set counters
                        blockEntryCount ++;
                        payloadBytesWritten += entryLength;
                        entry.close();
                    } else {
                        // this block has no space left for a whole message entry
                        entry.close();
                        break;
                    }
                }

                iterator.forEachRemaining(LedgerEntry::close);
                IntStream.range(0, writer.writableBytes()).forEach(i -> writer.writeByte(blockEndPadding));

                // construct and write block header
                dataBlockHeader = DataBlockHeaderImpl.of(blockSize, blockEntryCount, startEntryId);
                writer.writerIndex(0);
                writer.writeBytes(dataBlockHeader.toStream(), dataBlockHeader.getHeaderSize());
                writer.writerIndex(blockSize);

                future.complete(null);
            } catch (Exception ioe) {
                future.completeExceptionally(ioe);
            }
        });

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

