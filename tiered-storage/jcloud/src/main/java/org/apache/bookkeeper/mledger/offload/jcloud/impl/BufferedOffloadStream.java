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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

@Slf4j
public class BufferedOffloadStream extends InputStream {
    static final int[] BLOCK_END_PADDING = BlockAwareSegmentInputStreamImpl.BLOCK_END_PADDING;

    private final long ledgerId;
    private final long beginEntryId;

    public BufferedOffloadStream(int blockSize, List<Entry> entries, long ledgerId, long beginEntryId) {
        this.ledgerId = ledgerId;
        this.beginEntryId = beginEntryId;
        this.endEntryId = beginEntryId;
        this.blockSize = blockSize;
        this.entryBuffer = entries;
        this.blockHead = StreamingDataBlockHeaderImpl.of(blockSize, ledgerId, beginEntryId)
                .toStream();
    }

    public long getEndEntryId() {
        return endEntryId;
    }

    private volatile long endEntryId;
    static final int ENTRY_HEADER_SIZE = 4 /* entry size */ + 8 /* entry id */;
    private final long blockSize;
    private final List<Entry> entryBuffer;
    private final InputStream blockHead;
    int offset = 0;
    static final int NOT_INITIALIZED = -1;
    int validDataOffset = NOT_INITIALIZED;
    CompositeByteBuf currentEntry;

    public long getLedgerId() {
        return ledgerId;
    }

    public long getBeginEntryId() {
        return beginEntryId;
    }

    public long getBlockSize() {
        return blockSize;
    }

    @Override
    public int read() throws IOException {
        if (blockHead.available() > 0) {
            offset++;
            return blockHead.read();
        }
        //if current exists, use current first
        if (currentEntry != null) {
            if (currentEntry.readableBytes() > 0) {
                offset += 1;
                return currentEntry.readUnsignedByte();
            } else {
                currentEntry.release();
                currentEntry = null;
            }
        }

        if (blockSize <= offset) {
            return -1;
        } else if (validDataOffset != NOT_INITIALIZED) {
            return BLOCK_END_PADDING[(offset++ - validDataOffset) % BLOCK_END_PADDING.length];
        }


        if (entryBuffer.isEmpty()) {
            validDataOffset = offset;
            return read();
        }

        Entry headEntry = entryBuffer.remove(0);

        //create new block when a ledger end
        if (headEntry.getLedgerId() != this.ledgerId) {
            throw new RuntimeException(
                    String.format("there should not be multi ledger in a block %s %s", headEntry.getLedgerId(),
                            this.ledgerId));
        }

        final int entryLength = headEntry.getLength();
        final long entryId = headEntry.getEntryId();
        CompositeByteBuf entryBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer(2);
        ByteBuf entryHeaderBuf = PulsarByteBufAllocator.DEFAULT.buffer(ENTRY_HEADER_SIZE, ENTRY_HEADER_SIZE);
        entryHeaderBuf.writeInt(entryLength).writeLong(entryId);
        entryBuf.addComponents(true, entryHeaderBuf, headEntry.getDataBuffer().retain());
        endEntryId = headEntry.getEntryId();
        headEntry.release();
        currentEntry = entryBuf;
        return read();

    }

    @Override
    public void close() throws IOException {
        blockHead.close();
    }

    public static int calculateBlockSize(int streamingBlockSize, int entryCount, int entrySize) {
        int validDataSize = (entryCount * ENTRY_HEADER_SIZE
                + entrySize
                + StreamingDataBlockHeaderImpl.getDataStartOffset());
        return Math.max(streamingBlockSize, validDataSize);
    }
}
