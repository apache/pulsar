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

import com.google.common.io.CountingInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.DataBlockHeader;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * The data block header in tiered storage for each data block.
 */
public class StreamingDataBlockHeaderImpl implements DataBlockHeader {
    // Magic Word for streaming data block.
    // It is a sequence of bytes used to identify the start of a block.
    static final int MAGIC_WORD = 0x26A66D32;
    // This is bigger than header size. Leaving some place for alignment and future enhancement.
    // Payload use this as the start offset.
    public static final int HEADER_MAX_SIZE = 128;
    private static final int HEADER_BYTES_USED = 4 /* magic */
            + 8 /* header len */
            + 8 /* block len */
            + 8 /* first entry id */
            + 8 /* ledger id */;
    private static final byte[] PADDING = new byte[HEADER_MAX_SIZE - HEADER_BYTES_USED];

    public long getLedgerId() {
        return ledgerId;
    }

    private final long ledgerId;

    public static StreamingDataBlockHeaderImpl of(int blockLength, long ledgerId, long firstEntryId) {
        return new StreamingDataBlockHeaderImpl(HEADER_MAX_SIZE, blockLength, ledgerId, firstEntryId);
    }

    private final long headerLength;
    private final long blockLength;
    private final long firstEntryId;

    public static int getBlockMagicWord() {
        return MAGIC_WORD;
    }

    public static int getDataStartOffset() {
        return HEADER_MAX_SIZE;
    }

    @Override
    public long getBlockLength() {
        return this.blockLength;
    }

    @Override
    public long getHeaderLength() {
        return this.headerLength;
    }

    @Override
    public long getFirstEntryId() {
        return this.firstEntryId;
    }

    public StreamingDataBlockHeaderImpl(long headerLength, long blockLength, long ledgerId, long firstEntryId) {
        this.headerLength = headerLength;
        this.blockLength = blockLength;
        this.firstEntryId = firstEntryId;
        this.ledgerId = ledgerId;
    }

    // Construct DataBlockHeader from InputStream, which contains `HEADER_MAX_SIZE` bytes readable.
    public static StreamingDataBlockHeaderImpl fromStream(InputStream stream) throws IOException {
        CountingInputStream countingStream = new CountingInputStream(stream);
        DataInputStream dis = new DataInputStream(countingStream);
        int magic = dis.readInt();
        if (magic != MAGIC_WORD) {
            throw new IOException("Data block header magic word not match. read: " + magic
                    + " expected: " + MAGIC_WORD);
        }

        long headerLen = dis.readLong();
        long blockLen = dis.readLong();
        long firstEntryId = dis.readLong();
        long ledgerId = dis.readLong();
        long toSkip = headerLen - countingStream.getCount();
        if (dis.skip(toSkip) != toSkip) {
            throw new EOFException("Header was too small");
        }

        return new StreamingDataBlockHeaderImpl(headerLen, blockLen, ledgerId, firstEntryId);
    }

    /**
     * Get the content of the data block header as InputStream.
     * Read out in format:
     *   [ magic_word -- int ][ block_len -- int ][ first_entry_id  -- long] [padding zeros]
     */
    @Override
    public InputStream toStream() {
        ByteBuf out = PulsarByteBufAllocator.DEFAULT.buffer(HEADER_MAX_SIZE, HEADER_MAX_SIZE);
        out.writeInt(MAGIC_WORD)
                .writeLong(headerLength)
                .writeLong(blockLength)
                .writeLong(firstEntryId)
                .writeLong(ledgerId)
                .writeBytes(PADDING);

        // true means the input stream will release the ByteBuf on close
        return new ByteBufInputStream(out, true);
    }

    @Override
    public String toString() {
        return String.format("StreamingDataBlockHeader(len:%d,hlen:%d,firstEntry:%d,ledger:%d)",
                blockLength, headerLength, firstEntryId, ledgerId);
    }
}

