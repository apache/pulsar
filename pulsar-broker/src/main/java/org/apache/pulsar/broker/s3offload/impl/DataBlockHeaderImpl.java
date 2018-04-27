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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.pulsar.broker.s3offload.DataBlockHeader;

/**
 *
 * The data block header in code storage for each data block.
 *
 */
public class DataBlockHeaderImpl implements DataBlockHeader {
    // Magic Word for data block.
    // It is a sequence of bytes used to identify the start of a block.
    private static final int dataBlockMagicWord = 0xDBDBDBDB;
    // This is bigger than header size. Leaving some place for alignment and future enhancement.
    // Payload use this as the start offset.
    private static final int dataBlockHeaderAlign = 1024;
    // The size of this header.
    private static final int dataBlockHeaderSize = 4 /* magic word */
        + 4 /* index block length */
        + 4 /* block entry count */
        + 8 /* first entry id */;

    public static DataBlockHeaderImpl of(int blockLength, int blockEntryCount, long firstEntryId) {
        return new DataBlockHeaderImpl(blockLength, blockEntryCount, firstEntryId);
    }

    // Construct DataBlockHeader from InputStream
    public static DataBlockHeaderImpl fromStream(InputStream stream) throws IOException {
        DataInputStream dis = new DataInputStream(stream);
        int magic = dis.readInt();
        checkState(magic == dataBlockMagicWord);
        return new DataBlockHeaderImpl(dis.readInt(), dis.readInt(), dis.readLong());
    }

    private final int blockLength;
    private final int blockEntryCount;
    private final long firstEntryId;

    @Override
    public int getBlockMagicWord() {
        return dataBlockMagicWord;
    }

    @Override
    public int getBlockLength() {
        return this.blockLength;
    }

    @Override
    public int getBlockEntryCount() {
        return this.blockEntryCount;
    }

    @Override
    public long getFirstEntryId() {
        return this.firstEntryId;
    }

    @Override
    public int getHeaderSize() {
        return dataBlockHeaderSize;
    }

    static public int getDataStartOffset() {
        return dataBlockHeaderAlign;
    }

    public DataBlockHeaderImpl(int blockLength, int blockEntryCount, long firstEntryId) {
        this.blockLength = blockLength;
        this.blockEntryCount = blockEntryCount;
        this.firstEntryId = firstEntryId;
    }

    /**
     * Get the content of the data block header as InputStream.
     * Read out in format:
     *   [ magic_word -- int ][ block_len -- int ][ block_entry_count -- int ][ first_entry_id  -- long]
     */
    public InputStream toStream() throws IOException {
        int headerSize = 4 /* magic word */
            + 4 /* index block length */
            + 4 /* block entry count */
            + 8 /* first entry id */;

        ByteBuf out = PooledByteBufAllocator.DEFAULT.buffer(headerSize, headerSize);
        out.writeInt(dataBlockMagicWord)
            .writeInt(blockLength)
            .writeInt(blockEntryCount)
            .writeLong(firstEntryId);

        return new ByteBufInputStream(out, true);
    }
}

