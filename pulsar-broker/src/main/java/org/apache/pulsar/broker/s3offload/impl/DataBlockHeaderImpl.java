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
    private static final int dataBlockHeaderAlign = 128;
    private static final byte padding = 0;
    // The size of this header.
    private static final int dataBlockHeaderSize = 4 /* magic word */
        + 4 /* index block length */
        + 8 /* first entry id */;

    public static DataBlockHeaderImpl of(int blockLength, long firstEntryId) {
        return new DataBlockHeaderImpl(blockLength, firstEntryId);
    }

    // Construct DataBlockHeader from InputStream, which contains `dataBlockHeaderAlign` bytes readable.
    public static DataBlockHeader fromStream(InputStream stream) throws IOException {
        DataInputStream dis = new DataInputStream(stream);
        int magic = dis.readInt();
        if (magic != dataBlockMagicWord) {
            throw new IOException("Data block header magic word not match.");
        }

        int blockLen = dis.readInt();
        long firstEntryId = dis.readLong();

        // verify padding
        for (int index = 0; index < dataBlockHeaderAlign - dataBlockHeaderSize; index ++) {
            if (dis.readByte() != padding) {
                throw new IOException("Data block header magic word not match.");
            }
        };

        return new DataBlockHeaderImpl(blockLen, firstEntryId);
    }

    private final int blockLength;
    private final long firstEntryId;

    static public int getBlockMagicWord() {
        return dataBlockMagicWord;
    }

    static public int getDataStartOffset() {
        return dataBlockHeaderAlign;
    }

    @Override
    public int getBlockLength() {
        return this.blockLength;
    }

    @Override
    public long getFirstEntryId() {
        return this.firstEntryId;
    }

    @Override
    public int getHeaderSize() {
        return dataBlockHeaderAlign;
    }

    public DataBlockHeaderImpl(int blockLength, long firstEntryId) {
        this.blockLength = blockLength;
        this.firstEntryId = firstEntryId;
    }

    /**
     * Get the content of the data block header as InputStream.
     * Read out in format:
     *   [ magic_word -- int ][ block_len -- int ][ first_entry_id  -- long] [padding zeros]
     */
    @Override
    public InputStream toStream() throws IOException {
        int headerSize = 4 /* magic word */
            + 4 /* index block length */
            + 8 /* first entry id */;

        ByteBuf out = PooledByteBufAllocator.DEFAULT.buffer(dataBlockHeaderAlign, dataBlockHeaderAlign);
        out.writeInt(dataBlockMagicWord)
            .writeInt(blockLength)
            .writeLong(firstEntryId)
            .writeBytes(new byte[dataBlockHeaderAlign - headerSize]);

        // true means the input stream will release the ByteBuf on close
        return new ByteBufInputStream(out, true);
    }
}

