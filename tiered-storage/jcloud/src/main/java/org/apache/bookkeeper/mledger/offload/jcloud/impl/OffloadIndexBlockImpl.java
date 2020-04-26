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

import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.TreeMap;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.mledger.offload.OffloadUtils.buildLedgerMetadataFormat;
import static org.apache.bookkeeper.mledger.offload.OffloadUtils.parseLedgerMetadata;

public class OffloadIndexBlockImpl implements OffloadIndexBlock {
    private static final Logger log = LoggerFactory.getLogger(OffloadIndexBlockImpl.class);

    private static final int INDEX_MAGIC_WORD = 0xDE47DE47;

    private LedgerMetadata segmentMetadata;
    private long dataObjectLength;
    private long dataHeaderLength;
    private TreeMap<Long, OffloadIndexEntryImpl> indexEntries;

    private final Handle<OffloadIndexBlockImpl> recyclerHandle;

    private static final Recycler<OffloadIndexBlockImpl> RECYCLER = new Recycler<OffloadIndexBlockImpl>() {
        @Override
        protected OffloadIndexBlockImpl newObject(Recycler.Handle<OffloadIndexBlockImpl> handle) {
            return new OffloadIndexBlockImpl(handle);
        }
    };

    private OffloadIndexBlockImpl(Handle<OffloadIndexBlockImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static OffloadIndexBlockImpl get(LedgerMetadata metadata, long dataObjectLength,
                                            long dataHeaderLength,
                                            List<OffloadIndexEntryImpl> entries) {
        OffloadIndexBlockImpl block = RECYCLER.get();
        block.indexEntries = Maps.newTreeMap();
        entries.forEach(entry -> block.indexEntries.putIfAbsent(entry.getEntryId(), entry));
        checkState(entries.size() == block.indexEntries.size());
        block.segmentMetadata = metadata;
        block.dataObjectLength = dataObjectLength;
        block.dataHeaderLength = dataHeaderLength;
        return block;
    }

    public static OffloadIndexBlockImpl get(InputStream stream) throws IOException {
        OffloadIndexBlockImpl block = RECYCLER.get();
        block.indexEntries = Maps.newTreeMap();
        block.fromStream(stream);
        return block;
    }

    public void recycle() {
        dataObjectLength = -1;
        dataHeaderLength = -1;
        segmentMetadata = null;
        indexEntries.clear();
        indexEntries = null;
        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    @Override
    public OffloadIndexEntry getIndexEntryForEntry(long messageEntryId) throws IOException {
        if (messageEntryId > segmentMetadata.getLastEntryId()) {
            log.warn("Try to get entry: {}, which beyond lastEntryId {}, return null",
                messageEntryId, segmentMetadata.getLastEntryId());
            throw new IndexOutOfBoundsException("Entry index: " + messageEntryId +
                " beyond lastEntryId: " + segmentMetadata.getLastEntryId());
        }
        // find the greatest mapping Id whose entryId <= messageEntryId
        return this.indexEntries.floorEntry(messageEntryId).getValue();
    }

    @Override
    public int getEntryCount() {
        return this.indexEntries.size();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return this.segmentMetadata;
    }

    @Override
    public long getDataObjectLength() {
        return this.dataObjectLength;
    }

    @Override
    public long getDataBlockHeaderLength() {
        return this.dataHeaderLength;
    }

    /**
     * Get the content of the index block as InputStream.
     * Read out in format:
     *   | index_magic_header | index_block_len | data_object_len | data_header_len |
     *   | index_entry_count  | segment_metadata_len | segment metadata | index entries... |
     */
    @Override
    public OffloadIndexBlock.IndexInputStream toStream() throws IOException {
        int indexEntryCount = this.indexEntries.size();
        byte[] ledgerMetadataByte = buildLedgerMetadataFormat(this.segmentMetadata);
        int segmentMetadataLength = ledgerMetadataByte.length;

        int indexBlockLength = 4 /* magic header */
            + 4 /* index block length */
            + 8 /* data object length */
            + 8 /* data header length */
            + 4 /* index entry count */
            + 4 /* segment metadata length */
            + segmentMetadataLength
            + indexEntryCount * (8 + 4 + 8); /* messageEntryId + blockPartId + blockOffset */

        ByteBuf out = PulsarByteBufAllocator.DEFAULT.buffer(indexBlockLength, indexBlockLength);

        out.writeInt(INDEX_MAGIC_WORD)
            .writeInt(indexBlockLength)
            .writeLong(dataObjectLength)
            .writeLong(dataHeaderLength)
            .writeInt(indexEntryCount)
            .writeInt(segmentMetadataLength);
        // write metadata
        out.writeBytes(ledgerMetadataByte);

        // write entries
        this.indexEntries.entrySet().forEach(entry ->
            out.writeLong(entry.getValue().getEntryId())
                .writeInt(entry.getValue().getPartId())
                .writeLong(entry.getValue().getOffset()));

        return new OffloadIndexBlock.IndexInputStream(new ByteBufInputStream(out, true), indexBlockLength);
    }

    private OffloadIndexBlock fromStream(InputStream stream) throws IOException {
        DataInputStream dis = new DataInputStream(stream);
        int magic = dis.readInt();
        if (magic != this.INDEX_MAGIC_WORD) {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x",
                                                magic, INDEX_MAGIC_WORD));
        }
        int indexBlockLength = dis.readInt();
        this.dataObjectLength = dis.readLong();
        this.dataHeaderLength = dis.readLong();
        int indexEntryCount = dis.readInt();
        int segmentMetadataLength = dis.readInt();

        byte[] metadataBytes = new byte[segmentMetadataLength];

        if (segmentMetadataLength != dis.read(metadataBytes)) {
            log.error("Read ledgerMetadata from bytes failed");
            throw new IOException("Read ledgerMetadata from bytes failed");
        }
        this.segmentMetadata = parseLedgerMetadata(metadataBytes);

        for (int i = 0; i < indexEntryCount; i ++) {
            long entryId = dis.readLong();
            this.indexEntries.putIfAbsent(entryId, OffloadIndexEntryImpl.of(entryId, dis.readInt(),
                                                                            dis.readLong(), dataHeaderLength));
        }

        return this;
    }

    public static int getIndexMagicWord() {
        return INDEX_MAGIC_WORD;
    }

    @Override
    public void close() {
        recycle();
    }

}

