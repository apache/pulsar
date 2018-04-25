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

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.versioning.Version;
import org.apache.pulsar.broker.s3offload.OffloadIndexBlock;
import org.apache.pulsar.broker.s3offload.OffloadIndexEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffloadIndexBlockImpl implements OffloadIndexBlock {
    private static final Logger log = LoggerFactory.getLogger(OffloadIndexBlockImpl.class);

    private static final int indexMagicWord = 0xDE47DE47;

    private LedgerMetadata segmentMetadata;
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

    public static OffloadIndexBlockImpl get(LedgerMetadata metadata, List<OffloadIndexEntryImpl> entries) {
        OffloadIndexBlockImpl block = RECYCLER.get();
        block.indexEntries = Maps.newTreeMap();
        entries.forEach(entry -> block.indexEntries.putIfAbsent(entry.getEntryId(), entry));
        checkState(entries.size() == block.indexEntries.size());
        block.segmentMetadata = metadata;
        return block;
    }

    public static OffloadIndexBlockImpl get(InputStream stream) throws IOException {
        OffloadIndexBlockImpl block = RECYCLER.get();
        block.indexEntries = Maps.newTreeMap();
        block.fromStream(stream);
        return block;
    }

    public void recycle() {
        segmentMetadata = null;
        indexEntries.clear();
        indexEntries = null;
        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    @Override
    public OffloadIndexEntry getIndexEntryForEntry(long messageEntryId) {
        if(messageEntryId > segmentMetadata.getLastEntryId()) {
            log.warn("Try to get entry: {}, which beyond lastEntryId {}, return null",
                messageEntryId, segmentMetadata.getLastEntryId());
            return null;
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

    private static byte[] buildLedgerMetadataFormat(LedgerMetadata metadata) {
        checkState(metadata instanceof org.apache.bookkeeper.client.LedgerMetadata);

        org.apache.bookkeeper.client.LedgerMetadata ledgerMetadata =
            (org.apache.bookkeeper.client.LedgerMetadata) metadata;

        return ledgerMetadata.serialize();
    }

    /**
     * Get the content of the index block as InputStream.
     * Read out in format:
     *   | index_magic_header | index_block_len | index_entry_count |
     *   |segment_metadata_len | segment metadata | index entries |
     */
    @Override
    public InputStream toStream() throws IOException {
        int indexBlockLength;
        int segmentMetadataLength;
        int indexEntryCount = this.indexEntries.size();

        byte[] ledgerMetadataByte = buildLedgerMetadataFormat(this.segmentMetadata);
        segmentMetadataLength = ledgerMetadataByte.length;

        indexBlockLength = 4 /* magic header */
            + 4 /* index block length */
            + 4 /* segment metadata length */
            + 4 /* index entry count */
            + segmentMetadataLength
            + indexEntryCount * (8 + 4 + 8); /* messageEntryId + blockPartId + blockOffset */

        ByteBuf out = PooledByteBufAllocator.DEFAULT.buffer(indexBlockLength, indexBlockLength);

        out.writeInt(indexMagicWord)
            .writeInt(indexBlockLength)
            .writeInt(segmentMetadataLength)
            .writeInt(indexEntryCount);

        // write metadata
        checkState(out.writerIndex() == 16);
        out.writeBytes(ledgerMetadataByte);

        // write entries
        this.indexEntries.entrySet().forEach(entry ->
            out.writeLong(entry.getValue().getEntryId())
                .writeInt(entry.getValue().getPartId())
                .writeLong(entry.getValue().getOffset()));

        return new ByteBufInputStream(out, true);
    }

    private static LedgerMetadata parseLedgerMetadata(byte[] bytes) throws IOException {
        return org.apache.bookkeeper.client.LedgerMetadata.parseConfig(bytes, Version.ANY,
            org.apache.bookkeeper.shaded.com.google.common.base.Optional.<Long>absent());
    }

    private OffloadIndexBlock fromStream(InputStream stream) throws IOException {
        DataInputStream dis = new DataInputStream(stream);
        int magic = dis.readInt();
        checkState(magic == this.indexMagicWord);
        int indexBlockLength = dis.readInt();
        int segmentMetadataLength = dis.readInt();
        int indexEntryCount = dis.readInt();

        byte[] metadataBytes = new byte[segmentMetadataLength];

        if (segmentMetadataLength != dis.read(metadataBytes)) {
            log.error("Read ledgerMetadata from bytes failed");
            throw new IOException("Read ledgerMetadata from bytes failed");
        }
        this.segmentMetadata = parseLedgerMetadata(metadataBytes);

        for (int i = 0; i < indexEntryCount; i ++) {
            long entryId = dis.readLong();
            this.indexEntries.putIfAbsent(entryId, OffloadIndexEntryImpl.of(entryId, dis.readInt(), dis.readLong()));
        }

        return this;
    }

    @Override
    public int getIndexMagicWord() {
        return indexMagicWord;
    }

    @Override
    public void close() {
        recycle();
    }

}

