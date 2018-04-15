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
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.Beta;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.OffloadIndexEntry;

@Beta
@Data
@Slf4j
public class OffloadIndexBlockImpl implements OffloadIndexBlock {

    private static final int indexMagicWord = 1000;

    private LedgerMetadata segmentMetadata;
    private TreeSet<OffloadIndexEntryImpl> indexEntries;

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
        block.indexEntries = Sets.newTreeSet();
        entries.forEach(entry -> block.indexEntries.add(entry));
        checkState(entries.size() == block.indexEntries.size());
        block.segmentMetadata = metadata;
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
    public OffloadIndexEntry getEntry(long messageEntryId) {
        if(messageEntryId > segmentMetadata.getLastEntryId()) {
            log.warn("Try to get entry: {}, which beyond lastEntryId {}, return null",
                messageEntryId, segmentMetadata.getLastEntryId());
            return null;
        }
        // find the greatest mapping Id whose entryId <= messageEntryId
        return this.indexEntries.floor(OffloadIndexEntryImpl.builder().entryId(messageEntryId).build());
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
     * Get the content of the index block.
     * Read out in format:
     *   | index_magic_header | index_block_len | index_entry_count |
     *   |segment_metadata_len | segment metadata | index entries |
     */
    @Override
    public ByteBuf readOut() throws IOException {
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
            + indexEntryCount * (8 + 4 + 8);

        ByteBuf out = Unpooled.buffer(indexBlockLength, indexBlockLength);

        out.writeInt(indexMagicWord)
            .writeInt(indexBlockLength)
            .writeInt(segmentMetadataLength)
            .writeInt(indexEntryCount);

        // write metadata
        checkState(out.writerIndex() == 16);
        out.writeBytes(ledgerMetadataByte);

        // write entries
        this.indexEntries.forEach(entry ->
            out.writeLong(entry.getEntryId())
                .writeInt(entry.getPartId())
                .writeLong(entry.getOffset()));

        return out;
    }

    @Override
    public void close() {
        recycle();
    }

}

