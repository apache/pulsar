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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock.IndexInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffloadIndexBlockV2Impl implements OffloadIndexBlockV2 {
    private static final Logger log = LoggerFactory.getLogger(OffloadIndexBlockImpl.class);

    private static final int INDEX_MAGIC_WORD = 0x3D1FB0BC;

    private Map<Long, LedgerInfo> segmentMetadata;
    private final Map<Long, LedgerMetadata> compatibleMetadata = Maps.newTreeMap();
    private long dataObjectLength;
    private long dataHeaderLength;
    //    private TreeMap<Long, OffloadIndexEntryImpl> indexEntries;
    private Map<Long, TreeMap<Long, OffloadIndexEntryImpl>> indexEntries;


    private final Handle<OffloadIndexBlockV2Impl> recyclerHandle;

    private static final Recycler<OffloadIndexBlockV2Impl> RECYCLER = new Recycler<OffloadIndexBlockV2Impl>() {
        @Override
        protected OffloadIndexBlockV2Impl newObject(Handle<OffloadIndexBlockV2Impl> handle) {
            return new OffloadIndexBlockV2Impl(handle);
        }
    };

    private OffloadIndexBlockV2Impl(Handle<OffloadIndexBlockV2Impl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static OffloadIndexBlockV2Impl get(Map<Long, LedgerInfo> metadata, long dataObjectLength,
                                              long dataHeaderLength,
                                              Map<Long, List<OffloadIndexEntryImpl>> entries) {
        OffloadIndexBlockV2Impl block = RECYCLER.get();
        block.indexEntries = new HashMap<>();
        entries.forEach((ledgerId, list) -> {
            final TreeMap<Long, OffloadIndexEntryImpl> inLedger = block.indexEntries
                    .getOrDefault(ledgerId, new TreeMap<>());
            list.forEach(indexEntry -> {
                inLedger.put(indexEntry.getEntryId(), indexEntry);
            });
            block.indexEntries.put(ledgerId, inLedger);
        });

        block.segmentMetadata = metadata;
        block.dataObjectLength = dataObjectLength;
        block.dataHeaderLength = dataHeaderLength;
        return block;
    }

    public static OffloadIndexBlockV2Impl get(int magic, DataInputStream stream) throws IOException {
        OffloadIndexBlockV2Impl block = RECYCLER.get();
        block.indexEntries = Maps.newTreeMap();
        block.segmentMetadata = Maps.newTreeMap();
        if (magic != INDEX_MAGIC_WORD) {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x",
                    magic, INDEX_MAGIC_WORD));
        }
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
    public OffloadIndexEntry getIndexEntryForEntry(long ledgerId, long messageEntryId) throws IOException {
        if (messageEntryId > getLedgerMetadata(ledgerId).getLastEntryId()) {
            log.warn("Try to get entry: {}, which beyond lastEntryId {}, return null",
                    messageEntryId, getLedgerMetadata(ledgerId).getLastEntryId());
            throw new IndexOutOfBoundsException("Entry index: " + messageEntryId
                    + " beyond lastEntryId: " + getLedgerMetadata(ledgerId).getLastEntryId());
        }
        // find the greatest mapping Id whose entryId <= messageEntryId
        return this.indexEntries.get(ledgerId).floorEntry(messageEntryId).getValue();
    }

    public long getStartEntryId(long ledgerId) {
        return this.indexEntries.get(ledgerId).firstEntry().getValue().getEntryId();
    }

    @Override
    public int getEntryCount() {
        int ans = 0;
        for (TreeMap<Long, OffloadIndexEntryImpl> v : this.indexEntries.values()) {
            ans += v.size();
        }

        return ans;
    }

    @Override
    public LedgerMetadata getLedgerMetadata(long ledgerId) {
        if (compatibleMetadata.containsKey(ledgerId)) {
            return compatibleMetadata.get(ledgerId);
        } else if (segmentMetadata.containsKey(ledgerId)) {
            final CompatibleMetadata result = new CompatibleMetadata(segmentMetadata.get(ledgerId));
            compatibleMetadata.put(ledgerId, result);
            return result;
        } else {
            return null;
        }
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
    public IndexInputStream toStream() throws IOException {

        int indexBlockLength = 4 /* magic header */
                + 4 /* index block length */
                + 8 /* data object length */
                + 8; /* data header length */

        Map<Long, byte[]> metaBytesMap = new HashMap<>();
        for (Map.Entry<Long, TreeMap<Long, OffloadIndexEntryImpl>> e : this.indexEntries.entrySet()) {
            Long ledgerId = e.getKey();
            TreeMap<Long, OffloadIndexEntryImpl> ledgerIndexEntries = e.getValue();
            int indexEntryCount = ledgerIndexEntries.size();
            byte[] ledgerMetadataByte = this.segmentMetadata.get(ledgerId).toByteArray();
            int segmentMetadataLength = ledgerMetadataByte.length;
            indexBlockLength += 8 /* ledger id length */
                    + 4 /* index entry count */
                    + 4 /* segment metadata length */
                    + segmentMetadataLength
                    + indexEntryCount * (8 + 4 + 8);
            metaBytesMap.put(ledgerId, ledgerMetadataByte);
        }

        ByteBuf out = PulsarByteBufAllocator.DEFAULT.buffer(indexBlockLength, indexBlockLength);

        out.writeInt(INDEX_MAGIC_WORD)
                .writeInt(indexBlockLength)
                .writeLong(dataObjectLength)
                .writeLong(dataHeaderLength);

        for (Map.Entry<Long, TreeMap<Long, OffloadIndexEntryImpl>> e : this.indexEntries.entrySet()) {
            Long ledgerId = e.getKey();
            TreeMap<Long, OffloadIndexEntryImpl> ledgerIndexEntries = e.getValue();
            int indexEntryCount = ledgerIndexEntries.size();
            byte[] ledgerMetadataByte = metaBytesMap.get(ledgerId);
            out.writeLong(ledgerId)
                    .writeInt(indexEntryCount)
                    .writeInt(ledgerMetadataByte.length)
                    .writeBytes(ledgerMetadataByte);
            ledgerIndexEntries.values().forEach(idxEntry -> {
                out.writeLong(idxEntry.getEntryId())
                        .writeInt(idxEntry.getPartId())
                        .writeLong(idxEntry.getOffset());
            });
        }

        return new IndexInputStream(new ByteBufInputStream(out, true), indexBlockLength);
    }

    private static LedgerInfo parseLedgerInfo(byte[] bytes) throws IOException {
        return LedgerInfo.newBuilder().mergeFrom(bytes).build();
    }

    private OffloadIndexBlockV2 fromStream(DataInputStream dis) throws IOException {

        dis.readInt(); // no used index block length
        this.dataObjectLength = dis.readLong();
        this.dataHeaderLength = dis.readLong();
        while (dis.available() > 0) {
            long ledgerId = dis.readLong();
            int indexEntryCount = dis.readInt();
            int segmentMetadataLength = dis.readInt();

            byte[] metadataBytes = new byte[segmentMetadataLength];

            if (segmentMetadataLength != dis.read(metadataBytes)) {
                log.error("Read ledgerMetadata from bytes failed");
                throw new IOException("Read ledgerMetadata from bytes failed");
            }
            final LedgerInfo ledgerInfo = parseLedgerInfo(metadataBytes);
            this.segmentMetadata.put(ledgerId, ledgerInfo);
            final TreeMap<Long, OffloadIndexEntryImpl> indexEntries = new TreeMap<>();

            for (int i = 0; i < indexEntryCount; i++) {
                long entryId = dis.readLong();
                indexEntries.putIfAbsent(entryId, OffloadIndexEntryImpl.of(entryId, dis.readInt(),
                        dis.readLong(), dataHeaderLength));
            }
            this.indexEntries.put(ledgerId, indexEntries);
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

    @VisibleForTesting
    static class CompatibleMetadata implements LedgerMetadata {
        LedgerInfo ledgerInfo;

        public CompatibleMetadata(LedgerInfo ledgerInfo) {
            this.ledgerInfo = ledgerInfo;
        }

        @Override
        public long getLedgerId() {
            return ledgerInfo.getLedgerId();
        }

        @Override
        public int getEnsembleSize() {
            return 0;
        }

        @Override
        public int getWriteQuorumSize() {
            return 0;
        }

        @Override
        public int getAckQuorumSize() {
            return 0;
        }

        @Override
        public long getLastEntryId() {
            return ledgerInfo.getEntries() - 1;
        }

        @Override
        public long getLength() {
            return ledgerInfo.getSize();
        }

        @Override
        public boolean hasPassword() {
            return false;
        }

        @Override
        public byte[] getPassword() {
            return new byte[0];
        }

        @Override
        public DigestType getDigestType() {
            return null;
        }

        @Override
        public long getCtime() {
            return 0;
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public Map<String, byte[]> getCustomMetadata() {
            return null;
        }

        @Override
        public List<BookieId> getEnsembleAt(long entryId) {
            return null;
        }

        @Override
        public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
            return null;
        }

        @Override
        public State getState() {
            return null;
        }

        @Override
        public String toSafeString() {
            return null;
        }

        @Override
        public int getMetadataFormatVersion() {
            return 0;
        }

        @Override
        public long getCToken() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CompatibleMetadata that = (CompatibleMetadata) o;
            return ledgerInfo.equals(that.ledgerInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ledgerInfo);
        }
    }
}

