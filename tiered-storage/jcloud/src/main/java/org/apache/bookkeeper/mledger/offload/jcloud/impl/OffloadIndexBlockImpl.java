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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.mledger.offload.OffloadUtils.buildLedgerMetadataFormat;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static OffloadIndexBlockImpl get(int magic, DataInputStream stream) throws IOException {
        if (magic != INDEX_MAGIC_WORD) {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x",
                    magic, INDEX_MAGIC_WORD));
        }
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
            throw new IndexOutOfBoundsException("Entry index: " + messageEntryId
                + " beyond lastEntryId: " + segmentMetadata.getLastEntryId());
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

    private static class InternalLedgerMetadata implements LedgerMetadata {

        private int ensembleSize;
        private int writeQuorumSize;
        private int ackQuorumSize;
        private long lastEntryId;
        private long length;
        private DataFormats.LedgerMetadataFormat.DigestType digestType;
        private long ctime;
        private State state;
        private Map<String, byte[]> customMetadata = Maps.newHashMap();
        private TreeMap<Long, ArrayList<BookieId>> ensembles =
                new TreeMap<>();

        InternalLedgerMetadata(LedgerMetadataFormat ledgerMetadataFormat) {
            this.ensembleSize = ledgerMetadataFormat.getEnsembleSize();
            this.writeQuorumSize = ledgerMetadataFormat.getQuorumSize();
            this.ackQuorumSize = ledgerMetadataFormat.getAckQuorumSize();
            this.lastEntryId = ledgerMetadataFormat.getLastEntryId();
            this.length = ledgerMetadataFormat.getLength();
            this.digestType = ledgerMetadataFormat.getDigestType();
            this.ctime = ledgerMetadataFormat.getCtime();
            this.state = org.apache.bookkeeper.client.api.LedgerMetadata.State.valueOf(
                    ledgerMetadataFormat.getState().toString());

            if (ledgerMetadataFormat.getCustomMetadataCount() > 0) {
                ledgerMetadataFormat.getCustomMetadataList().forEach(
                        entry -> this.customMetadata.put(entry.getKey(), entry.getValue().toByteArray()));
            }

            ledgerMetadataFormat.getSegmentList().forEach(segment -> {
                ArrayList<BookieId> addressArrayList = new ArrayList<>();
                segment.getEnsembleMemberList().forEach(address -> {
                    try {
                        addressArrayList.add(BookieId.parse(address));
                    } catch (IllegalArgumentException e) {
                        log.error("Exception when create BookieSocketAddress. ", e);
                    }
                });
                this.ensembles.put(segment.getFirstEntryId(), addressArrayList);
            });
        }

        @Override
        public long getLedgerId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getEnsembleSize() {
            return this.ensembleSize;
        }

        @Override
        public int getWriteQuorumSize() {
            return this.writeQuorumSize;
        }

        @Override
        public int getAckQuorumSize() {
            return this.ackQuorumSize;
        }

        @Override
        public long getLastEntryId() {
            return this.lastEntryId;
        }

        @Override
        public long getLength() {
            return this.length;
        }

        @Override
        public DigestType getDigestType() {
            switch (this.digestType) {
                case HMAC:
                    return DigestType.MAC;
                case CRC32:
                    return DigestType.CRC32;
                case CRC32C:
                    return DigestType.CRC32C;
                case DUMMY:
                    return DigestType.DUMMY;
                default:
                    throw new IllegalArgumentException("Unable to convert digest type " + digestType);
            }
        }

        @Override
        public long getCtime() {
            return this.ctime;
        }

        @Override
        public boolean isClosed() {
            return this.state == State.CLOSED;
        }

        @Override
        public Map<String, byte[]> getCustomMetadata() {
            return this.customMetadata;
        }

        @Override
        public List<BookieId> getEnsembleAt(long entryId) {
            return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
        }

        @Override
        public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
            return this.ensembles;
        }

        @Override
        public long getCToken() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int getMetadataFormatVersion() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public byte[] getPassword() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public State getState() {
            return this.state;
        }

        @Override
        public boolean hasPassword() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public String toSafeString() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private static LedgerMetadata parseLedgerMetadata(byte[] bytes) throws IOException {
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.mergeFrom(bytes);
        return new InternalLedgerMetadata(builder.build());
    }

    private OffloadIndexBlock fromStream(DataInputStream dis) throws IOException {
        dis.readInt(); // no used index block length
        this.dataObjectLength = dis.readLong();
        this.dataHeaderLength = dis.readLong();
        int indexEntryCount = dis.readInt();
        int segmentMetadataLength = dis.readInt();

        byte[] metadataBytes = new byte[segmentMetadataLength];
        dis.readFully(metadataBytes);
        this.segmentMetadata = parseLedgerMetadata(metadataBytes);

        for (int i = 0; i < indexEntryCount; i++) {
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

