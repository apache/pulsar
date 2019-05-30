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

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.net.BookieSocketAddress;
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

    private static byte[] buildLedgerMetadataFormat(LedgerMetadata metadata) {
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(metadata.getWriteQuorumSize())
            .setAckQuorumSize(metadata.getAckQuorumSize())
            .setEnsembleSize(metadata.getEnsembleSize())
            .setLength(metadata.getLength())
            .setState(metadata.isClosed() ? LedgerMetadataFormat.State.CLOSED : LedgerMetadataFormat.State.OPEN)
            .setLastEntryId(metadata.getLastEntryId())
            .setCtime(metadata.getCtime())
            .setDigestType(BookKeeper.DigestType.toProtoDigestType(
                BookKeeper.DigestType.fromApiDigestType(metadata.getDigestType())));

        for (Map.Entry<String, byte[]> e : metadata.getCustomMetadata().entrySet()) {
            builder.addCustomMetadataBuilder()
                .setKey(e.getKey()).setValue(ByteString.copyFrom(e.getValue()));
        }

        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> e : metadata.getAllEnsembles().entrySet()) {
            builder.addSegmentBuilder()
                .setFirstEntryId(e.getKey())
                .addAllEnsembleMember(e.getValue().stream().map(a -> a.toString()).collect(Collectors.toList()));
        }

        return builder.build().toByteArray();
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

    static private class InternalLedgerMetadata implements LedgerMetadata {
        private LedgerMetadataFormat ledgerMetadataFormat;

        private int ensembleSize;
        private int writeQuorumSize;
        private int ackQuorumSize;
        private long lastEntryId;
        private long length;
        private DataFormats.LedgerMetadataFormat.DigestType digestType;
        private long ctime;
        private byte[] password;
        private State state;
        private Map<String, byte[]> customMetadata = Maps.newHashMap();
        private TreeMap<Long, ArrayList<BookieSocketAddress>> ensembles = new TreeMap<Long, ArrayList<BookieSocketAddress>>();

        InternalLedgerMetadata(LedgerMetadataFormat ledgerMetadataFormat) {
            this.ensembleSize = ledgerMetadataFormat.getEnsembleSize();
            this.writeQuorumSize = ledgerMetadataFormat.getQuorumSize();
            this.ackQuorumSize = ledgerMetadataFormat.getAckQuorumSize();
            this.lastEntryId = ledgerMetadataFormat.getLastEntryId();
            this.length = ledgerMetadataFormat.getLength();
            this.digestType = ledgerMetadataFormat.getDigestType();
            this.ctime = ledgerMetadataFormat.getCtime();
            this.state = State.CLOSED;
            this.password = ledgerMetadataFormat.getPassword().toByteArray();

            if (ledgerMetadataFormat.getCustomMetadataCount() > 0) {
                ledgerMetadataFormat.getCustomMetadataList().forEach(
                    entry -> this.customMetadata.put(entry.getKey(), entry.getValue().toByteArray()));
            }

            ledgerMetadataFormat.getSegmentList().forEach(segment -> {
                ArrayList<BookieSocketAddress> addressArrayList = new ArrayList<BookieSocketAddress>();
                segment.getEnsembleMemberList().forEach(address -> {
                    try {
                        addressArrayList.add(new BookieSocketAddress(address));
                    } catch (IOException e) {
                        log.error("Exception when create BookieSocketAddress. ", e);
                    }
                });
                this.ensembles.put(segment.getFirstEntryId(), addressArrayList);
            });
        }

        @Override
        public boolean hasPassword() { return true; }

        @Override
        public byte[] getPassword() { return password; }

        @Override
        public State getState() { return state; }

        @Override
        public int getMetadataFormatVersion() { return 2; }

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
        public List<BookieSocketAddress> getEnsembleAt(long entryId) {
            return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
        }

        @Override
        public NavigableMap<Long, ? extends List<BookieSocketAddress>> getAllEnsembles() {
            return this.ensembles;
        }

        @Override
        public String toSafeString() {
            return toString();
        }
    }

    private static LedgerMetadata parseLedgerMetadata(byte[] bytes) throws IOException {
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.mergeFrom(bytes);
        return new InternalLedgerMetadata(builder.build());
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

