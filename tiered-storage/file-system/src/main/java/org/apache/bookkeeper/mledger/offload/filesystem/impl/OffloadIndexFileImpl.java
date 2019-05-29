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
package org.apache.bookkeeper.mledger.offload.filesystem.impl;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexFile;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class OffloadIndexFileImpl implements OffloadIndexFile {
    private static final Logger log = LoggerFactory.getLogger(OffloadIndexFileImpl.class);

    private static final int INDEX_MAGIC_WORD = 0xDE47DE47;

    private LedgerMetadata ledgerMetadata;
    private long dataObjectLength;
    private int dataHeaderLength;
    private TreeMap<Long, OffloadIndexEntryImpl> indexEntries;

    private final Handle<OffloadIndexFileImpl> recyclerHandle;

    private static final Recycler<OffloadIndexFileImpl> RECYCLER = new Recycler<OffloadIndexFileImpl>() {
        @Override
        protected OffloadIndexFileImpl newObject(Handle<OffloadIndexFileImpl> handle) {
            return new OffloadIndexFileImpl(handle);
        }
    };

    private OffloadIndexFileImpl(Handle<OffloadIndexFileImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static OffloadIndexFileImpl get(LedgerMetadata metadata, long dataObjectLength,
                                           int dataHeaderLength,
                                           List<OffloadIndexEntryImpl> entries) {
        OffloadIndexFileImpl indexfile = RECYCLER.get();
        indexfile.indexEntries = Maps.newTreeMap();
        entries.forEach(entry -> indexfile.indexEntries.putIfAbsent(entry.getEntryId(), entry));
        checkState(entries.size() == indexfile.indexEntries.size());
        indexfile.ledgerMetadata = metadata;
        indexfile.dataObjectLength = dataObjectLength;
        indexfile.dataHeaderLength = dataHeaderLength;
        return indexfile;
    }

    public static Recycler<OffloadIndexFileImpl> getRecycler() {
        return RECYCLER;
    }

    public void recycle() {
        dataObjectLength = -1;
        dataHeaderLength = -1;
        ledgerMetadata = null;
        indexEntries.clear();
        indexEntries = null;
        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    @Override
    public OffloadIndexEntry getFloorIndexEntryByEntryId(long entryId) throws IOException {
        if (entryId > ledgerMetadata.getLastEntryId()) {
            log.warn("Try to get entry: {}, which beyond lastEntryId {}, return null",
                    entryId, ledgerMetadata.getLastEntryId());
            throw new IndexOutOfBoundsException("Entry index: " + entryId +
                " beyond lastEntryId: " + ledgerMetadata.getLastEntryId());
        }
        // find the greatest mapping Id whose entryId <= messageEntryId
        return this.indexEntries.floorEntry(entryId).getValue();
    }

    @Override
    public int getEntryCount() {
        return this.indexEntries.size();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return this.ledgerMetadata;
    }

    @Override
    public void writeIndexDataIntoFile(FSDataOutputStream indexFileOutputStream) throws IOException {
        int indexEntryCount = this.indexEntries.size();
        byte[] ledgerMetadataByte = buildLedgerMetadataFormat(this.ledgerMetadata);
        int ledgerMetadataLength = ledgerMetadataByte.length;

        int indexFileLength = 4 /* magic header */
                + 4 /* index file length */
                + 8 /* data object length */
                + 4/* data header length */
                + 4 /* index entry count */
                + 4 /* ledger metadata length */
                + ledgerMetadataLength
                + indexEntryCount * (8 + 4); /**/

        ByteBuf out = PooledByteBufAllocator.DEFAULT.buffer(indexFileLength, indexFileLength);

        out.writeInt(INDEX_MAGIC_WORD)
                .writeInt(indexFileLength)
                .writeLong(dataObjectLength)
                .writeInt(dataHeaderLength)
                .writeInt(indexEntryCount)
                .writeInt(ledgerMetadataLength);
        // write metadata
        out.writeBytes(ledgerMetadataByte);

        // write entries
        this.indexEntries.entrySet().forEach(entry ->
                out.writeLong(entry.getValue().getEntryId())
                        .writeInt(entry.getValue().getOffset()));
        byte[] indexBytes = new byte[out.readableBytes()];
        out.readBytes(indexBytes);
        indexFileOutputStream.write(indexBytes);
        indexFileOutputStream.close();
    }

    @Override
    public OffloadIndexFile initIndexFile(FSDataInputStream indexInputStream) throws IOException {
        int magic = indexInputStream.readInt();
        if (magic != this.INDEX_MAGIC_WORD) {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x",
                    magic, INDEX_MAGIC_WORD));
        }
        int indexFileLength = indexInputStream.readInt();
        int restBytesSize = indexFileLength - 8;
        ByteBuffer byteBuffer = ByteBuffer.allocate(restBytesSize);
        int readAbleBytesSize = indexInputStream.read(byteBuffer);
        indexInputStream.close();
        checkState(readAbleBytesSize == restBytesSize);
        byteBuffer.position(0);
        this.dataObjectLength = byteBuffer.getLong();
        this.dataHeaderLength = byteBuffer.getInt();
        int indexEntryCount = byteBuffer.getInt();
        int ledgerMetadataLength = byteBuffer.getInt();
        byte[] ledgerMetadataBytes = new byte[ledgerMetadataLength];
        byteBuffer.get(ledgerMetadataBytes);
        this.ledgerMetadata = parseLedgerMetadata(ledgerMetadataBytes);
        this.indexEntries = Maps.newTreeMap();
        for (int i = 0; i < indexEntryCount; i ++) {
            long entryId = byteBuffer.getLong();
            this.indexEntries.put(entryId, OffloadIndexEntryImpl.of(entryId, byteBuffer.getInt()));
        }
        return this;
    }

    @Override
    public long getDataObjectLength() {
        return this.dataObjectLength;
    }

    @Override
    public long getDataHeaderLength() {
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

    static private class InternalLedgerMetadata implements LedgerMetadata {
        private LedgerMetadataFormat ledgerMetadataFormat;

        private int ensembleSize;
        private int writeQuorumSize;
        private int ackQuorumSize;
        private long lastEntryId;
        private long length;
        private LedgerMetadataFormat.DigestType digestType;
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

    public static int getIndexMagicWord() {
        return INDEX_MAGIC_WORD;
    }

    @Override
    public void close() {
        recycle();
    }

}

