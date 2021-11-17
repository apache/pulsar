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
import com.google.common.collect.Lists;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2Builder;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
public class OffloadIndexBlockV2BuilderImpl implements OffloadIndexBlockBuilder, OffloadIndexBlockV2Builder {

    private final Map<Long, LedgerInfo> ledgerMetadataMap;
    private LedgerMetadata ledgerMetadata;
    private long dataObjectLength;
    private long dataHeaderLength;
    private List<OffloadIndexEntryImpl> entries;
    private int lastBlockSize;
    private int lastStreamingBlockSize;
    private long streamingOffset = 0;
    private final SortedMap<Long, List<OffloadIndexEntryImpl>> entryMap = new TreeMap<>();


    public OffloadIndexBlockV2BuilderImpl() {
        this.entries = Lists.newArrayList();
        this.ledgerMetadataMap = new HashMap<>();
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl withDataObjectLength(long dataObjectLength) {
        this.dataObjectLength = dataObjectLength;
        return this;
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl withDataBlockHeaderLength(long dataHeaderLength) {
        this.dataHeaderLength = dataHeaderLength;
        return this;
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl withLedgerMetadata(LedgerMetadata metadata) {
        this.ledgerMetadata = metadata;
        return this;
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl addLedgerMeta(Long ledgerId, LedgerInfo metadata) {
        this.ledgerMetadataMap.put(ledgerId, metadata);
        return this;
    }

    @Override
    public OffloadIndexBlockBuilder addBlock(long firstEntryId, int partId, int blockSize) {
        checkState(dataHeaderLength > 0);

        // we should added one by one.
        long offset;
        if (firstEntryId == 0) {
            checkState(entries.size() == 0);
            offset = 0;
        } else {
            checkState(entries.size() > 0);
            offset = entries.get(entries.size() - 1).getOffset() + lastBlockSize;
        }
        lastBlockSize = blockSize;

        this.entries.add(OffloadIndexEntryImpl.of(firstEntryId, partId, offset, dataHeaderLength));
        return this;
    }

    @Override
    public OffloadIndexBlockV2Builder addBlock(long ledgerId, long firstEntryId, int partId, int blockSize) {
        checkState(dataHeaderLength > 0);

        streamingOffset = streamingOffset + lastStreamingBlockSize;
        lastStreamingBlockSize = blockSize;

        final List<OffloadIndexEntryImpl> list = entryMap.getOrDefault(ledgerId, new LinkedList<>());
        list.add(OffloadIndexEntryImpl.of(firstEntryId, partId, streamingOffset, dataHeaderLength));
        entryMap.put(ledgerId, list);
        return this;
    }

    @Override
    public OffloadIndexBlockV2 fromStream(InputStream is) throws IOException {
        final DataInputStream dataInputStream = new DataInputStream(is);
        final int magic = dataInputStream.readInt();
        if (magic == OffloadIndexBlockImpl.getIndexMagicWord()) {
            return OffloadIndexBlockImpl.get(magic, dataInputStream);
        } else if (magic == OffloadIndexBlockV2Impl.getIndexMagicWord()) {
            return OffloadIndexBlockV2Impl.get(magic, dataInputStream);
        } else {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x or 0x%x",
                    magic, OffloadIndexBlockImpl.getIndexMagicWord(),
                    OffloadIndexBlockV2Impl.getIndexMagicWord()));
        }
    }

    @Override
    public OffloadIndexBlock build() {
        checkState(ledgerMetadata != null);
        checkState(!entries.isEmpty());
        checkState(dataObjectLength > 0);
        checkState(dataHeaderLength > 0);
        return OffloadIndexBlockImpl.get(ledgerMetadata, dataObjectLength, dataHeaderLength, entries);
    }

    @Override
    public OffloadIndexBlockV2 buildV2() {
        checkState(!ledgerMetadataMap.isEmpty());
        checkState(true);
        checkState(!entryMap.isEmpty());
        checkState(dataObjectLength > 0);
        checkState(dataHeaderLength > 0);
        return OffloadIndexBlockV2Impl.get(ledgerMetadataMap, dataObjectLength, dataHeaderLength, entryMap);
    }

}
