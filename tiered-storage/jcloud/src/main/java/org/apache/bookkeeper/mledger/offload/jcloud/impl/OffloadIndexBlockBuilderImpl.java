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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
public class OffloadIndexBlockBuilderImpl implements OffloadIndexBlockBuilder {

    private LedgerMetadata ledgerMetadata;
    private long dataObjectLength;
    private long dataHeaderLength;
    private List<OffloadIndexEntryImpl> entries;
    private int lastBlockSize;

    public OffloadIndexBlockBuilderImpl() {
        this.entries = Lists.newArrayList();
    }

    @Override
    public OffloadIndexBlockBuilder withDataObjectLength(long dataObjectLength) {
        this.dataObjectLength = dataObjectLength;
        return this;
    }

    @Override
    public OffloadIndexBlockBuilder withDataBlockHeaderLength(long dataHeaderLength) {
        this.dataHeaderLength = dataHeaderLength;
        return this;
    }

    @Override
    public OffloadIndexBlockBuilder withLedgerMetadata(LedgerMetadata metadata) {
        this.ledgerMetadata = metadata;
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
    public OffloadIndexBlock fromStream(InputStream is) throws IOException {
        return OffloadIndexBlockImpl.get(is);
    }

    @Override
    public OffloadIndexBlock build() {
        checkState(ledgerMetadata != null);
        checkState(!entries.isEmpty());
        checkState(dataObjectLength > 0);
        checkState(dataHeaderLength > 0);
        return OffloadIndexBlockImpl.get(ledgerMetadata, dataObjectLength, dataHeaderLength, entries);
    }

}
