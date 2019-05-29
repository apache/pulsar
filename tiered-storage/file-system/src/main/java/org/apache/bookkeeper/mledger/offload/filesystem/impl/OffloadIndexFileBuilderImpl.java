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

import com.google.common.collect.Lists;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexFile;
import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexFileBuilder;


import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
public class OffloadIndexFileBuilderImpl implements OffloadIndexFileBuilder {

    private LedgerMetadata ledgerMetadata;
    private long dataObjectLength;
    private int dataHeaderLength;
    private List<OffloadIndexEntryImpl> entries;

    public OffloadIndexFileBuilderImpl() {
        this.entries = Lists.newArrayList();
    }

    @Override
    public OffloadIndexFileBuilder withDataObjectLength(long dataObjectLength) {
        this.dataObjectLength = dataObjectLength;
        return this;
    }

    @Override
    public OffloadIndexFileBuilder withDataHeaderLength(int dataHeaderLength) {
        this.dataHeaderLength = dataHeaderLength;
        return this;
    }

    @Override
    public OffloadIndexFileBuilder withLedgerMetadata(LedgerMetadata metadata) {
        this.ledgerMetadata = metadata;
        return this;
    }

    @Override
    public OffloadIndexFileBuilder addIndex(long entryId, int haveWrittenSize) {
        checkState(dataHeaderLength > 0);

        // we should added one by one.
        int offset;
        if (entryId == 0) {
            checkState(entries.size() == 0);
            offset = dataHeaderLength;
        } else {
            checkState(entries.size() > 0);
            offset = haveWrittenSize;
        }

        this.entries.add(OffloadIndexEntryImpl.of(entryId, offset));
        return this;
    }

    @Override
    public OffloadIndexFile build() {
        checkState(ledgerMetadata != null);
        checkState(!entries.isEmpty());
        checkState(dataObjectLength > 0);
        checkState(dataHeaderLength > 0);
        return OffloadIndexFileImpl.get(ledgerMetadata, dataObjectLength, dataHeaderLength, entries);
    }

}
