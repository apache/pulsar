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
package org.apache.bookkeeper.mledger.offload.filesystem;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.mledger.offload.filesystem.impl.OffloadIndexFileBuilderImpl;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
@Unstable
@LimitedPrivate
public interface OffloadIndexFileBuilder {

    /**
     * Add the index entry into index file.
     *
     * @param entryId the if of entry
     * @param haveWrittenSize  the size of storage file have written
     */
    OffloadIndexFileBuilder addIndex(long entryId, int haveWrittenSize);

    /**
     * The path of index file
     * @param indexFilePath The path of index file
     */
    OffloadIndexFileBuilder withIndexFilePath(String indexFilePath);

    /**
     * Build index block with the passed in ledger metadata.
     *
     * @param metadata the ledger metadata
     */
    OffloadIndexFileBuilder withLedgerMetadata(LedgerMetadata metadata);

    /**
     * Specify the length of data object this index is associated with.
     * @param dataObjectLength the length of the data object
     */
    OffloadIndexFileBuilder withDataObjectLength(long dataObjectLength);

    /**
     * Specify the length of the block headers in the data object.
     * @param dataHeaderLength the length of the headers
     */
    OffloadIndexFileBuilder withDataHeaderLength(int dataHeaderLength);

    /**
     * Finalize the immutable OffloadIndexFile
     */
    OffloadIndexFile build();

    /**
     * create an OffloadIndexBlockBuilder
     */
    static OffloadIndexFileBuilder create() {
        return new OffloadIndexFileBuilderImpl();
    }


}
