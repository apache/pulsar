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
package org.apache.bookkeeper.mledger;

import com.google.common.annotations.Beta;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.impl.OffloadIndexBlockBuilderImpl;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
@Beta
public interface OffloadIndexBlockBuilder {

    /**
     * Build index block with the passed in ledger metadata.
     *
     * @param metadata the ledger metadata
     */
    OffloadIndexBlockBuilder withMetadata(LedgerMetadata metadata);


    /**
     * Add one payload block related information into index block.
     * It contains the first entryId in payload block, the payload block Id,
     * and payload block size.
     * This information will be used to consist one index entry in OffloadIndexBlock.
     *
     * @param firstEntryId the first entryId in payload block
     * @param partId the payload block Id
     * @param blockSize the payload block size
     */
    OffloadIndexBlockBuilder addBlock(long firstEntryId, int partId, int blockSize);

    /**
     * Finalize the immutable OffloadIndexBlock
     */
    OffloadIndexBlock build();

    static OffloadIndexBlockBuilder create() {
        return new OffloadIndexBlockBuilderImpl();
    }
}
