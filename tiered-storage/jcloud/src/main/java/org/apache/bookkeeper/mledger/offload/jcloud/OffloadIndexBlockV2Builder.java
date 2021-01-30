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
package org.apache.bookkeeper.mledger.offload.jcloud;

import java.io.IOException;
import java.io.InputStream;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffloadIndexBlockV2BuilderImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
@Unstable
@LimitedPrivate
public interface OffloadIndexBlockV2Builder {

    /**
     * Build index block with the passed in ledger metadata.
     *
     * @param ledgerId
     * @param metadata the ledger metadata
     */
    OffloadIndexBlockV2Builder addLedgerMeta(Long ledgerId, LedgerInfo metadata);

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
    OffloadIndexBlockV2Builder addBlock(long ledgerId, long firstEntryId, int partId, int blockSize);

    /**
     * Specify the length of data object this index is associated with.
     * @param dataObjectLength the length of the data object
     */
    OffloadIndexBlockV2Builder withDataObjectLength(long dataObjectLength);

    /**
     * Specify the length of the block headers in the data object.
     * @param dataHeaderLength the length of the headers
     */
    OffloadIndexBlockV2Builder withDataBlockHeaderLength(long dataHeaderLength);

    /**
     * Finalize the immutable OffloadIndexBlock.
     */
    OffloadIndexBlockV2 buildV2();

    /**
     * Construct OffloadIndex from an InputStream.
     */
    OffloadIndexBlockV2 fromStream(InputStream is) throws IOException;

    /**
     * create an OffloadIndexBlockBuilder.
     */
    static OffloadIndexBlockV2Builder create() {
        return new OffloadIndexBlockV2BuilderImpl();
    }
}
