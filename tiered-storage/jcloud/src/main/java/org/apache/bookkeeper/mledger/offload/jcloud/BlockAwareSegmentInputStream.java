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

import java.io.InputStream;
import org.apache.bookkeeper.client.api.ReadHandle;

/**
 * The BlockAwareSegmentInputStream for each cold storage data block.
 * This interface should be implemented while extends InputStream.
 * It gets data from ledger, and will be read out the content for a data block.
 * DataBlockHeader + entries(each with format[[entry_size -- int][entry_id -- long][entry_data]]) + padding
 */
public abstract class BlockAwareSegmentInputStream extends InputStream {
    /**
     * Get the ledger, from which this InputStream read data.
     */
    public abstract ReadHandle getLedger();

    /**
     * Get start entry id contained in this InputStream.
     *
     * @return the start entry id
     */
    public abstract long getStartEntryId();

    /**
     * Get block size that could read out from this InputStream.
     *
     * @return the block size
     */
    public abstract int getBlockSize();

    /**
     * Get entry count that read out from this InputStream.
     *
     * @return the block entry count
     */
    public abstract int getBlockEntryCount();

    /**
     * Get end entry id contained in this InputStream.
     *
     * @return the end entry id
     */
    public abstract long getEndEntryId();

    /**
     * Get sum of entries data size read from the this InputStream.
     *
     * @return the block entry bytes count
     */
    public abstract int getBlockEntryBytesCount();
}
