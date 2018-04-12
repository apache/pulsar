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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.client.api.ReadHandle;

/**
 * Interface for offloading ledgers to longterm storage
 */
@Beta
public interface LedgerOffloader {
    /**
     * Offload the passed in ledger to longterm storage.
     * Metadata passed in is for inspection purposes only and should be stored
     * alongside the ledger data.
     *
     * When the returned future completes, the ledger has been persisted to the
     * loadterm storage, so it is safe to delete the original copy in bookkeeper.
     *
     * The returned futures completes with a opaque byte[] which contains context
     * information required to identify the ledger in the longterm storage. This
     * context is stored alongside the ledger info in the managed ledger metadata.
     * It is passed to #readOffloaded(byte[]) to read back the ledger.
     *
     * @param ledger the ledger to offload
     * @param extraMetadata metadata to be stored with the ledger for informational
     *                      purposes
     * @return a future, which when completed, returns a context byte[] to identify
     *         the stored ledger
     */
    CompletableFuture<byte[]> offload(ReadHandle ledger,
                                      Map<String, String> extraMetadata);

    /**
     * Create a ReadHandle which can be used to read a ledger back from longterm
     * storage.
     *
     * The passed offloadContext should be a byte[] that has previously been received
     * from a call to #offload(ReadHandle,Map).
     *
     * @param ledgerId the ID of the ledger to load from longterm storage
     * @param offloadContext a context that identifies the ledger in longterm storage
     * @return a future, which when completed, returns a ReadHandle
     */
    CompletableFuture<ReadHandle> readOffloaded(long ledgerId, byte[] offloadContext);

    /**
     * Delete a ledger from long term storage.
     *
     * The passed offloadContext should be a byte[] that has previously been received
     * from a call to #offload(ReadHandle,Map).
     *
     * @param ledgerId the ID of the ledger to delete from longterm storage
     * @param offloadContext a context that identifies the ledger in longterm storage
     * @return a future, which when completed, signifies that the ledger has
     *         been deleted
     */
    CompletableFuture<Void> deleteOffloaded(long ledgerId, byte[] offloadContext);
}

