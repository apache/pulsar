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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.ToString;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

/**
 * Interface for offloading ledgers to long-term storage.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface LedgerOffloader {

    @ToString
    class OffloadResult {
        public final long beginLedger;
        public final long beginEntry;
        public final long endLedger;
        public final long endEntry;

        public OffloadResult(long beginLedger, long beginEntry, long endLedger, long endEntry) {
            this.beginLedger = beginLedger;
            this.beginEntry = beginEntry;
            this.endLedger = endLedger;
            this.endEntry = endEntry;
        }
    }

    /**
     * Used to store driver info, buffer entries, mark progress, etc.
     * Create one per segment.
     */
    interface OffloadHandle {
        enum OfferEntryResult {
            SUCCESS,
            FAIL_BUFFER_FULL,
            FAIL_SEGMENT_CLOSED,
            FAIL_NOT_CONSECUTIVE
        }

        Position lastOffered();

        CompletableFuture<Position> lastOfferedAsync();

        /**
         * The caller should manually release entry no matter what the offer result is.
         */
        OfferEntryResult offerEntry(Entry entry);

        CompletableFuture<OfferEntryResult> offerEntryAsync(Entry entry);

        CompletableFuture<OffloadResult> getOffloadResultAsync();

        /**
         * Manually close current offloading segment
         * @return true if the segment is not already closed
         */
        boolean close();

        default CompletableFuture<Boolean> AsyncClose() {
            return CompletableFuture.completedFuture(close());
        }
    }

    // TODO: improve the user metadata in subsequent changes
    String METADATA_SOFTWARE_VERSION_KEY = "S3ManagedLedgerOffloaderSoftwareVersion";
    String METADATA_SOFTWARE_GITSHA_KEY = "S3ManagedLedgerOffloaderSoftwareGitSha";

    /**
     * Get offload driver name.
     *
     * @return offload driver name.
     */
    String getOffloadDriverName();

    /**
     * Get offload driver metadata.
     *
     * <p>The driver metadata will be recorded as part of the metadata of the original ledger.
     *
     * @return offload driver metadata.
     */
    default Map<String, String> getOffloadDriverMetadata() {
        return Collections.emptyMap();
    }

    /**
     * Offload the passed in ledger to longterm storage.
     * Metadata passed in is for inspection purposes only and should be stored
     * alongside the ledger data.
     *
     * When the returned future completes, the ledger has been persisted to the
     * longterm storage, so it is safe to delete the original copy in bookkeeper.
     *
     * The uid is used to identify an attempt to offload. The implementation should
     * use this to deterministically generate a unique name for the offloaded object.
     * This uid will be stored in the managed ledger metadata before attempting the
     * call to offload(). If a subsequent or concurrent call to offload() finds
     * a uid in the metadata, it will attempt to cleanup this attempt with a call
     * to #deleteOffloaded(ReadHandle,UUID). Once the offload attempt completes,
     * the managed ledger will update its metadata again, to record the completion,
     * ensuring that subsequent calls will not attempt to offload the same ledger
     * again.
     *
     * @param ledger the ledger to offload
     * @param uid unique id to identity this offload attempt
     * @param extraMetadata metadata to be stored with the offloaded ledger for informational
     *                      purposes
     * @return a future, which when completed, denotes that the offload has been successful.
     */
    CompletableFuture<Void> offload(ReadHandle ledger,
                                    UUID uid,
                                    Map<String, String> extraMetadata);

    /**
     * Begin offload the passed in ledgers to longterm storage, it will finish
     * when a segment reached it's size or time.
     * Should only be called once for a LedgerOffloader instance.
     * Metadata passed in is for inspection purposes only and should be stored
     * alongside the segment data.
     *
     * When the returned OffloaderHandle.getOffloadResultAsync completes, the corresponding
     * ledgers has been persisted to the
     * longterm storage, so it is safe to delete the original copy in bookkeeper.
     *
     * The uid is used to identify an attempt to offload. The implementation should
     * use this to deterministically generate a unique name for the offloaded object.
     * This uid will be stored in the managed ledger metadata before attempting the
     * call to streamingOffload(). If a subsequent or concurrent call to streamingOffload() finds
     * a uid in the metadata, it will attempt to cleanup this attempt with a call
     * to #deleteOffloaded(ReadHandle,UUID). Once the offload attempt completes,
     * the managed ledger will update its metadata again, to record the completion,
     * ensuring that subsequent calls will not attempt to offload the same ledger
     * again.
     *
     * @return an OffloaderHandle, which when `completeFuture()` completed, denotes that the offload has been successful.
     */
    default CompletableFuture<OffloadHandle> streamingOffload(ManagedLedger ml, UUID uid, long beginLedger,
                                                              long beginEntry,
                                                              Map<String, String> driverMetadata) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a ReadHandle which can be used to read a ledger back from longterm
     * storage.
     *
     * The passed uid, will be match the uid of a previous successful call to
     * #offload(ReadHandle,UUID,Map).
     *
     * @param ledgerId the ID of the ledger to load from longterm storage
     * @param uid unique ID for previous successful offload attempt
     * @param offloadDriverMetadata offload driver metadata
     * @return a future, which when completed, returns a ReadHandle
     */
    CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                Map<String, String> offloadDriverMetadata);

    /**
     * Delete a ledger from long term storage.
     *
     * The passed uid, will be match the uid of a previous call to
     * #offload(ReadHandle,UUID,Map), which may or may not have been successful.
     *
     * @param ledgerId the ID of the ledger to delete from longterm storage
     * @param uid unique ID for previous offload attempt
     * @param offloadDriverMetadata offload driver metadata
     * @return a future, which when completed, signifies that the ledger has
     *         been deleted
     */
    CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                                            Map<String, String> offloadDriverMetadata);

    default CompletableFuture<ReadHandle> readOffloaded(long ledgerId, MLDataFormats.OffloadContext ledgerContext,
                                                        Map<String, String> offloadDriverMetadata) {
        throw new UnsupportedOperationException();
    }

    default CompletableFuture<Void> deleteOffloaded(UUID uid,
                                                    Map<String, String> offloadDriverMetadata) {
        throw new UnsupportedOperationException();
    }

    /**
     * Get offload policies of this LedgerOffloader
     *
     * @return offload policies
     */
    OffloadPoliciesImpl getOffloadPolicies();

    /**
     * Close the resources if necessary
     */
    void close();
}

