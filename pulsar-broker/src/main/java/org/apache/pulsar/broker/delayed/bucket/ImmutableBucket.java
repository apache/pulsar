/*
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
package org.apache.pulsar.broker.delayed.bucket;

import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.AsyncOperationTimeoutSeconds;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;

@Slf4j
class ImmutableBucket extends Bucket {
    ImmutableBucket(ManagedCursor cursor, BucketSnapshotStorage storage, long startLedgerId, long endLedgerId) {
        super(cursor, storage, startLedgerId, endLedgerId);
    }

    /**
     * Asynchronous load next bucket snapshot entry.
     * @param isRecover whether used to recover bucket snapshot
     * @return CompletableFuture
     */
    CompletableFuture<List<DelayedIndex>> asyncLoadNextBucketSnapshotEntry(boolean isRecover) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Load next bucket snapshot data, bucket: {}", cursor.getName(), this);
        }

        // Wait bucket snapshot create finish
        CompletableFuture<Void> snapshotCreateFuture =
                getSnapshotCreateFuture().orElseGet(() -> CompletableFuture.completedFuture(null))
                        .thenApply(__ -> null);

        return snapshotCreateFuture.thenCompose(__ -> {
            final long bucketId = getAndUpdateBucketId();
            CompletableFuture<Integer> loadMetaDataFuture = new CompletableFuture<>();
            if (isRecover) {
                // TODO Recover bucket snapshot
            } else {
                loadMetaDataFuture.complete(currentSegmentEntryId + 1);
            }

            return loadMetaDataFuture.thenCompose(nextSegmentEntryId -> {
                if (nextSegmentEntryId > lastSegmentEntryId) {
                    // TODO Delete bucket snapshot
                    return CompletableFuture.completedFuture(null);
                }

                return bucketSnapshotStorage.getBucketSnapshotSegment(bucketId, nextSegmentEntryId, nextSegmentEntryId)
                        .thenApply(bucketSnapshotSegments -> {
                            if (CollectionUtils.isEmpty(bucketSnapshotSegments)) {
                                return Collections.emptyList();
                            }

                            DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment snapshotSegment =
                                    bucketSnapshotSegments.get(0);
                            List<DelayedMessageIndexBucketSnapshotFormat.DelayedIndex> indexList =
                                    snapshotSegment.getIndexesList();
                            this.setCurrentSegmentEntryId(nextSegmentEntryId);
                            return indexList;
                        });
            });
        });
    }

    void clear(boolean delete) {
        delayedIndexBitMap.clear();
        getSnapshotCreateFuture().ifPresent(snapshotGenerateFuture -> {
            if (delete) {
                snapshotGenerateFuture.cancel(true);
                // TODO delete bucket snapshot
            } else {
                try {
                    snapshotGenerateFuture.get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.warn("Failed wait to snapshot generate, bucketId: {}, bucketKey: {}", getBucketId(),
                            bucketKey());
                }
            }
        });
    }
}
