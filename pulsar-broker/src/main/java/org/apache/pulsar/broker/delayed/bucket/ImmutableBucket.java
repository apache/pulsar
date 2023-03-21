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

import static org.apache.bookkeeper.mledger.util.Futures.executeWithRetry;
import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.AsyncOperationTimeoutSeconds;
import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.NULL_LONG_PROMISE;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

@Slf4j
class ImmutableBucket extends Bucket {

    @Setter
    private volatile List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> snapshotSegments;

    ImmutableBucket(String dispatcherName, ManagedCursor cursor,
                    BucketSnapshotStorage storage, long startLedgerId, long endLedgerId) {
        super(dispatcherName, cursor, storage, startLedgerId, endLedgerId);
    }

    public Optional<List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment>> getSnapshotSegments() {
        return Optional.ofNullable(snapshotSegments);
    }

    CompletableFuture<List<DelayedIndex>> asyncLoadNextBucketSnapshotEntry() {
        return asyncLoadNextBucketSnapshotEntry(false, null);
    }

    CompletableFuture<List<DelayedIndex>> asyncRecoverBucketSnapshotEntry(Supplier<Long> cutoffTimeSupplier) {
        return asyncLoadNextBucketSnapshotEntry(true, cutoffTimeSupplier);
    }

    private CompletableFuture<List<DelayedIndex>> asyncLoadNextBucketSnapshotEntry(boolean isRecover,
                                                                                   Supplier<Long> cutoffTimeSupplier) {
        final long bucketId = getAndUpdateBucketId();
        final CompletableFuture<Integer> loadMetaDataFuture;
        if (isRecover) {
            final long cutoffTime = cutoffTimeSupplier.get();
            // Load Metadata of bucket snapshot
            final String bucketKey = bucketKey();
            loadMetaDataFuture = executeWithRetry(() -> bucketSnapshotStorage.getBucketSnapshotMetadata(bucketId)
                    .whenComplete((___, ex) -> {
                        if (ex != null) {
                            log.warn("[{}] Failed to get bucket snapshot metadata,"
                                            + " bucketKey: {}, bucketId: {}",
                                    dispatcherName, bucketKey, bucketId, ex);
                        }
                    }), BucketSnapshotPersistenceException.class, MaxRetryTimes)
                    .thenApply(snapshotMetadata -> {
                        List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata> metadataList =
                                snapshotMetadata.getMetadataListList();

                        // Skip all already reach schedule time snapshot segments
                        int nextSnapshotEntryIndex = 0;
                        while (nextSnapshotEntryIndex < metadataList.size()
                                && metadataList.get(nextSnapshotEntryIndex).getMaxScheduleTimestamp() <= cutoffTime) {
                            nextSnapshotEntryIndex++;
                        }

                        this.setLastSegmentEntryId(metadataList.size());
                        this.recoverDelayedIndexBitMapAndNumber(nextSnapshotEntryIndex, metadataList);

                        return nextSnapshotEntryIndex + 1;
                    });
        } else {
            loadMetaDataFuture = CompletableFuture.completedFuture(currentSegmentEntryId + 1);
        }

        return loadMetaDataFuture.thenCompose(nextSegmentEntryId -> {
            if (nextSegmentEntryId > lastSegmentEntryId) {
                return CompletableFuture.completedFuture(null);
            }

            return executeWithRetry(
                    () -> bucketSnapshotStorage.getBucketSnapshotSegment(bucketId, nextSegmentEntryId,
                            nextSegmentEntryId).whenComplete((___, ex) -> {
                        if (ex != null) {
                            log.warn("[{}] Failed to get bucket snapshot segment. bucketKey: {}, bucketId: {}",
                                    dispatcherName, bucketKey(), bucketId, ex);
                        }
                    }), BucketSnapshotPersistenceException.class, MaxRetryTimes)
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
    }

    private void recoverDelayedIndexBitMapAndNumber(int startSnapshotIndex,
                                                    List<SnapshotSegmentMetadata> segmentMetadata) {
        this.delayedIndexBitMap.clear();
        MutableLong numberMessages = new MutableLong(0);
        for (int i = startSnapshotIndex; i < segmentMetadata.size(); i++) {
            Map<Long, ByteString> bitByteStringMap = segmentMetadata.get(i).getDelayedIndexBitMapMap();
            bitByteStringMap.forEach((leaderId, bitSetString) -> {
                boolean exist = this.delayedIndexBitMap.containsKey(leaderId);
                RoaringBitmap bitSet =
                        new ImmutableRoaringBitmap(bitSetString.asReadOnlyByteBuffer()).toRoaringBitmap();
                numberMessages.add(bitSet.getCardinality());
                if (!exist) {
                    this.delayedIndexBitMap.put(leaderId, bitSet);
                } else {
                    this.delayedIndexBitMap.get(leaderId).or(bitSet);
                }
            });
        }
        this.setNumberBucketDelayedMessages(numberMessages.getValue());
    }

    CompletableFuture<List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment>> getRemainSnapshotSegment() {
        int nextSegmentEntryId = currentSegmentEntryId + 1;
        if (nextSegmentEntryId > lastSegmentEntryId) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return executeWithRetry(() -> {
            return bucketSnapshotStorage.getBucketSnapshotSegment(getAndUpdateBucketId(), nextSegmentEntryId,
                    lastSegmentEntryId).whenComplete((__, ex) -> {
                if (ex != null) {
                    log.warn("[{}] Failed to get remain bucket snapshot segment, bucketKey: {}.",
                            dispatcherName, bucketKey(), ex);
                }
            });
        }, BucketSnapshotPersistenceException.class, MaxRetryTimes);
    }

    CompletableFuture<Void> asyncDeleteBucketSnapshot() {
        String bucketKey = bucketKey();
        long bucketId = getAndUpdateBucketId();
        return removeBucketCursorProperty(bucketKey).thenCompose(__ ->
                executeWithRetry(() -> bucketSnapshotStorage.deleteBucketSnapshot(bucketId),
                        BucketSnapshotPersistenceException.class, MaxRetryTimes)).whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("[{}] Failed to delete bucket snapshot, bucketId: {}, bucketKey: {}",
                                dispatcherName, bucketId, bucketKey, ex);
                    } else {
                        log.info("[{}] Delete bucket snapshot finish, bucketId: {}, bucketKey: {}",
                                 dispatcherName, bucketId, bucketKey);
                    }
        });
    }

    void clear(boolean delete) {
        delayedIndexBitMap.clear();
        if (delete) {
            final String bucketKey = bucketKey();
            try {
                getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).exceptionally(e -> null).thenCompose(__ -> {
                        if (getSnapshotCreateFuture().isPresent() && getBucketId().isEmpty()) {
                            log.error("[{}] Can't found bucketId, don't execute delete operate, bucketKey: {}",
                                    dispatcherName, bucketKey);
                            return CompletableFuture.completedFuture(null);
                        }
                        long bucketId = getAndUpdateBucketId();
                        return removeBucketCursorProperty(bucketKey()).thenAccept(___ -> {
                            executeWithRetry(() -> bucketSnapshotStorage.deleteBucketSnapshot(bucketId),
                            BucketSnapshotPersistenceException.class, MaxRetryTimes)
                            .whenComplete((____, ex) -> {
                                if (ex != null) {
                                    log.error("[{}] Failed to delete bucket snapshot, bucketId: {}, bucketKey: {}",
                                            dispatcherName, bucketId, bucketKey, ex);
                                } else {
                                    log.info("[{}] Delete bucket snapshot finish, bucketId: {}, bucketKey: {}",
                                            dispatcherName, bucketId, bucketKey);
                                }
                            });
                    });
                }).get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed to clear bucket snapshot, bucketKey: {}", bucketKey, e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException(e);
            }
        } else {
            getSnapshotCreateFuture().ifPresent(snapshotGenerateFuture -> {
                try {
                    snapshotGenerateFuture.get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.warn("[{}] Failed wait to snapshot generate, bucketId: {}, bucketKey: {}", dispatcherName,
                            getBucketId(), bucketKey());
                }
            });
        }
    }
}
