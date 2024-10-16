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
import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.NULL_LONG_PROMISE;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegmentMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.roaringbitmap.InvalidRoaringFormat;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
class ImmutableBucket extends Bucket {

    @Setter
    private List<SnapshotSegment> snapshotSegments;

    boolean merging = false;

    @Setter
    List<Long> firstScheduleTimestamps = new ArrayList<>();

    ImmutableBucket(String dispatcherName, ManagedCursor cursor, FutureUtil.Sequencer<Void> sequencer,
                    BucketSnapshotStorage storage, long startLedgerId, long endLedgerId) {
        super(dispatcherName, cursor, sequencer, storage, startLedgerId, endLedgerId);
    }

    public Optional<List<SnapshotSegment>> getSnapshotSegments() {
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
                        List<SnapshotSegmentMetadata> metadataList =
                                snapshotMetadata.getMetadataListList();

                        // Skip all already reach schedule time snapshot segments
                        int nextSnapshotEntryIndex = 0;
                        while (nextSnapshotEntryIndex < metadataList.size()
                                && metadataList.get(nextSnapshotEntryIndex).getMaxScheduleTimestamp() <= cutoffTime) {
                            nextSnapshotEntryIndex++;
                        }

                        this.setLastSegmentEntryId(metadataList.size());
                        this.recoverDelayedIndexBitMapAndNumber(nextSnapshotEntryIndex, metadataList);
                        List<Long> firstScheduleTimestamps = metadataList.stream().map(
                                SnapshotSegmentMetadata::getMinScheduleTimestamp).toList();
                        this.setFirstScheduleTimestamps(firstScheduleTimestamps);

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
                            log.warn("[{}] Failed to get bucket snapshot segment. bucketKey: {},"
                                            + " bucketId: {}, segmentEntryId: {}", dispatcherName, bucketKey(),
                                    bucketId, nextSegmentEntryId, ex);
                        }
                    }), BucketSnapshotPersistenceException.class, MaxRetryTimes)
                    .thenApply(bucketSnapshotSegments -> {
                        if (CollectionUtils.isEmpty(bucketSnapshotSegments)) {
                            return Collections.emptyList();
                        }

                        SnapshotSegment snapshotSegment =
                                bucketSnapshotSegments.get(0);
                        List<DelayedIndex> indexList = snapshotSegment.getIndexesList();
                        this.setCurrentSegmentEntryId(nextSegmentEntryId);
                        if (isRecover) {
                            this.asyncUpdateSnapshotLength();
                        }
                        return indexList;
                    });
        });
    }

    /**
     * Recover delayed index bit map and message numbers.
     * @throws InvalidRoaringFormat invalid bitmap serialization format
     */
    private void recoverDelayedIndexBitMapAndNumber(int startSnapshotIndex,
                                                    List<SnapshotSegmentMetadata> segmentMetaList) {
        delayedIndexBitMap.clear(); // cleanup dirty bm
        final var numberMessages = new MutableLong(0);
        for (int i = startSnapshotIndex; i < segmentMetaList.size(); i++) {
            for (final var entry : segmentMetaList.get(i).getDelayedIndexBitMapMap().entrySet()) {
                final var ledgerId = entry.getKey();
                final var bs = entry.getValue();
                final var sbm = new RoaringBitmap();
                try {
                    sbm.deserialize(bs.asReadOnlyByteBuffer());
                } catch (IOException e) {
                    throw new InvalidRoaringFormat(e.getMessage());
                }
                numberMessages.add(sbm.getCardinality());
                delayedIndexBitMap.compute(ledgerId, (lId, bm) -> {
                    if (bm == null) {
                        return sbm;
                    }
                    bm.or(sbm);
                    return bm;
                });
            }
        }
        // optimize bm
        delayedIndexBitMap.values().forEach(RoaringBitmap::runOptimize);
        setNumberBucketDelayedMessages(numberMessages.getValue());
    }

    CompletableFuture<List<SnapshotSegment>> getRemainSnapshotSegment() {
        int nextSegmentEntryId = currentSegmentEntryId + 1;
        if (nextSegmentEntryId > lastSegmentEntryId) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return executeWithRetry(() -> {
            return bucketSnapshotStorage.getBucketSnapshotSegment(getAndUpdateBucketId(), nextSegmentEntryId,
                    lastSegmentEntryId).whenComplete((__, ex) -> {
                if (ex != null) {
                    log.warn(
                            "[{}] Failed to get remain bucket snapshot segment, bucketKey: {},"
                                    + " nextSegmentEntryId: {}, lastSegmentEntryId: {}",
                            dispatcherName, bucketKey(), nextSegmentEntryId, lastSegmentEntryId, ex);
                }
            });
        }, BucketSnapshotPersistenceException.class, MaxRetryTimes);
    }

    CompletableFuture<Void> asyncDeleteBucketSnapshot(BucketDelayedMessageIndexStats stats) {
        long deleteStartTime = System.currentTimeMillis();
        stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.delete);
        String bucketKey = bucketKey();
        long bucketId = getAndUpdateBucketId();

        return executeWithRetry(() -> bucketSnapshotStorage.deleteBucketSnapshot(bucketId),
                BucketSnapshotPersistenceException.class, MaxRetryTimes)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("[{}] Failed to delete bucket snapshot, bucketId: {}, bucketKey: {}",
                                dispatcherName, bucketId, bucketKey, ex);

                        stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.delete);
                    } else {
                        log.info("[{}] Delete bucket snapshot finish, bucketId: {}, bucketKey: {}",
                                dispatcherName, bucketId, bucketKey);

                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.delete,
                                System.currentTimeMillis() - deleteStartTime);
                    }
                })
                .thenCompose(__ -> removeBucketCursorProperty(bucketKey));
    }

    CompletableFuture<Void> clear(BucketDelayedMessageIndexStats stats) {
        delayedIndexBitMap.clear();
        return getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).exceptionally(e -> null)
                .thenCompose(__ -> asyncDeleteBucketSnapshot(stats));
    }

    protected CompletableFuture<Long> asyncUpdateSnapshotLength() {
        long bucketId = getAndUpdateBucketId();
        return bucketSnapshotStorage.getBucketSnapshotLength(bucketId).whenComplete((length, ex) -> {
            if (ex != null) {
                log.error("[{}] Failed to get snapshot length, bucketId: {}, bucketKey: {}",
                        dispatcherName, bucketId, bucketKey(), ex);
            } else {
                setSnapshotLength(length);
            }
        });
    }
}
