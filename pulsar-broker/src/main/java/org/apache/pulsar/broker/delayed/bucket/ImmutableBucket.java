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
import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.DELAYED_BUCKET_KEY_PREFIX;
import static org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker.NULL_LONG_PROMISE;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.LongBitmaps;

@CustomLog
class ImmutableBucket {

    static final String DELIMITER = "_";
    static final int MaxRetryTimes = 3;

    private final BucketContext ctx;

    @Getter
    private final long startLedgerId;

    @Getter
    private final long endLedgerId;

    @Getter
    @Setter
    private Map<Long, LongBitmap> delayedIndexBitMap = new Long2ObjectOpenHashMap<>();

    @Setter
    private List<SnapshotSegment> snapshotSegments;

    boolean merging = false;

    @Setter
    @Getter
    List<Long> firstScheduleTimestamps = new ArrayList<>();

    @Getter
    @Setter
    private long numberBucketDelayedMessages;

    @Getter
    @Setter
    private int lastSegmentEntryId;

    @Getter
    @Setter
    private volatile int currentSegmentEntryId;

    @Getter
    @Setter
    private volatile long snapshotLength;

    @Setter
    private volatile Long bucketId;

    @Setter
    private volatile CompletableFuture<Long> snapshotCreateFuture;

    // Set when a delete has claimed this bucket, so a late create-failure callback does not
    // requeue indexes the delete is about to remove. Volatile for visibility: the set can be
    // observed before the delete's synchronized sections run, which is harmless.
    private volatile boolean deleteStarted;

    boolean isDeleteStarted() {
        return deleteStarted;
    }

    ImmutableBucket(BucketContext ctx, long startLedgerId, long endLedgerId) {
        this.ctx = ctx;
        this.startLedgerId = startLedgerId;
        this.endLedgerId = endLedgerId;
    }

    String bucketKey() {
        return String.join(DELIMITER, DELAYED_BUCKET_KEY_PREFIX, String.valueOf(startLedgerId),
                String.valueOf(endLedgerId));
    }

    Optional<CompletableFuture<Long>> getSnapshotCreateFuture() {
        return Optional.ofNullable(snapshotCreateFuture);
    }

    Optional<Long> getBucketId() {
        return Optional.ofNullable(bucketId);
    }

    long getAndUpdateBucketId() {
        Optional<Long> bucketIdOptional = getBucketId();
        if (bucketIdOptional.isPresent()) {
            return bucketIdOptional.get();
        }

        String bucketIdStr = ctx.cursor().getCursorProperties().get(bucketKey());
        long bucketId = Long.parseLong(bucketIdStr);
        setBucketId(bucketId);
        return bucketId;
    }

    CompletableFuture<Long> asyncSaveBucketSnapshot(
            BucketDelayedMessageIndexStats stats, SnapshotMetadata snapshotMetadata,
            List<SnapshotSegment> bucketSnapshotSegments) {
        final String bucketKey = bucketKey();
        final String cursorName = Codec.decode(ctx.cursor().getName());
        final String dispatcher = ctx.dispatcherName();
        final String topicName = dispatcher.substring(0, dispatcher.lastIndexOf(" / " + cursorName));
        return deleteSupersededSnapshotLedger(bucketKey, stats)
                .thenCompose(ignored -> executeWithRetry(
                        () -> ctx.bucketSnapshotStorage().createBucketSnapshot(snapshotMetadata,
                                        bucketSnapshotSegments, bucketKey, topicName, cursorName)
                                .whenComplete((__, ex) -> {
                                    if (ex != null) {
                                        log.warn()
                                                .attr("dispatcher", dispatcher)
                                                .attr("bucketKey", bucketKey)
                                                .exception(ex)
                                                .log("Failed to create bucket snapshot");
                                    }
                                }), BucketSnapshotPersistenceException.class, MaxRetryTimes))
                .thenCompose(newBucketId -> {
                    setBucketId(newBucketId);

                    // Durable cursor publication is the success point: the superseded ledger
                    // is already deleted, so swallowing a publication failure would leave
                    // recovery unable to open any snapshot for this bucket.
                    return putBucketKeyId(bucketKey, newBucketId).thenApply(ignored -> newBucketId);
                });
    }

    /**
     * The property is the only durable reference to the superseded ledger: delete it before the
     * replacement is allocated, or fail the save so the reference survives for a retry.
     */
    private CompletableFuture<Void> deleteSupersededSnapshotLedger(String bucketKey,
                                                                   BucketDelayedMessageIndexStats stats) {
        String supersededBucketIdStr = ctx.cursor().getCursorProperties().get(bucketKey);
        if (supersededBucketIdStr == null) {
            return CompletableFuture.completedFuture(null);
        }
        final long supersededBucketId;
        try {
            supersededBucketId = Long.parseLong(supersededBucketIdStr);
        } catch (NumberFormatException e) {
            log.warn()
                    .attr("bucketKey", bucketKey)
                    .attr("bucketId", supersededBucketIdStr)
                    .log("Skipping the superseded snapshot ledger delete, the bucket id is malformed");
            return CompletableFuture.completedFuture(null);
        }
        long deleteStartTime = System.currentTimeMillis();
        stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.delete);
        return executeWithRetry(() -> ctx.bucketSnapshotStorage().deleteBucketSnapshot(supersededBucketId),
                BucketSnapshotPersistenceException.class, MaxRetryTimes)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error()
                                .attr("dispatcher", ctx.dispatcherName())
                                .attr("bucketId", supersededBucketId)
                                .attr("bucketKey", bucketKey)
                                .exception(ex)
                                .log("Failed to delete superseded bucket snapshot");
                        stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.delete);
                    } else {
                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.delete,
                                System.currentTimeMillis() - deleteStartTime);
                    }
                });
    }

    private CompletableFuture<Void> putBucketKeyId(String bucketKey, Long bucketId) {
        if (bucketId == null) {
            return FutureUtil.failedFuture(new NullPointerException("Expected bucketId should not be null"));
        }
        return ctx.sequencer().sequential(() ->
                executeWithRetry(() -> ctx.cursor().putCursorProperty(bucketKey, String.valueOf(bucketId)),
                        ManagedLedgerException.BadVersionException.class, MaxRetryTimes));
    }

    /**
     * Removes the property only while it still references the deleted ledger: the check and the
     * removal are one cursor update, so a save that publishes a replacement for the same bucket
     * key while the delete is in flight cannot lose its only durable reference.
     */
    private CompletableFuture<Void> removeBucketCursorPropertyIfStillReferences(String bucketKey,
                                                                                long bucketId) {
        return ctx.sequencer().sequential(() -> executeWithRetry(
                () -> ctx.cursor().updateCursorProperties(properties -> {
                    if (String.valueOf(bucketId).equals(properties.get(bucketKey))) {
                        properties.remove(bucketKey);
                    }
                    return properties;
                }), ManagedLedgerException.BadVersionException.class, MaxRetryTimes));
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
            loadMetaDataFuture = executeWithRetry(() -> ctx.bucketSnapshotStorage().getBucketSnapshotMetadata(bucketId)
                    .whenComplete((___, ex) -> {
                        if (ex != null) {
                            log.warn()
                                    .attr("dispatcher", ctx.dispatcherName())
                                    .attr("bucketKey", bucketKey)
                                    .attr("bucketId", bucketId)
                                    .exception(ex)
                                    .log("Failed to get bucket snapshot metadata");
                        }
                    }), BucketSnapshotPersistenceException.class, MaxRetryTimes)
                    .thenApply(snapshotMetadata -> {
                        int metadataListSize = snapshotMetadata.getMetadataListCount();

                        // Skip all already reach schedule time snapshot segments
                        int nextSnapshotEntryIndex = 0;
                        while (nextSnapshotEntryIndex < metadataListSize
                                && snapshotMetadata.getMetadataAt(nextSnapshotEntryIndex)
                                        .getMaxScheduleTimestamp() <= cutoffTime) {
                            nextSnapshotEntryIndex++;
                        }

                        this.setLastSegmentEntryId(metadataListSize);
                        this.recoverDelayedIndexBitMapAndNumber(nextSnapshotEntryIndex, snapshotMetadata);
                        List<Long> firstScheduleTimestamps = new ArrayList<>();
                        for (int i = 0; i < metadataListSize; i++) {
                            firstScheduleTimestamps.add(
                                    snapshotMetadata.getMetadataAt(i).getMinScheduleTimestamp());
                        }
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
                    () -> ctx.bucketSnapshotStorage().getBucketSnapshotSegment(bucketId, nextSegmentEntryId,
                            nextSegmentEntryId).whenComplete((___, ex) -> {
                        if (ex != null) {
                            log.warn()
                                    .attr("dispatcher", ctx.dispatcherName())
                                    .attr("bucketKey", bucketKey())
                                    .attr("bucketId", bucketId)
                                    .attr("segmentEntryId", nextSegmentEntryId)
                                    .exception(ex)
                                    .log("Failed to get bucket snapshot segment");
                        }
                    }), BucketSnapshotPersistenceException.class, MaxRetryTimes)
                    .thenCompose(bucketSnapshotSegments -> {
                        if (CollectionUtils.isEmpty(bucketSnapshotSegments)) {
                            return CompletableFuture.completedFuture(Collections.emptyList());
                        }

                        SnapshotSegment snapshotSegment =
                                bucketSnapshotSegments.get(0);
                        List<DelayedIndex> indexList = snapshotSegment.getIndexesList();
                        this.setCurrentSegmentEntryId(nextSegmentEntryId);
                        if (isRecover) {
                            return this.asyncUpdateSnapshotLength()
                                    .thenAccept(this::setSnapshotLength)
                                    .thenApply(__ -> indexList);
                        }
                        return CompletableFuture.completedFuture(indexList);
                    });
        });
    }

    /**
     * Recover delayed index bit map and message numbers.
     */
    private void recoverDelayedIndexBitMapAndNumber(int startSnapshotIndex,
                                                    SnapshotMetadata snapshotMetadata) {
        delayedIndexBitMap.clear(); // cleanup dirty bm
        final var numberMessages = new MutableLong(0);
        for (int i = startSnapshotIndex; i < snapshotMetadata.getMetadataListCount(); i++) {
            snapshotMetadata.getMetadataAt(i).forEachDelayedIndexBitMap((ledgerId, bs) -> {
                final ByteBuf buf = Unpooled.wrappedBuffer(bs);
                try {
                    final LongBitmap sbm = LongBitmaps.deserialize(buf);
                    numberMessages.add(sbm.cardinality());
                    delayedIndexBitMap.compute(ledgerId, (lId, bm) -> {
                        if (bm == null) {
                            return sbm;
                        }
                        bm.or(sbm);
                        return bm;
                    });
                } finally {
                    buf.release();
                }
            });
        }
        setNumberBucketDelayedMessages(numberMessages.longValue());
    }

    CompletableFuture<List<SnapshotSegment>> getAllSnapshotSegments() {
        if (lastSegmentEntryId < 1) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return executeWithRetry(() -> {
            return ctx.bucketSnapshotStorage().getBucketSnapshotSegment(getAndUpdateBucketId(), 1,
                    lastSegmentEntryId).whenComplete((__, ex) -> {
                if (ex != null) {
                    log.warn()
                            .attr("dispatcher", ctx.dispatcherName())
                            .attr("bucketKey", bucketKey())
                            .attr("lastSegmentEntryId", lastSegmentEntryId)
                            .exception(ex)
                            .log("Failed to get all bucket snapshot segments for merge");
                }
            });
        }, BucketSnapshotPersistenceException.class, MaxRetryTimes);
    }

    CompletableFuture<Void> asyncDeleteBucketSnapshot(BucketDelayedMessageIndexStats stats) {
        String bucketKey = bucketKey();
        deleteStarted = true;
        CompletableFuture<Optional<Long>> bucketIdFuture = resolveBucketIdForDelete();

        return bucketIdFuture.thenCompose(optionalBucketId -> {
            if (optionalBucketId.isEmpty()) {
                // A failed creation left no snapshot to delete: not a delete attempt, so no metric.
                return CompletableFuture.completedFuture(null);
            }
            long deleteStartTime = System.currentTimeMillis();
            stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.delete);
            long bucketId = optionalBucketId.get();
            return executeWithRetry(() -> ctx.bucketSnapshotStorage().deleteBucketSnapshot(bucketId),
                    BucketSnapshotPersistenceException.class, MaxRetryTimes)
                    .whenComplete((__, ex) -> {
                        if (ex != null) {
                            log.error()
                                    .attr("dispatcher", ctx.dispatcherName())
                                    .attr("bucketId", bucketId)
                                    .attr("bucketKey", bucketKey)
                                    .exception(ex)
                                    .log("Failed to delete bucket snapshot");

                            stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.delete);
                        } else {
                            log.info()
                                    .attr("dispatcher", ctx.dispatcherName())
                                    .attr("bucketId", bucketId)
                                    .attr("bucketKey", bucketKey)
                                    .log("Delete bucket snapshot finish");

                            stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.delete,
                                    System.currentTimeMillis() - deleteStartTime);
                        }
                    })
                    .thenCompose(__ -> removeBucketCursorPropertyIfStillReferences(bucketKey, bucketId));
        });
    }

    private CompletableFuture<Optional<Long>> resolveBucketIdForDelete() {
        Optional<CompletableFuture<Long>> snapshotCreateFuture = getSnapshotCreateFuture();
        if (snapshotCreateFuture.isPresent()) {
            return snapshotCreateFuture.get().handle((bucketId, ex) -> {
                if (ex != null) {
                    return Optional.empty();
                }
                setBucketId(bucketId);
                return Optional.of(bucketId);
            });
        }
        try {
            return CompletableFuture.completedFuture(Optional.of(getAndUpdateBucketId()));
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
    }

    CompletableFuture<Void> clear(BucketDelayedMessageIndexStats stats) {
        delayedIndexBitMap.clear();
        return getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).exceptionally(e -> null)
                .thenCompose(__ -> asyncDeleteBucketSnapshot(stats));
    }

    protected CompletableFuture<Long> asyncUpdateSnapshotLength() {
        long bucketId = getAndUpdateBucketId();
        return ctx.bucketSnapshotStorage().getBucketSnapshotLength(bucketId).whenComplete((length, ex) -> {
            if (ex != null) {
                log.error()
                        .attr("dispatcher", ctx.dispatcherName())
                        .attr("bucketId", bucketId)
                        .attr("bucketKey", bucketKey())
                        .exception(ex)
                        .log("Failed to get snapshot length");
            }
        });
    }
}
