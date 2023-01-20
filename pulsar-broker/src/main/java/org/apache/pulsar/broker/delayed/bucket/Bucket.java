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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
@Data
@AllArgsConstructor
abstract class Bucket {

    static final String DELAYED_BUCKET_KEY_PREFIX = "#pulsar.internal.delayed.bucket";
    static final String DELIMITER = "_";
    static final int MaxRetryTimes = 3;

    protected final ManagedCursor cursor;
    protected final BucketSnapshotStorage bucketSnapshotStorage;

    long startLedgerId;
    long endLedgerId;

    Map<Long, RoaringBitmap> delayedIndexBitMap;

    long numberBucketDelayedMessages;

    int lastSegmentEntryId;

    int currentSegmentEntryId;

    long snapshotLength;

    private volatile Long bucketId;

    private volatile CompletableFuture<Long> snapshotCreateFuture;


    Bucket(ManagedCursor cursor, BucketSnapshotStorage storage, long startLedgerId, long endLedgerId) {
        this(cursor, storage, startLedgerId, endLedgerId, new HashMap<>(), -1, -1, 0, 0, null, null);
    }

    boolean containsMessage(long ledgerId, long entryId) {
        RoaringBitmap bitSet = delayedIndexBitMap.get(ledgerId);
        if (bitSet == null) {
            return false;
        }
        return bitSet.contains(entryId, entryId + 1);
    }

    void putIndexBit(long ledgerId, long entryId) {
        delayedIndexBitMap.computeIfAbsent(ledgerId, k -> new RoaringBitmap()).add(entryId, entryId + 1);
    }

    boolean removeIndexBit(long ledgerId, long entryId) {
        boolean contained = false;
        RoaringBitmap bitSet = delayedIndexBitMap.get(ledgerId);
        if (bitSet != null && bitSet.contains(entryId, entryId + 1)) {
            contained = true;
            bitSet.remove(entryId, entryId + 1);

            if (bitSet.isEmpty()) {
                delayedIndexBitMap.remove(ledgerId);
            }

            if (numberBucketDelayedMessages > 0) {
                numberBucketDelayedMessages--;
            }
        }
        return contained;
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

        String bucketIdStr = cursor.getCursorProperties().get(bucketKey());
        long bucketId = Long.parseLong(bucketIdStr);
        setBucketId(bucketId);
        return bucketId;
    }

    CompletableFuture<Long> asyncSaveBucketSnapshot(
            ImmutableBucket bucket, DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata snapshotMetadata,
            List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> bucketSnapshotSegments) {
        final String bucketKey = bucket.bucketKey();
        return bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata, bucketSnapshotSegments, bucketKey)
                .thenCompose(newBucketId -> {
                    bucket.setBucketId(newBucketId);

                    return putBucketKeyId(bucketKey, newBucketId).exceptionally(ex -> {
                        log.warn("Failed to record bucketId to cursor property, bucketKey: {}, bucketId: {}",
                                bucketKey, bucketId);
                        return null;
                    }).thenApply(__ -> newBucketId);
                });
    }

    private CompletableFuture<Void> putBucketKeyId(String bucketKey, Long bucketId) {
        Objects.requireNonNull(bucketId);
        return executeWithRetry(() -> cursor.putCursorProperty(bucketKey, String.valueOf(bucketId)),
                ManagedLedgerException.BadVersionException.class, MaxRetryTimes);
    }

    protected CompletableFuture<Void> removeBucketCursorProperty(String bucketKey) {
        return executeWithRetry(() -> cursor.removeCursorProperty(bucketKey),
                ManagedLedgerException.BadVersionException.class, MaxRetryTimes);
    }
}
