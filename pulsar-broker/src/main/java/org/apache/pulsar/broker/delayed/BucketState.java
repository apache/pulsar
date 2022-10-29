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
package org.apache.pulsar.broker.delayed;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.roaringbitmap.RoaringBitmap;

@Data
@AllArgsConstructor
public class BucketState {

    public static final String DELAYED_BUCKET_KEY_PREFIX = "#pulsar.internal.delayed.bucket";

    public static final String DELIMITER = "_";

    long startLedgerId;
    long endLedgerId;

    Map<Long, RoaringBitmap> delayedIndexBitMap;

    long numberBucketDelayedMessages;

    int lastSegmentEntryId;

    int currentSegmentEntryId;

    long snapshotLength;

    private volatile Long bucketId;

    private volatile CompletableFuture<Long> snapshotCreateFuture;

    BucketState(long startLedgerId, long endLedgerId) {
        this(startLedgerId, endLedgerId, new HashMap<>(), -1, -1, 0, 0, null, null);
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

    public String bucketKey() {
        return String.join(DELIMITER, DELAYED_BUCKET_KEY_PREFIX, String.valueOf(startLedgerId),
                String.valueOf(endLedgerId));
    }

    public Optional<CompletableFuture<Long>> getSnapshotCreateFuture() {
        return Optional.ofNullable(snapshotCreateFuture);
    }

    public Optional<Long> getBucketId() {
        return Optional.ofNullable(bucketId);
    }
}
