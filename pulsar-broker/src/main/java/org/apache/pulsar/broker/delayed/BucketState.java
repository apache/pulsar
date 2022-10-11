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
package org.apache.pulsar.broker.delayed;

import static org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTracker.DELAYED_BUCKET_KEY_PREFIX;
import static org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTracker.DELIMITER;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BucketState {

    long startLedgerId;
    long endLedgerId;

    Map<Long, BitSet> delayedIndexBitMap;

    long numberBucketDelayedMessages;

    int lastSegmentEntryId;

    int currentSegmentEntryId;

    long snapshotLength;

    long bucketId;

    volatile CompletableFuture<Long> snapshotCreateFuture;

    BucketState(long startLedgerId, long endLedgerId) {
        this(startLedgerId, endLedgerId, new HashMap<>(), -1, -1, 0, 0, -1, null);
    }

    boolean containsMessage(long ledgerId, int entryId) {
        BitSet bitSet = delayedIndexBitMap.get(ledgerId);
        if (bitSet == null) {
            return false;
        }
        return bitSet.get(entryId);
    }

    void putIndexBit(long ledgerId, long entryId) {
        delayedIndexBitMap.computeIfAbsent(ledgerId, k -> new BitSet()).set((int) entryId);
    }

    boolean removeIndexBit(long ledgerId, int entryId) {
        boolean contained = false;
        BitSet bitSet = delayedIndexBitMap.get(ledgerId);
        if (bitSet != null && bitSet.get(entryId)) {
            contained = true;
            bitSet.clear(entryId);

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
}
