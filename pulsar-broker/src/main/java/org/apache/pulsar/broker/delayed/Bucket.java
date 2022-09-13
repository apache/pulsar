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
import com.google.protobuf.ByteString;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata;

@Data
@AllArgsConstructor
public class Bucket {

    long startLedgerId;
    long endLedgerId;

    Map<Long, BitSet> delayedIndexBitMap;

    long numberBucketDelayedMessages;

    int lastSegmentEntryId;

    int currentSegmentEntryId;

    long snapshotLength;

    boolean active;

    volatile CompletableFuture<Long> snapshotCreateFuture;

    Bucket(long startLedgerId, long endLedgerId, Map<Long, BitSet> delayedIndexBitMap) {
        this(startLedgerId, endLedgerId, delayedIndexBitMap, -1, -1, 0, 0, true, null);
    }

    long covertDelayIndexMapAndCount(int startSnapshotIndex, List<SnapshotSegmentMetadata> segmentMetadata) {
        delayedIndexBitMap.clear();
        MutableLong numberMessages = new MutableLong(0);
        for (int i = startSnapshotIndex; i < segmentMetadata.size(); i++) {
            Map<Long, ByteString> bitByteStringMap = segmentMetadata.get(i).getDelayedIndexBitMapMap();
            bitByteStringMap.forEach((k, v) -> {
                boolean exist = delayedIndexBitMap.containsKey(k);
                byte[] bytes = v.toByteArray();
                BitSet bitSet = BitSet.valueOf(bytes);
                numberMessages.add(bitSet.cardinality());
                if (!exist) {
                    delayedIndexBitMap.put(k, bitSet);
                } else {
                    delayedIndexBitMap.get(k).or(bitSet);
                }
            });
        }
        return numberMessages.longValue();
    }

    boolean containsMessage(long ledgerId, int entryId) {
        if (delayedIndexBitMap == null) {
            return false;
        }

        BitSet bitSet = delayedIndexBitMap.get(ledgerId);
        if (bitSet == null) {
            return false;
        }
        return bitSet.get(entryId);
    }

    void putIndexBit(long ledgerId, long entryId) {
        if (entryId < Integer.MAX_VALUE) {
            delayedIndexBitMap.compute(ledgerId, (k, v) -> new BitSet()).set((int) entryId);
        }
    }

    boolean removeIndexBit(long ledgerId, int entryId) {
        if (delayedIndexBitMap == null) {
            return false;
        }

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
