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
package org.apache.pulsar.broker.service.scalable;

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.common.scalable.HashRange;

/**
 * PIP-486 controller boundary-selection policy: turns a segment's entry-bucket count {@code N} into the
 * equal-width split points stored on the segment — the ascending, inclusive start hashes of buckets
 * {@code 1..N-1} (bucket 0 implicitly starts at {@code 0x0000}). {@code N = 1} is a single bucket
 * spanning the whole ring, i.e. no splits.
 *
 * <p>The entry-bucket ring is always the full {@code [0x0000, 0xFFFF]} (the low 16 bits of the key's
 * {@code Murmur3_32} hash), independent of the segment's segment-routing hash range. The split values
 * must match the producer's bucketing (a key falls in the bucket equal to the number of splits {@code <=}
 * its entry-bucket hash).
 *
 * <p>Equal-width is the initial policy; the wire format is range-based, so a future controller could
 * place arbitrary (traffic-balanced) boundaries with no wire or dispatch change.
 */
final class EntryBucketSplits {

    private EntryBucketSplits() {
    }

    /**
     * The number of entry-buckets a segment gets from a topic budget shared across its segments:
     * {@code floor(budget / segmentCount)}, but at least 1.
     */
    static int bucketsForBudget(int budget, int segmentCount) {
        return Math.max(1, budget / segmentCount);
    }

    /**
     * The per-bucket hash ranges (inclusive, over the 16-bit entry-bucket ring) defined by
     * {@code splits}. Empty splits yield a single range spanning the whole ring. The i-th range is
     * the i-th entry-bucket, so the result has {@code splits.size() + 1} elements.
     */
    static List<HashRange> ranges(List<Integer> splits) {
        if (splits.isEmpty()) {
            return List.of(HashRange.of(0, HashRange.MAX_HASH));
        }
        List<HashRange> ranges = new ArrayList<>(splits.size() + 1);
        int start = 0;
        for (int split : splits) {
            ranges.add(HashRange.of(start, split - 1));
            start = split;
        }
        ranges.add(HashRange.of(start, HashRange.MAX_HASH));
        return ranges;
    }

    /** Equal-width split points for {@code bucketCount} buckets; empty when {@code bucketCount <= 1}. */
    static List<Integer> equalWidth(int bucketCount) {
        if (bucketCount <= 1) {
            return List.of();
        }
        int ringSize = HashRange.MAX_HASH + 1;
        List<Integer> splits = new ArrayList<>(bucketCount - 1);
        for (int i = 1; i < bucketCount; i++) {
            splits.add((int) ((long) i * ringSize / bucketCount));
        }
        return splits;
    }
}
