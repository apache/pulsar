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
package org.apache.pulsar.broker.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Range;

/**
 * PIP-486: assigns a scalable-topic segment's entry-buckets to the connected consumers.
 *
 * <p>The bucket boundaries are fixed for the selector's life (a segment's bucketing is immutable —
 * every consumer of the subscription declares the identical boundary list at subscribe time, see
 * {@code KeySharedMeta.entryBucketDispatch}). The selector's only moving part is membership: the
 * {@code N} buckets are spread <b>deterministically</b> over the connected consumers — consumers
 * sorted by name (id as tiebreak), consumer {@code j} of {@code k} owning the contiguous bucket
 * slice {@code [j*N/k, (j+1)*N/k)} — so every broker computes the same spread from the same
 * membership, and a membership change moves as few whole buckets as possible.
 *
 * <p>Every hash this selector sees or produces is a bucket's <b>canonical hash</b> (the bucket's
 * start hash, nudged to 1 for bucket 0 since 0 is the reserved "not set" value). The owning
 * dispatcher normalizes each entry's stamped {@code entry_hash_min} — and, for unstamped entries,
 * the message key's hash — to the canonical value, which is what makes the existing per-hash
 * machinery (pending acks, replay ordering, {@link DrainingHashesTracker} draining) operate at
 * exactly bucket granularity: one hash value per bucket.
 */
public class EntryBucketConsumerSelector implements StickyKeyConsumerSelector {

    private final List<Range> bucketRanges;
    private final int[] bucketStarts;

    /** Connected consumers, sorted by (name, id). Guarded by this. */
    private final List<Consumer> consumers = new ArrayList<>();
    /** Bucket index → owning consumer; empty array while no consumer is connected. */
    private volatile Consumer[] bucketOwners = new Consumer[0];
    private volatile ConsumerHashAssignmentsSnapshot assignmentsSnapshot =
            ConsumerHashAssignmentsSnapshot.empty();

    /**
     * @param bucketRanges ascending, inclusive, contiguous ranges tiling {@code [0, 0xFFFF]};
     *                     already validated by the caller
     */
    public EntryBucketConsumerSelector(List<Range> bucketRanges) {
        this.bucketRanges = List.copyOf(bucketRanges);
        this.bucketStarts = new int[bucketRanges.size()];
        for (int i = 0; i < bucketRanges.size(); i++) {
            bucketStarts[i] = bucketRanges.get(i).getStart();
        }
    }

    public List<Range> getBucketRanges() {
        return bucketRanges;
    }

    /** The bucket index owning the given (16-bit) hash. */
    public int bucketIndexOfHash(int hash) {
        int low = 0;
        int high = bucketStarts.length - 1;
        while (low < high) {
            int mid = (low + high + 1) >>> 1;
            if (bucketStarts[mid] <= hash) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;
    }

    /**
     * The canonical hash representing a bucket — its start hash, nudged to 1 for bucket 0
     * (0 is the reserved {@link #STICKY_KEY_HASH_NOT_SET} value; 1 is still inside bucket 0).
     */
    public int canonicalHashOfBucket(int bucketIndex) {
        return Math.max(bucketStarts[bucketIndex], 1);
    }

    /** Normalize any 16-bit hash to its bucket's canonical hash. */
    public int canonicalHashOf(int hash) {
        return canonicalHashOfBucket(bucketIndexOfHash(hash));
    }

    @Override
    public int makeStickyKeyHash(byte[] stickyKey) {
        return canonicalHashOf(
                StickyKeyConsumerSelectorUtils.makeStickyKeyHash(stickyKey, getKeyHashRange()));
    }

    @Override
    public synchronized CompletableFuture<Optional<ImpactedConsumersResult>> addConsumer(Consumer consumer) {
        consumers.add(consumer);
        consumers.sort(Comparator.comparing(Consumer::consumerName)
                .thenComparingLong(Consumer::consumerId));
        return CompletableFuture.completedFuture(Optional.of(recomputeAssignments()));
    }

    @Override
    public synchronized Optional<ImpactedConsumersResult> removeConsumer(Consumer consumer) {
        consumers.remove(consumer);
        return Optional.of(recomputeAssignments());
    }

    private ImpactedConsumersResult recomputeAssignments() {
        int bucketCount = bucketRanges.size();
        Consumer[] owners = new Consumer[consumers.isEmpty() ? 0 : bucketCount];
        List<HashRangeAssignment> assignments = new ArrayList<>(bucketCount);
        if (!consumers.isEmpty()) {
            int consumerCount = consumers.size();
            for (int j = 0; j < consumerCount; j++) {
                int from = (int) ((long) j * bucketCount / consumerCount);
                int to = (int) ((long) (j + 1) * bucketCount / consumerCount);
                for (int bucket = from; bucket < to; bucket++) {
                    owners[bucket] = consumers.get(j);
                    assignments.add(new HashRangeAssignment(bucketRanges.get(bucket), consumers.get(j)));
                }
            }
        }
        ConsumerHashAssignmentsSnapshot after = ConsumerHashAssignmentsSnapshot.of(assignments);
        ImpactedConsumersResult impacted = assignmentsSnapshot.resolveImpactedConsumers(after);
        bucketOwners = owners;
        assignmentsSnapshot = after;
        return impacted;
    }

    @Override
    public Consumer select(int hash) {
        Consumer[] owners = bucketOwners;
        if (owners.length == 0) {
            return null;
        }
        return owners[bucketIndexOfHash(hash)];
    }

    @Override
    public Range getKeyHashRange() {
        return Range.of(0, DEFAULT_RANGE_SIZE - 1);
    }

    @Override
    public ConsumerHashAssignmentsSnapshot getConsumerHashAssignmentsSnapshot() {
        return assignmentsSnapshot;
    }
}
