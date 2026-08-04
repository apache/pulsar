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
package org.apache.pulsar.broker.service.persistent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryAndMetadata;
import org.apache.pulsar.broker.service.EntryBucketConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.IntRange;
import org.apache.pulsar.common.api.proto.KeySharedMeta;

/**
 * PIP-486: Key_Shared dispatcher for a scalable-topic segment shared by <b>entry-bucket</b>.
 *
 * <p>Selected by {@link PersistentSubscription} when the consumers' {@link KeySharedMeta} carries
 * {@code entryBucketDispatch}. Producers of a scalable segment batch per entry-bucket and stamp
 * each entry's effective bucket hash range ({@code entry_hash_min}); this dispatcher routes each
 * <b>whole entry</b> to the consumer owning its bucket — no per-message key hashing, no entry
 * filtering, batching intact.
 *
 * <p>The mechanics reuse the battle-tested per-hash Key_Shared machinery at bucket granularity by
 * <b>normalizing every entry's hash to its bucket's canonical value</b> (one hash value per
 * bucket, see {@link EntryBucketConsumerSelector#canonicalHashOf}): pending acks, replay-order
 * blocking, and the draining tracker then operate per bucket automatically. A membership change
 * therefore hands buckets over exactly like AUTO_SPLIT hands over hash ranges — the moving bucket
 * is blocked until the previous owner's outstanding entries are acked, then flows to the new owner
 * in order. Clients never re-subscribe, are never rejected, and implement nothing beyond the
 * subscribe flag: the handoff is entirely broker-side.
 *
 * <p>The bucket boundaries arrive in the subscribe itself ({@code KeySharedMeta.hashRanges}): a
 * segment's bucketing is immutable for its life, so every consumer declares the identical
 * boundary list; the first consumer's boundaries create the selector and later consumers are
 * validated against them.
 */
public class PersistentEntryBucketDispatcherMultipleConsumers
        extends PersistentStickyKeyDispatcherMultipleConsumers {

    private final EntryBucketConsumerSelector bucketSelector;

    PersistentEntryBucketDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription, ServiceConfiguration conf, KeySharedMeta ksm) {
        // Draining is required: the inherited DrainingHashesTracker tracks the canonical bucket
        // hashes, which makes it a per-bucket handoff tracker.
        super(topic, cursor, subscription, conf, ksm,
                new EntryBucketConsumerSelector(validateBucketBoundaries(ksm)), true);
        this.bucketSelector = (EntryBucketConsumerSelector) getSelector();
    }

    /**
     * Validate and convert the boundaries declared at subscribe time: ascending, inclusive,
     * contiguous ranges tiling the whole 16-bit entry-bucket ring, with bucket 0 wide enough to
     * contain the canonical hash 1.
     */
    static List<Range> validateBucketBoundaries(KeySharedMeta ksm) {
        int count = ksm.getHashRangesCount();
        if (count == 0) {
            throw new IllegalArgumentException(
                    "Entry-bucket subscription must declare the segment's bucket boundaries");
        }
        List<Range> ranges = new ArrayList<>(count);
        int expectedStart = 0;
        for (int i = 0; i < count; i++) {
            IntRange r = ksm.getHashRangeAt(i);
            if (r.getStart() != expectedStart || r.getEnd() < r.getStart()) {
                throw new IllegalArgumentException("Entry-bucket boundaries must be ascending, "
                        + "contiguous and start at 0: found [" + r.getStart() + "," + r.getEnd()
                        + "] where start " + expectedStart + " was expected");
            }
            ranges.add(Range.of(r.getStart(), r.getEnd()));
            expectedStart = r.getEnd() + 1;
        }
        if (expectedStart != EntryBucketConsumerSelector.DEFAULT_RANGE_SIZE) {
            throw new IllegalArgumentException("Entry-bucket boundaries must tile the 16-bit ring: "
                    + "last range ends at " + (expectedStart - 1));
        }
        if (ranges.get(0).getEnd() < 1) {
            throw new IllegalArgumentException(
                    "Entry-bucket 0 must span at least [0,1] to hold the canonical hash");
        }
        return ranges;
    }

    @Override
    public synchronized CompletableFuture<Void> addConsumer(Consumer consumer) {
        // A segment's bucketing is immutable, so every consumer must declare the boundaries the
        // dispatcher was created with — a mismatch is a client bug or a stale layout, not a race.
        try {
            List<Range> declared = validateBucketBoundaries(consumer.getKeySharedMeta());
            if (!declared.equals(bucketSelector.getBucketRanges())) {
                return CompletableFuture.failedFuture(new BrokerServiceException.ConsumerAssignException(
                        "Consumer declares different entry-bucket boundaries than the subscription: "
                                + declared + " != " + bucketSelector.getBucketRanges()));
            }
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(
                    new BrokerServiceException.ConsumerAssignException(e.getMessage()));
        }
        return super.addConsumer(consumer);
    }

    @Override
    public boolean hasSameKeySharedPolicy(KeySharedMeta ksm) {
        return ksm.getKeySharedMode() == getKeySharedMode()
                && ksm.isAllowOutOfOrderDelivery() == isAllowOutOfOrderDelivery()
                && ksm.isEntryBucketDispatch();
    }

    @Override
    protected int getStickyKeyHash(Entry entry) {
        if (entry instanceof EntryAndMetadata entryAndMetadata) {
            // Cached so pending acks, redelivery and draining all see the same canonical value.
            return entryAndMetadata.getOrUpdateCachedStickyKeyHash(stickyKey -> {
                var metadata = entryAndMetadata.getMetadata();
                if (metadata != null && metadata.hasEntryHashMin()) {
                    // The producer stamped the entry's effective bucket range: route the whole
                    // entry by its bucket.
                    return bucketSelector.canonicalHashOf(metadata.getEntryHashMin());
                }
                // Unstamped (non-batched) entry: the key's low-16 Murmur hash is the same value
                // the producer would have stamped, normalized to the same bucket.
                return bucketSelector.makeStickyKeyHash(stickyKey);
            });
        }
        return bucketSelector.makeStickyKeyHash(peekStickyKey(entry));
    }
}
