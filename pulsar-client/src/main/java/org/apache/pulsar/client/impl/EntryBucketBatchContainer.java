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
package org.apache.pulsar.client.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.common.scalable.ScalableTopicHashing;

/**
 * PIP-486 entry-bucket batch container for scalable topics.
 *
 * <p>Within one segment, each message's key maps to an <i>entry-bucket</i> — a contiguous sub-range of
 * the 16-bit entry-bucket hash ring (the low 16 bits of the key's {@code Murmur3_32} hash). This
 * container keeps each batch within a single bucket (one {@link BatchMessageContainerImpl} per bucket),
 * so the broker can route a whole entry to the consumer owning that bucket without reading per-message
 * keys. Each emitted batch stamps {@code entry_hash_min}/{@code entry_hash_max} — the effective range of
 * its messages — via {@link BatchMessageContainerImpl}'s hook.
 *
 * <p>The buckets are defined by {@code splits}: the ascending, inclusive bucket-start hashes of buckets
 * {@code 1..N-1} (bucket 0 implicitly starts at {@code 0x0000}). An empty list means a single bucket
 * spanning the whole ring — in which case this behaves like the default batcher plus the stamping.
 */
@CustomLog
class EntryBucketBatchContainer extends AbstractBatchMessageContainer {

    private final int[] splits;
    private final Map<Integer, BatchMessageContainerImpl> batches = new HashMap<>();

    EntryBucketBatchContainer(List<Integer> entryBucketSplits) {
        this.splits = entryBucketSplits == null ? new int[0]
                : entryBucketSplits.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public boolean add(MessageImpl<?> msg, SendCallback callback) {
        int bucket = bucketOf(splits, entryBucketHash(msg));
        final BatchMessageContainerImpl batchMessageContainer = batches.computeIfAbsent(bucket, __ -> {
            BatchMessageContainerImpl c = new BatchMessageContainerImpl(producer);
            c.setEntryBucketHashFn(this::entryBucketHash);
            return c;
        });
        batchMessageContainer.add(msg, callback);
        // `add` fails iff the container was empty and `msg` (the first message) failed; then the
        // container is cleared and there is nothing to count.
        if (!batchMessageContainer.isEmpty()) {
            numMessagesInBatch++;
            currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
        }
        tryUpdateTimestamp();
        return isBatchFull();
    }

    /** The 16-bit entry-bucket hash (low 16 bits of {@code Murmur3_32}) of a message's key. */
    private int entryBucketHash(MessageImpl<?> msg) {
        byte[] keyBytes;
        if (msg.hasOrderingKey()) {
            keyBytes = msg.getOrderingKey();
        } else if (msg.getKey() != null) {
            keyBytes = msg.getKey().getBytes(StandardCharsets.UTF_8);
        } else {
            return 0;
        }
        return ScalableTopicHashing.entryBucketHash(ScalableTopicHashing.murmur(keyBytes));
    }

    /** The bucket a hash falls in: the number of split points {@code <=} it (splits are ascending). */
    static int bucketOf(int[] splits, int hash) {
        int idx = 0;
        for (int split : splits) {
            if (split <= hash) {
                idx++;
            } else {
                break;
            }
        }
        return idx;
    }

    @Override
    public void clear() {
        clearTimestamp();
        numMessagesInBatch = 0;
        currentBatchSizeBytes = 0;
        batches.clear();
        currentTxnidMostBits = -1L;
        currentTxnidLeastBits = -1L;
        batchAllocatedSizeBytes = 0;
    }

    @Override
    public boolean isEmpty() {
        return batches.isEmpty();
    }

    @Override
    public void discard(Exception ex) {
        batches.forEach((k, v) -> v.discard(ex));
        clear();
    }

    @Override
    public boolean isMultiBatches() {
        return true;
    }

    @Override
    public int getBatchAllocatedSizeBytes() {
        return batches.values().stream().mapToInt(AbstractBatchMessageContainer::getBatchAllocatedSizeBytes).sum();
    }

    @Override
    public List<ProducerImpl.OpSendMsg> createOpSendMsgs() throws IOException {
        try {
            // As in key-based batching: within a bucket the sequence ids need not be contiguous, so
            // collapse to the highest sequence id and drop the highest_sequence_id field to allow the
            // broker's weak-order check to pass.
            batches.values().forEach(c -> c.setLowestSequenceId(c.getHighestSequenceId()));
            return batches.values().stream()
                    .sorted((o1, o2) -> (int) (o1.getLowestSequenceId() - o2.getLowestSequenceId()))
                    .map(c -> {
                        try {
                            return c.createOpSendMsg();
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }).collect(Collectors.toList());
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public void resetPayloadAfterFailedPublishing() {
        for (BatchMessageContainerImpl batch : batches.values()) {
            batch.resetPayloadAfterFailedPublishing();
        }
    }

    @Override
    public boolean hasSameSchema(MessageImpl<?> msg) {
        BatchMessageContainerImpl c = batches.get(bucketOf(splits, entryBucketHash(msg)));
        return c == null || c.hasSameSchema(msg);
    }
}
