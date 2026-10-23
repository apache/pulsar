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


import io.netty.util.Recycler;
import org.apache.pulsar.common.util.LongArrayAckSets;

/**
 * Holds, for each entry in a dispatch batch, the batch size and the ack set (bits of batch messages already acked).
 * The data lives in two parallel arrays so that storing one entry's info does not require allocating a Pair.
 */
public class EntryBatchIndexesAcks {
    private int size = 100;
    // batch size for each entry (0 means no ack info was recorded for this entry)
    private int[] batchSizes = new int[size];
    // ack set (long bitmap of acked indexes) for each entry; null means no partial acks recorded
    private long[][] ackSets = new long[size][];

    /**
     * Store the batch size and ack set for entry at {@code entryIdx} without allocating a Pair.
     */
    public void setIndexesAcks(int entryIdx, int batchSize, long[] ackSet) {
        batchSizes[entryIdx] = batchSize;
        ackSets[entryIdx] = ackSet;
    }

    /**
     * @deprecated use {@link #setIndexesAcks(int, int, long[])} to avoid Pair allocation on the hot path.
     */
    @Deprecated
    public void setIndexesAcks(int entryIdx, org.apache.commons.lang3.tuple.Pair<Integer, long[]> indexesAcks) {
        batchSizes[entryIdx] = indexesAcks == null ? 0 : indexesAcks.getLeft();
        ackSets[entryIdx] = indexesAcks == null ? null : indexesAcks.getRight();
    }

    public long[] getAckSet(int entryIdx) {
        return ackSets[entryIdx];
    }

    public int getBatchSize(int entryIdx) {
        return batchSizes[entryIdx];
    }

    /**
     * Sum of acked individual indexes across all entries in the current batch.
     * Only entries with a recorded ackSet (i.e. partial batch acks) contribute to this count.
     */
    public int getTotalAckedIndexCount() {
        int count = 0;
        for (int i = 0; i < size; i++) {
            long[] ackSet = ackSets[i];
            if (ackSet != null) {
                count += batchSizes[i] - LongArrayAckSets.cardinality(ackSet);
            }
        }
        return count;
    }

    public void recycle() {
        for (int i = 0; i < size; i++) {
            batchSizes[i] = 0;
            ackSets[i] = null;
        }
        handle.recycle(this);
    }

    private void ensureCapacityAndSetSize(int entriesListSize) {
        size = entriesListSize;
        if (batchSizes.length < size) {
            batchSizes = new int[size];
        }
        if (ackSets.length < size) {
            ackSets = new long[size][];
        }
    }

    public static EntryBatchIndexesAcks get(int entriesListSize) {
        EntryBatchIndexesAcks ebi = RECYCLER.get();
        ebi.ensureCapacityAndSetSize(entriesListSize);
        return ebi;
    }

    private EntryBatchIndexesAcks(Recycler.Handle<EntryBatchIndexesAcks> handle) {
        this.handle = handle;
    }

    private final Recycler.Handle<EntryBatchIndexesAcks> handle;
    private static final Recycler<EntryBatchIndexesAcks> RECYCLER = new Recycler<EntryBatchIndexesAcks>() {
        @Override
        protected EntryBatchIndexesAcks newObject(Handle<EntryBatchIndexesAcks> handle) {
            return new EntryBatchIndexesAcks(handle);
        }
    };
}
