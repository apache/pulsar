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
package org.apache.pulsar.client.impl;

import java.util.BitSet;

public class BatchMessageAcker {

    private BatchMessageAcker() {
        this.bitSet = new BitSet();
        this.batchSize = 0;
    }

    static BatchMessageAcker newAcker(int batchSize) {
        BitSet bitSet = new BitSet(batchSize);
        bitSet.set(0, batchSize);
        return new BatchMessageAcker(bitSet, batchSize);
    }

    // Use the param bitSet as the BatchMessageAcker's bitSet, don't care about the batchSize.
    static BatchMessageAcker newAcker(BitSet bitSet) {
        return new BatchMessageAcker(bitSet, -1);
    }

    // bitset shared across messages in the same batch.
    private final int batchSize;
    private final BitSet bitSet;
    private volatile boolean prevBatchCumulativelyAcked = false;

    BatchMessageAcker(BitSet bitSet, int batchSize) {
        this.bitSet = bitSet;
        this.batchSize = batchSize;
    }

    BitSet getBitSet() {
        return bitSet;
    }

    public synchronized int getBatchSize() {
        return batchSize;
    }

    public synchronized boolean ackIndividual(int batchIndex) {
        bitSet.clear(batchIndex);
        return bitSet.isEmpty();
    }

    public synchronized int getBitSetSize() {
        return bitSet.size();
    }

    public synchronized boolean ackCumulative(int batchIndex) {
        // +1 since to argument is exclusive
        bitSet.clear(0, batchIndex + 1);
        return bitSet.isEmpty();
    }

    // debug purpose
    public synchronized int getOutstandingAcks() {
        return bitSet.cardinality();
    }

    public void setPrevBatchCumulativelyAcked(boolean acked) {
        this.prevBatchCumulativelyAcked = acked;
    }

    public boolean isPrevBatchCumulativelyAcked() {
        return prevBatchCumulativelyAcked;
    }

    @Override
    public String toString() {
        return "BatchMessageAcker{"
                + "batchSize=" + batchSize
                + ", bitSet=" + bitSet
                + ", prevBatchCumulativelyAcked=" + prevBatchCumulativelyAcked
                + '}';
    }
}
