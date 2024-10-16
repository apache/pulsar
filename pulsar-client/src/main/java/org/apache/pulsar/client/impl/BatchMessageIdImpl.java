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

import java.util.BitSet;
import org.apache.pulsar.client.api.MessageIdAdv;

public class BatchMessageIdImpl extends MessageIdImpl {

    private static final long serialVersionUID = 1L;
    private final int batchIndex;
    private final int batchSize;

    private final BitSet ackSet;

    // Private constructor used only for json deserialization
    @SuppressWarnings("unused")
    private BatchMessageIdImpl() {
        this(-1, -1, -1, -1);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex) {
        this(ledgerId, entryId, partitionIndex, batchIndex, 0, null);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex, int batchSize,
                              BitSet ackSet) {
        super(ledgerId, entryId, partitionIndex);
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.ackSet = ackSet;
    }

    public BatchMessageIdImpl(MessageIdAdv other) {
        this(other.getLedgerId(), other.getEntryId(), other.getPartitionIndex(),
                other.getBatchIndex(), other.getBatchSize(), other.getAckSet());
    }

    @Override
    public int getBatchIndex() {
        return batchIndex;
    }

    @Override
    public int hashCode() {
        return MessageIdAdvUtils.hashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        return MessageIdAdvUtils.equals(this, o);
    }

    @Override
    public String toString() {
        return ledgerId + ":" + entryId + ":" + partitionIndex + ":" + batchIndex;
    }

    // Serialization
    @Override
    public byte[] toByteArray() {
        return toByteArray(batchIndex, batchSize);
    }

    @Deprecated
    public boolean ackIndividual() {
        return MessageIdAdvUtils.acknowledge(this, true);
    }

    @Deprecated
    public boolean ackCumulative() {
        return MessageIdAdvUtils.acknowledge(this, false);
    }

    @Deprecated
    public int getOutstandingAcksInSameBatch() {
        return 0;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Deprecated
    public int getOriginalBatchSize() {
        return this.batchSize;
    }

    @Deprecated
    public MessageIdImpl prevBatchMessageId() {
        return (MessageIdImpl) MessageIdAdvUtils.prevMessageId(this);
    }

    // MessageIdImpl is widely used as the key of a hash map, in this case, we should convert the batch message id to
    // have the correct hash code.
    @Deprecated
    public MessageIdImpl toMessageIdImpl() {
        return (MessageIdImpl) MessageIdAdvUtils.discardBatch(this);
    }

    @Override
    public BitSet getAckSet() {
        return ackSet;
    }

    static BitSet newAckSet(int batchSize) {
        final BitSet ackSet = new BitSet(batchSize);
        ackSet.set(0, batchSize);
        return ackSet;
    }
}
