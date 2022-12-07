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
import javax.annotation.Nonnull;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarApiMessageId;
import org.apache.pulsar.client.util.MessageIdUtils;

// When the batch index ACK is disabled and a batched message is acknowledged cumulatively, the previous message might
// be acknowledged instead. Since a message should not be acknowledged repeatedly, this interface defines a method to
// determine whether the previous message can be acknowledged.
interface PreviousMessageAcknowledger {

    boolean canAckPreviousMessage();
}

/**
 */
public class BatchMessageIdImpl extends MessageIdImpl implements PreviousMessageAcknowledger {

    private static final long serialVersionUID = 1L;
    private final int batchIndex;
    private final int batchSize;

    private final BitSet ackSet;
    private volatile boolean prevBatchCumulativelyAcked = false;

    // Private constructor used only for json deserialization
    @SuppressWarnings("unused")
    private BatchMessageIdImpl() {
        this(-1, -1, -1, -1);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex) {
        this(ledgerId, entryId, partitionIndex, batchIndex, 0, BatchMessageAckerDisabled.INSTANCE);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex, int batchSize,
                              BitSet ackSet) {
        super(ledgerId, entryId, partitionIndex);
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.ackSet = ackSet;
    }

    @Deprecated
    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex, int batchSize,
                              BatchMessageAcker acker) {
        super(ledgerId, entryId, partitionIndex);
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.ackSet = acker.getBitSet();
    }

    public BatchMessageIdImpl(PulsarApiMessageId other) {
        this(other.getLedgerId(), other.getEntryId(), other.getPartition(),
                other.getBatchIndex(), other.getBatchSize(), other.getAckSet());
    }

    @Override
    public int getBatchIndex() {
        return batchIndex;
    }

    @Override
    public int compareTo(@Nonnull MessageId o) {
        return super.compareTo(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
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
        return MessageIdUtils.acknowledge(this, true);
    }

    @Deprecated
    public boolean ackCumulative() {
        return MessageIdUtils.acknowledge(this, false);
    }

    @Deprecated
    public int getOutstandingAcksInSameBatch() {
        return 0;
    }

    @Override
    public BitSet getAckSet() {
        return ackSet;
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
        return MessageIdImpl.prevMessageId(this);
    }

    public static BatchMessageIdImpl prevMessageId(PulsarApiMessageId msgId) {
        if (msgId.isBatch()) {
            return new BatchMessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), msgId.getPartition(),
                    msgId.getBatchIndex() - 1);
        } else {
            return new BatchMessageIdImpl(msgId.getLedgerId(), msgId.getEntryId() - 1,
                    msgId.getPartition(), -1);
        }
    }

    // MessageIdImpl is widely used as the key of a hash map, in this case, we should convert the batch message id to
    // have the correct hash code.
    @Deprecated
    public MessageIdImpl toMessageIdImpl() {
        return MessageIdImpl.from(this);
    }

    @Override
    public boolean canAckPreviousMessage() {
        if (prevBatchCumulativelyAcked) {
            return false;
        } else {
            prevBatchCumulativelyAcked = true;
            return true;
        }
    }
}
