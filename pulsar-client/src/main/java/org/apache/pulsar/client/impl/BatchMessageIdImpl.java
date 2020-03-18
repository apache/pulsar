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

import com.google.common.collect.ComparisonChain;
import org.apache.pulsar.client.api.MessageId;

/**
 */
public class BatchMessageIdImpl extends MessageIdImpl {
    private final static int NO_BATCH = -1;
    private final int batchIndex;

    private final transient BatchMessageAcker acker;

    // Private constructor used only for json deserialization
    @SuppressWarnings("unused")
    private BatchMessageIdImpl() {
        this(-1, -1, -1, -1);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex) {
        this(ledgerId, entryId, partitionIndex, batchIndex, BatchMessageAckerDisabled.INSTANCE);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex, BatchMessageAcker acker) {
        super(ledgerId, entryId, partitionIndex);
        this.batchIndex = batchIndex;
        this.acker = acker;
    }

    public BatchMessageIdImpl(MessageIdImpl other) {
        super(other.ledgerId, other.entryId, other.partitionIndex);
        if (other instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl otherId = (BatchMessageIdImpl) other;
            this.batchIndex = otherId.batchIndex;
            this.acker = otherId.acker;
        } else {
            this.batchIndex = NO_BATCH;
            this.acker = BatchMessageAckerDisabled.INSTANCE;
        }
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    @Override
    public int compareTo(MessageId o) {
        if (o instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl other = (BatchMessageIdImpl) o;
            return ComparisonChain.start()
                .compare(this.ledgerId, other.ledgerId)
                .compare(this.entryId, other.entryId)
                .compare(this.batchIndex, other.batchIndex)
                .compare(this.getPartitionIndex(), other.getPartitionIndex())
                .result();
        } else if (o instanceof MessageIdImpl) {
            int res = super.compareTo(o);
            if (res == 0 && batchIndex > NO_BATCH) {
                return 1;
            } else {
                return res;
            }
        } else if (o instanceof TopicMessageIdImpl) {
            return compareTo(((TopicMessageIdImpl) o).getInnerMessageId());
        } else {
            throw new IllegalArgumentException(
                    "expected BatchMessageIdImpl object. Got instance of " + o.getClass().getName());
        }
    }

    @Override
    public int hashCode() {
        return (int) (31 * (ledgerId + 31 * entryId) + (31 * partitionIndex) + batchIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl other = (BatchMessageIdImpl) obj;
            return ledgerId == other.ledgerId && entryId == other.entryId && partitionIndex == other.partitionIndex
                    && batchIndex == other.batchIndex;
        } else if (obj instanceof MessageIdImpl) {
            MessageIdImpl other = (MessageIdImpl) obj;
            return ledgerId == other.ledgerId && entryId == other.entryId && partitionIndex == other.partitionIndex
                    && batchIndex == NO_BATCH;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("%d:%d:%d:%d", ledgerId, entryId, partitionIndex, batchIndex);
    }

    // Serialization
    @Override
    public byte[] toByteArray() {
        return toByteArray(batchIndex);
    }

    public boolean ackIndividual() {
        return acker.ackIndividual(batchIndex);
    }

    public boolean ackCumulative() {
        return acker.ackCumulative(batchIndex);
    }

    public int getOutstandingAcksInSameBatch() {
        return acker.getOutstandingAcks();
    }

    public int getBatchSize() {
        return acker.getBatchSize();
    }

    public MessageIdImpl prevBatchMessageId() {
        return new MessageIdImpl(
            ledgerId, entryId - 1, partitionIndex);
    }

    public BatchMessageAcker getAcker() {
        return acker;
    }

}
