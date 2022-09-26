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

import org.apache.pulsar.client.api.MessageId;

/**
 */
public class BatchMessageIdImpl extends MessageIdImpl {

    private static final long serialVersionUID = 1L;
    static final int NO_BATCH = -1;
    private final int batchIndex;
    private final int batchSize;

    private final transient BatchMessageAcker acker;

    // Private constructor used only for json deserialization
    @SuppressWarnings("unused")
    private BatchMessageIdImpl() {
        this(-1, -1, -1, -1);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex) {
        this(ledgerId, entryId, partitionIndex, batchIndex, 0, BatchMessageAckerDisabled.INSTANCE);
    }

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex, int batchSize,
                              BatchMessageAcker acker) {
        super(ledgerId, entryId, partitionIndex);
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.acker = acker;
    }

    public BatchMessageIdImpl(MessageIdImpl other) {
        super(other.ledgerId, other.entryId, other.partitionIndex);
        if (other instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl otherId = (BatchMessageIdImpl) other;
            this.batchIndex = otherId.batchIndex;
            this.batchSize = otherId.batchSize;
            this.acker = otherId.acker;
        } else {
            this.batchIndex = NO_BATCH;
            this.batchSize = 0;
            this.acker = BatchMessageAckerDisabled.INSTANCE;
        }
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    @Override
    public int compareTo(MessageId o) {
        if (o instanceof MessageIdImpl) {
            MessageIdImpl other = (MessageIdImpl) o;
            int batchIndex = (o instanceof BatchMessageIdImpl) ? ((BatchMessageIdImpl) o).batchIndex : NO_BATCH;
            return messageIdCompare(
                this.ledgerId, this.entryId, this.partitionIndex, this.batchIndex,
                other.ledgerId, other.entryId, other.partitionIndex, batchIndex
            );
        } else if (o instanceof TopicMessageIdImpl) {
            return compareTo(((TopicMessageIdImpl) o).getInnerMessageId());
        } else {
            throw new UnsupportedOperationException("Unknown MessageId type: " + o.getClass().getName());
        }
    }

    @Override
    public int hashCode() {
        return messageIdHashCode(ledgerId, entryId, partitionIndex, batchIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MessageIdImpl) {
            MessageIdImpl other = (MessageIdImpl) o;
            int batchIndex = (o instanceof BatchMessageIdImpl) ? ((BatchMessageIdImpl) o).batchIndex : NO_BATCH;
            return messageIdEquals(
                this.ledgerId, this.entryId, this.partitionIndex, this.batchIndex,
                other.ledgerId, other.entryId, other.partitionIndex, batchIndex
            );
        } else if (o instanceof TopicMessageIdImpl) {
            return equals(((TopicMessageIdImpl) o).getInnerMessageId());
        }
        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
          .append(ledgerId)
          .append(':')
          .append(entryId)
          .append(':')
          .append(partitionIndex)
          .append(':')
          .append(batchIndex)
          .toString();
    }

    // Serialization
    @Override
    public byte[] toByteArray() {
        return toByteArray(batchIndex, batchSize);
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

    public int getOriginalBatchSize() {
        return this.batchSize;
    }

    public MessageIdImpl prevBatchMessageId() {
        return new MessageIdImpl(
            ledgerId, entryId - 1, partitionIndex);
    }

    // MessageIdImpl is widely used as the key of a hash map, in this case, we should convert the batch message id to
    // have the correct hash code.
    public MessageIdImpl toMessageIdImpl() {
        return new MessageIdImpl(ledgerId, entryId, partitionIndex);
    }

    public BatchMessageAcker getAcker() {
        return acker;
    }

}
