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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

import java.io.IOException;
import java.util.Objects;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.naming.TopicName;

import static org.apache.pulsar.client.impl.BatchMessageIdImpl.NO_BATCH;

public class MessageIdImpl implements MessageId {
    protected final long ledgerId;
    protected final long entryId;
    protected final int partitionIndex;

    // Private constructor used only for json deserialization
    @SuppressWarnings("unused")
    private MessageIdImpl() {
        this(-1, -1, -1);
    }

    public MessageIdImpl(long ledgerId, long entryId, int partitionIndex) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.partitionIndex = partitionIndex;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    public int hashCode() {
        return messageIdHashCode(ledgerId, entryId, partitionIndex, NO_BATCH);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MessageIdImpl) {
            MessageIdImpl other = (MessageIdImpl) o;
            int batchIndex = (o instanceof BatchMessageIdImpl) ? ((BatchMessageIdImpl) o).getBatchIndex() : NO_BATCH;
            return messageIdEquals(
                this.ledgerId, this.entryId, this.partitionIndex, NO_BATCH,
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
          .toString();
    }

    // / Serialization

    private static final FastThreadLocal<MessageIdData> LOCAL_MESSAGE_ID = new FastThreadLocal<MessageIdData>() {
        protected MessageIdData initialValue() throws Exception {
            return new MessageIdData();
        };
    };

    public static MessageId fromByteArray(byte[] data) throws IOException {
        Objects.requireNonNull(data);

        MessageIdData idData = LOCAL_MESSAGE_ID.get();
        try {
            idData.parseFrom(Unpooled.wrappedBuffer(data, 0, data.length), data.length);
        } catch (Exception e) {
            throw new IOException(e);
        }

        MessageIdImpl messageId;
        if (idData.hasBatchIndex()) {
            if (idData.hasBatchSize()) {
                messageId = new BatchMessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition(),
                    idData.getBatchIndex(), idData.getBatchSize(), BatchMessageAcker.newAcker(idData.getBatchSize()));
            } else {
                messageId = new BatchMessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition(),
                    idData.getBatchIndex());
            }
        } else {
            messageId = new MessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition());
        }

        return messageId;
    }

    public static MessageIdImpl convertToMessageIdImpl(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            return (BatchMessageIdImpl) messageId;
        } else if (messageId instanceof MessageIdImpl) {
            return (MessageIdImpl) messageId;
        } else if (messageId instanceof TopicMessageIdImpl) {
            return convertToMessageIdImpl(((TopicMessageIdImpl) messageId).getInnerMessageId());
        }
        return null;
    }

    public static MessageId fromByteArrayWithTopic(byte[] data, String topicName) throws IOException {
        return fromByteArrayWithTopic(data, TopicName.get(topicName));
    }

    public static MessageId fromByteArrayWithTopic(byte[] data, TopicName topicName) throws IOException {
        Objects.requireNonNull(data);
        MessageIdData idData = LOCAL_MESSAGE_ID.get();
        try {
            idData.parseFrom(Unpooled.wrappedBuffer(data, 0, data.length), data.length);
        } catch (Exception e) {
            throw new IOException(e);
        }

        MessageId messageId;
        if (idData.hasBatchIndex()) {
            messageId = new BatchMessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition(),
                idData.getBatchIndex(), idData.getBatchSize(), BatchMessageAcker.newAcker(idData.getBatchSize()));
        } else {
            messageId = new MessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition());
        }
        if (idData.getPartition() > -1 && topicName != null) {
            messageId = new TopicMessageIdImpl(
                    topicName.getPartition(idData.getPartition()).toString(), topicName.toString(), messageId);
        }

        return messageId;
    }

    // batchIndex is -1 if message is non-batched message and has the batchIndex for a batch message
    protected byte[] toByteArray(int batchIndex, int batchSize) {
        MessageIdData msgId = LOCAL_MESSAGE_ID.get()
                .clear()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);
        if (partitionIndex >= 0) {
            msgId.setPartition(partitionIndex);
        }

        if (batchIndex != -1) {
            msgId.setBatchIndex(batchIndex);
        }

        if (batchSize > -1) {
            msgId.setBatchSize(batchSize);
        }

        int size = msgId.getSerializedSize();
        ByteBuf serialized = Unpooled.buffer(size, size);
        msgId.writeTo(serialized);

        return serialized.array();
    }

    @Override
    public byte[] toByteArray() {
        // there is no message batch so we pass -1
        return toByteArray(-1, 0);
    }

    @Override
    public int compareTo(MessageId o) {
        if (o == null) {
            throw new UnsupportedOperationException("MessageId is null");
        }
        if (o instanceof MessageIdImpl) {
            MessageIdImpl other = (MessageIdImpl) o;
            int batchIndex = (o instanceof BatchMessageIdImpl) ? ((BatchMessageIdImpl) o).getBatchIndex() : NO_BATCH;
            return messageIdCompare(
                this.ledgerId, this.entryId, this.partitionIndex, NO_BATCH,
                other.ledgerId, other.entryId, other.partitionIndex, batchIndex
            );
        } else if (o instanceof TopicMessageIdImpl) {
            return compareTo(((TopicMessageIdImpl) o).getInnerMessageId());
        } else {
            throw new UnsupportedOperationException("Unknown MessageId type: " + o.getClass().getName());
        }
    }

    static int messageIdHashCode(long ledgerId, long entryId, int partitionIndex, int batchIndex) {
        return (int) (31 * (ledgerId + 31 * entryId) + (31 * (long) partitionIndex) + batchIndex);
    }

    static boolean messageIdEquals(
        long ledgerId1, long entryId1, int partitionIndex1, int batchIndex1,
        long ledgerId2, long entryId2, int partitionIndex2, int batchIndex2
    ) {
        return ledgerId1 == ledgerId2
            && entryId1 == entryId2
            && partitionIndex1 == partitionIndex2
            && batchIndex1 == batchIndex2;
    }

    static int messageIdCompare(
        long ledgerId1, long entryId1, int partitionIndex1, int batchIndex1,
        long ledgerId2, long entryId2, int partitionIndex2, int batchIndex2
    ) {
        return ComparisonChain.start()
            .compare(ledgerId1, ledgerId2)
            .compare(entryId1, entryId2)
            .compare(partitionIndex1, partitionIndex2)
            .compare(batchIndex1, batchIndex2)
            .result();
    }
}
