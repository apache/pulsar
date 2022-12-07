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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.util.BitSet;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarApiMessageId;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.naming.TopicName;

public class MessageIdImpl implements PulsarApiMessageId {
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

    public int getPartition() {
        return partitionIndex;
    }

    @Deprecated
    public int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    public int hashCode() {
        return PulsarApiMessageId.hashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PulsarApiMessageId) {
            return PulsarApiMessageId.equals(this, (PulsarApiMessageId) o);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return ledgerId + ":" + entryId + ":" + partitionIndex;
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
                BitSet ackSet = new BitSet(idData.getBatchSize());
                ackSet.set(0, idData.getBatchSize());
                messageId = new BatchMessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition(),
                    idData.getBatchIndex(), idData.getBatchSize(), ackSet);
            } else {
                messageId = new BatchMessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition(),
                    idData.getBatchIndex());
            }
        } else if (idData.hasFirstChunkMessageId()) {
            MessageIdData firstChunkIdData = idData.getFirstChunkMessageId();
            messageId = new ChunkMessageIdImpl(
                    new MessageIdImpl(firstChunkIdData.getLedgerId(), firstChunkIdData.getEntryId(),
                            firstChunkIdData.getPartition()),
                    new MessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition()));
        } else {
            messageId = new MessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition());
        }

        return messageId;
    }

    @Deprecated
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

        PulsarApiMessageId messageId;
        if (idData.hasBatchIndex()) {
            if (idData.hasBatchSize()) {
                BitSet ackSet = new BitSet(idData.getBatchSize());
                ackSet.set(0, idData.getBatchSize());
                messageId = new BatchMessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition(),
                        idData.getBatchIndex(), idData.getBatchSize(), ackSet);
            } else {
                messageId = new BatchMessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition(),
                        idData.getBatchIndex(), 0, (BitSet) null);
            }
        } else {
            messageId = new MessageIdImpl(idData.getLedgerId(), idData.getEntryId(), idData.getPartition());
        }
        if (idData.getPartition() > -1 && topicName != null) {
            messageId = new TopicMessageIdImpl(
                    topicName.getPartition(idData.getPartition()).toString(), topicName.toString(), messageId);
        }

        return messageId;
    }

    protected MessageIdData writeMessageIdData(MessageIdData msgId, int batchIndex, int batchSize) {
        if (msgId == null) {
            msgId = LOCAL_MESSAGE_ID.get()
                    .clear();
        }

        msgId.setLedgerId(ledgerId).setEntryId(entryId);

        if (partitionIndex >= 0) {
            msgId.setPartition(partitionIndex);
        }

        if (batchIndex != -1) {
            msgId.setBatchIndex(batchIndex);
        }

        if (batchSize > -1) {
            msgId.setBatchSize(batchSize);
        }

        return msgId;
    }

    // batchIndex is -1 if message is non-batched message and has the batchIndex for a batch message
    protected byte[] toByteArray(int batchIndex, int batchSize) {
        MessageIdData msgId = writeMessageIdData(null, batchIndex, batchSize);

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
    public int compareTo(@Nonnull MessageId o) {
        return PulsarApiMessageId.legacyCompare(this, (PulsarApiMessageId) o);
    }

    public static MessageIdImpl from(PulsarApiMessageId msgId) {
        if (msgId.getClass() == MessageIdImpl.class) {
            return (MessageIdImpl) msgId;
        } else {
            return new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), msgId.getPartition());
        }
    }

    public static MessageIdImpl prevMessageId(PulsarApiMessageId msgId) {
        return new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId() - 1, msgId.getPartition());
    }
}
