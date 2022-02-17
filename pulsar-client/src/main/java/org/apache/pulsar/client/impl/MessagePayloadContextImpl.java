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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import java.util.List;
import lombok.NonNull;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;

public class MessagePayloadContextImpl implements MessagePayloadContext {

    private static final Recycler<MessagePayloadContextImpl> RECYCLER = new Recycler<MessagePayloadContextImpl>() {
        @Override
        protected MessagePayloadContextImpl newObject(Handle<MessagePayloadContextImpl> handle) {
            return new MessagePayloadContextImpl(handle);
        }
    };

    private final Recycler.Handle<MessagePayloadContextImpl> recyclerHandle;
    private BrokerEntryMetadata brokerEntryMetadata;
    private MessageMetadata messageMetadata;
    private SingleMessageMetadata singleMessageMetadata;
    private MessageIdImpl messageId;
    private ConsumerImpl<?> consumer;
    private int redeliveryCount;
    private BatchMessageAcker acker;
    private BitSetRecyclable ackBitSet;
    private long consumerEpoch;

    private MessagePayloadContextImpl(final Recycler.Handle<MessagePayloadContextImpl> handle) {
        this.recyclerHandle = handle;
    }

    public static MessagePayloadContextImpl get(final BrokerEntryMetadata brokerEntryMetadata,
                                                @NonNull final MessageMetadata messageMetadata,
                                                @NonNull final MessageIdImpl messageId,
                                                @NonNull final ConsumerImpl<?> consumer,
                                                final int redeliveryCount,
                                                final List<Long> ackSet,
                                                final long consumerEpoch) {
        final MessagePayloadContextImpl context = RECYCLER.get();
        context.consumerEpoch = consumerEpoch;
        context.brokerEntryMetadata = brokerEntryMetadata;
        context.messageMetadata = messageMetadata;
        context.singleMessageMetadata = new SingleMessageMetadata();
        context.messageId = messageId;
        context.consumer = consumer;
        context.redeliveryCount = redeliveryCount;
        context.acker = BatchMessageAcker.newAcker(context.getNumMessages());
        context.ackBitSet = (ackSet != null && ackSet.size() > 0)
                ? BitSetRecyclable.valueOf(SafeCollectionUtils.longListToArray(ackSet))
                : null;
        return context;
    }

    public void recycle() {
        brokerEntryMetadata = null;
        messageMetadata = null;
        singleMessageMetadata = null;
        messageId = null;
        consumer = null;
        redeliveryCount = 0;
        consumerEpoch = DEFAULT_CONSUMER_EPOCH;
        acker = null;
        if (ackBitSet != null) {
            ackBitSet.recycle();
            ackBitSet = null;
        }
        recyclerHandle.recycle(this);
    }

    @Override
    public String getProperty(String key) {
        for (KeyValue keyValue : messageMetadata.getPropertiesList()) {
            if (keyValue.hasKey() && keyValue.getKey().equals(key)) {
                return keyValue.getValue();
            }
        }
        return null;
    }

    @Override
    public int getNumMessages() {
        return messageMetadata.getNumMessagesInBatch();
    }

    @Override
    public boolean isBatch() {
        return consumer.isBatch(messageMetadata);
    }

    @Override
    public <T> Message<T> getMessageAt(int index,
                                       int numMessages,
                                       MessagePayload payload,
                                       boolean containMetadata,
                                       Schema<T> schema) {
        final ByteBuf payloadBuffer = MessagePayloadUtils.convertToByteBuf(payload);
        try {
            return consumer.newSingleMessage(index,
                    numMessages,
                    brokerEntryMetadata,
                    messageMetadata,
                    singleMessageMetadata,
                    payloadBuffer,
                    messageId,
                    schema,
                    containMetadata,
                    ackBitSet,
                    acker,
                    redeliveryCount,
                    consumerEpoch);
        } finally {
            payloadBuffer.release();
        }
    }

    @Override
    public <T> Message<T> asSingleMessage(MessagePayload payload, Schema<T> schema) {
        final ByteBuf payloadBuffer = MessagePayloadUtils.convertToByteBuf(payload);
        try {
            return consumer.newMessage(messageId, brokerEntryMetadata,
                    messageMetadata, payloadBuffer, schema, redeliveryCount, consumerEpoch);
        } finally {
            payloadBuffer.release();
        }
    }
}
