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
package org.apache.pulsar.common.api.raw;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

public class RawMessageImpl implements RawMessage {

    private final RawMessageIdImpl messageId = new RawMessageIdImpl();

    private ReferenceCountedObject<MessageMetadata> msgMetadata;
    private PulsarApi.SingleMessageMetadata.Builder singleMessageMetadata;
    private ByteBuf payload;

    private static final Recycler<RawMessageImpl> RECYCLER = new Recycler<RawMessageImpl>() {
        @Override
        protected RawMessageImpl newObject(Handle<RawMessageImpl> handle) {
            return new RawMessageImpl(handle);
        }
    };

    private final Handle<RawMessageImpl> handle;

    private RawMessageImpl(Handle<RawMessageImpl> handle) {
        this.handle = handle;
    }

    @Override
    public void release() {
        msgMetadata.release();
        msgMetadata = null;

        if (singleMessageMetadata != null) {
            singleMessageMetadata.recycle();
            singleMessageMetadata = null;
        }

        payload.release();
        handle.recycle(this);
    }

    public static RawMessage get(ReferenceCountedObject<MessageMetadata> msgMetadata,
            PulsarApi.SingleMessageMetadata.Builder singleMessageMetadata,
            ByteBuf payload,
            long ledgerId, long entryId, long batchIndex) {
        RawMessageImpl msg = RECYCLER.get();
        msg.msgMetadata = msgMetadata;
        msg.msgMetadata.retain();
        msg.singleMessageMetadata = singleMessageMetadata;
        msg.messageId.ledgerId = ledgerId;
        msg.messageId.entryId = entryId;
        msg.messageId.batchIndex = batchIndex;
        msg.payload = payload;
        return msg;
    }

    @Override
    public Map<String, String> getProperties() {
        if (singleMessageMetadata != null && singleMessageMetadata.getPropertiesCount() > 0) {
            return singleMessageMetadata.getPropertiesList().stream()
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
        } else if (msgMetadata.get().getPropertiesCount() > 0) {
            return msgMetadata.get().getPropertiesList().stream()
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public ByteBuf getData() {
        return payload;
    }

    @Override
    public RawMessageId getMessageId() {
        return messageId;
    }

    @Override
    public long getPublishTime() {
        return msgMetadata.get().getPublishTime();
    }

    @Override
    public long getEventTime() {
        if (singleMessageMetadata != null && singleMessageMetadata.hasEventTime()) {
            return singleMessageMetadata.getEventTime();
        } else if (msgMetadata.get().hasEventTime()) {
            return msgMetadata.get().getEventTime();
        } else {
            return 0;
        }
    }

    @Override
    public long getSequenceId() {
        return msgMetadata.get().getSequenceId() + messageId.batchIndex;
    }

    @Override
    public String getProducerName() {
        return msgMetadata.get().getProducerName();
    }

    @Override
    public Optional<String> getKey() {
        if (singleMessageMetadata != null && singleMessageMetadata.hasPartitionKey()) {
            return Optional.of(singleMessageMetadata.getPartitionKey());
        } else if (msgMetadata.get().hasPartitionKey()){
            return Optional.of(msgMetadata.get().getPartitionKey());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public byte[] getSchemaVersion() {
        if (msgMetadata != null && msgMetadata.get().hasSchemaVersion()) {
            return msgMetadata.get().getSchemaVersion().toByteArray();
        } else {
            return null;
        }
    }

    public Optional<ByteBuf> getKeyBytes() {
        if (getKey().isPresent()) {
            if (hasBase64EncodedKey()) {
                return Optional.of(Unpooled.wrappedBuffer(Base64.getDecoder().decode(getKey().get())));
            } else {
                return Optional.of(Unpooled.wrappedBuffer(getKey().get().getBytes()));
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean hasBase64EncodedKey() {
        if (singleMessageMetadata != null) {
            return singleMessageMetadata.getPartitionKeyB64Encoded();
        }
        return msgMetadata.get().getPartitionKeyB64Encoded();
    }

    public int getBatchSize() {
        return msgMetadata.get().getNumMessagesInBatch();
    }
}
