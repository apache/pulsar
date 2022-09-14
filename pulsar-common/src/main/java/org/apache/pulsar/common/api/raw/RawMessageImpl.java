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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;

public class RawMessageImpl implements RawMessage {

    private final RawMessageIdImpl messageId = new RawMessageIdImpl();

    private ReferenceCountedMessageMetadata msgMetadata;
    private final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
    private volatile boolean setSingleMessageMetadata;
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
        singleMessageMetadata.clear();
        setSingleMessageMetadata = false;

        payload.release();
        handle.recycle(this);
    }

    public static RawMessage get(ReferenceCountedMessageMetadata msgMetadata,
            SingleMessageMetadata singleMessageMetadata,
            ByteBuf payload,
            long ledgerId, long entryId, long batchIndex) {
        RawMessageImpl msg = RECYCLER.get();
        msg.msgMetadata = msgMetadata;
        msg.msgMetadata.retain();

        if (singleMessageMetadata != null) {
            msg.singleMessageMetadata.copyFrom(singleMessageMetadata);
            msg.setSingleMessageMetadata = true;
        }
        msg.messageId.ledgerId = ledgerId;
        msg.messageId.entryId = entryId;
        msg.messageId.batchIndex = batchIndex;
        msg.payload = payload;
        return msg;
    }

    public RawMessage updatePayloadForChunkedMessage(ByteBuf chunkedTotalPayload) {
        if (!msgMetadata.getMetadata().hasNumChunksFromMsg() || msgMetadata.getMetadata().getNumChunksFromMsg() <= 1) {
            throw new RuntimeException("The update payload operation only support multi chunked messages.");
        }
        payload = chunkedTotalPayload;
        return this;
    }

    @Override
    public Map<String, String> getProperties() {
        if (setSingleMessageMetadata && singleMessageMetadata.getPropertiesCount() > 0) {
            return singleMessageMetadata.getPropertiesList().stream()
                      .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue,
                              (oldValue, newValue) -> newValue));
        } else if (msgMetadata.getMetadata().getPropertiesCount() > 0) {
            return msgMetadata.getMetadata().getPropertiesList().stream()
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
        return msgMetadata.getMetadata().getPublishTime();
    }

    @Override
    public long getEventTime() {
        if (setSingleMessageMetadata && singleMessageMetadata.hasEventTime()) {
            return singleMessageMetadata.getEventTime();
        } else if (msgMetadata.getMetadata().hasEventTime()) {
            return msgMetadata.getMetadata().getEventTime();
        } else {
            return 0;
        }
    }

    @Override
    public long getSequenceId() {
        return msgMetadata.getMetadata().getSequenceId() + messageId.batchIndex;
    }

    @Override
    public String getProducerName() {
        return msgMetadata.getMetadata().getProducerName();
    }

    @Override
    public Optional<String> getKey() {
        if (setSingleMessageMetadata && singleMessageMetadata.hasPartitionKey()) {
            return Optional.of(singleMessageMetadata.getPartitionKey());
        } else if (msgMetadata.getMetadata().hasPartitionKey()){
            return Optional.of(msgMetadata.getMetadata().getPartitionKey());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public byte[] getSchemaVersion() {
        if (msgMetadata != null && msgMetadata.getMetadata().hasSchemaVersion()) {
            return msgMetadata.getMetadata().getSchemaVersion();
        } else {
            return null;
        }
    }

    public Optional<ByteBuf> getKeyBytes() {
        if (getKey().isPresent()) {
            if (hasBase64EncodedKey()) {
                return Optional.of(Unpooled.wrappedBuffer(Base64.getDecoder().decode(getKey().get())));
            } else {
                return Optional.of(Unpooled.wrappedBuffer(getKey().get().getBytes(StandardCharsets.UTF_8)));
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean hasBase64EncodedKey() {
        if (setSingleMessageMetadata) {
            return singleMessageMetadata.isPartitionKeyB64Encoded();
        }
        return msgMetadata.getMetadata().isPartitionKeyB64Encoded();
    }

    @Override
    public String getUUID() {
        if (msgMetadata.getMetadata().hasUuid()) {
            return msgMetadata.getMetadata().getUuid();
        } else {
            return null;
        }
    }

    @Override
    public int getChunkId() {
        if (msgMetadata.getMetadata().hasChunkId()) {
            return msgMetadata.getMetadata().getChunkId();
        } else {
            return -1;
        }
    }

    @Override
    public int getNumChunksFromMsg() {
        if (msgMetadata.getMetadata().hasNumChunksFromMsg()) {
            return msgMetadata.getMetadata().getNumChunksFromMsg();
        } else {
            return -1;
        }
    }

    @Override
    public int getTotalChunkMsgSize() {
        if (msgMetadata.getMetadata().hasTotalChunkMsgSize()) {
            return msgMetadata.getMetadata().getTotalChunkMsgSize();
        } else {
            return -1;
        }
    }

    public int getBatchSize() {
        return msgMetadata.getMetadata().getNumMessagesInBatch();
    }
}
