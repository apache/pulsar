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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

public class MessageImpl<T> extends MessageRecordImpl<T, MessageId> {

    private MessageMetadata.Builder msgMetadataBuilder;
    private ClientCnx cnx;
    private ByteBuf payload;
    private Schema<T> schema;
    private Optional<EncryptionContext> encryptionCtx = Optional.empty();

    transient private Map<String, String> properties;

    // Constructor for out-going message
    static <T> MessageImpl<T> create(MessageMetadata.Builder msgMetadataBuilder, ByteBuffer payload, Schema<T> schema) {
        @SuppressWarnings("unchecked")
        MessageImpl<T> msg = (MessageImpl<T>) RECYCLER.get();
        msg.msgMetadataBuilder = msgMetadataBuilder;
        msg.messageId = null;
        msg.cnx = null;
        msg.payload = Unpooled.wrappedBuffer(payload);
        msg.properties = null;
        msg.schema = schema;
        return msg;
    }

    static MessageImpl<byte[]> create(MessageMetadata.Builder msgMetadataBuilder, ByteBuffer payload) {
        @SuppressWarnings("unchecked")
        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) RECYCLER.get();
        msg.msgMetadataBuilder = msgMetadataBuilder;
        msg.messageId = null;
        msg.cnx = null;
        msg.payload = Unpooled.wrappedBuffer(payload);
        msg.properties = null;
        msg.schema = Schema.BYTES;
        return msg;
    }

    // Constructor for incoming message
    MessageImpl(MessageIdImpl messageId, MessageMetadata msgMetadata, ByteBuf payload, ClientCnx cnx,
            Schema<T> schema) {
        this(messageId, msgMetadata, payload, null, cnx, schema);
    }
    
    MessageImpl(MessageIdImpl messageId, MessageMetadata msgMetadata, ByteBuf payload,
            Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema) {
        this.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        this.messageId = messageId;
        this.cnx = cnx;

        // Need to make a copy since the passed payload is using a ref-count buffer that we don't know when could
        // release, since the Message is passed to the user. Also, the passed ByteBuf is coming from network and is
        // backed by a direct buffer which we could not expose as a byte[]
        this.payload = Unpooled.copiedBuffer(payload);
        this.encryptionCtx = encryptionCtx;

        if (msgMetadata.getPropertiesCount() > 0) {
            this.properties = Collections.unmodifiableMap(msgMetadataBuilder.getPropertiesList().stream()
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)));
        } else {
            properties = Collections.emptyMap();
        }
        this.schema = schema;
    }

    MessageImpl(BatchMessageIdImpl batchMessageIdImpl, MessageMetadata msgMetadata,
            PulsarApi.SingleMessageMetadata singleMessageMetadata, ByteBuf payload,
            Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema) {
        this.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        this.messageId = batchMessageIdImpl;
        this.cnx = cnx;

        this.payload = Unpooled.copiedBuffer(payload);
        this.encryptionCtx = encryptionCtx;

        if (singleMessageMetadata.getPropertiesCount() > 0) {
            Map<String, String> properties = Maps.newTreeMap();
            for (KeyValue entry : singleMessageMetadata.getPropertiesList()) {
                properties.put(entry.getKey(), entry.getValue());
            }
            this.properties = Collections.unmodifiableMap(properties);
        } else {
            properties = Collections.emptyMap();
        }

        if (singleMessageMetadata.hasPartitionKey()) {
            msgMetadataBuilder.setPartitionKey(singleMessageMetadata.getPartitionKey());
        }

        if (singleMessageMetadata.hasEventTime()) {
            msgMetadataBuilder.setEventTime(singleMessageMetadata.getEventTime());
        }

        this.schema = schema;
    }

    public MessageImpl(String msgId, Map<String, String> properties, byte[] payload, Schema<T> schema) {
        this(msgId, properties, Unpooled.wrappedBuffer(payload), schema);
    }

    public MessageImpl(String msgId, Map<String, String> properties, ByteBuf payload, Schema<T> schema) {
        String[] data = msgId.split(":");
        long ledgerId = Long.parseLong(data[0]);
        long entryId = Long.parseLong(data[1]);
        if (data.length == 3) {
            this.messageId = new BatchMessageIdImpl(ledgerId, entryId, -1, Integer.parseInt(data[2]));
        } else {
            this.messageId = new MessageIdImpl(ledgerId, entryId, -1);
        }
        this.cnx = null;
        this.payload = payload;
        this.properties = Collections.unmodifiableMap(properties);
    }

    public static MessageImpl<byte[]> deserialize(ByteBuf headersAndPayload) throws IOException {
        @SuppressWarnings("unchecked")
        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) RECYCLER.get();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);

        msg.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        msgMetadata.recycle();
        msg.payload = headersAndPayload;
        msg.messageId = null;
        msg.cnx = null;
        msg.properties = Collections.emptyMap();
        return msg;
    }

    public void setReplicatedFrom(String cluster) {
        checkNotNull(msgMetadataBuilder);
        msgMetadataBuilder.setReplicatedFrom(cluster);
    }

    public boolean isReplicated() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.hasReplicatedFrom();
    }

    public String getReplicatedFrom() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getReplicatedFrom();
    }

    @Override
    public long getPublishTime() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getPublishTime();
    }

    @Override
    public long getEventTime() {
        checkNotNull(msgMetadataBuilder);
        if (msgMetadataBuilder.hasEventTime()) {
            return msgMetadataBuilder.getEventTime();
        }
        return 0;
    }

    public boolean isExpired(int messageTTLInSeconds) {
        return messageTTLInSeconds != 0
                && System.currentTimeMillis() > (getPublishTime() + TimeUnit.SECONDS.toMillis(messageTTLInSeconds));
    }

    @Override
    public byte[] getData() {
        if (payload.arrayOffset() == 0 && payload.capacity() == payload.array().length) {
            return payload.array();
        } else {
            // Need to copy into a smaller byte array
            byte[] data = new byte[payload.readableBytes()];
            payload.readBytes(data);
            return data;
        }
    }

    @Override
    public T getValue() {
        return schema.decode(getData());
    }

    public long getSequenceId() {
        checkNotNull(msgMetadataBuilder);
        if (msgMetadataBuilder.hasSequenceId()) {
            return msgMetadataBuilder.getSequenceId();
        }
        return -1;
    }

    @Override
    public String getProducerName() {
        checkNotNull(msgMetadataBuilder);
        if (msgMetadataBuilder.hasProducerName()) {
            return msgMetadataBuilder.getProducerName();
        }
        return null;
    }

    ByteBuf getDataBuffer() {
        return payload;
    }

    @Override
    public MessageId getMessageId() {
        checkNotNull(messageId, "Cannot get the message id of a message that was not received");
        return messageId;
    }

    @Override
    public synchronized Map<String, String> getProperties() {
        if (this.properties == null) {
            if (msgMetadataBuilder.getPropertiesCount() > 0) {
                this.properties = Collections.unmodifiableMap(msgMetadataBuilder.getPropertiesList().stream()
                        .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)));
            } else {
                this.properties = Collections.emptyMap();
            }
        }
        return this.properties;
    }

    @Override
    public boolean hasProperty(String name) {
        return getProperties().containsKey(name);
    }

    @Override
    public String getProperty(String name) {
        return properties.get(name);
    }

    MessageMetadata.Builder getMessageBuilder() {
        return msgMetadataBuilder;
    }

    @Override
    public boolean hasKey() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.hasPartitionKey();
    }

    @Override
    public String getKey() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getPartitionKey();
    }

    public ClientCnx getCnx() {
        return cnx;
    }

    public void recycle() {
        msgMetadataBuilder = null;
        messageId = null;
        payload = null;
        properties = null;

        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    private MessageImpl(Handle<MessageImpl<?>> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private Handle<MessageImpl<?>> recyclerHandle;

    private final static Recycler<MessageImpl<?>> RECYCLER = new Recycler<MessageImpl<?>>() {
        @Override
        protected MessageImpl<?> newObject(Handle<MessageImpl<?>> handle) {
            return new MessageImpl<>(handle);
        }
    };

    public boolean hasReplicateTo() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getReplicateToCount() > 0;
    }

    public List<String> getReplicateTo() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getReplicateToList();
    }

    void setMessageId(MessageIdImpl messageId) {
        this.messageId = messageId;
    }
    
    public Optional<EncryptionContext> getEncryptionCtx() {
        return encryptionCtx;
    }
}
