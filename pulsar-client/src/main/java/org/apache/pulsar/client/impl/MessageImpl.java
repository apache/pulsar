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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;

public class MessageImpl<T> implements Message<T> {

    protected MessageId messageId;
    private MessageMetadata.Builder msgMetadataBuilder;
    private ClientCnx cnx;
    private ByteBuf payload;
    private Schema<T> schema;
    private SchemaState schemaState = SchemaState.None;
    private Optional<EncryptionContext> encryptionCtx = Optional.empty();

    private String topic; // only set for incoming messages
    transient private Map<String, String> properties;
    private final int redeliveryCount;

    // Constructor for out-going message
    public static <T> MessageImpl<T> create(MessageMetadata.Builder msgMetadataBuilder, ByteBuffer payload, Schema<T> schema) {
        @SuppressWarnings("unchecked")
        MessageImpl<T> msg = (MessageImpl<T>) RECYCLER.get();
        msg.msgMetadataBuilder = msgMetadataBuilder;
        msg.messageId = null;
        msg.topic = null;
        msg.cnx = null;
        msg.payload = Unpooled.wrappedBuffer(payload);
        msg.properties = null;
        msg.schema = schema;
        return msg;
    }

    // Constructor for incoming message
    MessageImpl(String topic, MessageIdImpl messageId, MessageMetadata msgMetadata,
                ByteBuf payload, ClientCnx cnx, Schema<T> schema) {
        this(topic, messageId, msgMetadata, payload, Optional.empty(), cnx, schema);
    }

    MessageImpl(String topic, MessageIdImpl messageId, MessageMetadata msgMetadata, ByteBuf payload,
                Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema) {
        this(topic, messageId, msgMetadata, payload, encryptionCtx, cnx, schema, 0);
    }

    MessageImpl(String topic, MessageIdImpl messageId, MessageMetadata msgMetadata, ByteBuf payload,
                Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema, int redeliveryCount) {
        this.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        this.messageId = messageId;
        this.topic = topic;
        this.cnx = cnx;
        this.redeliveryCount = redeliveryCount;

        // Need to make a copy since the passed payload is using a ref-count buffer that we don't know when could
        // release, since the Message is passed to the user. Also, the passed ByteBuf is coming from network and is
        // backed by a direct buffer which we could not expose as a byte[]
        this.payload = Unpooled.copiedBuffer(payload);
        this.encryptionCtx = encryptionCtx;

        if (msgMetadata.getPropertiesCount() > 0) {
            this.properties = Collections.unmodifiableMap(msgMetadataBuilder.getPropertiesList().stream()
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue,
                            (oldValue,newValue) -> newValue)));
        } else {
            properties = Collections.emptyMap();
        }
        this.schema = schema;
    }

    MessageImpl(String topic, BatchMessageIdImpl batchMessageIdImpl, MessageMetadata msgMetadata,
                PulsarApi.SingleMessageMetadata singleMessageMetadata, ByteBuf payload,
                Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema) {
        this(topic, batchMessageIdImpl, msgMetadata, singleMessageMetadata, payload, encryptionCtx, cnx, schema, 0);
    }

    MessageImpl(String topic, BatchMessageIdImpl batchMessageIdImpl, MessageMetadata msgMetadata,
                PulsarApi.SingleMessageMetadata singleMessageMetadata, ByteBuf payload,
                Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema, int redeliveryCount) {
        this.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        this.messageId = batchMessageIdImpl;
        this.topic = topic;
        this.cnx = cnx;
        this.redeliveryCount = redeliveryCount;

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
            msgMetadataBuilder.setPartitionKeyB64Encoded(singleMessageMetadata.getPartitionKeyB64Encoded());
            msgMetadataBuilder.setPartitionKey(singleMessageMetadata.getPartitionKey());
        } else if (msgMetadataBuilder.hasPartitionKey()) {
            msgMetadataBuilder.clearPartitionKey();
            msgMetadataBuilder.clearPartitionKeyB64Encoded();
        }

        if (singleMessageMetadata.hasOrderingKey()) {
            msgMetadataBuilder.setOrderingKey(singleMessageMetadata.getOrderingKey());
        } else if (msgMetadataBuilder.hasOrderingKey()) {
            msgMetadataBuilder.clearOrderingKey();
        }

        if (singleMessageMetadata.hasEventTime()) {
            msgMetadataBuilder.setEventTime(singleMessageMetadata.getEventTime());
        }

        if (singleMessageMetadata.hasSequenceId()) {
            msgMetadataBuilder.setSequenceId(singleMessageMetadata.getSequenceId());
        }

        if (singleMessageMetadata.hasNullValue()) {
            msgMetadataBuilder.setNullValue(singleMessageMetadata.hasNullValue());
        }

        if (singleMessageMetadata.hasNullPartitionKey()) {
            msgMetadataBuilder.setNullPartitionKey(singleMessageMetadata.hasNullPartitionKey());
        }

        this.schema = schema;
    }

    public MessageImpl(String topic, String msgId, Map<String, String> properties,
            byte[] payload, Schema<T> schema, MessageMetadata.Builder msgMetadataBuilder) {
        this(topic, msgId, properties, Unpooled.wrappedBuffer(payload), schema, msgMetadataBuilder);
    }

    public MessageImpl(String topic, String msgId, Map<String, String> properties,
                       ByteBuf payload, Schema<T> schema, MessageMetadata.Builder msgMetadataBuilder) {
        String[] data = msgId.split(":");
        long ledgerId = Long.parseLong(data[0]);
        long entryId = Long.parseLong(data[1]);
        if (data.length == 3) {
            this.messageId = new BatchMessageIdImpl(ledgerId, entryId, -1, Integer.parseInt(data[2]));
        } else {
            this.messageId = new MessageIdImpl(ledgerId, entryId, -1);
        }
        this.topic = topic;
        this.cnx = null;
        this.payload = payload;
        this.properties = Collections.unmodifiableMap(properties);
        this.schema = schema;
        this.redeliveryCount = 0;
        this.msgMetadataBuilder = msgMetadataBuilder;
    }

    public static MessageImpl<byte[]> deserialize(ByteBuf headersAndPayload) throws IOException {
        @SuppressWarnings("unchecked")
        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) RECYCLER.get();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);

        msg.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        msgMetadata.recycle();
        msg.payload = headersAndPayload;
        msg.messageId = null;
        msg.topic = null;
        msg.cnx = null;
        msg.properties = Collections.emptyMap();
        return msg;
    }

    public void setReplicatedFrom(String cluster) {
        checkNotNull(msgMetadataBuilder);
        msgMetadataBuilder.setReplicatedFrom(cluster);
    }

    @Override
    public boolean isReplicated() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.hasReplicatedFrom();
    }

    @Override
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
        checkNotNull(msgMetadataBuilder);
        if (msgMetadataBuilder.hasNullValue()) {
            return null;
        }
        if (payload.arrayOffset() == 0 && payload.capacity() == payload.array().length) {
            return payload.array();
        } else {
            // Need to copy into a smaller byte array
            byte[] data = new byte[payload.readableBytes()];
            payload.readBytes(data);
            return data;
        }
    }

    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public byte[] getSchemaVersion() {
        if (msgMetadataBuilder != null && msgMetadataBuilder.hasSchemaVersion()) {
            return msgMetadataBuilder.getSchemaVersion().toByteArray();
        } else {
            return null;
        }
    }

    @Override
    public T getValue() {
        checkNotNull(msgMetadataBuilder);
        if (schema.getSchemaInfo() != null && SchemaType.KEY_VALUE == schema.getSchemaInfo().getType()) {
            if (schema.supportSchemaVersioning()) {
                return getKeyValueBySchemaVersion();
            } else {
                return getKeyValue();
            }
        } else {
            if (msgMetadataBuilder.hasNullValue()) {
                return null;
            }
            // check if the schema passed in from client supports schema versioning or not
            // this is an optimization to only get schema version when necessary
            if (schema.supportSchemaVersioning()) {
                byte[] schemaVersion = getSchemaVersion();
                if (null == schemaVersion) {
                    return schema.decode(getData());
                } else {
                    return schema.decode(getData(), schemaVersion);
                }
            } else {
                return schema.decode(getData());
            }
        }
    }

    private T getKeyValueBySchemaVersion() {
        KeyValueSchema kvSchema = (KeyValueSchema) schema;
        byte[] schemaVersion = getSchemaVersion();
        if (kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED) {
            return (T) kvSchema.decode(
                    msgMetadataBuilder.hasNullPartitionKey() ? null : getKeyBytes(),
                    msgMetadataBuilder.hasNullValue() ? null : getData(), schemaVersion);
        } else {
            return schema.decode(getData(), schemaVersion);
        }
    }

    private T getKeyValue() {
        KeyValueSchema kvSchema = (KeyValueSchema) schema;
        if (kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED) {
            return (T) kvSchema.decode(
                    msgMetadataBuilder.hasNullPartitionKey() ? null : getKeyBytes(),
                    msgMetadataBuilder.hasNullValue() ? null : getData(), null);
        } else {
            return schema.decode(getData());
        }
    }

    @Override
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

    public ByteBuf getDataBuffer() {
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
                           .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue,
                                   (oldValue,newValue) -> newValue)));
                
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
        return this.getProperties().get(name);
    }

    public MessageMetadata.Builder getMessageBuilder() {
        return msgMetadataBuilder;
    }

    @Override
    public boolean hasKey() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.hasPartitionKey();
    }

    @Override
    public String getTopicName() {
        return topic;
    }

    @Override
    public String getKey() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getPartitionKey();
    }

    @Override
    public boolean hasBase64EncodedKey() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getPartitionKeyB64Encoded();
    }

    @Override
    public byte[] getKeyBytes() {
        checkNotNull(msgMetadataBuilder);
        if (hasBase64EncodedKey()) {
            return Base64.getDecoder().decode(getKey());
        } else {
            return getKey().getBytes(UTF_8);
        }
    }

    @Override
    public boolean hasOrderingKey() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.hasOrderingKey();
    }

    @Override
    public byte[] getOrderingKey() {
        checkNotNull(msgMetadataBuilder);
        return msgMetadataBuilder.getOrderingKey().toByteArray();
    }

    public ClientCnx getCnx() {
        return cnx;
    }

    public void recycle() {
        msgMetadataBuilder = null;
        messageId = null;
        topic = null;
        payload = null;
        properties = null;
        schema = null;
        schemaState = SchemaState.None;

        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    private MessageImpl(Handle<MessageImpl<?>> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
        this.redeliveryCount = 0;
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

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        return encryptionCtx;
    }

    @Override
    public int getRedeliveryCount() {
        return redeliveryCount;
    }

    SchemaState getSchemaState() {
        return schemaState;
    }

    void setSchemaState(SchemaState schemaState) {
        this.schemaState = schemaState;
    }

    enum SchemaState {
        None, Ready, Broken
    }
}
