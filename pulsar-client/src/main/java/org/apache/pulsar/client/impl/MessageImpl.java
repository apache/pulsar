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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Recycler.Handle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AbstractSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class MessageImpl<T> implements Message<T> {

    protected MessageId messageId;
    private final MessageMetadata msgMetadata;
    private ClientCnx cnx;
    private ByteBuf payload;

    private Schema<T> schema;
    private SchemaInfo schemaInfoForReplicator;
    private SchemaState schemaState = SchemaState.None;
    private Optional<EncryptionContext> encryptionCtx = Optional.empty();

    private String topic; // only set for incoming messages
    private transient Map<String, String> properties;
    private int redeliveryCount;
    private int uncompressedSize;

    private BrokerEntryMetadata brokerEntryMetadata;

    private boolean poolMessage;
    
    // Constructor for out-going message
    public static <T> MessageImpl<T> create(MessageMetadata msgMetadata, ByteBuffer payload, Schema<T> schema,
            String topic) {
        @SuppressWarnings("unchecked")
        MessageImpl<T> msg = (MessageImpl<T>) RECYCLER.get();
        msg.msgMetadata.clear();
        msg.msgMetadata.copyFrom(msgMetadata);
        msg.messageId = null;
        msg.topic = topic;
        msg.cnx = null;
        msg.payload = Unpooled.wrappedBuffer(payload);
        msg.properties = null;
        msg.schema = schema;
        msg.uncompressedSize = payload.remaining();
        return msg;
    }

    // Constructor for incoming message
    MessageImpl(String topic, MessageIdImpl messageId, MessageMetadata msgMetadata,
                ByteBuf payload, ClientCnx cnx, Schema<T> schema) {
        this(topic, messageId, msgMetadata, payload, Optional.empty(), cnx, schema);
    }

    MessageImpl(String topic, MessageIdImpl messageId, MessageMetadata msgMetadata, ByteBuf payload,
                Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema) {
        this(topic, messageId, msgMetadata, payload, encryptionCtx, cnx, schema, 0, false);
    }

    MessageImpl(String topic, MessageIdImpl messageId, MessageMetadata msgMetadata, ByteBuf payload,
                Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema, int redeliveryCount,
                boolean pooledMessage) {
        this.msgMetadata = new MessageMetadata();
        init(this, topic, messageId, msgMetadata, payload, encryptionCtx, cnx, schema, redeliveryCount, pooledMessage);
    }

    public static <T> MessageImpl<T> create(String topic, MessageIdImpl messageId, MessageMetadata msgMetadata,
            ByteBuf payload, Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema,
            int redeliveryCount, boolean pooledMessage) {
        if (pooledMessage) {
            @SuppressWarnings("unchecked")
            MessageImpl<T> msg = (MessageImpl<T>) RECYCLER.get();
            init(msg, topic, messageId, msgMetadata, payload, encryptionCtx, cnx, schema, redeliveryCount,
                    pooledMessage);
            return msg;
        } else {
            return new MessageImpl<>(topic, messageId, msgMetadata, payload, encryptionCtx, cnx, schema,
                    redeliveryCount, pooledMessage);
        }
    }

    MessageImpl(String topic, BatchMessageIdImpl batchMessageIdImpl, MessageMetadata msgMetadata,
            SingleMessageMetadata singleMessageMetadata, ByteBuf payload, Optional<EncryptionContext> encryptionCtx,
            ClientCnx cnx, Schema<T> schema) {
        this(topic, batchMessageIdImpl, msgMetadata, singleMessageMetadata, payload, encryptionCtx, cnx, schema, 0,
                false);
    }

    MessageImpl(String topic, BatchMessageIdImpl batchMessageIdImpl, MessageMetadata batchMetadata,
            SingleMessageMetadata singleMessageMetadata, ByteBuf payload, Optional<EncryptionContext> encryptionCtx,
            ClientCnx cnx, Schema<T> schema, int redeliveryCount, boolean keepMessageInDirectMemory) {
        this.msgMetadata = new MessageMetadata();
        init(this, topic, batchMessageIdImpl, batchMetadata, singleMessageMetadata, payload, encryptionCtx, cnx, schema,
                redeliveryCount, keepMessageInDirectMemory);

    }

    public static <T> MessageImpl<T> create(String topic, BatchMessageIdImpl batchMessageIdImpl,
            MessageMetadata batchMetadata, SingleMessageMetadata singleMessageMetadata, ByteBuf payload,
            Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema, int redeliveryCount,
            boolean pooledMessage) {
        if (pooledMessage) {
            @SuppressWarnings("unchecked")
            MessageImpl<T> msg = (MessageImpl<T>) RECYCLER.get();
            init(msg, topic, batchMessageIdImpl, batchMetadata, singleMessageMetadata, payload, encryptionCtx, cnx,
                    schema, redeliveryCount, pooledMessage);
            return msg;
        } else {
            return new MessageImpl<>(topic, batchMessageIdImpl, batchMetadata, singleMessageMetadata, payload,
                    encryptionCtx, cnx, schema, redeliveryCount, pooledMessage);
        }
    }

    static <T> void init(MessageImpl<T> msg, String topic, MessageIdImpl messageId, MessageMetadata msgMetadata,
            ByteBuf payload, Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema,
            int redeliveryCount, boolean poolMessage) {
        init(msg, topic, null /* batchMessageIdImpl */, msgMetadata, null /* singleMessageMetadata */, payload,
                encryptionCtx, cnx, schema, redeliveryCount, poolMessage);
        msg.messageId = messageId;
    }
    
    private static <T> void init(MessageImpl<T> msg, String topic, BatchMessageIdImpl batchMessageIdImpl,
            MessageMetadata msgMetadata, SingleMessageMetadata singleMessageMetadata, ByteBuf payload,
            Optional<EncryptionContext> encryptionCtx, ClientCnx cnx, Schema<T> schema, int redeliveryCount,
            boolean poolMessage) {
        msg.msgMetadata.clear();
        msg.msgMetadata.copyFrom(msgMetadata);
        msg.messageId = batchMessageIdImpl;
        msg.topic = topic;
        msg.cnx = cnx;
        msg.redeliveryCount = redeliveryCount;
        msg.encryptionCtx = encryptionCtx;
        msg.schema = schema;

        msg.poolMessage = poolMessage;
        // If it's not pool message then need to make a copy since the passed payload is 
        // using a ref-count buffer that we don't know when could release, since the 
        // Message is passed to the user. Also, the passed ByteBuf is coming from network 
        // and is backed by a direct buffer which we could not expose as a byte[]
        msg.payload = poolMessage ? payload.retain() : Unpooled.copiedBuffer(payload);
        
        if (singleMessageMetadata != null) {
            if (singleMessageMetadata.getPropertiesCount() > 0) {
                Map<String, String> properties = new TreeMap<>();
                for (KeyValue entry : singleMessageMetadata.getPropertiesList()) {
                    properties.put(entry.getKey(), entry.getValue());
                }
                msg.properties = Collections.unmodifiableMap(properties);
            } else {
                msg.properties = Collections.emptyMap();
            }
            if (singleMessageMetadata.hasPartitionKey()) {
                msg.msgMetadata.setPartitionKeyB64Encoded(singleMessageMetadata.isPartitionKeyB64Encoded())
                        .setPartitionKey(singleMessageMetadata.getPartitionKey());
            } else if (msg.msgMetadata.hasPartitionKey()) {
                msg.msgMetadata.clearPartitionKey();
                msg.msgMetadata.clearPartitionKeyB64Encoded();
            }

            if (singleMessageMetadata.hasOrderingKey()) {
                msg.msgMetadata.setOrderingKey(singleMessageMetadata.getOrderingKey());
            } else if (msg.msgMetadata.hasOrderingKey()) {
                msg.msgMetadata.clearOrderingKey();
            }

            if (singleMessageMetadata.hasEventTime()) {
                msg.msgMetadata.setEventTime(singleMessageMetadata.getEventTime());
            }

            if (singleMessageMetadata.hasSequenceId()) {
                msg.msgMetadata.setSequenceId(singleMessageMetadata.getSequenceId());
            }

            if (singleMessageMetadata.hasNullValue()) {
                msg.msgMetadata.setNullValue(singleMessageMetadata.isNullValue());
            }

            if (singleMessageMetadata.hasNullPartitionKey()) {
                msg.msgMetadata.setNullPartitionKey(singleMessageMetadata.isNullPartitionKey());
            }
        } else if (msgMetadata.getPropertiesCount() > 0) {
            msg.properties = Collections.unmodifiableMap(msgMetadata.getPropertiesList().stream().collect(
                    Collectors.toMap(KeyValue::getKey, KeyValue::getValue, (oldValue, newValue) -> newValue)));
        } else {
            msg.properties = Collections.emptyMap();
        }
    }
    
    public MessageImpl(String topic, String msgId, Map<String, String> properties,
            byte[] payload, Schema<T> schema, MessageMetadata msgMetadata) {
        this(topic, msgId, properties, Unpooled.wrappedBuffer(payload), schema, msgMetadata);
    }

    public MessageImpl(String topic, String msgId, Map<String, String> properties,
                       ByteBuf payload, Schema<T> schema, MessageMetadata msgMetadata) {
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
        this.msgMetadata = new MessageMetadata().copyFrom(msgMetadata);
    }

    public static MessageImpl<byte[]> deserialize(ByteBuf headersAndPayload) throws IOException {
        @SuppressWarnings("unchecked")
        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) RECYCLER.get();
        Commands.parseMessageMetadata(headersAndPayload, msg.msgMetadata);
        msg.payload = headersAndPayload;
        msg.messageId = null;
        msg.topic = null;
        msg.cnx = null;
        msg.properties = Collections.emptyMap();
        msg.brokerEntryMetadata = null;
        return msg;
    }

    public static boolean isEntryExpired(int messageTTLInSeconds, long entryTimestamp) {
        return messageTTLInSeconds != 0 &&
                (System.currentTimeMillis() > entryTimestamp + TimeUnit.SECONDS.toMillis(messageTTLInSeconds));
    }

    public static boolean isEntryPublishedEarlierThan(long entryTimestamp, long timestamp) {
        return entryTimestamp < timestamp;
    }

    public static MessageImpl<byte[]> deserializeSkipBrokerEntryMetaData(
            ByteBuf headersAndPayloadWithBrokerEntryMetadata) throws IOException {
        @SuppressWarnings("unchecked")
        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) RECYCLER.get();

        BrokerEntryMetadata brokerEntryMetadata = Commands.parseBrokerEntryMetadataIfExist(headersAndPayloadWithBrokerEntryMetadata);
        Commands.parseMessageMetadata(headersAndPayloadWithBrokerEntryMetadata, msg.msgMetadata);
        msg.payload = headersAndPayloadWithBrokerEntryMetadata;
        msg.messageId = null;
        msg.topic = null;
        msg.cnx = null;
        msg.brokerEntryMetadata = brokerEntryMetadata;
        return msg;
    }

    public void setReplicatedFrom(String cluster) {
        msgMetadata.setReplicatedFrom(cluster);
    }

    @Override
    public boolean isReplicated() {
        return msgMetadata.hasReplicatedFrom();
    }

    @Override
    public String getReplicatedFrom() {
        if (isReplicated()) {
            return msgMetadata.getReplicatedFrom();
        } else {
            return null;
        }
    }

    @Override
    public long getPublishTime() {
        return msgMetadata.getPublishTime();
    }

    @Override
    public long getEventTime() {
        if (msgMetadata.hasEventTime()) {
            return msgMetadata.getEventTime();
        }
        return 0;
    }

    public long getDeliverAtTime() {
        if (msgMetadata.hasDeliverAtTime()) {
            return msgMetadata.getDeliverAtTime();
        }
        return 0;
    }

    public boolean isExpired(int messageTTLInSeconds) {
        return messageTTLInSeconds != 0 && (brokerEntryMetadata == null || !brokerEntryMetadata.hasBrokerTimestamp()
                ? (System.currentTimeMillis() >
                    getPublishTime() + TimeUnit.SECONDS.toMillis(messageTTLInSeconds))
                : (System.currentTimeMillis() >
                    brokerEntryMetadata.getBrokerTimestamp() + TimeUnit.SECONDS.toMillis(messageTTLInSeconds)));
    }

    @Override
    public byte[] getData() {
        if (msgMetadata.isNullValue()) {
            return null;
        }
        if (payload.isDirect()) {
            byte[] data = new byte[payload.readableBytes()];
            payload.getBytes(payload.readerIndex(), data);
            return data;
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

    @Override
    public int size() {
        if (msgMetadata.isNullValue()) {
            return 0;
        }
        return payload.readableBytes();
    }

    public Schema<T> getSchemaInternal() {
        return this.schema;
    }

    @Override
    public Optional<Schema<?>> getReaderSchema() {
        ensureSchemaIsLoaded();
        if (schema == null) {
            return Optional.empty();
        }
        if (schema instanceof AutoConsumeSchema) {
            byte[] schemaVersion = getSchemaVersion();
            return Optional.of(((AutoConsumeSchema) schema)
                    .atSchemaVersion(schemaVersion));
        } else if (schema instanceof AbstractSchema) {
            byte[] schemaVersion = getSchemaVersion();
            return Optional.of(((AbstractSchema<?>) schema)
                    .atSchemaVersion(schemaVersion));
        } else {
            return Optional.of(schema);
        }
    }

    @Override
    public byte[] getSchemaVersion() {
        if (msgMetadata.hasSchemaVersion()) {
            return msgMetadata.getSchemaVersion();
        } else {
            return null;
        }
    }

    private void ensureSchemaIsLoaded() {
        if (schema instanceof AutoConsumeSchema) {
            ((AutoConsumeSchema) schema).fetchSchemaIfNeeded(BytesSchemaVersion.of(getSchemaVersion()));
        } else if (schema instanceof KeyValueSchemaImpl) {
            ((KeyValueSchemaImpl) schema).fetchSchemaIfNeeded(getTopicName(), BytesSchemaVersion.of(getSchemaVersion()));
        }
    }

    public SchemaInfo getSchemaInfo() {
        if (schema == null) {
            return null;
        }
        ensureSchemaIsLoaded();
        if (schema instanceof AutoConsumeSchema) {
            return ((AutoConsumeSchema) schema).getSchemaInfo(getSchemaVersion());
        }
        return schema.getSchemaInfo();
    }

    public void setSchemaInfoForReplicator(SchemaInfo schemaInfo) {
        if (msgMetadata.hasReplicatedFrom()) {
            this.schemaInfoForReplicator = schemaInfo;
        } else {
            throw new IllegalArgumentException("Only allowed to set schemaInfoForReplicator for a replicated message.");
        }
    }

    public SchemaInfo getSchemaInfoForReplicator() {
        return msgMetadata.hasReplicatedFrom() ? this.schemaInfoForReplicator : null;
    }

    @Override
    public T getValue() {
        SchemaInfo schemaInfo = getSchemaInfo();
        if (schemaInfo != null && SchemaType.KEY_VALUE == schemaInfo.getType()) {
            if (schema.supportSchemaVersioning()) {
                return getKeyValueBySchemaVersion();
            } else {
                return getKeyValue();
            }
        } else {
            if (msgMetadata.isNullValue()) {
                return null;
            }
            // check if the schema passed in from client supports schema versioning or not
            // this is an optimization to only get schema version when necessary
            return decode(schema.supportSchemaVersioning() ? getSchemaVersion() : null);
        }
    }


    private KeyValueSchemaImpl getKeyValueSchema() {
        if (schema instanceof AutoConsumeSchema) {
            return (KeyValueSchemaImpl) ((AutoConsumeSchema) schema).getInternalSchema(getSchemaVersion());
        } else {
            return (KeyValueSchemaImpl) schema;
        }
    }


    private T decode(byte[] schemaVersion) {
        T value = poolMessage ? schema.decode(payload.nioBuffer(), schemaVersion) : null;
        if (value != null) {
            return value;
        }
        if (null == schemaVersion) {
            return schema.decode(getData());
        } else {
            return schema.decode(getData(), schemaVersion);
        }
    }
    
    private T getKeyValueBySchemaVersion() {
        KeyValueSchemaImpl kvSchema = getKeyValueSchema();
        byte[] schemaVersion = getSchemaVersion();
        if (kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED) {
            org.apache.pulsar.common.schema.KeyValue keyValue =
                    (org.apache.pulsar.common.schema.KeyValue) kvSchema.decode(getKeyBytes(), getData(), schemaVersion);
            if (schema instanceof AutoConsumeSchema) {
                return (T) AutoConsumeSchema.wrapPrimitiveObject(keyValue,
                        ((AutoConsumeSchema) schema).getSchemaInfo(schemaVersion).getType(), schemaVersion);
            } else {
                return (T) keyValue;
            }
        } else {
            return decode(schemaVersion);
        }
    }

    private T getKeyValue() {
        KeyValueSchemaImpl kvSchema = getKeyValueSchema();
        if (kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED) {
            org.apache.pulsar.common.schema.KeyValue keyValue =
                    (org.apache.pulsar.common.schema.KeyValue) kvSchema.decode(getKeyBytes(), getData(), null);
            if (schema instanceof AutoConsumeSchema) {
                return (T) AutoConsumeSchema.wrapPrimitiveObject(keyValue,
                        ((AutoConsumeSchema) schema).getSchemaInfo(getSchemaVersion()).getType(), null);
            } else {
                return (T) keyValue;
            }
        } else {
            return decode(null);
        }
    }

    @Override
    public long getSequenceId() {
        if (msgMetadata.hasSequenceId()) {
            return msgMetadata.getSequenceId();
        }
        return -1;
    }

    @Override
    public String getProducerName() {
        if (msgMetadata.hasProducerName()) {
            return msgMetadata.getProducerName();
        }
        return null;
    }

    public ByteBuf getDataBuffer() {
        return payload;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public synchronized Map<String, String> getProperties() {
        if (this.properties == null) {
            if (msgMetadata.getPropertiesCount() > 0) {
                  this.properties = Collections.unmodifiableMap(msgMetadata.getPropertiesList().stream()
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

    public MessageMetadata getMessageBuilder() {
        return msgMetadata;
    }

    @Override
    public boolean hasKey() {
        return msgMetadata.hasPartitionKey();
    }

    @Override
    public String getTopicName() {
        return topic;
    }

    @Override
    public String getKey() {
        if (msgMetadata.hasPartitionKey()) {
            return msgMetadata.getPartitionKey();
        } else {
            return null;
        }
    }

    @Override
    public boolean hasBase64EncodedKey() {
        return msgMetadata.isPartitionKeyB64Encoded();
    }

    @Override
    public byte[] getKeyBytes() {
        if (!msgMetadata.hasPartitionKey() || msgMetadata.isNullPartitionKey()) {
            return null;
        } else if (hasBase64EncodedKey()) {
            return Base64.getDecoder().decode(getKey());
        } else {
            return getKey().getBytes(UTF_8);
        }
    }

    @Override
    public boolean hasOrderingKey() {
        return msgMetadata.hasOrderingKey();
    }

    @Override
    public byte[] getOrderingKey() {
        if (msgMetadata.hasOrderingKey()) {
            return msgMetadata.getOrderingKey();
        } else {
            return null;
        }
    }

    public BrokerEntryMetadata getBrokerEntryMetadata() {
        return brokerEntryMetadata;
    }

    public void setBrokerEntryMetadata(BrokerEntryMetadata brokerEntryMetadata) {
        this.brokerEntryMetadata = brokerEntryMetadata;
    }

    public ClientCnx getCnx() {
        return cnx;
    }

    public void recycle() {
        if (msgMetadata != null) {
            msgMetadata.clear();
        }
        if (brokerEntryMetadata != null) {
            brokerEntryMetadata.clear();
        }
        cnx = null;
        messageId = null;
        topic = null;
        payload = null;
        encryptionCtx = null;
        redeliveryCount = 0;
        uncompressedSize = 0;
        properties = null;
        schema = null;
        schemaState = SchemaState.None;
        poolMessage = false;

        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    @Override
    public void release() {
        if (poolMessage) {
            ReferenceCountUtil.safeRelease(payload);
            recycle();
        }
    }

    @Override
    public boolean hasBrokerPublishTime() {
        return brokerEntryMetadata != null && brokerEntryMetadata.hasBrokerTimestamp();
    }

    @Override
    public Optional<Long> getBrokerPublishTime() {
        if (brokerEntryMetadata != null && brokerEntryMetadata.hasBrokerTimestamp()) {
            return Optional.of(brokerEntryMetadata.getBrokerTimestamp());
        }
        return Optional.empty();
    }

    @Override
    public boolean hasIndex() {
        return brokerEntryMetadata != null && brokerEntryMetadata.hasIndex();
    }

    @Override
    public Optional<Long> getIndex() {
        if (brokerEntryMetadata != null && brokerEntryMetadata.hasIndex()) {
            if (msgMetadata.hasNumMessagesInBatch() && messageId instanceof BatchMessageIdImpl) {
                int batchSize = ((BatchMessageIdImpl) messageId).getBatchSize();
                int batchIndex = ((BatchMessageIdImpl) messageId).getBatchIndex();
                return Optional.of(brokerEntryMetadata.getIndex() - batchSize + batchIndex + 1);
            }
            return Optional.of(brokerEntryMetadata.getIndex());
        }
        return Optional.empty();
    }

    private MessageImpl(Handle<MessageImpl<?>> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
        this.redeliveryCount = 0;
        this.msgMetadata = new MessageMetadata();
        this.brokerEntryMetadata = new BrokerEntryMetadata();
    }

    private Handle<MessageImpl<?>> recyclerHandle;

    private static final Recycler<MessageImpl<?>> RECYCLER = new Recycler<MessageImpl<?>>() {
        @Override
        protected MessageImpl<?> newObject(Handle<MessageImpl<?>> handle) {
            return new MessageImpl<>(handle);
        }
    };

    public boolean hasReplicateTo() {
        return msgMetadata.getReplicateTosCount() > 0;
    }

    public List<String> getReplicateTo() {
        return msgMetadata.getReplicateTosList();
    }

    public boolean hasReplicateFrom() {
        return msgMetadata.hasReplicatedFrom();
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

    int getUncompressedSize() {
        return uncompressedSize;
    }

    SchemaState getSchemaState() {
        if (getSchemaInfo() == null) {
            return SchemaState.Ready;
        }
        return schemaState;
    }

    void setSchemaState(SchemaState schemaState) {
        this.schemaState = schemaState;
    }

    /**
     * used only for unit-test to validate payload's state and ref-cnt.
     * 
     * @return
     */
    @VisibleForTesting
    ByteBuf getPayload() {
        return payload;
    }

    enum SchemaState {
        None, Ready, Broken
    }
}
