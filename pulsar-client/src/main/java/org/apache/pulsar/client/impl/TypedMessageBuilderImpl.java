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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.client.util.TypeCheckUtil.checkType;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;

public class TypedMessageBuilderImpl<T> implements TypedMessageBuilder<T> {

    private static final long serialVersionUID = 0L;

    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);

    private final ProducerBase<?> producer;
    private final MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
    private final Schema<T> schema;
    private ByteBuffer content;
    private final TransactionImpl txn;

    public TypedMessageBuilderImpl(ProducerBase<?> producer, Schema<T> schema) {
        this(producer, schema, null);
    }

    public TypedMessageBuilderImpl(ProducerBase<?> producer,
                                   Schema<T> schema,
                                   TransactionImpl txn) {
        this.producer = producer;
        this.schema = schema;
        this.content = EMPTY_CONTENT;
        this.txn = txn;
    }

    private long beforeSend() {
        if (txn == null) {
            return -1L;
        }
        msgMetadataBuilder.setTxnidLeastBits(txn.getTxnIdLeastBits());
        msgMetadataBuilder.setTxnidMostBits(txn.getTxnIdMostBits());
        long sequenceId = txn.nextSequenceId();
        msgMetadataBuilder.setSequenceId(sequenceId);
        return sequenceId;
    }

    @Override
    public MessageId send() throws PulsarClientException {
        if (null != txn) {
            // NOTE: it makes no sense to send a transactional message in a blocking way.
            //       because #send only completes when a transaction is committed or aborted.
            throw new IllegalStateException("Use sendAsync to send a transactional message");
        }
        return producer.send(getMessage());
    }

    @Override
    public CompletableFuture<MessageId> sendAsync() {
        Message<T> message = getMessage();
        final long sequenceId = message.getSequenceId();
        CompletableFuture<MessageId> sendFuture = producer.internalSendAsync(message, txn);
        if (txn != null) {
            // register the sendFuture as part of the transaction
            return txn.registerSendOp(sequenceId, sendFuture);
        } else {
            return sendFuture;
        }
    }

    @Override
    public TypedMessageBuilder<T> key(String key) {
        if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            checkArgument(!(kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED),
                    "This method is not allowed to set keys when in encoding type is SEPARATED");
            if (key == null) {
                msgMetadataBuilder.setNullPartitionKey(true);
                return this;
            }
        }
        msgMetadataBuilder.setPartitionKey(key);
        msgMetadataBuilder.setPartitionKeyB64Encoded(false);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> keyBytes(byte[] key) {
        if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            checkArgument(!(kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED),
                    "This method is not allowed to set keys when in encoding type is SEPARATED");
            if (key == null) {
                msgMetadataBuilder.setNullPartitionKey(true);
                return this;
            }
        }
        msgMetadataBuilder.setPartitionKey(Base64.getEncoder().encodeToString(key));
        msgMetadataBuilder.setPartitionKeyB64Encoded(true);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> orderingKey(byte[] orderingKey) {
        msgMetadataBuilder.setOrderingKey(ByteString.copyFrom(orderingKey));
        return this;
    }

    @Override
    public TypedMessageBuilder<T> value(T value) {
        if (value == null) {
            msgMetadataBuilder.setNullValue(true);
            return this;
        }
        if (schema.getSchemaInfo() != null && schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            org.apache.pulsar.common.schema.KeyValue kv = (org.apache.pulsar.common.schema.KeyValue) value;
            if (kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED) {
                // set key as the message key
                if (kv.getKey() != null) {
                    msgMetadataBuilder.setPartitionKey(
                            Base64.getEncoder().encodeToString(kvSchema.getKeySchema().encode(kv.getKey())));
                    msgMetadataBuilder.setPartitionKeyB64Encoded(true);
                } else {
                    this.msgMetadataBuilder.setNullPartitionKey(true);
                }

                // set value as the payload
                if (kv.getValue() != null) {
                    this.content = ByteBuffer.wrap(kvSchema.getValueSchema().encode(kv.getValue()));
                } else {
                    this.msgMetadataBuilder.setNullValue(true);
                }
                return this;
            }
        }
        this.content = ByteBuffer.wrap(schema.encode(value));
        return this;
    }

    @Override
    public TypedMessageBuilder<T> property(String name, String value) {
        checkArgument(name != null, "Need Non-Null name");
        checkArgument(value != null, "Need Non-Null value for name: " + name);
        msgMetadataBuilder.addProperties(KeyValue.newBuilder().setKey(name).setValue(value).build());
        return this;
    }

    @Override
    public TypedMessageBuilder<T> properties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            checkArgument(entry.getKey() != null, "Need Non-Null key");
            checkArgument(entry.getValue() != null, "Need Non-Null value for key: " + entry.getKey());
            msgMetadataBuilder
                    .addProperties(KeyValue.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
        }

        return this;
    }

    @Override
    public TypedMessageBuilder<T> eventTime(long timestamp) {
        checkArgument(timestamp > 0, "Invalid timestamp : '%s'", timestamp);
        msgMetadataBuilder.setEventTime(timestamp);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> sequenceId(long sequenceId) {
        checkArgument(sequenceId >= 0);
        msgMetadataBuilder.setSequenceId(sequenceId);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> replicationClusters(List<String> clusters) {
        Preconditions.checkNotNull(clusters);
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addAllReplicateTo(clusters);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> disableReplication() {
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addReplicateTo("__local__");
        return this;
    }

    @Override
    public TypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
        return deliverAt(System.currentTimeMillis() + unit.toMillis(delay));
    }

    @Override
    public TypedMessageBuilder<T> deliverAt(long timestamp) {
        msgMetadataBuilder.setDeliverAtTime(timestamp);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypedMessageBuilder<T> loadConf(Map<String, Object> config) {
        config.forEach((key, value) -> {
            switch (key) {
                case CONF_KEY:
                    this.key(checkType(value, String.class));
                    break;
                case CONF_PROPERTIES:
                    this.properties(checkType(value, Map.class));
                    break;
                case CONF_EVENT_TIME:
                    this.eventTime(checkType(value, Long.class));
                    break;
                case CONF_SEQUENCE_ID:
                    this.sequenceId(checkType(value, Long.class));
                    break;
                case CONF_REPLICATION_CLUSTERS:
                    this.replicationClusters(checkType(value, List.class));
                    break;
                case CONF_DISABLE_REPLICATION:
                    boolean disableReplication = checkType(value, Boolean.class);
                    if (disableReplication) {
                        this.disableReplication();
                    }
                    break;
                case CONF_DELIVERY_AFTER_SECONDS:
                    this.deliverAfter(checkType(value, Long.class), TimeUnit.SECONDS);
                    break;
                case CONF_DELIVERY_AT:
                    this.deliverAt(checkType(value, Long.class));
                    break;
                default:
                    throw new RuntimeException("Invalid message config key '" + key + "'");
            }
        });
        return this;
    }

    public MessageMetadata.Builder getMetadataBuilder() {
        return msgMetadataBuilder;
    }

    public Message<T> getMessage() {
        beforeSend();
        return MessageImpl.create(msgMetadataBuilder, content, schema);
    }

    public long getPublishTime() {
        return msgMetadataBuilder.getPublishTime();
    }

    public boolean hasKey() {
        return msgMetadataBuilder.hasPartitionKey();
    }

    public String getKey() {
        return msgMetadataBuilder.getPartitionKey();
    }

    public ByteBuffer getContent() {
        return content;
    }
}
