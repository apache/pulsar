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

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;

public class TypedMessageBuilderImpl<T> implements TypedMessageBuilder<T> {

    private static final long serialVersionUID = 0L;

    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);

    private transient final ProducerBase<?> producer;
    private transient final MessageMetadata msgMetadata = new MessageMetadata();
    private transient final Schema<T> schema;
    private transient ByteBuffer content;
    private transient final TransactionImpl txn;

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
        msgMetadata.setTxnidLeastBits(txn.getTxnIdLeastBits());
        msgMetadata.setTxnidMostBits(txn.getTxnIdMostBits());
        return -1L;
    }

    @Override
    public MessageId send() throws PulsarClientException {
        try {
            // enqueue the message to the buffer
            CompletableFuture<MessageId> sendFuture = sendAsync();

            if (!sendFuture.isDone()) {
                // the send request wasn't completed yet (e.g. not failing at enqueuing), then attempt to triggerFlush it out
                producer.triggerFlush();
            }

            return sendFuture.get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<MessageId> sendAsync() {
        Message<T> message = getMessage();
        CompletableFuture<MessageId> sendFuture;
        if (txn != null) {
            sendFuture = producer.internalSendWithTxnAsync(message, txn);
            txn.registerSendOp(sendFuture);
        } else {
            sendFuture = producer.internalSendAsync(message);
        }
        return sendFuture;
    }

    @Override
    public TypedMessageBuilder<T> key(String key) {
        if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchemaImpl kvSchema = (KeyValueSchemaImpl) schema;
            checkArgument(kvSchema.getKeyValueEncodingType() != KeyValueEncodingType.SEPARATED,
                    "This method is not allowed to set keys when in encoding type is SEPARATED");
            if (key == null) {
                msgMetadata.setNullPartitionKey(true);
                return this;
            }
        }
        msgMetadata.setPartitionKey(key);
        msgMetadata.setPartitionKeyB64Encoded(false);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> keyBytes(byte[] key) {
        if (schema instanceof KeyValueSchemaImpl && schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchemaImpl kvSchema = (KeyValueSchemaImpl) schema;
            checkArgument(!(kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED),
                    "This method is not allowed to set keys when in encoding type is SEPARATED");
            if (key == null) {
                msgMetadata.setNullPartitionKey(true);
                return this;
            }
        }
        msgMetadata.setPartitionKey(Base64.getEncoder().encodeToString(key));
        msgMetadata.setPartitionKeyB64Encoded(true);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> orderingKey(byte[] orderingKey) {
        msgMetadata.setOrderingKey(orderingKey);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> value(T value) {
        if (value == null) {
            msgMetadata.setNullValue(true);
            return this;
        }
        if (value instanceof org.apache.pulsar.common.schema.KeyValue
                && schema.getSchemaInfo() != null && schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchemaImpl kvSchema = (KeyValueSchemaImpl) schema;
            org.apache.pulsar.common.schema.KeyValue kv = (org.apache.pulsar.common.schema.KeyValue) value;
            if (kvSchema.getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED) {
                // set key as the message key
                if (kv.getKey() != null) {
                    msgMetadata.setPartitionKey(
                            Base64.getEncoder().encodeToString(kvSchema.getKeySchema().encode(kv.getKey())));
                    msgMetadata.setPartitionKeyB64Encoded(true);
                } else {
                    this.msgMetadata.setNullPartitionKey(true);
                }

                // set value as the payload
                if (kv.getValue() != null) {
                    this.content = ByteBuffer.wrap(kvSchema.getValueSchema().encode(kv.getValue()));
                } else {
                    this.msgMetadata.setNullValue(true);
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
        msgMetadata.addProperty()
                    .setKey(name)
                    .setValue(value);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> properties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            checkArgument(entry.getKey() != null, "Need Non-Null key");
            checkArgument(entry.getValue() != null, "Need Non-Null value for key: " + entry.getKey());
            msgMetadata.addProperty()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue());
        }

        return this;
    }

    @Override
    public TypedMessageBuilder<T> eventTime(long timestamp) {
        checkArgument(timestamp > 0, "Invalid timestamp : '%s'", timestamp);
        msgMetadata.setEventTime(timestamp);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> sequenceId(long sequenceId) {
        checkArgument(sequenceId >= 0);
        msgMetadata.setSequenceId(sequenceId);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> replicationClusters(List<String> clusters) {
        Objects.requireNonNull(clusters);
        msgMetadata.clearReplicateTo();
        msgMetadata.addAllReplicateTos(clusters);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> disableReplication() {
        msgMetadata.clearReplicateTo();
        msgMetadata.addReplicateTo("__local__");
        return this;
    }

    @Override
    public TypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
        return deliverAt(System.currentTimeMillis() + unit.toMillis(delay));
    }

    @Override
    public TypedMessageBuilder<T> deliverAt(long timestamp) {
        msgMetadata.setDeliverAtTime(timestamp);
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

    public MessageMetadata getMetadataBuilder() {
        return msgMetadata;
    }

    public Message<T> getMessage() {
        beforeSend();
        return MessageImpl.create(msgMetadata, content, schema, producer != null ? producer.getTopic() : null);
    }

    public long getPublishTime() {
        return msgMetadata.getPublishTime();
    }

    public boolean hasKey() {
        return msgMetadata.hasPartitionKey();
    }

    public String getKey() {
        return msgMetadata.getPartitionKey();
    }

    public ByteBuffer getContent() {
        return content;
    }
}
