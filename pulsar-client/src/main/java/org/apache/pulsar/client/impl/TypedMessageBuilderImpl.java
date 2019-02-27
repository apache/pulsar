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

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

public class TypedMessageBuilderImpl<T> implements TypedMessageBuilder<T> {
    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);

    private final ProducerBase<T> producer;
    private final MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
    private final Schema<T> schema;
    private ByteBuffer content;

    public TypedMessageBuilderImpl(ProducerBase<T> producer, Schema<T> schema) {
        this.producer = producer;
        this.schema = schema;
        this.content = EMPTY_CONTENT;
    }

    @Override
    public MessageId send() throws PulsarClientException {
        return producer.send(getMessage());
    }

    @Override
    public CompletableFuture<MessageId> sendAsync() {
        return producer.internalSendAsync(getMessage());
    }

    @Override
    public TypedMessageBuilder<T> key(String key) {
        msgMetadataBuilder.setPartitionKey(key);
        msgMetadataBuilder.setPartitionKeyB64Encoded(false);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> keyBytes(byte[] key) {
        msgMetadataBuilder.setPartitionKey(Base64.getEncoder().encodeToString(key));
        msgMetadataBuilder.setPartitionKeyB64Encoded(true);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> value(T value) {
        checkArgument(value != null, "Need Non-Null content value");
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

    public MessageMetadata.Builder getMetadataBuilder() {
        return msgMetadataBuilder;
    }

    public Message<T> getMessage() {
        return (Message<T>) MessageImpl.create(msgMetadataBuilder, content, schema);
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
