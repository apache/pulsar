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
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

public class MessageBuilderImpl<T> implements MessageBuilder<T> {
    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);
    private final MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
    private final Schema<T> schema;
    private ByteBuffer content = EMPTY_CONTENT;

    public MessageBuilderImpl(Schema<T> schema) {
        this.schema = schema;
    }

    @Override
    public Message<T> build() {
        return MessageImpl.create(msgMetadataBuilder, content, schema);
    }

    @Override
    public MessageBuilder<T> setValue(T value) {
        return setContent(schema.encode(value));
    }

    @Override
    public MessageBuilder<T> setContent(byte[] data) {
        setContent(data, 0, data.length);
        return this;
    }

    @Override
    public MessageBuilder<T> setContent(byte[] data, int offet, int length) {
        this.content = ByteBuffer.wrap(data, offet, length);
        return this;
    }

    @Override
    public MessageBuilder<T> setContent(ByteBuffer buf) {
        this.content = buf.duplicate();
        return this;
    }

    @Override
    public MessageBuilder<T> setProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            msgMetadataBuilder
                    .addProperties(KeyValue.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
        }

        return this;
    }

    @Override
    public MessageBuilder<T> setProperty(String name, String value) {
        msgMetadataBuilder.addProperties(KeyValue.newBuilder().setKey(name).setValue(value).build());
        return this;
    }

    @Override
    public MessageBuilder<T> setKey(String key) {
        msgMetadataBuilder.setPartitionKey(key);
        return this;
    }

    @Override
    public MessageBuilder<T> setEventTime(long timestamp) {
        checkArgument(timestamp > 0, "Invalid timestamp : '%s'", timestamp);
        msgMetadataBuilder.setEventTime(timestamp);
        return this;
    }

    @Override
    public MessageBuilder<T> setSequenceId(long sequenceId) {
        checkArgument(sequenceId >= 0);
        msgMetadataBuilder.setSequenceId(sequenceId);
        return this;
    }

    @Override
    public MessageBuilder<T> setReplicationClusters(List<String> clusters) {
        Preconditions.checkNotNull(clusters);
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addAllReplicateTo(clusters);
        return this;
    }

    @Override
    public MessageBuilder<T> disableReplication() {
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addReplicateTo("__local__");
        return this;
    }


}
