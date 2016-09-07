/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageBuilder;
import com.yahoo.pulsar.common.api.PulsarDecoder;
import com.yahoo.pulsar.common.api.proto.PulsarApi.KeyValue;
import com.yahoo.pulsar.common.api.proto.PulsarApi.MessageMetadata;

public class MessageBuilderImpl implements MessageBuilder {

    private final MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
    private ByteBuffer content;

    @Override
    public Message build() {
        if (content.remaining() > PulsarDecoder.MaxMessageSize) {
            throw new IllegalArgumentException(
                    "Message payload cannot exceed " + PulsarDecoder.MaxMessageSize + " bytes");
        }
        return MessageImpl.create(msgMetadataBuilder, content);
    }

    @Override
    public MessageBuilder setContent(byte[] data) {
        setContent(data, 0, data.length);
        return this;
    }

    @Override
    public MessageBuilder setContent(byte[] data, int offet, int length) {
        this.content = ByteBuffer.wrap(data, offet, length);
        return this;
    }

    @Override
    public MessageBuilder setContent(ByteBuffer buf) {
        this.content = buf.duplicate();
        return this;
    }

    @Override
    public MessageBuilder setProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            msgMetadataBuilder
                    .addProperties(KeyValue.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
        }

        return this;
    }

    @Override
    public MessageBuilder setProperty(String name, String value) {
        msgMetadataBuilder.addProperties(KeyValue.newBuilder().setKey(name).setValue(value).build());
        return this;
    }

    @Override
    public MessageBuilder setKey(String key) {
        msgMetadataBuilder.setPartitionKey(key);
        return this;
    }

    @Override
    public MessageBuilder setReplicationClusters(List<String> clusters) {
        Preconditions.checkNotNull(clusters);
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addAllReplicateTo(clusters);
        return this;
    }

    @Override
    public MessageBuilder disableReplication() {
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addReplicateTo("__local__");
        return this;
    }
}
