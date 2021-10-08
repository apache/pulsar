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
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;

@SuppressWarnings("deprecation")
public class MessageBuilderImpl implements MessageBuilder {
    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);
    private final MessageMetadata msgMetadataBuilder = new MessageMetadata();
    private ByteBuffer content = EMPTY_CONTENT;

    @Override
    public Message<byte[]> build() {
        return MessageImpl.create(msgMetadataBuilder, content, Schema.BYTES, null);
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
            msgMetadataBuilder.addProperty()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue());
        }

        return this;
    }

    @Override
    public MessageBuilder setProperty(String name, String value) {
        msgMetadataBuilder.addProperty()
                .setKey(name)
                .setValue(value);
        return this;
    }

    @Override
    public MessageBuilder setKey(String key) {
        msgMetadataBuilder.setPartitionKey(key);
        return this;
    }

    @Override
    public MessageBuilder setEventTime(long timestamp) {
        checkArgument(timestamp > 0, "Invalid timestamp : '%s'", timestamp);
        msgMetadataBuilder.setEventTime(timestamp);
        return this;
    }

    @Override
    public MessageBuilder setSequenceId(long sequenceId) {
        checkArgument(sequenceId >= 0);
        msgMetadataBuilder.setSequenceId(sequenceId);
        return this;
    }

    @Override
    public MessageBuilder setReplicationClusters(List<String> clusters) {
        Preconditions.checkNotNull(clusters);
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addAllReplicateTos(clusters);
        return this;
    }

    @Override
    public MessageBuilder disableReplication() {
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addReplicateTo("__local__");
        return this;
    }


}
