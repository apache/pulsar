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
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

public class MessageImpl implements Message {

    private MessageMetadata.Builder msgMetadataBuilder;
    private MessageId messageId;
    private ClientCnx cnx;
    private ByteBuf payload;

    transient private Map<String, String> properties;

    // Constructor for out-going message
    static MessageImpl create(MessageMetadata.Builder msgMetadataBuilder, ByteBuffer payload) {
        MessageImpl msg = RECYCLER.get();
        msg.msgMetadataBuilder = msgMetadataBuilder;
        msg.messageId = null;
        msg.cnx = null;
        msg.payload = Unpooled.wrappedBuffer(payload);
        msg.properties = Collections.emptyMap();
        return msg;
    }

    // Constructor for incoming message
    MessageImpl(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload, int partitionIndex,
            ClientCnx cnx) {
        this.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        this.messageId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
        this.cnx = cnx;

        // Need to make a copy since the passed payload is using a ref-count buffer that we don't know when could
        // release, since the Message is passed to the user. Also, the passed ByteBuf is coming from network and is
        // backed by a direct buffer which we could not expose as a byte[]
        this.payload = Unpooled.copiedBuffer(payload);

        if (msgMetadata.getPropertiesCount() > 0) {
            Map<String, String> properties = Maps.newTreeMap();
            for (KeyValue entry : msgMetadata.getPropertiesList()) {
                properties.put(entry.getKey(), entry.getValue());
            }

            this.properties = Collections.unmodifiableMap(properties);
        } else {
            properties = Collections.emptyMap();
        }
    }

    MessageImpl(BatchMessageIdImpl batchMessageIdImpl, MessageMetadata msgMetadata,
            PulsarApi.SingleMessageMetadata singleMessageMetadata, ByteBuf payload, ClientCnx cnx) {
        this.msgMetadataBuilder = MessageMetadata.newBuilder(msgMetadata);
        this.messageId = batchMessageIdImpl;
        this.cnx = cnx;

        this.payload = Unpooled.copiedBuffer(payload);

        if (singleMessageMetadata.getPropertiesCount() > 0) {
            Map<String, String> properties = Maps.newTreeMap();
            for (KeyValue entry : singleMessageMetadata.getPropertiesList()) {
                properties.put(entry.getKey(), entry.getValue());
            }
            this.properties = Collections.unmodifiableMap(properties);
        } else {
            properties = Collections.emptyMap();
        }
    }

    public MessageImpl(String msgId, Map<String, String> properties, byte[] payload) {
        this(msgId, properties, Unpooled.wrappedBuffer(payload));
    }

    public MessageImpl(String msgId, Map<String, String> properties, ByteBuf payload) {
        long ledgerId = Long.parseLong(msgId.substring(0, msgId.indexOf(':')));
        long entryId = Long.parseLong(msgId.substring(msgId.indexOf(':') + 1));
        this.messageId = new MessageIdImpl(ledgerId, entryId, -1);
        this.cnx = null;
        this.payload = payload;
        this.properties = Collections.unmodifiableMap(properties);
    }

    public static MessageImpl deserialize(ByteBuf headersAndPayload) throws IOException {
        MessageImpl msg = RECYCLER.get();
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

    ByteBuf getDataBuffer() {
        return payload;
    }

    @Override
    public MessageId getMessageId() {
        checkNotNull(messageId, "Cannot get the message id of a message that was not received");
        return messageId;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean hasProperty(String name) {
        return properties.containsKey(name);
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
            RECYCLER.recycle(this, recyclerHandle);
        }
    }

    private MessageImpl(Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private Handle recyclerHandle;

    private final static Recycler<MessageImpl> RECYCLER = new Recycler<MessageImpl>() {
        @Override
        protected MessageImpl newObject(Handle handle) {
            return new MessageImpl(handle);
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
}
