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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawMessageImpl implements RawMessage {
    private static final Logger log = LoggerFactory.getLogger(RawMessageImpl.class);

    private final MessageIdData id;
    private ByteBuf headersAndPayload;

    public RawMessageImpl(MessageIdData id, ByteBuf headersAndPayload) {
        this.id = id;
        this.headersAndPayload = headersAndPayload.retainedSlice();
    }

    @Override
    public MessageId getMessageId() {
        return new BatchMessageIdImpl(id.getLedgerId(), id.getEntryId(),
                                      id.getPartition(), id.getBatchIndex());
    }

    @Override
    public MessageIdData getMessageIdData() {
        return id;
    }

    @Override
    public ByteBuf getHeadersAndPayload() {
        return headersAndPayload.slice();
    }

    @Override
    public void close() {
        headersAndPayload.release();
        headersAndPayload = Unpooled.EMPTY_BUFFER;
    }

    @Override
    public ByteBuf serialize() {
        ByteBuf headersAndPayload = this.headersAndPayload.slice();

        // Format: [IdSize][Id][PayloadAndMetadataSize][PayloadAndMetadata]
        int idSize = id.getSerializedSize();
        int headerSize = 4 /* IdSize */ + idSize + 4 /* PayloadAndMetadataSize */;
        int totalSize = headerSize + headersAndPayload.readableBytes();

        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(totalSize);
        buf.writeInt(idSize);
        try {
            ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);
            id.writeTo(outStream);
            outStream.recycle();
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            log.error("IO exception serializing to ByteBuf (this shouldn't happen as operation is in-memory)", e);
            throw new RuntimeException(e);
        }
        buf.writeInt(headersAndPayload.readableBytes());
        buf.writeBytes(headersAndPayload);

        return buf;
    }

    static public RawMessage deserializeFrom(ByteBuf buffer) {
        try {
            int idSize = buffer.readInt();

            int writerIndex = buffer.writerIndex();
            buffer.writerIndex(buffer.readerIndex() + idSize);
            ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(buffer);
            MessageIdData.Builder builder = MessageIdData.newBuilder();
            MessageIdData id = builder.mergeFrom(stream, null).build();
            buffer.writerIndex(writerIndex);
            builder.recycle();

            int payloadAndMetadataSize = buffer.readInt();
            ByteBuf metadataAndPayload = buffer.slice(buffer.readerIndex(), payloadAndMetadataSize);

            return new RawMessageImpl(id, metadataAndPayload);
        } catch (IOException e) {
            // This is in-memory deserialization, should not fail
            log.error("IO exception deserializing ByteBuf (this shouldn't happen as operation is in-memory)", e);
            throw new RuntimeException(e);
        }
    }
}
