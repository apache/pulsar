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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.*;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.pulsar.broker.service.ConsumerFilter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;

public class RawBatchConverter {

    public static boolean isReadableBatch(RawMessage msg) {
        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        try {
            return metadata.hasNumMessagesInBatch() && metadata.getEncryptionKeysCount() == 0;
        } finally {
            metadata.recycle();
        }
    }

    public static List<ImmutableTriple<MessageId, String, Integer>> extractIdsAndKeysAndSize(RawMessage msg)
            throws IOException {
        checkArgument(msg.getMessageIdData().getBatchIndex() == -1);

        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        int batchSize = metadata.getNumMessagesInBatch();

        CompressionType compressionType = metadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = metadata.getUncompressedSize();
        ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        metadata.recycle();

        List<ImmutableTriple<MessageId, String, Integer>> idsAndKeysAndSize = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata.newBuilder();
            ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                                                                                    singleMessageMetadataBuilder,
                                                                                    0, batchSize);
            MessageId id = new BatchMessageIdImpl(msg.getMessageIdData().getLedgerId(),
                                                  msg.getMessageIdData().getEntryId(),
                                                  msg.getMessageIdData().getPartition(),
                                                  i);
            if (!singleMessageMetadataBuilder.getCompactedOut()) {
                idsAndKeysAndSize.add(ImmutableTriple.of(id, singleMessageMetadataBuilder.getPartitionKey(), singleMessageMetadataBuilder.getPayloadSize()));
            }
            singleMessageMetadataBuilder.recycle();
            singleMessagePayload.release();
        }
        uncompressedPayload.release();
        return idsAndKeysAndSize;
    }

    /**
     * Take a batched message and a filter, and returns a message with the only the sub-messages
     * which match the filter. Returns an empty optional if no messages match.
     *
     *  NOTE: this message does not alter the reference count of the RawMessage argument.
     */
    public static Optional<RawMessage> rebatchMessage(RawMessage msg,
                                                      BiPredicate<String, MessageId> filter)
            throws IOException {
        checkArgument(msg.getMessageIdData().getBatchIndex() == -1);

        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        ByteBuf batchBuffer = PulsarByteBufAllocator.DEFAULT.buffer(payload.capacity());

        CompressionType compressionType = metadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);

        int uncompressedSize = metadata.getUncompressedSize();
        ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        try {
            int batchSize = metadata.getNumMessagesInBatch();
            int messagesRetained = 0;

            SingleMessageMetadata.Builder emptyMetadataBuilder = SingleMessageMetadata.newBuilder().setCompactedOut(true);
            for (int i = 0; i < batchSize; i++) {
                SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata.newBuilder();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                                                                                        singleMessageMetadataBuilder,
                                                                                        0, batchSize);
                MessageId id = new BatchMessageIdImpl(msg.getMessageIdData().getLedgerId(),
                                                      msg.getMessageIdData().getEntryId(),
                                                      msg.getMessageIdData().getPartition(),
                                                      i);
                if (!singleMessageMetadataBuilder.hasPartitionKey()) {
                    messagesRetained++;
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadataBuilder,
                                                                      singleMessagePayload, batchBuffer);
                } else if (filter.test(singleMessageMetadataBuilder.getPartitionKey(), id)
                           && singleMessagePayload.readableBytes() > 0) {
                    messagesRetained++;
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadataBuilder,
                                                                      singleMessagePayload, batchBuffer);
                } else {
                    Commands.serializeSingleMessageInBatchWithPayload(emptyMetadataBuilder,
                                                                      Unpooled.EMPTY_BUFFER, batchBuffer);
                }
                singleMessageMetadataBuilder.recycle();
                singleMessagePayload.release();
            }
            emptyMetadataBuilder.recycle();

            if (messagesRetained > 0) {
                int newUncompressedSize = batchBuffer.readableBytes();
                ByteBuf compressedPayload = codec.encode(batchBuffer);

                MessageMetadata.Builder metadataBuilder = metadata.toBuilder();
                metadataBuilder.setUncompressedSize(newUncompressedSize);
                MessageMetadata newMetadata = metadataBuilder.build();

                ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                                                                                  newMetadata, compressedPayload);
                Optional<RawMessage> result = Optional.of(new RawMessageImpl(msg.getMessageIdData(),
                                                                             metadataAndPayload));
                metadataBuilder.recycle();
                newMetadata.recycle();
                metadataAndPayload.release();
                compressedPayload.release();
                return result;
            } else {
                return Optional.empty();
            }
        } finally {
            uncompressedPayload.release();
            batchBuffer.release();
            metadata.recycle();
        }
    }

    /**
     * Filter messages from payload using consumer filter.
     *
     * @param payload The payload of messages.
     * @param consumerFilter The tags to OR filter on.
     * @return An optional ByteBuf that is a replacement payload or an empty option if no message matched.
     * The payload passed in is owned by the caller unless replaced by the buffer returned. In which case
     * the caller then own the returned with the one passed in released.
     * @throws IOException
     */
    public static Optional<ByteBuf> filter(ByteBuf payload, ConsumerFilter consumerFilter) throws IOException {

        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        ByteBuf batchBuffer = PulsarByteBufAllocator.DEFAULT.buffer(payload.capacity()); // Allocation could be delayed until needed.

        CompressionType compressionType = metadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);

        int uncompressedSize = metadata.getUncompressedSize();
        ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        try {
            int batchSize = metadata.getNumMessagesInBatch();
            int messagesRetained = 0;

            for (int i = 0; i < batchSize; i++) {
                SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata.newBuilder();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                        singleMessageMetadataBuilder,
                        0, batchSize);

                List<PulsarApi.KeyValue> properties = singleMessageMetadataBuilder.getPropertiesList();

                if (consumerFilter.filter(properties, singleMessagePayload) && singleMessagePayload.readableBytes() > 0) {
                    messagesRetained++;
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadataBuilder,
                            singleMessagePayload, batchBuffer);
                }
                singleMessageMetadataBuilder.recycle();
                singleMessagePayload.release();
            }

            if (messagesRetained > 0) {
                int newUncompressedSize = batchBuffer.readableBytes();
                ByteBuf compressedPayload = codec.encode(batchBuffer);

                MessageMetadata.Builder metadataBuilder = metadata.toBuilder();
                metadataBuilder.setUncompressedSize(newUncompressedSize);
                metadataBuilder.setNumMessagesInBatch(messagesRetained);
                MessageMetadata newMetadata = metadataBuilder.build();

                ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                        newMetadata, compressedPayload);

                metadataBuilder.recycle();
                newMetadata.recycle();
                compressedPayload.release();

                return Optional.of(metadataAndPayload);
            } else {
                return Optional.empty();
            }
        } finally {
            batchBuffer.release();
            metadata.recycle();
        }
    }
}
