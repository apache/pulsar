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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;

public class RawBatchConverter {

    public static boolean isReadableBatch(RawMessage msg) {
        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        return isReadableBatch(metadata);
    }

    public static boolean isReadableBatch(MessageMetadata metadata) {
        return metadata.hasNumMessagesInBatch() && metadata.getEncryptionKeysCount() == 0;
    }

    public static List<ImmutableTriple<MessageId, String, Integer>> extractIdsAndKeysAndSize(RawMessage msg)
        throws IOException {
        return extractIdsAndKeysAndSize(msg, true);
    }

    public static List<ImmutableTriple<MessageId, String, Integer>> extractIdsAndKeysAndSize(RawMessage msg,
                                                                                             boolean extractNullKey)
            throws IOException {
        checkArgument(msg.getMessageIdData().getBatchIndex() == -1);

        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        int batchSize = metadata.getNumMessagesInBatch();

        CompressionType compressionType = metadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = metadata.getUncompressedSize();
        ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);

        List<ImmutableTriple<MessageId, String, Integer>> idsAndKeysAndSize = new ArrayList<>();

        SingleMessageMetadata smm = new SingleMessageMetadata();
        for (int i = 0; i < batchSize; i++) {
            ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                                                                                    smm,
                                                                                    0, batchSize);
            MessageId id = new BatchMessageIdImpl(msg.getMessageIdData().getLedgerId(),
                                                  msg.getMessageIdData().getEntryId(),
                                                  msg.getMessageIdData().getPartition(),
                                                  i);
            if (!smm.isCompactedOut() && (extractNullKey || smm.hasPartitionKey())) {
                idsAndKeysAndSize.add(ImmutableTriple.of(id,
                        smm.hasPartitionKey() ? smm.getPartitionKey() : null,
                        smm.hasPayloadSize() ? smm.getPayloadSize() : 0));
            }
            singleMessagePayload.release();
        }
        uncompressedPayload.release();
        return idsAndKeysAndSize;
    }

    public static Optional<RawMessage> rebatchMessage(RawMessage msg,
                                                      BiPredicate<String, MessageId> filter) throws IOException {
        return rebatchMessage(msg, filter, true);
    }

    /**
     * Take a batched message and a filter, and returns a message with the only the sub-messages
     * which match the filter. Returns an empty optional if no messages match.
     *
     *  NOTE: this message does not alter the reference count of the RawMessage argument.
     */
    public static Optional<RawMessage> rebatchMessage(RawMessage msg,
                                                      BiPredicate<String, MessageId> filter,
                                                      boolean retainNullKey)
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

            SingleMessageMetadata emptyMetadata = new SingleMessageMetadata().setCompactedOut(true);
            SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
            for (int i = 0; i < batchSize; i++) {
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                                                                                        singleMessageMetadata,
                                                                                        0, batchSize);
                MessageId id = new BatchMessageIdImpl(msg.getMessageIdData().getLedgerId(),
                                                      msg.getMessageIdData().getEntryId(),
                                                      msg.getMessageIdData().getPartition(),
                                                      i);
                if (singleMessageMetadata.isCompactedOut()) {
                    // we may read compacted out message from the compacted topic
                    Commands.serializeSingleMessageInBatchWithPayload(emptyMetadata,
                            Unpooled.EMPTY_BUFFER, batchBuffer);
                } else if (!singleMessageMetadata.hasPartitionKey()) {
                    if (retainNullKey) {
                        messagesRetained++;
                        Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadata,
                                singleMessagePayload, batchBuffer);
                    } else {
                        Commands.serializeSingleMessageInBatchWithPayload(emptyMetadata,
                                Unpooled.EMPTY_BUFFER, batchBuffer);
                    }
                } else if (filter.test(singleMessageMetadata.getPartitionKey(), id)
                           && singleMessagePayload.readableBytes() > 0) {
                    messagesRetained++;
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadata,
                                                                      singleMessagePayload, batchBuffer);
                } else {
                    Commands.serializeSingleMessageInBatchWithPayload(emptyMetadata,
                                                                      Unpooled.EMPTY_BUFFER, batchBuffer);
                }

                singleMessagePayload.release();
            }

            if (messagesRetained > 0) {
                int newUncompressedSize = batchBuffer.readableBytes();
                ByteBuf compressedPayload = codec.encode(batchBuffer);

                metadata.setUncompressedSize(newUncompressedSize);

                ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                                                                                  metadata, compressedPayload);
                Optional<RawMessage> result = Optional.of(new RawMessageImpl(msg.getMessageIdData(),
                                                                             metadataAndPayload));
                metadataAndPayload.release();
                compressedPayload.release();
                return result;
            } else {
                return Optional.empty();
            }
        } finally {
            uncompressedPayload.release();
            batchBuffer.release();
        }
    }
}
