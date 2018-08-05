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

import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static org.apache.pulsar.common.api.Commands.hasChecksum;
import static org.apache.pulsar.common.api.Commands.readChecksum;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Optional;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;

@UtilityClass
@Slf4j
public class MessageParser {
    public interface MessageProcessor {
        void process(MessageId messageId, Message<?> message, ByteBuf payload);
    }

    /**
     * Parse a raw Pulsar entry payload and extract all the individual message that may be included in the batch. The
     * provided {@link MessageProcessor} will be invoked for each individual message.
     */
    public static void parseMessage(TopicName topicName, long ledgerId, long entryId, ByteBuf headersAndPayload,
            MessageProcessor processor) throws IOException {
        MessageIdImpl msgId = new MessageIdImpl(ledgerId, entryId, -1);

        MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
        MessageIdData messageId = messageIdBuilder.build();

        MessageMetadata msgMetadata = null;
        ByteBuf payload = headersAndPayload;
        ByteBuf uncompressedPayload = null;

        try {
            if (!verifyChecksum(headersAndPayload, messageId, topicName.toString(), "reader")) {
                // discard message with checksum error
                return;
            }

            try {
                msgMetadata = Commands.parseMessageMetadata(payload);
            } catch (Throwable t) {
                log.warn("[{}] Failed to deserialize metadata for message {} - Ignoring", topicName, messageId);
                return;
            }

            if (msgMetadata.getEncryptionKeysCount() > 0) {
                throw new IOException("Cannot parse encrypted message " + msgMetadata + " on topic " + topicName);
            }

            uncompressedPayload = uncompressPayloadIfNeeded(messageId, msgMetadata, headersAndPayload,
                    topicName.toString(), "reader");

            if (uncompressedPayload == null) {
                // Message was discarded on decompression error
                return;
            }

            final int numMessages = msgMetadata.getNumMessagesInBatch();

            if (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch()) {
                final MessageImpl<?> message = new MessageImpl<>(msgId, msgMetadata, uncompressedPayload, null, null);
                processor.process(msgId, message, uncompressedPayload);

                uncompressedPayload.release();

            } else {
                // handle batch message enqueuing; uncompressed payload has all messages in batch
                receiveIndividualMessagesFromBatch(msgMetadata, uncompressedPayload, messageId, null, -1, processor);
                uncompressedPayload.release();
            }

        } finally {
            if (uncompressedPayload != null) {
                uncompressedPayload.release();
            }

            messageIdBuilder.recycle();
            messageId.recycle();
            msgMetadata.recycle();
        }
    }

    public static boolean verifyChecksum(ByteBuf headersAndPayload, MessageIdData messageId, String topic,
            String subscription) {
        if (hasChecksum(headersAndPayload)) {
            int checksum = readChecksum(headersAndPayload);
            int computedChecksum = computeChecksum(headersAndPayload);
            if (checksum != computedChecksum) {
                log.error(
                        "[{}][{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
                        topic, subscription, messageId.getLedgerId(), messageId.getEntryId(),
                        Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }

    public static ByteBuf uncompressPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata,
            ByteBuf payload, String topic, String subscription) {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(msgMetadata.getCompression());
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (payloadSize > PulsarDecoder.MaxMessageSize) {
            // payload size is itself corrupted since it cannot be bigger than the MaxMessageSize
            log.error("[{}][{}] Got corrupted payload message size {} at {}", topic, subscription, payloadSize,
                    messageId);
            return null;
        }

        try {
            ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
            return uncompressedPayload;
        } catch (IOException e) {
            log.error("[{}][{}] Failed to decompress message with {} at {}: {}", topic, subscription,
                    msgMetadata.getCompression(), messageId, e.getMessage(), e);
            return null;
        }
    }

    public static void receiveIndividualMessagesFromBatch(MessageMetadata msgMetadata, ByteBuf uncompressedPayload,
            MessageIdData messageId, ClientCnx cnx, int partitionIndex, MessageProcessor processor) {
        int batchSize = msgMetadata.getNumMessagesInBatch();

        try {
            for (int i = 0; i < batchSize; ++i) {
                PulsarApi.SingleMessageMetadata.Builder singleMessageMetadataBuilder = PulsarApi.SingleMessageMetadata
                        .newBuilder();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                        singleMessageMetadataBuilder, i, batchSize);

                if (singleMessageMetadataBuilder.getCompactedOut()) {
                    // message has been compacted out, so don't send to the user
                    singleMessagePayload.release();
                    singleMessageMetadataBuilder.recycle();

                    continue;
                }

                BatchMessageIdImpl batchMessageIdImpl = new BatchMessageIdImpl(messageId.getLedgerId(),
                        messageId.getEntryId(), partitionIndex, i, null);
                final MessageImpl<?> message = new MessageImpl<>(batchMessageIdImpl, msgMetadata,
                        singleMessageMetadataBuilder.build(), singleMessagePayload, Optional.empty(), cnx, null);

                processor.process(batchMessageIdImpl, message, singleMessagePayload);

                singleMessagePayload.release();
                singleMessageMetadataBuilder.recycle();
            }
        } catch (IOException e) {
            log.warn("Unable to obtain messages in batch", e);
        }
    }

}
