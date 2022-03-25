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
package org.apache.pulsar.common.api.raw;

import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Helper class to work with a raw Pulsar entry payload.
 */
@UtilityClass
@Slf4j
public class MessageParser {

    private static final FastThreadLocal<SingleMessageMetadata> LOCAL_SINGLE_MESSAGE_METADATA = //
            new FastThreadLocal<SingleMessageMetadata>() {
                protected SingleMessageMetadata initialValue() throws Exception {
                    return new SingleMessageMetadata();
                };
            };

    /**
     * Definition of an interface to process a raw Pulsar entry payload.
     */
    public interface MessageProcessor {
        void process(RawMessage message) throws IOException;
    }

    /**
     * Parse a raw Pulsar entry payload and extract all the individual message that may be included in the batch. The
     * provided {@link MessageProcessor} will be invoked for each individual message.
     */
    public static void parseMessage(TopicName topicName, long ledgerId, long entryId, ByteBuf headersAndPayload,
            MessageProcessor processor, int maxMessageSize) throws IOException {
        ByteBuf payload = headersAndPayload;
        ByteBuf uncompressedPayload = null;
        ReferenceCountedMessageMetadata refCntMsgMetadata = null;

        try {
            if (!verifyChecksum(topicName, headersAndPayload, ledgerId, entryId)) {
                // discard message with checksum error
                return;
            }

            refCntMsgMetadata = ReferenceCountedMessageMetadata.get(headersAndPayload);
            MessageMetadata msgMetadata = refCntMsgMetadata.getMetadata();

            try {
                Commands.parseMessageMetadata(payload, msgMetadata);
            } catch (Throwable t) {
                log.warn("[{}] Failed to deserialize metadata for message {}:{} - Ignoring",
                    topicName, ledgerId, entryId);
                return;
            }

            if (msgMetadata.hasMarkerType()) {
                // Ignore marker messages as they don't contain user data
                return;
            }

            if (msgMetadata.getEncryptionKeysCount() > 0) {
                throw new IOException("Cannot parse encrypted message " + msgMetadata + " on topic " + topicName);
            }

            uncompressedPayload = uncompressPayloadIfNeeded(topicName, msgMetadata, headersAndPayload, ledgerId,
                    entryId, maxMessageSize);

            if (uncompressedPayload == null) {
                // Message was discarded on decompression error
                return;
            }

            final int numMessages = msgMetadata.getNumMessagesInBatch();

            if (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch()) {
                processor.process(
                    RawMessageImpl.get(refCntMsgMetadata, null, uncompressedPayload.retain(), ledgerId, entryId, 0));
            } else {
                // handle batch message enqueuing; uncompressed payload has all messages in batch
                receiveIndividualMessagesFromBatch(
                    refCntMsgMetadata, uncompressedPayload, ledgerId, entryId, processor);
            }


        } finally {
            ReferenceCountUtil.safeRelease(uncompressedPayload);
            ReferenceCountUtil.safeRelease(refCntMsgMetadata);
        }
    }

    public static boolean verifyChecksum(TopicName topic, ByteBuf headersAndPayload, long ledgerId, long entryId) {
        if (hasChecksum(headersAndPayload)) {
            int checksum = readChecksum(headersAndPayload);
            int computedChecksum = computeChecksum(headersAndPayload);
            if (checksum != computedChecksum) {
                log.error(
                        "[{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
                        topic, ledgerId, entryId, Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }

    public static ByteBuf uncompressPayloadIfNeeded(TopicName topic, MessageMetadata msgMetadata,
            ByteBuf payload, long ledgerId, long entryId, int maxMessageSize) {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(msgMetadata.getCompression());
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (payloadSize > maxMessageSize) {
            // payload size is itself corrupted since it cannot be bigger than the MaxMessageSize
            log.error("[{}] Got corrupted payload message size {} at {}:{}", topic, payloadSize,
                    ledgerId, entryId);
            return null;
        }

        try {
            ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
            return uncompressedPayload;
        } catch (IOException e) {
            log.error("[{}] Failed to decompress message with {} at {}:{} : {}", topic,
                    msgMetadata.getCompression(), ledgerId, entryId, e.getMessage(), e);
            return null;
        }
    }

    private static void receiveIndividualMessagesFromBatch(ReferenceCountedMessageMetadata msgMetadata,
            ByteBuf uncompressedPayload, long ledgerId, long entryId, MessageProcessor processor) {
        int batchSize = msgMetadata.getMetadata().getNumMessagesInBatch();

        try {
            for (int i = 0; i < batchSize; ++i) {
               SingleMessageMetadata singleMessageMetadata = LOCAL_SINGLE_MESSAGE_METADATA.get();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                        singleMessageMetadata, i, batchSize);

                if (singleMessageMetadata.isCompactedOut()) {
                    // message has been compacted out, so don't send to the user
                    singleMessagePayload.release();
                    continue;
                }

                processor.process(RawMessageImpl.get(msgMetadata, singleMessageMetadata, singleMessagePayload,
                        ledgerId, entryId, i));
            }
        } catch (IOException e) {
            log.warn("Unable to obtain messages in batch", e);
        }
    }

}
