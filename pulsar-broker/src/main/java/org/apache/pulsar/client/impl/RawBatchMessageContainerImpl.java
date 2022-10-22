/*
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
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecNone;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;

/**
 * A raw batch message container without producer. (Used for StrategicTwoPhaseCompactor)
 *
 * incoming single messages:
 * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
 *
 * batched into single batch message:
 * [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]
 */
public class RawBatchMessageContainerImpl extends BatchMessageContainerImpl {
    MessageCrypto msgCrypto;
    Set<String> encryptionKeys;
    CryptoKeyReader cryptoKeyReader;
    public RawBatchMessageContainerImpl(int maxNumMessagesInBatch) {
        super();
        compressionType = CompressionType.NONE;
        compressor = new CompressionCodecNone();
        if (maxNumMessagesInBatch > 0) {
            this.maxNumMessagesInBatch = maxNumMessagesInBatch;
        }
    }
    private ByteBuf encrypt(ByteBuf compressedPayload) {
        if (msgCrypto == null) {
            return compressedPayload;
        }
        int maxSize = msgCrypto.getMaxOutputSize(compressedPayload.readableBytes());
        ByteBuf encryptedPayload = allocator.buffer(maxSize);
        ByteBuffer targetBuffer = encryptedPayload.nioBuffer(0, maxSize);

        try {
            msgCrypto.encrypt(encryptionKeys, cryptoKeyReader, () -> messageMetadata,
                    compressedPayload.nioBuffer(), targetBuffer);
        } catch (PulsarClientException e) {
            throw new RuntimeException("Failed to encrypt payload", e);
        }
        encryptedPayload.writerIndex(targetBuffer.remaining());
        compressedPayload.release();
        return encryptedPayload;
    }

    @Override
    public ProducerImpl.OpSendMsg createOpSendMsg() {
        throw new UnsupportedOperationException();
    }
    public void setCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        this.cryptoKeyReader = cryptoKeyReader;
    }

    public ByteBuf toByteBuf() {
        if (numMessagesInBatch > 1) {
            messageMetadata.setNumMessagesInBatch(numMessagesInBatch);
            messageMetadata.setSequenceId(lowestSequenceId);
            messageMetadata.setHighestSequenceId(highestSequenceId);
        }
        MessageImpl lastMessage = messages.get(messages.size() - 1);
        MessageIdImpl lastMessageId = (MessageIdImpl) lastMessage.getMessageId();
        MessageMetadata lastMessageMetadata = lastMessage.getMessageBuilder();

        this.compressionType = lastMessageMetadata.getCompression();
        this.compressor = CompressionCodecProvider.getCompressionCodec(lastMessageMetadata.getCompression());

        if (!lastMessage.getEncryptionCtx().isEmpty()) {
            EncryptionContext encryptionContext = (EncryptionContext) lastMessage.getEncryptionCtx().get();

            if (cryptoKeyReader == null) {
                throw new IllegalStateException("Messages are encrypted but no cryptoKeyReader is provided.");
            }

            this.encryptionKeys = encryptionContext.getKeys().keySet();
            this.msgCrypto =
                    new MessageCryptoBc(String.format(
                            "[%s] [%s]", topicName, "RawBatchMessageContainer"), true);
            try {
                msgCrypto.addPublicKeyCipher(encryptionKeys, cryptoKeyReader);
            } catch (PulsarClientException.CryptoException e) {
                throw new IllegalArgumentException("Failed to set encryption keys", e);
            }
        }

        ByteBuf encryptedPayload = encrypt(getCompressedBatchMetadataAndPayload());
        updateAndReserveBatchAllocatedSize(encryptedPayload.capacity());
        ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                messageMetadata, encryptedPayload);

        MessageIdData idData = new MessageIdData();
        idData.setLedgerId(lastMessageId.getLedgerId());
        idData.setEntryId(lastMessageId.getEntryId());
        idData.setPartition(lastMessageId.getPartitionIndex());

        // Format: [IdSize][Id][metadataAndPayloadSize][metadataAndPayload]
        // Following RawMessage.serialize() format as the compacted messages will be parsed as RawMessage in broker
        int idSize = idData.getSerializedSize();
        int headerSize = 4 /* IdSize */ + idSize + 4 /* metadataAndPayloadSize */;
        int totalSize = headerSize + metadataAndPayload.readableBytes();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(totalSize);
        buf.writeInt(idSize);
        idData.writeTo(buf);
        buf.writeInt(metadataAndPayload.readableBytes());
        buf.writeBytes(metadataAndPayload);
        encryptedPayload.release();
        clear();
        return buf;
    }
}
