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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageIdAdv;
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
    private MessageCrypto<MessageMetadata, MessageMetadata> msgCrypto;
    private Set<String> encryptionKeys;
    private CryptoKeyReader cryptoKeyReader;
    private MessageIdAdv lastAddedMessageId;

    public RawBatchMessageContainerImpl() {
        super();
        this.compressionType = CompressionType.NONE;
        this.compressor = new CompressionCodecNone();
    }

    private ByteBuf encrypt(ByteBuf compressedPayload) {
        if (msgCrypto == null) {
            return compressedPayload;
        }
        ByteBuf encryptedPayload = null;
        try {
            int maxSize = msgCrypto.getMaxOutputSize(compressedPayload.readableBytes());
            encryptedPayload = allocator.buffer(maxSize);
            ByteBuffer targetBuffer = encryptedPayload.nioBuffer(0, maxSize);
            msgCrypto.encrypt(encryptionKeys, cryptoKeyReader, () -> messageMetadata,
                    compressedPayload.nioBuffer(), targetBuffer);
            encryptedPayload.writerIndex(targetBuffer.remaining());
            compressedPayload.release();
            return encryptedPayload;
        } catch (PulsarClientException e) {
            // Release the compressed payload and any partially built encrypted buffer before failing the batch.
            ReferenceCountUtil.safeRelease(encryptedPayload);
            ReferenceCountUtil.safeRelease(compressedPayload);
            discard(e);
            throw new RuntimeException("Failed to encrypt payload", e);
        } catch (Throwable t) {
            // Never orphan the compressed payload or a partially built encrypted buffer when encryption fails,
            // whatever the failure is (e.g. an OOM while allocating the encrypted buffer or an unexpected
            // runtime exception from the crypto provider). Unlike the PulsarClientException branch, the batch is
            // deliberately not discarded here: the caller owns recovery (StrategicTwoPhaseCompactor discards the
            // container on any Throwable from toByteBuf()), so the batch is failed exactly once at the call site.
            ReferenceCountUtil.safeRelease(encryptedPayload);
            ReferenceCountUtil.safeRelease(compressedPayload);
            throw t;
        }
    }

    @Override
    public ProducerImpl.OpSendMsg createOpSendMsg() {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets a CryptoKeyReader instance to encrypt batched messages during serialization, `toByteBuf()`.
     * @param cryptoKeyReader a CryptoKeyReader instance
     */
    public void setCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        this.cryptoKeyReader = cryptoKeyReader;
    }

    @VisibleForTesting
    void setMsgCryptoForTesting(MessageCrypto<MessageMetadata, MessageMetadata> msgCrypto) {
        this.msgCrypto = msgCrypto;
    }

    @Override
    public boolean add(MessageImpl<?> msg, SendCallback callback) {
        this.lastAddedMessageId = (MessageIdAdv) msg.getMessageId();
        return super.add(msg, callback);
    }

    @Override
    protected boolean isBatchFull() {
        return false;
    }

    @Override
    public boolean haveEnoughSpace(MessageImpl<?> msg) {
        if (lastAddedMessageId == null) {
            return true;
        }
        // Keep same batch compact to same batch.
        MessageIdAdv msgId = (MessageIdAdv) msg.getMessageId();
        return msgId.getLedgerId() == lastAddedMessageId.getLedgerId()
                && msgId.getEntryId() == lastAddedMessageId.getEntryId();
    }

    /**
     * Serializes the batched messages and return the ByteBuf.
     * It sets the CompressionType and Encryption Keys from the batched messages.
     * If successful, it calls `clear()` at the end to release buffers from this container.
     *
     * The returned byte buffer follows this format:
     * [IdSize][Id][metadataAndPayloadSize][metadataAndPayload].
     * This format is the same as RawMessage.serialize()'s format
     * as the compacted messages is deserialized as RawMessage in broker.
     *
     * It throws the following runtime exceptions from encryption:
     * IllegalStateException if cryptoKeyReader is not set for encrypted messages.
     * IllegalArgumentException if encryption key init fails.
     * RuntimeException if message encryption fails.
     *
     * @return a ByteBuf instance
     */
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
                IllegalStateException ex =
                        new IllegalStateException("Messages are encrypted but no cryptoKeyReader is provided.");
                discard(ex);
                throw ex;
            }

            encryptionKeys = encryptionContext.getKeys().keySet();
            if (msgCrypto == null) {
                msgCrypto =
                        new MessageCryptoBc(String.format(
                                "[%s] [%s]", topicName, "RawBatchMessageContainer"), true);
                try {
                    msgCrypto.addPublicKeyCipher(encryptionKeys, cryptoKeyReader);
                } catch (PulsarClientException.CryptoException e) {
                    discard(e);
                    throw new IllegalArgumentException("Failed to set encryption keys", e);
                }
            }
        }

        ByteBuf encryptedPayload = encrypt(getCompressedBatchMetadataAndPayload(false));
        ByteBuf metadataAndPayload = null;
        try {
            updateAndReserveBatchAllocatedSize(encryptedPayload.capacity());
            metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
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
            return buf;
        } finally {
            // Release everything allocated for this serialization on both success and failure, so a failure after
            // the batch buffer was built (e.g. an OOM) cannot orphan it.
            ReferenceCountUtil.safeRelease(metadataAndPayload);
            ReferenceCountUtil.safeRelease(encryptedPayload);
            clear();
        }
    }

    @Override
    public void clear() {
        this.lastAddedMessageId = null;
        super.clear();
    }
}
