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

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Key based batch message container
 *
 * incoming single messages:
 * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
 *
 * batched into multiple batch messages:
 * [(k1, v1), (k1, v2), (k1, v3)], [(k2, v1), (k2, v2), (k2, v3)], [(k3, v1), (k3, v2), (k3, v3)]
 */
class BatchMessageKeyBasedContainer extends AbstractBatchMessageContainer {

    private Map<String, KeyedBatch> batches = new HashMap<>();

    @Override
    public void add(MessageImpl<?> msg, SendCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] add message to batch, num messages in batch so far is {}", topicName, producerName,
                    numMessagesInBatch);
        }
        numMessagesInBatch++;
        currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
        String key = getKey(msg);
        KeyedBatch part = batches.get(key);
        if (part == null) {
            part = new KeyedBatch();
            part.addMsg(msg, callback);
            part.compressionType = compressionType;
            part.compressor = compressor;
            part.maxBatchSize = maxBatchSize;
            part.topicName = topicName;
            part.producerName = producerName;
            batches.putIfAbsent(key, part);
        } else {
            part.addMsg(msg, callback);
        }
    }

    @Override
    public void clear() {
        numMessagesInBatch = 0;
        currentBatchSizeBytes = 0;
        batches = new HashMap<>();
    }

    @Override
    public boolean isEmpty() {
        return batches.isEmpty();
    }

    @Override
    public void discard(Exception ex) {
        try {
            // Need to protect ourselves from any exception being thrown in the future handler from the application
            batches.forEach((k, v) -> v.firstCallback.sendComplete(ex));
        } catch (Throwable t) {
            log.warn("[{}] [{}] Got exception while completing the callback", topicName, producerName, t);
        }
        batches.forEach((k, v) -> ReferenceCountUtil.safeRelease(v.batchedMessageMetadataAndPayload));
        clear();
    }

    @Override
    public boolean isMultiBatches() {
        return true;
    }

    private ProducerImpl.OpSendMsg createOpSendMsg(KeyedBatch keyedBatch) throws IOException {
        ByteBuf encryptedPayload = producer.encryptMessage(keyedBatch.messageMetadata, keyedBatch.getCompressedBatchMetadataAndPayload());
        final int numMessagesInBatch = keyedBatch.messages.size();
        long currentBatchSizeBytes = 0;
        for (MessageImpl<?> message : keyedBatch.messages) {
            currentBatchSizeBytes += message.getDataBuffer().readableBytes();
        }
        keyedBatch.messageMetadata.setNumMessagesInBatch(numMessagesInBatch);
        ByteBufPair cmd = producer.sendMessage(producer.producerId, keyedBatch.sequenceId, numMessagesInBatch,
                keyedBatch.messageMetadata.build(), encryptedPayload);

        ProducerImpl.OpSendMsg op = ProducerImpl.OpSendMsg.create(keyedBatch.messages, cmd, keyedBatch.sequenceId, keyedBatch.firstCallback);

        if (encryptedPayload.readableBytes() > ClientCnx.getMaxMessageSize()) {
            cmd.release();
            keyedBatch.discard(new PulsarClientException.InvalidMessageException(
                    "Message size is bigger than " + ClientCnx.getMaxMessageSize() + " bytes"));
            if (op != null) {
                op.recycle();
            }
            return null;
        }
        op.setNumMessagesInBatch(numMessagesInBatch);
        op.setBatchSizeByte(currentBatchSizeBytes);
        return op;
    }

    @Override
    public List<ProducerImpl.OpSendMsg> createOpSendMsgs() throws IOException {
        List<ProducerImpl.OpSendMsg> result = new ArrayList<>();
        List<KeyedBatch> list = new ArrayList<>(batches.values());
        list.sort(((o1, o2) -> ComparisonChain.start()
                .compare(o1.sequenceId, o2.sequenceId)
                .result()));
        for (KeyedBatch keyedBatch : list) {
            ProducerImpl.OpSendMsg op = createOpSendMsg(keyedBatch);
            if (op != null) {
                result.add(op);
            }
        }
        return result;
    }

    @Override
    public boolean hasSameSchema(MessageImpl<?> msg) {
        String key = getKey(msg);
        KeyedBatch part = batches.get(key);
        if (part == null || part.messages.isEmpty()) {
            return true;
        }
        if (!part.messageMetadata.hasSchemaVersion()) {
            return msg.getSchemaVersion() == null;
        }
        return Arrays.equals(msg.getSchemaVersion(),
                             part.messageMetadata.getSchemaVersion().toByteArray());
    }

    private String getKey(MessageImpl<?> msg) {
        if (msg.hasOrderingKey()) {
            return Base64.getEncoder().encodeToString(msg.getOrderingKey());
        }
        return msg.getKey();
    }

    private static class KeyedBatch {
        private PulsarApi.MessageMetadata.Builder messageMetadata = PulsarApi.MessageMetadata.newBuilder();
        // sequence id for this batch which will be persisted as a single entry by broker
        private long sequenceId = -1;
        private ByteBuf batchedMessageMetadataAndPayload;
        private List<MessageImpl<?>> messages = Lists.newArrayList();
        private SendCallback previousCallback = null;
        private PulsarApi.CompressionType compressionType;
        private CompressionCodec compressor;
        private int maxBatchSize;
        private String topicName;
        private String producerName;

        // keep track of callbacks for individual messages being published in a batch
        private SendCallback firstCallback;

        private ByteBuf getCompressedBatchMetadataAndPayload() {
            for (MessageImpl<?> msg : messages) {
                PulsarApi.MessageMetadata.Builder msgBuilder = msg.getMessageBuilder();
                batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(msgBuilder,
                        msg.getDataBuffer(), batchedMessageMetadataAndPayload);
                msgBuilder.recycle();
            }
            int uncompressedSize = batchedMessageMetadataAndPayload.readableBytes();
            ByteBuf compressedPayload = compressor.encode(batchedMessageMetadataAndPayload);
            batchedMessageMetadataAndPayload.release();
            if (compressionType != PulsarApi.CompressionType.NONE) {
                messageMetadata.setCompression(compressionType);
                messageMetadata.setUncompressedSize(uncompressedSize);
            }

            // Update the current max batch size using the uncompressed size, which is what we need in any case to
            // accumulate the batch content
            maxBatchSize = Math.max(maxBatchSize, uncompressedSize);
            return compressedPayload;
        }

        private void addMsg(MessageImpl<?> msg, SendCallback callback) {
            if (messages.size() == 0) {
                sequenceId = Commands.initBatchMessageMetadata(messageMetadata, msg.getMessageBuilder());
                if (msg.hasKey()) {
                    messageMetadata.setPartitionKey(msg.getKey());
                    if (msg.hasBase64EncodedKey()) {
                        messageMetadata.setPartitionKeyB64Encoded(true);
                    }
                }
                if (msg.hasOrderingKey()) {
                    messageMetadata.setOrderingKey(ByteString.copyFrom(msg.getOrderingKey()));
                }
                batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT
                        .buffer(Math.min(maxBatchSize, MAX_MESSAGE_BATCH_SIZE_BYTES));
                firstCallback = callback;
            }
            if (previousCallback != null) {
                previousCallback.addCallback(msg, callback);
            }
            previousCallback = callback;
            messages.add(msg);
        }

        public void discard(Exception ex) {
            try {
                // Need to protect ourselves from any exception being thrown in the future handler from the application
                if (firstCallback != null) {
                    firstCallback.sendComplete(ex);
                }
            } catch (Throwable t) {
                log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topicName, producerName,
                        sequenceId, t);
            }
            ReferenceCountUtil.safeRelease(batchedMessageMetadataAndPayload);
            clear();
        }

        public void clear() {
            messages = Lists.newArrayList();
            firstCallback = null;
            previousCallback = null;
            messageMetadata.clear();
            sequenceId = -1;
            batchedMessageMetadataAndPayload = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(BatchMessageKeyBasedContainer.class);

}
