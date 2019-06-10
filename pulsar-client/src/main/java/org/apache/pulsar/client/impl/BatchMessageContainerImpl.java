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

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

import io.netty.util.ReferenceCountUtil;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default batch message container
 *
 * incoming single messages:
 * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
 *
 * batched into single batch message:
 * [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]
 */
class BatchMessageContainerImpl extends AbstractBatchMessageContainer {

    private PulsarApi.MessageMetadata.Builder messageMetadata = PulsarApi.MessageMetadata.newBuilder();
    // sequence id for this batch which will be persisted as a single entry by broker
    private long sequenceId = -1;
    private ByteBuf batchedMessageMetadataAndPayload;
    private List<MessageImpl<?>> messages = Lists.newArrayList();
    protected SendCallback previousCallback = null;
    // keep track of callbacks for individual messages being published in a batch
    protected SendCallback firstCallback;

    @Override
    public void add(MessageImpl<?> msg, SendCallback callback) {

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] add message to batch, num messages in batch so far {}", topicName, producerName,
                    numMessagesInBatch);
        }

        if (++numMessagesInBatch == 1) {
            // some properties are common amongst the different messages in the batch, hence we just pick it up from
            // the first message
            sequenceId = Commands.initBatchMessageMetadata(messageMetadata, msg.getMessageBuilder());
            this.firstCallback = callback;
            batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT
                    .buffer(Math.min(maxBatchSize, MAX_MESSAGE_BATCH_SIZE_BYTES));
        }

        if (previousCallback != null) {
            previousCallback.addCallback(msg, callback);
        }
        previousCallback = callback;
        currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
        messages.add(msg);
    }

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

    @Override
    public void clear() {
        messages = Lists.newArrayList();
        firstCallback = null;
        previousCallback = null;
        messageMetadata.clear();
        numMessagesInBatch = 0;
        currentBatchSizeBytes = 0;
        sequenceId = -1;
        batchedMessageMetadataAndPayload = null;
    }

    @Override
    public boolean isEmpty() {
        return messages.isEmpty();
    }

    @Override
    public void discard(Exception ex) {
        try {
            // Need to protect ourselves from any exception being thrown in the future handler from the application
            firstCallback.sendComplete(ex);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topicName, producerName,
                sequenceId, t);
        }
        ReferenceCountUtil.safeRelease(batchedMessageMetadataAndPayload);
        clear();
    }

    @Override
    public boolean isMultiBatches() {
        return false;
    }

    @Override
    public OpSendMsg createOpSendMsg() throws IOException {
        ByteBuf encryptedPayload = producer.encryptMessage(messageMetadata, getCompressedBatchMetadataAndPayload());
        messageMetadata.setNumMessagesInBatch(numMessagesInBatch);
        ByteBufPair cmd = producer.sendMessage(producer.producerId, sequenceId, numMessagesInBatch,
            messageMetadata.build(), encryptedPayload);

        OpSendMsg op = OpSendMsg.create(messages, cmd, sequenceId, firstCallback);

        if (encryptedPayload.readableBytes() > ClientCnx.getMaxMessageSize()) {
            cmd.release();
            if (op != null) {
                op.callback.sendComplete(new PulsarClientException.InvalidMessageException(
                    "Message size is bigger than " + ClientCnx.getMaxMessageSize() + " bytes"));
                op.recycle();
            }
            return null;
        }

        op.setNumMessagesInBatch(numMessagesInBatch);
        op.setBatchSizeByte(currentBatchSizeBytes);
        return op;
    }

    private static final Logger log = LoggerFactory.getLogger(BatchMessageContainerImpl.class);
}
