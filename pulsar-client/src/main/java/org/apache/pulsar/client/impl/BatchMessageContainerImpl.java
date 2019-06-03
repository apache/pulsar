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
import java.util.Collections;
import java.util.List;

import io.netty.util.ReferenceCountUtil;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.ByteBufPair;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * container for individual messages being published until they are batched and sent to broker
 */

class BatchMessageContainerImpl implements BatchMessageContainerBase {

    private SendCallback previousCallback = null;
    private PulsarApi.CompressionType compressionType;
    private CompressionCodec compressor;
    private String topicName;
    private String producerName;
    private ProducerImpl producer;

    int maxNumMessagesInBatch;

    PulsarApi.MessageMetadata.Builder messageMetadata = PulsarApi.MessageMetadata.newBuilder();
    int numMessagesInBatch = 0;
    long currentBatchSizeBytes = 0;
    // sequence id for this batch which will be persisted as a single entry by broker
    long sequenceId = -1;
    ByteBuf batchedMessageMetadataAndPayload;
    List<MessageImpl<?>> messages = Lists.newArrayList();
    // keep track of callbacks for individual messages being published in a batch
    SendCallback firstCallback;

    private static final int INITIAL_BATCH_BUFFER_SIZE = 1024;
    protected static final int MAX_MESSAGE_BATCH_SIZE_BYTES = 128 * 1024;

    // This will be the largest size for a batch sent from this particular producer. This is used as a baseline to
    // allocate a new buffer that can hold the entire batch without needing costly reallocations
    private int maxBatchSize = INITIAL_BATCH_BUFFER_SIZE;


    @Override
    public boolean haveEnoughSpace(MessageImpl<?> msg) {
        int messageSize = msg.getDataBuffer().readableBytes();
        return ((messageSize + currentBatchSizeBytes) <= MAX_MESSAGE_BATCH_SIZE_BYTES
                && numMessagesInBatch < maxNumMessagesInBatch);
    }

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
        PulsarApi.MessageMetadata.Builder msgBuilder = msg.getMessageBuilder();
        batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(msgBuilder,
                msg.getDataBuffer(), batchedMessageMetadataAndPayload);
        messages.add(msg);
        msgBuilder.recycle();
    }

    private ByteBuf getCompressedBatchMetadataAndPayload() {
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
    public int getNumMessagesInBatch() {
        return numMessagesInBatch;
    }

    @Override
    public long getCurrentBatchSizeBytes() {
        return currentBatchSizeBytes;
    }

    @Override
    public void setProducer(ProducerImpl<?> producer) {
        this.producer = producer;
        this.topicName = producer.getTopic();
        this.producerName = producer.getProducerName();
        this.compressionType = CompressionCodecProvider
            .convertToWireProtocol(producer.getConfiguration().getCompressionType());
        this.compressor = CompressionCodecProvider.getCompressionCodec(compressionType);
        this.maxNumMessagesInBatch = producer.getConfiguration().getBatchingMaxMessages();
    }

    @Override
    public void handleException(Exception ex) {
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

    private OpSendMsg createOpSendMsg() throws IOException {
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

    @Override
    public List<OpSendMsg> createOpSendMsgs() throws IOException {
        return Collections.singletonList(createOpSendMsg());
    }

    private static final Logger log = LoggerFactory.getLogger(BatchMessageContainerImpl.class);
}
