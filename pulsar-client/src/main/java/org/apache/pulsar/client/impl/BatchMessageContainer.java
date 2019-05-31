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

import java.util.List;

import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * container for individual messages being published until they are batched and sent to broker
 */

class BatchMessageContainer {

    private SendCallback previousCallback = null;
    private final PulsarApi.CompressionType compressionType;
    private final CompressionCodec compressor;
    private final String topicName;
    private final String producerName;

    final int maxNumMessagesInBatch;

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

    BatchMessageContainer(int maxNumMessagesInBatch, PulsarApi.CompressionType compressionType, String topicName,
            String producerName) {
        this.maxNumMessagesInBatch = maxNumMessagesInBatch;
        this.compressionType = compressionType;
        this.compressor = CompressionCodecProvider.getCompressionCodec(compressionType);
        this.topicName = topicName;
        this.producerName = producerName;
    }

    boolean hasSpaceInBatch(MessageImpl<?> msg) {
        int messageSize = msg.getDataBuffer().readableBytes();
        return ((messageSize + currentBatchSizeBytes) <= MAX_MESSAGE_BATCH_SIZE_BYTES
                && numMessagesInBatch < maxNumMessagesInBatch);
    }

    void add(MessageImpl<?> msg, SendCallback callback) {

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

    ByteBuf getCompressedBatchMetadataAndPayload() {
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

    PulsarApi.MessageMetadata setBatchAndBuild() {
        messageMetadata.setNumMessagesInBatch(numMessagesInBatch);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] num messages in batch being closed are {}", topicName, producerName,
                    numMessagesInBatch);
        }
        return messageMetadata.build();
    }

    ByteBuf getBatchedSingleMessageMetadataAndPayload() {
        return batchedMessageMetadataAndPayload;
    }

    void clear() {
        messages = Lists.newArrayList();
        firstCallback = null;
        previousCallback = null;
        messageMetadata.clear();
        numMessagesInBatch = 0;
        currentBatchSizeBytes = 0;
        sequenceId = -1;
        batchedMessageMetadataAndPayload = null;
    }

    boolean isEmpty() {
        return messages.isEmpty();
    }

    private static final Logger log = LoggerFactory.getLogger(BatchMessageContainer.class);
}
