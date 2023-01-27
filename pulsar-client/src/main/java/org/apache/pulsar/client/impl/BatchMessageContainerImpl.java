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
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default batch message container.
 *
 * incoming single messages:
 * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
 *
 * batched into single batch message:
 * [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]
 */
class BatchMessageContainerImpl extends AbstractBatchMessageContainer {

    protected MessageMetadata messageMetadata = new MessageMetadata();
    // sequence id for this batch which will be persisted as a single entry by broker
    @Getter
    @Setter
    protected long lowestSequenceId = -1L;
    @Getter
    @Setter
    protected long highestSequenceId = -1L;
    protected ByteBuf batchedMessageMetadataAndPayload;
    protected List<MessageImpl<?>> messages = new ArrayList<>(maxMessagesNum);
    protected SendCallback previousCallback = null;
    // keep track of callbacks for individual messages being published in a batch
    protected SendCallback firstCallback;

    protected final ByteBufAllocator allocator;
    private static final int SHRINK_COOLING_OFF_PERIOD = 10;
    private int consecutiveShrinkTime = 0;

    public BatchMessageContainerImpl() {
        this(PulsarByteBufAllocator.DEFAULT);
    }

    /**
     * This constructor is for testing only. The global allocator is always
     * {@link PulsarByteBufAllocator#DEFAULT}.
     */
    @VisibleForTesting
    BatchMessageContainerImpl(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    public BatchMessageContainerImpl(ProducerImpl<?> producer) {
        this();
        setProducer(producer);
    }

    @Override
    public boolean add(MessageImpl<?> msg, SendCallback callback) {

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] add message to batch, num messages in batch so far {}", topicName, producerName,
                    numMessagesInBatch);
        }

        if (++numMessagesInBatch == 1) {
            try {
                // some properties are common amongst the different messages in the batch, hence we just pick it up from
                // the first message
                messageMetadata.setSequenceId(msg.getSequenceId());
                lowestSequenceId = Commands.initBatchMessageMetadata(messageMetadata, msg.getMessageBuilder());
                this.firstCallback = callback;
                batchedMessageMetadataAndPayload = allocator.buffer(
                        Math.min(maxBatchSize, ClientCnx.getMaxMessageSize()));
                updateAndReserveBatchAllocatedSize(batchedMessageMetadataAndPayload.capacity());
                if (msg.getMessageBuilder().hasTxnidMostBits() && currentTxnidMostBits == -1) {
                    currentTxnidMostBits = msg.getMessageBuilder().getTxnidMostBits();
                }
                if (msg.getMessageBuilder().hasTxnidLeastBits() && currentTxnidLeastBits == -1) {
                    currentTxnidLeastBits = msg.getMessageBuilder().getTxnidLeastBits();
                }
            } catch (Throwable e) {
                log.error("construct first message failed, exception is ", e);
                if (producer != null) {
                    producer.semaphoreRelease(getNumMessagesInBatch());
                    producer.client.getMemoryLimitController().releaseMemory(msg.getUncompressedSize()
                            + batchAllocatedSizeBytes);
                }
                discard(new PulsarClientException(e));
                return false;
            }
        }

        if (previousCallback != null) {
            previousCallback.addCallback(msg, callback);
        }
        previousCallback = callback;
        currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
        messages.add(msg);

        if (lowestSequenceId == -1L) {
            lowestSequenceId = msg.getSequenceId();
            messageMetadata.setSequenceId(lowestSequenceId);
        }
        highestSequenceId = msg.getSequenceId();
        if (producer != null) {
            ProducerImpl.LAST_SEQ_ID_PUSHED_UPDATER.getAndUpdate(producer, prev -> Math.max(prev, msg.getSequenceId()));
        }

        return isBatchFull();
    }

    protected ByteBuf getCompressedBatchMetadataAndPayload() {
        int batchWriteIndex = batchedMessageMetadataAndPayload.writerIndex();
        int batchReadIndex = batchedMessageMetadataAndPayload.readerIndex();

        for (int i = 0, n = messages.size(); i < n; i++) {
            MessageImpl<?> msg = messages.get(i);
            msg.getDataBuffer().markReaderIndex();
            try {
                if (n == 1) {
                    batchedMessageMetadataAndPayload.writeBytes(msg.getDataBuffer());
                } else  {
                    batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(
                        msg.getMessageBuilder(), msg.getDataBuffer(), batchedMessageMetadataAndPayload);
                }
            } catch (Throwable th) {
                // serializing batch message can corrupt the index of message and batch-message. Reset the index so,
                // next iteration doesn't send corrupt message to broker.
                for (int j = 0; j <= i; j++) {
                    MessageImpl<?> previousMsg = messages.get(j);
                    previousMsg.getDataBuffer().resetReaderIndex();
                }
                batchedMessageMetadataAndPayload.writerIndex(batchWriteIndex);
                batchedMessageMetadataAndPayload.readerIndex(batchReadIndex);
                throw new RuntimeException(th);
            }
        }

        int uncompressedSize = batchedMessageMetadataAndPayload.readableBytes();
        ByteBuf compressedPayload = compressor.encode(batchedMessageMetadataAndPayload);
        batchedMessageMetadataAndPayload.release();
        if (compressionType != CompressionType.NONE) {
            messageMetadata.setCompression(compressionType);
            messageMetadata.setUncompressedSize(uncompressedSize);
        }

        // Update the current max batch size using the uncompressed size, which is what we need in any case to
        // accumulate the batch content
        updateMaxBatchSize(uncompressedSize);
        maxMessagesNum = Math.max(maxMessagesNum, numMessagesInBatch);
        return compressedPayload;
    }

    void updateMaxBatchSize(int uncompressedSize) {
        if (uncompressedSize > maxBatchSize) {
            maxBatchSize = uncompressedSize;
            consecutiveShrinkTime = 0;
        } else {
            int shrank = maxBatchSize - (maxBatchSize >> 2);
            if (uncompressedSize <= shrank) {
                if (consecutiveShrinkTime <= SHRINK_COOLING_OFF_PERIOD) {
                    consecutiveShrinkTime++;
                } else {
                    maxBatchSize = shrank;
                    consecutiveShrinkTime = 0;
                }
            } else {
                consecutiveShrinkTime = 0;
            }
        }
    }

    @Override
    public void clear() {
        messages = new ArrayList<>(maxMessagesNum);
        firstCallback = null;
        previousCallback = null;
        messageMetadata.clear();
        numMessagesInBatch = 0;
        currentBatchSizeBytes = 0;
        lowestSequenceId = -1L;
        highestSequenceId = -1L;
        batchedMessageMetadataAndPayload = null;
        currentTxnidMostBits = -1L;
        currentTxnidLeastBits = -1L;
        batchAllocatedSizeBytes = 0;
    }

    @Override
    public boolean isEmpty() {
        return messages.isEmpty();
    }

    @Override
    public void discard(Exception ex) {
        try {
            // Need to protect ourselves from any exception being thrown in the future handler from the application
            if (firstCallback != null) {
                firstCallback.sendComplete(ex);
            }
            if (batchedMessageMetadataAndPayload != null) {
                ReferenceCountUtil.safeRelease(batchedMessageMetadataAndPayload);
                batchedMessageMetadataAndPayload = null;
            }
        } catch (Throwable t) {
            log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topicName, producerName,
                    lowestSequenceId, t);
        }
        clear();
    }

    @Override
    public boolean isMultiBatches() {
        return false;
    }

    @Override
    public OpSendMsg createOpSendMsg() throws IOException {
        if (messages.size() == 1) {
            messageMetadata.clear();
            messageMetadata.copyFrom(messages.get(0).getMessageBuilder());
            ByteBuf encryptedPayload = producer.encryptMessage(messageMetadata, getCompressedBatchMetadataAndPayload());
            updateAndReserveBatchAllocatedSize(encryptedPayload.capacity());
            ByteBufPair cmd = producer.sendMessage(producer.producerId, messageMetadata.getSequenceId(),
                1, null, messageMetadata, encryptedPayload);
            final OpSendMsg op;

            // Shouldn't call create(MessageImpl<?> msg, ByteBufPair cmd, long sequenceId, SendCallback callback),
            // otherwise it will bring message out of order problem.
            // Because when invoke `ProducerImpl.processOpSendMsg` on flush,
            // if `op.msg != null && isBatchMessagingEnabled()` checks true, it will call `batchMessageAndSend` to flush
            // messageContainers before publishing this one-batch message.
            op = OpSendMsg.create(messages, cmd, messageMetadata.getSequenceId(), firstCallback,
                    batchAllocatedSizeBytes);

            // NumMessagesInBatch and BatchSizeByte will not be serialized to the binary cmd. It's just useful for the
            // ProducerStats
            op.setNumMessagesInBatch(1);
            op.setBatchSizeByte(encryptedPayload.readableBytes());

            // handle mgs size check as non-batched in `ProducerImpl.isMessageSizeExceeded`
            if (op.getMessageHeaderAndPayloadSize() > ClientCnx.getMaxMessageSize()) {
                producer.semaphoreRelease(1);
                producer.client.getMemoryLimitController().releaseMemory(
                        messages.get(0).getUncompressedSize() + batchAllocatedSizeBytes);
                discard(new PulsarClientException.InvalidMessageException(
                    "Message size is bigger than " + ClientCnx.getMaxMessageSize() + " bytes"));
                return null;
            }
            lowestSequenceId = -1L;
            return op;
        }
        ByteBuf encryptedPayload = producer.encryptMessage(messageMetadata, getCompressedBatchMetadataAndPayload());
        updateAndReserveBatchAllocatedSize(encryptedPayload.capacity());
        if (encryptedPayload.readableBytes() > ClientCnx.getMaxMessageSize()) {
            producer.semaphoreRelease(messages.size());
            messages.forEach(msg -> producer.client.getMemoryLimitController()
                    .releaseMemory(msg.getUncompressedSize()));
            producer.client.getMemoryLimitController().releaseMemory(batchAllocatedSizeBytes);
            discard(new PulsarClientException.InvalidMessageException(
                    "Message size is bigger than " + ClientCnx.getMaxMessageSize() + " bytes"));
            return null;
        }
        messageMetadata.setNumMessagesInBatch(numMessagesInBatch);
        messageMetadata.setSequenceId(lowestSequenceId);
        messageMetadata.setHighestSequenceId(highestSequenceId);
        if (currentTxnidMostBits != -1) {
            messageMetadata.setTxnidMostBits(currentTxnidMostBits);
        }
        if (currentTxnidLeastBits != -1) {
            messageMetadata.setTxnidLeastBits(currentTxnidLeastBits);
        }
        ByteBufPair cmd = producer.sendMessage(producer.producerId, messageMetadata.getSequenceId(),
                messageMetadata.getHighestSequenceId(), numMessagesInBatch, messageMetadata, encryptedPayload);

        OpSendMsg op = OpSendMsg.create(messages, cmd, messageMetadata.getSequenceId(),
                messageMetadata.getHighestSequenceId(), firstCallback, batchAllocatedSizeBytes);

        op.setNumMessagesInBatch(numMessagesInBatch);
        op.setBatchSizeByte(currentBatchSizeBytes);
        lowestSequenceId = -1L;
        return op;
    }

    protected void updateAndReserveBatchAllocatedSize(int updatedSizeBytes) {
        int delta = updatedSizeBytes - batchAllocatedSizeBytes;
        batchAllocatedSizeBytes = updatedSizeBytes;
        if (delta != 0) {
            if (producer != null) {
                producer.client.getMemoryLimitController().forceReserveMemory(delta);
            }
        }
    }

    @Override
    public boolean hasSameSchema(MessageImpl<?> msg) {
        if (numMessagesInBatch == 0) {
            return true;
        }
        if (!messageMetadata.hasSchemaVersion()) {
            return msg.getSchemaVersion() == null;
        }
        return Arrays.equals(msg.getSchemaVersion(), messageMetadata.getSchemaVersion());
    }

    private static final Logger log = LoggerFactory.getLogger(BatchMessageContainerImpl.class);
}
