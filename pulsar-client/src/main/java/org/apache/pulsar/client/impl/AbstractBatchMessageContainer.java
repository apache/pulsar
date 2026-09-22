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
import java.io.IOException;
import java.util.List;
import lombok.CustomLog;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Batch message container framework.
 */
@CustomLog
public abstract class AbstractBatchMessageContainer implements BatchMessageContainerBase {

    protected CompressionType compressionType;
    protected CompressionCodec compressor;
    protected String topicName;
    protected ProducerImpl producer;

    protected int maxNumMessagesInBatch;
    protected int maxBytesInBatch;
    protected int numMessagesInBatch = 0;
    protected long currentBatchSizeBytes = 0;
    protected int batchAllocatedSizeBytes = 0;

    protected long currentTxnidMostBits = -1L;
    protected long currentTxnidLeastBits = -1L;

    protected static final int INITIAL_BATCH_BUFFER_SIZE = 1024;
    protected static final int INITIAL_MESSAGES_NUM = 32;

    // This will be the largest size for a batch sent from this particular producer. This is used as a baseline to
    // allocate a new buffer that can hold the entire batch without needing costly reallocations
    protected int maxBatchSize = INITIAL_BATCH_BUFFER_SIZE;
    protected int maxMessagesNum = INITIAL_MESSAGES_NUM;
    private volatile long firstAddedTimestamp = 0L;

    @Override
    public boolean haveEnoughSpace(MessageImpl<?> msg) {
        int messageSize = msg.getDataBuffer().readableBytes();
        return (
            (maxBytesInBatch <= 0 && (messageSize + currentBatchSizeBytes) <= getMaxMessageSize())
            || (maxBytesInBatch > 0 && (messageSize + currentBatchSizeBytes) <= maxBytesInBatch)
        ) && (maxNumMessagesInBatch <= 0 || numMessagesInBatch < maxNumMessagesInBatch);
    }
    protected int getMaxMessageSize() {
        return producer != null && producer.getConnectionHandler() != null
                ? producer.getConnectionHandler().getMaxMessageSize() : Commands.DEFAULT_MAX_MESSAGE_SIZE;
    }

    protected boolean isBatchFull() {
        return (maxBytesInBatch > 0 && currentBatchSizeBytes >= maxBytesInBatch)
            || (maxBytesInBatch <= 0 && currentBatchSizeBytes >= getMaxMessageSize())
            || (maxNumMessagesInBatch > 0 && numMessagesInBatch >= maxNumMessagesInBatch);
    }

    @Override
    public int getNumMessagesInBatch() {
        return numMessagesInBatch;
    }

    @VisibleForTesting
    public int getMaxMessagesNum() {
        return maxMessagesNum;
    }

    @Override
    public long getCurrentBatchSize() {
        return currentBatchSizeBytes;
    }

    @Override
    public int getBatchAllocatedSizeBytes() {
        return batchAllocatedSizeBytes;
    }

    int getMaxBatchSize() {
        return maxBatchSize;
    }

    @Override
    public List<ProducerImpl.OpSendMsg> createOpSendMsgs() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProducerImpl.OpSendMsg createOpSendMsg() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProducer(ProducerImpl<?> producer) {
        this.producer = producer;
        this.topicName = producer.getTopic();
        this.compressionType = CompressionCodecProvider
                .convertToWireProtocol(producer.getConfiguration().getCompressionType());
        this.compressor = CompressionCodecProvider.getCompressionCodec(compressionType);
        this.maxNumMessagesInBatch = producer.getConfiguration().getBatchingMaxMessages();
        this.maxBytesInBatch = producer.getConfiguration().getBatchingMaxBytes();
    }

    /**
     * Whether {@code msg} belongs to the same transaction as the messages already in this batch.
     *
     * <p>A batch carries a single transaction id in its metadata, so every message in it inherits that
     * transaction. "No transaction" is therefore an identity of its own and is not compatible with any
     * transaction: mixing the two in one batch would either enroll a plain message in a transaction (invisible
     * until commit, dropped on abort) or publish a transactional message outside its transaction.
     *
     * <p>This is a pure query. The batch adopts its transaction id from its first message in {@code add}.
     */
    @Override
    public boolean hasSameTxn(MessageImpl<?> msg) {
        if (numMessagesInBatch == 0) {
            return true;
        }
        boolean msgHasTxn = msg.getMessageBuilder().hasTxnidMostBits()
                && msg.getMessageBuilder().hasTxnidLeastBits();
        boolean batchHasTxn = currentTxnidMostBits != -1L && currentTxnidLeastBits != -1L;
        if (msgHasTxn != batchHasTxn) {
            return false;
        }
        if (!msgHasTxn) {
            return true;
        }
        return currentTxnidMostBits == msg.getMessageBuilder().getTxnidMostBits()
                && currentTxnidLeastBits == msg.getMessageBuilder().getTxnidLeastBits();
    }

    @Override
    public long getFirstAddedTimestamp() {
        return firstAddedTimestamp;
    }

    protected void tryUpdateTimestamp() {
        if (numMessagesInBatch == 1) {
            firstAddedTimestamp = System.nanoTime();
        }
    }

    protected void clearTimestamp() {
        firstAddedTimestamp = 0L;
    }
}
