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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.resumeChecksum;
import static java.lang.String.format;
import static org.apache.pulsar.client.impl.MessageImpl.SchemaState.Broken;
import static org.apache.pulsar.client.impl.MessageImpl.SchemaState.None;
import static org.apache.pulsar.client.impl.ProducerBase.MultiSchemaMode.Auto;
import static org.apache.pulsar.client.impl.ProducerBase.MultiSchemaMode.Enabled;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.ScheduledFuture;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.client.api.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerImpl<T> extends ProducerBase<T> implements TimerTask, ConnectionHandler.Connection {

    // Producer id, used to identify a producer within a single connection
    protected final long producerId;

    // Variable is used through the atomic updater
    private volatile long msgIdGenerator;

    private final OpSendMsgQueue pendingMessages;
    private final Optional<Semaphore> semaphore;
    private volatile Timeout sendTimeout = null;
    private final long lookupDeadline;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ProducerImpl> PRODUCER_DEADLINE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "producerDeadline");
    @SuppressWarnings("unused")
    private volatile long producerDeadline = 0; // gets set on first successful connection

    private final BatchMessageContainerBase batchMessageContainer;
    private CompletableFuture<MessageId> lastSendFuture = CompletableFuture.completedFuture(null);

    // Globally unique producer name
    private String producerName;
    private final boolean userProvidedProducerName;

    private String connectionId;
    private String connectedSince;
    private final int partitionIndex;

    private final ProducerStatsRecorder stats;

    private final CompressionCodec compressor;

    static final AtomicLongFieldUpdater<ProducerImpl> LAST_SEQ_ID_PUBLISHED_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "lastSequenceIdPublished");
    private volatile long lastSequenceIdPublished;

    static final AtomicLongFieldUpdater<ProducerImpl> LAST_SEQ_ID_PUSHED_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "lastSequenceIdPushed");
    protected volatile long lastSequenceIdPushed;
    private volatile boolean isLastSequenceIdPotentialDuplicated;

    private final MessageCrypto msgCrypto;

    private ScheduledFuture<?> keyGeneratorTask = null;

    private final Map<String, String> metadata;

    private Optional<byte[]> schemaVersion = Optional.empty();

    private final ConnectionHandler connectionHandler;

    private ScheduledFuture<?> batchTimerTask;

    private Optional<Long> topicEpoch = Optional.empty();
    private final List<Throwable> previousExceptions = new CopyOnWriteArrayList<Throwable>();

    private boolean errorState;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ProducerImpl> msgIdGeneratorUpdater = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "msgIdGenerator");

    public ProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
                        CompletableFuture<Producer<T>> producerCreatedFuture, int partitionIndex, Schema<T> schema,
                        ProducerInterceptors interceptors) {
        super(client, topic, conf, producerCreatedFuture, schema, interceptors);
        this.producerId = client.newProducerId();
        this.producerName = conf.getProducerName();
        this.userProvidedProducerName = StringUtils.isNotBlank(producerName);
        this.partitionIndex = partitionIndex;
        this.pendingMessages = createPendingMessagesQueue();
        if (conf.getMaxPendingMessages() > 0) {
            this.semaphore = Optional.of(new Semaphore(conf.getMaxPendingMessages(), true));
        } else {
            this.semaphore = Optional.empty();
        }

        this.compressor = CompressionCodecProvider.getCompressionCodec(conf.getCompressionType());

        if (conf.getInitialSequenceId() != null) {
            long initialSequenceId = conf.getInitialSequenceId();
            this.lastSequenceIdPublished = initialSequenceId;
            this.lastSequenceIdPushed = initialSequenceId;
            this.msgIdGenerator = initialSequenceId + 1L;
        } else {
            this.lastSequenceIdPublished = -1L;
            this.lastSequenceIdPushed = -1L;
            this.msgIdGenerator = 0L;
        }

        if (conf.isEncryptionEnabled()) {
            String logCtx = "[" + topic + "] [" + producerName + "] [" + producerId + "]";

            if (conf.getMessageCrypto() != null) {
                this.msgCrypto = conf.getMessageCrypto();
            } else {
                // default to use MessageCryptoBc;
                MessageCrypto msgCryptoBc;
                try {
                    msgCryptoBc = new MessageCryptoBc(logCtx, true);
                } catch (Exception e) {
                    log.error("MessageCryptoBc may not included in the jar in Producer. e:", e);
                    msgCryptoBc = null;
                }
                this.msgCrypto = msgCryptoBc;
            }
        } else {
            this.msgCrypto = null;
        }

        if (this.msgCrypto != null) {
            // Regenerate data key cipher at fixed interval
            keyGeneratorTask = client.eventLoopGroup().scheduleWithFixedDelay(catchingAndLoggingThrowables(() -> {
                try {
                    msgCrypto.addPublicKeyCipher(conf.getEncryptionKeys(), conf.getCryptoKeyReader());
                } catch (CryptoException e) {
                    if (!producerCreatedFuture.isDone()) {
                        log.warn("[{}] [{}] [{}] Failed to add public key cipher.", topic, producerName, producerId);
                        producerCreatedFuture.completeExceptionally(
                                PulsarClientException.wrap(e,
                                        String.format("The producer %s of the topic %s " +
                                                      "adds the public key cipher was failed",
                                                producerName, topic)));
                    }
                }
            }), 0L, 4L, TimeUnit.HOURS);
        }

        if (conf.getSendTimeoutMs() > 0) {
            sendTimeout = client.timer().newTimeout(this, conf.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        this.lookupDeadline = System.currentTimeMillis() + client.getConfiguration().getLookupTimeoutMs();
        if (conf.isBatchingEnabled()) {
            BatcherBuilder containerBuilder = conf.getBatcherBuilder();
            if (containerBuilder == null) {
                containerBuilder = BatcherBuilder.DEFAULT;
            }
            this.batchMessageContainer = (BatchMessageContainerBase)containerBuilder.build();
            this.batchMessageContainer.setProducer(this);
        } else {
            this.batchMessageContainer = null;
        }
        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ProducerStatsRecorderImpl(client, conf, this);
        } else {
            stats = ProducerStatsDisabled.INSTANCE;
        }

        if (conf.getProperties().isEmpty()) {
            metadata = Collections.emptyMap();
        } else {
            metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
        }

        this.connectionHandler = new ConnectionHandler(this,
        	new BackoffBuilder()
        	    .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
			    .setMax(client.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
			    .setMandatoryStop(Math.max(100, conf.getSendTimeoutMs() - 100), TimeUnit.MILLISECONDS)
			    .create(),
            this);

        grabCnx();
    }

    protected void semaphoreRelease(final int releaseCountRequest) {
        if (semaphore.isPresent()) {
            if (!errorState) {
                final int availablePermits = semaphore.get().availablePermits();
                if (availablePermits - releaseCountRequest < 0) {
                    log.error("Semaphore permit release count request greater then availablePermits" +
                                    " : availablePermits={}, releaseCountRequest={}",
                            availablePermits, releaseCountRequest);
                    errorState = true;
                }
            }
            semaphore.get().release(releaseCountRequest);
        }
    }

    protected OpSendMsgQueue createPendingMessagesQueue() {
        return new OpSendMsgQueue();
    }

    public ConnectionHandler getConnectionHandler() {
        return connectionHandler;
    }

    private boolean isBatchMessagingEnabled() {
        return conf.isBatchingEnabled();
    }

    private boolean isMultiSchemaEnabled(boolean autoEnable) {
        if (multiSchemaMode != Auto) {
            return multiSchemaMode == Enabled;
        }
        if (autoEnable) {
            multiSchemaMode = Enabled;
            return true;
        }
        return false;
    }

    @Override
    public long getLastSequenceId() {
        return lastSequenceIdPublished;
    }

    @Override
    CompletableFuture<MessageId> internalSendAsync(Message<?> message) {
        CompletableFuture<MessageId> future = new CompletableFuture<>();

        MessageImpl<?> interceptorMessage = (MessageImpl) beforeSend(message);
        //Retain the buffer used by interceptors callback to get message. Buffer will release after complete interceptors.
        interceptorMessage.getDataBuffer().retain();
        if (interceptors != null) {
            interceptorMessage.getProperties();
        }
        sendAsync(interceptorMessage, new SendCallback() {
            SendCallback nextCallback = null;
            MessageImpl<?> nextMsg = null;
            long createdAt = System.nanoTime();

            @Override
            public CompletableFuture<MessageId> getFuture() {
                return future;
            }

            @Override
            public SendCallback getNextSendCallback() {
                return nextCallback;
            }

            @Override
            public MessageImpl<?> getNextMessage() {
                return nextMsg;
            }

            @Override
            public void sendComplete(Exception e) {
                try {
                    if (e != null) {
                        stats.incrementSendFailed();
                        onSendAcknowledgement(interceptorMessage, null, e);
                        future.completeExceptionally(e);
                    } else {
                        onSendAcknowledgement(interceptorMessage, interceptorMessage.getMessageId(), null);
                        future.complete(interceptorMessage.getMessageId());
                        stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                    }
                } finally {
                    interceptorMessage.getDataBuffer().release();
                }

                while (nextCallback != null) {
                    SendCallback sendCallback = nextCallback;
                    MessageImpl<?> msg = nextMsg;
                    //Retain the buffer used by interceptors callback to get message. Buffer will release after complete interceptors.
                    try {
                        msg.getDataBuffer().retain();
                        if (e != null) {
                            stats.incrementSendFailed();
                            onSendAcknowledgement(msg, null, e);
                            sendCallback.getFuture().completeExceptionally(e);
                        } else {
                            onSendAcknowledgement(msg, msg.getMessageId(), null);
                            sendCallback.getFuture().complete(msg.getMessageId());
                            stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                        }
                        nextMsg = nextCallback.getNextMessage();
                        nextCallback = nextCallback.getNextSendCallback();
                    } finally {
                        msg.getDataBuffer().release();
                    }
                }
            }

            @Override
            public void addCallback(MessageImpl<?> msg, SendCallback scb) {
                nextMsg = msg;
                nextCallback = scb;
            }
        });
        return future;
    }

    @Override
    CompletableFuture<MessageId> internalSendWithTxnAsync(Message<?> message, Transaction txn) {
        if (txn == null) {
            return internalSendAsync(message);
        } else {
            return ((TransactionImpl) txn).registerProducedTopic(topic)
                        .thenCompose(ignored -> internalSendAsync(message));
        }
    }

    /**
     * Compress the payload if compression is configured
     * @param payload
     * @return a new payload
     */
    private ByteBuf applyCompression(ByteBuf payload) {
        ByteBuf compressedPayload = compressor.encode(payload);
        payload.release();
        return compressedPayload;
    }

    public void sendAsync(Message<?> message, SendCallback callback) {
        checkArgument(message instanceof MessageImpl);

        if (!isValidProducerState(callback, message.getSequenceId())) {
            return;
        }

        MessageImpl<?> msg = (MessageImpl<?>) message;
        MessageMetadata msgMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        int uncompressedSize = payload.readableBytes();

        if (!canEnqueueRequest(callback, message.getSequenceId(), uncompressedSize)) {
            return;
        }

        // If compression is enabled, we are compressing, otherwise it will simply use the same buffer
        ByteBuf compressedPayload = payload;
        boolean compressed = false;
        // Batch will be compressed when closed
        // If a message has a delayed delivery time, we'll always send it individually
        if (!isBatchMessagingEnabled() || msgMetadata.hasDeliverAtTime()) {
            compressedPayload = applyCompression(payload);
            compressed = true;

            // validate msg-size (For batching this will be check at the batch completion size)
            int compressedSize = compressedPayload.readableBytes();
            if (compressedSize > ClientCnx.getMaxMessageSize() && !this.conf.isChunkingEnabled()) {
                compressedPayload.release();
                String compressedStr = (!isBatchMessagingEnabled() && conf.getCompressionType() != CompressionType.NONE)
                                           ? "Compressed"
                                           : "";
                PulsarClientException.InvalidMessageException invalidMessageException = new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s sends a %s message with %d bytes that exceeds %d bytes",
                        producerName, topic, compressedStr, compressedSize, ClientCnx.getMaxMessageSize()));
                completeCallbackAndReleaseSemaphore(uncompressedSize, callback, invalidMessageException);
                return;
            }
        }

        if (!msg.isReplicated() && msgMetadata.hasProducerName()) {
            PulsarClientException.InvalidMessageException invalidMessageException =
                new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s can not reuse the same message", producerName, topic), msg.getSequenceId());
            completeCallbackAndReleaseSemaphore(uncompressedSize, callback, invalidMessageException);
            compressedPayload.release();
            return;
        }

        if (!populateMessageSchema(msg, callback)) {
            compressedPayload.release();
            return;
        }

        // send in chunks
        int totalChunks = canAddToBatch(msg) ? 1
                : Math.max(1, compressedPayload.readableBytes()) / ClientCnx.getMaxMessageSize()
                        + (Math.max(1, compressedPayload.readableBytes()) % ClientCnx.getMaxMessageSize() == 0 ? 0 : 1);
        // chunked message also sent individually so, try to acquire send-permits
        for (int i = 0; i < (totalChunks - 1); i++) {
            if (!canEnqueueRequest(callback, message.getSequenceId(), 0 /* The memory was already reserved */)) {
                return;
            }
        }

        try {
            synchronized (this) {
                int readStartIndex = 0;
                long sequenceId;
                if (!msgMetadata.hasSequenceId()) {
                    sequenceId = msgIdGeneratorUpdater.getAndIncrement(this);
                    msgMetadata.setSequenceId(sequenceId);
                } else {
                    sequenceId = msgMetadata.getSequenceId();
                }
                String uuid = totalChunks > 1 ? String.format("%s-%d", producerName, sequenceId) : null;
                byte[] schemaVersion = totalChunks > 1 && msg.getMessageBuilder().hasSchemaVersion() ?
                        msg.getMessageBuilder().getSchemaVersion() : null;
                for (int chunkId = 0; chunkId < totalChunks; chunkId++) {
                    // Need to reset the schemaVersion, because the schemaVersion is based on a ByteBuf object in
                    // `MessageMetadata`, if we want to re-serialize the `SEND` command using a same `MessageMetadata`,
                    // we need to reset the ByteBuf of the schemaVersion in `MessageMetadata`, I think we need to
                    // reset `ByteBuf` objects in `MessageMetadata` after call the method `MessageMetadata#writeTo()`.
                    if (chunkId > 0 && schemaVersion != null) {
                        msg.getMessageBuilder().setSchemaVersion(schemaVersion);
                    }
                    serializeAndSendMessage(msg, payload, sequenceId, uuid, chunkId, totalChunks,
                            readStartIndex, ClientCnx.getMaxMessageSize(), compressedPayload, compressed,
                            compressedPayload.readableBytes(), uncompressedSize, callback);
                    readStartIndex = ((chunkId + 1) * ClientCnx.getMaxMessageSize());
                }
            }
        } catch (PulsarClientException e) {
            e.setSequenceId(msg.getSequenceId());
            completeCallbackAndReleaseSemaphore(uncompressedSize, callback, e);
        } catch (Throwable t) {
            completeCallbackAndReleaseSemaphore(uncompressedSize, callback, new PulsarClientException(t, msg.getSequenceId()));
        }
    }

    private void serializeAndSendMessage(MessageImpl<?> msg, ByteBuf payload,
            long sequenceId, String uuid, int chunkId, int totalChunks, int readStartIndex, int chunkMaxSizeInBytes, ByteBuf compressedPayload,
            boolean compressed, int compressedPayloadSize,
            int uncompressedSize, SendCallback callback) throws IOException, InterruptedException {
        ByteBuf chunkPayload = compressedPayload;
        MessageMetadata msgMetadata = msg.getMessageBuilder();
        if (totalChunks > 1 && TopicName.get(topic).isPersistent()) {
            chunkPayload = compressedPayload.slice(readStartIndex,
                    Math.min(chunkMaxSizeInBytes, chunkPayload.readableBytes() - readStartIndex));
            // don't retain last chunk payload and builder as it will be not needed for next chunk-iteration and it will
            // be released once this chunk-message is sent
            if (chunkId != totalChunks - 1) {
                chunkPayload.retain();
            }
            if (uuid != null) {
                msgMetadata.setUuid(uuid);
            }
            msgMetadata.setChunkId(chunkId)
                .setNumChunksFromMsg(totalChunks)
                .setTotalChunkMsgSize(compressedPayloadSize);
        }
        if (!msgMetadata.hasPublishTime()) {
            msgMetadata.setPublishTime(client.getClientClock().millis());

            checkArgument(!msgMetadata.hasProducerName());

            msgMetadata.setProducerName(producerName);

            if (conf.getCompressionType() != CompressionType.NONE) {
                msgMetadata
                        .setCompression(CompressionCodecProvider.convertToWireProtocol(conf.getCompressionType()));
            }
            msgMetadata.setUncompressedSize(uncompressedSize);
        }

        if (canAddToBatch(msg) && totalChunks <= 1) {
            if (canAddToCurrentBatch(msg)) {
                // should trigger complete the batch message, new message will add to a new batch and new batch
                // sequence id use the new message, so that broker can handle the message duplication
                if (sequenceId <= lastSequenceIdPushed) {
                    isLastSequenceIdPotentialDuplicated = true;
                    if (sequenceId <= lastSequenceIdPublished) {
                        log.warn("Message with sequence id {} is definitely a duplicate", sequenceId);
                    } else {
                        log.info("Message with sequence id {} might be a duplicate but cannot be determined at this time.",
                            sequenceId);
                    }
                    doBatchSendAndAdd(msg, callback, payload);
                } else {
                    // Should flush the last potential duplicated since can't combine potential duplicated messages
                    // and non-duplicated messages into a batch.
                    if (isLastSequenceIdPotentialDuplicated) {
                        doBatchSendAndAdd(msg, callback, payload);
                    } else {
                        // handle boundary cases where message being added would exceed
                        // batch size and/or max message size
                        boolean isBatchFull = batchMessageContainer.add(msg, callback);
                        lastSendFuture = callback.getFuture();
                        payload.release();
                        if (isBatchFull) {
                            batchMessageAndSend();
                        }
                    }
                    isLastSequenceIdPotentialDuplicated = false;
                }
            } else {
                doBatchSendAndAdd(msg, callback, payload);
            }
        } else {
            // in this case compression has not been applied by the caller
            // but we have to compress the payload if compression is configured
            if (!compressed) {
                chunkPayload = applyCompression(chunkPayload);
            }
            ByteBuf encryptedPayload = encryptMessage(msgMetadata, chunkPayload);

            // When publishing during replication, we need to set the correct number of message in batch
            // This is only used in tracking the publish rate stats
            int numMessages = msg.getMessageBuilder().hasNumMessagesInBatch()
                    ? msg.getMessageBuilder().getNumMessagesInBatch()
                    : 1;
            final OpSendMsg op;
            if (msg.getSchemaState() == MessageImpl.SchemaState.Ready) {
                ByteBufPair cmd = sendMessage(producerId, sequenceId, numMessages, msgMetadata, encryptedPayload);
                op = OpSendMsg.create(msg, cmd, sequenceId, callback);
            } else {
                op = OpSendMsg.create(msg, null, sequenceId, callback);
                final MessageMetadata finalMsgMetadata = msgMetadata;
                op.rePopulate = () -> {
                    op.cmd = sendMessage(producerId, sequenceId, numMessages, finalMsgMetadata, encryptedPayload);
                };
            }
            op.setNumMessagesInBatch(numMessages);
            op.setBatchSizeByte(encryptedPayload.readableBytes());
            if (totalChunks > 1) {
                op.totalChunks = totalChunks;
                op.chunkId = chunkId;
            }
            lastSendFuture = callback.getFuture();
            processOpSendMsg(op);
        }
    }

    private boolean populateMessageSchema(MessageImpl msg, SendCallback callback) {
        MessageMetadata msgMetadataBuilder = msg.getMessageBuilder();
        if (msg.getSchemaInternal() == schema) {
            schemaVersion.ifPresent(v -> msgMetadataBuilder.setSchemaVersion(v));
            msg.setSchemaState(MessageImpl.SchemaState.Ready);
            return true;
        }
        if (!isMultiSchemaEnabled(true)) {
            PulsarClientException.InvalidMessageException e = new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s is disabled the `MultiSchema`", producerName, topic)
                    , msg.getSequenceId());
            completeCallbackAndReleaseSemaphore(msg.getUncompressedSize(), callback, e);
            return false;
        }
        SchemaHash schemaHash = SchemaHash.of(msg.getSchemaInternal());
        byte[] schemaVersion = schemaCache.get(schemaHash);
        if (schemaVersion != null) {
            msgMetadataBuilder.setSchemaVersion(schemaVersion);
            msg.setSchemaState(MessageImpl.SchemaState.Ready);
        }
        return true;
    }

    private boolean rePopulateMessageSchema(MessageImpl msg) {
        SchemaHash schemaHash = SchemaHash.of(msg.getSchemaInternal());
        byte[] schemaVersion = schemaCache.get(schemaHash);
        if (schemaVersion == null) {
            return false;
        }
        msg.getMessageBuilder().setSchemaVersion(schemaVersion);
        msg.setSchemaState(MessageImpl.SchemaState.Ready);
        return true;
    }

    private void tryRegisterSchema(ClientCnx cnx, MessageImpl msg, SendCallback callback, long expectedCnxEpoch) {
        if (!changeToRegisteringSchemaState()) {
            return;
        }
        SchemaInfo schemaInfo = msg.hasReplicateFrom() ? msg.getSchemaInfoForReplicator() : msg.getSchemaInfo();
        schemaInfo = Optional.ofNullable(schemaInfo)
                                        .filter(si -> si.getType().getValue() > 0)
                                        .orElse(Schema.BYTES.getSchemaInfo());
        getOrCreateSchemaAsync(cnx, schemaInfo).handle((v, ex) -> {
            if (ex != null) {
                Throwable t = FutureUtil.unwrapCompletionException(ex);
                log.warn("[{}] [{}] GetOrCreateSchema error", topic, producerName, t);
                if (t instanceof PulsarClientException.IncompatibleSchemaException) {
                    msg.setSchemaState(MessageImpl.SchemaState.Broken);
                    callback.sendComplete((PulsarClientException.IncompatibleSchemaException) t);
                }
            } else {
                log.info("[{}] [{}] GetOrCreateSchema succeed", topic, producerName);
                SchemaHash schemaHash = SchemaHash.of(msg.getSchemaInternal());
                schemaCache.putIfAbsent(schemaHash, v);
                msg.getMessageBuilder().setSchemaVersion(v);
                msg.setSchemaState(MessageImpl.SchemaState.Ready);
            }
            cnx.ctx().channel().eventLoop().execute(() -> {
                synchronized (ProducerImpl.this) {
                    recoverProcessOpSendMsgFrom(cnx, msg, expectedCnxEpoch);
                }
            });
            return null;
        });
    }

    private CompletableFuture<byte[]> getOrCreateSchemaAsync(ClientCnx cnx, SchemaInfo schemaInfo) {
        if (!Commands.peerSupportsGetOrCreateSchema(cnx.getRemoteEndpointProtocolVersion())) {
            return FutureUtil.failedFuture(
                new PulsarClientException.NotSupportedException(
                    format("The command `GetOrCreateSchema` is not supported for the protocol version %d. " +
                        "The producer is %s, topic is %s", cnx.getRemoteEndpointProtocolVersion(), producerName, topic)));
        }
        long requestId = client.newRequestId();
        ByteBuf request = Commands.newGetOrCreateSchema(requestId, topic, schemaInfo);
        log.info("[{}] [{}] GetOrCreateSchema request", topic, producerName);
        return cnx.sendGetOrCreateSchema(request, requestId);
    }

    protected ByteBuf encryptMessage(MessageMetadata msgMetadata, ByteBuf compressedPayload)
            throws PulsarClientException {

        if (!conf.isEncryptionEnabled() || msgCrypto == null) {
            return compressedPayload;
        }

        try {
            int maxSize = msgCrypto.getMaxOutputSize(compressedPayload.readableBytes());
            ByteBuf encryptedPayload = PulsarByteBufAllocator.DEFAULT.buffer(maxSize);
            ByteBuffer targetBuffer = encryptedPayload.nioBuffer(0, maxSize);

            msgCrypto.encrypt(conf.getEncryptionKeys(), conf.getCryptoKeyReader(), () -> msgMetadata,
                    compressedPayload.nioBuffer(), targetBuffer);

            encryptedPayload.writerIndex(targetBuffer.remaining());
            compressedPayload.release();
            return encryptedPayload;
        } catch (PulsarClientException e) {
            // Unless config is set to explicitly publish un-encrypted message upon failure, fail the request
            if (conf.getCryptoFailureAction() == ProducerCryptoFailureAction.SEND) {
                log.warn("[{}] [{}] Failed to encrypt message {}. Proceeding with publishing unencrypted message",
                        topic, producerName, e.getMessage());
                return compressedPayload;
            }
            throw e;
        }
    }

    protected ByteBufPair sendMessage(long producerId, long sequenceId, int numMessages, MessageMetadata msgMetadata,
            ByteBuf compressedPayload) {
        return Commands.newSend(producerId, sequenceId, numMessages, getChecksumType(), msgMetadata, compressedPayload);
    }

    protected ByteBufPair sendMessage(long producerId, long lowestSequenceId, long highestSequenceId, int numMessages, MessageMetadata msgMetadata,
                                      ByteBuf compressedPayload) {
        return Commands.newSend(producerId, lowestSequenceId, highestSequenceId, numMessages, getChecksumType(), msgMetadata, compressedPayload);
    }

    protected ChecksumType getChecksumType() {
        if (connectionHandler.cnx() == null
                || connectionHandler.cnx().getRemoteEndpointProtocolVersion() >= brokerChecksumSupportedVersion()) {
            return ChecksumType.Crc32c;
        } else {
            return ChecksumType.None;
        }
    }

    private boolean canAddToBatch(MessageImpl<?> msg) {
        return msg.getSchemaState() == MessageImpl.SchemaState.Ready
                && isBatchMessagingEnabled() && !msg.getMessageBuilder().hasDeliverAtTime();
    }

    private boolean canAddToCurrentBatch(MessageImpl<?> msg) {
        return batchMessageContainer.haveEnoughSpace(msg)
               && (!isMultiSchemaEnabled(false) || batchMessageContainer.hasSameSchema(msg))
                && batchMessageContainer.hasSameTxn(msg);
    }

    private void doBatchSendAndAdd(MessageImpl<?> msg, SendCallback callback, ByteBuf payload) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Closing out batch to accommodate large message with size {}", topic, producerName,
                    msg.getUncompressedSize());
        }
        try {
            batchMessageAndSend();
            batchMessageContainer.add(msg, callback);
            lastSendFuture = callback.getFuture();
        } finally {
            payload.release();
        }
    }

    private boolean isValidProducerState(SendCallback callback, long sequenceId) {
        switch (getState()) {
        case Ready:
            // OK
        case Connecting:
            // We are OK to queue the messages on the client, it will be sent to the broker once we get the connection
        case RegisteringSchema:
            // registering schema
            return true;
        case Closing:
        case Closed:
            callback.sendComplete(new PulsarClientException.AlreadyClosedException("Producer already closed", sequenceId));
            return false;
        case ProducerFenced:
            callback.sendComplete(new PulsarClientException.ProducerFencedException("Producer was fenced"));
            return false;
        case Terminated:
            callback.sendComplete(new PulsarClientException.TopicTerminatedException("Topic was terminated", sequenceId));
            return false;
        case Failed:
        case Uninitialized:
        default:
            callback.sendComplete(new PulsarClientException.NotConnectedException(sequenceId));
            return false;
        }
    }

    private boolean canEnqueueRequest(SendCallback callback, long sequenceId, int payloadSize) {
        try {
            if (conf.isBlockIfQueueFull()) {
                if (semaphore.isPresent()) {
                    semaphore.get().acquire();
                }
                client.getMemoryLimitController().reserveMemory(payloadSize);
            } else {
                if (!semaphore.map(Semaphore::tryAcquire).orElse(true)) {
                    callback.sendComplete(new PulsarClientException.ProducerQueueIsFullError("Producer send queue is full", sequenceId));
                    return false;
                }

                if (!client.getMemoryLimitController().tryReserveMemory(payloadSize)) {
                    semaphore.ifPresent(Semaphore::release);
                    callback.sendComplete(new PulsarClientException.MemoryBufferIsFullError("Client memory buffer is full", sequenceId));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.sendComplete(new PulsarClientException(e, sequenceId));
            return false;
        }

        return true;
    }

    private static final class WriteInEventLoopCallback implements Runnable {
        private ProducerImpl<?> producer;
        private ByteBufPair cmd;
        private long sequenceId;
        private ClientCnx cnx;
        private OpSendMsg op;

        static WriteInEventLoopCallback create(ProducerImpl<?> producer, ClientCnx cnx, OpSendMsg op) {
            WriteInEventLoopCallback c = RECYCLER.get();
            c.producer = producer;
            c.cnx = cnx;
            c.sequenceId = op.sequenceId;
            c.cmd = op.cmd;
            c.op = op;
            return c;
        }

        @Override
        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Sending message cnx {}, sequenceId {}", producer.topic, producer.producerName, cnx,
                        sequenceId);
            }

            try {
                cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                op.updateSentTimestamp();
            } finally {
                recycle();
            }
        }

        private void recycle() {
            producer = null;
            cnx = null;
            cmd = null;
            sequenceId = -1;
            op = null;
            recyclerHandle.recycle(this);
        }

        private final Handle<WriteInEventLoopCallback> recyclerHandle;

        private WriteInEventLoopCallback(Handle<WriteInEventLoopCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<WriteInEventLoopCallback> RECYCLER = new Recycler<WriteInEventLoopCallback>() {
            @Override
            protected WriteInEventLoopCallback newObject(Handle<WriteInEventLoopCallback> handle) {
                return new WriteInEventLoopCallback(handle);
            }
        };
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final State currentState = getAndUpdateState(state -> {
            if (state == State.Closed) {
                return state;
            }
            return State.Closing;
        });

        if (currentState == State.Closed || currentState == State.Closing) {
            return CompletableFuture.completedFuture(null);
        }

        Timeout timeout = sendTimeout;
        if (timeout != null) {
            timeout.cancel();
            sendTimeout = null;
        }

        ScheduledFuture<?> batchTimerTask = this.batchTimerTask;
        if (batchTimerTask != null) {
            batchTimerTask.cancel(false);
            this.batchTimerTask = null;
        }

        if (keyGeneratorTask != null && !keyGeneratorTask.isCancelled()) {
            keyGeneratorTask.cancel(false);
        }

        stats.cancelStatsTimeout();

        ClientCnx cnx = cnx();
        if (cnx == null || currentState != State.Ready) {
            log.info("[{}] [{}] Closed Producer (not connected)", topic, producerName);
            closeAndClearPendingMessages();
            return CompletableFuture.completedFuture(null);
        }

        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newCloseProducer(producerId, requestId);

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
            cnx.removeProducer(producerId);
            if (exception == null || !cnx.ctx().channel().isActive()) {
                // Either we've received the success response for the close producer command from the broker, or the
                // connection did break in the meantime. In any case, the producer is gone.
                log.info("[{}] [{}] Closed Producer", topic, producerName);
                closeAndClearPendingMessages();
                closeFuture.complete(null);
            } else {
                closeFuture.completeExceptionally(exception);
            }

            return null;
        });

        return closeFuture;
    }

    private synchronized void closeAndClearPendingMessages() {
        setState(State.Closed);
        client.cleanupProducer(this);
        PulsarClientException ex = new PulsarClientException.AlreadyClosedException(
                format("The producer %s of the topic %s was already closed when closing the producers",
                        producerName, topic));
        // Use null for cnx to ensure that the pending messages are failed immediately
        failPendingMessages(null, ex);
    }

    @Override
    public boolean isConnected() {
        return getCnxIfReady() != null;
    }

    /**
     * Hook method for testing. By returning null, it's possible to prevent messages
     * being delivered to the broker.
     *
     * @return cnx if OpSend messages should be written to open connection. Caller must
     * verify that the returned cnx is not null before using reference.
     */
    protected ClientCnx getCnxIfReady() {
        if (getState() == State.Ready) {
            return connectionHandler.cnx();
        } else {
            return null;
        }
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return connectionHandler.lastConnectionClosedTimestamp;
    }

    public boolean isWritable() {
        ClientCnx cnx = connectionHandler.cnx();
        return cnx != null && cnx.channel().isWritable();
    }

    public void terminated(ClientCnx cnx) {
        State previousState = getAndUpdateState(state -> (state == State.Closed ? State.Closed : State.Terminated));
        if (previousState != State.Terminated && previousState != State.Closed) {
            log.info("[{}] [{}] The topic has been terminated", topic, producerName);
            setClientCnx(null);
            synchronized (this) {
                failPendingMessages(cnx,
                        new PulsarClientException.TopicTerminatedException(
                                format("The topic %s that the producer %s produces to has been terminated", topic, producerName)));
            }
        }
    }

    void ackReceived(ClientCnx cnx, long sequenceId, long highestSequenceId, long ledgerId, long entryId) {
        OpSendMsg op = null;
        synchronized (this) {
            op = pendingMessages.peek();
            if (op == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg {} - {}", topic, producerName, sequenceId, highestSequenceId);
                }
                return;
            }

            if (sequenceId > op.sequenceId) {
                log.warn("[{}] [{}] Got ack for msg. expecting: {} - {} - got: {} - {} - queue-size: {}", topic, producerName,
                        op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId, pendingMessages.size());
                // Force connection closing so that messages can be re-transmitted in a new connection
                cnx.channel().close();
                return;
            } else if (sequenceId < op.sequenceId) {
                // Ignoring the ack since it's referring to a message that has already timed out.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg. expecting: {} - {} - got: {} - {}", topic, producerName,
                            op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId);
                }
                return;
            } else {
                // Add check `sequenceId >= highestSequenceId` for backward compatibility.
                if (sequenceId >= highestSequenceId || highestSequenceId == op.highestSequenceId) {
                    // Message was persisted correctly
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Received ack for msg {} ", topic, producerName, sequenceId);
                    }
                    pendingMessages.remove();
                    releaseSemaphoreForSendOp(op);
                } else {
                    log.warn("[{}] [{}] Got ack for batch msg error. expecting: {} - {} - got: {} - {} - queue-size: {}", topic, producerName,
                            op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId, pendingMessages.size());
                    // Force connection closing so that messages can be re-transmitted in a new connection
                    cnx.channel().close();
                    return;
                }
            }
        }

        OpSendMsg finalOp = op;
        LAST_SEQ_ID_PUBLISHED_UPDATER.getAndUpdate(this, last -> Math.max(last, getHighestSequenceId(finalOp)));
        op.setMessageId(ledgerId, entryId, partitionIndex);
        // if message is chunked then call callback only on last chunk
        if (op.totalChunks <= 1 || (op.chunkId == op.totalChunks - 1)) {
            try {
                // Need to protect ourselves from any exception being thrown in the future handler from the
                // application
                op.sendComplete(null);
            } catch (Throwable t) {
                log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic,
                        producerName, sequenceId, t);
            }
        }
        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
    }

    private long getHighestSequenceId(OpSendMsg op) {
        return Math.max(op.highestSequenceId, op.sequenceId);
    }

    private void releaseSemaphoreForSendOp(OpSendMsg op) {

        semaphoreRelease(isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);

        client.getMemoryLimitController().releaseMemory(op.uncompressedSize);
    }

    private void completeCallbackAndReleaseSemaphore(long payloadSize, SendCallback callback, Exception exception) {
        semaphore.ifPresent(Semaphore::release);
        client.getMemoryLimitController().releaseMemory(payloadSize);
        callback.sendComplete(exception);
    }

    /**
     * Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
     * message header-payload again.
     * <ul>
     * <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
     * message</li>
     * <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
     * So, fail send-message by failing callback</li>
     * </ul>
     *
     * @param cnx
     * @param sequenceId
     */
    protected synchronized void recoverChecksumError(ClientCnx cnx, long sequenceId) {
        OpSendMsg op = pendingMessages.peek();
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Got send failure for timed out msg {}", topic, producerName, sequenceId);
            }
        } else {
            long expectedSequenceId = getHighestSequenceId(op);
            if (sequenceId == expectedSequenceId) {
                boolean corrupted = !verifyLocalBufferIsNotCorrupted(op);
                if (corrupted) {
                    // remove message from pendingMessages queue and fail callback
                    pendingMessages.remove();
                    releaseSemaphoreForSendOp(op);
                    try {
                        op.sendComplete(
                            new PulsarClientException.ChecksumException(
                                format("The checksum of the message which is produced by producer %s to the topic " +
                                    "%s is corrupted", producerName, topic)));
                    } catch (Throwable t) {
                        log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic,
                                producerName, sequenceId, t);
                    }
                    ReferenceCountUtil.safeRelease(op.cmd);
                    op.recycle();
                    return;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Message is not corrupted, retry send-message with sequenceId {}", topic,
                                producerName, sequenceId);
                    }
                }

            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Corrupt message is already timed out {}", topic, producerName, sequenceId);
                }
            }
        }
        // as msg is not corrupted : let producer resend pending-messages again including checksum failed message
        resendMessages(cnx, this.connectionHandler.getEpoch());
    }

    protected synchronized void recoverNotAllowedError(long sequenceId) {
        OpSendMsg op = pendingMessages.peek();
        if(op != null && sequenceId == getHighestSequenceId(op)){
            pendingMessages.remove();
            releaseSemaphoreForSendOp(op);
            try {
                op.sendComplete(
                        new PulsarClientException.NotAllowedException(
                                format("The size of the message which is produced by producer %s to the topic " +
                                        "%s is not allowed", producerName, topic)));
            } catch (Throwable t) {
                log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic,
                        producerName, sequenceId, t);
            }
            ReferenceCountUtil.safeRelease(op.cmd);
            op.recycle();
        }
    }

    /**
     * Computes checksum again and verifies it against existing checksum. If checksum doesn't match it means that
     * message is corrupt.
     *
     * @param op
     * @return returns true only if message is not modified and computed-checksum is same as previous checksum else
     *         return false that means that message is corrupted. Returns true if checksum is not present.
     */
    protected boolean verifyLocalBufferIsNotCorrupted(OpSendMsg op) {
        ByteBufPair msg = op.cmd;

        if (msg != null) {
            ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                // skip bytes up to checksum index
                headerFrame.skipBytes(4); // skip [total-size]
                int cmdSize = (int) headerFrame.readUnsignedInt();
                headerFrame.skipBytes(cmdSize);
                // verify if checksum present
                if (hasChecksum(headerFrame)) {
                    int checksum = readChecksum(headerFrame);
                    // msg.readerIndex is already at header-payload index, Recompute checksum for headers-payload
                    int metadataChecksum = computeChecksum(headerFrame);
                    long computedChecksum = resumeChecksum(metadataChecksum, msg.getSecond());
                    return checksum == computedChecksum;
                } else {
                    log.warn("[{}] [{}] checksum is not present into message with id {}", topic, producerName,
                            op.sequenceId);
                }
            } finally {
                headerFrame.resetReaderIndex();
            }
            return true;
        } else {
            log.warn("[{}] Failed while casting empty ByteBufPair, ", producerName);
            return false;
        }
    }

    protected static final class OpSendMsg {
        MessageImpl<?> msg;
        List<MessageImpl<?>> msgs;
        ByteBufPair cmd;
        SendCallback callback;
        Runnable rePopulate;
        long uncompressedSize;
        long sequenceId;
        long createdAt;
        long firstSentAt;
        long lastSentAt;
        int retryCount;
        long batchSizeByte = 0;
        int numMessagesInBatch = 1;
        long highestSequenceId;
        int totalChunks = 0;
        int chunkId = -1;

        static OpSendMsg create(MessageImpl<?> msg, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msg = msg;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.nanoTime();
            op.uncompressedSize = msg.getUncompressedSize();
            return op;
        }

        static OpSendMsg create(List<MessageImpl<?>> msgs, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msgs = msgs;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.nanoTime();
            op.uncompressedSize = 0;
            for (int i = 0; i < msgs.size(); i++) {
                op.uncompressedSize += msgs.get(i).getUncompressedSize();
            }
            return op;
        }

        static OpSendMsg create(List<MessageImpl<?>> msgs, ByteBufPair cmd, long lowestSequenceId,
                                long highestSequenceId,  SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msgs = msgs;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = lowestSequenceId;
            op.highestSequenceId = highestSequenceId;
            op.createdAt = System.nanoTime();
            op.uncompressedSize = 0;
            for (int i = 0; i < msgs.size(); i++) {
                op.uncompressedSize += msgs.get(i).getUncompressedSize();
            }
            return op;
        }

        void updateSentTimestamp() {
            this.lastSentAt = System.nanoTime();
            if (this.firstSentAt == -1L) {
                this.firstSentAt = this.lastSentAt;
            }
            ++this.retryCount;
        }

        void sendComplete(final Exception e) {
            SendCallback callback = this.callback;
            if (null != callback) {
                Exception finalEx = e;
                if (finalEx != null && finalEx instanceof TimeoutException) {
                    TimeoutException te = (TimeoutException) e;
                    long sequenceId = te.getSequenceId();
                    long ns = System.nanoTime();
                    String errMsg = String.format(
                        "%s : createdAt %s ns ago, firstSentAt %s ns ago, lastSentAt %s ns ago, retryCount %s",
                        te.getMessage(),
                        ns - this.createdAt,
                        this.firstSentAt <= 0 ? ns - this.lastSentAt : ns - this.firstSentAt,
                        ns - this.lastSentAt,
                        retryCount
                    );

                    finalEx = new TimeoutException(errMsg, sequenceId);
                }

                callback.sendComplete(finalEx);
            }
        }

        void recycle() {
            msg = null;
            msgs = null;
            cmd = null;
            callback = null;
            rePopulate = null;
            sequenceId = -1L;
            createdAt = -1L;
            firstSentAt = -1L;
            lastSentAt = -1L;
            highestSequenceId = -1L;
            totalChunks = 0;
            chunkId = -1;
            uncompressedSize = 0;
            retryCount = 0;
            batchSizeByte = 0;
            numMessagesInBatch = 1;
            recyclerHandle.recycle(this);
        }

        void setNumMessagesInBatch(int numMessagesInBatch) {
            this.numMessagesInBatch = numMessagesInBatch;
        }

        void setBatchSizeByte(long batchSizeByte) {
            this.batchSizeByte = batchSizeByte;
        }

        void setMessageId(long ledgerId, long entryId, int partitionIndex) {
            if (msg != null) {
                msg.setMessageId(new MessageIdImpl(ledgerId, entryId, partitionIndex));
            } else {
                for (int batchIndex = 0; batchIndex < msgs.size(); batchIndex++) {
                    msgs.get(batchIndex)
                            .setMessageId(new BatchMessageIdImpl(ledgerId, entryId, partitionIndex, batchIndex));
                }
            }
        }

        private OpSendMsg(Handle<OpSendMsg> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private final Handle<OpSendMsg> recyclerHandle;
        private static final Recycler<OpSendMsg> RECYCLER = new Recycler<OpSendMsg>() {
            @Override
            protected OpSendMsg newObject(Handle<OpSendMsg> handle) {
                return new OpSendMsg(handle);
            }
        };
    }

    /**
     * Queue implementation that is used as the pending messages queue.
     *
     * This implementation postpones adding of new OpSendMsg entries that happen
     * while the forEach call is in progress. This is needed for preventing
     * ConcurrentModificationExceptions that would occur when the forEach action
     * calls the add method via a callback in user code.
     *
     * This queue is not thread safe.
     */
    protected static class OpSendMsgQueue implements Iterable<OpSendMsg> {
        private final Queue<OpSendMsg> delegate = new ArrayDeque<>();
        private int forEachDepth = 0;
        private List<OpSendMsg> postponedOpSendMgs;

        @Override
        public void forEach(Consumer<? super OpSendMsg> action) {
            try {
                // track any forEach call that is in progress in the current call stack
                // so that adding a new item while iterating doesn't cause ConcurrentModificationException
                forEachDepth++;
                delegate.forEach(action);
            } finally {
                forEachDepth--;
                // if this is the top-most forEach call and there are postponed items, add them
                if (forEachDepth == 0 && postponedOpSendMgs != null && !postponedOpSendMgs.isEmpty()) {
                    delegate.addAll(postponedOpSendMgs);
                    postponedOpSendMgs.clear();
                }
            }
        }

        public boolean add(OpSendMsg o) {
            // postpone adding to the queue while forEach iteration is in progress
            if (forEachDepth > 0) {
                if (postponedOpSendMgs == null) {
                    postponedOpSendMgs = new ArrayList<>();
                }
                return postponedOpSendMgs.add(o);
            } else {
                return delegate.add(o);
            }
        }

        public void clear() {
            delegate.clear();
        }

        public void remove() {
            delegate.remove();
        }

        public OpSendMsg peek() {
            return delegate.peek();
        }

        public int size() {
            return delegate.size();
        }

        @Override
        public Iterator<OpSendMsg> iterator() {
            return delegate.iterator();
        }
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        previousExceptions.clear();

        final long epoch;
        synchronized (this) {
            // Because the state could have been updated while retrieving the connection, we set it back to connecting,
            // as long as the change from current state to connecting is a valid state change.
            if (!changeToConnecting()) {
                return;
            }
            // We set the cnx reference before registering the producer on the cnx, so if the cnx breaks before creating
            // the producer, it will try to grab a new cnx. We also increment and get the epoch value for the producer.
            epoch = connectionHandler.switchClientCnx(cnx);
        }
        cnx.registerProducer(producerId, this);

        log.info("[{}] [{}] Creating producer on cnx {}", topic, producerName, cnx.ctx().channel());

        long requestId = client.newRequestId();

        PRODUCER_DEADLINE_UPDATER
            .compareAndSet(this, 0, System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs());

        SchemaInfo schemaInfo = null;
        if (schema != null) {
            if (schema.getSchemaInfo() != null) {
                if (schema.getSchemaInfo().getType() == SchemaType.JSON) {
                    // for backwards compatibility purposes
                    // JSONSchema originally generated a schema for pojo based of of the JSON schema standard
                    // but now we have standardized on every schema to generate an Avro based schema
                    if (Commands.peerSupportJsonSchemaAvroFormat(cnx.getRemoteEndpointProtocolVersion())) {
                        schemaInfo = schema.getSchemaInfo();
                    } else if (schema instanceof JSONSchema){
                        JSONSchema jsonSchema = (JSONSchema) schema;
                        schemaInfo = jsonSchema.getBackwardsCompatibleJsonSchemaInfo();
                    } else {
                        schemaInfo = schema.getSchemaInfo();
                    }
                } else if (schema.getSchemaInfo().getType() == SchemaType.BYTES
                        || schema.getSchemaInfo().getType() == SchemaType.NONE) {
                    // don't set schema info for Schema.BYTES
                    schemaInfo = null;
                } else {
                    schemaInfo = schema.getSchemaInfo();
                }
            }
        }

        cnx.sendRequestWithId(
                Commands.newProducer(topic, producerId, requestId, producerName, conf.isEncryptionEnabled(), metadata,
                       schemaInfo, epoch, userProvidedProducerName,
                       conf.getAccessMode(), topicEpoch, client.conf.isEnableTransaction()),
                requestId).thenAccept(response -> {
                    String producerName = response.getProducerName();
                    long lastSequenceId = response.getLastSequenceId();
                    schemaVersion = Optional.ofNullable(response.getSchemaVersion());
                    schemaVersion.ifPresent(v -> schemaCache.put(SchemaHash.of(schema), v));

                    // We are now reconnected to broker and clear to send messages. Re-send all pending messages and
                    // set the cnx pointer so that new messages will be sent immediately
                    synchronized (ProducerImpl.this) {
                        if (getState() == State.Closing || getState() == State.Closed) {
                            // Producer was closed while reconnecting, close the connection to make sure the broker
                            // drops the producer on its side
                            cnx.removeProducer(producerId);
                            cnx.channel().close();
                            return;
                        }
                        resetBackoff();

                        log.info("[{}] [{}] Created producer on cnx {}", topic, producerName, cnx.ctx().channel());
                        connectionId = cnx.ctx().channel().toString();
                        connectedSince = DateFormatter.now();
                        if (conf.getAccessMode() != ProducerAccessMode.Shared && !topicEpoch.isPresent()) {
                            log.info("[{}] [{}] Producer epoch is {}", topic, producerName, response.getTopicEpoch());
                        }
                        topicEpoch = response.getTopicEpoch();


                        if (this.producerName == null) {
                            this.producerName = producerName;
                        }

                        if (this.msgIdGenerator == 0 && conf.getInitialSequenceId() == null) {
                            // Only update sequence id generator if it wasn't already modified. That means we only want
                            // to update the id generator the first time the producer gets established, and ignore the
                            // sequence id sent by broker in subsequent producer reconnects
                            this.lastSequenceIdPublished = lastSequenceId;
                            this.msgIdGenerator = lastSequenceId + 1;
                        }

                        if (!producerCreatedFuture.isDone() && isBatchMessagingEnabled()) {
                            // schedule the first batch message task
                            batchTimerTask = cnx.ctx().executor()
                                    .scheduleAtFixedRate(catchingAndLoggingThrowables(() -> {
                                        if (log.isTraceEnabled()) {
                                            log.trace(
                                                    "[{}] [{}] Batching the messages from the batch container from "
                                                            + "timer thread",
                                                    topic,
                                                    producerName);
                                        }
                                        // semaphore acquired when message was enqueued to container
                                        synchronized (ProducerImpl.this) {
                                            // If it's closing/closed we need to ignore the send batch timer and not
                                            // schedule next timeout.
                                            if (getState() == State.Closing || getState() == State.Closed) {
                                                return;
                                            }

                                            batchMessageAndSend();
                                        }
                                    }), 0, conf.getBatchingMaxPublishDelayMicros(), TimeUnit.MICROSECONDS);
                        }
                        resendMessages(cnx, epoch);
                    }
                }).exceptionally((e) -> {
                    Throwable cause = e.getCause();
                    cnx.removeProducer(producerId);
                    if (getState() == State.Closing || getState() == State.Closed) {
                        // Producer was closed while reconnecting, close the connection to make sure the broker
                        // drops the producer on its side
                        cnx.channel().close();
                        return null;
                    }

                    if (cause instanceof TimeoutException) {
                        // Creating the producer has timed out. We need to ensure the broker closes the producer
                        // in case it was indeed created, otherwise it might prevent new create producer operation,
                        // since we are not necessarily closing the connection.
                        long closeRequestId = client.newRequestId();
                        ByteBuf cmd = Commands.newCloseProducer(producerId, closeRequestId);
                        cnx.sendRequestWithId(cmd, closeRequestId);
                    }

                    if (cause instanceof PulsarClientException.ProducerFencedException) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] Failed to create producer: {}",
                                    topic, producerName, cause.getMessage());
                        }
                    } else {
                        log.error("[{}] [{}] Failed to create producer: {}", topic, producerName, cause.getMessage());
                    }
                    // Close the producer since topic does not exist.
                    if (cause instanceof PulsarClientException.TopicDoesNotExistException) {
                        closeAsync().whenComplete((v, ex) -> {
                            if (ex != null) {
                                log.error("Failed to close producer on TopicDoesNotExistException.", ex);
                            }
                            producerCreatedFuture.completeExceptionally(cause);
                        });
                        return null;
                    }
                    if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededException) {
                        synchronized (this) {
                            log.warn("[{}] [{}] Topic backlog quota exceeded. Throwing Exception on producer.", topic,
                                    producerName);

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] [{}] Pending messages: {}", topic, producerName,
                                        pendingMessages.size());
                            }

                            PulsarClientException bqe = new PulsarClientException.ProducerBlockedQuotaExceededException(
                                format("The backlog quota of the topic %s that the producer %s produces to is exceeded",
                                    topic, producerName));
                            failPendingMessages(cnx(), bqe);
                        }
                    } else if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededError) {
                        log.warn("[{}] [{}] Producer is blocked on creation because backlog exceeded on topic.",
                                producerName, topic);
                    }

                    if (cause instanceof PulsarClientException.TopicTerminatedException) {
                        setState(State.Terminated);
                        synchronized (this) {
                            failPendingMessages(cnx(), (PulsarClientException) cause);
                        }
                        producerCreatedFuture.completeExceptionally(cause);
                        client.cleanupProducer(this);
                    } else if (cause instanceof PulsarClientException.ProducerFencedException) {
                        setState(State.ProducerFenced);
                        synchronized (this) {
                            failPendingMessages(cnx(), (PulsarClientException) cause);
                        }
                        producerCreatedFuture.completeExceptionally(cause);
                        client.cleanupProducer(this);
                    } else if (producerCreatedFuture.isDone() || //
                               (cause instanceof PulsarClientException && PulsarClientException.isRetriableError(cause)
                                && System.currentTimeMillis() < PRODUCER_DEADLINE_UPDATER.get(ProducerImpl.this))) {
                        // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                        // still within the initial timeout budget and we are dealing with a retriable error
                        reconnectLater(cause);
                    } else {
                        setState(State.Failed);
                        producerCreatedFuture.completeExceptionally(cause);
                        client.cleanupProducer(this);
                        Timeout timeout = sendTimeout;
                        if (timeout != null) {
                            timeout.cancel();
                            sendTimeout = null;
                        }
                    }

                    return null;
                });
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        boolean timeout = System.currentTimeMillis() > lookupDeadline;
        if (nonRetriableError || timeout) {
            exception.setPreviousExceptions(previousExceptions);
            if (producerCreatedFuture.completeExceptionally(exception)) {
                if (nonRetriableError) {
                    log.info("[{}] Producer creation failed for producer {} with unretriableError = {}", topic, producerId, exception);
                } else {
                    log.info("[{}] Producer creation failed for producer {} after producerTimeout", topic, producerId);
                }
                setState(State.Failed);
                client.cleanupProducer(this);
            }
        } else {
            previousExceptions.add(exception);
        }
    }

    private void resendMessages(ClientCnx cnx, long expectedEpoch) {
        cnx.ctx().channel().eventLoop().execute(() -> {
            synchronized (this) {
                if (getState() == State.Closing || getState() == State.Closed) {
                    // Producer was closed while reconnecting, close the connection to make sure the broker
                    // drops the producer on its side
                    cnx.channel().close();
                    return;
                }
                int messagesToResend = pendingMessages.size();
                if (messagesToResend == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] No pending messages to resend {}", topic, producerName, messagesToResend);
                    }
                    if (changeToReadyState()) {
                        producerCreatedFuture.complete(ProducerImpl.this);
                        return;
                    } else {
                        // Producer was closed while reconnecting, close the connection to make sure the broker
                        // drops the producer on its side
                        cnx.channel().close();
                        return;
                    }

                }

                log.info("[{}] [{}] Re-Sending {} messages to server", topic, producerName, messagesToResend);
                recoverProcessOpSendMsgFrom(cnx, null, expectedEpoch);
            }
        });
    }

    /**
     * Strips checksum from {@link OpSendMsg} command if present else ignore it.
     *
     * @param op
     */
    private void stripChecksum(OpSendMsg op) {
        ByteBufPair msg = op.cmd;
        if (msg != null) {
            int totalMsgBufSize = msg.readableBytes();
            ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                headerFrame.skipBytes(4); // skip [total-size]
                int cmdSize = (int) headerFrame.readUnsignedInt();

                // verify if checksum present
                headerFrame.skipBytes(cmdSize);

                if (!hasChecksum(headerFrame)) {
                    return;
                }

                int headerSize = 4 + 4 + cmdSize; // [total-size] [cmd-length] [cmd-size]
                int checksumSize = 4 + 2; // [magic-number] [checksum-size]
                int checksumMark = (headerSize + checksumSize); // [header-size] [checksum-size]
                int metaPayloadSize = (totalMsgBufSize - checksumMark); // metadataPayload = totalSize - checksumMark
                int newTotalFrameSizeLength = 4 + cmdSize + metaPayloadSize; // new total-size without checksum
                headerFrame.resetReaderIndex();
                int headerFrameSize = headerFrame.readableBytes();

                headerFrame.setInt(0, newTotalFrameSizeLength); // rewrite new [total-size]
                ByteBuf metadata = headerFrame.slice(checksumMark, headerFrameSize - checksumMark); // sliced only
                                                                                                    // metadata
                headerFrame.writerIndex(headerSize); // set headerFrame write-index to overwrite metadata over checksum
                metadata.readBytes(headerFrame, metadata.readableBytes());
                headerFrame.capacity(headerFrameSize - checksumSize); // reduce capacity by removed checksum bytes
            } finally {
                headerFrame.resetReaderIndex();
            }
        } else {
            log.warn("[{}] Failed while casting null into ByteBufPair", producerName);
        }
    }

    public int brokerChecksumSupportedVersion() {
        return ProtocolVersion.v6.getValue();
    }

    @Override
    String getHandlerName() {
        return producerName;
    }

    /**
     * Process sendTimeout events
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }

        long timeToWaitMs;

        synchronized (this) {
            // If it's closing/closed we need to ignore this timeout and not schedule next timeout.
            if (getState() == State.Closing || getState() == State.Closed) {
                return;
            }

            OpSendMsg firstMsg = pendingMessages.peek();
            if (firstMsg == null) {
                // If there are no pending messages, reset the timeout to the configured value.
                timeToWaitMs = conf.getSendTimeoutMs();
            } else {
                // If there is at least one message, calculate the diff between the message timeout and the elapsed
                // time since first message was created.
                long diff = conf.getSendTimeoutMs()
                        - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstMsg.createdAt);
                if (diff <= 0) {
                    // The diff is less than or equal to zero, meaning that the message has been timed out.
                    // Set the callback to timeout on every message, then clear the pending queue.
                    log.info("[{}] [{}] Message send timed out. Failing {} messages", topic, producerName,
                            pendingMessages.size());

                    PulsarClientException te = new PulsarClientException.TimeoutException(
                        format("The producer %s can not send message to the topic %s within given timeout",
                            producerName, topic), firstMsg.sequenceId);
                    failPendingMessages(cnx(), te);
                    // Since the pending queue is cleared now, set timer to expire after configured value.
                    timeToWaitMs = conf.getSendTimeoutMs();
                } else {
                    // The diff is greater than zero, set the timeout to the diff value
                    timeToWaitMs = diff;
                }
            }

            sendTimeout = client.timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * This fails and clears the pending messages with the given exception. This method should be called from within the
     * ProducerImpl object mutex.
     */
    private void failPendingMessages(ClientCnx cnx, PulsarClientException ex) {
        if (cnx == null) {
            final AtomicInteger releaseCount = new AtomicInteger();
            final boolean batchMessagingEnabled = isBatchMessagingEnabled();
            pendingMessages.forEach(op -> {
                releaseCount.addAndGet(batchMessagingEnabled ? op.numMessagesInBatch: 1);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    ex.setSequenceId(op.sequenceId);
                    // if message is chunked then call callback only on last chunk
                    if (op.totalChunks <= 1 || (op.chunkId == op.totalChunks - 1)) {
                        // Need to protect ourselves from any exception being thrown in the future handler from the
                        // application
                        op.sendComplete(ex);
                    }
                } catch (Throwable t) {
                    log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                            op.sequenceId, t);
                }
                client.getMemoryLimitController().releaseMemory(op.uncompressedSize);
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            });

            pendingMessages.clear();
            semaphoreRelease(releaseCount.get());
            if (batchMessagingEnabled) {
                failPendingBatchMessages(ex);
            }

        } else {
            // If we have a connection, we schedule the callback and recycle on the event loop thread to avoid any
            // race condition since we also write the message on the socket from this thread
            cnx.ctx().channel().eventLoop().execute(() -> {
                synchronized (ProducerImpl.this) {
                    failPendingMessages(null, ex);
                }
            });
        }
    }

    /**
     * fail any pending batch messages that were enqueued, however batch was not closed out
     *
     */
    private void failPendingBatchMessages(PulsarClientException ex) {
        if (batchMessageContainer.isEmpty()) {
            return;
        }
        final int numMessagesInBatch = batchMessageContainer.getNumMessagesInBatch();
        batchMessageContainer.discard(ex);
        semaphoreRelease(numMessagesInBatch);
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        CompletableFuture<MessageId> lastSendFuture;
        synchronized (ProducerImpl.this) {
            if (isBatchMessagingEnabled()) {
                batchMessageAndSend();
            }
            lastSendFuture = this.lastSendFuture;
        }
        return lastSendFuture.thenApply(ignored -> null);
    }

    @Override
    protected void triggerFlush() {
        if (isBatchMessagingEnabled()) {
            synchronized (ProducerImpl.this) {
                batchMessageAndSend();
            }
        }
    }

    // must acquire semaphore before enqueuing
    private void batchMessageAndSend() {
        if (log.isTraceEnabled()) {
            log.trace("[{}] [{}] Batching the messages from the batch container with {} messages", topic, producerName,
                    batchMessageContainer.getNumMessagesInBatch());
        }
        if (!batchMessageContainer.isEmpty()) {
            try {
                List<OpSendMsg> opSendMsgs;
                if (batchMessageContainer.isMultiBatches()) {
                    opSendMsgs = batchMessageContainer.createOpSendMsgs();
                } else {
                    opSendMsgs = Collections.singletonList(batchMessageContainer.createOpSendMsg());
                }
                batchMessageContainer.clear();
                for (OpSendMsg opSendMsg : opSendMsgs) {
                    processOpSendMsg(opSendMsg);
                }
            } catch (PulsarClientException e) {
                semaphoreRelease(batchMessageContainer.getNumMessagesInBatch());
            } catch (Throwable t) {
                semaphoreRelease(batchMessageContainer.getNumMessagesInBatch());
                log.warn("[{}] [{}] error while create opSendMsg by batch message container", topic, producerName, t);
            }
        }
    }

    protected void processOpSendMsg(OpSendMsg op) {
        if (op == null) {
            return;
        }
        try {
            if (op.msg != null && isBatchMessagingEnabled()) {
                batchMessageAndSend();
            }
            pendingMessages.add(op);
            if (op.msg != null) {
                LAST_SEQ_ID_PUSHED_UPDATER.getAndUpdate(this,
                        last -> Math.max(last, getHighestSequenceId(op)));
            }

            final ClientCnx cnx = getCnxIfReady();
            if (cnx != null) {
                if (op.msg != null && op.msg.getSchemaState() == None) {
                    tryRegisterSchema(cnx, op.msg, op.callback, this.connectionHandler.getEpoch());
                    return;
                }
                // If we do have a connection, the message is sent immediately, otherwise we'll try again once a new
                // connection is established
                op.cmd.retain();
                cnx.ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx, op));
                stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", topic, producerName,
                        op.sequenceId);
                }
            }
        } catch (Throwable t) {
            releaseSemaphoreForSendOp(op);
            log.warn("[{}] [{}] error while closing out batch -- {}", topic, producerName, t);
            op.sendComplete(new PulsarClientException(t, op.sequenceId));
        }
    }

    // Must acquire a lock on ProducerImpl.this before calling method.
    private void recoverProcessOpSendMsgFrom(ClientCnx cnx, MessageImpl from, long expectedEpoch) {
        if (expectedEpoch != this.connectionHandler.getEpoch() || cnx() == null) {
            // In this case, the cnx passed to this method is no longer the active connection. This method will get
            // called again once the new connection registers the producer with the broker.
            log.info("[{}][{}] Producer epoch mismatch or the current connection is null. Skip re-sending the "
                    + " {} pending messages since they will deliver using another connection.", topic, producerName,
                    pendingMessages.size());
            return;
        }
        final boolean stripChecksum = cnx.getRemoteEndpointProtocolVersion() < brokerChecksumSupportedVersion();
        Iterator<OpSendMsg> msgIterator = pendingMessages.iterator();
        OpSendMsg pendingRegisteringOp = null;
        while (msgIterator.hasNext()) {
            OpSendMsg op = msgIterator.next();
            if (from != null) {
                if (op.msg == from) {
                    from = null;
                } else {
                    continue;
                }
            }
            if (op.msg != null) {
                if (op.msg.getSchemaState() == None) {
                    if (!rePopulateMessageSchema(op.msg)) {
                        pendingRegisteringOp = op;
                        break;
                    }
                } else if (op.msg.getSchemaState() == Broken) {
                    op.recycle();
                    msgIterator.remove();
                    continue;
                }
            }
            if (op.cmd == null) {
                checkState(op.rePopulate != null);
                op.rePopulate.run();
            }
            if (stripChecksum) {
                stripChecksum(op);
            }
            op.cmd.retain();
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Re-Sending message in cnx {}, sequenceId {}", topic, producerName,
                          cnx.channel(), op.sequenceId);
            }
            cnx.ctx().write(op.cmd, cnx.ctx().voidPromise());
            op.updateSentTimestamp();
            stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
        }
        cnx.ctx().flush();
        if (!changeToReadyState()) {
            // Producer was closed while reconnecting, close the connection to make sure the broker
            // drops the producer on its side
            cnx.channel().close();
            return;
        }
        if (pendingRegisteringOp != null) {
            tryRegisterSchema(cnx, pendingRegisteringOp.msg, pendingRegisteringOp.callback, expectedEpoch);
        }
    }

    public long getDelayInMillis() {
        OpSendMsg firstMsg = pendingMessages.peek();
        if (firstMsg != null) {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstMsg.createdAt);
        }
        return 0L;
    }

    public String getConnectionId() {
        return cnx() != null ? connectionId : null;
    }

    public String getConnectedSince() {
        return cnx() != null ? connectedSince : null;
    }

    public int getPendingQueueSize() {
        if (!isBatchMessagingEnabled()) {
            return pendingMessages.size();
        }
        MutableInt size = new MutableInt(0);
        pendingMessages.forEach(op -> {
            size.add(Math.max(op.numMessagesInBatch, 1));
        });
        return size.getValue();
    }

    @Override
    public ProducerStatsRecorder getStats() {
        return stats;
    }

    @Override
    public String getProducerName() {
        return producerName;
    }

    // wrapper for connection methods
    ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    void resetBackoff() {
        this.connectionHandler.resetBackoff();
    }

    void connectionClosed(ClientCnx cnx) {
        this.connectionHandler.connectionClosed(cnx);
    }

    public ClientCnx getClientCnx() {
        return this.connectionHandler.cnx();
    }

    void setClientCnx(ClientCnx clientCnx) {
        this.connectionHandler.setClientCnx(clientCnx);
    }

    void reconnectLater(Throwable exception) {
        this.connectionHandler.reconnectLater(exception);
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }

    @VisibleForTesting
    Optional<Semaphore> getSemaphore() {
        return semaphore;
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);
}
