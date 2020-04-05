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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerImpl<T> extends ProducerBase<T> implements TimerTask, ConnectionHandler.Connection {

    // Producer id, used to identify a producer within a single connection
    protected final long producerId;

    // Variable is used through the atomic updater
    private volatile long msgIdGenerator;

    private final BlockingQueue<OpSendMsg> pendingMessages;
    private final BlockingQueue<OpSendMsg> pendingCallbacks;
    private final Semaphore semaphore;
    private volatile Timeout sendTimeout = null;
    private volatile Timeout batchMessageAndSendTimeout = null;
    private long createProducerTimeout;
    private final BatchMessageContainerBase batchMessageContainer;
    private CompletableFuture<MessageId> lastSendFuture = CompletableFuture.completedFuture(null);

    // Globally unique producer name
    private String producerName;
    private boolean userProvidedProducerName = false;

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

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ProducerImpl> msgIdGeneratorUpdater = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "msgIdGenerator");

    public ProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
                        CompletableFuture<Producer<T>> producerCreatedFuture, int partitionIndex, Schema<T> schema,
                        ProducerInterceptors interceptors) {
        super(client, topic, conf, producerCreatedFuture, schema, interceptors);
        this.producerId = client.newProducerId();
        this.producerName = conf.getProducerName();
        if (StringUtils.isNotBlank(producerName)) {
            this.userProvidedProducerName = true;
        }
        this.partitionIndex = partitionIndex;
        this.pendingMessages = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
        this.pendingCallbacks = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
        this.semaphore = new Semaphore(conf.getMaxPendingMessages(), true);

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
                    msgCryptoBc = new MessageCryptoBc(logCtx, true);;
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
            keyGeneratorTask = client.eventLoopGroup().scheduleWithFixedDelay(() -> {
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
            }, 0L, 4L, TimeUnit.HOURS);
        }

        if (conf.getSendTimeoutMs() > 0) {
            sendTimeout = client.timer().newTimeout(this, conf.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        this.createProducerTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
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

    public void sendAsync(Message<?> message, SendCallback callback) {
        checkArgument(message instanceof MessageImpl);

        if (!isValidProducerState(callback)) {
            return;
        }

        if (!canEnqueueRequest(callback)) {
            return;
        }

        MessageImpl<?> msg = (MessageImpl) message;
        MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // If compression is enabled, we are compressing, otherwise it will simply use the same buffer
        int uncompressedSize = payload.readableBytes();
        ByteBuf compressedPayload = payload;
        // Batch will be compressed when closed
        // If a message has a delayed delivery time, we'll always send it individually
        if (!isBatchMessagingEnabled() || msgMetadataBuilder.hasDeliverAtTime()) {
            compressedPayload = compressor.encode(payload);
            payload.release();

            // validate msg-size (For batching this will be check at the batch completion size)
            int compressedSize = compressedPayload.readableBytes();
            if (compressedSize > ClientCnx.getMaxMessageSize()) {
                compressedPayload.release();
                String compressedStr = (!isBatchMessagingEnabled() && conf.getCompressionType() != CompressionType.NONE)
                                           ? "Compressed"
                                           : "";
                PulsarClientException.InvalidMessageException invalidMessageException = new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s sends a %s message with %d bytes that exceeds %d bytes",
                        producerName, topic, compressedStr, compressedSize, ClientCnx.getMaxMessageSize()));
                completeCallbackAndReleaseSemaphore(callback, invalidMessageException);
                return;
            }
        }

        if (!msg.isReplicated() && msgMetadataBuilder.hasProducerName()) {
            PulsarClientException.InvalidMessageException invalidMessageException =
                new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s can not reuse the same message", producerName, topic));
            completeCallbackAndReleaseSemaphore(callback, invalidMessageException);
            compressedPayload.release();
            return;
        }

        if (!populateMessageSchema(msg, callback)) {
            compressedPayload.release();
            return;
        }

        try {
            synchronized (this) {
                long sequenceId;
                if (!msgMetadataBuilder.hasSequenceId()) {
                    sequenceId = msgIdGeneratorUpdater.getAndIncrement(this);
                    msgMetadataBuilder.setSequenceId(sequenceId);
                } else {
                    sequenceId = msgMetadataBuilder.getSequenceId();
                }
                if (!msgMetadataBuilder.hasPublishTime()) {
                    msgMetadataBuilder.setPublishTime(client.getClientClock().millis());

                    checkArgument(!msgMetadataBuilder.hasProducerName());

                    msgMetadataBuilder.setProducerName(producerName);

                    if (conf.getCompressionType() != CompressionType.NONE) {
                        msgMetadataBuilder.setCompression(
                                CompressionCodecProvider.convertToWireProtocol(conf.getCompressionType()));
                    }
                    msgMetadataBuilder.setUncompressedSize(uncompressedSize);
                }
                if (canAddToBatch(msg)) {
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
                    ByteBuf encryptedPayload = encryptMessage(msgMetadataBuilder, compressedPayload);
                    // When publishing during replication, we need to set the correct number of message in batch
                    // This is only used in tracking the publish rate stats
                    int numMessages = msg.getMessageBuilder().hasNumMessagesInBatch()
                            ? msg.getMessageBuilder().getNumMessagesInBatch()
                            : 1;
                    final OpSendMsg op;
                    if (msg.getSchemaState() == MessageImpl.SchemaState.Ready) {
                        MessageMetadata msgMetadata = msgMetadataBuilder.build();
                        ByteBufPair cmd = sendMessage(producerId, sequenceId, numMessages, msgMetadata, encryptedPayload);
                        op = OpSendMsg.create(msg, cmd, sequenceId, callback);
                        msgMetadataBuilder.recycle();
                        msgMetadata.recycle();
                    } else {
                        op = OpSendMsg.create(msg, null, sequenceId, callback);
                        op.rePopulate = () -> {
                            MessageMetadata msgMetadata = msgMetadataBuilder.build();
                            op.cmd = sendMessage(producerId, sequenceId, numMessages, msgMetadata, encryptedPayload);
                            msgMetadataBuilder.recycle();
                            msgMetadata.recycle();
                        };
                    }
                    op.setNumMessagesInBatch(numMessages);
                    op.setBatchSizeByte(encryptedPayload.readableBytes());
                    lastSendFuture = callback.getFuture();
                    processOpSendMsg(op);
                }
            }
        } catch (PulsarClientException e) {
            completeCallbackAndReleaseSemaphore(callback, e);
        } catch (Throwable t) {
            completeCallbackAndReleaseSemaphore(callback, new PulsarClientException(t));
        }
    }

    private boolean populateMessageSchema(MessageImpl msg, SendCallback callback) {
        MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        if (msg.getSchema() == schema) {
            schemaVersion.ifPresent(v -> msgMetadataBuilder.setSchemaVersion(ByteString.copyFrom(v)));
            msg.setSchemaState(MessageImpl.SchemaState.Ready);
            return true;
        }
        if (!isMultiSchemaEnabled(true)) {
            PulsarClientException.InvalidMessageException e = new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s is disabled the `MultiSchema`", producerName, topic));
            completeCallbackAndReleaseSemaphore(callback, e);
            return false;
        }
        SchemaHash schemaHash = SchemaHash.of(msg.getSchema());
        byte[] schemaVersion = schemaCache.get(schemaHash);
        if (schemaVersion != null) {
            msgMetadataBuilder.setSchemaVersion(ByteString.copyFrom(schemaVersion));
            msg.setSchemaState(MessageImpl.SchemaState.Ready);
        }
        return true;
    }

    private boolean rePopulateMessageSchema(MessageImpl msg) {
        SchemaHash schemaHash = SchemaHash.of(msg.getSchema());
        byte[] schemaVersion = schemaCache.get(schemaHash);
        if (schemaVersion == null) {
            return false;
        }
        msg.getMessageBuilder().setSchemaVersion(ByteString.copyFrom(schemaVersion));
        msg.setSchemaState(MessageImpl.SchemaState.Ready);
        return true;
    }

    private void tryRegisterSchema(ClientCnx cnx, MessageImpl msg, SendCallback callback) {
        if (!changeToRegisteringSchemaState()) {
            return;
        }
        SchemaInfo schemaInfo = Optional.ofNullable(msg.getSchema())
                                        .map(Schema::getSchemaInfo)
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
                log.warn("[{}] [{}] GetOrCreateSchema succeed", topic, producerName);
                SchemaHash schemaHash = SchemaHash.of(msg.getSchema());
                schemaCache.putIfAbsent(schemaHash, v);
                msg.getMessageBuilder().setSchemaVersion(ByteString.copyFrom(v));
                msg.setSchemaState(MessageImpl.SchemaState.Ready);
            }
            cnx.ctx().channel().eventLoop().execute(() -> {
                synchronized (ProducerImpl.this) {
                    recoverProcessOpSendMsgFrom(cnx, msg);
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

    protected ByteBuf encryptMessage(MessageMetadata.Builder msgMetadata, ByteBuf compressedPayload)
            throws PulsarClientException {

        ByteBuf encryptedPayload = compressedPayload;
        if (!conf.isEncryptionEnabled() || msgCrypto == null) {
            return encryptedPayload;
        }
        try {
            encryptedPayload = msgCrypto.encrypt(conf.getEncryptionKeys(), conf.getCryptoKeyReader(), () -> msgMetadata,
                    compressedPayload);
        } catch (PulsarClientException e) {
            // Unless config is set to explicitly publish un-encrypted message upon failure, fail the request
            if (conf.getCryptoFailureAction() == ProducerCryptoFailureAction.SEND) {
                log.warn("[{}] [{}] Failed to encrypt message {}. Proceeding with publishing unencrypted message",
                        topic, producerName, e.getMessage());
                return compressedPayload;
            }
            throw e;
        }
        return encryptedPayload;
    }

    protected ByteBufPair sendMessage(long producerId, long sequenceId, int numMessages, MessageMetadata msgMetadata,
            ByteBuf compressedPayload) {
        return Commands.newSend(producerId, sequenceId, numMessages, getChecksumType(), msgMetadata, compressedPayload);
    }

    protected ByteBufPair sendMessage(long producerId, long lowestSequenceId, long highestSequenceId, int numMessages, MessageMetadata msgMetadata,
                                      ByteBuf compressedPayload) {
        return Commands.newSend(producerId, lowestSequenceId, highestSequenceId, numMessages, getChecksumType(), msgMetadata, compressedPayload);
    }

    private ChecksumType getChecksumType() {
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
               && (!isMultiSchemaEnabled(false) || batchMessageContainer.hasSameSchema(msg));
    }

    private void doBatchSendAndAdd(MessageImpl<?> msg, SendCallback callback, ByteBuf payload) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Closing out batch to accommodate large message with size {}", topic, producerName,
                    msg.getDataBuffer().readableBytes());
        }
        try {
            batchMessageAndSend();
            batchMessageContainer.add(msg, callback);
            lastSendFuture = callback.getFuture();
        } finally {
            payload.release();
        }
    }

    private boolean isValidProducerState(SendCallback callback) {
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
            callback.sendComplete(new PulsarClientException.AlreadyClosedException("Producer already closed"));
            return false;
        case Terminated:
            callback.sendComplete(new PulsarClientException.TopicTerminatedException("Topic was terminated"));
            return false;
        case Failed:
        case Uninitialized:
        default:
            callback.sendComplete(new PulsarClientException.NotConnectedException());
            return false;
        }
    }

    private boolean canEnqueueRequest(SendCallback callback) {
        try {
            if (conf.isBlockIfQueueFull()) {
                semaphore.acquire();
            } else {
                if (!semaphore.tryAcquire()) {
                    callback.sendComplete(new PulsarClientException.ProducerQueueIsFullError("Producer send queue is full"));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.sendComplete(new PulsarClientException(e));
            return false;
        }

        return true;
    }

    private static final class WriteInEventLoopCallback implements Runnable {
        private ProducerImpl<?> producer;
        private ByteBufPair cmd;
        private long sequenceId;
        private ClientCnx cnx;

        static WriteInEventLoopCallback create(ProducerImpl<?> producer, ClientCnx cnx, OpSendMsg op) {
            WriteInEventLoopCallback c = RECYCLER.get();
            c.producer = producer;
            c.cnx = cnx;
            c.sequenceId = op.sequenceId;
            c.cmd = op.cmd;
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
            } finally {
                recycle();
            }
        }

        private void recycle() {
            producer = null;
            cnx = null;
            cmd = null;
            sequenceId = -1;
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

        Timeout batchTimeout = batchMessageAndSendTimeout;
        if (batchTimeout != null) {
            batchTimeout.cancel();
            batchMessageAndSendTimeout = null;
        }

        if (keyGeneratorTask != null && !keyGeneratorTask.isCancelled()) {
            keyGeneratorTask.cancel(false);
        }

        stats.cancelStatsTimeout();

        ClientCnx cnx = cnx();
        if (cnx == null || currentState != State.Ready) {
            log.info("[{}] [{}] Closed Producer (not connected)", topic, producerName);
            synchronized (this) {
                setState(State.Closed);
                client.cleanupProducer(this);
                PulsarClientException ex = new PulsarClientException.AlreadyClosedException(
                    format("The producer %s of the topic %s was already closed when closing the producers",
                        producerName, topic));
                pendingMessages.forEach(msg -> {
                    msg.callback.sendComplete(ex);
                    msg.cmd.release();
                    msg.recycle();
                });
                pendingMessages.clear();
            }

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
                synchronized (ProducerImpl.this) {
                    log.info("[{}] [{}] Closed Producer", topic, producerName);
                    setState(State.Closed);
                    pendingMessages.forEach(msg -> {
                        msg.cmd.release();
                        msg.recycle();
                    });
                    pendingMessages.clear();
                }

                closeFuture.complete(null);
                client.cleanupProducer(this);
            } else {
                closeFuture.completeExceptionally(exception);
            }

            return null;
        });

        return closeFuture;
    }

    @Override
    public boolean isConnected() {
        return connectionHandler.cnx() != null && (getState() == State.Ready);
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

            failPendingMessages(cnx,
                new PulsarClientException.TopicTerminatedException(
                    format("The topic %s that the producer %s produces to has been terminated", topic, producerName)));
        }
    }

    void ackReceived(ClientCnx cnx, long sequenceId, long highestSequenceId, long ledgerId, long entryId) {
        OpSendMsg op = null;
        boolean callback = false;
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
            } else if (sequenceId < op.sequenceId) {
                // Ignoring the ack since it's referring to a message that has already timed out.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg. expecting: {} - {} - got: {} - {}", topic, producerName,
                            op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId);
                }
            } else {
                // Add check `sequenceId >= highestSequenceId` for backward compatibility.
                if (sequenceId >= highestSequenceId || highestSequenceId == op.highestSequenceId) {
                    // Message was persisted correctly
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Received ack for msg {} ", topic, producerName, sequenceId);
                    }
                    pendingMessages.remove();
                    releaseSemaphoreForSendOp(op);
                    callback = true;
                    pendingCallbacks.add(op);
                } else {
                    log.warn("[{}] [{}] Got ack for batch msg error. expecting: {} - {} - got: {} - {} - queue-size: {}", topic, producerName,
                            op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId, pendingMessages.size());
                    // Force connection closing so that messages can be re-transmitted in a new connection
                    cnx.channel().close();
                }
            }
        }
        if (callback) {
            op = pendingCallbacks.poll();
            if (op != null) {
                OpSendMsg finalOp = op;
                LAST_SEQ_ID_PUBLISHED_UPDATER.getAndUpdate(this,
                        last -> Math.max(last, getHighestSequenceId(finalOp)));
                op.setMessageId(ledgerId, entryId, partitionIndex);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    op.callback.sendComplete(null);
                } catch (Throwable t) {
                    log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                            sequenceId, t);
                }
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            }
        }
    }

    private long getHighestSequenceId(OpSendMsg op) {
        return Math.max(op.highestSequenceId, op.sequenceId);
    }

    private void releaseSemaphoreForSendOp(OpSendMsg op) {
        semaphore.release(isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);
    }

    private void completeCallbackAndReleaseSemaphore(SendCallback callback, Exception exception) {
        semaphore.release();
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
                        op.callback.sendComplete(
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
        resendMessages(cnx);
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
            log.warn("[{}] Failed while casting {} into ByteBufPair", producerName,
                    (op.cmd == null ? null : op.cmd.getClass().getName()));
            return false;
        }
    }

    protected static final class OpSendMsg {
        MessageImpl<?> msg;
        List<MessageImpl<?>> msgs;
        ByteBufPair cmd;
        SendCallback callback;
        Runnable rePopulate;
        long sequenceId;
        long createdAt;
        long batchSizeByte = 0;
        int numMessagesInBatch = 1;
        long highestSequenceId;

        static OpSendMsg create(MessageImpl<?> msg, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msg = msg;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.nanoTime();
            return op;
        }

        static OpSendMsg create(List<MessageImpl<?>> msgs, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msgs = msgs;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.nanoTime();
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
            return op;
        }

        void recycle() {
            msg = null;
            msgs = null;
            cmd = null;
            callback = null;
            rePopulate = null;
            sequenceId = -1L;
            createdAt = -1L;
            highestSequenceId = -1L;
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

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        // we set the cnx reference before registering the producer on the cnx, so if the cnx breaks before creating the
        // producer, it will try to grab a new cnx
        connectionHandler.setClientCnx(cnx);
        cnx.registerProducer(producerId, this);

        log.info("[{}] [{}] Creating producer on cnx {}", topic, producerName, cnx.ctx().channel());

        long requestId = client.newRequestId();

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
                       schemaInfo, connectionHandler.epoch, userProvidedProducerName),
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
                            client.timer().newTimeout(batchMessageAndSendTask, conf.getBatchingMaxPublishDelayMicros(),
                                    TimeUnit.MICROSECONDS);
                        }
                        resendMessages(cnx);
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
                    log.error("[{}] [{}] Failed to create producer: {}", topic, producerName, cause.getMessage());

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
                        failPendingMessages(cnx(), (PulsarClientException) cause);
                        producerCreatedFuture.completeExceptionally(cause);
                        client.cleanupProducer(this);
                    } else if (producerCreatedFuture.isDone() || //
                    (cause instanceof PulsarClientException && PulsarClientException.isRetriableError(cause)
                            && System.currentTimeMillis() < createProducerTimeout)) {
                        // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                        // still within the initial timeout budget and we are dealing with a retriable error
                        reconnectLater(cause);
                    } else {
                        setState(State.Failed);
                        producerCreatedFuture.completeExceptionally(cause);
                        client.cleanupProducer(this);
                    }

                    return null;
                });
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        boolean producerTimeout = System.currentTimeMillis() > createProducerTimeout;
        if ((nonRetriableError || producerTimeout) && producerCreatedFuture.completeExceptionally(exception)) {
            if (nonRetriableError) {
                log.info("[{}] Producer creation failed for producer {} with unretriableError = {}", topic, producerId, exception);
            } else {
                log.info("[{}] Producer creation failed for producer {} after producerTimeout", topic, producerId);
            }
            setState(State.Failed);
            client.cleanupProducer(this);
        }
    }

    private void resendMessages(ClientCnx cnx) {
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
                recoverProcessOpSendMsgFrom(cnx, null);
            }
        });
    }

    /**
     * Strips checksum from {@link OpSendMsg} command if present else ignore it.
     *
     * @param op
     */
    private void stripChecksum(OpSendMsg op) {
        int totalMsgBufSize = op.cmd.readableBytes();
        ByteBufPair msg = op.cmd;
        if (msg != null) {
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
            log.warn("[{}] Failed while casting {} into ByteBufPair", producerName,
                    (op.cmd == null ? null : op.cmd.getClass().getName()));
        }
    }

    public int brokerChecksumSupportedVersion() {
        return ProtocolVersion.v6.getNumber();
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
                            producerName, topic));
                    failPendingMessages(cnx(), te);
                    stats.incrementSendFailed(pendingMessages.size());
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
                    op.callback.sendComplete(ex);
                } catch (Throwable t) {
                    log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                            op.sequenceId, t);
                }
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            });

            pendingMessages.clear();
            pendingCallbacks.clear();
            semaphore.release(releaseCount.get());
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
        semaphore.release(numMessagesInBatch);
    }

    TimerTask batchMessageAndSendTask = new TimerTask() {

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            if (log.isTraceEnabled()) {
                log.trace("[{}] [{}] Batching the messages from the batch container from timer thread", topic,
                        producerName);
            }
            // semaphore acquired when message was enqueued to container
            synchronized (ProducerImpl.this) {
                // If it's closing/closed we need to ignore the send batch timer and not schedule next timeout.
                if (getState() == State.Closing || getState() == State.Closed) {
                    return;
                }

                batchMessageAndSend();
                // schedule the next batch message task
                batchMessageAndSendTimeout = client.timer()
                    .newTimeout(this, conf.getBatchingMaxPublishDelayMicros(), TimeUnit.MICROSECONDS);
            }
        }
    };

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
                Thread.currentThread().interrupt();
                semaphore.release(batchMessageContainer.getNumMessagesInBatch());
            } catch (Throwable t) {
                semaphore.release(batchMessageContainer.getNumMessagesInBatch());
                log.warn("[{}] [{}] error while create opSendMsg by batch message container", topic, producerName, t);
            }
        }
    }

    private void processOpSendMsg(OpSendMsg op) {
        if (op == null) {
            return;
        }
        try {
            if (op.msg != null && isBatchMessagingEnabled()) {
                batchMessageAndSend();
            }
            pendingMessages.put(op);
            if (op.msg != null) {
                LAST_SEQ_ID_PUSHED_UPDATER.getAndUpdate(this,
                        last -> Math.max(last, getHighestSequenceId(op)));
            }
            ClientCnx cnx = cnx();
            if (isConnected()) {
                if (op.msg != null && op.msg.getSchemaState() == None) {
                    tryRegisterSchema(cnx, op.msg, op.callback);
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
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            releaseSemaphoreForSendOp(op);
            if (op != null) {
                op.callback.sendComplete(new PulsarClientException(ie));
            }
        } catch (Throwable t) {
            releaseSemaphoreForSendOp(op);
            log.warn("[{}] [{}] error while closing out batch -- {}", topic, producerName, t);
            if (op != null) {
                op.callback.sendComplete(new PulsarClientException(t));
            }
        }
    }

    private void recoverProcessOpSendMsgFrom(ClientCnx cnx, MessageImpl from) {
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
            tryRegisterSchema(cnx, pendingRegisteringOp.msg, pendingRegisteringOp.callback);
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
        return pendingMessages.size();
    }

    @Override
    public ProducerStatsRecorder getStats() {
        return stats;
    }

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

    ClientCnx getClientCnx() {
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
    Semaphore getSemaphore() {
        return semaphore;
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);
}
