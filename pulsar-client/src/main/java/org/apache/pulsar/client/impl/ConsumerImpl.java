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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static java.lang.String.format;
import static org.apache.pulsar.common.api.Commands.hasChecksum;
import static org.apache.pulsar.common.api.Commands.readChecksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import static java.util.Base64.getEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.ValidationError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.common.api.proto.PulsarApi.EncryptionKeys;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerImpl<T> extends ConsumerBase<T> implements ConnectionHandler.Connection {
    private static final int MAX_REDELIVER_UNACKNOWLEDGED = 1000;

    final long consumerId;

    // Number of messages that have delivered to the application. Every once in a while, this number will be sent to the
    // broker to notify that we are ready to get (and store in the incoming messages queue) more messages
    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<ConsumerImpl> AVAILABLE_PERMITS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(ConsumerImpl.class, "availablePermits");
    @SuppressWarnings("unused")
    private volatile int availablePermits = 0;

    private MessageId lastDequeuedMessage = MessageId.earliest;
    private MessageId lastMessageIdInBroker = MessageId.earliest;

    private long subscribeTimeout;
    private final int partitionIndex;

    private final int receiverQueueRefillThreshold;

    private volatile boolean waitingOnReceiveForZeroQueueSize = false;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ReadWriteLock zeroQueueLock;

    private final UnAckedMessageTracker unAckedMessageTracker;
    private final AcknowledgmentsGroupingTracker acknowledgmentsGroupingTracker;

    protected final ConsumerStatsRecorder stats;
    private final int priorityLevel;
    private final SubscriptionMode subscriptionMode;
    private BatchMessageIdImpl startMessageId;

    private volatile boolean hasReachedEndOfTopic;

    private MessageCrypto msgCrypto = null;

    private final Map<String, String> metadata;

    private final boolean readCompacted;

    private final SubscriptionInitialPosition subscriptionInitialPosition;
    private final ConnectionHandler connectionHandler;

    enum SubscriptionMode {
        // Make the subscription to be backed by a durable cursor that will retain messages and persist the current
        // position
        Durable,

        // Lightweight subscription mode that doesn't have a durable cursor associated
        NonDurable
    }

    ConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
            ExecutorService listenerExecutor, int partitionIndex, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema) {
        this(client, topic, conf, listenerExecutor, partitionIndex, subscribeFuture, SubscriptionMode.Durable, null, schema);
    }

    ConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                 ExecutorService listenerExecutor, int partitionIndex, CompletableFuture<Consumer<T>> subscribeFuture,
                 SubscriptionMode subscriptionMode, MessageId startMessageId, Schema<T> schema) {
        super(client, topic, conf, conf.getReceiverQueueSize(), listenerExecutor, subscribeFuture, schema);
        this.consumerId = client.newConsumerId();
        this.subscriptionMode = subscriptionMode;
        this.startMessageId = startMessageId != null ? new BatchMessageIdImpl((MessageIdImpl) startMessageId) : null;
        AVAILABLE_PERMITS_UPDATER.set(this, 0);
        this.subscribeTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
        this.partitionIndex = partitionIndex;
        this.receiverQueueRefillThreshold = conf.getReceiverQueueSize() / 2;
        this.priorityLevel = conf.getPriorityLevel();
        this.readCompacted = conf.isReadCompacted();
        this.subscriptionInitialPosition = conf.getSubscriptionInitialPosition();

        TopicName topicName = TopicName.get(topic);
        if (topicName.isPersistent()) {
            this.acknowledgmentsGroupingTracker =
                new PersistentAcknowledgmentsGroupingTracker(this, conf, client.eventLoopGroup());
        } else {
            this.acknowledgmentsGroupingTracker =
                NonPersistentAcknowledgmentGroupingTracker.of();
        }

        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ConsumerStatsRecorderImpl(client, conf, this);
        } else {
            stats = ConsumerStatsDisabled.INSTANCE;
        }

        if (conf.getReceiverQueueSize() <= 1) {
            zeroQueueLock = new ReentrantReadWriteLock();
        } else {
            zeroQueueLock = null;
        }

        if (conf.getAckTimeoutMillis() != 0) {
            this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf.getAckTimeoutMillis());
        } else {
            this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
        }

        // Create msgCrypto if not created already
        if (conf.getCryptoKeyReader() != null) {
            String logCtx = "[" + topic + "] [" + subscription + "]";
            this.msgCrypto = new MessageCrypto(logCtx, false);
        }

        if (conf.getProperties().isEmpty()) {
            metadata = Collections.emptyMap();
        } else {
            metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
        }

        this.connectionHandler = new ConnectionHandler(this,
            new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 0, TimeUnit.MILLISECONDS),
            this);

        grabCnx();
    }

    public ConnectionHandler getConnectionHandler() {
        return connectionHandler;
    }

    public UnAckedMessageTracker getUnAckedMessageTracker() {
        return unAckedMessageTracker;
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
        }
        final CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        if (isConnected()) {
            setState(State.Closing);
            long requestId = client.newRequestId();
            ByteBuf unsubscribe = Commands.newUnsubscribe(consumerId, requestId);
            ClientCnx cnx = cnx();
            cnx.sendRequestWithId(unsubscribe, requestId).thenRun(() -> {
                cnx.removeConsumer(consumerId);
                unAckedMessageTracker.close();
                client.cleanupConsumer(ConsumerImpl.this);
                log.info("[{}][{}] Successfully unsubscribed from topic", topic, subscription);
                unsubscribeFuture.complete(null);
                setState(State.Closed);
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed to unsubscribe: {}", topic, subscription, e.getCause().getMessage());
                unsubscribeFuture.completeExceptionally(e.getCause());
                setState(State.Ready);
                return null;
            });
        } else {
            unsubscribeFuture.completeExceptionally(new PulsarClientException("Not connected to broker"));
        }
        return unsubscribeFuture;
    }

    @Override
    protected Message<T> internalReceive() throws PulsarClientException {
        if (conf.getReceiverQueueSize() == 0) {
            checkArgument(zeroQueueLock != null, "Receiver queue size can't be modified");
            zeroQueueLock.writeLock().lock();
            try {
                return fetchSingleMessageFromBroker();
            } finally {
                zeroQueueLock.writeLock().unlock();
            }
        }
        Message<T> message;
        try {
            message = incomingMessages.take();
            messageProcessed(message);
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stats.incrementNumReceiveFailed();
            throw new PulsarClientException(e);
        }
    }

    @Override
    protected CompletableFuture<Message<T>> internalReceiveAsync() {

        CompletableFuture<Message<T>> result = new CompletableFuture<>();
        Message<T> message = null;
        try {
            lock.writeLock().lock();
            message = incomingMessages.poll(0, TimeUnit.MILLISECONDS);
            if (message == null) {
                pendingReceives.add(result);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(e);
        } finally {
            lock.writeLock().unlock();
        }

        if (message == null && conf.getReceiverQueueSize() == 0) {
            sendFlowPermitsToBroker(cnx(), 1);
        } else if (message != null) {
            messageProcessed(message);
            result.complete(message);
        }

        return result;
    }

    private Message<T> fetchSingleMessageFromBroker() throws PulsarClientException {
        checkArgument(conf.getReceiverQueueSize() == 0);

        // Just being cautious
        if (incomingMessages.size() > 0) {
            log.error("The incoming message queue should never be greater than 0 when Queue size is 0");
            incomingMessages.clear();
        }

        Message<T> message;
        try {
            // is cnx is null or if the connection breaks the connectionOpened function will send the flow again
            waitingOnReceiveForZeroQueueSize = true;
            synchronized (this) {
                if (isConnected()) {
                    sendFlowPermitsToBroker(cnx(), 1);
                }
            }
            do {
                message = incomingMessages.take();
                lastDequeuedMessage = message.getMessageId();
                ClientCnx msgCnx = ((MessageImpl<?>) message).getCnx();
                // synchronized need to prevent race between connectionOpened and the check "msgCnx == cnx()"
                synchronized (ConsumerImpl.this) {
                    // if message received due to an old flow - discard it and wait for the message from the
                    // latest flow command
                    if (msgCnx == cnx()) {
                        waitingOnReceiveForZeroQueueSize = false;
                        break;
                    }
                }
            } while (true);

            stats.updateNumMsgsReceived(message);
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stats.incrementNumReceiveFailed();
            throw new PulsarClientException(e);
        } finally {
            // Finally blocked is invoked in case the block on incomingMessages is interrupted
            waitingOnReceiveForZeroQueueSize = false;
            // Clearing the queue in case there was a race with messageReceived
            incomingMessages.clear();
        }
    }

    @Override
    protected Message<T> internalReceive(int timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> message;
        try {
            message = incomingMessages.poll(timeout, unit);
            if (message != null) {
                messageProcessed(message);
            }
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            State state = getState();
            if (state != State.Closing && state != State.Closed) {
                stats.incrementNumReceiveFailed();
                throw new PulsarClientException(e);
            } else {
                return null;
            }
        }
    }

    boolean markAckForBatchMessage(BatchMessageIdImpl batchMessageId, AckType ackType,
                                   Map<String,Long> properties) {
        boolean isAllMsgsAcked;
        if (ackType == AckType.Individual) {
            isAllMsgsAcked = batchMessageId.ackIndividual();
        } else {
            isAllMsgsAcked = batchMessageId.ackCumulative();
        }
        int outstandingAcks = 0;
        if (log.isDebugEnabled()) {
            outstandingAcks = batchMessageId.getOutstandingAcksInSameBatch();
        }

        int batchSize = batchMessageId.getBatchSize();
        // all messages in this batch have been acked
        if (isAllMsgsAcked) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] can ack message to broker {}, acktype {}, cardinality {}, length {}", subscription,
                        consumerName, batchMessageId, ackType, outstandingAcks, batchSize);
            }
            // increment Acknowledge-msg counter with number of messages in batch only if AckType is Individual.
            // CumulativeAckType is handled while sending ack to broker
            if (ackType == AckType.Individual) {
                stats.incrementNumAcksSent(batchSize);
            }
            return true;
        } else {
            if (AckType.Cumulative == ackType
                && !batchMessageId.getAcker().isPrevBatchCumulativelyAcked()) {
                sendAcknowledge(batchMessageId.prevBatchMessageId(), AckType.Cumulative, properties);
                batchMessageId.getAcker().setPrevBatchCumulativelyAcked(true);
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] cannot ack message to broker {}, acktype {}, pending acks - {}", subscription,
                        consumerName, batchMessageId, ackType, outstandingAcks);
            }
        }
        return false;
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String,Long> properties) {
        checkArgument(messageId instanceof MessageIdImpl);
        if (getState() != State.Ready && getState() != State.Connecting) {
            stats.incrementNumAcksFailed();
            return FutureUtil.failedFuture(new PulsarClientException("Consumer not ready. State: " + getState()));
        }

        if (messageId instanceof BatchMessageIdImpl) {
            if (markAckForBatchMessage((BatchMessageIdImpl) messageId, ackType, properties)) {
                // all messages in batch have been acked so broker can be acked via sendAcknowledge()
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] acknowledging message - {}, acktype {}", subscription, consumerName, messageId,
                            ackType);
                }
            } else {
                // other messages in batch are still pending ack.
                return CompletableFuture.completedFuture(null);
            }
        }
        return sendAcknowledge(messageId, ackType, properties);
    }

    private CompletableFuture<Void> sendAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String,Long> properties) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;


        if (ackType == AckType.Individual) {
            unAckedMessageTracker.remove(msgId);
            // increment counter by 1 for non-batch msg
            if (!(messageId instanceof BatchMessageIdImpl)) {
                stats.incrementNumAcksSent(1);
            }
        } else if (ackType == AckType.Cumulative) {
            stats.incrementNumAcksSent(unAckedMessageTracker.removeMessagesTill(msgId));
        }

        acknowledgmentsGroupingTracker.addAcknowledgment(msgId, ackType, properties);

        // Consumer acknowledgment operation immediately succeeds. In any case, if we're not able to send ack to broker,
        // the messages will be re-delivered
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        setClientCnx(cnx);
        cnx.registerConsumer(consumerId, this);

        log.info("[{}][{}] Subscribing to topic on cnx {}", topic, subscription, cnx.ctx().channel());

        long requestId = client.newRequestId();

        int currentSize;
        synchronized (this) {
            currentSize = incomingMessages.size();
            startMessageId = clearReceiverQueue();
            unAckedMessageTracker.clear();
        }

        boolean isDurable = subscriptionMode == SubscriptionMode.Durable;
        MessageIdData startMessageIdData;
        if (isDurable) {
            // For regular durable subscriptions, the message id from where to restart will be determined by the broker.
            startMessageIdData = null;
        } else {
            // For non-durable we are going to restart from the next entry
            MessageIdData.Builder builder = MessageIdData.newBuilder();
            builder.setLedgerId(startMessageId.getLedgerId());
            builder.setEntryId(startMessageId.getEntryId());
            if (startMessageId instanceof BatchMessageIdImpl) {
                builder.setBatchIndex(((BatchMessageIdImpl) startMessageId).getBatchIndex());
            }

            startMessageIdData = builder.build();
            builder.recycle();
        }

        ByteBuf request = Commands.newSubscribe(topic, subscription, consumerId, requestId, getSubType(), priorityLevel,
                consumerName, isDurable, startMessageIdData, metadata, readCompacted, InitialPosition.valueOf(subscriptionInitialPosition.getValue()), schema.getSchemaInfo());
        if (startMessageIdData != null) {
            startMessageIdData.recycle();
        }

        cnx.sendRequestWithId(request, requestId).thenRun(() -> {
            synchronized (ConsumerImpl.this) {
                if (changeToReadyState()) {
                    log.info("[{}][{}] Subscribed to topic on {} -- consumer: {}", topic, subscription,
                            cnx.channel().remoteAddress(), consumerId);

                    AVAILABLE_PERMITS_UPDATER.set(this, 0);
                    // For zerosize queue : If the connection is reset and someone is waiting for the messages
                    // or queue was not empty: send a flow command
                    if (waitingOnReceiveForZeroQueueSize
                            || (conf.getReceiverQueueSize() == 0 && (currentSize > 0 || listener != null))) {
                        sendFlowPermitsToBroker(cnx, 1);
                    }
                } else {
                    // Consumer was closed while reconnecting, close the connection to make sure the broker
                    // drops the consumer on its side
                    setState(State.Closed);
                    cnx.removeConsumer(consumerId);
                    cnx.channel().close();
                    return;
                }
            }

            resetBackoff();

            boolean firstTimeConnect = subscribeFuture.complete(this);
            // if the consumer is not partitioned or is re-connected and is partitioned, we send the flow
            // command to receive messages
            if (!(firstTimeConnect && partitionIndex > -1) && conf.getReceiverQueueSize() != 0) {
                sendFlowPermitsToBroker(cnx, conf.getReceiverQueueSize());
            }
        }).exceptionally((e) -> {
            cnx.removeConsumer(consumerId);
            if (getState() == State.Closing || getState() == State.Closed) {
                // Consumer was closed while reconnecting, close the connection to make sure the broker
                // drops the consumer on its side
                cnx.channel().close();
                return null;
            }
            log.warn("[{}][{}] Failed to subscribe to topic on {}", topic, subscription, cnx.channel().remoteAddress());
            if (e.getCause() instanceof PulsarClientException && getConnectionHandler().isRetriableError((PulsarClientException) e.getCause())
                    && System.currentTimeMillis() < subscribeTimeout) {
                reconnectLater(e.getCause());
                return null;
            }

            if (!subscribeFuture.isDone()) {
                // unable to create new consumer, fail operation
                setState(State.Failed);
                subscribeFuture.completeExceptionally(e);
                client.cleanupConsumer(this);
            } else {
                // consumer was subscribed and connected but we got some error, keep trying
                reconnectLater(e.getCause());
            }
            return null;
        });
    }

    /**
     * Clear the internal receiver queue and returns the message id of what was the 1st message in the queue that was
     * not seen by the application
     */
    private BatchMessageIdImpl clearReceiverQueue() {
        List<Message<?>> currentMessageQueue = new ArrayList<>(incomingMessages.size());
        incomingMessages.drainTo(currentMessageQueue);
        if (!currentMessageQueue.isEmpty()) {
            MessageIdImpl nextMessageInQueue = (MessageIdImpl) currentMessageQueue.get(0).getMessageId();
            BatchMessageIdImpl previousMessage;
            if (nextMessageInQueue instanceof BatchMessageIdImpl) {
                // Get on the previous message within the current batch
                previousMessage = new BatchMessageIdImpl(nextMessageInQueue.getLedgerId(),
                        nextMessageInQueue.getEntryId(), nextMessageInQueue.getPartitionIndex(),
                        ((BatchMessageIdImpl) nextMessageInQueue).getBatchIndex() - 1);
            } else {
                // Get on previous message in previous entry
                previousMessage = new BatchMessageIdImpl(nextMessageInQueue.getLedgerId(),
                        nextMessageInQueue.getEntryId() - 1, nextMessageInQueue.getPartitionIndex(), -1);
            }

            return previousMessage;
        } else if (!lastDequeuedMessage.equals(MessageId.earliest)) {
            // If the queue was empty we need to restart from the message just after the last one that has been dequeued
            // in the past
            return new BatchMessageIdImpl((MessageIdImpl) lastDequeuedMessage);
        } else {
            // No message was received or dequeued by this consumer. Next message would still be the startMessageId
            return startMessageId;
        }
    }

    /**
     * send the flow command to have the broker start pushing messages
     */
    void sendFlowPermitsToBroker(ClientCnx cnx, int numMessages) {
        if (cnx != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Adding {} additional permits", topic, subscription, numMessages);
            }

            cnx.ctx().writeAndFlush(Commands.newFlow(consumerId, numMessages), cnx.ctx().voidPromise());
        }
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        if (System.currentTimeMillis() > subscribeTimeout && subscribeFuture.completeExceptionally(exception)) {
            setState(State.Failed);
            log.info("[{}] Consumer creation failed for consumer {}", topic, consumerId);
            client.cleanupConsumer(this);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            unAckedMessageTracker.close();
            return CompletableFuture.completedFuture(null);
        }

        if (!isConnected()) {
            log.info("[{}] [{}] Closed Consumer (not connected)", topic, subscription);
            setState(State.Closed);
            unAckedMessageTracker.close();
            client.cleanupConsumer(this);
            return CompletableFuture.completedFuture(null);
        }

        stats.getStatTimeout().ifPresent(Timeout::cancel);

        setState(State.Closing);

        acknowledgmentsGroupingTracker.close();

        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newCloseConsumer(consumerId, requestId);

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        ClientCnx cnx = cnx();
        cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
            cnx.removeConsumer(consumerId);
            if (exception == null || !cnx.ctx().channel().isActive()) {
                log.info("[{}] [{}] Closed consumer", topic, subscription);
                setState(State.Closed);
                unAckedMessageTracker.close();
                closeFuture.complete(null);
                client.cleanupConsumer(this);
                // fail all pending-receive futures to notify application
                failPendingReceive();
            } else {
                closeFuture.completeExceptionally(exception);
            }
            return null;
        });

        return closeFuture;
    }

    private void failPendingReceive() {
        lock.readLock().lock();
        try {
            if (listenerExecutor != null && !listenerExecutor.isShutdown()) {
                while (!pendingReceives.isEmpty()) {
                    CompletableFuture<Message<T>> receiveFuture = pendingReceives.poll();
                    if (receiveFuture != null) {
                        receiveFuture.completeExceptionally(
                                new PulsarClientException.AlreadyClosedException("Consumer is already closed"));
                    } else {
                        break;
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    void activeConsumerChanged(boolean isActive) {
        if (consumerEventListener == null) {
            return;
        }

        listenerExecutor.execute(() -> {
            if (isActive) {
                consumerEventListener.becameActive(this, partitionIndex);
            } else {
                consumerEventListener.becameInactive(this, partitionIndex);
            }
        });
    }

    void messageReceived(MessageIdData messageId, ByteBuf headersAndPayload, ClientCnx cnx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received message: {}/{}", topic, subscription, messageId.getLedgerId(),
                    messageId.getEntryId());
        }

        MessageIdImpl msgId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), getPartitionIndex());
        if (acknowledgmentsGroupingTracker.isDuplicate(msgId)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignoring message as it was already being acked earlier by same consumer {}/{}",
                        topic, subscription, msgId);
            }
            if (conf.getReceiverQueueSize() == 0) {
                increaseAvailablePermits(cnx);
            }
            return;
        }

        MessageMetadata msgMetadata = null;
        ByteBuf payload = headersAndPayload;

        if (!verifyChecksum(headersAndPayload, messageId)) {
            // discard message with checksum error
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        try {
            msgMetadata = Commands.parseMessageMetadata(payload);
        } catch (Throwable t) {
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        ByteBuf decryptedPayload = decryptPayloadIfNeeded(messageId, msgMetadata, payload, cnx);
        
        boolean isMessageUndecryptable = isMessageUndecryptable(msgMetadata);
        
        if (decryptedPayload == null) {
            // Message was discarded or CryptoKeyReader isn't implemented
            return;
        }
        
        // uncompress decryptedPayload and release decryptedPayload-ByteBuf
        ByteBuf uncompressedPayload = isMessageUndecryptable ? decryptedPayload.retain() 
                : uncompressPayloadIfNeeded(messageId, msgMetadata, decryptedPayload, cnx);
        decryptedPayload.release();
        if (uncompressedPayload == null) {
            // Message was discarded on decompression error
            return;
        }

        final int numMessages = msgMetadata.getNumMessagesInBatch();

        // if message is not decryptable then it can't be parsed as a batch-message. so, add EncyrptionCtx to message
        // and return undecrypted payload
        if (isMessageUndecryptable || (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch())) {
            final MessageImpl<T> message = new MessageImpl<>(msgId, msgMetadata, uncompressedPayload,
                    createEncryptionContext(msgMetadata), cnx, schema);
            uncompressedPayload.release();
            msgMetadata.recycle();

            lock.readLock().lock();
            try {
                // Enqueue the message so that it can be retrieved when application calls receive()
                // if the conf.getReceiverQueueSize() is 0 then discard message if no one is waiting for it.
                // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
                unAckedMessageTracker.add((MessageIdImpl) message.getMessageId());
                if (!pendingReceives.isEmpty()) {
                    notifyPendingReceivedCallback(message, null);
                } else if (conf.getReceiverQueueSize() != 0 || waitingOnReceiveForZeroQueueSize) {
                    incomingMessages.add(message);
                } else if (conf.getReceiverQueueSize() == 0 && listener != null) {
                    triggerZeroQueueSizeListener(message);
                }
            } finally {
                lock.readLock().unlock();
            }
        } else {
            if (conf.getReceiverQueueSize() == 0) {
                log.warn(
                        "Closing consumer [{}]-[{}] due to unsupported received batch-message with zero receiver queue size",
                        subscription, consumerName);
                // close connection
                closeAsync().handle((ok, e) -> {
                    // notify callback with failure result
                    notifyPendingReceivedCallback(null,
                            new PulsarClientException.InvalidMessageException(
                                    format("Unsupported Batch message with 0 size receiver queue for [%s]-[%s] ",
                                            subscription, consumerName)));
                    return null;
                });
            } else {
                // handle batch message enqueuing; uncompressed payload has all messages in batch
                receiveIndividualMessagesFromBatch(msgMetadata, uncompressedPayload, messageId, cnx);
            }
            uncompressedPayload.release();
            msgMetadata.recycle();
        }

        if (listener != null && conf.getReceiverQueueSize() != 0) {
            // Trigger the notification on the message listener in a separate thread to avoid blocking the networking
            // thread while the message processing happens
            listenerExecutor.execute(() -> {
                for (int i = 0; i < numMessages; i++) {
                    try {
                        Message<T> msg = internalReceive(0, TimeUnit.MILLISECONDS);
                        // complete the callback-loop in case queue is cleared up
                        if (msg == null) {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] [{}] Message has been cleared from the queue", topic, subscription);
                            }
                            break;
                        }
                        try {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}][{}] Calling message listener for message {}", topic, subscription,
                                        msg.getMessageId());
                            }
                            listener.received(ConsumerImpl.this, msg);
                        } catch (Throwable t) {
                            log.error("[{}][{}] Message listener error in processing message: {}", topic, subscription,
                                    msg.getMessageId(), t);
                        }

                    } catch (PulsarClientException e) {
                        log.warn("[{}] [{}] Failed to dequeue the message for listener", topic, subscription, e);
                        return;
                    }
                }
            });
        }
    }

    /**
     * Notify waiting asyncReceive request with the received message
     *
     * @param message
     */
    void notifyPendingReceivedCallback(final Message<T> message, Exception exception) {
        if (!pendingReceives.isEmpty()) {
            // fetch receivedCallback from queue
            CompletableFuture<Message<T>> receivedFuture = pendingReceives.poll();
            if (exception == null) {
                checkNotNull(message, "received message can't be null");
                if (receivedFuture != null) {
                    if (conf.getReceiverQueueSize() == 0) {
                        // return message to receivedCallback
                        receivedFuture.complete(message);
                    } else {
                        // increase permits for available message-queue
                        messageProcessed(message);
                        // return message to receivedCallback
                        listenerExecutor.execute(() -> receivedFuture.complete(message));
                    }
                }
            } else {
                listenerExecutor.execute(() -> receivedFuture.completeExceptionally(exception));
            }
        }
    }

    private void triggerZeroQueueSizeListener(final Message<T> message) {
        checkArgument(conf.getReceiverQueueSize() == 0);
        checkNotNull(listener, "listener can't be null");
        checkNotNull(message, "unqueued message can't be null");

        listenerExecutor.execute(() -> {
            stats.updateNumMsgsReceived(message);
            try {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Calling message listener for unqueued message {}", topic, subscription,
                            message.getMessageId());
                }
                listener.received(ConsumerImpl.this, message);
            } catch (Throwable t) {
                log.error("[{}][{}] Message listener error in processing unqueued message: {}", topic, subscription,
                        message.getMessageId(), t);
            }
            increaseAvailablePermits(cnx());
        });
    }

    void receiveIndividualMessagesFromBatch(MessageMetadata msgMetadata, ByteBuf uncompressedPayload,
            MessageIdData messageId, ClientCnx cnx) {
        int batchSize = msgMetadata.getNumMessagesInBatch();

        // create ack tracker for entry aka batch
        MessageIdImpl batchMessage = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                getPartitionIndex());
        BatchMessageAcker acker = BatchMessageAcker.newAcker(batchSize);
        unAckedMessageTracker.add(batchMessage);

        int skippedMessages = 0;
        try {
            for (int i = 0; i < batchSize; ++i) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] processing message num - {} in batch", subscription, consumerName, i);
                }
                PulsarApi.SingleMessageMetadata.Builder singleMessageMetadataBuilder = PulsarApi.SingleMessageMetadata
                        .newBuilder();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                        singleMessageMetadataBuilder, i, batchSize);

                if (subscriptionMode == SubscriptionMode.NonDurable && startMessageId != null
                        && messageId.getLedgerId() == startMessageId.getLedgerId()
                        && messageId.getEntryId() == startMessageId.getEntryId()
                        && i <= startMessageId.getBatchIndex()) {
                    // If we are receiving a batch message, we need to discard messages that were prior
                    // to the startMessageId
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Ignoring message from before the startMessageId", subscription,
                                consumerName);
                    }

                    ++skippedMessages;
                    continue;
                }
                if (singleMessageMetadataBuilder.getCompactedOut()) {
                    // message has been compacted out, so don't send to the user
                    singleMessagePayload.release();
                    singleMessageMetadataBuilder.recycle();

                    ++skippedMessages;
                    continue;
                }

                BatchMessageIdImpl batchMessageIdImpl = new BatchMessageIdImpl(messageId.getLedgerId(),
                        messageId.getEntryId(), getPartitionIndex(), i, acker);
                final MessageImpl<T> message = new MessageImpl<>(batchMessageIdImpl, msgMetadata,
                        singleMessageMetadataBuilder.build(), singleMessagePayload,
                        createEncryptionContext(msgMetadata), cnx, schema);
                lock.readLock().lock();
                try {
                    if (pendingReceives.isEmpty()) {
                        incomingMessages.add(message);
                    } else {
                        notifyPendingReceivedCallback(message, null);
                    }
                } finally {
                    lock.readLock().unlock();
                }
                singleMessagePayload.release();
                singleMessageMetadataBuilder.recycle();
            }
        } catch (IOException e) {
            log.warn("[{}] [{}] unable to obtain message in batch", subscription, consumerName);
            discardCorruptedMessage(messageId, cnx, ValidationError.BatchDeSerializeError);
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] enqueued messages in batch. queue size - {}, available queue size - {}", subscription,
                    consumerName, incomingMessages.size(), incomingMessages.remainingCapacity());
        }

        if (skippedMessages > 0) {
            increaseAvailablePermits(cnx, skippedMessages);
        }
    }

    /**
     * Record the event that one message has been processed by the application.
     *
     * Periodically, it sends a Flow command to notify the broker that it can push more messages
     */
    protected synchronized void messageProcessed(Message<?> msg) {
        ClientCnx currentCnx = cnx();
        ClientCnx msgCnx = ((MessageImpl<?>) msg).getCnx();
        lastDequeuedMessage = msg.getMessageId();

        if (msgCnx != currentCnx) {
            // The processed message did belong to the old queue that was cleared after reconnection.
            return;
        }

        increaseAvailablePermits(currentCnx);
        stats.updateNumMsgsReceived(msg);

        if (conf.getAckTimeoutMillis() != 0) {
            // reset timer for messages that are received by the client
            MessageIdImpl id = (MessageIdImpl) msg.getMessageId();
            if (id instanceof BatchMessageIdImpl) {
                id = new MessageIdImpl(id.getLedgerId(), id.getEntryId(), getPartitionIndex());
            }
            if (partitionIndex != -1) {
                // we should no longer track this message, TopicsConsumer will take care from now onwards
                unAckedMessageTracker.remove(id);
            } else {
                unAckedMessageTracker.add(id);
            }
        }
    }

    void increaseAvailablePermits(ClientCnx currentCnx) {
        increaseAvailablePermits(currentCnx, 1);
    }

    private void increaseAvailablePermits(ClientCnx currentCnx, int delta) {
        int available = AVAILABLE_PERMITS_UPDATER.addAndGet(this, delta);

        while (available >= receiverQueueRefillThreshold) {
            if (AVAILABLE_PERMITS_UPDATER.compareAndSet(this, available, 0)) {
                sendFlowPermitsToBroker(currentCnx, available);
                break;
            } else {
                available = AVAILABLE_PERMITS_UPDATER.get(this);
            }
        }
    }

    private ByteBuf decryptPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
            ClientCnx currentCnx) {

        if (msgMetadata.getEncryptionKeysCount() == 0) {
            return payload.retain();
        }

        // If KeyReader is not configured throw exception based on config param
        if (conf.getCryptoKeyReader() == null) {

            if (conf.getCryptoFailureAction() == ConsumerCryptoFailureAction.CONSUME) {
                log.warn("[{}][{}][{}] CryptoKeyReader interface is not implemented. Consuming encrypted message.",
                        topic, subscription, consumerName);
                return payload.retain();
            } else if (conf.getCryptoFailureAction() == ConsumerCryptoFailureAction.DISCARD) {
                log.warn(
                        "[{}][{}][{}] Skipping decryption since CryptoKeyReader interface is not implemented and config is set to discard",
                        topic, subscription, consumerName);
                discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
            } else {
                log.error(
                        "[{}][{}][{}] Message delivery failed since CryptoKeyReader interface is not implemented to consume encrypted message",
                        topic, subscription, consumerName);
            }
            return null;
        }

        ByteBuf decryptedData = this.msgCrypto.decrypt(msgMetadata, payload, conf.getCryptoKeyReader());
        if (decryptedData != null) {
            return decryptedData;
        }

        if (conf.getCryptoFailureAction() == ConsumerCryptoFailureAction.CONSUME) {
            // Note, batch message will fail to consume even if config is set to consume
            log.warn("[{}][{}][{}][{}] Decryption failed. Consuming encrypted message since config is set to consume.",
                    topic, subscription, consumerName, messageId);
            return payload.retain();
        } else if (conf.getCryptoFailureAction() == ConsumerCryptoFailureAction.DISCARD) {
            log.warn("[{}][{}][{}][{}] Discarding message since decryption failed and config is set to discard", topic,
                    subscription, consumerName, messageId);
            discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
        } else {
            log.error("[{}][{}][{}][{}] Message delivery failed since unable to decrypt incoming message", topic,
                    subscription, consumerName, messageId);
        }
        return null;

    }

    private ByteBuf uncompressPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
            ClientCnx currentCnx) {
        CompressionType compressionType = msgMetadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (payloadSize > PulsarDecoder.MaxMessageSize) {
            // payload size is itself corrupted since it cannot be bigger than the MaxMessageSize
            log.error("[{}][{}] Got corrupted payload message size {} at {}", topic, subscription, payloadSize,
                    messageId);
            discardCorruptedMessage(messageId, currentCnx, ValidationError.UncompressedSizeCorruption);
            return null;
        }

        try {
            ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
            return uncompressedPayload;
        } catch (IOException e) {
            log.error("[{}][{}] Failed to decompress message with {} at {}: {}", topic, subscription, compressionType,
                    messageId, e.getMessage(), e);
            discardCorruptedMessage(messageId, currentCnx, ValidationError.DecompressionError);
            return null;
        }
    }

    private boolean verifyChecksum(ByteBuf headersAndPayload, MessageIdData messageId) {

        if (hasChecksum(headersAndPayload)) {
            int checksum = readChecksum(headersAndPayload);
            int computedChecksum = computeChecksum(headersAndPayload);
            if (checksum != computedChecksum) {
                log.error(
                        "[{}][{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
                        topic, subscription, messageId.getLedgerId(), messageId.getEntryId(),
                        Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }

    private void discardCorruptedMessage(MessageIdData messageId, ClientCnx currentCnx,
            ValidationError validationError) {
        log.error("[{}][{}] Discarding corrupted message at {}:{}", topic, subscription, messageId.getLedgerId(),
                messageId.getEntryId());
        discardMessage(messageId, currentCnx, validationError);
    }

    private void discardMessage(MessageIdData messageId, ClientCnx currentCnx, ValidationError validationError) {
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), AckType.Individual,
                                      validationError, Collections.emptyMap());
        currentCnx.ctx().writeAndFlush(cmd, currentCnx.ctx().voidPromise());
        increaseAvailablePermits(currentCnx);
        stats.incrementNumReceiveFailed();
    }

    @Override
    String getHandlerName() {
        return subscription;
    }

    @Override
    public boolean isConnected() {
        return getClientCnx() != null && (getState() == State.Ready);
    }

    int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    public int getAvailablePermits() {
        return AVAILABLE_PERMITS_UPDATER.get(this);
    }

    @Override
    public int numMessagesInQueue() {
        return incomingMessages.size();
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        ClientCnx cnx = cnx();
        if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getNumber()) {
            int currentSize = 0;
            synchronized (this) {
                currentSize = incomingMessages.size();
                incomingMessages.clear();
                unAckedMessageTracker.clear();
            }
            cnx.ctx().writeAndFlush(Commands.newRedeliverUnacknowledgedMessages(consumerId), cnx.ctx().voidPromise());
            if (currentSize > 0) {
                increaseAvailablePermits(cnx, currentSize);
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Redeliver unacked messages and send {} permits", subscription, topic,
                        consumerName, currentSize);
            }
            return;
        }
        if (cnx == null || (getState() == State.Connecting)) {
            log.warn("[{}] Client Connection needs to be established for redelivery of unacknowledged messages", this);
        } else {
            log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
            cnx.ctx().close();
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
        checkArgument(messageIds.stream().findFirst().get() instanceof MessageIdImpl);

        if (conf.getSubscriptionType() != SubscriptionType.Shared) {
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        ClientCnx cnx = cnx();
        if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getNumber()) {
            int messagesFromQueue = removeExpiredMessagesFromQueue(messageIds);
            Iterable<List<MessageIdImpl>> batches = Iterables.partition(
                messageIds.stream()
                    .map(messageId -> (MessageIdImpl)messageId)
                    .collect(Collectors.toSet()), MAX_REDELIVER_UNACKNOWLEDGED);
            MessageIdData.Builder builder = MessageIdData.newBuilder();
            batches.forEach(ids -> {
                List<MessageIdData> messageIdDatas = ids.stream().map(messageId -> {
                    // attempt to remove message from batchMessageAckTracker
                    builder.setPartition(messageId.getPartitionIndex());
                    builder.setLedgerId(messageId.getLedgerId());
                    builder.setEntryId(messageId.getEntryId());
                    return builder.build();
                }).collect(Collectors.toList());
                ByteBuf cmd = Commands.newRedeliverUnacknowledgedMessages(consumerId, messageIdDatas);
                cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                messageIdDatas.forEach(MessageIdData::recycle);
            });
            if (messagesFromQueue > 0) {
                increaseAvailablePermits(cnx, messagesFromQueue);
            }
            builder.recycle();
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Redeliver unacked messages and increase {} permits", subscription, topic,
                        consumerName, messagesFromQueue);
            }
            return;
        }
        if (cnx == null || (getState() == State.Connecting)) {
            log.warn("[{}] Client Connection needs to be established for redelivery of unacknowledged messages", this);
        } else {
            log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
            cnx.ctx().close();
        }
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        try {
            seekAsync(messageId).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
        }

        if (!isConnected()) {
            return FutureUtil.failedFuture(new PulsarClientException("Not connected to broker"));
        }

        final CompletableFuture<Void> seekFuture = new CompletableFuture<>();

        long requestId = client.newRequestId();
        MessageIdImpl msgId = (MessageIdImpl) messageId;
        ByteBuf seek = Commands.newSeek(consumerId, requestId, msgId.getLedgerId(), msgId.getEntryId());
        ClientCnx cnx = cnx();

        log.info("[{}][{}] Seek subscription to message id {}", topic, subscription, messageId);

        cnx.sendRequestWithId(seek, requestId).thenRun(() -> {
            log.info("[{}][{}] Successfully reset subscription to message id {}", topic, subscription, messageId);
            seekFuture.complete(null);
        }).exceptionally(e -> {
            log.error("[{}][{}] Failed to reset subscription: {}", topic, subscription, e.getCause().getMessage());
            seekFuture.completeExceptionally(e.getCause());
            return null;
        });
        return seekFuture;
    }

    public boolean hasMessageAvailable() throws PulsarClientException {
        try {
            if (lastMessageIdInBroker.compareTo(lastDequeuedMessage) > 0 &&
                ((MessageIdImpl)lastMessageIdInBroker).getEntryId() != -1) {
                return true;
            }

            return hasMessageAvailableAsync().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new PulsarClientException(e);
        }
    }

    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        final CompletableFuture<Boolean> booleanFuture = new CompletableFuture<>();

        if (lastMessageIdInBroker.compareTo(lastDequeuedMessage) > 0 &&
            ((MessageIdImpl)lastMessageIdInBroker).getEntryId() != -1) {
            booleanFuture.complete(true);
        } else {
            getLastMessageIdAsync().thenAccept(messageId -> {
                lastMessageIdInBroker = messageId;
                if (lastMessageIdInBroker.compareTo(lastDequeuedMessage) > 0 &&
                    ((MessageIdImpl)lastMessageIdInBroker).getEntryId() != -1) {
                    booleanFuture.complete(true);
                } else {
                    booleanFuture.complete(false);
                }
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                booleanFuture.completeExceptionally(e.getCause());
                return null;
            });
        }
        return booleanFuture;
    }

    CompletableFuture<MessageId> getLastMessageIdAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
        }

        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = new Backoff(100, TimeUnit.MILLISECONDS,
            opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS,
            0 , TimeUnit.MILLISECONDS);
        CompletableFuture<MessageId> getLastMessageIdFuture = new CompletableFuture<>();

        internalGetLastMessageIdAsync(backoff, opTimeoutMs, getLastMessageIdFuture);
        return getLastMessageIdFuture;
    }

    private void internalGetLastMessageIdAsync(final Backoff backoff,
                                               final AtomicLong remainingTime,
                                               CompletableFuture<MessageId> future) {
        ClientCnx cnx = cnx();
        if (isConnected() && cnx != null) {
            if (!Commands.peerSupportsGetLastMessageId(cnx.getRemoteEndpointProtocolVersion())) {
                future.completeExceptionally(new PulsarClientException
                    .NotSupportedException("GetLastMessageId Not supported for ProtocolVersion: " +
                    cnx.getRemoteEndpointProtocolVersion()));
            }

            long requestId = client.newRequestId();
            ByteBuf getLastIdCmd = Commands.newGetLastMessageId(consumerId, requestId);
            log.info("[{}][{}] Get topic last message Id", topic, subscription);

            cnx.sendGetLastMessageId(getLastIdCmd, requestId).thenAccept((result) -> {
                log.info("[{}][{}] Successfully getLastMessageId {}:{}",
                    topic, subscription, result.getLedgerId(), result.getEntryId());
                future.complete(new MessageIdImpl(result.getLedgerId(),
                    result.getEntryId(), result.getPartition()));
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                future.completeExceptionally(e.getCause());
                return null;
            });
        } else {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            if (nextDelay <= 0) {
                future.completeExceptionally(new PulsarClientException
                    .TimeoutException("Could not getLastMessageId within configured timeout."));
                return;
            }

            ((ScheduledExecutorService) listenerExecutor).schedule(() -> {
                log.warn("[{}] [{}] Could not get connection while getLastMessageId -- Will try again in {} ms",
                    topic, getHandlerName(), nextDelay);
                remainingTime.addAndGet(-nextDelay);
                internalGetLastMessageIdAsync(backoff, remainingTime, future);
            }, nextDelay, TimeUnit.MILLISECONDS);
        }
    }

    private MessageIdImpl getMessageIdImpl(Message<?> msg) {
        MessageIdImpl messageId = (MessageIdImpl) msg.getMessageId();
        if (messageId instanceof BatchMessageIdImpl) {
            // messageIds contain MessageIdImpl, not BatchMessageIdImpl
            messageId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), getPartitionIndex());
        }
        return messageId;
    }


    private boolean isMessageUndecryptable(MessageMetadata msgMetadata) {
        return (msgMetadata.getEncryptionKeysCount() > 0 && conf.getCryptoKeyReader() == null
                && conf.getCryptoFailureAction() == ConsumerCryptoFailureAction.CONSUME);
    }

    /**
     * Create EncryptionContext if message payload is encrypted
     * 
     * @param msgMetadata
     * @return {@link Optional}<{@link EncryptionContext}>
     */
    private Optional<EncryptionContext> createEncryptionContext(MessageMetadata msgMetadata) {

        EncryptionContext encryptionCtx = null;
        if (msgMetadata.getEncryptionKeysCount() > 0) {
            encryptionCtx = new EncryptionContext();
            Map<String, EncryptionKey> keys = msgMetadata.getEncryptionKeysList().stream()
                    .collect(
                            Collectors.toMap(EncryptionKeys::getKey,
                                    e -> new EncryptionKey(e.getValue().toByteArray(),
                                            e.getMetadataList() != null
                                                    ? e.getMetadataList().stream().collect(
                                                            Collectors.toMap(KeyValue::getKey, KeyValue::getValue))
                                                    : null)));
            byte[] encParam = new byte[MessageCrypto.ivLen];
            msgMetadata.getEncryptionParam().copyTo(encParam, 0);
            Optional<Integer> batchSize = Optional
                    .ofNullable(msgMetadata.hasNumMessagesInBatch() ? msgMetadata.getNumMessagesInBatch() : null);
            encryptionCtx.setKeys(keys);
            encryptionCtx.setParam(encParam);
            encryptionCtx.setAlgorithm(msgMetadata.getEncryptionAlgo());
            encryptionCtx.setCompressionType(msgMetadata.getCompression());
            encryptionCtx.setUncompressedMessageSize(msgMetadata.getUncompressedSize());
            encryptionCtx.setBatchSize(batchSize);
        }
        return Optional.ofNullable(encryptionCtx);
    }

    private int removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
        int messagesFromQueue = 0;
        Message<T> peek = incomingMessages.peek();
        if (peek != null) {
            MessageIdImpl messageId = getMessageIdImpl(peek);
            if (!messageIds.contains(messageId)) {
                // first message is not expired, then no message is expired in queue.
                return 0;
            }

            // try not to remove elements that are added while we remove
            Message<T> message = incomingMessages.poll();
            while (message != null) {
                messagesFromQueue++;
                MessageIdImpl id = getMessageIdImpl(message);
                if (!messageIds.contains(id)) {
                    messageIds.add(id);
                    break;
                }
                message = incomingMessages.poll();
            }
        }
        return messagesFromQueue;
    }

    @Override
    public ConsumerStats getStats() {
        return stats;
    }

    void setTerminated() {
        log.info("[{}] [{}] [{}] Consumer has reached the end of topic", subscription, topic, consumerName);
        hasReachedEndOfTopic = true;
        if (listener != null) {
            // Propagate notification to listener
            listener.reachedEndOfTopic(this);
        }
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return hasReachedEndOfTopic;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, subscription, consumerName);
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

    @VisibleForTesting
    public ClientCnx getClientCnx() {
        return this.connectionHandler.getClientCnx();
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

    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

}
