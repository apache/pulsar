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
import static java.lang.String.format;
import static org.apache.pulsar.checksum.utils.Crc32cChecksum.computeChecksum;
import static org.apache.pulsar.common.api.Commands.hasChecksum;
import static org.apache.pulsar.common.api.Commands.readChecksum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.ValidationError;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class ConsumerImpl extends ConsumerBase {
    private static final int MAX_REDELIVER_UNACKNOWLEDGED = 1000;

    private final long consumerId;

    // Number of messages that have delivered to the application. Every once in a while, this number will be sent to the
    // broker to notify that we are ready to get (and store in the incoming messages queue) more messages
    private static final AtomicIntegerFieldUpdater<ConsumerImpl> AVAILABLE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ConsumerImpl.class, "availablePermits");
    private volatile int availablePermits = 0;

    private MessageIdImpl lastDequeuedMessage;

    private long subscribeTimeout;
    private final int partitionIndex;

    private final int receiverQueueRefillThreshold;
    private final CompressionCodecProvider codecProvider;

    private volatile boolean waitingOnReceiveForZeroQueueSize = false;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ReadWriteLock zeroQueueLock;

    private final UnAckedMessageTracker unAckedMessageTracker;
    private final ConcurrentNavigableMap<MessageIdImpl, BitSet> batchMessageAckTracker;

    protected final ConsumerStats stats;
    private final int priorityLevel;
    private final SubscriptionMode subscriptionMode;
    private final MessageId startMessageId;

    private volatile boolean hasReachedEndOfTopic;

    static enum SubscriptionMode {
        // Make the subscription to be backed by a durable cursor that will retain messages and persist the current
        // position
        Durable,

        // Lightweight subscription mode that doesn't have a durable cursor associated
        NonDurable
    }

    ConsumerImpl(PulsarClientImpl client, String topic, String subscription, ConsumerConfiguration conf,
            ExecutorService listenerExecutor, int partitionIndex, CompletableFuture<Consumer> subscribeFuture) {
        this(client, topic, subscription, conf, listenerExecutor, partitionIndex, subscribeFuture,
                SubscriptionMode.Durable, null);
    }

    ConsumerImpl(PulsarClientImpl client, String topic, String subscription, ConsumerConfiguration conf,
            ExecutorService listenerExecutor, int partitionIndex, CompletableFuture<Consumer> subscribeFuture,
            SubscriptionMode subscriptionMode, MessageId startMessageId) {
        super(client, topic, subscription, conf, conf.getReceiverQueueSize(), listenerExecutor, subscribeFuture);
        this.consumerId = client.newConsumerId();
        this.subscriptionMode = subscriptionMode;
        this.startMessageId = startMessageId;
        AVAILABLE_PERMITS_UPDATER.set(this, 0);
        this.subscribeTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
        this.partitionIndex = partitionIndex;
        this.receiverQueueRefillThreshold = conf.getReceiverQueueSize() / 2;
        this.codecProvider = new CompressionCodecProvider();
        this.priorityLevel = conf.getPriorityLevel();
        this.batchMessageAckTracker = new ConcurrentSkipListMap<>();
        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ConsumerStats(client, conf, this);
        } else {
            stats = ConsumerStats.CONSUMER_STATS_DISABLED;
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

        grabCnx();
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
                log.info("[{}][{}] Successfully unsubscribed from topic", topic, subscription);
                batchMessageAckTracker.clear();
                unAckedMessageTracker.close();
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
    protected Message internalReceive() throws PulsarClientException {
        if (conf.getReceiverQueueSize() == 0) {
            checkArgument(zeroQueueLock != null, "Receiver queue size can't be modified");
            zeroQueueLock.writeLock().lock();
            try {
                return fetchSingleMessageFromBroker();
            } finally {
                zeroQueueLock.writeLock().unlock();
            }
        }
        Message message;
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
    protected CompletableFuture<Message> internalReceiveAsync() {

        CompletableFuture<Message> result = new CompletableFuture<Message>();
        Message message = null;
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

    private Message fetchSingleMessageFromBroker() throws PulsarClientException {
        checkArgument(conf.getReceiverQueueSize() == 0);

        // Just being cautious
        if (incomingMessages.size() > 0) {
            log.error("The incoming message queue should never be greater than 0 when Queue size is 0");
            incomingMessages.clear();
        }

        Message message;
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
                lastDequeuedMessage = (MessageIdImpl) message.getMessageId();
                ClientCnx msgCnx = ((MessageImpl) message).getCnx();
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
    protected Message internalReceive(int timeout, TimeUnit unit) throws PulsarClientException {
        Message message;
        try {
            message = incomingMessages.poll(timeout, unit);
            if (message != null) {
                messageProcessed(message);
            }
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stats.incrementNumReceiveFailed();
            throw new PulsarClientException(e);
        }
    }

    // we may not be able to ack message being acked by client. However messages in prior
    // batch may be ackable
    private void ackMessagesInEarlierBatch(BatchMessageIdImpl batchMessageId, MessageIdImpl message) {
        // get entry before this message and ack that message on broker
        MessageIdImpl lowerKey = batchMessageAckTracker.lowerKey(message);
        if (lowerKey != null) {
            NavigableMap<MessageIdImpl, BitSet> entriesUpto = batchMessageAckTracker.headMap(lowerKey, true);
            for (Object key : entriesUpto.keySet()) {
                entriesUpto.remove(key);
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] ack prior message {} to broker on cumulative ack for message {}", subscription,
                    consumerId, lowerKey, batchMessageId);
            }
            sendAcknowledge(lowerKey, AckType.Cumulative);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] no messages prior to message {}", subscription, consumerId, batchMessageId);
            }
        }
    }

    boolean markAckForBatchMessage(BatchMessageIdImpl batchMessageId, AckType ackType) {
        // we keep track of entire batch and so need MessageIdImpl and cannot use BatchMessageIdImpl
        MessageIdImpl message = new MessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(),
            batchMessageId.getPartitionIndex());
        BitSet bitSet = batchMessageAckTracker.get(message);
        if (bitSet == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] message not found {} for ack {}", subscription, consumerId, batchMessageId,
                    ackType);
            }
            return true;
        }
        int batchIndex = batchMessageId.getBatchIndex();
        int batchSize = bitSet.length();
        if (ackType == AckType.Individual) {
            bitSet.clear(batchIndex);
        } else {
            // +1 since to argument is exclusive
            bitSet.clear(0, batchIndex + 1);
        }
        // all messages in this batch have been acked
        if (bitSet.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] can ack message to broker {}, acktype {}, cardinality {}, length {}", subscription,
                    consumerName, batchMessageId, ackType, bitSet.cardinality(), bitSet.length());
            }
            if (ackType == AckType.Cumulative) {
                batchMessageAckTracker.keySet().removeIf(m -> (m.compareTo(message) <= 0));
            }
            batchMessageAckTracker.remove(message);
            // increment Acknowledge-msg counter with number of messages in batch only if AckType is Individual.
            // CumulativeAckType is handled while sending ack to broker
            if (ackType == AckType.Individual) {
                stats.incrementNumAcksSent(batchSize);
            }
            return true;
        } else {
            // we cannot ack this message to broker. but prior message may be ackable
            if (ackType == AckType.Cumulative) {
                ackMessagesInEarlierBatch(batchMessageId, message);
            }
            if (log.isDebugEnabled()) {
                int outstandingAcks = batchMessageAckTracker.get(message).cardinality();
                log.debug("[{}] [{}] cannot ack message to broker {}, acktype {}, pending acks - {}", subscription,
                    consumerName, batchMessageId, ackType, outstandingAcks);
            }
        }
        return false;
    }

    // if we are consuming a mix of batch and non-batch messages then cumulative ack on non-batch messages
    // should clean up the ack tracker as well
    private void updateBatchAckTracker(MessageIdImpl message, AckType ackType) {
        if (batchMessageAckTracker.isEmpty()) {
            return;
        }
        MessageIdImpl lowerKey = batchMessageAckTracker.lowerKey(message);
        if (lowerKey != null) {
            NavigableMap<MessageIdImpl, BitSet> entriesUpto = batchMessageAckTracker.headMap(lowerKey, true);
            for (Object key : entriesUpto.keySet()) {
                entriesUpto.remove(key);
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] updated batch ack tracker up to message {} on cumulative ack for message {}",
                    subscription, consumerId, lowerKey, message);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] no messages to clean up prior to message {}", subscription, consumerId, message);
            }
        }
    }

    /**
     * helper method that returns current state of data structure used to track acks for batch messages
     *
     * @return true if all batch messages have been acknowledged
     */
    public boolean isBatchingAckTrackerEmpty() {
        return batchMessageAckTracker.isEmpty();
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType) {
        checkArgument(messageId instanceof MessageIdImpl);
        if (getState() != State.Ready && getState() != State.Connecting) {
            stats.incrementNumAcksFailed();
            return FutureUtil.failedFuture(new PulsarClientException("Consumer not ready. State: " + getState()));
        }

        if (messageId instanceof BatchMessageIdImpl) {
            if (markAckForBatchMessage((BatchMessageIdImpl) messageId, ackType)) {
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
        // if we got a cumulative ack on non batch message, check if any earlier batch messages need to be removed
        // from batch message tracker
        if (ackType == AckType.Cumulative && !(messageId instanceof BatchMessageIdImpl)) {
            updateBatchAckTracker((MessageIdImpl) messageId, ackType);
        }
        return sendAcknowledge(messageId, ackType);
    }

    private CompletableFuture<Void> sendAcknowledge(MessageId messageId, AckType ackType) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;
        final ByteBuf cmd = Commands.newAck(consumerId, msgId.getLedgerId(), msgId.getEntryId(), ackType, null);

        // There's no actual response from ack messages
        final CompletableFuture<Void> ackFuture = new CompletableFuture<Void>();

        if (isConnected()) {
            cnx().ctx().writeAndFlush(cmd).addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
                    if (future.isSuccess()) {
                        if (ackType == AckType.Individual) {
                            unAckedMessageTracker.remove(msgId);
                            // increment counter by 1 for non-batch msg
                            if (!(messageId instanceof BatchMessageIdImpl)) {
                                stats.incrementNumAcksSent(1);
                            }
                        } else if (ackType == AckType.Cumulative) {
                            stats.incrementNumAcksSent(unAckedMessageTracker.removeMessagesTill(msgId));
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] [{}] Successfully acknowledged message - {}, acktype {}", subscription,
                                    topic, consumerName, messageId, ackType);
                        }
                        ackFuture.complete(null);
                    } else {
                        stats.incrementNumAcksFailed();
                        ackFuture.completeExceptionally(new PulsarClientException(future.cause()));
                    }
                }
            });
        } else {
            stats.incrementNumAcksFailed();
            ackFuture
                .completeExceptionally(new PulsarClientException("Not connected to broker. State: " + getState()));
        }

        return ackFuture;
    }

    @Override
    void connectionOpened(final ClientCnx cnx) {
        setClientCnx(cnx);
        cnx.registerConsumer(consumerId, this);

        log.info("[{}][{}] Subscribing to topic on cnx {}", topic, subscription, cnx.ctx().channel());

        long requestId = client.newRequestId();

        int currentSize;
        MessageIdImpl startMessageId;
        synchronized (this) {
            currentSize = incomingMessages.size();
            startMessageId = clearReceiverQueue();
            unAckedMessageTracker.clear();
            batchMessageAckTracker.clear();
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
            startMessageIdData = builder.build();
            builder.recycle();
        }

        ByteBuf request = Commands.newSubscribe(topic, subscription, consumerId, requestId, getSubType(), priorityLevel,
                consumerName, isDurable, startMessageIdData);
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
                            || (conf.getReceiverQueueSize() == 0 && currentSize > 0)) {
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
            log.warn("[{}][{}] Failed to subscribe to topic on {}", topic, subscription,
                cnx.channel().remoteAddress());
            if (e.getCause() instanceof PulsarClientException
                && isRetriableError((PulsarClientException) e.getCause())
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
    private MessageIdImpl clearReceiverQueue() {
        List<Message> currentMessageQueue = new ArrayList<>(incomingMessages.size());
        incomingMessages.drainTo(currentMessageQueue);
        if (!currentMessageQueue.isEmpty()) {
            MessageIdImpl nextMessageInQueue = (MessageIdImpl) currentMessageQueue.get(0).getMessageId();
            MessageIdImpl previousMessage = new MessageIdImpl(nextMessageInQueue.getLedgerId(),
                    nextMessageInQueue.getEntryId() - 1, nextMessageInQueue.getPartitionIndex());
            return previousMessage;
        } else if (lastDequeuedMessage != null) {
            // If the queue was empty we need to restart from the message just after the last one that has been dequeued
            // in the past
            return lastDequeuedMessage;
        } else {
            // No message was received or dequeued by this consumer. Next message would still be the startMessageId
            return (MessageIdImpl) startMessageId;
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
    void connectionFailed(PulsarClientException exception) {
        if (System.currentTimeMillis() > subscribeTimeout && subscribeFuture.completeExceptionally(exception)) {
            setState(State.Failed);
            client.cleanupConsumer(this);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            batchMessageAckTracker.clear();
            unAckedMessageTracker.close();
            return CompletableFuture.completedFuture(null);
        }

        if (!isConnected()) {
            log.info("[{}] [{}] Closed Consumer (not connected)", topic, subscription);
            setState(State.Closed);
            batchMessageAckTracker.clear();
            unAckedMessageTracker.close();
            client.cleanupConsumer(this);
            return CompletableFuture.completedFuture(null);
        }

        Timeout timeout = stats.getStatTimeout();
        if (timeout != null) {
            timeout.cancel();
        }

        setState(State.Closing);

        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newCloseConsumer(consumerId, requestId);

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        ClientCnx cnx = cnx();
        cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
            cnx.removeConsumer(consumerId);
            if (exception == null || !cnx.ctx().channel().isActive()) {
                log.info("[{}] [{}] Closed consumer", topic, subscription);
                setState(State.Closed);
                batchMessageAckTracker.clear();
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
                    CompletableFuture<Message> receiveFuture = pendingReceives.poll();
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

	void messageReceived(MessageIdData messageId, ByteBuf headersAndPayload, ClientCnx cnx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received message: {}/{}", topic, subscription, messageId.getLedgerId(),
                    messageId.getEntryId());
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

        ByteBuf uncompressedPayload = uncompressPayloadIfNeeded(messageId, msgMetadata, payload, cnx);
        if (uncompressedPayload == null) {
            // Message was discarded on decompression error
            return;
        }

        final int numMessages = msgMetadata.getNumMessagesInBatch();

        if (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch()) {
            final MessageImpl message = new MessageImpl(messageId, msgMetadata, uncompressedPayload,
                getPartitionIndex(), cnx);
            uncompressedPayload.release();
            msgMetadata.recycle();

            try {
                lock.readLock().lock();
                // Enqueue the message so that it can be retrieved when application calls receive()
                // if the conf.getReceiverQueueSize() is 0 then discard message if no one is waiting for it.
                // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
                unAckedMessageTracker.add((MessageIdImpl) message.getMessageId());
                boolean asyncReceivedWaiting = !pendingReceives.isEmpty();
                if ((conf.getReceiverQueueSize() != 0 || waitingOnReceiveForZeroQueueSize) && !asyncReceivedWaiting) {
                    incomingMessages.add(message);
                }
                if (asyncReceivedWaiting) {
                    notifyPendingReceivedCallback(message, null);
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

        if (listener != null) {
            // Trigger the notification on the message listener in a separate thread to avoid blocking the networking
            // thread while the message processing happens
            listenerExecutor.execute(() -> {
                for (int i = 0; i < numMessages; i++) {
                    try {
                        Message msg = internalReceive(0, TimeUnit.MILLISECONDS);
                        // complete the callback-loop in case queue is cleared up
                        if (msg == null) {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] [{}] Message has been cleared from the queue", topic, subscription);
                            }
                            break;
                        }
                        try {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}][{}] Calling message listener for message {}", topic, subscription, msg.getMessageId());
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
    void notifyPendingReceivedCallback(final MessageImpl message, Exception exception) {
        if (!pendingReceives.isEmpty()) {
            // fetch receivedCallback from queue
            CompletableFuture<Message> receivedFuture = pendingReceives.poll();
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

    void receiveIndividualMessagesFromBatch(MessageMetadata msgMetadata, ByteBuf uncompressedPayload,
                                            MessageIdData messageId, ClientCnx cnx) {
        int batchSize = msgMetadata.getNumMessagesInBatch();

        // create ack tracker for entry aka batch
        BitSet bitSet = new BitSet(batchSize);
        MessageIdImpl batchMessage = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
            getPartitionIndex());
        bitSet.set(0, batchSize);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] added bit set for message {}, cardinality {}, length {}", subscription, consumerName,
                batchMessage, bitSet.cardinality(), bitSet.length());
        }
        batchMessageAckTracker.put(batchMessage, bitSet);
        unAckedMessageTracker.add(batchMessage);
        try {
            for (int i = 0; i < batchSize; ++i) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] processing message num - {} in batch", subscription, consumerName, i);
                }
                PulsarApi.SingleMessageMetadata.Builder singleMessageMetadataBuilder = PulsarApi.SingleMessageMetadata
                    .newBuilder();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                    singleMessageMetadataBuilder, i, batchSize);
                BatchMessageIdImpl batchMessageIdImpl = new BatchMessageIdImpl(messageId.getLedgerId(),
                    messageId.getEntryId(), getPartitionIndex(), i);
                final MessageImpl message = new MessageImpl(batchMessageIdImpl, msgMetadata,
                    singleMessageMetadataBuilder.build(), singleMessagePayload, cnx);
                lock.readLock().lock();
                if (pendingReceives.isEmpty()) {
                    incomingMessages.add(message);
                } else {
                    notifyPendingReceivedCallback(message, null);
                }
                lock.readLock().unlock();
                singleMessagePayload.release();
                singleMessageMetadataBuilder.recycle();
            }
        } catch (IOException e) {
            //
            log.warn("[{}] [{}] unable to obtain message in batch", subscription, consumerName);
            batchMessageAckTracker.remove(batchMessage);
            discardCorruptedMessage(messageId, cnx, ValidationError.BatchDeSerializeError);
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] enqueued messages in batch. queue size - {}, available queue size - {}", subscription,
                consumerName, incomingMessages.size(), incomingMessages.remainingCapacity());
        }
    }

    /**
     * Record the event that one message has been processed by the application.
     *
     * Periodically, it sends a Flow command to notify the broker that it can push more messages
     */
    protected synchronized void messageProcessed(Message msg) {
        ClientCnx currentCnx = cnx();
        ClientCnx msgCnx = ((MessageImpl) msg).getCnx();
        lastDequeuedMessage = (MessageIdImpl) msg.getMessageId();

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
            unAckedMessageTracker.add(id);
        }
    }

    private void increaseAvailablePermits(ClientCnx currentCnx) {
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

    private ByteBuf uncompressPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
                                              ClientCnx currentCnx) {
        CompressionType compressionType = msgMetadata.getCompression();
        CompressionCodec codec = codecProvider.getCodec(compressionType);
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

        if(hasChecksum(headersAndPayload)) {
            int checksum = readChecksum(headersAndPayload).intValue();
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
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), AckType.Individual,
            validationError);
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
                batchMessageAckTracker.clear();
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
            log.warn("[{}] Client Connection needs to be establised for redelivery of unacknowledged messages", this);
        } else {
            log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
            cnx.ctx().close();
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageIdImpl> messageIds) {
        if (conf.getSubscriptionType() != SubscriptionType.Shared) {
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        ClientCnx cnx = cnx();
        if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getNumber()) {
            int messagesFromQueue = removeExpiredMessagesFromQueue(messageIds);
            Iterable<List<MessageIdImpl>> batches = Iterables.partition(messageIds, MAX_REDELIVER_UNACKNOWLEDGED);
            MessageIdData.Builder builder = MessageIdData.newBuilder();
            batches.forEach(ids -> {
                List<MessageIdData> messageIdDatas = ids.stream()
                    .map(messageId -> {
                        // attempt to remove message from batchMessageAckTracker
                        batchMessageAckTracker.remove(messageId);
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
            log.warn("[{}] Client Connection needs to be establised for redelivery of unacknowledged messages", this);
        } else {
            log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
            cnx.ctx().close();
        }
    }

    private MessageIdImpl getMessageIdImpl(Message msg) {
        MessageIdImpl messageId = (MessageIdImpl) msg.getMessageId();
        if (messageId instanceof BatchMessageIdImpl) {
            // messageIds contain MessageIdImpl, not BatchMessageIdImpl
            messageId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                getPartitionIndex());
        }
        return messageId;
    }

    private int removeExpiredMessagesFromQueue(Set<MessageIdImpl> messageIds) {
        int messagesFromQueue = 0;
        Message peek = incomingMessages.peek();
        if (peek != null) {
            MessageIdImpl messageId = getMessageIdImpl(peek);
            if (!messageIds.contains(messageId)) {
                // first message is not expired, then no message is expired in queue.
                return 0;
            }

            // try not to remove elements that are added while we remove
            Message message = incomingMessages.poll();
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
        if (stats instanceof ConsumerStatsDisabled) {
            return null;
        }
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

    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

}
