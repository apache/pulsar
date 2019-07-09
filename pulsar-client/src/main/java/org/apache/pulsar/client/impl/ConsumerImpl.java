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
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
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
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
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

    protected volatile MessageId lastDequeuedMessage = MessageId.earliest;
    private volatile MessageId lastMessageIdInBroker = MessageId.earliest;

    private long subscribeTimeout;
    private final int partitionIndex;
    private final boolean hasParentConsumer;

    private final int receiverQueueRefillThreshold;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final UnAckedMessageTracker unAckedMessageTracker;
    private final AcknowledgmentsGroupingTracker acknowledgmentsGroupingTracker;
    private final NegativeAcksTracker negativeAcksTracker;

    protected final ConsumerStatsRecorder stats;
    private final int priorityLevel;
    private final SubscriptionMode subscriptionMode;
    private volatile BatchMessageIdImpl startMessageId;

    private volatile boolean hasReachedEndOfTopic;

    private final MessageCrypto msgCrypto;

    private final Map<String, String> metadata;

    private final boolean readCompacted;
    private final boolean resetIncludeHead;

    private final SubscriptionInitialPosition subscriptionInitialPosition;
    private final ConnectionHandler connectionHandler;

    private final TopicName topicName;
    private final String topicNameWithoutPartition;

    private final Map<MessageIdImpl, List<MessageImpl<T>>> possibleSendToDeadLetterTopicMessages;

    private final DeadLetterPolicy deadLetterPolicy;

    private Producer<T> deadLetterProducer;

    private final long backoffIntervalNanos;
    private final long maxBackoffIntervalNanos;

    protected volatile boolean paused;

    enum SubscriptionMode {
        // Make the subscription to be backed by a durable cursor that will retain messages and persist the current
        // position
        Durable,

        // Lightweight subscription mode that doesn't have a durable cursor associated
        NonDurable
    }

    static <T> ConsumerImpl<T> newConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                 ExecutorService listenerExecutor, int partitionIndex, boolean hasParentConsumer, CompletableFuture<Consumer<T>> subscribeFuture,
                 SubscriptionMode subscriptionMode, MessageId startMessageId, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        return ConsumerImpl.newConsumerImpl(client, topic, conf, listenerExecutor, partitionIndex, hasParentConsumer, subscribeFuture, subscriptionMode,
                                            startMessageId, schema, interceptors, Backoff.DEFAULT_INTERVAL_IN_NANOSECONDS, Backoff.MAX_BACKOFF_INTERVAL_NANOSECONDS);
    }

    static <T> ConsumerImpl<T> newConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
            ExecutorService listenerExecutor, int partitionIndex, boolean hasParentConsumer, CompletableFuture<Consumer<T>> subscribeFuture,
            SubscriptionMode subscriptionMode, MessageId startMessageId, Schema<T> schema, ConsumerInterceptors<T> interceptors,
            long backoffIntervalNanos, long maxBackoffIntervalNanos) {
    	if (conf.getReceiverQueueSize() == 0) {
            return new ZeroQueueConsumerImpl<>(client, topic, conf, listenerExecutor, partitionIndex, hasParentConsumer, subscribeFuture,
                    subscriptionMode, startMessageId, schema, interceptors, backoffIntervalNanos, maxBackoffIntervalNanos);
        } else {
            return new ConsumerImpl<>(client, topic, conf, listenerExecutor, partitionIndex, hasParentConsumer, subscribeFuture,
                    subscriptionMode, startMessageId, schema, interceptors, backoffIntervalNanos, maxBackoffIntervalNanos);
        }
    }

    protected ConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                 ExecutorService listenerExecutor, int partitionIndex, boolean hasParentConsumer, CompletableFuture<Consumer<T>> subscribeFuture,
                 SubscriptionMode subscriptionMode, MessageId startMessageId, Schema<T> schema, ConsumerInterceptors<T> interceptors,
                 long backoffIntervalNanos, long maxBackoffIntervalNanos) {
        super(client, topic, conf, conf.getReceiverQueueSize(), listenerExecutor, subscribeFuture, schema, interceptors);
        this.consumerId = client.newConsumerId();
        this.subscriptionMode = subscriptionMode;
        this.startMessageId = startMessageId != null ? new BatchMessageIdImpl((MessageIdImpl) startMessageId) : null;
        AVAILABLE_PERMITS_UPDATER.set(this, 0);
        this.subscribeTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
        this.partitionIndex = partitionIndex;
        this.hasParentConsumer = hasParentConsumer;
        this.receiverQueueRefillThreshold = conf.getReceiverQueueSize() / 2;
        this.priorityLevel = conf.getPriorityLevel();
        this.readCompacted = conf.isReadCompacted();
        this.subscriptionInitialPosition = conf.getSubscriptionInitialPosition();
        this.negativeAcksTracker = new NegativeAcksTracker(this, conf);
        this.resetIncludeHead = conf.isResetIncludeHead();

        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ConsumerStatsRecorderImpl(client, conf, this);
        } else {
            stats = ConsumerStatsDisabled.INSTANCE;
        }

        if (conf.getAckTimeoutMillis() != 0) {
            if (conf.getTickDurationMillis() > 0) {
                this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf.getAckTimeoutMillis(),
                        Math.min(conf.getTickDurationMillis(), conf.getAckTimeoutMillis()));
            } else {
                this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf.getAckTimeoutMillis());
            }
        } else {
            this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
        }

        // Create msgCrypto if not created already
        if (conf.getCryptoKeyReader() != null) {
            this.msgCrypto = new MessageCrypto(String.format("[%s] [%s]", topic, subscription), false);
        } else {
            this.msgCrypto = null;
        }

        if (conf.getProperties().isEmpty()) {
            metadata = Collections.emptyMap();
        } else {
            metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
        }

        this.connectionHandler = new ConnectionHandler(this,
        		        new BackoffBuilder()
                           .setInitialTime(100, TimeUnit.MILLISECONDS)
                           .setMax(60, TimeUnit.SECONDS)
                           .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                           .useUserConfiguredIntervals(backoffIntervalNanos,
        	                                           maxBackoffIntervalNanos)
        	               .create(),
                        this);

        this.topicName = TopicName.get(topic);
        if (this.topicName.isPersistent()) {
            this.acknowledgmentsGroupingTracker =
                new PersistentAcknowledgmentsGroupingTracker(this, conf, client.eventLoopGroup());
        } else {
            this.acknowledgmentsGroupingTracker =
                NonPersistentAcknowledgmentGroupingTracker.of();
        }

        if (conf.getDeadLetterPolicy() != null) {
            possibleSendToDeadLetterTopicMessages = new ConcurrentHashMap<>();
            if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getDeadLetterTopic())) {
                this.deadLetterPolicy = DeadLetterPolicy.builder()
                        .maxRedeliverCount(conf.getDeadLetterPolicy().getMaxRedeliverCount())
                        .deadLetterTopic(conf.getDeadLetterPolicy().getDeadLetterTopic())
                        .build();
            } else {
                this.deadLetterPolicy = DeadLetterPolicy.builder()
                        .maxRedeliverCount(conf.getDeadLetterPolicy().getMaxRedeliverCount())
                        .deadLetterTopic(String.format("%s-%s-DLQ", topic, subscription))
                        .build();
            }
        } else {
            deadLetterPolicy = null;
            possibleSendToDeadLetterTopicMessages = null;
        }

        this.backoffIntervalNanos = backoffIntervalNanos;
        this.maxBackoffIntervalNanos = maxBackoffIntervalNanos;

        topicNameWithoutPartition = topicName.getPartitionedTopicName();

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
                if (possibleSendToDeadLetterTopicMessages != null) {
                    possibleSendToDeadLetterTopicMessages.clear();
                }
                client.cleanupConsumer(ConsumerImpl.this);
                log.info("[{}][{}] Successfully unsubscribed from topic", topic, subscription);
                setState(State.Closed);
                unsubscribeFuture.complete(null);
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed to unsubscribe: {}", topic, subscription, e.getCause().getMessage());
                setState(State.Ready);
                unsubscribeFuture.completeExceptionally(e.getCause());
                return null;
            });
        } else {
            unsubscribeFuture.completeExceptionally(new PulsarClientException("Not connected to broker"));
        }
        return unsubscribeFuture;
    }

    @Override
    protected Message<T> internalReceive() throws PulsarClientException {
        Message<T> message;
        try {
            message = incomingMessages.take();
            messageProcessed(message);
            return beforeConsume(message);
        } catch (InterruptedException e) {
            stats.incrementNumReceiveFailed();
            throw PulsarClientException.unwrap(e);
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

        if (message != null) {
            messageProcessed(message);
            result.complete(beforeConsume(message));
        }

        return result;
    }

    @Override
    protected Message<T> internalReceive(int timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> message;
        try {
            message = incomingMessages.poll(timeout, unit);
            if (message == null) {
                return null;
            }
            messageProcessed(message);
            return beforeConsume(message);
        } catch (InterruptedException e) {
            State state = getState();
            if (state != State.Closing && state != State.Closed) {
                stats.incrementNumReceiveFailed();
                throw PulsarClientException.unwrap(e);
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
            return true;
        } else {
            if (AckType.Cumulative == ackType
                && !batchMessageId.getAcker().isPrevBatchCumulativelyAcked()) {
                sendAcknowledge(batchMessageId.prevBatchMessageId(), AckType.Cumulative, properties);
                batchMessageId.getAcker().setPrevBatchCumulativelyAcked(true);
            } else {
                onAcknowledge(batchMessageId, null);
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
            PulsarClientException exception = new PulsarClientException("Consumer not ready. State: " + getState());
            if (AckType.Individual.equals(ackType)) {
                onAcknowledge(messageId, exception);
            } else if (AckType.Cumulative.equals(ackType)) {
                onAcknowledgeCumulative(messageId, exception);
            }
            return FutureUtil.failedFuture(exception);
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
            if (messageId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;

                stats.incrementNumAcksSent(batchMessageId.getBatchSize());
                unAckedMessageTracker.remove(new MessageIdImpl(batchMessageId.getLedgerId(),
                        batchMessageId.getEntryId(), batchMessageId.getPartitionIndex()));
                if (possibleSendToDeadLetterTopicMessages != null) {
                    possibleSendToDeadLetterTopicMessages.remove(new MessageIdImpl(batchMessageId.getLedgerId(),
                            batchMessageId.getEntryId(), batchMessageId.getPartitionIndex()));
                }
            } else {
                // increment counter by 1 for non-batch msg
                unAckedMessageTracker.remove(msgId);
                if (possibleSendToDeadLetterTopicMessages != null) {
                    possibleSendToDeadLetterTopicMessages.remove(msgId);
                }
                stats.incrementNumAcksSent(1);
            }
            onAcknowledge(messageId, null);
        } else if (ackType == AckType.Cumulative) {
            onAcknowledgeCumulative(messageId, null);
            stats.incrementNumAcksSent(unAckedMessageTracker.removeMessagesTill(msgId));
        }

        acknowledgmentsGroupingTracker.addAcknowledgment(msgId, ackType, properties);

        // Consumer acknowledgment operation immediately succeeds. In any case, if we're not able to send ack to broker,
        // the messages will be re-delivered
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        negativeAcksTracker.add(messageId);

        // Ensure the message is not redelivered for ack-timeout, since we did receive an "ack"
        unAckedMessageTracker.remove(messageId);
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
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.clear();
            }
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

        SchemaInfo si = schema.getSchemaInfo();
        if (si != null && (SchemaType.BYTES == si.getType() || SchemaType.NONE == si.getType())) {
            // don't set schema for Schema.BYTES
            si = null;
        }
        ByteBuf request = Commands.newSubscribe(topic, subscription, consumerId, requestId, getSubType(), priorityLevel,
                consumerName, isDurable, startMessageIdData, metadata, readCompacted,
                conf.isReplicateSubscriptionState(), InitialPosition.valueOf(subscriptionInitialPosition.getValue()),
                si);
        if (startMessageIdData != null) {
            startMessageIdData.recycle();
        }

        cnx.sendRequestWithId(request, requestId).thenRun(() -> {
            synchronized (ConsumerImpl.this) {
                if (changeToReadyState()) {
                    consumerIsReconnectedToBroker(cnx, currentSize);
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
            // command to receive messages.
            // For readers too (isDurable==false), the partition idx will be set though we have to
            // send available permits immediately after establishing the reader session
            if (!(firstTimeConnect && hasParentConsumer && isDurable) && conf.getReceiverQueueSize() != 0) {
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

    protected void consumerIsReconnectedToBroker(ClientCnx cnx, int currentQueueSize) {
        log.info("[{}][{}] Subscribed to topic on {} -- consumer: {}", topic, subscription,
                cnx.channel().remoteAddress(), consumerId);

        AVAILABLE_PERMITS_UPDATER.set(this, 0);
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
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.clear();
            }
            return CompletableFuture.completedFuture(null);
        }

        if (!isConnected()) {
            log.info("[{}] [{}] Closed Consumer (not connected)", topic, subscription);
            setState(State.Closed);
            unAckedMessageTracker.close();
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.clear();
            }
            client.cleanupConsumer(this);
            return CompletableFuture.completedFuture(null);
        }

        stats.getStatTimeout().ifPresent(Timeout::cancel);

        setState(State.Closing);

        acknowledgmentsGroupingTracker.close();

        long requestId = client.newRequestId();

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        ClientCnx cnx = cnx();
        if (null == cnx) {
            cleanupAtClose(closeFuture);
        } else {
            ByteBuf cmd = Commands.newCloseConsumer(consumerId, requestId);
            cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
                cnx.removeConsumer(consumerId);
                if (exception == null || !cnx.ctx().channel().isActive()) {
                    cleanupAtClose(closeFuture);
                } else {
                    closeFuture.completeExceptionally(exception);
                }
                return null;
            });
        }

        return closeFuture;
    }

    private void cleanupAtClose(CompletableFuture<Void> closeFuture) {
        log.info("[{}] [{}] Closed consumer", topic, subscription);
        setState(State.Closed);
        unAckedMessageTracker.close();
        if (possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.clear();
        }
        closeFuture.complete(null);
        client.cleanupConsumer(this);
        // fail all pending-receive futures to notify application
        failPendingReceive();
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

    void messageReceived(MessageIdData messageId, int redeliveryCount, ByteBuf headersAndPayload, ClientCnx cnx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received message: {}/{}", topic, subscription, messageId.getLedgerId(),
                    messageId.getEntryId());
        }

        if (!verifyChecksum(headersAndPayload, messageId)) {
            // discard message with checksum error
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        MessageMetadata msgMetadata;
        try {
            msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        } catch (Throwable t) {
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        final int numMessages = msgMetadata.getNumMessagesInBatch();

        MessageIdImpl msgId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), getPartitionIndex());
        if (acknowledgmentsGroupingTracker.isDuplicate(msgId)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Ignoring message as it was already being acked earlier by same consumer {}/{}",
                        topic, subscription, consumerName, msgId);
            }

            increaseAvailablePermits(cnx, numMessages);
            return;
        }

        ByteBuf decryptedPayload = decryptPayloadIfNeeded(messageId, msgMetadata, headersAndPayload, cnx);

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

        // if message is not decryptable then it can't be parsed as a batch-message. so, add EncyrptionCtx to message
        // and return undecrypted payload
        if (isMessageUndecryptable || (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch())) {

            if (isNonDurableAndSameEntryAndLedger(messageId) && isPriorEntryIndex(messageId.getEntryId())) {
                // We need to discard entries that were prior to startMessageId
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Ignoring message from before the startMessageId: {}", subscription,
                            consumerName, startMessageId);
                }

                uncompressedPayload.release();
                msgMetadata.recycle();
                return;
            }

            final MessageImpl<T> message = new MessageImpl<>(topicName.toString(), msgId, msgMetadata,
                    uncompressedPayload, createEncryptionContext(msgMetadata), cnx, schema, redeliveryCount);
            uncompressedPayload.release();
            msgMetadata.recycle();

            lock.readLock().lock();
            try {
                // Enqueue the message so that it can be retrieved when application calls receive()
                // if the conf.getReceiverQueueSize() is 0 then discard message if no one is waiting for it.
                // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
                if (deadLetterPolicy != null && possibleSendToDeadLetterTopicMessages != null && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
                    possibleSendToDeadLetterTopicMessages.put((MessageIdImpl)message.getMessageId(), Collections.singletonList(message));
                }
                if (!pendingReceives.isEmpty()) {
                    notifyPendingReceivedCallback(message, null);
                } else if (canEnqueueMessage(message)) {
                    incomingMessages.add(message);
                }
            } finally {
                lock.readLock().unlock();
            }
        } else {
            // handle batch message enqueuing; uncompressed payload has all messages in batch
            receiveIndividualMessagesFromBatch(msgMetadata, redeliveryCount, uncompressedPayload, messageId, cnx);

            uncompressedPayload.release();
            msgMetadata.recycle();
        }

        if (listener != null) {
            triggerListener(numMessages);
        }
    }

    protected void triggerListener(int numMessages) {
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

    protected boolean canEnqueueMessage(Message<T> message) {
        // Default behavior, can be overridden in subclasses
        return true;
    }

    /**
     * Notify waiting asyncReceive request with the received message
     *
     * @param message
     */
    void notifyPendingReceivedCallback(final Message<T> message, Exception exception) {
        if (pendingReceives.isEmpty()) {
            return;
        }

        // fetch receivedCallback from queue
        final CompletableFuture<Message<T>> receivedFuture = pendingReceives.poll();
        if (receivedFuture == null) {
            return;
        }

        if (exception != null) {
            listenerExecutor.execute(() -> receivedFuture.completeExceptionally(exception));
            return;
        }

        if (message == null) {
            IllegalStateException e = new IllegalStateException("received message can't be null");
            listenerExecutor.execute(() -> receivedFuture.completeExceptionally(e));
            return;
        }

        if (conf.getReceiverQueueSize() == 0) {
            // call interceptor and complete received callback
            interceptAndComplete(message, receivedFuture);
            return;
        }

        // increase permits for available message-queue
        messageProcessed(message);
        // call interceptor and complete received callback
        interceptAndComplete(message, receivedFuture);
    }

    private void interceptAndComplete(final Message<T> message, final CompletableFuture<Message<T>> receivedFuture) {
        // call proper interceptor
        final Message<T> interceptMessage = beforeConsume(message);
        // return message to receivedCallback
        listenerExecutor.execute(() -> receivedFuture.complete(interceptMessage));
    }

    void receiveIndividualMessagesFromBatch(MessageMetadata msgMetadata, int redeliveryCount, ByteBuf uncompressedPayload,
            MessageIdData messageId, ClientCnx cnx) {
        int batchSize = msgMetadata.getNumMessagesInBatch();

        // create ack tracker for entry aka batch
        MessageIdImpl batchMessage = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                getPartitionIndex());
        BatchMessageAcker acker = BatchMessageAcker.newAcker(batchSize);
        List<MessageImpl<T>> possibleToDeadLetter = null;
        if (deadLetterPolicy != null && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
            possibleToDeadLetter = new ArrayList<>();
        }
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

                if (isNonDurableAndSameEntryAndLedger(messageId) && isPriorBatchIndex(i)) {
                    // If we are receiving a batch message, we need to discard messages that were prior
                    // to the startMessageId
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Ignoring message from before the startMessageId: {}", subscription,
                                consumerName, startMessageId);
                    }
                    singleMessagePayload.release();
                    singleMessageMetadataBuilder.recycle();

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
                final MessageImpl<T> message = new MessageImpl<>(topicName.toString(), batchMessageIdImpl,
                        msgMetadata, singleMessageMetadataBuilder.build(), singleMessagePayload,
                        createEncryptionContext(msgMetadata), cnx, schema, redeliveryCount);
                if (possibleToDeadLetter != null) {
                    possibleToDeadLetter.add(message);
                }
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

        if (possibleToDeadLetter != null && possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.put(batchMessage, possibleToDeadLetter);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] enqueued messages in batch. queue size - {}, available queue size - {}", subscription,
                    consumerName, incomingMessages.size(), incomingMessages.remainingCapacity());
        }

        if (skippedMessages > 0) {
            increaseAvailablePermits(cnx, skippedMessages);
        }
    }

    private boolean isPriorEntryIndex(long idx) {
        return resetIncludeHead ? idx < startMessageId.getEntryId() : idx <= startMessageId.getEntryId();
    }

    private boolean isPriorBatchIndex(long idx) {
        return resetIncludeHead ? idx < startMessageId.getBatchIndex() : idx <= startMessageId.getBatchIndex();
    }

    private boolean isNonDurableAndSameEntryAndLedger(MessageIdData messageId) {
        return subscriptionMode == SubscriptionMode.NonDurable && startMessageId != null
                && messageId.getLedgerId() == startMessageId.getLedgerId()
                && messageId.getEntryId() == startMessageId.getEntryId();
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

        trackMessage(msg);
    }

    protected void trackMessage(Message<?> msg) {
        if (msg != null) {
            MessageId messageId = msg.getMessageId();
            if (conf.getAckTimeoutMillis() > 0 && messageId instanceof MessageIdImpl) {
                MessageIdImpl id = (MessageIdImpl)messageId;
                if (id instanceof BatchMessageIdImpl) {
                    // do not add each item in batch message into tracker
                    id = new MessageIdImpl(id.getLedgerId(), id.getEntryId(), getPartitionIndex());
                }
                if (hasParentConsumer) {
                    // we should no longer track this message, TopicsConsumer will take care from now onwards
                    unAckedMessageTracker.remove(id);
                } else {
                    unAckedMessageTracker.add(id);
                }
            }
        }
    }

    void increaseAvailablePermits(ClientCnx currentCnx) {
        increaseAvailablePermits(currentCnx, 1);
    }

    private void increaseAvailablePermits(ClientCnx currentCnx, int delta) {
        int available = AVAILABLE_PERMITS_UPDATER.addAndGet(this, delta);

        while (available >= receiverQueueRefillThreshold && !paused) {
            if (AVAILABLE_PERMITS_UPDATER.compareAndSet(this, available, 0)) {
                sendFlowPermitsToBroker(currentCnx, available);
                break;
            } else {
                available = AVAILABLE_PERMITS_UPDATER.get(this);
            }
        }
    }

    @Override
    public void pause() {
        paused = true;
    }

    @Override
    public void resume() {
        if (paused) {
            paused = false;
            increaseAvailablePermits(cnx(), 0);
        }
    }

    private ByteBuf decryptPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
            ClientCnx currentCnx) {

        if (msgMetadata.getEncryptionKeysCount() == 0) {
            return payload.retain();
        }

        // If KeyReader is not configured throw exception based on config param
        if (conf.getCryptoKeyReader() == null) {
            switch (conf.getCryptoFailureAction()) {
                case CONSUME:
                    log.warn("[{}][{}][{}] CryptoKeyReader interface is not implemented. Consuming encrypted message.",
                            topic, subscription, consumerName);
                    return payload.retain();
                case DISCARD:
                    log.warn(
                            "[{}][{}][{}] Skipping decryption since CryptoKeyReader interface is not implemented and config is set to discard",
                            topic, subscription, consumerName);
                    discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
                    return null;
                case FAIL:
                    MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
                    log.error(
                            "[{}][{}][{}][{}] Message delivery failed since CryptoKeyReader interface is not implemented to consume encrypted message",
                             topic, subscription, consumerName, m);
                    unAckedMessageTracker.add(m);
                    return null;
            }
        }

        ByteBuf decryptedData = this.msgCrypto.decrypt(msgMetadata, payload, conf.getCryptoKeyReader());
        if (decryptedData != null) {
            return decryptedData;
        }

        switch (conf.getCryptoFailureAction()) {
            case CONSUME:
                // Note, batch message will fail to consume even if config is set to consume
                log.warn("[{}][{}][{}][{}] Decryption failed. Consuming encrypted message since config is set to consume.",
                        topic, subscription, consumerName, messageId);
                return payload.retain();
            case DISCARD:
                log.warn("[{}][{}][{}][{}] Discarding message since decryption failed and config is set to discard", topic,
                        subscription, consumerName, messageId);
                discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
                return null;
            case FAIL:
                MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
                log.error(
                        "[{}][{}][{}][{}] Message delivery failed since unable to decrypt incoming message",
                         topic, subscription, consumerName, m);
                unAckedMessageTracker.add(m);
                return null;
        }
        return null;
    }

    private ByteBuf uncompressPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
            ClientCnx currentCnx) {
        CompressionType compressionType = msgMetadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (payloadSize > ClientCnx.getMaxMessageSize()) {
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
        if (messageIds.isEmpty()) {
            return;
        }

        checkArgument(messageIds.stream().findFirst().get() instanceof MessageIdImpl);

        if (conf.getSubscriptionType() != SubscriptionType.Shared
                && conf.getSubscriptionType() != SubscriptionType.Key_Shared) {
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
                List<MessageIdData> messageIdDatas = ids.stream()
                    .filter(messageId -> !processPossibleToDLQ(messageId))
                    .map(messageId -> {
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

    private boolean processPossibleToDLQ(MessageIdImpl messageId) {
        List<MessageImpl<T>> deadLetterMessages = null;
        if (possibleSendToDeadLetterTopicMessages != null) {
            if (messageId instanceof BatchMessageIdImpl) {
                deadLetterMessages = possibleSendToDeadLetterTopicMessages.get(new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                        getPartitionIndex()));
            } else {
                deadLetterMessages = possibleSendToDeadLetterTopicMessages.get(messageId);
            }
        }
        if (deadLetterMessages != null) {
            if (deadLetterProducer == null) {
                try {
                    deadLetterProducer = client.newProducer(schema)
                            .topic(this.deadLetterPolicy.getDeadLetterTopic())
                            .blockIfQueueFull(false)
                            .create();
                } catch (Exception e) {
                    log.error("Create dead letter producer exception with topic: {}", deadLetterPolicy.getDeadLetterTopic(), e);
                }
            }
            if (deadLetterProducer != null) {
                try {
                    for (MessageImpl<T> message : deadLetterMessages) {
                        deadLetterProducer.newMessage()
                                .value(message.getValue())
                                .properties(message.getProperties())
                                .send();
                    }
                    acknowledge(messageId);
                    return true;
                } catch (Exception e) {
                    log.error("Send to dead letter topic exception with topic: {}, messageId: {}", deadLetterProducer.getTopic(), messageId, e);
                }
            }
        }
        return false;
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        try {
            seekAsync(messageId).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void seek(long timestamp) throws PulsarClientException {
        try {
            seekAsync(timestamp).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
        }

        if (!isConnected()) {
            return FutureUtil.failedFuture(new PulsarClientException("Not connected to broker"));
        }

        final CompletableFuture<Void> seekFuture = new CompletableFuture<>();

        long requestId = client.newRequestId();
        ByteBuf seek = Commands.newSeek(consumerId, requestId, timestamp);
        ClientCnx cnx = cnx();

        log.info("[{}][{}] Seek subscription to publish time {}", topic, subscription, timestamp);

        cnx.sendRequestWithId(seek, requestId).thenRun(() -> {
            log.info("[{}][{}] Successfully reset subscription to publish time {}", topic, subscription, timestamp);
            acknowledgmentsGroupingTracker.flushAndClean();
            lastDequeuedMessage = MessageId.earliest;
            incomingMessages.clear();
            seekFuture.complete(null);
        }).exceptionally(e -> {
            log.error("[{}][{}] Failed to reset subscription: {}", topic, subscription, e.getCause().getMessage());
            seekFuture.completeExceptionally(e.getCause());
            return null;
        });
        return seekFuture;
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
            acknowledgmentsGroupingTracker.flushAndClean();
            lastDequeuedMessage = messageId;
            incomingMessages.clear();
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
            if (hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessage)) {
                return true;
            }

            return hasMessageAvailableAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        final CompletableFuture<Boolean> booleanFuture = new CompletableFuture<>();

        if (hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessage)) {
            booleanFuture.complete(true);
        } else {
            getLastMessageIdAsync().thenAccept(messageId -> {
                lastMessageIdInBroker = messageId;
                if (hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessage)) {
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

    private boolean hasMoreMessages(MessageId lastMessageIdInBroker, MessageId lastDequeuedMessage) {
        if (lastMessageIdInBroker.compareTo(lastDequeuedMessage) > 0 &&
                ((MessageIdImpl)lastMessageIdInBroker).getEntryId() != -1) {
            return true;
        } else {
            // Make sure batching message can be read completely.
            return lastMessageIdInBroker.compareTo(lastDequeuedMessage) == 0
                && incomingMessages.size() > 0;
        }
    }

    CompletableFuture<MessageId> getLastMessageIdAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
        }

        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMax(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                .useUserConfiguredIntervals(backoffIntervalNanos,
                                            maxBackoffIntervalNanos)
                .create();

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
            encryptionCtx
                    .setCompressionType(CompressionCodecProvider.convertFromWireProtocol(msgMetadata.getCompression()));
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

    public String getTopicNameWithoutPartition() {
        return topicNameWithoutPartition;
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

}
