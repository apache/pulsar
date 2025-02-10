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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.serializeWithSize;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandAck.ValidationError;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.BackoffBuilder;
import org.apache.pulsar.common.util.CompletableFutureCancellationHandler;
import org.apache.pulsar.common.util.ExceptionHandler;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentBitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
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

    protected volatile MessageId lastDequeuedMessageId = MessageId.earliest;
    private volatile MessageId lastMessageIdInBroker = MessageId.earliest;

    private final long lookupDeadline;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ConsumerImpl> SUBSCRIBE_DEADLINE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ConsumerImpl.class, "subscribeDeadline");
    @SuppressWarnings("unused")
    private volatile long subscribeDeadline = 0; // gets set on first successful connection

    private final int partitionIndex;
    private final boolean hasParentConsumer;
    private final boolean parentConsumerHasListener;

    private final AcknowledgmentsGroupingTracker acknowledgmentsGroupingTracker;
    private final NegativeAcksTracker negativeAcksTracker;

    protected final ConsumerStatsRecorder stats;
    @Getter(AccessLevel.PACKAGE)
    private final int priorityLevel;
    private final SubscriptionMode subscriptionMode;
    private volatile MessageIdAdv startMessageId;

    private volatile MessageIdAdv seekMessageId;
    @VisibleForTesting
    final AtomicReference<SeekStatus> seekStatus;
    private volatile CompletableFuture<Void> seekFuture;

    private final MessageIdAdv initialStartMessageId;

    private final long startMessageRollbackDurationInSec;

    private volatile boolean hasReachedEndOfTopic;

    private final MessageCrypto msgCrypto;

    private final Map<String, String> metadata;

    private final boolean readCompacted;
    private final boolean resetIncludeHead;

    private final SubscriptionInitialPosition subscriptionInitialPosition;
    private final ConnectionHandler connectionHandler;

    private final TopicName topicName;
    private final String topicNameWithoutPartition;

    private final Map<MessageIdAdv, List<MessageImpl<T>>> possibleSendToDeadLetterTopicMessages;

    private final DeadLetterPolicy deadLetterPolicy;

    private volatile CompletableFuture<Producer<byte[]>> deadLetterProducer;
    private volatile int deadLetterProducerFailureCount;
    private volatile CompletableFuture<Producer<byte[]>> retryLetterProducer;
    private volatile int retryLetterProducerFailureCount;
    private final ReadWriteLock createProducerLock = new ReentrantReadWriteLock();

    protected volatile boolean paused;

    protected ConcurrentOpenHashMap<String, ChunkedMessageCtx> chunkedMessagesMap =
            ConcurrentOpenHashMap.<String, ChunkedMessageCtx>newBuilder().build();
    private int pendingChunkedMessageCount = 0;
    protected long expireTimeOfIncompleteChunkedMessageMillis = 0;
    private final AtomicBoolean expireChunkMessageTaskScheduled = new AtomicBoolean(false);
    private final int maxPendingChunkedMessage;
    // if queue size is reasonable (most of the time equal to number of producers try to publish messages concurrently
    // on the topic) then it guards against broken chunked message which was not fully published
    private final boolean autoAckOldestChunkedMessageOnQueueFull;
    // it will be used to manage N outstanding chunked message buffers
    private final BlockingQueue<String> pendingChunkedMessageUuidQueue;

    private final boolean createTopicIfDoesNotExist;
    private final boolean poolMessages;

    private final AtomicReference<ClientCnx> clientCnxUsedForConsumerRegistration = new AtomicReference<>();
    private final List<Throwable> previousExceptions = new CopyOnWriteArrayList<Throwable>();
    private volatile boolean hasSoughtByTimestamp = false;

    static <T> ConsumerImpl<T> newConsumerImpl(PulsarClientImpl client,
                                               String topic,
                                               ConsumerConfigurationData<T> conf,
                                               ExecutorProvider executorProvider,
                                               int partitionIndex,
                                               boolean hasParentConsumer,
                                               CompletableFuture<Consumer<T>> subscribeFuture,
                                               MessageId startMessageId,
                                               Schema<T> schema,
                                               ConsumerInterceptors<T> interceptors,
                                               boolean createTopicIfDoesNotExist) {
        return newConsumerImpl(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer, false,
                subscribeFuture, startMessageId, schema, interceptors, createTopicIfDoesNotExist, 0);
    }

    static <T> ConsumerImpl<T> newConsumerImpl(PulsarClientImpl client,
                                               String topic,
                                               ConsumerConfigurationData<T> conf,
                                               ExecutorProvider executorProvider,
                                               int partitionIndex,
                                               boolean hasParentConsumer,
                                               boolean parentConsumerHasListener,
                                               CompletableFuture<Consumer<T>> subscribeFuture,
                                               MessageId startMessageId,
                                               Schema<T> schema,
                                               ConsumerInterceptors<T> interceptors,
                                               boolean createTopicIfDoesNotExist,
                                               long startMessageRollbackDurationInSec) {
        if (conf.getReceiverQueueSize() == 0) {
            return new ZeroQueueConsumerImpl<>(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer,
                    subscribeFuture,
                    startMessageId, schema, interceptors,
                    createTopicIfDoesNotExist);
        } else {
            return new ConsumerImpl<>(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer,
                    parentConsumerHasListener,
                    subscribeFuture, startMessageId,
                    startMessageRollbackDurationInSec /* rollback time in sec to start msgId */,
                    schema, interceptors, createTopicIfDoesNotExist);
        }
    }

    protected ConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                           ExecutorProvider executorProvider, int partitionIndex, boolean hasParentConsumer,
                           boolean parentConsumerHasListener, CompletableFuture<Consumer<T>> subscribeFuture,
                           MessageId startMessageId,
                           long startMessageRollbackDurationInSec, Schema<T> schema,
                           ConsumerInterceptors<T> interceptors,
                           boolean createTopicIfDoesNotExist) {
        super(client, topic, conf, conf.getReceiverQueueSize(), executorProvider, subscribeFuture, schema,
                interceptors);
        this.consumerId = client.newConsumerId();
        this.subscriptionMode = conf.getSubscriptionMode();
        if (startMessageId != null) {
            MessageIdAdv firstChunkMessageId = ((MessageIdAdv) startMessageId).getFirstChunkMessageId();
            if (conf.isResetIncludeHead() && firstChunkMessageId != null) {
                // The chunk message id's ledger id and entry id are the last chunk's ledger id and entry id, when
                // startMessageIdInclusive() is enabled, we need to start from the first chunk's message id
                this.startMessageId = firstChunkMessageId;
            } else {
                this.startMessageId = (MessageIdAdv) startMessageId;
            }
        }
        this.initialStartMessageId = this.startMessageId;
        this.startMessageRollbackDurationInSec = startMessageRollbackDurationInSec;
        AVAILABLE_PERMITS_UPDATER.set(this, 0);
        this.lookupDeadline = System.currentTimeMillis() + client.getConfiguration().getLookupTimeoutMs();
        this.partitionIndex = partitionIndex;
        this.hasParentConsumer = hasParentConsumer;
        this.parentConsumerHasListener = parentConsumerHasListener;
        this.priorityLevel = conf.getMatchingTopicConfiguration(topic).getPriorityLevel();
        this.readCompacted = conf.isReadCompacted();
        this.subscriptionInitialPosition = conf.getSubscriptionInitialPosition();
        this.negativeAcksTracker = new NegativeAcksTracker(this, conf);
        this.resetIncludeHead = conf.isResetIncludeHead();
        this.createTopicIfDoesNotExist = createTopicIfDoesNotExist;
        this.maxPendingChunkedMessage = conf.getMaxPendingChunkedMessage();
        this.pendingChunkedMessageUuidQueue = new GrowableArrayBlockingQueue<>();
        this.expireTimeOfIncompleteChunkedMessageMillis = conf.getExpireTimeOfIncompleteChunkedMessageMillis();
        this.autoAckOldestChunkedMessageOnQueueFull = conf.isAutoAckOldestChunkedMessageOnQueueFull();
        this.poolMessages = conf.isPoolMessages();
        this.paused = conf.isStartPaused();

        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ConsumerStatsRecorderImpl(client, conf, this);
        } else {
            stats = ConsumerStatsDisabled.INSTANCE;
        }

        seekStatus = new AtomicReference<>(SeekStatus.NOT_STARTED);

        // Create msgCrypto if not created already
        if (conf.getCryptoKeyReader() != null) {
            if (conf.getMessageCrypto() != null) {
                this.msgCrypto = conf.getMessageCrypto();
            } else {
                // default to use MessageCryptoBc;
                MessageCrypto msgCryptoBc;
                try {
                    msgCryptoBc = new MessageCryptoBc(
                            String.format("[%s] [%s]", topic, subscription),
                            false);
                } catch (Exception e) {
                    log.error("MessageCryptoBc may not included in the jar. e:", e);
                    msgCryptoBc = null;
                }
                this.msgCrypto = msgCryptoBc;
            }
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
                        .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(),
                                TimeUnit.NANOSECONDS)
                        .setMax(client.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                        .setMandatoryStop(0, TimeUnit.MILLISECONDS)
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
                        .deadLetterTopic(String.format("%s-%s" + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX,
                                topic, subscription))
                        .build();
            }

            if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getRetryLetterTopic())) {
                this.deadLetterPolicy.setRetryLetterTopic(conf.getDeadLetterPolicy().getRetryLetterTopic());
            } else {
                this.deadLetterPolicy.setRetryLetterTopic(String.format(
                        "%s-%s" + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX,
                        topic, subscription));
            }

            if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getInitialSubscriptionName())) {
                this.deadLetterPolicy.setInitialSubscriptionName(
                        conf.getDeadLetterPolicy().getInitialSubscriptionName());
            }

        } else {
            deadLetterPolicy = null;
            possibleSendToDeadLetterTopicMessages = null;
        }

        topicNameWithoutPartition = topicName.getPartitionedTopicName();

        grabCnx();
    }

    public ConnectionHandler getConnectionHandler() {
        return connectionHandler;
    }

    @Override
    public UnAckedMessageTracker getUnAckedMessageTracker() {
        return unAckedMessageTracker;
    }

    @VisibleForTesting
    NegativeAcksTracker getNegativeAcksTracker() {
        return negativeAcksTracker;
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
                closeConsumerTasks();
                deregisterFromClientCnx();
                client.cleanupConsumer(this);
                log.info("[{}][{}] Successfully unsubscribed from topic", topic, subscription);
                setState(State.Closed);
                unsubscribeFuture.complete(null);
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed to unsubscribe: {}", topic, subscription, e.getCause().getMessage());
                setState(State.Ready);
                unsubscribeFuture.completeExceptionally(
                        PulsarClientException.wrap(e.getCause(),
                                String.format("Failed to unsubscribe the subscription %s of topic %s",
                                        subscription, topicName.toString())));
                return null;
            });
        } else {
            unsubscribeFuture.completeExceptionally(
                    new PulsarClientException(
                            String.format("The client is not connected to the broker when unsubscribing the "
                                    + "subscription %s of the topic %s", subscription, topicName.toString())));
        }
        return unsubscribeFuture;
    }

    @Override
    public int minReceiverQueueSize() {
        int size = Math.min(INITIAL_RECEIVER_QUEUE_SIZE, maxReceiverQueueSize);
        if (batchReceivePolicy.getMaxNumMessages() > 0) {
            // consumerImpl may store (half-1) permits locally.
            size = Math.max(size, 2 * batchReceivePolicy.getMaxNumMessages() - 2);
        }
        return size;
    }

    @Override
    protected Message<T> internalReceive() throws PulsarClientException {
        Message<T> message;
        try {
            if (incomingMessages.isEmpty()) {
                expectMoreIncomingMessages();
            }
            message = incomingMessages.take();
            messageProcessed(message);
            return beforeConsume(message);
        } catch (InterruptedException e) {
            ExceptionHandler.handleInterruptedException(e);
            stats.incrementNumReceiveFailed();
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    protected CompletableFuture<Message<T>> internalReceiveAsync() {
        CompletableFutureCancellationHandler cancellationHandler = new CompletableFutureCancellationHandler();
        CompletableFuture<Message<T>> result = cancellationHandler.createFuture();
        internalPinnedExecutor.execute(() -> {
            Message<T> message = incomingMessages.poll();
            if (message == null) {
                expectMoreIncomingMessages();
                pendingReceives.add(result);
                cancellationHandler.setCancelAction(() -> pendingReceives.remove(result));
            } else {
                messageProcessed(message);
                result.complete(beforeConsume(message));
            }
        });

        return result;
    }

    @Override
    protected Message<T> internalReceive(long timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> message;
        try {
            if (incomingMessages.isEmpty()) {
                expectMoreIncomingMessages();
            }
            message = incomingMessages.poll(timeout, unit);
            if (message == null) {
                return null;
            }
            messageProcessed(message);
            message = listener == null ? beforeConsume(message) : message;
            return message;
        } catch (InterruptedException e) {
            ExceptionHandler.handleInterruptedException(e);
            State state = getState();
            if (state != State.Closing && state != State.Closed) {
                stats.incrementNumReceiveFailed();
                throw PulsarClientException.unwrap(e);
            } else {
                return null;
            }
        }
    }

    @Override
    protected Messages<T> internalBatchReceive() throws PulsarClientException {
        try {
            return internalBatchReceiveAsync().get();
        } catch (InterruptedException | ExecutionException e) {
            ExceptionHandler.handleInterruptedException(e);
            State state = getState();
            if (state != State.Closing && state != State.Closed) {
                stats.incrementNumBatchReceiveFailed();
                throw PulsarClientException.unwrap(e);
            } else {
                return null;
            }
        }
    }

    @Override
    protected CompletableFuture<Messages<T>> internalBatchReceiveAsync() {
        CompletableFutureCancellationHandler cancellationHandler = new CompletableFutureCancellationHandler();
        CompletableFuture<Messages<T>> result = cancellationHandler.createFuture();
        internalPinnedExecutor.execute(() -> {
            if (hasEnoughMessagesForBatchReceive()) {
                notifyPendingBatchReceivedCallBack(result);
            } else {
                expectMoreIncomingMessages();
                OpBatchReceive<T> opBatchReceive = OpBatchReceive.of(result);
                pendingBatchReceives.add(opBatchReceive);
                triggerBatchReceiveTimeoutTask();
                cancellationHandler.setCancelAction(() -> pendingBatchReceives.remove(opBatchReceive));
            }
        });
        return result;
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String, Long> properties,
                                                    TransactionImpl txn) {
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

        if (txn != null) {
            return doTransactionAcknowledgeForResponse(messageId, ackType, null, properties,
                    new TxnID(txn.getTxnIdMostBits(), txn.getTxnIdLeastBits()));
        }
        return acknowledgmentsGroupingTracker.addAcknowledgment(messageId, ackType, properties);
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(List<MessageId> messageIdList, AckType ackType,
                                                    Map<String, Long> properties, TransactionImpl txn) {
        if (getState() != State.Ready && getState() != State.Connecting) {
            stats.incrementNumAcksFailed();
            PulsarClientException exception = new PulsarClientException("Consumer not ready. State: " + getState());
            if (AckType.Individual.equals(ackType)) {
                onAcknowledge(messageIdList, exception);
            } else if (AckType.Cumulative.equals(ackType)) {
                onAcknowledgeCumulative(messageIdList, exception);
            }
            return FutureUtil.failedFuture(exception);
        }
        if (txn != null) {
            return doTransactionAcknowledgeForResponse(messageIdList, ackType,
                    properties, new TxnID(txn.getTxnIdMostBits(), txn.getTxnIdLeastBits()));
        } else {
            return this.acknowledgmentsGroupingTracker.addListAcknowledgment(messageIdList, ackType, properties);
        }
    }

    private static void copyMessageKeysIfNeeded(Message<?> message, TypedMessageBuilder<?> typedMessageBuilderNew) {
        if (message.hasKey()) {
            if (message.hasBase64EncodedKey()) {
                typedMessageBuilderNew.keyBytes(message.getKeyBytes());
            } else {
                typedMessageBuilderNew.key(message.getKey());
            }
        }
        if (message.hasOrderingKey()) {
            typedMessageBuilderNew.orderingKey(message.getOrderingKey());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected CompletableFuture<Void> doReconsumeLater(Message<?> message, AckType ackType,
                                                       Map<String, String> customProperties,
                                                       long delayTime,
                                                       TimeUnit unit) {

        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .InvalidMessageException("Cannot handle message with null messageId"));
        }

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

        CompletableFuture<Void> result = new CompletableFuture<>();
        if (initRetryLetterProducerIfNeeded() != null) {
            try {
                MessageImpl<T> retryMessage = (MessageImpl<T>) getMessageImpl(message);
                String originMessageIdStr = message.getMessageId().toString();
                String originTopicNameStr = getOriginTopicNameStr(message);
                SortedMap<String, String> propertiesMap =
                        getPropertiesMap(message, originMessageIdStr, originTopicNameStr);
                if (customProperties != null) {
                    propertiesMap.putAll(customProperties);
                }
                int reconsumeTimes = 1;
                if (propertiesMap.containsKey(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES)) {
                    reconsumeTimes = Integer.parseInt(
                            propertiesMap.get(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES));
                    reconsumeTimes = reconsumeTimes + 1;
                }
                propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES, String.valueOf(reconsumeTimes));
                propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_DELAY_TIME,
                        String.valueOf(unit.toMillis(delayTime < 0 ? 0 : delayTime)));

                MessageId finalMessageId = messageId;
                if (reconsumeTimes > this.deadLetterPolicy.getMaxRedeliverCount()
                        && StringUtils.isNotBlank(deadLetterPolicy.getDeadLetterTopic())) {
                    initDeadLetterProducerIfNeeded().thenAcceptAsync(dlqProducer -> {
                        try {
                            TypedMessageBuilder<byte[]> typedMessageBuilderNew =
                                    dlqProducer.newMessage(
                                                    Schema.AUTO_PRODUCE_BYTES(retryMessage.getReaderSchema().get()))
                                            .value(retryMessage.getData())
                                            .properties(propertiesMap);
                            copyMessageKeysIfNeeded(message, typedMessageBuilderNew);
                            typedMessageBuilderNew.sendAsync().thenAccept(msgId -> {
                                doAcknowledge(finalMessageId, ackType, Collections.emptyMap(), null).thenAccept(v -> {
                                    result.complete(null);
                                }).exceptionally(ex -> {
                                    result.completeExceptionally(ex);
                                    return null;
                                });
                            }).exceptionally(ex -> {
                                result.completeExceptionally(ex);
                                return null;
                            });
                        } catch (Exception e) {
                            result.completeExceptionally(e);
                        }
                    }, internalPinnedExecutor).exceptionally(ex -> {
                        result.completeExceptionally(ex);
                        return null;
                    });
                } else {
                    assert retryMessage != null;
                    initRetryLetterProducerIfNeeded().thenAcceptAsync(rtlProducer -> {
                        try {
                            TypedMessageBuilder<byte[]> typedMessageBuilderNew = rtlProducer
                                    .newMessage(Schema.AUTO_PRODUCE_BYTES(message.getReaderSchema().get()))
                                    .value(retryMessage.getData())
                                    .properties(propertiesMap);
                            if (delayTime > 0) {
                                typedMessageBuilderNew.deliverAfter(delayTime, unit);
                            }
                            copyMessageKeysIfNeeded(message, typedMessageBuilderNew);
                            typedMessageBuilderNew.sendAsync()
                                    .thenCompose(
                                            __ -> doAcknowledge(finalMessageId, ackType, Collections.emptyMap(), null))
                                    .thenAccept(v -> {
                                        result.complete(null);
                                    })
                                    .exceptionally(ex -> {
                                        result.completeExceptionally(ex);
                                        return null;
                                    });
                        } catch (Exception e) {
                            result.completeExceptionally(e);
                        }
                    }, internalPinnedExecutor).exceptionally(ex -> {
                        result.completeExceptionally(ex);
                        return null;
                    });
                }
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        } else {
            result.completeExceptionally(new PulsarClientException("Retry letter producer is null."));
        }
        MessageId finalMessageId = messageId;
        result.exceptionally(ex -> {
            log.error("Send to retry letter topic exception with topic: {}, messageId: {}",
                    this.deadLetterPolicy.getRetryLetterTopic(), finalMessageId, ex);
            Set<MessageId> messageIds = Collections.singleton(finalMessageId);
            unAckedMessageTracker.remove(finalMessageId);
            redeliverUnacknowledgedMessages(messageIds);
            return null;
        });
        return result;
    }

    private SortedMap<String, String> getPropertiesMap(Message<?> message,
                                                       String originMessageIdStr,
                                                       String originTopicNameStr) {
        SortedMap<String, String> propertiesMap = new TreeMap<>();
        if (message.getProperties() != null) {
            propertiesMap.putAll(message.getProperties());
        }
        propertiesMap.putIfAbsent(RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC, originTopicNameStr);
        //Compatible with the old version, will be deleted in the future
        propertiesMap.putIfAbsent(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID, originMessageIdStr);
        propertiesMap.putIfAbsent(RetryMessageUtil.PROPERTY_ORIGIN_MESSAGE_ID, originMessageIdStr);
        return propertiesMap;
    }

    private String getOriginTopicNameStr(Message<?> message) {
        MessageId messageId = message.getMessageId();
        if (messageId instanceof TopicMessageId) {
            String topic = ((TopicMessageId) messageId).getOwnerTopic();
            int index = topic.lastIndexOf(TopicName.PARTITIONED_TOPIC_SUFFIX);
            if (index < 0) {
                return topic;
            } else {
                return topic.substring(0, index);
            }
        } else {
            return message.getTopicName();
        }
    }

    private MessageImpl<?> getMessageImpl(Message<?> message) {
        if (message instanceof TopicMessageImpl) {
            return (MessageImpl<?>) ((TopicMessageImpl<?>) message).getMessage();
        } else if (message instanceof MessageImpl) {
            return (MessageImpl<?>) message;
        }
        return null;
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        negativeAcksTracker.add(messageId);

        // Ensure the message is not redelivered for ack-timeout, since we did receive an "ack"
        unAckedMessageTracker.remove(MessageIdAdvUtils.discardBatch(messageId));
    }

    @Override
    public void negativeAcknowledge(Message<?> message) {
        negativeAcksTracker.add(message);

        // Ensure the message is not redelivered for ack-timeout, since we did receive an "ack"
        unAckedMessageTracker.remove(MessageIdAdvUtils.discardBatch(message.getMessageId()));
    }

    @Override
    public CompletableFuture<Void> connectionOpened(final ClientCnx cnx) {
        previousExceptions.clear();
        getConnectionHandler().setMaxMessageSize(cnx.getMaxMessageSize());

        final State state = getState();
        if (state == State.Closing || state == State.Closed) {
            setState(State.Closed);
            closeConsumerTasks();
            deregisterFromClientCnx();
            client.cleanupConsumer(this);
            clearReceiverQueue(false);
            return CompletableFuture.completedFuture(null);
        }

        log.info("[{}][{}] Subscribing to topic on cnx {}, consumerId {}",
                topic, subscription, cnx.ctx().channel(), consumerId);

        long requestId = client.newRequestId();
        if (seekStatus.get() != SeekStatus.NOT_STARTED) {
            acknowledgmentsGroupingTracker.flushAndClean();
        }

        SUBSCRIBE_DEADLINE_UPDATER
                .compareAndSet(this, 0L, System.currentTimeMillis()
                        + client.getConfiguration().getOperationTimeoutMs());

        int currentSize;
        synchronized (this) {
            currentSize = incomingMessages.size();
            setClientCnx(cnx);
            clearReceiverQueue(true);
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.clear();
            }
        }

        boolean isDurable = subscriptionMode == SubscriptionMode.Durable;
        final MessageIdData startMessageIdData;

        // For regular durable subscriptions, the message id from where to restart will be determined by the broker.
        // For non-durable we are going to restart from the next entry.
        if (!isDurable && startMessageId != null) {
            startMessageIdData = new MessageIdData()
                    .setLedgerId(startMessageId.getLedgerId())
                    .setEntryId(startMessageId.getEntryId())
                    .setBatchIndex(startMessageId.getBatchIndex());
        } else {
            startMessageIdData = null;
        }

        SchemaInfo si = schema.getSchemaInfo();
        if (si != null && (SchemaType.BYTES == si.getType() || SchemaType.NONE == si.getType())) {
            // don't set schema for Schema.BYTES
            si = null;
        } else {
            if (schema instanceof AutoConsumeSchema
                    && Commands.peerSupportsCarryAutoConsumeSchemaToBroker(cnx.getRemoteEndpointProtocolVersion())) {
                si = AutoConsumeSchema.SCHEMA_INFO;
            }
        }
        // startMessageRollbackDurationInSec should be consider only once when consumer connects to first time
        long startMessageRollbackDuration = (startMessageRollbackDurationInSec > 0
                && startMessageId != null
                && startMessageId.equals(initialStartMessageId)) ? startMessageRollbackDurationInSec : 0;

        // synchronized this, because redeliverUnAckMessage eliminate the epoch inconsistency between them
        final CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (this) {
            ByteBuf request = Commands.newSubscribe(topic, subscription, consumerId, requestId, getSubType(),
                    priorityLevel, consumerName, isDurable, startMessageIdData, metadata, readCompacted,
                    conf.getReplicateSubscriptionState(),
                    InitialPosition.valueOf(subscriptionInitialPosition.getValue()),
                    startMessageRollbackDuration, si, createTopicIfDoesNotExist, conf.getKeySharedPolicy(),
                    // Use the current epoch to subscribe.
                    conf.getSubscriptionProperties(), CONSUMER_EPOCH.get(this));

            cnx.sendRequestWithId(request, requestId).thenRun(() -> {
                synchronized (ConsumerImpl.this) {
                    if (changeToReadyState()) {
                        consumerIsReconnectedToBroker(cnx, currentSize);
                    } else {
                        // Consumer was closed while reconnecting, close the connection to make sure the broker
                        // drops the consumer on its side
                        setState(State.Closed);
                        deregisterFromClientCnx();
                        client.cleanupConsumer(this);
                        cnx.channel().close();
                        future.complete(null);
                        return;
                    }
                }

                resetBackoff();

                boolean firstTimeConnect = subscribeFuture.complete(this);
                // if the consumer is not partitioned or is re-connected and is partitioned, we send the flow
                // command to receive messages.
                if (!(firstTimeConnect && hasParentConsumer) && getCurrentReceiverQueueSize() != 0) {
                    increaseAvailablePermits(cnx, getCurrentReceiverQueueSize());
                }
                future.complete(null);
            }).exceptionally((e) -> {
                deregisterFromClientCnx();
                if (getState() == State.Closing || getState() == State.Closed) {
                    // Consumer was closed while reconnecting, close the connection to make sure the broker
                    // drops the consumer on its side
                    cnx.channel().close();
                    future.complete(null);
                    return null;
                }
                log.warn("[{}][{}] Failed to subscribe to topic on {}", topic,
                        subscription, cnx.channel().remoteAddress());

                if (e.getCause() instanceof PulsarClientException.TimeoutException) {
                    // Creating the consumer has timed out. We need to ensure the broker closes the consumer
                    // in case it was indeed created, otherwise it might prevent new create consumer operation,
                    // since we are not necessarily closing the connection.
                    long closeRequestId = client.newRequestId();
                    ByteBuf cmd = Commands.newCloseConsumer(consumerId, closeRequestId);
                    cnx.sendRequestWithId(cmd, closeRequestId);
                }

                if (e.getCause() instanceof PulsarClientException
                        && PulsarClientException.isRetriableError(e.getCause())
                        && System.currentTimeMillis() < SUBSCRIBE_DEADLINE_UPDATER.get(ConsumerImpl.this)) {
                    future.completeExceptionally(e.getCause());
                } else if (!subscribeFuture.isDone()) {
                    // unable to create new consumer, fail operation
                    setState(State.Failed);
                    closeConsumerTasks();
                    subscribeFuture.completeExceptionally(
                            PulsarClientException.wrap(e, String.format("Failed to subscribe the topic %s "
                                            + "with subscription name %s when connecting to the broker",
                                    topicName.toString(), subscription)));
                    client.cleanupConsumer(this);
                } else if (e.getCause() instanceof TopicDoesNotExistException) {
                    // The topic was deleted after the consumer was created, and we're
                    // not allowed to recreate the topic. This can happen in few cases:
                    //  * Regex consumer getting error after topic gets deleted
                    //  * Regular consumer after topic is manually delete and with
                    //    auto-topic-creation set to false
                    // No more retries are needed in this case.
                    setState(State.Failed);
                    closeConsumerTasks();
                    client.cleanupConsumer(this);
                    log.warn("[{}][{}] Closed consumer because topic does not exist anymore {}",
                            topic, subscription, cnx.channel().remoteAddress());
                } else {
                    // consumer was subscribed and connected but we got some error, keep trying
                    future.completeExceptionally(e.getCause());
                }

                if (!future.isDone()) {
                    future.complete(null);
                }
                return null;
            });
        }
        return future;
    }

    protected void consumerIsReconnectedToBroker(ClientCnx cnx, int currentQueueSize) {
        log.info("[{}][{}] Subscribed to topic on {} -- consumer: {}", topic, subscription,
                cnx.channel().remoteAddress(), consumerId);

        AVAILABLE_PERMITS_UPDATER.set(this, 0);
    }

    /**
     * Clear the internal receiver queue and returns the message id of what was the 1st message in the queue that was
     * not seen by the application.
     */
    private void clearReceiverQueue(boolean updateStartMessageId) {
        List<Message<?>> currentMessageQueue = new ArrayList<>(incomingMessages.size());
        incomingMessages.drainTo(currentMessageQueue);
        resetIncomingMessageSize();

        CompletableFuture<Void> seekFuture = this.seekFuture;
        MessageIdAdv seekMessageId = this.seekMessageId;

        if (seekStatus.get() != SeekStatus.NOT_STARTED) {
            if (updateStartMessageId) {
                startMessageId = seekMessageId;
            }
            if (seekStatus.compareAndSet(SeekStatus.COMPLETED, SeekStatus.NOT_STARTED)) {
                internalPinnedExecutor.execute(() -> seekFuture.complete(null));
            }
            return;
        } else if (subscriptionMode == SubscriptionMode.Durable) {
            return;
        }

        if (!currentMessageQueue.isEmpty()) {
            MessageIdAdv nextMessageInQueue = (MessageIdAdv) currentMessageQueue.get(0).getMessageId();
            MessageIdAdv previousMessage;
            if (MessageIdAdvUtils.isBatch(nextMessageInQueue)) {
                // Get on the previous message within the current batch
                previousMessage = new BatchMessageIdImpl(nextMessageInQueue.getLedgerId(),
                        nextMessageInQueue.getEntryId(), nextMessageInQueue.getPartitionIndex(),
                        nextMessageInQueue.getBatchIndex() - 1);
            } else {
                // Get on previous message in previous entry
                previousMessage = MessageIdAdvUtils.prevMessageId(nextMessageInQueue);
            }
            // release messages if they are pooled messages
            currentMessageQueue.forEach(Message::release);
            if (updateStartMessageId) {
                startMessageId = previousMessage;
            }
        } else if (updateStartMessageId && !lastDequeuedMessageId.equals(MessageId.earliest)) {
            // If the queue was empty we need to restart from the message just after the last one that has been dequeued
            // in the past
            startMessageId = new BatchMessageIdImpl((MessageIdImpl) lastDequeuedMessageId);
        } // else: No message was received or dequeued by this consumer. Next message would still be the startMessageId
    }

    /**
     * send the flow command to have the broker start pushing messages.
     */
    private void sendFlowPermitsToBroker(ClientCnx cnx, int numMessages) {
        if (cnx != null && numMessages > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Adding {} additional permits", topic, subscription, numMessages);
            }
            if (log.isDebugEnabled()) {
                cnx.ctx().writeAndFlush(Commands.newFlow(consumerId, numMessages))
                        .addListener(writeFuture -> {
                            if (!writeFuture.isSuccess()) {
                                log.debug("Consumer {} failed to send {} permits to broker: {}",
                                        consumerId, numMessages, writeFuture.cause().getMessage());
                            } else {
                                log.debug("Consumer {} sent {} permits to broker", consumerId, numMessages);
                            }
                        });
            } else {
                cnx.ctx().writeAndFlush(Commands.newFlow(consumerId, numMessages), cnx.ctx().voidPromise());
            }
        }
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        boolean timeout = System.currentTimeMillis() > lookupDeadline;
        if (nonRetriableError || timeout) {
            exception.setPreviousExceptions(previousExceptions);
            if (subscribeFuture.completeExceptionally(exception)) {
                setState(State.Failed);
                if (nonRetriableError) {
                    log.info("[{}] Consumer creation failed for consumer {} with unretriableError {}",
                            topic, consumerId, exception.getMessage());
                } else {
                    log.info("[{}] Consumer creation failed for consumer {} after timeout", topic, consumerId);
                }
                closeConsumerTasks();
                deregisterFromClientCnx();
                client.cleanupConsumer(this);
            }
        } else {
            previousExceptions.add(exception);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        ArrayList<CompletableFuture<Void>> closeFutures = new ArrayList<>(4);
        closeFutures.add(closeFuture);
        if (retryLetterProducer != null) {
            closeFutures.add(retryLetterProducer.thenCompose(p -> p.closeAsync()).whenComplete((ignore, ex) -> {
                if (ex != null) {
                    log.warn("Exception ignored in closing retryLetterProducer of consumer", ex);
                }
            }));
        }
        if (deadLetterProducer != null) {
            closeFutures.add(deadLetterProducer.thenCompose(p -> p.closeAsync()).whenComplete((ignore, ex) -> {
                if (ex != null) {
                    log.warn("Exception ignored in closing deadLetterProducer of consumer", ex);
                }
            }));
        }
        CompletableFuture<Void> compositeCloseFuture = FutureUtil.waitForAll(closeFutures);


        if (getState() == State.Closing || getState() == State.Closed) {
            closeConsumerTasks();
            failPendingReceive().whenComplete((r, t) -> closeFuture.complete(null));
            return compositeCloseFuture;
        }

        if (!isConnected()) {
            log.info("[{}] [{}] Closed Consumer (not connected)", topic, subscription);
            setState(State.Closed);
            closeConsumerTasks();
            deregisterFromClientCnx();
            client.cleanupConsumer(this);
            failPendingReceive().whenComplete((r, t) -> closeFuture.complete(null));
            return compositeCloseFuture;
        }

        stats.getStatTimeout().ifPresent(Timeout::cancel);

        setState(State.Closing);

        closeConsumerTasks();

        long requestId = client.newRequestId();

        ClientCnx cnx = cnx();
        if (null == cnx) {
            cleanupAtClose(closeFuture, null);
        } else {
            ByteBuf cmd = Commands.newCloseConsumer(consumerId, requestId);
            cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
                final ChannelHandlerContext ctx = cnx.ctx();
                boolean ignoreException = ctx == null || !ctx.channel().isActive();
                if (ignoreException && exception != null) {
                    log.debug("Exception ignored in closing consumer", exception);
                }
                cleanupAtClose(closeFuture, ignoreException ? null : exception);
                return null;
            });
        }

        return compositeCloseFuture;
    }

    private void cleanupAtClose(CompletableFuture<Void> closeFuture, Throwable exception) {
        log.info("[{}] [{}] Closed consumer", topic, subscription);
        setState(State.Closed);
        closeConsumerTasks();
        deregisterFromClientCnx();
        client.cleanupConsumer(this);

        // fail all pending-receive futures to notify application
        failPendingReceive().whenComplete((r, t) -> {
            if (exception != null) {
                closeFuture.completeExceptionally(exception);
            } else {
                closeFuture.complete(null);
            }
        });
    }

    private void closeConsumerTasks() {
        unAckedMessageTracker.close();
        if (possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.clear();
        }
        acknowledgmentsGroupingTracker.close();
        if (batchReceiveTimeout != null) {
            batchReceiveTimeout.cancel();
        }
        negativeAcksTracker.close();
        stats.getStatTimeout().ifPresent(Timeout::cancel);
        if (poolMessages) {
            releasePooledMessagesAndStopAcceptNew();
        }
    }

    /**
     * If enabled pooled messages, we should release the messages after closing consumer and stop accept the new
     * messages.
     */
    private void releasePooledMessagesAndStopAcceptNew() {
        incomingMessages.terminate(message -> message.release());
        clearIncomingMessages();
    }

    void activeConsumerChanged(boolean isActive) {
        if (consumerEventListener == null) {
            return;
        }

        externalPinnedExecutor.execute(() -> {
            if (isActive) {
                consumerEventListener.becameActive(this, partitionIndex);
            } else {
                consumerEventListener.becameInactive(this, partitionIndex);
            }
        });
    }

    protected boolean isBatch(MessageMetadata messageMetadata) {
        // if message is not decryptable then it can't be parsed as a batch-message. so, add EncyrptionCtx to message
        // and return undecrypted payload
        return !isMessageUndecryptable(messageMetadata)
                && (messageMetadata.hasNumMessagesInBatch() || messageMetadata.getNumMessagesInBatch() != 1);
    }

    protected <V> MessageImpl<V> newSingleMessage(final int index,
                                                  final int numMessages,
                                                  final BrokerEntryMetadata brokerEntryMetadata,
                                                  final MessageMetadata msgMetadata,
                                                  final SingleMessageMetadata singleMessageMetadata,
                                                  final ByteBuf payload,
                                                  final MessageIdImpl messageId,
                                                  final Schema<V> schema,
                                                  final boolean containMetadata,
                                                  final BitSetRecyclable ackBitSet,
                                                  final BitSet ackSetInMessageId,
                                                  final int redeliveryCount,
                                                  final long consumerEpoch) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] processing message num - {} in batch", subscription, consumerName, index);
        }

        ByteBuf singleMessagePayload = null;
        try {
            if (containMetadata) {
                singleMessagePayload =
                        Commands.deSerializeSingleMessageInBatch(payload, singleMessageMetadata, index, numMessages);
            }

            // If the topic is non-persistent, we should not ignore any messages.
            if (this.topicName.isPersistent() && isSameEntry(messageId) && isPriorBatchIndex(index)) {
                // If we are receiving a batch message, we need to discard messages that were prior
                // to the startMessageId
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Ignoring message from before the startMessageId: {}", subscription,
                            consumerName, startMessageId);
                }
                return null;
            }

            if (singleMessageMetadata != null && singleMessageMetadata.isCompactedOut()) {
                // message has been compacted out, so don't send to the user
                return null;
            }

            if (isSingleMessageAcked(ackBitSet, index)) {
                return null;
            }

            BatchMessageIdImpl batchMessageIdImpl = new BatchMessageIdImpl(messageId.getLedgerId(),
                    messageId.getEntryId(), getPartitionIndex(), index, numMessages, ackSetInMessageId);

            final ByteBuf payloadBuffer = (singleMessagePayload != null) ? singleMessagePayload : payload;
            final MessageImpl<V> message = MessageImpl.create(topicName.toString(), batchMessageIdImpl,
                    msgMetadata, singleMessageMetadata, payloadBuffer,
                    createEncryptionContext(msgMetadata), cnx(), schema, redeliveryCount, poolMessages, consumerEpoch);
            message.setBrokerEntryMetadata(brokerEntryMetadata);
            return message;
        } catch (IOException | IllegalStateException e) {
            throw new IllegalStateException(e);
        } finally {
            if (singleMessagePayload != null) {
                singleMessagePayload.release();
            }
        }
    }

    protected <V> MessageImpl<V> newMessage(final MessageIdImpl messageId,
                                            final BrokerEntryMetadata brokerEntryMetadata,
                                            final MessageMetadata messageMetadata,
                                            final ByteBuf payload,
                                            final Schema<V> schema,
                                            final int redeliveryCount,
                                            final long consumerEpoch) {
        final MessageImpl<V> message = MessageImpl.create(topicName.toString(), messageId, messageMetadata, payload,
                createEncryptionContext(messageMetadata), cnx(), schema, redeliveryCount, poolMessages, consumerEpoch);
        message.setBrokerEntryMetadata(brokerEntryMetadata);
        return message;
    }

    private void executeNotifyCallback(final MessageImpl<T> message) {
        // Enqueue the message so that it can be retrieved when application calls receive()
        // if the conf.getReceiverQueueSize() is 0 then discard message if no one is waiting for it.
        // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
        internalPinnedExecutor.execute(() -> {
            if (!isValidConsumerEpoch(message)) {
                increaseAvailablePermits(cnx());
                return;
            }
            if (hasNextPendingReceive()) {
                notifyPendingReceivedCallback(message, null);
            } else if (enqueueMessageAndCheckBatchReceive(message) && hasPendingBatchReceive()) {
                notifyPendingBatchReceivedCallBack();
            }
        });
    }

    private void processPayloadByProcessor(final BrokerEntryMetadata brokerEntryMetadata,
                                           final MessageMetadata messageMetadata,
                                           final ByteBuf byteBuf,
                                           final MessageIdImpl messageId,
                                           final Schema<T> schema,
                                           final int redeliveryCount,
                                           final List<Long> ackSet,
                                           long consumerEpoch) {
        final MessagePayloadImpl payload = MessagePayloadImpl.create(byteBuf);
        final MessagePayloadContextImpl entryContext = MessagePayloadContextImpl.get(
                brokerEntryMetadata, messageMetadata, messageId, this, redeliveryCount, ackSet, consumerEpoch);
        final AtomicInteger skippedMessages = new AtomicInteger(0);
        try {
            conf.getPayloadProcessor().process(payload, entryContext, schema, message -> {
                if (message != null) {
                    executeNotifyCallback((MessageImpl<T>) message);
                } else {
                    skippedMessages.incrementAndGet();
                }
            });
        } catch (Throwable throwable) {
            log.warn("[{}] [{}] unable to obtain message in batch", subscription, consumerName, throwable);
            discardCorruptedMessage(messageId, cnx(), ValidationError.BatchDeSerializeError);
        } finally {
            entryContext.recycle();
            payload.release(); // byteBuf.release() is called in this method
        }

        if (skippedMessages.get() > 0) {
            increaseAvailablePermits(cnx(), skippedMessages.get());
        }

        tryTriggerListener();
    }

    void messageReceived(CommandMessage cmdMessage, ByteBuf headersAndPayload, ClientCnx cnx) {
        List<Long> ackSet = Collections.emptyList();
        if (cmdMessage.getAckSetsCount() > 0) {
            ackSet = new ArrayList<>(cmdMessage.getAckSetsCount());
            for (int i = 0; i < cmdMessage.getAckSetsCount(); i++) {
                ackSet.add(cmdMessage.getAckSetAt(i));
            }
        }
        int redeliveryCount = cmdMessage.getRedeliveryCount();
        MessageIdData messageId = cmdMessage.getMessageId();
        long consumerEpoch = DEFAULT_CONSUMER_EPOCH;
        // if broker send messages to client with consumerEpoch, we should set consumerEpoch to message
        if (cmdMessage.hasConsumerEpoch()) {
            consumerEpoch = cmdMessage.getConsumerEpoch();
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received message: {}/{}", topic, subscription, messageId.getLedgerId(),
                    messageId.getEntryId());
        }

        if (!verifyChecksum(headersAndPayload, messageId)) {
            // discard message with checksum error
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        BrokerEntryMetadata brokerEntryMetadata;
        MessageMetadata msgMetadata;
        try {
            brokerEntryMetadata = Commands.parseBrokerEntryMetadataIfExist(headersAndPayload);
            msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        } catch (Throwable t) {
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        final int numMessages = msgMetadata.getNumMessagesInBatch();
        final int numChunks = msgMetadata.hasNumChunksFromMsg() ? msgMetadata.getNumChunksFromMsg() : 0;
        final boolean isChunkedMessage = numChunks > 1;
        MessageIdImpl msgId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), getPartitionIndex());
        if (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch()
                && acknowledgmentsGroupingTracker.isDuplicate(msgId)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Ignoring message as it was already being acked earlier by same consumer {}/{}",
                        topic, subscription, consumerName, msgId);
            }

            increaseAvailablePermits(cnx, numMessages);
            return;
        }

        ByteBuf decryptedPayload = decryptPayloadIfNeeded(messageId, redeliveryCount, msgMetadata, headersAndPayload,
                cnx);

        boolean isMessageUndecryptable = isMessageUndecryptable(msgMetadata);

        if (decryptedPayload == null) {
            // Message was discarded or CryptoKeyReader isn't implemented
            return;
        }

        // uncompress decryptedPayload and release decryptedPayload-ByteBuf
        ByteBuf uncompressedPayload = (isMessageUndecryptable || isChunkedMessage) ? decryptedPayload.retain()
                : uncompressPayloadIfNeeded(messageId, msgMetadata, decryptedPayload, cnx, true);
        decryptedPayload.release();
        if (uncompressedPayload == null) {
            // Message was discarded on decompression error
            return;
        }

        if (conf.getPayloadProcessor() != null) {
            // uncompressedPayload is released in this method so we don't need to call release() again
            processPayloadByProcessor(brokerEntryMetadata, msgMetadata,
                    uncompressedPayload, msgId, schema, redeliveryCount, ackSet, consumerEpoch);
            return;
        }

        // if message is not decryptable then it can't be parsed as a batch-message. so, add EncyrptionCtx to message
        // and return undecrypted payload
        if (isMessageUndecryptable || (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch())) {

            // right now, chunked messages are only supported by non-shared subscription
            if (isChunkedMessage) {
                uncompressedPayload = processMessageChunk(uncompressedPayload, msgMetadata, msgId, messageId, cnx);
                if (uncompressedPayload == null) {
                    return;
                }

                // last chunk received: so, stitch chunked-messages and clear up chunkedMsgBuffer
                if (log.isDebugEnabled()) {
                    log.debug("Chunked message completed chunkId {}, total-chunks {}, msgId {} sequenceId {}",
                            msgMetadata.getChunkId(), msgMetadata.getNumChunksFromMsg(), msgId,
                            msgMetadata.getSequenceId());
                }

                // remove buffer from the map, set the chunk message id
                ChunkedMessageCtx chunkedMsgCtx = chunkedMessagesMap.remove(msgMetadata.getUuid());
                if (chunkedMsgCtx.chunkedMessageIds.length > 0) {
                    msgId = new ChunkMessageIdImpl(chunkedMsgCtx.chunkedMessageIds[0],
                            chunkedMsgCtx.chunkedMessageIds[chunkedMsgCtx.chunkedMessageIds.length - 1]);
                }
                // add chunked messageId to unack-message tracker, and reduce pending-chunked-message count
                unAckedChunkedMessageIdSequenceMap.put(msgId, chunkedMsgCtx.chunkedMessageIds);
                pendingChunkedMessageCount--;
                chunkedMsgCtx.recycle();
            }

            // If the topic is non-persistent, we should not ignore any messages.
            if (this.topicName.isPersistent() && isSameEntry(msgId) && isPriorEntryIndex(messageId.getEntryId())) {
                // We need to discard entries that were prior to startMessageId
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Ignoring message from before the startMessageId: {}", subscription,
                            consumerName, startMessageId);
                }

                uncompressedPayload.release();
                return;
            }

            final MessageImpl<T> message =
                    newMessage(msgId, brokerEntryMetadata, msgMetadata, uncompressedPayload,
                            schema, redeliveryCount, consumerEpoch);
            uncompressedPayload.release();

            if (deadLetterPolicy != null && possibleSendToDeadLetterTopicMessages != null) {
                if (redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
                    possibleSendToDeadLetterTopicMessages.put((MessageIdImpl) message.getMessageId(),
                            Collections.singletonList(message));
                    if (redeliveryCount > deadLetterPolicy.getMaxRedeliverCount()) {
                        redeliverUnacknowledgedMessages(Collections.singleton(message.getMessageId()));
                        // The message is skipped due to reaching the max redelivery count,
                        // so we need to increase the available permits
                        increaseAvailablePermits(cnx);
                        return;
                    }
                }
            }
            executeNotifyCallback(message);
        } else {
            // handle batch message enqueuing; uncompressed payload has all messages in batch
            receiveIndividualMessagesFromBatch(brokerEntryMetadata, msgMetadata, redeliveryCount, ackSet,
                    uncompressedPayload, messageId, cnx, consumerEpoch);

            uncompressedPayload.release();
        }
        tryTriggerListener();

    }

    private ByteBuf processMessageChunk(ByteBuf compressedPayload, MessageMetadata msgMetadata, MessageIdImpl msgId,
            MessageIdData messageId, ClientCnx cnx) {
        if (msgMetadata.getChunkId() != (msgMetadata.getNumChunksFromMsg() - 1)) {
            increaseAvailablePermits(cnx);
        }
        // Lazy task scheduling to expire incomplete chunk message
        if (expireTimeOfIncompleteChunkedMessageMillis > 0 && expireChunkMessageTaskScheduled.compareAndSet(false,
                true)) {
            ((ScheduledExecutorService) client.getScheduledExecutorProvider().getExecutor()).scheduleAtFixedRate(
                    () -> internalPinnedExecutor
                            .execute(catchingAndLoggingThrowables(this::removeExpireIncompleteChunkedMessages)),
                    expireTimeOfIncompleteChunkedMessageMillis, expireTimeOfIncompleteChunkedMessageMillis,
                    TimeUnit.MILLISECONDS
            );
        }

        ChunkedMessageCtx chunkedMsgCtx = chunkedMessagesMap.get(msgMetadata.getUuid());

        if (msgMetadata.getChunkId() == 0) {
            if (chunkedMsgCtx != null) {
                // Handle ack hole case when receive duplicated chunks.
                // There are two situation that receives chunks with the same sequence ID and chunk ID.
                // Situation 1 - Message redeliver:
                // For example:
                //     Chunk-1 sequence ID: 0, chunk ID: 0, msgID: 1:1
                //     Chunk-2 sequence ID: 0, chunk ID: 1, msgID: 1:2
                //     Chunk-3 sequence ID: 0, chunk ID: 0, msgID: 1:1
                //     Chunk-4 sequence ID: 0, chunk ID: 1, msgID: 1:2
                //     Chunk-5 sequence ID: 0, chunk ID: 2, msgID: 1:3
                // In this case, chunk-3 and chunk-4 have the same msgID with chunk-1 and chunk-2.
                // This may be caused by message redeliver, we can't ack any chunk in this case here.
                // Situation 2 - Corrupted chunk message
                // For example:
                //     Chunk-1 sequence ID: 0, chunk ID: 0, msgID: 1:1
                //     Chunk-2 sequence ID: 0, chunk ID: 1, msgID: 1:2
                //     Chunk-3 sequence ID: 0, chunk ID: 0, msgID: 1:3
                //     Chunk-4 sequence ID: 0, chunk ID: 1, msgID: 1:4
                //     Chunk-5 sequence ID: 0, chunk ID: 2, msgID: 1:5
                // In this case, all the chunks with different msgIDs and are persistent in the topic.
                // But Chunk-1 and Chunk-2 belong to a corrupted chunk message that must be skipped since
                // they will not be delivered to end users. So we should ack them here to avoid ack hole.
                boolean isCorruptedChunkMessageDetected = Arrays.stream(chunkedMsgCtx.chunkedMessageIds)
                        .noneMatch(messageId1 -> messageId1 != null && messageId1.ledgerId == messageId.getLedgerId()
                                && messageId1.entryId == messageId.getEntryId());
                if (isCorruptedChunkMessageDetected) {
                    Arrays.stream(chunkedMsgCtx.chunkedMessageIds).forEach(messageId1 -> {
                        if (messageId1 != null) {
                            doAcknowledge(messageId1, AckType.Individual, Collections.emptyMap(), null);
                        }
                    });
                }
                // The first chunk of a new chunked-message received before receiving other chunks of previous
                // chunked-message
                // so, remove previous chunked-message from map and release buffer
                if (chunkedMsgCtx.chunkedMsgBuffer != null) {
                    ReferenceCountUtil.safeRelease(chunkedMsgCtx.chunkedMsgBuffer);
                }
                chunkedMsgCtx.recycle();
                chunkedMessagesMap.remove(msgMetadata.getUuid());
            }
            pendingChunkedMessageCount++;
            if (maxPendingChunkedMessage > 0 && pendingChunkedMessageCount > maxPendingChunkedMessage) {
                removeOldestPendingChunkedMessage();
            }
            int totalChunks = msgMetadata.getNumChunksFromMsg();
            ByteBuf chunkedMsgBuffer = PulsarByteBufAllocator.DEFAULT.buffer(msgMetadata.getTotalChunkMsgSize(),
                    msgMetadata.getTotalChunkMsgSize());
            chunkedMsgCtx = chunkedMessagesMap.computeIfAbsent(msgMetadata.getUuid(),
                    (key) -> ChunkedMessageCtx.get(totalChunks, chunkedMsgBuffer));
            pendingChunkedMessageUuidQueue.add(msgMetadata.getUuid());
        }

        // discard message if chunk is out-of-order
        if (chunkedMsgCtx == null || chunkedMsgCtx.chunkedMsgBuffer == null
                || msgMetadata.getChunkId() != (chunkedMsgCtx.lastChunkedMessageId + 1)) {
            // Filter and ack duplicated chunks instead of discard ctx.
            // For example:
            //     Chunk-1 sequence ID: 0, chunk ID: 0, msgID: 1:1
            //     Chunk-2 sequence ID: 0, chunk ID: 1, msgID: 1:2
            //     Chunk-3 sequence ID: 0, chunk ID: 2, msgID: 1:3
            //     Chunk-4 sequence ID: 0, chunk ID: 1, msgID: 1:4
            //     Chunk-5 sequence ID: 0, chunk ID: 2, msgID: 1:5
            //     Chunk-6 sequence ID: 0, chunk ID: 3, msgID: 1:6
            // We should filter and ack chunk-4 and chunk-5.
            if (chunkedMsgCtx != null && msgMetadata.getChunkId() <= chunkedMsgCtx.lastChunkedMessageId) {
                log.warn("[{}] Receive a duplicated chunk message with messageId [{}], last-chunk-Id [{}], "
                                + "chunkId [{}], sequenceId [{}]",
                        msgMetadata.getProducerName(), msgId, chunkedMsgCtx.lastChunkedMessageId,
                        msgMetadata.getChunkId(), msgMetadata.getSequenceId());
                compressedPayload.release();
                // Just like the above logic of receiving the first chunk again. We only ack this chunk in the message
                // duplication case.
                boolean isDuplicatedChunk = Arrays.stream(chunkedMsgCtx.chunkedMessageIds)
                        .noneMatch(messageId1 -> messageId1 != null && messageId1.ledgerId == messageId.getLedgerId()
                                && messageId1.entryId == messageId.getEntryId());
                if (isDuplicatedChunk) {
                    doAcknowledge(msgId, AckType.Individual, Collections.emptyMap(), null);
                }
                return null;
            }
            // means we lost the first chunk: should never happen
            log.info("[{}] [{}] Received unexpected chunk messageId {}, last-chunk-id = {}, chunkId = {}", topic,
                    subscription, msgId,
                    (chunkedMsgCtx != null ? chunkedMsgCtx.lastChunkedMessageId : null), msgMetadata.getChunkId());
            if (chunkedMsgCtx != null) {
                if (chunkedMsgCtx.chunkedMsgBuffer != null) {
                    ReferenceCountUtil.safeRelease(chunkedMsgCtx.chunkedMsgBuffer);
                }
                chunkedMsgCtx.recycle();
            }
            chunkedMessagesMap.remove(msgMetadata.getUuid());
            compressedPayload.release();
            if (expireTimeOfIncompleteChunkedMessageMillis > 0
                    && System.currentTimeMillis() > (msgMetadata.getPublishTime()
                            + expireTimeOfIncompleteChunkedMessageMillis)) {
                doAcknowledge(msgId, AckType.Individual, Collections.emptyMap(), null);
            } else {
                trackMessage(msgId);
            }
            return null;
        }

        chunkedMsgCtx.chunkedMessageIds[msgMetadata.getChunkId()] = msgId;
        // append the chunked payload and update lastChunkedMessage-id
        chunkedMsgCtx.chunkedMsgBuffer.writeBytes(compressedPayload);
        chunkedMsgCtx.lastChunkedMessageId = msgMetadata.getChunkId();

        // if final chunk is not received yet then release payload and return
        if (msgMetadata.getChunkId() != (msgMetadata.getNumChunksFromMsg() - 1)) {
            compressedPayload.release();
            return null;
        }

        compressedPayload.release();
        compressedPayload = chunkedMsgCtx.chunkedMsgBuffer;
        ByteBuf uncompressedPayload = uncompressPayloadIfNeeded(messageId, msgMetadata, compressedPayload, cnx, false);
        compressedPayload.release();
        return uncompressedPayload;
    }

    /**
     * Notify waiting asyncReceive request with the received message.
     *
     * @param message
     */
    void notifyPendingReceivedCallback(final Message<T> message, Exception exception) {
        if (pendingReceives.isEmpty()) {
            return;
        }

        // fetch receivedCallback from queue
        final CompletableFuture<Message<T>> receivedFuture = nextPendingReceive();
        if (receivedFuture == null) {
            return;
        }

        if (exception != null) {
            internalPinnedExecutor.execute(() -> receivedFuture.completeExceptionally(exception));
            return;
        }

        if (message == null) {
            IllegalStateException e = new IllegalStateException("received message can't be null");
            internalPinnedExecutor.execute(() -> receivedFuture.completeExceptionally(e));
            return;
        }

        if (getCurrentReceiverQueueSize() == 0) {
            // call interceptor and complete received callback
            trackMessage(message);
            interceptAndComplete(message, receivedFuture);
            return;
        }

        // increase incomingMessageSize here because the size would be decreased in messageProcessed() next step
        increaseIncomingMessageSize(message);
        // increase permits for available message-queue
        messageProcessed(message);
        // call interceptor and complete received callback
        interceptAndComplete(message, receivedFuture);
    }

    private void interceptAndComplete(final Message<T> message, final CompletableFuture<Message<T>> receivedFuture) {
        // call proper interceptor
        final Message<T> interceptMessage = beforeConsume(message);
        // return message to receivedCallback
        completePendingReceive(receivedFuture, interceptMessage);
    }

    void receiveIndividualMessagesFromBatch(BrokerEntryMetadata brokerEntryMetadata, MessageMetadata msgMetadata,
                                            int redeliveryCount, List<Long> ackSet, ByteBuf uncompressedPayload,
                                            MessageIdData messageId, ClientCnx cnx, long consumerEpoch) {
        int batchSize = msgMetadata.getNumMessagesInBatch();

        // create ack tracker for entry aka batch
        MessageIdImpl batchMessage = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                getPartitionIndex());
        List<MessageImpl<T>> possibleToDeadLetter = null;
        if (deadLetterPolicy != null && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
            possibleToDeadLetter = new ArrayList<>();
        }

        BitSet ackSetInMessageId = BatchMessageIdImpl.newAckSet(batchSize);
        BitSetRecyclable ackBitSet = null;
        if (ackSet != null && ackSet.size() > 0) {
            ackBitSet = BitSetRecyclable.valueOf(SafeCollectionUtils.longListToArray(ackSet));
        }

        SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        int skippedMessages = 0;
        try {
            for (int i = 0; i < batchSize; ++i) {
                final MessageImpl<T> message = newSingleMessage(i, batchSize, brokerEntryMetadata, msgMetadata,
                        singleMessageMetadata, uncompressedPayload, batchMessage, schema, true,
                        ackBitSet, ackSetInMessageId, redeliveryCount, consumerEpoch);
                if (message == null) {
                    // If it is not in ackBitSet, it means Broker does not want to deliver it to the client, and
                    // did not decrease the permits in the broker-side.
                    // So do not acquire more permits for this message.
                    // Why not skip this single message in the first line of for-loop block? We need call
                    // "newSingleMessage" to move "payload.readerIndex" to a correct value to get the correct data.
                    if (!isSingleMessageAcked(ackBitSet, i)) {
                        skippedMessages++;
                    }
                    continue;
                }
                if (possibleToDeadLetter != null) {
                    possibleToDeadLetter.add(message);
                    // Skip the message which reaches the max redelivery count.
                    if (redeliveryCount > deadLetterPolicy.getMaxRedeliverCount()) {
                        skippedMessages++;
                        continue;
                    }
                }
                if (acknowledgmentsGroupingTracker.isDuplicate(message.getMessageId())) {
                    skippedMessages++;
                    continue;
                }
                executeNotifyCallback(message);
            }
            if (ackBitSet != null) {
                ackBitSet.recycle();
            }
        } catch (IllegalStateException e) {
            log.warn("[{}] [{}] unable to obtain message in batch", subscription, consumerName, e);
            discardCorruptedMessage(messageId, cnx, ValidationError.BatchDeSerializeError);
        }

        if (deadLetterPolicy != null && possibleSendToDeadLetterTopicMessages != null) {
            if (redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
                possibleSendToDeadLetterTopicMessages.put(batchMessage,
                        possibleToDeadLetter);
                if (redeliveryCount > deadLetterPolicy.getMaxRedeliverCount()) {
                    redeliverUnacknowledgedMessages(Collections.singleton(batchMessage));
                }
            }
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

    private boolean isSameEntry(MessageIdImpl messageId) {
        return startMessageId != null
                && messageId.getLedgerId() == startMessageId.getLedgerId()
                && messageId.getEntryId() == startMessageId.getEntryId();
    }

    /**
     * Record the event that one message has been processed by the application.
     *
     * Periodically, it sends a Flow command to notify the broker that it can push more messages
     */
    @Override
    protected synchronized void messageProcessed(Message<?> msg) {
        ClientCnx currentCnx = cnx();
        ClientCnx msgCnx = ((MessageImpl<?>) msg).getCnx();
        lastDequeuedMessageId = msg.getMessageId();

        if (msgCnx != currentCnx) {
            // The processed message did belong to the old queue that was cleared after reconnection.
        } else {
            if (listener == null && !parentConsumerHasListener) {
                increaseAvailablePermits(currentCnx);
            }
            stats.updateNumMsgsReceived(msg);

            trackMessage(msg);
        }
        decreaseIncomingMessageSize(msg);
    }

    protected void trackMessage(Message<?> msg) {
        if (msg != null) {
            trackMessage(msg.getMessageId(), msg.getRedeliveryCount());
        }
    }

    protected void trackMessage(MessageId messageId) {
            trackMessage(messageId, 0);
    }

    protected void trackMessage(MessageId messageId, int redeliveryCount) {
        if (conf.getAckTimeoutMillis() > 0 && messageId instanceof MessageIdImpl) {
            MessageId id = MessageIdAdvUtils.discardBatch(messageId);
            if (hasParentConsumer) {
                //TODO: check parent consumer here
                // we should no longer track this message, TopicsConsumer will take care from now onwards
                unAckedMessageTracker.remove(id);
            } else {
                trackUnAckedMsgIfNoListener(id, redeliveryCount);
            }
        }
    }

    void increaseAvailablePermits(MessageImpl<?> msg) {
        ClientCnx currentCnx = cnx();
        ClientCnx msgCnx = msg.getCnx();
        if (msgCnx == currentCnx) {
            increaseAvailablePermits(currentCnx);
        }
    }

    void increaseAvailablePermits(ClientCnx currentCnx) {
        increaseAvailablePermits(currentCnx, 1);
    }

    protected void increaseAvailablePermits(ClientCnx currentCnx, int delta) {
        int available = AVAILABLE_PERMITS_UPDATER.addAndGet(this, delta);
        while (available >= getCurrentReceiverQueueSize() / 2 && !paused) {
            if (AVAILABLE_PERMITS_UPDATER.compareAndSet(this, available, 0)) {
                sendFlowPermitsToBroker(currentCnx, available);
                break;
            } else {
                available = AVAILABLE_PERMITS_UPDATER.get(this);
            }
        }
    }

    public void increaseAvailablePermits(int delta) {
        increaseAvailablePermits(cnx(), delta);
    }

    @Override
    protected void setCurrentReceiverQueueSize(int newSize) {
        checkArgument(newSize > 0, "receiver queue size should larger than 0");
        int oldSize = CURRENT_RECEIVER_QUEUE_SIZE_UPDATER.getAndSet(this, newSize);
        int delta = newSize - oldSize;
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] update currentReceiverQueueSize from {} to {}, increaseAvailablePermits by {}",
                    topic, subscription, oldSize, newSize, delta);
        }
        increaseAvailablePermits(delta);
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

    @Override
    public long getLastDisconnectedTimestamp() {
        return connectionHandler.lastConnectionClosedTimestamp;
    }

    private ByteBuf decryptPayloadIfNeeded(MessageIdData messageId, int redeliveryCount, MessageMetadata msgMetadata,
                                           ByteBuf payload, ClientCnx currentCnx) {

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
                            "[{}][{}][{}] Skipping decryption since CryptoKeyReader interface is not implemented and"
                                    + " config is set to discard",
                            topic, subscription, consumerName);
                    discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
                    return null;
                case FAIL:
                    MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
                    log.error(
                            "[{}][{}][{}][{}] Message delivery failed since CryptoKeyReader interface is not"
                                    + " implemented to consume encrypted message",
                            topic, subscription, consumerName, m);
                    unAckedMessageTracker.add(m, redeliveryCount);
                    return null;
            }
        }


        int maxDecryptedSize = msgCrypto.getMaxOutputSize(payload.readableBytes());
        ByteBuf decryptedData = PulsarByteBufAllocator.DEFAULT.buffer(maxDecryptedSize);
        ByteBuffer nioDecryptedData = decryptedData.nioBuffer(0, maxDecryptedSize);
        if (msgCrypto.decrypt(() -> msgMetadata, payload.nioBuffer(), nioDecryptedData, conf.getCryptoKeyReader())) {
            decryptedData.writerIndex(nioDecryptedData.limit());
            return decryptedData;
        }

        decryptedData.release();

        switch (conf.getCryptoFailureAction()) {
            case CONSUME:
                // Note, batch message will fail to consume even if config is set to consume
                log.warn("[{}][{}][{}][{}] Decryption failed. Consuming encrypted message since config is set to"
                                + " consume.",
                        topic, subscription, consumerName, messageId);
                return payload.retain();
            case DISCARD:
                log.warn("[{}][{}][{}][{}] Discarding message since decryption failed and config is set to discard",
                        topic, subscription, consumerName, messageId);
                discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
                return null;
            case FAIL:
                MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
                log.error(
                        "[{}][{}][{}][{}] Message delivery failed since unable to decrypt incoming message",
                        topic, subscription, consumerName, m);
                unAckedMessageTracker.add(m, redeliveryCount);
                return null;
        }
        return null;
    }

    private ByteBuf uncompressPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
            ClientCnx currentCnx, boolean checkMaxMessageSize) {
        CompressionType compressionType = msgMetadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (checkMaxMessageSize && payloadSize > getConnectionHandler().getMaxMessageSize()) {
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
            int checksum = Commands.readChecksum(headersAndPayload);
            int computedChecksum = Crc32cIntChecksum.computeChecksum(headersAndPayload);
            if (checksum != computedChecksum) {
                log.error(
                        "[{}][{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{},"
                                + " Computed checksum: 0x{}",
                        topic, subscription, messageId.getLedgerId(), messageId.getEntryId(),
                        Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }

    private void discardCorruptedMessage(MessageIdImpl messageId, ClientCnx currentCnx,
                                         ValidationError validationError) {
        log.error("[{}][{}] Discarding corrupted message at {}:{}", topic, subscription, messageId.getLedgerId(),
                messageId.getEntryId());
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), null,
                AckType.Individual, validationError, Collections.emptyMap(), -1);
        currentCnx.ctx().writeAndFlush(cmd, currentCnx.ctx().voidPromise());
        increaseAvailablePermits(currentCnx);
        stats.incrementNumReceiveFailed();
    }

    private void discardCorruptedMessage(MessageIdData messageId, ClientCnx currentCnx,
            ValidationError validationError) {
        log.error("[{}][{}] Discarding corrupted message at {}:{}", topic, subscription, messageId.getLedgerId(),
                messageId.getEntryId());
        discardMessage(messageId, currentCnx, validationError);
    }

    private void discardMessage(MessageIdData messageId, ClientCnx currentCnx, ValidationError validationError) {
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), null,
                AckType.Individual, validationError, Collections.emptyMap(), -1);
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

    public boolean isConnected(ClientCnx cnx) {
        return cnx != null && (getState() == State.Ready);
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
        // First : synchronized in order to handle consumer reconnect produce race condition, when broker receive
        // redeliverUnacknowledgedMessages and consumer have not be created and
        // then receive reconnect epoch change the broker is smaller than the client epoch, this will cause client epoch
        // smaller than broker epoch forever. client will not receive message anymore.
        // Second : we should synchronized `ClientCnx cnx = cnx()` to
        // prevent use old cnx to send redeliverUnacknowledgedMessages to a old broker
        synchronized (ConsumerImpl.this) {
            ClientCnx cnx = cnx();
            // V1 don't support redeliverUnacknowledgedMessages
            if (cnx != null && cnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v2.getValue()) {
                if ((getState() == State.Connecting)) {
                    log.warn("[{}] Client Connection needs to be established "
                            + "for redelivery of unacknowledged messages", this);
                } else {
                    log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
                    cnx.ctx().close();
                }

                return;
            }

            // clear local message
            int currentSize;
            incomingQueueLock.lock();
            try {
                // we should increase epoch every time, because MultiTopicsConsumerImpl also increase it,
                // we need to keep both epochs the same
                if (conf.getSubscriptionType() == SubscriptionType.Failover
                        || conf.getSubscriptionType() == SubscriptionType.Exclusive) {
                    CONSUMER_EPOCH.incrementAndGet(this);
                }

                // clear local message
                currentSize = incomingMessages.size();
                clearIncomingMessages();
                unAckedMessageTracker.clear();
            } finally {
                incomingQueueLock.unlock();
            }

            // is channel is connected, we should send redeliver command to broker
            if (cnx != null && isConnected(cnx)) {
                cnx.ctx().writeAndFlush(Commands.newRedeliverUnacknowledgedMessages(
                        consumerId, CONSUMER_EPOCH.get(this)), cnx.ctx().voidPromise());
                if (currentSize > 0) {
                    increaseAvailablePermits(cnx, currentSize);
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] Redeliver unacked messages and send {} permits", subscription, topic,
                            consumerName, currentSize);
                }
            } else {
                log.warn("[{}] Send redeliver messages command but the client is reconnect or close, "
                        + "so don't need to send redeliver command to broker", this);
            }
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
        if (messageIds.isEmpty()) {
            return;
        }

        if (conf.getSubscriptionType() != SubscriptionType.Shared
                && conf.getSubscriptionType() != SubscriptionType.Key_Shared) {
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        ClientCnx cnx = cnx();
        if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getValue()) {
            int messagesFromQueue = removeExpiredMessagesFromQueue(messageIds);
            Iterables.partition(messageIds, MAX_REDELIVER_UNACKNOWLEDGED).forEach(ids -> {
                getRedeliveryMessageIdData(ids).thenAccept(messageIdData -> {
                    if (!messageIdData.isEmpty()) {
                        ByteBuf cmd = Commands.newRedeliverUnacknowledgedMessages(consumerId, messageIdData);
                        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                    }
                });
            });
            if (messagesFromQueue > 0) {
                increaseAvailablePermits(cnx, messagesFromQueue);
            }
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
    protected void updateAutoScaleReceiverQueueHint() {
        boolean prev = scaleReceiverQueueHint.getAndSet(
                getAvailablePermits() + incomingMessages.size() >= getCurrentReceiverQueueSize());
        if (log.isDebugEnabled() && prev != scaleReceiverQueueHint.get()) {
            log.debug("updateAutoScaleReceiverQueueHint {} -> {}", prev, scaleReceiverQueueHint.get());
        }
    }

    @Override
    protected void completeOpBatchReceive(OpBatchReceive<T> op) {
        notifyPendingBatchReceivedCallBack(op.future);
    }

    private CompletableFuture<List<MessageIdData>> getRedeliveryMessageIdData(List<MessageId> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        List<CompletableFuture<MessageIdData>> futures = messageIds.stream().map(originalMessageId -> {
            final MessageIdAdv messageId = (MessageIdAdv) originalMessageId;
            CompletableFuture<Boolean> future = processPossibleToDLQ(messageId);
            return future.thenApply(sendToDLQ -> {
                if (!sendToDLQ) {
                    return new MessageIdData()
                            .setPartition(messageId.getPartitionIndex())
                            .setLedgerId(messageId.getLedgerId())
                            .setEntryId(messageId.getEntryId());
                }
                return null;
            });
        }).collect(Collectors.toList());
        return FutureUtil.waitForAll(futures).thenApply(v ->
                futures.stream().map(CompletableFuture::join).filter(Objects::nonNull).collect(Collectors.toList()));
    }

    private CompletableFuture<Boolean> processPossibleToDLQ(MessageIdAdv messageId) {
        List<MessageImpl<T>> deadLetterMessages = null;
        if (possibleSendToDeadLetterTopicMessages != null) {
            deadLetterMessages = possibleSendToDeadLetterTopicMessages.get(MessageIdAdvUtils.discardBatch(messageId));
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (deadLetterMessages != null) {
            List<MessageImpl<T>> finalDeadLetterMessages = deadLetterMessages;
            initDeadLetterProducerIfNeeded().thenAcceptAsync(producerDLQ -> {
                for (MessageImpl<T> message : finalDeadLetterMessages) {
                    try {
                        String originMessageIdStr = message.getMessageId().toString();
                        String originTopicNameStr = getOriginTopicNameStr(message);
                        TypedMessageBuilder<byte[]> typedMessageBuilderNew =
                                producerDLQ.newMessage(Schema.AUTO_PRODUCE_BYTES(message.getReaderSchema().get()))
                                        .value(message.getData())
                                        .properties(getPropertiesMap(message, originMessageIdStr, originTopicNameStr));
                        copyMessageKeysIfNeeded(message, typedMessageBuilderNew);
                        typedMessageBuilderNew.sendAsync()
                                .thenAccept(messageIdInDLQ -> {
                                    possibleSendToDeadLetterTopicMessages.remove(messageId);
                                    acknowledgeAsync(messageId).whenComplete((v, ex) -> {
                                        if (ex != null) {
                                            log.warn(
                                                    "[{}] [{}] [{}] Failed to acknowledge the message {} of the "
                                                            + "original topic but send to the DLQ successfully.",
                                                    topicName, subscription, consumerName, messageId, ex);
                                            result.complete(false);
                                        } else {
                                            result.complete(true);
                                        }
                                    });
                                }).exceptionally(ex -> {
                                    if (ex instanceof PulsarClientException.ProducerQueueIsFullError) {
                                        log.warn(
                                                "[{}] [{}] [{}] Failed to send DLQ message to {} for message id {}: {}",
                                                topicName, subscription, consumerName,
                                                deadLetterPolicy.getDeadLetterTopic(), messageId, ex.getMessage());
                                    } else {
                                        log.warn("[{}] [{}] [{}] Failed to send DLQ message to {} for message id {}",
                                                topicName, subscription, consumerName,
                                                deadLetterPolicy.getDeadLetterTopic(), messageId, ex);
                                    }
                                    result.complete(false);
                                    return null;
                                });
                    } catch (Exception e) {
                        log.warn("[{}] [{}] [{}] Failed to send DLQ message to {} for message id {}",
                                topicName, subscription, consumerName, deadLetterPolicy.getDeadLetterTopic(), messageId,
                                e);
                        result.complete(false);
                    }
                }
            }, internalPinnedExecutor).exceptionally(ex -> {
                log.error("Dead letter producer exception with topic: {}", deadLetterPolicy.getDeadLetterTopic(), ex);
                result.complete(false);
                return null;
            });
        } else {
            result.complete(false);
        }
        return result;
    }

    private CompletableFuture<Producer<byte[]>> initDeadLetterProducerIfNeeded() {
        CompletableFuture<Producer<byte[]>> p = deadLetterProducer;
        if (p == null || p.isCompletedExceptionally()) {
            createProducerLock.writeLock().lock();
            try {
                p = deadLetterProducer;
                if (p == null || p.isCompletedExceptionally()) {
                    p = createProducerWithBackOff(() -> {
                        CompletableFuture<Producer<byte[]>> newProducer =
                                ((ProducerBuilderImpl<byte[]>) client.newProducer(Schema.AUTO_PRODUCE_BYTES(schema)))
                                        .initialSubscriptionName(this.deadLetterPolicy.getInitialSubscriptionName())
                                        .topic(this.deadLetterPolicy.getDeadLetterTopic())
                                        .blockIfQueueFull(false)
                                        .enableBatching(false)
                                        .enableChunking(true)
                                        .createAsync();
                        newProducer.whenComplete((producer, ex) -> {
                            if (ex != null) {
                                log.error("[{}] [{}] [{}] Failed to create dead letter producer for topic {}",
                                        topicName, subscription, consumerName, deadLetterPolicy.getDeadLetterTopic(),
                                        ex);
                                deadLetterProducerFailureCount++;
                            } else {
                                deadLetterProducerFailureCount = 0;
                            }
                        });
                        return newProducer;
                    }, deadLetterProducerFailureCount, () -> "dead letter producer (topic: "
                            + deadLetterPolicy.getDeadLetterTopic() + ")");
                    deadLetterProducer = p;
                }
            } finally {
                createProducerLock.writeLock().unlock();
            }
        }
        return p;
    }

    private CompletableFuture<Producer<byte[]>> createProducerWithBackOff(
            Supplier<CompletableFuture<Producer<byte[]>>> producerSupplier, int failureCount,
            Supplier<String> logDescription) {
        if (failureCount == 0) {
            return producerSupplier.get();
        } else {
            // calculate backoff time for given failure count
            Backoff backoff = new BackoffBuilder()
                    .setInitialTime(100, TimeUnit.MILLISECONDS)
                    .setMandatoryStop(client.getConfiguration().getOperationTimeoutMs() * 2,
                            TimeUnit.MILLISECONDS)
                    .setMax(1, TimeUnit.MINUTES)
                    .create();
            long backoffTimeMillis = 0;
            for (int i = 0; i < failureCount; i++) {
                backoffTimeMillis = backoff.next();
            }
            CompletableFuture<Producer<byte[]>> newProducer = new CompletableFuture<>();
            ScheduledExecutorService executor =
                    (ScheduledExecutorService) client.getScheduledExecutorProvider().getExecutor(this);
            log.info("Creating {} with backoff time of {} ms", logDescription.get(), backoffTimeMillis);
            executor.schedule(() -> {
                FutureUtil.completeAfter(newProducer, producerSupplier.get());
            }, backoffTimeMillis, TimeUnit.MILLISECONDS);
            return newProducer;
        }
    }

    private CompletableFuture<Producer<byte[]>> initRetryLetterProducerIfNeeded() {
        CompletableFuture<Producer<byte[]>> p = retryLetterProducer;
        if (p == null || p.isCompletedExceptionally()) {
            createProducerLock.writeLock().lock();
            try {
                p = retryLetterProducer;
                if (p == null || p.isCompletedExceptionally()) {
                    p = createProducerWithBackOff(() -> {
                        CompletableFuture<Producer<byte[]>> newProducer = client
                                .newProducer(Schema.AUTO_PRODUCE_BYTES(schema))
                                .topic(this.deadLetterPolicy.getRetryLetterTopic())
                                .enableBatching(false)
                                .enableChunking(true)
                                .blockIfQueueFull(false)
                                .createAsync();
                        newProducer.whenComplete((producer, ex) -> {
                            if (ex != null) {
                                log.error("[{}] [{}] [{}] Failed to create retry letter producer for topic {}",
                                        topicName, subscription, consumerName, deadLetterPolicy.getRetryLetterTopic(),
                                        ex);
                                retryLetterProducerFailureCount++;
                            } else {
                                retryLetterProducerFailureCount = 0;
                            }
                        });
                        return newProducer;
                    }, retryLetterProducerFailureCount, () -> "retry letter producer (topic: "
                            + deadLetterPolicy.getRetryLetterTopic() + ")");
                    retryLetterProducer = p;
                }
            } finally {
                createProducerLock.writeLock().unlock();
            }
        }
        return p;
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
    public void seek(Function<String, Object> function) throws PulsarClientException {
        try {
            seekAsync(function).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(Function<String, Object> function) {
        if (function == null) {
            return FutureUtil.failedFuture(new PulsarClientException("Function must be set"));
        }
        Object seekPosition = function.apply(topic);
        if (seekPosition == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (seekPosition instanceof MessageId) {
            return seekAsync((MessageId) seekPosition);
        } else if (seekPosition.getClass().getTypeName()
                .equals(Long.class.getTypeName())) {
            return seekAsync((long) seekPosition);
        }
        return FutureUtil.failedFuture(
                new PulsarClientException("Only support seek by messageId or timestamp"));
    }

    private CompletableFuture<Void> seekAsyncInternal(long requestId, ByteBuf seek, MessageId seekId,
                                                      Long seekTimestamp, String seekBy) {
        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMax(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                .create();

        if (!seekStatus.compareAndSet(SeekStatus.NOT_STARTED, SeekStatus.IN_PROGRESS)) {
            final String message = String.format(
                    "[%s][%s] attempting to seek operation that is already in progress (seek by %s)",
                    topic, subscription, seekBy);
            log.warn("[{}][{}] Attempting to seek operation that is already in progress, cancelling {}",
                    topic, subscription, seekBy);
            return FutureUtil.failedFuture(new IllegalStateException(message));
        }
        seekFuture = new CompletableFuture<>();
        seekAsyncInternal(requestId, seek, seekId, seekTimestamp, seekBy, backoff, opTimeoutMs);
        return seekFuture;
    }

    private void seekAsyncInternal(long requestId, ByteBuf seek, MessageId seekId, Long seekTimestamp, String seekBy,
                                   final Backoff backoff, final AtomicLong remainingTime) {
        ClientCnx cnx = cnx();
        if (isConnected() && cnx != null) {
            MessageIdAdv originSeekMessageId = seekMessageId;
            seekMessageId = (MessageIdAdv) seekId;
            log.info("[{}][{}] Seeking subscription to {}", topic, subscription, seekBy);

            final boolean originalHasSoughtByTimestamp = hasSoughtByTimestamp;
            hasSoughtByTimestamp = (seekTimestamp != null);
            cnx.sendRequestWithId(seek, requestId).thenRun(() -> {
                log.info("[{}][{}] Successfully reset subscription to {}", topic, subscription, seekBy);
                acknowledgmentsGroupingTracker.flushAndClean();

                lastDequeuedMessageId = MessageId.earliest;

                clearIncomingMessages();
                CompletableFuture<Void> future = null;
                synchronized (this) {
                    if (!hasParentConsumer && cnx() == null) {
                        // It's during reconnection, complete the seek future after connection is established
                        seekStatus.set(SeekStatus.COMPLETED);
                    } else {
                        future = seekFuture;
                        startMessageId = seekMessageId;
                        seekStatus.set(SeekStatus.NOT_STARTED);
                    }
                }
                if (future != null) {
                    future.complete(null);
                }
            }).exceptionally(e -> {
                seekMessageId = originSeekMessageId;
                hasSoughtByTimestamp = originalHasSoughtByTimestamp;
                log.error("[{}][{}] Failed to reset subscription: {}", topic, subscription, e.getCause().getMessage());

                failSeek(
                        PulsarClientException.wrap(e.getCause(),
                                String.format("Failed to seek the subscription %s of the topic %s to %s",
                                        subscription, topicName.toString(), seekBy)));
                return null;
            });
        } else {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            if (nextDelay <= 0) {
                failSeek(
                        new PulsarClientException.TimeoutException(
                                String.format("The subscription %s of the topic %s could not seek "
                                        + "withing configured timeout", subscription, topicName.toString())));
                return;
            }

            ((ScheduledExecutorService) client.getScheduledExecutorProvider().getExecutor()).schedule(() -> {
                log.warn("[{}] [{}] Could not get connection while seek -- Will try again in {} ms",
                        topic, getHandlerName(), nextDelay);
                remainingTime.addAndGet(-nextDelay);
                seekAsyncInternal(requestId, seek, seekId, seekTimestamp, seekBy, backoff, remainingTime);
            }, nextDelay, TimeUnit.MILLISECONDS);
        }
    }

    private void failSeek(Throwable throwable) {
        CompletableFuture<Void> seekFuture = this.seekFuture;
        if (seekStatus.compareAndSet(SeekStatus.IN_PROGRESS, SeekStatus.NOT_STARTED)) {
            seekFuture.completeExceptionally(throwable);
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        String seekBy = String.format("the timestamp %d", timestamp);
        long requestId = client.newRequestId();
        return seekAsyncInternal(requestId, Commands.newSeek(consumerId, requestId, timestamp),
                MessageId.earliest, timestamp, seekBy);
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        String seekBy = String.format("the message %s", messageId.toString());
        long requestId = client.newRequestId();
        final MessageIdAdv msgId = (MessageIdAdv) messageId;
        final MessageIdAdv firstChunkMsgId = msgId.getFirstChunkMessageId();
        final ByteBuf seek;
        if (msgId.getFirstChunkMessageId() != null) {
            seek = Commands.newSeek(consumerId, requestId, firstChunkMsgId.getLedgerId(),
                    firstChunkMsgId.getEntryId(), new long[0]);
        } else {
            final long[] ackSetArr;
            if (MessageIdAdvUtils.isBatch(msgId)) {
                final BitSetRecyclable ackSet = BitSetRecyclable.create();
                ackSet.set(0, msgId.getBatchSize());
                ackSet.clear(0, Math.max(msgId.getBatchIndex(), 0));
                ackSetArr = ackSet.toLongArray();
                ackSet.recycle();
            } else {
                ackSetArr = new long[0];
            }
            seek = Commands.newSeek(consumerId, requestId, msgId.getLedgerId(), msgId.getEntryId(), ackSetArr);
        }
        return seekAsyncInternal(requestId, seek, messageId, null, seekBy);
    }

    public boolean hasMessageAvailable() throws PulsarClientException {
        try {
            return hasMessageAvailableAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        final CompletableFuture<Boolean> booleanFuture = new CompletableFuture<>();

        if (incomingMessages != null && !incomingMessages.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }

        // we haven't read yet. use startMessageId for comparison
        if (lastDequeuedMessageId == MessageId.earliest) {
            // If the last seek is called with timestamp, startMessageId cannot represent the position to start, so we
            // have to get the mark-delete position from the GetLastMessageId response to compare as well.
            // if we are starting from latest, we should seek to the actual last message first.
            // allow the last one to be read when read head inclusively.
            final boolean hasSoughtByTimestamp = this.hasSoughtByTimestamp;
            if (MessageId.latest.equals(startMessageId) || hasSoughtByTimestamp) {
                CompletableFuture<GetLastMessageIdResponse> future = internalGetLastMessageIdAsync();
                // if the consumer is configured to read inclusive then we need to seek to the last message
                if (resetIncludeHead && !hasSoughtByTimestamp) {
                    future = future.thenCompose((lastMessageIdResponse) ->
                            seekAsync(lastMessageIdResponse.lastMessageId)
                                    .thenApply((ignore) -> lastMessageIdResponse));
                }

                future.thenAccept(response -> {
                    MessageIdAdv lastMessageId = (MessageIdAdv) response.lastMessageId;
                    MessageIdAdv markDeletePosition = (MessageIdAdv) response.markDeletePosition;

                    if (markDeletePosition != null && !(markDeletePosition.getEntryId() < 0
                            && markDeletePosition.getLedgerId() > lastMessageId.getLedgerId())) {
                        // we only care about comparing ledger ids and entry ids as mark delete position doesn't have
                        // other ids such as batch index
                        int result = ComparisonChain.start()
                                .compare(markDeletePosition.getLedgerId(), lastMessageId.getLedgerId())
                                .compare(markDeletePosition.getEntryId(), lastMessageId.getEntryId())
                                .result();
                        if (lastMessageId.getEntryId() < 0) {
                            completehasMessageAvailableWithValue(booleanFuture, false);
                        } else if (hasSoughtByTimestamp) {
                            completehasMessageAvailableWithValue(booleanFuture, result < 0);
                        } else {
                            completehasMessageAvailableWithValue(booleanFuture,
                                    resetIncludeHead ? result <= 0 : result < 0);
                        }
                    } else if (lastMessageId == null || lastMessageId.getEntryId() < 0) {
                        completehasMessageAvailableWithValue(booleanFuture, false);
                    } else {
                        completehasMessageAvailableWithValue(booleanFuture, resetIncludeHead);
                    }
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed getLastMessageId command", topic, subscription, ex);
                    booleanFuture.completeExceptionally(ex.getCause());
                    return null;
                });

                return booleanFuture;
            }

            if (hasMoreMessages(lastMessageIdInBroker, startMessageId, resetIncludeHead)) {
                completehasMessageAvailableWithValue(booleanFuture, true);
                return booleanFuture;
            }

            getLastMessageIdAsync().thenAccept(messageId -> {
                lastMessageIdInBroker = messageId;
                completehasMessageAvailableWithValue(booleanFuture,
                        hasMoreMessages(lastMessageIdInBroker, startMessageId, resetIncludeHead));
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                booleanFuture.completeExceptionally(e.getCause());
                return null;
            });

        } else {
            // read before, use lastDequeueMessage for comparison
            if (hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessageId, false)) {
                completehasMessageAvailableWithValue(booleanFuture, true);
                return booleanFuture;
            }

            getLastMessageIdAsync().thenAccept(messageId -> {
                lastMessageIdInBroker = messageId;
                completehasMessageAvailableWithValue(booleanFuture,
                        hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessageId, false));
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                booleanFuture.completeExceptionally(e.getCause());
                return null;
            });
        }

        return booleanFuture;
    }

    private void completehasMessageAvailableWithValue(CompletableFuture<Boolean> future, boolean value) {
        internalPinnedExecutor.execute(() -> {
            future.complete(value);
        });
    }

    private boolean hasMoreMessages(MessageId lastMessageIdInBroker, MessageId messageId, boolean inclusive) {
        if (inclusive && lastMessageIdInBroker.compareTo(messageId) >= 0
                && ((MessageIdImpl) lastMessageIdInBroker).getEntryId() != -1) {
            return true;
        }

        return !inclusive && lastMessageIdInBroker.compareTo(messageId) > 0
                && ((MessageIdImpl) lastMessageIdInBroker).getEntryId() != -1;
    }

    private static final class GetLastMessageIdResponse {
        final MessageId lastMessageId;
        final MessageId markDeletePosition;

        GetLastMessageIdResponse(MessageId lastMessageId, MessageId markDeletePosition) {
            this.lastMessageId = lastMessageId;
            this.markDeletePosition = markDeletePosition;
        }
    }

    @Deprecated
    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return internalGetLastMessageIdAsync().thenApply(r -> r.lastMessageId);
    }

    @Override
    public CompletableFuture<List<TopicMessageId>> getLastMessageIdsAsync() {
        return getLastMessageIdAsync()
                .thenApply(msgId -> Collections.singletonList(new TopicMessageIdImpl(topic, (MessageIdAdv) msgId)));
    }

    public CompletableFuture<GetLastMessageIdResponse> internalGetLastMessageIdAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                .failedFuture(new PulsarClientException.AlreadyClosedException(
                    String.format("The consumer %s was already closed when the subscription %s of the topic %s "
                            + "getting the last message id", consumerName, subscription, topicName.toString())));
                }

        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMax(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                .create();

        CompletableFuture<GetLastMessageIdResponse> getLastMessageIdFuture = new CompletableFuture<>();

        internalGetLastMessageIdAsync(backoff, opTimeoutMs, getLastMessageIdFuture);
        return getLastMessageIdFuture;
    }

    private void internalGetLastMessageIdAsync(final Backoff backoff,
                                               final AtomicLong remainingTime,
                                               CompletableFuture<GetLastMessageIdResponse> future) {
        ClientCnx cnx = cnx();
        if (isConnected() && cnx != null) {
            if (!Commands.peerSupportsGetLastMessageId(cnx.getRemoteEndpointProtocolVersion())) {
                future.completeExceptionally(
                    new PulsarClientException.NotSupportedException(
                        String.format("The command `GetLastMessageId` is not supported for the protocol version %d. "
                                        + "The consumer is %s, topic %s, subscription %s",
                                cnx.getRemoteEndpointProtocolVersion(),
                                consumerName, topicName.toString(), subscription)));
                return;
            }

            long requestId = client.newRequestId();
            ByteBuf getLastIdCmd = Commands.newGetLastMessageId(consumerId, requestId);
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Get topic last message Id", topic, subscription);
            }

            cnx.sendGetLastMessageId(getLastIdCmd, requestId).thenAccept(cmd -> {
                MessageIdData lastMessageId = cmd.getLastMessageId();
                MessageIdImpl markDeletePosition = null;
                if (cmd.hasConsumerMarkDeletePosition()) {
                    markDeletePosition = new MessageIdImpl(cmd.getConsumerMarkDeletePosition().getLedgerId(),
                            cmd.getConsumerMarkDeletePosition().getEntryId(), -1);
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Successfully getLastMessageId {}:{}",
                        topic, subscription, lastMessageId.getLedgerId(), lastMessageId.getEntryId());
                }

                MessageId lastMsgId = lastMessageId.getBatchIndex() <= 0
                        ? new MessageIdImpl(lastMessageId.getLedgerId(),
                                lastMessageId.getEntryId(), lastMessageId.getPartition())
                        : new BatchMessageIdImpl(lastMessageId.getLedgerId(), lastMessageId.getEntryId(),
                                lastMessageId.getPartition(), lastMessageId.getBatchIndex());

                future.complete(new GetLastMessageIdResponse(lastMsgId, markDeletePosition));
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                future.completeExceptionally(
                    PulsarClientException.wrap(e.getCause(),
                        String.format("The subscription %s of the topic %s gets the last message id was failed",
                            subscription, topicName.toString())));
                return null;
            });
        } else {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            if (nextDelay <= 0) {
                future.completeExceptionally(
                    new PulsarClientException.TimeoutException(
                        String.format("The subscription %s of the topic %s could not get the last message id "
                                + "withing configured timeout", subscription, topicName.toString())));
                return;
            }

            log.warn("[{}] [{}] Could not get connection while getLastMessageId -- Will try again in {} ms",
                    topic, getHandlerName(), nextDelay);
            ((ScheduledExecutorService) client.getScheduledExecutorProvider().getExecutor()).schedule(() -> {
                remainingTime.addAndGet(-nextDelay);
                internalGetLastMessageIdAsync(backoff, remainingTime, future);
            }, nextDelay, TimeUnit.MILLISECONDS);
        }
    }

    private boolean isMessageUndecryptable(MessageMetadata msgMetadata) {
        return (msgMetadata.getEncryptionKeysCount() > 0 && conf.getCryptoKeyReader() == null
                && conf.getCryptoFailureAction() == ConsumerCryptoFailureAction.CONSUME);
    }

    /**
     * Create EncryptionContext if message payload is encrypted.
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
                                    e -> new EncryptionKey(e.getValue(),
                                            e.getMetadatasList().stream().collect(
                                                    Collectors.toMap(KeyValue::getKey, KeyValue::getValue)))));
            byte[] encParam = msgMetadata.getEncryptionParam();
            Optional<Integer> batchSize = Optional
                    .ofNullable(msgMetadata.hasNumMessagesInBatch() ? msgMetadata.getNumMessagesInBatch() : null);
            encryptionCtx.setKeys(keys);
            encryptionCtx.setParam(encParam);
            if (msgMetadata.hasEncryptionAlgo()) {
                encryptionCtx.setAlgorithm(msgMetadata.getEncryptionAlgo());
            }
            encryptionCtx
                    .setCompressionType(CompressionCodecProvider.convertFromWireProtocol(msgMetadata.getCompression()));
            encryptionCtx.setUncompressedMessageSize(msgMetadata.getUncompressedSize());
            encryptionCtx.setBatchSize(batchSize);
        }
        return Optional.ofNullable(encryptionCtx);
    }

    private int removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
        int messagesFromQueue = 0;
        Message<T> message;
        while (true) {
            message = incomingMessages.pollIf(msg -> {
                MessageId idPolled = MessageIdAdvUtils.discardBatch(msg.getMessageId());
                return messageIds.contains(idPolled);
            });
            if (message == null) {
                break;
            }
            decreaseIncomingMessageSize(message);
            messagesFromQueue++;
            message.release();
        }
        return messagesFromQueue;
    }

    @Override
    public ConsumerStatsRecorder getStats() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsumerImpl)) {
            return false;
        }
        ConsumerImpl<?> consumer = (ConsumerImpl<?>) o;
        return consumerId == consumer.consumerId;
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
        if (clientCnx != null) {
            this.connectionHandler.setClientCnx(clientCnx);
            clientCnx.registerConsumer(consumerId, this);
            if (conf.isAckReceiptEnabled()
                    && !Commands.peerSupportsAckReceipt(clientCnx.getRemoteEndpointProtocolVersion())) {
                log.warn("Server don't support ack for receipt! "
                        + "ProtoVersion >=17 support! nowVersion : {}", clientCnx.getRemoteEndpointProtocolVersion());
            }
        }
        ClientCnx previousClientCnx = clientCnxUsedForConsumerRegistration.getAndSet(clientCnx);
        if (previousClientCnx != null && previousClientCnx != clientCnx) {
            previousClientCnx.removeConsumer(consumerId);
        }
    }

    void deregisterFromClientCnx() {
        setClientCnx(null);
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }

    @Deprecated
    public String getTopicNameWithoutPartition() {
        return topicNameWithoutPartition;
    }

    static class ChunkedMessageCtx {

        protected int totalChunks = -1;
        protected ByteBuf chunkedMsgBuffer;
        protected int lastChunkedMessageId = -1;
        protected MessageIdImpl[] chunkedMessageIds;
        protected long receivedTime = 0;

        static ChunkedMessageCtx get(int numChunksFromMsg, ByteBuf chunkedMsgBuffer) {
            ChunkedMessageCtx ctx = RECYCLER.get();
            ctx.totalChunks = numChunksFromMsg;
            ctx.chunkedMsgBuffer = chunkedMsgBuffer;
            ctx.chunkedMessageIds = new MessageIdImpl[numChunksFromMsg];
            ctx.receivedTime = System.currentTimeMillis();
            return ctx;
        }

        private final Handle<ChunkedMessageCtx> recyclerHandle;

        private ChunkedMessageCtx(Handle<ChunkedMessageCtx> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<ChunkedMessageCtx> RECYCLER = new Recycler<ChunkedMessageCtx>() {
            protected ChunkedMessageCtx newObject(Recycler.Handle<ChunkedMessageCtx> handle) {
                return new ChunkedMessageCtx(handle);
            }
        };

        public void recycle() {
            this.totalChunks = -1;
            this.chunkedMsgBuffer = null;
            this.lastChunkedMessageId = -1;
            recyclerHandle.recycle(this);
        }
    }

    private void removeOldestPendingChunkedMessage() {
        ChunkedMessageCtx chunkedMsgCtx = null;
        String firstPendingMsgUuid = null;
        while (chunkedMsgCtx == null && !pendingChunkedMessageUuidQueue.isEmpty()) {
            // remove oldest pending chunked-message group and free memory
            firstPendingMsgUuid = pendingChunkedMessageUuidQueue.poll();
            chunkedMsgCtx = StringUtils.isNotBlank(firstPendingMsgUuid) ? chunkedMessagesMap.get(firstPendingMsgUuid)
                    : null;
        }
        removeChunkMessage(firstPendingMsgUuid, chunkedMsgCtx, this.autoAckOldestChunkedMessageOnQueueFull);
    }

    protected void removeExpireIncompleteChunkedMessages() {
        if (expireTimeOfIncompleteChunkedMessageMillis <= 0) {
            return;
        }
        ChunkedMessageCtx chunkedMsgCtx = null;
        String messageUUID;
        while ((messageUUID = pendingChunkedMessageUuidQueue.peek()) != null) {
            chunkedMsgCtx = StringUtils.isNotBlank(messageUUID) ? chunkedMessagesMap.get(messageUUID) : null;
            if (chunkedMsgCtx != null && System
                    .currentTimeMillis() > (chunkedMsgCtx.receivedTime + expireTimeOfIncompleteChunkedMessageMillis)) {
                pendingChunkedMessageUuidQueue.remove(messageUUID);
                removeChunkMessage(messageUUID, chunkedMsgCtx, true);
            } else {
                return;
            }
        }
    }

    private void removeChunkMessage(String msgUUID, ChunkedMessageCtx chunkedMsgCtx, boolean autoAck) {
        if (chunkedMsgCtx == null) {
            return;
        }
        // clean up pending chuncked-Message
        chunkedMessagesMap.remove(msgUUID);
        if (chunkedMsgCtx.chunkedMessageIds != null) {
            for (MessageIdImpl msgId : chunkedMsgCtx.chunkedMessageIds) {
                if (msgId == null) {
                    continue;
                }
                if (autoAck) {
                    log.info("Removing chunk message-id {}", msgId);
                    doAcknowledge(msgId, AckType.Individual, Collections.emptyMap(), null);
                } else {
                    trackMessage(msgId);
                }
            }
        }
        if (chunkedMsgCtx.chunkedMsgBuffer != null) {
            chunkedMsgCtx.chunkedMsgBuffer.release();
        }
        chunkedMsgCtx.recycle();
        pendingChunkedMessageCount--;
    }

    private CompletableFuture<Void> doTransactionAcknowledgeForResponse(MessageId messageId, AckType ackType,
                                                                        ValidationError validationError,
                                                                        Map<String, Long> properties, TxnID txnID) {
        long requestId = client.newRequestId();
        final MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
        final long ledgerId = messageIdAdv.getLedgerId();
        final long entryId = messageIdAdv.getEntryId();
        final List<ByteBuf> cmdList;
        if (MessageIdAdvUtils.isBatch(messageIdAdv)) {
            BitSetRecyclable bitSetRecyclable = BitSetRecyclable.create();
            bitSetRecyclable.set(0, messageIdAdv.getBatchSize());
            if (ackType == AckType.Cumulative) {
                MessageIdAdvUtils.acknowledge(messageIdAdv, false);
                bitSetRecyclable.clear(0, messageIdAdv.getBatchIndex() + 1);
            } else {
                bitSetRecyclable.clear(messageIdAdv.getBatchIndex());
            }
            cmdList = Collections.singletonList(Commands.newAck(consumerId, ledgerId, entryId, bitSetRecyclable,
                    ackType, validationError, properties, txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId,
                    messageIdAdv.getBatchSize()));
            bitSetRecyclable.recycle();
        } else {
            MessageIdImpl[] chunkMsgIds = this.unAckedChunkedMessageIdSequenceMap.remove(messageIdAdv);
            // cumulative ack chunk by the last messageId
            if (chunkMsgIds == null || ackType == AckType.Cumulative) {
                cmdList = Collections.singletonList(Commands.newAck(consumerId, ledgerId, entryId, null, ackType,
                        validationError, properties, txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId));
            } else {
                if (Commands.peerSupportsMultiMessageAcknowledgment(
                        getClientCnx().getRemoteEndpointProtocolVersion())) {
                    List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck =
                            new ArrayList<>(chunkMsgIds.length);
                    for (MessageIdImpl cMsgId : chunkMsgIds) {
                        if (cMsgId != null && chunkMsgIds.length > 1) {
                            entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                        }
                    }
                    cmdList = Collections.singletonList(
                            newMultiTransactionMessageAck(consumerId, txnID, entriesToAck, requestId));
                } else {
                    cmdList = new ArrayList<>();
                    for (MessageIdImpl cMsgId : chunkMsgIds) {
                        cmdList.add(Commands.newAck(consumerId, cMsgId.ledgerId, cMsgId.entryId, null, ackType,
                                validationError, properties,
                                txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId));
                    }
                }
            }
        }

        if (ackType == AckType.Cumulative) {
            unAckedMessageTracker.removeMessagesTill(messageId);
        } else {
            unAckedMessageTracker.remove(messageId);
        }
        ClientCnx cnx = cnx();
        if (cnx == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .ConnectException("Failed to ack message [" + messageId + "] "
                    + "for transaction [" + txnID + "] due to consumer connect fail, consumer state: " + getState()));
        } else {
            List<CompletableFuture<Void>> completableFutures = new LinkedList<>();
            cmdList.forEach(cmd -> completableFutures.add(cnx.newAckForReceipt(cmd, requestId)));
            return FutureUtil.waitForAll(completableFutures);
        }
    }

    private ByteBuf newMultiTransactionMessageAck(long consumerId, TxnID txnID,
                                                  List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entries,
                                                  long requestID) {
        BaseCommand cmd = newMultiMessageAckCommon(entries);
        cmd.getAck()
                .setConsumerId(consumerId)
                .setAckType(AckType.Individual)
                .setTxnidLeastBits(txnID.getLeastSigBits())
                .setTxnidMostBits(txnID.getMostSigBits())
                .setRequestId(requestID);
        return serializeWithSize(cmd);
    }

    private static final FastThreadLocal<BaseCommand> LOCAL_BASE_COMMAND = new FastThreadLocal<BaseCommand>() {
        @Override
        protected BaseCommand initialValue() throws Exception {
            return new BaseCommand();
        }
    };

    private static BaseCommand newMultiMessageAckCommon(List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entries) {
        BaseCommand cmd = LOCAL_BASE_COMMAND.get()
                .clear()
                .setType(BaseCommand.Type.ACK);
        CommandAck ack = cmd.setAck();
        int entriesCount = entries.size();
        for (int i = 0; i < entriesCount; i++) {
            long ledgerId = entries.get(i).getLeft();
            long entryId = entries.get(i).getMiddle();
            ConcurrentBitSetRecyclable bitSet = entries.get(i).getRight();
            MessageIdData msgId = ack.addMessageId()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId);
            if (bitSet != null) {
                long[] ackSet = bitSet.toLongArray();
                for (int j = 0; j < ackSet.length; j++) {
                    msgId.addAckSet(ackSet[j]);
                }
                bitSet.recycle();
            }
        }

        return cmd;
    }

    private CompletableFuture<Void> doTransactionAcknowledgeForResponse(List<MessageId> messageIds, AckType ackType,
                                                                        Map<String, Long> properties, TxnID txnID) {
        long requestId = client.newRequestId();
        List<MessageIdData> messageIdDataList = new LinkedList<>();
        for (MessageId messageId : messageIds) {
            final MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
            final MessageIdData messageIdData = new MessageIdData();
            messageIdData.setLedgerId(messageIdAdv.getLedgerId());
            messageIdData.setEntryId(messageIdAdv.getEntryId());
            if (MessageIdAdvUtils.isBatch(messageIdAdv)) {
                final BitSetRecyclable bitSetRecyclable = BitSetRecyclable.create();
                bitSetRecyclable.set(0, messageIdAdv.getBatchSize());
                if (ackType == AckType.Cumulative) {
                    MessageIdAdvUtils.acknowledge(messageIdAdv, false);
                    bitSetRecyclable.clear(0, messageIdAdv.getBatchIndex() + 1);
                } else {
                    bitSetRecyclable.clear(messageIdAdv.getBatchIndex());
                }
                for (long x : bitSetRecyclable.toLongArray()) {
                    messageIdData.addAckSet(x);
                }
                bitSetRecyclable.recycle();
            }

            messageIdDataList.add(messageIdData);
            if (ackType == AckType.Cumulative) {
                unAckedMessageTracker.removeMessagesTill(messageId);
            } else {
                unAckedMessageTracker.remove(messageId);
            }
        }
        final ByteBuf cmd = Commands.newAck(consumerId, messageIdDataList, ackType, null, properties,
                txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId);
        return cnx().newAckForReceipt(cmd, requestId);
    }

    public Map<MessageIdAdv, List<MessageImpl<T>>> getPossibleSendToDeadLetterTopicMessages() {
        return possibleSendToDeadLetterTopicMessages;
    }

    boolean isAckReceiptEnabled() {
        ClientCnx cnx = getClientCnx();
        return conf.isAckReceiptEnabled() && cnx != null
                && Commands.peerSupportsAckReceipt(cnx.getRemoteEndpointProtocolVersion());
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

    @VisibleForTesting
    enum SeekStatus {
        NOT_STARTED,
        IN_PROGRESS,
        COMPLETED
    }
}
