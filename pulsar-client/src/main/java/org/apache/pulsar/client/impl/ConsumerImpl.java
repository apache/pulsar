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
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandAck.ValidationError;
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
import org.apache.pulsar.common.util.CompletableFutureCancellationHandler;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
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

    private final int receiverQueueRefillThreshold;

    private final UnAckedMessageTracker unAckedMessageTracker;
    private final AcknowledgmentsGroupingTracker acknowledgmentsGroupingTracker;
    private final NegativeAcksTracker negativeAcksTracker;

    protected final ConsumerStatsRecorder stats;
    private final int priorityLevel;
    private final SubscriptionMode subscriptionMode;
    private volatile BatchMessageIdImpl startMessageId;

    private volatile BatchMessageIdImpl seekMessageId;
    private final AtomicBoolean duringSeek;

    private final BatchMessageIdImpl initialStartMessageId;

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

    private final Map<MessageIdImpl, List<MessageImpl<T>>> possibleSendToDeadLetterTopicMessages;

    private final DeadLetterPolicy deadLetterPolicy;

    private volatile CompletableFuture<Producer<byte[]>> deadLetterProducer;

    private volatile Producer<T> retryLetterProducer;
    private final ReadWriteLock createProducerLock = new ReentrantReadWriteLock();

    protected volatile boolean paused;

    protected ConcurrentOpenHashMap<String, ChunkedMessageCtx> chunkedMessagesMap = new ConcurrentOpenHashMap<>();
    private int pendingChunkedMessageCount = 0;
    protected long expireTimeOfIncompleteChunkedMessageMillis = 0;
    private boolean expireChunkMessageTaskScheduled = false;
    private final int maxPendingChunkedMessage;
    // if queue size is reasonable (most of the time equal to number of producers try to publish messages concurrently on
    // the topic) then it guards against broken chunked message which was not fully published
    private final boolean autoAckOldestChunkedMessageOnQueueFull;
    // it will be used to manage N outstanding chunked message buffers
    private final BlockingQueue<String> pendingChunkedMessageUuidQueue;

    private final boolean createTopicIfDoesNotExist;
    private final boolean poolMessages;

    private final AtomicReference<ClientCnx> clientCnxUsedForConsumerRegistration = new AtomicReference<>();
    private final List<Throwable> previousExceptions = new CopyOnWriteArrayList<Throwable>();

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
        return newConsumerImpl(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer, subscribeFuture,
                startMessageId, schema, interceptors, createTopicIfDoesNotExist, 0);
    }

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
                                               boolean createTopicIfDoesNotExist,
                                               long startMessageRollbackDurationInSec) {
        if (conf.getReceiverQueueSize() == 0) {
            return new ZeroQueueConsumerImpl<>(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer,
                    subscribeFuture,
                    startMessageId, schema, interceptors,
                    createTopicIfDoesNotExist);
        } else {
            return new ConsumerImpl<>(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer,
                    subscribeFuture, startMessageId, startMessageRollbackDurationInSec /* rollback time in sec to start msgId */,
                    schema, interceptors, createTopicIfDoesNotExist);
        }
    }

    protected ConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
           ExecutorProvider executorProvider, int partitionIndex, boolean hasParentConsumer,
           CompletableFuture<Consumer<T>> subscribeFuture, MessageId startMessageId,
           long startMessageRollbackDurationInSec, Schema<T> schema, ConsumerInterceptors<T> interceptors,
           boolean createTopicIfDoesNotExist) {
        super(client, topic, conf, conf.getReceiverQueueSize(), executorProvider, subscribeFuture, schema, interceptors);
        this.consumerId = client.newConsumerId();
        this.subscriptionMode = conf.getSubscriptionMode();
        this.startMessageId = startMessageId != null ? new BatchMessageIdImpl((MessageIdImpl) startMessageId) : null;
        this.initialStartMessageId = this.startMessageId;
        this.startMessageRollbackDurationInSec = startMessageRollbackDurationInSec;
        AVAILABLE_PERMITS_UPDATER.set(this, 0);
        this.lookupDeadline = System.currentTimeMillis() + client.getConfiguration().getLookupTimeoutMs();
        this.partitionIndex = partitionIndex;
        this.hasParentConsumer = hasParentConsumer;
        this.receiverQueueRefillThreshold = conf.getReceiverQueueSize() / 2;
        this.priorityLevel = conf.getPriorityLevel();
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

        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ConsumerStatsRecorderImpl(client, conf, this);
        } else {
            stats = ConsumerStatsDisabled.INSTANCE;
        }

        duringSeek = new AtomicBoolean(false);

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
                                .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
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
                        .deadLetterTopic(String.format("%s-%s" + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX, topic, subscription))
                        .build();
            }

            if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getRetryLetterTopic())) {
                this.deadLetterPolicy.setRetryLetterTopic(conf.getDeadLetterPolicy().getRetryLetterTopic());
            } else {
                this.deadLetterPolicy.setRetryLetterTopic(String.format("%s-%s" + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX,
                        topic, subscription));
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
                            topicName.toString(), subscription)));
                return null;
            });
        } else {
            unsubscribeFuture.completeExceptionally(
                new PulsarClientException(
                    String.format("The client is not connected to the broker when unsubscribing the " +
                        "subscription %s of the topic %s", subscription, topicName.toString())));
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
        CompletableFutureCancellationHandler cancellationHandler = new CompletableFutureCancellationHandler();
        CompletableFuture<Message<T>> result = cancellationHandler.createFuture();
        internalPinnedExecutor.execute(() -> {
            Message<T> message = incomingMessages.poll();
            if (message == null) {
                pendingReceives.add(result);
                cancellationHandler.setCancelAction(() -> pendingReceives.remove(result));
            }
            if (message != null) {
                messageProcessed(message);
                result.complete(beforeConsume(message));
            }
        });

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

    @Override
    protected Messages<T> internalBatchReceive() throws PulsarClientException {
        try {
            return internalBatchReceiveAsync().get();
        } catch (InterruptedException | ExecutionException e) {
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
                MessagesImpl<T> messages = getNewMessagesImpl();
                Message<T> msgPeeked = incomingMessages.peek();
                while (msgPeeked != null && messages.canAdd(msgPeeked)) {
                    Message<T> msg = incomingMessages.poll();
                    if (msg != null) {
                        messageProcessed(msg);
                        Message<T> interceptMsg = beforeConsume(msg);
                        messages.add(interceptMsg);
                    }
                    msgPeeked = incomingMessages.peek();
                }
                result.complete(messages);
            } else {
                OpBatchReceive<T> opBatchReceive = OpBatchReceive.of(result);
                pendingBatchReceives.add(opBatchReceive);
                cancellationHandler.setCancelAction(() -> pendingBatchReceives.remove(opBatchReceive));
            }
        });
        return result;
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String, Long> properties,
                                                    TransactionImpl txn) {
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

        if (txn != null) {
            return doTransactionAcknowledgeForResponse(messageId, ackType, null, properties,
                    new TxnID(txn.getTxnIdMostBits(), txn.getTxnIdLeastBits()));
        }
        return acknowledgmentsGroupingTracker.addAcknowledgment((MessageIdImpl) messageId, ackType, properties);
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(List<MessageId> messageIdList, AckType ackType, Map<String, Long> properties, TransactionImpl txn) {
        return this.acknowledgmentsGroupingTracker.addListAcknowledgment(messageIdList, ackType, properties);
    }


    @SuppressWarnings("unchecked")
    @Override
    protected CompletableFuture<Void> doReconsumeLater(Message<?> message, AckType ackType,
                                                       Map<String, Long> properties,
                                                       long delayTime,
                                                       TimeUnit unit) {
        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .InvalidMessageException("Cannot handle message with null messageId"));
        }

        if (messageId instanceof TopicMessageIdImpl) {
            messageId = ((TopicMessageIdImpl) messageId).getInnerMessageId();
        }
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
        if (delayTime < 0) {
            delayTime = 0;
        }
        if (retryLetterProducer == null) {
            createProducerLock.writeLock().lock();
            try {
                if (retryLetterProducer == null) {
                    retryLetterProducer = client.newProducer(schema)
                            .topic(this.deadLetterPolicy.getRetryLetterTopic())
                            .enableBatching(false)
                            .blockIfQueueFull(false)
                            .create();
                }
            } catch (Exception e) {
                log.error("Create retry letter producer exception with topic: {}", deadLetterPolicy.getRetryLetterTopic(), e);
            } finally {
                createProducerLock.writeLock().unlock();
            }
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (retryLetterProducer != null) {
            try {
                MessageImpl<T> retryMessage = (MessageImpl<T>) getMessageImpl(message);
                String originMessageIdStr = getOriginMessageIdStr(message);
                String originTopicNameStr = getOriginTopicNameStr(message);
                SortedMap<String, String> propertiesMap
                        = getPropertiesMap(message, originMessageIdStr, originTopicNameStr);
                int reconsumetimes = 1;
                if (propertiesMap.containsKey(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES)) {
                    reconsumetimes = Integer.parseInt(propertiesMap.get(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES));
                    reconsumetimes = reconsumetimes + 1;
                }
                propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES, String.valueOf(reconsumetimes));
                propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_DELAY_TIME, String.valueOf(unit.toMillis(delayTime)));

                if (reconsumetimes > this.deadLetterPolicy.getMaxRedeliverCount() && StringUtils.isNotBlank(deadLetterPolicy.getDeadLetterTopic())) {
                    initDeadLetterProducerIfNeeded();
                    MessageId finalMessageId = messageId;
                    deadLetterProducer.thenAccept(dlqProducer -> {
                        TypedMessageBuilder<byte[]> typedMessageBuilderNew =
                                dlqProducer.newMessage(Schema.AUTO_PRODUCE_BYTES(retryMessage.getReaderSchema().get()))
                                        .value(retryMessage.getData())
                                        .properties(propertiesMap);
                        typedMessageBuilderNew.sendAsync().thenAccept(msgId -> {
                            doAcknowledge(finalMessageId, ackType, properties, null).thenAccept(v -> {
                                result.complete(null);
                            }).exceptionally(ex -> {
                                result.completeExceptionally(ex);
                                return null;
                            });
                        }).exceptionally(ex -> {
                            result.completeExceptionally(ex);
                            return null;
                        });
                    }).exceptionally(ex -> {
                        result.completeExceptionally(ex);
                        deadLetterProducer = null;
                        return null;
                    });
                } else {
                    TypedMessageBuilder<T> typedMessageBuilderNew = retryLetterProducer.newMessage()
                            .value(retryMessage.getValue())
                            .properties(propertiesMap);
                    if (delayTime > 0) {
                        typedMessageBuilderNew.deliverAfter(delayTime, unit);
                    }
                    if (message.hasKey()) {
                        typedMessageBuilderNew.key(message.getKey());
                    }
                    typedMessageBuilderNew.send();
                    return doAcknowledge(messageId, ackType, properties, null);
                }
            } catch (Exception e) {
                log.error("Send to retry letter topic exception with topic: {}, messageId: {}", retryLetterProducer.getTopic(), messageId, e);
                Set<MessageId> messageIds = Collections.singleton(messageId);
                unAckedMessageTracker.remove(messageId);
                redeliverUnacknowledgedMessages(messageIds);
            }
        }
        MessageId finalMessageId = messageId;
        result.exceptionally(ex -> {
            Set<MessageId> messageIds = Collections.singleton(finalMessageId);
            unAckedMessageTracker.remove(finalMessageId);
            redeliverUnacknowledgedMessages(messageIds);
            return null;
        });
        return result;
    }

    private SortedMap<String, String> getPropertiesMap(Message<?> message, String originMessageIdStr, String originTopicNameStr) {
        SortedMap<String, String> propertiesMap = new TreeMap<>();
        if (message.getProperties() != null) {
            propertiesMap.putAll(message.getProperties());
        }
        propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC, originTopicNameStr);
        propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID, originMessageIdStr);
        return propertiesMap;
    }

    private String getOriginMessageIdStr(Message<?> message) {
        if (message instanceof TopicMessageImpl) {
            return ((TopicMessageIdImpl) message.getMessageId()).getInnerMessageId().toString();
        } else if (message instanceof MessageImpl) {
            return message.getMessageId().toString();
        }
        return null;
    }

    private String getOriginTopicNameStr(Message<?> message) {
        if (message instanceof TopicMessageImpl) {
            return ((TopicMessageIdImpl) message.getMessageId()).getTopicName();
        } else if (message instanceof MessageImpl) {
            return message.getTopicName();
        }
        return null;
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
        unAckedMessageTracker.remove(messageId);
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        previousExceptions.clear();

        if (getState() == State.Closing || getState() == State.Closed) {
            setState(State.Closed);
            closeConsumerTasks();
            deregisterFromClientCnx();
            client.cleanupConsumer(this);
            clearReceiverQueue();
            return;
        }
        setClientCnx(cnx);

        log.info("[{}][{}] Subscribing to topic on cnx {}, consumerId {}", topic, subscription, cnx.ctx().channel(), consumerId);

        long requestId = client.newRequestId();
        if (duringSeek.get()) {
            acknowledgmentsGroupingTracker.flushAndClean();
        }

        SUBSCRIBE_DEADLINE_UPDATER
            .compareAndSet(this, 0L, System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs());

        int currentSize;
        synchronized (this) {
            currentSize = incomingMessages.size();
            startMessageId = clearReceiverQueue();
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.clear();
            }
        }

        boolean isDurable = subscriptionMode == SubscriptionMode.Durable;
        MessageIdData startMessageIdData = null;
        if (isDurable) {
            // For regular durable subscriptions, the message id from where to restart will be determined by the broker.
            startMessageIdData = null;
        } else if (startMessageId != null) {
            // For non-durable we are going to restart from the next entry
            startMessageIdData = new MessageIdData()
                    .setLedgerId(startMessageId.getLedgerId())
                    .setEntryId(startMessageId.getEntryId())
                    .setBatchIndex(startMessageId.getBatchIndex());
        }

        SchemaInfo si = schema.getSchemaInfo();
        if (si != null && (SchemaType.BYTES == si.getType() || SchemaType.NONE == si.getType())) {
            // don't set schema for Schema.BYTES
            si = null;
        }
        // startMessageRollbackDurationInSec should be consider only once when consumer connects to first time
        long startMessageRollbackDuration = (startMessageRollbackDurationInSec > 0
                && startMessageId != null && startMessageId.equals(initialStartMessageId)) ? startMessageRollbackDurationInSec : 0;
        ByteBuf request = Commands.newSubscribe(topic, subscription, consumerId, requestId, getSubType(), priorityLevel,
                consumerName, isDurable, startMessageIdData, metadata, readCompacted,
                conf.isReplicateSubscriptionState(), InitialPosition.valueOf(subscriptionInitialPosition.getValue()),
                startMessageRollbackDuration, si, createTopicIfDoesNotExist, conf.getKeySharedPolicy());

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
                    return;
                }
            }

            resetBackoff();

            boolean firstTimeConnect = subscribeFuture.complete(this);
            // if the consumer is not partitioned or is re-connected and is partitioned, we send the flow
            // command to receive messages.
            if (!(firstTimeConnect && hasParentConsumer) && conf.getReceiverQueueSize() != 0) {
                increaseAvailablePermits(cnx, conf.getReceiverQueueSize());
            }
        }).exceptionally((e) -> {
            deregisterFromClientCnx();
            if (getState() == State.Closing || getState() == State.Closed) {
                // Consumer was closed while reconnecting, close the connection to make sure the broker
                // drops the consumer on its side
                cnx.channel().close();
                return null;
            }
            log.warn("[{}][{}] Failed to subscribe to topic on {}", topic, subscription, cnx.channel().remoteAddress());

            if (e.getCause() instanceof PulsarClientException
                    && PulsarClientException.isRetriableError(e.getCause())
                    && System.currentTimeMillis() < SUBSCRIBE_DEADLINE_UPDATER.get(ConsumerImpl.this)) {
                reconnectLater(e.getCause());
            } else if (!subscribeFuture.isDone()) {
                // unable to create new consumer, fail operation
                setState(State.Failed);
                closeConsumerTasks();
                subscribeFuture.completeExceptionally(
                    PulsarClientException.wrap(e, String.format("Failed to subscribe the topic %s with subscription " +
                        "name %s when connecting to the broker", topicName.toString(), subscription)));
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
                log.warn("[{}][{}] Closed consumer because topic does not exist anymore {}", topic, subscription, cnx.channel().remoteAddress());
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
        resetIncomingMessageSize();

        if (duringSeek.compareAndSet(true, false)) {
            return seekMessageId;
        } else if (subscriptionMode == SubscriptionMode.Durable) {
            return startMessageId;
        }

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
            // release messages if they are pooled messages
            currentMessageQueue.forEach(Message::release);
            return previousMessage;
        } else if (!lastDequeuedMessageId.equals(MessageId.earliest)) {
            // If the queue was empty we need to restart from the message just after the last one that has been dequeued
            // in the past
            return new BatchMessageIdImpl((MessageIdImpl) lastDequeuedMessageId);
        } else {
            // No message was received or dequeued by this consumer. Next message would still be the startMessageId
            return startMessageId;
        }
    }

    /**
     * send the flow command to have the broker start pushing messages
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
                                log.debug("Consumer {} failed to send {} permits to broker: {}", consumerId, numMessages,
                                        writeFuture.cause().getMessage());
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
                    log.info("[{}] Consumer creation failed for consumer {} with unretriableError {}", topic, consumerId, exception);
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
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        if (getState() == State.Closing || getState() == State.Closed) {
            closeConsumerTasks();
            failPendingReceive().whenComplete((r, t) -> closeFuture.complete(null));
            return closeFuture;
        }

        if (!isConnected()) {
            log.info("[{}] [{}] Closed Consumer (not connected)", topic, subscription);
            setState(State.Closed);
            closeConsumerTasks();
            deregisterFromClientCnx();
            client.cleanupConsumer(this);
            failPendingReceive().whenComplete((r, t) -> closeFuture.complete(null));
            return closeFuture;
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

        return closeFuture;
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
        stats.getStatTimeout().ifPresent(Timeout::cancel);
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
        return !isMessageUndecryptable(messageMetadata) &&
                (messageMetadata.hasNumMessagesInBatch() || messageMetadata.getNumMessagesInBatch() != 1);
    }

    protected <U> MessageImpl<U> newSingleMessage(final int index,
                                                  final int numMessages,
                                                  final BrokerEntryMetadata brokerEntryMetadata,
                                                  final MessageMetadata msgMetadata,
                                                  final SingleMessageMetadata singleMessageMetadata,
                                                  final ByteBuf payload,
                                                  final MessageIdImpl messageId,
                                                  final Schema<U> schema,
                                                  final boolean containMetadata,
                                                  final BitSetRecyclable ackBitSet,
                                                  final BatchMessageAcker acker,
                                                  final int redeliveryCount) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] processing message num - {} in batch", subscription, consumerName, index);
        }

        ByteBuf singleMessagePayload = null;
        try {
            if (containMetadata) {
                singleMessagePayload =
                        Commands.deSerializeSingleMessageInBatch(payload, singleMessageMetadata, index, numMessages);
            }

            if (isSameEntry(messageId) && isPriorBatchIndex(index)) {
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

            if (ackBitSet != null && !ackBitSet.get(index)) {
                return null;
            }

            BatchMessageIdImpl batchMessageIdImpl = new BatchMessageIdImpl(messageId.getLedgerId(),
                    messageId.getEntryId(), getPartitionIndex(), index, numMessages, acker);

            final ByteBuf payloadBuffer = (singleMessagePayload != null) ? singleMessagePayload : payload;
            final MessageImpl<U> message = MessageImpl.create(topicName.toString(), batchMessageIdImpl,
                    msgMetadata, singleMessageMetadata, payloadBuffer,
                    createEncryptionContext(msgMetadata), cnx(), schema, redeliveryCount, poolMessages
            );
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

    protected <U> MessageImpl<U> newMessage(final MessageIdImpl messageId,
                                            final BrokerEntryMetadata brokerEntryMetadata,
                                            final MessageMetadata messageMetadata,
                                            final ByteBuf payload,
                                            final Schema<U> schema,
                                            final int redeliveryCount) {
        final MessageImpl<U> message = MessageImpl.create(topicName.toString(), messageId, messageMetadata, payload,
                createEncryptionContext(messageMetadata), cnx(), schema, redeliveryCount, poolMessages
        );
        message.setBrokerEntryMetadata(brokerEntryMetadata);
        return message;
    }

    private void executeNotifyCallback(final MessageImpl<T> message) {
        // Enqueue the message so that it can be retrieved when application calls receive()
        // if the conf.getReceiverQueueSize() is 0 then discard message if no one is waiting for it.
        // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
        internalPinnedExecutor.execute(() -> {
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
                                           final List<Long> ackSet) {
        final MessagePayloadImpl payload = MessagePayloadImpl.create(byteBuf);
        final MessagePayloadContextImpl entryContext = MessagePayloadContextImpl.get(
                brokerEntryMetadata, messageMetadata, messageId, this, redeliveryCount, ackSet);
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

        internalPinnedExecutor.execute(()
                -> tryTriggerListener());
    }

    void messageReceived(MessageIdData messageId, int redeliveryCount, List<Long> ackSet, ByteBuf headersAndPayload, ClientCnx cnx) {
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
        final boolean isChunkedMessage = numChunks > 1 && conf.getSubscriptionType() != SubscriptionType.Shared;

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
        ByteBuf uncompressedPayload = (isMessageUndecryptable || isChunkedMessage) ? decryptedPayload.retain()
                : uncompressPayloadIfNeeded(messageId, msgMetadata, decryptedPayload, cnx, true);
        decryptedPayload.release();
        if (uncompressedPayload == null) {
            // Message was discarded on decompression error
            return;
        }

        if (conf.getPayloadProcessor() != null) {
            // uncompressedPayload is released in this method so we don't need to call release() again
            processPayloadByProcessor(
                    brokerEntryMetadata, msgMetadata, uncompressedPayload, msgId, schema, redeliveryCount, ackSet);
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
            }

            if (isSameEntry(msgId) && isPriorEntryIndex(messageId.getEntryId())) {
                // We need to discard entries that were prior to startMessageId
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Ignoring message from before the startMessageId: {}", subscription,
                            consumerName, startMessageId);
                }

                uncompressedPayload.release();
                return;
            }

            final MessageImpl<T> message =
                    newMessage(msgId, brokerEntryMetadata, msgMetadata, uncompressedPayload, schema, redeliveryCount);
            uncompressedPayload.release();

            if (deadLetterPolicy != null && possibleSendToDeadLetterTopicMessages != null &&
                    redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
                possibleSendToDeadLetterTopicMessages.put((MessageIdImpl) message.getMessageId(),
                        Collections.singletonList(message));
            }
            executeNotifyCallback(message);
        } else {
            // handle batch message enqueuing; uncompressed payload has all messages in batch
            receiveIndividualMessagesFromBatch(brokerEntryMetadata, msgMetadata, redeliveryCount, ackSet, uncompressedPayload, messageId, cnx);

            uncompressedPayload.release();
        }
        internalPinnedExecutor.execute(()
                -> tryTriggerListener());

    }

    private void tryTriggerListener() {
        if (listener != null) {
            triggerListener();
        }
    }

    private boolean isTxnMessage(MessageMetadata messageMetadata) {
        return messageMetadata.hasTxnidMostBits() && messageMetadata.hasTxnidLeastBits();
    }

    private ByteBuf processMessageChunk(ByteBuf compressedPayload, MessageMetadata msgMetadata, MessageIdImpl msgId,
            MessageIdData messageId, ClientCnx cnx) {

        // Lazy task scheduling to expire incomplete chunk message
        if (!expireChunkMessageTaskScheduled && expireTimeOfIncompleteChunkedMessageMillis > 0) {
            internalPinnedExecutor.scheduleAtFixedRate(() -> {
                        removeExpireIncompleteChunkedMessages();
                    }, expireTimeOfIncompleteChunkedMessageMillis, expireTimeOfIncompleteChunkedMessageMillis,
                    TimeUnit.MILLISECONDS);
            expireChunkMessageTaskScheduled = true;
        }

        if (msgMetadata.getChunkId() == 0) {
            ByteBuf chunkedMsgBuffer = Unpooled.directBuffer(msgMetadata.getTotalChunkMsgSize(),
                    msgMetadata.getTotalChunkMsgSize());
            int totalChunks = msgMetadata.getNumChunksFromMsg();
            chunkedMessagesMap.computeIfAbsent(msgMetadata.getUuid(),
                    (key) -> ChunkedMessageCtx.get(totalChunks, chunkedMsgBuffer));
            pendingChunkedMessageCount++;
            if (maxPendingChunkedMessage > 0 && pendingChunkedMessageCount > maxPendingChunkedMessage) {
                removeOldestPendingChunkedMessage();
            }
            pendingChunkedMessageUuidQueue.add(msgMetadata.getUuid());
        }

        ChunkedMessageCtx chunkedMsgCtx = chunkedMessagesMap.get(msgMetadata.getUuid());
        // discard message if chunk is out-of-order
        if (chunkedMsgCtx == null || chunkedMsgCtx.chunkedMsgBuffer == null
                || msgMetadata.getChunkId() != (chunkedMsgCtx.lastChunkedMessageId + 1)
                || msgMetadata.getChunkId() >= msgMetadata.getTotalChunkMsgSize()) {
            // means we lost the first chunk: should never happen
            log.info("Received unexpected chunk messageId {}, last-chunk-id{}, chunkId = {}, total-chunks {}", msgId,
                    (chunkedMsgCtx != null ? chunkedMsgCtx.lastChunkedMessageId : null), msgMetadata.getChunkId(),
                    msgMetadata.getTotalChunkMsgSize());
            if (chunkedMsgCtx != null) {
                if (chunkedMsgCtx.chunkedMsgBuffer != null) {
                    ReferenceCountUtil.safeRelease(chunkedMsgCtx.chunkedMsgBuffer);
                }
                chunkedMsgCtx.recycle();
            }
            chunkedMessagesMap.remove(msgMetadata.getUuid());
            compressedPayload.release();
            increaseAvailablePermits(cnx);
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
            increaseAvailablePermits(cnx);
            return null;
        }

        // last chunk received: so, stitch chunked-messages and clear up chunkedMsgBuffer
        if (log.isDebugEnabled()) {
            log.debug("Chunked message completed chunkId {}, total-chunks {}, msgId {} sequenceId {}",
                    msgMetadata.getChunkId(), msgMetadata.getNumChunksFromMsg(), msgId, msgMetadata.getSequenceId());
        }
        // remove buffer from the map, add chunked messageId to unack-message tracker, and reduce pending-chunked-message count
        chunkedMessagesMap.remove(msgMetadata.getUuid());
        unAckedChunkedMessageIdSequenceMap.put(msgId, chunkedMsgCtx.chunkedMessageIds);
        pendingChunkedMessageCount--;
        compressedPayload.release();
        compressedPayload = chunkedMsgCtx.chunkedMsgBuffer;
        chunkedMsgCtx.recycle();
        ByteBuf uncompressedPayload = uncompressPayloadIfNeeded(messageId, msgMetadata, compressedPayload, cnx, false);
        compressedPayload.release();
        return uncompressedPayload;
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

        if (conf.getReceiverQueueSize() == 0) {
            // call interceptor and complete received callback
            trackMessage(message);
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
        completePendingReceive(receivedFuture, interceptMessage);
    }

    void receiveIndividualMessagesFromBatch(BrokerEntryMetadata brokerEntryMetadata, MessageMetadata msgMetadata,
                                            int redeliveryCount, List<Long> ackSet, ByteBuf uncompressedPayload,
                                            MessageIdData messageId, ClientCnx cnx) {
        int batchSize = msgMetadata.getNumMessagesInBatch();

        // create ack tracker for entry aka batch
        MessageIdImpl batchMessage = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                getPartitionIndex());
        List<MessageImpl<T>> possibleToDeadLetter = null;
        if (deadLetterPolicy != null && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
            possibleToDeadLetter = new ArrayList<>();
        }

        BatchMessageAcker acker = BatchMessageAcker.newAcker(batchSize);
        BitSetRecyclable ackBitSet = null;
        if (ackSet != null && ackSet.size() > 0) {
            ackBitSet = BitSetRecyclable.valueOf(SafeCollectionUtils.longListToArray(ackSet));
        }

        SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        int skippedMessages = 0;
        try {
            for (int i = 0; i < batchSize; ++i) {
                final MessageImpl<T> message = newSingleMessage(i, batchSize, brokerEntryMetadata, msgMetadata,
                        singleMessageMetadata, uncompressedPayload, batchMessage, schema, true, ackBitSet, acker,
                        redeliveryCount);
                if (message == null) {
                    skippedMessages++;
                    continue;
                }
                if (possibleToDeadLetter != null) {
                    possibleToDeadLetter.add(message);
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
            increaseAvailablePermits(currentCnx);
            stats.updateNumMsgsReceived(msg);

            trackMessage(msg);
        }
        decreaseIncomingMessageSize(msg);
    }

    protected void trackMessage(Message<?> msg) {
        if (msg != null) {
            trackMessage(msg.getMessageId());
        }
    }

    protected void trackMessage(MessageId messageId) {
        if (conf.getAckTimeoutMillis() > 0 && messageId instanceof MessageIdImpl) {
            MessageIdImpl id = (MessageIdImpl) messageId;
            if (id instanceof BatchMessageIdImpl) {
                // do not add each item in batch message into tracker
                id = new MessageIdImpl(id.getLedgerId(), id.getEntryId(), getPartitionIndex());
            }
            if (hasParentConsumer) {
                //TODO: check parent consumer here
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

    protected void increaseAvailablePermits(ClientCnx currentCnx, int delta) {
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

    public void increaseAvailablePermits(int delta) {
        increaseAvailablePermits(cnx(), delta);
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
            ClientCnx currentCnx, boolean checkMaxMessageSize) {
        CompressionType compressionType = msgMetadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (checkMaxMessageSize && payloadSize > ClientCnx.getMaxMessageSize()) {
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
                        "[{}][{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
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
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), null, AckType.Individual,
                validationError, Collections.emptyMap(), -1);
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
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), null, AckType.Individual,
                                      validationError, Collections.emptyMap(), -1);
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
        if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getValue()) {
            int currentSize = 0;
            synchronized (this) {
                currentSize = incomingMessages.size();
                clearIncomingMessages();
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

    public int clearIncomingMessagesAndGetMessageNumber() {
        int messagesNumber = incomingMessages.size();
        incomingMessages.forEach(Message::release);
        clearIncomingMessages();
        unAckedMessageTracker.clear();
        return messagesNumber;
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
        if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getValue()) {
            int messagesFromQueue = removeExpiredMessagesFromQueue(messageIds);
            Iterable<List<MessageIdImpl>> batches = Iterables.partition(
                messageIds.stream()
                    .map(messageId -> (MessageIdImpl)messageId)
                    .collect(Collectors.toSet()), MAX_REDELIVER_UNACKNOWLEDGED);
            batches.forEach(ids -> {
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
    protected void completeOpBatchReceive(OpBatchReceive<T> op) {
        notifyPendingBatchReceivedCallBack(op);
    }

    private CompletableFuture<List<MessageIdData>> getRedeliveryMessageIdData(List<MessageIdImpl> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        List<MessageIdData> data = new ArrayList<>(messageIds.size());
        List<CompletableFuture<Boolean>> futures = new ArrayList<>(messageIds.size());
        messageIds.forEach(messageId -> {
            CompletableFuture<Boolean> future = processPossibleToDLQ(messageId);
            futures.add(future);
            future.thenAccept(sendToDLQ -> {
                if (!sendToDLQ) {
                    data.add(new MessageIdData()
                            .setPartition(messageId.getPartitionIndex())
                            .setLedgerId(messageId.getLedgerId())
                            .setEntryId(messageId.getEntryId()));
                }
            });
        });
        return FutureUtil.waitForAll(futures).thenCompose(v -> CompletableFuture.completedFuture(data));
    }

    private CompletableFuture<Boolean> processPossibleToDLQ(MessageIdImpl messageId) {
        List<MessageImpl<T>> deadLetterMessages = null;
        if (possibleSendToDeadLetterTopicMessages != null) {
            if (messageId instanceof BatchMessageIdImpl) {
                messageId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                        getPartitionIndex());
            }
            deadLetterMessages = possibleSendToDeadLetterTopicMessages.get(messageId);
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (deadLetterMessages != null) {
            initDeadLetterProducerIfNeeded();
            List<MessageImpl<T>> finalDeadLetterMessages = deadLetterMessages;
            MessageIdImpl finalMessageId = messageId;
            deadLetterProducer.thenAcceptAsync(producerDLQ -> {
                for (MessageImpl<T> message : finalDeadLetterMessages) {
                    String originMessageIdStr = getOriginMessageIdStr(message);
                    String originTopicNameStr = getOriginTopicNameStr(message);
                    producerDLQ.newMessage(Schema.AUTO_PRODUCE_BYTES(message.getReaderSchema().get()))
                            .value(message.getData())
                            .properties(getPropertiesMap(message, originMessageIdStr, originTopicNameStr))
                            .sendAsync()
                            .thenAccept(messageIdInDLQ -> {
                                possibleSendToDeadLetterTopicMessages.remove(finalMessageId);
                                acknowledgeAsync(finalMessageId).whenComplete((v, ex) -> {
                                    if (ex != null) {
                                        log.warn("[{}] [{}] [{}] Failed to acknowledge the message {} of the original topic but send to the DLQ successfully.",
                                                topicName, subscription, consumerName, finalMessageId, ex);
                                    } else {
                                        result.complete(true);
                                    }
                                });
                            }).exceptionally(ex -> {
                                log.warn("[{}] [{}] [{}] Failed to send DLQ message to {} for message id {}",
                                        topicName, subscription, consumerName, finalMessageId, ex);
                                result.complete(false);
                                return null;
                    });
                }
            }).exceptionally(ex -> {
                log.error("Dead letter producer exception with topic: {}", deadLetterPolicy.getDeadLetterTopic(), ex);
                deadLetterProducer = null;
                result.complete(false);
                return null;
            });
        } else {
            result.complete(false);
        }
        return result;
    }

    private void initDeadLetterProducerIfNeeded() {
        if (deadLetterProducer == null) {
            createProducerLock.writeLock().lock();
            try {
                if (deadLetterProducer == null) {
                    deadLetterProducer = client.newProducer(Schema.AUTO_PRODUCE_BYTES(schema))
                            .topic(this.deadLetterPolicy.getDeadLetterTopic())
                            .blockIfQueueFull(false)
                            .createAsync();
                }
            } finally {
                createProducerLock.writeLock().unlock();
            }
        }
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

    private Optional<CompletableFuture<Void>> seekAsyncCheckState(String seekBy) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return Optional.of(FutureUtil
                    .failedFuture(new PulsarClientException.AlreadyClosedException(
                            String.format("The consumer %s was already closed when seeking the subscription %s of the"
                                    + " topic %s to %s", consumerName, subscription, topicName.toString(), seekBy))));
        }

        if (!isConnected()) {
            return Optional.of(FutureUtil.failedFuture(new PulsarClientException(
                    String.format("The client is not connected to the broker when seeking the subscription %s of the "
                            + "topic %s to %s", subscription, topicName.toString(), seekBy))));
        }

        return Optional.empty();
    }

    private CompletableFuture<Void> seekAsyncInternal(long requestId, ByteBuf seek, MessageId seekId, String seekBy) {
        final CompletableFuture<Void> seekFuture = new CompletableFuture<>();
        ClientCnx cnx = cnx();

        BatchMessageIdImpl originSeekMessageId = seekMessageId;
        seekMessageId = new BatchMessageIdImpl((MessageIdImpl) seekId);
        duringSeek.set(true);
        log.info("[{}][{}] Seeking subscription to {}", topic, subscription, seekBy);

        cnx.sendRequestWithId(seek, requestId).thenRun(() -> {
            log.info("[{}][{}] Successfully reset subscription to {}", topic, subscription, seekBy);
            acknowledgmentsGroupingTracker.flushAndClean();

            lastDequeuedMessageId = MessageId.earliest;

            clearIncomingMessages();
            seekFuture.complete(null);
        }).exceptionally(e -> {
            // re-set duringSeek and seekMessageId if seek failed
            seekMessageId = originSeekMessageId;
            duringSeek.set(false);
            log.error("[{}][{}] Failed to reset subscription: {}", topic, subscription, e.getCause().getMessage());

            seekFuture.completeExceptionally(
                PulsarClientException.wrap(e.getCause(),
                    String.format("Failed to seek the subscription %s of the topic %s to %s",
                        subscription, topicName.toString(), seekBy)));
            return null;
        });
        return seekFuture;
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        String seekBy = String.format("the timestamp %d", timestamp);
        return seekAsyncCheckState(seekBy).orElseGet(() -> {
            long requestId = client.newRequestId();
            return seekAsyncInternal(requestId, Commands.newSeek(consumerId, requestId, timestamp),
                MessageId.earliest, seekBy);
        });
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        String seekBy = String.format("the message %s", messageId.toString());
        return seekAsyncCheckState(seekBy).orElseGet(() -> {
            long requestId = client.newRequestId();
            ByteBuf seek = null;
            if (messageId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl msgId = (BatchMessageIdImpl) messageId;
                // Initialize ack set
                BitSetRecyclable ackSet = BitSetRecyclable.create();
                ackSet.set(0, msgId.getBatchSize());
                ackSet.clear(0, Math.max(msgId.getBatchIndex(), 0));
                long[] ackSetArr = ackSet.toLongArray();
                ackSet.recycle();

                seek = Commands.newSeek(consumerId, requestId, msgId.getLedgerId(), msgId.getEntryId(), ackSetArr);
            } else {
                MessageIdImpl msgId = (MessageIdImpl) messageId;
                seek = Commands.newSeek(consumerId, requestId, msgId.getLedgerId(), msgId.getEntryId(), new long[0]);
            }
            return seekAsyncInternal(requestId, seek, messageId, seekBy);
        });
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

        // we haven't read yet. use startMessageId for comparison
        if (lastDequeuedMessageId == MessageId.earliest) {
            // if we are starting from latest, we should seek to the actual last message first.
            // allow the last one to be read when read head inclusively.
            if (startMessageId.equals(MessageId.latest)) {

                CompletableFuture<GetLastMessageIdResponse> future = internalGetLastMessageIdAsync();
                // if the consumer is configured to read inclusive then we need to seek to the last message
                if (resetIncludeHead) {
                    future = future.thenCompose((lastMessageIdResponse) ->
                            seekAsync(lastMessageIdResponse.lastMessageId)
                                    .thenApply((ignore) -> lastMessageIdResponse));
                }

                future.thenAccept(response -> {
                    MessageIdImpl lastMessageId = MessageIdImpl.convertToMessageIdImpl(response.lastMessageId);
                    MessageIdImpl markDeletePosition = MessageIdImpl
                            .convertToMessageIdImpl(response.markDeletePosition);

                    if (markDeletePosition != null) {
                        // we only care about comparing ledger ids and entry ids as mark delete position doesn't have other ids such as batch index
                        int result = ComparisonChain.start()
                                .compare(markDeletePosition.getLedgerId(), lastMessageId.getLedgerId())
                                .compare(markDeletePosition.getEntryId(), lastMessageId.getEntryId())
                                .result();
                        if (lastMessageId.getEntryId() < 0) {
                            completehasMessageAvailableWithValue(booleanFuture, false);
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
                if (hasMoreMessages(lastMessageIdInBroker, startMessageId, resetIncludeHead)) {
                    completehasMessageAvailableWithValue(booleanFuture, true);
                } else {
                    completehasMessageAvailableWithValue(booleanFuture, false);
                }
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
                if (hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessageId, false)) {
                    completehasMessageAvailableWithValue(booleanFuture, true);
                } else {
                    completehasMessageAvailableWithValue(booleanFuture, false);
                }
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
        if (inclusive && lastMessageIdInBroker.compareTo(messageId) >= 0 &&
                ((MessageIdImpl) lastMessageIdInBroker).getEntryId() != -1) {
            return true;
        }

        if (!inclusive && lastMessageIdInBroker.compareTo(messageId) > 0 &&
                ((MessageIdImpl) lastMessageIdInBroker).getEntryId() != -1) {
            return true;
        }

        return false;
    }

    private static final class GetLastMessageIdResponse {
        final MessageId lastMessageId;
        final MessageId markDeletePosition;

        GetLastMessageIdResponse(MessageId lastMessageId, MessageId markDeletePosition) {
            this.lastMessageId = lastMessageId;
            this.markDeletePosition = markDeletePosition;
        }
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return internalGetLastMessageIdAsync().thenApply(r -> r.lastMessageId);
    }

    public CompletableFuture<GetLastMessageIdResponse> internalGetLastMessageIdAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                .failedFuture(new PulsarClientException.AlreadyClosedException(
                    String.format("The consumer %s was already closed when the subscription %s of the topic %s " +
                        "getting the last message id", consumerName, subscription, topicName.toString())));
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
                        String.format("The command `GetLastMessageId` is not supported for the protocol version %d. " +
                                "The consumer is %s, topic %s, subscription %s", cnx.getRemoteEndpointProtocolVersion(),
                            consumerName, topicName.toString(), subscription)));
                return;
            }

            long requestId = client.newRequestId();
            ByteBuf getLastIdCmd = Commands.newGetLastMessageId(consumerId, requestId);
            log.info("[{}][{}] Get topic last message Id", topic, subscription);

            cnx.sendGetLastMessageId(getLastIdCmd, requestId).thenAccept(cmd -> {
                MessageIdData lastMessageId = cmd.getLastMessageId();
                MessageIdImpl markDeletePosition = null;
                if (cmd.hasConsumerMarkDeletePosition()) {
                    markDeletePosition = new MessageIdImpl(cmd.getConsumerMarkDeletePosition().getLedgerId(),
                            cmd.getConsumerMarkDeletePosition().getEntryId(), -1);
                }
                log.info("[{}][{}] Successfully getLastMessageId {}:{}",
                    topic, subscription, lastMessageId.getLedgerId(), lastMessageId.getEntryId());

                MessageId lastMsgId = lastMessageId.getBatchIndex() <= 0 ?
                        new MessageIdImpl(lastMessageId.getLedgerId(),
                                lastMessageId.getEntryId(), lastMessageId.getPartition())
                        : new BatchMessageIdImpl(lastMessageId.getLedgerId(),
                                lastMessageId.getEntryId(), lastMessageId.getPartition(), lastMessageId.getBatchIndex());

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
                        String.format("The subscription %s of the topic %s could not get the last message id " +
                            "withing configured timeout", subscription, topicName.toString())));
                return;
            }

            internalPinnedExecutor.schedule(() -> {
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
                decreaseIncomingMessageSize(message);
                messagesFromQueue++;
                MessageIdImpl id = getMessageIdImpl(message);
                if (!messageIds.contains(id)) {
                    messageIds.add(id);
                    break;
                }
                message.release();
                message = incomingMessages.poll();
            }
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
        if (this == o) return true;
        if (!(o instanceof ConsumerImpl)) return false;
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
            if (conf.isAckReceiptEnabled() &&
                    !Commands.peerSupportsAckReceipt(clientCnx.getRemoteEndpointProtocolVersion())) {
                log.warn("Server don't support ack for receipt! " +
                        "ProtoVersion >=17 support! nowVersion : {}", clientCnx.getRemoteEndpointProtocolVersion());
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

    void reconnectLater(Throwable exception) {
        this.connectionHandler.reconnectLater(exception);
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }

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
        BitSetRecyclable bitSetRecyclable = null;
        long ledgerId;
        long entryId;
        ByteBuf cmd;
        long requestId = client.newRequestId();
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            bitSetRecyclable = BitSetRecyclable.create();
            ledgerId = batchMessageId.getLedgerId();
            entryId = batchMessageId.getEntryId();
            if (ackType == AckType.Cumulative) {
                batchMessageId.ackCumulative();
                bitSetRecyclable.set(0, batchMessageId.getBatchSize());
                bitSetRecyclable.clear(0, batchMessageId.getBatchIndex() + 1);
            } else {
                bitSetRecyclable.set(0, batchMessageId.getBatchSize());
                bitSetRecyclable.clear(batchMessageId.getBatchIndex());
            }
            cmd = Commands.newAck(consumerId, ledgerId, entryId, bitSetRecyclable, ackType, validationError, properties,
                    txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId, batchMessageId.getBatchSize());
            bitSetRecyclable.recycle();
        } else {
            MessageIdImpl singleMessage = (MessageIdImpl) messageId;
            ledgerId = singleMessage.getLedgerId();
            entryId = singleMessage.getEntryId();
            cmd = Commands.newAck(consumerId, ledgerId, entryId, bitSetRecyclable, ackType,
                    validationError, properties, txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId);
        }

        if (ackType == AckType.Cumulative) {
            unAckedMessageTracker.removeMessagesTill(messageId);
        } else {
            unAckedMessageTracker.remove(messageId);
        }
        return cnx().newAckForReceipt(cmd, requestId);
    }

    public Map<MessageIdImpl, List<MessageImpl<T>>> getPossibleSendToDeadLetterTopicMessages() {
        return possibleSendToDeadLetterTopicMessages;
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

}
