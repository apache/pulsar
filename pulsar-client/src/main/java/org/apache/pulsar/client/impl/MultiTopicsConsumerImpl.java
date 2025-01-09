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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotSupportedException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.util.ConsumerName;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.CompletableFutureCancellationHandler;
import org.apache.pulsar.common.util.ExceptionHandler;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTopicsConsumerImpl<T> extends ConsumerBase<T> {

    public static final String DUMMY_TOPIC_NAME_PREFIX = "MultiTopicsConsumer-";

    // Map <topic+partition, consumer>, when get do ACK, consumer will by find by topic name
    protected final ConcurrentHashMap<String, ConsumerImpl<T>> consumers;

    // Map <topic, numPartitions>, store partition number for each topic
    protected final ConcurrentHashMap<String, Integer> partitionedTopics;

    // Queue of partition consumers on which we have stopped calling receiveAsync() because the
    // shared incoming queue was full
    protected final ConcurrentLinkedQueue<ConsumerImpl<T>> pausedConsumers;

    // sum of topicPartitions, simple topic has 1, partitioned topic equals to partition number.
    AtomicInteger allTopicPartitionsNumber;

    private volatile boolean paused = false;
    private final Object pauseMutex = new Object();
    // timeout related to auto check and subscribe partition increasement
    private volatile Timeout partitionsAutoUpdateTimeout = null;
    TopicsPartitionChangedListener topicsPartitionChangedListener;
    CompletableFuture<Void> partitionsAutoUpdateFuture = null;
    private final MultiTopicConsumerStatsRecorderImpl stats;
    private final ConsumerConfigurationData<T> internalConfig;

    private final MessageIdAdv startMessageId;
    private volatile boolean duringSeek = false;
    private final long startMessageRollbackDurationInSec;
    private final ConsumerInterceptors<T> internalConsumerInterceptors;
    MultiTopicsConsumerImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf,
            ExecutorProvider executorProvider, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema,
            ConsumerInterceptors<T> interceptors, boolean createTopicIfDoesNotExist) {
        this(client, DUMMY_TOPIC_NAME_PREFIX + ConsumerName.generateRandomName(), conf, executorProvider,
                subscribeFuture, schema, interceptors, createTopicIfDoesNotExist);
    }

    MultiTopicsConsumerImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf,
            ExecutorProvider executorProvider, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema,
            ConsumerInterceptors<T> interceptors, boolean createTopicIfDoesNotExist, MessageId startMessageId,
            long startMessageRollbackDurationInSec) {
        this(client, DUMMY_TOPIC_NAME_PREFIX + ConsumerName.generateRandomName(), conf, executorProvider,
                subscribeFuture, schema, interceptors, createTopicIfDoesNotExist, startMessageId,
                startMessageRollbackDurationInSec);
    }

    MultiTopicsConsumerImpl(PulsarClientImpl client, String singleTopic, ConsumerConfigurationData<T> conf,
            ExecutorProvider executorProvider, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema,
            ConsumerInterceptors<T> interceptors, boolean createTopicIfDoesNotExist) {
        this(client, singleTopic, conf, executorProvider, subscribeFuture, schema, interceptors,
                createTopicIfDoesNotExist, null, 0);
    }

    MultiTopicsConsumerImpl(PulsarClientImpl client, String singleTopic, ConsumerConfigurationData<T> conf,
            ExecutorProvider executorProvider, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema,
            ConsumerInterceptors<T> interceptors, boolean createTopicIfDoesNotExist, MessageId startMessageId,
            long startMessageRollbackDurationInSec) {
        super(client, singleTopic, conf, Math.max(2, conf.getReceiverQueueSize()), executorProvider, subscribeFuture,
                schema, interceptors);
        if (interceptors != null) {
           this.internalConsumerInterceptors = getInternalConsumerInterceptors(interceptors);
        } else {
            this.internalConsumerInterceptors = null;
        }

        checkArgument(conf.getReceiverQueueSize() > 0,
            "Receiver queue size needs to be greater than 0 for Topics Consumer");

        this.partitionedTopics = new ConcurrentHashMap<>();
        this.consumers = new ConcurrentHashMap<>();
        this.pausedConsumers = new ConcurrentLinkedQueue<>();
        this.allTopicPartitionsNumber = new AtomicInteger(0);
        this.startMessageId = (MessageIdAdv) startMessageId;
        this.startMessageRollbackDurationInSec = startMessageRollbackDurationInSec;
        this.paused = conf.isStartPaused();

        this.internalConfig = getInternalConsumerConfig();
        this.stats = client.getConfiguration().getStatsIntervalSeconds() > 0
                ? new MultiTopicConsumerStatsRecorderImpl(this)
                : null;

        // start track and auto subscribe partition increment
        if (conf.isAutoUpdatePartitions()) {
            topicsPartitionChangedListener = new TopicsPartitionChangedListener();
            partitionsAutoUpdateTimeout = client.timer()
                .newTimeout(partitionsAutoUpdateTimerTask, conf.getAutoUpdatePartitionsIntervalSeconds(),
                        TimeUnit.SECONDS);
        }

        if (conf.getTopicNames().isEmpty()) {
            setState(State.Ready);
            subscribeFuture().complete(MultiTopicsConsumerImpl.this);
            return;
        }

        checkArgument(topicNamesValid(conf.getTopicNames()), "Subscription topics include duplicate items"
                + " or invalid names.");

        List<CompletableFuture<Void>> futures = conf.getTopicNames().stream()
                .map(t -> subscribeAsync(t, createTopicIfDoesNotExist))
                .collect(Collectors.toList());
        FutureUtil.waitForAll(futures)
            .thenAccept(finalFuture -> {
                if (allTopicPartitionsNumber.get() > getCurrentReceiverQueueSize()) {
                    setCurrentReceiverQueueSize(allTopicPartitionsNumber.get());
                }
                setState(State.Ready);
                // We have successfully created N consumers, so we can start receiving messages now
                startReceivingMessages(new ArrayList<>(consumers.values()));
                log.info("[{}] [{}] Created topics consumer with {} sub-consumers",
                    topic, subscription, allTopicPartitionsNumber.get());
                subscribeFuture().complete(MultiTopicsConsumerImpl.this);
            })
            .exceptionally(ex -> {
                log.warn("[{}] Failed to subscribe topics: {}, closing consumer", topic, ex.getMessage());
                closeAsync().whenComplete((res, closeEx) -> {
                    if (closeEx != null) {
                        log.error("[{}] Failed to unsubscribe after failed consumer creation: {}",
                                topic, closeEx.getMessage());
                    }
                    subscribeFuture.completeExceptionally(ex);
                });
                return null;
            });
    }

    // Check topics are valid.
    // - each topic is valid,
    // - topic names are unique.
    private static boolean topicNamesValid(Collection<String> topics) {
        checkState(topics != null && topics.size() >= 1,
            "topics should contain more than 1 topic");

        Set<TopicName> topicNames = new HashSet<>();

        for (String topic : topics) {
            if (!TopicName.isValid(topic)) {
                log.warn("Received invalid topic name: {}", topic);
                return false;
            }
            topicNames.add(TopicName.get(topic));
        }

        // check topic names are unique
        if (topicNames.size() == topics.size()) {
            return true;
        } else {
            log.warn("Topic names not unique. unique/all : {}/{}", topicNames.size(), topics.size());
            return false;
        }
    }

    private void startReceivingMessages(List<ConsumerImpl<T>> newConsumers) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] startReceivingMessages for {} new consumers in topics consumer, state: {}",
                topic, newConsumers.size(), getState());
        }

        if (getState() == State.Ready) {
            newConsumers.forEach(consumer -> {
                consumer.increaseAvailablePermits(consumer.getConnectionHandler().cnx(),
                        consumer.getCurrentReceiverQueueSize());
                internalPinnedExecutor.execute(() -> receiveMessageFromConsumer(consumer, true));
            });
        }
    }

    private void receiveMessageFromConsumer(ConsumerImpl<T> consumer, boolean batchReceive) {
        if (duringSeek) {
            log.info("[{}] Pause receiving messages for topic {} due to seek", subscription, consumer.getTopic());
            return;
        }
        CompletableFuture<List<Message<T>>> messagesFuture;
        if (batchReceive) {
            messagesFuture = consumer.batchReceiveAsync().thenApply(msgs -> ((MessagesImpl<T>) msgs).getMessageList());
        } else {
            messagesFuture = consumer.receiveAsync().thenApply(Collections::singletonList);
        }
        messagesFuture.thenAcceptAsync(messages -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Receive message from sub consumer:{}",
                    topic, subscription, consumer.getTopic());
            }
            // Stop to process the remaining message after the consumer is closed.
            if (getState() == State.Closed) {
                return;
            }
            // Process the message, add to the queue and trigger listener or async callback
            messages.forEach(msg -> {
                final boolean skipDueToSeek = duringSeek;
                if (isValidConsumerEpoch((MessageImpl<T>) msg) && !skipDueToSeek) {
                    messageReceived(consumer, msg);
                } else if (skipDueToSeek) {
                    log.info("[{}] [{}] Skip processing message {} received during seek", topic, subscription,
                            msg.getMessageId());
                }
            });

            int size = incomingMessages.size();
            int maxReceiverQueueSize = getCurrentReceiverQueueSize();
            int sharedQueueResumeThreshold = maxReceiverQueueSize / 2;
            if (size >= maxReceiverQueueSize
                    || (size > sharedQueueResumeThreshold && !pausedConsumers.isEmpty())) {
                // mark this consumer to be resumed later: if No more space left in shared queue,
                // or if any consumer is already paused (to create fair chance for already paused consumers)
                pausedConsumers.add(consumer);

                // Since we didn't get a mutex, the condition on the incoming queue might have changed after
                // we have paused the current consumer. We need to re-check in order to avoid this consumer
                // from getting stalled.
                resumeReceivingFromPausedConsumersIfNeeded();
            } else {
                // Call receiveAsync() if the incoming queue is not full. Because this block is run with
                // thenAcceptAsync, there is no chance for recursion that would lead to stack overflow.
                receiveMessageFromConsumer(consumer, messages.size() > 0);
            }
        }, internalPinnedExecutor).exceptionally(ex -> {
            if (ex instanceof PulsarClientException.AlreadyClosedException
                    || ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                // ignore the exception that happens when the consumer is closed
                return null;
            }
            log.error("Receive operation failed on consumer {} - Retrying later", consumer, ex);
            ((ScheduledExecutorService) client.getScheduledExecutorProvider().getExecutor())
                    .schedule(() -> receiveMessageFromConsumer(consumer, true), 10, TimeUnit.SECONDS);
            return null;
        });
    }

    // Must be called from the internalPinnedExecutor thread
    private void messageReceived(ConsumerImpl<T> consumer, Message<T> message) {
        checkArgument(message instanceof MessageImpl);
        TopicMessageImpl<T> topicMessage = new TopicMessageImpl<>(consumer.getTopic(), message, consumer);

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received message from topics-consumer {}",
                    topic, subscription, message.getMessageId());
        }

        // if asyncReceive is waiting : return message to callback without adding to incomingMessages queue
        CompletableFuture<Message<T>> receivedFuture = nextPendingReceive();
        if (receivedFuture != null) {
            unAckedMessageTracker.add(topicMessage.getMessageId(), topicMessage.getRedeliveryCount());
            final Message<T> interceptMessage = beforeConsume(topicMessage);
            completePendingReceive(receivedFuture, interceptMessage);
        } else if (enqueueMessageAndCheckBatchReceive(topicMessage) && hasPendingBatchReceive()) {
            notifyPendingBatchReceivedCallBack();
        }

        tryTriggerListener();
    }

    @Override
    protected synchronized void messageProcessed(Message<?> msg) {
        unAckedMessageTracker.add(msg.getMessageId(), msg.getRedeliveryCount());
        decreaseIncomingMessageSize(msg);
    }

    private void resumeReceivingFromPausedConsumersIfNeeded() {
        if (incomingMessages.size() <= getCurrentReceiverQueueSize() / 2 && !pausedConsumers.isEmpty()) {
            while (true) {
                ConsumerImpl<T> consumer = pausedConsumers.poll();
                if (consumer == null) {
                    break;
                }

                internalPinnedExecutor.execute(() -> {
                    receiveMessageFromConsumer(consumer, true);
                });
            }
        }
    }

    @Override
    public int minReceiverQueueSize() {
        int size = Math.min(INITIAL_RECEIVER_QUEUE_SIZE, maxReceiverQueueSize);
        if (batchReceivePolicy.getMaxNumMessages() > 0) {
            size = Math.max(size, batchReceivePolicy.getMaxNumMessages());
        }
        if (allTopicPartitionsNumber != null) {
            size = Math.max(allTopicPartitionsNumber.get(), size);
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
            decreaseIncomingMessageSize(message);
            checkState(message instanceof TopicMessageImpl);
            unAckedMessageTracker.add(message.getMessageId(), message.getRedeliveryCount());
            resumeReceivingFromPausedConsumersIfNeeded();
            return beforeConsume(message);
        } catch (Exception e) {
            ExceptionHandler.handleInterruptedException(e);
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    protected Message<T> internalReceive(long timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> message;
        try {
            if (incomingMessages.isEmpty()) {
                expectMoreIncomingMessages();
            }
            message = incomingMessages.poll(timeout, unit);
            if (message != null) {
                decreaseIncomingMessageSize(message);
                checkArgument(message instanceof TopicMessageImpl);
                trackUnAckedMsgIfNoListener(message.getMessageId(), message.getRedeliveryCount());
                message = listener == null ? beforeConsume(message) : message;
            }
            resumeReceivingFromPausedConsumersIfNeeded();
            return message;
        } catch (Exception e) {
            ExceptionHandler.handleInterruptedException(e);
            throw PulsarClientException.unwrap(e);
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
            resumeReceivingFromPausedConsumersIfNeeded();
        });
        return result;
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
                decreaseIncomingMessageSize(message);
                checkState(message instanceof TopicMessageImpl);
                unAckedMessageTracker.add(message.getMessageId(), message.getRedeliveryCount());
                resumeReceivingFromPausedConsumersIfNeeded();
                result.complete(beforeConsume(message));
            }
        });
        return result;
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String, Long> properties,
                                                    TransactionImpl txnImpl) {
        if (!(messageId instanceof TopicMessageId)) {
            return FutureUtil.failedFuture(new PulsarClientException.NotAllowedException(
                    "Only TopicMessageId is allowed to acknowledge for a multi-topics consumer, while messageId is "
                            + messageId.getClass().getName()));
        }

        if (getState() != State.Ready) {
            return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
        }

        ConsumerImpl<T> consumer = consumers.get(((TopicMessageId) messageId).getOwnerTopic());
        if (consumer == null) {
            return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }
        if (ackType == AckType.Cumulative) {
            return consumer.acknowledgeCumulativeAsync(messageId);
        } else {
            return consumer.doAcknowledgeWithTxn(messageId, ackType, properties, txnImpl)
                .thenRun(() -> unAckedMessageTracker.remove(messageId));
        }
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(List<MessageId> messageIdList,
                                                    AckType ackType,
                                                    Map<String, Long> properties,
                                                    TransactionImpl txn) {
        for (MessageId messageId : messageIdList) {
            if (!(messageId instanceof TopicMessageId)) {
                return FutureUtil.failedFuture(new PulsarClientException.NotAllowedException(
                        "Only TopicMessageId is allowed to acknowledge for a multi-topics consumer, while messageId is "
                                + messageId.getClass().getName()));
            }
        }
        List<CompletableFuture<Void>> resultFutures = new ArrayList<>();
        if (ackType == AckType.Cumulative) {
            messageIdList.forEach(messageId -> resultFutures.add(doAcknowledge(messageId, ackType, properties, txn)));
        } else {
            if (getState() != State.Ready) {
                return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
            }
            Map<String, List<MessageId>> topicToMessageIdMap = new HashMap<>();
            for (MessageId messageId : messageIdList) {
                String ownerTopic = ((TopicMessageId) messageId).getOwnerTopic();
                topicToMessageIdMap.putIfAbsent(ownerTopic, new ArrayList<>());
                topicToMessageIdMap.get(ownerTopic).add(messageId);
            }
            final Map<ConsumerImpl<T>, List<MessageId>> consumerToMessageIds = new IdentityHashMap<>();
            for (Map.Entry<String, List<MessageId>> entry : topicToMessageIdMap.entrySet()) {
                ConsumerImpl<T> consumer = consumers.get(entry.getKey());
                if (consumer == null) {
                    return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
                }
                // Trigger the acknowledgment later to avoid sending partial acknowledgments
                consumerToMessageIds.put(consumer, entry.getValue());
            }
            consumerToMessageIds.forEach((consumer, messageIds) -> {
                resultFutures.add(consumer.doAcknowledgeWithTxn(messageIds, ackType, properties, txn)
                        .thenAccept((res) -> messageIdList.forEach(unAckedMessageTracker::remove)));
            });
        }
        return CompletableFuture.allOf(resultFutures.toArray(new CompletableFuture[0]));
    }

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
        if (!(messageId instanceof TopicMessageId)) {
            return FutureUtil.failedFuture(new PulsarClientException.NotAllowedException(
                    "Only TopicMessageId is allowed for reconsumeLater for a multi-topics consumer, while messageId is "
                            + message.getClass().getName()));
        }
        TopicMessageId topicMessageId = (TopicMessageId) messageId;
        if (getState() != State.Ready) {
            return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
        }

        if (ackType == AckType.Cumulative) {
            Consumer<T> individualConsumer = consumers.get(topicMessageId.getOwnerTopic());
            if (individualConsumer != null) {
                return individualConsumer.reconsumeLaterCumulativeAsync(message, delayTime, unit);
            } else {
                return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
            }
        } else {
            ConsumerImpl<T> consumer = consumers.get(topicMessageId.getOwnerTopic());
            return consumer.doReconsumeLater(message, ackType, customProperties, delayTime, unit)
                     .thenRun(() ->unAckedMessageTracker.remove(topicMessageId));
        }
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        checkArgument(messageId instanceof TopicMessageId);
        ConsumerImpl<T> consumer = consumers.get(((TopicMessageId) messageId).getOwnerTopic());
        consumer.negativeAcknowledge(messageId);
        unAckedMessageTracker.remove(messageId);
    }

    @Override
    public void negativeAcknowledge(Message<?> message) {
        MessageId messageId = message.getMessageId();
        checkArgument(messageId instanceof TopicMessageId);
        ConsumerImpl<T> consumer = consumers.get(((TopicMessageId) messageId).getOwnerTopic());
        consumer.negativeAcknowledge(message);
        unAckedMessageTracker.remove(messageId);
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }
        setState(State.Closing);

        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futureList = consumers.values().stream()
            .map(ConsumerImpl::unsubscribeAsync).collect(Collectors.toList());

        FutureUtil.waitForAll(futureList)
            .thenComposeAsync((r) -> {
                setState(State.Closed);
                cleanupMultiConsumer();
                log.info("[{}] [{}] [{}] Unsubscribed Topics Consumer",
                        topic, subscription, consumerName);
                // fail all pending-receive futures to notify application
                return failPendingReceive();
            }, internalPinnedExecutor)
            .whenComplete((r, ex) -> {
                if (ex == null) {
                    unsubscribeFuture.complete(null);
                } else {
                    setState(State.Failed);
                    unsubscribeFuture.completeExceptionally(ex);
                    log.error("[{}] [{}] [{}] Could not unsubscribe Topics Consumer",
                        topic, subscription, consumerName, ex.getCause());
                }
            });

        return unsubscribeFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            if (unAckedMessageTracker != null) {
                unAckedMessageTracker.close();
            }
            return CompletableFuture.completedFuture(null);
        }
        setState(State.Closing);

        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futureList = consumers.values().stream()
            .map(consumer -> consumer.closeAsync().exceptionally(t -> {
                Throwable cause = FutureUtil.unwrapCompletionException(t);
                if (!(cause instanceof PulsarClientException.AlreadyClosedException)) {
                    log.warn("[{}] [{}] Error closing individual consumer", consumer.getTopic(),
                            consumer.getSubscription(), cause);
                }
                return null;
            })).collect(Collectors.toList());

        FutureUtil.waitForAll(futureList)
            .thenComposeAsync((r) -> {
                setState(State.Closed);
                cleanupMultiConsumer();
                log.info("[{}] [{}] Closed Topics Consumer", topic, subscription);
                // fail all pending-receive futures to notify application
                return failPendingReceive();
            }, internalPinnedExecutor)
            .whenComplete((r, ex) -> {
                if (ex == null) {
                    closeFuture.complete(null);
                } else {
                    setState(State.Failed);
                    closeFuture.completeExceptionally(ex);
                    log.error("[{}] [{}] Could not close Topics Consumer", topic, subscription,
                        ex.getCause());
                }
            });

        return closeFuture;
    }

    private void cleanupMultiConsumer() {
        if (unAckedMessageTracker != null) {
            unAckedMessageTracker.close();
            unAckedMessageTracker = null;
        }
        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }
        client.cleanupConsumer(this);
    }

    @Override
    public boolean isConnected() {
        return consumers.values().stream().allMatch(consumer -> consumer.isConnected());
    }

    @Override
    String getHandlerName() {
        return subscription;
    }

    private ConsumerConfigurationData<T> getInternalConsumerConfig() {
        ConsumerConfigurationData<T> internalConsumerConfig = conf.clone();
        internalConsumerConfig.setSubscriptionName(subscription);
        internalConsumerConfig.setConsumerName(consumerName);
        internalConsumerConfig.setMessageListener(null);
        return internalConsumerConfig;
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        internalPinnedExecutor.execute(() -> {
            incomingQueueLock.lock();
            try {
                CONSUMER_EPOCH.incrementAndGet(this);
                consumers.values().stream().forEach(consumer -> {
                    consumer.redeliverUnacknowledgedMessages();
                    consumer.unAckedChunkedMessageIdSequenceMap.clear();
                });
                clearIncomingMessages();
                unAckedMessageTracker.clear();
                resumeReceivingFromPausedConsumersIfNeeded();
            } finally {
                incomingQueueLock.unlock();
            }
        });
    }

    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
        if (messageIds.isEmpty()) {
            return;
        }

        for (MessageId messageId : messageIds) {
            checkArgument(messageId instanceof TopicMessageId);
        }

        if (conf.getSubscriptionType() != SubscriptionType.Shared
                && conf.getSubscriptionType() != SubscriptionType.Key_Shared) {
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        removeExpiredMessagesFromQueue(messageIds);
        messageIds.stream()
                .collect(Collectors.groupingBy(
                        msgId -> ((TopicMessageIdImpl) msgId).getOwnerTopic(), Collectors.toSet()))
                .forEach((topicName, messageIds1) ->
                        consumers.get(topicName).redeliverUnacknowledgedMessages(messageIds1));
        resumeReceivingFromPausedConsumersIfNeeded();
    }

    @Override
    protected void updateAutoScaleReceiverQueueHint() {
        scaleReceiverQueueHint.set(incomingMessages.size() >= getCurrentReceiverQueueSize());
    }

    @Override
    protected void completeOpBatchReceive(OpBatchReceive<T> op) {
        notifyPendingBatchReceivedCallBack(op.future);
        resumeReceivingFromPausedConsumersIfNeeded();
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
        return seekAllAsync(consumer -> consumer.seekAsync(function));
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        final ConsumerImpl<T> internalConsumer;
        if (messageId instanceof TopicMessageId) {
            TopicMessageId topicMessageId = (TopicMessageId) messageId;
            internalConsumer = consumers.get(topicMessageId.getOwnerTopic());
            if (internalConsumer == null) {
                return FutureUtil.failedFuture(new PulsarClientException.NotAllowedException(
                        "The owner topic " + topicMessageId.getOwnerTopic() + " is not subscribed"));
            }
        } else {
            internalConsumer = null;
        }
        if (internalConsumer == null && isIllegalMultiTopicsMessageId(messageId)) {
            return FutureUtil.failedFuture(
                    new PulsarClientException("Illegal messageId, messageId can only be earliest/latest")
            );
        }

        if (internalConsumer == null) {
            return seekAllAsync(consumer -> consumer.seekAsync(messageId));
        } else {
            return seekAsyncInternal(Collections.singleton(internalConsumer), __ -> __.seekAsync(messageId));
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return seekAllAsync(consumer -> consumer.seekAsync(timestamp));
    }

    private CompletableFuture<Void> seekAsyncInternal(Collection<ConsumerImpl<T>> consumers,
                                                      Function<ConsumerImpl<T>, CompletableFuture<Void>> seekFunc) {
        beforeSeek();
        final CompletableFuture<Void> future = new CompletableFuture<>();
        FutureUtil.waitForAll(consumers.stream().map(seekFunc).collect(Collectors.toList()))
                .whenComplete((__, e) -> afterSeek(future, e));
        return future;
    }

    private CompletableFuture<Void> seekAllAsync(Function<ConsumerImpl<T>, CompletableFuture<Void>> seekFunc) {
        return seekAsyncInternal(consumers.values(), seekFunc);
    }

    private void beforeSeek() {
        duringSeek = true;
        unAckedMessageTracker.clear();
        clearIncomingMessages();
    }

    private void afterSeek(CompletableFuture<Void> seekFuture, @Nullable Throwable throwable) {
        duringSeek = false;
        log.info("[{}] Resume receiving messages for {} since seek is done", subscription, consumers.keySet());
        startReceivingMessages(new ArrayList<>(consumers.values()));
        if (throwable == null) {
            seekFuture.complete(null);
        } else {
            seekFuture.completeExceptionally(throwable);
        }
    }

    @Override
    public int getAvailablePermits() {
        return consumers.values().stream().mapToInt(ConsumerImpl::getAvailablePermits).sum();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return consumers.values().stream().allMatch(Consumer::hasReachedEndOfTopic);
    }

    public boolean hasMessageAvailable() throws PulsarClientException {
        try {
            return hasMessageAvailableAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        if (numMessagesInQueue() > 0) {
            return CompletableFuture.completedFuture(true);
        }
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        final AtomicBoolean hasMessageAvailable = new AtomicBoolean(false);
        for (ConsumerImpl<T> consumer : consumers.values()) {
            futureList.add(consumer.hasMessageAvailableAsync().thenAccept(isAvailable -> {
                if (isAvailable) {
                    hasMessageAvailable.compareAndSet(false, true);
                }
            }));
        }
        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        FutureUtil.waitForAll(futureList).whenComplete((result, exception) -> {
            if (exception != null) {
                completableFuture.completeExceptionally(exception);
            } else {
                completableFuture.complete(hasMessageAvailable.get() || numMessagesInQueue() > 0);
            }
        });
        return completableFuture;
    }

    @Override
    public int numMessagesInQueue() {
        return incomingMessages.size() + consumers.values().stream().mapToInt(ConsumerImpl::numMessagesInQueue).sum();
    }

    @Override
    public synchronized ConsumerStats getStats() {
        if (stats == null) {
            return null;
        }
        stats.reset();

        consumers.forEach((partition, consumer) -> stats.updateCumulativeStats(partition, consumer.getStats()));
        return stats;
    }

    @Override
    public UnAckedMessageTracker getUnAckedMessageTracker() {
        return unAckedMessageTracker;
    }

    private void removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
        Message<T> peek = incomingMessages.peek();
        if (peek != null) {
            if (!messageIds.contains(peek.getMessageId())) {
                // first message is not expired, then no message is expired in queue.
                return;
            }

            // try not to remove elements that are added while we remove
            Message<T> message = incomingMessages.poll();
            checkState(message instanceof TopicMessageImpl);
            while (message != null) {
                decreaseIncomingMessageSize(message);
                MessageId messageId = message.getMessageId();
                if (!messageIds.contains(messageId)) {
                    messageIds.add(messageId);
                    break;
                }
                message.release();
                message = incomingMessages.poll();
            }
        }
    }

    private TopicName getTopicName(String topic) {
        try {
            return TopicName.get(topic);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String getFullTopicName(String topic) {
        TopicName topicName = getTopicName(topic);
        return (topicName != null) ? topicName.toString() : null;
    }

    private void removeTopic(String topic) {
        String fullTopicName = getFullTopicName(topic);
        if (fullTopicName != null) {
            partitionedTopics.remove(topic);
        }
    }

    /***
     * Subscribe one more given topic.
     * @param topicName topic name without the partition suffix.
     */
    public CompletableFuture<Void> subscribeAsync(String topicName, boolean createTopicIfDoesNotExist) {
        TopicName topicNameInstance = getTopicName(topicName);
        if (topicNameInstance == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Topic name not valid"));
        }
        String fullTopicName = topicNameInstance.toString();
        if (consumers.containsKey(fullTopicName)
                || partitionedTopics.containsKey(topicNameInstance.getPartitionedTopicName())) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Already subscribed to " + topicName));
        }

        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }

        CompletableFuture<Void> subscribeResult = new CompletableFuture<>();

        client.getPartitionedTopicMetadata(topicName, true, false)
                .thenAccept(metadata -> subscribeTopicPartitions(subscribeResult, fullTopicName, metadata.partitions,
                    createTopicIfDoesNotExist))
                .exceptionally(ex1 -> {
                    log.warn("[{}] Failed to get partitioned topic metadata: {}", fullTopicName, ex1.getMessage());
                    subscribeResult.completeExceptionally(ex1);
                    return null;
                });

        return subscribeResult;
    }

    // create consumer for a single topic with already known partitions.
    // first create a consumer with no topic, then do subscription for already know partitionedTopic.
    public static <T> MultiTopicsConsumerImpl<T> createPartitionedConsumer(
            PulsarClientImpl client,
            ConsumerConfigurationData<T> conf,
            ExecutorProvider executorProvider,
            CompletableFuture<Consumer<T>> subscribeFuture,
            int numPartitions,
            Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        checkArgument(conf.getTopicNames().size() == 1,
                "Should have only 1 topic for partitioned consumer");

        // get topic name, then remove it from conf, so constructor will create a consumer with no topic.
        ConsumerConfigurationData<T> cloneConf = conf.clone();
        String topicName = cloneConf.getSingleTopic();
        cloneConf.getTopicNames().remove(topicName);

        CompletableFuture<Consumer<T>> future = new CompletableFuture<>();
        MultiTopicsConsumerImpl<T> consumer = new MultiTopicsConsumerImpl<T>(client, topicName, cloneConf,
                executorProvider, future, schema, interceptors, true /* createTopicIfDoesNotExist */);

        future.thenCompose(c -> ((MultiTopicsConsumerImpl<T>) c).subscribeAsync(topicName, numPartitions))
            .thenRun(()-> subscribeFuture.complete(consumer))
            .exceptionally(e -> {
                log.warn("Failed subscription for createPartitionedConsumer: {} {}, e:{}",
                    topicName, numPartitions,  e);
                consumer.cleanupMultiConsumer();
                subscribeFuture.completeExceptionally(
                    PulsarClientException.wrap(((Throwable) e).getCause(),
                            String.format("Failed to subscribe %s with %d partitions", topicName, numPartitions)));
                return null;
            });
        return consumer;
    }

    // subscribe one more given topic, but already know the numberPartitions
    CompletableFuture<Void> subscribeAsync(String topicName, int numberPartitions) {
        TopicName topicNameInstance = getTopicName(topicName);
        if (topicNameInstance == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Topic name not valid"));
        }
        String fullTopicName = topicNameInstance.toString();
        if (consumers.containsKey(fullTopicName)) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Already subscribed to " + topicName));
        }
        if (!topicNameInstance.isPartitioned()
                && partitionedTopics.containsKey(topicNameInstance.getPartitionedTopicName())) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Already subscribed to " + topicName));
        }

        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }

        CompletableFuture<Void> subscribeResult = new CompletableFuture<>();
        subscribeTopicPartitions(subscribeResult, fullTopicName, numberPartitions,
                true /* createTopicIfDoesNotExist */);

        return subscribeResult;
    }

    private void subscribeTopicPartitions(CompletableFuture<Void> subscribeResult, String topicName, int numPartitions,
            boolean createIfDoesNotExist) {
        client.preProcessSchemaBeforeSubscribe(client, schema, topicName)
                .thenAccept(schema -> {
                    doSubscribeTopicPartitions(schema, subscribeResult, topicName, numPartitions, createIfDoesNotExist);
                }).exceptionally(cause -> {
                    subscribeResult.completeExceptionally(cause);
                    return null;
                });
    }

    private void doSubscribeTopicPartitions(Schema<T> schema,
                                            CompletableFuture<Void> subscribeResult,
                                            String topicName,
                                            int numPartitions,
                                            boolean createIfDoesNotExist) {
        if (log.isDebugEnabled()) {
            log.debug("Subscribe to topic {} metadata.partitions: {}", topicName, numPartitions);
        }

        CompletableFuture<Void> subscribeAllPartitionsFuture;
        if (numPartitions != PartitionedTopicMetadata.NON_PARTITIONED) {
            // Below condition is true if subscribeAsync() has been invoked second time with same
            // topicName before the first invocation had reached this point.
            boolean isTopicBeingSubscribedForInOtherThread =
                    partitionedTopics.putIfAbsent(topicName, numPartitions) != null;
            if (isTopicBeingSubscribedForInOtherThread) {
                String errorMessage = String.format("[%s] Failed to subscribe for topic [%s] in topics consumer. "
                    + "Topic is already being subscribed for in other thread.", topic, topicName);
                log.warn(errorMessage);
                subscribeResult.completeExceptionally(new PulsarClientException(errorMessage));
                return;
            }
            allTopicPartitionsNumber.addAndGet(numPartitions);

            int receiverQueueSize = Math.min(conf.getReceiverQueueSize(),
                conf.getMaxTotalReceiverQueueSizeAcrossPartitions() / numPartitions);
            ConsumerConfigurationData<T> configurationData = getInternalConsumerConfig();
            configurationData.setReceiverQueueSize(receiverQueueSize);

            CompletableFuture<List<Integer>> partitionsFuture;
            if (createIfDoesNotExist || !TopicName.get(topicName).isPersistent()) {
                partitionsFuture = CompletableFuture.completedFuture(IntStream.range(0, numPartitions)
                        .mapToObj(i -> Integer.valueOf(i))
                        .collect(Collectors.toList()));
            } else {
                partitionsFuture = getExistsPartitions(topicName.toString());
            }
            subscribeAllPartitionsFuture = partitionsFuture.thenCompose(partitions -> {
                if (partitions.isEmpty()) {
                    partitionedTopics.remove(topicName, numPartitions);
                    return CompletableFuture.completedFuture(null);
                }
                List<CompletableFuture<Consumer<T>>> subscribeList = new ArrayList<>();
                for (int partitionIndex : partitions) {
                    String partitionName = TopicName.get(topicName).getPartition(partitionIndex).toString();
                    CompletableFuture<Consumer<T>> subFuture = new CompletableFuture<>();
                    configurationData.setStartPaused(paused);
                    ConsumerImpl<T> newConsumer = createInternalConsumer(configurationData, partitionName,
                            partitionIndex, subFuture, createIfDoesNotExist, schema);
                    synchronized (pauseMutex) {
                        if (paused) {
                            newConsumer.pause();
                        } else {
                            newConsumer.resume();
                        }
                        Consumer originalValue = consumers.putIfAbsent(newConsumer.getTopic(), newConsumer);
                        if (originalValue != null) {
                            newConsumer.closeAsync().exceptionally(ex -> {
                                log.error("[{}] [{}] Failed to close the orphan consumer",
                                        partitionName, subscription, ex);
                                return null;
                            });
                        }
                    }
                    subscribeList.add(subFuture);
                }
                return FutureUtil.waitForAll(subscribeList);
            });
        } else {
            allTopicPartitionsNumber.incrementAndGet();

            CompletableFuture<Consumer<T>> subscribeFuture = new CompletableFuture<>();
            subscribeAllPartitionsFuture = subscribeFuture.thenAccept(__ -> {});

            synchronized (pauseMutex) {
                consumers.compute(topicName, (key, existingValue) -> {
                    if (existingValue != null) {
                        String errorMessage =
                                String.format("[%s] Failed to subscribe for topic [%s] in topics consumer. "
                                + "Topic is already being subscribed for in other thread.", topic, topicName);
                        log.warn(errorMessage);
                        subscribeResult.completeExceptionally(new PulsarClientException(errorMessage));
                        return existingValue;
                    } else {
                        internalConfig.setStartPaused(paused);
                        ConsumerImpl<T> newConsumer = createInternalConsumer(internalConfig, topicName,
                                -1, subscribeFuture, createIfDoesNotExist, schema);
                        if (paused) {
                            newConsumer.pause();
                        } else {
                            newConsumer.resume();
                        }
                        return newConsumer;
                    }
                });
            }

        }

        subscribeAllPartitionsFuture.thenAccept(finalFuture -> {
                if (allTopicPartitionsNumber.get() > getCurrentReceiverQueueSize()) {
                    setCurrentReceiverQueueSize(allTopicPartitionsNumber.get());
                }

                // We have successfully created new consumers, so we can start receiving messages for them
                startReceivingMessages(consumers.values().stream()
                                .filter(consumer1 -> {
                                    String consumerTopicName = consumer1.getTopic();
                                    return TopicName.get(consumerTopicName).getPartitionedTopicName().equals(
                                            TopicName.get(topicName).getPartitionedTopicName());
                                })
                                .collect(Collectors.toList()));

                subscribeResult.complete(null);
                log.info("[{}] [{}] Success subscribe new topic {} in topics consumer, partitions: {},"
                                + " allTopicPartitionsNumber: {}",
                    topic, subscription, topicName, numPartitions, allTopicPartitionsNumber.get());
                return;
            })
            .exceptionally(ex -> {
                log.warn("[{}] Failed to subscribe for topic [{}] in topics consumer {}", topic, topicName,
                        ex.getMessage());
                handleSubscribeOneTopicError(topicName, ex, subscribeResult);
                return null;
            });
    }

    private ConsumerImpl<T> createInternalConsumer(ConsumerConfigurationData<T> configurationData, String partitionName,
                                                   int partitionIndex, CompletableFuture<Consumer<T>> subFuture,
                                                   boolean createIfDoesNotExist, Schema<T> schema) {
        BatchReceivePolicy internalBatchReceivePolicy = BatchReceivePolicy.builder()
                .maxNumMessages(Math.max(configurationData.getReceiverQueueSize() / 2, 1))
                .maxNumBytes(-1)
                .timeout(1, TimeUnit.MILLISECONDS)
                .build();
        configurationData.setBatchReceivePolicy(internalBatchReceivePolicy);
        return ConsumerImpl.newConsumerImpl(client, partitionName,
                configurationData, client.externalExecutorProvider(),
                partitionIndex, true, listener != null, subFuture,
                startMessageId, schema, this.internalConsumerInterceptors,
                createIfDoesNotExist, startMessageRollbackDurationInSec);
    }

    // handling failure during subscribe new topic, unsubscribe success created partitions
    protected void handleSubscribeOneTopicError(String topicName,
                                              Throwable error,
                                              CompletableFuture<Void> subscribeFuture) {
        log.warn("[{}] Failed to subscribe for topic [{}] in topics consumer {}", topic, topicName, error.getMessage());
        client.externalExecutorProvider().getExecutor().execute(() -> {
            AtomicInteger toCloseNum = new AtomicInteger(0);
            List<ConsumerImpl> filterConsumers = consumers.values().stream().filter(consumer1 -> {
                String consumerTopicName = consumer1.getTopic();
                if (TopicName.get(consumerTopicName).getPartitionedTopicName()
                        .equals(TopicName.get(topicName).getPartitionedTopicName())) {
                    toCloseNum.incrementAndGet();
                    return true;
                } else {
                    return false;
                }
            }).collect(Collectors.toList());

            if (filterConsumers.isEmpty()) {
                subscribeFuture.completeExceptionally(error);
                return;
            }

            filterConsumers.forEach(consumer2 -> {
                consumer2.closeAsync().whenComplete((r, ex) -> {
                    consumer2.subscribeFuture().completeExceptionally(error);
                    allTopicPartitionsNumber.decrementAndGet();
                    consumers.remove(consumer2.getTopic());
                    if (toCloseNum.decrementAndGet() == 0) {
                        log.warn("[{}] Failed to subscribe for topic [{}] in topics consumer, subscribe error: {}",
                            topic, topicName, error.getMessage());
                        removeTopic(topicName);
                        subscribeFuture.completeExceptionally(error);
                    }
                    return;
                });
            });
        });
    }

    // un-subscribe a given topic
    public CompletableFuture<Void> unsubscribeAsync(String topicName) {
        checkArgument(TopicName.isValid(topicName), "Invalid topic name:" + topicName);

        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }

        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }

        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        String topicPartName = TopicName.get(topicName).getPartitionedTopicName();

        List<ConsumerImpl<T>> consumersToUnsub = consumers.values().stream()
            .filter(consumer -> {
                String consumerTopicName = consumer.getTopic();
                return TopicName.get(consumerTopicName).getPartitionedTopicName().equals(topicPartName);
            }).collect(Collectors.toList());

        List<CompletableFuture<Void>> futureList = consumersToUnsub.stream()
            .map(ConsumerImpl::unsubscribeAsync).collect(Collectors.toList());

        FutureUtil.waitForAll(futureList)
            .whenComplete((r, ex) -> {
                if (ex == null) {
                    consumersToUnsub.forEach(consumer1 -> {
                        consumers.remove(consumer1.getTopic());
                        pausedConsumers.remove(consumer1);
                        allTopicPartitionsNumber.decrementAndGet();
                    });

                    removeTopic(topicName);
                    if (unAckedMessageTracker instanceof UnAckedTopicMessageTracker) {
                        ((UnAckedTopicMessageTracker) unAckedMessageTracker).removeTopicMessages(topicName);
                    }

                    unsubscribeFuture.complete(null);
                    log.info("[{}] [{}] [{}] Unsubscribed Topics Consumer, allTopicPartitionsNumber: {}",
                        topicName, subscription, consumerName, allTopicPartitionsNumber);
                } else {
                    unsubscribeFuture.completeExceptionally(ex);
                    setState(State.Failed);
                    log.error("[{}] [{}] [{}] Could not unsubscribe Topics Consumer",
                        topicName, subscription, consumerName, ex.getCause());
                }
            });

        return unsubscribeFuture;
    }


    // get topics name
    public List<String> getPartitionedTopics() {
        return partitionedTopics.keySet().stream().collect(Collectors.toList());
    }

    // get partitioned topics name
    public List<String> getPartitions() {
        return consumers.keySet().stream().collect(Collectors.toList());
    }

    // get partitioned consumers
    public List<ConsumerImpl<T>> getConsumers() {
        return consumers.values().stream().collect(Collectors.toList());
    }

    // get all partitions that in the topics map
    int getPartitionsOfTheTopicMap() {
        return partitionedTopics.values().stream().mapToInt(Integer::intValue).sum();
    }

    @Override
    public void pause() {
        synchronized (pauseMutex) {
            paused = true;
            consumers.forEach((name, consumer) -> consumer.pause());
        }
    }

    @Override
    public void resume() {
        synchronized (pauseMutex) {
            paused = false;
            consumers.forEach((name, consumer) -> consumer.resume());
        }
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        long lastDisconnectedTimestamp = 0;
        Optional<ConsumerImpl<T>> c = consumers.values().stream()
                .max(Comparator.comparingLong(ConsumerImpl::getLastDisconnectedTimestamp));
        if (c.isPresent()) {
            lastDisconnectedTimestamp = c.get().getLastDisconnectedTimestamp();
        }
        return lastDisconnectedTimestamp;
    }

    // This listener is triggered when topics partitions are updated.
    private class TopicsPartitionChangedListener implements PartitionsChangedListener {
        // Check partitions changes of passed in topics, and subscribe new added partitions.
        @Override
        public CompletableFuture<Void> onTopicsExtended(Collection<String> topicsExtended) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (topicsExtended.isEmpty()) {
                future.complete(null);
                return future;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}]  run onTopicsExtended: {}, size: {}",
                    topic, topicsExtended.toString(), topicsExtended.size());
            }

            List<CompletableFuture<Void>> futureList = Lists.newArrayListWithExpectedSize(topicsExtended.size());
            topicsExtended.forEach(topic -> futureList.add(subscribeIncreasedTopicPartitions(topic)));
            FutureUtil.waitForAll(futureList)
                .thenAccept(finalFuture -> future.complete(null))
                .exceptionally(ex -> {
                    log.warn("[{}] Failed to subscribe increased topics partitions: {}", topic, ex.getMessage());
                    future.completeExceptionally(ex);
                    return null;
                });

            return future;
        }
    }

    // subscribe increased partitions for a given topic
    private CompletableFuture<Void> subscribeIncreasedTopicPartitions(String topicName) {
        int oldPartitionNumber = partitionedTopics.get(topicName);

        return client.getPartitionsForTopic(topicName).thenCompose(list -> {
            int currentPartitionNumber = Long.valueOf(list.stream()
                    .filter(t -> TopicName.get(t).isPartitioned()).count()).intValue();

            if (log.isDebugEnabled()) {
                log.debug("[{}] partitions number. old: {}, new: {}",
                    topicName, oldPartitionNumber, currentPartitionNumber);
            }

            if (oldPartitionNumber == currentPartitionNumber) {
                // topic partition number not changed
                return CompletableFuture.completedFuture(null);
            } else if (currentPartitionNumber == PartitionedTopicMetadata.NON_PARTITIONED) {
                // The topic was initially partitioned but then it was deleted. We keep it in the topics
                partitionedTopics.put(topicName, 0);

                allTopicPartitionsNumber.addAndGet(-oldPartitionNumber);
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (Iterator<Map.Entry<String, ConsumerImpl<T>>> it = consumers.entrySet().iterator(); it.hasNext();) {
                    Map.Entry<String, ConsumerImpl<T>> e = it.next();
                    String partitionedTopicName = TopicName.get(e.getKey()).getPartitionedTopicName();

                    // Remove the consumers that belong to the deleted partitioned topic
                    if (partitionedTopicName.equals(topicName)) {
                        futures.add(e.getValue().closeAsync());
                        consumers.remove(e.getKey());
                    }
                }

                return FutureUtil.waitForAll(futures);
            } else if (oldPartitionNumber < currentPartitionNumber) {
                allTopicPartitionsNumber.addAndGet(currentPartitionNumber - oldPartitionNumber);
                partitionedTopics.put(topicName, currentPartitionNumber);
                List<String> newPartitions = list.subList(oldPartitionNumber, currentPartitionNumber);
                // subscribe new added partitions
                List<CompletableFuture<Consumer<T>>> futureList = newPartitions
                    .stream()
                    .map(partitionName -> {
                        int partitionIndex = TopicName.getPartitionIndex(partitionName);
                        CompletableFuture<Consumer<T>> subFuture = new CompletableFuture<>();
                        ConsumerConfigurationData<T> configurationData = getInternalConsumerConfig();
                        configurationData.setStartPaused(paused);
                        ConsumerImpl<T> newConsumer = createInternalConsumer(configurationData, partitionName,
                                partitionIndex, subFuture, true, schema);
                        synchronized (pauseMutex) {
                            if (paused) {
                                newConsumer.pause();
                            } else {
                                newConsumer.resume();
                            }
                            consumers.putIfAbsent(newConsumer.getTopic(), newConsumer);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] create consumer {} for partitionName: {}",
                                    topicName, newConsumer.getTopic(), partitionName);
                        }
                        return subFuture;
                    })
                    .collect(Collectors.toList());
                // call interceptor
                onPartitionsChange(topicName, currentPartitionNumber);
                // wait for all partitions subscribe future complete, then startReceivingMessages
                return FutureUtil.waitForAll(futureList)
                    .thenAccept(finalFuture -> {
                        List<ConsumerImpl<T>> newConsumerList = newPartitions.stream()
                            .map(partitionTopic -> consumers.get(partitionTopic))
                            .collect(Collectors.toList());
                        startReceivingMessages(newConsumerList);
                    });
            } else {
                log.error("[{}] not support shrink topic partitions. old: {}, new: {}",
                    topicName, oldPartitionNumber, currentPartitionNumber);
                return FutureUtil.failedFuture(new NotSupportedException("not support shrink topic partitions"));
            }
        }).exceptionally(throwable -> {
            log.warn("Failed to get partitions for topic to determine if new partitions are added", throwable);
            return null;
        });
    }

    private TimerTask partitionsAutoUpdateTimerTask = new TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
            try {
                if (timeout.isCancelled() || getState() != State.Ready) {
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] run partitionsAutoUpdateTimerTask", topic);
                }

                // if last auto update not completed yet, do nothing.
                if (partitionsAutoUpdateFuture == null || partitionsAutoUpdateFuture.isDone()) {
                    partitionsAutoUpdateFuture =
                            topicsPartitionChangedListener.onTopicsExtended(partitionedTopics.keySet());
                }
            } catch (Throwable th) {
                log.warn("Encountered error in partition auto update timer task for multi-topic consumer."
                        + " Another task will be scheduled.", th);
            } finally {
                // schedule the next re-check task
                partitionsAutoUpdateTimeout = client.timer()
                        .newTimeout(partitionsAutoUpdateTimerTask, conf.getAutoUpdatePartitionsIntervalSeconds(),
                                TimeUnit.SECONDS);
            }
        }
    };

    @VisibleForTesting
    public Timeout getPartitionsAutoUpdateTimeout() {
        return partitionsAutoUpdateTimeout;
    }

    @Deprecated
    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        CompletableFuture<MessageId> returnFuture = new CompletableFuture<>();

        Map<String, CompletableFuture<MessageId>> messageIdFutures = consumers.entrySet().stream()
            .map(entry -> Pair.of(entry.getKey(), entry.getValue().getLastMessageIdAsync()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        CompletableFuture
            .allOf(messageIdFutures.values().toArray(new CompletableFuture<?>[0]))
            .whenComplete((ignore, ex) -> {
                Builder<String, MessageId> builder = ImmutableMap.builder();
                messageIdFutures.forEach((key, future) -> {
                    MessageId messageId;
                    try {
                        messageId = future.get();
                    } catch (Exception e) {
                        log.warn("[{}] Exception when topic {} getLastMessageId.", key, e);
                        messageId = MessageId.earliest;
                    }
                    builder.put(key, messageId);
                });
                returnFuture.complete(new MultiMessageIdImpl(builder.build()));
            });

        return returnFuture;
    }

    @Override
    public CompletableFuture<List<TopicMessageId>> getLastMessageIdsAsync() {
        final List<CompletableFuture<List<TopicMessageId>>> futures = consumers.values().stream()
                .map(ConsumerImpl::getLastMessageIdsAsync)
                .collect(Collectors.toList());
        return FutureUtil.waitForAll(futures).thenApply(__ -> {
            final List<TopicMessageId> messageIds = new ArrayList<>();
            futures.stream().map(CompletableFuture::join).forEach(messageIds::addAll);
            return messageIds;
        });
    }

    private static final Logger log = LoggerFactory.getLogger(MultiTopicsConsumerImpl.class);

    public static boolean isIllegalMultiTopicsMessageId(MessageId messageId) {
        //only support earliest/latest
        return !messageId.equals(MessageId.earliest) && !messageId.equals(MessageId.latest);
    }

    public void tryAcknowledgeMessage(Message<T> msg) {
        if (msg != null) {
            acknowledgeCumulativeAsync(msg)
                    .exceptionally(ex -> {
                        log.warn("[{}][{}] acknowledge message {} cumulative fail.", topic, subscription,
                                msg.getMessageId(), ex);
                        return null;
                    });
        }
    }

    @Override
    protected void setCurrentReceiverQueueSize(int newSize) {
        checkArgument(newSize > 0, "receiver queue size should larger than 0");
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] setMaxReceiverQueueSize={}, previous={}", topic, subscription,
                    newSize, getCurrentReceiverQueueSize());
        }
        CURRENT_RECEIVER_QUEUE_SIZE_UPDATER.set(this, newSize);
        resumeReceivingFromPausedConsumersIfNeeded();
    }

    /**
     * Get the exists partitions of a partitioned topic, the result does not contain the partitions which has not been
     * created yet(in other words, the partitions that do not exist in the response of "pulsar-admin topics list").
     * @return sorted partitions list if it is a partitioned topic; @return an empty list if it is a non-partitioned
     * topic.
     */
    private CompletableFuture<List<Integer>> getExistsPartitions(String topic) {
        TopicName topicName = TopicName.get(topic);
        if (!topicName.isPersistent()) {
            return FutureUtil.failedFuture(new IllegalArgumentException("The method getExistsPartitions"
                    + " does not support non-persistent topic yet."));
        }
        return client.getLookup().getTopicsUnderNamespace(topicName.getNamespaceObject(),
                CommandGetTopicsOfNamespace.Mode.PERSISTENT,
                TopicName.getPattern(topicName.getPartitionedTopicName()),
                null).thenApply(getTopicsResult -> {
            if (getTopicsResult.getNonPartitionedOrPartitionTopics() == null
                    || getTopicsResult.getNonPartitionedOrPartitionTopics().isEmpty()) {
                return Collections.emptyList();
            }
            // If broker version is less than "2.11.x", it does not support broker-side pattern check, so append
            // a client-side pattern check.
            // If lookup service is typed HttpLookupService, the HTTP API does not support broker-side pattern
            // check yet, so append a client-side pattern check.
            Predicate<String> clientSideFilter;
            if (getTopicsResult.isFiltered()) {
                clientSideFilter = __ -> true;
            } else {
                clientSideFilter =
                        tp -> Pattern.compile(TopicName.getPartitionPattern(topic)).matcher(tp).matches();
            }
            ArrayList<Integer> list = new ArrayList<>(getTopicsResult.getNonPartitionedOrPartitionTopics().size());
            for (String partition : getTopicsResult.getNonPartitionedOrPartitionTopics()) {
                int partitionIndex = TopicName.get(partition).getPartitionIndex();
                if (partitionIndex < 0) {
                    // It is not a partition.
                    continue;
                }
                if (clientSideFilter.test(partition)) {
                    list.add(partitionIndex);
                }
            }
            Collections.sort(list);
            return list;
        });
    }

    private ConsumerInterceptors<T> getInternalConsumerInterceptors(ConsumerInterceptors<T> multiTopicInterceptors) {
        return new ConsumerInterceptors<T>(new ArrayList<>()) {

            @Override
            public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
                return message;
            }

            @Override
            public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
                multiTopicInterceptors.onAcknowledge(consumer, messageId, exception);
            }

            @Override
            public void onAcknowledgeCumulative(Consumer<T> consumer,
                                                MessageId messageId, Throwable exception) {
                multiTopicInterceptors.onAcknowledgeCumulative(consumer, messageId, exception);
            }

            @Override
            public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> set) {
                multiTopicInterceptors.onNegativeAcksSend(consumer, set);
            }

            @Override
            public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> set) {
                multiTopicInterceptors.onAckTimeoutSend(consumer, set);
            }

            @Override
            public void onPartitionsChange(String topicName, int partitions) {
                multiTopicInterceptors.onPartitionsChange(topicName, partitions);
            }

            @Override
            public void close() throws IOException {
                multiTopicInterceptors.close();
            }
        };
    }
}
