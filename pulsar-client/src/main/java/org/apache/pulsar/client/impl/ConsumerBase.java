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

import com.google.common.collect.Queues;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import io.netty.util.Timeout;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.util.ConsumerName;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConsumerBase<T> extends HandlerState implements Consumer<T> {

    protected final String subscription;
    protected final ConsumerConfigurationData<T> conf;
    protected final String consumerName;
    protected final CompletableFuture<Consumer<T>> subscribeFuture;
    protected final MessageListener<T> listener;
    protected final ConsumerEventListener consumerEventListener;
    protected final ExecutorProvider executorProvider;
    protected final ScheduledExecutorService pinnedExecutor;
    final BlockingQueue<Message<T>> incomingMessages;
    protected ConcurrentOpenHashMap<MessageIdImpl, MessageIdImpl[]> unAckedChunkedMessageIdSequenceMap;
    protected final ConcurrentLinkedQueue<CompletableFuture<Message<T>>> pendingReceives;
    protected int maxReceiverQueueSize;
    protected final Schema<T> schema;
    protected final ConsumerInterceptors<T> interceptors;
    protected final BatchReceivePolicy batchReceivePolicy;
    protected ConcurrentLinkedQueue<OpBatchReceive<T>> pendingBatchReceives;
    private static final AtomicLongFieldUpdater<ConsumerBase> INCOMING_MESSAGES_SIZE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ConsumerBase.class, "incomingMessagesSize");
    protected volatile long incomingMessagesSize = 0;
    protected volatile Timeout batchReceiveTimeout = null;
    protected final Lock reentrantLock = new ReentrantLock();

    protected ConsumerBase(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                           int receiverQueueSize, ExecutorProvider executorProvider,
                           CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema, ConsumerInterceptors interceptors) {
        super(client, topic);
        this.maxReceiverQueueSize = receiverQueueSize;
        this.subscription = conf.getSubscriptionName();
        this.conf = conf;
        this.consumerName = conf.getConsumerName() == null ? ConsumerName.generateRandomName() : conf.getConsumerName();
        this.subscribeFuture = subscribeFuture;
        this.listener = conf.getMessageListener();
        this.consumerEventListener = conf.getConsumerEventListener();
        // Always use growable queue since items can exceed the advertised size
        this.incomingMessages = new GrowableArrayBlockingQueue<>();
        this.unAckedChunkedMessageIdSequenceMap = new ConcurrentOpenHashMap<>();
        this.executorProvider = executorProvider;
        this.pinnedExecutor = (ScheduledExecutorService) executorProvider.getExecutor();
        this.pendingReceives = Queues.newConcurrentLinkedQueue();
        this.schema = schema;
        this.interceptors = interceptors;
        if (conf.getBatchReceivePolicy() != null) {
            BatchReceivePolicy userBatchReceivePolicy = conf.getBatchReceivePolicy();
            if (userBatchReceivePolicy.getMaxNumMessages() > this.maxReceiverQueueSize) {
                this.batchReceivePolicy = BatchReceivePolicy.builder()
                        .maxNumMessages(this.maxReceiverQueueSize)
                        .maxNumBytes(userBatchReceivePolicy.getMaxNumBytes())
                        .timeout((int) userBatchReceivePolicy.getTimeoutMs(), TimeUnit.MILLISECONDS)
                        .build();
                log.warn("BatchReceivePolicy maxNumMessages: {} is greater than maxReceiverQueueSize: {}, " +
                        "reset to maxReceiverQueueSize. batchReceivePolicy: {}",
                        userBatchReceivePolicy.getMaxNumMessages(), this.maxReceiverQueueSize,
                        this.batchReceivePolicy.toString());
            } else if (userBatchReceivePolicy.getMaxNumMessages() <= 0 && userBatchReceivePolicy.getMaxNumBytes() <= 0) {
                this.batchReceivePolicy = BatchReceivePolicy.builder()
                        .maxNumMessages(BatchReceivePolicy.DEFAULT_POLICY.getMaxNumMessages())
                        .maxNumBytes(BatchReceivePolicy.DEFAULT_POLICY.getMaxNumBytes())
                        .timeout((int) userBatchReceivePolicy.getTimeoutMs(), TimeUnit.MILLISECONDS)
                        .build();
                log.warn("BatchReceivePolicy maxNumMessages: {} or maxNumBytes: {} is less than 0. " +
                        "Reset to DEFAULT_POLICY. batchReceivePolicy: {}", userBatchReceivePolicy.getMaxNumMessages(),
                        userBatchReceivePolicy.getMaxNumBytes(), this.batchReceivePolicy.toString());
            } else {
                this.batchReceivePolicy = conf.getBatchReceivePolicy();
            }
        } else {
            this.batchReceivePolicy = BatchReceivePolicy.DEFAULT_POLICY;
        }

        if (batchReceivePolicy.getTimeoutMs() > 0) {
            batchReceiveTimeout = client.timer().newTimeout(this::pendingBatchReceiveTask, batchReceivePolicy.getTimeoutMs(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Message<T> receive() throws PulsarClientException {
        if (listener != null) {
            throw new PulsarClientException.InvalidConfigurationException(
                    "Cannot use receive() when a listener has been set");
        }
        verifyConsumerState();
        return internalReceive();
    }

    @Override
    public CompletableFuture<Message<T>> receiveAsync() {
        if (listener != null) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Cannot use receive() when a listener has been set"));
        }
        try {
            verifyConsumerState();
        } catch (PulsarClientException e) {
            return FutureUtil.failedFuture(e);
        }
        return internalReceiveAsync();
    }

    protected abstract Message<T> internalReceive() throws PulsarClientException;

    protected abstract CompletableFuture<Message<T>> internalReceiveAsync();

    @Override
    public Message<T> receive(int timeout, TimeUnit unit) throws PulsarClientException {
        if (conf.getReceiverQueueSize() == 0) {
            throw new PulsarClientException.InvalidConfigurationException(
                    "Can't use receive with timeout, if the queue size is 0");
        }
        if (listener != null) {
            throw new PulsarClientException.InvalidConfigurationException(
                    "Cannot use receive() when a listener has been set");
        }

        verifyConsumerState();
        return internalReceive(timeout, unit);
    }

    protected abstract Message<T> internalReceive(int timeout, TimeUnit unit) throws PulsarClientException;

    @Override
    public Messages<T> batchReceive() throws PulsarClientException {
        verifyBatchReceive();
        verifyConsumerState();
        return internalBatchReceive();
    }

    @Override
    public CompletableFuture<Messages<T>> batchReceiveAsync() {
        try {
            verifyBatchReceive();
            verifyConsumerState();
            return internalBatchReceiveAsync();
        } catch (PulsarClientException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    protected CompletableFuture<Message<T>> peekPendingReceive() {
        CompletableFuture<Message<T>> receivedFuture = null;
        while (receivedFuture == null) {
            receivedFuture = pendingReceives.peek();
            if (receivedFuture == null) {
                break;
            }
            // skip done futures (cancelling a future could mark it done)
            if (receivedFuture.isDone()) {
                CompletableFuture<Message<T>> removed = pendingReceives.poll();
                if (removed != receivedFuture) {
                    log.error("Bug! Removed future wasn't the expected one. expected={} removed={}", receivedFuture, removed);
                }
                receivedFuture = null;
            }
        }
        return receivedFuture;
    }

    protected CompletableFuture<Message<T>> pollPendingReceive() {
        CompletableFuture<Message<T>> receivedFuture;
        while (true) {
            receivedFuture = pendingReceives.poll();
            // skip done futures (cancelling a future could mark it done)
            if (receivedFuture == null || !receivedFuture.isDone()) {
                break;
            }
        }
        return receivedFuture;
    }

    protected void completePendingReceive(CompletableFuture<Message<T>> receivedFuture, Message<T> message) {
        getExecutor(message).execute(() -> {
            if (!receivedFuture.complete(message)) {
                log.warn("Race condition detected. receive future was already completed (cancelled={}) and message was dropped. message={}",
                        receivedFuture.isCancelled(), message);
            }
        });
    }

    protected void failPendingReceives(ConcurrentLinkedQueue<CompletableFuture<Message<T>>> pendingReceives) {
        while (!pendingReceives.isEmpty()) {
            CompletableFuture<Message<T>> receiveFuture = pendingReceives.poll();
            if (receiveFuture == null) {
                break;
            }
            if (!receiveFuture.isDone()) {
                receiveFuture.completeExceptionally(
                        new PulsarClientException.AlreadyClosedException(String.format("The consumer which subscribes the topic %s with subscription name %s " +
                                "was already closed when cleaning and closing the consumers", topic, subscription)));
            }
        }
    }

    protected void failPendingBatchReceives(ConcurrentLinkedQueue<OpBatchReceive<T>> pendingBatchReceives) {
        while (!pendingBatchReceives.isEmpty()) {
            OpBatchReceive<T> opBatchReceive = pendingBatchReceives.poll();
            if (opBatchReceive == null || opBatchReceive.future == null) {
                break;
            }
            if (!opBatchReceive.future.isDone()) {
                opBatchReceive.future.completeExceptionally(
                        new PulsarClientException.AlreadyClosedException(String.format("The consumer which subscribes the topic %s with subscription name %s " +
                                "was already closed when cleaning and closing the consumers", topic, subscription)));
            }
        }
    }

    abstract protected Messages<T> internalBatchReceive() throws PulsarClientException;

    abstract protected CompletableFuture<Messages<T>> internalBatchReceiveAsync();

    private static void validateMessageId(Message<?> message) throws PulsarClientException {
        if (message == null) {
            throw new PulsarClientException.InvalidMessageException("Non-null message is required");
        }
        if (message.getMessageId() == null) {
            throw new PulsarClientException.InvalidMessageException("Cannot handle message with null messageId");
        }
    }

    private static void validateMessageId(MessageId messageId) throws PulsarClientException {
        if (messageId == null) {
            throw new PulsarClientException.InvalidMessageException("Cannot handle message with null messageId");
        }
    }

    @Override
    public void acknowledge(Message<?> message) throws PulsarClientException {
        validateMessageId(message);
        acknowledge(message.getMessageId());
    }

    @Override
    public void acknowledge(MessageId messageId) throws PulsarClientException {
        validateMessageId(messageId);
        try {
            acknowledgeAsync(messageId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void acknowledge(List<MessageId> messageIdList) throws PulsarClientException {
        try {
            acknowledgeAsync(messageIdList).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void acknowledge(Messages<?> messages) throws PulsarClientException {
        try {
            acknowledgeAsync(messages).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void reconsumeLater(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException {
        if (!conf.isRetryEnable()) {
            throw new PulsarClientException("reconsumeLater method not support!");
        }
        try {
            reconsumeLaterAsync(message, delayTime, unit).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void reconsumeLater(Messages<?> messages, long delayTime, TimeUnit unit) throws PulsarClientException {
        try {
            reconsumeLaterAsync(messages, delayTime, unit).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void acknowledgeCumulative(Message<?> message) throws PulsarClientException {
        validateMessageId(message);
        acknowledgeCumulative(message.getMessageId());
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException {
        validateMessageId(messageId);
        try {
            acknowledgeCumulativeAsync(messageId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void reconsumeLaterCumulative(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException {
        try {
            reconsumeLaterCumulativeAsync(message, delayTime, unit).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Message<?> message) {
        try {
            validateMessageId(message);
        } catch (PulsarClientException e) {
            return FutureUtil.failedFuture(e);
        }
        return acknowledgeAsync(message.getMessageId());
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Messages<?> messages) {
        List<MessageId> messageIds = new ArrayList<>(messages.size());
        for (Message<?> message: messages) {
            try {
                validateMessageId(message);
            } catch (PulsarClientException e) {
                return FutureUtil.failedFuture(e);
            }
            messageIds.add(message.getMessageId());
        }
        return acknowledgeAsync(messageIds);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList) {
        return doAcknowledgeWithTxn(messageIdList, AckType.Individual, Collections.emptyMap(), null);
    }

    @Override
    public CompletableFuture<Void> reconsumeLaterAsync(Message<?> message, long delayTime, TimeUnit unit) {
        if (!conf.isRetryEnable()) {
            return FutureUtil.failedFuture(new PulsarClientException("reconsumeLater method not support!"));
        }
        try {
            validateMessageId(message);
        } catch (PulsarClientException e) {
            return FutureUtil.failedFuture(e);
        }
        return doReconsumeLater(message, AckType.Individual, Collections.emptyMap(), delayTime, unit);
    }

    @Override
    public CompletableFuture<Void> reconsumeLaterAsync(Messages<?> messages, long delayTime, TimeUnit unit) {
        for (Message<?> message: messages) {
            try {
                validateMessageId(message);
            } catch (PulsarClientException e) {
                return FutureUtil.failedFuture(e);
            }
        }
        messages.forEach(message -> reconsumeLaterAsync(message,delayTime, unit));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message) {
        try {
            validateMessageId(message);
        } catch (PulsarClientException e) {
            return FutureUtil.failedFuture(e);
        }
        return acknowledgeCumulativeAsync(message.getMessageId());
    }

    @Override
    public CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> message, long delayTime, TimeUnit unit) {
        if (!conf.isRetryEnable()) {
            return FutureUtil.failedFuture(new PulsarClientException("reconsumeLater method not support!"));
        }
        if (!isCumulativeAcknowledgementAllowed(conf.getSubscriptionType())) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Cannot use cumulative acks on a non-exclusive subscription"));
        }
        return doReconsumeLater(message, AckType.Cumulative, Collections.emptyMap(), delayTime, unit);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
        return acknowledgeAsync(messageId, null);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId,
                                                    Transaction txn) {
        TransactionImpl txnImpl = null;
        if (null != txn) {
            checkArgument(txn instanceof TransactionImpl);
            txnImpl = (TransactionImpl) txn;
        }
        return doAcknowledgeWithTxn(messageId, AckType.Individual, Collections.emptyMap(), txnImpl);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
        return acknowledgeCumulativeAsync(messageId, null);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId, Transaction txn) {
        if (!isCumulativeAcknowledgementAllowed(conf.getSubscriptionType())) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Cannot use cumulative acks on a non-exclusive/non-failover subscription"));
        }

        TransactionImpl txnImpl = null;
        if (null != txn) {
            checkArgument(txn instanceof TransactionImpl);
            txnImpl = (TransactionImpl) txn;
        }
        return doAcknowledgeWithTxn(messageId, AckType.Cumulative, Collections.emptyMap(), txnImpl);
    }

    @Override
    public void negativeAcknowledge(Message<?> message) {
        negativeAcknowledge(message.getMessageId());
    }

    protected CompletableFuture<Void> doAcknowledgeWithTxn(List<MessageId> messageIdList, AckType ackType,
                                                           Map<String,Long> properties,
                                                           TransactionImpl txn) {
        CompletableFuture<Void> ackFuture;
        if (txn != null) {
            ackFuture = txn.registerAckedTopic(getTopic(), subscription)
                    .thenCompose(ignored -> doAcknowledge(messageIdList, ackType, properties, txn));
            txn.registerAckOp(ackFuture);
        } else {
            ackFuture = doAcknowledge(messageIdList, ackType, properties, null);
        }
        return ackFuture;
    }

    protected CompletableFuture<Void> doAcknowledgeWithTxn(MessageId messageId, AckType ackType,
                                                           Map<String,Long> properties,
                                                           TransactionImpl txn) {
        CompletableFuture<Void> ackFuture;
        if (txn != null && (this instanceof ConsumerImpl)) {
            // it is okay that we register acked topic after sending the acknowledgements. because
            // the transactional ack will not be visiable for consumers until the transaction is
            // committed
            if (ackType == AckType.Cumulative) {
                txn.registerCumulativeAckConsumer((ConsumerImpl<?>) this);
            }

            ackFuture = txn.registerAckedTopic(getTopic(), subscription)
                    .thenCompose(ignored -> doAcknowledge(messageId, ackType, properties, txn));
            // register the ackFuture as part of the transaction
            txn.registerAckOp(ackFuture);
            return ackFuture;
        } else {
            ackFuture = doAcknowledge(messageId, ackType, properties, txn);
        }
        return ackFuture;
    }

    protected abstract CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                             Map<String,Long> properties,
                                                             TransactionImpl txn);

    protected abstract CompletableFuture<Void> doAcknowledge(List<MessageId> messageIdList, AckType ackType,
                                                    Map<String, Long> properties,
                                                    TransactionImpl txn);

    protected abstract CompletableFuture<Void> doReconsumeLater(Message<?> message, AckType ackType,
                                                                Map<String,Long> properties,
                                                                long delayTime,
                                                                TimeUnit unit);

    @Override
    public void negativeAcknowledge(Messages<?> messages) {
        messages.forEach(this::negativeAcknowledge);
    }


    @Override
    public void unsubscribe() throws PulsarClientException {
        try {
            unsubscribeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public abstract CompletableFuture<Void> unsubscribeAsync();

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public abstract CompletableFuture<Void> closeAsync();


    @Override
    public MessageId getLastMessageId() throws PulsarClientException {
        try {
            return getLastMessageIdAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public abstract CompletableFuture<MessageId> getLastMessageIdAsync();

    private boolean isCumulativeAcknowledgementAllowed(SubscriptionType type) {
        return SubscriptionType.Shared != type && SubscriptionType.Key_Shared != type;
    }

    protected SubType getSubType() {
        SubscriptionType type = conf.getSubscriptionType();
        switch (type) {
        case Exclusive:
            return SubType.Exclusive;

        case Shared:
            return SubType.Shared;

        case Failover:
            return SubType.Failover;

        case Key_Shared:
            return SubType.Key_Shared;
        }

        // Should not happen since we cover all cases above
        return null;
    }

    public abstract int getAvailablePermits();

    public abstract int numMessagesInQueue();

    public CompletableFuture<Consumer<T>> subscribeFuture() {
        return subscribeFuture;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public String getSubscription() {
        return subscription;
    }

    @Override
    public String getConsumerName() {
        return this.consumerName;
    }

    /**
     * Redelivers the given unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
     * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
     * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
     * breaks, the messages are redelivered after reconnect.
     */
    protected abstract void redeliverUnacknowledgedMessages(Set<MessageId> messageIds);

    @Override
    public String toString() {
        return "ConsumerBase{" +
                "subscription='" + subscription + '\'' +
                ", consumerName='" + consumerName + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }

    protected void setMaxReceiverQueueSize(int newSize) {
        this.maxReceiverQueueSize = newSize;
    }

    protected Message<T> beforeConsume(Message<T> message) {
        if (interceptors != null) {
            return interceptors.beforeConsume(this, message);
        } else {
            return message;
        }
    }

    protected void onAcknowledge(MessageId messageId, Throwable exception) {
        if (interceptors != null) {
            interceptors.onAcknowledge(this, messageId, exception);
        }
    }

    protected void onAcknowledgeCumulative(MessageId messageId, Throwable exception) {
        if (interceptors != null) {
            interceptors.onAcknowledgeCumulative(this, messageId, exception);
        }
    }

    protected void onNegativeAcksSend(Set<MessageId> messageIds) {
        if (interceptors != null) {
            interceptors.onNegativeAcksSend(this, messageIds);
        }
    }

    protected void onAckTimeoutSend(Set<MessageId> messageIds) {
        if (interceptors != null) {
            interceptors. onAckTimeoutSend(this, messageIds);
        }
    }

    protected boolean canEnqueueMessage(Message<T> message) {
        // Default behavior, can be overridden in subclasses
        return true;
    }

    protected boolean enqueueMessageAndCheckBatchReceive(Message<T> message) {
        if (canEnqueueMessage(message) && incomingMessages.offer(message)) {
            increaseIncomingMessageSize(message);
        }
        return hasEnoughMessagesForBatchReceive();
    }

    protected boolean hasEnoughMessagesForBatchReceive() {
        if (batchReceivePolicy.getMaxNumMessages() <= 0 && batchReceivePolicy.getMaxNumBytes() <= 0) {
            return false;
        }
        return (batchReceivePolicy.getMaxNumMessages() > 0 && incomingMessages.size() >= batchReceivePolicy.getMaxNumMessages())
                || (batchReceivePolicy.getMaxNumBytes() > 0 && getIncomingMessageSize() >= batchReceivePolicy.getMaxNumBytes());
    }

    private void verifyConsumerState() throws PulsarClientException {
        switch (getState()) {
            case Ready:
            case Connecting:
                break; // Ok
            case Closing:
            case Closed:
                throw  new PulsarClientException.AlreadyClosedException("Consumer already closed");
            case Terminated:
                throw new PulsarClientException.AlreadyClosedException("Topic was terminated");
            case Failed:
            case Uninitialized:
                throw new PulsarClientException.NotConnectedException();
            default:
                break;
        }
    }

    private void verifyBatchReceive() throws PulsarClientException {
        if (listener != null) {
            throw new PulsarClientException.InvalidConfigurationException(
                "Cannot use receive() when a listener has been set");
        }
        if (conf.getReceiverQueueSize() == 0) {
            throw new PulsarClientException.InvalidConfigurationException(
                "Can't use batch receive, if the queue size is 0");
        }
    }

    protected static final class OpBatchReceive<T> {

        final CompletableFuture<Messages<T>> future;
        final long createdAt;

        private OpBatchReceive(CompletableFuture<Messages<T>> future) {
            this.future = future;
            this.createdAt = System.nanoTime();
        }

        static <T> OpBatchReceive<T> of(CompletableFuture<Messages<T>> future) {
            return new OpBatchReceive<>(future);
        }
    }

    protected void notifyPendingBatchReceivedCallBack() {
        OpBatchReceive<T> opBatchReceive = pollNextBatchReceive();
        if (opBatchReceive == null) {
            return;
        }

        reentrantLock.lock();
        try {
            notifyPendingBatchReceivedCallBack(opBatchReceive);
        } finally {
            reentrantLock.unlock();
        }
    }

    private OpBatchReceive<T> peekNextBatchReceive() {
        OpBatchReceive<T> opBatchReceive = null;
        while (opBatchReceive == null) {
            opBatchReceive = pendingBatchReceives.peek();
            // no entry available
            if (opBatchReceive == null) {
                return null;
            }
            // remove entries where future is null or has been completed (cancel / timeout)
            if (opBatchReceive.future == null || opBatchReceive.future.isDone()) {
                OpBatchReceive<T> removed = pendingBatchReceives.poll();
                if (removed != opBatchReceive) {
                    log.error("Bug: Removed entry wasn't the expected one. expected={}, removed={}", opBatchReceive, removed);
                }
                opBatchReceive = null;
            }
        }
        return opBatchReceive;
    }


    private OpBatchReceive<T> pollNextBatchReceive() {
        OpBatchReceive<T> opBatchReceive = null;
        while (opBatchReceive == null) {
            opBatchReceive = pendingBatchReceives.poll();
            // no entry available
            if (opBatchReceive == null) {
                return null;
            }
            // skip entries where future is null or has been completed (cancel / timeout)
            if (opBatchReceive.future == null || opBatchReceive.future.isDone()) {
                opBatchReceive = null;
            }
        }
        return opBatchReceive;
    }

    protected final void notifyPendingBatchReceivedCallBack(OpBatchReceive<T> opBatchReceive) {
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

        completePendingBatchReceive(opBatchReceive.future, messages);
    }

    protected void completePendingBatchReceive(CompletableFuture<Messages<T>> future, Messages<T> messages) {
        if (!future.complete(messages)) {
            log.warn("Race condition detected. batch receive future was already completed (cancelled={}) and messages were dropped. messages={}",
                    future.isCancelled(), messages);
        }
    }

    protected abstract void messageProcessed(Message<?> msg);


    private void pendingBatchReceiveTask(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }

        long timeToWaitMs;

        synchronized (this) {
            // If it's closing/closed we need to ignore this timeout and not schedule next timeout.
            if (getState() == State.Closing || getState() == State.Closed) {
                return;
            }
            if (pendingBatchReceives == null) {
                pendingBatchReceives = Queues.newConcurrentLinkedQueue();
            }
            OpBatchReceive<T> firstOpBatchReceive = peekNextBatchReceive();
            timeToWaitMs = batchReceivePolicy.getTimeoutMs();

            while (firstOpBatchReceive != null) {
                // If there is at least one batch receive, calculate the diff between the batch receive timeout
                // and the elapsed time since the operation was created.
                long diff = batchReceivePolicy.getTimeoutMs()
                        - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstOpBatchReceive.createdAt);
                if (diff <= 0) {
                    // The diff is less than or equal to zero, meaning that the batch receive has been timed out.
                    // complete the OpBatchReceive and continue to check the next OpBatchReceive in pendingBatchReceives.
                    OpBatchReceive<T> op = pollNextBatchReceive();
                    if (op != null) {
                        completeOpBatchReceive(op);
                    }
                    firstOpBatchReceive = peekNextBatchReceive();
                } else {
                    // The diff is greater than zero, set the timeout to the diff value
                    timeToWaitMs = diff;
                    break;
                }
            }
            batchReceiveTimeout = client.timer().newTimeout(this::pendingBatchReceiveTask, timeToWaitMs, TimeUnit.MILLISECONDS);
        }
    }

    protected void triggerListener() {
        // Trigger the notification on the message listener in a separate thread to avoid blocking the networking
        // thread while the message processing happens
        Message<T> msg;
        do {
            try {
                msg = internalReceive(0, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    final Message<T> finalMsg = msg;
                    if (SubscriptionType.Key_Shared == conf.getSubscriptionType()) {
                        executorProvider.getExecutor(peekMessageKey(finalMsg)).execute(() ->
                                callMessageListener(finalMsg));
                    } else {
                        getExecutor(msg).execute(() -> callMessageListener(finalMsg));
                    }
                }
            } catch (PulsarClientException e) {
                log.warn("[{}] [{}] Failed to dequeue the message for listener", topic, subscription, e);
                return;
            }
        } while (msg != null);

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Message has been cleared from the queue", topic, subscription);
        }
    }

    protected void callMessageListener(Message<T> msg) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Calling message listener for message {}", topic, subscription,
                        msg.getMessageId());
            }
            listener.received(ConsumerBase.this, msg);
        } catch (Throwable t) {
            log.error("[{}][{}] Message listener error in processing message: {}", topic, subscription,
                    msg.getMessageId(), t);
        }
    }

    static final byte[] NONE_KEY = "NONE_KEY".getBytes(StandardCharsets.UTF_8);
    protected byte[] peekMessageKey(Message<T> msg) {
        byte[] key = NONE_KEY;
        if (msg.hasKey()) {
            key = msg.getKeyBytes();
        }
        if (msg.hasOrderingKey()) {
            key = msg.getOrderingKey();
        }
        return key;
    }

    protected MessagesImpl<T> getNewMessagesImpl() {
        return new MessagesImpl<>(batchReceivePolicy.getMaxNumMessages(),
                batchReceivePolicy.getMaxNumBytes());
    }

    protected boolean hasPendingBatchReceive() {
        return pendingBatchReceives != null && peekNextBatchReceive() != null;
    }

    protected void increaseIncomingMessageSize(final Message<?> message) {
        INCOMING_MESSAGES_SIZE_UPDATER.addAndGet(this, message.size());
    }

    protected void resetIncomingMessageSize() {
        INCOMING_MESSAGES_SIZE_UPDATER.set(this, 0);
    }

    protected void decreaseIncomingMessageSize(final Message<?> message) {
        INCOMING_MESSAGES_SIZE_UPDATER.addAndGet(this, -message.size());
    }

    public long getIncomingMessageSize() {
        return INCOMING_MESSAGES_SIZE_UPDATER.get(this);
    }

    public int getTotalIncomingMessages() {
        return incomingMessages.size();
    }

    protected void clearIncomingMessages() {
        // release messages if they are pooled messages
        incomingMessages.forEach(Message::release);
        incomingMessages.clear();
        resetIncomingMessageSize();
    }

    protected abstract void completeOpBatchReceive(OpBatchReceive<T> op);

    private ExecutorService getExecutor(Message<T> msg) {
        ConsumerImpl receivedConsumer = (msg instanceof TopicMessageImpl) ? ((TopicMessageImpl) msg).receivedByconsumer
                : null;
        ExecutorService executor = receivedConsumer != null && receivedConsumer.pinnedExecutor != null
                ? receivedConsumer.pinnedExecutor
                : pinnedExecutor;
        return executor;
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerBase.class);
}
