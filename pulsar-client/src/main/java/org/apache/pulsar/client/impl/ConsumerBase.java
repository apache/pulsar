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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import io.netty.util.Timeout;
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
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConsumerBase<T> extends HandlerState implements Consumer<T> {

    enum ConsumerType {
        PARTITIONED, NON_PARTITIONED
    }

    protected final String subscription;
    protected final ConsumerConfigurationData<T> conf;
    protected final String consumerName;
    protected final CompletableFuture<Consumer<T>> subscribeFuture;
    protected final MessageListener<T> listener;
    protected final ConsumerEventListener consumerEventListener;
    protected final ExecutorService listenerExecutor;
    final BlockingQueue<Message<T>> incomingMessages;
    protected ConcurrentOpenHashMap<MessageIdImpl, MessageIdImpl[]> unAckedChunckedMessageIdSequenceMap;
    protected final ConcurrentLinkedQueue<CompletableFuture<Message<T>>> pendingReceives;
    protected int maxReceiverQueueSize;
    protected final Schema<T> schema;
    protected final ConsumerInterceptors<T> interceptors;
    protected final BatchReceivePolicy batchReceivePolicy;
    protected ConcurrentLinkedQueue<OpBatchReceive<T>> pendingBatchReceives;
    protected static final AtomicLongFieldUpdater<ConsumerBase> INCOMING_MESSAGES_SIZE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ConsumerBase.class, "incomingMessagesSize");
    protected volatile long incomingMessagesSize = 0;
    protected volatile Timeout batchReceiveTimeout = null;

    protected ConsumerBase(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                           int receiverQueueSize, ExecutorService listenerExecutor,
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
        this.unAckedChunckedMessageIdSequenceMap = new ConcurrentOpenHashMap<>();

        this.listenerExecutor = listenerExecutor;
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

    protected void failPendingReceives(ConcurrentLinkedQueue<CompletableFuture<Message<T>>> pendingReceives) {
        while (!pendingReceives.isEmpty()) {
            CompletableFuture<Message<T>> receiveFuture = pendingReceives.poll();
            if (receiveFuture == null) {
                break;
            }
            receiveFuture.completeExceptionally(
                    new PulsarClientException.AlreadyClosedException(String.format("The consumer which subscribes the topic %s with subscription name %s " +
                            "was already closed when cleaning and closing the consumers", topic, subscription)));
        }
    }

    protected void failPendingBatchReceives(ConcurrentLinkedQueue<OpBatchReceive<T>> pendingBatchReceives) {
        while (!pendingBatchReceives.isEmpty()) {
            OpBatchReceive<T> opBatchReceive = pendingBatchReceives.poll();
            if (opBatchReceive == null || opBatchReceive.future == null) {
                break;
            }
            opBatchReceive.future.completeExceptionally(
                    new PulsarClientException.AlreadyClosedException(String.format("The consumer which subscribes the topic %s with subscription name %s " +
                            "was already closed when cleaning and closing the consumers", topic, subscription)));
        }
    }

    abstract protected Messages<T> internalBatchReceive() throws PulsarClientException;

    abstract protected CompletableFuture<Messages<T>> internalBatchReceiveAsync();

    @Override
    public void acknowledge(Message<?> message) throws PulsarClientException {
        try {
            acknowledge(message.getMessageId());
        } catch (NullPointerException npe) {
            throw new PulsarClientException.InvalidMessageException(npe.getMessage());
        }
    }

    @Override
    public void acknowledge(MessageId messageId) throws PulsarClientException {
        try {
            acknowledgeAsync(messageId).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void acknowledge(List<MessageId> messageIdList) throws PulsarClientException {
        try {
            acknowledgeAsync(messageIdList).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void acknowledge(Messages<?> messages) throws PulsarClientException {
        try {
            acknowledgeAsync(messages).get();
        } catch (Exception e) {
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
        } catch (Exception e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        }
    }

    @Override
    public void reconsumeLater(Messages<?> messages, long delayTime, TimeUnit unit) throws PulsarClientException {
        try {
            reconsumeLaterAsync(messages, delayTime, unit).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void acknowledgeCumulative(Message<?> message) throws PulsarClientException {
        try {
            acknowledgeCumulative(message.getMessageId());
        } catch (NullPointerException npe) {
            throw new PulsarClientException.InvalidMessageException(npe.getMessage());
        }
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException {
        try {
            acknowledgeCumulativeAsync(messageId).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void reconsumeLaterCumulative(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException {
        try {
            reconsumeLaterCumulativeAsync(message, delayTime, unit).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Message<?> message) {
        try {
            return acknowledgeAsync(message.getMessageId());
        } catch (NullPointerException npe) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
        }
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Messages<?> messages) {
        try {
            messages.forEach(this::acknowledgeAsync);
            return CompletableFuture.completedFuture(null);
        } catch (NullPointerException npe) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
        }
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
            return doReconsumeLater(message, AckType.Individual, Collections.emptyMap(), delayTime, unit);
        } catch (NullPointerException npe) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
        }
    }

    @Override
    public CompletableFuture<Void> reconsumeLaterAsync(Messages<?> messages, long delayTime, TimeUnit unit) {
        try {
            messages.forEach(message -> reconsumeLaterAsync(message,delayTime, unit));
            return CompletableFuture.completedFuture(null);
        } catch (NullPointerException npe) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
        }
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message) {
        try {
            return acknowledgeCumulativeAsync(message.getMessageId());
        } catch (NullPointerException npe) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
        }
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

    // TODO: expose this method to consumer interface when the transaction feature is completed
    // @Override
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

    // TODO: expose this method to consumer interface when the transaction feature is completed
    // @Override
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
        CompletableFuture<Void> ackFuture = doAcknowledge(messageIdList, ackType, properties, txn);
        if (txn != null) {
            txn.registerAckedTopic(getTopic(), subscription);
            return txn.registerAckOp(ackFuture);
        } else {
            return ackFuture;
        }
    }

    protected CompletableFuture<Void> doAcknowledgeWithTxn(MessageId messageId, AckType ackType,
                                                           Map<String,Long> properties,
                                                           TransactionImpl txn) {
        CompletableFuture<Void> ackFuture = doAcknowledge(messageId, ackType, properties, txn);
        if (txn != null && (this instanceof ConsumerImpl)) {
            // it is okay that we register acked topic after sending the acknowledgements. because
            // the transactional ack will not be visiable for consumers until the transaction is
            // committed
            txn.registerAckedTopic(getTopic(), subscription);
            // register the ackFuture as part of the transaction
            return txn.registerAckOp(ackFuture);
        } else {
            return ackFuture;
        }
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
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public abstract CompletableFuture<Void> unsubscribeAsync();

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public abstract CompletableFuture<Void> closeAsync();


    @Override
    public MessageId getLastMessageId() throws PulsarClientException {
        try {
            return getLastMessageIdAsync().get();
        } catch (Exception e) {
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
        if (canEnqueueMessage(message)) {
            incomingMessages.add(message);
            INCOMING_MESSAGES_SIZE_UPDATER.addAndGet(
                    this, message.getData() == null ? 0 : message.getData().length);
        }
        return hasEnoughMessagesForBatchReceive();
    }

    protected boolean hasEnoughMessagesForBatchReceive() {
        if (batchReceivePolicy.getMaxNumMessages() <= 0 && batchReceivePolicy.getMaxNumBytes() <= 0) {
            return false;
        }
        return (batchReceivePolicy.getMaxNumMessages() > 0 && incomingMessages.size() >= batchReceivePolicy.getMaxNumMessages())
                || (batchReceivePolicy.getMaxNumBytes() > 0 && INCOMING_MESSAGES_SIZE_UPDATER.get(this) >= batchReceivePolicy.getMaxNumBytes());
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
        final OpBatchReceive<T> opBatchReceive = pendingBatchReceives.poll();
        if (opBatchReceive == null || opBatchReceive.future == null) {
            return;
        }
        notifyPendingBatchReceivedCallBack(opBatchReceive);
    }

    protected void notifyPendingBatchReceivedCallBack(OpBatchReceive<T> opBatchReceive) {
        MessagesImpl<T> messages = getNewMessagesImpl();
        Message<T> msgPeeked = incomingMessages.peek();
        while (msgPeeked != null && messages.canAdd(msgPeeked)) {
            Message<T> msg = null;
            try {
                msg = incomingMessages.poll(0L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
            if (msg != null) {
                messageProcessed(msg);
                Message<T> interceptMsg = beforeConsume(msg);
                messages.add(interceptMsg);
            }
            msgPeeked = incomingMessages.peek();
        }
        opBatchReceive.future.complete(messages);
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
            OpBatchReceive<T> firstOpBatchReceive = pendingBatchReceives.peek();
            timeToWaitMs = batchReceivePolicy.getTimeoutMs();

            while (firstOpBatchReceive != null) {
                // If there is at least one batch receive, calculate the diff between the batch receive timeout
                // and the elapsed time since the operation was created.
                long diff = batchReceivePolicy.getTimeoutMs()
                        - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstOpBatchReceive.createdAt);
                if (diff <= 0) {
                    // The diff is less than or equal to zero, meaning that the batch receive has been timed out.
                    // complete the OpBatchReceive and continue to check the next OpBatchReceive in pendingBatchReceives.
                    OpBatchReceive<T> op = pendingBatchReceives.poll();
                    completeOpBatchReceive(op);
                    firstOpBatchReceive = pendingBatchReceives.peek();
                } else {
                    // The diff is greater than zero, set the timeout to the diff value
                    timeToWaitMs = diff;
                    break;
                }
            }
            batchReceiveTimeout = client.timer().newTimeout(this::pendingBatchReceiveTask, timeToWaitMs, TimeUnit.MILLISECONDS);
        }
    }

    protected MessagesImpl<T> getNewMessagesImpl() {
        return new MessagesImpl<>(batchReceivePolicy.getMaxNumMessages(),
                batchReceivePolicy.getMaxNumBytes());
    }

    protected boolean hasPendingBatchReceive() {
        return pendingBatchReceives != null && !pendingBatchReceives.isEmpty();
    }

    protected abstract void completeOpBatchReceive(OpBatchReceive<T> op);

    private static final Logger log = LoggerFactory.getLogger(ConsumerBase.class);
}
