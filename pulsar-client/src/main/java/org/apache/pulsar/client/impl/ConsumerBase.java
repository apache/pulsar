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

import com.google.common.collect.Queues;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ConsumerName;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

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
    protected final ConcurrentLinkedQueue<CompletableFuture<Message<T>>> pendingReceives;
    protected int maxReceiverQueueSize;
    protected final Schema<T> schema;
    protected final ConsumerInterceptors<T> interceptors;

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

        this.listenerExecutor = listenerExecutor;
        this.pendingReceives = Queues.newConcurrentLinkedQueue();
        this.schema = schema;
        this.interceptors = interceptors;
    }

    @Override
    public Message<T> receive() throws PulsarClientException {
        if (listener != null) {
            throw new PulsarClientException.InvalidConfigurationException(
                    "Cannot use receive() when a listener has been set");
        }

        switch (getState()) {
        case Ready:
        case Connecting:
            break; // Ok
        case Closing:
        case Closed:
            throw new PulsarClientException.AlreadyClosedException("Consumer already closed");
        case Terminated:
            throw new PulsarClientException.AlreadyClosedException("Topic was terminated");
        case Failed:
        case Uninitialized:
            throw new PulsarClientException.NotConnectedException();
        default:
            break;
        }

        return internalReceive();
    }

    @Override
    public CompletableFuture<Message<T>> receiveAsync() {

        if (listener != null) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Cannot use receive() when a listener has been set"));
        }

        switch (getState()) {
        case Ready:
        case Connecting:
            break; // Ok
        case Closing:
        case Closed:
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Consumer already closed"));
        case Terminated:
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Topic was terminated"));
        case Failed:
        case Uninitialized:
            return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }

        return internalReceiveAsync();
    }

    abstract protected Message<T> internalReceive() throws PulsarClientException;

    abstract protected CompletableFuture<Message<T>> internalReceiveAsync();

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

        switch (getState()) {
        case Ready:
        case Connecting:
            break; // Ok
        case Closing:
        case Closed:
            throw new PulsarClientException.AlreadyClosedException("Consumer already closed");
        case Terminated:
            throw new PulsarClientException.AlreadyClosedException("Topic was terminated");
        case Failed:
        case Uninitialized:
            throw new PulsarClientException.NotConnectedException();
        }

        return internalReceive(timeout, unit);
    }

    abstract protected Message<T> internalReceive(int timeout, TimeUnit unit) throws PulsarClientException;

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
    public CompletableFuture<Void> acknowledgeAsync(Message<?> message) {
        try {
            return acknowledgeAsync(message.getMessageId());
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
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
        return doAcknowledge(messageId, AckType.Individual, Collections.emptyMap());
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
        if (!isCumulativeAcknowledgementAllowed(conf.getSubscriptionType())) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Cannot use cumulative acks on a non-exclusive subscription"));
        }

        return doAcknowledge(messageId, AckType.Cumulative, Collections.emptyMap());
    }

    @Override
    public void negativeAcknowledge(Message<?> message) {
        negativeAcknowledge(message.getMessageId());
    }

    abstract protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                             Map<String,Long> properties);

    @Override
    public void unsubscribe() throws PulsarClientException {
        try {
            unsubscribeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    abstract public CompletableFuture<Void> unsubscribeAsync();

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    abstract public CompletableFuture<Void> closeAsync();

    private boolean isCumulativeAcknowledgementAllowed(SubscriptionType type) {
        return SubscriptionType.Shared != type;
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

    abstract public int getAvailablePermits();

    abstract public int numMessagesInQueue();

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
}
