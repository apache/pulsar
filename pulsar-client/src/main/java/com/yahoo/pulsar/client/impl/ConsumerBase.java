/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.yahoo.pulsar.client.api.*;
import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.collect.Queues;
import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.common.util.collections.GrowableArrayBlockingQueue;

public abstract class ConsumerBase extends HandlerBase implements Consumer {

    enum ConsumerType {
        PARTITIONED, NON_PARTITIONED
    }

    protected final String subscription;
    protected final ConsumerConfiguration conf;
    protected final String consumerName;
    protected final CompletableFuture<Consumer> subscribeFuture;
    protected final MessageListener listener;
    protected final ExecutorService listenerExecutor;
    final BlockingQueue<Message> incomingMessages;
    protected final ConcurrentLinkedQueue<CompletableFuture<Message>> pendingReceives;
    protected final int maxReceiverQueueSize;

    protected ConsumerBase(PulsarClientImpl client, String topic, String subscription, ConsumerConfiguration conf,
            int receiverQueueSize, ExecutorService listenerExecutor, CompletableFuture<Consumer> subscribeFuture) {
        super(client, topic);
        this.maxReceiverQueueSize = receiverQueueSize;
        this.subscription = subscription;
        this.conf = conf;
        this.consumerName = conf.getConsumerName() == null
                ? DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 5) : conf.getConsumerName();
        this.subscribeFuture = subscribeFuture;
        this.listener = conf.getMessageListener();
        if (receiverQueueSize <= 1) {
            this.incomingMessages = Queues.newArrayBlockingQueue(1);
        } else {
            this.incomingMessages = new GrowableArrayBlockingQueue<>();
        }

        this.listenerExecutor = listenerExecutor;
        this.pendingReceives = Queues.newConcurrentLinkedQueue();
    }

    @Override
    public Message receive() throws PulsarClientException {
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
        case Failed:
        case Uninitialized:
            throw new PulsarClientException.NotConnectedException();
        }

        return internalReceive();
    }

    @Override
    public CompletableFuture<Message> receiveAsync() {

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
        case Failed:
        case Uninitialized:
            return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }

        return internalReceiveAsync();
    }

    abstract protected Message internalReceive() throws PulsarClientException;

    abstract protected CompletableFuture<Message> internalReceiveAsync();

    @Override
    public Message receive(int timeout, TimeUnit unit) throws PulsarClientException {
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
        case Failed:
        case Uninitialized:
            throw new PulsarClientException.NotConnectedException();
        }

        return internalReceive(timeout, unit);
    }

    abstract protected Message internalReceive(int timeout, TimeUnit unit) throws PulsarClientException;

    @Override
    public void acknowledge(Message message) throws PulsarClientException {
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
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public void acknowledgeCumulative(Message message) throws PulsarClientException {
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
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Message message) {
        try {
            return acknowledgeAsync(message.getMessageId());
        } catch (NullPointerException npe) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
        }
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(Message message) {
        try {
            return acknowledgeCumulativeAsync(message.getMessageId());
        } catch (NullPointerException npe) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
        }
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
        return doAcknowledge(messageId, AckType.Individual);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
        if (conf.getSubscriptionType() != SubscriptionType.Exclusive) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Cannot use cumulative acks on a non-exclusive subscription"));
        }

        return doAcknowledge(messageId, AckType.Cumulative);
    }

    abstract protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType);

    @Override
    public void unsubscribe() throws PulsarClientException {
        try {
            unsubscribeAsync().get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    abstract public CompletableFuture<Void> unsubscribeAsync();

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    abstract public CompletableFuture<Void> closeAsync();

    protected SubType getSubType() {
        SubscriptionType type = conf.getSubscriptionType();
        switch (type) {
        case Exclusive:
            return SubType.Exclusive;

        case Shared:
            return SubType.Shared;

        case Failover:
            return SubType.Failover;
        }

        // Should not happen since we cover all cases above
        return null;
    }

    abstract public boolean isConnected();

    abstract public int getAvailablePermits();

    abstract public int numMessagesInQueue();

    public CompletableFuture<Consumer> subscribeFuture() {
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

    /**
     * Redelivers the given unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
     * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
     * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
     * breaks, the messages are redelivered after reconnect.
     */
    protected abstract void redeliverUnacknowledgedMessages(Set<MessageIdImpl> messageIds);
}
