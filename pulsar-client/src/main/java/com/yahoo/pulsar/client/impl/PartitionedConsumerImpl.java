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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import com.yahoo.pulsar.common.naming.DestinationName;

public class PartitionedConsumerImpl extends ConsumerBase {

    private List<ConsumerImpl> consumers;
    private int numPartitions;
    private final ExecutorService internalListenerExecutor;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConsumerStats stats;

    PartitionedConsumerImpl(PulsarClientImpl client, String topic, String subscription, ConsumerConfiguration conf,
            int numPartitions, ExecutorService listenerExecutor, CompletableFuture<Consumer> subscribeFuture) {
        super(client, topic, subscription, conf, listenerExecutor, subscribeFuture, false /* use fixed size queue */ );
        this.consumers = Lists.newArrayListWithCapacity(numPartitions);
        this.numPartitions = numPartitions;
        // gets a new listener thread for the internal listener
        this.internalListenerExecutor = client.internalExecutorProvider().getExecutor();
        stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ConsumerStats() : null;
        checkArgument(conf.getReceiverQueueSize() > 0,
                "Receiver queue size needs to be greater than 0 for Partitioned Topics");
        start();
    }

    private void start() {
        AtomicReference<Throwable> subscribeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger();
        ConsumerConfiguration internalConfig = getInternalConsumerConfig();
        for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
            String partitionName = DestinationName.get(topic).getPartition(partitionIndex).toString();
            // use the same internal listener executor for all the partitions so that when the consumer queue is full
            // all the partitions get blocked
            ConsumerImpl consumer = new ConsumerImpl(client, partitionName, subscription, internalConfig,
                    internalListenerExecutor, partitionIndex, new CompletableFuture<Consumer>());
            consumers.add(consumer);
            consumer.subscribeFuture().handle((cons, subscribeException) -> {
                if (subscribeException != null) {
                    state.set(State.Failed);
                    subscribeFail.compareAndSet(null, subscribeException);
                    client.cleanupConsumer(this);
                }
                if (completed.incrementAndGet() == numPartitions) {
                    if (subscribeFail.get() == null) {
                        try {
                            // We have successfully created N consumers, so we can start receiving messages now
                            receiveMessages();
                            state.set(State.Ready);
                            subscribeFuture().complete(PartitionedConsumerImpl.this);
                            log.info("[{}] [{}] Created partitioned consumer", topic, subscription);
                            return null;
                        } catch (PulsarClientException e) {
                            subscribeFail.set(e);
                        }
                    }
                    closeAsync().handle((ok, closeException) -> {
                        subscribeFuture().completeExceptionally(subscribeFail.get());
                        client.cleanupConsumer(this);
                        return null;
                    });
                    log.error("[{}] [{}] Could not create partitioned consumer.", topic, subscription,
                            subscribeFail.get().getCause());
                }
                return null;
            });
        }
    }

    private void receiveMessages() throws PulsarClientException {
        for (ConsumerImpl consumer : consumers) {
            consumer.receiveMessages(consumer.cnx(), conf.getReceiverQueueSize());
        }
    }

    @Override
    protected Message internalReceive() throws PulsarClientException {
        Message message;
        try {
            message = incomingMessages.take();
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    protected Message internalReceive(int timeout, TimeUnit unit) throws PulsarClientException {
        Message message;
        try {
            message = incomingMessages.poll(timeout, unit);
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    protected CompletableFuture<Message> internalReceiveAsync() {

        CompletableFuture<Message> result = new CompletableFuture<Message>();
        Message message;
        try {
            lock.writeLock().lock();
            message = incomingMessages.poll(0, TimeUnit.SECONDS);
            if (message == null) {
                pendingReceives.add(result);
            } else {
                result.complete(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(new PulsarClientException(e));
        } finally {
            lock.writeLock().unlock();
        }

        return result;
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType) {
        checkArgument(messageId instanceof MessageIdImpl);

        if (state.get() != State.Ready) {
            return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
        }

        if (ackType == AckType.Cumulative) {
            return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                    "Cumulative acknowledge not supported for partitioned topics"));
        } else {

            ConsumerImpl consumer = consumers.get(((MessageIdImpl) messageId).getPartitionIndex());
            return consumer.doAcknowledge(messageId, ackType);
        }

    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        if (state.get() == State.Closing || state.get() == State.Closed) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Partitioned Consumer was already closed"));
        }
        state.set(State.Closing);

        AtomicReference<Throwable> unsubscribeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(numPartitions);
        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        for (Consumer consumer : consumers) {
            if (consumer != null) {
                consumer.unsubscribeAsync().handle((unsubscribed, ex) -> {
                    if (ex != null) {
                        unsubscribeFail.compareAndSet(null, ex);
                    }
                    if (completed.decrementAndGet() == 0) {
                        if (unsubscribeFail.get() == null) {
                            state.set(State.Closed);
                            unsubscribeFuture.complete(null);
                            log.info("[{}] [{}] Unsubscribed Partitioned Consumer", topic, subscription);
                        } else {
                            state.set(State.Failed);
                            unsubscribeFuture.completeExceptionally(unsubscribeFail.get());
                            log.error("[{}] [{}] Could not unsubscribe Partitioned Consumer", topic, subscription,
                                    unsubscribeFail.get().getCause());
                        }
                    }

                    return null;
                });
            }

        }

        return unsubscribeFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {

        if (state.get() == State.Closing || state.get() == State.Closed) {
            return CompletableFuture.completedFuture(null);
        }
        state.set(State.Closing);

        AtomicReference<Throwable> closeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(numPartitions);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        for (Consumer consumer : consumers) {
            if (consumer != null) {
                consumer.closeAsync().handle((closed, ex) -> {
                    if (ex != null) {
                        closeFail.compareAndSet(null, ex);
                    }
                    if (completed.decrementAndGet() == 0) {
                        if (closeFail.get() == null) {
                            state.set(State.Closed);
                            closeFuture.complete(null);
                            log.info("[{}] [{}] Closed Partitioned Consumer", topic, subscription);
                            client.cleanupConsumer(this);
                        } else {
                            state.set(State.Failed);
                            closeFuture.completeExceptionally(closeFail.get());
                            log.error("[{}] [{}] Could not close Partitioned Consumer", topic, subscription,
                                    closeFail.get().getCause());
                        }
                    }

                    return null;
                });
            }

        }

        return closeFuture;
    }

    @Override
    public boolean isConnected() {
        for (ConsumerImpl consumer : consumers) {
            if (!consumer.isConnected()) {
                return false;
            }
        }
        return true;
    }

    @Override
    void connectionFailed(PulsarClientException exception) {
        // noop

    }

    @Override
    void connectionOpened(ClientCnx cnx) {
        // noop

    }

    void messageReceived(Message message) {

        boolean shouldLock = false;
        try {
            /**
             * this method is thread-safe: as only 1 ConsumerImpl thread calls at a time. Lock only if incomingMessages
             * (blocking-queue) is not full: else put(..) will block thread and will hold lock which may create deadlock
             */
            shouldLock = !(incomingMessages.remainingCapacity() == 0);
            if (shouldLock) {
                lock.readLock().lock();
            }
            // if asyncReceive is waiting : return message to callback without adding to incomingMessages queue
            if (!pendingReceives.isEmpty()) {
                CompletableFuture<Message> receivedFuture = pendingReceives.poll();
                listenerExecutor.execute(() -> receivedFuture.complete(message));
                // unlock if it is already locked
                if (shouldLock) {
                    lock.readLock().unlock();
                }
            } else {
                // Enqueue the message so that it can be retrieved when application calls receive()
                // Waits for the queue to have space for the message
                incomingMessages.put(message);
                // unlock if it is already locked
                if (shouldLock) {
                    lock.readLock().unlock();
                }
            }
        } catch (InterruptedException e) {
            // unlock if it is already locked
            if (shouldLock) {
                lock.readLock().unlock();
            }
            Thread.currentThread().interrupt();
        }

        if (listener != null) {
            // Trigger the notification on the message listener in a separate thread to avoid blocking the networking
            // thread while the message processing happens
            listenerExecutor.execute(() -> {
                Message msg;
                try {
                    msg = internalReceive();
                } catch (PulsarClientException e) {
                    log.warn("[{}] [{}] Failed to dequeue the message for listener", topic, subscription, e);
                    return;
                }

                try {
                    log.debug("[{}][{}] Calling message listener for message {}", topic, subscription, message);
                    listener.received(PartitionedConsumerImpl.this, msg);
                } catch (Throwable t) {
                    log.error("[{}][{}] Message listener error in processing message: {}", topic, subscription, message,
                            t);
                }
            });
        }
    }

    @Override
    String getHandlerName() {
        return subscription;
    }

    private ConsumerConfiguration getInternalConsumerConfig() {
        ConsumerConfiguration internalConsumerConfig = new ConsumerConfiguration();
        internalConsumerConfig.setReceiverQueueSize(conf.getReceiverQueueSize());
        internalConsumerConfig.setSubscriptionType(conf.getSubscriptionType());
        internalConsumerConfig.setConsumerName(consumerName);
        if (conf.getAckTimeoutMillis() != 0) {
            internalConsumerConfig.setAckTimeout(conf.getAckTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        internalConsumerConfig.setMessageListener((consumer, msg) -> {
            if (msg != null) {
                messageReceived(msg);
            }
        });

        return internalConsumerConfig;
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        for (ConsumerImpl c : consumers) {
            c.redeliverUnacknowledgedMessages();
        }
    }

    /**
     * helper method that returns current state of data structure used to track acks for batch messages
     * 
     * @return true if all batch messages have been acknowledged
     */
    public boolean isBatchingAckTrackerEmpty() {
        boolean state = true;
        for (Consumer consumer : consumers) {
            state &= ((ConsumerImpl) consumer).isBatchingAckTrackerEmpty();
        }
        return state;
    }

    @Override
    public synchronized ConsumerStats getStats() {
        if (stats == null) {
            return null;
        }
        stats.reset();
        for (int i = 0; i < numPartitions; i++) {
            stats.updateCumulativeStats(consumers.get(i).getStats());
        }
        return stats;
    }

    private static final Logger log = LoggerFactory.getLogger(PartitionedConsumerImpl.class);

}
