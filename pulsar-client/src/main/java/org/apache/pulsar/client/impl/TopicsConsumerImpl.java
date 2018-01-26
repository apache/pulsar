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
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.naming.DestinationName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicsConsumerImpl extends ConsumerBase {

    // Map <topic+partition, consumer>, when get do ACK, consumer will by find by topic name
    private final ConcurrentHashMap<String, ConsumerImpl> consumers;

    // Map <topic, partitionNumber>, store partition number for each topic
    private final ConcurrentHashMap<String, Integer> topics;

    // Queue of partition consumers on which we have stopped calling receiveAsync() because the
    // shared incoming queue was full
    private final ConcurrentLinkedQueue<ConsumerImpl> pausedConsumers;

    // Threshold for the shared queue. When the size of the shared queue goes below the threshold, we are going to
    // resume receiving from the paused consumer partitions
    private final int sharedQueueResumeThreshold;

    // sum of topicPartitions, simple topic has 1, partitioned topic equals to partition number.
    AtomicInteger numberTopicPartitions;

    //private final int numPartitions;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConsumerStats stats;
    private final UnAckedMessageTracker unAckedMessageTracker;

    // use in addConsumer
    private AtomicReference<Throwable> subscribeFail = new AtomicReference<Throwable>();
    private AtomicInteger completed = new AtomicInteger(0);

    TopicsConsumerImpl(PulsarClientImpl client, Collection<String> topics, String subscription,
                       ConsumerConfiguration conf, ExecutorService listenerExecutor,
                       CompletableFuture<Consumer> subscribeFuture) {
        super(client, "TopicsConsumerFakeTopicName", subscription,
            conf, Math.max(2, conf.getReceiverQueueSize()), listenerExecutor,
            subscribeFuture);

        topics.forEach(topic -> checkArgument(DestinationName.isValid(topic), "Invalid topic name:" + topic));

        this.topics = new ConcurrentHashMap<>();
        this.consumers = new ConcurrentHashMap<>();
        this.pausedConsumers = new ConcurrentLinkedQueue<>();
        this.sharedQueueResumeThreshold = maxReceiverQueueSize / 2;
        this.numberTopicPartitions = new AtomicInteger(0);

        if (conf.getAckTimeoutMillis() != 0) {
            this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf.getAckTimeoutMillis());
        } else {
            this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
        }

        stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ConsumerStats() : null;
        checkArgument(conf.getReceiverQueueSize() > 0,
            "Receiver queue size needs to be greater than 0 for Topics Consumer");

        ConsumerConfiguration internalConfig = getInternalConsumerConfig();
        topics.forEach(topic ->
            client.getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
                }

                if (metadata.partitions > 1) {
                    numberTopicPartitions.addAndGet(metadata.partitions);
                    this.topics.putIfAbsent(topic, metadata.partitions);

                    IntStream.range(0, metadata.partitions).forEach(partitionIndex -> {
                        String partitionName = DestinationName.get(topic).getPartition(partitionIndex).toString();
                        addConsumer(
                            new ConsumerImpl(client, partitionName, subscription, internalConfig,
                                client.externalExecutorProvider().getExecutor(), partitionIndex,
                                new CompletableFuture<Consumer>()));
                    });
                } else {
                    numberTopicPartitions.incrementAndGet();
                    this.topics.putIfAbsent(topic, 1);

                    addConsumer(
                        new ConsumerImpl(client, topic, subscription, internalConfig,
                            client.externalExecutorProvider().getExecutor(), 0,
                            new CompletableFuture<Consumer>()));
                }
            }).exceptionally(ex -> {
                log.warn("[{}] Failed to get partitioned topic metadata: {}", topic, ex.getMessage());
                subscribeFuture.completeExceptionally(ex);
                return null;
            })
        );
    }

    private void addConsumer(ConsumerImpl consumer) {
        consumers.putIfAbsent(consumer.getTopic(), consumer);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Put topic consumer-{} into topics consumer", topic, consumer.getTopic());
        }
        consumer.subscribeFuture().handle((cons, subscribeException) -> {
            if (subscribeException != null) {
                setState(State.Failed);
                subscribeFail.compareAndSet(null, subscribeException);
                client.cleanupConsumer(this);
            }
            if (completed.incrementAndGet() == numberTopicPartitions.get()) {
                if (subscribeFail.get() == null) {
                    try {
                        if (numberTopicPartitions.get() > maxReceiverQueueSize) {
                            setMaxReceiverQueueSize(numberTopicPartitions.get());
                        }
                        // We have successfully created N consumers, so we can start receiving messages now
                        startReceivingMessages();
                        setState(State.Ready);
                        subscribeFuture().complete(TopicsConsumerImpl.this);
                        log.info("[{}] [{}] Created topics consumer with {} sub-consumers",
                            topic, subscription, numberTopicPartitions.get());
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
                log.error("[{}] [{}] Could not create topics consumer.", topic, subscription,
                    subscribeFail.get().getCause());
            }
            return null;
        });
    }

    private void startReceivingMessages() throws PulsarClientException {
        consumers.values().stream().forEach(consumer -> {
            consumer.sendFlowPermitsToBroker(consumer.cnx(), conf.getReceiverQueueSize());
            receiveMessageFromConsumer(consumer);
        });
    }

    private void receiveMessageFromConsumer(ConsumerImpl consumer) {
        consumer.receiveAsync().thenAccept(message -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Receive message from sub consumer:{}",
                    topic, subscription, consumer.getTopic());
            }
            // Process the message, add to the queue and trigger listener or async callback
            messageReceived(consumer, message);

            // we're modifying pausedConsumers
            lock.writeLock().lock();
            try {
                int size = incomingMessages.size();
                if (size >= maxReceiverQueueSize
                        || (size > sharedQueueResumeThreshold && !pausedConsumers.isEmpty())) {
                    // mark this consumer to be resumed later: if No more space left in shared queue,
                    // or if any consumer is already paused (to create fair chance for already paused consumers)
                    pausedConsumers.add(consumer);
                } else {
                    // Schedule next receiveAsync() if the incoming queue is not full. Use a different thread to avoid
                    // recursion and stack overflow
                    client.eventLoopGroup().execute(() -> {
                        receiveMessageFromConsumer(consumer);
                    });
                }
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    private void messageReceived(ConsumerImpl consumer, Message message) {
        checkArgument(message instanceof MessageImpl);
        lock.writeLock().lock();
        try {
            TopicMessageImpl topicMessage = new TopicMessageImpl(consumer.getTopic(), message);
            unAckedMessageTracker.add(topicMessage.getMessageId());

            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Received message from topics-consumer {}",
                    topic, subscription, message.getMessageId());
            }

            // if asyncReceive is waiting : return message to callback without adding to incomingMessages queue
            if (!pendingReceives.isEmpty()) {
                CompletableFuture<Message> receivedFuture = pendingReceives.poll();
                listenerExecutor.execute(() -> receivedFuture.complete(topicMessage));
            } else {
                // Enqueue the message so that it can be retrieved when application calls receive()
                // Waits for the queue to have space for the message
                // This should never block cause PartitonedConsumerImpl should always use GrowableArrayBlockingQueue
                incomingMessages.put(topicMessage);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.writeLock().unlock();
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
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Calling message listener for message {}",
                            topic, subscription, message.getMessageId());
                    }
                    listener.received(TopicsConsumerImpl.this, msg);
                } catch (Throwable t) {
                    log.error("[{}][{}] Message listener error in processing message: {}",
                        topic, subscription, message, t);
                }
            });
        }
    }

    private void resumeReceivingFromPausedConsumersIfNeeded() {
        lock.readLock().lock();
        try {
            if (incomingMessages.size() <= sharedQueueResumeThreshold && !pausedConsumers.isEmpty()) {
                while (true) {
                    ConsumerImpl consumer = pausedConsumers.poll();
                    if (consumer == null) {
                        break;
                    }

                    // if messages are readily available on consumer we will attempt to writeLock on the same thread
                    client.eventLoopGroup().execute(() -> {
                        receiveMessageFromConsumer(consumer);
                    });
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    protected Message internalReceive() throws PulsarClientException {
        Message message;
        try {
            message = incomingMessages.take();
            checkState(message instanceof TopicMessageImpl);
            unAckedMessageTracker.add(message.getMessageId());
            resumeReceivingFromPausedConsumersIfNeeded();
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
            if (message != null) {
                checkArgument(message instanceof TopicMessageImpl);
                unAckedMessageTracker.add(message.getMessageId());
            }
            resumeReceivingFromPausedConsumersIfNeeded();
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    protected CompletableFuture<Message> internalReceiveAsync() {
        CompletableFuture<Message> result = new CompletableFuture<>();
        Message message;
        try {
            lock.writeLock().lock();
            message = incomingMessages.poll(0, TimeUnit.SECONDS);
            if (message == null) {
                pendingReceives.add(result);
            } else {
                checkState(message instanceof TopicMessageImpl);
                unAckedMessageTracker.add(message.getMessageId());
                resumeReceivingFromPausedConsumersIfNeeded();
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
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String,Long> properties) {
        checkArgument(messageId instanceof TopicMessageIdImpl);

        if (getState() != State.Ready) {
            return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
        }

        if (ackType == AckType.Cumulative) {
            return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                    "Cumulative acknowledge not supported for topics consumer"));
        } else {
            ConsumerImpl consumer = consumers.get(messageId.getTopicName());

            MessageId innerId = ((TopicMessageIdImpl)messageId).getInnerMessageId();
            return consumer.doAcknowledge(innerId, ackType, properties)
                .thenRun(() ->
                    unAckedMessageTracker.remove(messageId));
        }
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }
        setState(State.Closing);

        AtomicReference<Throwable> unsubscribeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(numberTopicPartitions.get());
        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();

        consumers.values().stream().forEach(consumer -> {
            if (consumer != null) {
                consumer.unsubscribeAsync().handle((unsubscribed, ex) -> {
                    if (ex != null) {
                        unsubscribeFail.compareAndSet(null, ex);
                    }
                    if (completed.decrementAndGet() == 0) {
                        if (unsubscribeFail.get() == null) {
                            setState(State.Closed);
                            unAckedMessageTracker.close();
                            unsubscribeFuture.complete(null);
                            log.info("[{}] [{}] [{}] Unsubscribed Topics Consumer",
                                topic, subscription, consumerName);
                        } else {
                            setState(State.Failed);
                            unsubscribeFuture.completeExceptionally(unsubscribeFail.get());
                            log.error("[{}] [{}] [{}] Could not unsubscribe Topics Consumer",
                                topic, subscription, consumerName, unsubscribeFail.get().getCause());
                        }
                    }

                    return null;
                });
            }
        });

        return unsubscribeFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            unAckedMessageTracker.close();
            return CompletableFuture.completedFuture(null);
        }
        setState(State.Closing);

        AtomicReference<Throwable> closeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(numberTopicPartitions.get());
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        consumers.values().stream().forEach(consumer -> {
            if (consumer != null) {
                consumer.closeAsync().handle((closed, ex) -> {
                    if (ex != null) {
                        closeFail.compareAndSet(null, ex);
                    }
                    if (completed.decrementAndGet() == 0) {
                        if (closeFail.get() == null) {
                            setState(State.Closed);
                            unAckedMessageTracker.close();
                            closeFuture.complete(null);
                            log.info("[{}] [{}] Closed Topics Consumer", topic, subscription);
                            client.cleanupConsumer(this);
                            // fail all pending-receive futures to notify application
                            failPendingReceive();
                        } else {
                            setState(State.Failed);
                            closeFuture.completeExceptionally(closeFail.get());
                            log.error("[{}] [{}] Could not close Topics Consumer", topic, subscription,
                                closeFail.get().getCause());
                        }
                    }

                    return null;
                });
            }
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

    @Override
    public boolean isConnected() {
        return consumers.values().stream().allMatch(consumer -> consumer.isConnected());
    }

    @Override
    void connectionFailed(PulsarClientException exception) {
        // noop

    }

    @Override
    void connectionOpened(ClientCnx cnx) {
        // noop

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
        if (conf.getCryptoKeyReader() != null) {
            internalConsumerConfig.setCryptoKeyReader(conf.getCryptoKeyReader());
            internalConsumerConfig.setCryptoFailureAction(conf.getCryptoFailureAction());
        }
        if (conf.getAckTimeoutMillis() != 0) {
            internalConsumerConfig.setAckTimeout(conf.getAckTimeoutMillis(), TimeUnit.MILLISECONDS);
        }

        return internalConsumerConfig;
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        synchronized (this) {
            consumers.values().stream().forEach(consumer -> consumer.redeliverUnacknowledgedMessages());
            incomingMessages.clear();
            unAckedMessageTracker.clear();
            resumeReceivingFromPausedConsumersIfNeeded();
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
        checkArgument(messageIds.stream().findFirst().get() instanceof TopicMessageIdImpl);

        if (conf.getSubscriptionType() != SubscriptionType.Shared) {
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        removeExpiredMessagesFromQueue(messageIds);
        messageIds.stream().map(messageId -> (TopicMessageIdImpl)messageId)
            .collect(Collectors.groupingBy(TopicMessageIdImpl::getTopicName, Collectors.toSet()))
            .forEach((topicName, messageIds1) ->
                consumers.get(topicName)
                    .redeliverUnacknowledgedMessages(messageIds1.stream()
                        .map(mid -> mid.getInnerMessageId()).collect(Collectors.toSet())));
        resumeReceivingFromPausedConsumersIfNeeded();
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        try {
            seekAsync(messageId).get();
        } catch (ExecutionException e) {
            throw new PulsarClientException(e.getCause());
        } catch (InterruptedException e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return FutureUtil.failedFuture(new PulsarClientException("Seek operation not supported on topics consumer"));
    }

    /**
     * helper method that returns current state of data structure used to track acks for batch messages
     *
     * @return true if all batch messages have been acknowledged
     */
    public boolean isBatchingAckTrackerEmpty() {
        return consumers.values().stream().allMatch(consumer -> consumer.isBatchingAckTrackerEmpty());
    }


    @Override
    public int getAvailablePermits() {
        return consumers.values().stream().mapToInt(ConsumerImpl::getAvailablePermits).sum();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return consumers.values().stream().allMatch(Consumer::hasReachedEndOfTopic);
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

        consumers.values().stream().forEach(consumer -> stats.updateCumulativeStats(consumer.getStats()));
        return stats;
    }

    public UnAckedMessageTracker getUnAckedMessageTracker() {
        return unAckedMessageTracker;
    }

    private void removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
        Message peek = incomingMessages.peek();
        if (peek != null) {
            if (!messageIds.contains(peek.getMessageId())) {
                // first message is not expired, then no message is expired in queue.
                return;
            }

            // try not to remove elements that are added while we remove
            Message message = incomingMessages.poll();
            checkState(message instanceof TopicMessageImpl);
            while (message != null) {
                MessageId messageId = message.getMessageId();
                if (!messageIds.contains(messageId)) {
                    messageIds.add(messageId);
                    break;
                }
                message = incomingMessages.poll();
            }
        }
    }

    // get topics name
    public List<String> getTopics() {
        return topics.keySet().stream().collect(Collectors.toList());
    }

    // get partitioned topics name
    public List<String> getPartitionedTopics() {
        return consumers.keySet().stream().collect(Collectors.toList());
    }

    // get partitioned consumers
    public List<ConsumerImpl> getConsumers() {
        return consumers.values().stream().collect(Collectors.toList());
    }

    private static final Logger log = LoggerFactory.getLogger(TopicsConsumerImpl.class);
}
