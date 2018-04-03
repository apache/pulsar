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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class PartitionedConsumerImpl<T> extends ConsumerBase<T> {

    private final List<ConsumerImpl<T>> consumers;

    // Queue of partition consumers on which we have stopped calling receiveAsync() because the
    // shared incoming queue was full
    private final ConcurrentLinkedQueue<ConsumerImpl<T>> pausedConsumers;

    // Threshold for the shared queue. When the size of the shared queue goes below the threshold, we are going to
    // resume receiving from the paused consumer partitions
    private final int sharedQueueResumeThreshold;

    private final int numPartitions;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConsumerStatsRecorderImpl stats;
    private final UnAckedMessageTracker unAckedMessageTracker;

    PartitionedConsumerImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf, int numPartitions,
                            ExecutorService listenerExecutor, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema) {
        super(client, conf.getSingleTopic(), conf, Math.max(Math.max(2, numPartitions), conf.getReceiverQueueSize()),
                listenerExecutor, subscribeFuture, schema);
        this.consumers = Lists.newArrayListWithCapacity(numPartitions);
        this.pausedConsumers = new ConcurrentLinkedQueue<>();
        this.sharedQueueResumeThreshold = maxReceiverQueueSize / 2;
        this.numPartitions = numPartitions;

        if (conf.getAckTimeoutMillis() != 0) {
            this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf.getAckTimeoutMillis());
        } else {
            this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
        }

        stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ConsumerStatsRecorderImpl() : null;
        checkArgument(conf.getReceiverQueueSize() > 0,
                "Receiver queue size needs to be greater than 0 for Partitioned Topics");
        start();
    }

    private void start() {
        AtomicReference<Throwable> subscribeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger();
        ConsumerConfigurationData<T> internalConfig = getInternalConsumerConfig();
        for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
            String partitionName = TopicName.get(topic).getPartition(partitionIndex).toString();
            ConsumerImpl<T> consumer = new ConsumerImpl<>(client, partitionName, internalConfig,
                    client.externalExecutorProvider().getExecutor(), partitionIndex, new CompletableFuture<>(), schema);
            consumers.add(consumer);
            consumer.subscribeFuture().handle((cons, subscribeException) -> {
                if (subscribeException != null) {
                    setState(State.Failed);
                    subscribeFail.compareAndSet(null, subscribeException);
                    client.cleanupConsumer(this);
                }
                if (completed.incrementAndGet() == numPartitions) {
                    if (subscribeFail.get() == null) {
                        try {
                            // We have successfully created N consumers, so we can start receiving messages now
                            starReceivingMessages();
                            setState(State.Ready);
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

    private void starReceivingMessages() throws PulsarClientException {
        for (ConsumerImpl<T> consumer : consumers) {
            consumer.sendFlowPermitsToBroker(consumer.getConnectionHandler().cnx(), conf.getReceiverQueueSize());
            receiveMessageFromConsumer(consumer);
        }
    }

    private void receiveMessageFromConsumer(ConsumerImpl<T> consumer) {
        consumer.receiveAsync().thenAccept(message -> {
            // Process the message, add to the queue and trigger listener or async callback
            messageReceived(message);

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

    private void resumeReceivingFromPausedConsumersIfNeeded() {
        lock.readLock().lock();
        try {
            if (incomingMessages.size() <= sharedQueueResumeThreshold && !pausedConsumers.isEmpty()) {
                while (true) {
                    ConsumerImpl<T> consumer = pausedConsumers.poll();
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
    protected Message<T> internalReceive() throws PulsarClientException {
        Message<T> message;
        try {
            message = incomingMessages.take();
            unAckedMessageTracker.add((MessageIdImpl) message.getMessageId());
            resumeReceivingFromPausedConsumersIfNeeded();
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    protected Message<T> internalReceive(int timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> message;
        try {
            message = incomingMessages.poll(timeout, unit);
            if (message != null) {
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
    protected CompletableFuture<Message<T>> internalReceiveAsync() {
        CompletableFuture<Message<T>> result = new CompletableFuture<>();
        Message<T> message;
        try {
            lock.writeLock().lock();
            message = incomingMessages.poll(0, TimeUnit.SECONDS);
            if (message == null) {
                pendingReceives.add(result);
            } else {
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
        checkArgument(messageId instanceof MessageIdImpl);

        if (getState() != State.Ready) {
            return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
        }

        if (ackType == AckType.Cumulative) {
            return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                    "Cumulative acknowledge not supported for partitioned topics"));
        } else {

            ConsumerImpl<T> consumer = consumers.get(((MessageIdImpl) messageId).getPartitionIndex());
            return consumer.doAcknowledge(messageId, ackType, properties).thenRun(() ->
                    unAckedMessageTracker.remove(messageId));
        }

    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Partitioned Consumer was already closed"));
        }
        setState(State.Closing);

        AtomicReference<Throwable> unsubscribeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(numPartitions);
        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        for (Consumer<T> consumer : consumers) {
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
                            log.info("[{}] [{}] Unsubscribed Partitioned Consumer", topic, subscription);
                        } else {
                            setState(State.Failed);
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
        if (getState() == State.Closing || getState() == State.Closed) {
            unAckedMessageTracker.close();
            return CompletableFuture.completedFuture(null);
        }
        setState(State.Closing);

        AtomicReference<Throwable> closeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(numPartitions);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        for (Consumer<T> consumer : consumers) {
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
                            log.info("[{}] [{}] Closed Partitioned Consumer", topic, subscription);
                            client.cleanupConsumer(this);
                            // fail all pending-receive futures to notify application
                            failPendingReceive();
                        } else {
                            setState(State.Failed);
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

    @Override
    public boolean isConnected() {
        return consumers.stream().allMatch(ConsumerImpl::isConnected);
    }

    void messageReceived(Message<T> message) {
        lock.writeLock().lock();
        try {
            unAckedMessageTracker.add(message.getMessageId());
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Received message from partitioned-consumer {}", topic, subscription, message.getMessageId());
            }
            // if asyncReceive is waiting : return message to callback without adding to incomingMessages queue
            if (!pendingReceives.isEmpty()) {
                CompletableFuture<Message<T>> receivedFuture = pendingReceives.poll();
                listenerExecutor.execute(() -> receivedFuture.complete(message));
            } else {
                // Enqueue the message so that it can be retrieved when application calls receive()
                // Waits for the queue to have space for the message
                // This should never block cause PartitonedConsumerImpl should always use GrowableArrayBlockingQueue
                incomingMessages.put(message);
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
                Message<T> msg;
                try {
                    msg = internalReceive();
                } catch (PulsarClientException e) {
                    log.warn("[{}] [{}] Failed to dequeue the message for listener", topic, subscription, e);
                    return;
                }

                try {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Calling message listener for message {}", topic, subscription, message.getMessageId());
                    }
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

    private ConsumerConfigurationData<T> getInternalConsumerConfig() {
        ConsumerConfigurationData<T> internalConsumerConfig = new ConsumerConfigurationData<>();
        internalConsumerConfig.setReceiverQueueSize(conf.getReceiverQueueSize());
        internalConsumerConfig.setSubscriptionName(conf.getSubscriptionName());
        internalConsumerConfig.setSubscriptionType(conf.getSubscriptionType());
        internalConsumerConfig.setConsumerName(consumerName);
        internalConsumerConfig.setAcknowledgementsGroupTimeMicros(conf.getAcknowledgementsGroupTimeMicros());
        internalConsumerConfig.setPriorityLevel(conf.getPriorityLevel());
        internalConsumerConfig.setProperties(conf.getProperties());
        internalConsumerConfig.setReadCompacted(conf.isReadCompacted());
        if (null != conf.getConsumerEventListener()) {
            internalConsumerConfig.setConsumerEventListener(conf.getConsumerEventListener());
        }

        int receiverQueueSize = Math.min(conf.getReceiverQueueSize(),
                conf.getMaxTotalReceiverQueueSizeAcrossPartitions() / numPartitions);
        internalConsumerConfig.setReceiverQueueSize(receiverQueueSize);

        if (conf.getCryptoKeyReader() != null) {
            internalConsumerConfig.setCryptoKeyReader(conf.getCryptoKeyReader());
            internalConsumerConfig.setCryptoFailureAction(conf.getCryptoFailureAction());
        }
        if (conf.getAckTimeoutMillis() != 0) {
            internalConsumerConfig.setAckTimeoutMillis(conf.getAckTimeoutMillis());
        }

        return internalConsumerConfig;
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        synchronized (this) {
            for (ConsumerImpl<T> c : consumers) {
                c.redeliverUnacknowledgedMessages();
            }
            incomingMessages.clear();
            unAckedMessageTracker.clear();
            resumeReceivingFromPausedConsumersIfNeeded();
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
        checkArgument(messageIds.stream().findFirst().get() instanceof MessageIdImpl);
        if (conf.getSubscriptionType() != SubscriptionType.Shared) {
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        removeExpiredMessagesFromQueue(messageIds);
        messageIds.stream()
            .map(messageId -> (MessageIdImpl)messageId)
            .collect(Collectors.groupingBy(MessageIdImpl::getPartitionIndex, Collectors.toSet()))
            .forEach((partitionIndex, messageIds1) ->
                consumers.get(partitionIndex).redeliverUnacknowledgedMessages(
                    messageIds1.stream().map(mid -> (MessageId)mid).collect(Collectors.toSet())));
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
        return FutureUtil.failedFuture(new PulsarClientException("Seek operation not supported on partitioned topics"));
    }

    List<ConsumerImpl<T>> getConsumers() {
        return consumers;
    }

    @Override
    public int getAvailablePermits() {
        return consumers.stream().mapToInt(ConsumerImpl::getAvailablePermits).sum();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return consumers.stream().allMatch(Consumer::hasReachedEndOfTopic);
    }

    @Override
    public int numMessagesInQueue() {
        return incomingMessages.size() + consumers.stream().mapToInt(ConsumerImpl::numMessagesInQueue).sum();
    }

    @Override
    public synchronized ConsumerStatsRecorderImpl getStats() {
        if (stats == null) {
            return null;
        }
        stats.reset();
        for (int i = 0; i < numPartitions; i++) {
            stats.updateCumulativeStats(consumers.get(i).getStats());
        }
        return stats;
    }

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
            while (message != null) {
                MessageIdImpl messageId = (MessageIdImpl) message.getMessageId();
                if (!messageIds.contains(messageId)) {
                    messageIds.add(messageId);
                    break;
                }
                message = incomingMessages.poll();
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PartitionedConsumerImpl.class);
}
