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
import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotSupportedException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

public class PartitionedProducerImpl<T> extends ProducerBase<T> {

    private static final Logger LOG = Logger.get(PartitionedProducerImpl.class);
    private final Logger log;

    private final Map<Integer, ProducerImpl<T>> producers = new ConcurrentHashMap<>();
    // For each producer that a send started, until its creation was handled, see creationHandled()
    private final Map<ProducerImpl<T>, CompletableFuture<Void>> lazyStarts = new ConcurrentHashMap<>();
    // The queue of each producer that has one. Changed while holding the map's lock
    private final Map<ProducerImpl<T>, LazyStartQueue> lazyStartQueues = new ConcurrentHashMap<>();
    // The threads that are handing queued sends over to a producer
    private final Set<Thread> handingOverThreads = ConcurrentHashMap.newKeySet();
    private final CompletableFuture<Void> closeStarted = new CompletableFuture<>();
    private final MessageRouter routerPolicy;
    private final PartitionedTopicProducerStatsRecorderImpl stats;
    private final ProducerInterceptors internalProducerInterceptors;
    private TopicMetadata topicMetadata;
    private final int firstPartitionIndex;
    private String overrideProducerName;

    // timeout related to auto check and subscribe partition increasement
    private volatile Timeout partitionsAutoUpdateTimeout = null;
    TopicsPartitionChangedListener topicsPartitionChangedListener;
    CompletableFuture<Void> partitionsAutoUpdateFuture = null;

    public PartitionedProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
                                   int numPartitions, CompletableFuture<Producer<T>> producerCreatedFuture,
                                   Schema<T> schema, ProducerInterceptors interceptors) {
        super(client, topic, conf, producerCreatedFuture, schema, interceptors);
        this.log = LOG.with()
                .attr("topic", topic)
                .attr("producerName", () -> overrideProducerName)
                .build();
        this.topicMetadata = new TopicMetadataImpl(numPartitions);
        this.routerPolicy = getMessageRouter();
        this.internalProducerInterceptors = interceptors != null
                ? createInternalProducerInterceptors(interceptors)
                : null;
        stats = client.getConfiguration().getStatsIntervalSeconds() > 0
                ? new PartitionedTopicProducerStatsRecorderImpl()
                : null;

        // The across-partitions budget is a total shared by every partition, so it is divided between
        // them here, where the partition count is finally known. Both limits are read through their
        // "configured" markers rather than by comparing against their unset value, because that value
        // is 0 for both and 0 is also a meaningful explicit setting.
        // MaxPendingMessagesAcrossPartitions doesn't support partial partition such as SinglePartition correctly
        int maxPendingMessages = conf.getMaxPendingMessages();
        int maxPendingMessagesAcrossPartitions = conf.getMaxPendingMessagesAcrossPartitions();
        if (conf.isMaxPendingMessagesAcrossPartitionsConfigured() && maxPendingMessagesAcrossPartitions > 0) {
            // Never divide down to 0: a budget smaller than the partition count still asks for the
            // smallest possible queue, not for the queue bound to be removed altogether.
            int maxPendingMsgsForOnePartition =
                    Math.max(1, maxPendingMessagesAcrossPartitions / numPartitions);
            // A per-producer limit the application set wins, including an explicit 0 ("no message-count
            // limit"). Only a limit it left unset — or one filled in as a default — adopts the share.
            maxPendingMessages = conf.isMaxPendingMessagesConfigured()
                    ? Math.min(maxPendingMessages, maxPendingMsgsForOnePartition)
                    : maxPendingMsgsForOnePartition;
            conf.setMaxPendingMessages(maxPendingMessages);
        }


        final List<Integer> indexList;
        if (conf.isLazyStartPartitionedProducers()
                && conf.getAccessMode() == ProducerAccessMode.Shared) {
            // try to create producer at least one partition
            indexList = Collections.singletonList(routerPolicy
                    .choosePartition(((TypedMessageBuilderImpl<T>) newMessage()).getMessage(), topicMetadata));
        } else {
            // try to create producer for all partitions
            indexList = IntStream.range(0, topicMetadata.numPartitions()).boxed().collect(Collectors.toList());
        }

        firstPartitionIndex = indexList.get(0);
        start(indexList);

        // start track and auto subscribe partition increasement
        if (conf.isAutoUpdatePartitions()) {
            topicsPartitionChangedListener = new TopicsPartitionChangedListener();
            Timeout timeout = client.timer()
                .newTimeout(partitionsAutoUpdateTimerTask,
                        conf.getAutoUpdatePartitionsIntervalSeconds(), TimeUnit.SECONDS);
            partitionsAutoUpdateTimeout = timeout;
            // The creation can end inside start(), before there is a timer for closeAsync() to cancel.
            // closeAsync() sets the state before it reads the timer, so one of the two always cancels it.
            if (isTerminalState()) {
                timeout.cancel();
            }
        }
    }

    private MessageRouter getMessageRouter() {
        MessageRouter messageRouter;

        MessageRoutingMode messageRouteMode = conf.getMessageRoutingMode();

        switch (messageRouteMode) {
            case CustomPartition:
                messageRouter = Objects.requireNonNull(conf.getCustomMessageRouter());
                break;
            case SinglePartition:
                messageRouter = new SinglePartitionMessageRouterImpl(
                        ThreadLocalRandom.current().nextInt(topicMetadata.numPartitions()), conf.getHashingScheme());
                break;
            case RoundRobinPartition:
            default:
                messageRouter = new RoundRobinPartitionMessageRouterImpl(
                        conf.getHashingScheme(),
                        ThreadLocalRandom.current().nextInt(topicMetadata.numPartitions()),
                        conf.isBatchingEnabled(),
                        TimeUnit.MICROSECONDS.toMillis(conf.batchingPartitionSwitchFrequencyIntervalMicros()));
        }

        return messageRouter;
    }

    @Override
    public String getProducerName() {
        return producers.get(firstPartitionIndex).getProducerName();
    }

    @Override
    public long getLastSequenceId() {
        // Return the highest sequence id across all partitions. This will be correct,
        // since there is a single id generator across all partitions for the same producer
        return producers.values().stream().map(Producer::getLastSequenceId).mapToLong(Long::longValue).max().orElse(-1);
    }

    private void start(List<Integer> indexList) {
        AtomicReference<Throwable> createFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger();

        final BiConsumer<Boolean, Throwable> afterCreatingProducer = (failFast, createException) -> {
            final Runnable closeRunnable = () -> {
                log.error()
                        .exception(createFail.get().getCause())
                        .log("Could not create partitioned producer.");
                closeAsync().handle((ok, closeException) -> {
                    producerCreatedFuture().completeExceptionally(createFail.get());
                    client.cleanupProducer(this);
                    return null;
                });
            };

            if (createException != null) {
                setState(State.Failed);
                createFail.compareAndSet(null, createException);
                if (failFast) {
                    closeRunnable.run();
                }
            }
            // we mark success if all the partitions are created
            // successfully, else we throw an exception
            // due to any
            // failure in one of the partitions and close the successfully
            // created partitions
            if (completed.incrementAndGet() == indexList.size()) {
                if (createFail.get() == null) {
                    setState(State.Ready);
                    log.info("Created partitioned producer");
                    producerCreatedFuture().complete(PartitionedProducerImpl.this);
                } else {
                    closeRunnable.run();
                }
            }
        };

        final ProducerImpl<T> firstProducer = createProducer(indexList.get(0));
        firstProducer.producerCreatedFuture().handle((prod, createException) -> {
            afterCreatingProducer.accept(true, createException);
            if (createException != null) {
                throw new RuntimeException(createException);
            }
            overrideProducerName = firstProducer.getProducerName();
            return Optional.of(overrideProducerName);
        }).thenApply(name -> {
            for (int i = 1; i < indexList.size(); i++) {
                createProducer(indexList.get(i), name).producerCreatedFuture().handle((prod, createException) -> {
                    afterCreatingProducer.accept(false, createException);
                    return null;
                });
            }
            return null;
        });
    }

    private ProducerImpl<T> createProducer(final int partitionIndex) {
        return createProducer(partitionIndex, Optional.empty());
    }

    private ProducerImpl<T> createProducer(final int partitionIndex, final Optional<String> overrideProducerName) {
        return producers.computeIfAbsent(partitionIndex,
                (idx) -> newPartitionProducer(idx, overrideProducerName));
    }

    private ProducerImpl<T> newPartitionProducer(int partitionIndex, Optional<String> overrideProducerName) {
        String partitionName = TopicName.get(topic).getPartition(partitionIndex).toString();
        return client.newProducerImpl(partitionName, partitionIndex,
                conf, schema, internalProducerInterceptors, new CompletableFuture<>(), overrideProducerName);
    }

    @Override
    CompletableFuture<MessageId> internalSendAsync(Message<?> message) {
        return internalSendWithTxnAsync(message, null);
    }

    @Override
    CompletableFuture<MessageId> internalSendWithTxnAsync(Message<?> message, Transaction txn) {
        CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
        if (txn != null && !((TransactionImpl) txn).checkIfOpen(completableFuture)) {
            return completableFuture;
        }
        int partition = routerPolicy.choosePartition(message, topicMetadata);
        checkArgument(partition >= 0 && partition < topicMetadata.numPartitions(),
                "Illegal partition index chosen by the message routing policy: " + partition);

        // Check the state first: a closed producer must not start a partition producer
        switch (getState()) {
            case Ready:
            case Connecting:
                break; // Ok
            case Closing:
            case Closed:
                return FutureUtil.failedFuture(
                        new PulsarClientException.AlreadyClosedException("Producer already closed"));
            case ProducerFenced:
                return FutureUtil.failedFuture(
                        new PulsarClientException.ProducerFencedException("Producer was fenced"));
            case Terminated:
                return FutureUtil.failedFuture(
                        new PulsarClientException.TopicTerminatedException("Topic was terminated"));
            case Failed:
            case Uninitialized:
                return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }

        if (conf.isLazyStartPartitionedProducers() && !producers.containsKey(partition)) {
            startLazyProducer(partition);
        }

        final ProducerImpl<T> producer = producers.get(partition);
        if (producer == null) {
            return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }
        if (conf.isLazyStartPartitionedProducers()) {
            final CompletableFuture<?> created = creationHandled(producer);
            if (!created.isDone() || lazyStartQueues.containsKey(producer)) {
                if (mustNotWait()) {
                    return sendWhenCreated(producer, created, message, txn);
                }
                // Other threads wait, so that the producer applies its admission limits and flushes as usual
                waitForLazyStart(producer, created).join();
            }
            return sendToCreatedProducer(producer, message, txn);
        }
        return producer.internalSendWithTxnAsync(message, txn);
    }

    // A thread that the creation of a producer, or the sends queued for it, may need must not wait for them. An IO
    // thread may have to read the creation response. The executors and the timer of the client look the topic up,
    // reconnect, and complete the registration of a topic with a transaction, which a queued send waits for. A
    // thread that hands queued sends over would wait for itself.
    private boolean mustNotWait() {
        return handingOverThreads.contains(Thread.currentThread()) || client.isClientThread() || isOnIoThread();
    }

    private boolean isOnIoThread() {
        for (EventExecutor executor : client.eventLoopGroup()) {
            if (executor.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    private void startLazyProducer(int partition) {
        final CompletableFuture<Void> handled = new CompletableFuture<>();
        final ProducerImpl<T> newProducer = producers.computeIfAbsent(partition, idx -> {
            final ProducerImpl<T> started = newPartitionProducer(idx, Optional.ofNullable(overrideProducerName));
            // In the same step, so that no send finds the producer without the stage that it has to wait for
            lazyStarts.put(started, handled);
            return started;
        });
        if (lazyStarts.get(newProducer) != handled) {
            // Another send started the producer
            return;
        }
        newProducer.producerCreatedFuture().whenComplete((prod, createException) -> {
            try {
                if (createException != null) {
                    log.error()
                            .attr("partitionIndex", partition)
                            .exception(createException)
                            .log("Could not create internal producer");
                    producers.remove(partition, newProducer);
                    closeLazyStartedProducer(partition, newProducer);
                } else if (getState() == State.Closing || getState() == State.Closed) {
                    // close() may have iterated the producers before this one was added, and would leave it open
                    producers.remove(partition, newProducer);
                    closeLazyStartedProducer(partition, newProducer);
                } else {
                    log.debug()
                            .attr("partitionIndex", partition)
                            .log("Created internal producer");
                }
            } finally {
                lazyStarts.remove(newProducer);
                handled.complete(null);
            }
        });
    }

    // Completes once the creation of the producer was handled. Every send waits for this stage, or is queued until
    // it completes, and not for the creation itself: a producer whose creation failed is no longer in producers
    // then, so a send that it failed can be retried at once, from any thread, and reaches another producer.
    private CompletableFuture<?> creationHandled(ProducerImpl<T> producer) {
        final CompletableFuture<Void> handled = lazyStarts.get(producer);
        // Without a stage the creation was handled, or the partitioned producer started this one when it was created
        return handled != null ? handled : producer.producerCreatedFuture();
    }

    private CompletableFuture<?> waitForLazyStart(ProducerImpl<T> producer, CompletableFuture<?> created) {
        final LazyStartQueue queue = lazyStartQueues.get(producer);
        final CompletableFuture<?> ready = queue == null ? created : CompletableFuture.allOf(created, queue.drained);
        // A producer closed while it is created never completes its creation
        return CompletableFuture.anyOf(ready, closeStarted).handle((ignored, ignoredException) -> null);
    }

    private CompletableFuture<MessageId> sendWhenCreated(ProducerImpl<T> producer, CompletableFuture<?> created,
                                                         Message<?> message, Transaction txn) {
        // The topic is registered while the transaction is open, as for a send to a created producer. The queued
        // send does not check the transaction again, so a commit that follows it at once still waits for it.
        final CompletableFuture<Void> registered =
                txn == null ? null : ((TransactionImpl) txn).registerProducedTopic(producer.getTopic());
        final ByteBuf payload = ((MessageImpl<?>) message).getDataBuffer();
        final int payloadSize = payload.readableBytes();
        final CompletableFuture<MessageId> sent = new CompletableFuture<>();
        final LazyStartQueue queue;
        final boolean newQueue;
        PulsarClientException rejected = null;
        synchronized (lazyStartQueues) {
            final LazyStartQueue existing = lazyStartQueues.get(producer);
            newQueue = existing == null;
            if (existing == null && created.isDone()) {
                queue = null;
            } else {
                // The producer applies its limits only once it is created, and this thread cannot wait for room.
                // So the send takes its permit and its memory from the producer now, or fails, even with
                // blockIfQueueFull, and brings both along when it is handed over.
                rejected = producer.tryAdmit(message.getSequenceId(), payloadSize);
                queue = existing != null ? existing : new LazyStartQueue();
                if (rejected == null) {
                    queue.sends.add(new QueuedSend(message, payloadSize, registered, sent));
                    if (newQueue) {
                        lazyStartQueues.put(producer, queue);
                    }
                }
            }
        }
        if (rejected != null) {
            payload.release();
            return FutureUtil.failedFuture(rejected);
        }
        if (queue == null) {
            // The creation completed and its queued sends were handed over in the meantime
            return sendToCreatedProducer(producer, message, txn);
        }
        if (newQueue) {
            // A producer closed while it is created never completes its creation
            CompletableFuture.anyOf(created, closeStarted).whenComplete((ignored, e) -> handOver(producer, queue));
        }
        return sent;
    }

    // Hands the queued sends to their producer, in the order they were made. No thread waits here: a send whose
    // topic is still being registered with its transaction is handed over by the thread that completes the
    // registration, or fails on the thread that closes the producer.
    private void handOver(ProducerImpl<T> producer, LazyStartQueue queue) {
        final Thread thread = Thread.currentThread();
        // An interceptor or a callback that sends on this thread meanwhile queues its send, see mustNotWait()
        final boolean entered = handingOverThreads.add(thread);
        try {
            while (true) {
                final QueuedSend send;
                final boolean registering;
                synchronized (lazyStartQueues) {
                    send = queue.sends.peek();
                    registering = send != null && send.registered() != null && !send.registered().isDone()
                            && failureAfterLazyStart(producer) == null;
                    if (send == null) {
                        // A later send finds no queue and goes to the created producer directly
                        lazyStartQueues.remove(producer, queue);
                    } else if (!registering) {
                        queue.sends.poll();
                    }
                }
                if (send == null) {
                    queue.drained.complete(null);
                    return;
                }
                if (registering) {
                    CompletableFuture.anyOf(send.registered(), closeStarted)
                            .whenComplete((ignored, e) -> handOver(producer, queue));
                    return;
                }
                FutureUtil.completeAfter(send.sent(), FutureUtil.supplySafely(() -> sendQueued(producer, send)));
                if (queue.flushRequested) {
                    producer.triggerFlush();
                }
            }
        } finally {
            if (entered) {
                handingOverThreads.remove(thread);
            }
        }
    }

    // The sends that threads which must not wait made to a producer while it is created, in the order they were made
    private static final class LazyStartQueue {
        private final ArrayDeque<QueuedSend> sends = new ArrayDeque<>();
        private final CompletableFuture<Void> drained = new CompletableFuture<>();
        // A flush requested while the queue exists covers its sends: each is flushed once it reaches the producer
        private volatile boolean flushRequested;
    }

    // A queued send, with the size it reserved when it took its permit and its memory from the producer, and the
    // registration of its topic with its transaction if it has one
    private record QueuedSend(Message<?> message, int admittedBytes, CompletableFuture<Void> registered,
                              CompletableFuture<MessageId> sent) {
    }

    private CompletableFuture<MessageId> sendToCreatedProducer(ProducerImpl<T> producer, Message<?> message,
                                                               Transaction txn) {
        final PulsarClientException failure = failureAfterLazyStart(producer);
        return failure != null ? FutureUtil.failedFuture(failure) : producer.internalSendWithTxnAsync(message, txn);
    }

    // Hands a queued send to the producer, with its permit and its memory, or gives them back when it cannot
    private CompletableFuture<MessageId> sendQueued(ProducerImpl<T> producer, QueuedSend send) {
        final PulsarClientException failure = failureAfterLazyStart(producer);
        final CompletableFuture<Void> registered = send.registered();
        if (failure == null && (registered == null || !registered.isCompletedExceptionally())) {
            return producer.internalSendAsync(send.message(), send.admittedBytes());
        }
        producer.releaseAdmission(send.admittedBytes());
        ((MessageImpl<?>) send.message()).getDataBuffer().release();
        // A topic that cannot be registered fails the send as it does for a created producer
        return failure != null ? FutureUtil.failedFuture(failure) : registered.<MessageId>thenApply(ignored -> null);
    }

    private PulsarClientException failureAfterLazyStart(ProducerImpl<T> producer) {
        if (closeStarted.isDone() || getState() == State.Closing || getState() == State.Closed) {
            return new PulsarClientException.AlreadyClosedException("Producer already closed");
        }
        if (producer.producerCreatedFuture().isCompletedExceptionally()) {
            return new PulsarClientException.NotConnectedException();
        }
        return null;
    }

    private void closeLazyStartedProducer(int partition, ProducerImpl<T> producer) {
        producer.closeAsync().exceptionally(e -> {
            log.error()
                    .attr("partitionIndex", partition)
                    .exception(e)
                    .log("Could not close internal producer");
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        final List<LazyStartQueue> queues = new ArrayList<>(lazyStartQueues.values());
        if (queues.isEmpty()) {
            return flushProducersAsync();
        }
        // The queued sends are flushed as they reach their producer. The batches of the sends before them are
        // flushed now.
        queues.forEach(queue -> queue.flushRequested = true);
        return flushProducersAsync()
                .thenCompose(ignored -> CompletableFuture.allOf(
                        queues.stream().map(queue -> queue.drained).toArray(CompletableFuture[]::new)))
                .thenCompose(ignored -> flushProducersAsync());
    }

    private CompletableFuture<Void> flushProducersAsync() {
        return CompletableFuture.allOf(
                producers.values().stream().map(ProducerImpl::flushAsync).toArray(CompletableFuture[]::new));
    }

    @Override
    void triggerFlush() {
        // A queued send is flushed once it reaches its producer
        lazyStartQueues.values().forEach(queue -> queue.flushRequested = true);
        producers.values().forEach(ProducerImpl::triggerFlush);
    }

    @Override
    public boolean isConnected() {
        // returns false if any of the partition is not connected
        return producers.values().stream().allMatch(ProducerImpl::isConnected);
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        long lastDisconnectedTimestamp = 0;
        Optional<ProducerImpl<T>> p = producers.values().stream()
                .max(Comparator.comparingLong(ProducerImpl::getLastDisconnectedTimestamp));
        if (p.isPresent()) {
            lastDisconnectedTimestamp = p.get().getLastDisconnectedTimestamp();
        }
        return lastDisconnectedTimestamp;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return CompletableFuture.completedFuture(null);
        }
        setState(State.Closing);
        closeStarted.complete(null);

        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }

        AtomicReference<Throwable> closeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(producers.size());
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        for (Producer<T> producer : producers.values()) {
            if (producer != null) {
                producer.closeAsync().handle((closed, ex) -> {
                    if (ex != null) {
                        closeFail.compareAndSet(null, ex);
                    }
                    if (completed.decrementAndGet() == 0) {
                        closeInterceptors();
                        if (closeFail.get() == null) {
                            setState(State.Closed);
                            closeFuture.complete(null);
                            log.info("Closed Partitioned Producer");
                            client.cleanupProducer(this);
                        } else {
                            setState(State.Failed);
                            closeFuture.completeExceptionally(closeFail.get());
                            log.error()
                                    .exception(closeFail.get().getCause())
                                    .log("Could not close Partitioned Producer");
                        }
                    }

                    return null;
                });
            }

        }

        return closeFuture;
    }

    @Override
    public synchronized ProducerStatsRecorderImpl getStats() {
        if (stats == null) {
            return null;
        }
        stats.reset();
        producers.forEach(
                (partition, producer) -> stats.updateCumulativeStats(producer.getTopic(), producer.getStats()));
        return stats;
    }

    public List<ProducerImpl<T>> getProducers() {
        return producers.values().stream()
                .sorted(Comparator.comparingInt(e -> TopicName.getPartitionIndex(e.getTopic())))
                .collect(Collectors.toList());
    }

    @Override
    String getHandlerName() {
        return "partition-producer";
    }

    // This listener is triggered when topics partitions are updated.
    private class TopicsPartitionChangedListener implements PartitionsChangedListener {
        // Check partitions changes of passed in topics, and add new topic partitions.
        @Override
        public CompletableFuture<Void> onTopicsExtended(Collection<String> topicsExtended) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (topicsExtended.isEmpty() || !topicsExtended.contains(topic)) {
                future.complete(null);
                return future;
            }

            client.getPartitionsForTopic(topic, false).thenCompose(list -> {
                int oldPartitionNumber = topicMetadata.numPartitions();
                int currentPartitionNumber = list.size();
                    log.debug()
                            .attr("old", oldPartitionNumber)
                            .attr("new", currentPartitionNumber)
                            .log("Topic partitions changed");

                if (oldPartitionNumber == currentPartitionNumber) {
                    // topic partition number not changed
                    future.complete(null);
                    return future;
                } else if (oldPartitionNumber < currentPartitionNumber) {
                    if (conf.isLazyStartPartitionedProducers() && conf.getAccessMode() == ProducerAccessMode.Shared) {
                        topicMetadata = new TopicMetadataImpl(currentPartitionNumber);
                        future.complete(null);
                        // call interceptor with the metadata change
                        onPartitionsChange(topic, currentPartitionNumber);
                        return future;
                    } else {
                        List<CompletableFuture<Producer<T>>> futureList = list
                                .subList(oldPartitionNumber, currentPartitionNumber)
                                .stream()
                                .map(partitionName -> {
                                    int partitionIndex = TopicName.getPartitionIndex(partitionName);
                                    return producers.computeIfAbsent(partitionIndex, (idx) -> new ProducerImpl<>(
                                            client, partitionName, conf, new CompletableFuture<>(),
                                            idx, schema, internalProducerInterceptors,
                                            Optional.ofNullable(overrideProducerName))).producerCreatedFuture();
                                }).collect(Collectors.toList());

                        FutureUtil.waitForAll(futureList)
                                .thenAccept(finalFuture -> {
                                        log.debug()
                                                .attr("old", oldPartitionNumber)
                                                .attr("new", currentPartitionNumber)
                                                .log("success create producers for"
                                                        + " extended partitions");
                                    topicMetadata = new TopicMetadataImpl(currentPartitionNumber);
                                    future.complete(null);
                                })
                                .exceptionally(ex -> {
                                    // error happened, remove
                                    log.warn()
                                            .attr("old", oldPartitionNumber)
                                            .attr("new", currentPartitionNumber)
                                            .log("Failed to create producers for extended partitions");
                                    IntStream.range(oldPartitionNumber, producers.size())
                                            .forEach(i -> producers.remove(i).closeAsync());
                                    future.completeExceptionally(ex);
                                    return null;
                                });
                        // call interceptor with the metadata change
                        onPartitionsChange(topic, currentPartitionNumber);
                        return future;
                    }
                } else {
                    log.error()
                            .attr("old", oldPartitionNumber)
                            .attr("new", currentPartitionNumber)
                            .log("Shrinking topic partitions is not supported");
                    future.completeExceptionally(new NotSupportedException("not support shrink topic partitions"));
                }
                return future;
            }).exceptionally(throwable -> {
                log.error().exception(throwable).log("Auto getting partitions failed");
                future.completeExceptionally(throwable);
                return null;
            });

            return future;
        }
    }

    private ProducerInterceptors createInternalProducerInterceptors(ProducerInterceptors interceptors) {
        return new ProducerInterceptors(Collections.emptyList()) {
            @Override
            public Message beforeSend(Producer producer, Message message) {
                return interceptors.beforeSend(producer, message);
            }

            @Override
            public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId,
                                              Throwable exception) {
                interceptors.onSendAcknowledgement(producer, message, msgId, exception);
            }

            @Override
            public void onPartitionsChange(String topicName, int partitions) {
                // do nothing
            }

            @Override
            public void close() {
                // do nothing
            }
        };
    }

    private TimerTask partitionsAutoUpdateTimerTask = new TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
            try {
                if (timeout.isCancelled() || getState() != State.Ready) {
                    return;
                }
                    log.debug("run partitionsAutoUpdateTimerTask for partitioned producer");

                // if last auto update not completed yet, do nothing.
                if (partitionsAutoUpdateFuture == null || partitionsAutoUpdateFuture.isDone()) {
                    partitionsAutoUpdateFuture =
                            topicsPartitionChangedListener.onTopicsExtended(List.of(topic));
                }
            } catch (Throwable th) {
                log.warn().exception(th)
                        .log("Encountered error in partition auto update"
                                + " timer task for partition producer."
                                + " Another task will be scheduled.");
            } finally {
                // schedule the next re-check task while the producer is alive
                if (!isTerminalState()) {
                    partitionsAutoUpdateTimeout = client.timer()
                            .newTimeout(partitionsAutoUpdateTimerTask,
                                    conf.getAutoUpdatePartitionsIntervalSeconds(), TimeUnit.SECONDS);
                }
            }
        }
    };

    @VisibleForTesting
    public CompletableFuture<Void> getPartitionsAutoUpdateFuture() {
        return partitionsAutoUpdateFuture;
    }

    @VisibleForTesting
    public Timeout getPartitionsAutoUpdateTimeout() {
        return partitionsAutoUpdateTimeout;
    }

    @VisibleForTesting
    public CompletableFuture<Void> getOriginalLastSendFuture() {
        return CompletableFuture.allOf(
                producers.values().stream().map(ProducerImpl::getOriginalLastSendFuture)
                        .toArray(CompletableFuture[]::new));
    }

    @Override
    public int getNumOfPartitions() {
        return topicMetadata.numPartitions();
    }

}
