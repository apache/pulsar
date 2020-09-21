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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotSupportedException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionedProducerImpl<T> extends ProducerBase<T> {

    private static final Logger log = LoggerFactory.getLogger(PartitionedProducerImpl.class);

    private List<ProducerImpl<T>> producers;
    private MessageRouter routerPolicy;
    private final ProducerStatsRecorderImpl stats;
    private TopicMetadata topicMetadata;

    // timeout related to auto check and subscribe partition increasement
    private volatile Timeout partitionsAutoUpdateTimeout = null;
    TopicsPartitionChangedListener topicsPartitionChangedListener;
    CompletableFuture<Void> partitionsAutoUpdateFuture = null;

    public PartitionedProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf, int numPartitions,
            CompletableFuture<Producer<T>> producerCreatedFuture, Schema<T> schema, ProducerInterceptors interceptors) {
        super(client, topic, conf, producerCreatedFuture, schema, interceptors);
        this.producers = Lists.newArrayListWithCapacity(numPartitions);
        this.topicMetadata = new TopicMetadataImpl(numPartitions);
        this.routerPolicy = getMessageRouter();
        stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ProducerStatsRecorderImpl() : null;

        int maxPendingMessages = Math.min(conf.getMaxPendingMessages(),
                conf.getMaxPendingMessagesAcrossPartitions() / numPartitions);
        conf.setMaxPendingMessages(maxPendingMessages);
        start();

        // start track and auto subscribe partition increasement
        if (conf.isAutoUpdatePartitions()) {
            topicsPartitionChangedListener = new TopicsPartitionChangedListener();
            partitionsAutoUpdateTimeout = client.timer()
                .newTimeout(partitionsAutoUpdateTimerTask, conf.getAutoUpdatePartitionsIntervalSeconds(), TimeUnit.SECONDS);
        }
    }

    private MessageRouter getMessageRouter() {
        MessageRouter messageRouter;

        MessageRoutingMode messageRouteMode = conf.getMessageRoutingMode();

        switch (messageRouteMode) {
            case CustomPartition:
                messageRouter = checkNotNull(conf.getCustomMessageRouter());
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
        return producers.get(0).getProducerName();
    }

    @Override
    public long getLastSequenceId() {
        // Return the highest sequence id across all partitions. This will be correct,
        // since there is a single id generator across all partitions for the same producer
        return producers.stream().map(Producer::getLastSequenceId).mapToLong(Long::longValue).max().orElse(-1);
    }

    private void start() {
        AtomicReference<Throwable> createFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger();
        for (int partitionIndex = 0; partitionIndex < topicMetadata.numPartitions(); partitionIndex++) {
            String partitionName = TopicName.get(topic).getPartition(partitionIndex).toString();
            ProducerImpl<T> producer = new ProducerImpl<>(client, partitionName, conf, new CompletableFuture<>(),
                    partitionIndex, schema, interceptors);
            producers.add(producer);
            producer.producerCreatedFuture().handle((prod, createException) -> {
                if (createException != null) {
                    setState(State.Failed);
                    createFail.compareAndSet(null, createException);
                }
                // we mark success if all the partitions are created
                // successfully, else we throw an exception
                // due to any
                // failure in one of the partitions and close the successfully
                // created partitions
                if (completed.incrementAndGet() == topicMetadata.numPartitions()) {
                    if (createFail.get() == null) {
                        setState(State.Ready);
                        log.info("[{}] Created partitioned producer", topic);
                        producerCreatedFuture().complete(PartitionedProducerImpl.this);
                    } else {
                        log.error("[{}] Could not create partitioned producer.", topic, createFail.get().getCause());
                        closeAsync().handle((ok, closeException) -> {
                            producerCreatedFuture().completeExceptionally(createFail.get());
                            client.cleanupProducer(this);
                            return null;
                        });
                    }
                }

                return null;
            });
        }

    }

    @Override
    CompletableFuture<MessageId> internalSendAsync(Message<?> message, Transaction txn) {

        switch (getState()) {
        case Ready:
        case Connecting:
            break; // Ok
        case Closing:
        case Closed:
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Producer already closed"));
        case Terminated:
            return FutureUtil.failedFuture(new PulsarClientException.TopicTerminatedException("Topic was terminated"));
        case Failed:
        case Uninitialized:
            return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }

        int partition = routerPolicy.choosePartition(message, topicMetadata);
        checkArgument(partition >= 0 && partition < topicMetadata.numPartitions(),
                "Illegal partition index chosen by the message routing policy: " + partition);
        return producers.get(partition).internalSendAsync(message, txn);
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        List<CompletableFuture<Void>> flushFutures =
            producers.stream().map(ProducerImpl::flushAsync).collect(Collectors.toList());
        return CompletableFuture.allOf(flushFutures.toArray(new CompletableFuture[flushFutures.size()]));
    }

    @Override
    void triggerFlush() {
        producers.forEach(ProducerImpl::triggerFlush);
    }

    @Override
    public boolean isConnected() {
        // returns false if any of the partition is not connected
        return producers.stream().allMatch(ProducerImpl::isConnected);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return CompletableFuture.completedFuture(null);
        }
        setState(State.Closing);

        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }

        AtomicReference<Throwable> closeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger(topicMetadata.numPartitions());
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        for (Producer<T> producer : producers) {
            if (producer != null) {
                producer.closeAsync().handle((closed, ex) -> {
                    if (ex != null) {
                        closeFail.compareAndSet(null, ex);
                    }
                    if (completed.decrementAndGet() == 0) {
                        if (closeFail.get() == null) {
                            setState(State.Closed);
                            closeFuture.complete(null);
                            log.info("[{}] Closed Partitioned Producer", topic);
                            client.cleanupProducer(this);
                        } else {
                            setState(State.Failed);
                            closeFuture.completeExceptionally(closeFail.get());
                            log.error("[{}] Could not close Partitioned Producer", topic, closeFail.get().getCause());
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
        for (int i = 0; i < topicMetadata.numPartitions(); i++) {
            stats.updateCumulativeStats(producers.get(i).getStats());
        }
        return stats;
    }

    public List<ProducerImpl<T>> getProducers() {
        return producers.stream().collect(Collectors.toList());
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

            client.getPartitionsForTopic(topic).thenCompose(list -> {
                int oldPartitionNumber = topicMetadata.numPartitions();
                int currentPartitionNumber = list.size();

                if (log.isDebugEnabled()) {
                    log.debug("[{}] partitions number. old: {}, new: {}",
                        topic, oldPartitionNumber, currentPartitionNumber);
                }

                if (oldPartitionNumber == currentPartitionNumber) {
                    // topic partition number not changed
                    future.complete(null);
                    return future;
                } else if (oldPartitionNumber < currentPartitionNumber) {
                    List<CompletableFuture<Producer<T>>> futureList = list
                        .subList(oldPartitionNumber, currentPartitionNumber)
                        .stream()
                        .map(partitionName -> {
                            int partitionIndex = TopicName.getPartitionIndex(partitionName);
                            ProducerImpl<T> producer =
                                new ProducerImpl<>(client,
                                    partitionName, conf, new CompletableFuture<>(),
                                    partitionIndex, schema, interceptors);
                            producers.add(producer);
                            return producer.producerCreatedFuture();
                        }).collect(Collectors.toList());

                    FutureUtil.waitForAll(futureList)
                        .thenAccept(finalFuture -> {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] success create producers for extended partitions. old: {}, new: {}",
                                    topic, oldPartitionNumber, currentPartitionNumber);
                            }
                            topicMetadata = new TopicMetadataImpl(currentPartitionNumber);
                            future.complete(null);
                        })
                        .exceptionally(ex -> {
                            // error happened, remove
                            log.warn("[{}] fail create producers for extended partitions. old: {}, new: {}",
                                topic, oldPartitionNumber, currentPartitionNumber);
                            List<ProducerImpl<T>> sublist = producers.subList(oldPartitionNumber, producers.size());
                            sublist.forEach(newProducer -> newProducer.closeAsync());
                            sublist.clear();
                            future.completeExceptionally(ex);
                            return null;
                        });
                    return null;
                } else {
                    log.error("[{}] not support shrink topic partitions. old: {}, new: {}",
                        topic, oldPartitionNumber, currentPartitionNumber);
                    future.completeExceptionally(new NotSupportedException("not support shrink topic partitions"));
                }
                return future;
            });

            return future;
        }
    }

    private TimerTask partitionsAutoUpdateTimerTask = new TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled() || getState() != State.Ready) {
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] run partitionsAutoUpdateTimerTask for partitioned producer", topic);
            }

            // if last auto update not completed yet, do nothing.
            if (partitionsAutoUpdateFuture == null || partitionsAutoUpdateFuture.isDone()) {
                partitionsAutoUpdateFuture = topicsPartitionChangedListener.onTopicsExtended(ImmutableList.of(topic));
            }

            // schedule the next re-check task
            partitionsAutoUpdateTimeout = client.timer()
                .newTimeout(partitionsAutoUpdateTimerTask, 1, TimeUnit.MINUTES);
        }
    };

    @VisibleForTesting
    public Timeout getPartitionsAutoUpdateTimeout() {
        return partitionsAutoUpdateTimeout;
    }

}
