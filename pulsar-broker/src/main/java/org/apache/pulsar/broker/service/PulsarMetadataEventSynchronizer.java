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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarMetadataEventSynchronizer implements MetadataEventSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(PulsarMetadataEventSynchronizer.class);
    protected PulsarService pulsar;
    protected BrokerService brokerService;
    @Getter
    protected String topicName;
    protected volatile PulsarClientImpl client;
    protected volatile Producer<MetadataEvent> producer;
    protected volatile Consumer<MetadataEvent> consumer;
    private final CopyOnWriteArrayList<Function<MetadataEvent, CompletableFuture<Void>>>
    listeners = new CopyOnWriteArrayList<>();

    protected static final AtomicReferenceFieldUpdater<PulsarMetadataEventSynchronizer, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PulsarMetadataEventSynchronizer.class, State.class, "state");
    @Getter
    private volatile State state;
    public static final String SUBSCRIPTION_NAME = "metadata-syncer";
    private static final int MAX_PRODUCER_PENDING_SIZE = 1000;
    protected final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0,
            TimeUnit.MILLISECONDS);
    private volatile CompletableFuture<Void> closeFuture;

    public enum State {
        Init,
        Starting_Producer,
        Starting_Consumer,
        Started,
        Closing,
        Closed;
    }

    public PulsarMetadataEventSynchronizer(PulsarService pulsar, String topicName) {
        this.pulsar = pulsar;
        this.brokerService = pulsar.getBrokerService();
        this.topicName = topicName;
        this.state = State.Init;
        if (!StringUtils.isNotBlank(topicName)) {
            log.info("Metadata synchronizer is disabled");
        }
    }

    public void start() throws PulsarServerException {
        if (StringUtils.isBlank(topicName)) {
            log.info("metadata topic doesn't exist.. skipping metadata synchronizer init..");
            return;
        }
        log.info("Metadata event synchronizer is starting on topic {}", topicName);
        this.client = (PulsarClientImpl) pulsar.getClient();
        if (STATE_UPDATER.compareAndSet(this, State.Init, State.Starting_Producer)) {
            startProducer();
        }
    }

    @Override
    public CompletableFuture<Void> notify(MetadataEvent event) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        publishAsync(event, future);
        return future;
    }

    @Override
    public void registerSyncListener(Function<MetadataEvent, CompletableFuture<Void>> listener) {
        listeners.add(listener);
    }

    @Override
    public String getClusterName() {
        return pulsar.getConfig().getClusterName();
    }

    private void publishAsync(MetadataEvent event, CompletableFuture<Void> future) {
        if (!isProducerStarted()) {
            log.info("Producer is not started on {}, failed to publish {}", topicName, event);
            future.completeExceptionally(new IllegalStateException("producer is not started yet"));
            return;
        }
        producer.newMessage().value(event).sendAsync().thenAccept(__ -> {
            log.info("successfully published metadata change event {}", event);
            future.complete(null);
        }).exceptionally(ex -> {
            log.warn("failed to publish metadata update {}, will retry in {}", topicName, MESSAGE_RATE_BACKOFF_MS, ex);
            pulsar.getBrokerService().executor().schedule(() -> publishAsync(event, future), MESSAGE_RATE_BACKOFF_MS,
                    TimeUnit.MILLISECONDS);
            return null;
        });
    }

    protected void startProducer() {
        if (isClosingOrClosed()) {
            log.info("[{}] Skip to start new producer because the synchronizer is closed", topicName);
            return;
        }
        if (producer != null) {
            log.error("[{}] Failed to start the producer because the producer has been set, state: {}",
                    topicName, state);
            return;
        }
        log.info("[{}] Starting producer", topicName);
        client.newProducer(Schema.AVRO(MetadataEvent.class)).topic(topicName)
            .messageRoutingMode(MessageRoutingMode.SinglePartition).enableBatching(false).enableBatching(false)
            .sendTimeout(0, TimeUnit.SECONDS) //
            .maxPendingMessages(MAX_PRODUCER_PENDING_SIZE).createAsync().thenAccept(prod -> {
                backOff.reset();
                if (STATE_UPDATER.compareAndSet(this, State.Starting_Producer, State.Starting_Consumer)) {
                    producer = prod;
                    log.info("producer is created successfully {}", topicName);
                    PulsarMetadataEventSynchronizer.this.startConsumer();
                } else {
                    State stateTransient = state;
                    log.info("[{}] Closing the new producer because the synchronizer state is {}", prod,
                            stateTransient);
                    CompletableFuture closeProducer = new CompletableFuture<>();
                    closeResource(() -> prod.closeAsync(), closeProducer);
                    closeProducer.thenRun(() -> {
                        log.info("[{}] Closed the new producer because the synchronizer state is {}", prod,
                                stateTransient);
                    });
                }
            }).exceptionally(ex -> {
                long waitTimeMs = backOff.next();
                log.warn("[{}] Failed to create producer ({}), retrying in {} s", topicName, ex.getMessage(),
                        waitTimeMs / 1000.0);
                // BackOff before retrying
                pulsar.getExecutor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
                return null;
            });
    }

    @VisibleForTesting
    public Producer<MetadataEvent> getProducer() {
        return producer;
    }

    private void startConsumer() {
        if (isClosingOrClosed()) {
            log.info("[{}] Skip to start new consumer because the synchronizer is closed", topicName);
        }
        if (consumer != null) {
            log.error("[{}] Failed to start the consumer because the consumer has been set, state: {}",
                    topicName, state);
            return;
        }
        log.info("[{}] Starting consumer", topicName);
        ConsumerBuilder<MetadataEvent> consumerBuilder = client.newConsumer(Schema.AVRO(MetadataEvent.class))
            .topic(topicName).subscriptionName(SUBSCRIPTION_NAME).ackTimeout(60, TimeUnit.SECONDS)
            .subscriptionType(SubscriptionType.Failover).messageListener((c, msg) -> {
                log.info("Processing metadata event for {} with listeners {}", msg.getValue().getPath(),
                        listeners.size());
                try {
                    if (listeners.size() == 0) {
                        c.acknowledgeAsync(msg);
                        return;

                    }
                    if (listeners.size() == 1) {
                        listeners.get(0).apply(msg.getValue()).thenApply(__ -> c.acknowledgeAsync(msg))
                                .exceptionally(ex -> {
                                    log.warn("Failed to synchronize {} for {}", msg.getMessageId(), topicName,
                                            ex.getCause());
                                    return null;
                                });
                    } else {
                        FutureUtil
                                .waitForAll(listeners.stream().map(listener -> listener.apply(msg.getValue()))
                                        .collect(Collectors.toList()))
                                .thenApply(__ -> c.acknowledgeAsync(msg)).exceptionally(ex -> {
                                    log.warn("Failed to synchronize {} for {}", msg.getMessageId(), topicName);
                                    return null;
                                });
                    }
                } catch (Exception e) {
                    log.warn("Failed to synchronize {} for {}", msg.getMessageId(), topicName);
                }
            });
        consumerBuilder.subscribeAsync().thenAccept(consumer -> {
            backOff.reset();
            if (STATE_UPDATER.compareAndSet(this, State.Starting_Consumer, State.Started)) {
                this.consumer = consumer;
                log.info("successfully created consumer {}", topicName);
            } else {
                State stateTransient = state;
                log.info("[{}] Closing the new consumer because the synchronizer state is {}", topicName,
                        stateTransient);
                CompletableFuture closeConsumer = new CompletableFuture<>();
                closeResource(() -> consumer.closeAsync(), closeConsumer);
                closeConsumer.thenRun(() -> {
                    log.info("[{}] Closed the new consumer because the synchronizer state is {}", topicName,
                            stateTransient);
                });
            }
        }).exceptionally(ex -> {
            long waitTimeMs = backOff.next();
            log.warn("[{}] Failed to create consumer ({}), retrying in {} s", topicName, ex.getMessage(),
                    waitTimeMs / 1000.0);
            // BackOff before retrying
            brokerService.executor().schedule(this::startConsumer, waitTimeMs, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    public boolean isStarted() {
        return this.state == State.Started;
    }

    public boolean isProducerStarted() {
        return this.state.ordinal() > State.Starting_Producer.ordinal()
                && this.state.ordinal() < State.Closing.ordinal();
    }

    public boolean isClosingOrClosed() {
        return this.state == State.Closing || this.state == State.Closed;
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        int tryChangeStateCounter = 0;
        while (true) {
            if (isClosingOrClosed()) {
                return closeFuture;
            }
            if (STATE_UPDATER.compareAndSet(this, State.Init, State.Closing)
                || STATE_UPDATER.compareAndSet(this, State.Starting_Producer, State.Closing)
                || STATE_UPDATER.compareAndSet(this, State.Starting_Consumer, State.Closing)
                || STATE_UPDATER.compareAndSet(this, State.Started, State.Closing)) {
                break;
            }
            // Just for avoid spinning loop which would cause 100% CPU consumption here.
            if (++tryChangeStateCounter > 100) {
                log.error("Unexpected error: the state can not be changed to closing {}, state: {}", topicName, state);
                return CompletableFuture.failedFuture(new RuntimeException("Unexpected error,"
                        + " the state can not be changed to closing"));
            }
        }
        CompletableFuture<Void> closeProducer = new CompletableFuture<>();
        CompletableFuture<Void> closeConsumer = new CompletableFuture<>();
        if (producer == null) {
            closeProducer.complete(null);
        } else {
            closeResource(() -> producer.closeAsync(), closeProducer);
        }
        if (consumer == null) {
            closeConsumer.complete(null);
        } else {
            closeResource(() -> consumer.closeAsync(), closeConsumer);
        }

        // Add logs.
        closeProducer.thenRun(() -> log.info("Successfully close producer {}", topicName));
        closeConsumer.thenRun(() -> log.info("Successfully close consumer {}", topicName));

        closeFuture = FutureUtil.waitForAll(Arrays.asList(closeProducer, closeConsumer));
        closeFuture.thenRun(() -> {
            this.state = State.Closed;
            log.info("Successfully close metadata store synchronizer {}", topicName);
        });
        return closeFuture;
    }

    private void closeResource(final Supplier<CompletableFuture<Void>> asyncCloseable,
                               final CompletableFuture<Void> future) {
        if (asyncCloseable == null) {
            future.complete(null);
            return;
        }
        asyncCloseable.get().whenComplete((ignore, ex) -> {
            if (ex == null) {
                backOff.reset();
                future.complete(null);
                return;
            }
            // Retry.
            long waitTimeMs = backOff.next();
            log.warn("[{}] Exception: '{}' occurred while trying to close the {}. Retrying again in {} s.",
                    topicName, ex.getMessage(), asyncCloseable.getClass().getSimpleName(), waitTimeMs / 1000.0, ex);
            brokerService.executor().schedule(() -> closeResource(asyncCloseable, future), waitTimeMs,
                    TimeUnit.MILLISECONDS);
        });
    }
}
