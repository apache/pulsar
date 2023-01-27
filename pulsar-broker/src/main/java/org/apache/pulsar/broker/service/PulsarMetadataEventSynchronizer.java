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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarMetadataEventSynchronizer implements MetadataEventSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(PulsarMetadataEventSynchronizer.class);
    protected PulsarService pulsar;
    protected BrokerService brokerService;
    protected String topicName;
    protected PulsarClientImpl client;
    protected volatile Producer<MetadataEvent> producer;
    protected volatile Consumer<MetadataEvent> consumer;
    private final CopyOnWriteArrayList<Function<MetadataEvent, CompletableFuture<Void>>>
    listeners = new CopyOnWriteArrayList<>();

    private volatile boolean started = false;
    public static final String SUBSCRIPTION_NAME = "metadata-syncer";
    private static final int MAX_PRODUCER_PENDING_SIZE = 1000;
    protected final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0,
            TimeUnit.MILLISECONDS);

    public PulsarMetadataEventSynchronizer(PulsarService pulsar, String topicName) throws PulsarServerException {
        this.pulsar = pulsar;
        this.brokerService = pulsar.getBrokerService();
        this.topicName = topicName;
        if (!StringUtils.isNotBlank(topicName)) {
            log.info("Metadata synchronizer is disabled");
            return;
        }
    }

    public void start() throws PulsarServerException {
        if (StringUtils.isBlank(topicName)) {
            log.info("metadata topic doesn't exist.. skipping metadata synchronizer init..");
            return;
        }
        this.client = (PulsarClientImpl) pulsar.getClient();
        startProducer();
        startConsumer();
        log.info("Metadata event synchronizer started on topic {}", topicName);
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
        if (!started) {
            log.info("Producer is not started on {}, failed to publish {}", topicName, event);
            future.completeExceptionally(new IllegalStateException("producer is not started yet"));
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

    private void startProducer() {
        log.info("[{}] Starting producer", topicName);
        client.newProducer(Schema.AVRO(MetadataEvent.class)).topic(topicName)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).enableBatching(false).enableBatching(false)
                .sendTimeout(0, TimeUnit.SECONDS) //
                .maxPendingMessages(MAX_PRODUCER_PENDING_SIZE).createAsync().thenAccept(prod -> {
                    producer = prod;
                    started = true;
                    log.info("producer is created successfully {}", topicName);
                }).exceptionally(ex -> {
                    long waitTimeMs = backOff.next();
                    log.warn("[{}] Failed to create producer ({}), retrying in {} s", topicName, ex.getMessage(),
                            waitTimeMs / 1000.0);
                    // BackOff before retrying
                    brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
                    return null;
                });
    }

    private void startConsumer() {
        if (consumer != null) {
            return;
        }
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
            log.info("successfully created consumer {}", topicName);
            this.consumer = consumer;
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
        return started;
    }

    @Override
    public void close() {
        started = false;
        if (producer != null) {
            producer.closeAsync();
            producer = null;
        }
        if (consumer != null) {
            consumer.closeAsync();
            consumer = null;
        }
    }
}
