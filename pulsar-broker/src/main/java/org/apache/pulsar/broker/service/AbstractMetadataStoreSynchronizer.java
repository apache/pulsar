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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.BaseResources;
import org.apache.pulsar.broker.service.MetadataChangeEvent.EventType;
import org.apache.pulsar.broker.service.MetadataChangeEvent.ResourceType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.metadata.api.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMetadataStoreSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(AbstractMetadataStoreSynchronizer.class);

    protected PulsarService pulsar;
    protected BrokerService brokerService;
    protected String topicName;
    protected PulsarClientImpl client;
    protected volatile Producer<MetadataChangeEvent> producer;
    protected ProducerBuilder<MetadataChangeEvent> producerBuilder;
    private volatile ScheduledFuture<?> snapshotScheduler;

    protected static final AtomicReferenceFieldUpdater<AbstractMetadataStoreSynchronizer, State> STATE_UPDATER =
    AtomicReferenceFieldUpdater.newUpdater(AbstractMetadataStoreSynchronizer.class, State.class, "state");
    private volatile State state = State.Stopped;
    private volatile boolean isActive = false;
    public static final String SUBSCRIPTION_NAME = "metadata-syncer";
    protected final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0,
            TimeUnit.MILLISECONDS);

    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    public AbstractMetadataStoreSynchronizer(PulsarService pulsar) throws PulsarServerException {
        this.pulsar = pulsar;
        this.brokerService = pulsar.getBrokerService();
        this.topicName = pulsar.getConfig().getMetadataSyncEventTopic();
        if (!StringUtils.isNotBlank(topicName)) {
            log.info("Metadata synchronizer is disabled");
            return;
        }
        this.client = (PulsarClientImpl) pulsar.getClient();
        @SuppressWarnings("serial")
        ConsumerEventListener listener = new ConsumerEventListener() {
            @Override
            public void becameActive(Consumer<?> consumer, int partitionId) {
                startProducer();
                isActive = true;
            }

            @Override
            public void becameInactive(Consumer<?> consumer, int partitionId) {
                isActive = false;
                closeProducerAsync();
            }
        };
        ConsumerBuilder<MetadataChangeEvent> consumerBuilder = client
                .newConsumer(AvroSchema.of(MetadataChangeEvent.class)).topic(topicName)
                .subscriptionName(SUBSCRIPTION_NAME).ackTimeout(60, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Failover).messageListener((c, msg) -> {
                    updateMetadata(c, msg);
                }).consumerEventListener(listener);

        this.producerBuilder = client.newProducer(AvroSchema.of(MetadataChangeEvent.class)) //
                .topic(topicName).messageRoutingMode(MessageRoutingMode.SinglePartition).enableBatching(false)
                .sendTimeout(0, TimeUnit.SECONDS);

        registerListeners();
        startProducer();
        startConsumer(consumerBuilder);
    }

    private void updateMetadata(Consumer<MetadataChangeEvent> c, Message<MetadataChangeEvent> msg) {
        if (msg.getValue().getResource() == null) {
            log.info("Metadata change event has null resource {}.. skipping event", msg.getMessageId());
            c.acknowledgeAsync(msg);
            return;
        }

        MetadataChangeEvent event = msg.getValue();
        if (pulsar.getConfig().getClusterName().equals(event.getSourceCluster())) {
            if (log.isDebugEnabled()) {
                log.debug("ignoring event because source cluster is local cluster {}",
                        pulsar.getConfig().getClusterName());
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("received metadata sync event {}", event);
        }
        CompletableFuture<Void> result;
        switch (event.getResource()) {
        case Tenants:
            result = updateTenantMetadata(event);
            break;
        case Namespaces:
            result = updateNamespaceMetadata(event);
            break;
        case TopicPartition:
            result = updatePartitionMetadata(event);
            break;
        default:
            result = CompletableFuture.completedFuture(null);
            log.warn("Unknown metadata event resource {}, msgId={}", event.getResource(), msg.getMessageId());
        }
        result.thenAccept(__ -> c.acknowledgeAsync(msg)).exceptionally(ex -> {
            log.warn("Failed to process {} on {}", msg.getMessageId(), topicName, ex.getCause());
            return null;
        });
    }

    /**
     * Handle metadata change event and update namespace metadata into metadata store.
     * @param event
     * @return
     */
    public abstract CompletableFuture<Void> updateNamespaceMetadata(MetadataChangeEvent event);

    /**
     * Handle metadata change event and update tenant metadata into metadata store.
     * @param event
     * @return
     */
    public abstract CompletableFuture<Void> updateTenantMetadata(MetadataChangeEvent event);

    /**
     * Handle metadata change event and update partitioned metadata into metadata store.
     * @param event
     * @return
     */
    public abstract CompletableFuture<Void> updatePartitionMetadata(MetadataChangeEvent event);

    /**
     * Trigger snapshot synchronizer to publish metadata event for every metadata resources.
     */
    public abstract void triggerSyncSnapshot();

    protected void publishAsync(String metadataStorePath, ResourceType resourceType, String resource, EventType type) {
        if (STATE_UPDATER.get(this) == State.Stopping || STATE_UPDATER.get(this) == State.Stopped) {
            log.warn("Producer is already closed. ignoring event publish for {}", metadataStorePath);
            return;
        }
        log.info("publishing metadata change event {}-{}-{} from {}", resource, resourceType, type, metadataStorePath);
        pulsar.getConfigurationMetadataStore().get(metadataStorePath).thenAccept(result -> {
            if (result.isPresent()) {
                byte[] data = result.get().getValue();
                publishAsync(data, resourceType, resource, type);
            }
        });
    }

    private void publishAsync(byte[] data, ResourceType resourceType, String resource, EventType type) {
        if (STATE_UPDATER.get(this) == State.Stopping || STATE_UPDATER.get(this) == State.Stopped) {
            log.warn("Producer is already closed. ignoring event publish for {}-{}", resource, resourceType);
            return;
        }
        MetadataChangeEvent event = new MetadataChangeEvent(type, resourceType, resource, data,
                pulsar.getConfig().getClusterName(), Instant.now().toEpochMilli());
        producer.newMessage().value(event).sendAsync()
                .thenAccept(__ -> log.info("successfully published metadata change event {}", event))
                .exceptionally(ex -> {
                    log.warn("failed to publish metadata update {}, will retry in {}", topicName,
                            MESSAGE_RATE_BACKOFF_MS, ex);
                    pulsar.getBrokerService().executor().schedule(
                            () -> publishAsync(data, resourceType, resource, type), MESSAGE_RATE_BACKOFF_MS,
                            TimeUnit.MILLISECONDS);
                    return null;
                });

    }

    private void registerListeners() {
        pulsar.getPulsarResources().getNamespaceResources().registerListener(n -> {
            String path = n.getPath();
            if (!isActive || NotificationType.ChildrenChanged.equals(n.getType())
                    || !path.startsWith(BaseResources.BASE_POLICIES_PATH)) {
                log.warn("synchronizer is not active = {} or invalid resource path ={}", isActive, path);
                return;
            }
            ResourceType resourceType = (n.getPath().split("/").length == 3) ? ResourceType.Tenants
                    : ResourceType.Namespaces;
            String resource = (n.getPath().split(BaseResources.BASE_POLICIES_PATH + "/")[1]);
            EventType eventType;
            switch (n.getType()) {
            case Created:
                eventType = EventType.Created;
                break;
            case Modified:
                eventType = EventType.Modified;
                break;
            case Deleted:
                eventType = EventType.Deleted;
                break;
            default:
                return;
            }
            publishAsync(path, resourceType, resource, eventType);
        });
    }

    public boolean isActive() {
        return isActive;
    }

    // This method needs to be synchronized with disconnects else if there is a disconnect followed by startProducer
    // the end result can be disconnect.
    private synchronized void startProducer() {
        if (STATE_UPDATER.get(this) == State.Stopping) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug("Waiting for change-event producer to close before attempting to reconnect, retrying in {} s",
                        waitTimeMs / 1000.0);
            }
            // BackOff before retrying
            brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
            return;
        }
        State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            if (state == State.Started) {
                // Already running
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Change-event producer already running");
                }
            } else {
                log.info("[{}][{} -> {}] Change-event producer already being started. state: {}", state);
            }

            return;
        }

        log.info("[{}] Starting producer", topicName);
        producerBuilder.createAsync().thenAccept(prod -> {
            this.producer = prod;
            startSnapshotScheduler();
            log.info("producer is created successfully {}", topicName);
        }).exceptionally(ex -> {
            if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
                long waitTimeMs = backOff.next();
                log.warn("[{}] Failed to create remote producer ({}), retrying in {} s", topicName, ex.getMessage(),
                        waitTimeMs / 1000.0);

                // BackOff before retrying
                brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
            } else {
                log.warn("[{}] Failed to create remote producer. Replicator state: {}", topicName,
                        STATE_UPDATER.get(this), ex);
            }
            return null;
        });
    }

    private void startSnapshotScheduler() {
        long interval = pulsar.getConfig().getMetadataSyncSnapshotDurationSecond();
        if (interval > 0) {
            this.snapshotScheduler = pulsar.getBrokerService().executor()
                    .scheduleAtFixedRate(() -> triggerSyncSnapshot(), interval, interval, TimeUnit.SECONDS);
        }
    }

    private void stopSnapshotScheduler() {
        if (this.snapshotScheduler != null) {
            this.snapshotScheduler.cancel(true);
            this.snapshotScheduler = null;
        }
    }

    private void startConsumer(ConsumerBuilder<MetadataChangeEvent> consumerBuilder) {
        consumerBuilder.subscribeAsync().thenAccept(consumer -> {
            log.info("successfully created consumer {}", topicName);
        }).exceptionally(ex -> {
            log.warn("failed to start consumer for {}. {}", topicName, ex.getMessage());
            startConsumer(consumerBuilder);
            return null;
        });
    }

    private synchronized CompletableFuture<Void> closeProducerAsync() {
        if (producer == null) {
            STATE_UPDATER.set(this, State.Stopped);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = producer.closeAsync();
        future.thenRun(() -> {
            STATE_UPDATER.set(this, State.Stopped);
            stopSnapshotScheduler();
            this.producer = null;
        }).exceptionally(ex -> {
            long waitTimeMs = backOff.next();
            log.warn("[{}] Exception: '{}' occurred while trying to close the producer." + " retrying again in {} s",
                    topicName, ex.getMessage(), waitTimeMs / 1000.0);
            // BackOff before retrying
            brokerService.executor().schedule(this::closeProducerAsync, waitTimeMs, TimeUnit.MILLISECONDS);
            return null;
        });
        return future;
    }
}
