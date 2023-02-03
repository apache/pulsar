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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStoreException;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStoreFactory;
import org.apache.pulsar.broker.loadbalance.extensions.strategy.BrokerSelectionStrategy;
import org.apache.pulsar.broker.loadbalance.extensions.strategy.LeastResourceUsageWithWeight;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

@Slf4j
public class ExtensibleLoadManagerImpl implements ExtensibleLoadManager {

    public static final String BROKER_LOAD_DATA_STORE_TOPIC = TopicName.get(
            TopicDomain.non_persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE,
            "loadbalancer-broker-load-data").toString();

    public static final String TOP_BUNDLES_LOAD_DATA_STORE_TOPIC = TopicName.get(
            TopicDomain.non_persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE,
            "loadbalancer-top-bundles-load-data").toString();

    private PulsarService pulsar;

    private ServiceConfiguration conf;

    @Getter
    private BrokerRegistry brokerRegistry;

    private ServiceUnitStateChannel serviceUnitStateChannel;

    private LoadDataStore<BrokerLoadData> brokerLoadDataStore;
    private LoadDataStore<TopBundlesLoadData> topBundlesLoadDataStore;

    @Getter
    private LoadManagerContext context;

    @Getter
    private final BrokerSelectionStrategy brokerSelectionStrategy;

    @Getter
    private final List<BrokerFilter> brokerFilterPipeline;

    private boolean started = false;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<BrokerLookupData>>>
            lookupRequests = ConcurrentOpenHashMap.<String,
                    CompletableFuture<Optional<BrokerLookupData>>>newBuilder()
            .build();

    /**
     * Life cycle: Constructor -> initialize -> start -> close.
     */
    public ExtensibleLoadManagerImpl() {
        this.brokerFilterPipeline = new ArrayList<>();
        // TODO: Make brokerSelectionStrategy configurable.
        this.brokerSelectionStrategy = new LeastResourceUsageWithWeight();
    }

    public static boolean isLoadManagerExtensionEnabled(ServiceConfiguration conf) {
        return ExtensibleLoadManagerImpl.class.getName().equals(conf.getLoadManagerClassName());
    }

    @Override
    public void start() throws PulsarServerException {
        if (this.started) {
            return;
        }
        this.brokerRegistry = new BrokerRegistryImpl(pulsar);
        this.serviceUnitStateChannel = new ServiceUnitStateChannelImpl(pulsar);
        this.brokerRegistry.start();
        this.serviceUnitStateChannel.start();

        try {
            this.brokerLoadDataStore = LoadDataStoreFactory
                    .create(pulsar.getClient(), BROKER_LOAD_DATA_STORE_TOPIC, BrokerLoadData.class);
            this.topBundlesLoadDataStore = LoadDataStoreFactory
                    .create(pulsar.getClient(), TOP_BUNDLES_LOAD_DATA_STORE_TOPIC, TopBundlesLoadData.class);
        } catch (LoadDataStoreException e) {
            throw new PulsarServerException(e);
        }

        this.context = LoadManagerContextImpl.builder()
                .configuration(conf)
                .brokerRegistry(brokerRegistry)
                .brokerLoadDataStore(brokerLoadDataStore)
                .topBundleLoadDataStore(topBundlesLoadDataStore).build();
        // TODO: Start load data reporter.

        // TODO: Start unload scheduler and bundle split scheduler
        this.started = true;
    }

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
    }

    @Override
    public CompletableFuture<Optional<BrokerLookupData>> assign(Optional<ServiceUnitId> topic,
                                                                ServiceUnitId serviceUnit) {

        final String bundle = serviceUnit.toString();

        CompletableFuture<Optional<BrokerLookupData>> future = lookupRequests.computeIfAbsent(bundle, k -> {
            final CompletableFuture<Optional<String>> owner;
            // Assign the bundle to channel owner if is internal topic, to avoid circular references.
            if (topic.isPresent() && isInternalTopic(topic.get().toString())) {
                owner = serviceUnitStateChannel.getChannelOwnerAsync();
            } else {
                owner = serviceUnitStateChannel.getOwnerAsync(bundle).thenCompose(broker -> {
                    // If the bundle not assign yet, select and publish assign event to channel.
                    if (broker.isEmpty()) {
                        return this.selectAsync(serviceUnit).thenCompose(brokerOpt -> {
                            if (brokerOpt.isPresent()) {
                                log.info("Selected new owner broker: {} for bundle: {}.", brokerOpt.get(), bundle);
                                return serviceUnitStateChannel.publishAssignEventAsync(bundle, brokerOpt.get())
                                        .thenApply(Optional::of);
                            } else {
                                throw new IllegalStateException(
                                        "Failed to select the new owner broker for bundle: " + bundle);
                            }
                        });
                    }
                    // Already assigned, return it.
                    return CompletableFuture.completedFuture(broker);
                });
            }

            return owner.thenCompose(broker -> {
                if (broker.isEmpty()) {
                    String errorMsg = String.format(
                            "Failed to look up a broker registry:%s for bundle:%s", broker, bundle);
                    log.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
                return CompletableFuture.completedFuture(broker.get());
            }).thenCompose(broker -> this.getBrokerRegistry().lookupAsync(broker).thenCompose(brokerLookupData -> {
                if (brokerLookupData.isEmpty()) {
                    String errorMsg = String.format(
                            "Failed to look up a broker registry:%s for bundle:%s", broker, bundle);
                    log.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
                return CompletableFuture.completedFuture(brokerLookupData);
            }));
        });
        future.whenComplete((r, t) -> lookupRequests.remove(bundle));
        return future;
    }

    private CompletableFuture<Optional<String>> selectAsync(ServiceUnitId bundle) {
        BrokerRegistry brokerRegistry = getBrokerRegistry();
        return brokerRegistry.getAvailableBrokerLookupDataAsync()
                .thenCompose(availableBrokers -> {
                    // TODO: Support isolation policies
                    LoadManagerContext context = this.getContext();

                    Map<String, BrokerLookupData> availableBrokerCandidates = new HashMap<>(availableBrokers);

                    // Filter out brokers that do not meet the rules.
                    List<BrokerFilter> filterPipeline = getBrokerFilterPipeline();
                    for (final BrokerFilter filter : filterPipeline) {
                        try {
                            filter.filter(availableBrokerCandidates, context);
                        } catch (BrokerFilterException e) {
                            // TODO: We may need to revisit this error case.
                            log.error("Failed to filter out brokers.", e);
                            availableBrokerCandidates = availableBrokers;
                        }
                    }
                    if (availableBrokerCandidates.isEmpty()) {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    Set<String> candidateBrokers = availableBrokerCandidates.keySet();

                    return CompletableFuture.completedFuture(
                            getBrokerSelectionStrategy().select(candidateBrokers, bundle, context));
                });
    }

    @Override
    public CompletableFuture<Boolean> checkOwnershipAsync(Optional<ServiceUnitId> topic, ServiceUnitId bundleUnit) {
        final String bundle = bundleUnit.toString();
        CompletableFuture<Optional<String>> owner;
        if (topic.isPresent() && isInternalTopic(topic.get().toString())) {
            owner = serviceUnitStateChannel.getChannelOwnerAsync();
        } else {
            owner = serviceUnitStateChannel.getOwnerAsync(bundle);
        }

        return owner.thenApply(broker -> brokerRegistry.getBrokerId().equals(broker.orElse(null)));
    }

    @Override
    public void close() throws PulsarServerException {
        if (!this.started) {
            return;
        }
        try {
            this.brokerLoadDataStore.close();
            this.topBundlesLoadDataStore.close();
        } catch (IOException ex) {
            throw new PulsarServerException(ex);
        } finally {
            try {
                this.brokerRegistry.close();
            } finally {
                try {
                    this.serviceUnitStateChannel.close();
                } finally {
                    this.started = false;
                }
            }
        }
    }

    private boolean isInternalTopic(String topic) {
        return topic.startsWith(ServiceUnitStateChannelImpl.TOPIC)
                || topic.startsWith(BROKER_LOAD_DATA_STORE_TOPIC)
                || topic.startsWith(TOP_BUNDLES_LOAD_DATA_STORE_TOPIC);
    }
}
