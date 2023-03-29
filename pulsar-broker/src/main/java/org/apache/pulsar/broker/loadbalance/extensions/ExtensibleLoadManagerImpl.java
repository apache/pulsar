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

import static java.lang.String.format;
import static org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl.Role.Follower;
import static org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl.Role.Leader;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Admin;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.AntiAffinityGroupPolicyFilter;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerIsolationPoliciesFilter;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerMaxTopicCountFilter;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerVersionFilter;
import org.apache.pulsar.broker.loadbalance.extensions.manager.SplitManager;
import org.apache.pulsar.broker.loadbalance.extensions.manager.UnloadManager;
import org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.broker.loadbalance.extensions.policies.AntiAffinityGroupPolicyHelper;
import org.apache.pulsar.broker.loadbalance.extensions.reporter.BrokerLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensions.reporter.TopBundleLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.LoadManagerScheduler;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.SplitScheduler;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.UnloadScheduler;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStoreException;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStoreFactory;
import org.apache.pulsar.broker.loadbalance.extensions.strategy.BrokerSelectionStrategy;
import org.apache.pulsar.broker.loadbalance.extensions.strategy.LeastResourceUsageWithWeight;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;

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

    private static final long MAX_ROLE_CHANGE_RETRY_DELAY_IN_MILLIS = 200;

    private PulsarService pulsar;

    private ServiceConfiguration conf;

    @Getter
    private BrokerRegistry brokerRegistry;

    private ServiceUnitStateChannel serviceUnitStateChannel;

    private AntiAffinityGroupPolicyFilter antiAffinityGroupPolicyFilter;

    private AntiAffinityGroupPolicyHelper antiAffinityGroupPolicyHelper;

    private LoadDataStore<BrokerLoadData> brokerLoadDataStore;
    private LoadDataStore<TopBundlesLoadData> topBundlesLoadDataStore;

    private LoadManagerScheduler unloadScheduler;

    @Getter
    private LeaderElectionService leaderElectionService;

    @Getter
    private LoadManagerContext context;

    @Getter
    private final BrokerSelectionStrategy brokerSelectionStrategy;

    @Getter
    private final List<BrokerFilter> brokerFilterPipeline;
    /**
     * The load data reporter.
     */
    private BrokerLoadDataReporter brokerLoadDataReporter;

    private TopBundleLoadDataReporter topBundleLoadDataReporter;

    private ScheduledFuture brokerLoadDataReportTask;
    private ScheduledFuture topBundlesLoadDataReportTask;
    private SplitScheduler splitScheduler;

    private UnloadManager unloadManager;

    private SplitManager splitManager;

    private boolean started = false;

    private final AssignCounter assignCounter = new AssignCounter();
    private final UnloadCounter unloadCounter = new UnloadCounter();
    private final SplitCounter splitCounter = new SplitCounter();

    // record load metrics
    private final AtomicReference<List<Metrics>> brokerLoadMetrics = new AtomicReference<>();
    // record unload metrics
    private final AtomicReference<List<Metrics>> unloadMetrics = new AtomicReference();
    // record split metrics
    private final AtomicReference<List<Metrics>> splitMetrics = new AtomicReference<>();

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<BrokerLookupData>>>
            lookupRequests = ConcurrentOpenHashMap.<String,
                    CompletableFuture<Optional<BrokerLookupData>>>newBuilder()
            .build();
    private final CountDownLatch loadStoreInitWaiter = new CountDownLatch(1);

    public enum Role {
        Leader,
        Follower
    }

    private Role role;

    /**
     * Life cycle: Constructor -> initialize -> start -> close.
     */
    public ExtensibleLoadManagerImpl() {
        this.brokerFilterPipeline = new ArrayList<>();
        this.brokerFilterPipeline.add(new BrokerMaxTopicCountFilter());
        this.brokerFilterPipeline.add(new BrokerIsolationPoliciesFilter());
        this.brokerFilterPipeline.add(new BrokerVersionFilter());
        // TODO: Make brokerSelectionStrategy configurable.
        this.brokerSelectionStrategy = new LeastResourceUsageWithWeight();
    }

    public static boolean isLoadManagerExtensionEnabled(ServiceConfiguration conf) {
        return ExtensibleLoadManagerImpl.class.getName().equals(conf.getLoadManagerClassName());
    }

    public static ExtensibleLoadManagerImpl get(LoadManager loadManager) {
        if (!(loadManager instanceof ExtensibleLoadManagerWrapper loadManagerWrapper)) {
            throw new IllegalArgumentException("The load manager should be 'ExtensibleLoadManagerWrapper'.");
        }
        return loadManagerWrapper.get();
    }

    @Override
    public void start() throws PulsarServerException {
        if (this.started) {
            return;
        }
        this.brokerRegistry = new BrokerRegistryImpl(pulsar);
        this.leaderElectionService = new LeaderElectionService(
                pulsar.getCoordinationService(), pulsar.getSafeWebServiceAddress(),
                state -> {
                    pulsar.getLoadManagerExecutor().execute(() -> {
                        if (state == LeaderElectionState.Leading) {
                            playLeader();
                        } else {
                            playFollower();
                        }
                    });
                });
        this.serviceUnitStateChannel = new ServiceUnitStateChannelImpl(pulsar);
        this.brokerRegistry.start();
        this.splitManager = new SplitManager(splitCounter);
        this.unloadManager = new UnloadManager(unloadCounter);
        this.serviceUnitStateChannel.listen(unloadManager);
        this.serviceUnitStateChannel.listen(splitManager);
        this.leaderElectionService.start();
        this.serviceUnitStateChannel.start();
        this.antiAffinityGroupPolicyHelper =
                new AntiAffinityGroupPolicyHelper(pulsar, serviceUnitStateChannel);
        antiAffinityGroupPolicyHelper.listenFailureDomainUpdate();
        this.antiAffinityGroupPolicyFilter = new AntiAffinityGroupPolicyFilter(antiAffinityGroupPolicyHelper);
        this.brokerFilterPipeline.add(antiAffinityGroupPolicyFilter);

        try {
            this.brokerLoadDataStore = LoadDataStoreFactory
                    .create(pulsar.getClient(), BROKER_LOAD_DATA_STORE_TOPIC, BrokerLoadData.class);
            this.brokerLoadDataStore.startTableView();
            this.topBundlesLoadDataStore = LoadDataStoreFactory
                    .create(pulsar.getClient(), TOP_BUNDLES_LOAD_DATA_STORE_TOPIC, TopBundlesLoadData.class);
            this.loadStoreInitWaiter.countDown();
        } catch (LoadDataStoreException e) {
            throw new PulsarServerException(e);
        }

        this.context = LoadManagerContextImpl.builder()
                .configuration(conf)
                .brokerRegistry(brokerRegistry)
                .brokerLoadDataStore(brokerLoadDataStore)
                .topBundleLoadDataStore(topBundlesLoadDataStore).build();

        this.brokerLoadDataReporter =
                new BrokerLoadDataReporter(pulsar, brokerRegistry.getBrokerId(), brokerLoadDataStore);

        this.topBundleLoadDataReporter =
                new TopBundleLoadDataReporter(pulsar, brokerRegistry.getBrokerId(), topBundlesLoadDataStore);

        var interval = conf.getLoadBalancerReportUpdateMinIntervalMillis();
        this.brokerLoadDataReportTask = this.pulsar.getLoadManagerExecutor()
                .scheduleAtFixedRate(() -> {
                            try {
                                brokerLoadDataReporter.reportAsync(false);
                                // TODO: update broker load metrics using getLocalData
                            } catch (Throwable e) {
                                log.error("Failed to run the broker load manager executor job.", e);
                            }
                        },
                        interval,
                        interval, TimeUnit.MILLISECONDS);

        this.topBundlesLoadDataReportTask = this.pulsar.getLoadManagerExecutor()
                .scheduleAtFixedRate(() -> {
                            try {
                                // TODO: consider excluding the bundles that are in the process of split.
                                topBundleLoadDataReporter.reportAsync(false);
                            } catch (Throwable e) {
                                log.error("Failed to run the top bundles load manager executor job.", e);
                            }
                        },
                        interval,
                        interval, TimeUnit.MILLISECONDS);

        this.unloadScheduler = new UnloadScheduler(
                pulsar, pulsar.getLoadManagerExecutor(), unloadManager,
                context, serviceUnitStateChannel, antiAffinityGroupPolicyHelper, unloadCounter, unloadMetrics);
        this.unloadScheduler.start();
        this.splitScheduler = new SplitScheduler(
                pulsar, serviceUnitStateChannel, splitManager, splitCounter, splitMetrics, context);
        this.splitScheduler.start();
        this.started = true;
    }

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        this.brokerFilterPipeline.forEach(brokerFilter -> brokerFilter.initialize(pulsar));
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
                                assignCounter.incrementSuccess();
                                log.info("Selected new owner broker: {} for bundle: {}.", brokerOpt.get(), bundle);
                                return serviceUnitStateChannel.publishAssignEventAsync(bundle, brokerOpt.get())
                                        .thenApply(Optional::of);
                            } else {
                                assignCounter.incrementEmpty();
                                throw new IllegalStateException(
                                        "Failed to select the new owner broker for bundle: " + bundle);
                            }
                        });
                    }
                    assignCounter.incrementSkip();
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
                    LoadManagerContext context = this.getContext();

                    Map<String, BrokerLookupData> availableBrokerCandidates = new HashMap<>(availableBrokers);

                    // Filter out brokers that do not meet the rules.
                    List<BrokerFilter> filterPipeline = getBrokerFilterPipeline();
                    for (final BrokerFilter filter : filterPipeline) {
                        try {
                            filter.filter(availableBrokerCandidates, bundle, context);
                            // Preserve the filter successes result.
                            availableBrokers.keySet().retainAll(availableBrokerCandidates.keySet());
                        } catch (BrokerFilterException e) {
                            // TODO: We may need to revisit this error case.
                            log.error("Failed to filter out brokers.", e);
                            availableBrokerCandidates = new HashMap<>(availableBrokers);
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
        return getOwnershipAsync(topic, bundleUnit)
                .thenApply(broker -> brokerRegistry.getBrokerId().equals(broker.orElse(null)));
    }

    public CompletableFuture<Optional<String>> getOwnershipAsync(Optional<ServiceUnitId> topic,
                                                                 ServiceUnitId bundleUnit) {
        final String bundle = bundleUnit.toString();
        CompletableFuture<Optional<String>> owner;
        if (topic.isPresent() && isInternalTopic(topic.get().toString())) {
            owner = serviceUnitStateChannel.getChannelOwnerAsync();
        } else {
            owner = serviceUnitStateChannel.getOwnerAsync(bundle);
        }
        return owner;
    }

    public CompletableFuture<Optional<BrokerLookupData>> getOwnershipWithLookupDataAsync(ServiceUnitId bundleUnit) {
        return getOwnershipAsync(Optional.empty(), bundleUnit).thenCompose(broker -> {
            if (broker.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return getBrokerRegistry().lookupAsync(broker.get());
        });
    }

    public CompletableFuture<Void> unloadNamespaceBundleAsync(ServiceUnitId bundle,
                                                              Optional<String> destinationBroker) {
        return getOwnershipAsync(Optional.empty(), bundle)
                .thenCompose(brokerOpt -> {
                    if (brokerOpt.isEmpty()) {
                        String msg = String.format("Namespace bundle: %s is not owned by any broker.", bundle);
                        log.warn(msg);
                        throw new IllegalStateException(msg);
                    }
                    String sourceBroker = brokerOpt.get();
                    if (destinationBroker.isPresent() && sourceBroker.endsWith(destinationBroker.get())) {
                        String msg = String.format("Namespace bundle: %s own by %s, cannot be transfer to same broker.",
                                bundle, sourceBroker);
                        log.warn(msg);
                        throw new IllegalArgumentException(msg);
                    }
                    Unload unload = new Unload(sourceBroker, bundle.toString(), destinationBroker);
                    UnloadDecision unloadDecision =
                            new UnloadDecision(unload, UnloadDecision.Label.Success, UnloadDecision.Reason.Admin);
                    return unloadAsync(unloadDecision,
                            conf.getNamespaceBundleUnloadingTimeoutMs(), TimeUnit.MILLISECONDS);
                });
    }

    private CompletableFuture<Void> unloadAsync(UnloadDecision unloadDecision,
                                               long timeout,
                                               TimeUnit timeoutUnit) {
        Unload unload = unloadDecision.getUnload();
        CompletableFuture<Void> future = serviceUnitStateChannel.publishUnloadEventAsync(unload);
        return unloadManager.waitAsync(future, unload.serviceUnit(), unloadDecision, timeout, timeoutUnit)
                .thenRun(() -> unloadCounter.updateUnloadBrokerCount(1));
    }

    public CompletableFuture<Void> splitNamespaceBundleAsync(ServiceUnitId bundle,
                                                             NamespaceBundleSplitAlgorithm splitAlgorithm,
                                                             List<Long> boundaries) {
        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle.toString());
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle.toString());
        NamespaceBundle namespaceBundle =
                pulsar.getNamespaceService().getNamespaceBundleFactory().getBundle(namespaceName, bundleRange);
        return pulsar.getNamespaceService().getSplitBoundary(namespaceBundle, splitAlgorithm, boundaries)
                .thenCompose(splitBundlesPair -> {
                    if (splitBundlesPair == null) {
                        String msg = format("Bundle %s not found under namespace", namespaceBundle);
                        log.error(msg);
                        return FutureUtil.failedFuture(new IllegalStateException(msg));
                    }

                    return getOwnershipAsync(Optional.empty(), bundle)
                            .thenCompose(brokerOpt -> {
                                if (brokerOpt.isEmpty()) {
                                    String msg = String.format("Namespace bundle: %s is not owned by any broker.",
                                            bundle);
                                    log.warn(msg);
                                    throw new IllegalStateException(msg);
                                }
                                String sourceBroker = brokerOpt.get();
                                SplitDecision splitDecision = new SplitDecision();
                                List<NamespaceBundle> splitBundles = splitBundlesPair.getRight();
                                Map<String, Optional<String>> splitServiceUnitToDestBroker = new HashMap<>();
                                splitBundles.forEach(splitBundle -> splitServiceUnitToDestBroker
                                        .put(splitBundle.getBundleRange(), Optional.empty()));
                                splitDecision.setSplit(
                                        new Split(bundle.toString(), sourceBroker, splitServiceUnitToDestBroker));
                                splitDecision.setLabel(Success);
                                splitDecision.setReason(Admin);
                                return splitAsync(splitDecision,
                                        conf.getNamespaceBundleUnloadingTimeoutMs(), TimeUnit.MILLISECONDS);
                            });
                });
    }

    private CompletableFuture<Void> splitAsync(SplitDecision decision,
                                               long timeout,
                                               TimeUnit timeoutUnit) {
        Split split = decision.getSplit();
        CompletableFuture<Void> future = serviceUnitStateChannel.publishSplitEventAsync(split);
        return splitManager.waitAsync(future, decision.getSplit().serviceUnit(), decision, timeout, timeoutUnit);
    }

    @Override
    public void close() throws PulsarServerException {
        if (!this.started) {
            return;
        }
        try {
            if (brokerLoadDataReportTask != null) {
                brokerLoadDataReportTask.cancel(true);
            }

            if (topBundlesLoadDataReportTask != null) {
                topBundlesLoadDataReportTask.cancel(true);
            }

            this.brokerLoadDataStore.close();
            this.topBundlesLoadDataStore.close();
            this.unloadScheduler.close();
            this.splitScheduler.close();
        } catch (IOException ex) {
            throw new PulsarServerException(ex);
        } finally {
            try {
                this.brokerRegistry.close();
            } finally {
                try {
                    this.serviceUnitStateChannel.close();
                } finally {
                    this.unloadManager.close();
                    try {
                        this.leaderElectionService.close();
                    } catch (Exception e) {
                        throw new PulsarServerException(e);
                    } finally {
                        this.started = false;
                    }
                }

            }
        }
    }

    private boolean isInternalTopic(String topic) {
        return topic.startsWith(ServiceUnitStateChannelImpl.TOPIC)
                || topic.startsWith(BROKER_LOAD_DATA_STORE_TOPIC)
                || topic.startsWith(TOP_BUNDLES_LOAD_DATA_STORE_TOPIC);
    }

    @VisibleForTesting
    void playLeader() {
        if (role != Leader) {
            log.info("This broker:{} is changing the role from {} to {}",
                    pulsar.getLookupServiceAddress(), role, Leader);
            int retry = 0;
            while (true) {
                try {
                    serviceUnitStateChannel.scheduleOwnershipMonitor();
                    loadStoreInitWaiter.await();
                    topBundlesLoadDataStore.startTableView();
                    unloadScheduler.start();
                    break;
                } catch (Throwable e) {
                    log.error("The broker:{} failed to change the role. Retrying {} th ...",
                            pulsar.getLookupServiceAddress(), ++retry, e);
                    try {
                        Thread.sleep(Math.min(retry * 10, MAX_ROLE_CHANGE_RETRY_DELAY_IN_MILLIS));
                    } catch (InterruptedException ex) {
                        log.warn("Interrupted while sleeping.");
                    }
                }
            }
            role = Leader;
            log.info("This broker:{} plays the leader now.", pulsar.getLookupServiceAddress());
        }

        // flush the load data when the leader is elected.
        if (brokerLoadDataReporter != null) {
            brokerLoadDataReporter.reportAsync(true);
        }
        if (topBundleLoadDataReporter != null) {
            topBundleLoadDataReporter.reportAsync(true);
        }
    }

    @VisibleForTesting
    void playFollower() {
        if (role != Follower) {
            log.info("This broker:{} is changing the role from {} to {}",
                    pulsar.getLookupServiceAddress(), role, Follower);
            int retry = 0;
            while (true) {
                try {
                    serviceUnitStateChannel.cancelOwnershipMonitor();
                    loadStoreInitWaiter.await();
                    topBundlesLoadDataStore.closeTableView();
                    unloadScheduler.close();
                    break;
                } catch (Throwable e) {
                    log.error("The broker:{} failed to change the role. Retrying {} th ...",
                            pulsar.getLookupServiceAddress(), ++retry, e);
                    try {
                        Thread.sleep(Math.min(retry * 10, MAX_ROLE_CHANGE_RETRY_DELAY_IN_MILLIS));
                    } catch (InterruptedException ex) {
                        log.warn("Interrupted while sleeping.");
                    }
                }
            }
            role = Follower;
            log.info("This broker:{} plays a follower now.", pulsar.getLookupServiceAddress());
        }

        // flush the load data when the leader is elected.
        if (brokerLoadDataReporter != null) {
            brokerLoadDataReporter.reportAsync(true);
        }
        if (topBundleLoadDataReporter != null) {
            topBundleLoadDataReporter.reportAsync(true);
        }
    }

    void updateBrokerLoadMetrics(BrokerLoadData loadData) {
        this.brokerLoadMetrics.set(loadData.toMetrics(pulsar.getAdvertisedAddress()));
    }

    private void updateUnloadMetrics(UnloadDecision decision) {
        unloadCounter.update(decision);
        this.unloadMetrics.set(unloadCounter.toMetrics(pulsar.getAdvertisedAddress()));
    }

    public List<Metrics> getMetrics() {
        List<Metrics> metricsCollection = new ArrayList<>();

        if (this.brokerLoadMetrics.get() != null) {
            metricsCollection.addAll(this.brokerLoadMetrics.get());
        }
        if (this.unloadMetrics.get() != null) {
            metricsCollection.addAll(this.unloadMetrics.get());
        }
        if (this.splitMetrics.get() != null) {
            metricsCollection.addAll(this.splitMetrics.get());
        }

        metricsCollection.addAll(this.assignCounter.toMetrics(pulsar.getAdvertisedAddress()));

        metricsCollection.addAll(this.serviceUnitStateChannel.getMetrics());

        return metricsCollection;
    }
}
