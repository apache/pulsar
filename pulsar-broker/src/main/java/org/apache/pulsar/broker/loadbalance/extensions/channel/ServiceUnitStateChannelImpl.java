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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Deleted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Init;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Releasing;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.isActiveState;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.isInFlightState;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.Closed;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.Constructed;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.LeaderElectionServiceStarted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.Started;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Assign;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Split;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Unload;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MetadataState.Jittery;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MetadataState.Stable;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MetadataState.Unstable;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData.state;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.topics.TopicCompactionStrategy.TABLE_VIEW_TAG;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionReestablished;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.pulsar.PulsarClusterMetadataSetup;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.manager.StateChangeListener;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@Slf4j
public class ServiceUnitStateChannelImpl implements ServiceUnitStateChannel {
    public static final String TOPIC = TopicName.get(
            TopicDomain.persistent.value(),
            SYSTEM_NAMESPACE,
            "loadbalancer-service-unit-state").toString();

    public static final CompressionType MSG_COMPRESSION_TYPE = CompressionType.ZSTD;
    private static final int OWNERSHIP_CLEAN_UP_MAX_WAIT_TIME_IN_MILLIS = 5000;
    private static final int OWNERSHIP_CLEAN_UP_WAIT_RETRY_DELAY_IN_MILLIS = 100;
    private static final int OWNERSHIP_CLEAN_UP_CONVERGENCE_DELAY_IN_MILLIS = 3000;
    public static final long VERSION_ID_INIT = 1; // initial versionId
    public static final long MAX_CLEAN_UP_DELAY_TIME_IN_SECS = 3 * 60; // 3 mins
    private static final long MIN_CLEAN_UP_DELAY_TIME_IN_SECS = 0; // 0 secs to clean immediately
    private static final long MAX_CHANNEL_OWNER_ELECTION_WAITING_TIME_IN_SECS = 10;
    private static final int MAX_OUTSTANDING_PUB_MESSAGES = 500;
    private static final long MAX_OWNED_BUNDLE_COUNT_DELAY_TIME_IN_MILLIS = 10 * 60 * 1000;
    private final PulsarService pulsar;
    private final ServiceConfiguration config;
    private final Schema<ServiceUnitStateData> schema;
    private final Map<String, CompletableFuture<String>> getOwnerRequests;
    private final String brokerId;
    private final Map<String, CompletableFuture<Void>> cleanupJobs;
    private final StateChangeListeners stateChangeListeners;
    private ExtensibleLoadManagerImpl loadManager;
    private BrokerRegistry brokerRegistry;
    private LeaderElectionService leaderElectionService;
    private TableView<ServiceUnitStateData> tableview;
    private Producer<ServiceUnitStateData> producer;
    private ScheduledFuture<?> monitorTask;
    private SessionEvent lastMetadataSessionEvent = SessionReestablished;
    private long lastMetadataSessionEventTimestamp = 0;
    private long inFlightStateWaitingTimeInMillis;

    private long ownershipMonitorDelayTimeInSecs;
    private long stateTombstoneDelayTimeInMillis;
    private long maxCleanupDelayTimeInSecs;
    private long minCleanupDelayTimeInSecs;
    // cleanup metrics
    private long totalInactiveBrokerCleanupCnt = 0;
    private long totalServiceUnitTombstoneCleanupCnt = 0;

    private long totalOrphanServiceUnitCleanupCnt = 0;
    private AtomicLong totalCleanupErrorCnt = new AtomicLong();
    private long totalInactiveBrokerCleanupScheduledCnt = 0;
    private long totalInactiveBrokerCleanupIgnoredCnt = 0;
    private long totalInactiveBrokerCleanupCancelledCnt = 0;
    private volatile ChannelState channelState;
    private volatile long lastOwnEventHandledAt = 0;
    private long lastOwnedServiceUnitCountAt = 0;
    private int totalOwnedServiceUnitCnt = 0;

    public enum EventType {
        Assign,
        Split,
        Unload,
        Override
    }

    @Getter
    @AllArgsConstructor
    public static class Counters {
        private final AtomicLong total;
        private final AtomicLong failure;
        public Counters(){
            total = new AtomicLong();
            failure = new AtomicLong();
        }
    }

    // operation metrics
    final Map<ServiceUnitState, Counters> ownerLookUpCounters;
    final Map<EventType, Counters> eventCounters;
    final Map<ServiceUnitState, Counters> handlerCounters;

    enum ChannelState {
        Closed(0),
        Constructed(1),
        LeaderElectionServiceStarted(2),
        Started(3);

        ChannelState(int id) {
            this.id = id;
        }
        int id;
    }

    enum MetadataState {
        Stable,
        Jittery,
        Unstable
    }

    @VisibleForTesting
    public ServiceUnitStateChannelImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.config = pulsar.getConfig();
        this.brokerId = pulsar.getBrokerId();
        this.schema = Schema.JSON(ServiceUnitStateData.class);
        this.getOwnerRequests = new ConcurrentHashMap<>();
        this.cleanupJobs = new ConcurrentHashMap<>();
        this.stateChangeListeners = new StateChangeListeners();
        this.stateTombstoneDelayTimeInMillis = config.getLoadBalancerServiceUnitStateTombstoneDelayTimeInSeconds()
                * 1000;
        this.inFlightStateWaitingTimeInMillis = config.getLoadBalancerInFlightServiceUnitStateWaitingTimeInMillis();
        this.ownershipMonitorDelayTimeInSecs = config.getLoadBalancerServiceUnitStateMonitorIntervalInSeconds();
        if (stateTombstoneDelayTimeInMillis < inFlightStateWaitingTimeInMillis) {
            throw new IllegalArgumentException(
                    "Invalid Config: loadBalancerServiceUnitStateTombstoneDelayTimeInSeconds"
                            + stateTombstoneDelayTimeInMillis / 1000 + " secs"
                            + "< loadBalancerInFlightServiceUnitStateWaitingTimeInMillis"
                            + inFlightStateWaitingTimeInMillis + " millis");
        }
        this.maxCleanupDelayTimeInSecs = MAX_CLEAN_UP_DELAY_TIME_IN_SECS;
        this.minCleanupDelayTimeInSecs = MIN_CLEAN_UP_DELAY_TIME_IN_SECS;

        Map<ServiceUnitState, Counters> tmpOwnerLookUpCounters = new HashMap<>();
        Map<ServiceUnitState, Counters> tmpHandlerCounters = new HashMap<>();
        Map<EventType, Counters> tmpEventCounters = new HashMap<>();
        for (var state : ServiceUnitState.values()) {
            tmpOwnerLookUpCounters.put(state, new Counters());
            tmpHandlerCounters.put(state, new Counters());
        }
        for (var event : EventType.values()) {
            tmpEventCounters.put(event, new Counters());
        }
        ownerLookUpCounters = Map.copyOf(tmpOwnerLookUpCounters);
        handlerCounters = Map.copyOf(tmpHandlerCounters);
        eventCounters = Map.copyOf(tmpEventCounters);
        this.channelState = Constructed;
    }

    public void scheduleOwnershipMonitor() {
        if (monitorTask == null) {
            this.monitorTask = this.pulsar.getLoadManagerExecutor()
                    .scheduleWithFixedDelay(() -> {
                                try {
                                    monitorOwnerships(brokerRegistry.getAvailableBrokersAsync()
                                            .get(inFlightStateWaitingTimeInMillis, MILLISECONDS));
                                } catch (Exception e) {
                                    log.info("Failed to monitor the ownerships. will retry..", e);
                                }
                            },
                            0, ownershipMonitorDelayTimeInSecs, SECONDS);
            log.info("This leader broker:{} started the ownership monitor.",
                    brokerId);
        }
    }

    public void cancelOwnershipMonitor() {
        if (monitorTask != null) {
            monitorTask.cancel(false);
            monitorTask = null;
            log.info("This previous leader broker:{} stopped the ownership monitor.",
                    brokerId);
        }
    }

    @Override
    public void cleanOwnerships() {
        doCleanup(brokerId);
    }

    public synchronized void start() throws PulsarServerException {
        if (!validateChannelState(LeaderElectionServiceStarted, false)) {
            throw new IllegalStateException("Invalid channel state:" + channelState.name());
        }

        boolean debug = debug();
        try {
            this.brokerRegistry = getBrokerRegistry();
            this.brokerRegistry.addListener(this::handleBrokerRegistrationEvent);
            this.leaderElectionService = getLeaderElectionService();
            var leader = leaderElectionService.readCurrentLeader().get(
                    MAX_CHANNEL_OWNER_ELECTION_WAITING_TIME_IN_SECS, TimeUnit.SECONDS);
            if (leader.isPresent()) {
                log.info("Successfully found the channel leader:{}.", leader.get());
            } else {
                log.warn("Failed to find the channel leader.");
            }
            this.channelState = LeaderElectionServiceStarted;
            loadManager = getLoadManager();

            if (producer != null) {
                producer.close();
                if (debug) {
                    log.info("Closed the channel producer.");
                }
            }

            PulsarClusterMetadataSetup.createTenantIfAbsent
                    (pulsar.getPulsarResources(), SYSTEM_NAMESPACE.getTenant(), config.getClusterName());

            PulsarClusterMetadataSetup.createNamespaceIfAbsent
                    (pulsar.getPulsarResources(), SYSTEM_NAMESPACE, config.getClusterName());

            ExtensibleLoadManagerImpl.createSystemTopic(pulsar, TOPIC);

            producer = pulsar.getClient().newProducer(schema)
                    .enableBatching(true)
                    .compressionType(MSG_COMPRESSION_TYPE)
                    .maxPendingMessages(MAX_OUTSTANDING_PUB_MESSAGES)
                    .blockIfQueueFull(true)
                    .topic(TOPIC)
                    .create();

            if (debug) {
                log.info("Successfully started the channel producer.");
            }

            if (tableview != null) {
                tableview.close();
                if (debug) {
                    log.info("Closed the channel tableview.");
                }
            }
            tableview = pulsar.getClient().newTableViewBuilder(schema)
                    .topic(TOPIC)
                    .loadConf(Map.of(
                            "topicCompactionStrategyClassName",
                            ServiceUnitStateCompactionStrategy.class.getName()))
                    .create();
            tableview.listen((key, value) -> handle(key, value));
            var strategy = (ServiceUnitStateCompactionStrategy) TopicCompactionStrategy.getInstance(TABLE_VIEW_TAG);
            if (strategy == null) {
                String err = TABLE_VIEW_TAG + "tag TopicCompactionStrategy is null.";
                log.error(err);
                throw new IllegalStateException(err);
            }
            strategy.setSkippedMsgHandler((key, value) -> handleSkippedEvent(key));
            if (debug) {
                log.info("Successfully started the channel tableview.");
            }
            pulsar.getLocalMetadataStore().registerSessionListener(this::handleMetadataSessionEvent);
            if (debug) {
                log.info("Successfully registered the handleMetadataSessionEvent");
            }

            channelState = Started;
            log.info("Successfully started the channel.");
        } catch (Exception e) {
            String msg = "Failed to start the channel.";
            log.error(msg, e);
            throw new PulsarServerException(msg, e);
        }
    }

    @VisibleForTesting
    protected BrokerRegistry getBrokerRegistry() {
        return ((ExtensibleLoadManagerWrapper) pulsar.getLoadManager().get())
                .get().getBrokerRegistry();
    }

    @VisibleForTesting
    protected LoadManagerContext getContext() {
        return ((ExtensibleLoadManagerWrapper) pulsar.getLoadManager().get())
                .get().getContext();
    }

    @VisibleForTesting
    protected ExtensibleLoadManagerImpl getLoadManager() {
        return ExtensibleLoadManagerImpl.get(pulsar.getLoadManager().get());
    }

    @VisibleForTesting
    protected LeaderElectionService getLeaderElectionService() {
        return ((ExtensibleLoadManagerWrapper) pulsar.getLoadManager().get())
                .get().getLeaderElectionService();
    }

    public synchronized void close() throws PulsarServerException {
        channelState = Closed;
        boolean debug = debug();
        try {
            leaderElectionService = null;
            if (tableview != null) {
                tableview.close();
                tableview = null;
                if (debug) {
                    log.info("Successfully closed the channel tableview.");
                }
            }

            if (producer != null) {
                producer.close();
                producer = null;
                log.info("Successfully closed the channel producer.");
            }

            if (brokerRegistry != null) {
                brokerRegistry = null;
            }

            if (monitorTask != null) {
                monitorTask.cancel(true);
                monitorTask = null;
                log.info("Successfully cancelled the cleanup tasks");
            }

            if (stateChangeListeners != null) {
                stateChangeListeners.close();
            }

            log.info("Successfully closed the channel.");

        } catch (Exception e) {
            String msg = "Failed to close the channel.";
            log.error(msg, e);
            throw new PulsarServerException(msg, e);
        }
    }

    private boolean validateChannelState(ChannelState targetState, boolean checkLowerIds) {
        int order = checkLowerIds ? -1 : 1;
        if (Integer.compare(channelState.id, targetState.id) * order > 0) {
            return false;
        }
        return true;
    }

    private boolean debug() {
        return ExtensibleLoadManagerImpl.debug(config, log);
    }

    public CompletableFuture<Optional<String>> getChannelOwnerAsync() {
        if (!validateChannelState(LeaderElectionServiceStarted, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Invalid channel state:" + channelState.name()));
        }

        return leaderElectionService.readCurrentLeader()
                .thenApply(leader -> leader.map(LeaderBroker::getBrokerId));
    }

    public CompletableFuture<Boolean> isChannelOwnerAsync() {
        return getChannelOwnerAsync().thenApply(owner -> {
            if (owner.isPresent()) {
                return isTargetBroker(owner.get());
            } else {
                String msg = "There is no channel owner now.";
                log.error(msg);
                throw new IllegalStateException(msg);
            }
        });
    }

    public boolean isChannelOwner() {
        try {
            return isChannelOwnerAsync().get(
                    MAX_CHANNEL_OWNER_ELECTION_WAITING_TIME_IN_SECS, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String msg = "Failed to get the channel owner.";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public boolean isOwner(String serviceUnit, String targetBroker) {
        if (!validateChannelState(Started, true)) {
            throw new IllegalStateException("Invalid channel state:" + channelState.name());
        }
        var ownerFuture = getOwnerAsync(serviceUnit);
        if (!ownerFuture.isDone() || ownerFuture.isCompletedExceptionally() || ownerFuture.isCancelled()) {
            return false;
        }
        var owner = ownerFuture.join();
        if (owner.isPresent() && StringUtils.equals(targetBroker, owner.get())) {
            return true;
        }
        return false;
    }

    public boolean isOwner(String serviceUnit) {
        return isOwner(serviceUnit, brokerId);
    }

    private CompletableFuture<Optional<String>> getActiveOwnerAsync(
            String serviceUnit,
            ServiceUnitState state,
            Optional<String> owner) {
        return deferGetOwnerRequest(serviceUnit)
                .thenCompose(newOwner -> {
                    if (newOwner == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return brokerRegistry.lookupAsync(newOwner)
                            .thenApply(lookupData -> {
                                if (lookupData.isPresent()) {
                                    return newOwner;
                                } else {
                                    throw new IllegalStateException(
                                            "The new owner " + newOwner + " is inactive.");
                                }
                            });
                }).whenComplete((__, e) -> {
                    if (e != null) {
                        log.error("{} failed to get active owner broker. serviceUnit:{}, state:{}, owner:{}",
                                brokerId, serviceUnit, state, owner, e);
                        ownerLookUpCounters.get(state).getFailure().incrementAndGet();
                    }
                }).thenApply(Optional::ofNullable);
    }

    public CompletableFuture<Optional<String>> getOwnerAsync(String serviceUnit) {
        if (!validateChannelState(Started, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Invalid channel state:" + channelState.name()));
        }

        ServiceUnitStateData data = tableview.get(serviceUnit);
        ServiceUnitState state = state(data);
        ownerLookUpCounters.get(state).getTotal().incrementAndGet();
        switch (state) {
            case Owned -> {
                return getActiveOwnerAsync(serviceUnit, state, Optional.of(data.dstBroker()));
            }
            case Splitting -> {
                return getActiveOwnerAsync(serviceUnit, state, Optional.of(data.sourceBroker()));
            }
            case Assigning, Releasing -> {
                if (isTargetBroker(data.dstBroker())) {
                    return getActiveOwnerAsync(serviceUnit, state, Optional.of(data.dstBroker()));
                }
                // If this broker is not the dst broker, return the dst broker as the owner(or empty).
                // Clients need to connect(redirect) to the dst broker anyway
                // and wait for the dst broker to receive `Owned`.
                // This is also required to return getOwnerAsync on the src broker immediately during unloading.
                // Otherwise, topic creation(getOwnerAsync) could block unloading bundles,
                // if the topic creation(getOwnerAsync) happens during unloading on the src broker.
                return CompletableFuture.completedFuture(Optional.ofNullable(data.dstBroker()));
            }
            case Init, Free -> {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            case Deleted -> {
                ownerLookUpCounters.get(state).getFailure().incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalArgumentException(serviceUnit + " is deleted."));
            }
            default -> {
                ownerLookUpCounters.get(state).getFailure().incrementAndGet();
                String errorMsg = String.format("Failed to process service unit state data: %s when get owner.", data);
                log.error(errorMsg);
                return CompletableFuture.failedFuture(new IllegalStateException(errorMsg));
            }
        }
    }

    private Optional<String> getOwner(String serviceUnit) {
        ServiceUnitStateData data = tableview.get(serviceUnit);
        ServiceUnitState state = state(data);
        switch (state) {
            case Owned -> {
                return Optional.of(data.dstBroker());
            }
            case Splitting -> {
                return Optional.of(data.sourceBroker());
            }
            case Init, Free -> {
                return Optional.empty();
            }
            default -> {
                return null;
            }
        }
    }

    @Override
    public Optional<String> getAssigned(String serviceUnit) {
        if (!validateChannelState(Started, true)) {
            return Optional.empty();
        }

        ServiceUnitStateData data = tableview.get(serviceUnit);
        if (data == null) {
            return Optional.empty();
        }
        ServiceUnitState state = state(data);
        switch (state) {
            case Owned, Assigning -> {
                return Optional.of(data.dstBroker());
            }
            case Releasing -> {
                return Optional.ofNullable(data.dstBroker());
            }
            case Splitting -> {
                return Optional.of(data.sourceBroker());
            }
            case Init, Free -> {
                return Optional.empty();
            }
            case Deleted -> {
                log.warn("Trying to get the assigned broker from the deleted serviceUnit:{}", serviceUnit);
                return Optional.empty();
            }
            default -> {
                log.warn("Trying to get the assigned broker from unknown state:{} serviceUnit:{}", state, serviceUnit);
                return Optional.empty();
            }
        }
    }

    private long getNextVersionId(String serviceUnit) {
        var data = tableview.get(serviceUnit);
        return getNextVersionId(data);
    }

    private long getNextVersionId(ServiceUnitStateData data) {
        return data == null ? VERSION_ID_INIT : data.versionId() + 1;
    }

    public CompletableFuture<String> publishAssignEventAsync(String serviceUnit, String broker) {
        if (!validateChannelState(Started, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Invalid channel state:" + channelState.name()));
        }
        EventType eventType = Assign;
        eventCounters.get(eventType).getTotal().incrementAndGet();
        CompletableFuture<String> getOwnerRequest = deferGetOwnerRequest(serviceUnit);

        pubAsync(serviceUnit, new ServiceUnitStateData(Assigning, broker, getNextVersionId(serviceUnit)))
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        getOwnerRequests.remove(serviceUnit, getOwnerRequest);
                        if (!getOwnerRequest.isCompletedExceptionally()) {
                            getOwnerRequest.completeExceptionally(ex);
                        }
                        eventCounters.get(eventType).getFailure().incrementAndGet();
                    }
                });
        return getOwnerRequest;
    }

    private CompletableFuture<Void> publishOverrideEventAsync(String serviceUnit,
                                           ServiceUnitStateData orphanData,
                                           ServiceUnitStateData override) {
        if (!validateChannelState(Started, true)) {
            throw new IllegalStateException("Invalid channel state:" + channelState.name());
        }
        EventType eventType = EventType.Override;
        eventCounters.get(eventType).getTotal().incrementAndGet();
        return pubAsync(serviceUnit, override).whenComplete((__, e) -> {
            if (e != null) {
                eventCounters.get(eventType).getFailure().incrementAndGet();
                log.error("Failed to override serviceUnit:{} from orphanData:{} to overrideData:{}",
                        serviceUnit, orphanData, override, e);
            }
        }).thenApply(__ -> null);
    }

    public CompletableFuture<Void> publishUnloadEventAsync(Unload unload) {
        if (!validateChannelState(Started, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Invalid channel state:" + channelState.name()));
        }
        EventType eventType = Unload;
        eventCounters.get(eventType).getTotal().incrementAndGet();
        String serviceUnit = unload.serviceUnit();
        ServiceUnitStateData next;
        if (isTransferCommand(unload)) {
            next = new ServiceUnitStateData(
                    Releasing, unload.destBroker().get(), unload.sourceBroker(),
                    unload.force(), getNextVersionId(serviceUnit));
        } else {
            next = new ServiceUnitStateData(
                    Releasing, null, unload.sourceBroker(), unload.force(), getNextVersionId(serviceUnit));
        }
        return pubAsync(serviceUnit, next).whenComplete((__, ex) -> {
            if (ex != null) {
                eventCounters.get(eventType).getFailure().incrementAndGet();
            }
        }).thenApply(__ -> null);
    }

    public CompletableFuture<Void> publishSplitEventAsync(Split split) {
        if (!validateChannelState(Started, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Invalid channel state:" + channelState.name()));
        }
        EventType eventType = Split;
        eventCounters.get(eventType).getTotal().incrementAndGet();
        String serviceUnit = split.serviceUnit();
        ServiceUnitStateData next =
                new ServiceUnitStateData(Splitting, null, split.sourceBroker(),
                        split.splitServiceUnitToDestBroker(), getNextVersionId(serviceUnit));
        return pubAsync(serviceUnit, next).whenComplete((__, ex) -> {
            if (ex != null) {
                eventCounters.get(eventType).getFailure().incrementAndGet();
            }
        }).thenApply(__ -> null);
    }

    private void handle(String serviceUnit, ServiceUnitStateData data) {
        long totalHandledRequests = getHandlerTotalCounter(data).incrementAndGet();
        if (debug()) {
            log.info("{} received a handle request for serviceUnit:{}, data:{}. totalHandledRequests:{}",
                    brokerId, serviceUnit, data, totalHandledRequests);
        }

        ServiceUnitState state = state(data);
        try {
            switch (state) {
                case Owned -> handleOwnEvent(serviceUnit, data);
                case Assigning -> handleAssignEvent(serviceUnit, data);
                case Releasing -> handleReleaseEvent(serviceUnit, data);
                case Splitting -> handleSplitEvent(serviceUnit, data);
                case Deleted -> handleDeleteEvent(serviceUnit, data);
                case Free -> handleFreeEvent(serviceUnit, data);
                case Init -> handleInitEvent(serviceUnit);
                default -> throw new IllegalStateException("Failed to handle channel data:" + data);
            }
        } catch (Throwable e){
            log.error("Failed to handle the event. serviceUnit:{}, data:{}, handlerFailureCount:{}",
                    serviceUnit, data, getHandlerFailureCounter(data).incrementAndGet(), e);
            throw e;
        }
    }

    private static boolean isTransferCommand(ServiceUnitStateData data) {
        if (data == null) {
            return false;
        }
        return StringUtils.isNotEmpty(data.dstBroker())
                && StringUtils.isNotEmpty(data.sourceBroker());
    }

    private static boolean isTransferCommand(Unload data) {
        return data.destBroker().isPresent();
    }

    private static String getLogEventTag(ServiceUnitStateData data) {
        return data == null ? Init.toString() :
                isTransferCommand(data) ? "Transfer:" + data.state() : data.state().toString();
    }

    private AtomicLong getHandlerTotalCounter(ServiceUnitStateData data) {
        return getHandlerCounter(data, true);
    }

    private AtomicLong getHandlerFailureCounter(ServiceUnitStateData data) {
        return getHandlerCounter(data, false);
    }

    private AtomicLong getHandlerCounter(ServiceUnitStateData data, boolean total) {
        var state = state(data);
        var counter = total
                ? handlerCounters.get(state).getTotal() : handlerCounters.get(state).getFailure();
        if (counter == null) {
            throw new IllegalStateException("Unknown state:" + state);
        }
        return counter;
    }

    private void log(Throwable e, String serviceUnit, ServiceUnitStateData data, ServiceUnitStateData next) {
        if (e == null) {
            if (debug() || isTransferCommand(data)) {
                long handlerTotalCount = getHandlerTotalCounter(data).get();
                long handlerFailureCount = getHandlerFailureCounter(data).get();
                log.info("{} handled {} event for serviceUnit:{}, cur:{}, next:{}, "
                                + "totalHandledRequests:{}, totalFailedRequests:{}",
                        brokerId, getLogEventTag(data), serviceUnit,
                        data == null ? "" : data,
                        next == null ? "" : next,
                        handlerTotalCount, handlerFailureCount
                );
            }
        } else {
            long handlerTotalCount = getHandlerTotalCounter(data).get();
            long handlerFailureCount = getHandlerFailureCounter(data).incrementAndGet();
            log.error("{} failed to handle {} event for serviceUnit:{}, cur:{}, next:{}, "
                            + "totalHandledRequests:{}, totalFailedRequests:{}",
                    brokerId, getLogEventTag(data), serviceUnit,
                    data == null ? "" : data,
                    next == null ? "" : next,
                    handlerTotalCount, handlerFailureCount,
                    e);
        }
    }

    private void handleSkippedEvent(String serviceUnit) {
        var getOwnerRequest = getOwnerRequests.get(serviceUnit);
        if (getOwnerRequest != null) {
            var data = tableview.get(serviceUnit);
            if (data != null && data.state() == Owned) {
                getOwnerRequest.complete(data.dstBroker());
                getOwnerRequests.remove(serviceUnit);
                stateChangeListeners.notify(serviceUnit, data, null);
            }
        }
    }

    private void handleOwnEvent(String serviceUnit, ServiceUnitStateData data) {
        var getOwnerRequest = getOwnerRequests.remove(serviceUnit);
        if (getOwnerRequest != null) {
            if (debug()) {
                log.info("Returned owner request for serviceUnit:{}", serviceUnit);
            }
            getOwnerRequest.complete(data.dstBroker());
        }

        if (isTargetBroker(data.dstBroker())) {
            pulsar.getNamespaceService()
                    .onNamespaceBundleOwned(LoadManagerShared.getNamespaceBundle(pulsar, serviceUnit));
            lastOwnEventHandledAt = System.currentTimeMillis();
            stateChangeListeners.notify(serviceUnit, data, null);
            log(null, serviceUnit, data, null);
        } else if (isTargetBroker(data.sourceBroker())) {
            var isOrphanCleanup = data.force();
            var isTransfer = isTransferCommand(data) && pulsar.getConfig().isLoadBalancerMultiPhaseBundleUnload();
            var future = isOrphanCleanup || isTransfer
                    ? closeServiceUnit(serviceUnit, true) : CompletableFuture.completedFuture(null);
            stateChangeListeners.notifyOnCompletion(future, serviceUnit, data)
                    .whenComplete((__, e) -> log(e, serviceUnit, data, null));
        } else {
            stateChangeListeners.notify(serviceUnit, data, null);
        }
    }

    private void handleAssignEvent(String serviceUnit, ServiceUnitStateData data) {
        if (isTargetBroker(data.dstBroker())) {
            ServiceUnitStateData next = new ServiceUnitStateData(
                    Owned, data.dstBroker(), data.sourceBroker(), getNextVersionId(data));
            stateChangeListeners.notifyOnCompletion(pubAsync(serviceUnit, next), serviceUnit, data)
                    .whenComplete((__, e) -> log(e, serviceUnit, data, next));
        }
    }

    private void handleReleaseEvent(String serviceUnit, ServiceUnitStateData data) {
        if (isTargetBroker(data.sourceBroker())) {
            ServiceUnitStateData next;
            CompletableFuture<Integer> unloadFuture;
            if (isTransferCommand(data)) {
                next = new ServiceUnitStateData(
                        Assigning, data.dstBroker(), data.sourceBroker(), getNextVersionId(data));
                // If the optimized bundle unload is disabled, disconnect the clients at time of RELEASE.
                var disconnectClients = !pulsar.getConfig().isLoadBalancerMultiPhaseBundleUnload();
                unloadFuture = closeServiceUnit(serviceUnit, disconnectClients);
            } else {
                next = new ServiceUnitStateData(
                        Free, null, data.sourceBroker(), getNextVersionId(data));
                unloadFuture = closeServiceUnit(serviceUnit, true);
            }
            stateChangeListeners.notifyOnCompletion(unloadFuture
                            .thenCompose(__ -> pubAsync(serviceUnit, next)), serviceUnit, data)
                    .whenComplete((__, e) -> log(e, serviceUnit, data, next));
        }
    }

    private void handleSplitEvent(String serviceUnit, ServiceUnitStateData data) {
        if (isTargetBroker(data.sourceBroker())) {
            stateChangeListeners.notifyOnCompletion(splitServiceUnit(serviceUnit, data), serviceUnit, data)
                    .whenComplete((__, e) -> log(e, serviceUnit, data, null));
        }
    }

    private void handleFreeEvent(String serviceUnit, ServiceUnitStateData data) {
        var getOwnerRequest = getOwnerRequests.remove(serviceUnit);
        if (getOwnerRequest != null) {
            getOwnerRequest.complete(null);
        }

        if (isTargetBroker(data.sourceBroker())) {
            stateChangeListeners.notifyOnCompletion(
                            data.force() ? closeServiceUnit(serviceUnit, true)
                                    : CompletableFuture.completedFuture(0), serviceUnit, data)
                    .whenComplete((__, e) -> log(e, serviceUnit, data, null));
        } else {
            stateChangeListeners.notify(serviceUnit, data, null);
        }
    }

    private void handleDeleteEvent(String serviceUnit, ServiceUnitStateData data) {
        var getOwnerRequest = getOwnerRequests.remove(serviceUnit);
        if (getOwnerRequest != null) {
            getOwnerRequest.completeExceptionally(new IllegalStateException(serviceUnit + "has been deleted."));
        }
        stateChangeListeners.notify(serviceUnit, data, null);
        if (isTargetBroker(data.sourceBroker())) {
            log(null, serviceUnit, data, null);
        }
    }

    private void handleInitEvent(String serviceUnit) {
        var getOwnerRequest = getOwnerRequests.remove(serviceUnit);
        if (getOwnerRequest != null) {
            getOwnerRequest.complete(null);
        }
        stateChangeListeners.notify(serviceUnit, null, null);
        log(null, serviceUnit, null, null);
    }

    private CompletableFuture<MessageId> pubAsync(String serviceUnit, ServiceUnitStateData data) {
        CompletableFuture<MessageId> future = new CompletableFuture<>();
        producer.newMessage()
                .key(serviceUnit)
                .value(data)
                .sendAsync()
                .whenComplete((messageId, e) -> {
                    if (e != null) {
                        log.error("Failed to publish the message: serviceUnit:{}, data:{}",
                                serviceUnit, data, e);
                        future.completeExceptionally(e);
                    } else {
                        future.complete(messageId);
                    }
                });
        return future;
    }

    private CompletableFuture<MessageId> tombstoneAsync(String serviceUnit) {
        return pubAsync(serviceUnit, null);
    }

    private boolean isTargetBroker(String broker) {
        if (broker == null) {
            return false;
        }
        return broker.equals(brokerId);
    }

    private CompletableFuture<String> deferGetOwnerRequest(String serviceUnit) {

        var requested = new MutableObject<CompletableFuture<String>>();
        try {
            return getOwnerRequests
                    .computeIfAbsent(serviceUnit, k -> {
                        var ownerBefore = getOwner(serviceUnit);
                        if (ownerBefore != null && ownerBefore.isPresent()) {
                            // Here, we do a quick active check first with the computeIfAbsent lock
                            brokerRegistry.lookupAsync(ownerBefore.get()).getNow(Optional.empty())
                                    .ifPresent(__ -> requested.setValue(
                                            CompletableFuture.completedFuture(ownerBefore.get())));

                            if (requested.getValue() != null) {
                                return requested.getValue();
                            }
                        }


                        CompletableFuture<String> future =
                                new CompletableFuture<String>().orTimeout(inFlightStateWaitingTimeInMillis,
                                                TimeUnit.MILLISECONDS)
                                        .exceptionally(e -> {
                                            var ownerAfter = getOwner(serviceUnit);
                                            log.warn("{} failed to wait for owner for serviceUnit:{}; Trying to "
                                                            + "return the current owner:{}",
                                                    brokerId, serviceUnit, ownerAfter, e);
                                            if (ownerAfter == null) {
                                                throw new IllegalStateException(e);
                                            }
                                            return ownerAfter.orElse(null);
                                        });
                        if (debug()) {
                            log.info("{} is waiting for owner for serviceUnit:{}", brokerId, serviceUnit);
                        }
                        requested.setValue(future);
                        return future;
                    });
        } finally {
            var future = requested.getValue();
            if (future != null) {
                future.whenComplete((__, e) -> {
                    getOwnerRequests.remove(serviceUnit);
                    if (e != null) {
                        log.warn("{} failed to getOwner for serviceUnit:{}", brokerId, serviceUnit, e);
                    }
                });
            }
        }
    }

    private CompletableFuture<Integer> closeServiceUnit(String serviceUnit, boolean disconnectClients) {
        long startTime = System.nanoTime();
        MutableInt unloadedTopics = new MutableInt();
        NamespaceBundle bundle = LoadManagerShared.getNamespaceBundle(pulsar, serviceUnit);
        return pulsar.getBrokerService().unloadServiceUnit(
                        bundle,
                        disconnectClients,
                        true,
                        pulsar.getConfig().getNamespaceBundleUnloadingTimeoutMs(),
                        TimeUnit.MILLISECONDS)
                .thenApply(numUnloadedTopics -> {
                    unloadedTopics.setValue(numUnloadedTopics);
                    return numUnloadedTopics;
                })
                .whenComplete((__, ex) -> {
                    if (disconnectClients) {
                        // clean up topics that failed to unload from the broker ownership cache
                        pulsar.getBrokerService().cleanUnloadedTopicFromCache(bundle);
                    }
                    pulsar.getNamespaceService().onNamespaceBundleUnload(bundle);
                    double unloadBundleTime = TimeUnit.NANOSECONDS
                            .toMillis((System.nanoTime() - startTime));
                    if (ex != null) {
                        log.error("Failed to close topics under bundle:{} in {} ms",
                                bundle.toString(), unloadBundleTime, ex);
                    } else {
                        log.info("Unloading bundle:{} with {} topics completed in {} ms",
                                bundle, unloadedTopics, unloadBundleTime);
                    }
                });
    }

    private CompletableFuture<Void> splitServiceUnit(String serviceUnit, ServiceUnitStateData data) {
        // Write the child ownerships to channel.
        long startTime = System.nanoTime();
        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceBundleFactory bundleFactory = namespaceService.getNamespaceBundleFactory();
        NamespaceBundle bundle = LoadManagerShared.getNamespaceBundle(pulsar, serviceUnit);
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        Map<String, Optional<String>> bundleToDestBroker = data.splitServiceUnitToDestBroker();
        List<Long> boundaries = null;
        NamespaceBundleSplitAlgorithm nsBundleSplitAlgorithm =
                namespaceService.getNamespaceBundleSplitAlgorithmByName(
                        config.getDefaultNamespaceBundleSplitAlgorithm());
        if (bundleToDestBroker != null && bundleToDestBroker.size() == 2) {
            Set<Long> boundariesSet = new HashSet<>();
            String namespace = bundle.getNamespaceObject().toString();
            bundleToDestBroker.forEach((bundleRange, destBroker) -> {
                NamespaceBundle subBundle = bundleFactory.getBundle(namespace, bundleRange);
                boundariesSet.add(subBundle.getKeyRange().lowerEndpoint());
                boundariesSet.add(subBundle.getKeyRange().upperEndpoint());
            });
            boundaries = new ArrayList<>(boundariesSet);
            nsBundleSplitAlgorithm = NamespaceBundleSplitAlgorithm.SPECIFIED_POSITIONS_DIVIDE_FORCE_ALGO;
        }
        final AtomicInteger counter = new AtomicInteger(0);
        var childBundles = data.splitServiceUnitToDestBroker().keySet().stream()
                .map(child -> bundleFactory.getBundle(
                        bundle.getNamespaceObject().toString(), child))
                .collect(Collectors.toList());
        this.splitServiceUnitOnceAndRetry(namespaceService, bundleFactory, nsBundleSplitAlgorithm,
                bundle, childBundles, boundaries, data, counter, startTime, completionFuture);
        return completionFuture;
    }



    @VisibleForTesting
    protected void splitServiceUnitOnceAndRetry(NamespaceService namespaceService,
                                                NamespaceBundleFactory bundleFactory,
                                                NamespaceBundleSplitAlgorithm algorithm,
                                                NamespaceBundle parentBundle,
                                                List<NamespaceBundle> childBundles,
                                                List<Long> boundaries,
                                                ServiceUnitStateData parentData,
                                                AtomicInteger counter,
                                                long startTime,
                                                CompletableFuture<Void> completionFuture) {
        ownChildBundles(childBundles, parentData)
                .thenCompose(__ -> getSplitNamespaceBundles(
                        namespaceService, bundleFactory, algorithm, parentBundle, childBundles, boundaries))
                .thenCompose(namespaceBundles -> updateSplitNamespaceBundlesAsync(
                        namespaceService, bundleFactory, parentBundle, namespaceBundles))
                .thenAccept(__ -> // Update bundled_topic cache for load-report-generation
                        pulsar.getBrokerService().refreshTopicToStatsMaps(parentBundle))
                .thenAccept(__ -> pubAsync(parentBundle.toString(), new ServiceUnitStateData(
                                Deleted, null, parentData.sourceBroker(), getNextVersionId(parentData))))
                .thenAccept(__ -> {
                    double splitBundleTime = TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - startTime));
                    log.info("Successfully split {} parent namespace-bundle to {} in {} ms",
                            parentBundle, childBundles, splitBundleTime);
                    namespaceService.onNamespaceBundleSplit(parentBundle);
                    completionFuture.complete(null);
                })
                .exceptionally(ex -> {
                    // Retry several times on BadVersion
                    Throwable throwable = FutureUtil.unwrapCompletionException(ex);
                    if ((throwable instanceof MetadataStoreException.BadVersionException)
                            && (counter.incrementAndGet() < NamespaceService.BUNDLE_SPLIT_RETRY_LIMIT)) {
                        log.warn("Failed to update bundle range in metadata store. Retrying {} th / {} limit",
                                counter.get(), NamespaceService.BUNDLE_SPLIT_RETRY_LIMIT, ex);
                        pulsar.getExecutor().schedule(() -> splitServiceUnitOnceAndRetry(
                                namespaceService, bundleFactory, algorithm, parentBundle, childBundles,
                                        boundaries, parentData, counter, startTime, completionFuture),
                                100, MILLISECONDS);
                    } else {
                        // Retry enough, or meet other exception
                        String msg = format("Failed to split bundle %s, Retried %d th / %d limit, reason %s",
                                parentBundle.toString(), counter.get(),
                                NamespaceService.BUNDLE_SPLIT_RETRY_LIMIT, throwable.getMessage());
                        log.warn(msg, throwable);
                        completionFuture.completeExceptionally(
                                new BrokerServiceException.ServiceUnitNotReadyException(msg));
                    }
                    return null;
                });
    }

    private CompletableFuture<Void> ownChildBundles(List<NamespaceBundle> childBundles,
                                                    ServiceUnitStateData parentData) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(childBundles.size());
        var debug = debug();
        for (var childBundle : childBundles) {
            var childBundleStr = childBundle.toString();
            var childData = tableview.get(childBundleStr);
            if (childData != null) {
                if (debug) {
                    log.info("Already owned child bundle:{}", childBundleStr);
                }
            } else {
                childData = new ServiceUnitStateData(Owned, parentData.sourceBroker(),
                        VERSION_ID_INIT);
                futures.add(pubAsync(childBundleStr, childData).thenApply(__ -> null));
            }
        }

        if (!futures.isEmpty()) {
            return FutureUtil.waitForAll(futures);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<NamespaceBundles> getSplitNamespaceBundles(NamespaceService namespaceService,
                                                                         NamespaceBundleFactory bundleFactory,
                                                                         NamespaceBundleSplitAlgorithm algorithm,
                                                                         NamespaceBundle parentBundle,
                                                                         List<NamespaceBundle> childBundles,
                                                                         List<Long> boundaries) {
        CompletableFuture future = new CompletableFuture();
        final var debug = debug();
        var targetNsBundle = bundleFactory.getBundles(parentBundle.getNamespaceObject());
        boolean found = false;
        try {
            targetNsBundle.validateBundle(parentBundle);
        } catch (IllegalArgumentException e) {
            if (debug) {
                log.info("Namespace bundles do not contain the parent bundle:{}",
                        parentBundle);
            }
            for (var childBundle : childBundles) {
                try {
                    targetNsBundle.validateBundle(childBundle);
                    if (debug) {
                        log.info("Namespace bundles contain the child bundle:{}",
                                childBundle);
                    }
                } catch (Exception ex) {
                    future.completeExceptionally(
                            new BrokerServiceException.ServiceUnitNotReadyException(
                                    "Namespace bundles do not contain the child bundle:" + childBundle, e));
                    return future;
                }
            }
            found = true;
        } catch (Exception e) {
            future.completeExceptionally(
                    new BrokerServiceException.ServiceUnitNotReadyException(
                            "Failed to validate the parent bundle in the namespace bundles.", e));
            return future;
        }
        if (found) {
            future.complete(targetNsBundle);
            return future;
        } else {
            return namespaceService.getSplitBoundary(parentBundle, algorithm, boundaries)
                    .thenApply(splitBundlesPair -> splitBundlesPair.getLeft());
        }
    }

    private CompletableFuture<Void> updateSplitNamespaceBundlesAsync(
            NamespaceService namespaceService,
            NamespaceBundleFactory bundleFactory,
            NamespaceBundle parentBundle,
            NamespaceBundles splitNamespaceBundles) {
        var namespaceName = parentBundle.getNamespaceObject();
        return namespaceService.updateNamespaceBundles(
                        namespaceName, splitNamespaceBundles)
                .thenCompose(__ -> namespaceService.updateNamespaceBundlesForPolicies(
                        namespaceName, splitNamespaceBundles))
                .thenAccept(__ -> {
                    bundleFactory.invalidateBundleCache(parentBundle.getNamespaceObject());
                    if (debug()) {
                        log.info("Successfully updated split namespace bundles and namespace bundle cache.");
                    }
                });
    }

    public void handleMetadataSessionEvent(SessionEvent e) {
        if (e == SessionReestablished || e == SessionLost) {
            lastMetadataSessionEvent = e;
            lastMetadataSessionEventTimestamp = System.currentTimeMillis();
            log.info("Received metadata session event:{} at timestamp:{}",
                    lastMetadataSessionEvent, lastMetadataSessionEventTimestamp);
        }
    }

    public void handleBrokerRegistrationEvent(String broker, NotificationType type) {
        if (type == NotificationType.Created) {
            log.info("BrokerRegistry detected the broker:{} registry has been created.", broker);
            handleBrokerCreationEvent(broker);
        } else if (type == NotificationType.Deleted) {
            log.info("BrokerRegistry detected the broker:{} registry has been deleted.", broker);
            handleBrokerDeletionEvent(broker);
        }
    }

    private MetadataState getMetadataState() {
        long now = System.currentTimeMillis();
        if (lastMetadataSessionEvent == SessionReestablished) {
            if (now - lastMetadataSessionEventTimestamp > 1000 * maxCleanupDelayTimeInSecs) {
                return Stable;
            }
            return Jittery;
        }
        return Unstable;
    }

    private void handleBrokerCreationEvent(String broker) {
        CompletableFuture<Void> future = cleanupJobs.remove(broker);
        if (future != null) {
            future.cancel(false);
            totalInactiveBrokerCleanupCancelledCnt++;
            log.info("Successfully cancelled the ownership cleanup for broker:{}."
                            + " Active cleanup job count:{}",
                    broker, cleanupJobs.size());
        } else {
            if (debug()) {
                log.info("No needs to cancel the ownership cleanup for broker:{}."
                                + " There was no scheduled cleanup job. Active cleanup job count:{}",
                        broker, cleanupJobs.size());
            }
        }
    }

    private void handleBrokerDeletionEvent(String broker) {
        if (!isChannelOwner()) {
            log.warn("This broker is not the leader now. Ignoring BrokerDeletionEvent for broker {}.", broker);
            return;
        }
        MetadataState state = getMetadataState();
        log.info("Handling broker:{} ownership cleanup based on metadata connection state:{}, event:{}, event_ts:{}:",
                broker, state, lastMetadataSessionEvent, lastMetadataSessionEventTimestamp);
        switch (state) {
            case Stable -> scheduleCleanup(broker, minCleanupDelayTimeInSecs);
            case Jittery -> scheduleCleanup(broker, maxCleanupDelayTimeInSecs);
            case Unstable -> {
                totalInactiveBrokerCleanupIgnoredCnt++;
                log.error("MetadataState state is unstable. "
                        + "Ignoring the ownership cleanup request for the reported broker :{}", broker);
            }
        }
    }

    private void scheduleCleanup(String broker, long delayInSecs) {
        var scheduled = new MutableObject<CompletableFuture<Void>>();
        try {
            cleanupJobs.computeIfAbsent(broker, k -> {
                Executor delayed = CompletableFuture
                        .delayedExecutor(delayInSecs, TimeUnit.SECONDS, pulsar.getLoadManagerExecutor());
                totalInactiveBrokerCleanupScheduledCnt++;
                var future = CompletableFuture
                        .runAsync(() -> {
                                    try {
                                        doCleanup(broker);
                                    } catch (Throwable e) {
                                        log.error("Failed to run the cleanup job for the broker {}, "
                                                        + "totalCleanupErrorCnt:{}.",
                                                broker, totalCleanupErrorCnt.incrementAndGet(), e);
                                    }
                                }
                                , delayed);
                scheduled.setValue(future);
                return future;
            });
        } finally {
            var future = scheduled.getValue();
            if (future != null) {
                future.whenComplete((v, ex) -> {
                    cleanupJobs.remove(broker);
                });
            }
        }

        log.info("Scheduled ownership cleanup for broker:{} with delay:{} secs. Pending clean jobs:{}.",
                broker, delayInSecs, cleanupJobs.size());
    }


    private void overrideOwnership(String serviceUnit, ServiceUnitStateData orphanData, String inactiveBroker) {
        final var version = getNextVersionId(orphanData);
        final var override = selectBroker(serviceUnit, inactiveBroker).map(selectedBroker -> {
            if (orphanData.state() == Splitting) {
                return new ServiceUnitStateData(Splitting, orphanData.dstBroker(), selectedBroker,
                        Map.copyOf(orphanData.splitServiceUnitToDestBroker()), true, version);
            } else {
                return new ServiceUnitStateData(Owned, selectedBroker, inactiveBroker, true, version);
            }
        }).orElseGet(() -> new ServiceUnitStateData(Free, null, inactiveBroker, true, version));
        log.info("Overriding ownership serviceUnit:{} from orphanData:{} to overrideData:{}",
                serviceUnit, orphanData, override);
        publishOverrideEventAsync(serviceUnit, orphanData, override)
                .exceptionally(e -> {
                    log.error(
                            "Failed to override the ownership serviceUnit:{} orphanData:{}. "
                                    + "Failed to publish override event. totalCleanupErrorCnt:{}",
                            serviceUnit, orphanData, totalCleanupErrorCnt.incrementAndGet());
                    return null;
                });
    }

    private void waitForCleanups(String broker, boolean excludeSystemTopics, int maxWaitTimeInMillis) {
        long started = System.currentTimeMillis();
        while (System.currentTimeMillis() - started < maxWaitTimeInMillis) {
            boolean cleaned = true;
            for (var etr : tableview.entrySet()) {
                var serviceUnit = etr.getKey();
                var data = etr.getValue();

                if (excludeSystemTopics && serviceUnit.startsWith(SYSTEM_NAMESPACE.toString())) {
                    continue;
                }

                if (data.state() == Owned && broker.equals(data.dstBroker())) {
                    cleaned = false;
                    break;
                }
            }
            if (cleaned) {
                try {
                    MILLISECONDS.sleep(OWNERSHIP_CLEAN_UP_CONVERGENCE_DELAY_IN_MILLIS);
                } catch (InterruptedException e) {
                    log.warn("Interrupted while gracefully waiting for the cleanup convergence.");
                }
                break;
            } else {
                try {
                    MILLISECONDS.sleep(OWNERSHIP_CLEAN_UP_WAIT_RETRY_DELAY_IN_MILLIS);
                } catch (InterruptedException e) {
                    log.warn("Interrupted while delaying the next service unit clean-up. Cleaning broker:{}",
                            brokerId);
                }
            }
        }
    }

    private synchronized void doCleanup(String broker)  {
        long startTime = System.nanoTime();
        log.info("Started ownership cleanup for the inactive broker:{}", broker);
        int orphanServiceUnitCleanupCnt = 0;
        long totalCleanupErrorCntStart = totalCleanupErrorCnt.get();
        Map<String, ServiceUnitStateData> orphanSystemServiceUnits = new HashMap<>();
        for (var etr : tableview.entrySet()) {
            var stateData = etr.getValue();
            var serviceUnit = etr.getKey();
            var state = state(stateData);
            if (StringUtils.equals(broker, stateData.dstBroker()) && isActiveState(state)
                    || StringUtils.equals(broker, stateData.sourceBroker()) && isInFlightState(state)) {
                if (serviceUnit.startsWith(SYSTEM_NAMESPACE.toString())) {
                    orphanSystemServiceUnits.put(serviceUnit, stateData);
                } else {
                    overrideOwnership(serviceUnit, stateData, broker);
                }
                orphanServiceUnitCleanupCnt++;
            }
        }

        try {
            producer.flushAsync().get(OWNERSHIP_CLEAN_UP_MAX_WAIT_TIME_IN_MILLIS, MILLISECONDS);
        } catch (Exception e) {
            log.error("Failed to flush the in-flight non-system bundle override messages.", e);
        }


        if (orphanServiceUnitCleanupCnt > 0) {
            // System bundles can contain this channel's system topic and other important system topics.
            // Cleaning such system bundles(closing the system topics) together with the non-system bundles
            // can cause the cluster to be temporarily unstable.
            // Hence, we clean the non-system bundles first and gracefully wait for them.
            // After that, we clean the system bundles, if any.
            waitForCleanups(broker, true, OWNERSHIP_CLEAN_UP_MAX_WAIT_TIME_IN_MILLIS);
            this.totalOrphanServiceUnitCleanupCnt += orphanServiceUnitCleanupCnt;
            this.totalInactiveBrokerCleanupCnt++;
        }

        // clean system bundles in the end
        for (var orphanSystemServiceUnit : orphanSystemServiceUnits.entrySet()) {
            log.info("Overriding orphan system service unit:{}", orphanSystemServiceUnit.getKey());
            overrideOwnership(orphanSystemServiceUnit.getKey(), orphanSystemServiceUnit.getValue(), broker);
        }

        try {
            producer.flushAsync().get(OWNERSHIP_CLEAN_UP_MAX_WAIT_TIME_IN_MILLIS, MILLISECONDS);
        } catch (Exception e) {
            log.error("Failed to flush the in-flight system bundle override messages.", e);
        }

        double cleanupTime = TimeUnit.NANOSECONDS
                .toMillis((System.nanoTime() - startTime));

        // clean load data stores
        getContext().topBundleLoadDataStore().removeAsync(broker);
        getContext().brokerLoadDataStore().removeAsync(broker);

        log.info("Completed a cleanup for the inactive broker:{} in {} ms. "
                        + "Cleaned up orphan service units: orphanServiceUnitCleanupCnt:{}, "
                        + "approximate cleanupErrorCnt:{}, metrics:{} ",
                broker,
                cleanupTime,
                orphanServiceUnitCleanupCnt,
                totalCleanupErrorCnt.get() - totalCleanupErrorCntStart,
                printCleanupMetrics());

    }

    private Optional<String> selectBroker(String serviceUnit, String inactiveBroker) {
        try {
            return loadManager.selectAsync(
                    LoadManagerShared.getNamespaceBundle(pulsar, serviceUnit),
                            Set.of(inactiveBroker), LookupOptions.builder().build())
                    .get(inFlightStateWaitingTimeInMillis, MILLISECONDS);
        } catch (Throwable e) {
            log.error("Failed to select a broker for serviceUnit:{}", serviceUnit);
        }
        return Optional.empty();
    }

    private Optional<ServiceUnitStateData> getRollForwardStateData(String serviceUnit,
                                                                   String inactiveBroker,
                                                                   long nextVersionId) {
        Optional<String> selectedBroker = selectBroker(serviceUnit, inactiveBroker);
        if (selectedBroker.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ServiceUnitStateData(Owned, selectedBroker.get(), true, nextVersionId));
    }


    private Optional<ServiceUnitStateData> getOverrideInFlightStateData(
            String serviceUnit, ServiceUnitStateData orphanData,
            Set<String> availableBrokers) {
        long nextVersionId = getNextVersionId(orphanData);
        var state = orphanData.state();
        switch (state) {
            case Assigning: {
                return getRollForwardStateData(serviceUnit, orphanData.dstBroker(), nextVersionId);
            }
            case Splitting: {
                return Optional.of(new ServiceUnitStateData(Splitting,
                        orphanData.dstBroker(), orphanData.sourceBroker(),
                        Map.copyOf(orphanData.splitServiceUnitToDestBroker()),
                        true, nextVersionId));
            }
            case Releasing: {
                if (availableBrokers.contains(orphanData.sourceBroker())) {
                    // rollback to the src
                    return Optional.of(new ServiceUnitStateData(Owned, orphanData.sourceBroker(), true, nextVersionId));
                } else {
                    return getRollForwardStateData(serviceUnit, orphanData.sourceBroker(), nextVersionId);
                }
            }
            default: {
                var msg = String.format("Failed to get the overrideStateData from serviceUnit=%s, orphanData=%s",
                        serviceUnit, orphanData);
                log.error(msg);
                throw new IllegalStateException(msg);
            }
        }
    }

    @VisibleForTesting
    protected void monitorOwnerships(List<String> brokers) {
        if (!isChannelOwner()) {
            log.warn("This broker is not the leader now. Skipping ownership monitor.");
            return;
        }

        if (brokers == null || brokers.size() == 0) {
            log.error("no active brokers found. Skipping ownership monitor.");
            return;
        }

        var metadataState = getMetadataState();
        if (metadataState != Stable) {
            log.warn("metadata state:{} is not Stable. Skipping ownership monitor.", metadataState);
            return;
        }

        var debug = debug();
        if (debug) {
            log.info("Started the ownership monitor run for activeBrokerCount:{}", brokers.size());
        }

        long startTime = System.nanoTime();
        Set<String> inactiveBrokers = new HashSet<>();
        Set<String> activeBrokers = new HashSet<>(brokers);
        Map<String, ServiceUnitStateData> orphanServiceUnits = new HashMap<>();
        int serviceUnitTombstoneCleanupCnt = 0;
        int orphanServiceUnitCleanupCnt = 0;
        long totalCleanupErrorCntStart = totalCleanupErrorCnt.get();
        long now = System.currentTimeMillis();
        for (var etr : tableview.entrySet()) {
            String serviceUnit = etr.getKey();
            ServiceUnitStateData stateData = etr.getValue();
            String dstBroker = stateData.dstBroker();
            String srcBroker = stateData.sourceBroker();
            var state = stateData.state();

            if (isActiveState(state) && StringUtils.isNotBlank(srcBroker) && !activeBrokers.contains(srcBroker)) {
                inactiveBrokers.add(srcBroker);
                continue;
            }
            if (isActiveState(state) && StringUtils.isNotBlank(dstBroker) && !activeBrokers.contains(dstBroker)) {
                inactiveBrokers.add(dstBroker);
                continue;
            }
            if (isActiveState(state) && isInFlightState(state)
                    && now - stateData.timestamp() > inFlightStateWaitingTimeInMillis) {
                orphanServiceUnits.put(serviceUnit, stateData);
                continue;
            }

            if (!isActiveState(state) && now - stateData.timestamp() > stateTombstoneDelayTimeInMillis) {
                log.info("Found semi-terminal states to tombstone"
                        + " serviceUnit:{}, stateData:{}", serviceUnit, stateData);
                tombstoneAsync(serviceUnit).whenComplete((__, e) -> {
                    if (e != null) {
                        log.error("Failed cleaning the ownership serviceUnit:{}, stateData:{}, "
                                        + "cleanupErrorCnt:{}.",
                                serviceUnit, stateData,
                                totalCleanupErrorCnt.incrementAndGet() - totalCleanupErrorCntStart, e);
                    }
                });
                serviceUnitTombstoneCleanupCnt++;
            }
        }

        // Skip cleaning orphan bundles if inactiveBrokers exist. This is a bigger problem.
        if (!inactiveBrokers.isEmpty()) {
            for (String inactiveBroker : inactiveBrokers) {
                handleBrokerDeletionEvent(inactiveBroker);
            }
        } else if (!orphanServiceUnits.isEmpty()) {
            for (var etr : orphanServiceUnits.entrySet()) {
                var orphanServiceUnit = etr.getKey();
                var orphanData = etr.getValue();
                var overrideData = getOverrideInFlightStateData(
                        orphanServiceUnit, orphanData, activeBrokers);
                if (overrideData.isPresent()) {
                    log.info("Overriding in-flight state ownership serviceUnit:{} "
                                    + "from orphanData:{} to overrideData:{}",
                            orphanServiceUnit, orphanData, overrideData);
                    publishOverrideEventAsync(orphanServiceUnit, orphanData, overrideData.get())
                            .whenComplete((__, e) -> {
                                if (e != null) {
                                    log.error("Failed cleaning the ownership orphanServiceUnit:{}, orphanData:{}, "
                                                    + "cleanupErrorCnt:{}.",
                                            orphanServiceUnit, orphanData,
                                            totalCleanupErrorCnt.incrementAndGet() - totalCleanupErrorCntStart, e);
                                }
                            });
                    orphanServiceUnitCleanupCnt++;
                } else {
                    log.warn("Failed get the overrideStateData from orphanServiceUnit:{}, orphanData:{},"
                                    + " cleanupErrorCnt:{}. will retry..",
                            orphanServiceUnit, orphanData,
                            totalCleanupErrorCnt.incrementAndGet() - totalCleanupErrorCntStart);
                }
            }
        }

        try {
            producer.flushAsync().get(OWNERSHIP_CLEAN_UP_MAX_WAIT_TIME_IN_MILLIS, MILLISECONDS);
        } catch (Exception e) {
            log.error("Failed to flush the in-flight messages.", e);
        }

        boolean cleaned = false;
        if (serviceUnitTombstoneCleanupCnt > 0) {
            this.totalServiceUnitTombstoneCleanupCnt += serviceUnitTombstoneCleanupCnt;
            cleaned = true;
        }

        if (orphanServiceUnitCleanupCnt > 0) {
            this.totalOrphanServiceUnitCleanupCnt += orphanServiceUnitCleanupCnt;
            cleaned = true;
        }

        if (debug || cleaned) {
            double monitorTime = TimeUnit.NANOSECONDS
                    .toMillis((System.nanoTime() - startTime));
            log.info("Completed the ownership monitor run in {} ms. "
                            + "Scheduled cleanups for inactive brokers:{}. inactiveBrokerCount:{}. "
                            + "Published cleanups for orphan service units, orphanServiceUnitCleanupCnt:{}. "
                            + "Tombstoned semi-terminal state service units, serviceUnitTombstoneCleanupCnt:{}. "
                            + "Approximate cleanupErrorCnt:{}, metrics:{}. ",
                    monitorTime,
                    inactiveBrokers, inactiveBrokers.size(),
                    orphanServiceUnitCleanupCnt,
                    serviceUnitTombstoneCleanupCnt,
                    totalCleanupErrorCnt.get() - totalCleanupErrorCntStart,
                    printCleanupMetrics());
        }

    }

    private String printCleanupMetrics() {
        return String.format(
                "{totalInactiveBrokerCleanupCnt:%d, "
                        + "totalServiceUnitTombstoneCleanupCnt:%d, totalOrphanServiceUnitCleanupCnt:%d, "
                        + "totalCleanupErrorCnt:%d, "
                        + "totalInactiveBrokerCleanupScheduledCnt%d, totalInactiveBrokerCleanupIgnoredCnt:%d, "
                        + "totalInactiveBrokerCleanupCancelledCnt:%d, "
                        + "  activeCleanupJobs:%d}",
                totalInactiveBrokerCleanupCnt,
                totalServiceUnitTombstoneCleanupCnt,
                totalOrphanServiceUnitCleanupCnt,
                totalCleanupErrorCnt.get(),
                totalInactiveBrokerCleanupScheduledCnt,
                totalInactiveBrokerCleanupIgnoredCnt,
                totalInactiveBrokerCleanupCancelledCnt,
                cleanupJobs.size()
        );
    }

    private int getTotalOwnedServiceUnitCnt() {
        if (tableview == null) {
            return 0;
        }
        long now = System.currentTimeMillis();
        if (lastOwnEventHandledAt > lastOwnedServiceUnitCountAt
                || now - lastOwnedServiceUnitCountAt > MAX_OWNED_BUNDLE_COUNT_DELAY_TIME_IN_MILLIS) {
            int cnt = 0;
            for (var data : tableview.values()) {
                if (data.state() == Owned && isTargetBroker(data.dstBroker())) {
                    cnt++;
                }
            }
            lastOwnedServiceUnitCountAt = now;
            totalOwnedServiceUnitCnt = cnt;
        }
        return totalOwnedServiceUnitCnt;
    }


    @Override
    public List<Metrics> getMetrics() {
        var metrics = new ArrayList<Metrics>();
        var dimensions = new HashMap<String, String>();
        dimensions.put("metric", "sunitStateChn");
        dimensions.put("broker", pulsar.getAdvertisedAddress());

        for (var etr : ownerLookUpCounters.entrySet()) {
            {
                var dim = new HashMap<>(dimensions);
                dim.put("state", etr.getKey().toString());
                dim.put("result", "Total");
                var metric = Metrics.create(dim);
                metric.put("brk_sunit_state_chn_owner_lookup_total",
                        etr.getValue().getTotal().get());
                metrics.add(metric);
            }

            {
                var dim = new HashMap<>(dimensions);
                dim.put("state", etr.getKey().toString());
                dim.put("result", "Failure");
                var metric = Metrics.create(dim);
                metric.put("brk_sunit_state_chn_owner_lookup_total",
                        etr.getValue().getFailure().get());
                metrics.add(metric);
            }
        }

        for (var etr : eventCounters.entrySet()) {
            {
                var dim = new HashMap<>(dimensions);
                dim.put("event", etr.getKey().toString());
                dim.put("result", "Total");
                var metric = Metrics.create(dim);
                metric.put("brk_sunit_state_chn_event_publish_ops_total",
                        etr.getValue().getTotal().get());
                metrics.add(metric);
            }

            {
                var dim = new HashMap<>(dimensions);
                dim.put("event", etr.getKey().toString());
                dim.put("result", "Failure");
                var metric = Metrics.create(dim);
                metric.put("brk_sunit_state_chn_event_publish_ops_total",
                        etr.getValue().getFailure().get());
                metrics.add(metric);
            }
        }

        for (var etr : handlerCounters.entrySet()) {
            {
                var dim = new HashMap<>(dimensions);
                dim.put("event", etr.getKey().toString());
                dim.put("result", "Total");
                var metric = Metrics.create(dim);
                metric.put("brk_sunit_state_chn_subscribe_ops_total",
                        etr.getValue().getTotal().get());
                metrics.add(metric);
            }

            {
                var dim = new HashMap<>(dimensions);
                dim.put("event", etr.getKey().toString());
                dim.put("result", "Failure");
                var metric = Metrics.create(dim);
                metric.put("brk_sunit_state_chn_subscribe_ops_total",
                        etr.getValue().getFailure().get());
                metrics.add(metric);
            }
        }

        {
            var dim = new HashMap<>(dimensions);
            dim.put("result", "Failure");
            var metric = Metrics.create(dim);
            metric.put("brk_sunit_state_chn_cleanup_ops_total", totalCleanupErrorCnt.get());
            metrics.add(metric);
        }

        {
            var dim = new HashMap<>(dimensions);
            dim.put("result", "Skip");
            var metric = Metrics.create(dim);
            metric.put("brk_sunit_state_chn_inactive_broker_cleanup_ops_total", totalInactiveBrokerCleanupIgnoredCnt);
            metrics.add(metric);
        }

        {
            var dim = new HashMap<>(dimensions);
            dim.put("result", "Cancel");
            var metric = Metrics.create(dim);
            metric.put("brk_sunit_state_chn_inactive_broker_cleanup_ops_total", totalInactiveBrokerCleanupCancelledCnt);
            metrics.add(metric);
        }

        {
            var dim = new HashMap<>(dimensions);
            dim.put("result", "Schedule");
            var metric = Metrics.create(dim);
            metric.put("brk_sunit_state_chn_inactive_broker_cleanup_ops_total", totalInactiveBrokerCleanupScheduledCnt);
            metrics.add(metric);
        }

        {
            var dim = new HashMap<>(dimensions);
            dim.put("result", "Success");
            var metric = Metrics.create(dim);
            metric.put("brk_sunit_state_chn_inactive_broker_cleanup_ops_total", totalInactiveBrokerCleanupCnt);
            metrics.add(metric);
        }

        var metric = Metrics.create(dimensions);

        metric.put("brk_sunit_state_chn_orphan_su_cleanup_ops_total", totalOrphanServiceUnitCleanupCnt);
        metric.put("brk_sunit_state_chn_su_tombstone_cleanup_ops_total", totalServiceUnitTombstoneCleanupCnt);
        metric.put("brk_sunit_state_chn_owned_su_total", getTotalOwnedServiceUnitCnt());
        metrics.add(metric);

        return metrics;
    }

    @Override
    public void listen(StateChangeListener listener) {
        this.stateChangeListeners.addListener(listener);

    }

    @Override
    public Set<Map.Entry<String, ServiceUnitStateData>> getOwnershipEntrySet() {
        return tableview.entrySet();
    }

    public static ServiceUnitStateChannel get(PulsarService pulsar) {
        return ExtensibleLoadManagerImpl.get(pulsar.getLoadManager().get()).getServiceUnitStateChannel();
    }
}
