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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Released;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.Closed;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.Constructed;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.LeaderElectionServiceStarted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.ChannelState.Started;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MetadataState.Jittery;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MetadataState.Stable;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MetadataState.Unstable;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionReestablished;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@Slf4j
public class ServiceUnitStateChannelImpl implements ServiceUnitStateChannel {
    public static final String TOPIC = TopicName.get(
            TopicDomain.persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE,
            "loadbalancer-service-unit-state").toString();

    // TODO: define StateCompactionStrategy
    private static final long MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS = 30 * 1000; // 30sec
    public static final long MAX_CLEAN_UP_DELAY_TIME_IN_SECS = 3 * 60; // 3 mins
    private static final long MIN_CLEAN_UP_DELAY_TIME_IN_SECS = 0; // 0 secs to clean immediately

    private static final long MAX_CHANNEL_OWNER_ELECTION_WAITING_TIME_IN_SECS = 10;
    private static final int MAX_OUTSTANDING_PUB_MESSAGES = 500;
    private final PulsarService pulsar;
    private final Schema<ServiceUnitStateData> schema;
    private final ConcurrentOpenHashMap<String, CompletableFuture<String>> getOwnerRequests;
    private final String lookupServiceAddress;
    // TODO: define BrokerRegistry
    private final ConcurrentOpenHashMap<String, CompletableFuture<Void>> cleanupJobs;
    private final LeaderElectionService leaderElectionService;
    private TableView<ServiceUnitStateData> tableview;
    private Producer<ServiceUnitStateData> producer;
    private ScheduledFuture<?> cleanupTasks;
    private SessionEvent lastMetadataSessionEvent = SessionReestablished;
    private long lastMetadataSessionEventTimestamp = 0;
    private long inFlightStateWaitingTimeInMillis;
    private long maxCleanupDelayTimeInSecs;
    private long minCleanupDelayTimeInSecs;
    // cleanup metrics
    private long totalCleanupCnt = 0;
    private long totalBrokerCleanupTombstoneCnt = 0;
    private long totalServiceUnitCleanupTombstoneCnt = 0;
    private long totalServiceUnitCleanupErrorCnt = 0;
    private long totalCleanupScheduledCnt = 0;
    private long totalCleanupIgnoredCnt = 0;
    private long totalCleanupCancelledCnt = 0;
    private volatile ChannelState channelState;

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

    public ServiceUnitStateChannelImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.lookupServiceAddress = pulsar.getLookupServiceAddress();
        this.schema = Schema.JSON(ServiceUnitStateData.class);
        this.getOwnerRequests = ConcurrentOpenHashMap.<String,
                CompletableFuture<String>>newBuilder().build();
        this.cleanupJobs = ConcurrentOpenHashMap.<String, CompletableFuture<Void>>newBuilder().build();
        this.inFlightStateWaitingTimeInMillis = MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS;
        this.maxCleanupDelayTimeInSecs = MAX_CLEAN_UP_DELAY_TIME_IN_SECS;
        this.minCleanupDelayTimeInSecs = MIN_CLEAN_UP_DELAY_TIME_IN_SECS;
        this.leaderElectionService = new LeaderElectionService(
                pulsar.getCoordinationService(), pulsar.getSafeWebServiceAddress(),
                state -> {
                    if (state == LeaderElectionState.Leading) {
                        log.debug("This broker:{} is the leader now.", lookupServiceAddress);
                        // TODO: schedule monitorOwnerships by brokerRegistry
                    } else {
                        log.debug("This broker:{} is a follower now.", lookupServiceAddress);
                        // TODO: cancel scheduled monitorOwnerships if any
                    }
                });
        this.channelState = Constructed;
    }

    public synchronized void start() throws PulsarServerException {
        validateChannelState(LeaderElectionServiceStarted, false);
        try {
            leaderElectionService.start();
            this.channelState = LeaderElectionServiceStarted;
            log.debug("Successfully started the channel leader election service.");

            if (producer != null) {
                producer.close();
                log.debug("Closed the channel producer.");
            }
            producer = pulsar.getClient().newProducer(schema)
                    .enableBatching(true)
                    .maxPendingMessages(MAX_OUTSTANDING_PUB_MESSAGES)
                    .blockIfQueueFull(true)
                    .topic(TOPIC)
                    .create();

            log.debug("Successfully started the channel producer.");

            if (tableview != null) {
                tableview.close();
                log.debug("Closed the channel tableview.");
            }
            tableview = pulsar.getClient().newTableViewBuilder(schema)
                    .topic(TOPIC)
                    // TODO: enable CompactionStrategy
                    .create();
            // TODO: schedule listen instead of foreachAndListen
            tableview.forEachAndListen((key, value) -> handle(key, value));
            log.debug("Successfully started the channel tableview.");

            pulsar.getLocalMetadataStore().registerSessionListener(this::handleMetadataSessionEvent);
            log.debug("Successfully registered the handleMetadataSessionEvent");

            channelState = Started;
            log.info("Successfully started the channel.");
        } catch (Exception e) {
            String msg = "Failed to start the channel.";
            log.error(msg, e);
            throw new PulsarServerException(msg, e);
        }
    }

    public synchronized void close() throws PulsarServerException {
        channelState = Closed;
        try {
            leaderElectionService.close();
            log.debug("Successfully closed the channel leader election service.");

            if (tableview != null) {
                tableview.close();
                tableview = null;
                log.debug("Successfully closed the channel tableview.");
            }

            if (producer != null) {
                producer.close();
                producer = null;
                log.info("Successfully closed the channel producer.");
            }

            // TODO: clean brokerRegistry

            if (cleanupTasks != null) {
                cleanupTasks.cancel(true);
                cleanupTasks = null;
                log.info("Successfully cancelled the cleanup tasks");
            }

            log.info("Successfully closed the channel.");

        } catch (Exception e) {
            String msg = "Failed to close the channel.";
            log.error(msg, e);
            throw new PulsarServerException(msg, e);
        }
    }

    private void validateChannelState(ChannelState targetState, boolean checkLowerIds) {
        int order = checkLowerIds ? -1 : 1;
        if (Integer.compare(channelState.id, targetState.id) * order > 0) {
            throw new IllegalStateException("Invalid channel state:" + channelState.name());
        }
    }

    public CompletableFuture<Optional<String>> getChannelOwnerAsync() {
        validateChannelState(LeaderElectionServiceStarted, true);
        return leaderElectionService.readCurrentLeader().thenApply(leader -> {
                    //expecting http://broker-xyz:port
                    // TODO: discard this protocol prefix removal
                    //  by a util func that returns lookupServiceAddress(serviceUrl)
                    if (leader.isPresent()) {
                        String broker = leader.get().getServiceUrl();
                        broker = broker.substring(broker.lastIndexOf('/') + 1);
                        return Optional.of(broker);
                    } else {
                        return Optional.empty();
                    }
                }
        );
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

    private boolean isChannelOwner() {
        try {
            return isChannelOwnerAsync().get(
                    MAX_CHANNEL_OWNER_ELECTION_WAITING_TIME_IN_SECS, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String msg = "Failed to get the channel owner.";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public CompletableFuture<String> getOwnerAsync(String serviceUnit) {
        validateChannelState(Started, true);
        ServiceUnitStateData data = tableview.get(serviceUnit);
        if (data == null) {
            return CompletableFuture.completedFuture(null);
        }
        switch (data.state()) {
            case Owned, Splitting -> {
                return CompletableFuture.completedFuture(data.broker());
            }
            case Assigned, Released -> {
                return deferGetOwnerRequest(serviceUnit);
            }
            default -> {
                return null;
            }
        }
    }

    public CompletableFuture<String> publishAssignEventAsync(String serviceUnit, String broker) {
        CompletableFuture<String> getOwnerRequest = deferGetOwnerRequest(serviceUnit);
        pubAsync(serviceUnit, new ServiceUnitStateData(Assigned, broker))
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        getOwnerRequests.remove(serviceUnit, getOwnerRequest);
                        if (!getOwnerRequest.isCompletedExceptionally()) {
                            getOwnerRequest.completeExceptionally(ex);
                        }
                    }
                });

        return getOwnerRequest;
    }

    public CompletableFuture<Void> publishUnloadEventAsync(Unload unload) {
        String serviceUnit = unload.serviceUnit();
        if (isTransferCommand(unload)) {
            ServiceUnitStateData next = new ServiceUnitStateData(Assigned,
                    unload.destBroker().get(), unload.sourceBroker());
            return pubAsync(serviceUnit, next).thenApply(__ -> null);
        }
        return tombstoneAsync(serviceUnit).thenApply(__ -> null);
    }

    public CompletableFuture<Void> publishSplitEventAsync(Split split) {
        String serviceUnit = split.serviceUnit();
        ServiceUnitStateData next = new ServiceUnitStateData(Splitting, split.sourceBroker());
        return pubAsync(serviceUnit, next).thenApply(__ -> null);
    }

    private void handle(String serviceUnit, ServiceUnitStateData data) {
        if (log.isDebugEnabled()) {
            log.info("{} received a handle request for serviceUnit:{}, data:{}",
                    lookupServiceAddress, serviceUnit, data);
        }

        ServiceUnitState state = data == null ? Free : data.state();

        // TODO : Add state validation in tableview by the compaction strategy
        switch (state) {
            case Owned -> handleOwnEvent(serviceUnit, data);
            case Assigned -> handleAssignEvent(serviceUnit, data);
            case Released -> handleReleaseEvent(serviceUnit, data);
            case Splitting -> handleSplitEvent(serviceUnit, data);
            case Free -> handleFreeEvent(serviceUnit);
            default -> throw new IllegalStateException("Failed to handle channel data:" + data);
        }
    }

    private static boolean isTransferCommand(ServiceUnitStateData data) {
        if (data == null) {
            return false;
        }
        return StringUtils.isNotEmpty(data.sourceBroker());
    }

    private static boolean isTransferCommand(Unload data) {
        return data.destBroker().isPresent();
    }

    private static String getLogEventTag(ServiceUnitStateData data) {
        return data == null ? "Free" :
                isTransferCommand(data) ? "Transfer:" + data.state() : data.state().toString();
    }

    private void log(Throwable e, String serviceUnit, ServiceUnitStateData data, ServiceUnitStateData next) {
        if (e == null) {
            if (log.isDebugEnabled() || isTransferCommand(data)) {
                log.info("{} handled {} event for serviceUnit:{}, cur:{}, next:{}",
                        lookupServiceAddress, getLogEventTag(data), serviceUnit,
                        data == null ? "" : data,
                        next == null ? "" : next);
            }
        } else {
            log.error("{} failed to handle {} event for serviceUnit:{}, cur:{}, next:{}",
                    lookupServiceAddress, getLogEventTag(data), serviceUnit,
                    data == null ? "" : data,
                    next == null ? "" : next,
                    e);
        }
    }

    private void handleOwnEvent(String serviceUnit, ServiceUnitStateData data) {
        var getOwnerRequest = getOwnerRequests.remove(serviceUnit);
        if (getOwnerRequest != null) {
            getOwnerRequest.complete(data.broker());
        }
        if (isTargetBroker(data.broker())) {
            log(null, serviceUnit, data, null);
        }

    }

    private void handleAssignEvent(String serviceUnit, ServiceUnitStateData data) {
        deferGetOwnerRequest(serviceUnit);
        if (isTargetBroker(data.broker())) {
            ServiceUnitStateData next = new ServiceUnitStateData(
                    isTransferCommand(data) ? Released : Owned, data.broker(), data.sourceBroker());
            pubAsync(serviceUnit, next)
                    .whenComplete((__, e) -> log(e, serviceUnit, data, next));
        }
    }

    private void handleReleaseEvent(String serviceUnit, ServiceUnitStateData data) {

        if (isTargetBroker(data.sourceBroker())) {
            ServiceUnitStateData next = new ServiceUnitStateData(Owned, data.broker(), data.sourceBroker());
            // TODO: when close, pass message to clients to connect to the new broker
            closeServiceUnit(serviceUnit)
                    .thenCompose(__ -> pubAsync(serviceUnit, next))
                    .whenComplete((__, e) -> log(e, serviceUnit, data, next));
        }
    }

    private void handleSplitEvent(String serviceUnit, ServiceUnitStateData data) {
        if (isTargetBroker(data.broker())) {
            splitServiceUnit(serviceUnit)
                    .thenCompose(__ -> tombstoneAsync(serviceUnit))
                    .whenComplete((__, e) -> log(e, serviceUnit, data, null));
        }
    }

    private void handleFreeEvent(String serviceUnit) {
        closeServiceUnit(serviceUnit)
                .thenAccept(__ -> {
                    var request = getOwnerRequests.remove(serviceUnit);
                    if (request != null) {
                        request.completeExceptionally(new IllegalStateException("The ownership has been unloaded. "
                                + "No owner is found for serviceUnit: " + serviceUnit));
                    }
                })
                .whenComplete((__, e) -> log(e, serviceUnit, null, null));
    }

    private CompletableFuture<MessageId> pubAsync(String serviceUnit, ServiceUnitStateData data) {
        validateChannelState(Started, true);
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
        return broker.equals(lookupServiceAddress);
    }

    private NamespaceBundle getNamespaceBundle(String bundle) {
        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        return pulsar.getNamespaceService().getNamespaceBundleFactory().getBundle(namespaceName, bundleRange);
    }

    private CompletableFuture<String> deferGetOwnerRequest(String serviceUnit) {
        return getOwnerRequests
                .computeIfAbsent(serviceUnit, k -> {
                    CompletableFuture<String> future = new CompletableFuture<>();
                    future.orTimeout(inFlightStateWaitingTimeInMillis, TimeUnit.MILLISECONDS)
                            .whenComplete((v, e) -> {
                                        if (e != null) {
                                            getOwnerRequests.remove(serviceUnit, future);
                                            log.warn("Failed to getOwner for serviceUnit:{}",
                                                    serviceUnit, e);
                                        }
                                    }
                            );
                    return future;
                });
    }

    private CompletableFuture<Integer> closeServiceUnit(String serviceUnit) {
        long startTime = System.nanoTime();
        MutableInt unloadedTopics = new MutableInt();
        NamespaceBundle bundle = getNamespaceBundle(serviceUnit);
        return pulsar.getBrokerService().unloadServiceUnit(
                        bundle,
                        false,
                        pulsar.getConfig().getNamespaceBundleUnloadingTimeoutMs(),
                        TimeUnit.MILLISECONDS)
                .thenApply(numUnloadedTopics -> {
                    unloadedTopics.setValue(numUnloadedTopics);
                    return numUnloadedTopics;
                })
                .whenComplete((__, ex) -> {
                    // clean up topics that failed to unload from the broker ownership cache
                    pulsar.getBrokerService().cleanUnloadedTopicFromCache(bundle);
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

    private CompletableFuture<Void> splitServiceUnit(String serviceUnit) {
        // TODO: after the split we need to write the child ownerships to BSC instead of ZK.
        long startTime = System.nanoTime();
        return pulsar.getNamespaceService()
                .splitAndOwnBundle(getNamespaceBundle(serviceUnit),
                        false,
                        NamespaceBundleSplitAlgorithm.of(pulsar.getConfig().getDefaultNamespaceBundleSplitAlgorithm()),
                        null)
                .whenComplete((__, ex) -> {
                    double splitBundleTime = TimeUnit.NANOSECONDS
                            .toMillis((System.nanoTime() - startTime));
                    if (ex == null) {
                        log.info("Successfully split {} namespace-bundle in {} ms",
                                serviceUnit, splitBundleTime);
                    } else {
                        log.error("Failed to split {} namespace-bundle in {} ms",
                                serviceUnit, splitBundleTime, ex);
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
            handleBrokerCreationEvent(broker);
        } else if (type == NotificationType.Deleted) {
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
            totalCleanupCancelledCnt++;
            log.info("Successfully cancelled the ownership cleanup for broker:{}."
                            + " Active cleanup job count:{}",
                    broker, cleanupJobs.size());
        } else {
            log.info("Failed to cancel the ownership cleanup for broker:{}."
                            + " There was no scheduled cleanup job. Active cleanup job count:{}",
                    broker, cleanupJobs.size());
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
                totalCleanupIgnoredCnt++;
                log.error("MetadataState state is unstable. "
                        + "Ignoring the ownership cleanup request for the reported broker :{}", broker);
            }
        }
    }

    private void scheduleCleanup(String broker, long delayInSecs) {
        cleanupJobs.computeIfAbsent(broker, k -> {
            Executor delayed = CompletableFuture
                    .delayedExecutor(delayInSecs, TimeUnit.SECONDS, pulsar.getLoadManagerExecutor());
            totalCleanupScheduledCnt++;
            return CompletableFuture
                    .runAsync(() -> doCleanup(broker), delayed);
        });

        log.info("Scheduled ownership cleanup for broker:{} with delay:{} secs. Pending clean jobs:{}.",
                broker, delayInSecs, cleanupJobs.size());
    }


    private void doCleanup(String broker) {
        long startTime = System.nanoTime();
        log.info("Started ownership cleanup for the inactive broker:{}", broker);
        AtomicInteger serviceUnitTombstoneCnt = new AtomicInteger();
        AtomicInteger serviceUnitTombstoneErrorCnt = new AtomicInteger();
        for (Map.Entry<String, ServiceUnitStateData> etr : tableview.entrySet()) {
            ServiceUnitStateData stateData = etr.getValue();
            String serviceUnit = etr.getKey();
            if (StringUtils.equals(broker, stateData.broker())
                    || StringUtils.equals(broker, stateData.sourceBroker())) {
                log.info("Cleaning ownership serviceUnit:{}, stateData:{}.", serviceUnit, stateData);
                tombstoneAsync(serviceUnit).whenComplete((__, e) -> {
                    if (e == null) {
                        serviceUnitTombstoneCnt.incrementAndGet();
                    } else {
                        log.error("Failed cleaning the ownership serviceUnit:{}, stateData:{}.",
                                serviceUnit, stateData);
                        serviceUnitTombstoneErrorCnt.incrementAndGet();
                    }
                });
            }
        }

        try {
            producer.flush();
        } catch (PulsarClientException e) {
            log.error("Failed to flush the in-flight messages.", e);
        }

        if (serviceUnitTombstoneCnt.get() > 0) {
            this.totalCleanupCnt++;
            this.totalServiceUnitCleanupTombstoneCnt += serviceUnitTombstoneCnt.get();
            this.totalBrokerCleanupTombstoneCnt++;
        }

        if (serviceUnitTombstoneErrorCnt.get() > 0) {
            this.totalServiceUnitCleanupErrorCnt += serviceUnitTombstoneErrorCnt.get();
        }

        double cleanupTime = TimeUnit.NANOSECONDS
                .toMillis((System.nanoTime() - startTime));
        // TODO: clean load data stores
        log.info("Completed a cleanup for the inactive broker:{} in {} ms. "
                        + "Published tombstone for orphan service units: serviceUnitTombstoneCnt:{}, "
                        + "serviceUnitTombstoneErrorCnt:{}, metrics:{} ",
                broker,
                cleanupTime,
                serviceUnitTombstoneCnt,
                serviceUnitTombstoneErrorCnt,
                printCleanupMetrics());
        cleanupJobs.remove(broker);
    }

    // TODO: integrate this monitor logic when broker registry is added
    private void monitorOwnerships(List<String> brokers) {
        if (!isChannelOwner()) {
            log.warn("This broker is not the leader now. Skipping ownership monitor");
            return;
        }
        if (brokers == null || brokers.size() == 0) {
            log.error("no active brokers found. Skipping the ownership monitor run.");
            return;
        }

        log.info("Started the ownership monitor run for activeBrokerCount:{}", brokers.size());
        long startTime = System.nanoTime();
        Set<String> inactiveBrokers = new HashSet<>();
        Set<String> activeBrokers = new HashSet<>(brokers);
        AtomicInteger serviceUnitTombstoneCnt = new AtomicInteger();
        AtomicInteger serviceUnitTombstoneErrorCnt = new AtomicInteger();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ServiceUnitStateData> etr : tableview.entrySet()) {
            String serviceUnit = etr.getKey();
            ServiceUnitStateData stateData = etr.getValue();
            String broker = stateData.broker();
            if (!activeBrokers.contains(broker)) {
                inactiveBrokers.add(stateData.broker());
            } else if (stateData.state() != Owned
                    && now - stateData.timestamp() > inFlightStateWaitingTimeInMillis) {
                log.warn("Found long-running orphan(in-flight) serviceUnit:{}, stateData:{}",
                        serviceUnit, stateData);

                tombstoneAsync(serviceUnit).whenComplete((__, e) -> {
                    if (e == null) {
                        serviceUnitTombstoneCnt.incrementAndGet();
                    } else {
                        log.error("Failed cleaning the ownership serviceUnit:{}, stateData:{}.",
                                serviceUnit, stateData);
                        serviceUnitTombstoneErrorCnt.incrementAndGet();
                    }
                });
            }
        }

        for (String inactiveBroker : inactiveBrokers) {
            handleBrokerDeletionEvent(inactiveBroker);
        }

        try {
            producer.flush();
        } catch (PulsarClientException e) {
            log.error("Failed to flush the in-flight messages.", e);
        }

        if (serviceUnitTombstoneCnt.get() > 0) {
            this.totalServiceUnitCleanupTombstoneCnt += serviceUnitTombstoneCnt.get();
        }
        this.totalServiceUnitCleanupErrorCnt += serviceUnitTombstoneErrorCnt.get();

        double monitorTime = TimeUnit.NANOSECONDS
                .toMillis((System.nanoTime() - startTime));
        log.info("Completed the ownership monitor run in {} ms. "
                        + "Scheduled cleanups for inactiveBrokers:{}. inactiveBrokerCount:{}. "
                        + "Published tombstone for orphan service units: serviceUnitTombstoneCnt:{}, "
                        + "serviceUnitTombstoneErrorCnt:{}, metrics:{} ",
                monitorTime,
                inactiveBrokers,
                inactiveBrokers.size(),
                serviceUnitTombstoneCnt,
                serviceUnitTombstoneErrorCnt,
                printCleanupMetrics());

    }

    private String printCleanupMetrics() {
        return String.format(
                "{totalCleanupCnt:%d, totalBrokerCleanupTombstoneCnt:%d, "
                        + "totalServiceUnitCleanupTombstoneCnt:%d, totalServiceUnitCleanupErrorCnt:%d, "
                        + "totalCleanupScheduledCnt%d, totalCleanupIgnoredCnt:%d, totalCleanupCancelledCnt:%d, "
                        + "  activeCleanupJobs:%d}",
                totalCleanupCnt,
                totalBrokerCleanupTombstoneCnt,
                totalServiceUnitCleanupTombstoneCnt,
                totalServiceUnitCleanupErrorCnt,
                totalCleanupScheduledCnt,
                totalCleanupIgnoredCnt,
                totalCleanupCancelledCnt,
                cleanupJobs.size()
        );
    }

}
