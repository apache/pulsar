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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;
import static org.apache.pulsar.broker.web.PulsarWebResource.joinPath;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerLoader;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.zookeeper.aspectj.ClientCnxnAspect;
import org.apache.pulsar.broker.zookeeper.aspectj.ClientCnxnAspect.EventListner;
import org.apache.pulsar.broker.zookeeper.aspectj.ClientCnxnAspect.EventType;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FieldParser;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.zookeeper.ZooKeeperCacheListener;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerService implements Closeable, ZooKeeperCacheListener<Policies> {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    private final PulsarService pulsar;
    private final ManagedLedgerFactory managedLedgerFactory;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics;

    private final ConcurrentOpenHashMap<String, PulsarClient> replicationClients;

    // Multi-layer topics map:
    // Namespace --> Bundle --> topicName --> topic
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>>> multiLayerTopicsMap;
    private int numberOfNamespaceBundles = 0;

    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;
    private final OrderedExecutor topicOrderedExecutor;
    // offline topic backlog cache
    private final ConcurrentOpenHashMap<TopicName, PersistentOfflineTopicStats> offlineTopicStatCache;
    private static final ConcurrentOpenHashMap<String, ConfigField> dynamicConfigurationMap = prepareDynamicConfigurationMap();
    private final ConcurrentOpenHashMap<String, Consumer<?>> configRegisteredListeners;

    private final ConcurrentLinkedQueue<Pair<String, CompletableFuture<Optional<Topic>>>> pendingTopicLoadingQueue;

    private AuthorizationService authorizationService = null;
    private final ScheduledExecutorService statsUpdater;
    private final ScheduledExecutorService backlogQuotaChecker;

    protected final AtomicReference<Semaphore> lookupRequestSemaphore;
    protected final AtomicReference<Semaphore> topicLoadRequestSemaphore;

    private final ScheduledExecutorService inactivityMonitor;
    private final ScheduledExecutorService messageExpiryMonitor;
    private final ScheduledExecutorService compactionMonitor;

    private DistributedIdGenerator producerNameGenerator;

    private final static String producerNameGeneratorPath = "/counters/producer-name";

    private final BacklogQuotaManager backlogQuotaManager;

    private final int keepAliveIntervalSeconds;
    private final PulsarStats pulsarStats;
    private final EventListner zkStatsListener;
    private final AuthenticationService authenticationService;

    public static final String BROKER_SERVICE_CONFIGURATION_PATH = "/admin/configuration";
    private final ZooKeeperDataCache<Map<String, String>> dynamicConfigurationCache;

    private static final LongAdder totalUnackedMessages = new LongAdder();
    private final int maxUnackedMessages;
    public final int maxUnackedMsgsPerDispatcher;
    private static final AtomicBoolean blockedDispatcherOnHighUnackedMsgs = new AtomicBoolean(false);
    private final ConcurrentOpenHashSet<PersistentDispatcherMultipleConsumers> blockedDispatchers;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final DelayedDeliveryTrackerFactory delayedDeliveryTrackerFactory;

    public BrokerService(PulsarService pulsar) throws Exception {
        this.pulsar = pulsar;
        this.managedLedgerFactory = pulsar.getManagedLedgerFactory();
        this.topics = new ConcurrentOpenHashMap<>();
        this.replicationClients = new ConcurrentOpenHashMap<>();
        this.keepAliveIntervalSeconds = pulsar.getConfiguration().getKeepAliveIntervalSeconds();
        this.configRegisteredListeners = new ConcurrentOpenHashMap<>();
        this.pendingTopicLoadingQueue = Queues.newConcurrentLinkedQueue();

        this.multiLayerTopicsMap = new ConcurrentOpenHashMap<>();
        this.pulsarStats = new PulsarStats(pulsar);
        this.offlineTopicStatCache = new ConcurrentOpenHashMap<>();

        this.topicOrderedExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(pulsar.getConfiguration().getNumWorkerThreadsForNonPersistentTopic())
                .name("broker-topic-workers").build();
        final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-acceptor");
        final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-io");
        final int numThreads = pulsar.getConfiguration().getNumIOThreads();
        log.info("Using {} threads for broker service IO", numThreads);

        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);
        this.statsUpdater = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-stats-updater"));
        if (pulsar.getConfiguration().isAuthorizationEnabled()) {
            this.authorizationService = new AuthorizationService(pulsar.getConfiguration(),
                    pulsar.getConfigurationCache());
        }

        if (pulsar.getConfigurationCache() != null) {
            pulsar.getConfigurationCache().policiesCache().registerListener(this);
        }

        this.inactivityMonitor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-inactivity-monitor"));
        this.messageExpiryMonitor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-msg-expiry-monitor"));
        this.compactionMonitor =
            Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-compaction-monitor"));

        this.backlogQuotaManager = new BacklogQuotaManager(pulsar);
        this.backlogQuotaChecker = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-backlog-quota-checker"));
        this.authenticationService = new AuthenticationService(pulsar.getConfiguration());
        this.dynamicConfigurationCache = new ZooKeeperDataCache<Map<String, String>>(pulsar().getLocalZkCache()) {
            @Override
            public Map<String, String> deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, HashMap.class);
            }
        };
        this.blockedDispatchers = new ConcurrentOpenHashSet<>();
        // update dynamic configuration and register-listener
        updateConfigurationAndRegisterListeners();
        this.lookupRequestSemaphore = new AtomicReference<Semaphore>(
                new Semaphore(pulsar.getConfiguration().getMaxConcurrentLookupRequest(), false));
        this.topicLoadRequestSemaphore = new AtomicReference<Semaphore>(
                new Semaphore(pulsar.getConfiguration().getMaxConcurrentTopicLoadRequest(), false));
        if (pulsar.getConfiguration().getMaxUnackedMessagesPerBroker() > 0
                && pulsar.getConfiguration().getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked() > 0.0) {
            this.maxUnackedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerBroker();
            this.maxUnackedMsgsPerDispatcher = (int) ((maxUnackedMessages
                    * pulsar.getConfiguration().getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked()) / 100);
            log.info("Enabling per-broker unack-message limit {} and dispatcher-limit {} on blocked-broker",
                    maxUnackedMessages, maxUnackedMsgsPerDispatcher);
            // block misbehaving dispatcher by checking periodically
            pulsar.getExecutor().scheduleAtFixedRate(() -> checkUnAckMessageDispatching(), 600, 30, TimeUnit.SECONDS);
        } else {
            this.maxUnackedMessages = 0;
            this.maxUnackedMsgsPerDispatcher = 0;
            log.info(
                    "Disabling per broker unack-msg blocking due invalid unAckMsgSubscriptionPercentageLimitOnBrokerBlocked {} ",
                    pulsar.getConfiguration().getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked());
        }

        // register listener to capture zk-latency
        zkStatsListener = new EventListner() {
            @Override
            public void recordLatency(EventType eventType, long latencyMs) {
                pulsarStats.recordZkLatencyTimeValue(eventType, latencyMs);
            }
        };

        this.delayedDeliveryTrackerFactory = DelayedDeliveryTrackerLoader
                .loadDelayedDeliveryTrackerFactory(pulsar.getConfiguration());
    }

    public void start() throws Exception {
        this.producerNameGenerator = new DistributedIdGenerator(pulsar.getZkClient(), producerNameGeneratorPath,
                pulsar.getConfiguration().getClusterName());

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        ServiceConfiguration serviceConfig = pulsar.getConfiguration();

        bootstrap.childHandler(new PulsarChannelInitializer(pulsar, false));

        Optional<Integer> port = serviceConfig.getBrokerServicePort();
        if (port.isPresent()) {
            // Bind and start to accept incoming connections.
            InetSocketAddress addr = new InetSocketAddress(pulsar.getBindAddress(), port.get());
            try {
                bootstrap.bind(addr).sync();
            } catch (Exception e) {
                throw new IOException("Failed to bind Pulsar broker on " + addr, e);
            }
            log.info("Started Pulsar Broker service on port {}", port.get());
        }

        Optional<Integer> tlsPort = serviceConfig.getBrokerServicePortTls();
        if (tlsPort.isPresent()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new PulsarChannelInitializer(pulsar, true));
            tlsBootstrap.bind(new InetSocketAddress(pulsar.getBindAddress(), tlsPort.get())).sync();
            log.info("Started Pulsar Broker TLS service on port {} - TLS provider: {}", tlsPort.get(),
                    SslContext.defaultServerProvider());
        }

        // start other housekeeping functions
        this.startStatsUpdater(
                serviceConfig.getStatsUpdateInitialDelayInSecs(),
                serviceConfig.getStatsUpdateFrequencyInSecs());
        this.startInactivityMonitor();
        this.startMessageExpiryMonitor();
        this.startCompactionMonitor();
        this.startBacklogQuotaChecker();
        // register listener to capture zk-latency
        ClientCnxnAspect.addListener(zkStatsListener);
        ClientCnxnAspect.registerExecutor(pulsar.getExecutor());
    }

    void startStatsUpdater(int statsUpdateInitailDelayInSecs, int statsUpdateFrequencyInSecs) {
        statsUpdater.scheduleAtFixedRate(safeRun(this::updateRates),
                statsUpdateInitailDelayInSecs, statsUpdateFrequencyInSecs, TimeUnit.SECONDS);

        // Ensure the broker starts up with initial stats
        updateRates();
    }

    void startInactivityMonitor() {
        if (pulsar().getConfiguration().isBrokerDeleteInactiveTopicsEnabled()) {
            int interval = pulsar().getConfiguration().getBrokerServicePurgeInactiveFrequencyInSeconds();
            inactivityMonitor.scheduleAtFixedRate(safeRun(() -> checkGC(interval)), interval, interval,
                    TimeUnit.SECONDS);
        }

        // Deduplication info checker
        long duplicationCheckerIntervalInSeconds = TimeUnit.MINUTES
                .toSeconds(pulsar().getConfiguration().getBrokerDeduplicationProducerInactivityTimeoutMinutes()) / 3;
        inactivityMonitor.scheduleAtFixedRate(safeRun(this::checkMessageDeduplicationInfo), duplicationCheckerIntervalInSeconds,
                duplicationCheckerIntervalInSeconds, TimeUnit.SECONDS);

        // Inactive subscriber checker
        if (pulsar().getConfiguration().getSubscriptionExpirationTimeMinutes() > 0) {
            long subscriptionExpiryCheckIntervalInSeconds =
                    TimeUnit.MINUTES.toSeconds(pulsar().getConfiguration().getSubscriptionExpiryCheckIntervalInMinutes());
            inactivityMonitor.scheduleAtFixedRate(safeRun(this::checkInactiveSubscriptions),
                    subscriptionExpiryCheckIntervalInSeconds, subscriptionExpiryCheckIntervalInSeconds, TimeUnit.SECONDS);
        }
    }

    void startMessageExpiryMonitor() {
        int interval = pulsar().getConfiguration().getMessageExpiryCheckIntervalInMinutes();
        messageExpiryMonitor.scheduleAtFixedRate(safeRun(this::checkMessageExpiry), interval, interval,
                TimeUnit.MINUTES);
    }

    void startCompactionMonitor() {
        int interval = pulsar().getConfiguration().getBrokerServiceCompactionMonitorIntervalInSeconds();
        if (interval > 0) {
            compactionMonitor.scheduleAtFixedRate(safeRun(() -> checkCompaction()),
                                                  interval, interval, TimeUnit.SECONDS);
        }
    }

    void startBacklogQuotaChecker() {
        if (pulsar().getConfiguration().isBacklogQuotaCheckEnabled()) {
            final int interval = pulsar().getConfiguration().getBacklogQuotaCheckIntervalInSeconds();
            log.info("Scheduling a thread to check backlog quota after [{}] seconds in background", interval);
            backlogQuotaChecker.scheduleAtFixedRate(safeRun(this::monitorBacklogQuota), interval, interval,
                    TimeUnit.SECONDS);
        } else {
            log.info("Backlog quota check monitoring is disabled");
        }

    }

    @Override
    public void close() throws IOException {
        log.info("Shutting down Pulsar Broker service");

        if (pulsar.getConfigurationCache() != null) {
            pulsar.getConfigurationCache().policiesCache().unregisterListener(this);
        }

        // unloads all namespaces gracefully without disrupting mutually
        unloadNamespaceBundlesGracefully();

        // close replication clients
        replicationClients.forEach((cluster, client) -> {
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                log.warn("Error shutting down repl client for cluster {}", cluster, e);
            }
        });

        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        statsUpdater.shutdown();
        inactivityMonitor.shutdown();
        messageExpiryMonitor.shutdown();
        compactionMonitor.shutdown();
        backlogQuotaChecker.shutdown();
        authenticationService.close();
        pulsarStats.close();
        ClientCnxnAspect.removeListener(zkStatsListener);
        ClientCnxnAspect.registerExecutor(null);
        topicOrderedExecutor.shutdown();
        delayedDeliveryTrackerFactory.close();
        log.info("Broker service completely shut down");
    }

    /**
     * It unloads all owned namespacebundles gracefully.
     * <ul>
     * <li>First it makes current broker unavailable and isolates from the clusters so, it will not serve any new
     * requests.</li>
     * <li>Second it starts unloading namespace bundle one by one without closing the connection in order to avoid
     * disruption for other namespacebundles which are sharing the same connection from the same client.</li>
     * <ul>
     *
     */
    public void unloadNamespaceBundlesGracefully() {
        try {
            // make broker-node unavailable from the cluster
            if (pulsar.getLoadManager() != null && pulsar.getLoadManager().get() != null) {
                try {
                    pulsar.getLoadManager().get().disableBroker();
                } catch (PulsarServerException.NotFoundException ne) {
                    log.warn("Broker load-manager znode doesn't exist ", ne);
                    // still continue and release bundle ownership as broker's registration node doesn't exist.
                }
            }

            // unload all namespace-bundles gracefully
            long closeTopicsStartTime = System.nanoTime();
            Set<NamespaceBundle> serviceUnits = pulsar.getNamespaceService().getOwnedServiceUnits();
            serviceUnits.forEach(su -> {
                if (su instanceof NamespaceBundle) {
                    try {
                        pulsar.getNamespaceService().unloadNamespaceBundle((NamespaceBundle) su, 1, TimeUnit.MINUTES);
                    } catch (Exception e) {
                        log.warn("Failed to unload namespace bundle {}", su, e);
                    }
                }
            });

            double closeTopicsTimeSeconds = TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - closeTopicsStartTime))
                    / 1000.0;
            log.info("Unloading {} namespace-bundles completed in {} seconds", serviceUnits.size(),
                    closeTopicsTimeSeconds);
        } catch (Exception e) {
            log.error("Failed to disable broker from loadbalancer list {}", e.getMessage(), e);
        }
    }

    public CompletableFuture<Optional<Topic>> getTopicIfExists(final String topic) {
        return getTopic(topic, false /* createIfMissing */);
    }

    public CompletableFuture<Topic> getOrCreateTopic(final String topic) {
        return getTopic(topic, pulsar.getConfiguration().isAllowAutoTopicCreation()).thenApply(Optional::get);
    }

    public CompletableFuture<Optional<Topic>> getTopic(final String topic, boolean createIfMissing) {
        try {
            CompletableFuture<Optional<Topic>> topicFuture = topics.get(topic);
            if (topicFuture != null) {
                if (topicFuture.isCompletedExceptionally()
                        || (topicFuture.isDone() && !topicFuture.getNow(Optional.empty()).isPresent())) {
                    // Exceptional topics should be recreated.
                    topics.remove(topic, topicFuture);
                } else {
                    return topicFuture;
                }
            }
            final boolean isPersistentTopic = TopicName.get(topic).getDomain().equals(TopicDomain.persistent);
            return topics.computeIfAbsent(topic, (topicName) -> {
                    return isPersistentTopic ? this.loadOrCreatePersistentTopic(topicName, createIfMissing)
                        : createNonPersistentTopic(topicName);
            });
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Illegalargument exception when loading topic", topic, e);
            return failedFuture(e);
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ServiceUnitNotReadyException) {
                log.warn("[{}] Service unit is not ready when loading the topic", topic);
            } else {
                log.warn("[{}] Unexpected exception when loading topic: {}", topic, e.getMessage(), e);
            }

            return failedFuture(cause);
        }
    }

    private CompletableFuture<Optional<Topic>> createNonPersistentTopic(String topic) {
        CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();

        if (!pulsar.getConfiguration().isEnableNonPersistentTopics()) {
            if (log.isDebugEnabled()) {
                log.debug("Broker is unable to load non-persistent topic {}", topic);
            }
            topicFuture.completeExceptionally(
                    new NotAllowedException("Broker is not unable to load non-persistent topic"));
            return topicFuture;
        }
        final long topicCreateTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        NonPersistentTopic nonPersistentTopic = new NonPersistentTopic(topic, this);
        CompletableFuture<Void> replicationFuture = nonPersistentTopic.checkReplication();
        replicationFuture.thenRun(() -> {
            log.info("Created topic {}", nonPersistentTopic);
            long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - topicCreateTimeMs;
            pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
            addTopicToStatsMaps(TopicName.get(topic), nonPersistentTopic);
            topicFuture.complete(Optional.of(nonPersistentTopic));
        });
        replicationFuture.exceptionally((ex) -> {
            log.warn("Replication check failed. Removing topic from topics list {}, {}", topic, ex);
            nonPersistentTopic.stopReplProducers().whenComplete((v, exception) -> {
                pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                topicFuture.completeExceptionally(ex);
            });

            return null;
        });

        return topicFuture;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    public PulsarClient getReplicationClient(String cluster) {
        PulsarClient client = replicationClients.get(cluster);
        if (client != null) {
            return client;
        }

        return replicationClients.computeIfAbsent(cluster, key -> {
            try {
                String path = PulsarWebResource.path("clusters", cluster);
                ClusterData data = this.pulsar.getConfigurationCache().clustersCache().get(path)
                        .orElseThrow(() -> new KeeperException.NoNodeException(path));
                ClientBuilder clientBuilder = PulsarClient.builder()
                        .enableTcpNoDelay(false)
                        .connectionsPerBroker(pulsar.getConfiguration().getReplicationConnectionsPerBroker())
                        .statsInterval(0, TimeUnit.SECONDS);
                if (pulsar.getConfiguration().isAuthenticationEnabled()) {
                    clientBuilder.authentication(pulsar.getConfiguration().getBrokerClientAuthenticationPlugin(),
                            pulsar.getConfiguration().getBrokerClientAuthenticationParameters());
                }
                if (pulsar.getConfiguration().isBrokerClientTlsEnabled()) {
                    clientBuilder
                            .serviceUrl(isNotBlank(data.getBrokerServiceUrlTls()) ? data.getBrokerServiceUrlTls()
                                    : data.getServiceUrlTls())
                            .enableTls(true)
                            .tlsTrustCertsFilePath(pulsar.getConfiguration().getBrokerClientTrustCertsFilePath())
                            .allowTlsInsecureConnection(pulsar.getConfiguration().isTlsAllowInsecureConnection());
                } else {
                    clientBuilder.serviceUrl(
                            isNotBlank(data.getBrokerServiceUrl()) ? data.getBrokerServiceUrl() : data.getServiceUrl());
                }

                // Share all the IO threads across broker and client connections
                ClientConfigurationData conf = ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();
                return new PulsarClientImpl(conf, workerGroup);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * It creates a topic async and returns CompletableFuture. It also throttles down configured max-concurrent topic
     * loading and puts them into queue once in-process topics are created.
     *
     * @param topic persistent-topic name
     * @return CompletableFuture<Topic>
     * @throws RuntimeException
     */
    protected CompletableFuture<Optional<Topic>> loadOrCreatePersistentTopic(final String topic,
            boolean createIfMissing) throws RuntimeException {
        checkTopicNsOwnership(topic);

        final CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
        if (!pulsar.getConfiguration().isEnablePersistentTopics()) {
            if (log.isDebugEnabled()) {
                log.debug("Broker is unable to load persistent topic {}", topic);
            }
            topicFuture.completeExceptionally(new NotAllowedException("Broker is not unable to load persistent topic"));
            return topicFuture;
        }

        final Semaphore topicLoadSemaphore = topicLoadRequestSemaphore.get();

        if (topicLoadSemaphore.tryAcquire()) {
            createPersistentTopic(topic, createIfMissing, topicFuture);
            topicFuture.handle((persistentTopic, ex) -> {
                // release permit and process pending topic
                topicLoadSemaphore.release();
                createPendingLoadTopic();
                return null;
            });
        } else {
            pendingTopicLoadingQueue.add(new ImmutablePair<String, CompletableFuture<Optional<Topic>>>(topic, topicFuture));
            if (log.isDebugEnabled()) {
                log.debug("topic-loading for {} added into pending queue", topic);
            }
        }
        return topicFuture;
    }

    private void createPersistentTopic(final String topic, boolean createIfMissing,
    		CompletableFuture<Optional<Topic>> topicFuture) {

        final long topicCreateTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        TopicName topicName = TopicName.get(topic);
        if (!pulsar.getNamespaceService().isServiceUnitActive(topicName)) {
            // namespace is being unloaded
            String msg = String.format("Namespace is being unloaded, cannot add topic %s", topic);
            log.warn(msg);
            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
            topicFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
            return;
        }

        getManagedLedgerConfig(topicName).thenAccept(managedLedgerConfig -> {
            managedLedgerConfig.setCreateIfMissing(createIfMissing);

            // Once we have the configuration, we can proceed with the async open operation
            managedLedgerFactory.asyncOpen(topicName.getPersistenceNamingEncoding(), managedLedgerConfig,
                    new OpenLedgerCallback() {
                        @Override
                        public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                            try {
                                PersistentTopic persistentTopic = new PersistentTopic(topic, ledger,
                                        BrokerService.this);
                                CompletableFuture<Void> replicationFuture = persistentTopic.checkReplication();
                                replicationFuture.thenCompose(v -> {
                                    // Also check dedup status
                                    return persistentTopic.checkDeduplicationStatus();
                                }).thenRun(() -> {
                                    log.info("Created topic {} - dedup is {}", topic,
                                            persistentTopic.isDeduplicationEnabled() ? "enabled" : "disabled");
                                    long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
                                            - topicCreateTimeMs;
                                    pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
                                    addTopicToStatsMaps(topicName, persistentTopic);
                                    topicFuture.complete(Optional.of(persistentTopic));
                                }).exceptionally((ex) -> {
                                    log.warn(
                                            "Replication or dedup check failed. Removing topic from topics list {}, {}",
                                            topic, ex);
                                    persistentTopic.stopReplProducers().whenComplete((v, exception) -> {
                                        topics.remove(topic, topicFuture);
                                        topicFuture.completeExceptionally(ex);
                                    });

                                    return null;
                                });
                            } catch (NamingException e) {
                                log.warn("Failed to create topic {}-{}", topic, e.getMessage());
                                pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                                topicFuture.completeExceptionally(e);
                            }
                        }

                        @Override
                        public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            if (!createIfMissing && exception instanceof ManagedLedgerNotFoundException) {
                                // We were just trying to load a topic and the topic doesn't exist
                                topicFuture.complete(Optional.empty());
                            } else {
                                log.warn("Failed to create topic {}", topic, exception);
                                pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                                topicFuture.completeExceptionally(new PersistenceException(exception));
                            }
                        }
                    }, null);

        }).exceptionally((exception) -> {
            log.warn("[{}] Failed to get topic configuration: {}", topic, exception.getMessage(), exception);
            // remove topic from topics-map in different thread to avoid possible deadlock if
            // createPersistentTopic-thread only tries to handle this future-result
            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
            topicFuture.completeExceptionally(exception);
            return null;
        });
    }

    public CompletableFuture<ManagedLedgerConfig> getManagedLedgerConfig(TopicName topicName) {
        CompletableFuture<ManagedLedgerConfig> future = new CompletableFuture<>();
        // Execute in background thread, since getting the policies might block if the z-node wasn't already cached
        pulsar.getOrderedExecutor().executeOrdered(topicName, safeRun(() -> {
            NamespaceName namespace = topicName.getNamespaceObject();
            ServiceConfiguration serviceConfig = pulsar.getConfiguration();

            // Get persistence policy for this topic
            Optional<Policies> policies = Optional.empty();
            Optional<LocalPolicies> localPolicies = Optional.empty();
            try {
                policies = pulsar
                        .getConfigurationCache().policiesCache().get(AdminResource.path(POLICIES,
                                namespace.toString()));
                String path = joinPath(LOCAL_POLICIES_ROOT, topicName.getNamespaceObject().toString());
                localPolicies = pulsar().getLocalZkCacheService().policiesCache().get(path);
            } catch (Throwable t) {
                // Ignoring since if we don't have policies, we fallback on the default
                log.warn("Got exception when reading persistence policy for {}: {}", topicName, t.getMessage(), t);
                future.completeExceptionally(t);
                return;
            }

            PersistencePolicies persistencePolicies = policies.map(p -> p.persistence).orElseGet(
                    () -> new PersistencePolicies(serviceConfig.getManagedLedgerDefaultEnsembleSize(),
                            serviceConfig.getManagedLedgerDefaultWriteQuorum(),
                            serviceConfig.getManagedLedgerDefaultAckQuorum(),
                            serviceConfig.getManagedLedgerDefaultMarkDeleteRateLimit()));

            RetentionPolicies retentionPolicies = policies.map(p -> p.retention_policies).orElseGet(
                    () -> new RetentionPolicies(serviceConfig.getDefaultRetentionTimeInMinutes(),
                            serviceConfig.getDefaultRetentionSizeInMB())
            );

            ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
            managedLedgerConfig.setEnsembleSize(persistencePolicies.getBookkeeperEnsemble());
            managedLedgerConfig.setWriteQuorumSize(persistencePolicies.getBookkeeperWriteQuorum());
            managedLedgerConfig.setAckQuorumSize(persistencePolicies.getBookkeeperAckQuorum());
            if (localPolicies.isPresent() && localPolicies.get().bookieAffinityGroup != null) {
                managedLedgerConfig
                        .setBookKeeperEnsemblePlacementPolicyClassName(ZkIsolatedBookieEnsemblePlacementPolicy.class);
                Map<String, Object> properties = Maps.newHashMap();
                properties.put(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                        localPolicies.get().bookieAffinityGroup.bookkeeperAffinityGroupPrimary);
                properties.put(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                        localPolicies.get().bookieAffinityGroup.bookkeeperAffinityGroupSecondary);
                managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyProperties(properties);
            }
            managedLedgerConfig.setThrottleMarkDelete(persistencePolicies.getManagedLedgerMaxMarkDeleteRate());
            managedLedgerConfig.setDigestType(serviceConfig.getManagedLedgerDigestType());

            managedLedgerConfig.setMaxUnackedRangesToPersist(serviceConfig.getManagedLedgerMaxUnackedRangesToPersist());
            managedLedgerConfig.setMaxUnackedRangesToPersistInZk(serviceConfig.getManagedLedgerMaxUnackedRangesToPersistInZooKeeper());
            managedLedgerConfig.setMaxEntriesPerLedger(serviceConfig.getManagedLedgerMaxEntriesPerLedger());
            managedLedgerConfig.setMinimumRolloverTime(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            managedLedgerConfig.setMaximumRolloverTime(serviceConfig.getManagedLedgerMaxLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            managedLedgerConfig.setMaxSizePerLedgerMb(2048);

            managedLedgerConfig.setMetadataOperationsTimeoutSeconds(
                    serviceConfig.getManagedLedgerMetadataOperationsTimeoutSeconds());
            managedLedgerConfig.setReadEntryTimeoutSeconds(serviceConfig.getManagedLedgerReadEntryTimeoutSeconds());
            managedLedgerConfig.setAddEntryTimeoutSeconds(serviceConfig.getManagedLedgerAddEntryTimeoutSeconds());
            managedLedgerConfig.setMetadataEnsembleSize(serviceConfig.getManagedLedgerDefaultEnsembleSize());
            managedLedgerConfig.setUnackedRangesOpenCacheSetEnabled(
                    serviceConfig.isManagedLedgerUnackedRangesOpenCacheSetEnabled());
            managedLedgerConfig.setMetadataWriteQuorumSize(serviceConfig.getManagedLedgerDefaultWriteQuorum());
            managedLedgerConfig.setMetadataAckQuorumSize(serviceConfig.getManagedLedgerDefaultAckQuorum());
            managedLedgerConfig
                    .setMetadataMaxEntriesPerLedger(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger());

            managedLedgerConfig.setLedgerRolloverTimeout(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds());
            managedLedgerConfig.setRetentionTime(retentionPolicies.getRetentionTimeInMinutes(), TimeUnit.MINUTES);
            managedLedgerConfig.setRetentionSizeInMB(retentionPolicies.getRetentionSizeInMB());

            managedLedgerConfig.setLedgerOffloader(pulsar.getManagedLedgerOffloader());
            policies.ifPresent(p -> {
                    long lag = serviceConfig.getManagedLedgerOffloadDeletionLagMs();
                    if (p.offload_deletion_lag_ms != null) {
                        lag = p.offload_deletion_lag_ms;
                    }
                    managedLedgerConfig.setOffloadLedgerDeletionLag(lag, TimeUnit.MILLISECONDS);
                    managedLedgerConfig.setOffloadAutoTriggerSizeThresholdBytes(p.offload_threshold);
                });

            future.complete(managedLedgerConfig);
        }, (exception) -> future.completeExceptionally(exception)));

        return future;
    }

    private void addTopicToStatsMaps(TopicName topicName, Topic topic) {
        try {
            NamespaceBundle namespaceBundle = pulsar.getNamespaceService().getBundle(topicName);

            if (namespaceBundle != null) {
                synchronized (multiLayerTopicsMap) {
                    String serviceUnit = namespaceBundle.toString();
                    multiLayerTopicsMap //
                            .computeIfAbsent(topicName.getNamespace(), k -> new ConcurrentOpenHashMap<>()) //
                            .computeIfAbsent(serviceUnit, k -> new ConcurrentOpenHashMap<>()) //
                            .put(topicName.toString(), topic);
                }
            }
            invalidateOfflineTopicStatCache(topicName);
        } catch (Exception e) {
            log.warn("Got exception when retrieving bundle name during create persistent topic", e);
        }
    }

    public void refreshTopicToStatsMaps(NamespaceBundle oldBundle) {
        checkNotNull(oldBundle);
        try {
            // retrieve all topics under existing old bundle
            List<Topic> topics = getAllTopicsFromNamespaceBundle(oldBundle.getNamespaceObject().toString(),
                    oldBundle.toString());
            if (!isEmpty(topics)) {
                // add topic under new split bundles which already updated into NamespaceBundleFactory.bundleCache
                topics.stream().forEach(t -> {
                    addTopicToStatsMaps(TopicName.get(t.getName()), t);
                });
                // remove old bundle from the map
                synchronized (multiLayerTopicsMap) {
                    multiLayerTopicsMap.get(oldBundle.getNamespaceObject().toString()).remove(oldBundle.toString());
                    pulsarStats.invalidBundleStats(oldBundle.toString());
                }
            }
        } catch (Exception e) {
            log.warn("Got exception while refreshing topicStats map", e);
        }
    }

    public PersistentOfflineTopicStats getOfflineTopicStat(TopicName topicName) {
        return offlineTopicStatCache.get(topicName);
    }

    public void cacheOfflineTopicStats(TopicName topicName, PersistentOfflineTopicStats offlineTopicStats) {
        offlineTopicStatCache.put(topicName, offlineTopicStats);
    }

    public void invalidateOfflineTopicStatCache(TopicName topicName) {
        PersistentOfflineTopicStats removed = offlineTopicStatCache.remove(topicName);
        if (removed != null) {
            log.info("Removed cached offline topic stat for {} ", topicName.getPersistenceNamingEncoding());
        }
    }

    /**
     * Get a reference to a topic that is currently loaded in the broker.
     *
     * This method will not make the broker attempt to load the topic if it's not already.
     */
    public Optional<Topic> getTopicReference(String topic) {
          CompletableFuture<Optional<Topic>> future = topics.get(topic);
        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            return future.join();
        } else {
            return Optional.empty();
        }
    }

    public void updateRates() {
        synchronized (pulsarStats) {
            pulsarStats.updateStats(multiLayerTopicsMap);
        }
    }

    public void getDimensionMetrics(Consumer<ByteBuf> consumer) {
        pulsarStats.getDimensionMetrics(consumer);
    }

    public List<Metrics> getTopicMetrics() {
        return pulsarStats.getTopicMetrics();
    }

    public Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsarStats.getBundleStats();
    }

    public Semaphore getLookupRequestSemaphore() {
        return lookupRequestSemaphore.get();
    }

    public void checkGC(int gcIntervalInSeconds) {
        forEachTopic(topic -> topic.checkGC(gcIntervalInSeconds));
    }

    public void checkMessageExpiry() {
        forEachTopic(Topic::checkMessageExpiry);
    }

    public void checkCompaction() {
        forEachTopic((t) -> {
                if (t instanceof PersistentTopic) {
                    ((PersistentTopic) t).checkCompaction();
                }
            });
    }

    public void checkMessageDeduplicationInfo() {
        forEachTopic(Topic::checkMessageDeduplicationInfo);
    }

    public void checkInactiveSubscriptions() {
        forEachTopic(Topic::checkInactiveSubscriptions);
    }

    /**
     * Iterates over all loaded topics in the broker
     */
    public void forEachTopic(Consumer<Topic> consumer) {
        topics.forEach((n, t) -> {
            Optional<Topic> topic = extractTopic(t);
            if (topic.isPresent()) {
                consumer.accept(topic.get());
            }
        });
    }

    public BacklogQuotaManager getBacklogQuotaManager() {
        return this.backlogQuotaManager;
    }

    /**
     *
     * @param topic
     *            needing quota enforcement check
     * @return determine if quota enforcement needs to be done for topic
     */
    public boolean isBacklogExceeded(PersistentTopic topic) {
        TopicName topicName = TopicName.get(topic.getName());
        long backlogQuotaLimitInBytes = getBacklogQuotaManager().getBacklogQuotaLimit(topicName.getNamespace());
        if (backlogQuotaLimitInBytes < 0) {
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] - backlog quota limit = [{}]", topic.getName(), backlogQuotaLimitInBytes);
        }

        // check if backlog exceeded quota
        long storageSize = topic.getBacklogSize();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Storage size = [{}], limit [{}]", topic.getName(), storageSize, backlogQuotaLimitInBytes);
        }

        return (storageSize >= backlogQuotaLimitInBytes);
    }

    public void monitorBacklogQuota() {
        forEachTopic(topic -> {
            if (topic instanceof PersistentTopic) {
                PersistentTopic persistentTopic = (PersistentTopic) topic;
                if (isBacklogExceeded(persistentTopic)) {
                    getBacklogQuotaManager().handleExceededBacklogQuota(persistentTopic);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("quota not exceeded for [{}]", topic.getName());
                    }
                }
            }
        });
    }

    public void checkTopicNsOwnership(final String topic) throws RuntimeException {
        TopicName topicName = TopicName.get(topic);
        boolean ownedByThisInstance;
        try {
            ownedByThisInstance = pulsar.getNamespaceService().isServiceUnitOwned(topicName);
        } catch (Exception e) {
            log.debug(String.format("Failed to check the ownership of the topic: %s", topicName), e);
            throw new RuntimeException(new ServerMetadataException(e));
        }

        if (!ownedByThisInstance) {
            String msg = String.format("Namespace bundle for topic (%s) not served by this instance. Please redo the lookup. "
                    + "Request is denied: namespace=%s", topic, topicName.getNamespace());
            log.warn(msg);
            throw new RuntimeException(new ServiceUnitNotReadyException(msg));
        }
    }

    /**
     * Unload all the topic served by the broker service under the given service unit
     *
     * @param serviceUnit
     * @return
     */
    public CompletableFuture<Integer> unloadServiceUnit(NamespaceBundle serviceUnit) {
        CompletableFuture<Integer> result = new CompletableFuture<Integer>();
        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        topics.forEach((name, topicFuture) -> {
            TopicName topicName = TopicName.get(name);
            if (serviceUnit.includes(topicName)) {
                // Topic needs to be unloaded
                log.info("[{}] Unloading topic", topicName);
                closeFutures.add(topicFuture
                        .thenCompose(t -> t.isPresent() ? t.get().close() : CompletableFuture.completedFuture(null)));
            }
        });
        CompletableFuture<Void> aggregator = FutureUtil.waitForAll(closeFutures);
        aggregator.thenAccept(res -> result.complete(closeFutures.size())).exceptionally(ex -> {
            result.completeExceptionally(ex);
            return null;
        });
        return result;
    }

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
    }

    public void removeTopicFromCache(String topic) {
        TopicName topicName = null;
        NamespaceBundle namespaceBundle = null;
        try {
            topicName = TopicName.get(topic);
            namespaceBundle = pulsar.getNamespaceService().getBundle(topicName);
            checkArgument(namespaceBundle instanceof NamespaceBundle);

            String bundleName = namespaceBundle.toString();
            String namespaceName = topicName.getNamespaceObject().toString();

            synchronized (multiLayerTopicsMap) {
                ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>> namespaceMap = multiLayerTopicsMap
                        .get(namespaceName);
                ConcurrentOpenHashMap<String, Topic> bundleMap = namespaceMap.get(bundleName);
                bundleMap.remove(topic);
                if (bundleMap.isEmpty()) {
                    namespaceMap.remove(bundleName);
                }

                if (namespaceMap.isEmpty()) {
                    multiLayerTopicsMap.remove(namespaceName);
                    final ClusterReplicationMetrics clusterReplicationMetrics = pulsarStats
                            .getClusterReplicationMetrics();
                    replicationClients.forEach((cluster, client) -> {
                        clusterReplicationMetrics.remove(clusterReplicationMetrics.getKeyName(namespaceName, cluster));
                    });
                }
            }
        } catch (Exception e) {
            log.warn("Got exception when retrieving bundle name {} for topic {} during removeTopicFromCache", topicName,
                    namespaceBundle, e);
        }

        topics.remove(topic);
    }

    public int getNumberOfNamespaceBundles() {
        this.numberOfNamespaceBundles = 0;
        this.multiLayerTopicsMap.forEach((namespaceName, bundles) -> {
            this.numberOfNamespaceBundles += bundles.size();
        });
        return this.numberOfNamespaceBundles;
    }

    public ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> getTopics() {
        return topics;
    }

    @Override
    public void onUpdate(String path, Policies data, Stat stat) {
        final NamespaceName namespace = NamespaceName.get(NamespaceBundleFactory.getNamespaceFromPoliciesPath(path));

        log.info("{} updating with {}", path, data);

        topics.forEach((name, topicFuture) -> {
            if (namespace.includes(TopicName.get(name))) {
                // If the topic is already created, immediately apply the updated policies, otherwise once the topic is
                // created it'll apply the policies update
                topicFuture.thenAccept(topic -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Notifying topic that policies have changed: {}", name);
                    }

                    topic.ifPresent(t -> t.onPoliciesUpdate(data));
                });
            }
        });

        // sometimes, some brokers don't receive policies-update watch and miss to remove replication-cluster and still
        // own the bundle. That can cause data-loss for TODO: git-issue
        unloadDeletedReplNamespace(data, namespace);
    }

    /**
     * Unloads the namespace bundles if local cluster is not part of replication-cluster list into the namespace.
     * So, broker that owns the bundle and doesn't receive the zk-watch will unload the namespace.
     * @param data
     * @param namespace
     */
    private void unloadDeletedReplNamespace(Policies data, NamespaceName namespace) {
        if (!namespace.isGlobal()) {
            return;
        }
        final String localCluster = this.pulsar.getConfiguration().getClusterName();
        if (!data.replication_clusters.contains(localCluster)) {
            try {
                NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                        .getBundles(namespace);
                bundles.getBundles().forEach(bundle -> {
                    pulsar.getNamespaceService().isNamespaceBundleOwned(bundle).thenAccept(isExist -> {
                        if (isExist) {
                            this.pulsar().getExecutor().submit(() -> {
                                try {
                                    pulsar().getAdminClient().namespaces().unloadNamespaceBundle(namespace.toString(),
                                            bundle.getBundleRange());
                                } catch (Exception e) {
                                    log.error("Failed to unload namespace-bundle {}-{} that not owned by {}, {}",
                                            namespace.toString(), bundle.toString(), localCluster, e.getMessage());
                                }
                            });
                        }
                    });
                });
            } catch (Exception e) {
                log.error("Failed to unload locally not owned bundles {}", e.getMessage(), e);
            }
        }
    }

    public PulsarService pulsar() {
        return pulsar;
    }

    public ScheduledExecutorService executor() {
        return workerGroup;
    }

    public ConcurrentOpenHashMap<String, PulsarClient> getReplicationClients() {
        return replicationClients;
    }

    public boolean isAuthenticationEnabled() {
        return pulsar.getConfiguration().isAuthenticationEnabled();
    }

    public boolean isAuthorizationEnabled() {
        return authorizationService != null;
    }

    public int getKeepAliveIntervalSeconds() {
        return keepAliveIntervalSeconds;
    }

    public String generateUniqueProducerName() {
        return producerNameGenerator.getNextId();
    }

    public Map<String, TopicStats> getTopicStats() {
        HashMap<String, TopicStats> stats = new HashMap<>();

        forEachTopic(topic -> stats.put(topic.getName(), topic.getStats()));

        return stats;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public List<Topic> getAllTopicsFromNamespaceBundle(String namespace, String bundle) {
        ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>> map1 = multiLayerTopicsMap.get(namespace);
        if (map1 == null) {
            return Collections.emptyList();
        }

        ConcurrentOpenHashMap<String, Topic> map2 = map1.get(bundle);
        if (map2 == null) {
            return Collections.emptyList();
        }

        return map2.values();
    }

    public ZooKeeperDataCache<Map<String, String>> getDynamicConfigurationCache() {
        return dynamicConfigurationCache;
    }

    /**
     * Update dynamic-ServiceConfiguration with value present into zk-configuration-map and register listeners on
     * dynamic-ServiceConfiguration field to take appropriate action on change of zk-configuration-map.
     */
    private void updateConfigurationAndRegisterListeners() {
        // (1) Dynamic-config value validation: add validator if updated value required strict check before considering
        // validate configured load-manager classname present into classpath
        addDynamicConfigValidator("loadManagerClassName", (className) -> {
            try {
                Class.forName(className);
            } catch (ClassNotFoundException e) {
                log.warn("Configured load-manager class {} not found {}", className, e.getMessage());
                return false;
            }
            return true;
        });

        // (2) update ServiceConfiguration value by reading zk-configuration-map
        updateDynamicServiceConfiguration();

        // (3) Listener Registration
        // add listener on "maxConcurrentLookupRequest" value change
        registerConfigurationListener("maxConcurrentLookupRequest",
                (maxConcurrentLookupRequest) -> lookupRequestSemaphore.set(new Semaphore((int) maxConcurrentLookupRequest, false)));
        // add listener on "maxConcurrentTopicLoadRequest" value change
        registerConfigurationListener("maxConcurrentTopicLoadRequest",
                (maxConcurrentTopicLoadRequest) -> topicLoadRequestSemaphore.set(new Semaphore((int) maxConcurrentTopicLoadRequest, false)));
        registerConfigurationListener("loadManagerClassName", className -> {
            try {
                final LoadManager newLoadManager = LoadManager.create(pulsar);
                log.info("Created load manager: {}", className);
                pulsar.getLoadManager().get().stop();
                newLoadManager.start();
                pulsar.getLoadManager().set(newLoadManager);
            } catch (Exception ex) {
                log.warn("Failed to change load manager due to {}", ex);
            }
        });
        // add listener to update message-dispatch-rate in msg for topic
        registerConfigurationListener("dispatchThrottlingRatePerTopicInMsg", (dispatchRatePerTopicInMsg) -> {
            updateTopicMessageDispatchRate();
        });
        // add listener to update message-dispatch-rate in byte for topic
        registerConfigurationListener("dispatchThrottlingRatePerTopicInByte", (dispatchRatePerTopicInByte) -> {
            updateTopicMessageDispatchRate();
        });
        // add listener to update managed-ledger config to skipNonRecoverableLedgers
        registerConfigurationListener("autoSkipNonRecoverableData", (skipNonRecoverableLedger) -> {
            updateManagedLedgerConfig();
        });
        // add listener to update message-dispatch-rate in msg for subscription
        registerConfigurationListener("dispatchThrottlingRatePerSubscriptionInMsg", (dispatchRatePerTopicInMsg) -> {
            updateSubscriptionMessageDispatchRate();
        });
        // add listener to update message-dispatch-rate in byte for subscription
        registerConfigurationListener("dispatchThrottlingRatePerSubscriptionInByte", (dispatchRatePerTopicInByte) -> {
            updateSubscriptionMessageDispatchRate();
        });
        // add listener to update message-dispatch-rate in msg for replicator
        registerConfigurationListener("dispatchThrottlingRatePerReplicatorInMsg", (dispatchRatePerTopicInMsg) -> {
            updateReplicatorMessageDispatchRate();
        });
        // add listener to update message-dispatch-rate in byte for replicator
        registerConfigurationListener("dispatchThrottlingRatePerReplicatorInByte", (dispatchRatePerTopicInByte) -> {
            updateReplicatorMessageDispatchRate();
        });
        // add more listeners here
    }

    private void updateTopicMessageDispatchRate() {
        this.pulsar().getExecutor().execute(() -> {
            // update message-rate for each topic
            forEachTopic(topic -> {
                if (topic.getDispatchRateLimiter().isPresent()) {
                    topic.getDispatchRateLimiter().get().updateDispatchRate();
                }
            });
        });
    }

    private void updateSubscriptionMessageDispatchRate() {
        this.pulsar().getExecutor().submit(() -> {
            // update message-rate for each topic subscription
            forEachTopic(topic -> {
                topic.getSubscriptions().forEach((subName, persistentSubscription) -> {
                    Dispatcher dispatcher = persistentSubscription.getDispatcher();
                    if (dispatcher.getRateLimiter().isPresent()) {
                        dispatcher.getRateLimiter().get().updateDispatchRate();
                    }
                });
            });
        });
    }

    private void updateReplicatorMessageDispatchRate() {
        this.pulsar().getExecutor().submit(() -> {
            // update message-rate for each topic Replicator in Geo-replication
            forEachTopic(topic ->
                topic.getReplicators().forEach((name, persistentReplicator) -> {
                    if (persistentReplicator.getRateLimiter().isPresent()) {
                        persistentReplicator.getRateLimiter().get().updateDispatchRate();
                    }
                }));
        });
    }

    private void updateManagedLedgerConfig() {
        this.pulsar().getExecutor().execute(() -> {
            // update managed-ledger config of each topic

            forEachTopic(topic -> {
                try {
                    if (topic instanceof PersistentTopic) {
                        PersistentTopic persistentTopic = (PersistentTopic) topic;
                        // update skipNonRecoverableLedger configuration
                        persistentTopic.getManagedLedger().getConfig().setAutoSkipNonRecoverableData(
                                pulsar.getConfiguration().isAutoSkipNonRecoverableData());
                    }
                } catch (Exception e) {
                    log.warn("[{}] failed to update managed-ledger config", topic.getName(), e);
                }
            });
        });
    }

    /**
     * Allows a listener to listen on update of {@link ServiceConfiguration} change, so listener can take appropriate
     * action if any specific config-field value has been changed.
     * </p>
     * On notification, listener should first check if config value has been changed and after taking appropriate
     * action, listener should update config value with new value if it has been changed (so, next time listener can
     * compare values on configMap change).
     * @param <T>
     *
     * @param configKey
     *            : configuration field name
     * @param listener
     *            : listener which takes appropriate action on config-value change
     */
    public <T> void registerConfigurationListener(String configKey, Consumer<T> listener) {
        validateConfigKey(configKey);
        configRegisteredListeners.put(configKey, listener);
    }

    private void addDynamicConfigValidator(String key, Predicate<String> validator) {
        validateConfigKey(key);
        if (dynamicConfigurationMap.containsKey(key)) {
            dynamicConfigurationMap.get(key).validator = validator;
        }
    }

    private void validateConfigKey(String key) {
        try {
            ServiceConfiguration.class.getDeclaredField(key);
        } catch (Exception e) {
            log.error("ServiceConfiguration key {} not found {}", key, e.getMessage());
            throw new IllegalArgumentException("Invalid service config " + key, e);
        }
    }

    /**
     * Updates pulsar.ServiceConfiguration's dynamic field with value persent into zk-dynamic path. It also validates
     * dynamic-value before updating it and throws {@code IllegalArgumentException} if validation fails
     */
    private void updateDynamicServiceConfiguration() {

        Optional<Map<String, String>> configCache = Optional.empty();
        try {
            // create dynamic-config znode if not present
            if (pulsar.getZkClient().exists(BROKER_SERVICE_CONFIGURATION_PATH, false) == null) {
                try {
                    byte[] data = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(Maps.newHashMap());
                    ZkUtils.createFullPathOptimistic(pulsar.getZkClient(), BROKER_SERVICE_CONFIGURATION_PATH, data,
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // Ok
                }
            }
            configCache = dynamicConfigurationCache.get(BROKER_SERVICE_CONFIGURATION_PATH);
        } catch (Exception e) {
            log.warn("Failed to read zookeeper path [{}]:", BROKER_SERVICE_CONFIGURATION_PATH, e);
        }
        if (configCache.isPresent()) {
            configCache.get().forEach((key, value) -> {
                // validate field
                if (dynamicConfigurationMap.containsKey(key) && dynamicConfigurationMap.get(key).validator != null) {
                    if (!dynamicConfigurationMap.get(key).validator.test(value)) {
                        log.error("Failed to validate dynamic config {} with value {}", key, value);
                        throw new IllegalArgumentException(
                                String.format("Failed to validate dynamic-config %s/%s", key, value));
                    }
                }
                // update field value
                try {
                    Field field = ServiceConfiguration.class.getDeclaredField(key);
                    if (field != null && field.isAnnotationPresent(FieldContext.class)) {
                        field.setAccessible(true);
                        field.set(pulsar().getConfiguration(), FieldParser.value(value, field));
                        log.info("Successfully updated {}/{}", key, value);
                    }
                } catch (Exception e) {
                    log.warn("Failed to update service configuration {}/{}, {}", key, value, e.getMessage());
                }
            });
        }
        // register a listener: it updates field value and triggers appropriate registered field-listener only if
        // field's value has been changed so, registered doesn't have to update field value in ServiceConfiguration
        dynamicConfigurationCache.registerListener(new ZooKeeperCacheListener<Map<String, String>>() {
            @SuppressWarnings("unchecked")
            @Override
            public void onUpdate(String path, Map<String, String> data, Stat stat) {
                if (BROKER_SERVICE_CONFIGURATION_PATH.equalsIgnoreCase(path) && data != null) {
                    data.forEach((configKey, value) -> {
                        Field configField = dynamicConfigurationMap.get(configKey).field;
                        Object newValue = FieldParser.value(data.get(configKey), configField);
                        if (configField != null) {
                            Consumer listener = configRegisteredListeners.get(configKey);
                            try {
                                Object existingValue = configField.get(pulsar.getConfiguration());
                                configField.set(pulsar.getConfiguration(), newValue);
                                log.info("Successfully updated configuration {}/{}", configKey,
                                        data.get(configKey));
                                if (listener != null && !existingValue.equals(newValue)) {
                                    listener.accept(newValue);
                                }
                            } catch (Exception e) {
                                log.error("Failed to update config {}/{}", configKey, newValue);
                            }
                        } else {
                            log.error("Found non-dynamic field in dynamicConfigMap {}/{}", configKey, newValue);
                        }
                    });
                }
            }
        });

    }

    public DelayedDeliveryTrackerFactory getDelayedDeliveryTrackerFactory() {
        return delayedDeliveryTrackerFactory;
    }

    public static List<String> getDynamicConfiguration() {
        return dynamicConfigurationMap.keys();
    }

    public Map<String, String> getRuntimeConfiguration() {
        Map<String, String> configMap = Maps.newHashMap();
        ConcurrentOpenHashMap<String, Object> runtimeConfigurationMap = getRuntimeConfigurationMap();
        runtimeConfigurationMap.forEach((key, value) -> {
            configMap.put(key, String.valueOf(value));
        });
        return configMap;
    }

    public static boolean isDynamicConfiguration(String key) {
        return dynamicConfigurationMap.containsKey(key);
    }

    public static boolean validateDynamicConfiguration(String key, String value) {
        if (dynamicConfigurationMap.containsKey(key) && dynamicConfigurationMap.get(key).validator != null) {
            return dynamicConfigurationMap.get(key).validator.test(value);
        }
        return true;
    }

    private static ConcurrentOpenHashMap<String, ConfigField> prepareDynamicConfigurationMap() {
        ConcurrentOpenHashMap<String, ConfigField> dynamicConfigurationMap = new ConcurrentOpenHashMap<>();
        for (Field field : ServiceConfiguration.class.getDeclaredFields()) {
            if (field != null && field.isAnnotationPresent(FieldContext.class)) {
                field.setAccessible(true);
                if (((FieldContext) field.getAnnotation(FieldContext.class)).dynamic()) {
                    dynamicConfigurationMap.put(field.getName(), new ConfigField(field));
                }
            }
        }
        return dynamicConfigurationMap;
    }

    private ConcurrentOpenHashMap<String, Object> getRuntimeConfigurationMap() {
        ConcurrentOpenHashMap<String, Object> runtimeConfigurationMap = new ConcurrentOpenHashMap<>();
        for (Field field : ServiceConfiguration.class.getDeclaredFields()) {
            if (field != null && field.isAnnotationPresent(FieldContext.class)) {
                field.setAccessible(true);
                try {
                    Object configValue = field.get(pulsar.getConfiguration());
                    runtimeConfigurationMap.put(field.getName(), configValue == null ? "" : configValue);
                } catch (Exception e) {
                    log.error("Failed to get value of field {}, {}", field.getName(), e.getMessage());
                }
            }
        }
        return runtimeConfigurationMap;
    }

    /**
     * Create pending topic and on completion it picks the next one until processes all topics in
     * {@link #pendingTopicLoadingQueue}.<br/>
     * It also tries to acquire {@link #topicLoadRequestSemaphore} so throttle down newly incoming topics and release
     * permit if it was successful to acquire it.
     */
    private void createPendingLoadTopic() {
        Pair<String, CompletableFuture<Optional<Topic>>> pendingTopic = pendingTopicLoadingQueue.poll();
        if (pendingTopic == null) {
            return;
        }

        final String topic = pendingTopic.getLeft();
        try {
            checkTopicNsOwnership(topic);
            CompletableFuture<Optional<Topic>> pendingFuture = pendingTopic.getRight();
            final Semaphore topicLoadSemaphore = topicLoadRequestSemaphore.get();
            final boolean acquiredPermit = topicLoadSemaphore.tryAcquire();
            createPersistentTopic(topic, true, pendingFuture);
            pendingFuture.handle((persistentTopic, ex) -> {
                // release permit and process next pending topic
                if (acquiredPermit) {
                    topicLoadSemaphore.release();
                }
                createPendingLoadTopic();
                return null;
            });
        } catch (RuntimeException re) {
            log.error("Failed to create pending topic {} {}", topic, re);
            pendingTopic.getRight().completeExceptionally(re.getCause());
            // schedule to process next pending topic
            inactivityMonitor.schedule(() -> createPendingLoadTopic(), 100, TimeUnit.MILLISECONDS);
        }

    }

    public OrderedExecutor getTopicOrderedExecutor() {
        return topicOrderedExecutor;
    }

    public ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>>> getMultiLayerTopicMap() {
        return multiLayerTopicsMap;
    }

    /**
     * If per-broker unack message reached to limit then it blocks dispatcher if its unack message limit has been
     * reached to {@link #maxUnackedMsgsPerDispatcher}
     *
     * @param dispatcher
     * @param numberOfMessages
     */
    public void addUnAckedMessages(PersistentDispatcherMultipleConsumers dispatcher, int numberOfMessages) {
        // don't block dispatchers if maxUnackedMessages = 0
        if (maxUnackedMessages > 0) {
            totalUnackedMessages.add(numberOfMessages);

            // block dispatcher: if broker is already blocked and dispatcher reaches to max dispatcher limit when broker
            // is blocked
            if (blockedDispatcherOnHighUnackedMsgs.get() && !dispatcher.isBlockedDispatcherOnUnackedMsgs()
                    && dispatcher.getTotalUnackedMessages() > maxUnackedMsgsPerDispatcher) {
                lock.readLock().lock();
                try {
                    log.info("[{}] dispatcher reached to max unack msg limit on blocked-broker {}",
                            dispatcher.getName(), dispatcher.getTotalUnackedMessages());
                    dispatcher.blockDispatcherOnUnackedMsgs();
                    blockedDispatchers.add(dispatcher);
                } finally {
                    lock.readLock().unlock();
                }
            }
        }
    }

    /**
     * Adds given dispatcher's unackMessage count to broker-unack message count and if it reaches to the
     * {@link #maxUnackedMessages} then it blocks all the dispatchers which has unack-messages higher than
     * {@link #maxUnackedMsgsPerDispatcher}. It unblocks all dispatchers once broker-unack message counts decreased to
     * ({@link #maxUnackedMessages}/2)
     *
     */
    public void checkUnAckMessageDispatching() {

        // don't block dispatchers if maxUnackedMessages = 0
        if (maxUnackedMessages <= 0) {
            return;
        }
        long unAckedMessages = totalUnackedMessages.sum();
        if (unAckedMessages >= maxUnackedMessages && blockedDispatcherOnHighUnackedMsgs.compareAndSet(false, true)) {
            // block dispatcher with higher unack-msg when it reaches broker-unack msg limit
            log.info("[{}] Starting blocking dispatchers with unacked msgs {} due to reached max broker limit {}",
                    maxUnackedMessages, maxUnackedMsgsPerDispatcher);
            executor().execute(() -> blockDispatchersWithLargeUnAckMessages());
        } else if (blockedDispatcherOnHighUnackedMsgs.get() && unAckedMessages < maxUnackedMessages / 2) {
            // unblock broker-dispatching if received enough acked messages back
            if (blockedDispatcherOnHighUnackedMsgs.compareAndSet(true, false)) {
                unblockDispatchersOnUnAckMessages(blockedDispatchers.values());
            }
        }

    }

    public boolean isBrokerDispatchingBlocked() {
        return blockedDispatcherOnHighUnackedMsgs.get();
    }

    private void blockDispatchersWithLargeUnAckMessages() {
        lock.readLock().lock();
        try {
            forEachTopic(topic -> {
                topic.getSubscriptions().forEach((subName, persistentSubscription) -> {
                    if (persistentSubscription.getDispatcher() instanceof PersistentDispatcherMultipleConsumers) {
                        PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers) persistentSubscription
                                .getDispatcher();
                        int dispatcherUnAckMsgs = dispatcher.getTotalUnackedMessages();
                        if (dispatcherUnAckMsgs > maxUnackedMsgsPerDispatcher) {
                            log.info("[{}] Blocking dispatcher due to reached max broker limit {}",
                                    dispatcher.getName(), dispatcher.getTotalUnackedMessages());
                            dispatcher.blockDispatcherOnUnackedMsgs();
                            blockedDispatchers.add(dispatcher);
                        }
                    }
                });
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Unblocks the dispatchers and removes it from the {@link #blockedDispatchers} list
     *
     * @param dispatcherList
     */
    public void unblockDispatchersOnUnAckMessages(List<PersistentDispatcherMultipleConsumers> dispatcherList) {
        lock.writeLock().lock();
        try {
            dispatcherList.forEach(dispatcher -> {
                dispatcher.unBlockDispatcherOnUnackedMsgs();
                executor().execute(() -> dispatcher.readMoreEntries());
                log.info("[{}] Dispatcher is unblocked", dispatcher.getName());
                blockedDispatchers.remove(dispatcher);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class ConfigField {
        final Field field;
        Predicate<String> validator;

        public ConfigField(Field field) {
            super();
            this.field = field;
        }
    }

    /**
     * Safely extract optional topic instance from a future, in a way to avoid unchecked exceptions and race conditions.
     */
    public static Optional<Topic> extractTopic(CompletableFuture<Optional<Topic>> topicFuture) {
        if (topicFuture.isDone() && !topicFuture.isCompletedExceptionally()) {
            return topicFuture.join();
        } else {
            return Optional.empty();
        }
    }
}
