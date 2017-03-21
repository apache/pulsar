/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.commons.lang.SystemUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.admin.AdminResource;
import com.yahoo.pulsar.broker.authentication.AuthenticationService;
import com.yahoo.pulsar.broker.authorization.AuthorizationManager;
import com.yahoo.pulsar.broker.service.BrokerServiceException.PersistenceException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import com.yahoo.pulsar.broker.service.persistent.PersistentReplicator;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.broker.stats.ClusterReplicationMetrics;
import com.yahoo.pulsar.broker.stats.Metrics;
import com.yahoo.pulsar.broker.web.PulsarWebResource;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;
import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.common.configuration.FieldContext;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.PersistencePolicies;
import com.yahoo.pulsar.common.policies.data.PersistentOfflineTopicStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicStats;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.policies.data.RetentionPolicies;
import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.util.FieldParser;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashMap;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashSet;
import com.yahoo.pulsar.zookeeper.ZooKeeperCacheListener;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

public class BrokerService implements Closeable, ZooKeeperCacheListener<Policies> {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    private final PulsarService pulsar;
    private final ManagedLedgerFactory managedLedgerFactory;
    private final int port;
    private final int tlsPort;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Topic>> topics;

    private final ConcurrentOpenHashMap<String, PulsarClient> replicationClients;

    // Multi-layer topics map:
    // Namespace --> Bundle --> topicName --> topic
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, PersistentTopic>>> multiLayerTopicsMap;
    private int numberOfNamespaceBundles = 0;

    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;
    // offline topic backlog cache
    private final ConcurrentOpenHashMap<DestinationName, PersistentOfflineTopicStats> offlineTopicStatCache;
    private static final ConcurrentOpenHashMap<String, Field> dynamicConfigurationMap = prepareDynamicConfigurationMap();
    private final ConcurrentOpenHashMap<String, Consumer> configRegisteredListeners;

    private AuthorizationManager authorizationManager = null;
    private final ScheduledExecutorService statsUpdater;
    private final ScheduledExecutorService backlogQuotaChecker;
    
    protected final AtomicReference<Semaphore> lookupRequestSemaphore;

    private final ScheduledExecutorService inactivityMonitor;
    private final ScheduledExecutorService messageExpiryMonitor;

    private DistributedIdGenerator producerNameGenerator;

    private final static String producerNameGeneratorPath = "/counters/producer-name";

    private final BacklogQuotaManager backlogQuotaManager;

    private final int keepAliveIntervalSeconds;
    private final PulsarStats pulsarStats;
    private final AuthenticationService authenticationService;
    
    public static final String BROKER_SERVICE_CONFIGURATION_PATH = "/admin/configuration";
    private final ZooKeeperDataCache<Map<String, String>> dynamicConfigurationCache;

    public BrokerService(PulsarService pulsar) throws Exception {
        this.pulsar = pulsar;
        this.managedLedgerFactory = pulsar.getManagedLedgerFactory();
        this.port = new URI(pulsar.getBrokerServiceUrl()).getPort();
        this.tlsPort = new URI(pulsar.getBrokerServiceUrlTls()).getPort();
        this.topics = new ConcurrentOpenHashMap<>();
        this.replicationClients = new ConcurrentOpenHashMap<>();
        this.keepAliveIntervalSeconds = pulsar.getConfiguration().getKeepAliveIntervalSeconds();
        this.configRegisteredListeners = new ConcurrentOpenHashMap<>();

        this.multiLayerTopicsMap = new ConcurrentOpenHashMap<>();
        this.pulsarStats = new PulsarStats(pulsar);
        this.offlineTopicStatCache = new ConcurrentOpenHashMap<>();

        final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-acceptor");
        final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-io");
        final int numThreads = Runtime.getRuntime().availableProcessors() * 2;
        log.info("Using {} threads for broker service IO", numThreads);

        EventLoopGroup acceptorEventLoop, workersEventLoop;
        if (SystemUtils.IS_OS_LINUX) {
            try {
                acceptorEventLoop = new EpollEventLoopGroup(1, acceptorThreadFactory);
                workersEventLoop = new EpollEventLoopGroup(numThreads, workersThreadFactory);
            } catch (UnsatisfiedLinkError e) {
                acceptorEventLoop = new NioEventLoopGroup(1, acceptorThreadFactory);
                workersEventLoop = new NioEventLoopGroup(numThreads, workersThreadFactory);
            }
        } else {
            acceptorEventLoop = new NioEventLoopGroup(1, acceptorThreadFactory);
            workersEventLoop = new NioEventLoopGroup(numThreads, workersThreadFactory);
        }

        this.acceptorGroup = acceptorEventLoop;
        this.workerGroup = workersEventLoop;
        this.statsUpdater = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-stats-updater"));
        if (pulsar.getConfiguration().isAuthorizationEnabled()) {
            this.authorizationManager = new AuthorizationManager(pulsar.getConfiguration(),
                    pulsar.getConfigurationCache());
        }

        if (pulsar.getConfigurationCache() != null) {
            pulsar.getConfigurationCache().policiesCache().registerListener(this);
        }

        this.inactivityMonitor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-inactivity-monitor"));
        this.messageExpiryMonitor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-msg-expiry-monitor"));
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
        // update dynamic configuration and register-listener
        updateConfigurationAndRegisterListeners();
        this.lookupRequestSemaphore = new AtomicReference<Semaphore>(
                new Semaphore(pulsar.getConfiguration().getMaxConcurrentLookupRequest(), true));

        PersistentReplicator.setReplicatorQueueSize(pulsar.getConfiguration().getReplicationProducerQueueSize());
    }

    public void start() throws Exception {
        this.producerNameGenerator = new DistributedIdGenerator(pulsar.getZkClient(), producerNameGeneratorPath,
                pulsar.getConfiguration().getClusterName());

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

        if (workerGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollServerSocketChannel.class);
            bootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
        } else {
            bootstrap.channel(NioServerSocketChannel.class);
        }

        ServiceConfiguration serviceConfig = pulsar.getConfiguration();

        bootstrap.childHandler(new PulsarChannelInitializer(this, serviceConfig, false));
        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(pulsar.getBindAddress(), port)).sync();
        log.info("Started Pulsar Broker service on port {}", port);

        if (serviceConfig.isTlsEnabled()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new PulsarChannelInitializer(this, serviceConfig, true));
            tlsBootstrap.bind(new InetSocketAddress(pulsar.getBindAddress(), tlsPort)).sync();
            log.info("Started Pulsar Broker TLS service on port {}", tlsPort);
        }

        // start other housekeeping functions
        this.startStatsUpdater();
        this.startInactivityMonitor();
        this.startMessageExpiryMonitor();
        this.startBacklogQuotaChecker();
    }

    void startStatsUpdater() {
        statsUpdater.scheduleAtFixedRate(safeRun(this::updateRates), 60, 60, TimeUnit.SECONDS);

        // Ensure the broker starts up with initial stats
        updateRates();
    }

    void startInactivityMonitor() {
        if (pulsar().getConfiguration().isBrokerDeleteInactiveTopicsEnabled()) {
            int interval = pulsar().getConfiguration().getBrokerServicePurgeInactiveFrequencyInSeconds();
            inactivityMonitor.scheduleAtFixedRate(safeRun(() -> checkGC(interval)), interval, interval,
                    TimeUnit.SECONDS);
        }
    }

    void startMessageExpiryMonitor() {
        int interval = pulsar().getConfiguration().getMessageExpiryCheckIntervalInMinutes();
        messageExpiryMonitor.scheduleAtFixedRate(safeRun(this::checkMessageExpiry), interval, interval,
                TimeUnit.MINUTES);
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
        backlogQuotaChecker.shutdown();
        authenticationService.close();
        pulsarStats.close();
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
            if (pulsar.getLoadManager() != null) {
                pulsar.getLoadManager().get().disableBroker();
            }

            // unload all namespace-bundles gracefully
            long closeTopicsStartTime = System.nanoTime();
            Set<NamespaceBundle> serviceUnits = pulsar.getNamespaceService().getOwnedServiceUnits();
            serviceUnits.forEach(su -> {
                if (su instanceof NamespaceBundle) {
                    try {
                        pulsar.getNamespaceService().unloadNamespaceBundle((NamespaceBundle) su);
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

    public CompletableFuture<Topic> getTopic(final String topic) {
        try {
            CompletableFuture<Topic> topicFuture = topics.get(topic);
            if (topicFuture != null) {
                return topicFuture;
            }
            return topics.computeIfAbsent(topic, this::createPersistentTopic);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Illegalargument exception when loading topic", topic, e);
            return failedFuture(e);
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ServiceUnitNotReadyException) {
                log.warn("[{}] Service unit is not ready when loading the topic", topic);
            } else {
                log.warn("[{}] Unexpected exception when loading topic: {}", topic, cause);
            }

            return failedFuture(cause);
        }
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
                ClientConfiguration configuration = new ClientConfiguration();
                configuration.setUseTcpNoDelay(false);
                configuration.setConnectionsPerBroker(pulsar.getConfiguration().getReplicationConnectionsPerBroker());
                configuration.setStatsInterval(0, TimeUnit.SECONDS);
                if (pulsar.getConfiguration().isAuthenticationEnabled()) {
                    configuration.setAuthentication(pulsar.getConfiguration().getBrokerClientAuthenticationPlugin(),
                            pulsar.getConfiguration().getBrokerClientAuthenticationParameters());
                }
                String clusterUrl = configuration.isUseTls() ? (isNotBlank(data.getBrokerServiceUrlTls())
                        ? data.getBrokerServiceUrlTls() : data.getServiceUrlTls()) : null;
                clusterUrl = (isNotBlank(clusterUrl)) ? clusterUrl
                        : (isNotBlank(data.getBrokerServiceUrl()) ? data.getBrokerServiceUrl() : data.getServiceUrl());
                return new PulsarClientImpl(clusterUrl, configuration, this.workerGroup);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<Topic> createPersistentTopic(final String topic) throws RuntimeException {
        checkTopicNsOwnership(topic);

        final long topicCreateTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        DestinationName destinationName = DestinationName.get(topic);
        if (!pulsar.getNamespaceService().isServiceUnitActive(destinationName)) {
            // namespace is being unloaded
            String msg = String.format("Namespace is being unloaded, cannot add topic %s", topic);
            log.warn(msg);
            throw new RuntimeException(new ServiceUnitNotReadyException(msg));
        }

        final CompletableFuture<Topic> topicFuture = new CompletableFuture<>();

        getManagedLedgerConfig(destinationName).thenAccept(config -> {
            // Once we have the configuration, we can proceed with the async open operation

            managedLedgerFactory.asyncOpen(destinationName.getPersistenceNamingEncoding(), config,
                    new OpenLedgerCallback() {
                        @Override
                        public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                            PersistentTopic persistentTopic = new PersistentTopic(topic, ledger, BrokerService.this);

                            CompletableFuture<Void> replicationFuture = persistentTopic.checkReplication();
                            replicationFuture.thenRun(() -> {
                                log.info("Created topic {}", topic);
                                long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
                                        - topicCreateTimeMs;
                                pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
                                addTopicToStatsMaps(destinationName, persistentTopic);
                                topicFuture.complete(persistentTopic);
                            });
                            replicationFuture.exceptionally((ex) -> {
                                log.warn("Replication check failed. Removing topic from topics list {}, {}", topic, ex);
                                persistentTopic.stopReplProducers().whenComplete((v, exception) -> {
                                    topics.remove(topic, topicFuture);
                                    topicFuture.completeExceptionally(ex);
                                });

                                return null;
                            });
                        }

                        @Override
                        public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            log.warn("Failed to create topic {}", topic, exception);
                            topics.remove(topic, topicFuture);
                            topicFuture.completeExceptionally(new PersistenceException(exception));
                        }
                    }, null);

        }).exceptionally((exception) -> {
            log.warn("[{}] Failed to get topic configuration: {}", topic, exception.getMessage(), exception);
            topics.remove(topic, topicFuture);
            topicFuture.completeExceptionally(exception);
            return null;
        });

        return topicFuture;
    }

    public CompletableFuture<ManagedLedgerConfig> getManagedLedgerConfig(DestinationName topicName) {
        CompletableFuture<ManagedLedgerConfig> future = new CompletableFuture<>();
        // Execute in background thread, since getting the policies might block if the z-node wasn't already cached
        pulsar.getOrderedExecutor().submitOrdered(topicName, safeRun(() -> {
            NamespaceName namespace = topicName.getNamespaceObject();
            ServiceConfiguration serviceConfig = pulsar.getConfiguration();

            // Get persistence policy for this destination
            Policies policies;
            try {
                policies = pulsar
                        .getConfigurationCache().policiesCache().get(AdminResource.path("policies",
                                namespace.getProperty(), namespace.getCluster(), namespace.getLocalName()))
                        .orElse(null);
            } catch (Throwable t) {
                // Ignoring since if we don't have policies, we fallback on the default
                log.warn("Got exception when reading persistence policy for {}: {}", topicName, t.getMessage(), t);
                future.completeExceptionally(t);
                return;
            }

            PersistencePolicies persistencePolicies = policies != null ? policies.persistence : null;
            RetentionPolicies retentionPolicies = policies != null ? policies.retention_policies : null;

            if (persistencePolicies == null) {
                // Apply default values
                persistencePolicies = new PersistencePolicies(serviceConfig.getManagedLedgerDefaultEnsembleSize(),
                        serviceConfig.getManagedLedgerDefaultWriteQuorum(),
                        serviceConfig.getManagedLedgerDefaultAckQuorum(),
                        serviceConfig.getManagedLedgerDefaultMarkDeleteRateLimit());
            }

            if (retentionPolicies == null) {
                retentionPolicies = new RetentionPolicies(serviceConfig.getDefaultRetentionTimeInMinutes(),
                        serviceConfig.getDefaultRetentionSizeInMB());
            }

            ManagedLedgerConfig config = new ManagedLedgerConfig();
            config.setEnsembleSize(persistencePolicies.getBookkeeperEnsemble());
            config.setWriteQuorumSize(persistencePolicies.getBookkeeperWriteQuorum());
            config.setAckQuorumSize(persistencePolicies.getBookkeeperAckQuorum());
            config.setThrottleMarkDelete(persistencePolicies.getManagedLedgerMaxMarkDeleteRate());
            config.setDigestType(DigestType.CRC32);

            config.setMaxUnackedRangesToPersist(serviceConfig.getManagedLedgerMaxUnackedRangesToPersist());
            config.setMaxEntriesPerLedger(serviceConfig.getManagedLedgerMaxEntriesPerLedger());
            config.setMinimumRolloverTime(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            config.setMaximumRolloverTime(serviceConfig.getManagedLedgerMaxLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            config.setMaxSizePerLedgerMb(2048);

            config.setMetadataEnsembleSize(serviceConfig.getManagedLedgerDefaultEnsembleSize());
            config.setMetadataWriteQuorumSize(serviceConfig.getManagedLedgerDefaultWriteQuorum());
            config.setMetadataAckQuorumSize(serviceConfig.getManagedLedgerDefaultAckQuorum());
            config.setMetadataMaxEntriesPerLedger(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger());

            config.setLedgerRolloverTimeout(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds());
            config.setRetentionTime(retentionPolicies.getRetentionTimeInMinutes(), TimeUnit.MINUTES);
            config.setRetentionSizeInMB(retentionPolicies.getRetentionSizeInMB());

            future.complete(config);
        }, (exception) -> future.completeExceptionally(exception)));

        return future;
    }

    private void addTopicToStatsMaps(DestinationName topicName, PersistentTopic topic) {
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
            List<PersistentTopic> topics = getAllTopicsFromNamespaceBundle(oldBundle.getNamespaceObject().toString(),
                    oldBundle.toString());
            if (!isEmpty(topics)) {
                // add topic under new split bundles which already updated into NamespaceBundleFactory.bundleCache
                topics.stream().forEach(t -> {
                    addTopicToStatsMaps(DestinationName.get(t.getName()), t);
                });
                // remove old bundle from the map
                synchronized (multiLayerTopicsMap) {
                    multiLayerTopicsMap.get(oldBundle.getNamespaceObject().toString()).remove(oldBundle.toString());
                }
            }
        } catch (Exception e) {
            log.warn("Got exception while refreshing topicStats map", e);
        }
    }

    public PersistentOfflineTopicStats getOfflineTopicStat(DestinationName topicName) {
        return offlineTopicStatCache.get(topicName);
    }

    public void cacheOfflineTopicStats(DestinationName topicName, PersistentOfflineTopicStats offlineTopicStats) {
        offlineTopicStatCache.put(topicName, offlineTopicStats);
    }

    public void invalidateOfflineTopicStatCache(DestinationName topicName) {
        PersistentOfflineTopicStats removed = offlineTopicStatCache.remove(topicName);
        if (removed != null) {
            log.info("Removed cached offline topic stat for {} ", topicName.getPersistenceNamingEncoding());
        }
    }

    public Topic getTopicReference(String topic) throws Exception {
        CompletableFuture<Topic> future = topics.get(topic);
        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            return future.get();
        } else {
            return null;
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

    public List<Metrics> getDestinationMetrics() {
        return pulsarStats.getDestinationMetrics();
    }

    public Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsarStats.getBundleStats();
    }
    
    public Semaphore getLookupRequestSemaphore() {
        return lookupRequestSemaphore.get();
    }

    public void checkGC(int gcIntervalInSeconds) {
        topics.forEach((n, t) -> {
            PersistentTopic topic = (PersistentTopic) t.getNow(null);
            if (topic != null) {
                topic.checkGC(gcIntervalInSeconds);
            }
        });
    }

    public void checkMessageExpiry() {
        topics.forEach((n, t) -> {
            PersistentTopic topic = (PersistentTopic) t.getNow(null);
            if (topic != null) {
                topic.checkMessageExpiry();
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
        DestinationName destination = DestinationName.get(topic.getName());
        long backlogQuotaLimitInBytes = getBacklogQuotaManager().getBacklogQuotaLimit(destination.getNamespace());
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
        topics.forEach((n, t) -> {
            try {
                PersistentTopic topic = (PersistentTopic) t.getNow(null);
                if (topic != null && isBacklogExceeded(topic)) {
                    getBacklogQuotaManager().handleExceededBacklogQuota(topic);
                } else if (topic == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("topic is null ");
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("quota not exceeded for [{}]", topic.getName());
                    }
                }
            } catch (Exception xle) {
                log.warn("Backlog quota monitoring encountered :" + xle.getLocalizedMessage());
            }
        });

    }

    void checkTopicNsOwnership(final String topic) throws RuntimeException {
        DestinationName destination = DestinationName.get(topic);
        boolean ownedByThisInstance;
        try {
            ownedByThisInstance = pulsar.getNamespaceService().isServiceUnitOwned(destination);
        } catch (Exception e) {
            log.debug(String.format("Failed to check the ownership of the destination: %s", destination), e);
            throw new RuntimeException(new ServerMetadataException(e));
        }

        if (!ownedByThisInstance) {
            String msg = String.format("Namespace not served by this instance. Please redo the lookup. "
                    + "Request is denied: namespace=%s", destination.getNamespace());
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
            DestinationName topicName = DestinationName.get(name);
            if (serviceUnit.includes(topicName)) {
                // Topic needs to be unloaded
                log.info("[{}] Unloading topic", topicName);
                closeFutures.add(topicFuture.thenCompose(Topic::close));
            }
        });
        CompletableFuture<Void> aggregator = FutureUtil.waitForAll(closeFutures);
        aggregator.thenAccept(res -> result.complete(closeFutures.size())).exceptionally(ex -> {
            result.completeExceptionally(ex);
            return null;
        });
        return result;
    }

    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }

    public void removeTopicFromCache(String topic) {
        try {
            DestinationName destination = DestinationName.get(topic);
            NamespaceBundle namespaceBundle = pulsar.getNamespaceService().getBundle(destination);
            checkArgument(namespaceBundle instanceof NamespaceBundle);

            String bundleName = namespaceBundle.toString();
            String namespaceName = destination.getNamespaceObject().toString();

            synchronized (multiLayerTopicsMap) {
                ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, PersistentTopic>> namespaceMap = multiLayerTopicsMap
                        .get(namespaceName);
                ConcurrentOpenHashMap<String, PersistentTopic> bundleMap = namespaceMap.get(bundleName);
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
            log.warn("Got exception when retrieving bundle name during removeTopicFromCache", e);
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

    public ConcurrentOpenHashMap<String, CompletableFuture<Topic>> getTopics() {
        return topics;
    }

    @Override
    public void onUpdate(String path, Policies data, Stat stat) {
        final NamespaceName namespace = new NamespaceName(NamespaceBundleFactory.getNamespaceFromPoliciesPath(path));

        log.info("Updated {}", path);

        topics.forEach((name, topicFuture) -> {
            if (namespace.includes(DestinationName.get(name))) {
                // If the topic is already created, immediately apply the updated policies, otherwise once the topic is
                // created it'll apply the policies update
                topicFuture.thenAccept(topic -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Notifying topic that policies have changed: {}", name);
                    }
                    topic.onPoliciesUpdate(data);
                });
            }
        });
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
        return authorizationManager != null;
    }

    public int getKeepAliveIntervalSeconds() {
        return keepAliveIntervalSeconds;
    }

    public String generateUniqueProducerName() {
        return producerNameGenerator.getNextId();
    }

    public Map<String, PersistentTopicStats> getTopicStats() {
        HashMap<String, PersistentTopicStats> stats = new HashMap<>();
        topics.forEach((name, topicFuture) -> {
            PersistentTopic currentTopic = (PersistentTopic) topicFuture.getNow(null);
            if (currentTopic != null) {
                stats.put(name, currentTopic.getStats());
            }
        });
        return stats;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public List<PersistentTopic> getAllTopicsFromNamespaceBundle(String namespace, String bundle) {
        return multiLayerTopicsMap.get(namespace).get(bundle).values();
    }
    
    public ZooKeeperDataCache<Map<String, String>> getDynamicConfigurationCache() {
        return dynamicConfigurationCache;
    }

    /**
     * Update dynamic-ServiceConfiguration with value present into zk-configuration-map and register listeners on
     * dynamic-ServiceConfiguration field to take appropriate action on change of zk-configuration-map.
     */
    private void updateConfigurationAndRegisterListeners() {
        // update ServiceConfiguration value by reading zk-configuration-map
        updateDynamicServiceConfiguration();
        // add listener on "maxConcurrentLookupRequest" value change
        registerConfigurationListener("maxConcurrentLookupRequest",
                (pendingLookupRequest) -> lookupRequestSemaphore.set(new Semaphore((int) pendingLookupRequest, true)));
        // add more listeners here
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
        configRegisteredListeners.put(configKey, listener);
        dynamicConfigurationCache.registerListener(new ZooKeeperCacheListener<Map<String, String>>() {
            @SuppressWarnings("unchecked")
            @Override
            public void onUpdate(String path, Map<String, String> data, Stat stat) {
                if (BROKER_SERVICE_CONFIGURATION_PATH.equalsIgnoreCase(path) && data != null
                        && data.containsKey(configKey)) {
                    log.info("Updating configuration {}/{}", configKey, data.get(configKey));
                    listener.accept((T) FieldParser.value(data.get(configKey), dynamicConfigurationMap.get(configKey)));
                }
            }
        });
    }

    private void updateDynamicServiceConfiguration() {
        try {
            Optional<Map<String, String>> data = dynamicConfigurationCache.get(BROKER_SERVICE_CONFIGURATION_PATH);
            if (data.isPresent() && data.get() != null) {
                data.get().forEach((key,value)-> {
                    try {
                        Field field = ServiceConfiguration.class.getDeclaredField(key);
                        if (field != null && field.isAnnotationPresent(FieldContext.class)) {
                            field.setAccessible(true);
                            field.set(pulsar().getConfiguration(), FieldParser.value(value,field));
                            log.info("Successfully updated {}/{}", key, value);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to update service configuration {}/{}, {}",key,value,e.getMessage());
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
                            Field configField = dynamicConfigurationMap.get(configKey);
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
        } catch (Exception e) {
            log.warn("Failed to read zookeeper path [{}]:", BROKER_SERVICE_CONFIGURATION_PATH, e);
        }
    }
    
    public static ConcurrentOpenHashMap<String, Field> getDynamicConfigurationMap() {
        return dynamicConfigurationMap;
    }

    private static ConcurrentOpenHashMap<String, Field> prepareDynamicConfigurationMap() {
        ConcurrentOpenHashMap<String, Field> dynamicConfigurationMap = new ConcurrentOpenHashMap<>();
        for (Field field : ServiceConfiguration.class.getDeclaredFields()) {
            if (field != null && field.isAnnotationPresent(FieldContext.class)) {
                field.setAccessible(true);
                if (((FieldContext) field.getAnnotation(FieldContext.class)).dynamic()) {
                    dynamicConfigurationMap.put(field.getName(), field);
                }
            }
        }
        return dynamicConfigurationMap;
    }

}
