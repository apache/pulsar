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
import static org.apache.pulsar.common.events.EventsTopicNames.checkTopicIsEventsNames;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.ws.rs.core.Response;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerLoader;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SystemTopic;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.prometheus.metrics.ObserverGauge;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FieldParser;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.zookeeper.ZooKeeperCacheListener;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter(AccessLevel.PUBLIC)
@Setter(AccessLevel.PROTECTED)
public class BrokerService implements Closeable, ZooKeeperCacheListener<Policies> {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);
    private static final Duration FUTURE_DEADLINE_TIMEOUT_DURATION = Duration.ofSeconds(60);
    private static final TimeoutException FUTURE_DEADLINE_TIMEOUT_EXCEPTION =
            FutureUtil.createTimeoutException("Future didn't finish within deadline", BrokerService.class,
                    "futureWithDeadline(...)");
    private static final TimeoutException FAILED_TO_LOAD_TOPIC_TIMEOUT_EXCEPTION =
            FutureUtil.createTimeoutException("Failed to load topic within timeout", BrokerService.class,
                    "futureWithDeadline(...)");
    private static final long GRACEFUL_SHUTDOWN_QUIET_PERIOD_MAX_MS = 5000L;
    private static final double GRACEFUL_SHUTDOWN_QUIET_PERIOD_RATIO_OF_TOTAL_TIMEOUT = 0.25d;
    private static final double GRACEFUL_SHUTDOWN_TIMEOUT_RATIO_OF_TOTAL_TIMEOUT = 0.5d;

    private final PulsarService pulsar;
    private final ManagedLedgerFactory managedLedgerFactory;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics;

    private final ConcurrentOpenHashMap<String, PulsarClient> replicationClients;
    private final ConcurrentOpenHashMap<String, PulsarAdmin> clusterAdmins;

    // Multi-layer topics map:
    // Namespace --> Bundle --> topicName --> topic
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>>>
            multiLayerTopicsMap;
    private int numberOfNamespaceBundles = 0;

    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;
    private final OrderedExecutor topicOrderedExecutor;
    // offline topic backlog cache
    private final ConcurrentOpenHashMap<TopicName, PersistentOfflineTopicStats> offlineTopicStatCache;
    private static final ConcurrentOpenHashMap<String, ConfigField> dynamicConfigurationMap =
            prepareDynamicConfigurationMap();
    private final ConcurrentOpenHashMap<String, Consumer<?>> configRegisteredListeners;

    private final ConcurrentLinkedQueue<Pair<String, CompletableFuture<Optional<Topic>>>> pendingTopicLoadingQueue;

    private AuthorizationService authorizationService = null;
    private final ScheduledExecutorService statsUpdater;
    private final ScheduledExecutorService backlogQuotaChecker;

    protected final AtomicReference<Semaphore> lookupRequestSemaphore;
    protected final AtomicReference<Semaphore> topicLoadRequestSemaphore;

    private final ObserverGauge pendingLookupRequests;
    private final ObserverGauge pendingTopicLoadRequests;

    private final ScheduledExecutorService inactivityMonitor;
    private final ScheduledExecutorService messageExpiryMonitor;
    private final ScheduledExecutorService compactionMonitor;
    private final ScheduledExecutorService consumedLedgersMonitor;
    private ScheduledExecutorService topicPublishRateLimiterMonitor;
    private ScheduledExecutorService brokerPublishRateLimiterMonitor;
    private ScheduledExecutorService deduplicationSnapshotMonitor;
    protected volatile PublishRateLimiter brokerPublishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;

    private DistributedIdGenerator producerNameGenerator;

    public static final String PRODUCER_NAME_GENERATOR_PATH = "/counters/producer-name";

    private final BacklogQuotaManager backlogQuotaManager;

    private final int keepAliveIntervalSeconds;
    private final PulsarStats pulsarStats;
    private final AuthenticationService authenticationService;

    public static final String BROKER_SERVICE_CONFIGURATION_PATH = "/admin/configuration";
    public static final String MANAGED_LEDGER_PATH_ZNODE = "/managed-ledgers";

    private final ZooKeeperDataCache<Map<String, String>> dynamicConfigurationCache;

    private static final LongAdder totalUnackedMessages = new LongAdder();
    private final int maxUnackedMessages;
    public final int maxUnackedMsgsPerDispatcher;
    private static final AtomicBoolean blockedDispatcherOnHighUnackedMsgs = new AtomicBoolean(false);
    private final ConcurrentOpenHashSet<PersistentDispatcherMultipleConsumers> blockedDispatchers;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final DelayedDeliveryTrackerFactory delayedDeliveryTrackerFactory;
    private final ServerBootstrap defaultServerBootstrap;

    private Channel listenChannel;
    private Channel listenChannelTls;

    private boolean preciseTopicPublishRateLimitingEnable;
    private final LongAdder pausedConnections = new LongAdder();
    private BrokerInterceptor interceptor;

    private Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

    public BrokerService(PulsarService pulsar, EventLoopGroup eventLoopGroup) throws Exception {
        this.pulsar = pulsar;
        this.preciseTopicPublishRateLimitingEnable =
                pulsar.getConfiguration().isPreciseTopicPublishRateLimiterEnable();
        this.managedLedgerFactory = pulsar.getManagedLedgerFactory();
        this.topics = new ConcurrentOpenHashMap<>();
        this.replicationClients = new ConcurrentOpenHashMap<>();
        this.clusterAdmins = new ConcurrentOpenHashMap<>();
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

        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(
                pulsar.getConfiguration().getNumAcceptorThreads(), false, acceptorThreadFactory);
        this.workerGroup = eventLoopGroup;
        this.statsUpdater = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-stats-updater"));
        this.authorizationService = new AuthorizationService(
                pulsar.getConfiguration(), pulsar.getConfigurationCache());

        if (pulsar.getConfigurationCache() != null) {
            pulsar.getConfigurationCache().policiesCache().registerListener(this);
        }

        this.inactivityMonitor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-inactivity-monitor"));
        this.messageExpiryMonitor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-msg-expiry-monitor"));
        this.compactionMonitor =
                Executors.newSingleThreadScheduledExecutor(
                        new DefaultThreadFactory("pulsar-compaction-monitor"));
        this.consumedLedgersMonitor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("consumed-Ledgers-monitor"));

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
            pulsar.getExecutor().scheduleAtFixedRate(() -> checkUnAckMessageDispatching(),
                    600, 30, TimeUnit.SECONDS);
        } else {
            this.maxUnackedMessages = 0;
            this.maxUnackedMsgsPerDispatcher = 0;
            log.info(
                    "Disabling per broker unack-msg blocking due invalid"
                            + " unAckMsgSubscriptionPercentageLimitOnBrokerBlocked {} ",
                    pulsar.getConfiguration().getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked());
        }

        this.delayedDeliveryTrackerFactory = DelayedDeliveryTrackerLoader
                .loadDelayedDeliveryTrackerFactory(pulsar.getConfiguration());

        this.defaultServerBootstrap = defaultServerBootstrap();

        this.pendingLookupRequests = ObserverGauge.build("pulsar_broker_lookup_pending_requests", "-")
                .supplier(() -> pulsar.getConfig().getMaxConcurrentLookupRequest()
                        - lookupRequestSemaphore.get().availablePermits())
                .register();

        this.pendingTopicLoadRequests = ObserverGauge.build(
                "pulsar_broker_topic_load_pending_requests", "-")
                .supplier(() -> pulsar.getConfig().getMaxConcurrentTopicLoadRequest()
                        - topicLoadRequestSemaphore.get().availablePermits())
                .register();

        this.brokerEntryMetadataInterceptors = BrokerEntryMetadataUtils
                .loadBrokerEntryMetadataInterceptors(pulsar.getConfiguration().getBrokerEntryMetadataInterceptors(),
                        BrokerService.class.getClassLoader());
    }

    // This call is used for starting additional protocol handlers
    public void startProtocolHandlers(
        Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> protocolHandlers) {

        protocolHandlers.forEach((protocol, initializers) -> {
            initializers.forEach((address, initializer) -> {
                try {
                    startProtocolHandler(protocol, address, initializer);
                } catch (IOException e) {
                    log.error("{}", e.getMessage(), e.getCause());
                    throw new RuntimeException(e.getMessage(), e.getCause());
                }
            });
        });
    }

    private void startProtocolHandler(String protocol,
                                      SocketAddress address,
                                      ChannelInitializer<SocketChannel> initializer) throws IOException {
        ServerBootstrap bootstrap = defaultServerBootstrap.clone();
        bootstrap.childHandler(initializer);
        try {
            bootstrap.bind(address).sync();
        } catch (Exception e) {
            throw new IOException("Failed to bind protocol `" + protocol + "` on " + address, e);
        }
        log.info("Successfully bind protocol `{}` on {}", protocol, address);
    }

    private ServerBootstrap defaultServerBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));
        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);
        return bootstrap;
    }

    public void start() throws Exception {
        this.producerNameGenerator = new DistributedIdGenerator(pulsar.getCoordinationService(),
                PRODUCER_NAME_GENERATOR_PATH, pulsar.getConfiguration().getClusterName());

        ServerBootstrap bootstrap = defaultServerBootstrap.clone();

        ServiceConfiguration serviceConfig = pulsar.getConfiguration();

        bootstrap.childHandler(new PulsarChannelInitializer(pulsar, false));

        Optional<Integer> port = serviceConfig.getBrokerServicePort();
        if (port.isPresent()) {
            // Bind and start to accept incoming connections.
            InetSocketAddress addr = new InetSocketAddress(pulsar.getBindAddress(), port.get());
            try {
                listenChannel = bootstrap.bind(addr).sync().channel();
                log.info("Started Pulsar Broker service on {}", listenChannel.localAddress());
            } catch (Exception e) {
                throw new IOException("Failed to bind Pulsar broker on " + addr, e);
            }
        }

        Optional<Integer> tlsPort = serviceConfig.getBrokerServicePortTls();
        if (tlsPort.isPresent()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new PulsarChannelInitializer(pulsar, true));
            try {
                listenChannelTls = tlsBootstrap.bind(new InetSocketAddress(
                        pulsar.getBindAddress(), tlsPort.get())).sync()
                        .channel();
                log.info("Started Pulsar Broker TLS service on {} - TLS provider: {}", listenChannelTls.localAddress(),
                        SslContext.defaultServerProvider());
            } catch (Exception e) {
                throw new IOException(String.format("Failed to start Pulsar Broker TLS service on %s:%d",
                        pulsar.getBindAddress(), tlsPort.get()), e);
            }
        }

        // start other housekeeping functions
        this.startStatsUpdater(
                serviceConfig.getStatsUpdateInitialDelayInSecs(),
                serviceConfig.getStatsUpdateFrequencyInSecs());
        this.startInactivityMonitor();
        this.startMessageExpiryMonitor();
        this.startCompactionMonitor();
        this.startConsumedLedgersMonitor();
        this.startBacklogQuotaChecker();
        this.updateBrokerPublisherThrottlingMaxRate();
        this.startCheckReplicationPolicies();
        this.startDeduplicationSnapshotMonitor();
    }

    protected void startStatsUpdater(int statsUpdateInitailDelayInSecs, int statsUpdateFrequencyInSecs) {
        statsUpdater.scheduleAtFixedRate(safeRun(this::updateRates),
                statsUpdateInitailDelayInSecs, statsUpdateFrequencyInSecs, TimeUnit.SECONDS);

        // Ensure the broker starts up with initial stats
        updateRates();
    }

    protected void startDeduplicationSnapshotMonitor() {
        int interval = pulsar().getConfiguration().getBrokerDeduplicationSnapshotFrequencyInSeconds();
        if (interval > 0 && pulsar().getConfiguration().isBrokerDeduplicationEnabled()) {
            this.deduplicationSnapshotMonitor =
                    Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory(
                            "deduplication-snapshot-monitor"));
            deduplicationSnapshotMonitor.scheduleAtFixedRate(safeRun(() -> forEachTopic(
                    Topic::checkDeduplicationSnapshot))
                    , interval, interval, TimeUnit.SECONDS);
        }
    }

    protected void startInactivityMonitor() {
        if (pulsar().getConfiguration().isBrokerDeleteInactiveTopicsEnabled()) {
            int interval = pulsar().getConfiguration().getBrokerDeleteInactiveTopicsFrequencySeconds();
            inactivityMonitor.scheduleAtFixedRate(safeRun(() -> checkGC()), interval, interval,
                    TimeUnit.SECONDS);
        }

        // Deduplication info checker
        long duplicationCheckerIntervalInSeconds = TimeUnit.MINUTES
                .toSeconds(pulsar().getConfiguration().getBrokerDeduplicationProducerInactivityTimeoutMinutes()) / 3;
        inactivityMonitor.scheduleAtFixedRate(safeRun(this::checkMessageDeduplicationInfo),
                duplicationCheckerIntervalInSeconds,
                duplicationCheckerIntervalInSeconds, TimeUnit.SECONDS);

        // Inactive subscriber checker
        if (pulsar().getConfiguration().getSubscriptionExpiryCheckIntervalInMinutes() > 0) {
            long subscriptionExpiryCheckIntervalInSeconds =
                    TimeUnit.MINUTES.toSeconds(pulsar().getConfiguration()
                            .getSubscriptionExpiryCheckIntervalInMinutes());
            inactivityMonitor.scheduleAtFixedRate(safeRun(this::checkInactiveSubscriptions),
                    subscriptionExpiryCheckIntervalInSeconds,
                    subscriptionExpiryCheckIntervalInSeconds, TimeUnit.SECONDS);
        }
    }

    protected void startMessageExpiryMonitor() {
        int interval = pulsar().getConfiguration().getMessageExpiryCheckIntervalInMinutes();
        messageExpiryMonitor.scheduleAtFixedRate(safeRun(this::checkMessageExpiry), interval, interval,
                TimeUnit.MINUTES);
    }

    protected void startCheckReplicationPolicies() {
        int interval = pulsar.getConfig().getReplicationPolicyCheckDurationSeconds();
        if (interval > 0) {
            messageExpiryMonitor.scheduleAtFixedRate(safeRun(this::checkReplicationPolicies), interval, interval,
                    TimeUnit.SECONDS);
        }
    }

    protected void startCompactionMonitor() {
        int interval = pulsar().getConfiguration().getBrokerServiceCompactionMonitorIntervalInSeconds();
        if (interval > 0) {
            compactionMonitor.scheduleAtFixedRate(safeRun(() -> checkCompaction()),
                                                  interval, interval, TimeUnit.SECONDS);
        }
    }

    protected void startConsumedLedgersMonitor() {
        int interval = pulsar().getConfiguration().getRetentionCheckIntervalInSeconds();
        if (interval > 0) {
            consumedLedgersMonitor.scheduleAtFixedRate(safeRun(this::checkConsumedLedgers),
                                                            interval, interval, TimeUnit.SECONDS);
        }
    }

    protected void startBacklogQuotaChecker() {
        if (pulsar().getConfiguration().isBacklogQuotaCheckEnabled()) {
            final int interval = pulsar().getConfiguration().getBacklogQuotaCheckIntervalInSeconds();
            log.info("Scheduling a thread to check backlog quota after [{}] seconds in background", interval);
            backlogQuotaChecker.scheduleAtFixedRate(safeRun(this::monitorBacklogQuota), interval, interval,
                    TimeUnit.SECONDS);
        } else {
            log.info("Backlog quota check monitoring is disabled");
        }

    }

    /**
     * Schedules and monitors publish-throttling for all owned topics that has publish-throttling configured. It also
     * disables and shutdowns publish-rate-limiter monitor task if broker disables it.
     */
    public synchronized void setupTopicPublishRateLimiterMonitor() {
        // set topic PublishRateLimiterMonitor
        long topicTickTimeMs = pulsar().getConfiguration().getTopicPublisherThrottlingTickTimeMillis();
        if (topicTickTimeMs > 0) {
            if (this.topicPublishRateLimiterMonitor == null) {
                this.topicPublishRateLimiterMonitor = Executors.newSingleThreadScheduledExecutor(
                        new DefaultThreadFactory("pulsar-topic-publish-rate-limiter-monitor"));
                if (topicTickTimeMs > 0) {
                    // schedule task that sums up publish-rate across all cnx on a topic
                    topicPublishRateLimiterMonitor.scheduleAtFixedRate(safeRun(() -> checkTopicPublishThrottlingRate()),
                            topicTickTimeMs, topicTickTimeMs, TimeUnit.MILLISECONDS);
                    // schedule task that refreshes rate-limiting bucket
                    topicPublishRateLimiterMonitor.scheduleAtFixedRate(safeRun(() -> refreshTopicPublishRate()), 1, 1,
                            TimeUnit.SECONDS);
                }
            }
        } else {
            // disable publish-throttling for all topics
            if (this.topicPublishRateLimiterMonitor != null) {
                try {
                    this.topicPublishRateLimiterMonitor.awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.warn("failed to shutdown topicPublishRateLimiterMonitor", e);
                }
                // make sure topics are not being throttled
                refreshTopicPublishRate();
                this.topicPublishRateLimiterMonitor = null;
            }
        }
    }

    /**
     * Schedules and monitors publish-throttling for broker that has publish-throttling configured. It also
     * disables and shutdowns publish-rate-limiter monitor for broker task if broker disables it.
     */
    public synchronized void setupBrokerPublishRateLimiterMonitor() {
        // set broker PublishRateLimiterMonitor
        long brokerTickTimeMs = pulsar().getConfiguration().getBrokerPublisherThrottlingTickTimeMillis();
        if (brokerTickTimeMs > 0) {
            if (this.brokerPublishRateLimiterMonitor == null) {
                this.brokerPublishRateLimiterMonitor = Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("pulsar-broker-publish-rate-limiter-monitor"));
                if (brokerTickTimeMs > 0) {
                    // schedule task that sums up publish-rate across all cnx on a topic,
                    // and check the rate limit exceeded or not.
                    brokerPublishRateLimiterMonitor.scheduleAtFixedRate(
                        safeRun(() -> checkBrokerPublishThrottlingRate()),
                        brokerTickTimeMs,
                        brokerTickTimeMs,
                        TimeUnit.MILLISECONDS);
                    // schedule task that refreshes rate-limiting bucket
                    brokerPublishRateLimiterMonitor.scheduleAtFixedRate(
                        safeRun(() -> refreshBrokerPublishRate()),
                        1,
                        1,
                        TimeUnit.SECONDS);
                }
            }
        } else {
            // disable publish-throttling for broker.
            if (this.brokerPublishRateLimiterMonitor != null) {
                try {
                    this.brokerPublishRateLimiterMonitor.awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.warn("failed to shutdown brokerPublishRateLimiterMonitor", e);
                }
                // make sure topics are not being throttled
                refreshBrokerPublishRate();
                this.brokerPublishRateLimiterMonitor = null;
            }
        }
    }

    public void close() throws IOException {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new PulsarServerException(e.getCause());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public CompletableFuture<Void> closeAsync() {
        try {
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
                } catch (Exception e) {
                    log.warn("Error shutting down repl client for cluster {}", cluster, e);
                }
            });

            // close replication admins
            clusterAdmins.forEach((cluster, admin) -> {
                try {
                    admin.close();
                } catch (Exception e) {
                    log.warn("Error shutting down repl admin for cluster {}", cluster, e);
                }
            });

            CompletableFuture<CompletableFuture<Void>> cancellableDownstreamFutureReference = new CompletableFuture<>();
            log.info("Event loops shutting down gracefully...");
            CompletableFuture<Void> shutdownFuture =
                    CompletableFuture.allOf(shutdownEventLoopGracefully(acceptorGroup),
                            shutdownEventLoopGracefully(workerGroup))
                            .handle((v, t) -> {
                                if (t != null) {
                                    log.warn("Error shutting down event loops gracefully", t);
                                } else {
                                    log.info("Event loops shutdown completed.");
                                }
                                return null;
                            })
                            .thenCompose(__ -> {
                                log.info("Continuing to second phase in shutdown.");

                                List<CompletableFuture<Void>> asyncCloseFutures = new ArrayList<>();

                                if (listenChannel != null && listenChannel.isOpen()) {
                                    asyncCloseFutures.add(closeChannel(listenChannel));
                                }

                                if (listenChannelTls != null && listenChannelTls.isOpen()) {
                                    asyncCloseFutures.add(closeChannel(listenChannelTls));
                                }

                                if (interceptor != null) {
                                    interceptor.close();
                                    interceptor = null;
                                }

                                try {
                                    authenticationService.close();
                                } catch (IOException e) {
                                    log.warn("Error in closing authenticationService", e);
                                }
                                pulsarStats.close();
                                try {
                                    delayedDeliveryTrackerFactory.close();
                                } catch (IOException e) {
                                    log.warn("Error in closing delayedDeliveryTrackerFactory", e);
                                }

                                asyncCloseFutures.add(GracefulExecutorServicesShutdown
                                        .initiate()
                                        .timeout(
                                                Duration.ofMillis(
                                                        (long) (GRACEFUL_SHUTDOWN_TIMEOUT_RATIO_OF_TOTAL_TIMEOUT
                                                                * pulsar.getConfiguration()
                                                                .getBrokerShutdownTimeoutMs())))
                                        .shutdown(
                                                statsUpdater,
                                                inactivityMonitor,
                                                messageExpiryMonitor,
                                                compactionMonitor,
                                                consumedLedgersMonitor,
                                                backlogQuotaChecker,
                                                topicOrderedExecutor,
                                                topicPublishRateLimiterMonitor,
                                                brokerPublishRateLimiterMonitor,
                                                deduplicationSnapshotMonitor)
                                        .handle());

                                CompletableFuture<Void> combined =
                                        FutureUtil.waitForAllAndSupportCancel(asyncCloseFutures);
                                cancellableDownstreamFutureReference.complete(combined);
                                combined.handle((v, t) -> {
                                    if (t == null) {
                                        log.info("Broker service completely shut down");
                                    } else {
                                        if (t instanceof CancellationException) {
                                            log.warn("Broker service didn't complete gracefully. "
                                                    + "Terminating Broker service.");
                                        } else {
                                            log.warn("Broker service shut down completed with exception", t);
                                        }
                                    }
                                    return null;
                                });
                                return combined;
                            });
            FutureUtil.whenCancelledOrTimedOut(shutdownFuture, () -> cancellableDownstreamFutureReference
                    .thenAccept(future -> future.cancel(false)));
            return shutdownFuture;
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
    }

    CompletableFuture<Void> shutdownEventLoopGracefully(EventLoopGroup eventLoopGroup) {
        long brokerShutdownTimeoutMs = pulsar.getConfiguration().getBrokerShutdownTimeoutMs();
        long quietPeriod = Math.min((long) (
                GRACEFUL_SHUTDOWN_QUIET_PERIOD_RATIO_OF_TOTAL_TIMEOUT * brokerShutdownTimeoutMs),
                GRACEFUL_SHUTDOWN_QUIET_PERIOD_MAX_MS);
        long timeout = (long) (GRACEFUL_SHUTDOWN_TIMEOUT_RATIO_OF_TOTAL_TIMEOUT * brokerShutdownTimeoutMs);
        return NettyFutureUtil.toCompletableFutureVoid(
                eventLoopGroup.shutdownGracefully(quietPeriod,
                        timeout, TimeUnit.MILLISECONDS));
    }

    private CompletableFuture<Void> closeChannel(Channel channel) {
        return ChannelFutures.toCompletableFuture(channel.close())
                .handle((c, t) -> {
                    // log problem if closing of channel fails
                    // ignore RejectedExecutionException
                    if (t != null && !(t instanceof RejectedExecutionException)) {
                        log.warn("Cannot close channel {}", channel, t);
                    }
                    return null;
                });
    }

    /**
     * It unloads all owned namespacebundles gracefully.
     * <ul>
     * <li>First it makes current broker unavailable and isolates from the clusters so, it will not serve any new
     * requests.</li>
     * <li>Second it starts unloading namespace bundle one by one without closing the connection in order to avoid
     * disruption for other namespacebundles which are sharing the same connection from the same client.</li>
     * </ul>
     */
    public void unloadNamespaceBundlesGracefully() {
        try {
            log.info("Unloading namespace-bundles...");
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
                        pulsar.getNamespaceService().unloadNamespaceBundle(su, 1, TimeUnit.MINUTES).get();
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
        return getTopic(topic, isAllowAutoTopicCreation(topic)).thenApply(Optional::get);
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
                    // a non-existing topic in the cache shouldn't prevent creating a topic
                    if (createIfMissing) {
                        if (topicFuture.isDone() && topicFuture.getNow(Optional.empty()).isPresent()) {
                            return topicFuture;
                        } else {
                            return topicFuture.thenCompose(value -> {
                                if (!value.isPresent()) {
                                    // retry and create topic
                                    return getTopic(topic, createIfMissing);
                                } else {
                                    // in-progress future completed successfully
                                    return CompletableFuture.completedFuture(value);
                                }
                            });
                        }
                    } else {
                        return topicFuture;
                    }
                }
            }
            final boolean isPersistentTopic = TopicName.get(topic).getDomain().equals(TopicDomain.persistent);
            if (isPersistentTopic) {
                return topics.computeIfAbsent(topic, (topicName) -> {
                    return this.loadOrCreatePersistentTopic(topicName, createIfMissing);
                });
            } else {
                return topics.computeIfAbsent(topic, (name) -> {
                    final TopicName topicName = TopicName.get(name);
                    if (topicName.isPartitioned()) {
                        final TopicName partitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());
                        return this.fetchPartitionedTopicMetadataAsync(partitionedTopicName).thenCompose((metadata) -> {
                            if (topicName.getPartitionIndex() < metadata.partitions) {
                                return createNonPersistentTopic(name);
                            }
                            return CompletableFuture.completedFuture(Optional.empty());
                        });
                    } else if (createIfMissing) {
                        return createNonPersistentTopic(name);
                    } else {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                });
            }
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Illegalargument exception when loading topic", topic, e);
            return FutureUtil.failedFuture(e);
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ServiceUnitNotReadyException) {
                log.warn("[{}] Service unit is not ready when loading the topic", topic);
            } else {
                log.warn("[{}] Unexpected exception when loading topic: {}", topic, e.getMessage(), e);
            }

            return FutureUtil.failedFuture(cause);
        }
    }

    public CompletableFuture<SchemaVersion> deleteSchemaStorage(String topic) {
        Optional<Topic> optTopic = getTopicReference(topic);
        if (optTopic.isPresent()) {
            return optTopic.get().deleteSchema();
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<Void> deleteTopic(String topic, boolean forceDelete) {
        return deleteTopic(topic, forceDelete, false);
    }

    public CompletableFuture<Void> deleteTopic(String topic, boolean forceDelete, boolean deleteSchema) {
        Optional<Topic> optTopic = getTopicReference(topic);
        if (optTopic.isPresent()) {
            Topic t = optTopic.get();
            if (forceDelete) {
                if (deleteSchema) {
                    return t.deleteSchema().thenCompose(schemaVersion -> {
                        log.info("Successfully delete topic {}'s schema of version {}", t.getName(), schemaVersion);
                        return t.deleteForcefully();
                    });
                } else {
                    return t.deleteForcefully();
                }
            }

            // v2 topics have a global name so check if the topic is replicated.
            if (t.isReplicated()) {
                // Delete is disallowed on global topic
                final List<String> clusters = t.getReplicators().keys();
                log.error("Delete forbidden topic {} is replicated on clusters {}", topic, clusters);
                return FutureUtil.failedFuture(
                        new IllegalStateException("Delete forbidden topic is replicated on clusters " + clusters));
            }

            if (deleteSchema) {
                return t.deleteSchema().thenCompose(schemaVersion -> {
                    log.info("Successfully delete topic {}'s schema of version {}", t.getName(), schemaVersion);
                    return t.delete();
                });
            } else {
                return t.delete();
            }
        }

        // Topic is not loaded, though we still might be able to delete from metadata
        TopicName tn = TopicName.get(topic);
        if (!tn.isPersistent()) {
            // Nothing to do if it's not persistent
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        managedLedgerFactory.asyncDelete(tn.getPersistenceNamingEncoding(), new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    private CompletableFuture<Optional<Topic>> createNonPersistentTopic(String topic) {
        CompletableFuture<Optional<Topic>> topicFuture = futureWithDeadline();

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

    private <T> CompletableFuture<T> futureWithDeadline() {
        return FutureUtil.createFutureWithTimeout(FUTURE_DEADLINE_TIMEOUT_DURATION, executor(),
                () -> FUTURE_DEADLINE_TIMEOUT_EXCEPTION);
    }

    public PulsarClient getReplicationClient(String cluster) {
        PulsarClient client = replicationClients.get(cluster);
        if (client != null) {
            return client;
        }

        return replicationClients.computeIfAbsent(cluster, key -> {
            try {
                String path = PulsarWebResource.path("clusters", cluster);
                ClusterDataImpl data = this.pulsar.getConfigurationCache().clustersCache().get(path)
                        .orElseThrow(() -> new KeeperException.NoNodeException(path));
                ClientBuilder clientBuilder = PulsarClient.builder()
                        .enableTcpNoDelay(false)
                        .connectionsPerBroker(pulsar.getConfiguration().getReplicationConnectionsPerBroker())
                        .statsInterval(0, TimeUnit.SECONDS);
                if (data.getAuthenticationPlugin() != null && data.getAuthenticationParameters() != null) {
                    clientBuilder.authentication(data.getAuthenticationPlugin(), data.getAuthenticationParameters());
                } else if (pulsar.getConfiguration().isAuthenticationEnabled()) {
                    clientBuilder.authentication(pulsar.getConfiguration().getBrokerClientAuthenticationPlugin(),
                            pulsar.getConfiguration().getBrokerClientAuthenticationParameters());
                }
                String serviceUrlTls = isNotBlank(data.getBrokerServiceUrlTls()) ? data.getBrokerServiceUrlTls()
                        : data.getServiceUrlTls();
                if (data.isBrokerClientTlsEnabled()) {
                    configTlsSettings(clientBuilder, serviceUrlTls,
                            data.isBrokerClientTlsEnabledWithKeyStore(), data.isTlsAllowInsecureConnection(),
                            data.getBrokerClientTlsTrustStoreType(), data.getBrokerClientTlsTrustStore(),
                            data.getBrokerClientTlsTrustStorePassword(), data.getBrokerClientTrustCertsFilePath());
                } else if (pulsar.getConfiguration().isBrokerClientTlsEnabled()) {
                    configTlsSettings(clientBuilder, serviceUrlTls,
                            pulsar.getConfiguration().isBrokerClientTlsEnabledWithKeyStore(),
                            pulsar.getConfiguration().isTlsAllowInsecureConnection(),
                            pulsar.getConfiguration().getBrokerClientTlsTrustStoreType(),
                            pulsar.getConfiguration().getBrokerClientTlsTrustStore(),
                            pulsar.getConfiguration().getBrokerClientTlsTrustStorePassword(),
                            pulsar.getConfiguration().getBrokerClientTrustCertsFilePath());
                } else {
                    clientBuilder.serviceUrl(
                            isNotBlank(data.getBrokerServiceUrl()) ? data.getBrokerServiceUrl() : data.getServiceUrl());
                }
                if (data.getProxyProtocol() != null && StringUtils.isNotBlank(data.getProxyServiceUrl())) {
                    clientBuilder.proxyServiceUrl(data.getProxyServiceUrl(), data.getProxyProtocol());
                    log.info("Configuring proxy-url {} with protocol {}", data.getProxyServiceUrl(),
                            data.getProxyProtocol());
                }
                if (data.getListenerName() != null && StringUtils.isNotBlank(data.getListenerName())) {
                    clientBuilder.listenerName(data.getListenerName());
                    log.info("Configuring listenerName {}", data.getListenerName());
                }
                // Share all the IO threads across broker and client connections
                ClientConfigurationData conf = ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();
                return new PulsarClientImpl(conf, workerGroup);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void configTlsSettings(ClientBuilder clientBuilder, String serviceUrl,
                                   boolean brokerClientTlsEnabledWithKeyStore, boolean isTlsAllowInsecureConnection,
                                   String brokerClientTlsTrustStoreType, String brokerClientTlsTrustStore,
                                   String brokerClientTlsTrustStorePassword, String brokerClientTrustCertsFilePath) {
        clientBuilder
                .serviceUrl(serviceUrl)
                .enableTls(true)
                .allowTlsInsecureConnection(isTlsAllowInsecureConnection);
        if (brokerClientTlsEnabledWithKeyStore) {
            clientBuilder.useKeyStoreTls(true)
                    .tlsTrustStoreType(brokerClientTlsTrustStoreType)
                    .tlsTrustStorePath(brokerClientTlsTrustStore)
                    .tlsTrustStorePassword(brokerClientTlsTrustStorePassword);
        } else {
            clientBuilder.tlsTrustCertsFilePath(brokerClientTrustCertsFilePath);
        }
    }

    public PulsarAdmin getClusterPulsarAdmin(String cluster) {
        PulsarAdmin admin = clusterAdmins.get(cluster);
        if (admin != null) {
            return admin;
        }
        return clusterAdmins.computeIfAbsent(cluster, key -> {
            try {
                String path = PulsarWebResource.path("clusters", cluster);
                ClusterDataImpl data = this.pulsar.getConfigurationCache().clustersCache().get(path)
                        .orElseThrow(() -> new KeeperException.NoNodeException(path));

                ServiceConfiguration conf = pulsar.getConfig();

                boolean isTlsUrl = conf.isBrokerClientTlsEnabled() && isNotBlank(data.getServiceUrlTls());
                String adminApiUrl = isTlsUrl ? data.getServiceUrlTls() : data.getServiceUrl();
                PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl(adminApiUrl)
                        .authentication(
                                conf.getBrokerClientAuthenticationPlugin(),
                                conf.getBrokerClientAuthenticationParameters());

                if (isTlsUrl) {
                    builder.allowTlsInsecureConnection(conf.isTlsAllowInsecureConnection());
                    if (conf.isBrokerClientTlsEnabledWithKeyStore()) {
                        builder.useKeyStoreTls(true)
                                .tlsTrustStoreType(conf.getBrokerClientTlsTrustStoreType())
                                .tlsTrustStorePath(conf.getBrokerClientTlsTrustStore())
                                .tlsTrustStorePassword(conf.getBrokerClientTlsTrustStorePassword());
                    } else {
                        builder.tlsTrustCertsFilePath(conf.getBrokerClientTrustCertsFilePath());
                    }
                }

                // most of the admin request requires to make zk-call so, keep the max read-timeout based on
                // zk-operation timeout
                builder.readTimeout(conf.getZooKeeperOperationTimeoutSeconds(), TimeUnit.SECONDS);

                PulsarAdmin adminClient = builder.build();
                log.info("created admin with url {} ", adminApiUrl);
                return adminClient;
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
        final CompletableFuture<Optional<Topic>> topicFuture = FutureUtil.createFutureWithTimeout(
                Duration.ofSeconds(pulsar.getConfiguration().getTopicLoadTimeoutSeconds()), executor(),
                () -> FAILED_TO_LOAD_TOPIC_TIMEOUT_EXCEPTION);
        if (!pulsar.getConfiguration().isEnablePersistentTopics()) {
            if (log.isDebugEnabled()) {
                log.debug("Broker is unable to load persistent topic {}", topic);
            }
            topicFuture.completeExceptionally(new NotAllowedException(
                    "Broker is not unable to load persistent topic"));
            return topicFuture;
        }

        checkTopicNsOwnershipAsync(topic).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                topicFuture.completeExceptionally(throwable);
                return;
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
                pendingTopicLoadingQueue.add(new ImmutablePair<>(topic, topicFuture));
                if (log.isDebugEnabled()) {
                    log.debug("topic-loading for {} added into pending queue", topic);
                }
            }
        });

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

        if (createIfMissing && !checkMaxTopicsPerNamespace(topicName, 1, topicFuture)) {
            return;
        }

        getManagedLedgerConfig(topicName).thenAccept(managedLedgerConfig -> {

            if (isBrokerEntryMetadataEnabled()) {
                // init managedLedger interceptor
                Set<BrokerEntryMetadataInterceptor> interceptors = new HashSet<>();
                for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
                    // add individual AppendOffsetMetadataInterceptor for each topic
                    if (interceptor instanceof AppendIndexMetadataInterceptor) {
                        interceptors.add(new AppendIndexMetadataInterceptor());
                    } else {
                        interceptors.add(interceptor);
                    }
                }
                managedLedgerConfig.setManagedLedgerInterceptor(new ManagedLedgerInterceptorImpl(interceptors));
            }

            managedLedgerConfig.setCreateIfMissing(createIfMissing);

            // Once we have the configuration, we can proceed with the async open operation
            managedLedgerFactory.asyncOpen(topicName.getPersistenceNamingEncoding(), managedLedgerConfig,
                    new OpenLedgerCallback() {
                        @Override
                        public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                            try {
                                PersistentTopic persistentTopic = isSystemTopic(topic)
                                        ? new SystemTopic(topic, ledger, BrokerService.this)
                                        : new PersistentTopic(topic, ledger, BrokerService.this);
                                CompletableFuture<Void> preCreateSubForCompaction =
                                        CompletableFuture.completedFuture(null);
                                if (persistentTopic instanceof SystemTopic) {
                                    preCreateSubForCompaction = ((SystemTopic) persistentTopic)
                                            .preCreateSubForCompactionIfNeeded();
                                }
                                CompletableFuture<Void> replicationFuture = persistentTopic.checkReplication();
                                FutureUtil.waitForAll(Lists.newArrayList(preCreateSubForCompaction, replicationFuture))
                                .thenCompose(v -> {
                                    // Also check dedup status
                                    return persistentTopic.checkDeduplicationStatus();
                                }).thenRun(() -> {
                                    log.info("Created topic {} - dedup is {}", topic,
                                            persistentTopic.isDeduplicationEnabled() ? "enabled" : "disabled");
                                    long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
                                            - topicCreateTimeMs;
                                    pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
                                    if (topicFuture.isCompletedExceptionally()) {
                                        log.warn("{} future is already completed with failure {}, closing the topic",
                                                topic, FutureUtil.getException(topicFuture));
                                        persistentTopic.stopReplProducers().whenComplete((v, exception) -> {
                                            topics.remove(topic, topicFuture);
                                        });
                                    } else {
                                        addTopicToStatsMaps(topicName, persistentTopic);
                                        topicFuture.complete(Optional.of(persistentTopic));
                                    }
                                }).exceptionally((ex) -> {
                                    log.warn(
                                            "Replication or dedup check failed."
                                                    + " Removing topic from topics list {}, {}",
                                            topic, ex);
                                    persistentTopic.stopReplProducers().whenComplete((v, exception) -> {
                                        topics.remove(topic, topicFuture);
                                        topicFuture.completeExceptionally(ex);
                                    });

                                    return null;
                                });
                            } catch (NamingException | PulsarServerException e) {
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
                    }, () -> isTopicNsOwnedByBroker(topicName), null);

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
        CompletableFuture<ManagedLedgerConfig> future = futureWithDeadline();
        // Execute in background thread, since getting the policies might block if the z-node wasn't already cached
        pulsar.getOrderedExecutor().executeOrdered(topicName, safeRun(() -> {
            NamespaceName namespace = topicName.getNamespaceObject();
            ServiceConfiguration serviceConfig = pulsar.getConfiguration();

            // Get persistence policy for this topic
            Optional<Policies> policies = Optional.empty();
            Optional<LocalPolicies> localPolicies = Optional.empty();

            PersistencePolicies persistencePolicies = null;
            RetentionPolicies retentionPolicies = null;
            OffloadPoliciesImpl topicLevelOffloadPolicies = null;

            if (pulsar.getConfig().isTopicLevelPoliciesEnabled()) {
                TopicName cloneTopicName = topicName;
                if (topicName.isPartitioned()) {
                    cloneTopicName = TopicName.get(topicName.getPartitionedTopicName());
                }
                try {
                    TopicPolicies topicPolicies = pulsar.getTopicPoliciesService().getTopicPolicies(cloneTopicName);
                    if (topicPolicies != null) {
                        persistencePolicies = topicPolicies.getPersistence();
                        retentionPolicies = topicPolicies.getRetentionPolicies();
                        topicLevelOffloadPolicies = topicPolicies.getOffloadPolicies();
                    }
                } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
                    log.debug("Topic {} policies have not been initialized yet.", topicName);
                }
            }

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

            if (persistencePolicies == null) {
                persistencePolicies = policies.map(p -> p.persistence).orElseGet(
                        () -> new PersistencePolicies(serviceConfig.getManagedLedgerDefaultEnsembleSize(),
                                serviceConfig.getManagedLedgerDefaultWriteQuorum(),
                                serviceConfig.getManagedLedgerDefaultAckQuorum(),
                                serviceConfig.getManagedLedgerDefaultMarkDeleteRateLimit()));
            }

            if (retentionPolicies == null) {
                retentionPolicies = policies.map(p -> p.retention_policies).orElseGet(
                        () -> new RetentionPolicies(serviceConfig.getDefaultRetentionTimeInMinutes(),
                                serviceConfig.getDefaultRetentionSizeInMB())
                );
            }

            ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
            managedLedgerConfig.setEnsembleSize(persistencePolicies.getBookkeeperEnsemble());
            managedLedgerConfig.setWriteQuorumSize(persistencePolicies.getBookkeeperWriteQuorum());
            managedLedgerConfig.setAckQuorumSize(persistencePolicies.getBookkeeperAckQuorum());
            if (localPolicies.isPresent() && localPolicies.get().bookieAffinityGroup != null) {
                managedLedgerConfig
                        .setBookKeeperEnsemblePlacementPolicyClassName(ZkIsolatedBookieEnsemblePlacementPolicy.class);
                Map<String, Object> properties = Maps.newHashMap();
                properties.put(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                        localPolicies.get().bookieAffinityGroup.getBookkeeperAffinityGroupPrimary());
                properties.put(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                        localPolicies.get().bookieAffinityGroup.getBookkeeperAffinityGroupSecondary());
                managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyProperties(properties);
            }
            managedLedgerConfig.setThrottleMarkDelete(persistencePolicies.getManagedLedgerMaxMarkDeleteRate());
            managedLedgerConfig.setDigestType(serviceConfig.getManagedLedgerDigestType());
            managedLedgerConfig.setPassword(serviceConfig.getManagedLedgerPassword());

            managedLedgerConfig.setMaxUnackedRangesToPersist(serviceConfig.getManagedLedgerMaxUnackedRangesToPersist());
            managedLedgerConfig.setMaxUnackedRangesToPersistInZk(
                    serviceConfig.getManagedLedgerMaxUnackedRangesToPersistInZooKeeper());
            managedLedgerConfig.setMaxEntriesPerLedger(serviceConfig.getManagedLedgerMaxEntriesPerLedger());
            managedLedgerConfig.setMinimumRolloverTime(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            managedLedgerConfig.setMaximumRolloverTime(serviceConfig.getManagedLedgerMaxLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            managedLedgerConfig.setMaxSizePerLedgerMb(serviceConfig.getManagedLedgerMaxSizePerLedgerMbytes());

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
            managedLedgerConfig.setAutoSkipNonRecoverableData(serviceConfig.isAutoSkipNonRecoverableData());
            managedLedgerConfig.setLazyCursorRecovery(serviceConfig.isLazyCursorRecovery());

            OffloadPoliciesImpl nsLevelOffloadPolicies =
                    (OffloadPoliciesImpl) policies.map(p -> p.offload_policies).orElse(null);
            OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.mergeConfiguration(
                    topicLevelOffloadPolicies,
                    OffloadPoliciesImpl.oldPoliciesCompatible(nsLevelOffloadPolicies, policies.orElse(null)),
                    getPulsar().getConfig().getProperties());
            if (topicLevelOffloadPolicies != null) {
                try {
                    LedgerOffloader topicLevelLedgerOffLoader = pulsar().createManagedLedgerOffloader(offloadPolicies);
                    managedLedgerConfig.setLedgerOffloader(topicLevelLedgerOffLoader);
                } catch (PulsarServerException e) {
                    future.completeExceptionally(e);
                    return;
                }
            } else {
                //If the topic level policy is null, use the namespace level
                managedLedgerConfig.setLedgerOffloader(pulsar.getManagedLedgerOffloader(namespace, offloadPolicies));
            }

            managedLedgerConfig.setDeletionAtBatchIndexLevelEnabled(
                    serviceConfig.isAcknowledgmentAtBatchIndexLevelEnabled());
            managedLedgerConfig.setNewEntriesCheckDelayInMillis(
                    serviceConfig.getManagedLedgerNewEntriesCheckDelayInMillis());


            future.complete(managedLedgerConfig);
        }, (exception) -> future.completeExceptionally(exception)));

        return future;
    }

    private void addTopicToStatsMaps(TopicName topicName, Topic topic) {
        pulsar.getNamespaceService().getBundleAsync(topicName)
                .thenAccept(namespaceBundle -> {
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
                })
                .exceptionally(ex -> {
                    log.warn("Got exception when retrieving bundle name during create persistent topic", ex);
                    return null;
                });
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

            Summary.rotateLatencyCollection();
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

    public void checkGC() {
        forEachTopic(Topic::checkGC);
    }

    public void checkMessageExpiry() {
        forEachTopic(Topic::checkMessageExpiry);
    }

    public void checkReplicationPolicies() {
        forEachTopic(Topic::checkReplication);
    }

    public void checkCompaction() {
        forEachTopic((t) -> {
                if (t instanceof PersistentTopic) {
                    ((PersistentTopic) t).checkCompaction();
                }
            });
    }

    private void checkConsumedLedgers() {
        forEachTopic((t) -> {
            if (t instanceof PersistentTopic) {
                Optional.ofNullable(((PersistentTopic) t).getManagedLedger()).ifPresent(
                        managedLedger -> {
                            managedLedger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
                        }
                );
            }
        });
    }

    public void checkMessageDeduplicationInfo() {
        forEachTopic(Topic::checkMessageDeduplicationInfo);
    }

    public void checkInactiveSubscriptions() {
        forEachTopic(Topic::checkInactiveSubscriptions);
    }

    public void checkTopicPublishThrottlingRate() {
        forEachTopic(Topic::checkTopicPublishThrottlingRate);
    }

    private void refreshTopicPublishRate() {
        forEachTopic(Topic::resetTopicPublishCountAndEnableReadIfRequired);
    }

    public void checkBrokerPublishThrottlingRate() {
        brokerPublishRateLimiter.checkPublishRate();
        if (brokerPublishRateLimiter.isPublishRateExceeded()) {
            forEachTopic(topic -> ((AbstractTopic) topic).disableProducerRead());
        }
    }

    private void refreshBrokerPublishRate() {
        boolean doneReset = brokerPublishRateLimiter.resetPublishCount();
        forEachTopic(topic -> topic.resetBrokerPublishCountAndEnableReadIfRequired(doneReset));
    }

    /**
     * Iterates over all loaded topics in the broker.
     */
    public void forEachTopic(Consumer<Topic> consumer) {
        topics.forEach((n, t) -> {
            Optional<Topic> topic = extractTopic(t);
            topic.ifPresent(consumer::accept);
        });
    }

    public BacklogQuotaManager getBacklogQuotaManager() {
        return this.backlogQuotaManager;
    }

    public synchronized void monitorBacklogQuota() {
        forEachTopic(topic -> {
            if (topic instanceof PersistentTopic) {
                PersistentTopic persistentTopic = (PersistentTopic) topic;
                if (persistentTopic.isSizeBacklogExceeded()) {
                    getBacklogQuotaManager().handleExceededBacklogQuota(persistentTopic,
                            BacklogQuota.BacklogQuotaType.destination_storage, false);
                } else if (persistentTopic.isTimeBacklogExceeded()) {
                    getBacklogQuotaManager().handleExceededBacklogQuota(persistentTopic,
                            BacklogQuota.BacklogQuotaType.message_age,
                            pulsar.getConfiguration().isPreciseTimeBasedBacklogQuotaCheck());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("quota not exceeded for [{}]", topic.getName());
                    }
                }
            }
        });
    }

    public boolean isTopicNsOwnedByBroker(TopicName topicName) {
        try {
            return pulsar.getNamespaceService().isServiceUnitOwned(topicName);
        } catch (Exception e) {
            log.warn("Failed to check the ownership of the topic: {}, {}", topicName, e.getMessage());
        }
        return false;
    }

    public CompletableFuture<Void> checkTopicNsOwnershipAsync(final String topic) {
        TopicName topicName = TopicName.get(topic);
        CompletableFuture<Void> checkFuture = new CompletableFuture<>();
        pulsar.getNamespaceService().checkTopicOwnership(topicName).whenComplete((ownedByThisInstance, throwable) -> {
            if (throwable != null) {
                log.debug("Failed to check the ownership of the topic: {}", topicName, throwable);
                checkFuture.completeExceptionally(new ServerMetadataException(throwable));
            } else if (!ownedByThisInstance) {
                String msg = String.format("Namespace bundle for topic (%s) not served by this instance. "
                        + "Please redo the lookup. Request is denied: namespace=%s", topic, topicName.getNamespace());
                log.warn(msg);
                checkFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
            } else {
                checkFuture.complete(null);
            }
        });
        return checkFuture;
    }

    public void checkTopicNsOwnership(final String topic) throws BrokerServiceException {
        try {
            checkTopicNsOwnershipAsync(topic).join();
        } catch (CompletionException ex) {
            if (ex.getCause() instanceof BrokerServiceException) {
                throw (BrokerServiceException) ex.getCause();
            }
            throw new BrokerServiceException(ex.getCause());
        } catch (Exception ex) {
            throw new BrokerServiceException(ex);
        }
    }

    public CompletableFuture<Integer> unloadServiceUnit(NamespaceBundle serviceUnit,
            boolean closeWithoutWaitingClientDisconnect, long timeout, TimeUnit unit) {
        CompletableFuture<Integer> future = unloadServiceUnit(serviceUnit, closeWithoutWaitingClientDisconnect);
        ScheduledFuture<?> taskTimeout = executor().schedule(() -> {
            if (!future.isDone()) {
                log.warn("Unloading of {} has timed out", serviceUnit);
                // Complete the future with no error
                future.complete(0);
            }
        }, timeout, unit);

        future.whenComplete((r, ex) -> taskTimeout.cancel(true));
        return future;
    }

    /**
     * Unload all the topic served by the broker service under the given service unit.
     *
     * @param serviceUnit
     * @param closeWithoutWaitingClientDisconnect don't wait for clients to disconnect
     *                                           and forcefully close managed-ledger
     * @return
     */
    private CompletableFuture<Integer> unloadServiceUnit(NamespaceBundle serviceUnit,
                                                         boolean closeWithoutWaitingClientDisconnect) {
        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        topics.forEach((name, topicFuture) -> {
            TopicName topicName = TopicName.get(name);
            if (serviceUnit.includes(topicName)) {
                // Topic needs to be unloaded
                log.info("[{}] Unloading topic", topicName);
                closeFutures.add(topicFuture
                        .thenCompose(t -> t.isPresent() ? t.get().close(closeWithoutWaitingClientDisconnect)
                                : CompletableFuture.completedFuture(null)));
            }
        });

        return FutureUtil.waitForAll(closeFutures).thenApply(v -> closeFutures.size());
    }

    public void cleanUnloadedTopicFromCache(NamespaceBundle serviceUnit) {
        for (String topic : topics.keys()) {
            TopicName topicName = TopicName.get(topic);
            if (serviceUnit.includes(topicName)) {
                pulsar.getBrokerService().removeTopicFromCache(topicName.toString(), serviceUnit);
            }
        }
    }

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
    }

    public CompletableFuture<Void> removeTopicFromCache(String topic) {
        TopicName topicName = TopicName.get(topic);
        return pulsar.getNamespaceService().getBundleAsync(topicName)
                .thenAccept(namespaceBundle -> {
                    removeTopicFromCache(topic, namespaceBundle);
                });
    }

    public void removeTopicFromCache(String topic, NamespaceBundle namespaceBundle) {
        String bundleName = namespaceBundle.toString();
        String namespaceName = TopicName.get(topic).getNamespaceObject().toString();

        synchronized (multiLayerTopicsMap) {
            ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>> namespaceMap = multiLayerTopicsMap
                    .get(namespaceName);
            if (namespaceMap != null) {
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
                        clusterReplicationMetrics.remove(clusterReplicationMetrics.getKeyName(namespaceName,
                                cluster));
                    });
                }
            }
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

    public EventLoopGroup executor() {
        return workerGroup;
    }

    public ConcurrentOpenHashMap<String, PulsarClient> getReplicationClients() {
        return replicationClients;
    }

    public boolean isAuthenticationEnabled() {
        return pulsar.getConfiguration().isAuthenticationEnabled();
    }

    public boolean isAuthorizationEnabled() {
        return pulsar.getConfiguration().isAuthorizationEnabled();
    }

    public int getKeepAliveIntervalSeconds() {
        return keepAliveIntervalSeconds;
    }

    public String generateUniqueProducerName() {
        return producerNameGenerator.getNextId();
    }

    public Map<String, TopicStatsImpl> getTopicStats() {
        HashMap<String, TopicStatsImpl> stats = new HashMap<>();

        forEachTopic(topic -> stats.put(topic.getName(), topic.getStats(false, false)));

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
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
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
                (maxConcurrentLookupRequest) -> lookupRequestSemaphore.set(
                        new Semaphore((int) maxConcurrentLookupRequest, false)));
        // add listener on "maxConcurrentTopicLoadRequest" value change
        registerConfigurationListener("maxConcurrentTopicLoadRequest",
                (maxConcurrentTopicLoadRequest) -> topicLoadRequestSemaphore.set(
                        new Semaphore((int) maxConcurrentTopicLoadRequest, false)));
        registerConfigurationListener("loadManagerClassName", className -> {
            try {
                final LoadManager newLoadManager = LoadManager.create(pulsar);
                log.info("Created load manager: {}", className);
                pulsar.getLoadManager().get().stop();
                newLoadManager.start();
                pulsar.getLoadManager().set(newLoadManager);
            } catch (Exception ex) {
                log.warn("Failed to change load manager", ex);
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
        registerConfigurationListener("dispatchThrottlingRatePerReplicatorInMsg",
                (dispatchRatePerTopicInMsg) -> {
                    updateReplicatorMessageDispatchRate();
                });
        // add listener to update message-dispatch-rate in byte for replicator
        registerConfigurationListener("dispatchThrottlingRatePerReplicatorInByte",
                (dispatchRatePerTopicInByte) -> {
                    updateReplicatorMessageDispatchRate();
                });

        // add listener to notify broker publish-rate monitoring
        registerConfigurationListener("brokerPublisherThrottlingTickTimeMillis",
                (publisherThrottlingTickTimeMillis) -> {
                    setupBrokerPublishRateLimiterMonitor();
                });
        // add listener to notify broker publish-rate dynamic config
        registerConfigurationListener("brokerPublisherThrottlingMaxMessageRate",
                (brokerPublisherThrottlingMaxMessageRate) ->
                        updateBrokerPublisherThrottlingMaxRate());
        registerConfigurationListener("brokerPublisherThrottlingMaxByteRate",
                (brokerPublisherThrottlingMaxByteRate) ->
                        updateBrokerPublisherThrottlingMaxRate());

        // add listener to notify topic publish-rate monitoring
        if (!preciseTopicPublishRateLimitingEnable) {
            registerConfigurationListener("topicPublisherThrottlingTickTimeMillis",
                    (publisherThrottlingTickTimeMillis) -> {
                        setupTopicPublishRateLimiterMonitor();
                    });
        }

        // add more listeners here
    }

    private void updateBrokerPublisherThrottlingMaxRate() {
        int currentMaxMessageRate = pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate();
        long currentMaxByteRate = pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate();
        int brokerTickMs = pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis();

        // not enable
        if (brokerTickMs <= 0 || (currentMaxByteRate <= 0 && currentMaxMessageRate <= 0)) {
            if (brokerPublishRateLimiter != PublishRateLimiter.DISABLED_RATE_LIMITER) {
                refreshBrokerPublishRate();
                brokerPublishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;
            }
            return;
        }

        final PublishRate publishRate = new PublishRate(currentMaxMessageRate, currentMaxByteRate);

        log.info("Update broker publish rate limiting {}", publishRate);
        // lazy init broker Publish-rateLimiting monitoring if not initialized yet
        this.setupBrokerPublishRateLimiterMonitor();
        if (brokerPublishRateLimiter == null
            || brokerPublishRateLimiter == PublishRateLimiter.DISABLED_RATE_LIMITER) {
            // create new rateLimiter if rate-limiter is disabled
            brokerPublishRateLimiter = new PublishRateLimiterImpl(publishRate);
        } else {
            brokerPublishRateLimiter.update(publishRate);
        }
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
                    if (dispatcher != null) {
                        dispatcher.getRateLimiter().ifPresent(DispatchRateLimiter::updateDispatchRate);
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
     *
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
     * Updates pulsar.ServiceConfiguration's dynamic field with value persistent into zk-dynamic path. It also validates
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
        configCache.ifPresent(stringStringMap -> stringStringMap.forEach((key, value) -> {
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
        }));
        // register a listener: it updates field value and triggers appropriate registered field-listener only if
        // field's value has been changed so, registered doesn't have to update field value in ServiceConfiguration
        dynamicConfigurationCache.registerListener((path, data, stat) -> {
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
                if (field.getAnnotation(FieldContext.class).dynamic()) {
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
        } catch (Exception e) {
            log.error("Failed to create pending topic {}", topic, e);
            pendingTopic.getRight()
                    .completeExceptionally((e instanceof RuntimeException && e.getCause() != null) ? e.getCause() : e);
            // schedule to process next pending topic
            inactivityMonitor.schedule(() -> createPendingLoadTopic(), 100, TimeUnit.MILLISECONDS);
        }

    }

    public CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(
            TopicName topicName) {
        if (pulsar.getNamespaceService() == null) {
            return FutureUtil.failedFuture(new NamingException("namespace service is not ready"));
        }
        return pulsar.getNamespaceService().checkTopicExists(topicName)
                .thenCompose(topicExists -> {
                    return fetchPartitionedTopicMetadataAsync(topicName)
                            .thenCompose(metadata -> {
                                // If topic is already exist, creating partitioned topic is not allowed.
                                if (metadata.partitions == 0
                                        && !topicExists
                                        && !topicName.isPartitioned()
                                        && pulsar.getBrokerService().isAllowAutoTopicCreation(topicName)
                                        && pulsar.getBrokerService().isDefaultTopicTypePartitioned(topicName)) {
                                    return pulsar.getBrokerService().createDefaultPartitionedTopicAsync(topicName);
                                } else {
                                    return CompletableFuture.completedFuture(metadata);
                                }
                            });
                });
    }

    @SuppressWarnings("deprecation")
    private CompletableFuture<PartitionedTopicMetadata> createDefaultPartitionedTopicAsync(TopicName topicName) {
        final int defaultNumPartitions = pulsar.getBrokerService().getDefaultNumPartitions(topicName);
        final int maxPartitions = pulsar().getConfig().getMaxNumPartitionsPerPartitionedTopic();
        checkArgument(defaultNumPartitions > 0,
                "Default number of partitions should be more than 0");
        checkArgument(maxPartitions <= 0 || defaultNumPartitions <= maxPartitions,
                "Number of partitions should be less than or equal to " + maxPartitions);

        PartitionedTopicMetadata configMetadata = new PartitionedTopicMetadata(defaultNumPartitions);
        CompletableFuture<PartitionedTopicMetadata> partitionedTopicFuture = futureWithDeadline();

        if (!checkMaxTopicsPerNamespace(topicName, defaultNumPartitions, partitionedTopicFuture)) {
            return partitionedTopicFuture;
        }

        try {
            PartitionedTopicResources partitionResources = pulsar.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources();
            partitionResources.createAsync(partitionedTopicPath(topicName), configMetadata).thenAccept((r) -> {
                log.info("partitioned metadata successfully created for {}", topicName);
                partitionedTopicFuture.complete(configMetadata);
            }).exceptionally(ex -> {
                partitionedTopicFuture.completeExceptionally(ex.getCause());
                return null;
            });
        } catch (Exception e) {
            log.error("Failed to create default partitioned topic.", e);
            return FutureUtil.failedFuture(e);
        }

        return partitionedTopicFuture;
    }

    public CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataAsync(TopicName topicName) {
        // gets the number of partitions from the configuration cache
        return pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getAsync(partitionedTopicPath(topicName)).thenApply(metadata -> {
                    // if the partitioned topic is not found in zk, then the topic is not partitioned
                    return metadata.orElseGet(() -> new PartitionedTopicMetadata());
                });
    }

    private static String partitionedTopicPath(TopicName topicName) {
        return String.format("%s/%s/%s/%s",
                ConfigurationCacheService.PARTITIONED_TOPICS_ROOT,
                topicName.getNamespace(),
                topicName.getDomain(),
                topicName.getEncodedLocalName());
    }

    public OrderedExecutor getTopicOrderedExecutor() {
        return topicOrderedExecutor;
    }

    public ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>>>
    getMultiLayerTopicMap() {
        return multiLayerTopicsMap;
    }

    /**
     * If per-broker unacked message reached to limit then it blocks dispatcher if its unacked message limit has been
     * reached to {@link #maxUnackedMsgsPerDispatcher}.
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
            log.info("Starting blocking dispatchers with unacked msgs {} due to reached max broker limit {}",
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
                        PersistentDispatcherMultipleConsumers dispatcher =
                                (PersistentDispatcherMultipleConsumers) persistentSubscription
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
     * Unblocks the dispatchers and removes it from the {@link #blockedDispatchers} list.
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

    public Optional<Integer> getListenPort() {
        if (listenChannel != null) {
            return Optional.of(((InetSocketAddress) listenChannel.localAddress()).getPort());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Integer> getListenPortTls() {
        if (listenChannelTls != null) {
            return Optional.of(((InetSocketAddress) listenChannelTls.localAddress()).getPort());
        } else {
            return Optional.empty();
        }
    }

    private void foreachCnx(Consumer<TransportCnx> consumer) {
        Set<TransportCnx> cnxSet = new HashSet<>();
        topics.forEach((n, t) -> {
            Optional<Topic> topic = extractTopic(t);
            topic.ifPresent(value -> value.getProducers().values().forEach(producer -> cnxSet.add(producer.getCnx())));
        });
        cnxSet.forEach(consumer);
    }

    public boolean isAllowAutoTopicCreation(final String topic) {
        TopicName topicName = TopicName.get(topic);
        return isAllowAutoTopicCreation(topicName);
    }

    public boolean isAllowAutoTopicCreation(final TopicName topicName) {
        //System topic can always be created automatically
        if (pulsar.getConfiguration().isSystemTopicEnabled() && checkTopicIsEventsNames(topicName)) {
            return true;
        }
        AutoTopicCreationOverride autoTopicCreationOverride = getAutoTopicCreationOverride(topicName);
        if (autoTopicCreationOverride != null) {
            return autoTopicCreationOverride.isAllowAutoTopicCreation();
        } else {
            return pulsar.getConfiguration().isAllowAutoTopicCreation();
        }
    }

    public boolean isDefaultTopicTypePartitioned(final TopicName topicName) {
        AutoTopicCreationOverride autoTopicCreationOverride = getAutoTopicCreationOverride(topicName);
        if (autoTopicCreationOverride != null) {
            return TopicType.PARTITIONED.toString().equals(autoTopicCreationOverride.getTopicType());
        } else {
            return pulsar.getConfiguration().isDefaultTopicTypePartitioned();
        }
    }

    public int getDefaultNumPartitions(final TopicName topicName) {
        AutoTopicCreationOverride autoTopicCreationOverride = getAutoTopicCreationOverride(topicName);
        if (autoTopicCreationOverride != null) {
            return autoTopicCreationOverride.getDefaultNumPartitions();
        } else {
            return pulsar.getConfiguration().getDefaultNumPartitions();
        }
    }

    private AutoTopicCreationOverride getAutoTopicCreationOverride(final TopicName topicName) {
        try {
            Optional<Policies> policies = pulsar.getConfigurationCache().policiesCache()
                            .get(AdminResource.path(POLICIES, topicName.getNamespace()));
            // If namespace policies have the field set, it will override the broker-level setting
            if (policies.isPresent() && policies.get().autoTopicCreationOverride != null) {
                return policies.get().autoTopicCreationOverride;
            }
        } catch (Throwable t) {
            // Ignoring since if we don't have policies, we fallback on the default
            log.warn("Got exception when reading autoTopicCreateOverride policy for {}: {};",
                    topicName, t.getMessage(), t);
            return null;
        }
        log.debug("No autoTopicCreateOverride policy found for {}", topicName);
        return null;
    }

    public boolean isAllowAutoSubscriptionCreation(final String topic) {
        TopicName topicName = TopicName.get(topic);
        return isAllowAutoSubscriptionCreation(topicName);
    }

    public boolean isAllowAutoSubscriptionCreation(final TopicName topicName) {
        AutoSubscriptionCreationOverride autoSubscriptionCreationOverride =
                getAutoSubscriptionCreationOverride(topicName);
        if (autoSubscriptionCreationOverride != null) {
            return autoSubscriptionCreationOverride.isAllowAutoSubscriptionCreation();
        } else {
            return pulsar.getConfiguration().isAllowAutoSubscriptionCreation();
        }
    }

    private AutoSubscriptionCreationOverride getAutoSubscriptionCreationOverride(final TopicName topicName) {
        try {
            Optional<Policies> policies = pulsar.getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, topicName.getNamespace()));
            // If namespace policies have the field set, it will override the broker-level setting
            if (policies.isPresent() && policies.get().autoSubscriptionCreationOverride != null) {
                return policies.get().autoSubscriptionCreationOverride;
            }
        } catch (Throwable t) {
            // Ignoring since if we don't have policies, we fallback on the default
            log.warn("Got exception when reading autoSubscriptionCreateOverride policy for {}: {};",
                    topicName, t.getMessage(), t);
            return null;
        }
        log.debug("No autoSubscriptionCreateOverride policy found for {}", topicName);
        return null;
    }
    private boolean isSystemTopic(String topic) {
        return SystemTopicClient.isSystemTopic(TopicName.get(topic));
    }

    /**
     * Get {@link TopicPolicies} for the parameterized topic.
     * @param topicName
     * @return TopicPolicies, if they exist. Otherwise, the value will not be present.
     */
    public Optional<TopicPolicies> getTopicPolicies(TopicName topicName) {
        if (!pulsar().getConfig().isTopicLevelPoliciesEnabled()) {
            return Optional.empty();
        }
        TopicName cloneTopicName = topicName;
        if (topicName.isPartitioned()) {
            cloneTopicName = TopicName.get(topicName.getPartitionedTopicName());
        }
        try {
            return Optional.ofNullable(pulsar.getTopicPoliciesService().getTopicPolicies(cloneTopicName));
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.debug("Topic {} policies have not been initialized yet.", topicName.getPartitionedTopicName());
            return Optional.empty();
        }
    }

    private <T> boolean checkMaxTopicsPerNamespace(TopicName topicName, int numPartitions,
                                            CompletableFuture<T> topicFuture) {
        Integer maxTopicsPerNamespace;
        try {
            maxTopicsPerNamespace = pulsar.getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, topicName.getNamespace()))
                    .map(p -> p.max_topics_per_namespace)
                    .orElse(null);

            if (maxTopicsPerNamespace == null) {
                maxTopicsPerNamespace = pulsar.getConfig().getMaxTopicsPerNamespace();
            }

            // new create check
            if (maxTopicsPerNamespace > 0 && !SystemTopicClient.isSystemTopic(topicName)) {
                String partitionedTopicPath = PulsarWebResource.joinPath(MANAGED_LEDGER_PATH_ZNODE,
                        topicName.getNamespace(), topicName.getDomain().value());
                List<String> topics = pulsar().getLocalZkCache().getZooKeeper()
                        .getChildren(partitionedTopicPath, false);
                // exclude created system topic
                long topicsCount =
                        topics.stream().filter(t -> !SystemTopicClient.isSystemTopic(TopicName.get(t))).count();
                if (topicsCount + numPartitions > maxTopicsPerNamespace) {
                    log.error("Failed to create persistent topic {}, "
                            + "exceed maximum number of topics in namespace", topicName);
                    topicFuture.completeExceptionally(new RestException(Response.Status.PRECONDITION_FAILED,
                            "Exceed maximum number of topics in namespace."));
                    return false;
                }
            }
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no partitioned topics in this domain for this namespace
        } catch (Exception e) {
            log.error("Failed to create partitioned topic {}", topicName, e);
                topicFuture.completeExceptionally(new RestException(e));
                return false;
        }

        return true;
    }

    public void setInterceptor(BrokerInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        return brokerEntryMetadataInterceptors;
    }

    public boolean isBrokerEntryMetadataEnabled() {
        return !brokerEntryMetadataInterceptors.isEmpty();
    }

    public void pausedConnections(int numberOfConnections) {
        pausedConnections.add(numberOfConnections);
    }

    public void resumedConnections(int numberOfConnections) {
        pausedConnections.add(-numberOfConnections);
    }

    public long getPausedConnections() {
        return pausedConnections.longValue();
    }
}
