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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.bookkeeper.mledger.ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.util.RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX;
import static org.apache.pulsar.client.util.RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX;
import static org.apache.pulsar.common.naming.SystemTopicNames.isTransactionInternalName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.RateLimiter;
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
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
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
import org.apache.bookkeeper.mledger.impl.NonAppendableLedgerOffloader;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.BundlesQuotas;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerLoader;
import org.apache.pulsar.broker.delayed.InMemoryDelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.DynamicConfigurationResources;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicMigratedException;
import org.apache.pulsar.broker.service.TopicEventsListener.EventStage;
import org.apache.pulsar.broker.service.TopicEventsListener.TopicEvent;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSystemTopic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiterFactory;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiterFactoryClassic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SystemTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.prometheus.metrics.ObserverGauge;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.broker.validator.BindAddressValidator;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.configuration.BindAddress;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FieldParser;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.ConnectionRateLimitOperationName;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter(AccessLevel.PUBLIC)
@Setter(AccessLevel.PROTECTED)
public class BrokerService implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);
    private static final TimeoutException FAILED_TO_LOAD_TOPIC_TIMEOUT_EXCEPTION =
            FutureUtil.createTimeoutException("Failed to load topic within timeout", BrokerService.class,
                    "futureWithDeadline(...)");
    private static final long GRACEFUL_SHUTDOWN_QUIET_PERIOD_MAX_MS = 5000L;
    private static final double GRACEFUL_SHUTDOWN_QUIET_PERIOD_RATIO_OF_TOTAL_TIMEOUT = 0.25d;
    private static final double GRACEFUL_SHUTDOWN_TIMEOUT_RATIO_OF_TOTAL_TIMEOUT = 0.5d;

    private static final Histogram backlogQuotaCheckDuration = Histogram.build()
            .name("pulsar_storage_backlog_quota_check_duration_seconds")
            .help("The duration of the backlog quota check process.")
            .buckets(5, 10, 30, 60, 300)
            .register();

    private final PulsarService pulsar;
    private final ManagedLedgerStorage managedLedgerStorage;

    private final Map<String, CompletableFuture<Optional<Topic>>> topics = new ConcurrentHashMap<>();

    private final Map<String, PulsarClient> replicationClients = new ConcurrentHashMap<>();
    private final Map<String, PulsarAdmin> clusterAdmins = new ConcurrentHashMap<>();

    // Multi-layer topics map:
    // Namespace --> Bundle --> topicName --> topic
    private final Map<String, Map<String, Map<String, Topic>>> multiLayerTopicsMap = new ConcurrentHashMap<>();
    // Keep track of topics and partitions served by this broker for fast lookup.
    @Getter
    private final Map<String, Set<Integer>> owningTopics = new ConcurrentHashMap<>();
    private long numberOfNamespaceBundles = 0;

    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;
    private final OrderedExecutor topicOrderedExecutor;
    // offline topic backlog cache
    private final Map<TopicName, PersistentOfflineTopicStats> offlineTopicStatCache = new ConcurrentHashMap<>();
    private final Map<String, ConfigField> dynamicConfigurationMap;
    private final Map<String, Consumer<?>> configRegisteredListeners = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<TopicLoadingContext> pendingTopicLoadingQueue;

    private AuthorizationService authorizationService;
    private final ScheduledExecutorService statsUpdater;

    @Getter
    private final ScheduledExecutorService backlogQuotaChecker;

    protected final AtomicReference<Semaphore> lookupRequestSemaphore;
    protected final AtomicReference<Semaphore> topicLoadRequestSemaphore;

    public static final String TOPIC_LOOKUP_USAGE_METRIC_NAME = "pulsar.broker.request.topic.lookup.concurrent.usage";
    public static final String TOPIC_LOOKUP_LIMIT_METRIC_NAME = "pulsar.broker.request.topic.lookup.concurrent.limit";
    @PulsarDeprecatedMetric(newMetricName = TOPIC_LOOKUP_USAGE_METRIC_NAME)
    private final ObserverGauge pendingLookupRequests;
    private final ObservableLongUpDownCounter pendingLookupOperationsCounter;
    private final ObservableLongUpDownCounter pendingLookupOperationsLimitCounter;

    public static final String TOPIC_LOAD_USAGE_METRIC_NAME = "pulsar.broker.topic.load.concurrent.usage";
    public static final String TOPIC_LOAD_LIMIT_METRIC_NAME = "pulsar.broker.topic.load.concurrent.limit";
    @PulsarDeprecatedMetric(newMetricName = TOPIC_LOAD_USAGE_METRIC_NAME)
    private final ObserverGauge pendingTopicLoadRequests;
    private final ObservableLongUpDownCounter pendingTopicLoadOperationsCounter;
    private final ObservableLongUpDownCounter pendingTopicLoadOperationsLimitCounter;

    public static final String CONNECTION_RATE_LIMIT_COUNT_METRIC_NAME = "pulsar.broker.connection.rate_limit.count";
    private final LongCounter rateLimitedConnectionsCounter;
    @PulsarDeprecatedMetric(newMetricName = CONNECTION_RATE_LIMIT_COUNT_METRIC_NAME)
    @Deprecated
    private static final Gauge throttledConnectionsGauge = Gauge.build()
            .name("pulsar_broker_throttled_connections")
            .help("Counter of connections throttled because of per-connection limit")
            .register();

    private final ScheduledExecutorService inactivityMonitor;
    private final ScheduledExecutorService messageExpiryMonitor;
    private final ScheduledExecutorService compactionMonitor;
    private final ScheduledExecutorService consumedLedgersMonitor;
    private ScheduledExecutorService deduplicationSnapshotMonitor;
    protected final PublishRateLimiter brokerPublishRateLimiter;
    private final DispatchRateLimiterFactory dispatchRateLimiterFactory;
    protected volatile DispatchRateLimiter brokerDispatchRateLimiter = null;

    private DistributedIdGenerator producerNameGenerator;

    public static final String PRODUCER_NAME_GENERATOR_PATH = "/counters/producer-name";

    private final BacklogQuotaManager backlogQuotaManager;

    private final int keepAliveIntervalSeconds;
    private final PulsarStats pulsarStats;
    private final AuthenticationService authenticationService;
    private final Clock clock;

    public static final String MANAGED_LEDGER_PATH_ZNODE = "/managed-ledgers";

    private final LongAdder totalUnackedMessages = new LongAdder();
    private final int maxUnackedMessages;
    public final int maxUnackedMsgsPerDispatcher;
    private final AtomicBoolean blockedDispatcherOnHighUnackedMsgs = new AtomicBoolean(false);
    private final Set<AbstractPersistentDispatcherMultipleConsumers> blockedDispatchers = ConcurrentHashMap.newKeySet();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    @VisibleForTesting
    private final DelayedDeliveryTrackerFactory delayedDeliveryTrackerFactory;
    // InMemoryDelayedDeliveryTrackerFactory is for the purpose of
    // fallback if recover BucketDelayedDeliveryTracker failed.
    private volatile DelayedDeliveryTrackerFactory fallbackDelayedDeliveryTrackerFactory;
    private final ServerBootstrap defaultServerBootstrap;
    private final List<EventLoopGroup> protocolHandlersWorkerGroups = new ArrayList<>();

    @Getter
    private final BundlesQuotas bundlesQuotas;

    private PulsarChannelInitializer.Factory pulsarChannelInitFactory = PulsarChannelInitializer.DEFAULT_FACTORY;

    private final List<Channel> listenChannels = new ArrayList<>(2);
    private Channel listenChannel;
    private Channel listenChannelTls;

    private BrokerInterceptor interceptor;
    private final EntryFilterProvider entryFilterProvider;
    private TopicFactory topicFactory;

    private Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;
    private Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors;

    private final TopicEventsDispatcher topicEventsDispatcher = new TopicEventsDispatcher();
    private volatile boolean unloaded = false;

    public BrokerService(PulsarService pulsar, EventLoopGroup eventLoopGroup) throws Exception {
        this.pulsar = pulsar;
        this.clock = pulsar.getClock();
        this.dynamicConfigurationMap = prepareDynamicConfigurationMap();
        this.brokerPublishRateLimiter = new PublishRateLimiterImpl(pulsar.getMonotonicClock());
        this.dispatchRateLimiterFactory = createDispatchRateLimiterFactory(pulsar.getConfig());
        this.managedLedgerStorage = pulsar.getManagedLedgerStorage();
        this.keepAliveIntervalSeconds = pulsar.getConfiguration().getKeepAliveIntervalSeconds();
        this.pendingTopicLoadingQueue = Queues.newConcurrentLinkedQueue();
        this.pulsarStats = new PulsarStats(pulsar);

        this.topicOrderedExecutor = OrderedExecutor.newBuilder()
                .numThreads(pulsar.getConfiguration().getTopicOrderedExecutorThreadNum())
                .name("broker-topic-workers").build();
        final DefaultThreadFactory acceptorThreadFactory =
                new ExecutorProvider.ExtendedThreadFactory("pulsar-acceptor");

        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(
                pulsar.getConfiguration().getNumAcceptorThreads(), false, acceptorThreadFactory);
        this.workerGroup = eventLoopGroup;

        this.statsUpdater = OrderedScheduler.newSchedulerBuilder()
                .name("pulsar-stats-updater")
                .numThreads(1)
                .build();
        this.authorizationService = new AuthorizationService(
                pulsar.getConfiguration(), pulsar().getPulsarResources());
        this.entryFilterProvider = new EntryFilterProvider(pulsar.getConfiguration());

        pulsar.getLocalMetadataStore().registerListener(this::handleMetadataChanges);
        if (pulsar.getConfigurationMetadataStore() != pulsar.getLocalMetadataStore()) {
            pulsar.getConfigurationMetadataStore().registerListener(this::handleMetadataChanges);
        }

        this.inactivityMonitor = OrderedScheduler.newSchedulerBuilder()
                .name("pulsar-inactivity-monitor")
                .numThreads(1)
                .build();
        this.messageExpiryMonitor = OrderedScheduler.newSchedulerBuilder()
                .name("pulsar-msg-expiry-monitor")
                .numThreads(1)
                .build();
        this.compactionMonitor = OrderedScheduler.newSchedulerBuilder()
                .name("pulsar-compaction-monitor")
                .numThreads(1)
                .build();
        this.consumedLedgersMonitor = OrderedScheduler.newSchedulerBuilder()
                .name("pulsar-consumed-ledgers-monitor")
                .numThreads(1)
                .build();
        this.backlogQuotaManager = new BacklogQuotaManager(pulsar);
        this.backlogQuotaChecker = OrderedScheduler.newSchedulerBuilder()
                .name("pulsar-backlog-quota-checker")
                .numThreads(1)
                .build();
        this.authenticationService = new AuthenticationService(pulsar.getConfiguration(),
                pulsar.getOpenTelemetry().getOpenTelemetry());
        this.topicFactory = createPersistentTopicFactory();
        // update dynamic configuration and register-listener
        updateConfigurationAndRegisterListeners();
        this.lookupRequestSemaphore = new AtomicReference<>(
                new Semaphore(pulsar.getConfiguration().getMaxConcurrentLookupRequest(), false));
        this.topicLoadRequestSemaphore = new AtomicReference<>(
                new Semaphore(pulsar.getConfiguration().getMaxConcurrentTopicLoadRequest(), false));
        if (pulsar.getConfiguration().getMaxUnackedMessagesPerBroker() > 0
                && pulsar.getConfiguration().getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked() > 0.0) {
            this.maxUnackedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerBroker();
            this.maxUnackedMsgsPerDispatcher = (int) (maxUnackedMessages
                    * pulsar.getConfiguration().getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked());
            log.info("Enabling per-broker unack-message limit {} and dispatcher-limit {} on blocked-broker",
                    maxUnackedMessages, maxUnackedMsgsPerDispatcher);
            // block misbehaving dispatcher by checking periodically
            pulsar.getExecutor().scheduleAtFixedRate(this::checkUnAckMessageDispatching,
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
                .loadDelayedDeliveryTrackerFactory(pulsar);

        this.defaultServerBootstrap = defaultServerBootstrap();

        this.pendingLookupRequests = ObserverGauge.build("pulsar_broker_lookup_pending_requests", "-")
                .supplier(this::getPendingLookupRequest)
                .register();
        this.pendingLookupOperationsCounter = pulsar.getOpenTelemetry().getMeter()
                .upDownCounterBuilder(TOPIC_LOOKUP_USAGE_METRIC_NAME)
                .setDescription("The number of pending lookup operations in the broker. "
                        + "When it reaches threshold \"maxConcurrentLookupRequest\" defined in broker.conf, "
                        + "new requests are rejected.")
                .setUnit("{operation}")
                .buildWithCallback(measurement -> measurement.record(getPendingLookupRequest()));
        this.pendingLookupOperationsLimitCounter = pulsar.getOpenTelemetry().getMeter()
                .upDownCounterBuilder(TOPIC_LOOKUP_LIMIT_METRIC_NAME)
                .setDescription("The maximum number of pending lookup operations in the broker. "
                        + "Equal to \"maxConcurrentLookupRequest\" defined in broker.conf.")
                .setUnit("{operation}")
                .buildWithCallback(
                        measurement -> measurement.record(pulsar.getConfig().getMaxConcurrentLookupRequest()));

        this.pendingTopicLoadRequests = ObserverGauge.build(
                        "pulsar_broker_topic_load_pending_requests", "-")
                .supplier(this::getPendingTopicLoadRequests)
                .register();
        this.pendingTopicLoadOperationsCounter = pulsar.getOpenTelemetry().getMeter()
                .upDownCounterBuilder(TOPIC_LOAD_USAGE_METRIC_NAME)
                .setDescription("The number of pending topic load operations in the broker. "
                        + "When it reaches threshold \"maxConcurrentTopicLoadRequest\" defined in broker.conf, "
                        + "new requests are rejected.")
                .setUnit("{operation}")
                .buildWithCallback(measurement -> measurement.record(getPendingTopicLoadRequests()));
        this.pendingTopicLoadOperationsLimitCounter = pulsar.getOpenTelemetry().getMeter()
                .upDownCounterBuilder(TOPIC_LOAD_LIMIT_METRIC_NAME)
                .setDescription("The maximum number of pending topic load operations in the broker. "
                        + "Equal to \"maxConcurrentTopicLoadRequest\" defined in broker.conf.")
                .setUnit("{operation}")
                .buildWithCallback(
                        measurement -> measurement.record(pulsar.getConfig().getMaxConcurrentTopicLoadRequest()));

        this.rateLimitedConnectionsCounter = pulsar.getOpenTelemetry().getMeter()
                .counterBuilder(BrokerService.CONNECTION_RATE_LIMIT_COUNT_METRIC_NAME)
                .setDescription("The number of times a connection has been rate limited.")
                .setUnit("{operation}")
                .build();

        this.brokerEntryMetadataInterceptors = BrokerEntryMetadataUtils
                .loadBrokerEntryMetadataInterceptors(pulsar.getConfiguration().getBrokerEntryMetadataInterceptors(),
                        BrokerService.class.getClassLoader());

        this.brokerEntryPayloadProcessors = BrokerEntryMetadataUtils.loadInterceptors(pulsar.getConfiguration()
                        .getBrokerEntryPayloadProcessors(), BrokerService.class.getClassLoader());

        this.bundlesQuotas = new BundlesQuotas(pulsar);
    }

    protected DispatchRateLimiterFactory createDispatchRateLimiterFactory(ServiceConfiguration config)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String dispatchRateLimiterFactoryClassName = config.getDispatchRateLimiterFactoryClassName();
        if (isNotBlank(dispatchRateLimiterFactoryClassName)) {
            try {
                return (DispatchRateLimiterFactory) Class.forName(dispatchRateLimiterFactoryClassName).newInstance();
            } catch (Exception e) {
                log.warn("Failed to initialize dispatch rate limiter factory class {}",
                        dispatchRateLimiterFactoryClassName, e);
                throw e;
            }
        } else {
            return new DispatchRateLimiterFactoryClassic();
        }
    }

    private int getPendingLookupRequest() {
        return pulsar.getConfig().getMaxConcurrentLookupRequest() - lookupRequestSemaphore.get().availablePermits();
    }

    private int getPendingTopicLoadRequests() {
        return pulsar.getConfig().getMaxConcurrentTopicLoadRequest()
                - topicLoadRequestSemaphore.get().availablePermits();
    }

    public void addTopicEventListener(TopicEventsListener... listeners) {
        topicEventsDispatcher.addTopicEventListener(listeners);
        topics.keySet().forEach(topic ->
                TopicEventsDispatcher.notify(listeners, topic, TopicEvent.LOAD, EventStage.SUCCESS, null));
    }

    public void removeTopicEventListener(TopicEventsListener... listeners) {
        topicEventsDispatcher.removeTopicEventListener(listeners);
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

        ServiceConfiguration configuration = pulsar.getConfiguration();
        boolean useSeparateThreadPool = configuration.isUseSeparateThreadPoolForProtocolHandlers();
        ServerBootstrap bootstrap;
        if (useSeparateThreadPool) {
            bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.SO_REUSEADDR, true);
            bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
            bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
            bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                    new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));
            EventLoopUtil.enableTriggeredMode(bootstrap);
            DefaultThreadFactory defaultThreadFactory =
                    new ExecutorProvider.ExtendedThreadFactory("pulsar-ph-" + protocol);
            EventLoopGroup dedicatedWorkerGroup =
                    EventLoopUtil.newEventLoopGroup(configuration.getNumIOThreads(), false, defaultThreadFactory);
            bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(dedicatedWorkerGroup));
            protocolHandlersWorkerGroups.add(dedicatedWorkerGroup);
            bootstrap.group(this.acceptorGroup, dedicatedWorkerGroup);
        } else {
            bootstrap = defaultServerBootstrap.clone();
        }
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
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));
        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);
        return bootstrap;
    }

    public Map<String, TopicStatsImpl> getTopicStats(NamespaceBundle bundle) {
        final var topicMap = multiLayerTopicsMap.computeIfAbsent(bundle.getNamespaceObject().toString(),
                __ -> new ConcurrentHashMap<>()).computeIfAbsent(bundle.toString(), __ -> new ConcurrentHashMap<>());

        Map<String, TopicStatsImpl> topicStatsMap = new HashMap<>();
        topicMap.forEach((name, topic) -> {
            topicStatsMap.put(name,
                    topic.getStats(false, false, false));
        });
        return topicStatsMap;
    }

    public void start() throws Exception {
        this.producerNameGenerator = new DistributedIdGenerator(pulsar.getCoordinationService(),
                PRODUCER_NAME_GENERATOR_PATH, pulsar.getConfiguration().getClusterName());

        ServiceConfiguration serviceConfig = pulsar.getConfiguration();
        List<BindAddress> bindAddresses = BindAddressValidator.validateBindAddresses(serviceConfig,
                Arrays.asList("pulsar", "pulsar+ssl"));
        String internalListenerName = serviceConfig.getInternalListenerName();

        // create a channel for each bind address
        if (bindAddresses.size() == 0) {
            throw new IllegalArgumentException("At least one broker bind address must be configured");
        }
        for (BindAddress a : bindAddresses) {
            InetSocketAddress addr = new InetSocketAddress(a.getAddress().getHost(), a.getAddress().getPort());
            boolean isTls = "pulsar+ssl".equals(a.getAddress().getScheme());
            PulsarChannelInitializer.PulsarChannelOptions opts = PulsarChannelInitializer.PulsarChannelOptions.builder()
                            .enableTLS(isTls)
                            .listenerName(a.getListenerName()).build();

            ServerBootstrap b = defaultServerBootstrap.clone();
            b.childHandler(
                    pulsarChannelInitFactory.newPulsarChannelInitializer(pulsar, opts));
            try {
                Channel ch = b.bind(addr).sync().channel();
                listenChannels.add(ch);

                // identify the primary channel. Note that the legacy bindings appear first and have no listener.
                if (StringUtils.isBlank(a.getListenerName())
                        || StringUtils.equalsIgnoreCase(a.getListenerName(), internalListenerName)) {
                    if (this.listenChannel == null && !isTls) {
                        this.listenChannel = ch;
                    }
                    if (this.listenChannelTls == null && isTls) {
                        this.listenChannelTls = ch;
                    }
                }

                log.info("Started Pulsar Broker service on {}, TLS: {}, listener: {}",
                        ch.localAddress(),
                        isTls ? SslContext.defaultServerProvider().toString() : "(none)",
                        StringUtils.defaultString(a.getListenerName(), "(none)"));
            } catch (Exception e) {
                throw new IOException("Failed to bind Pulsar broker on " + addr, e);
            }
        }

        // start other housekeeping functions
        this.startStatsUpdater(
                serviceConfig.getStatsUpdateInitialDelayInSecs(),
                serviceConfig.getStatsUpdateFrequencyInSecs());
        this.initializeHealthChecker();
        this.startInactivityMonitor();
        this.startMessageExpiryMonitor();
        this.startCompactionMonitor();
        this.startConsumedLedgersMonitor();
        this.startBacklogQuotaChecker();
        this.updateBrokerPublisherThrottlingMaxRate();
        this.updateBrokerDispatchThrottlingMaxRate();
        this.startCheckReplicationPolicies();
        this.startDeduplicationSnapshotMonitor();
        this.startClearInvalidateTopicNameCacheTask();
    }

    protected void startClearInvalidateTopicNameCacheTask() {
        final int maxSecondsToClearTopicNameCache = pulsar.getConfiguration().getMaxSecondsToClearTopicNameCache();
        inactivityMonitor.scheduleAtFixedRate(
            () -> TopicName.clearIfReachedMaxCapacity(pulsar.getConfiguration().getTopicNameCacheMaxCapacity()),
            maxSecondsToClearTopicNameCache,
            maxSecondsToClearTopicNameCache,
            TimeUnit.SECONDS);
    }

    protected void startStatsUpdater(int statsUpdateInitialDelayInSecs, int statsUpdateFrequencyInSecs) {
        statsUpdater.scheduleAtFixedRate(this::updateRates,
            statsUpdateInitialDelayInSecs, statsUpdateFrequencyInSecs, TimeUnit.SECONDS);

        // Ensure the broker starts up with initial stats
        updateRates();
    }

    protected void initializeHealthChecker() {
        ServiceConfiguration config = pulsar().getConfiguration();
        if (config.getHealthCheckMetricsUpdateTimeInSeconds() > 0) {
            int interval = config.getHealthCheckMetricsUpdateTimeInSeconds();
            statsUpdater.scheduleAtFixedRate(this::checkHealth,
                    interval, interval, TimeUnit.SECONDS);
        }
    }

    public CompletableFuture<Void> checkHealth() {
        if (!pulsar().isRunning()) {
            return CompletableFuture.completedFuture(null);
        }
        return pulsar().runHealthCheck(TopicVersion.V2, null).thenAccept(__ -> {
            this.pulsarStats.getBrokerOperabilityMetrics().recordHealthCheckStatusSuccess();
        }).exceptionally(ex -> {
            this.pulsarStats.getBrokerOperabilityMetrics().recordHealthCheckStatusFail();
            return null;
        });
    }

    protected void startDeduplicationSnapshotMonitor() {
        // We do not know whether users will enable deduplication on namespace level/topic level or not, so keep this
        // scheduled task runs.
        int interval = pulsar().getConfiguration().getBrokerDeduplicationSnapshotFrequencyInSeconds();
        if (interval > 0) {
            this.deduplicationSnapshotMonitor = OrderedScheduler.newSchedulerBuilder()
                    .name("deduplication-snapshot-monitor")
                    .numThreads(1)
                    .build();
            deduplicationSnapshotMonitor.scheduleAtFixedRate(() -> forEachTopic(
                    Topic::checkDeduplicationSnapshot)
                    , interval, interval, TimeUnit.SECONDS);
        }
    }

    protected void startInactivityMonitor() {
        if (pulsar().getConfiguration().isBrokerDeleteInactiveTopicsEnabled()) {
            int interval = pulsar().getConfiguration().getBrokerDeleteInactiveTopicsFrequencySeconds();
            inactivityMonitor.scheduleAtFixedRate(() -> checkGC(), interval, interval,
                    TimeUnit.SECONDS);
        }

        // Deduplication info checker
        long duplicationCheckerIntervalInSeconds = TimeUnit.MINUTES
                .toSeconds(pulsar().getConfiguration().getBrokerDeduplicationProducerInactivityTimeoutMinutes()) / 3;
        inactivityMonitor.scheduleAtFixedRate(this::checkMessageDeduplicationInfo,
                duplicationCheckerIntervalInSeconds,
                duplicationCheckerIntervalInSeconds, TimeUnit.SECONDS);

        // Inactive subscriber checker
        if (pulsar().getConfiguration().getSubscriptionExpiryCheckIntervalInMinutes() > 0) {
            long subscriptionExpiryCheckIntervalInSeconds =
                    TimeUnit.MINUTES.toSeconds(pulsar().getConfiguration()
                            .getSubscriptionExpiryCheckIntervalInMinutes());
            inactivityMonitor.scheduleAtFixedRate(this::checkInactiveSubscriptions,
                    subscriptionExpiryCheckIntervalInSeconds,
                    subscriptionExpiryCheckIntervalInSeconds, TimeUnit.SECONDS);
        }

        // check cluster migration
        int interval = pulsar().getConfiguration().getClusterMigrationCheckDurationSeconds();
        if (interval > 0) {
            inactivityMonitor.scheduleAtFixedRate(() -> checkClusterMigration(), interval, interval,
                    TimeUnit.SECONDS);
        }
    }

    protected void startMessageExpiryMonitor() {
        int interval = pulsar().getConfiguration().getMessageExpiryCheckIntervalInMinutes();
        messageExpiryMonitor.scheduleAtFixedRate(this::checkMessageExpiry, interval, interval,
                TimeUnit.MINUTES);
    }

    protected void startCheckReplicationPolicies() {
        int interval = pulsar.getConfig().getReplicationPolicyCheckDurationSeconds();
        if (interval > 0) {
            messageExpiryMonitor.scheduleAtFixedRate(this::checkReplicationPolicies, interval, interval,
                    TimeUnit.SECONDS);
        }
    }

    protected void startCompactionMonitor() {
        int interval = pulsar().getConfiguration().getBrokerServiceCompactionMonitorIntervalInSeconds();
        if (interval > 0) {
            compactionMonitor.scheduleAtFixedRate(this::checkCompaction,
                    interval, interval, TimeUnit.SECONDS);
        }
    }

    protected void startConsumedLedgersMonitor() {
        int interval = pulsar().getConfiguration().getRetentionCheckIntervalInSeconds();
        if (interval > 0) {
            consumedLedgersMonitor.scheduleAtFixedRate(this::checkConsumedLedgers,
                    interval, interval, TimeUnit.SECONDS);
        }
    }

    protected void startBacklogQuotaChecker() {
        if (pulsar().getConfiguration().isBacklogQuotaCheckEnabled()) {
            final int interval = pulsar().getConfiguration().getBacklogQuotaCheckIntervalInSeconds();
            log.info("Scheduling a thread to check backlog quota after [{}] seconds in background", interval);
            backlogQuotaChecker.scheduleAtFixedRate(this::monitorBacklogQuota, interval, interval,
                    TimeUnit.SECONDS);
        } else {
            log.info("Backlog quota check monitoring is disabled");
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

    public CompletableFuture<Void> closeAndRemoveReplicationClient(String clusterName) {
        List<CompletableFuture<Void>> futures = new ArrayList<>((int) topics.size());
        topics.forEach((__, future) -> {
            CompletableFuture<Void> f = new CompletableFuture<>();
            futures.add(f);
            future.whenComplete((ot, ex) -> {
                if (ot.isPresent()) {
                    Replicator r = ot.get().getReplicators().get(clusterName);
                    if (r != null && r.isConnected()) {
                        r.terminate().whenComplete((v, e) -> f.complete(null));
                        return;
                    }
                }
                f.complete(null);
            });
        });

        return FutureUtil.waitForAll(futures).thenCompose(__ -> {
            PulsarClient client = replicationClients.remove(clusterName);
            if (client == null) {
                return CompletableFuture.completedFuture(null);
            }
            return client.closeAsync();
        }).thenCompose(__ -> {
            PulsarAdmin pulsarAdmin = clusterAdmins.remove(clusterName);
            if (pulsarAdmin != null) {
                pulsarAdmin.close();
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    public CompletableFuture<Void> closeAsync() {
        try {
            log.info("Shutting down Pulsar Broker service");

            // unregister non-static metrics collectors
            pendingTopicLoadRequests.unregister();
            pendingLookupRequests.unregister();

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

            //close entry filters
            if (entryFilterProvider != null) {
                entryFilterProvider.close();
            }

            CompletableFuture<CompletableFuture<Void>> cancellableDownstreamFutureReference = new CompletableFuture<>();
            log.info("Event loops shutting down gracefully...");
            List<CompletableFuture<?>> shutdownEventLoops = new ArrayList<>();
            shutdownEventLoops.add(shutdownEventLoopGracefully(acceptorGroup));
            shutdownEventLoops.add(shutdownEventLoopGracefully(workerGroup));
            for (EventLoopGroup group : protocolHandlersWorkerGroups) {
                shutdownEventLoops.add(shutdownEventLoopGracefully(group));
            }

            CompletableFuture<Void> shutdownFuture =
                    CompletableFuture.allOf(shutdownEventLoops.toArray(new CompletableFuture[0]))
                            .handle((v, t) -> {
                                if (t != null) {
                                    log.warn("Error shutting down event loops gracefully", t);
                                } else {
                                    log.info("Event loops shutdown completed.");
                                }
                                return null;
                            })
                            .thenComposeAsync(__ -> {
                                log.info("Continuing to second phase in shutdown.");

                                List<CompletableFuture<Void>> asyncCloseFutures = new ArrayList<>();
                                listenChannels.forEach(ch -> {
                                    if (ch.isOpen()) {
                                        asyncCloseFutures.add(closeChannel(ch));
                                    }
                                });

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
                                pendingTopicLoadOperationsCounter.close();
                                pendingLookupOperationsCounter.close();
                                try {
                                    delayedDeliveryTrackerFactory.close();
                                    if (fallbackDelayedDeliveryTrackerFactory != null) {
                                        fallbackDelayedDeliveryTrackerFactory.close();
                                    }
                                } catch (Exception e) {
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
                            }, runnable -> {
                                // run the 2nd phase of the shutdown in a separate thread
                                Thread thread = new Thread(runnable);
                                thread.setName("BrokerService-shutdown-phase2");
                                thread.setDaemon(false);
                                thread.start();
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
                        timeout, MILLISECONDS));
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
        unloadNamespaceBundlesGracefully(0, true);
    }

    public void unloadNamespaceBundlesGracefully(int maxConcurrentUnload, boolean closeWithoutWaitingClientDisconnect) {
        if (unloaded) {
            return;
        }
        try {
            log.info("Unloading namespace-bundles...");
            // make broker-node unavailable from the cluster
            long disableBrokerStartTime = System.nanoTime();
            if (pulsar.getLoadManager() != null && pulsar.getLoadManager().get() != null) {
                try {
                    pulsar.getLoadManager().get().disableBroker();
                } catch (PulsarServerException.NotFoundException ne) {
                    log.warn("Broker load-manager znode doesn't exist ", ne);
                    // still continue and release bundle ownership as broker's registration node doesn't exist.
                }
            }
            double disableBrokerTimeSeconds =
                    TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - disableBrokerStartTime))
                            / 1000.0;
            log.info("Disable broker in load manager completed in {} seconds", disableBrokerTimeSeconds);

            // unload all namespace-bundles gracefully
            long closeTopicsStartTime = System.nanoTime();
            Set<NamespaceBundle> serviceUnits =
                    pulsar.getNamespaceService() != null ? pulsar.getNamespaceService().getOwnedServiceUnits() : null;
            if (serviceUnits != null) {
                RateLimiter rateLimiter = maxConcurrentUnload > 0 ? RateLimiter.create(maxConcurrentUnload) : null;
                serviceUnits.forEach(su -> {
                    if (su != null) {
                        try {
                            if (rateLimiter != null) {
                                rateLimiter.acquire(1);
                            }
                            long timeout = pulsar.getConfiguration().getNamespaceBundleUnloadingTimeoutMs();
                            pulsar.getNamespaceService().unloadNamespaceBundle(su, timeout, MILLISECONDS,
                                    closeWithoutWaitingClientDisconnect).get(timeout, MILLISECONDS);
                        } catch (Exception e) {
                            if (e instanceof ExecutionException
                                    && e.getCause() instanceof ServiceUnitNotReadyException) {
                                log.warn("Failed to unload namespace bundle {}: {}", su, e.getMessage());
                            } else {
                                log.warn("Failed to unload namespace bundle {}", su, e);
                            }
                        }
                    }
                });
                double closeTopicsTimeSeconds =
                        TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - closeTopicsStartTime))
                                / 1000.0;
                log.info("Unloading {} namespace-bundles completed in {} seconds", serviceUnits.size(),
                        closeTopicsTimeSeconds);
            }
        } catch (Exception e) {
            log.error("Failed to disable broker from loadbalancer list {}", e.getMessage(), e);
        } finally {
            unloaded = true;
        }
    }

    public CompletableFuture<Optional<Topic>> getTopicIfExists(final String topic) {
        return getTopic(topic, false /* createIfMissing */);
    }

    public CompletableFuture<Topic> getOrCreateTopic(final String topic) {
        return isAllowAutoTopicCreationAsync(topic)
                .thenCompose(isAllowed -> getTopic(topic, isAllowed))
                .thenApply(Optional::get);
    }

    public CompletableFuture<Optional<Topic>> getTopic(final String topic, boolean createIfMissing) {
        return getTopic(topic, createIfMissing, null);
    }

    public CompletableFuture<Optional<Topic>> getTopic(final String topic, boolean createIfMissing,
                                                       Map<String, String> properties) {
        return getTopic(TopicName.get(topic), createIfMissing, properties);
    }

    /**
     * Retrieves or creates a topic based on the specified parameters.
     * 0. If disable PersistentTopics or NonPersistentTopics, it will return a failed future with NotAllowedException.
     * 1. If topic future exists in the cache returned directly regardless of whether it fails or timeout.
     * 2. If the topic metadata exists, the topic is created regardless of {@code createIfMissing}.
     * 3. If the topic metadata not exists, and {@code createIfMissing} is false,
     *    returns an empty Optional in a CompletableFuture. And this empty future not be added to the map.
     * 4. Otherwise, use computeIfAbsent. It returns the existing topic or creates and adds a new topicFuture.
     *    Any exceptions will remove the topicFuture from the map.
     *
     * @param topicName The name of the topic, potentially including partition information.
     * @param createIfMissing If true, creates the topic if it does not exist.
     * @param properties Topic configuration properties used during creation.
     * @return CompletableFuture with an Optional of the topic if found or created, otherwise empty.
     */
    public CompletableFuture<Optional<Topic>> getTopic(final TopicName topicName, boolean createIfMissing,
                                                       Map<String, String> properties) {
        try {
            // If topic future exists in the cache returned directly regardless of whether it fails or timeout.
            CompletableFuture<Optional<Topic>> tp = topics.get(topicName.toString());
            if (tp != null) {
                return tp;
            }
            final boolean isPersistentTopic = topicName.getDomain().equals(TopicDomain.persistent);
            if (isPersistentTopic) {
                if (!pulsar.getConfiguration().isEnablePersistentTopics()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Broker is unable to load persistent topic {}", topicName);
                    }
                    return FutureUtil.failedFuture(new NotAllowedException(
                            "Broker is unable to load persistent topic"));
                }
                return checkNonPartitionedTopicExists(topicName).thenCompose(exists -> {
                    if (!exists && !createIfMissing) {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    // The topic level policies are not needed now, but the meaning of calling
                    // "getTopicPoliciesBypassSystemTopic" will wait for system topic policies initialization.
                    return getTopicPoliciesBypassSystemTopic(topicName, TopicPoliciesService.GetType.LOCAL_ONLY)
                            .exceptionally(ex -> {
                        final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                        final String errorInfo = String.format("Topic creation encountered an exception by initialize"
                                + " topic policies service. topic_name=%s error_message=%s", topicName,
                                rc.getMessage());
                        log.error(errorInfo, rc);
                        throw FutureUtil.wrapToCompletionException(new ServiceUnitNotReadyException(errorInfo));
                    }).thenCompose(optionalTopicPolicies -> {
                        final TopicPolicies topicPolicies = optionalTopicPolicies.orElse(null);
                        if (topicName.isPartitioned()) {
                            final TopicName topicNameEntity = TopicName.get(topicName.getPartitionedTopicName());
                            return fetchPartitionedTopicMetadataAsync(topicNameEntity)
                                    .thenCompose((metadata) -> {
                                        // Allow creating non-partitioned persistent topic that name includes
                                        // `partition`
                                        if (metadata.partitions == 0
                                                || topicName.getPartitionIndex() < metadata.partitions) {
                                            return topics.computeIfAbsent(topicName.toString(), (tpName) ->
                                                    loadOrCreatePersistentTopic(tpName,
                                                            createIfMissing, properties));
                                        } else {
                                            final String errorMsg =
                                                    String.format("Illegal topic partition name %s with max allowed "
                                                            + "%d partitions", topicName, metadata.partitions);
                                            log.warn(errorMsg);
                                            return FutureUtil.failedFuture(
                                                    new BrokerServiceException.NotAllowedException(errorMsg));
                                        }
                                    });
                        } else {
                            return topics.computeIfAbsent(topicName.toString(), (tpName) ->
                                    loadOrCreatePersistentTopic(tpName, createIfMissing, properties));
                        }
                    });
                });
            } else {
                if (!pulsar.getConfiguration().isEnableNonPersistentTopics()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Broker is unable to load non-persistent topic {}", topicName);
                    }
                    return FutureUtil.failedFuture(new NotAllowedException(
                            "Broker is unable to load persistent topic"));
                }
                if (!topics.containsKey(topicName.toString())) {
                    topicEventsDispatcher.notify(topicName.toString(), TopicEvent.LOAD, EventStage.BEFORE);
                }
                if (topicName.isPartitioned()) {
                    final TopicName partitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());
                    return this.fetchPartitionedTopicMetadataAsync(partitionedTopicName).thenCompose((metadata) -> {
                        if (topicName.getPartitionIndex() < metadata.partitions) {
                            return topics.computeIfAbsent(topicName.toString(), (name) -> {
                                topicEventsDispatcher
                                        .notify(topicName.toString(), TopicEvent.CREATE, EventStage.BEFORE);

                                CompletableFuture<Optional<Topic>> res = createNonPersistentTopic(name);

                                CompletableFuture<Optional<Topic>> eventFuture = topicEventsDispatcher
                                        .notifyOnCompletion(res, topicName.toString(), TopicEvent.CREATE);
                                topicEventsDispatcher
                                        .notifyOnCompletion(eventFuture, topicName.toString(), TopicEvent.LOAD);
                                return res;
                            });
                        }
                        topicEventsDispatcher.notify(topicName.toString(), TopicEvent.LOAD, EventStage.FAILURE);
                        return CompletableFuture.completedFuture(Optional.empty());
                    });
                } else if (createIfMissing) {
                    return topics.computeIfAbsent(topicName.toString(), (name) -> {
                        topicEventsDispatcher.notify(topicName.toString(), TopicEvent.CREATE, EventStage.BEFORE);

                        CompletableFuture<Optional<Topic>> res = createNonPersistentTopic(name);

                        CompletableFuture<Optional<Topic>> eventFuture = topicEventsDispatcher
                                .notifyOnCompletion(res, topicName.toString(), TopicEvent.CREATE);
                        topicEventsDispatcher
                                .notifyOnCompletion(eventFuture, topicName.toString(), TopicEvent.LOAD);
                        return res;
                    });
                } else {
                    CompletableFuture<Optional<Topic>> topicFuture = topics.get(topicName.toString());
                    if (topicFuture == null) {
                        topicEventsDispatcher.notify(topicName.toString(), TopicEvent.LOAD, EventStage.FAILURE);
                        topicFuture = CompletableFuture.completedFuture(Optional.empty());
                    }
                    return topicFuture;
                }
            }
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Illegalargument exception when loading topic", topicName, e);
            return FutureUtil.failedFuture(e);
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ServiceUnitNotReadyException) {
                log.warn("[{}] Service unit is not ready when loading the topic", topicName);
            } else {
                log.warn("[{}] Unexpected exception when loading topic: {}", topicName, e.getMessage(), e);
            }

            return FutureUtil.failedFuture(cause);
        }
    }

    private CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesBypassSystemTopic(@NonNull TopicName topicName,
                                                                                 TopicPoliciesService.GetType type) {
        if (ExtensibleLoadManagerImpl.isInternalTopic(topicName.toString())) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return pulsar.getTopicPoliciesService().getTopicPoliciesAsync(topicName, type);
    }

    public CompletableFuture<Void> deleteTopic(String topic, boolean forceDelete) {
        topicEventsDispatcher.notify(topic, TopicEvent.DELETE, EventStage.BEFORE);
        CompletableFuture<Void> result =  deleteTopicInternal(topic, forceDelete);
        topicEventsDispatcher.notifyOnCompletion(result, topic, TopicEvent.DELETE);
        return result;
    }

    private CompletableFuture<Void> deleteTopicInternal(String topic, boolean forceDelete) {
        TopicName topicName = TopicName.get(topic);
        Optional<Topic> optTopic = getTopicReference(topic);

        if (optTopic.isPresent()) {
            Topic t = optTopic.get();
            if (forceDelete) {
                return t.deleteForcefully();
            }

            // v2 topics have a global name so check if the topic is replicated.
            if (t.isReplicated()) {
                // Delete is disallowed on global topic
                final var clusters = t.getReplicators().keySet();
                log.error("Delete forbidden topic {} is replicated on clusters {}", topic, clusters);
                return FutureUtil.failedFuture(
                        new IllegalStateException("Delete forbidden topic is replicated on clusters " + clusters));
            }

            // shadow topic should be deleted first.
            if (t.isShadowReplicated()) {
                final var shadowTopics = t.getShadowReplicators().keySet();
                log.error("Delete forbidden. Topic {} is replicated to shadow topics: {}", topic, shadowTopics);
                return FutureUtil.failedFuture(new IllegalStateException(
                        "Delete forbidden. Topic " + topic + " is replicated to shadow topics."));
            }

            return t.delete();
        }

        log.info("Topic {} is not loaded, try to delete from metadata", topic);

        // Topic is not loaded, though we still might be able to delete from metadata
        TopicName tn = TopicName.get(topic);
        if (!tn.isPersistent()) {
            // Nothing to do if it's not persistent
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Void> deleteTopicAuthenticationFuture = new CompletableFuture<>();
        deleteTopicAuthenticationWithRetry(topic, deleteTopicAuthenticationFuture, 5);
        deleteTopicAuthenticationFuture
        .thenCompose(__ -> deleteSchema(tn))
        .thenCompose(__ -> {
            if (tn.isPartitioned()) {
                return CompletableFuture.completedFuture(null);
            }
            return pulsar.getTopicPoliciesService().deleteTopicPoliciesAsync(tn);
        })
        .whenComplete((v, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                return;
            }
            CompletableFuture<ManagedLedgerConfig> mlConfigFuture = getManagedLedgerConfig(topicName);
            mlConfigFuture.thenAccept(config -> {
                getManagedLedgerFactoryForTopic(topicName, config.getStorageClassName())
                        .asyncDelete(tn.getPersistenceNamingEncoding(),
                                mlConfigFuture, new DeleteLedgerCallback() {
                                    @Override
                                    public void deleteLedgerComplete(Object ctx) {
                                        future.complete(null);
                                    }

                                    @Override
                                    public void deleteLedgerFailed(ManagedLedgerException exception,
                                                                   Object ctx) {
                                        future.completeExceptionally(exception);
                                    }
                                }, null);
            }).exceptionally(ex1 -> {
                log.error("Failed to get managed ledger config for topic {}", topic, ex1);
                future.completeExceptionally(ex1);
                return null;
            });
         });

        return future;
    }

    public CompletableFuture<ManagedLedgerFactory> getManagedLedgerFactoryForTopic(TopicName topicName) {
        return getManagedLedgerConfig(topicName)
                .thenApply(config -> {
                    String storageClassName = config.getStorageClassName();
                    return getManagedLedgerFactoryForTopic(topicName, storageClassName);
                });
    }

    public ManagedLedgerFactory getManagedLedgerFactoryForTopic(TopicName topicName, String storageClassName) {
        Optional<ManagedLedgerStorageClass> managedLedgerStorageClass =
                managedLedgerStorage.getManagedLedgerStorageClass(storageClassName);
        if (!managedLedgerStorageClass.isPresent()) {
            throw new CompletionException(new ManagedLedgerException(
                    "ManagedLedgerStorageClass " + storageClassName + " not found for topic " + topicName));
        }
        return managedLedgerStorageClass
                .get()
                .getManagedLedgerFactory();
    }

    public void deleteTopicAuthenticationWithRetry(String topic, CompletableFuture<Void> future, int count) {
        if (count == 0) {
            log.error("The number of retries has exhausted for topic {}", topic);
            future.completeExceptionally(new MetadataStoreException("The number of retries has exhausted"));
            return;
        }
        // Check whether there are auth policies for the topic
        authorizationService.removePermissionsAsync(TopicName.get(topic))
                    .thenAccept(v -> {
                        log.info("Successfully delete authentication policies for topic {}", topic);
                        future.complete(null);
                    }).exceptionally(ex1 -> {
                        if (ex1.getCause() instanceof MetadataStoreException.BadVersionException) {
                            log.warn(
                                    "Failed to delete authentication policies because of bad version. "
                                            + "Retry to delete authentication policies for topic {}",
                                    topic);
                            deleteTopicAuthenticationWithRetry(topic, future, count - 1);
                        } else {
                            log.error("Failed to delete authentication policies for topic {}", topic, ex1);
                            future.completeExceptionally(ex1);
                        }
                        return null;
                    });
    }

    private CompletableFuture<Optional<Topic>> createNonPersistentTopic(String topic) {
        CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
        topicFuture.exceptionally(t -> {
            pulsarStats.recordTopicLoadFailed();
            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
            return null;
        });
        final long topicCreateTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        NonPersistentTopic nonPersistentTopic;
        try {
            if (isSystemTopic(topic)) {
                nonPersistentTopic = new NonPersistentSystemTopic(topic, this);
            } else {
                nonPersistentTopic = newTopic(topic, null, this, NonPersistentTopic.class);
            }
            nonPersistentTopic.setCreateFuture(topicFuture);
        } catch (Throwable e) {
            log.warn("Failed to create topic {}", topic, e);
            topicFuture.completeExceptionally(e);
            return topicFuture;
        }
        CompletableFuture<Void> isOwner = checkTopicNsOwnership(topic);
        isOwner.thenRun(() -> {
            nonPersistentTopic.initialize()
                    .thenCompose(__ -> nonPersistentTopic.checkReplication())
                    .thenRun(() -> {
                        log.info("Created topic {}", nonPersistentTopic);
                        long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - topicCreateTimeMs;
                        pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
                        addTopicToStatsMaps(TopicName.get(topic), nonPersistentTopic);
                        topicFuture.complete(Optional.of(nonPersistentTopic));
                    }).exceptionally(ex -> {
                log.warn("Replication check failed. Removing topic from topics list {}, {}", topic, ex.getCause());
                nonPersistentTopic.stopReplProducers().whenComplete((v, exception) -> {
                    topicFuture.completeExceptionally(ex);
                });
                return null;
            });
        }).exceptionally(e -> {
            log.warn("CheckTopicNsOwnership fail when createNonPersistentTopic! {}", topic, e.getCause());
            // CheckTopicNsOwnership fail dont create nonPersistentTopic, when topic do lookup will find the correct
            // broker. When client get non-persistent-partitioned topic
            // metadata will the non-persistent-topic will be created.
            // so we should add checkTopicNsOwnership logic otherwise the topic will be created
            // if it dont own by this broker,we should return success
            // otherwise it will keep retrying getPartitionedTopicMetadata
            topicFuture.complete(Optional.of(nonPersistentTopic));
            // after get metadata return success, we should delete this topic from this broker, because this topic not
            // owner by this broker and it don't initialize and checkReplication
            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
            return null;
        });

        return topicFuture;
    }

    public PulsarClient getReplicationClient(String cluster, Optional<ClusterData> clusterDataOp) {
        PulsarClient client = replicationClients.get(cluster);
        if (client != null) {
            return client;
        }

        return replicationClients.computeIfAbsent(cluster, key -> {
            try {
                ClusterData data = clusterDataOp
                        .orElseThrow(() -> new MetadataStoreException.NotFoundException(cluster));
                ClientBuilder clientBuilder = PulsarClient.builder()
                        .enableTcpNoDelay(false)
                        .connectionsPerBroker(pulsar.getConfiguration().getReplicationConnectionsPerBroker())
                        .statsInterval(0, TimeUnit.SECONDS);

                // Disable memory limit for replication client
                clientBuilder.memoryLimit(0, SizeUnit.BYTES);

                // Apply all arbitrary configuration. This must be called before setting any fields annotated as
                // @Secret on the ClientConfigurationData object because of the way they are serialized.
                // See https://github.com/apache/pulsar/issues/8509 for more information.
                clientBuilder.loadConf(PropertiesUtils.filterAndMapProperties(pulsar.getConfiguration().getProperties(),
                        "brokerClient_"));

                // Disabled auto release useless connection.
                clientBuilder.connectionMaxIdleSeconds(-1);

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
                            data.isBrokerClientTlsEnabledWithKeyStore(),
                            data.isTlsAllowInsecureConnection(),
                            data.getBrokerClientTlsTrustStoreType(),
                            data.getBrokerClientTlsTrustStore(),
                            data.getBrokerClientTlsTrustStorePassword(),
                            data.getBrokerClientTlsKeyStoreType(),
                            data.getBrokerClientTlsKeyStore(),
                            data.getBrokerClientTlsKeyStorePassword(),
                            data.getBrokerClientTrustCertsFilePath(),
                            data.getBrokerClientKeyFilePath(),
                            data.getBrokerClientCertificateFilePath(),
                            pulsar.getConfiguration().isTlsHostnameVerificationEnabled(),
                            data.getBrokerClientSslFactoryPlugin(),
                            data.getBrokerClientSslFactoryPluginParams()
                    );
                } else if (pulsar.getConfiguration().isBrokerClientTlsEnabled()) {
                    configTlsSettings(clientBuilder, serviceUrlTls,
                            pulsar.getConfiguration().isBrokerClientTlsEnabledWithKeyStore(),
                            pulsar.getConfiguration().isTlsAllowInsecureConnection(),
                            pulsar.getConfiguration().getBrokerClientTlsTrustStoreType(),
                            pulsar.getConfiguration().getBrokerClientTlsTrustStore(),
                            pulsar.getConfiguration().getBrokerClientTlsTrustStorePassword(),
                            pulsar.getConfiguration().getBrokerClientTlsKeyStoreType(),
                            pulsar.getConfiguration().getBrokerClientTlsKeyStore(),
                            pulsar.getConfiguration().getBrokerClientTlsKeyStorePassword(),
                            pulsar.getConfiguration().getBrokerClientTrustCertsFilePath(),
                            pulsar.getConfiguration().getBrokerClientKeyFilePath(),
                            pulsar.getConfiguration().getBrokerClientCertificateFilePath(),
                            pulsar.getConfiguration().isTlsHostnameVerificationEnabled(),
                            pulsar.getConfiguration().getBrokerClientSslFactoryPlugin(),
                            pulsar.getConfiguration().getBrokerClientSslFactoryPluginParams()
                    );
                } else {
                    clientBuilder.serviceUrl(
                            isNotBlank(data.getBrokerServiceUrl()) ? data.getBrokerServiceUrl() : data.getServiceUrl());
                }
                if (data.getProxyProtocol() != null && StringUtils.isNotBlank(data.getProxyServiceUrl())) {
                    clientBuilder.proxyServiceUrl(data.getProxyServiceUrl(), data.getProxyProtocol());
                    log.info("Configuring proxy-url {} with protocol {}", data.getProxyServiceUrl(),
                            data.getProxyProtocol());
                }
                if (StringUtils.isNotBlank(data.getListenerName())) {
                    clientBuilder.listenerName(data.getListenerName());
                    log.info("Configuring listenerName {}", data.getListenerName());
                }
                // Share all the IO threads across broker and client connections
                ClientConfigurationData conf = ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();
                return pulsar.createClientImpl(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void configTlsSettings(ClientBuilder clientBuilder, String serviceUrl,
                                   boolean brokerClientTlsEnabledWithKeyStore, boolean isTlsAllowInsecureConnection,
                                   String brokerClientTlsTrustStoreType, String brokerClientTlsTrustStore,
                                   String brokerClientTlsTrustStorePassword, String brokerClientTlsKeyStoreType,
                                   String brokerClientTlsKeyStore, String brokerClientTlsKeyStorePassword,
                                   String brokerClientTrustCertsFilePath,
                                   String brokerClientKeyFilePath, String brokerClientCertificateFilePath,
                                   boolean isTlsHostnameVerificationEnabled, String brokerClientSslFactoryPlugin,
                                   String brokerClientSslFactoryPluginParams) {
        clientBuilder
                .serviceUrl(serviceUrl)
                .allowTlsInsecureConnection(isTlsAllowInsecureConnection)
                .enableTlsHostnameVerification(isTlsHostnameVerificationEnabled);
        if (StringUtils.isNotBlank(brokerClientSslFactoryPlugin)) {
            clientBuilder.sslFactoryPlugin(brokerClientSslFactoryPlugin)
                    .sslFactoryPluginParams(brokerClientSslFactoryPluginParams);
        }
        if (brokerClientTlsEnabledWithKeyStore) {
            clientBuilder.useKeyStoreTls(true)
                    .tlsTrustStoreType(brokerClientTlsTrustStoreType)
                    .tlsTrustStorePath(brokerClientTlsTrustStore)
                    .tlsTrustStorePassword(brokerClientTlsTrustStorePassword)
                    .tlsKeyStoreType(brokerClientTlsKeyStoreType)
                    .tlsKeyStorePath(brokerClientTlsKeyStore)
                    .tlsKeyStorePassword(brokerClientTlsKeyStorePassword);
        } else {
            clientBuilder.tlsTrustCertsFilePath(brokerClientTrustCertsFilePath)
                    .tlsKeyFilePath(brokerClientKeyFilePath)
                    .tlsCertificateFilePath(brokerClientCertificateFilePath);
        }
    }

    private void configAdminTlsSettings(PulsarAdminBuilder adminBuilder, boolean brokerClientTlsEnabledWithKeyStore,
                                        boolean isTlsAllowInsecureConnection,
                                        String brokerClientTlsTrustStoreType, String brokerClientTlsTrustStore,
                                        String brokerClientTlsTrustStorePassword, String brokerClientTlsKeyStoreType,
                                        String brokerClientTlsKeyStore, String brokerClientTlsKeyStorePassword,
                                        String brokerClientTrustCertsFilePath,
                                        String brokerClientKeyFilePath, String brokerClientCertificateFilePath,
                                        boolean isTlsHostnameVerificationEnabled, String brokerClientSslFactoryPlugin,
                                        String brokerClientSslFactoryPluginParams) {
        if (brokerClientTlsEnabledWithKeyStore) {
            adminBuilder.useKeyStoreTls(true)
                    .tlsTrustStoreType(brokerClientTlsTrustStoreType)
                    .tlsTrustStorePath(brokerClientTlsTrustStore)
                    .tlsTrustStorePassword(brokerClientTlsTrustStorePassword)
                    .tlsKeyStoreType(brokerClientTlsKeyStoreType)
                    .tlsKeyStorePath(brokerClientTlsKeyStore)
                    .tlsKeyStorePassword(brokerClientTlsKeyStorePassword);
        } else {
            adminBuilder.tlsTrustCertsFilePath(brokerClientTrustCertsFilePath)
                    .tlsKeyFilePath(brokerClientKeyFilePath)
                    .tlsCertificateFilePath(brokerClientCertificateFilePath);
        }
        adminBuilder.allowTlsInsecureConnection(isTlsAllowInsecureConnection)
                .enableTlsHostnameVerification(isTlsHostnameVerificationEnabled)
                .sslFactoryPlugin(brokerClientSslFactoryPlugin)
                .sslFactoryPluginParams(brokerClientSslFactoryPluginParams);
    }

    public PulsarAdmin getClusterPulsarAdmin(String cluster, Optional<ClusterData> clusterDataOp) {
        PulsarAdmin admin = clusterAdmins.get(cluster);
        if (admin != null) {
            return admin;
        }
        return clusterAdmins.computeIfAbsent(cluster, key -> {
            try {
                ClusterData data = clusterDataOp
                        .orElseThrow(() -> new MetadataStoreException.NotFoundException(cluster));
                PulsarAdminBuilder builder = PulsarAdmin.builder();

                ServiceConfiguration conf = pulsar.getConfig();
                // Apply all arbitrary configuration. This must be called before setting any fields annotated as
                // @Secret on the ClientConfigurationData object because of the way they are serialized.
                // See https://github.com/apache/pulsar/issues/8509 for more information.
                builder.loadConf(PropertiesUtils.filterAndMapProperties(conf.getProperties(), "brokerClient_"));

                if (data.getAuthenticationPlugin() != null && data.getAuthenticationParameters() != null) {
                    builder.authentication(data.getAuthenticationPlugin(), data.getAuthenticationParameters());
                } else {
                    builder.authentication(pulsar.getConfiguration().getBrokerClientAuthenticationPlugin(),
                            pulsar.getConfiguration().getBrokerClientAuthenticationParameters());
                }

                boolean isTlsEnabled = data.isBrokerClientTlsEnabled() || conf.isBrokerClientTlsEnabled();
                final String adminApiUrl = isTlsEnabled ? data.getServiceUrlTls() : data.getServiceUrl();
                if (StringUtils.isEmpty(adminApiUrl)) {
                    throw new IllegalArgumentException("The adminApiUrl is empty, brokerClientTlsEnabled: "
                            + isTlsEnabled);
                }
                builder.serviceHttpUrl(adminApiUrl);
                if (data.isBrokerClientTlsEnabled()) {
                    configAdminTlsSettings(builder,
                            data.isBrokerClientTlsEnabledWithKeyStore(),
                            data.isTlsAllowInsecureConnection(),
                            data.getBrokerClientTlsTrustStoreType(),
                            data.getBrokerClientTlsTrustStore(),
                            data.getBrokerClientTlsTrustStorePassword(),
                            data.getBrokerClientTlsKeyStoreType(),
                            data.getBrokerClientTlsKeyStore(),
                            data.getBrokerClientTlsKeyStorePassword(),
                            data.getBrokerClientTrustCertsFilePath(),
                            data.getBrokerClientKeyFilePath(),
                            data.getBrokerClientCertificateFilePath(),
                            pulsar.getConfiguration().isTlsHostnameVerificationEnabled(),
                            data.getBrokerClientSslFactoryPlugin(),
                            data.getBrokerClientSslFactoryPluginParams()
                    );
                } else if (conf.isBrokerClientTlsEnabled()) {
                    configAdminTlsSettings(builder,
                            conf.isBrokerClientTlsEnabledWithKeyStore(),
                            conf.isTlsAllowInsecureConnection(),
                            conf.getBrokerClientTlsTrustStoreType(),
                            conf.getBrokerClientTlsTrustStore(),
                            conf.getBrokerClientTlsTrustStorePassword(),
                            conf.getBrokerClientTlsKeyStoreType(),
                            conf.getBrokerClientTlsKeyStore(),
                            conf.getBrokerClientTlsKeyStorePassword(),
                            conf.getBrokerClientTrustCertsFilePath(),
                            conf.getBrokerClientKeyFilePath(),
                            conf.getBrokerClientCertificateFilePath(),
                            pulsar.getConfiguration().isTlsHostnameVerificationEnabled(),
                            conf.getBrokerClientSslFactoryPlugin(),
                            conf.getBrokerClientSslFactoryPluginParams()
                    );
                }

                // most of the admin request requires to make zk-call so, keep the max read-timeout based on
                // zk-operation timeout
                builder.readTimeout(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);

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
            boolean createIfMissing, Map<String, String> properties) {
        final CompletableFuture<Optional<Topic>> topicFuture = FutureUtil.createFutureWithTimeout(
                Duration.ofSeconds(pulsar.getConfiguration().getTopicLoadTimeoutSeconds()), executor(),
                () -> FAILED_TO_LOAD_TOPIC_TIMEOUT_EXCEPTION);

        topicFuture.exceptionally(t -> {
            pulsarStats.recordTopicLoadFailed();
            return null;
        });

        checkTopicNsOwnership(topic)
                .thenRun(() -> {
                    final Semaphore topicLoadSemaphore = topicLoadRequestSemaphore.get();

                    if (topicLoadSemaphore.tryAcquire()) {
                        checkOwnershipAndCreatePersistentTopic(topic, createIfMissing, topicFuture,
                                properties);
                        topicFuture.handle((persistentTopic, ex) -> {
                            // release permit and process pending topic
                            topicLoadSemaphore.release();
                            // do not recreate topic if topic is already migrated and deleted by broker
                            // so, avoid creating a new topic if migration is already started
                            if (ex != null && (ex.getCause() instanceof TopicMigratedException)) {
                                pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                                topicFuture.completeExceptionally(ex.getCause());
                                return null;
                            }
                            createPendingLoadTopic();
                            return null;
                        });
                    } else {
                        pendingTopicLoadingQueue.add(new TopicLoadingContext(topic,
                                createIfMissing, topicFuture, properties));
                        if (log.isDebugEnabled()) {
                            log.debug("topic-loading for {} added into pending queue", topic);
                        }
                    }
                }).exceptionally(ex -> {
                    pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                    topicFuture.completeExceptionally(ex.getCause());
                    return null;
                });

        return topicFuture;
    }

    @VisibleForTesting
    protected CompletableFuture<Map<String, String>> fetchTopicPropertiesAsync(TopicName topicName) {
        if (!topicName.isPartitioned()) {
            return getManagedLedgerFactoryForTopic(topicName).thenCompose(
                    managedLedgerFactory -> managedLedgerFactory.getManagedLedgerPropertiesAsync(
                            topicName.getPersistenceNamingEncoding()));
        } else {
            TopicName partitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());
            return fetchPartitionedTopicMetadataAsync(partitionedTopicName)
                    .thenCompose(metadata -> {
                        if (metadata.partitions == PartitionedTopicMetadata.NON_PARTITIONED) {
                            return getManagedLedgerFactoryForTopic(topicName).thenCompose(
                                    managedLedgerFactory -> managedLedgerFactory.getManagedLedgerPropertiesAsync(
                                            topicName.getPersistenceNamingEncoding()));
                        } else {
                            // Check if the partitioned topic is a ShadowTopic
                            if (MapUtils.getString(metadata.properties, PROPERTY_SOURCE_TOPIC_KEY) != null) {
                                String sourceTopic = metadata.properties.get(PROPERTY_SOURCE_TOPIC_KEY);
                                Map<String, String> result = new HashMap<>();
                                result.put(PROPERTY_SOURCE_TOPIC_KEY, TopicName.getTopicPartitionNameString(
                                        sourceTopic, topicName.getPartitionIndex()));
                                return CompletableFuture.completedFuture(result);
                            }
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        }
    }

    private void checkOwnershipAndCreatePersistentTopic(final String topic, boolean createIfMissing,
                                       CompletableFuture<Optional<Topic>> topicFuture,
                                       Map<String, String> properties) {
        TopicName topicName = TopicName.get(topic);
        pulsar.getNamespaceService().isServiceUnitActiveAsync(topicName)
                .thenAccept(isActive -> {
                    if (isActive) {
                        CompletableFuture<Map<String, String>> propertiesFuture;
                        if (properties == null) {
                            //Read properties from storage when loading topic.
                            propertiesFuture = fetchTopicPropertiesAsync(topicName);
                        } else {
                            propertiesFuture = CompletableFuture.completedFuture(properties);
                        }
                        propertiesFuture.thenAccept(finalProperties ->
                                //TODO add topicName in properties?
                                createPersistentTopic0(topic, createIfMissing, topicFuture,
                                        finalProperties)
                        ).exceptionally(throwable -> {
                            log.warn("[{}] Read topic property failed", topic, throwable);
                            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                            topicFuture.completeExceptionally(throwable);
                            return null;
                        });
                    } else {
                        // namespace is being unloaded
                        String msg = String.format("Namespace is being unloaded, cannot add topic %s", topic);
                        log.warn(msg);
                        pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                        topicFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
                    }
                }).exceptionally(ex -> {
                    pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                    topicFuture.completeExceptionally(ex);
                    return null;
                });
    }

    @VisibleForTesting
    public void createPersistentTopic0(final String topic, boolean createIfMissing,
                                       CompletableFuture<Optional<Topic>> topicFuture,
                                       Map<String, String> properties) {
        TopicName topicName = TopicName.get(topic);
        final long topicCreateTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

        if (isTransactionInternalName(topicName)) {
            String msg = String.format("Can not create transaction system topic %s", topic);
            log.warn(msg);
            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
            topicFuture.completeExceptionally(new NotAllowedException(msg));
            return;
        }

        CompletableFuture<Void> maxTopicsCheck = createIfMissing
                ? checkMaxTopicsPerNamespace(topicName)
                : CompletableFuture.completedFuture(null);

        CompletableFuture<Void> isTopicAlreadyMigrated = checkTopicAlreadyMigrated(topicName);

        maxTopicsCheck.thenCompose(__ -> isTopicAlreadyMigrated)
                .thenCompose(__ -> getManagedLedgerConfig(topicName))
        .thenAccept(managedLedgerConfig -> {
            if (isBrokerEntryMetadataEnabled() || isBrokerPayloadProcessorEnabled()) {
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
                managedLedgerConfig.setManagedLedgerInterceptor(
                        new ManagedLedgerInterceptorImpl(interceptors, brokerEntryPayloadProcessors));
            }
            managedLedgerConfig.setCreateIfMissing(createIfMissing);
            managedLedgerConfig.setProperties(properties);
            String shadowSource = managedLedgerConfig.getShadowSource();
            if (shadowSource != null) {
                managedLedgerConfig.setShadowSourceName(TopicName.get(shadowSource).getPersistenceNamingEncoding());
            }

            topicEventsDispatcher.notify(topic, TopicEvent.LOAD, EventStage.BEFORE);
            // load can fail with topicFuture completed non-exceptionally
            // work around this
            final CompletableFuture<Void> loadFuture = new CompletableFuture<>();
            topicFuture.whenComplete((res, ex) -> {
                if (ex == null) {
                    loadFuture.complete(null);
                } else {
                    loadFuture.completeExceptionally(ex);
                }
            });

            if (createIfMissing) {
                topicEventsDispatcher.notify(topic, TopicEvent.CREATE, EventStage.BEFORE);
                topicEventsDispatcher.notifyOnCompletion(topicFuture, topic, TopicEvent.CREATE);
            }
            topicEventsDispatcher.notifyOnCompletion(loadFuture, topic, TopicEvent.LOAD);

            // Once we have the configuration, we can proceed with the async open operation
            ManagedLedgerFactory managedLedgerFactory =
                    getManagedLedgerFactoryForTopic(topicName, managedLedgerConfig.getStorageClassName());
            managedLedgerFactory.asyncOpen(topicName.getPersistenceNamingEncoding(), managedLedgerConfig,
                    new OpenLedgerCallback() {
                        @Override
                        public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                            try {
                                PersistentTopic persistentTopic = isSystemTopic(topic)
                                        ? new SystemTopic(topic, ledger, BrokerService.this)
                                        : newTopic(topic, ledger, BrokerService.this, PersistentTopic.class);
                                persistentTopic.setCreateFuture(topicFuture);
                                persistentTopic
                                        .initialize()
                                        .thenCompose(__ -> persistentTopic.preCreateSubscriptionForCompactionIfNeeded())
                                        .thenCompose(__ -> persistentTopic.checkReplication())
                                        .thenCompose(v -> {
                                            // Also check dedup status
                                            return persistentTopic.checkDeduplicationStatus();
                                        })
                                        .thenRun(() -> {
                                            log.info("Created topic {} - dedup is {}", topic,
                                            persistentTopic.isDeduplicationEnabled() ? "enabled" : "disabled");
                                            long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
                                                                        - topicCreateTimeMs;
                                            pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
                                            if (!topicFuture.complete(Optional.of(persistentTopic))) {
                                                // Check create persistent topic timeout.
                                                if (topicFuture.isCompletedExceptionally()) {
                                                    log.warn("{} future is already completed with failure {}, closing"
                                                        + " the topic", topic, FutureUtil.getException(topicFuture));
                                                } else {
                                                    // It should not happen.
                                                    log.error("{} future is already completed by another thread, "
                                                            + "which is not expected. Closing the current one", topic);
                                                }
                                                executor().submit(() -> {
                                                    persistentTopic.close().whenComplete((ignore, ex) -> {
                                                        topics.remove(topic, topicFuture);
                                                        if (ex != null) {
                                                            log.warn("[{}] Get an error when closing topic.",
                                                                    topic, ex);
                                                        }
                                                    });
                                                });
                                            } else {
                                                addTopicToStatsMaps(topicName, persistentTopic);
                                            }
                                        })
                                        .exceptionally((ex) -> {
                                            log.warn("Replication or dedup check failed."
                                                    + " Removing topic from topics list {}, {}", topic, ex);
                                            executor().submit(() -> {
                                                persistentTopic.close().whenComplete((ignore, closeEx) -> {
                                                    topics.remove(topic, topicFuture);
                                                    if (closeEx != null) {
                                                        log.warn("[{}] Get an error when closing topic.",
                                                                topic, closeEx);
                                                    }
                                                    topicFuture.completeExceptionally(ex);
                                                });
                                            });
                                            return null;
                                        });
                            } catch (Exception e) {
                                log.warn("Failed to create topic {}: {}", topic, e.getMessage());
                                pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                                topicFuture.completeExceptionally(e);
                            }
                        }

                        @Override
                        public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            if (!createIfMissing && exception instanceof ManagedLedgerNotFoundException) {
                                // We were just trying to load a topic and the topic doesn't exist
                                pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                                loadFuture.completeExceptionally(exception);
                                topicFuture.complete(Optional.empty());
                            } else {
                                log.warn("Failed to create topic {}", topic, exception);
                                pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                                topicFuture.completeExceptionally(new PersistenceException(exception));
                            }
                        }
                    }, () -> isTopicNsOwnedByBrokerAsync(topicName), null);

        }).exceptionally((exception) -> {
            boolean migrationFailure = exception.getCause() instanceof TopicMigratedException;
            String msg = migrationFailure ? "Topic is already migrated" :
                "Failed to get topic configuration:";
            log.warn("[{}] {} {}", topic, msg, exception.getMessage(), exception);
            // remove topic from topics-map in different thread to avoid possible deadlock if
            // createPersistentTopic-thread only tries to handle this future-result
            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
            topicFuture.completeExceptionally(exception);
            return null;
        });
    }

    private CompletableFuture<Void> checkTopicAlreadyMigrated(TopicName topicName) {
        if (ExtensibleLoadManagerImpl.isInternalTopic(topicName.toString())
                || SystemTopicNames.isEventSystemTopic(topicName)
                || NamespaceService.isHeartbeatNamespace(topicName.getNamespaceObject())) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        AbstractTopic.isClusterMigrationEnabled(pulsar, topicName.toString()).handle((isMigrated, ex) -> {
            if (isMigrated) {
                result.completeExceptionally(
                        new BrokerServiceException.TopicMigratedException(topicName + " already migrated"));
            } else {
                result.complete(null);
            }
            return null;
        });
        return result;
    }

    public CompletableFuture<ManagedLedgerConfig> getManagedLedgerConfig(@NonNull TopicName topicName) {
        requireNonNull(topicName);
        NamespaceName namespace = topicName.getNamespaceObject();
        ServiceConfiguration serviceConfig = pulsar.getConfiguration();

        NamespaceResources nsr = pulsar.getPulsarResources().getNamespaceResources();
        LocalPoliciesResources lpr = pulsar.getPulsarResources().getLocalPolicies();
        final CompletableFuture<Optional<TopicPolicies>> topicPoliciesFuture =
                getTopicPoliciesBypassSystemTopic(topicName, TopicPoliciesService.GetType.LOCAL_ONLY);
        final CompletableFuture<Optional<TopicPolicies>> globalTopicPoliciesFuture =
                getTopicPoliciesBypassSystemTopic(topicName, TopicPoliciesService.GetType.GLOBAL_ONLY);
        final CompletableFuture<Optional<Policies>> nsPolicies = nsr.getPoliciesAsync(namespace);
        final CompletableFuture<Optional<LocalPolicies>> lcPolicies = lpr.getLocalPoliciesAsync(namespace);
        return topicPoliciesFuture.thenCombine(globalTopicPoliciesFuture, (topicP, globalTopicP) -> {
            return new ImmutablePair<>(topicP, globalTopicP);
        }).thenCombine(nsPolicies, (topicPoliciesPair, np) -> {
            return new ImmutablePair<>(topicPoliciesPair, np);
        }).thenCombine(lcPolicies, (combined, localPolicies) -> {
            Optional<TopicPolicies> topicP = combined.getLeft().getLeft();
            Optional<TopicPolicies> globalTopicP = combined.getLeft().getRight();
            Optional<Policies> policies = combined.getRight();

            PersistencePolicies persistencePolicies = null;
            RetentionPolicies retentionPolicies = null;
            OffloadPoliciesImpl topicLevelOffloadPolicies = null;
            if (topicP.isPresent() && topicP.get().getPersistence() != null) {
                persistencePolicies = topicP.get().getPersistence();
            } else if (globalTopicP.isPresent() && globalTopicP.get().getPersistence() != null) {
                persistencePolicies = globalTopicP.get().getPersistence();
            }
            if (topicP.isPresent() && topicP.get().getRetentionPolicies() != null) {
                retentionPolicies = topicP.get().getRetentionPolicies();
            } else if (globalTopicP.isPresent() && globalTopicP.get().getRetentionPolicies() != null) {
                retentionPolicies = globalTopicP.get().getRetentionPolicies();
            }
            if (topicP.isPresent() && topicP.get().getOffloadPolicies() != null) {
                topicLevelOffloadPolicies = topicP.get().getOffloadPolicies();
            } else if (globalTopicP.isPresent() && globalTopicP.get().getOffloadPolicies() != null) {
                topicLevelOffloadPolicies = globalTopicP.get().getOffloadPolicies();
            }

            if (persistencePolicies == null) {
                persistencePolicies = policies.map(p -> p.persistence).orElseGet(
                        () -> new PersistencePolicies(serviceConfig.getManagedLedgerDefaultEnsembleSize(),
                                serviceConfig.getManagedLedgerDefaultWriteQuorum(),
                                serviceConfig.getManagedLedgerDefaultAckQuorum(),
                                serviceConfig.getManagedLedgerDefaultMarkDeleteRateLimit()));
            }

            if (retentionPolicies == null) {
                if (SystemTopicNames.isSystemTopic(topicName)) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Disable data retention policy for system topic.", topicName);
                    }
                    retentionPolicies = new RetentionPolicies(0, 0);
                } else {
                    retentionPolicies = policies.map(p -> p.retention_policies).orElseGet(
                            () -> new RetentionPolicies(serviceConfig.getDefaultRetentionTimeInMinutes(),
                                    serviceConfig.getDefaultRetentionSizeInMB())
                    );
                }
            }

            ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
            managedLedgerConfig.setEnsembleSize(persistencePolicies.getBookkeeperEnsemble());
            managedLedgerConfig.setWriteQuorumSize(persistencePolicies.getBookkeeperWriteQuorum());
            managedLedgerConfig.setAckQuorumSize(persistencePolicies.getBookkeeperAckQuorum());
            managedLedgerConfig.setStorageClassName(persistencePolicies.getManagedLedgerStorageClassName());

            if (serviceConfig.isStrictBookieAffinityEnabled()) {
                managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyClassName(
                        IsolatedBookieEnsemblePlacementPolicy.class);
                if (localPolicies.isPresent() && localPolicies.get().bookieAffinityGroup != null) {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                            localPolicies.get().bookieAffinityGroup.getBookkeeperAffinityGroupPrimary());
                    properties.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                            localPolicies.get().bookieAffinityGroup.getBookkeeperAffinityGroupSecondary());
                    managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyProperties(properties);
                } else if (isSystemTopic(topicName)) {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "*");
                    properties.put(IsolatedBookieEnsemblePlacementPolicy
                            .SECONDARY_ISOLATION_BOOKIE_GROUPS, "*");
                    managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyProperties(properties);
                } else {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "");
                    properties.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "");
                    managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyProperties(properties);
                }
            } else {
                if (localPolicies.isPresent() && localPolicies.get().bookieAffinityGroup != null) {
                    managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyClassName(
                            IsolatedBookieEnsemblePlacementPolicy.class);
                    Map<String, Object> properties = new HashMap<>();
                    properties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                            localPolicies.get().bookieAffinityGroup.getBookkeeperAffinityGroupPrimary());
                    properties.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                            localPolicies.get().bookieAffinityGroup.getBookkeeperAffinityGroupSecondary());
                    managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyProperties(properties);
                }
            }

            managedLedgerConfig.setThrottleMarkDelete(persistencePolicies.getManagedLedgerMaxMarkDeleteRate());
            managedLedgerConfig.setDigestType(serviceConfig.getManagedLedgerDigestType());
            managedLedgerConfig.setPassword(serviceConfig.getManagedLedgerPassword());

            managedLedgerConfig
                    .setMaxUnackedRangesToPersist(serviceConfig.getManagedLedgerMaxUnackedRangesToPersist());
            managedLedgerConfig.setMaxBatchDeletedIndexToPersist(
                    serviceConfig.getManagedLedgerMaxBatchDeletedIndexToPersist());
            managedLedgerConfig
                    .setPersistIndividualAckAsLongArray(serviceConfig.isManagedLedgerPersistIndividualAckAsLongArray());
            managedLedgerConfig.setPersistentUnackedRangesWithMultipleEntriesEnabled(
                    serviceConfig.isPersistentUnackedRangesWithMultipleEntriesEnabled());
            managedLedgerConfig.setMaxUnackedRangesToPersistInMetadataStore(
                    serviceConfig.getManagedLedgerMaxUnackedRangesToPersistInMetadataStore());
            managedLedgerConfig.setMaxEntriesPerLedger(serviceConfig.getManagedLedgerMaxEntriesPerLedger());
            managedLedgerConfig
                    .setMinimumRolloverTime(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(),
                            TimeUnit.MINUTES);
            managedLedgerConfig
                    .setMaximumRolloverTime(serviceConfig.getManagedLedgerMaxLedgerRolloverTimeMinutes(),
                            TimeUnit.MINUTES);
            managedLedgerConfig.setMaxSizePerLedgerMb(serviceConfig.getManagedLedgerMaxSizePerLedgerMbytes());

            managedLedgerConfig.setMetadataOperationsTimeoutSeconds(
                    serviceConfig.getManagedLedgerMetadataOperationsTimeoutSeconds());
            managedLedgerConfig
                    .setReadEntryTimeoutSeconds(serviceConfig.getManagedLedgerReadEntryTimeoutSeconds());
            managedLedgerConfig
                    .setAddEntryTimeoutSeconds(serviceConfig.getManagedLedgerAddEntryTimeoutSeconds());
            managedLedgerConfig.setMetadataEnsembleSize(serviceConfig.getManagedLedgerDefaultEnsembleSize());
            managedLedgerConfig.setUnackedRangesOpenCacheSetEnabled(
                    serviceConfig.isManagedLedgerUnackedRangesOpenCacheSetEnabled());
            managedLedgerConfig.setMetadataWriteQuorumSize(serviceConfig.getManagedLedgerDefaultWriteQuorum());
            managedLedgerConfig.setMetadataAckQuorumSize(serviceConfig.getManagedLedgerDefaultAckQuorum());
            managedLedgerConfig
                    .setMetadataMaxEntriesPerLedger(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger());

            managedLedgerConfig
                    .setLedgerRolloverTimeout(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds());
            managedLedgerConfig
                    .setRetentionTime(retentionPolicies.getRetentionTimeInMinutes(), TimeUnit.MINUTES);
            managedLedgerConfig.setRetentionSizeInMB(retentionPolicies.getRetentionSizeInMB());
            managedLedgerConfig.setAutoSkipNonRecoverableData(serviceConfig.isAutoSkipNonRecoverableData());
            managedLedgerConfig.setLedgerForceRecovery(serviceConfig.isManagedLedgerForceRecovery());
            managedLedgerConfig.setLazyCursorRecovery(serviceConfig.isLazyCursorRecovery());
            managedLedgerConfig.setInactiveLedgerRollOverTime(
                    serviceConfig.getManagedLedgerInactiveLedgerRolloverTimeSeconds(), TimeUnit.SECONDS);
            managedLedgerConfig.setInactiveOffloadedLedgerEvictionTime(
                    serviceConfig.getManagedLedgerInactiveOffloadedLedgerEvictionTimeSeconds(),
                    TimeUnit.SECONDS);

            managedLedgerConfig.setCacheEvictionByMarkDeletedPosition(
                    serviceConfig.isCacheEvictionByMarkDeletedPosition());
            managedLedgerConfig.setMinimumBacklogCursorsForCaching(
                    serviceConfig.getManagedLedgerMinimumBacklogCursorsForCaching());
            managedLedgerConfig.setMinimumBacklogEntriesForCaching(
                    serviceConfig.getManagedLedgerMinimumBacklogEntriesForCaching());
            managedLedgerConfig.setMaxBacklogBetweenCursorsForCaching(
                    serviceConfig.getManagedLedgerMaxBacklogBetweenCursorsForCaching());

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
                    throw new RuntimeException(e);
                }
            } else {
                //If the topic level policy is null, use the namespace level
                managedLedgerConfig
                        .setLedgerOffloader(pulsar.getManagedLedgerOffloader(namespace, offloadPolicies));
            }
            if (managedLedgerConfig.getLedgerOffloader() != null
                    && managedLedgerConfig.getLedgerOffloader().isAppendable()
                    && (NamespaceService.isSystemServiceNamespace(namespace.toString())
                            || SystemTopicNames.isSystemTopic(topicName))) {
                managedLedgerConfig.setLedgerOffloader(
                        new NonAppendableLedgerOffloader(managedLedgerConfig.getLedgerOffloader()));
            }

            managedLedgerConfig.setTriggerOffloadOnTopicLoad(serviceConfig.isTriggerOffloadOnTopicLoad());

            managedLedgerConfig.setDeletionAtBatchIndexLevelEnabled(
                    serviceConfig.isAcknowledgmentAtBatchIndexLevelEnabled());
            managedLedgerConfig.setNewEntriesCheckDelayInMillis(
                    serviceConfig.getManagedLedgerNewEntriesCheckDelayInMillis());
            return managedLedgerConfig;
        }).exceptionally(ex -> {
            final Throwable rc = FutureUtil.unwrapCompletionException(ex);
            final String errorInfo = String.format("Topic creation encountered an exception by initialize"
                            + " topic policies service. topic_name=%s error_message=%s", topicName,
                    rc.getMessage());
            log.error(errorInfo, rc);
            throw FutureUtil.wrapToCompletionException(new ServiceUnitNotReadyException(errorInfo));
        });
    }

    private void addTopicToStatsMaps(TopicName topicName, Topic topic) {
        pulsar.getNamespaceService().getBundleAsync(topicName)
                .thenAccept(namespaceBundle -> {
                    if (namespaceBundle != null) {
                        synchronized (multiLayerTopicsMap) {
                            String serviceUnit = namespaceBundle.toString();
                            multiLayerTopicsMap.computeIfAbsent(topicName.getNamespace(),
                                    __ -> new ConcurrentHashMap<>()
                            ).computeIfAbsent(serviceUnit, __ -> new ConcurrentHashMap<>()
                            ).put(topicName.toString(), topic);
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
        requireNonNull(oldBundle);
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

    public void checkClusterMigration() {
        forEachTopic(Topic::checkClusterMigration);
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
                            managedLedger.rolloverCursorsInBackground();
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

    /**
     * Iterates over all loaded topics in the broker.
     */
    public void forEachTopic(Consumer<Topic> consumer) {
        topics.forEach((n, t) -> {
            Optional<Topic> topic = extractTopic(t);
            topic.ifPresent(consumer::accept);
        });
    }

    public void forEachPersistentTopic(Consumer<PersistentTopic> consumer) {
        topics.values().stream().map(BrokerService::extractTopic)
                .map(topicOp -> topicOp.filter(topic -> topic instanceof PersistentTopic))
                .forEach(topicOp -> topicOp.ifPresent(topic -> consumer.accept((PersistentTopic) topic)));
    }

    public BacklogQuotaManager getBacklogQuotaManager() {
        return this.backlogQuotaManager;
    }

    public void monitorBacklogQuota() {
        long startTimeMillis = System.currentTimeMillis();
        forEachPersistentTopic(topic -> {
            topic.updateOldPositionInfo().thenAccept(__ -> {
                if (topic.isSizeBacklogExceeded()) {
                    getBacklogQuotaManager().handleExceededBacklogQuota(topic,
                            BacklogQuota.BacklogQuotaType.destination_storage, false);
                } else {
                    topic.checkTimeBacklogExceeded(false).thenAccept(isExceeded -> {
                        if (isExceeded) {
                            getBacklogQuotaManager().handleExceededBacklogQuota(topic,
                                    BacklogQuota.BacklogQuotaType.message_age,
                                    pulsar.getConfiguration().isPreciseTimeBasedBacklogQuotaCheck());
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("quota not exceeded for [{}]", topic.getName());
                            }
                        }
                    });
                }
            }).whenComplete((unused, throwable) -> {
                if (throwable != null) {
                    log.error("Error when checkBacklogQuota({}) in monitorBacklogQuota",
                            topic.getName(), throwable);
                }
                backlogQuotaCheckDuration.observe(
                        MILLISECONDS.toSeconds(System.currentTimeMillis() - startTimeMillis));
            });
        });
    }

    public CompletableFuture<Boolean> isTopicNsOwnedByBrokerAsync(TopicName topicName) {
        return pulsar.getNamespaceService().isServiceUnitOwnedAsync(topicName)
                .handle((hasOwnership, t) -> {
                    if (t == null) {
                        return hasOwnership;
                    } else {
                        log.warn("Failed to check the ownership of the topic: {}, {}", topicName, t.getMessage());
                        return false;
                    }
                });
    }

    public CompletableFuture<Void> checkTopicNsOwnership(final String topic) {
        TopicName topicName = TopicName.get(topic);
        final var namespaceService = pulsar.getNamespaceService();

        return namespaceService.getBundleAsync(topicName).thenCompose(bundle ->
                namespaceService.checkBundleOwnership(topicName, bundle).thenCompose(ownedByThisInstance -> {
                    if (ownedByThisInstance) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        String msg = String.format("Namespace bundle (%s) for topic (%s) not served by this instance:"
                                + "%s. Please redo the lookup.", bundle, topic, pulsar.getBrokerId());
                        log.warn(msg);
                        return FutureUtil.failedFuture(new ServiceUnitNotReadyException(msg));
                    }
                }));
    }

    public CompletableFuture<Integer> unloadServiceUnit(NamespaceBundle serviceUnit,
            boolean disconnectClients,
            boolean closeWithoutWaitingClientDisconnect, long timeout, TimeUnit unit) {
        CompletableFuture<Integer> future = unloadServiceUnit(
                serviceUnit, disconnectClients, closeWithoutWaitingClientDisconnect);
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
     * @param disconnectClients disconnect clients
     * @param closeWithoutWaitingClientDisconnect don't wait for clients to disconnect
     *                                           and forcefully close managed-ledger
     * @return
     */
    private CompletableFuture<Integer> unloadServiceUnit(NamespaceBundle serviceUnit,
                                                         boolean disconnectClients,
                                                         boolean closeWithoutWaitingClientDisconnect) {
        List<CompletableFuture<Void>> closeFutures = new ArrayList<>();
        topics.forEach((name, topicFuture) -> {
            TopicName topicName = TopicName.get(name);
            if (serviceUnit.includes(topicName)) {
                if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)
                        && ExtensibleLoadManagerImpl.isInternalTopic(topicName.toString())) {
                    if (ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log)) {
                        log.info("[{}] Skip unloading ExtensibleLoadManager internal topics. Such internal topic "
                                + "should be closed when shutting down the broker.", topicName);
                    }
                    return;
                }

                // Topic needs to be unloaded
                log.info("[{}] Unloading topic", topicName);
                if (topicFuture.isCompletedExceptionally()) {
                    try {
                        topicFuture.get();
                    } catch (InterruptedException | ExecutionException ex) {
                        if (ex.getCause() instanceof ServiceUnitNotReadyException) {
                            // Topic was already unloaded
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Topic was already unloaded", topicName);
                            }
                            return;
                        } else {
                            log.warn("[{}] Got exception when closing topic", topicName, ex);
                        }
                    }
                }
                closeFutures.add(topicFuture
                        .thenCompose(t -> t.isPresent() ? t.get().close(
                                disconnectClients, closeWithoutWaitingClientDisconnect)
                                : CompletableFuture.completedFuture(null))
                        .exceptionally(e -> {
                            if (e.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException
                                    && e.getMessage().contains("Please redo the lookup")) {
                                log.warn("[{}] Topic ownership check failed. Skipping it", topicName);
                                return null;
                            }
                            throw FutureUtil.wrapToCompletionException(e);
                        }));
            }
        });

        if (getPulsar().getConfig().isTransactionCoordinatorEnabled()
                && serviceUnit.getNamespaceObject().equals(NamespaceName.SYSTEM_NAMESPACE)) {
            TransactionMetadataStoreService metadataStoreService =
                    this.getPulsar().getTransactionMetadataStoreService();
            // if the store belongs to this bundle, remove and close the store
            this.getPulsar().getTransactionMetadataStoreService().getStores().values().stream().filter(store ->
                    serviceUnit.includes(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN
                            .getPartition((int) (store.getTransactionCoordinatorID().getId()))))
                    .map(TransactionMetadataStore::getTransactionCoordinatorID)
                    .forEach(tcId -> closeFutures.add(metadataStoreService.removeTransactionMetadataStore(tcId)));
        }

        return FutureUtil.waitForAll(closeFutures).thenApply(v -> closeFutures.size());
    }

    public void cleanUnloadedTopicFromCache(NamespaceBundle serviceUnit) {
        for (String topic : topics.keySet()) {
            TopicName topicName = TopicName.get(topic);
            if (serviceUnit.includes(topicName) && getTopicReference(topic).isPresent()) {
                log.info("[{}][{}] Clean unloaded topic from cache.", serviceUnit.toString(), topic);
                pulsar.getBrokerService().removeTopicFromCache(topicName.toString(), serviceUnit, null);
            }
        }
    }

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
    }

    /**
     * Removes the topic from the cache only if the topicName and associated createFuture match exactly.
     * The TopicEvent.UNLOAD event will be triggered before and after removal.
     *
     * @param topic The topic to be removed.
     * @return A CompletableFuture that completes when the operation is done.
     */
    public CompletableFuture<Void> removeTopicFromCache(AbstractTopic topic) {
        TopicName topicName = TopicName.get(topic.getName());
        return pulsar.getNamespaceService().getBundleAsync(topicName)
                .thenAccept(namespaceBundle -> {
                    removeTopicFromCache(topic.getName(), namespaceBundle, topic.getCreateFuture());
                });
    }

    private void removeTopicFromCache(String topic, NamespaceBundle namespaceBundle,
                                     CompletableFuture<Optional<Topic>> createTopicFuture) {
        String bundleName = namespaceBundle.toString();
        String namespaceName = TopicName.get(topic).getNamespaceObject().toString();

        topicEventsDispatcher.notify(topic, TopicEvent.UNLOAD, EventStage.BEFORE);

        synchronized (multiLayerTopicsMap) {
            final var namespaceMap = multiLayerTopicsMap.get(namespaceName);
            if (namespaceMap != null) {
                final var bundleMap = namespaceMap.get(bundleName);
                if (bundleMap != null) {
                    bundleMap.remove(topic);
                    if (bundleMap.isEmpty()) {
                        namespaceMap.remove(bundleName);
                    }
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

        if (createTopicFuture == null) {
            topics.remove(topic);
        } else {
            topics.remove(topic, createTopicFuture);
        }

        Compactor compactor = pulsar.getNullableCompactor();
        if (compactor != null) {
            compactor.getStats().removeTopic(topic);
        }
        topicEventsDispatcher.notify(topic, TopicEvent.UNLOAD, EventStage.SUCCESS);
    }

    public long getNumberOfNamespaceBundles() {
        this.numberOfNamespaceBundles = 0;
        this.multiLayerTopicsMap.forEach((namespaceName, bundles) -> {
            this.numberOfNamespaceBundles += bundles.size();
        });
        return this.numberOfNamespaceBundles;
    }


    private void handleMetadataChanges(Notification n) {
        if (pulsar.getState() != PulsarService.State.Started) {
            String brokerId;
            try {
                brokerId = pulsar.getBrokerId();
            } catch (Exception ex) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to get brokerId", ex);
                }
                // If broker is not running, we cannot get brokerId.
                brokerId = "unknown";
            }
            // Ignore metadata changes when broker is not running
            log.info("Ignoring metadata change since broker is not running (id={}, state={}) {}", brokerId,
                    pulsar.getState(), n);
            return;
        }
        if (n.getType() == NotificationType.Modified && NamespaceResources.pathIsFromNamespace(n.getPath())) {
            NamespaceName ns = NamespaceResources.namespaceFromPath(n.getPath());
            handlePoliciesUpdates(ns);
        } else if (n.getType() == NotificationType.Modified
                && NamespaceResources.pathIsNamespaceLocalPolicies(n.getPath())) {
            NamespaceName ns = NamespaceResources.namespaceFromLocalPoliciesPath(n.getPath());
            handleLocalPoliciesUpdates(ns);
        } else if (pulsar().getPulsarResources().getDynamicConfigResources().isDynamicConfigurationPath(n.getPath())) {
            handleDynamicConfigurationUpdates();
        }
        // Ignore unrelated notifications
    }

    private void handleLocalPoliciesUpdates(NamespaceName namespace) {
        pulsar.getPulsarResources().getLocalPolicies().getLocalPoliciesAsync(namespace)
                .thenAcceptAsync(optLocalPolicies -> {
                    if (!optLocalPolicies.isPresent()) {
                        return;
                    }
                    LocalPolicies localPolicies = optLocalPolicies.get();
                    log.info("[{}] updating with {}", namespace, localPolicies);
                    topics.forEach((name, topicFuture) -> {
                        if (namespace.includes(TopicName.get(name))) {
                            // If the topic is already created, immediately apply the updated policies, otherwise
                            // once the topic is created it'll apply the policies update
                            topicFuture.thenAccept(topic -> {
                                if (log.isDebugEnabled()) {
                                    log.debug("Notifying topic that local policies have changed: {}", name);
                                }
                                topic.ifPresent(t -> {
                                    if (t instanceof PersistentTopic) {
                                        PersistentTopic topic1 = (PersistentTopic) t;
                                        topic1.onLocalPoliciesUpdate();
                                    }
                                });
                            });
                        }
                    });
                }, pulsar.getExecutor());
    }

    private void handlePoliciesUpdates(NamespaceName namespace) {
        pulsar.getPulsarResources().getNamespaceResources().getPoliciesAsync(namespace)
                .thenAcceptAsync(optPolicies -> {
                    if (!optPolicies.isPresent() || optPolicies.get().deleted) {
                        return;
                    }

                    Policies policies = optPolicies.get();
                    log.info("[{}] updating with {}", namespace, policies);

                    topics.forEach((name, topicFuture) -> {
                        if (namespace.includes(TopicName.get(name))) {
                            // If the topic is already created, immediately apply the updated policies, otherwise
                            // once the topic is created it'll apply the policies update
                            topicFuture.thenAccept(topic -> {
                                if (log.isDebugEnabled()) {
                                    log.debug("Notifying topic that policies have changed: {}", name);
                                }

                                topic.ifPresent(t -> t.onPoliciesUpdate(policies));
                            });
                        }
                    });

                    // sometimes, some brokers don't receive policies-update watch and miss to remove
                    // replication-cluster and still own the bundle. That can cause data-loss for TODO: git-issue
                    unloadDeletedReplNamespace(policies, namespace);
                }, pulsar.getExecutor());
    }

    private void handleDynamicConfigurationUpdates() {
        DynamicConfigurationResources dynamicConfigResources = null;
        try {
            dynamicConfigResources = pulsar()
                    .getPulsarResources()
                    .getDynamicConfigResources();
        } catch (Exception e) {
            log.warn("Failed to read dynamic broker configuration", e);
        }

        if (dynamicConfigResources != null) {
            dynamicConfigResources.getDynamicConfigurationAsync()
                .thenAccept(optMap -> {
                    // Case some dynamic configs have been removed.
                    dynamicConfigurationMap.forEach((configKey, fieldWrapper) -> {
                        boolean configRemoved = optMap.isEmpty() || !optMap.get().containsKey(configKey);
                        if (fieldWrapper.lastDynamicValue != null && configRemoved) {
                            configValueChanged(configKey, null);
                        }
                    });
                    // Some configs have been changed.
                    if (!optMap.isPresent()) {
                        return;
                    }
                    Map<String, String> data = optMap.get();
                    data.forEach((configKey, value) -> {
                        configValueChanged(configKey, value);
                    });
                });
        }
    }

    private void configValueChanged(String configKey, String newValueStr) {
        ConfigField configFieldWrapper = dynamicConfigurationMap.get(configKey);
        if (configFieldWrapper == null) {
            log.warn("{} does not exist in dynamicConfigurationMap, skip this config.", configKey);
            return;
        }
        Consumer listener = configRegisteredListeners.get(configKey);
        try {
            // Convert existingValue and newValue.
            final Object existingValue;
            final Object newValue;
            if (configFieldWrapper.field != null) {
                if (StringUtils.isBlank(newValueStr)) {
                    newValue = configFieldWrapper.defaultValue;
                } else {
                    newValue = FieldParser.value(newValueStr, configFieldWrapper.field);
                }
                existingValue = configFieldWrapper.field.get(pulsar.getConfiguration());
                configFieldWrapper.field.set(pulsar.getConfiguration(), newValue);
            } else {
                // This case only occurs when it is a customized item.
                // See: https://github.com/apache/pulsar/blob/master/pip/pip-300.md.
                log.info("Skip update customized dynamic configuration {}/{} in memory, only trigger an event"
                        + " listeners.", configKey, newValueStr);
                existingValue = configFieldWrapper.lastDynamicValue;
                newValue = newValueStr == null ? configFieldWrapper.defaultValue : newValueStr;
            }
            // Record the latest dynamic config.
            configFieldWrapper.lastDynamicValue = newValueStr;

            if (newValueStr == null) {
                log.info("Successfully remove the dynamic configuration {}, and revert to the default value",
                        configKey);
            } else {
                log.info("Successfully updated configuration {}/{}", configKey, newValueStr);
            }

            if (listener != null && !Objects.equals(existingValue, newValue)) {
                // So far, all config items that related to configuration listeners, their default value is not null.
                // And the customized config can be null before.
                // So call "listener.accept(null)" is okay.
                listener.accept(newValue);
            }
        } catch (Exception e) {
            log.error("Failed to update config {}", configKey, e);
        }
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
            pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundlesAsync(namespace).thenAccept(bundles -> {
                bundles.getBundles().forEach(bundle -> {
                    pulsar.getNamespaceService().isNamespaceBundleOwned(bundle).thenAccept(isExist -> {
                        if (isExist) {
                            this.pulsar().getExecutor().execute(() -> {
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
            });
        }
    }

    public PulsarService pulsar() {
        return pulsar;
    }

    public EventLoopGroup executor() {
        return workerGroup;
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

        forEachTopic(topic -> stats.put(topic.getName(), topic.getStats(false, false, false)));

        return stats;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public List<Topic> getAllTopicsFromNamespaceBundle(String namespace, String bundle) {
        final var map1 = multiLayerTopicsMap.get(namespace);
        if (map1 == null) {
            return Collections.emptyList();
        }

        final var map2 = map1.get(bundle);
        if (map2 == null) {
            return Collections.emptyList();
        }

        return map2.values().stream().toList();
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

        // (2) Listener Registration
        // add listener on "maxConcurrentLookupRequest" value change
        registerConfigurationListener("maxConcurrentLookupRequest",
                (maxConcurrentLookupRequest) -> lookupRequestSemaphore.set(
                        new Semaphore((int) maxConcurrentLookupRequest, false)));
        // add listener on "maxConcurrentTopicLoadRequest" value change
        registerConfigurationListener("maxConcurrentTopicLoadRequest",
                (maxConcurrentTopicLoadRequest) -> topicLoadRequestSemaphore.set(
                        new Semaphore((int) maxConcurrentTopicLoadRequest, false)));
        registerConfigurationListener("loadManagerClassName", className -> {
            pulsar.getExecutor().execute(() -> {
                LoadManager newLoadManager = null;
                try {
                    newLoadManager = LoadManager.create(pulsar);
                    log.info("Created load manager: {}", className);
                    pulsar.getLoadManager().get().stop();
                    newLoadManager.start();
                } catch (Exception ex) {
                    log.warn("Failed to change load manager", ex);
                    try {
                        if (newLoadManager != null) {
                            newLoadManager.stop();
                            newLoadManager = null;
                        }
                    } catch (PulsarServerException e) {
                        log.warn("Failed to close created load manager", e);
                    }
                }
                if (newLoadManager != null) {
                    pulsar.getLoadManager().set(newLoadManager);
                }
            });
        });


        ManagedLedgerFactory defaultManagedLedgerFactory =
                managedLedgerStorage.getDefaultStorageClass().getManagedLedgerFactory();

        //  add listener to notify broker managedLedgerCacheSizeMB dynamic config
        registerConfigurationListener("managedLedgerCacheSizeMB", (managedLedgerCacheSizeMB) -> {
            defaultManagedLedgerFactory.getEntryCacheManager()
                    .updateCacheSizeAndThreshold(((int) managedLedgerCacheSizeMB) * 1024L * 1024L);
        });

        //  add listener to notify broker managedLedgerCacheEvictionWatermark dynamic config
        registerConfigurationListener(
                "managedLedgerCacheEvictionWatermark", (cacheEvictionWatermark) -> {
                    defaultManagedLedgerFactory.getEntryCacheManager()
                            .updateCacheEvictionWatermark((double) cacheEvictionWatermark);
                });

        //  add listener to notify broker managedLedgerCacheEvictionTimeThresholdMillis dynamic config
        registerConfigurationListener(
                "managedLedgerCacheEvictionTimeThresholdMillis", (cacheEvictionTimeThresholdMills) -> {
                    defaultManagedLedgerFactory.updateCacheEvictionTimeThreshold(MILLISECONDS
                            .toNanos((long) cacheEvictionTimeThresholdMills));
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
        // add listener to update "dispatcherPauseOnAckStatePersistentEnabled" in byte for subscription
        registerConfigurationListener("dispatcherPauseOnAckStatePersistentEnabled", (dispatchRatePerTopicInByte) -> {
            updateDispatchPauseOnAckStatePersistentEnabled();
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

        // add listener to update topic publish-rate dynamic config
        registerConfigurationListener("maxPublishRatePerTopicInMessages",
            maxPublishRatePerTopicInMessages -> updateMaxPublishRatePerTopicInMessages()
        );
        registerConfigurationListener("maxPublishRatePerTopicInBytes",
            maxPublishRatePerTopicInMessages -> updateMaxPublishRatePerTopicInMessages()
        );

        // add listener to update subscribe-rate dynamic config
        registerConfigurationListener("subscribeThrottlingRatePerConsumer",
            subscribeThrottlingRatePerConsumer -> updateSubscribeRate());
        registerConfigurationListener("subscribeRatePeriodPerConsumerInSecond",
            subscribeRatePeriodPerConsumerInSecond -> updateSubscribeRate());

        // add listener to notify broker publish-rate dynamic config
        registerConfigurationListener("brokerPublisherThrottlingMaxMessageRate",
                (brokerPublisherThrottlingMaxMessageRate) ->
                        updateBrokerPublisherThrottlingMaxRate());
        registerConfigurationListener("brokerPublisherThrottlingMaxByteRate",
                (brokerPublisherThrottlingMaxByteRate) ->
                        updateBrokerPublisherThrottlingMaxRate());
        // add listener to notify broker dispatch-rate dynamic config
        registerConfigurationListener("dispatchThrottlingRateInMsg",
                (dispatchThrottlingRateInMsg) ->
                        updateBrokerDispatchThrottlingMaxRate());
        registerConfigurationListener("dispatchThrottlingRateInByte",
                (dispatchThrottlingRateInByte) ->
                        updateBrokerDispatchThrottlingMaxRate());

        // add listener to notify topic subscriptionTypesEnabled changed.
        registerConfigurationListener("subscriptionTypesEnabled", this::updateBrokerSubscriptionTypesEnabled);

        // add listener to notify partitioned topic defaultNumPartitions changed
        registerConfigurationListener("defaultNumPartitions", defaultNumPartitions -> {
            this.updateDefaultNumPartitions((int) defaultNumPartitions);
        });

        // add listener to notify partitioned topic maxNumPartitionsPerPartitionedTopic changed
        registerConfigurationListener("maxNumPartitionsPerPartitionedTopic", maxNumPartitions -> {
            this.updateMaxNumPartitionsPerPartitionedTopic((int) maxNumPartitions);
        });

        // add listener to notify web service httpRequestsFailOnUnknownPropertiesEnabled changed.
        registerConfigurationListener("httpRequestsFailOnUnknownPropertiesEnabled", enabled -> {
            pulsar.getWebService().updateHttpRequestsFailOnUnknownPropertiesEnabled((boolean) enabled);
        });

        // add listener to notify web service httpRequestsFailOnUnknownPropertiesEnabled changed.
        registerConfigurationListener("configurationMetadataSyncEventTopic", enabled -> {
            pulsar.initConfigMetadataSynchronizerIfNeeded();
        });

        // add more listeners here

        // (3) create dynamic-config if not exist.
        createDynamicConfigPathIfNotExist();

        // (4) update ServiceConfiguration value by reading zk-configuration-map and trigger corresponding listeners.
        handleDynamicConfigurationUpdates();
    }

    private void updateDefaultNumPartitions(int numPartitions) {
        int maxNumPartitions = pulsar.getConfiguration().getMaxNumPartitionsPerPartitionedTopic();
        if (maxNumPartitions == 0 || maxNumPartitions > numPartitions) {
            this.pulsar.getConfiguration().setDefaultNumPartitions(numPartitions);
        } else {
            this.pulsar.getConfiguration().setDefaultNumPartitions(maxNumPartitions);
        }
    }

    private void updateMaxNumPartitionsPerPartitionedTopic(int maxNumPartitions) {
        if (maxNumPartitions == 0) {
            this.pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(maxNumPartitions);
            return;
        }
        if (this.pulsar.getConfiguration().getDefaultNumPartitions() > maxNumPartitions) {
            this.pulsar.getConfiguration().setDefaultNumPartitions(maxNumPartitions);
        }
        this.pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(maxNumPartitions);
    }

    private void updateBrokerDispatchThrottlingMaxRate() {
        if (brokerDispatchRateLimiter == null) {
            brokerDispatchRateLimiter = dispatchRateLimiterFactory.createBrokerDispatchRateLimiter(this);
        } else {
            brokerDispatchRateLimiter.updateDispatchRate();
        }
    }

    private void updateMaxPublishRatePerTopicInMessages() {
        this.pulsar().getExecutor().execute(() ->
            forEachTopic(topic -> {
                if (topic instanceof AbstractTopic) {
                    ((AbstractTopic) topic).updateBrokerPublishRate();
                    ((AbstractTopic) topic).updatePublishRateLimiter();
                }
            }));
    }

    private void updateSubscribeRate() {
        this.pulsar().getExecutor().execute(() ->
            forEachTopic(topic -> {
                if (topic instanceof PersistentTopic) {
                    ((PersistentTopic) topic).updateBrokerSubscribeRate();
                    ((PersistentTopic) topic).updateSubscribeRateLimiter();
                }
            }));
    }

    private void updateBrokerPublisherThrottlingMaxRate() {
        int currentMaxMessageRate = pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate();
        long currentMaxByteRate = pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate();

        final PublishRate publishRate = new PublishRate(currentMaxMessageRate, currentMaxByteRate);

        log.info("Update broker publish rate limiting {}", publishRate);
        brokerPublishRateLimiter.update(publishRate);
    }

    private void updateTopicMessageDispatchRate() {
        this.pulsar().getExecutor().execute(() -> {
            // update message-rate for each topic
            forEachTopic(topic -> {
                if (topic instanceof AbstractTopic) {
                    ((AbstractTopic) topic).updateBrokerDispatchRate();
                    ((AbstractTopic) topic).updateDispatchRateLimiter();
                }
            });
        });
    }

    private void updateDispatchPauseOnAckStatePersistentEnabled() {
        this.pulsar().getExecutor().execute(() -> {
            forEachTopic(topic -> {
                if (topic instanceof PersistentTopic) {
                    // Update policies.
                    PersistentTopic persistentTopic = (PersistentTopic) topic;
                    persistentTopic.updateBrokerDispatchPauseOnAckStatePersistentEnabled();
                }
            });
        });
    }

    private void updateBrokerSubscriptionTypesEnabled(Object subscriptionTypesEnabled) {
        this.pulsar().getExecutor().execute(() -> {
            // update subscriptionTypesEnabled
            forEachTopic(topic -> {
                if (topic instanceof AbstractTopic) {
                    ((AbstractTopic) topic).updateBrokerSubscriptionTypesEnabled();
                }
            });
        });
    }

    private void updateSubscriptionMessageDispatchRate() {
        this.pulsar().getExecutor().execute(() -> {
            // update message-rate for each topic subscription
            forEachTopic(topic -> {
                if (topic instanceof AbstractTopic) {
                    ((AbstractTopic) topic).updateBrokerSubscriptionDispatchRate();
                }
                topic.getSubscriptions().forEach((subName, persistentSubscription) -> {
                    Dispatcher dispatcher = persistentSubscription.getDispatcher();
                    if (dispatcher != null) {
                        dispatcher.updateRateLimiter();
                    }
                });
            });
        });
    }

    private void updateReplicatorMessageDispatchRate() {
        this.pulsar().getExecutor().execute(() -> {
            // update message-rate for each topic Replicator in Geo-replication
            forEachTopic(topic -> {
                    if (topic instanceof AbstractTopic) {
                        ((AbstractTopic) topic).updateBrokerReplicatorDispatchRate();
                    }
                    topic.getReplicators().forEach((name, persistentReplicator) ->
                        persistentReplicator.updateRateLimiter());
                    topic.getShadowReplicators().forEach((name, persistentReplicator) ->
                        persistentReplicator.updateRateLimiter());
                }
            );
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
     *
     * Note: The new value that the {@param listener} may accept could be a null value.
     *
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
        dynamicConfigurationMap.get(key).validator = validator;
    }

    private void validateConfigKey(String key) {
        if (!dynamicConfigurationMap.containsKey(key)) {
            throw new IllegalArgumentException(key + " doesn't exits in the dynamicConfigurationMap");
        }
    }

    /**
     * Allows the third-party plugin to register a custom dynamic configuration.
     */
    public void registerCustomDynamicConfiguration(String key, Predicate<String> validator) {
        if (dynamicConfigurationMap.containsKey(key)) {
            throw new IllegalArgumentException(key + " already exists in the dynamicConfigurationMap");
        }
        ConfigField configField = ConfigField.newCustomConfigField(null);
        configField.validator = validator;
        dynamicConfigurationMap.put(key, configField);
    }

    private void createDynamicConfigPathIfNotExist() {
        try {
            Optional<Map<String, String>> configCache =
                    pulsar().getPulsarResources().getDynamicConfigResources().getDynamicConfiguration();

            // create dynamic-config if not exist.
            if (!configCache.isPresent()) {
                pulsar().getPulsarResources().getDynamicConfigResources()
                        .setDynamicConfigurationWithCreate(n -> new HashMap<>());
            }
        } catch (Exception e) {
            log.warn("Failed to read dynamic broker configuration", e);
        }
    }

    public DelayedDeliveryTrackerFactory getDelayedDeliveryTrackerFactory() {
        return delayedDeliveryTrackerFactory;
    }

    public List<String> getDynamicConfiguration() {
        return dynamicConfigurationMap.keySet().stream().toList();
    }

    public Map<String, String> getRuntimeConfiguration() {
        Map<String, String> configMap = new HashMap<>();
        ConcurrentHashMap<String, Object> runtimeConfigurationMap = getRuntimeConfigurationMap();
        runtimeConfigurationMap.forEach((key, value) -> {
            configMap.put(key, String.valueOf(value));
        });
        return configMap;
    }

    public boolean isDynamicConfiguration(String key) {
        return dynamicConfigurationMap.containsKey(key);
    }

    public boolean validateDynamicConfiguration(String key, String value) {
        if (dynamicConfigurationMap.containsKey(key) && dynamicConfigurationMap.get(key).validator != null) {
            return dynamicConfigurationMap.get(key).validator.test(value);
        }
        return true;
    }

    private Map<String, ConfigField> prepareDynamicConfigurationMap() {
        final var dynamicConfigurationMap = new ConcurrentHashMap<String, ConfigField>();
        try {
            for (Field field : ServiceConfiguration.class.getDeclaredFields()) {
                if (field != null && field.isAnnotationPresent(FieldContext.class)) {
                    field.setAccessible(true);
                    if (field.getAnnotation(FieldContext.class).dynamic()) {
                        Object defaultValue = field.get(pulsar.getConfiguration());
                        dynamicConfigurationMap.put(field.getName(), new ConfigField(field, defaultValue));
                    }
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException ex) {
            // This error never occurs.
            log.error("Failed to initialize dynamic configuration map", ex);
            throw new RuntimeException(ex);
        }
        return dynamicConfigurationMap;
    }

    private ConcurrentHashMap<String, Object> getRuntimeConfigurationMap() {
        final var runtimeConfigurationMap = new ConcurrentHashMap<String, Object>();
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
        TopicLoadingContext pendingTopic = pendingTopicLoadingQueue.poll();
        if (pendingTopic == null) {
            return;
        }

        final String topic = pendingTopic.getTopic();
        checkTopicNsOwnership(topic).thenRun(() -> {
            CompletableFuture<Optional<Topic>> pendingFuture = pendingTopic.getTopicFuture();
            final Semaphore topicLoadSemaphore = topicLoadRequestSemaphore.get();
            final boolean acquiredPermit = topicLoadSemaphore.tryAcquire();
            checkOwnershipAndCreatePersistentTopic(topic,
                    pendingTopic.isCreateIfMissing(),
                    pendingFuture,
                    pendingTopic.getProperties());
            pendingFuture.handle((persistentTopic, ex) -> {
                // release permit and process next pending topic
                if (acquiredPermit) {
                    topicLoadSemaphore.release();
                }
                createPendingLoadTopic();
                return null;
            });
        }).exceptionally(e -> {
            log.error("Failed to create pending topic {}", topic, e);
            pendingTopic.getTopicFuture()
                    .completeExceptionally((e instanceof RuntimeException && e.getCause() != null) ? e.getCause() : e);
            // schedule to process next pending topic
            inactivityMonitor.schedule(this::createPendingLoadTopic, 100, MILLISECONDS);
            return null;
        });
    }

    public CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(
            TopicName topicName) {
        if (pulsar.getNamespaceService() == null) {
            return FutureUtil.failedFuture(new NamingException("namespace service is not ready"));
        }
        return pulsar.getNamespaceService().checkTopicExistsAsync(topicName).thenComposeAsync(topicExistsInfo -> {
            final boolean topicExists = topicExistsInfo.isExists();
            final TopicType topicType = topicExistsInfo.getTopicType();
            final Integer partitions = topicExistsInfo.getPartitions();
            topicExistsInfo.recycle();

            // Topic exists.
            if (topicExists) {
                if (topicType.equals(TopicType.PARTITIONED)) {
                    return CompletableFuture.completedFuture(new PartitionedTopicMetadata(partitions));
                }
                return CompletableFuture.completedFuture(new PartitionedTopicMetadata(0));
            }

            // Try created if allowed to create a partitioned topic automatically.
            return pulsar.getPulsarResources().getNamespaceResources().getPoliciesAsync(topicName.getNamespaceObject())
                .thenComposeAsync(policies -> {
                    return isAllowAutoTopicCreationAsync(topicName, policies).thenComposeAsync(allowed -> {
                        // Not Allow auto-creation.
                        if (!allowed) {
                            // Do not change the original behavior, or default return a non-partitioned topic.
                            return CompletableFuture.completedFuture(new PartitionedTopicMetadata(0));
                        }

                        // Allow auto create non-partitioned topic.
                        boolean autoCreatePartitionedTopic = pulsar.getBrokerService()
                                .isDefaultTopicTypePartitioned(topicName, policies);
                        if (!autoCreatePartitionedTopic || topicName.isPartitioned()) {
                            return CompletableFuture.completedFuture(new PartitionedTopicMetadata(0));
                        }

                        // Create partitioned metadata.
                        return pulsar.getBrokerService().createDefaultPartitionedTopicAsync(topicName, policies)
                            .exceptionallyCompose(ex -> {
                                // The partitioned topic might be created concurrently.
                                if (ex.getCause() instanceof MetadataStoreException.AlreadyExistsException) {
                                    log.info("[{}] The partitioned topic is already created, try to refresh the cache"
                                            + " and read again.", topicName);
                                    CompletableFuture<PartitionedTopicMetadata> recheckFuture =
                                            fetchPartitionedTopicMetadataAsync(topicName, true);
                                    recheckFuture.exceptionally(ex2 -> {
                                        // Just for printing a log if error occurs.
                                        log.error("[{}] Fetch partitioned topic metadata failed", topicName, ex);
                                        return null;
                                    });
                                    return recheckFuture;
                                } else {
                                    log.error("[{}] operation of creating partitioned topic metadata failed",
                                            topicName, ex);
                                    return CompletableFuture.failedFuture(ex);
                                }
                            });
                    }, pulsar.getExecutor()).exceptionallyCompose(ex -> {
                        log.error("[{}] operation of get partitioned metadata failed due to calling"
                                        + " isAllowAutoTopicCreationAsync failed",
                                topicName, ex);
                        return CompletableFuture.failedFuture(ex);
                    });
            }, pulsar.getExecutor());
        }, pulsar.getExecutor());
    }

    @SuppressWarnings("deprecation")
    private CompletableFuture<PartitionedTopicMetadata> createDefaultPartitionedTopicAsync(TopicName topicName,
                                                                                        Optional<Policies> policies) {
        final int defaultNumPartitions = pulsar.getBrokerService().getDefaultNumPartitions(topicName, policies);
        final int maxPartitions = pulsar().getConfig().getMaxNumPartitionsPerPartitionedTopic();
        checkArgument(defaultNumPartitions > 0,
                "Default number of partitions should be more than 0");
        checkArgument(maxPartitions <= 0 || defaultNumPartitions <= maxPartitions,
                "Number of partitions should be less than or equal to " + maxPartitions);

        PartitionedTopicMetadata configMetadata = new PartitionedTopicMetadata(defaultNumPartitions);

        return checkMaxTopicsPerNamespace(topicName, defaultNumPartitions, true)
                .thenCompose(__ -> {
                    PartitionedTopicResources partitionResources = pulsar.getPulsarResources().getNamespaceResources()
                            .getPartitionedTopicResources();
                    return partitionResources.createPartitionedTopicAsync(topicName, configMetadata)
                            .thenApply(v -> {
                                log.info("partitioned metadata successfully created for {}", topicName);
                                return configMetadata;
                            });
                });
    }

    public CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataAsync(TopicName topicName) {
        return fetchPartitionedTopicMetadataAsync(topicName, false);
    }

    public CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataAsync(TopicName topicName,
                                                                                      boolean refreshCacheAndGet) {
        // gets the number of partitions from the configuration cache
        return pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(topicName, refreshCacheAndGet).thenApply(metadata -> {
                    // if the partitioned topic is not found in metadata, then the topic is not partitioned
                    return metadata.orElseGet(() -> new PartitionedTopicMetadata());
                });
    }

    public OrderedExecutor getTopicOrderedExecutor() {
        return topicOrderedExecutor;
    }

    /**
     * If per-broker unacked message reached to limit then it blocks dispatcher if its unacked message limit has been
     * reached to {@link #maxUnackedMsgsPerDispatcher}.
     *
     * @param dispatcher
     * @param numberOfMessages
     */
    public void addUnAckedMessages(AbstractPersistentDispatcherMultipleConsumers dispatcher, int numberOfMessages) {
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
                unblockDispatchersOnUnAckMessages(blockedDispatchers.stream().toList());
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
                    if (persistentSubscription.getDispatcher()
                            instanceof AbstractPersistentDispatcherMultipleConsumers) {
                        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                                (AbstractPersistentDispatcherMultipleConsumers) persistentSubscription.getDispatcher();
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
    public void unblockDispatchersOnUnAckMessages(List<AbstractPersistentDispatcherMultipleConsumers> dispatcherList) {
        lock.writeLock().lock();
        try {
            dispatcherList.forEach(dispatcher -> {
                dispatcher.unBlockDispatcherOnUnackedMsgs();
                dispatcher.readMoreEntriesAsync();
                log.info("[{}] Dispatcher is unblocked", dispatcher.getName());
                blockedDispatchers.remove(dispatcher);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Initializes the in-memory delayed delivery tracker factory when
     * BucketDelayedDeliveryTrackerFactory.newTracker failed.
     */
    public synchronized void initializeFallbackDelayedDeliveryTrackerFactory() {
        if (fallbackDelayedDeliveryTrackerFactory != null) {
            return;
        }

        DelayedDeliveryTrackerFactory factory = new InMemoryDelayedDeliveryTrackerFactory();
        try {
            factory.initialize(pulsar);
            this.fallbackDelayedDeliveryTrackerFactory = factory;
        } catch (Exception e) {
            // it should never go here
            log.error("Failed to initialize InMemoryDelayedDeliveryTrackerFactory", e);
        }
    }

    private static class ConfigField {
        // field holds the pulsar dynamic configuration.
        final Field field;

        // It is the dynamic config value if set.
        // It is null if has does not set a dynamic config, even if the value of "pulsar.config" is present.
        volatile String lastDynamicValue;

        // The default value of "pulsar.config", which is initialized when the broker is starting.
        // After the dynamic config has been removed, revert the config to this default value.
        final Object defaultValue;

        Predicate<String> validator;

        public ConfigField(Field field, Object defaultValue) {
            super();
            this.field = field;
            this.defaultValue = defaultValue;
        }

        public static ConfigField newCustomConfigField(String customValue) {
            ConfigField configField = new ConfigField(null, null);
            configField.lastDynamicValue = customValue;
            return configField;
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

    public CompletableFuture<Boolean> isAllowAutoTopicCreationAsync(final String topic) {
        TopicName topicName = TopicName.get(topic);
        return isAllowAutoTopicCreationAsync(topicName);
    }

    public CompletableFuture<Boolean> isAllowAutoTopicCreationAsync(final TopicName topicName) {
        return pulsar.getPulsarResources().getNamespaceResources()
                        .getPoliciesAsync(topicName.getNamespaceObject())
                .thenCompose(policies -> isAllowAutoTopicCreationAsync(topicName, policies));
    }

    private CompletableFuture<Boolean> isAllowAutoTopicCreationAsync(final TopicName topicName,
                                                                     final Optional<Policies> policies) {
        if (policies.isPresent() && policies.get().deleted) {
            log.info("Preventing AutoTopicCreation on a namespace that is being deleted {}",
                    topicName.getNamespaceObject());
            return CompletableFuture.completedFuture(false);
        }

        // ExtensibleLoadManagerImpl.internal topics expects to be non-partitioned-topics now.
        // We don't allow the auto-creation here.
        // ExtensibleLoadManagerImpl.start() is responsible to create the internal system topics.
        if (ExtensibleLoadManagerImpl.isInternalTopic(topicName.toString())) {
            return CompletableFuture.completedFuture(false);
        }

        //Other system topics can be created automatically
        if (pulsar.getConfiguration().isSystemTopicEnabled() && isSystemTopic(topicName)) {
            return CompletableFuture.completedFuture(true);
        }

        final boolean allowed;
        AutoTopicCreationOverride autoTopicCreationOverride = getAutoTopicCreationOverride(topicName, policies);
        if (autoTopicCreationOverride != null) {
            allowed = autoTopicCreationOverride.isAllowAutoTopicCreation();
        } else {
            allowed = pulsar.getConfiguration().isAllowAutoTopicCreation();
        }

        if (topicName.isPartitioned()) {
            TopicName partitionedTopic = TopicName.get(topicName.getPartitionedTopicName());
            // cannot re-create topic while it is being deleted
            return pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(partitionedTopic, true)
                .thenApply(partitionedTopicMetadata -> {
                    if (partitionedTopicMetadata.isEmpty()) {
                        return allowed;
                    }
                    if (partitionedTopicMetadata.get().deleted) {
                        return false;
                    }
                    return partitionedTopicMetadata.get().partitions > topicName.getPartitionIndex();
                });
        } else {
            return CompletableFuture.completedFuture(allowed);
        }

    }

    public boolean isDefaultTopicTypePartitioned(final TopicName topicName, final Optional<Policies> policies) {
        if (topicName.getPartitionedTopicName().endsWith(DLQ_GROUP_TOPIC_SUFFIX)
                || topicName.getPartitionedTopicName().endsWith(RETRY_GROUP_TOPIC_SUFFIX)) {
            return false;
        }
        AutoTopicCreationOverride autoTopicCreationOverride = getAutoTopicCreationOverride(topicName, policies);
        if (autoTopicCreationOverride != null) {
            return TopicType.PARTITIONED.toString().equals(autoTopicCreationOverride.getTopicType());
        } else {
            return pulsar.getConfiguration().isDefaultTopicTypePartitioned();
        }
    }

    public int getDefaultNumPartitions(final TopicName topicName, final Optional<Policies> policies) {
        AutoTopicCreationOverride autoTopicCreationOverride = getAutoTopicCreationOverride(topicName, policies);
        if (autoTopicCreationOverride != null) {
            return autoTopicCreationOverride.getDefaultNumPartitions();
        } else {
            return pulsar.getConfiguration().getDefaultNumPartitions();
        }
    }

    private AutoTopicCreationOverride getAutoTopicCreationOverride(final TopicName topicName,
                                                                   Optional<Policies> policies) {
        // If namespace policies have the field set, it will override the broker-level setting
        if (policies.isPresent() && policies.get().autoTopicCreationOverride != null) {
            return policies.get().autoTopicCreationOverride;
        }
        log.debug("No autoTopicCreateOverride policy found for {}", topicName);
        return null;
    }

    public @NonNull CompletableFuture<Boolean> isAllowAutoSubscriptionCreationAsync(@NonNull TopicName tpName) {
        requireNonNull(tpName);
        // Policies priority: topic level -> namespace level -> broker level
        if (ExtensibleLoadManagerImpl.isInternalTopic(tpName.toString())) {
            return CompletableFuture.completedFuture(true);
        }
        return pulsar.getTopicPoliciesService()
                .getTopicPoliciesAsync(tpName, TopicPoliciesService.GetType.LOCAL_ONLY)
                .thenCompose(optionalTopicPolicies -> {
                    Boolean allowed = optionalTopicPolicies.map(TopicPolicies::getAutoSubscriptionCreationOverride)
                            .map(AutoSubscriptionCreationOverrideImpl::isAllowAutoSubscriptionCreation)
                            .orElse(null);
                    if (allowed != null) {
                        return CompletableFuture.completedFuture(allowed);
                    }
                    // namespace level policies
                    return pulsar.getPulsarResources().getNamespaceResources().getPoliciesAsync(
                            tpName.getNamespaceObject()
                    ).thenApply(optionalPolicies -> optionalPolicies.map(__ -> __.autoSubscriptionCreationOverride)
                            .map(AutoSubscriptionCreationOverride::isAllowAutoSubscriptionCreation)
                            // broker level policies
                            .orElse(pulsar.getConfiguration().isAllowAutoSubscriptionCreation()));
                });
    }

    public boolean isSystemTopic(String topic) {
        return isSystemTopic(TopicName.get(topic));
    }

    public boolean isSystemTopic(TopicName topicName) {
        return NamespaceService.isSystemServiceNamespace(topicName.getNamespace())
                || SystemTopicNames.isSystemTopic(topicName);
    }

    public CompletableFuture<SchemaVersion> deleteSchema(TopicName topicName) {
        // delete schema at the upper level when deleting the partitioned topic.
        if (topicName.isPartitioned()) {
            return CompletableFuture.completedFuture(null);
        }
        String base = topicName.getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return getPulsar().getSchemaRegistryService().deleteSchemaStorage(id).whenComplete((vid, ex) -> {
            if (vid != null && ex == null) {
                // It's different from `SchemasResource.deleteSchema`
                // because when we delete a topic, the schema
                // history is meaningless. But when we delete a schema of a topic, a new schema could be
                // registered in the future.
                log.info("Deleted schema storage of id: {}", id);
            }
        });
    }

    private CompletableFuture<Boolean> checkNonPartitionedTopicExists(TopicName topicName) {
        return pulsar.getPulsarResources().getTopicResources().persistentTopicExists(topicName);
    }

    private CompletableFuture<Void> checkMaxTopicsPerNamespace(TopicName topicName) {
        if (isSystemTopic(topicName)) {
            return CompletableFuture.completedFuture(null);
        }
        return checkNonPartitionedTopicExists(topicName).thenCompose(exists -> exists
                ? CompletableFuture.completedFuture(null)
                : checkMaxTopicsPerNamespace(topicName, 1, false));
    }

    private CompletableFuture<Void> checkMaxTopicsPerNamespace(TopicName topicName, int numPartitions,
                                                               boolean validateSystemTopic) {
        if (validateSystemTopic) {
            if (isSystemTopic(topicName)) {
                return CompletableFuture.completedFuture(null);
            }
        }
        return pulsar.getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(topicName.getNamespaceObject())
                .thenCompose(optPolicies -> {
                    int maxTopicsPerNamespace = optPolicies.map(p -> p.max_topics_per_namespace)
                            .orElse(pulsar.getConfig().getMaxTopicsPerNamespace());

                    if (maxTopicsPerNamespace > 0 && !isSystemTopic(topicName)) {
                        return pulsar().getPulsarResources().getTopicResources()
                                .getExistingPartitions(topicName)
                                .thenCompose(topics -> {
                                    // exclude created system topic
                                    long topicsCount = topics.stream()
                                            .filter(t -> !isSystemTopic(TopicName.get(t)))
                                            .count();
                                    if (topicsCount + numPartitions > maxTopicsPerNamespace) {
                                        log.error("Failed to create persistent topic {}, "
                                                + "exceed maximum number of topics in namespace", topicName);
                                        return FutureUtil.failedFuture(
                                                new NotAllowedException(
                                                        "Exceed maximum number of topics in namespace."));
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
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

    public boolean isBrokerPayloadProcessorEnabled() {
        return !brokerEntryPayloadProcessors.isEmpty();
    }

    public void recordConnectionPaused() {
        rateLimitedConnectionsCounter.add(1, ConnectionRateLimitOperationName.PAUSED.attributes);
    }

    public void recordConnectionResumed() {
        rateLimitedConnectionsCounter.add(1, ConnectionRateLimitOperationName.RESUMED.attributes);
    }

    public void recordConnectionThrottled() {
        rateLimitedConnectionsCounter.add(1, ConnectionRateLimitOperationName.THROTTLED.attributes);
        throttledConnectionsGauge.inc();
    }

    public void recordConnectionUnthrottled() {
        rateLimitedConnectionsCounter.add(1, ConnectionRateLimitOperationName.UNTHROTTLED.attributes);
        throttledConnectionsGauge.dec();
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    public <T extends Topic> T newTopic(String topic, ManagedLedger ledger, BrokerService brokerService,
            Class<T> topicClazz) throws PulsarServerException {
        if (topicFactory != null) {
            try {
                Topic newTopic = topicFactory.create(topic, ledger, brokerService, topicClazz);
                if (newTopic != null) {
                    return (T) newTopic;
                }
            } catch (Throwable e) {
                log.warn("Failed to create persistent topic using factory {}", topic, e);
                throw new PulsarServerException("Topic factory failed to create topic ", e);
            }
        }
        return topicClazz == NonPersistentTopic.class ? (T) new NonPersistentTopic(topic, BrokerService.this)
                : (T) new PersistentTopic(topic, ledger, brokerService);
    }

    private TopicFactory createPersistentTopicFactory() throws Exception {
        String topicFactoryClassName = pulsar.getConfig().getTopicFactoryClassName();
        if (StringUtils.isNotBlank(topicFactoryClassName)) {
            try {
                return (TopicFactory) Class.forName(topicFactoryClassName).newInstance();
            } catch (Exception e) {
                log.warn("Failed to initialize topic factory class {}", topicFactoryClassName, e);
                throw e;
            }
        }
        return null;
    }

    @VisibleForTesting
    public void setPulsarChannelInitializerFactory(PulsarChannelInitializer.Factory factory) {
        this.pulsarChannelInitFactory = factory;
    }

    @AllArgsConstructor
    @Getter
    private static class TopicLoadingContext {
        private final String topic;
        private final boolean createIfMissing;
        private final CompletableFuture<Optional<Topic>> topicFuture;
        private final Map<String, String> properties;
    }
}
