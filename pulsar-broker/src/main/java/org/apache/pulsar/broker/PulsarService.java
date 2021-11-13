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
package org.apache.pulsar.broker;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.resourcegroup.ResourceUsageTransportManager.DISABLE_RESOURCE_USAGE_TRANSPORT_MANAGER;
import static org.apache.pulsar.common.naming.TopicName.TRANSACTION_COORDINATOR_LOG;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.servlet.ServletException;
import javax.websocket.DeploymentException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.Offloaders;
import org.apache.bookkeeper.mledger.offload.OffloadersCache;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.intercept.BrokerInterceptors;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.LoadReportUpdaterTask;
import org.apache.pulsar.broker.loadbalance.LoadResourceQuotaUpdaterTask;
import org.apache.pulsar.broker.loadbalance.LoadSheddingTask;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.protocol.ProtocolHandlers;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupService;
import org.apache.pulsar.broker.resourcegroup.ResourceUsageTopicTransportManager;
import org.apache.pulsar.broker.resourcegroup.ResourceUsageTransportManager;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.GracefulExecutorServicesShutdown;
import org.apache.pulsar.broker.service.SystemTopicBaseTxnBufferSnapshotService;
import org.apache.pulsar.broker.service.SystemTopicBasedTopicPoliciesService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotService;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.stats.MetricsGenerator;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferClientImpl;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.validator.MultipleListenerValidator;
import org.apache.pulsar.broker.web.WebService;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlet;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServletWithClassLoader;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServletWithPulsarService;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlets;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.TwoPhaseCompactor;
import org.apache.pulsar.functions.worker.ErrorNotifier;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.packages.management.core.PackagesManagement;
import org.apache.pulsar.packages.management.core.PackagesStorage;
import org.apache.pulsar.packages.management.core.PackagesStorageProvider;
import org.apache.pulsar.packages.management.core.impl.DefaultPackagesStorageConfiguration;
import org.apache.pulsar.packages.management.core.impl.PackagesManagementImpl;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.websocket.WebSocketConsumerServlet;
import org.apache.pulsar.websocket.WebSocketPingPongServlet;
import org.apache.pulsar.websocket.WebSocketProducerServlet;
import org.apache.pulsar.websocket.WebSocketReaderServlet;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.zookeeper.ZooKeeper;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for Pulsar broker service.
 */

@Getter(AccessLevel.PUBLIC)
@Setter(AccessLevel.PROTECTED)
public class PulsarService implements AutoCloseable, ShutdownService {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarService.class);
    private static final double GRACEFUL_SHUTDOWN_TIMEOUT_RATIO_OF_TOTAL_TIMEOUT = 0.5d;
    private ServiceConfiguration config = null;
    private NamespaceService nsService = null;
    private ManagedLedgerStorage managedLedgerClientFactory = null;
    private LeaderElectionService leaderElectionService = null;
    private BrokerService brokerService = null;
    private WebService webService = null;
    private WebSocketService webSocketService = null;
    private TopicPoliciesService topicPoliciesService = TopicPoliciesService.DISABLED;
    private BookKeeperClientFactory bkClientFactory;
    private Compactor compactor;
    private ResourceUsageTransportManager resourceUsageTransportManager;
    private ResourceGroupService resourceGroupServiceManager;

    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService cacheExecutor;

    private OrderedExecutor orderedExecutor;
    private final ScheduledExecutorService loadManagerExecutor;
    private ScheduledExecutorService compactorExecutor;
    private OrderedScheduler offloaderScheduler;
    private OffloadersCache offloadersCache = new OffloadersCache();
    private LedgerOffloader defaultOffloader;
    private Map<NamespaceName, LedgerOffloader> ledgerOffloaderMap = new ConcurrentHashMap<>();
    private ScheduledFuture<?> loadReportTask = null;
    private ScheduledFuture<?> loadSheddingTask = null;
    private ScheduledFuture<?> loadResourceQuotaTask = null;
    private final AtomicReference<LoadManager> loadManager = new AtomicReference<>();
    private PulsarAdmin adminClient = null;
    private PulsarClient client = null;
    private ZooKeeperClientFactory zkClientFactory = null;
    private final String bindAddress;
    /**
     * The host component of the broker's canonical name.
     */
    private final String advertisedAddress;
    private String webServiceAddress;
    private String webServiceAddressTls;
    private String brokerServiceUrl;
    private String brokerServiceUrlTls;
    private final String brokerVersion;
    private SchemaStorage schemaStorage = null;
    private SchemaRegistryService schemaRegistryService = null;
    private final WorkerConfig workerConfig;
    private final Optional<WorkerService> functionWorkerService;
    private ProtocolHandlers protocolHandlers = null;

    private final Consumer<Integer> processTerminator;
    protected final EventLoopGroup ioEventLoopGroup;

    private MetricsGenerator metricsGenerator;

    private TransactionMetadataStoreService transactionMetadataStoreService;
    private TransactionBufferProvider transactionBufferProvider;
    private TransactionBufferClient transactionBufferClient;
    private HashedWheelTimer transactionTimer;

    private BrokerInterceptor brokerInterceptor;
    private AdditionalServlets brokerAdditionalServlets;

    // packages management service
    private PackagesManagement packagesManagement;
    private PrometheusMetricsServlet metricsServlet;
    private List<PrometheusRawMetricsProvider> pendingMetricsProviders;

    private MetadataStoreExtended localMetadataStore;
    private CoordinationService coordinationService;
    private TransactionBufferSnapshotService transactionBufferSnapshotService;

    private MetadataStore configurationMetadataStore;
    private boolean shouldShutdownConfigurationMetadataStore;

    private PulsarResources pulsarResources;

    private TransactionPendingAckStoreProvider transactionPendingAckStoreProvider;
    private final ScheduledExecutorService transactionReplayExecutor;

    public enum State {
        Init, Started, Closing, Closed
    }

    private volatile State state;

    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition isClosedCondition = mutex.newCondition();
    private volatile CompletableFuture<Void> closeFuture;
    // key is listener name , value is pulsar address and pulsar ssl address
    private Map<String, AdvertisedListener> advertisedListeners;

    public PulsarService(ServiceConfiguration config) {
        this(config, Optional.empty(), (exitCode) -> {
                LOG.info("Process termination requested with code {}. "
                         + "Ignoring, as this constructor is intended for tests. ", exitCode);
            });
    }

    public PulsarService(ServiceConfiguration config, Optional<WorkerService> functionWorkerService,
                         Consumer<Integer> processTerminator) {
        this(config, new WorkerConfig(), functionWorkerService, processTerminator);
    }

    public PulsarService(ServiceConfiguration config,
                         WorkerConfig workerConfig,
                         Optional<WorkerService> functionWorkerService,
                         Consumer<Integer> processTerminator) {
        state = State.Init;

        // Validate correctness of configuration
        PulsarConfigurationLoader.isComplete(config);

        // validate `advertisedAddress`, `advertisedListeners`, `internalListenerName`
        this.advertisedListeners = MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);

        // the advertised address is defined as the host component of the broker's canonical name.
        this.advertisedAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());

        // use `internalListenerName` listener as `advertisedAddress`
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getBindAddress());
        this.brokerVersion = PulsarVersion.getVersion();
        this.config = config;
        this.processTerminator = processTerminator;
        this.loadManagerExecutor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-load-manager"));
        this.workerConfig = workerConfig;
        this.functionWorkerService = functionWorkerService;
        this.executor = Executors.newScheduledThreadPool(config.getNumExecutorThreadPoolSize(),
                new DefaultThreadFactory("pulsar"));
        this.cacheExecutor = Executors.newScheduledThreadPool(config.getNumCacheExecutorThreadPoolSize(),
                new DefaultThreadFactory("zk-cache-callback"));

        if (config.isTransactionCoordinatorEnabled()) {
            this.transactionReplayExecutor = Executors.newScheduledThreadPool(
                    config.getNumTransactionReplayThreadPoolSize(),
                    new DefaultThreadFactory("transaction-replay"));
        } else {
            this.transactionReplayExecutor = null;
        }

        this.ioEventLoopGroup = EventLoopUtil.newEventLoopGroup(config.getNumIOThreads(), config.isEnableBusyWait(),
                new DefaultThreadFactory("pulsar-io"));
    }

    public MetadataStore createConfigurationMetadataStore() throws MetadataStoreException {
        return MetadataStoreFactory.create(config.getConfigurationStoreServers(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis((int) config.getZooKeeperSessionTimeoutMillis())
                        .allowReadOnlyOperations(false)
                        .build());
    }

    /**
     * Close the session to the metadata service.
     *
     * This will immediately release all the resource locks held by this broker on the coordination service.
     *
     * @throws Exception if the close operation fails
     */
    public void closeMetadataServiceSession() throws Exception {
        localMetadataStore.close();
    }

    @Override
    public void close() throws PulsarServerException {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PulsarServerException) {
                throw (PulsarServerException) cause;
            } else if (getConfiguration().getBrokerShutdownTimeoutMs() == 0
                    && (cause instanceof TimeoutException || cause instanceof CancellationException)) {
                // ignore shutdown timeout when timeout is 0, which is primarily used in tests
                // to forcefully shutdown the broker
            } else {
                throw new PulsarServerException(cause);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Close the current pulsar service. All resources are released.
     */
    public CompletableFuture<Void> closeAsync() {
        mutex.lock();
        try {
            if (closeFuture != null) {
                return closeFuture;
            }
            LOG.info("Closing PulsarService");
            state = State.Closing;

            // close the service in reverse order v.s. in which they are started
            if (this.resourceUsageTransportManager != null) {
                this.resourceUsageTransportManager.close();
                this.resourceUsageTransportManager = null;
            }

            if (this.webService != null) {
                try {
                    this.webService.close();
                    this.webService = null;
                } catch (Exception e) {
                    LOG.error("Web service closing failed", e);
                    // Even if the web service fails to close, the graceful shutdown process continues
                }
            }

            metricsServlet = null;

            if (this.webSocketService != null) {
                this.webSocketService.close();
            }

            if (brokerAdditionalServlets != null) {
                brokerAdditionalServlets.close();
                brokerAdditionalServlets = null;
            }

            GracefulExecutorServicesShutdown executorServicesShutdown =
                    GracefulExecutorServicesShutdown
                            .initiate()
                            .timeout(
                                    Duration.ofMillis(
                                            (long) (GRACEFUL_SHUTDOWN_TIMEOUT_RATIO_OF_TOTAL_TIMEOUT
                                                    * getConfiguration()
                                                    .getBrokerShutdownTimeoutMs())));

            // shutdown loadmanager before shutting down the broker
            executorServicesShutdown.shutdown(loadManagerExecutor);
            LoadManager loadManager = this.loadManager.get();
            if (loadManager != null) {
                loadManager.stop();
            }

            List<CompletableFuture<Void>> asyncCloseFutures = new ArrayList<>();
            if (this.brokerService != null) {
                asyncCloseFutures.add(this.brokerService.closeAsync());
                this.brokerService = null;
            }

            if (this.managedLedgerClientFactory != null) {
                this.managedLedgerClientFactory.close();
                this.managedLedgerClientFactory = null;
            }

            if (bkClientFactory != null) {
                this.bkClientFactory.close();
                this.bkClientFactory = null;
            }

            if (this.leaderElectionService != null) {
                this.leaderElectionService.close();
                this.leaderElectionService = null;
            }

            if (adminClient != null) {
                adminClient.close();
                adminClient = null;
            }

            if (transactionBufferSnapshotService != null) {
                transactionBufferSnapshotService.close();
                transactionBufferSnapshotService = null;
            }

            if (client != null) {
                client.close();
                client = null;
            }

            if (nsService != null) {
                nsService.close();
                nsService = null;
            }

            executorServicesShutdown.shutdown(compactorExecutor);
            executorServicesShutdown.shutdown(offloaderScheduler);
            executorServicesShutdown.shutdown(executor);
            executorServicesShutdown.shutdown(orderedExecutor);
            executorServicesShutdown.shutdown(cacheExecutor);



            if (schemaRegistryService != null) {
                schemaRegistryService.close();
            }

            offloadersCache.close();

            if (protocolHandlers != null) {
                protocolHandlers.close();
                protocolHandlers = null;
            }

            if (transactionBufferClient != null) {
                transactionBufferClient.close();
            }

            if (coordinationService != null) {
                coordinationService.close();
            }

            if (localMetadataStore != null) {
                localMetadataStore.close();
            }
            if (configurationMetadataStore != null && shouldShutdownConfigurationMetadataStore) {
                configurationMetadataStore.close();
            }

            if (transactionReplayExecutor != null) {
                transactionReplayExecutor.shutdown();
            }

            ioEventLoopGroup.shutdownGracefully();

            // add timeout handling for closing executors
            asyncCloseFutures.add(executorServicesShutdown.handle());

            closeFuture = addTimeoutHandling(FutureUtil.waitForAllAndSupportCancel(asyncCloseFutures));
            closeFuture.handle((v, t) -> {
                if (t == null) {
                    LOG.info("Closed");
                } else if (t instanceof CancellationException) {
                    LOG.info("Closed (shutdown cancelled)");
                } else if (t instanceof TimeoutException) {
                    LOG.info("Closed (shutdown timeout)");
                } else {
                    LOG.warn("Closed with errors", t);
                }
                state = State.Closed;
                isClosedCondition.signalAll();
                return null;
            });
            return closeFuture;
        } catch (Exception e) {
            PulsarServerException pse;
            if (e instanceof CompletionException && e.getCause() instanceof MetadataStoreException) {
                pse = new PulsarServerException(MetadataStoreException.unwrap(e));
            } else if (e.getCause() instanceof CompletionException
                    && e.getCause().getCause() instanceof MetadataStoreException) {
                pse = new PulsarServerException(MetadataStoreException.unwrap(e.getCause()));
            } else {
                pse = new PulsarServerException(e);
            }
            return FutureUtil.failedFuture(pse);
        } finally {
            mutex.unlock();
        }
    }

    private CompletableFuture<Void> addTimeoutHandling(CompletableFuture<Void> future) {
        ScheduledExecutorService shutdownExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory(getClass().getSimpleName() + "-shutdown"));
        FutureUtil.addTimeoutHandling(future,
                Duration.ofMillis(Math.max(1L, getConfiguration().getBrokerShutdownTimeoutMs())),
                shutdownExecutor, () -> FutureUtil.createTimeoutException("Timeout in close", getClass(), "close"));
        future.handle((v, t) -> {
            // shutdown the shutdown executor
            shutdownExecutor.shutdownNow();
            return null;
        });
        return future;
    }

    /**
     * Get the current service configuration.
     *
     * @return the current service configuration
     */
    public ServiceConfiguration getConfiguration() {
        return this.config;
    }

    /**
     * Get the current function worker service configuration.
     *
     * @return the current function worker service configuration.
     */
    public Optional<WorkerConfig> getWorkerConfig() {
        return functionWorkerService.map(service -> workerConfig);
    }

    public Map<String, String> getProtocolDataToAdvertise() {
        if (null == protocolHandlers) {
            return Collections.emptyMap();
        } else {
            return protocolHandlers.getProtocolDataToAdvertise();
        }
    }

    /**
     * Start the pulsar service instance.
     */
    public void start() throws PulsarServerException {
        LOG.info("Starting Pulsar Broker service; version: '{}'",
                (brokerVersion != null ? brokerVersion : "unknown"));
        LOG.info("Git Revision {}", PulsarVersion.getGitSha());
        LOG.info("Built by {} on {} at {}",
                PulsarVersion.getBuildUser(),
                PulsarVersion.getBuildHost(),
                PulsarVersion.getBuildTime());

        long startTimestamp = System.currentTimeMillis();  // start time mills

        mutex.lock();
        try {
            if (state != State.Init) {
                throw new PulsarServerException("Cannot start the service once it was stopped");
            }

            if (!config.getWebServicePort().isPresent() && !config.getWebServicePortTls().isPresent()) {
                throw new IllegalArgumentException("webServicePort/webServicePortTls must be present");
            }

            if (config.isAuthorizationEnabled() && !config.isAuthenticationEnabled()) {
                throw new IllegalStateException("Invalid broker configuration. Authentication must be enabled with "
                        + "authenticationEnabled=true when authorization is enabled with authorizationEnabled=true.");
            }

            localMetadataStore = createLocalMetadataStore();
            localMetadataStore.registerSessionListener(this::handleMetadataSessionEvent);

            coordinationService = new CoordinationServiceImpl(localMetadataStore);

            if (!StringUtils.equals(config.getConfigurationStoreServers(), config.getZookeeperServers())) {
                configurationMetadataStore = createConfigurationMetadataStore();
                shouldShutdownConfigurationMetadataStore = true;
            } else {
                configurationMetadataStore = localMetadataStore;
                shouldShutdownConfigurationMetadataStore = false;
            }
            pulsarResources = new PulsarResources(localMetadataStore, configurationMetadataStore,
                    config.getZooKeeperOperationTimeoutSeconds());

            pulsarResources.getClusterResources().getStore().registerListener(this::handleDeleteCluster);

            orderedExecutor = OrderedExecutor.newBuilder()
                    .numThreads(config.getNumOrderedExecutorThreads())
                    .name("pulsar-ordered")
                    .build();

            // Initialize the message protocol handlers
            protocolHandlers = ProtocolHandlers.load(config);
            protocolHandlers.initialize(config);

            // Now we are ready to start services
            this.bkClientFactory = newBookKeeperClientFactory();

            managedLedgerClientFactory = ManagedLedgerStorage.create(
                config, localMetadataStore, getZkClient(),
                    bkClientFactory, ioEventLoopGroup
            );

            this.brokerService = newBrokerService(this);

            // Start load management service (even if load balancing is disabled)
            this.loadManager.set(LoadManager.create(this));

            // needs load management service and before start broker service,
            this.startNamespaceService();

            schemaStorage = createAndStartSchemaStorage();
            schemaRegistryService = SchemaRegistryService.create(
                    schemaStorage, config.getSchemaRegistryCompatibilityCheckers());

            this.defaultOffloader = createManagedLedgerOffloader(
                    OffloadPoliciesImpl.create(this.getConfiguration().getProperties()));
            this.brokerInterceptor = BrokerInterceptors.load(config);
            brokerService.setInterceptor(getBrokerInterceptor());
            this.brokerInterceptor.initialize(this);
            brokerService.start();

            // Load additional servlets
            this.brokerAdditionalServlets = AdditionalServlets.load(config);

            this.webService = new WebService(this);
            this.metricsServlet = new PrometheusMetricsServlet(
                    this, config.isExposeTopicLevelMetricsInPrometheus(),
                    config.isExposeConsumerLevelMetricsInPrometheus(),
                    config.isExposeProducerLevelMetricsInPrometheus(),
                    config.isSplitTopicAndPartitionLabelInPrometheus());
            if (pendingMetricsProviders != null) {
                pendingMetricsProviders.forEach(provider -> metricsServlet.addRawMetricsProvider(provider));
                this.pendingMetricsProviders = null;
            }

            this.addWebServerHandlers(webService, metricsServlet, this.config);
            this.webService.start();

            // Refresh addresses and update configuration, since the port might have been dynamically assigned
            if (config.getBrokerServicePort().equals(Optional.of(0))) {
                config.setBrokerServicePort(brokerService.getListenPort());
            }
            if (config.getBrokerServicePortTls().equals(Optional.of(0))) {
                config.setBrokerServicePortTls(brokerService.getListenPortTls());
            }
            this.webServiceAddress = webAddress(config);
            this.webServiceAddressTls = webAddressTls(config);
            this.brokerServiceUrl = brokerUrl(config);
            this.brokerServiceUrlTls = brokerUrlTls(config);

            if (null != this.webSocketService) {
                ClusterDataImpl clusterData = ClusterDataImpl.builder()
                        .serviceUrl(webServiceAddress)
                        .serviceUrlTls(webServiceAddressTls)
                        .brokerServiceUrl(brokerServiceUrl)
                        .brokerServiceUrlTls(brokerServiceUrlTls)
                        .build();
                this.webSocketService.setLocalCluster(clusterData);
            }

            // Initialize namespace service, after service url assigned. Should init zk and refresh self owner info.
            this.nsService.initialize();

            // Start topic level policies service
            if (config.isTopicLevelPoliciesEnabled() && config.isSystemTopicEnabled()) {
                this.topicPoliciesService = new SystemTopicBasedTopicPoliciesService(this);
            }

            this.topicPoliciesService.start();

            // Start the leader election service
            startLeaderElectionService();

            // Register heartbeat and bootstrap namespaces.
            this.nsService.registerBootstrapNamespaces();

            // Register pulsar system namespaces and start transaction meta store service
            if (config.isTransactionCoordinatorEnabled()) {
                this.transactionBufferSnapshotService = new SystemTopicBaseTxnBufferSnapshotService(getClient());
                this.transactionTimer =
                        new HashedWheelTimer(new DefaultThreadFactory("pulsar-transaction-timer"));
                transactionBufferClient = TransactionBufferClientImpl.create(getClient(), transactionTimer);

                transactionMetadataStoreService = new TransactionMetadataStoreService(TransactionMetadataStoreProvider
                        .newProvider(config.getTransactionMetadataStoreProviderClassName()), this,
                        transactionBufferClient, transactionTimer);

                transactionBufferProvider = TransactionBufferProvider
                        .newProvider(config.getTransactionBufferProviderClassName());
                transactionPendingAckStoreProvider = TransactionPendingAckStoreProvider
                        .newProvider(config.getTransactionPendingAckStoreProviderClassName());
            }

            this.metricsGenerator = new MetricsGenerator(this);

            // By starting the Load manager service, the broker will also become visible
            // to the rest of the broker by creating the registration z-node. This needs
            // to be done only when the broker is fully operative.
            this.startLoadManagementService();

            // Initialize the message protocol handlers.
            // start the protocol handlers only after the broker is ready,
            // so that the protocol handlers can access broker service properly.
            this.protocolHandlers.start(brokerService);
            Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> protocolHandlerChannelInitializers =
                this.protocolHandlers.newChannelInitializers();
            this.brokerService.startProtocolHandlers(protocolHandlerChannelInitializers);

            acquireSLANamespace();

            // start function worker service if necessary
            this.startWorkerService(brokerService.getAuthenticationService(), brokerService.getAuthorizationService());

            // start packages management service if necessary
            if (config.isEnablePackagesManagement()) {
                this.startPackagesManagementService();
            }

            // Start the task to publish resource usage, if necessary
            this.resourceUsageTransportManager = DISABLE_RESOURCE_USAGE_TRANSPORT_MANAGER;
            if (isNotBlank(config.getResourceUsageTransportClassName())) {
                Class<?> clazz = Class.forName(config.getResourceUsageTransportClassName());
                Constructor<?> ctor = clazz.getConstructor(PulsarService.class);
                Object object = ctor.newInstance(new Object[]{this});
                this.resourceUsageTransportManager = (ResourceUsageTopicTransportManager) object;
            }
            this.resourceGroupServiceManager = new ResourceGroupService(this);

            long currentTimestamp = System.currentTimeMillis();
            final long bootstrapTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(currentTimestamp - startTimestamp);

            final String bootstrapMessage = "bootstrap service "
                    + (config.getWebServicePort().isPresent() ? "port = " + config.getWebServicePort().get() : "")
                    + (config.getWebServicePortTls().isPresent() ? ", tls-port = " + config.getWebServicePortTls() : "")
                    + (StringUtils.isNotEmpty(brokerServiceUrl) ? ", broker url= " + brokerServiceUrl : "")
                    + (StringUtils.isNotEmpty(brokerServiceUrlTls) ? ", broker tls url= " + brokerServiceUrlTls : "");
            LOG.info("messaging service is ready, bootstrap_seconds={}", bootstrapTimeSeconds);
            LOG.info("messaging service is ready, {}, cluster={}, configs={}", bootstrapMessage,
                    config.getClusterName(), ReflectionToStringBuilder.toString(config));

            state = State.Started;
        } catch (Exception e) {
            LOG.error("Failed to start Pulsar service: {}", e.getMessage(), e);
            throw new PulsarServerException(e);
        } finally {
            mutex.unlock();
        }
    }

    private void addWebServerHandlers(WebService webService,
                                      PrometheusMetricsServlet metricsServlet,
                                      ServiceConfiguration config)
            throws PulsarServerException, PulsarClientException, MalformedURLException, ServletException,
            DeploymentException {
        Map<String, Object> attributeMap = Maps.newHashMap();
        attributeMap.put(WebService.ATTRIBUTE_PULSAR_NAME, this);

        Map<String, Object> vipAttributeMap = Maps.newHashMap();
        vipAttributeMap.put(VipStatus.ATTRIBUTE_STATUS_FILE_PATH, config.getStatusFilePath());
        vipAttributeMap.put(VipStatus.ATTRIBUTE_IS_READY_PROBE, (Supplier<Boolean>) () -> {
            // Ensure the VIP status is only visible when the broker is fully initialized
            return state == State.Started;
        });
        // Add admin rest resources
        webService.addRestResources("/",
                VipStatus.class.getPackage().getName(), false, vipAttributeMap);
        webService.addRestResources("/",
                "org.apache.pulsar.broker.web", false, attributeMap);
        webService.addRestResources("/admin",
                "org.apache.pulsar.broker.admin.v1", true, attributeMap);
        webService.addRestResources("/admin/v2",
                "org.apache.pulsar.broker.admin.v2", true, attributeMap);
        webService.addRestResources("/admin/v3",
                "org.apache.pulsar.broker.admin.v3", true, attributeMap);
        webService.addRestResources("/lookup",
                "org.apache.pulsar.broker.lookup", true, attributeMap);
        webService.addRestResources("/topics",
                            "org.apache.pulsar.broker.rest", true, attributeMap);

        // Add metrics servlet
        webService.addServlet("/metrics",
                new ServletHolder(metricsServlet),
                false, attributeMap);

        // Add websocket service
        addWebSocketServiceHandler(webService, attributeMap, config);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Attempting to add static directory");
        }
        // Add static resources
        webService.addStaticResources("/static", "/static");

        // Add broker additional servlets
        addBrokerAdditionalServlets(webService, attributeMap, config);
    }

    private void handleMetadataSessionEvent(SessionEvent e) {
        LOG.info("Received metadata service session event: {}", e);
        if (e == SessionEvent.SessionLost
                && config.getZookeeperSessionExpiredPolicy() == MetadataSessionExpiredPolicy.shutdown) {
            LOG.warn("The session with metadata service was lost. Shutting down.");
            shutdownNow();
        }
    }

    private void addBrokerAdditionalServlets(WebService webService,
                                             Map<String, Object> attributeMap,
                                             ServiceConfiguration config) {
        if (this.getBrokerAdditionalServlets() != null) {
            Collection<AdditionalServletWithClassLoader> additionalServletCollection =
                    this.getBrokerAdditionalServlets().getServlets().values();
            for (AdditionalServletWithClassLoader servletWithClassLoader : additionalServletCollection) {
                servletWithClassLoader.loadConfig(config);
                AdditionalServlet additionalServlet = servletWithClassLoader.getServlet();
                if (additionalServlet instanceof AdditionalServletWithPulsarService) {
                    ((AdditionalServletWithPulsarService) additionalServlet).setPulsarService(this);
                }
                webService.addServlet(servletWithClassLoader.getBasePath(), servletWithClassLoader.getServletHolder(),
                        config.isAuthenticationEnabled(), attributeMap);
                LOG.info("Broker add additional servlet basePath {} ", servletWithClassLoader.getBasePath());
            }
        }
    }

    private void addWebSocketServiceHandler(WebService webService,
                                            Map<String, Object> attributeMap,
                                            ServiceConfiguration config)
            throws PulsarServerException, PulsarClientException, MalformedURLException, ServletException,
            DeploymentException {
        if (config.isWebSocketServiceEnabled()) {
            // Use local broker address to avoid different IP address when using a VIP for service discovery
            this.webSocketService = new WebSocketService(null, config);
            this.webSocketService.start();

            final WebSocketServlet producerWebSocketServlet = new WebSocketProducerServlet(webSocketService);
            webService.addServlet(WebSocketProducerServlet.SERVLET_PATH,
                    new ServletHolder(producerWebSocketServlet), true, attributeMap);
            webService.addServlet(WebSocketProducerServlet.SERVLET_PATH_V2,
                    new ServletHolder(producerWebSocketServlet), true, attributeMap);

            final WebSocketServlet consumerWebSocketServlet = new WebSocketConsumerServlet(webSocketService);
            webService.addServlet(WebSocketConsumerServlet.SERVLET_PATH,
                    new ServletHolder(consumerWebSocketServlet), true, attributeMap);
            webService.addServlet(WebSocketConsumerServlet.SERVLET_PATH_V2,
                    new ServletHolder(consumerWebSocketServlet), true, attributeMap);

            final WebSocketServlet readerWebSocketServlet = new WebSocketReaderServlet(webSocketService);
            webService.addServlet(WebSocketReaderServlet.SERVLET_PATH,
                    new ServletHolder(readerWebSocketServlet), true, attributeMap);
            webService.addServlet(WebSocketReaderServlet.SERVLET_PATH_V2,
                    new ServletHolder(readerWebSocketServlet), true, attributeMap);

            final WebSocketServlet pingPongWebSocketServlet = new WebSocketPingPongServlet(webSocketService);
            webService.addServlet(WebSocketPingPongServlet.SERVLET_PATH,
                    new ServletHolder(pingPongWebSocketServlet), true, attributeMap);
            webService.addServlet(WebSocketPingPongServlet.SERVLET_PATH_V2,
                    new ServletHolder(pingPongWebSocketServlet), true, attributeMap);
        }
    }

    private void handleDeleteCluster(Notification notification) {
        if (ClusterResources.pathRepresentsClusterName(notification.getPath())
                && notification.getType() == NotificationType.Deleted) {
            final String clusterName = ClusterResources.clusterNameFromPath(notification.getPath());
            getBrokerService().closeAndRemoveReplicationClient(clusterName);
        }
    }

    public MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
        return MetadataStoreExtended.create(config.getZookeeperServers(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis((int) config.getZooKeeperSessionTimeoutMillis())
                        .allowReadOnlyOperations(false)
                        .build());
    }

    protected void startLeaderElectionService() {
        this.leaderElectionService = new LeaderElectionService(coordinationService, getSafeWebServiceAddress(),
                state -> {
                    if (state == LeaderElectionState.Leading) {
                        LOG.info("This broker was elected leader");
                        if (getConfiguration().isLoadBalancerEnabled()) {
                            long loadSheddingInterval = TimeUnit.MINUTES
                                    .toMillis(getConfiguration().getLoadBalancerSheddingIntervalMinutes());
                            long resourceQuotaUpdateInterval = TimeUnit.MINUTES
                                    .toMillis(getConfiguration().getLoadBalancerResourceQuotaUpdateIntervalMinutes());

                            if (loadSheddingTask != null) {
                                loadSheddingTask.cancel(false);
                            }
                            if (loadResourceQuotaTask != null) {
                                loadResourceQuotaTask.cancel(false);
                            }
                            loadSheddingTask = loadManagerExecutor.scheduleAtFixedRate(
                                    new LoadSheddingTask(loadManager),
                                    loadSheddingInterval, loadSheddingInterval, TimeUnit.MILLISECONDS);
                            loadResourceQuotaTask = loadManagerExecutor.scheduleAtFixedRate(
                                    new LoadResourceQuotaUpdaterTask(loadManager), resourceQuotaUpdateInterval,
                                    resourceQuotaUpdateInterval, TimeUnit.MILLISECONDS);
                        }
                    } else {
                        if (leaderElectionService != null) {
                            LOG.info("This broker is a follower. Current leader is {}",
                                    leaderElectionService.getCurrentLeader());
                        }
                        if (loadSheddingTask != null) {
                            loadSheddingTask.cancel(false);
                            loadSheddingTask = null;
                        }
                        if (loadResourceQuotaTask != null) {
                            loadResourceQuotaTask.cancel(false);
                            loadResourceQuotaTask = null;
                        }
                    }
                });

        leaderElectionService.start();
    }

    protected void acquireSLANamespace() {
        try {
            // Namespace not created hence no need to unload it
            NamespaceName nsName = NamespaceService.getSLAMonitorNamespace(getAdvertisedAddress(), config);
            if (!this.pulsarResources.getNamespaceResources().namespaceExists(nsName)) {
                LOG.info("SLA Namespace = {} doesn't exist.", nsName);
                return;
            }

            boolean acquiredSLANamespace;
            try {
                acquiredSLANamespace = nsService.registerSLANamespace();
                LOG.info("Register SLA Namespace = {}, returned - {}.", nsName, acquiredSLANamespace);
            } catch (PulsarServerException e) {
                acquiredSLANamespace = false;
            }

            if (!acquiredSLANamespace) {
                this.nsService.unloadSLANamespace();
            }
        } catch (Exception ex) {
            LOG.warn(
                    "Exception while trying to unload the SLA namespace,"
                            + " will try to unload the namespace again after 1 minute. Exception:",
                    ex);
            executor.schedule(this::acquireSLANamespace, 1, TimeUnit.MINUTES);
        } catch (Throwable ex) {
            // To make sure SLA monitor doesn't interfere with the normal broker flow
            LOG.warn(
                    "Exception while trying to unload the SLA namespace,"
                            + " will not try to unload the namespace again. Exception:",
                    ex);
        }
    }

    /**
     * Block until the service is finally closed.
     */
    public void waitUntilClosed() throws InterruptedException {
        mutex.lock();

        try {
            while (state != State.Closed) {
                isClosedCondition.await();
            }
        } finally {
            mutex.unlock();
        }
    }

    protected void startNamespaceService() throws PulsarServerException {

        LOG.info("Starting name space service, bootstrap namespaces=" + config.getBootstrapNamespaces());

        this.nsService = getNamespaceServiceProvider().get();
    }

    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> new NamespaceService(PulsarService.this);
    }

    protected void startLoadManagementService() throws PulsarServerException {
        LOG.info("Starting load management service ...");
        this.loadManager.get().start();

        if (config.isLoadBalancerEnabled()) {
            LOG.info("Starting load balancer");
            if (this.loadReportTask == null) {
                long loadReportMinInterval = LoadManagerShared.LOAD_REPORT_UPDATE_MINIMUM_INTERVAL;
                this.loadReportTask = this.loadManagerExecutor.scheduleAtFixedRate(
                        new LoadReportUpdaterTask(loadManager), loadReportMinInterval, loadReportMinInterval,
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Load all the topics contained in a namespace.
     *
     * @param bundle <code>NamespaceBundle</code> to identify the service unit
     * @throws Exception
     */
    public void loadNamespaceTopics(NamespaceBundle bundle) {
        executor.submit(() -> {
            LOG.info("Loading all topics on bundle: {}", bundle);

            NamespaceName nsName = bundle.getNamespaceObject();
            List<CompletableFuture<Topic>> persistentTopics = Lists.newArrayList();
            long topicLoadStart = System.nanoTime();

            for (String topic : getNamespaceService().getListOfPersistentTopics(nsName)
                    .get(config.getZooKeeperOperationTimeoutSeconds(), TimeUnit.SECONDS)) {
                try {
                    TopicName topicName = TopicName.get(topic);
                    if (bundle.includes(topicName) && !isTransactionSystemTopic(topicName)) {
                        CompletableFuture<Topic> future = brokerService.getOrCreateTopic(topic);
                        if (future != null) {
                            persistentTopics.add(future);
                        }
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to preload topic {}", topic, t);
                }
            }

            if (!persistentTopics.isEmpty()) {
                FutureUtil.waitForAll(persistentTopics).thenRun(() -> {
                    double topicLoadTimeSeconds = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - topicLoadStart)
                            / 1000.0;
                    LOG.info("Loaded {} topics on {} -- time taken: {} seconds", persistentTopics.size(), bundle,
                            topicLoadTimeSeconds);
                });
            }
            return null;
        });
    }

    // No need to synchronize since config is only init once
    // We only read this from memory later
    public String getStatusFilePath() {
        if (config == null) {
            return null;
        }
        return config.getStatusFilePath();
    }

    /**
     * Get default bookkeeper metadata service uri.
     */
    public String getMetadataServiceUri() {
        return bookieMetadataServiceUri(this.getConfiguration());
    }

    public InternalConfigurationData getInternalConfigurationData() {

        String metadataServiceUri = getMetadataServiceUri();

        if (StringUtils.isNotBlank(config.getBookkeeperMetadataServiceUri())) {
            metadataServiceUri = this.getConfiguration().getBookkeeperMetadataServiceUri();
        }

        return new InternalConfigurationData(
            this.getConfiguration().getZookeeperServers(),
            this.getConfiguration().getConfigurationStoreServers(),
            new ClientConfiguration().getZkLedgersRootPath(),
            metadataServiceUri,
            this.getWorkerConfig().map(wc -> wc.getStateStorageServiceUrl()).orElse(null));
    }

    /**
     * Get the current pulsar state.
     */
    public State getState() {
        return this.state;
    }

    /**
     * Get a reference of the current <code>LeaderElectionService</code> instance associated with the current
     * <code>PulsarService</code> instance.
     *
     * @return a reference of the current <code>LeaderElectionService</code> instance.
     */
    public LeaderElectionService getLeaderElectionService() {
        return this.leaderElectionService;
    }

    /**
     * Get a reference of the current namespace service instance.
     *
     * @return a reference of the current namespace service instance.
     */
    public NamespaceService getNamespaceService() {
        return this.nsService;
    }


    public Optional<WorkerService> getWorkerServiceOpt() {
        return functionWorkerService;
    }

    public WorkerService getWorkerService() throws UnsupportedOperationException {
        return functionWorkerService.orElseThrow(() -> new UnsupportedOperationException("Pulsar Function Worker "
                + "is not enabled, probably functionsWorkerEnabled is set to false"));
    }

    /**
     * Get a reference of the current <code>BrokerService</code> instance associated with the current
     * <code>PulsarService</code> instance.
     *
     * @return a reference of the current <code>BrokerService</code> instance.
     */
    public BrokerService getBrokerService() {
        return this.brokerService;
    }

    public BookKeeper getBookKeeperClient() {
        return managedLedgerClientFactory.getBookKeeperClient();
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerClientFactory.getManagedLedgerFactory();
    }

    public ManagedLedgerStorage getManagedLedgerClientFactory() {
        return managedLedgerClientFactory;
    }

    /**
     * First, get <code>LedgerOffloader</code> from local map cache,
     * create new <code>LedgerOffloader</code> if not in cache or
     * the <code>OffloadPolicies</code> changed, return the <code>LedgerOffloader</code> directly if exist in cache
     * and the <code>OffloadPolicies</code> not changed.
     *
     * @param namespaceName NamespaceName
     * @param offloadPolicies the OffloadPolicies
     * @return LedgerOffloader
     */
    public LedgerOffloader getManagedLedgerOffloader(NamespaceName namespaceName, OffloadPoliciesImpl offloadPolicies) {
        if (offloadPolicies == null) {
            return getDefaultOffloader();
        }
        return ledgerOffloaderMap.compute(namespaceName, (ns, offloader) -> {
            try {
                if (offloader != null && Objects.equals(offloader.getOffloadPolicies(), offloadPolicies)) {
                    return offloader;
                } else {
                    if (offloader != null) {
                        offloader.close();
                    }
                    return createManagedLedgerOffloader(offloadPolicies);
                }
            } catch (PulsarServerException e) {
                LOG.error("create ledgerOffloader failed for namespace {}", namespaceName.toString(), e);
                return new NullLedgerOffloader();
            }
        });
    }

    public synchronized LedgerOffloader createManagedLedgerOffloader(OffloadPoliciesImpl offloadPolicies)
            throws PulsarServerException {
        try {
            if (StringUtils.isNotBlank(offloadPolicies.getManagedLedgerOffloadDriver())) {
                checkNotNull(offloadPolicies.getOffloadersDirectory(),
                    "Offloader driver is configured to be '%s' but no offloaders directory is configured.",
                        offloadPolicies.getManagedLedgerOffloadDriver());
                Offloaders offloaders = offloadersCache.getOrLoadOffloaders(
                        offloadPolicies.getOffloadersDirectory(), config.getNarExtractionDirectory());

                LedgerOffloaderFactory offloaderFactory = offloaders.getOffloaderFactory(
                        offloadPolicies.getManagedLedgerOffloadDriver());
                try {
                    return offloaderFactory.create(
                        offloadPolicies,
                        ImmutableMap.of(
                            LedgerOffloader.METADATA_SOFTWARE_VERSION_KEY.toLowerCase(), PulsarVersion.getVersion(),
                            LedgerOffloader.METADATA_SOFTWARE_GITSHA_KEY.toLowerCase(), PulsarVersion.getGitSha()
                        ),
                        schemaStorage,
                        getOffloaderScheduler(offloadPolicies));
                } catch (IOException ioe) {
                    throw new PulsarServerException(ioe.getMessage(), ioe.getCause());
                }
            } else {
                LOG.info("No ledger offloader configured, using NULL instance");
                return NullLedgerOffloader.INSTANCE;
            }
        } catch (Throwable t) {
            throw new PulsarServerException(t);
        }
    }

    private SchemaStorage createAndStartSchemaStorage() throws Exception {
        final Class<?> storageClass = Class.forName(config.getSchemaRegistryStorageClassName());
        Object factoryInstance = storageClass.getDeclaredConstructor().newInstance();
        Method createMethod = storageClass.getMethod("create", PulsarService.class, ZooKeeper.class);
        SchemaStorage schemaStorage = (SchemaStorage) createMethod.invoke(factoryInstance, this,
                getZkClient());
        schemaStorage.start();
        return schemaStorage;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public ScheduledExecutorService getCacheExecutor() {
        return cacheExecutor;
    }

    public ScheduledExecutorService getTransactionReplayExecutor() {
        return transactionReplayExecutor;
    }

    public ScheduledExecutorService getLoadManagerExecutor() {
        return loadManagerExecutor;
    }

    public OrderedExecutor getOrderedExecutor() {
        return orderedExecutor;
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperBkClientFactoryImpl(orderedExecutor);
        }
        // Return default factory
        return zkClientFactory;
    }

    public BookKeeperClientFactory newBookKeeperClientFactory() {
        return new BookKeeperClientFactoryImpl();
    }

    public BookKeeperClientFactory getBookKeeperClientFactory() {
        return bkClientFactory;
    }

    protected synchronized ScheduledExecutorService getCompactorExecutor() {
        if (this.compactorExecutor == null) {
            compactorExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("compaction"));
        }
        return this.compactorExecutor;
    }

    // only public so mockito can mock it
    public Compactor newCompactor() throws PulsarServerException {
        return new TwoPhaseCompactor(this.getConfiguration(),
                getClient(), getBookKeeperClient(),
                getCompactorExecutor());
    }

    public synchronized Compactor getCompactor() throws PulsarServerException {
        return getCompactor(true);
    }

    public synchronized Compactor getCompactor(boolean shouldInitialize) throws PulsarServerException {
        if (this.compactor == null && shouldInitialize) {
            this.compactor = newCompactor();
        }
        return this.compactor;
    }

    protected synchronized OrderedScheduler getOffloaderScheduler(OffloadPoliciesImpl offloadPolicies) {
        if (this.offloaderScheduler == null) {
            this.offloaderScheduler = OrderedScheduler.newSchedulerBuilder()
                    .numThreads(offloadPolicies.getManagedLedgerOffloadMaxThreads())
                    .name("offloader").build();
        }
        return this.offloaderScheduler;
    }

    public synchronized PulsarClient getClient() throws PulsarServerException {
        if (this.client == null) {
            try {
                ClientConfigurationData conf = new ClientConfigurationData();
                conf.setServiceUrl(this.getConfiguration().isTlsEnabled()
                                ? this.brokerServiceUrlTls : this.brokerServiceUrl);
                conf.setTlsAllowInsecureConnection(this.getConfiguration().isTlsAllowInsecureConnection());
                conf.setTlsTrustCertsFilePath(this.getConfiguration().getTlsCertificateFilePath());

                if (this.getConfiguration().isBrokerClientTlsEnabled()) {
                    if (this.getConfiguration().isBrokerClientTlsEnabledWithKeyStore()) {
                        conf.setUseKeyStoreTls(true);
                        conf.setTlsTrustStoreType(this.getConfiguration().getBrokerClientTlsTrustStoreType());
                        conf.setTlsTrustStorePath(this.getConfiguration().getBrokerClientTlsTrustStore());
                        conf.setTlsTrustStorePassword(this.getConfiguration().getBrokerClientTlsTrustStorePassword());
                    } else {
                        conf.setTlsTrustCertsFilePath(
                                isNotBlank(this.getConfiguration().getBrokerClientTrustCertsFilePath())
                                        ? this.getConfiguration().getBrokerClientTrustCertsFilePath()
                                        : this.getConfiguration().getTlsCertificateFilePath());
                    }
                }

                if (isNotBlank(this.getConfiguration().getBrokerClientAuthenticationPlugin())) {
                    conf.setAuthPluginClassName(this.getConfiguration().getBrokerClientAuthenticationPlugin());
                    conf.setAuthParams(this.getConfiguration().getBrokerClientAuthenticationParameters());
                    conf.setAuthParamMap(null);
                    conf.setAuthentication(AuthenticationFactory.create(
                            this.getConfiguration().getBrokerClientAuthenticationPlugin(),
                            this.getConfiguration().getBrokerClientAuthenticationParameters()));
                }

                conf.setStatsIntervalSeconds(0);
                this.client = new PulsarClientImpl(conf, ioEventLoopGroup);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }
        return this.client;
    }

    public synchronized PulsarAdmin getAdminClient() throws PulsarServerException {
        if (this.adminClient == null) {
            try {
                ServiceConfiguration conf = this.getConfiguration();
                final String adminApiUrl = conf.isBrokerClientTlsEnabled() ? webServiceAddressTls : webServiceAddress;
                if (adminApiUrl == null) {
                    throw new IllegalArgumentException("Web service address was not set properly "
                            + ", isBrokerClientTlsEnabled: " + conf.isBrokerClientTlsEnabled()
                            + ", webServiceAddressTls: " + webServiceAddressTls
                            + ", webServiceAddress: " + webServiceAddress);
                }
                PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl(adminApiUrl) //
                        .authentication(//
                                conf.getBrokerClientAuthenticationPlugin(), //
                                conf.getBrokerClientAuthenticationParameters());

                if (conf.isBrokerClientTlsEnabled()) {
                    if (conf.isBrokerClientTlsEnabledWithKeyStore()) {
                        builder.useKeyStoreTls(true)
                                .tlsTrustStoreType(conf.getBrokerClientTlsTrustStoreType())
                                .tlsTrustStorePath(conf.getBrokerClientTlsTrustStore())
                                .tlsTrustStorePassword(conf.getBrokerClientTlsTrustStorePassword());
                    } else {
                        builder.tlsTrustCertsFilePath(conf.getBrokerClientTrustCertsFilePath());
                    }
                    builder.allowTlsInsecureConnection(conf.isTlsAllowInsecureConnection());
                }

                // most of the admin request requires to make zk-call so, keep the max read-timeout based on
                // zk-operation timeout
                builder.readTimeout(conf.getZooKeeperOperationTimeoutSeconds(), TimeUnit.SECONDS);

                this.adminClient = builder.build();
                LOG.info("created admin with url {} ", adminApiUrl);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }

        return this.adminClient;
    }

    public MetricsGenerator getMetricsGenerator() {
        return metricsGenerator;
    }

    public TransactionMetadataStoreService getTransactionMetadataStoreService() {
        return transactionMetadataStoreService;
    }

    public TransactionBufferProvider getTransactionBufferProvider() {
        return transactionBufferProvider;
    }

    public TransactionBufferClient getTransactionBufferClient() {
        return transactionBufferClient;
    }

    /**
     * Gets the broker service URL (non-TLS) associated with the internal listener.
     */
    protected String brokerUrl(ServiceConfiguration config) {
        AdvertisedListener internalListener = ServiceConfigurationUtils.getInternalListener(config);
        return internalListener != null && internalListener.getBrokerServiceUrl() != null
                ? internalListener.getBrokerServiceUrl().toString() : null;
    }

    public static String brokerUrl(String host, int port) {
        return ServiceConfigurationUtils.brokerUrl(host, port);
    }

    /**
     * Gets the broker service URL (TLS) associated with the internal listener.
     */
    public String brokerUrlTls(ServiceConfiguration config) {
        AdvertisedListener internalListener = ServiceConfigurationUtils.getInternalListener(config);
        return internalListener != null && internalListener.getBrokerServiceUrlTls() != null
                ? internalListener.getBrokerServiceUrlTls().toString() : null;
    }

    public static String brokerUrlTls(String host, int port) {
        return ServiceConfigurationUtils.brokerUrlTls(host, port);
    }

    public String webAddress(ServiceConfiguration config) {
        if (config.getWebServicePort().isPresent()) {
            return webAddress(ServiceConfigurationUtils.getWebServiceAddress(config), getListenPortHTTP().get());
        } else {
            return null;
        }
    }

    public static String webAddress(String host, int port) {
        return String.format("http://%s:%d", host, port);
    }

    public String webAddressTls(ServiceConfiguration config) {
        if (config.getWebServicePortTls().isPresent()) {
            return webAddressTls(ServiceConfigurationUtils.getWebServiceAddress(config), getListenPortHTTPS().get());
        } else {
            return null;
        }
    }

    public static String webAddressTls(String host, int port) {
        return String.format("https://%s:%d", host, port);
    }

    public String getSafeWebServiceAddress() {
        return webServiceAddress != null ? webServiceAddress : webServiceAddressTls;
    }

    @Deprecated
    public String getSafeBrokerServiceUrl() {
        return brokerServiceUrl != null ? brokerServiceUrl : brokerServiceUrlTls;
    }

    /**
     * Get bookkeeper metadata service uri.
     *
     * @param config broker configuration
     * @return the metadata service uri that bookkeeper is used
     */
    public static String bookieMetadataServiceUri(ServiceConfiguration config) {
        ClientConfiguration bkConf = new ClientConfiguration();
        // init bookkeeper metadata service uri
        String metadataServiceUri = null;
        try {
            String zkServers = config.getZookeeperServers();
            bkConf.setZkServers(zkServers);
            metadataServiceUri = bkConf.getMetadataServiceUri();
        } catch (ConfigurationException e) {
            LOG.error("Failed to get bookkeeper metadata service uri", e);
        }
        return metadataServiceUri;
    }

    public TopicPoliciesService getTopicPoliciesService() {
        return topicPoliciesService;
    }

    public ResourceUsageTransportManager getResourceUsageTransportManager() {
        return resourceUsageTransportManager;
    }

    public void addPrometheusRawMetricsProvider(PrometheusRawMetricsProvider metricsProvider) {
        if (metricsServlet == null) {
            if (pendingMetricsProviders == null) {
                pendingMetricsProviders = new LinkedList<>();
            }
            pendingMetricsProviders.add(metricsProvider);
        } else {
            this.metricsServlet.addRawMetricsProvider(metricsProvider);
        }
    }

    private void startWorkerService(AuthenticationService authenticationService,
                                    AuthorizationService authorizationService)
            throws Exception {
        if (functionWorkerService.isPresent()) {
            if (workerConfig.isUseTls() || brokerServiceUrl == null) {
                workerConfig.setPulsarServiceUrl(brokerServiceUrlTls);
            } else {
                workerConfig.setPulsarServiceUrl(brokerServiceUrl);
            }
            if (workerConfig.isUseTls() || webServiceAddress == null) {
                workerConfig.setPulsarWebServiceUrl(webServiceAddressTls);
                workerConfig.setFunctionWebServiceUrl(webServiceAddressTls);
            } else {
                workerConfig.setPulsarWebServiceUrl(webServiceAddress);
                workerConfig.setFunctionWebServiceUrl(webServiceAddress);
            }

            LOG.info("Starting function worker service: serviceUrl = {},"
                + " webServiceUrl = {}, functionWebServiceUrl = {}",
                workerConfig.getPulsarServiceUrl(),
                workerConfig.getPulsarWebServiceUrl(),
                workerConfig.getFunctionWebServiceUrl());

            functionWorkerService.get().initInBroker(
                config,
                workerConfig,
                pulsarResources,
                getInternalConfigurationData()
            );

            // TODO figure out how to handle errors from function worker service
            functionWorkerService.get().start(
                authenticationService,
                authorizationService,
                ErrorNotifier.getShutdownServiceImpl(this));
            LOG.info("Function worker service started");
        }
    }

    private void startPackagesManagementService() throws IOException {
        // TODO: using provider to initialize the packages management service.
        this.packagesManagement = new PackagesManagementImpl();
        PackagesStorageProvider storageProvider = PackagesStorageProvider
            .newProvider(config.getPackagesManagementStorageProvider());
        DefaultPackagesStorageConfiguration storageConfiguration = new DefaultPackagesStorageConfiguration();
        storageConfiguration.setProperty(config.getProperties());
        PackagesStorage storage = storageProvider.getStorage(storageConfiguration);
        storage.initialize();
        packagesManagement.initialize(storage);
    }

    public Optional<Integer> getListenPortHTTP() {
        return webService.getListenPortHTTP();
    }

    public Optional<Integer> getListenPortHTTPS() {
        return webService.getListenPortHTTPS();
    }

    public Optional<Integer> getBrokerListenPort() {
        return brokerService.getListenPort();
    }

    public Optional<Integer> getBrokerListenPortTls() {
        return brokerService.getListenPortTls();
    }

    public MetadataStoreExtended getLocalMetadataStore() {
        return localMetadataStore;
    }

    public CoordinationService getCoordinationService() {
        return coordinationService;
    }

    public static WorkerConfig initializeWorkerConfigFromBrokerConfig(ServiceConfiguration brokerConfig,
                                                                      String workerConfigFile) throws IOException {
        WorkerConfig workerConfig = WorkerConfig.load(workerConfigFile);

        brokerConfig.getWebServicePort()
            .map(port -> workerConfig.setWorkerPort(port));
        brokerConfig.getWebServicePortTls()
            .map(port -> workerConfig.setWorkerPortTls(port));

        // worker talks to local broker
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(
                brokerConfig.getAdvertisedAddress());
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setPulsarFunctionsCluster(brokerConfig.getClusterName());
        // inherit broker authorization setting
        workerConfig.setAuthenticationEnabled(brokerConfig.isAuthenticationEnabled());
        workerConfig.setAuthenticationProviders(brokerConfig.getAuthenticationProviders());
        workerConfig.setAuthorizationEnabled(brokerConfig.isAuthorizationEnabled());
        workerConfig.setAuthorizationProvider(brokerConfig.getAuthorizationProvider());
        workerConfig.setConfigurationStoreServers(brokerConfig.getConfigurationStoreServers());
        workerConfig.setZooKeeperSessionTimeoutMillis(brokerConfig.getZooKeeperSessionTimeoutMillis());
        workerConfig.setZooKeeperOperationTimeoutSeconds(brokerConfig.getZooKeeperOperationTimeoutSeconds());

        workerConfig.setTlsAllowInsecureConnection(brokerConfig.isTlsAllowInsecureConnection());
        workerConfig.setTlsEnabled(brokerConfig.isTlsEnabled());
        workerConfig.setTlsEnableHostnameVerification(false);
        workerConfig.setBrokerClientTrustCertsFilePath(brokerConfig.getTlsTrustCertsFilePath());

        // client in worker will use this config to authenticate with broker
        workerConfig.setBrokerClientAuthenticationPlugin(brokerConfig.getBrokerClientAuthenticationPlugin());
        workerConfig.setBrokerClientAuthenticationParameters(brokerConfig.getBrokerClientAuthenticationParameters());

        // inherit super users
        workerConfig.setSuperUserRoles(brokerConfig.getSuperUserRoles());

        // inherit the nar package locations
        if (isBlank(workerConfig.getFunctionsWorkerServiceNarPackage())) {
            workerConfig.setFunctionsWorkerServiceNarPackage(
                brokerConfig.getFunctionsWorkerServiceNarPackage());
        }

        workerConfig.setWorkerId(
                "c-" + brokerConfig.getClusterName()
                        + "-fw-" + hostname
                        + "-" + (workerConfig.getTlsEnabled()
                        ? workerConfig.getWorkerPortTls() : workerConfig.getWorkerPort()));
        return workerConfig;
    }

    /**
     * Shutdown the broker immediately, without waiting for all resources to be released.
     * This possibly is causing the JVM process to exit, depending on how th PulsarService
     * was constructed.
     */
    @Override
    public void shutdownNow() {
        LOG.info("Invoking Pulsar service immediate shutdown");
        try {
            // Try to close metadata service session to ensure all ephemeral locks get released immediately
            closeMetadataServiceSession();
        } catch (Exception e) {
            LOG.warn("Failed to close metadata service session: {}", e.getMessage());
        }

        processTerminator.accept(-1);
    }


    public static boolean isTransactionSystemTopic(TopicName topicName) {
        String topic = topicName.toString();
        return topic.startsWith(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString())
                || topic.startsWith(TRANSACTION_COORDINATOR_LOG.toString())
                || topic.endsWith(MLPendingAckStore.PENDING_ACK_STORE_SUFFIX);
    }

    public static boolean isTransactionInternalName(TopicName topicName) {
        String topic = topicName.toString();
        return topic.startsWith(TRANSACTION_COORDINATOR_LOG.toString())
                || topic.endsWith(MLPendingAckStore.PENDING_ACK_STORE_SUFFIX);
    }

    @VisibleForTesting
    protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
        return new BrokerService(pulsar, ioEventLoopGroup);
    }

    /**
     * This is a temporary solution until we'll have migrated BK metadata to map to MetadataStore interface.
     */
    private ZooKeeper getZkClient() {
        if (localMetadataStore instanceof ZKMetadataStore) {
            return ((ZKMetadataStore) localMetadataStore).getZkClient();
        } else {
            throw new RuntimeException("MetadataStore implemenation is not based on ZooKeeper");
        }
    }
}
