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
package com.yahoo.pulsar.broker;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.*;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.broker.admin.AdminResource;
import com.yahoo.pulsar.broker.cache.ConfigurationCacheService;
import com.yahoo.pulsar.broker.cache.LocalZooKeeperCacheService;
import com.yahoo.pulsar.broker.loadbalance.LeaderElectionService;
import com.yahoo.pulsar.broker.loadbalance.LeaderElectionService.LeaderListener;
import com.yahoo.pulsar.broker.loadbalance.LoadManager;
import com.yahoo.pulsar.broker.loadbalance.LoadReportUpdaterTask;
import com.yahoo.pulsar.broker.loadbalance.LoadResourceQuotaUpdaterTask;
import com.yahoo.pulsar.broker.loadbalance.LoadSheddingTask;
import com.yahoo.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.broker.service.Topic;
import com.yahoo.pulsar.broker.stats.MetricsGenerator;
import com.yahoo.pulsar.broker.web.WebService;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.websocket.WebSocketConsumerServlet;
import com.yahoo.pulsar.websocket.WebSocketProducerServlet;
import com.yahoo.pulsar.websocket.WebSocketService;
import com.yahoo.pulsar.zookeeper.GlobalZooKeeperCache;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperCache;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperConnectionService;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Main class for Pulsar broker service
 */
public class PulsarService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarService.class);
    private static final String DYNAMIC_LOAD_MANAGER_ZPATH = "/loadbalance/settings/load-manager";
    private ServiceConfiguration config = null;
    private NamespaceService nsservice = null;
    private ManagedLedgerClientFactory managedLedgerClientFactory = null;
    private LeaderElectionService leaderElectionService = null;
    private BrokerService brokerService = null;
    private WebService webService = null;
    private WebSocketService webSocketService = null;
    private ConfigurationCacheService configurationCacheService = null;
    private LocalZooKeeperCacheService localZkCacheService = null;
    private ZooKeeperCache localZkCache;
    private GlobalZooKeeperCache globalZkCache;
    private LocalZooKeeperConnectionService localZooKeeperConnectionProvider;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
            new DefaultThreadFactory("pulsar"));
    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(8, "pulsar-ordered");
    private ScheduledExecutorService loadManagerExecutor = null;
    private ScheduledFuture<?> loadReportTask = null;
    private ScheduledFuture<?> loadSheddingTask = null;
    private ScheduledFuture<?> loadResourceQuotaTask = null;
    private AtomicReference<LoadManager> loadManager = null;
    private PulsarAdmin adminClient = null;
    private ZooKeeperClientFactory zkClientFactory = null;
    private final String bindAddress;
    private final String advertisedAddress;
    private final String webServiceAddress;
    private final String webServiceAddressTls;
    private final String brokerServiceUrl;
    private final String brokerServiceUrlTls;

    private final MessagingServiceShutdownHook shutdownService;

    private MetricsGenerator metricsGenerator;

    public enum State {
        Init, Started, Closed
    }

    private State state;

    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition isClosedCondition = mutex.newCondition();

    public PulsarService(ServiceConfiguration config) {
        state = State.Init;
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getBindAddress());
        this.advertisedAddress = advertisedAddress(config);
        this.webServiceAddress = webAddress(config);
        this.webServiceAddressTls = webAddressTls(config);
        this.brokerServiceUrl = brokerUrl(config);
        this.brokerServiceUrlTls = brokerUrlTls(config);
        this.config = config;
        this.shutdownService = new MessagingServiceShutdownHook(this);
        loadManagerExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Close the current pulsar service. All resources are released.
     */
    @Override
    public void close() throws PulsarServerException {
        mutex.lock();

        try {
            if (state == State.Closed) {
                return;
            }

            // close the service in reverse order v.s. in which they are started
            if (this.webService != null) {
                this.webService.close();
                this.webService = null;
            }

            if (this.brokerService != null) {
                this.brokerService.close();
                this.brokerService = null;
            }

            if (this.managedLedgerClientFactory != null) {
                this.managedLedgerClientFactory.close();
                this.managedLedgerClientFactory = null;
            }

            if (this.leaderElectionService != null) {
                this.leaderElectionService.stop();
                this.leaderElectionService = null;
            }

            if (loadManagerExecutor != null) {
                loadManagerExecutor.shutdownNow();
            }
            loadManager = null;

            if (globalZkCache != null) {
                globalZkCache.close();
                globalZkCache = null;
                localZooKeeperConnectionProvider.close();
                localZooKeeperConnectionProvider = null;
            }

            configurationCacheService = null;
            localZkCacheService = null;
            localZkCache = null;

            if (adminClient != null) {
                adminClient.close();
                adminClient = null;
            }

            nsservice = null;

            // executor is not initialized in mocks even when real close method is called
            // guard against null executors
            if (executor != null) {
                executor.shutdown();
            }

            orderedExecutor.shutdown();
            state = State.Closed;

        } catch (Exception e) {
            throw new PulsarServerException(e);
        } finally {
            mutex.unlock();
        }
    }

    private class LoadManagerWatcher implements Watcher {
        public void process(final WatchedEvent event) {
            new Thread(() -> {
                try {
                    LOG.info("Attempting to change load manager");
                    final String newLoadManagerName =
                            new String(getZkClient().getData(DYNAMIC_LOAD_MANAGER_ZPATH, this, null));

                    config.setLoadManagerClassName(newLoadManagerName);
                    final LoadManager newLoadManager = LoadManager.create(PulsarService.this);
                    LOG.info("Created load manager: {}", newLoadManagerName);
                    loadManager.get().disableBroker();
                    newLoadManager.start();
                    loadManager.set(newLoadManager);

                } catch (Exception ex) {
                    LOG.warn("Failed to change load manager due to {}", ex);
                }
            }).start();
        }
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
     * Start the pulsar service instance.
     */
    public void start() throws PulsarServerException {
        mutex.lock();

        try {
            if (state != State.Init) {
                throw new PulsarServerException("Cannot start the service once it was stopped");
            }

            // Now we are ready to start services
            localZooKeeperConnectionProvider = new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                    config.getZookeeperServers(), config.getZooKeeperSessionTimeoutMillis());
            localZooKeeperConnectionProvider.start(shutdownService);

            // Initialize and start service to access configuration repository.
            this.startZkCacheService();

            managedLedgerClientFactory = new ManagedLedgerClientFactory(config, getZkClient(),
                    getBookKeeperClientFactory());

            this.brokerService = new BrokerService(this);

            // Start load management service (even if load balancing is disabled)
            if (getZkClient().exists(DYNAMIC_LOAD_MANAGER_ZPATH, false) != null) {
                config.setLoadManagerClassName(new String(getZkClient().getData(DYNAMIC_LOAD_MANAGER_ZPATH, false, null)));
            }
            this.loadManager = new AtomicReference<>(LoadManager.create(this));

            this.startLoadManagementService();

            // needs load management service
            this.startNamespaceService();

            LOG.info("Starting Pulsar Broker service");
            brokerService.start();

            this.webService = new WebService(this);
            this.webService.addRestResources("/", "com.yahoo.pulsar.broker.web", false);
            this.webService.addRestResources("/admin", "com.yahoo.pulsar.broker.admin", true);
            this.webService.addRestResources("/lookup", "com.yahoo.pulsar.broker.lookup", true);

            if (config.isWebSocketServiceEnabled()) {
                // Use local broker address to avoid different IP address when using a VIP for service discovery
                this.webSocketService = new WebSocketService(new ClusterData(webServiceAddress, webServiceAddressTls),
                        config);
                this.webSocketService.start();
                this.webService.addServlet(WebSocketProducerServlet.SERVLET_PATH,
                        new ServletHolder(new WebSocketProducerServlet(webSocketService)), true);
                this.webService.addServlet(WebSocketConsumerServlet.SERVLET_PATH,
                        new ServletHolder(new WebSocketConsumerServlet(webSocketService)), true);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Attempting to add static directory");
            }
            this.webService.addStaticResources("/static", "/static");

            // Register heartbeat and bootstrap namespaces.
            this.nsservice.registerBootstrapNamespaces();

            // Start the leader election service
            this.leaderElectionService = new LeaderElectionService(this, new LeaderListener() {
                @Override
                public synchronized void brokerIsTheLeaderNow() {
                    if (getConfiguration().isLoadBalancerEnabled()) {
                        long loadSheddingInterval = TimeUnit.MINUTES
                                .toMillis(getConfiguration().getLoadBalancerSheddingIntervalMinutes());
                        long resourceQuotaUpdateInterval = TimeUnit.MINUTES
                                .toMillis(getConfiguration().getLoadBalancerResourceQuotaUpdateIntervalMinutes());

                        loadSheddingTask = loadManagerExecutor.scheduleAtFixedRate(new LoadSheddingTask(loadManager),
                                loadSheddingInterval, loadSheddingInterval, TimeUnit.MILLISECONDS);
                        loadResourceQuotaTask = loadManagerExecutor.scheduleAtFixedRate(
                                new LoadResourceQuotaUpdaterTask(loadManager), resourceQuotaUpdateInterval,
                                resourceQuotaUpdateInterval, TimeUnit.MILLISECONDS);
                    }
                }

                @Override
                public synchronized void brokerIsAFollowerNow() {
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
            webService.start();

            this.metricsGenerator = new MetricsGenerator(this);

            state = State.Started;

            acquireSLANamespace();

            try {
                ZkUtils.createFullPathOptimistic(getZkClient(), DYNAMIC_LOAD_MANAGER_ZPATH,
                        config.getLoadManagerClassName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Ignore
            }

            getZkClient().getData(DYNAMIC_LOAD_MANAGER_ZPATH, new LoadManagerWatcher(), null);

            LOG.info("messaging service is ready, bootstrap service on port={}, broker url={}, cluster={}, configs={}",
                    config.getWebServicePort(), brokerServiceUrl, config.getClusterName(), config);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        } finally {
            mutex.unlock();
        }
    }

    private void acquireSLANamespace() {
        try {
            // Namespace not created hence no need to unload it
            if (!this.globalZkCache.exists(
                    AdminResource.path("policies") + "/" + NamespaceService.getSLAMonitorNamespace(getAdvertisedAddress(), config))) {
                return;
            }

            boolean acquiredSLANamespace;
            try {
                acquiredSLANamespace = nsservice.registerSLANamespace();
            } catch (PulsarServerException e) {
                acquiredSLANamespace = false;
            }

            if (!acquiredSLANamespace) {
                this.nsservice.unloadSLANamespace();
            }
        } catch (Exception ex) {
            LOG.warn(
                    "Exception while trying to unload the SLA namespace, will try to unload the namespace again after 1 minute. Exception:",
                    ex);
            executor.schedule(this::acquireSLANamespace, 1, TimeUnit.MINUTES);
        } catch (Throwable ex) {
            // To make sure SLA monitor doesn't interfere with the normal broker flow
            LOG.warn(
                    "Exception while trying to unload the SLA namespace, will not try to unload the namespace again. Exception:",
                    ex);
        }
    }

    /**
     * Block until the service is finally closed
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

    private void startZkCacheService() throws PulsarServerException {

        LOG.info("starting configuration cache service");

        this.localZkCache = new LocalZooKeeperCache(getZkClient(), getOrderedExecutor());
        this.globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(),
                (int) config.getZooKeeperSessionTimeoutMillis(), config.getGlobalZookeeperServers(),
                getOrderedExecutor(), this.executor);
        try {
            this.globalZkCache.start();
        } catch (IOException e) {
            throw new PulsarServerException(e);
        }

        this.configurationCacheService = new ConfigurationCacheService(getGlobalZkCache());
        this.localZkCacheService = new LocalZooKeeperCacheService(getLocalZkCache(), this.configurationCacheService);
    }

    private void startNamespaceService() throws PulsarServerException {

        LOG.info("starting name space service, bootstrap namespaces=" + config.getBootstrapNamespaces());

        this.nsservice = getNamespaceServiceProvider().get();
    }

    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> new NamespaceService(PulsarService.this);
    }

    private void startLoadManagementService() throws PulsarServerException {
        LOG.info("Starting load management service ...");
        this.loadManager.get().start();

        if (config.isLoadBalancerEnabled()) {
            LOG.info("Starting load balancer");
            if (this.loadReportTask == null) {
                long loadReportMinInterval = SimpleLoadManagerImpl.LOAD_REPORT_UPDATE_MIMIMUM_INTERVAL;
                this.loadReportTask = this.loadManagerExecutor.scheduleAtFixedRate(
                        new LoadReportUpdaterTask(loadManager), loadReportMinInterval, loadReportMinInterval,
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Load all the destination contained in a namespace
     *
     * @param bundle
     *            <code>NamespaceBundle</code> to identify the service unit
     * @throws Exception
     */
    public void loadNamespaceDestinations(NamespaceBundle bundle) {
        executor.submit(() -> {
            LOG.info("Loading all topics on bundle: {}", bundle);

            NamespaceName nsName = bundle.getNamespaceObject();
            List<CompletableFuture<Topic>> persistentTopics = Lists.newArrayList();
            long topicLoadStart = System.nanoTime();

            for (String topic : getNamespaceService().getListOfDestinations(nsName.getProperty(), nsName.getCluster(),
                    nsName.getLocalName())) {
                try {
                    DestinationName dn = DestinationName.get(topic);
                    if (bundle.includes(dn)) {
                        CompletableFuture<Topic> future = brokerService.getTopic(topic);
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

    public ZooKeeper getZkClient() {
        return this.localZooKeeperConnectionProvider.getLocalZooKeeper();
    }

    public ConfigurationCacheService getConfigurationCache() {
        return configurationCacheService;
    }

    /**
     * Get the current pulsar state.
     */
    public State getState() {
        return this.state;
    }

    /**
     * Get a reference of the current <code>LeaderElectionService</code> instance associated with the current
     * <code>PulsarService<code> instance.
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
        return this.nsservice;
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

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerClientFactory.getManagedLedgerFactory();
    }

    public ZooKeeperCache getLocalZkCache() {
        return localZkCache;
    }

    public ZooKeeperCache getGlobalZkCache() {
        return globalZkCache;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public ScheduledExecutorService getLoadManagerExecutor() {
        return loadManagerExecutor;
    }

    public OrderedSafeExecutor getOrderedExecutor() {
        return orderedExecutor;
    }

    public LocalZooKeeperCacheService getLocalZkCacheService() {
        return this.localZkCacheService;
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperClientFactoryImpl();
        }
        // Return default factory
        return zkClientFactory;
    }

    public BookKeeperClientFactory getBookKeeperClientFactory() {
        return new BookKeeperClientFactoryImpl();
    }

    public synchronized PulsarAdmin getAdminClient() throws PulsarServerException {
        if (this.adminClient == null) {
            try {
                String adminApiUrl = webAddress(config);
                this.adminClient = new PulsarAdmin(new URL(adminApiUrl),
                        this.getConfiguration().getBrokerClientAuthenticationPlugin(),
                        this.getConfiguration().getBrokerClientAuthenticationParameters());
                LOG.info("Admin api url: " + adminApiUrl);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }

        return this.adminClient;
    }

    public MetricsGenerator getMetricsGenerator() {
        return metricsGenerator;
    }

    public MessagingServiceShutdownHook getShutdownService() {
        return shutdownService;
    }

    /**
     * Advertised service address.
     *
     * @return Hostname or IP address the service advertises to the outside world.
     */
    public static String advertisedAddress(ServiceConfiguration config) {
        return ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
    }

    public static String brokerUrl(ServiceConfiguration config) {
        return "pulsar://" + advertisedAddress(config) + ":" + config.getBrokerServicePort();
    }

    public static String brokerUrlTls(ServiceConfiguration config) {
        if (config.isTlsEnabled()) {
            return "pulsar://" + advertisedAddress(config) + ":" + config.getBrokerServicePortTls();
        } else {
            return "";
        }
    }

    public static String webAddress(ServiceConfiguration config) {
        return String.format("http://%s:%d", advertisedAddress(config), config.getWebServicePort());
    }

    public static String webAddressTls(ServiceConfiguration config) {
        if (config.isTlsEnabled()) {
            return String.format("https://%s:%d", advertisedAddress(config), config.getWebServicePortTls());
        } else {
            return "";
        }
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public String getAdvertisedAddress() {
        return advertisedAddress;
    }

    public String getWebServiceAddress() {
        return webServiceAddress;
    }

    public String getWebServiceAddressTls() {
        return webServiceAddressTls;
    }

    public String getBrokerServiceUrl() {
        return brokerServiceUrl;
    }

    public String getBrokerServiceUrlTls() {
        return brokerServiceUrlTls;
    }

    public AtomicReference<LoadManager> getLoadManager() {
        return loadManager;
    }

}
