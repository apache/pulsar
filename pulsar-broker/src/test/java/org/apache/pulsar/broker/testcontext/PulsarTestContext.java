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

package org.apache.pulsar.broker.testcontext;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.MockZooKeeperSession;
import org.apache.zookeeper.data.ACL;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;

/**
 * A test context that can be used to set up a Pulsar broker and associated resources.
 *
 * There are 2 types of Pulsar unit tests that use a PulsarService:
 * <ul>
 * <li>Some Pulsar unit tests use a PulsarService that isn't started</li>
 * <li>Some Pulsar unit tests start the PulsarService and use less mocking</li>
 * </ul>
 *
 * This class can be used to set up a PulsarService that can be used in both types of tests.
 *
 * There are few motivations for PulsarTestContext:
 * <ul>
 * <li>It reduces the reliance on Mockito for hooking into the PulsarService for injecting mocks or customizing the behavior of some
 * collaborators. Mockito is not thread-safe and some mocking operations get corrupted. Some examples of the issuess: https://github.com/apache/pulsar/issues/13620, https://github.com/apache/pulsar/issues/16444 and https://github.com/apache/pulsar/issues/16427.</li>
 * <li>Since the Mockito issue causes test flakiness, this change will improve reliability.</li>
 * <li>It makes it possible to use composition over inheritance in test classes. This can help reduce the dependency on
 * deep test base cases hierarchies.</li>
 * <li>It reduces code duplication across test classes.</li>
 * </ul>
 *
 * <h2>Example usage of a PulsarService that is started</h2>
 * <pre>{@code
 * PulsarTestContext testContext = PulsarTestContext.builder()
 *     .spyByDefault()
 *     .withMockZooKeeper()
 *     .build();
 * PulsarService pulsarService = testContext.getPulsarService();
 * try {
 *     // Do some testing
 * } finally {
 *     testContext.close();
 * }
 * }</pre>
 *
 * <h2>Example usage of a PulsarService that is not started at all</h2>
 * <pre>{@code
 * PulsarTestContext testContext = PulsarTestContext.builderForNonStartableContext()
 *     .spyByDefault()
 *     .build();
 * PulsarService pulsarService = testContext.getPulsarService();
 * try {
 *     // Do some testing
 * } finally {
 *     testContext.close();
 * }
 * }</pre>
 */
@Slf4j
@ToString
@Getter
@Builder(builderClassName = "Builder")
public class PulsarTestContext implements AutoCloseable {
    private final ServiceConfiguration config;
    private final MetadataStoreExtended localMetadataStore;
    private final MetadataStoreExtended configurationMetadataStore;
    private final PulsarResources pulsarResources;

    private final OrderedExecutor executor;

    private final ManagedLedgerStorage managedLedgerClientFactory;

    private final PulsarService pulsarService;

    private final Compactor compactor;

    private final BrokerService brokerService;

    @Getter(AccessLevel.NONE)
    @Singular("registerCloseable")
    private final List<AutoCloseable> closeables;

    private final BrokerInterceptor brokerInterceptor;

    private final BookKeeper bookKeeperClient;

    private final MockZooKeeper mockZooKeeper;

    private final MockZooKeeper mockZooKeeperGlobal;

    private final SpyConfig spyConfig;

    private final boolean startable;


    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerClientFactory.getManagedLedgerFactory();
    }

    public PulsarMockBookKeeper getMockBookKeeper() {
        return PulsarMockBookKeeper.class.cast(bookKeeperClient);
    }

    /**
     * Create a builder for a PulsarTestContext that creates a PulsarService that
     * is started.
     *
     * @return a builder for a PulsarTestContext
     */
    public static Builder builder() {
        return new StartableCustomBuilder();
    }

    /**
     * Create a builder for a PulsarTestContext that creates a PulsarService that
     * cannot be started. Some tests need to create a PulsarService that cannot be started.
     * This was added when this type of tests were migrated to use PulsarTestContext.
     *
     * @return a builder for a PulsarTestContext that cannot be started.
     */
    public static Builder builderForNonStartableContext() {
        return new NonStartableCustomBuilder();
    }

    /**
     * Close the PulsarTestContext and all the resources that it created.
     *
     * @throws Exception if there is an error closing the resources
     */
    public void close() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception e) {
                log.error("Failure in calling cleanup function", e);
            }
        }
    }

    /**
     * Create a ServerCnx instance that has a Mockito spy that is recording invocations.
     * This is useful for tests for ServerCnx.
     *
     * @return a ServerCnx instance
     */
    public ServerCnx createServerCnxSpy() {
        return BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations(ServerCnx.class,
                getPulsarService());
    }

    /**
     * A builder for a PulsarTestContext.
     *
     * The builder is created with Lombok. That is the reason why the builder source code doesn't show all behaviors
     */
    public static class Builder {
        protected boolean useTestPulsarResources = false;
        protected MetadataStore pulsarResourcesMetadataStore;
        protected SpyConfig.Builder spyConfigBuilder = SpyConfig.builder(SpyConfig.SpyType.NONE);
        protected Consumer<PulsarService> pulsarServiceCustomizer;
        protected ServiceConfiguration svcConfig = initializeConfig();
        protected Consumer<ServiceConfiguration> configOverrideCustomizer = this::defaultOverrideServiceConfiguration;
        protected Function<BrokerService, BrokerService> brokerServiceCustomizer = Function.identity();

        /**
         * Initialize the ServiceConfiguration with default values.
         */
        protected ServiceConfiguration initializeConfig() {
            ServiceConfiguration svcConfig = new ServiceConfiguration();
            defaultOverrideServiceConfiguration(svcConfig);
            return svcConfig;
        }

        /**
         * Some settings like the broker shutdown timeout and thread pool sizes are
         * overridden if the provided values are the default values.
         * This is used to run tests with smaller thread pools and shorter timeouts by default.
         * You can use <pre>{@code .configCustomizer(null)}</pre> to disable this behavior
         */
        protected void defaultOverrideServiceConfiguration(ServiceConfiguration svcConfig) {
            ServiceConfiguration unconfiguredDefaults = new ServiceConfiguration();

            // adjust brokerShutdownTimeoutMs if it is the default value or if it is 0
            if (svcConfig.getBrokerShutdownTimeoutMs() == unconfiguredDefaults.getBrokerShutdownTimeoutMs()
                    || svcConfig.getBrokerShutdownTimeoutMs() == 0L) {
                svcConfig.setBrokerShutdownTimeoutMs(resolveBrokerShutdownTimeoutMs());
            }

            // adjust thread pool sizes if they are the default values

            if (svcConfig.getNumIOThreads() == unconfiguredDefaults.getNumIOThreads()) {
                svcConfig.setNumIOThreads(4);
            }
            if (svcConfig.getNumOrderedExecutorThreads() == unconfiguredDefaults.getNumOrderedExecutorThreads()) {
                // use a single threaded ordered executor by default
                svcConfig.setNumOrderedExecutorThreads(1);
            }
            if (svcConfig.getNumExecutorThreadPoolSize() == unconfiguredDefaults.getNumExecutorThreadPoolSize()) {
                svcConfig.setNumExecutorThreadPoolSize(5);
            }
            if (svcConfig.getNumCacheExecutorThreadPoolSize()
                    == unconfiguredDefaults.getNumCacheExecutorThreadPoolSize()) {
                svcConfig.setNumCacheExecutorThreadPoolSize(2);
            }
            if (svcConfig.getNumHttpServerThreads() == unconfiguredDefaults.getNumHttpServerThreads()) {
                svcConfig.setNumHttpServerThreads(8);
            }

            // change the default value for ports so that a random port is used
            if (unconfiguredDefaults.getBrokerServicePort().equals(svcConfig.getBrokerServicePort())) {
                svcConfig.setBrokerServicePort(Optional.of(0));
            }
            if (unconfiguredDefaults.getWebServicePort().equals(svcConfig.getWebServicePort())) {
                svcConfig.setWebServicePort(Optional.of(0));
            }

            // change the default value for nic speed
            if (unconfiguredDefaults.getLoadBalancerOverrideBrokerNicSpeedGbps()
                    .equals(svcConfig.getLoadBalancerOverrideBrokerNicSpeedGbps())) {
                svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            }

            // set the cluster name if it's unset
            if (svcConfig.getClusterName() == null) {
                svcConfig.setClusterName("test");
            }

            // adjust managed ledger cache size
            if (svcConfig.getManagedLedgerCacheSizeMB() == unconfiguredDefaults.getManagedLedgerCacheSizeMB()) {
                svcConfig.setManagedLedgerCacheSizeMB(8);
            }
        }

        /**
         * Internal method used in the {@link StartableCustomBuilder} to override the default value for the broker
         * shutdown timeout.
         * @return the broker shutdown timeout in milliseconds
         */
        protected long resolveBrokerShutdownTimeoutMs() {
            return 0L;
        }

        /**
         * Configure the PulsarService instance and the PulsarService collaborator objects to use Mockito spies by default.
         * @see SpyConfig
         * @return the builder
         */
        public Builder spyByDefault() {
            spyConfigBuilder = SpyConfig.builder(SpyConfig.SpyType.SPY);
            return this;
        }

        public Builder spyConfigCustomizer(Consumer<SpyConfig.Builder> spyConfigCustomizer) {
            spyConfigCustomizer.accept(spyConfigBuilder);
            return this;
        }

        /**
         * Customize the ServiceConfiguration object that is used to configure the PulsarService instance.
         * @param configCustomerizer the function to customize the ServiceConfiguration instance
         * @return the builder
         */
        public Builder configCustomizer(Consumer<ServiceConfiguration> configCustomerizer) {
            configCustomerizer.accept(svcConfig);
            return this;
        }

        /**
         * Override configuration values in the ServiceConfiguration instance as a last step.
         * There are default overrides provided by
         * {@link PulsarTestContext.Builder#defaultOverrideServiceConfiguration(ServiceConfiguration)}
         * that can be disabled by using <pre>{@code .configOverride(null)}</pre>
         *
         * @param configOverrideCustomizer the function that contains overrides to ServiceConfiguration,
         *                                 set to null to disable default overrides
         * @return the builder
         */
        public Builder configOverride(Consumer<ServiceConfiguration> configOverrideCustomizer) {
            this.configOverrideCustomizer = configOverrideCustomizer;
            return this;
        }

        /**
         * Customize the PulsarService instance.
         * @param pulsarServiceCustomizer the function to customize the PulsarService instance
         * @return the builder
         */
        public Builder pulsarServiceCustomizer(
                Consumer<PulsarService> pulsarServiceCustomizer) {
            this.pulsarServiceCustomizer = pulsarServiceCustomizer;
            return this;
        }

        /**
         * Reuses the BookKeeper client and metadata stores from another PulsarTestContext.
         * @param otherContext the other PulsarTestContext
         * @return the builder
         */
        public Builder reuseMockBookkeeperAndMetadataStores(PulsarTestContext otherContext) {
            bookKeeperClient(otherContext.getBookKeeperClient());
            if (otherContext.getMockZooKeeper() != null) {
                mockZooKeeper(otherContext.getMockZooKeeper());
                if (otherContext.getMockZooKeeperGlobal() != null) {
                    mockZooKeeperGlobal(otherContext.getMockZooKeeperGlobal());
                }
            } else {
                localMetadataStore(NonClosingProxyHandler.createNonClosingProxy(otherContext.getLocalMetadataStore(),
                        MetadataStoreExtended.class
                ));
                configurationMetadataStore(NonClosingProxyHandler.createNonClosingProxy(
                        otherContext.getConfigurationMetadataStore(), MetadataStoreExtended.class
                ));
            }
            return this;
        }

        /**
         * Reuses the {@link SpyConfig} from another PulsarTestContext.
         * @param otherContext the other PulsarTestContext
         * @return the builder
         */
        public Builder reuseSpyConfig(PulsarTestContext otherContext) {
            spyConfigBuilder = otherContext.getSpyConfig().toBuilder();
            return this;
        }

        /**
         * Chains closing of the other PulsarTestContext to this one.
         * The other PulsarTestContext will be closed when this one is closed.
         */
        public Builder chainClosing(PulsarTestContext otherContext) {
            registerCloseable(otherContext);
            return this;
        }

        /**
         * Configure this PulsarTestContext to use a mock ZooKeeper instance which is
         * shared for both the local and configuration metadata stores.
         *
         * @return the builder
         */
        public Builder withMockZookeeper() {
            return withMockZookeeper(false);
        }

        /**
         * Configure this PulsarTestContext to use a mock ZooKeeper instance.
         *
         * @param useSeparateGlobalZk if true, the global (configuration) zookeeper will be a separate instance
         * @return the builder
         */
        public Builder withMockZookeeper(boolean useSeparateGlobalZk) {
            try {
                mockZooKeeper(createMockZooKeeper());
                if (useSeparateGlobalZk) {
                    mockZooKeeperGlobal(createMockZooKeeper());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        private MockZooKeeper createMockZooKeeper() throws Exception {
            MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
            List<ACL> dummyAclList = new ArrayList<>(0);

            ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                    "".getBytes(StandardCharsets.UTF_8), dummyAclList, CreateMode.PERSISTENT);

            zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(StandardCharsets.UTF_8), dummyAclList,
                    CreateMode.PERSISTENT);

            registerCloseable(zk::shutdown);
            return zk;
        }

        /**
         * Applicable only when PulsarTestContext is not startable. This will configure mocks
         * for PulsarTestResources and related classes.
         *
         * @return the builder
         */
        public Builder useTestPulsarResources() {
            if (startable) {
                throw new IllegalStateException("Cannot useTestPulsarResources when startable.");
            }
            useTestPulsarResources = true;
            return this;
        }

        /**
         * Applicable only when PulsarTestContext is not startable. This will configure mocks
         * for PulsarTestResources and related collaborator instances.
         * The {@link NamespaceResources} and {@link TopicResources} instances will use the provided
         * {@link MetadataStore} instance.
         * @param metadataStore the metadata store to use
         * @return the builder
         */
        public Builder useTestPulsarResources(MetadataStore metadataStore) {
            if (startable) {
                throw new IllegalStateException("Cannot useTestPulsarResources when startable.");
            }
            useTestPulsarResources = true;
            pulsarResourcesMetadataStore = metadataStore;
            return this;
        }

        /**
         * Applicable only when PulsarTestContext is not startable. This will configure the {@link BookKeeper}
         * and {@link ManagedLedgerFactory} instances to use for creating a {@link ManagedLedgerStorage} instance
         * for PulsarService.
         *
         * @param bookKeeperClient the bookkeeper client to use (mock bookkeeper)
         * @param managedLedgerFactory the managed ledger factory to use (could be a mock)
         * @return the builder
         */
        public Builder managedLedgerClients(BookKeeper bookKeeperClient,
                                            ManagedLedgerFactory managedLedgerFactory) {
            return managedLedgerClientFactory(
                    PulsarTestContext.createManagedLedgerClientFactory(bookKeeperClient, managedLedgerFactory));
        }

        /**
         * Configures a function to use for customizing the {@link BrokerService} instance when it gets created.
         * @return the builder
         */
        public Builder brokerServiceCustomizer(Function<BrokerService, BrokerService> brokerServiceCustomizer) {
            this.brokerServiceCustomizer = brokerServiceCustomizer;
            return this;
        }
    }

    /**
     * Internal class that contains customizations for the Lombok generated Builder.
     *
     * With Lombok, it is necessary to extend the generated Builder class for adding customizations related to
     * instantiation and completing the builder.
     */
    static abstract class AbstractCustomBuilder extends Builder {
        AbstractCustomBuilder(boolean startable) {
            super.startable = startable;
        }

        public Builder startable(boolean startable) {
            throw new UnsupportedOperationException("Cannot change startability after builder creation.");
        }

        @Override
        public final PulsarTestContext build() {
            SpyConfig spyConfig = spyConfigBuilder.build();
            spyConfig(spyConfig);
            if (super.config == null) {
                config(svcConfig);
            }
            if (configOverrideCustomizer != null) {
                configOverrideCustomizer.accept(super.config);
            }
            if (super.brokerInterceptor != null) {
                super.config.setDisableBrokerInterceptors(false);
            }
            initializeCommonPulsarServices(spyConfig);
            initializePulsarServices(spyConfig, this);
            if (pulsarServiceCustomizer != null) {
                pulsarServiceCustomizer.accept(super.pulsarService);
            }
            if (super.startable) {
                try {
                    super.pulsarService.start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            brokerService(super.pulsarService.getBrokerService());
            return super.build();
        }

        private void initializeCommonPulsarServices(SpyConfig spyConfig) {
            if (super.bookKeeperClient == null && super.managedLedgerClientFactory == null) {
                if (super.executor == null) {
                    OrderedExecutor createdExecutor = OrderedExecutor.newBuilder().numThreads(1)
                            .name(PulsarTestContext.class.getSimpleName() + "-executor").build();
                    registerCloseable(() -> GracefulExecutorServicesShutdown.initiate()
                            .timeout(Duration.ZERO)
                            .shutdown(createdExecutor)
                            .handle().get());
                    super.executor = createdExecutor;
                }
                NonClosableMockBookKeeper mockBookKeeper;
                try {
                    mockBookKeeper =
                            spyConfig.getBookKeeperClient().spy(NonClosableMockBookKeeper.class, super.executor);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                registerCloseable(() -> {
                    mockBookKeeper.reallyShutdown();
                    resetSpyOrMock(mockBookKeeper);
                });
                bookKeeperClient(mockBookKeeper);
            }
            if (super.bookKeeperClient == null && super.managedLedgerClientFactory != null) {
                bookKeeperClient(super.managedLedgerClientFactory.getBookKeeperClient());
            }
            if (super.localMetadataStore == null || super.configurationMetadataStore == null) {
                if (super.mockZooKeeper != null) {
                    MetadataStoreExtended mockZookeeperMetadataStore =
                            createMockZookeeperMetadataStore(super.mockZooKeeper, MetadataStoreConfig.METADATA_STORE);
                    if (super.localMetadataStore == null) {
                        localMetadataStore(mockZookeeperMetadataStore);
                    }
                    if (super.configurationMetadataStore == null) {
                        if (super.mockZooKeeperGlobal != null) {
                            configurationMetadataStore(createMockZookeeperMetadataStore(super.mockZooKeeperGlobal,
                                    MetadataStoreConfig.CONFIGURATION_METADATA_STORE));
                        } else {
                            configurationMetadataStore(mockZookeeperMetadataStore);
                        }
                    }
                } else {
                    try {
                        MetadataStoreExtended store = MetadataStoreFactoryImpl.createExtended("memory:local",
                                MetadataStoreConfig.builder().build());
                        registerCloseable(() -> {
                            store.close();
                            resetSpyOrMock(store);
                        });
                        MetadataStoreExtended nonClosingProxy =
                                NonClosingProxyHandler.createNonClosingProxy(store, MetadataStoreExtended.class
                                );
                        if (super.localMetadataStore == null) {
                            localMetadataStore(nonClosingProxy);
                        }
                        if (super.configurationMetadataStore == null) {
                            configurationMetadataStore(nonClosingProxy);
                        }
                    } catch (MetadataStoreException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private MetadataStoreExtended createMockZookeeperMetadataStore(MockZooKeeper mockZooKeeper,
                                                                       String metadataStoreName) {
            // provide a unique session id for each instance
            MockZooKeeperSession mockZooKeeperSession = MockZooKeeperSession.newInstance(mockZooKeeper, false);
            registerCloseable(() -> {
                mockZooKeeperSession.close();
                resetSpyOrMock(mockZooKeeperSession);
            });
            ZKMetadataStore zkMetadataStore = new ZKMetadataStore(mockZooKeeperSession,
                    MetadataStoreConfig.builder().metadataStoreName(metadataStoreName).build());
            registerCloseable(() -> {
                zkMetadataStore.close();
                resetSpyOrMock(zkMetadataStore);
            });
            MetadataStoreExtended nonClosingProxy =
                    NonClosingProxyHandler.createNonClosingProxy(zkMetadataStore, MetadataStoreExtended.class);
            return nonClosingProxy;
        }

        protected abstract void initializePulsarServices(SpyConfig spyConfig, Builder builder);
    }

    static void resetSpyOrMock(Object object) {
        if (MockUtil.isMock(object)) {
            Mockito.reset(object);
        }
    }

    /**
     * Customizations for a builder for creating a PulsarTestContext that is "startable".
     */
    static class StartableCustomBuilder extends AbstractCustomBuilder {
        StartableCustomBuilder() {
            super(true);
        }

        @Override
        public Builder managedLedgerClientFactory(ManagedLedgerStorage managedLedgerClientFactory) {
            throw new IllegalStateException("Cannot set managedLedgerClientFactory when startable.");
        }

        @Override
        public Builder pulsarResources(PulsarResources pulsarResources) {
            throw new IllegalStateException("Cannot set pulsarResources when startable.");
        }

        @Override
        protected void initializePulsarServices(SpyConfig spyConfig, Builder builder) {
            BookKeeperClientFactory bookKeeperClientFactory =
                    new MockBookKeeperClientFactory(builder.bookKeeperClient);
            PulsarService pulsarService = spyConfig.getPulsarService()
                    .spy(StartableTestPulsarService.class, spyConfig, builder.config, builder.localMetadataStore,
                            builder.configurationMetadataStore, builder.compactor, builder.brokerInterceptor,
                            bookKeeperClientFactory, builder.brokerServiceCustomizer);
            registerCloseable(() -> {
                pulsarService.close();
                resetSpyOrMock(pulsarService);
            });
            pulsarService(pulsarService);
        }

        @Override
        protected long resolveBrokerShutdownTimeoutMs() {
            // wait 5 seconds for the startable pulsar service to shutdown gracefully
            // this reduces the chances that some threads of the PulsarTestsContexts of subsequent test executions
            // are running in parallel. It doesn't prevent it completely.
            return 5000L;
        }
    }

    /**
     * Customizations for a builder for creating a PulsarTestContext that is "non-startable".
     */
    static class NonStartableCustomBuilder extends AbstractCustomBuilder {

        NonStartableCustomBuilder() {
            super(false);
        }

        @Override
        protected void initializePulsarServices(SpyConfig spyConfig, Builder builder) {
            if (builder.managedLedgerClientFactory == null) {
                ManagedLedgerFactory mlFactoryMock = Mockito.mock(ManagedLedgerFactory.class);
                managedLedgerClientFactory(
                        PulsarTestContext.createManagedLedgerClientFactory(builder.bookKeeperClient, mlFactoryMock));
            }
            if (builder.pulsarResources == null) {
                SpyConfig.SpyType spyConfigPulsarResources = spyConfig.getPulsarResources();
                if (useTestPulsarResources) {
                    MetadataStore metadataStore = pulsarResourcesMetadataStore;
                    if (metadataStore == null) {
                        metadataStore = builder.configurationMetadataStore;
                    }
                    NamespaceResources nsr = spyConfigPulsarResources.spy(NamespaceResources.class, metadataStore, 30);
                    TopicResources tsr = spyConfigPulsarResources.spy(TopicResources.class, metadataStore);
                    pulsarResources(
                            spyConfigPulsarResources.spy(
                                    NonStartableTestPulsarService.TestPulsarResources.class, builder.localMetadataStore,
                                    builder.configurationMetadataStore,
                                    tsr, nsr));
                } else {
                    pulsarResources(
                            spyConfigPulsarResources.spy(PulsarResources.class, builder.localMetadataStore,
                                    builder.configurationMetadataStore));
                }
            }
            BookKeeperClientFactory bookKeeperClientFactory =
                    new MockBookKeeperClientFactory(builder.bookKeeperClient);
            PulsarService pulsarService = spyConfig.getPulsarService()
                    .spy(NonStartableTestPulsarService.class, spyConfig, builder.config, builder.localMetadataStore,
                            builder.configurationMetadataStore, builder.compactor, builder.brokerInterceptor,
                            bookKeeperClientFactory, builder.pulsarResources,
                            builder.managedLedgerClientFactory, builder.brokerServiceCustomizer);
            registerCloseable(() -> {
                pulsarService.close();
                resetSpyOrMock(pulsarService);
            });
            pulsarService(pulsarService);
        }
    }

    @NotNull
    private static ManagedLedgerStorage createManagedLedgerClientFactory(BookKeeper bookKeeperClient,
                                                                         ManagedLedgerFactory managedLedgerFactory) {
        return new ManagedLedgerStorage() {

            @Override
            public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                                   BookKeeperClientFactory bookkeeperProvider, EventLoopGroup eventLoopGroup)
                    throws Exception {

            }

            @Override
            public ManagedLedgerFactory getManagedLedgerFactory() {
                return managedLedgerFactory;
            }

            @Override
            public StatsProvider getStatsProvider() {
                return new NullStatsProvider();
            }

            @Override
            public BookKeeper getBookKeeperClient() {
                return bookKeeperClient;
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
