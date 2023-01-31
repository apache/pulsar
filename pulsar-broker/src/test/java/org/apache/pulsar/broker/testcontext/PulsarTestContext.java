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

    public static Builder startableBuilder() {
        return new StartableCustomBuilder();
    }

    public static Builder builder() {
        return new NonStartableCustomBuilder();
    }

    public void close() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception e) {
                log.error("Failure in calling cleanup function", e);
            }
        }
    }

    public ServerCnx createServerCnxSpy() {
        return BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations(ServerCnx.class,
                getPulsarService());
    }

    public static class Builder {
        protected boolean useTestPulsarResources = false;
        protected MetadataStore pulsarResourcesMetadataStore;
        protected SpyConfig.Builder spyConfigBuilder = SpyConfig.builder(SpyConfig.SpyType.NONE);
        protected Consumer<PulsarService> pulsarServiceCustomizer;
        protected ServiceConfiguration svcConfig = initializeConfig();
        protected Consumer<ServiceConfiguration> configOverrideCustomizer = this::defaultOverrideServiceConfiguration;
        protected Function<BrokerService, BrokerService> brokerServiceCustomizer = Function.identity();
        protected boolean useSameThreadOrderedExecutor = false;

        protected ServiceConfiguration initializeConfig() {
            ServiceConfiguration svcConfig = new ServiceConfiguration();
            svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            svcConfig.setClusterName("pulsar-cluster");
            defaultOverrideServiceConfiguration(svcConfig);
            return svcConfig;
        }

        public Builder useSameThreadOrderedExecutor(boolean useSameThreadOrderedExecutor) {
            this.useSameThreadOrderedExecutor = useSameThreadOrderedExecutor;
            return this;
        }

        protected void defaultOverrideServiceConfiguration(ServiceConfiguration svcConfig) {
            svcConfig.setBrokerShutdownTimeoutMs(0L);
            svcConfig.setNumIOThreads(4);
            svcConfig.setNumOrderedExecutorThreads(1);
            svcConfig.setNumExecutorThreadPoolSize(5);
            svcConfig.setNumCacheExecutorThreadPoolSize(2);
            svcConfig.setNumHttpServerThreads(8);
        }

        public Builder spyByDefault() {
            spyConfigBuilder = SpyConfig.builder(SpyConfig.SpyType.SPY);
            return this;
        }

        public Builder spyConfigCustomizer(Consumer<SpyConfig.Builder> spyConfigCustomizer) {
            spyConfigCustomizer.accept(spyConfigBuilder);
            return this;
        }

        public Builder configCustomizer(Consumer<ServiceConfiguration> configCustomerizer) {
            configCustomerizer.accept(svcConfig);
            return this;
        }

        public Builder configOverride(Consumer<ServiceConfiguration> configOverrideCustomizer) {
            this.configOverrideCustomizer = configOverrideCustomizer;
            return this;
        }

        public Builder pulsarServiceCustomizer(
                Consumer<PulsarService> pulsarServiceCustomizer) {
            this.pulsarServiceCustomizer = pulsarServiceCustomizer;
            return this;
        }

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

        public Builder reuseSpyConfig(PulsarTestContext otherContext) {
            spyConfigBuilder = otherContext.getSpyConfig().toBuilder();
            return this;
        }

        public Builder chainClosing(PulsarTestContext otherContext) {
            registerCloseable(otherContext);
            return this;
        }

        public Builder withMockZookeeper() {
            return withMockZookeeper(false);
        }

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

        public Builder useTestPulsarResources() {
            if (startable) {
                throw new IllegalStateException("Cannot useTestPulsarResources when startable.");
            }
            useTestPulsarResources = true;
            return this;
        }

        public Builder useTestPulsarResources(MetadataStore metadataStore) {
            if (startable) {
                throw new IllegalStateException("Cannot useTestPulsarResources when startable.");
            }
            useTestPulsarResources = true;
            pulsarResourcesMetadataStore = metadataStore;
            return this;
        }

        public Builder managedLedgerClients(BookKeeper bookKeeperClient,
                                            ManagedLedgerFactory managedLedgerFactory) {
            return managedLedgerClientFactory(
                    PulsarTestContext.createManagedLedgerClientFactory(bookKeeperClient, managedLedgerFactory));
        }

        public Builder brokerServiceCustomizer(Function<BrokerService, BrokerService> brokerServiceCustomizer) {
            this.brokerServiceCustomizer = brokerServiceCustomizer;
            return this;
        }
    }

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
            PulsarService pulsarService = spyConfig.getPulsarBroker()
                    .spy(StartableTestPulsarService.class, spyConfig, builder.config, builder.localMetadataStore,
                            builder.configurationMetadataStore, builder.compactor, builder.brokerInterceptor,
                            bookKeeperClientFactory, builder.useSameThreadOrderedExecutor,
                            builder.brokerServiceCustomizer);
            registerCloseable(() -> {
                pulsarService.close();
                resetSpyOrMock(pulsarService);
            });
            pulsarService(pulsarService);
        }

        @Override
        protected void defaultOverrideServiceConfiguration(ServiceConfiguration svcConfig) {
            super.defaultOverrideServiceConfiguration(svcConfig);
            svcConfig.setBrokerShutdownTimeoutMs(5000L);
        }
    }

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
            PulsarService pulsarService = spyConfig.getPulsarBroker()
                    .spy(NonStartableTestPulsarService.class, spyConfig, builder.config, builder.localMetadataStore,
                            builder.configurationMetadataStore, builder.compactor, builder.brokerInterceptor,
                            bookKeeperClientFactory, builder.useSameThreadOrderedExecutor, builder.pulsarResources,
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
