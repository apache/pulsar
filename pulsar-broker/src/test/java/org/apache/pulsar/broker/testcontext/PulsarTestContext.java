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

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations;
import static org.mockito.Mockito.mock;
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
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
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

    private final boolean startable;


    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerClientFactory.getManagedLedgerFactory();
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
        return spyWithClassAndConstructorArgsRecordingInvocations(ServerCnx.class,
                getPulsarService());
    }

    public static class Builder {
        protected boolean useTestPulsarResources = false;
        protected MetadataStore pulsarResourcesMetadataStore;
        protected Function<PulsarService, BrokerService> brokerServiceFunction;
        protected SpyConfig.Builder spyConfigBuilder = SpyConfig.builder(SpyConfig.SpyType.NONE);

        public Builder spyByDefault() {
            spyConfigBuilder = SpyConfig.builder(SpyConfig.SpyType.SPY);
            return this;
        }

        public Builder spyConfig(Consumer<SpyConfig.Builder> spyConfigCustomizer) {
            spyConfigCustomizer.accept(spyConfigBuilder);
            return this;
        }


        public Builder reuseMockBookkeeperAndMetadataStores(PulsarTestContext otherContext) {
            bookKeeperClient(otherContext.getBookKeeperClient());
            localMetadataStore(NonClosingProxyHandler.createNonClosingProxy(otherContext.getLocalMetadataStore(),
                    MetadataStoreExtended.class
            ));
            configurationMetadataStore(NonClosingProxyHandler.createNonClosingProxy(
                    otherContext.getConfigurationMetadataStore(), MetadataStoreExtended.class
            ));
            return this;
        }

        public Builder withMockZookeeper() {
            try {
                MockZooKeeper mockZooKeeper = createMockZooKeeper();
                registerCloseable(mockZooKeeper::shutdown);
                MockZooKeeperSession mockZooKeeperSession = MockZooKeeperSession.newInstance(mockZooKeeper);
                ZKMetadataStore zkMetadataStore = new ZKMetadataStore(mockZooKeeperSession);
                registerCloseable(zkMetadataStore::close);
                localMetadataStore(zkMetadataStore);
                configurationMetadataStore(zkMetadataStore);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        private static MockZooKeeper createMockZooKeeper() throws Exception {
            MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
            List<ACL> dummyAclList = new ArrayList<>(0);

            ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                    "".getBytes(StandardCharsets.UTF_8), dummyAclList, CreateMode.PERSISTENT);

            zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(StandardCharsets.UTF_8), dummyAclList,
                    CreateMode.PERSISTENT);
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

        public Builder brokerServiceFunction(
                Function<PulsarService, BrokerService> brokerServiceFunction) {
            this.brokerServiceFunction = brokerServiceFunction;
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
            if (super.config == null) {
                ServiceConfiguration svcConfig = new ServiceConfiguration();
                initializeConfig(svcConfig);
                config(svcConfig);
            }
            initializeCommonPulsarServices(spyConfig);
            initializePulsarServices(spyConfig, this);
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

        protected void initializeConfig(ServiceConfiguration svcConfig) {
            svcConfig.setBrokerShutdownTimeoutMs(0L);
            svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            svcConfig.setClusterName("pulsar-cluster");
            svcConfig.setNumIOThreads(1);
            svcConfig.setNumOrderedExecutorThreads(1);
            svcConfig.setNumExecutorThreadPoolSize(2);
            svcConfig.setNumCacheExecutorThreadPoolSize(2);
            svcConfig.setNumHttpServerThreads(2);
        }

        private void initializeCommonPulsarServices(SpyConfig spyConfig) {
            if (super.bookKeeperClient == null && super.managedLedgerClientFactory == null) {
                if (super.executor == null) {
                    OrderedExecutor createdExecutor = OrderedExecutor.newBuilder().numThreads(1)
                            .name(NonStartableTestPulsarService.class.getSimpleName() + "-executor").build();
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
                registerCloseable(mockBookKeeper::reallyShutdown);
                bookKeeperClient(mockBookKeeper);
            }
            if (super.bookKeeperClient == null && super.managedLedgerClientFactory != null) {
                bookKeeperClient(super.managedLedgerClientFactory.getBookKeeperClient());
            }
            if (super.localMetadataStore == null || super.configurationMetadataStore == null) {
                try {
                    MetadataStoreExtended store = MetadataStoreFactoryImpl.createExtended("memory:local",
                            MetadataStoreConfig.builder().build());
                    registerCloseable(store::close);
                    if (super.localMetadataStore == null) {
                        localMetadataStore(store);
                    }
                    if (super.configurationMetadataStore == null) {
                        configurationMetadataStore(store);
                    }
                } catch (MetadataStoreException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        protected abstract void initializePulsarServices(SpyConfig spyConfig, Builder builder);
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
                    .spy(StartableTestPulsarService.class, builder.config, builder.localMetadataStore,
                            builder.configurationMetadataStore, builder.compactor, builder.brokerInterceptor,
                            bookKeeperClientFactory);
            registerCloseable(pulsarService::close);
            pulsarService(pulsarService);
        }

        @Override
        protected void initializeConfig(ServiceConfiguration svcConfig) {
            super.initializeConfig(svcConfig);
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
                ManagedLedgerFactory mlFactoryMock = mock(ManagedLedgerFactory.class);
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
                            bookKeeperClientFactory, builder.pulsarResources,
                            builder.managedLedgerClientFactory);
            registerCloseable(pulsarService::close);
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
