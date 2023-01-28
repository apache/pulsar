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

package org.apache.pulsar.broker;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations;
import static org.mockito.Mockito.mock;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarMetadataEventSynchronizer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.jetbrains.annotations.NotNull;

/**
 * Subclass of PulsarService that is used for some tests.
 * This was written as a replacement for the previous Mockito Spy over PulsarService solution which caused
 * a flaky test issue https://github.com/apache/pulsar/issues/13620.
 */

public class TestPulsarService extends PulsarService {


    @Slf4j
    @ToString
    @Getter
    @Builder
    public static class Factory implements AutoCloseable {
        private final ServiceConfiguration config;
        private final MetadataStoreExtended localMetadataStore;
        private final MetadataStoreExtended configurationMetadataStore;
        private final PulsarResources pulsarResources;

        private final OrderedExecutor executor;

        private final EventLoopGroup eventLoopGroup;

        private final ManagedLedgerStorage managedLedgerClientFactory;

        private final boolean useSpies;

        private final PulsarService pulsarService;

        private final Compactor compactor;

        private final BrokerService brokerService;

        @Getter(AccessLevel.NONE)
        @Singular
        private final List<AutoCloseable> cleanupFunctions;

        public ManagedLedgerFactory getManagedLedgerFactory() {
            return managedLedgerClientFactory.getManagedLedgerFactory();
        }

        public static FactoryBuilder builder() {
            return new CustomFactoryBuilder();
        }

        public void close() throws Exception {
            pulsarService.getBrokerService().close();
            pulsarService.close();
            GracefulExecutorServicesShutdown.initiate()
                    .timeout(Duration.ZERO)
                    .shutdown(executor)
                    .handle().get();
            eventLoopGroup.shutdownGracefully().get();
            if (localMetadataStore != configurationMetadataStore) {
                localMetadataStore.close();
                configurationMetadataStore.close();
            } else {
                localMetadataStore.close();
            }
            for (AutoCloseable cleanup : cleanupFunctions) {
                try {
                    cleanup.close();
                } catch (Exception e) {
                    log.error("Failure in calling cleanup function", e);
                }
            }
        }

        public ServerCnx createServerCnxSpy() {
            return spyWithClassAndConstructorArgsRecordingInvocations(ServerCnx.class,
                    getPulsarService());
        }

        public static class FactoryBuilder {
            protected boolean useTestPulsarResources = false;
            protected MetadataStore pulsarResourcesMetadataStore;
            protected Function<PulsarService, BrokerService> brokerServiceFunction;

            public FactoryBuilder useTestPulsarResources() {
                useTestPulsarResources = true;
                return this;
            }

            public FactoryBuilder useTestPulsarResources(MetadataStore metadataStore) {
                useTestPulsarResources = true;
                pulsarResourcesMetadataStore = metadataStore;
                return this;
            }

            public FactoryBuilder managedLedgerClients(BookKeeper bookKeeperClient,
                                                       ManagedLedgerFactory managedLedgerFactory) {
                return managedLedgerClientFactory(
                        Factory.createManagedLedgerClientFactory(bookKeeperClient, managedLedgerFactory));
            }

            public FactoryBuilder brokerServiceFunction(
                    Function<PulsarService, BrokerService> brokerServiceFunction) {
                this.brokerServiceFunction = brokerServiceFunction;
                return this;
            }
        }

        private static class CustomFactoryBuilder extends Factory.FactoryBuilder {

            @Override
            public Factory build() {
                initializeDefaults();
                return super.build();
            }

            private void initializeDefaults() {
                try {
                    if (super.managedLedgerClientFactory == null) {
                        if (super.executor == null) {
                            super.executor = OrderedExecutor.newBuilder().numThreads(1)
                                    .name(TestPulsarService.class.getSimpleName() + "-executor").build();
                        }
                        NonClosableMockBookKeeper mockBookKeeper;
                        if (super.useSpies) {
                            mockBookKeeper =
                                    spyWithClassAndConstructorArgs(NonClosableMockBookKeeper.class, super.executor);
                        } else {
                            mockBookKeeper = new NonClosableMockBookKeeper(super.executor);
                        }
                        cleanupFunction(() -> mockBookKeeper.reallyShutdown());
                        ManagedLedgerFactory mlFactoryMock = mock(ManagedLedgerFactory.class);

                        managedLedgerClientFactory(
                                Factory.createManagedLedgerClientFactory(mockBookKeeper, mlFactoryMock));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (super.config == null) {
                    ServiceConfiguration svcConfig = new ServiceConfiguration();
                    svcConfig.setBrokerShutdownTimeoutMs(0L);
                    svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
                    svcConfig.setClusterName("pulsar-cluster");
                    config(svcConfig);
                }
                if (super.localMetadataStore == null || super.configurationMetadataStore == null) {
                    try {
                        MetadataStoreExtended store = MetadataStoreFactoryImpl.createExtended("memory:local",
                                MetadataStoreConfig.builder().build());
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
                if (super.pulsarResources == null) {
                    if (useTestPulsarResources) {
                        MetadataStore metadataStore = pulsarResourcesMetadataStore;
                        if (metadataStore == null) {
                            metadataStore = super.configurationMetadataStore;
                        }
                        NamespaceResources nsr =
                                spyWithClassAndConstructorArgs(NamespaceResources.class, metadataStore, 30);
                        TopicResources tsr = spyWithClassAndConstructorArgs(TopicResources.class, metadataStore);
                        if (!super.useSpies) {
                            pulsarResources(
                                    new TestPulsarResources(super.localMetadataStore, super.configurationMetadataStore,
                                            tsr, nsr));
                        } else {
                            pulsarResources(
                                    spyWithClassAndConstructorArgs(TestPulsarResources.class, super.localMetadataStore,
                                            super.configurationMetadataStore, tsr, nsr));
                        }
                    } else {
                        if (!super.useSpies) {
                            pulsarResources(
                                    new PulsarResources(super.localMetadataStore, super.configurationMetadataStore));
                        } else {
                            pulsarResources(
                                    spyWithClassAndConstructorArgs(PulsarResources.class, super.localMetadataStore,
                                            super.configurationMetadataStore));
                        }
                    }
                }
                if (super.brokerServiceFunction == null) {
                    if (super.brokerService == null) {
                        if (super.eventLoopGroup == null) {
                            super.eventLoopGroup = new NioEventLoopGroup();
                        }
                        brokerServiceFunction(pulsarService -> {
                            try {
                                if (!super.useSpies) {
                                    return new TestBrokerService(pulsarService, super.eventLoopGroup);
                                } else {
                                    return spyWithClassAndConstructorArgs(TestBrokerService.class, pulsarService,
                                            super.eventLoopGroup);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } else {
                        brokerServiceFunction(pulsarService -> super.brokerService);
                    }
                }
                if (!super.useSpies) {
                    pulsarService(new TestPulsarService(super.config, super.localMetadataStore,
                            super.configurationMetadataStore, super.pulsarResources, super.managedLedgerClientFactory,
                            super.brokerServiceFunction, super.executor, super.compactor));
                } else {
                    pulsarService(spyWithClassAndConstructorArgs(TestPulsarService.class, super.config,
                            super.localMetadataStore, super.configurationMetadataStore, super.pulsarResources,
                            super.managedLedgerClientFactory, super.brokerServiceFunction, super.executor,
                            super.compactor));
                }
                if (super.brokerService == null) {
                    brokerService(super.pulsarService.getBrokerService());
                }
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

    private static class TestPulsarResources extends PulsarResources {

        private final TopicResources topicResources;
        private final NamespaceResources namespaceResources;

        public TestPulsarResources(MetadataStore localMetadataStore, MetadataStore configurationMetadataStore,
                                   TopicResources topicResources, NamespaceResources namespaceResources) {
            super(localMetadataStore, configurationMetadataStore);
            this.topicResources = topicResources;
            this.namespaceResources = namespaceResources;
        }

        @Override
        public TopicResources getTopicResources() {
            return topicResources;
        }

        @Override
        public NamespaceResources getNamespaceResources() {
            return namespaceResources;
        }
    }

    private static class TestBrokerService extends BrokerService {

        TestBrokerService(PulsarService pulsar, EventLoopGroup eventLoopGroup) throws Exception {
            super(pulsar, eventLoopGroup);
        }

        @Override
        protected CompletableFuture<Map<String, String>> fetchTopicPropertiesAsync(TopicName topicName) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
    }

    // Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test
    private static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
            super(executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    private final MetadataStoreExtended localMetadataStore;
    private final MetadataStoreExtended configurationMetadataStore;
    private final PulsarResources pulsarResources;
    private final ManagedLedgerStorage managedLedgerClientFactory;
    private final BrokerService brokerService;

    private final OrderedExecutor executor;

    private final Compactor compactor;

    private final SchemaRegistryService schemaRegistryService;

    private final PulsarClient pulsarClient;

    private final NamespaceService namespaceService;

    protected TestPulsarService(ServiceConfiguration config, MetadataStoreExtended localMetadataStore,
                                MetadataStoreExtended configurationMetadataStore, PulsarResources pulsarResources,
                                ManagedLedgerStorage managedLedgerClientFactory,
                                Function<PulsarService, BrokerService> brokerServiceFunction, OrderedExecutor executor,
                                Compactor compactor) {
        super(config);
        this.localMetadataStore = localMetadataStore;
        this.configurationMetadataStore = configurationMetadataStore;
        this.pulsarResources = pulsarResources;
        this.managedLedgerClientFactory = managedLedgerClientFactory;
        this.brokerService = brokerServiceFunction.apply(this);
        this.executor = executor;
        this.compactor = compactor;
        this.schemaRegistryService = spyWithClassAndConstructorArgs(DefaultSchemaRegistryService.class);
        this.pulsarClient = mock(PulsarClientImpl.class);
        this.namespaceService = mock(NamespaceService.class);
        try {
            startNamespaceService();
        } catch (PulsarServerException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> namespaceService;
    }

    @Override
    public synchronized PulsarClient getClient() throws PulsarServerException {
        return pulsarClient;
    }

    @Override
    public SchemaRegistryService getSchemaRegistryService() {
        return schemaRegistryService;
    }

    @Override
    public MetadataStore createConfigurationMetadataStore(PulsarMetadataEventSynchronizer synchronizer)
            throws MetadataStoreException {
        return configurationMetadataStore;
    }

    @Override
    public MetadataStoreExtended createLocalMetadataStore(PulsarMetadataEventSynchronizer synchronizer)
            throws MetadataStoreException, PulsarServerException {
        return localMetadataStore;
    }

    @Override
    public PulsarResources getPulsarResources() {
        return pulsarResources;
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    @Override
    public Compactor getCompactor() throws PulsarServerException {
        if (compactor != null) {
            return compactor;
        } else {
            return super.getCompactor();
        }
    }

    @Override
    public MetadataStore getConfigurationMetadataStore() {
        return configurationMetadataStore;
    }

    @Override
    public MetadataStoreExtended getLocalMetadataStore() {
        return localMetadataStore;
    }

    @Override
    public ManagedLedgerStorage getManagedLedgerClientFactory() {
        return managedLedgerClientFactory;
    }

    @Override
    public OrderedExecutor getOrderedExecutor() {
        return executor;
    }

    @Override
    protected PulsarResources newPulsarResources() {
        return pulsarResources;
    }

    @Override
    protected ManagedLedgerStorage newManagedLedgerClientFactory() throws Exception {
        return managedLedgerClientFactory;
    }

    @Override
    protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
        return brokerService;
    }
}
