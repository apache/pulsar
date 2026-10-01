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

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.compaction.CompactionServiceFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * This is an internal class used by {@link PulsarTestContext} as the {@link PulsarService} implementation
 * for a "non-startable" PulsarService. Please see {@link PulsarTestContext} for more details.
 */
class NonStartableTestPulsarService extends AbstractTestPulsarService {

    private final NamespaceService namespaceService;

    public NonStartableTestPulsarService(SpyConfig spyConfig, ServiceConfiguration config,
                                         MetadataStoreExtended localMetadataStore,
                                         MetadataStoreExtended configurationMetadataStore,
                                         CompactionServiceFactory compactionServiceFactory,
                                         BrokerInterceptor brokerInterceptor,
                                         BookKeeperClientFactory bookKeeperClientFactory,
                                         PulsarResources pulsarResources,
                                         ManagedLedgerStorage managedLedgerClientFactory,
                                         Function<BrokerService, BrokerService> brokerServiceCustomizer) {
        super(spyConfig, config, localMetadataStore, configurationMetadataStore, compactionServiceFactory,
                brokerInterceptor, bookKeeperClientFactory, null);
        setPulsarResources(pulsarResources);
        setManagedLedgerStorage(managedLedgerClientFactory);
        try {
            setBrokerService(brokerServiceCustomizer.apply(
                    spyConfig.getBrokerService().spy(TestBrokerService.class, this, getIoEventLoopGroup())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setSchemaRegistryService(spyWithClassAndConstructorArgs(DefaultSchemaRegistryService.class));
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(mockClient.getCnxPool()).thenReturn(connectionPool);
        setClient(mockClient);
        this.namespaceService = mock(NamespaceService.class);
        try {
            startNamespaceService();
        } catch (PulsarServerException e) {
            throw new RuntimeException(e);
        }
        if (config.isTransactionCoordinatorEnabled()) {
            try {
                setTransactionBufferProvider(TransactionBufferProvider
                        .newProvider(config.getTransactionBufferProviderClassName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                setTransactionPendingAckStoreProvider(TransactionPendingAckStoreProvider
                        .newProvider(config.getTransactionPendingAckStoreProviderClassName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void start() throws PulsarServerException {
        throw new UnsupportedOperationException("Cannot start a non-startable TestPulsarService");
    }

    @Override
    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> namespaceService;
    }

    @Override
    public PulsarClientImpl createClientImpl(ClientConfigurationData clientConf,
                                             Consumer<PulsarClientImpl.PulsarClientImplBuilder> customizer)
            throws PulsarClientException {
        try {
            return (PulsarClientImpl) getClient();
        } catch (PulsarServerException e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
        return getBrokerService();
    }

    static class TestBrokerService extends BrokerService {

        TestBrokerService(PulsarService pulsar, EventLoopGroup eventLoopGroup) throws Exception {
            super(pulsar, eventLoopGroup);
        }

        @Override
        protected CompletableFuture<Map<String, String>> fetchTopicPropertiesAsync(TopicName topicName) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
    }

    static class TestPulsarResources extends PulsarResources {

        private final TopicResources topicResources;
        private final NamespaceResources namespaceResources;

        public TestPulsarResources(MetadataStoreExtended localMetadataStore, MetadataStore configurationMetadataStore,
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
}
