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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFactoryClosedException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedLedgerClientFactoryShutdownTest {
    private static final EnsemblePlacementPolicyConfig POLICY = new EnsemblePlacementPolicyConfig(
            IsolatedBookieEnsemblePlacementPolicy.class,
            Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1"));

    @DataProvider
    public Object[][] pendingClientStages() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "pendingClientStages")
    public void testCloseWhileClientIsPending(boolean blockBuild) throws Exception {
        CompletableFuture<Void> allowBuild = new CompletableFuture<>();
        CompletableFuture<Void> buildStarted = new CompletableFuture<>();
        CompletableFuture<Void> readiness = new CompletableFuture<>();
        CompletableFuture<Void> readinessRequested = new CompletableFuture<>();
        CompletableFuture<Void> allowClose = new CompletableFuture<>();
        CompletableFuture<Thread> closingThread = new CompletableFuture<>();
        CompletableFuture<Void> clientClosed = new CompletableFuture<>();
        CompletableFuture<CompletableFuture<BookKeeper>> clientCreation = new CompletableFuture<>();
        if (blockBuild) {
            readiness.complete(null);
        } else {
            allowBuild.complete(null);
        }

        BookKeeper defaultClient = mock(BookKeeper.class);
        ClientContext defaultContext = mock(ClientContext.class);
        when(defaultClient.getClientCtx()).thenReturn(defaultContext);
        when(defaultContext.getPlacementPolicy()).thenReturn(mock(EnsemblePlacementPolicy.class));
        BookKeeper policyClient = mock(BookKeeper.class);
        ClientContext context = mock(ClientContext.class);
        when(policyClient.getClientCtx()).thenReturn(context);
        IsolatedBookieEnsemblePlacementPolicy policy = mock(IsolatedBookieEnsemblePlacementPolicy.class);
        when(context.getPlacementPolicy()).thenReturn(policy);
        when(policy.getInitializationFuture()).thenAnswer(invocation -> {
            readinessRequested.complete(null);
            return readiness;
        });
        doAnswer(invocation -> {
            closingThread.complete(Thread.currentThread());
            allowClose.get(5, TimeUnit.SECONDS);
            clientClosed.complete(null);
            return null;
        }).when(policyClient).close();

        BookKeeper.Builder defaultBuilder = mock(BookKeeper.Builder.class);
        when(defaultBuilder.build()).thenReturn(defaultClient);
        BookKeeper.Builder policyBuilder = mock(BookKeeper.Builder.class);
        when(policyBuilder.build()).thenAnswer(invocation -> {
            buildStarted.complete(null);
            allowBuild.get(30, TimeUnit.SECONDS);
            return policyClient;
        });
        BookKeeperClientFactoryImpl provider = spy(new BookKeeperClientFactoryImpl());
        doReturn(defaultBuilder, policyBuilder).when(provider).getBookKeeperBuilder(any(), any(), any(), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<BookKeeper> result = (CompletableFuture<BookKeeper>) invocation.callRealMethod();
            clientCreation.complete(result);
            return result;
        }).when(provider).create(any(), any(), any(), any(), anyMap(), any());

        ExecutorService metadataExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("test-client-readiness-completion"));
        try (MetadataStoreExtended store = newStore("pending-client-" + blockBuild)) {
            ManagedLedgerClientFactory factory = newFactory(store, provider);
            boolean closed = false;
            try {
                BookkeeperFactoryForCustomEnsemblePlacementPolicy policyFactory = policyFactory(factory);
                CompletableFuture<ManagedLedger> open = new CompletableFuture<>();
                open(factory, open);
                CompletableFuture<BookKeeper> creation = clientCreation.get(5, TimeUnit.SECONDS);
                buildStarted.get(5, TimeUnit.SECONDS);
                if (!blockBuild) {
                    readinessRequested.get(5, TimeUnit.SECONDS);
                }

                factory.close();
                closed = true;
                assertFactoryClosed(open);
                assertThat(creation).isNotDone();
                verify(defaultClient).close();
                verify(policyClient, never()).close();
                assertFactoryClosed(policyFactory.get(POLICY));
                assertFactoryClosed(policyFactory.get(null));

                Thread completionThread = metadataExecutor.submit(() -> {
                    allowBuild.complete(null);
                    readiness.complete(null);
                    return Thread.currentThread();
                }).get(5, TimeUnit.SECONDS);
                assertThat(creation.get(5, TimeUnit.SECONDS)).isSameAs(policyClient);
                assertThat(closingThread.get(5, TimeUnit.SECONDS)).isNotSameAs(completionThread);
                allowClose.complete(null);
                clientClosed.get(5, TimeUnit.SECONDS);
                factory.close();
                verify(defaultClient).close();
                verify(policyClient).close();
                verify(policyClient, never()).newCreateLedgerOp();
                verify(provider, times(2)).getBookKeeperBuilder(any(), any(), any(), any());
            } finally {
                allowBuild.complete(null);
                readiness.complete(null);
                allowClose.complete(null);
                if (!closed) {
                    factory.close();
                }
            }
        } finally {
            metadataExecutor.shutdownNow();
        }
    }

    @Test
    public void testCloseWhileProviderCallIsPending() throws Exception {
        CompletableFuture<Void> providerStarted = new CompletableFuture<>();
        CompletableFuture<Void> allowProvider = new CompletableFuture<>();
        CompletableFuture<Void> clientClosed = new CompletableFuture<>();
        BookKeeper defaultClient = mock(BookKeeper.class);
        BookKeeper policyClient = mock(BookKeeper.class);
        doAnswer(invocation -> {
            clientClosed.complete(null);
            return null;
        }).when(policyClient).close();
        BookKeeperClientFactory provider = provider(defaultClient, new CompletableFuture<>());
        when(provider.create(any(), any(), any(), any(), anyMap(), any())).thenAnswer(invocation -> {
            providerStarted.complete(null);
            allowProvider.get(5, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(policyClient);
        });
        ExecutorService executor = Executors.newFixedThreadPool(2, new DefaultThreadFactory("test-client-close-race"));
        try (MetadataStoreExtended store = newStore("provider-close-race")) {
            ManagedLedgerClientFactory factory = newFactory(store, provider);
            boolean closed = false;
            try {
                CompletableFuture<ManagedLedger> open = new CompletableFuture<>();
                Future<?> request = executor.submit(() -> open(factory, open));
                providerStarted.get(5, TimeUnit.SECONDS);
                executor.submit(() -> {
                    factory.close();
                    return null;
                }).get(5, TimeUnit.SECONDS);
                closed = true;
                verify(defaultClient).close();
                verify(policyClient, never()).close();

                allowProvider.complete(null);
                request.get(5, TimeUnit.SECONDS);
                assertFactoryClosed(open);
                clientClosed.get(5, TimeUnit.SECONDS);
                verify(policyClient).close();
                verify(policyClient, never()).newCreateLedgerOp();
            } finally {
                allowProvider.complete(null);
                if (!closed) {
                    factory.close();
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCloseWaitsForPublishedClientAndClosesItOnce() throws Exception {
        BookKeeper defaultClient = mock(BookKeeper.class);
        BookKeeper policyClient = mock(BookKeeper.class);
        CompletableFuture<Void> closeStarted = new CompletableFuture<>();
        CompletableFuture<Void> allowClose = new CompletableFuture<>();
        doAnswer(invocation -> {
            closeStarted.complete(null);
            allowClose.get(5, TimeUnit.SECONDS);
            return null;
        }).when(policyClient).close();
        BookKeeperClientFactory provider = provider(defaultClient, CompletableFuture.completedFuture(policyClient));
        ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("test-client-close"));
        try (MetadataStoreExtended store = newStore("published-client-close")) {
            ManagedLedgerClientFactory factory = newFactory(store, provider);
            boolean closed = false;
            try {
                BookkeeperFactoryForCustomEnsemblePlacementPolicy policyFactory = policyFactory(factory);
                assertThat(policyFactory.get(POLICY).get(5, TimeUnit.SECONDS)).isSameAs(policyClient);
                Future<?> close = executor.submit(() -> {
                    factory.close();
                    return null;
                });
                closeStarted.get(5, TimeUnit.SECONDS);
                assertThat(close.isDone()).isFalse();
                allowClose.complete(null);
                close.get(5, TimeUnit.SECONDS);
                closed = true;
                factory.close();
                verify(defaultClient).close();
                verify(policyClient).close();
                assertFactoryClosed(policyFactory.get(POLICY));
            } finally {
                allowClose.complete(null);
                if (!closed) {
                    factory.close();
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCancelledClientCreationClosesLateClient() throws Exception {
        BookKeeper defaultClient = mock(BookKeeper.class);
        BookKeeper policyClient = mock(BookKeeper.class);
        CompletableFuture<BookKeeper> creation = new CompletableFuture<>();
        CompletableFuture<Void> clientClosed = new CompletableFuture<>();
        doAnswer(invocation -> {
            clientClosed.complete(null);
            return null;
        }).when(policyClient).close();
        try (MetadataStoreExtended store = newStore("cancelled-client")) {
            ManagedLedgerClientFactory factory = newFactory(store, provider(defaultClient, creation));
            try {
                CompletableFuture<BookKeeper> result = policyFactory(factory).get(POLICY);
                assertThat(result.cancel(false)).isTrue();
                assertThat(creation).isNotDone();
                factory.close();
                creation.complete(policyClient);
                clientClosed.get(5, TimeUnit.SECONDS);
                verify(policyClient).close();
                verify(defaultClient).close();
            } finally {
                creation.complete(policyClient);
                factory.close();
            }
        }
    }

    private MetadataStoreExtended newStore(String name) throws Exception {
        return MetadataStoreExtended.create("memory:" + name, MetadataStoreConfig.builder().build());
    }

    private ManagedLedgerClientFactory newFactory(MetadataStoreExtended store, BookKeeperClientFactory provider)
            throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setMetadataStoreUrl("memory:client-shutdown");
        conf.setMetadataStoreSessionTimeoutMillis(60_000);
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        conf.setManagedLedgerCacheSizeMB(0);
        ManagedLedgerClientFactory factory = spy(new ManagedLedgerClientFactory());
        factory.initialize(conf, store, provider, mock(EventLoopGroup.class), OpenTelemetry.noop());
        return factory;
    }

    private BookKeeperClientFactory provider(BookKeeper defaultClient, CompletableFuture<BookKeeper> policyClient) {
        BookKeeperClientFactory provider = mock(BookKeeperClientFactory.class);
        when(provider.create(any(), any(), any(), any(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(defaultClient));
        when(provider.create(any(), any(), any(), any(), anyMap(), any())).thenReturn(policyClient);
        return provider;
    }

    private BookkeeperFactoryForCustomEnsemblePlacementPolicy policyFactory(ManagedLedgerClientFactory factory)
            throws Exception {
        ArgumentCaptor<BookkeeperFactoryForCustomEnsemblePlacementPolicy> captor =
                ArgumentCaptor.forClass(BookkeeperFactoryForCustomEnsemblePlacementPolicy.class);
        verify(factory).createManagedLedgerFactory(any(), any(), captor.capture(), any(), any());
        return captor.getValue();
    }

    private void open(ManagedLedgerClientFactory factory, CompletableFuture<ManagedLedger> result) {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setBookKeeperEnsemblePlacementPolicyClassName(IsolatedBookieEnsemblePlacementPolicy.class);
        config.setBookKeeperEnsemblePlacementPolicyProperties(POLICY.getProperties());
        factory.getDefaultStorageClass().getManagedLedgerFactory().asyncOpen("pending-client", config,
                new OpenLedgerCallback() {
                    @Override
                    public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                        result.complete(ledger);
                    }

                    @Override
                    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        result.completeExceptionally(exception);
                    }
                }, null, null);
    }

    private void assertFactoryClosed(CompletableFuture<?> future) {
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(ManagedLedgerFactoryClosedException.class);
    }
}
