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
import static org.mockito.Mockito.doThrow;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedLedgerClientFactoryTest {

    @Test
    public void testStartupWaitsForDefaultBookKeeperClient() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        BookKeeper defaultClient = mock(BookKeeper.class);
        CompletableFuture<BookKeeper> clientFuture = new CompletableFuture<>();
        CompletableFuture<Void> clientRequested = new CompletableFuture<>();
        BookKeeperClientFactory bookkeeperProvider = mock(BookKeeperClientFactory.class);
        when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any())).thenAnswer(invocation -> {
            clientRequested.complete(null);
            return clientFuture;
        });
        ExecutorService startupExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("test-managed-ledger-startup"));
        try (ManagedLedgerClientFactory factory = spy(new ManagedLedgerClientFactory())) {
            doReturn(mock(ManagedLedgerFactoryImpl.class)).when(factory)
                    .createManagedLedgerFactory(any(), any(), any(), any(), any());
            Future<?> startup = startupExecutor.submit(() -> {
                factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                        mock(EventLoopGroup.class), OpenTelemetry.noop());
                return null;
            });
            try {
                clientRequested.get(5, TimeUnit.SECONDS);
                assertThat(startup.isDone()).isFalse();
                verify(factory, never()).createManagedLedgerFactory(any(), any(), any(), any(), any());

                clientFuture.complete(defaultClient);

                startup.get(5, TimeUnit.SECONDS);
                assertThat(factory.getDefaultStorageClass()).isNotNull();
                verify(factory).createManagedLedgerFactory(any(), any(), any(), any(), any());
            } finally {
                clientFuture.complete(defaultClient);
                startup.cancel(true);
            }
        } finally {
            startupExecutor.shutdownNow();
        }
        verify(defaultClient).close();
    }

    @Test
    public void testDefaultBookKeeperClientFailureStopsStatsProvider() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        StatsProvider statsProvider = mock(StatsProvider.class);
        BookKeeperClientFactory bookkeeperProvider = mock(BookKeeperClientFactory.class);
        RuntimeException loadFailure = new RuntimeException("rack metadata unavailable");
        RuntimeException stopFailure = new RuntimeException("could not stop stats provider");
        when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any()))
                .thenReturn(CompletableFuture.failedFuture(loadFailure));
        doThrow(stopFailure).when(statsProvider).stop();

        ManagedLedgerClientFactory factory = new ManagedLedgerClientFactory(statsProvider);
        assertThatThrownBy(() -> factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                mock(EventLoopGroup.class), OpenTelemetry.noop()))
                .isInstanceOfSatisfying(ExecutionException.class, error -> {
                    assertThat(error.getCause()).isSameAs(loadFailure);
                    assertThat(error.getSuppressed()).containsExactly(stopFailure);
                });
        verify(statsProvider).start(any());
        verify(statsProvider).stop();
    }

    @Test
    public void testStartupInterruptionClosesLateDefaultBookKeeperClient() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        StatsProvider statsProvider = mock(StatsProvider.class);
        BookKeeper defaultClient = mock(BookKeeper.class);
        CompletableFuture<BookKeeper> clientFuture = new CompletableFuture<>();
        CompletableFuture<Thread> clientRequested = new CompletableFuture<>();
        CompletableFuture<Thread> closingThread = new CompletableFuture<>();
        BookKeeperClientFactory bookkeeperProvider = mock(BookKeeperClientFactory.class);
        when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any())).thenAnswer(invocation -> {
            clientRequested.complete(Thread.currentThread());
            return clientFuture;
        });
        doAnswer(invocation -> {
            closingThread.complete(Thread.currentThread());
            return null;
        }).when(defaultClient).close();

        ExecutorService startupExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("test-managed-ledger-interrupted-startup"));
        ManagedLedgerClientFactory factory = new ManagedLedgerClientFactory(statsProvider);
        try {
            Future<?> startup = startupExecutor.submit(() -> {
                factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                        mock(EventLoopGroup.class), OpenTelemetry.noop());
                return null;
            });
            clientRequested.get(5, TimeUnit.SECONDS).interrupt();

            assertThatThrownBy(() -> startup.get(5, TimeUnit.SECONDS))
                    .cause().isInstanceOf(InterruptedException.class);
            verify(statsProvider).stop();
            assertThat(clientFuture).isNotDone();

            clientFuture.complete(defaultClient);
            assertThat(closingThread.get(5, TimeUnit.SECONDS)).isNotSameAs(Thread.currentThread());
            verify(defaultClient).close();
        } finally {
            startupExecutor.shutdownNow();
            clientFuture.complete(defaultClient);
        }
    }

    @Test
    public void testManagedLedgerFactoryInitializationFailurePreservesCleanupFailures() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        StatsProvider statsProvider = mock(StatsProvider.class);
        BookKeeper defaultClient = mock(BookKeeper.class);
        BookKeeperClientFactory bookkeeperProvider = mock(BookKeeperClientFactory.class);
        RuntimeException initializationFailure = new RuntimeException("managed ledger initialization failed");
        RuntimeException stopFailure = new RuntimeException("could not stop stats provider");
        RuntimeException closeFailure = new RuntimeException("could not close default client");
        when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(defaultClient));
        doThrow(stopFailure).when(statsProvider).stop();
        doThrow(closeFailure).when(defaultClient).close();
        ManagedLedgerClientFactory factory = spy(new ManagedLedgerClientFactory(statsProvider));
        doThrow(initializationFailure).when(factory).createManagedLedgerFactory(any(), any(), any(), any(), any());

        assertThatThrownBy(() -> factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                mock(EventLoopGroup.class), OpenTelemetry.noop()))
                .isSameAs(initializationFailure);
        assertThat(initializationFailure.getSuppressed()).containsExactly(stopFailure, closeFailure);
        verify(statsProvider).stop();
        verify(defaultClient).close();
    }

    @Test
    public void testFailedLazyClientCanBeCreatedOnNextRequest() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        BookKeeper defaultClient = mock(BookKeeper.class);
        BookKeeper policyClient = mock(BookKeeper.class);
        RuntimeException loadFailure = new RuntimeException("rack metadata unavailable");
        CompletableFuture<BookKeeper> firstClient = new CompletableFuture<>();
        BookKeeperClientFactory bookkeeperProvider = mock(BookKeeperClientFactory.class);
        when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(defaultClient));
        when(bookkeeperProvider.create(any(), any(), any(), any(), anyMap(), any()))
                .thenReturn(firstClient, CompletableFuture.completedFuture(policyClient));

        try (ManagedLedgerClientFactory factory = spy(new ManagedLedgerClientFactory())) {
            doReturn(mock(ManagedLedgerFactoryImpl.class)).when(factory)
                    .createManagedLedgerFactory(any(), any(), any(), any(), any());
            factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                    mock(EventLoopGroup.class), OpenTelemetry.noop());
            ArgumentCaptor<BookkeeperFactoryForCustomEnsemblePlacementPolicy> policyFactory =
                    ArgumentCaptor.forClass(BookkeeperFactoryForCustomEnsemblePlacementPolicy.class);
            verify(factory).createManagedLedgerFactory(any(), any(), policyFactory.capture(), any(), any());
            EnsemblePlacementPolicyConfig config = new EnsemblePlacementPolicyConfig(
                    IsolatedBookieEnsemblePlacementPolicy.class,
                    Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1"));

            CompletableFuture<BookKeeper> failedClient = policyFactory.getValue().get(config);
            assertThat(failedClient).isNotDone();
            firstClient.completeExceptionally(loadFailure);
            assertThatThrownBy(() -> failedClient.get(5, TimeUnit.SECONDS)).cause().isSameAs(loadFailure);

            assertThat(policyFactory.getValue().get(config).get(5, TimeUnit.SECONDS)).isSameAs(policyClient);
            verify(bookkeeperProvider, times(2)).create(any(), any(), any(), any(), anyMap(), any());
        }
    }

    @DataProvider
    public Object[][] cacheExtensionSettings() {
        return new Object[][] {{null, 5}, {false, 2}, {true, 0}, {true, 5}};
    }

    @Test(dataProvider = "cacheExtensionSettings")
    public void testCacheExtensionSettingsAtStartup(Boolean extendRecentlyAccessed, int maxExtensions)
            throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        if (extendRecentlyAccessed == null) {
            assertThat(conf.isManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed()).isFalse();
        } else {
            conf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(extendRecentlyAccessed);
        }
        conf.setManagedLedgerCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(maxExtensions);
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        BookKeeperClientFactory bookkeeperProvider = mock(BookKeeperClientFactory.class);
        when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(BookKeeper.class)));
        try (ManagedLedgerClientFactory factory = spy(new ManagedLedgerClientFactory())) {
            doReturn(mock(ManagedLedgerFactoryImpl.class)).when(factory)
                    .createManagedLedgerFactory(any(), any(), any(), any(), any());
            factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                    mock(EventLoopGroup.class), OpenTelemetry.noop());
            ArgumentCaptor<ManagedLedgerFactoryConfig> config =
                    ArgumentCaptor.forClass(ManagedLedgerFactoryConfig.class);
            verify(factory).createManagedLedgerFactory(any(), any(), any(), config.capture(), any());
            assertThat(config.getValue().isCacheEvictionExtendTTLOfRecentlyAccessed())
                    .isEqualTo(Boolean.TRUE.equals(extendRecentlyAccessed));
            assertThat(config.getValue().getCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes())
                    .isEqualTo(maxExtensions);
        }
    }
}
