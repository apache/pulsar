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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFactoryClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedLedgerClientFactoryTest {

    @DataProvider
    public Object[][] isolatedClients() {
        return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
    }

    @Test(dataProvider = "isolatedClients")
    public void testPlacementReadinessKeepsBuiltClient(boolean customClient, boolean failFirstRead) throws Exception {
        var conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        var bookkeeperProvider = mock(BookKeeperClientFactory.class);
        var client = mock(BookKeeper.class);
        var clientContext = mock(ClientContext.class);
        var policy = mock(IsolatedBookieEnsemblePlacementPolicy.class);
        when(client.getClientCtx()).thenReturn(clientContext);
        when(clientContext.getPlacementPolicy()).thenReturn(policy);

        var policyConfig = customClient ? new EnsemblePlacementPolicyConfig(IsolatedBookieEnsemblePlacementPolicy.class,
                Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1"))
                : new EnsemblePlacementPolicyConfig(null, null);
        var firstLoad = new CompletableFuture<Void>();
        var retryLoad = new CompletableFuture<Void>();
        when(policy.awaitRackConfiguration(any(), anyLong())).thenReturn(firstLoad).thenReturn(retryLoad);
        when(bookkeeperProvider.create(any(), any(), any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(client));
        if (customClient) {
            when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any()))
                    .thenReturn(CompletableFuture.completedFuture(mock(BookKeeper.class)));
        }

        try (var factory = spy(new ManagedLedgerClientFactory())) {
            doReturn(mock(ManagedLedgerFactoryImpl.class)).when(factory)
                    .createManagedLedgerFactory(any(), any(), any(), any(), any());
            factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                    mock(EventLoopGroup.class), OpenTelemetry.noop());
            var captor = ArgumentCaptor.forClass(BookkeeperFactoryForCustomEnsemblePlacementPolicy.class);
            verify(factory).createManagedLedgerFactory(any(), any(), captor.capture(), any(), any());
            var clients = captor.getValue();

            var firstPlacement = clients.getForPlacement(policyConfig);
            assertThat(firstPlacement).isNotDone();
            assertThat(clients.get(policyConfig).get(5, TimeUnit.SECONDS)).isSameAs(client);
            CompletableFuture<BookKeeper> readyPlacement;
            if (failFirstRead) {
                var failure = new MetadataStoreException("rack configuration read failed");
                firstLoad.completeExceptionally(failure);
                assertThatThrownBy(() -> firstPlacement.get(5, TimeUnit.SECONDS)).cause().isSameAs(failure);
                readyPlacement = clients.getForPlacement(policyConfig);
                assertThat(readyPlacement).isNotDone();
                retryLoad.complete(null);
            } else {
                readyPlacement = firstPlacement;
                firstLoad.complete(null);
            }
            assertThat(readyPlacement.get(5, TimeUnit.SECONDS)).isSameAs(client);
            verify(policy, times(failFirstRead ? 2 : 1))
                    .awaitRackConfiguration(policyConfig,
                            TimeUnit.SECONDS.toMillis(conf.getMetadataStoreOperationTimeoutSeconds()));
            verify(bookkeeperProvider, times(customClient ? 2 : 1)).create(any(), any(), any(), any(), any(), any());
            verify(client, never()).close();
        }
    }

    @Test(timeOut = 30_000)
    public void testShutdownClosesLatePolicyClientWithoutCancellingItsBuild() throws Exception {
        var conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        var provider = mock(BookKeeperClientFactory.class);
        var defaultClient = mock(BookKeeper.class);
        var lateClient = mock(BookKeeper.class);
        var builderInvoked = new CompletableFuture<Void>();
        var build = new CompletableFuture<BookKeeper>();
        when(provider.create(any(), any(), any(), any(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(defaultClient));
        when(provider.create(any(), any(), any(), any(), anyMap(), any())).thenAnswer(invocation -> {
            builderInvoked.complete(null);
            return build;
        });
        var policyConfig = new EnsemblePlacementPolicyConfig(IsolatedBookieEnsemblePlacementPolicy.class, Map.of());

        try (var factory = spy(new ManagedLedgerClientFactory())) {
            var ml = mock(ManagedLedgerFactoryImpl.class);
            doReturn(ml).when(factory).createManagedLedgerFactory(any(), any(), any(), any(), any());
            factory.initialize(conf, mock(MetadataStoreExtended.class), provider,
                    mock(EventLoopGroup.class), OpenTelemetry.noop());
            var captor = ArgumentCaptor.forClass(BookkeeperFactoryForCustomEnsemblePlacementPolicy.class);
            verify(factory).createManagedLedgerFactory(any(), any(), captor.capture(), any(), any());
            var clients = captor.getValue();

            var waiting = clients.get(policyConfig);
            var cancelled = clients.get(policyConfig);
            builderInvoked.get(5, TimeUnit.SECONDS);
            assertThat(cancelled.cancel(false)).isTrue();
            assertThat(waiting).isNotDone();
            assertThat(build).isNotDone();

            factory.close();
            assertThatThrownBy(() -> waiting.get(5, TimeUnit.SECONDS))
                    .cause().isInstanceOf(ManagedLedgerFactoryClosedException.class);
            assertThatThrownBy(() -> clients.get(policyConfig).get(5, TimeUnit.SECONDS))
                    .cause().isInstanceOf(ManagedLedgerFactoryClosedException.class);
            assertThat(build.complete(lateClient)).isTrue();
            verify(lateClient, timeout(5_000)).close();
            factory.close();
            verify(ml).shutdown();
            verify(defaultClient).close();
            verify(provider).create(any(), any(), any(), any(), anyMap(), any());
        }
    }

    @Test(timeOut = 30_000)
    public void testInterruptedStartupClosesLateDefaultClient() throws Exception {
        var conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        var provider = mock(BookKeeperClientFactory.class);
        var stats = mock(StatsProvider.class);
        when(stats.getStatsLogger(any())).thenReturn(NullStatsLogger.INSTANCE);
        var builderInvoked = new CompletableFuture<Void>();
        var build = new CompletableFuture<BookKeeper>();
        var lateClient = mock(BookKeeper.class);
        when(provider.create(any(), any(), any(), any(), any(), any())).thenAnswer(invocation -> {
            builderInvoked.complete(null);
            return build;
        });
        try (var factory = new ManagedLedgerClientFactory(stats)) {
            var startupFailure = new CompletableFuture<Exception>();
            var interruptRestored = new CompletableFuture<Boolean>();
            Thread startup = Thread.ofPlatform().name("managed-ledger-startup").start(() -> {
                try {
                    factory.initialize(conf, mock(MetadataStoreExtended.class), provider,
                            mock(EventLoopGroup.class), OpenTelemetry.noop());
                    startupFailure.completeExceptionally(new AssertionError("Startup should be interrupted"));
                } catch (Exception error) {
                    interruptRestored.complete(Thread.currentThread().isInterrupted());
                    startupFailure.complete(error);
                }
            });
            try {
                builderInvoked.get(5, TimeUnit.SECONDS);
                startup.interrupt();
                assertThat(startupFailure.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
                assertThat(interruptRestored.get(5, TimeUnit.SECONDS)).isTrue();
            } finally {
                startup.interrupt();
                startup.join(TimeUnit.SECONDS.toMillis(5));
                build.complete(lateClient);
            }
            verify(stats).stop();
            verify(lateClient, timeout(5_000)).close();
        }
    }

    @Test
    public void testStartupPreservesBuildFailureWhenStatsStopFails() throws Exception {
        var conf = new ServiceConfiguration();
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        var provider = mock(BookKeeperClientFactory.class);
        var startupError = new IllegalStateException("client start");
        var stopError = new IllegalStateException("stats stop");
        var stats = mock(StatsProvider.class);
        when(stats.getStatsLogger(any())).thenReturn(NullStatsLogger.INSTANCE);
        doThrow(stopError).when(stats).stop();
        when(provider.create(any(), any(), any(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(startupError));

        try (var factory = new ManagedLedgerClientFactory(stats)) {
            assertThatThrownBy(() -> factory.initialize(conf, mock(MetadataStoreExtended.class), provider,
                    mock(EventLoopGroup.class), OpenTelemetry.noop()))
                    .hasCause(startupError)
                    .satisfies(error -> assertThat(error.getSuppressed()).singleElement().satisfies(suppressed ->
                            assertThat(suppressed).isInstanceOf(IOException.class).hasCause(stopError)));
            verify(stats).stop();
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
