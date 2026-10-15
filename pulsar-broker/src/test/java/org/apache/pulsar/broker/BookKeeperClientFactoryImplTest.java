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

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_VALIDATION;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_MINIMUM_REGIONS_FOR_DURABILITY;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_REGIONS_TO_WRITE;
import static org.apache.bookkeeper.conf.AbstractConfiguration.ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM;
import static org.apache.bookkeeper.conf.AbstractConfiguration.MIN_NUM_RACKS_PER_WRITE_QUORUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.net.CachedDNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test {@link BookKeeperClientFactoryImpl}.
 */
@Test(groups = "broker")
public class BookKeeperClientFactoryImplTest {

    @Test
    public void testClientWaitsForInitialRackConfiguration() throws Exception {
        ClientInitialization initialization = newClientInitialization(mock(BookKeeper.class));
        CompletableFuture<BookKeeper> clientFuture = createIsolatedClient(initialization, 10_000);

        initialization.readinessRequested().get(5, TimeUnit.SECONDS);
        assertThat(clientFuture).isNotDone();
        verify(initialization.client(), never()).close();

        initialization.readiness().complete(null);

        assertThat(clientFuture.get(5, TimeUnit.SECONDS)).isSameAs(initialization.client());
        verify(initialization.client(), never()).close();
    }

    @Test
    public void testClientDoesNotWaitWhenPolicyInitializationIsAlreadyComplete() throws Exception {
        ClientInitialization initialization = newClientInitialization(mock(BookKeeper.class));
        initialization.readiness().complete(null);

        assertThat(createIsolatedClient(initialization, 10_000).get(5, TimeUnit.SECONDS))
                .isSameAs(initialization.client());
        verify(initialization.client(), never()).close();
    }

    @Test
    public void testCancelledClientCreationClosesClientAfterBuildCompletes() throws Exception {
        ClientInitialization initialization = newClientInitialization(mock(BookKeeper.class));
        initialization.readiness().complete(null);
        CompletableFuture<Void> buildStarted = new CompletableFuture<>();
        CompletableFuture<BookKeeper> buildResult = new CompletableFuture<>();
        CompletableFuture<Void> clientClosed = new CompletableFuture<>();
        BookKeeper.Builder builder = mock(BookKeeper.Builder.class);
        when(builder.build()).thenAnswer(invocation -> {
            buildStarted.complete(null);
            return buildResult.get(5, TimeUnit.SECONDS);
        });
        doReturn(builder).when(initialization.factory()).getBookKeeperBuilder(any(), any(), any(), any());
        doAnswer(invocation -> {
            clientClosed.complete(null);
            return null;
        }).when(initialization.client()).close();

        CompletableFuture<BookKeeper> clientFuture = createIsolatedClient(initialization, 10_000);
        try {
            buildStarted.get(5, TimeUnit.SECONDS);
            assertThat(clientFuture.cancel(false)).isTrue();
        } finally {
            buildResult.complete(initialization.client());
        }

        clientClosed.get(5, TimeUnit.SECONDS);
        assertThat(clientFuture).isCancelled();
        verify(initialization.client()).close();
    }

    @Test
    public void testCancelledClientCreationDoesNotCancelPolicyInitialization() throws Exception {
        ClientInitialization initialization = newClientInitialization(mock(BookKeeper.class));
        CompletableFuture<Void> clientClosed = new CompletableFuture<>();
        doAnswer(invocation -> {
            clientClosed.complete(null);
            return null;
        }).when(initialization.client()).close();

        CompletableFuture<BookKeeper> clientFuture = createIsolatedClient(initialization, 10_000);
        initialization.readinessRequested().get(5, TimeUnit.SECONDS);
        assertThat(clientFuture.cancel(false)).isTrue();
        assertThat(initialization.readiness()).isNotDone();

        initialization.readiness().complete(null);

        clientClosed.get(5, TimeUnit.SECONDS);
        assertThat(clientFuture).isCancelled();
        verify(initialization.client()).close();
    }

    @Test
    public void testOtherPlacementPoliciesDoNotRequireRackInitialization() throws Exception {
        BookKeeper client = mock(BookKeeper.class);
        ClientContext clientContext = mock(ClientContext.class);
        when(client.getClientCtx()).thenReturn(clientContext);
        when(clientContext.getPlacementPolicy()).thenReturn(mock(EnsemblePlacementPolicy.class));
        BookKeeperClientFactoryImpl factory = factoryReturning(client);
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setMetadataStoreUrl("memory:client-readiness");

        assertThat(factory.create(conf, mock(MetadataStoreExtended.class), mock(EventLoopGroup.class),
                Optional.empty(), null).get(5, TimeUnit.SECONDS)).isSameAs(client);
        verify(client, never()).close();
    }

    @DataProvider
    public Object[][] rackInitializationFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "rackInitializationFailures")
    public void testRackInitializationFailureClosesClientAsynchronously(boolean timeout) throws Exception {
        ClientInitialization initialization = newClientInitialization(mock(BookKeeper.class));
        CompletableFuture<Thread> closingThread = new CompletableFuture<>();
        CountDownLatch allowClose = new CountDownLatch(1);
        doAnswer(invocation -> {
            closingThread.complete(Thread.currentThread());
            assertThat(allowClose.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(initialization.client()).close();

        ExecutorService metadataExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("test-rack-metadata"));
        RuntimeException loadFailure = new RuntimeException("rack metadata unavailable");
        CompletableFuture<BookKeeper> clientFuture = createIsolatedClient(initialization, timeout ? 100 : 10_000);
        try {
            initialization.readinessRequested().get(5, TimeUnit.SECONDS);
            Thread completionThread;
            if (timeout) {
                completionThread = Thread.currentThread();
            } else {
                completionThread = metadataExecutor.submit(() -> {
                    initialization.readiness().completeExceptionally(loadFailure);
                    return Thread.currentThread();
                }).get(5, TimeUnit.SECONDS);
            }
            Thread thread = closingThread.get(5, TimeUnit.SECONDS);
            assertThat(thread).isNotSameAs(completionThread);
            assertThat(thread.getName()).isNotEqualTo("CompletableFutureDelayScheduler");
        } finally {
            allowClose.countDown();
            metadataExecutor.shutdownNow();
        }

        if (timeout) {
            assertThatThrownBy(() -> clientFuture.get(5, TimeUnit.SECONDS))
                    .cause().isInstanceOf(TimeoutException.class);
            assertThat(initialization.readiness()).isNotDone();
            initialization.readiness().complete(null);
            assertThat(initialization.readiness()).isCompleted();
        } else {
            assertThatThrownBy(() -> clientFuture.get(5, TimeUnit.SECONDS)).cause().isSameAs(loadFailure);
        }
        verify(initialization.client()).close();
        assertThat(clientFuture).isCompletedExceptionally();
    }

    @Test
    public void testCloseFailureDoesNotReplaceRackInitializationFailure() throws Exception {
        ClientInitialization initialization = newClientInitialization(mock(BookKeeper.class));
        RuntimeException loadFailure = new RuntimeException("rack metadata unavailable");
        RuntimeException closeFailure = new RuntimeException("client close failed");
        doThrow(closeFailure).when(initialization.client()).close();
        CompletableFuture<BookKeeper> clientFuture = createIsolatedClient(initialization, 10_000);
        initialization.readinessRequested().get(5, TimeUnit.SECONDS);

        initialization.readiness().completeExceptionally(loadFailure);

        assertThatThrownBy(() -> clientFuture.get(5, TimeUnit.SECONDS)).cause().isSameAs(loadFailure);
        assertThat(loadFailure.getSuppressed()).containsExactly(closeFailure);
        verify(initialization.client()).close();
    }

    @Test
    public void testManagedLedgerOpenWaitsForClientRackConfiguration() throws Exception {
        OrderedExecutor bookKeeperExecutor = OrderedExecutor.newBuilder().numThreads(1)
                .name("test-client-readiness-bk").build();
        try (MetadataStoreExtended store = MetadataStoreExtended.create("memory:client-readiness",
                MetadataStoreConfig.builder().build());
             PulsarMockBookKeeper client = spy(new PulsarMockBookKeeper(bookKeeperExecutor))) {
            ClientInitialization initialization = newClientInitialization(client);
            ManagedLedgerFactoryImpl ledgerFactory = new ManagedLedgerFactoryImpl(store,
                    policyConfig -> createIsolatedClient(initialization, 10_000), new ManagedLedgerFactoryConfig());
            try {
                CompletableFuture<ManagedLedger> openFuture = new CompletableFuture<>();
                ledgerFactory.asyncOpen("client-readiness", new ManagedLedgerConfig(), new OpenLedgerCallback() {
                    @Override
                    public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                        openFuture.complete(ledger);
                    }

                    @Override
                    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        openFuture.completeExceptionally(exception);
                    }
                }, null, null);

                initialization.readinessRequested().get(5, TimeUnit.SECONDS);
                assertThat(openFuture).isNotDone();
                verify(client, never()).newCreateLedgerOp();

                initialization.readiness().complete(null);

                ManagedLedger ledger = openFuture.get(5, TimeUnit.SECONDS);
                assertThat(ledger.addEntry("ready".getBytes(StandardCharsets.UTF_8)).getEntryId()).isZero();
                verify(client).newCreateLedgerOp();
                ledger.close();
            } finally {
                ledgerFactory.shutdownAsync().get(5, TimeUnit.SECONDS);
            }
        } finally {
            bookKeeperExecutor.shutdownNow();
        }
    }

    private ClientInitialization newClientInitialization(BookKeeper client) throws Exception {
        CompletableFuture<Void> readiness = new CompletableFuture<>();
        CompletableFuture<Void> readinessRequested = new CompletableFuture<>();
        IsolatedBookieEnsemblePlacementPolicy policy = mock(IsolatedBookieEnsemblePlacementPolicy.class);
        when(policy.getInitializationFuture()).thenAnswer(invocation -> {
            readinessRequested.complete(null);
            return readiness;
        });
        ClientContext existingContext = client.getClientCtx();
        ClientContext clientContext = existingContext == null ? mock(ClientContext.class) : spy(existingContext);
        when(clientContext.getPlacementPolicy()).thenReturn(policy);
        doReturn(clientContext).when(client).getClientCtx();
        return new ClientInitialization(factoryReturning(client), client, readiness, readinessRequested);
    }

    @DataProvider
    public Object[][] rackInitializationRecoveryFailures() {
        return new Object[][] {{"immediate"}, {"delayed"}, {"timeout"}};
    }

    @Test(dataProvider = "rackInitializationRecoveryFailures")
    public void testManagedLedgerOpenRecoversAfterClientRackInitializationFailure(String failureMode) throws Exception {
        OrderedExecutor executor = OrderedExecutor.newBuilder().numThreads(1).name("test-rack-recovery-bk").build();
        try (MetadataStoreExtended store = MetadataStoreExtended.create("memory:rack-recovery-" + failureMode,
                MetadataStoreConfig.builder().build());
             PulsarMockBookKeeper defaultClient = new PulsarMockBookKeeper(executor);
             PulsarMockBookKeeper recoveredClient = new PulsarMockBookKeeper(executor)) {
            ClientInitialization initialization = newClientInitialization(mock(BookKeeper.class));
            BookKeeper.Builder builder = mock(BookKeeper.Builder.class);
            when(builder.build()).thenReturn(defaultClient, initialization.client(), recoveredClient);
            doReturn(builder).when(initialization.factory()).getBookKeeperBuilder(any(), any(), any(), any());
            RuntimeException loadFailure = new RuntimeException("temporary rack metadata failure");
            if (failureMode.equals("immediate")) {
                initialization.readiness().completeExceptionally(loadFailure);
            }

            ServiceConfiguration conf = new ServiceConfiguration();
            conf.setMetadataStoreUrl("memory:rack-recovery-" + failureMode);
            conf.setBookkeeperClientExposeStatsToPrometheus(false);
            conf.setMetadataStoreSessionTimeoutMillis(failureMode.equals("timeout") ? 100 : 10_000);
            try (ManagedLedgerClientFactory storage = new ManagedLedgerClientFactory()) {
                storage.initialize(conf, store, initialization.factory(), mock(EventLoopGroup.class),
                        OpenTelemetry.noop());
                ManagedLedgerFactory ledgerFactory = storage.getDefaultStorageClass().getManagedLedgerFactory();
                ManagedLedgerConfig config = new ManagedLedgerConfig();
                config.setBookKeeperEnsemblePlacementPolicyClassName(IsolatedBookieEnsemblePlacementPolicy.class);
                config.setBookKeeperEnsemblePlacementPolicyProperties(
                        Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1"));

                CompletableFuture<ManagedLedger> firstOpen = openManagedLedger(ledgerFactory, "rack-recovery", config);
                initialization.readinessRequested().get(5, TimeUnit.SECONDS);
                if (failureMode.equals("delayed")) {
                    initialization.readiness().completeExceptionally(loadFailure);
                }
                if (failureMode.equals("timeout")) {
                    assertThatThrownBy(() -> firstOpen.get(5, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(TimeoutException.class);
                } else {
                    assertThatThrownBy(() -> firstOpen.get(5, TimeUnit.SECONDS)).hasRootCause(loadFailure);
                }
                verify(initialization.client()).close();
                Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(storage.getBkEnsemblePolicyToBookKeeperMap()).isEmpty());

                ManagedLedger recovered = openManagedLedger(ledgerFactory, "rack-recovery", config)
                        .get(5, TimeUnit.SECONDS);
                assertThat(recovered.addEntry("recovered".getBytes(StandardCharsets.UTF_8)).getEntryId()).isZero();
                assertThat(storage.getBkEnsemblePolicyToBookKeeperMap().values()).containsExactly(recoveredClient);
                verify(builder, times(3)).build();
                recovered.close();
            } finally {
                initialization.readiness().complete(null);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private CompletableFuture<ManagedLedger> openManagedLedger(ManagedLedgerFactory factory, String name,
                                                               ManagedLedgerConfig config) {
        CompletableFuture<ManagedLedger> future = new CompletableFuture<>();
        factory.asyncOpen(name, config, new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                future.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, null);
        return future;
    }

    private BookKeeperClientFactoryImpl factoryReturning(BookKeeper client) throws Exception {
        BookKeeper.Builder builder = mock(BookKeeper.Builder.class);
        when(builder.build()).thenReturn(client);
        BookKeeperClientFactoryImpl factory = spy(new BookKeeperClientFactoryImpl());
        doReturn(builder).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        return factory;
    }

    private CompletableFuture<BookKeeper> createIsolatedClient(ClientInitialization initialization, int timeoutMillis) {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setMetadataStoreUrl("memory:client-readiness");
        conf.setMetadataStoreSessionTimeoutMillis(timeoutMillis);
        return initialization.factory().create(conf, mock(MetadataStoreExtended.class), mock(EventLoopGroup.class),
                Optional.of(IsolatedBookieEnsemblePlacementPolicy.class),
                Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1"));
    }

    private record ClientInitialization(BookKeeperClientFactoryImpl factory, BookKeeper client,
                                        CompletableFuture<Void> readiness,
                                        CompletableFuture<Void> readinessRequested) {
    }

    @Test
    public void testSetDefaultEnsemblePlacementPolicyRackAwareDisabled() {
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertNull(bkConf.getProperty(REPP_DNS_RESOLVER_CLASS));
        assertNull(bkConf.getProperty(MIN_NUM_RACKS_PER_WRITE_QUORUM));
        assertNull(bkConf.getProperty(ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM));

        BookKeeperClientFactoryImpl.setDefaultEnsemblePlacementPolicy(
            bkConf,
            conf,
            null
        );

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertEquals(
            bkConf.getProperty(REPP_DNS_RESOLVER_CLASS),
            BookieRackAffinityMapping.class.getName());
        assertFalse(bkConf.getEnforceMinNumRacksPerWriteQuorum());
        assertEquals(2, bkConf.getMinNumRacksPerWriteQuorum());
    }

    @Test
    public void testSetDefaultEnsemblePlacementPolicyRackAwareEnabled() {
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();
        MetadataStore store = mock(MetadataStore.class);

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertNull(bkConf.getProperty(REPP_DNS_RESOLVER_CLASS));
        assertNull(bkConf.getProperty(MIN_NUM_RACKS_PER_WRITE_QUORUM));
        assertNull(bkConf.getProperty(ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM));

        conf.setBookkeeperClientRegionawarePolicyEnabled(true);

        BookKeeperClientFactoryImpl.setDefaultEnsemblePlacementPolicy(
            bkConf,
            conf,
            store
        );

        assertTrue(bkConf.getBoolean(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getString(REPP_REGIONS_TO_WRITE));
        assertEquals(2, bkConf.getInt(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertTrue(bkConf.getBoolean(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertEquals(
            bkConf.getProperty(REPP_DNS_RESOLVER_CLASS),
            BookieRackAffinityMapping.class.getName());
        assertFalse(bkConf.getEnforceMinNumRacksPerWriteQuorum());
        assertEquals(2, bkConf.getMinNumRacksPerWriteQuorum());
    }

    @Test
    public void testSetDefaultEnsemblePlacementPolicyRackAwareEnabledChangedValues() {
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();
        MetadataStore store = mock(MetadataStore.class);

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertNull(bkConf.getProperty(REPP_DNS_RESOLVER_CLASS));
        assertNull(bkConf.getProperty(MIN_NUM_RACKS_PER_WRITE_QUORUM));
        assertNull(bkConf.getProperty(ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM));

        conf.setBookkeeperClientRegionawarePolicyEnabled(true);
        conf.getProperties().setProperty(REPP_ENABLE_VALIDATION, "false");
        conf.getProperties().setProperty(REPP_REGIONS_TO_WRITE, "region1;region2");
        conf.getProperties().setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, "4");
        conf.getProperties().setProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, "false");
        conf.getProperties().setProperty(REPP_DNS_RESOLVER_CLASS, CachedDNSToSwitchMapping.class.getName());
        conf.setBookkeeperClientMinNumRacksPerWriteQuorum(20);
        conf.setBookkeeperClientEnforceMinNumRacksPerWriteQuorum(true);

        BookKeeperClientFactoryImpl.setDefaultEnsemblePlacementPolicy(
            bkConf,
            conf,
            store
        );

        assertFalse(bkConf.getBoolean(REPP_ENABLE_VALIDATION));
        assertEquals("region1;region2", bkConf.getString(REPP_REGIONS_TO_WRITE));
        assertEquals(4, bkConf.getInt(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertFalse(bkConf.getBoolean(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertEquals(
            bkConf.getProperty(REPP_DNS_RESOLVER_CLASS),
            CachedDNSToSwitchMapping.class.getName());
        assertTrue(bkConf.getEnforceMinNumRacksPerWriteQuorum());
        assertEquals(20, bkConf.getMinNumRacksPerWriteQuorum());
    }

    @Test
    public void testSetEnsemblePlacementPolicys() {
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBookkeeperClientMinNumRacksPerWriteQuorum(3);
        conf.setBookkeeperClientEnforceMinNumRacksPerWriteQuorum(true);

        MetadataStore store = mock(MetadataStore.class);

        BookKeeperClientFactoryImpl.setEnsemblePlacementPolicy(
                bkConf,
                conf,
                store,
                ZkIsolatedBookieEnsemblePlacementPolicy.class);
        assertEquals(bkConf.getMinNumRacksPerWriteQuorum(), 3);
        assertTrue(bkConf.getEnforceMinNumRacksPerWriteQuorum());
    }

    @Test
    public void testSetDiskWeightBasedPlacementEnabled() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setMetadataStoreUrl("zk:localhost:2181");
        assertFalse(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getDiskWeightBasedPlacementEnabled());
        conf.setBookkeeperDiskWeightBasedPlacementEnabled(true);
        assertTrue(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getDiskWeightBasedPlacementEnabled());
    }

    @Test
    public void testSetExplicitLacInterval() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setMetadataStoreUrl("zk:localhost:2181");
        assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getExplictLacInterval(),
                0);
        conf.setBookkeeperExplicitLacIntervalInMills(5);
        assertEquals(
                factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf).getExplictLacInterval(),
                5);
    }

    @Test
    public void testSetMetadataServiceUriZookkeeperServers() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setMetadataStoreUrl("zk:localhost:2181");
        try {
            {
                final String expectedUri = "metadata-store:zk:localhost:2181/ledgers";
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), expectedUri);

            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
            fail("Get metadata service uri should be successful", e);
        }
    }


    @Test
    public void testSetMetadataServiceUriMetadataStoreUrl() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();

        try {
            {
                conf.setMetadataStoreUrl("zk:localhost:2181/chroot");
                final String expectedUri = "metadata-store:zk:localhost:2181/chroot/ledgers";
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), expectedUri);
            }
            {
                conf.setMetadataStoreUrl("zk:localhost:2181/chroot");
                final String expectedUri = "metadata-store:zk:localhost:2181/chroot/ledgers";
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), expectedUri);
            }

            {
                conf.setMetadataStoreUrl("zk:localhost:2181");
                final String expectedUri = "metadata-store:zk:localhost:2181/ledgers";
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), expectedUri);
            }

            {
                conf.setMetadataStoreUrl("zk:localhost:2181");
                final String expectedUri = "metadata-store:zk:localhost:2181/ledgers";
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), expectedUri);
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
            fail("Get metadata service uri should be successful", e);
        }
    }



    @Test
    public void testSetMetadataServiceUriBookkeeperMetadataServiceUri() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        try {

            {
                String uri = "metadata-store:localhost:2181";
                conf.setBookkeeperMetadataServiceUri(uri);
                final String expectedUri = "metadata-store:localhost:2181";
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), expectedUri);

            }
            {
                String uri = "metadata-store:localhost:2181/chroot/ledger";
                conf.setBookkeeperMetadataServiceUri(uri);
                final String expectedUri = "metadata-store:localhost:2181/chroot/ledger";
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), expectedUri);

            }
            {
                String uri = "metadata-store:oxia://oxia-server:6648/bookkeeper"
                        + "?batchingMaxDelayMillis=10&batchingMaxSizeKb=256&numSerDesThreads=4";
                conf.setBookkeeperMetadataServiceUri(uri);
                assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                        .getMetadataServiceUri(), uri);
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
            fail("Get metadata service uri should be successful", e);
        }
    }

    @Test
    public void testOpportunisticStripingConfiguration() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        // default value
        assertFalse(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getOpportunisticStriping());
        conf.getProperties().setProperty("bookkeeper_opportunisticStriping", "true");
        assertTrue(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getOpportunisticStriping());
        conf.getProperties().setProperty("bookkeeper_opportunisticStriping", "false");
        assertFalse(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getOpportunisticStriping());

    }

    @Test
    public void testBookKeeperIoThreadsConfiguration() throws Exception {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getNumIOThreads(), Runtime.getRuntime().availableProcessors() * 2);
        conf.setBookkeeperClientNumIoThreads(1);
        assertEquals(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getNumIOThreads(), 1);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        BookKeeper.Builder builder = factory.getBookKeeperBuilder(conf, eventLoopGroup,
                mock(StatsLogger.class), mock(ClientConfiguration.class));
        assertNull(FieldUtils.readField(builder, "eventLoopGroup", true));
        conf.setBookkeeperClientSeparatedIoThreadsEnabled(false);
        builder = factory.getBookKeeperBuilder(conf, eventLoopGroup,
                mock(StatsLogger.class), mock(ClientConfiguration.class));
        assertEquals(FieldUtils.readField(builder, "eventLoopGroup", true), eventLoopGroup);
    }

    @Test
    public void testBookKeeperLimitStatsLoggingConfiguration() throws Exception {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        assertTrue(factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf)
                .getLimitStatsLogging());
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        BookKeeper.Builder builder = factory.getBookKeeperBuilder(conf, eventLoopGroup, mock(StatsLogger.class),
                factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf));
        ClientConfiguration clientConfiguration =
                (ClientConfiguration) FieldUtils.readField(builder, "conf", true);
        assertTrue(clientConfiguration.getLimitStatsLogging());

        conf.setBookkeeperClientLimitStatsLogging(false);
        assertFalse(
                factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf).getLimitStatsLogging());
        builder = factory.getBookKeeperBuilder(conf, eventLoopGroup, mock(StatsLogger.class),
                factory.createBkClientConfiguration(mock(MetadataStoreExtended.class), conf));
        clientConfiguration =
                (ClientConfiguration) FieldUtils.readField(builder, "conf", true);
        assertFalse(clientConfiguration.getLimitStatsLogging());
    }
}
