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
import static org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.CachedDNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping;
import org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test {@link BookKeeperClientFactoryImpl}.
 */
@Test(groups = "broker")
public class BookKeeperClientFactoryImplTest {

    @Test
    public void testCreateRackAwareClientWithMetadataUriOnly() throws Exception {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var builder = mock(BookKeeper.Builder.class);
        var client = mock(BookKeeper.class);
        when(builder.build()).thenReturn(client);
        doReturn(builder).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        String uri = "metadata-store:zk:localhost:2181";
        var conf = new ServiceConfiguration();
        conf.setBookkeeperMetadataServiceUri(uri);
        conf.setBookkeeperClientRackawarePolicyEnabled(true);
        var configurations = ArgumentCaptor.forClass(ClientConfiguration.class);
        try {
            var result = factory.create(conf, null, mock(EventLoopGroup.class), Optional.empty(), null);
            assertThat(result.get(5, TimeUnit.SECONDS)).isSameAs(client);

            verify(factory).getBookKeeperBuilder(any(), any(), any(), configurations.capture());
            var bkConf = configurations.getValue();
            assertEquals(bkConf.getMetadataServiceUri(), uri);
            assertEquals(bkConf.getString(REPP_DNS_RESOLVER_CLASS), BookieRackAffinityMapping.class.getName());
            assertNull(bkConf.getProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE));
            assertNull(bkConf.getProperty(BookieRackConfigurationSnapshot.RACK_CONFIGURATION_SNAPSHOT));
        } finally {
            factory.close();
        }
    }

    @DataProvider
    public Object[][] defaultPlacementPolicies() {
        return new Object[][] {{new ServiceConfiguration()}, {regionAwareConfiguration("east;west", "2")}};
    }

    @Test(dataProvider = "defaultPlacementPolicies")
    public void testDefaultClientWaitsForInitialRackConfiguration(ServiceConfiguration conf) throws Exception {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var builder = mock(BookKeeper.Builder.class);
        var client = mock(BookKeeper.class);
        when(builder.build()).thenReturn(client);
        doReturn(builder).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        var store = mock(MetadataStoreExtended.class);
        var read = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var cache = mockRackConfigurationCache(store, read);
        var eventLoop = mock(EventLoopGroup.class);
        try {
            var result = factory.create(conf, store, eventLoop, Optional.empty(), null);
            verify(cache, timeout(5000)).get(BOOKIE_INFO_ROOT_PATH);
            assertThat(result).isNotCompleted();
            verify(builder, never()).build();
            read.complete(Optional.empty());
            assertThat(result.get(5, TimeUnit.SECONDS)).isSameAs(client);
        } finally {
            factory.close();
        }
    }

    @DataProvider
    public Object[][] rackReadFailurePolicies() {
        var hardRacks = new ServiceConfiguration();
        hardRacks.setBookkeeperClientEnforceMinNumRacksPerWriteQuorum(true);
        return new Object[][] {
                {new ServiceConfiguration(), false},
                {hardRacks, true},
                {regionAwareConfiguration("east;west", "2"), true}
        };
    }

    @Test(dataProvider = "rackReadFailurePolicies")
    public void testInitialRackReadFailurePreservesEnforcement(ServiceConfiguration conf, boolean strict)
            throws Exception {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var builder = mock(BookKeeper.Builder.class);
        var client = mock(BookKeeper.class);
        when(builder.build()).thenReturn(client);
        doReturn(builder).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        var store = mock(MetadataStoreExtended.class);
        var failure = new MetadataStoreException("Failed to read " + BOOKIE_INFO_ROOT_PATH);
        mockRackConfigurationCache(store, CompletableFuture.failedFuture(failure));
        try {
            var result = factory.create(conf, store, mock(EventLoopGroup.class), Optional.empty(), null);
            if (strict) {
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).cause().isSameAs(failure);
                verify(builder, never()).build();
            } else {
                assertThat(result.get(5, TimeUnit.SECONDS)).isSameAs(client);
            }
        } finally {
            factory.close();
        }
    }

    @Test
    public void testLazyClientDoesNotWaitForRackConfiguration() throws Exception {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var builder = mock(BookKeeper.Builder.class);
        var client = mock(BookKeeper.class);
        when(builder.build()).thenReturn(client);
        doReturn(builder).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        var store = mock(MetadataStoreExtended.class);
        var read = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        mockRackConfigurationCache(store, read);
        try {
            var result = factory.create(new ServiceConfiguration(), store, mock(EventLoopGroup.class),
                    Optional.of(IsolatedBookieEnsemblePlacementPolicy.class), null);
            assertThat(result.get(5, TimeUnit.SECONDS)).isSameAs(client);
            assertThat(read).isNotCompleted();
        } finally {
            factory.close();
        }
    }

    @Test
    public void testLazyCrossRegionClientFailsWithoutWaitingForRackConfiguration() {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var store = mock(MetadataStoreExtended.class);
        var read = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        mockRackConfigurationCache(store, read);
        try {
            // Explicit clients get region properties from their effective client configuration.
            var result = factory.create(new ServiceConfiguration(), store, mock(EventLoopGroup.class),
                    Optional.of(RegionAwareEnsemblePlacementPolicy.class),
                    Map.of(REPP_REGIONS_TO_WRITE, "east;west", REPP_MINIMUM_REGIONS_FOR_DURABILITY, 2));
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).cause()
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining(BOOKIE_INFO_ROOT_PATH);
            verify(factory, never()).getBookKeeperBuilder(any(), any(), any(), any());
            assertThat(read).isNotCompleted();
        } finally {
            factory.close();
        }
    }

    @DataProvider
    public Object[][] rackBootstrapTimeoutPolicies() {
        var hardRacks = new ServiceConfiguration();
        hardRacks.setBookkeeperClientEnforceMinNumRacksPerWriteQuorum(true);
        return new Object[][] {
                {hardRacks, true},
                {new ServiceConfiguration(), false},
                {regionAwareConfiguration("east;west", null), true},
                {regionAwareConfiguration("east;west", "0"), false},
                {regionAwareConfiguration("east;west", "1"), false},
                {regionAwareConfiguration(null, "2"), false}
        };
    }

    private static ServiceConfiguration regionAwareConfiguration(String writeRegions, String minRegions) {
        var conf = new ServiceConfiguration();
        conf.setBookkeeperClientRegionawarePolicyEnabled(true);
        if (writeRegions != null) {
            conf.getProperties().setProperty(REPP_REGIONS_TO_WRITE, writeRegions);
        }
        if (minRegions != null) {
            conf.getProperties().setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, minRegions);
        }
        return conf;
    }

    @Test(dataProvider = "rackBootstrapTimeoutPolicies")
    public void testRackBootstrapTimeoutPreservesPlacementConstraints(ServiceConfiguration conf, boolean strict)
            throws Exception {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var builder = mock(BookKeeper.Builder.class);
        var client = mock(BookKeeper.class);
        when(builder.build()).thenReturn(client);
        doReturn(builder).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        var store = mock(MetadataStoreExtended.class);
        var read = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        mockRackConfigurationCache(store, read);
        conf.setMetadataStoreOperationTimeoutSeconds(1);
        try {
            var result = factory.create(conf, store, mock(EventLoopGroup.class), Optional.empty(), null);
            if (strict) {
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).cause().isInstanceOf(TimeoutException.class)
                        .hasMessageContaining(BOOKIE_INFO_ROOT_PATH).hasMessageContaining("1000 ms");
                verify(builder, never()).build();
            } else {
                assertThat(result.get(5, TimeUnit.SECONDS)).isSameAs(client);
            }
            assertThat(read).isNotCompleted();
        } finally {
            factory.close();
        }
    }

    @Test
    public void testCreateAfterFactoryCloseDoesNotBuildClient() {
        var factory = spy(new BookKeeperClientFactoryImpl());
        factory.close();
        var result = factory.create(new ServiceConfiguration(), mock(MetadataStoreExtended.class),
                mock(EventLoopGroup.class), Optional.empty(), null);
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).cause()
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("factory is closed");
        verify(factory, never()).getBookKeeperBuilder(any(), any(), any(), any());
    }

    @Test
    public void testCloseFailsPendingBookieRackBootstrap() {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var store = mock(MetadataStoreExtended.class);
        var cache = mockRackConfigurationCache(store, new CompletableFuture<>());
        var result = factory.create(new ServiceConfiguration(), store,
                mock(EventLoopGroup.class), Optional.empty(), null);
        try {
            verify(cache, timeout(5000)).get(BOOKIE_INFO_ROOT_PATH);
        } finally {
            factory.close();
        }
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).cause()
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("factory is closed");
        verify(factory, never()).getBookKeeperBuilder(any(), any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private static MetadataCache<BookiesRackConfiguration> mockRackConfigurationCache(MetadataStoreExtended store,
            CompletableFuture<Optional<BookiesRackConfiguration>> read) {
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        when(store.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(read);
        return cache;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRackAwareAndIsolatedClientsShareSnapshotByStore() throws Exception {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var builder = mock(BookKeeper.Builder.class);
        when(builder.build()).thenReturn(mock(BookKeeper.class));
        doReturn(builder).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        var store = mock(MetadataStoreExtended.class);
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        when(store.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(store.registerCancellableListener(any())).thenReturn(() -> { });
        when(store.registerCancellableSessionListener(any())).thenReturn(() -> { });
        var conf = new ServiceConfiguration();
        var eventLoop = mock(EventLoopGroup.class);
        var configurations = ArgumentCaptor.forClass(ClientConfiguration.class);
        try {
            factory.create(conf, store, eventLoop, Optional.empty(), null).get(5, TimeUnit.SECONDS);
            factory.create(conf, store, eventLoop, Optional.of(IsolatedBookieEnsemblePlacementPolicy.class), null)
                    .get(5, TimeUnit.SECONDS);
            verify(factory, times(2)).getBookKeeperBuilder(any(), any(), any(), configurations.capture());
            String key = BookieRackConfigurationSnapshot.RACK_CONFIGURATION_SNAPSHOT;
            Object snapshot = configurations.getAllValues().getFirst().getProperty(key);
            assertThat(snapshot).isNotNull();
            assertThat(configurations.getValue().getProperty(key)).isSameAs(snapshot);
            verify(store).getMetadataCache(BookiesRackConfiguration.class);
        } finally {
            factory.close();
        }
        factory.close();
        verify(cache).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFailedBuildReleasesRegisteredResolverResources() throws Exception {
        var factory = spy(new BookKeeperClientFactoryImpl());
        var builder = mock(BookKeeper.Builder.class);
        var failure = new IOException("BookKeeper build failed");
        when(builder.build()).thenThrow(failure);
        var cleanup = mock(Runnable.class);
        doAnswer(invocation -> {
            ClientConfiguration config = invocation.getArgument(3);
            var register = (Consumer<Runnable>) config.getProperty(
                    BookieRackAffinityMapping.METADATA_STORE_LISTENER_CLEANUP);
            register.accept(cleanup);
            return builder;
        }).when(factory).getBookKeeperBuilder(any(), any(), any(), any());
        try {
            var conf = new ServiceConfiguration();
            conf.setBookkeeperClientRackawarePolicyEnabled(false);
            var result = factory.create(conf, mock(MetadataStoreExtended.class),
                    mock(EventLoopGroup.class), Optional.empty(), null);
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).cause().isSameAs(failure);
            verify(cleanup).run();
        } finally {
            factory.close();
        }
        verify(cleanup).run();
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
    public void testSetEnsemblePlacementPolicyUsesBrokerIsolationGroups() {
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();
        MetadataStore store = mock(MetadataStore.class);

        BookKeeperClientFactoryImpl.setEnsemblePlacementPolicy(bkConf, conf, store,
                IsolatedBookieEnsemblePlacementPolicy.class);
        assertNull(bkConf.getProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS));
        assertNull(bkConf.getProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS));

        conf.setBookkeeperClientIsolationGroups("broker-primary");
        conf.setBookkeeperClientSecondaryIsolationGroups("broker-secondary");
        BookKeeperClientFactoryImpl.setEnsemblePlacementPolicy(bkConf, conf, store,
                IsolatedBookieEnsemblePlacementPolicy.class);
        assertEquals(bkConf.getProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS),
                "broker-primary");
        assertEquals(bkConf.getProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS),
                "broker-secondary");
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
