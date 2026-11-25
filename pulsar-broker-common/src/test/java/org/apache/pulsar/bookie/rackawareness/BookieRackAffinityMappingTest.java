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
package org.apache.pulsar.bookie.rackawareness;

import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;
import static org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
import static org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.RACK_CONFIGURATION_SNAPSHOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException.BKBookieHandleNotAvailableException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.DefaultBookieAddressResolver;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.BookieServiceInfoUtils;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.NetworkTopologyImpl;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.VersionedConfiguration;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.bookkeeper.BookieServiceInfoSerde;
import org.apache.pulsar.metadata.bookkeeper.PulsarRegistrationClient;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BookieRackAffinityMappingTest {

    private BookieSocketAddress bookie1 = null;
    private BookieSocketAddress bookie2 = null;
    private BookieSocketAddress bookie3 = null;
    private MetadataStore store;

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
        bookie1 = new BookieSocketAddress("127.0.0.1:3181");
        bookie2 = new BookieSocketAddress("127.0.0.2:3181");
        bookie3 = new BookieSocketAddress("127.0.0.3:3181");
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        store.close();
    }

    private void writeInitialRackMetadata(byte[] metadata) throws Exception {
        var created = new CompletableFuture<Void>();
        @Cleanup("run") var cancel = store.registerCancellableListener(notification -> {
            if (BOOKIE_INFO_ROOT_PATH.equals(notification.getPath())
                    && notification.getType() == NotificationType.Created) {
                created.complete(null);
            }
        });
        store.put(BOOKIE_INFO_ROOT_PATH, metadata, Optional.empty()).get(5, TimeUnit.SECONDS);
        created.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testBasic() throws Exception {
        String data = "{\"group1\": {\"" + bookie1
                + "\": {\"rack\": \"/rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + bookie2
                + "\": {\"rack\": \"/rack1\", \"hostname\": \"bookie2.example.com\"}}}";
        store.put(BOOKIE_INFO_ROOT_PATH, data.getBytes(), Optional.empty()).join();

        // Case1: ZKCache is given
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()));

        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertNull(racks.get(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClientCleanupReleasesResolverCacheAndListener() {
        MetadataStore metadataStore = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> firstCache = mock(MetadataCache.class);
        MetadataCache<BookiesRackConfiguration> secondCache = mock(MetadataCache.class);
        when(metadataStore.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(firstCache, secondCache);
        when(firstCache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(secondCache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        Runnable firstUnregister = mock(Runnable.class);
        Runnable secondUnregister = mock(Runnable.class);
        when(metadataStore.registerCancellableListener(any())).thenReturn(firstUnregister, secondUnregister);

        List<Runnable> cleanups = new ArrayList<>();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, metadataStore);
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_LISTENER_CLEANUP, (Consumer<Runnable>) cleanups::add);
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

        mapping.setConf(conf);
        verify(firstCache, never()).close();
        mapping.setConf(conf);
        verify(firstUnregister).run();
        verify(firstCache).close();

        assertEquals(cleanups.size(), 2);
        cleanups.get(0).run();
        verify(secondUnregister, never()).run();
        verify(secondCache, never()).close();
        cleanups.get(1).run();
        verify(secondUnregister).run();
        verify(secondCache).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLegacyResolverKeepsLatestAppliedRackWhenReadsCompleteOutOfOrder() throws Exception {
        var source = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        when(source.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        var watcher = new AtomicReference<Consumer<Notification>>();
        when(source.registerCancellableListener(any())).thenAnswer(call -> {
            watcher.set(call.getArgument(0));
            return (Runnable) () -> { };
        });
        var bookie = bookie1.toString();
        var oldRacks = new BookiesRackConfiguration();
        oldRacks.updateBookie("group", bookie, BookieInfo.builder().rack("/old").build());
        var newRacks = new BookiesRackConfiguration();
        newRacks.updateBookie("group", bookie, BookieInfo.builder().rack("/new").build());
        var latestRacks = new BookiesRackConfiguration();
        latestRacks.updateBookie("group", bookie, BookieInfo.builder().rack("/latest").build());
        var initialRead = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var olderRefresh = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var initialReadStarted = new CompletableFuture<Void>();
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenAnswer(call -> {
            initialReadStarted.complete(null);
            return initialRead;
        }).thenReturn(CompletableFuture.completedFuture(Optional.of(newRacks)),
                olderRefresh, CompletableFuture.completedFuture(Optional.of(latestRacks)));

        var conf = new ClientConfiguration();
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, source);
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        var initializing = CompletableFuture.runAsync(() -> mapping.setConf(conf));
        var addresses = List.of(bookie1.getHostName());
        var modified = new Notification(NotificationType.Modified, BOOKIE_INFO_ROOT_PATH);
        try {
            initialReadStarted.get(5, TimeUnit.SECONDS);
            watcher.get().accept(modified);
            assertEquals(mapping.resolve(addresses), List.of("/new"));
        } finally {
            initialRead.complete(Optional.of(oldRacks));
        }
        initializing.get(5, TimeUnit.SECONDS);
        assertEquals(mapping.resolve(addresses), List.of("/new"));

        watcher.get().accept(modified);
        watcher.get().accept(modified);
        olderRefresh.complete(Optional.of(oldRacks));
        assertEquals(mapping.resolve(addresses), List.of("/latest"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLegacyResolverCannotReapplyPreviousConfiguration() throws Exception {
        var firstStore = mock(MetadataStore.class);
        var secondStore = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> firstCache = mock(MetadataCache.class);
        MetadataCache<BookiesRackConfiguration> secondCache = mock(MetadataCache.class);
        when(firstStore.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(firstCache);
        when(secondStore.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(secondCache);
        when(firstStore.registerCancellableListener(any())).thenReturn(() -> { });
        when(secondStore.registerCancellableListener(any())).thenReturn(() -> { });
        var registration = mock(RegistrationClient.class);
        var listeners = new ArrayList<RegistrationClient.RegistrationListener>();
        when(registration.watchWritableBookies(any())).thenAnswer(call -> {
            listeners.add(call.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });
        doAnswer(call -> {
            listeners.remove((RegistrationClient.RegistrationListener) call.getArgument(0));
            return null;
        }).when(registration).unwatchWritableBookies(any());
        var bookie = bookie1.toBookieId();
        var info = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie.toString());
        when(registration.getBookieServiceInfo(bookie))
                .thenReturn(CompletableFuture.completedFuture(new Versioned<>(info, Version.NEW)));
        var oldRacks = new BookiesRackConfiguration();
        oldRacks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/old").build());
        var newRacks = new BookiesRackConfiguration();
        newRacks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/new").build());
        var lateOldRead = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        when(firstCache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(CompletableFuture.completedFuture(Optional.of(oldRacks)))
                .thenReturn(lateOldRead);
        when(secondCache.get(BOOKIE_INFO_ROOT_PATH))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(newRacks)));
        var conf = new ClientConfiguration();
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(new DefaultBookieAddressResolver(registration));
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, firstStore);
        mapping.setConf(conf);
        var originalWatcher = listeners.get(0);
        originalWatcher.onBookiesChanged(new Versioned<>(Set.of(bookie), Version.NEW));
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, secondStore);
        mapping.setConf(conf);
        var addresses = List.of(bookie1.getHostName());
        assertEquals(mapping.resolve(addresses), List.of("/new"));
        verify(registration).unwatchWritableBookies(originalWatcher);
        assertEquals(listeners.size(), 1);
        lateOldRead.complete(Optional.of(oldRacks));
        assertEquals(mapping.resolve(addresses), List.of("/new"), "rack after a writable-bookie event");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSharedSnapshotIsAppliedToHardRackPlacement() throws Exception {
        MetadataStore metadataStore = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        when(metadataStore.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        var initialRead = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(initialRead);

        @Cleanup
        var snapshot = new BookieRackConfigurationSnapshot(metadataStore, 30_000);
        snapshot.start();

        var conf = new ClientConfiguration();
        conf.setProperty(RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS,
                BookieRackAffinityMapping.class.getName());
        conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        conf.setEnforceMinNumRacksPerWriteQuorum(true);
        conf.setMinNumRacksPerWriteQuorum(2);
        @Cleanup("stop")
        var timer = getTestHashedWheelTimer(conf);
        @Cleanup("uninitalize")
        var policy = new RackawareEnsemblePlacementPolicy();
        policy.initialize(conf, Optional.empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        BookieId first = bookie1.toBookieId();
        BookieId second = bookie2.toBookieId();
        policy.onClusterChanged(Set.of(first, second), Set.of());
        assertFalse(initialRead.isDone());
        // BookKeeper's direct create and replacement calls must reject unresolved racks even for a one-bookie quorum.
        expectThrows(BKNotEnoughBookiesException.class,
                () -> policy.newEnsemble(1, 1, 1, Map.of(), new HashSet<>()));
        expectThrows(BKNotEnoughBookiesException.class,
                () -> policy.replaceBookie(1, 1, 1, Map.of(), List.of(first), first, new HashSet<>()));

        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", first.toString(), BookieInfo.builder().rack("/rack0").build());
        racks.updateBookie("group", second.toString(), BookieInfo.builder().rack("/rack1").build());
        initialRead.complete(Optional.of(racks));
        Awaitility.await().untilAsserted(() ->
                assertEquals(Set.copyOf(policy.newEnsemble(2, 2, 2, Map.of(), new HashSet<>()).getResult()),
                        Set.of(first, second)));
        assertEquals(policy.replaceBookie(1, 1, 1, Map.of(), List.of(first), first, new HashSet<>()).getResult(),
                second);
        verify(metadataStore).getMetadataCache(BookiesRackConfiguration.class);
        verify(cache).get(BOOKIE_INFO_ROOT_PATH);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSharedSnapshotHandlesWritableBookiesAndUnsubscribesOnClose() throws Exception {
        BookieId bookieId = BookieId.parse("bookie-id");
        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", bookieId.toString(), BookieInfo.builder().rack("/rack0").build());
        writeInitialRackMetadata(jsonMapper.writeValueAsBytes(racks));
        @Cleanup
        var snapshot = new BookieRackConfigurationSnapshot(store, 30_000);
        snapshot.start();
        snapshot.initialLoad().get(10, TimeUnit.SECONDS);

        var registrationClient = mock(RegistrationClient.class);
        when(registrationClient.getBookieServiceInfo(bookieId))
                .thenReturn(CompletableFuture.failedFuture(new BKBookieHandleNotAvailableException()));
        var conf = new ClientConfiguration();
        var cleanup = new AtomicReference<Runnable>();
        conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_LISTENER_CLEANUP, (Consumer<Runnable>) cleanup::set);
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(new DefaultBookieAddressResolver(registrationClient));
        mapping.setConf(conf);
        ITopologyAwareEnsemblePlacementPolicy<BookieNode> policy = mock(ITopologyAwareEnsemblePlacementPolicy.class);
        mapping.registerRackChangeListener(policy);
        List<String> addresses = List.of(bookie1.getHostName());
        assertNull(mapping.resolve(addresses).get(0));

        var available = ArgumentCaptor.forClass(RegistrationClient.RegistrationListener.class);
        verify(registrationClient).watchWritableBookies(available.capture());
        var serviceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie1.toString());
        var resolvedOn = new AtomicReference<Thread>();
        when(registrationClient.getBookieServiceInfo(bookieId)).thenAnswer(call -> {
            resolvedOn.set(Thread.currentThread());
            return CompletableFuture.completedFuture(new Versioned<>(serviceInfo, Version.NEW));
        });
        available.getValue().onBookiesChanged(new Versioned<>(Set.of(bookieId), Version.NEW));
        assertEquals(resolvedOn.get(), Thread.currentThread());
        assertEquals(mapping.resolve(addresses), List.of("/rack0"));
        verify(policy).onBookieRackChange(List.of(bookieId));

        cleanup.get().run();
        verify(registrationClient).unwatchWritableBookies(available.getValue());
        long initialVersion = snapshot.latest().version();
        var refreshed = new CompletableFuture<Void>();
        @Cleanup("run")
        Runnable unsubscribe = snapshot.subscribe(configuration -> {
            if (configuration.version() > initialVersion) {
                refreshed.complete(null);
            }
        });
        store.delete(BOOKIE_INFO_ROOT_PATH, Optional.empty()).join();
        refreshed.get(10, TimeUnit.SECONDS);

        assertEquals(mapping.resolve(addresses), List.of("/rack0"));
        verify(policy).onBookieRackChange(List.of(bookieId));
    }

    @Test
    public void testUnchangedRackMetadataWriteRemapsBookieAddress() throws Exception {
        var bookie = BookieId.parse("bookie-id");
        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/rack0").build());
        byte[] metadata = jsonMapper.writeValueAsBytes(racks);
        writeInitialRackMetadata(metadata);

        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(store, 30_000);
        snapshot.start();
        snapshot.initialRead().get(5, TimeUnit.SECONDS);
        var initial = snapshot.latest();

        var endpoint = new AtomicReference<>(bookie1);
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(id -> endpoint.get());
        var conf = new ClientConfiguration();
        conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        mapping.setConf(conf);
        var addresses = List.of(bookie1.getHostName(), bookie2.getHostName());
        assertEquals(mapping.resolve(addresses), Lists.newArrayList("/rack0", null));

        endpoint.set(bookie2);
        store.put(BOOKIE_INFO_ROOT_PATH, metadata, Optional.empty()).get(5, TimeUnit.SECONDS);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(mapping.resolve(addresses), Lists.newArrayList(null, "/rack0")));
        assertThat(snapshot.latest()).isSameAs(initial);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnchangedRackMetadataRecreationRemapsBookieAddress() throws Exception {
        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", "bookie-id", BookieInfo.builder().rack("/rack0").build());
        var source = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        when(source.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        var watcher = new AtomicReference<Consumer<Notification>>();
        when(source.registerCancellableListener(any())).thenAnswer(call -> {
            watcher.set(call.getArgument(0));
            return (Runnable) () -> { };
        });
        var cached = CompletableFuture.completedFuture(Optional.of(racks));
        var deletionReadStarted = new CompletableFuture<Void>();
        var deleted = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(cached).thenAnswer(call -> {
            deletionReadStarted.complete(null);
            return deleted;
        }).thenReturn(cached);

        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(source, 30_000);
        snapshot.start();
        snapshot.initialRead().get(5, TimeUnit.SECONDS);
        var initial = snapshot.latest();
        var endpoint = new AtomicReference<>(bookie1);
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(id -> endpoint.get());
        var conf = new ClientConfiguration();
        conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        mapping.setConf(conf);

        watcher.get().accept(new Notification(NotificationType.Deleted, BOOKIE_INFO_ROOT_PATH));
        deletionReadStarted.get(5, TimeUnit.SECONDS);
        endpoint.set(bookie2);
        watcher.get().accept(new Notification(NotificationType.Created, BOOKIE_INFO_ROOT_PATH));
        deleted.complete(Optional.empty());

        var addresses = List.of(bookie1.getHostName(), bookie2.getHostName());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(mapping.resolve(addresses), Lists.newArrayList(null, "/rack0")));
        assertThat(snapshot.latest()).isSameAs(initial);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnchangedRackMetadataWriteReappliesAfterReadFailure() throws Exception {
        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", bookie1.toString(), BookieInfo.builder().rack("/rack0").build());
        var source = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        when(source.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        var watcher = new AtomicReference<Consumer<Notification>>();
        when(source.registerCancellableListener(any())).thenAnswer(call -> {
            watcher.set(call.getArgument(0));
            return (Runnable) () -> { };
        });
        var cached = CompletableFuture.completedFuture(Optional.of(racks));
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(cached,
                CompletableFuture.failedFuture(new MetadataStoreException("Transient refresh failure")), cached);

        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(source, 30_000);
        snapshot.start();
        snapshot.initialRead().get(5, TimeUnit.SECONDS);
        var initial = snapshot.latest();
        var updates = new ConcurrentLinkedQueue<VersionedConfiguration>();
        @Cleanup("run") var subscription = snapshot.subscribe(updates::add);
        assertEquals(updates.poll(), initial);

        watcher.get().accept(new Notification(NotificationType.Modified, BOOKIE_INFO_ROOT_PATH));
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(updates.peek()).isSameAs(initial));
        assertThat(snapshot.latest()).isSameAs(initial);
    }

    @Test
    public void testSubscriberRetriesUnchangedConfiguration() throws Exception {
        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(store, 30_000);
        var failedOnce = new CompletableFuture<Void>();
        var recovered = new CompletableFuture<Long>();
        @Cleanup("run") var subscription = snapshot.subscribe(configuration -> {
            if (failedOnce.complete(null)) {
                throw new IllegalStateException("Transient apply failure");
            }
            recovered.complete(configuration.version());
        });
        snapshot.start();
        snapshot.initialRead().get(10, TimeUnit.SECONDS);
        assertEquals(recovered.get(10, TimeUnit.SECONDS).longValue(), 1L);
    }

    @Test
    public void testNewSubscriberRetriesRefreshPublishedDuringInitialReplay() throws Exception {
        var racks = new BookiesRackConfiguration();
        var bookie = bookie1.toBookieId();
        racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/initial").build());
        writeInitialRackMetadata(jsonMapper.writeValueAsBytes(racks));
        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(store, 30_000);
        snapshot.start();
        snapshot.initialRead().get(5, TimeUnit.SECONDS);

        var initialReplayStarted = new CompletableFuture<Void>();
        var finishInitialReplay = new CompletableFuture<Void>();
        var firstRefreshAttempt = new CompletableFuture<Void>();
        var refreshRetried = new CompletableFuture<Long>();
        var attempts = new ConcurrentLinkedQueue<Long>();
        var subscription = CompletableFuture.supplyAsync(() -> snapshot.subscribe(configuration -> {
            attempts.add(configuration.version());
            if (configuration.version() == 1) {
                initialReplayStarted.complete(null);
                finishInitialReplay.orTimeout(10, TimeUnit.SECONDS).join();
            } else if (firstRefreshAttempt.complete(null)) {
                throw new IllegalStateException("Transient failure applying refresh");
            } else {
                refreshRetried.complete(configuration.version());
            }
        }));
        var otherClientRefreshed = new CompletableFuture<Long>();
        @Cleanup("run") var other = snapshot.subscribe(configuration -> {
            if (configuration.version() > 1) {
                otherClientRefreshed.complete(configuration.version());
            }
        });
        try {
            initialReplayStarted.get(5, TimeUnit.SECONDS);
            racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/updated").build());
            store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(racks), Optional.empty()).join();
            assertEquals(otherClientRefreshed.get(5, TimeUnit.SECONDS).longValue(), 2L);
            assertFalse(firstRefreshAttempt.isDone());
        } finally {
            finishInitialReplay.complete(null);
        }
        @Cleanup("run") var subscribed = subscription.get(5, TimeUnit.SECONDS);
        assertEquals(refreshRetried.get(5, TimeUnit.SECONDS).longValue(), 2L);
        assertEquals(List.copyOf(attempts), List.of(1L, 2L, 2L));
    }

    @Test
    public void testSlowResolverDoesNotBlockOtherClientsOrSnapshotRefresh() throws Exception {
        var racks = new BookiesRackConfiguration();
        var bookie = bookie1.toBookieId();
        racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/old").build());
        writeInitialRackMetadata(jsonMapper.writeValueAsBytes(racks));
        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(store, 30_000);
        snapshot.start();
        snapshot.initialRead().get(10, TimeUnit.SECONDS);

        var started = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        var addressResolver = mock(BookieAddressResolver.class);
        when(addressResolver.resolve(bookie)).thenReturn(bookie1).thenAnswer(call -> {
            started.complete(null);
            release.get(30, TimeUnit.SECONDS);
            return bookie1;
        });
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(addressResolver);
        var conf = new ClientConfiguration();
        conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        mapping.setConf(conf);
        var otherMapping = new BookieRackAffinityMapping();
        otherMapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        otherMapping.setConf(conf);
        var addresses = List.of(bookie1.getHostName());
        try {
            racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/new").build());
            store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(racks), Optional.empty()).join();
            started.get(10, TimeUnit.SECONDS);
            assertEquals(CompletableFuture.supplyAsync(() -> mapping.resolve(addresses)).get(5, TimeUnit.SECONDS),
                    List.of("/old"));
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(otherMapping.resolve(addresses), List.of("/new")));
            racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/latest").build());
            store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(racks), Optional.empty()).join();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(snapshot.latest().version(), 3L);
                assertEquals(otherMapping.resolve(addresses), List.of("/latest"));
            });
            assertEquals(mapping.resolve(addresses), List.of("/old"));
        } finally {
            release.complete(null);
        }
        Awaitility.await().untilAsserted(() -> assertEquals(mapping.resolve(addresses), List.of("/latest")));
    }

    @DataProvider
    public Object[][] rackUpdateStages() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "rackUpdateStages")
    @SuppressWarnings("unchecked")
    public void testCloseDoesNotWaitForRackUpdate(boolean pauseAddressResolution) throws Exception {
        var racks = new BookiesRackConfiguration();
        var bookie = bookie1.toBookieId();
        racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/old").build());
        writeInitialRackMetadata(jsonMapper.writeValueAsBytes(racks));
        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(store, 30_000);
        snapshot.start();
        snapshot.initialRead().get(10, TimeUnit.SECONDS);

        var started = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        var delivered = new CompletableFuture<Void>();
        Runnable pause = () -> {
            started.complete(null);
            release.orTimeout(30, TimeUnit.SECONDS).join();
        };
        var addressResolver = mock(BookieAddressResolver.class);
        var resolve = when(addressResolver.resolve(bookie)).thenReturn(bookie1);
        if (pauseAddressResolution) {
            resolve.thenAnswer(call -> {
                pause.run();
                return bookie1;
            });
        }
        ITopologyAwareEnsemblePlacementPolicy<BookieNode> policy = mock(ITopologyAwareEnsemblePlacementPolicy.class);
        if (!pauseAddressResolution) {
            doAnswer(call -> {
                pause.run();
                return null;
            }).when(policy).onBookieRackChange(any());
        }
        var cleanup = new AtomicReference<Runnable>();
        var conf = new ClientConfiguration();
        conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_LISTENER_CLEANUP, (Consumer<Runnable>) cleanup::set);
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(addressResolver);
        mapping.setConf(conf);
        mapping.registerRackChangeListener(policy);
        var initial = snapshot.latest();
        @Cleanup("run") var subscription = snapshot.subscribe(configuration -> {
            if (configuration.version() > initial.version()) {
                delivered.complete(null);
            }
        });

        try {
            racks.updateBookie("group", bookie.toString(), BookieInfo.builder().rack("/new").build());
            store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(racks), Optional.empty()).join();
            started.get(5, TimeUnit.SECONDS);
            CompletableFuture.runAsync(cleanup.get()).get(5, TimeUnit.SECONDS);
            delivered.get(5, TimeUnit.SECONDS);
            CompletableFuture.runAsync(snapshot::close).get(5, TimeUnit.SECONDS);
        } finally {
            release.complete(null);
        }
        // Registering acquires this resolver's binding monitor after its in-flight update has returned.
        CompletableFuture.runAsync(() -> mapping.registerRackChangeListener(policy)).get(5, TimeUnit.SECONDS);
        if (pauseAddressResolution) {
            assertEquals(mapping.resolve(List.of(bookie1.getHostName())), List.of("/old"));
            verify(policy, never()).onBookieRackChange(any());
        }
    }

    @Test
    public void testPreloadedSnapshotPreservesLocalRegion() throws Exception {
        String host = InetAddress.getLocalHost().getHostAddress();
        BookieId local = new BookieSocketAddress(host, 3181).toBookieId();
        String remoteHost = host.equals("127.0.0.2") ? "127.0.0.3" : "127.0.0.2";
        BookieId remote = new BookieSocketAddress(remoteHost, 3181).toBookieId();
        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", local.toString(), BookieInfo.builder().rack("/east/rack0").build());
        racks.updateBookie("group", remote.toString(), BookieInfo.builder().rack("/west/rack0").build());
        store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(racks), Optional.empty()).join();

        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(store, 30_000);
        snapshot.start();
        snapshot.initialRead().get(10, TimeUnit.SECONDS);
        var conf = new ClientConfiguration();
        conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
        conf.setProperty(RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS,
                BookieRackAffinityMapping.class.getName());
        @Cleanup("stop") var timer = getTestHashedWheelTimer(conf);
        @Cleanup("uninitalize") var policy = ReflectionUtils.newInstance(RegionAwareEnsemblePlacementPolicy.class);
        policy.initialize(conf, Optional.empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        policy.onClusterChanged(Set.of(local, remote), Set.of());
        // With no client region, BookKeeper rotates between available regions across successive ensembles.
        for (int i = 0; i < 2; i++) {
            assertEquals(policy.newEnsemble(1, 1, 1, Map.of(), new HashSet<>()).getResult(), List.of(local));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSupersededInitialReadWaitsForPublication() throws Exception {
        var source = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        when(source.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        var watcher = new AtomicReference<Consumer<Notification>>();
        when(source.registerCancellableListener(any())).thenAnswer(call -> {
            watcher.set(call.getArgument(0));
            return (Runnable) () -> { };
        });
        var initial = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var refreshed = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var started = new CompletableFuture<Void>();
        var retrying = new CompletableFuture<Void>();
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenAnswer(call -> {
            started.complete(null);
            return initial;
        }).thenAnswer(call -> {
            retrying.complete(null);
            return refreshed;
        });
        @Cleanup var snapshot = new BookieRackConfigurationSnapshot(source, 30_000);
        snapshot.start();
        started.get(10, TimeUnit.SECONDS);
        watcher.get().accept(new Notification(NotificationType.Modified, BOOKIE_INFO_ROOT_PATH));
        initial.complete(Optional.empty());
        retrying.get(10, TimeUnit.SECONDS);
        assertFalse(snapshot.initialRead().isDone());
        assertFalse(snapshot.hasLoaded());
        refreshed.complete(Optional.empty());
        snapshot.initialRead().get(10, TimeUnit.SECONDS);
        assertThat(snapshot.hasLoaded()).isTrue();
    }

    @Test
    public void testMultipleMetadataServiceUris() {
        BookieRackAffinityMapping mapping1 = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf1 = new ClientConfiguration();
        bkClientConf1.setProperty("metadataServiceUri", "memory:local,memory:local");
        bkClientConf1.setProperty("zkTimeout", "100000");

        mapping1.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        // This previously threw an exception when the metadataServiceUri was a comma delimited list.
        mapping1.setConf(bkClientConf1);
    }

    @Test
    public void testInvalidRackName() {
        String data = "{\"group1\": {\"" + bookie1
                + "\": {\"rack\": \"/\", \"hostname\": \"bookie1.example.com\"}, \"" + bookie2
                + "\": {\"rack\": \"\", \"hostname\": \"bookie2.example.com\"}}}";

        store.put(BOOKIE_INFO_ROOT_PATH, data.getBytes(), Optional.empty()).join();

        // Case1: ZKCache is given
        BookieRackAffinityMapping mapping1 = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf1 = new ClientConfiguration();
        bkClientConf1.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping1.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping1.setConf(bkClientConf1);
        List<String> racks = mapping1
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()));

        assertNull(racks.get(0));
        assertNull(racks.get(1));
        assertNull(racks.get(2));
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        assertNull(racks.get(0));
        assertNull(racks.get(1));
        assertNull(racks.get(2));

        Map<String, Map<BookieSocketAddress, BookieInfo>> bookieMapping = new HashMap<>();
        Map<BookieSocketAddress, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(bookie1, BookieInfo.builder().rack("/rack0").build());
        mainBookieGroup.put(bookie2, BookieInfo.builder().rack("/rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        Awaitility.await().untilAsserted(() -> {
            List<String> r = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
            assertEquals(r.get(0), "/rack0");
            assertEquals(r.get(1), "/rack1");
            assertNull(r.get(2));
        });

    }

    @Test
    public void testBookieInfoChange() throws Exception {
        Map<String, Map<BookieSocketAddress, BookieInfo>> bookieMapping = new HashMap<>();
        Map<BookieSocketAddress, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(bookie1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(bookie2, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()));
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertNull(racks.get(2));

        // add info for BOOKIE3 and check if the mapping picks up the change
        Map<BookieSocketAddress, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(bookie3, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put("group2", secondaryBookieGroup);
        store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();
        Awaitility.await().untilAsserted(() -> {
            List<String> r = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
            assertEquals(r.get(0), "/rack0");
            assertEquals(r.get(1), "/rack1");
            assertEquals(r.get(2), "/rack0");
        });
        store.put(BOOKIE_INFO_ROOT_PATH, "{}".getBytes(),
                Optional.empty()).join();

        Awaitility.await().untilAsserted(() -> {
            List<String> r = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
            assertNull(r.get(0));
            assertNull(r.get(1));
            assertNull(r.get(2));
        });
    }

    @Test
    public void testWithPulsarRegistrationClient() throws Exception {
        String data = "{\"group1\": {\"" + bookie1
                + "\": {\"rack\": \"/rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + bookie2
                + "\": {\"rack\": \"/rack1\", \"hostname\": \"bookie2.example.com\"}}}";
        store.put(BOOKIE_INFO_ROOT_PATH, data.getBytes(), Optional.empty()).join();

        // Case1: ZKCache is given
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        @Cleanup
        PulsarRegistrationClient pulsarRegistrationClient = new PulsarRegistrationClient(store, "/ledgers");
        DefaultBookieAddressResolver defaultBookieAddressResolver =
                new DefaultBookieAddressResolver(pulsarRegistrationClient);

        mapping.setBookieAddressResolver(defaultBookieAddressResolver);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()))
                .stream().filter(Objects::nonNull).toList();
        assertEquals(racks.size(), 0);

        @Cleanup("stop")
        HashedWheelTimer timer = getTestHashedWheelTimer(bkClientConf);
        RackawareEnsemblePlacementPolicy repp = new RackawareEnsemblePlacementPolicy();
        mapping.registerRackChangeListener(repp);
        Class<?> clazz1 = Class.forName("org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy");
        Field field1 = clazz1.getDeclaredField("knownBookies");
        field1.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<BookieId, BookieNode> knownBookies = (Map<BookieId, BookieNode>) field1.get(repp);
        repp.initialize(bkClientConf, Optional.of(mapping), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, defaultBookieAddressResolver);

        Class<?> clazz2 = Class.forName("org.apache.bookkeeper.client.BookieWatcherImpl");
        Constructor<?> constructor =
                clazz2.getDeclaredConstructor(ClientConfiguration.class, EnsemblePlacementPolicy.class,
                        RegistrationClient.class, BookieAddressResolver.class, StatsLogger.class);
        constructor.setAccessible(true);
        Object o = constructor.newInstance(bkClientConf, repp, pulsarRegistrationClient, defaultBookieAddressResolver,
                NullStatsLogger.INSTANCE);
        Method method = clazz2.getDeclaredMethod("initialBlockingBookieRead");
        method.setAccessible(true);
        method.invoke(o);

        Field field2 = BookieServiceInfoSerde.class.getDeclaredField("INSTANCE");
        field2.setAccessible(true);
        BookieServiceInfoSerde serviceInfoSerde = (BookieServiceInfoSerde) field2.get(null);

        BookieServiceInfo bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie1.toString());
        store.put("/ledgers/available/" + bookie1, serviceInfoSerde.serialize("", bookieServiceInfo),
                Optional.of(-1L)).get();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(
                () -> "/rack0".equals(mapping.resolve(List.of(bookie1.getHostName())).get(0)));
        racks = mapping
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()))
                .stream().filter(Objects::nonNull).toList();
        assertEquals(racks.size(), 1);
        assertEquals(racks.get(0), "/rack0");
        assertEquals(knownBookies.size(), 1);
        assertEquals(knownBookies.get(bookie1.toBookieId()).getNetworkLocation(), "/rack0");

        bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie2.toString());
        store.put("/ledgers/available/" + bookie2, serviceInfoSerde.serialize("", bookieServiceInfo),
                Optional.of(-1L)).get();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(
                () -> "/rack1".equals(mapping.resolve(List.of(bookie2.getHostName())).get(0)));

        racks = mapping
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()))
                .stream().filter(Objects::nonNull).toList();
        assertEquals(racks.size(), 2);
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(knownBookies.size(), 2);
        assertEquals(knownBookies.get(bookie1.toBookieId()).getNetworkLocation(), "/rack0");
        assertEquals(knownBookies.get(bookie2.toBookieId()).getNetworkLocation(), "/rack1");

        bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie3.toString());
        store.put("/ledgers/available/" + bookie3, serviceInfoSerde.serialize("", bookieServiceInfo),
                Optional.of(-1L)).get();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(
                () -> knownBookies.containsKey(bookie3.toBookieId()));

        racks = mapping
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()))
                .stream().filter(Objects::nonNull).toList();
        assertEquals(racks.size(), 2);
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(knownBookies.size(), 3);
        assertEquals(knownBookies.get(bookie1.toBookieId()).getNetworkLocation(), "/rack0");
        assertEquals(knownBookies.get(bookie2.toBookieId()).getNetworkLocation(), "/rack1");
        assertEquals(knownBookies.get(bookie3.toBookieId()).getNetworkLocation(), "/default-rack");

        //remove bookie2 rack, the bookie2 rack should be /default-rack
        data = "{\"group1\": {\"" + bookie1
                + "\": {\"rack\": \"/rack0\", \"hostname\": \"bookie1.example.com\"}}}";
        store.put(BOOKIE_INFO_ROOT_PATH, data.getBytes(), Optional.empty()).join();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(
                () -> knownBookies.get(bookie2.toBookieId()).getNetworkLocation().equals("/default-rack"));

        racks = mapping
                .resolve(Lists.newArrayList(bookie1.getHostName(), bookie2.getHostName(), bookie3.getHostName()))
                .stream().filter(Objects::nonNull).toList();
        assertEquals(racks.size(), 1);
        assertEquals(racks.get(0), "/rack0");
        assertEquals(knownBookies.size(), 3);
        assertEquals(knownBookies.get(bookie1.toBookieId()).getNetworkLocation(), "/rack0");
        assertEquals(knownBookies.get(bookie2.toBookieId()).getNetworkLocation(), "/default-rack");
        assertEquals(knownBookies.get(bookie3.toBookieId()).getNetworkLocation(), "/default-rack");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNoDeadlockWithRackawarePolicy() throws Exception {
        MetadataStore metadataStore = mock(MetadataStore.class);
        MetadataCache<BookiesRackConfiguration> cache = mock(MetadataCache.class);
        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", bookie1.toString(), BookieInfo.builder().rack("/region/rack0").build());
        when(cache.get(BOOKIE_INFO_ROOT_PATH)).thenReturn(CompletableFuture.completedFuture(Optional.of(racks)));
        when(metadataStore.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        var metadataListener = new AtomicReference<Consumer<Notification>>();
        when(metadataStore.registerCancellableListener(any())).thenAnswer(invocation -> {
            metadataListener.set(invocation.getArgument(0));
            return (Runnable) () -> { };
        });
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, metadataStore);

        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);

        @Cleanup("stop")
        HashedWheelTimer timer = getTestHashedWheelTimer(bkClientConf);
        RackawareEnsemblePlacementPolicy repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(bkClientConf, Optional.of(mapping), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        mapping.registerRackChangeListener(repp);

        // Both loops below log on every call, hundreds of thousands of lines in all: quiet those loggers
        Map<String, Level> quietLevels = Map.of(
                BookieRackAffinityMapping.class.getName(), Level.WARN,
                NetworkTopologyImpl.class.getName(), Level.WARN,
                "org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy", Level.ERROR);
        Map<String, Level> originalLevels = new HashMap<>();
        quietLevels.keySet().forEach(name -> originalLevels.put(name, LogManager.getLogger(name).getLevel()));
        Configurator.setLevel(quietLevels);
        @Cleanup
        AutoCloseable restoreLevels = () -> Configurator.setLevel(originalLevels);

        @Cleanup("shutdownNow")
        ExecutorService executor1 = Executors.newSingleThreadExecutor();
        @Cleanup("shutdownNow")
        ExecutorService executor2 = Executors.newSingleThreadExecutor();

        var rackUpdates = executor1.submit(() -> {
            var notification = new Notification(NotificationType.Modified, BOOKIE_INFO_ROOT_PATH);
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 2_000) {
                metadataListener.get().accept(notification);
            }
        });

        var topologyUpdates = executor2.submit(() -> {
            Set<BookieId> writableBookies = Set.of(bookie1.toBookieId());
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 2_000) {
                repp.onClusterChanged(writableBookies, Collections.emptySet());
                repp.onClusterChanged(Collections.emptySet(), Collections.emptySet());
            }
        });

        rackUpdates.get(5, TimeUnit.SECONDS);
        topologyUpdates.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testZKEventListenersOrdering() throws Exception {
        BookieId bookieId = BookieId.parse("bookie-id");
        var racks = new BookiesRackConfiguration();
        racks.updateBookie("group", bookieId.toString(), BookieInfo.builder().rack("/rack0").build());
        store.put(BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(racks), Optional.empty()).join();

        var registrationClient = mock(RegistrationClient.class);
        when(registrationClient.getBookieServiceInfo(bookieId))
                .thenReturn(CompletableFuture.failedFuture(new BKBookieHandleNotAvailableException()));
        var resolver = new DefaultBookieAddressResolver(registrationClient);
        var conf = new ClientConfiguration();
        conf.setEnforceMinNumRacksPerWriteQuorum(true);
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        var mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(resolver);
        mapping.setConf(conf);

        @Cleanup("stop")
        var timer = getTestHashedWheelTimer(conf);
        @Cleanup("uninitalize")
        var policy = new RackawareEnsemblePlacementPolicy();
        policy.initialize(conf, Optional.of(mapping), timer, DISABLE_ALL, NullStatsLogger.INSTANCE, resolver);
        mapping.registerRackChangeListener(policy);

        // BookKeeper can receive the writable-bookie event before its rack resolver receives the same event.
        var serviceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie1.toString());
        when(registrationClient.getBookieServiceInfo(bookieId))
                .thenReturn(CompletableFuture.completedFuture(new Versioned<>(serviceInfo, Version.NEW)));
        policy.onClusterChanged(Set.of(bookieId), Set.of());
        expectThrows(BKNotEnoughBookiesException.class,
                () -> policy.newEnsemble(1, 1, 1, Map.of(), new HashSet<>()));

        var available = ArgumentCaptor.forClass(RegistrationClient.RegistrationListener.class);
        verify(registrationClient).watchWritableBookies(available.capture());
        available.getValue().onBookiesChanged(new Versioned<>(Set.of(bookieId), Version.NEW));

        Awaitility.await().ignoreException(BKNotEnoughBookiesException.class).untilAsserted(() -> assertEquals(
                policy.newEnsemble(1, 1, 1, Map.of(), new HashSet<>()).getResult(), List.of(bookieId)));
    }

    private static HashedWheelTimer getTestHashedWheelTimer(ClientConfiguration bkClientConf) {
        return new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                bkClientConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                bkClientConf.getTimeoutTimerNumTicks());
    }
}
