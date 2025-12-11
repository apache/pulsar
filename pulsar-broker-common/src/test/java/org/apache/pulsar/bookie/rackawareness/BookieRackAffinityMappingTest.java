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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.DefaultBookieAddressResolver;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.BookieServiceInfoUtils;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.bookkeeper.BookieServiceInfoSerde;
import org.apache.pulsar.metadata.bookkeeper.PulsarRegistrationClient;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
        Field field = BookieRackAffinityMapping.class.getDeclaredField("racksWithHost");
        field.setAccessible(true);

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

        Set<BookieId> bookieIds = new HashSet<>();
        bookieIds.add(bookie1.toBookieId());

        Field field2 = BookieServiceInfoSerde.class.getDeclaredField("INSTANCE");
        field2.setAccessible(true);
        BookieServiceInfoSerde serviceInfoSerde = (BookieServiceInfoSerde) field2.get(null);

        BookieServiceInfo bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie1.toString());
        store.put("/ledgers/available/" + bookie1, serviceInfoSerde.serialize("", bookieServiceInfo),
                Optional.of(-1L)).get();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(
                () -> ((BookiesRackConfiguration) field.get(mapping)).get("group1").size() == 1);
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
                () -> ((BookiesRackConfiguration) field.get(mapping)).get("group1").size() == 2);

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
                () -> ((BookiesRackConfiguration) field.get(mapping)).get("group1").size() == 2);

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
                () -> ((BookiesRackConfiguration) field.get(mapping)).get("group1").size() == 1);

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

    @Test
    public void testNoDeadlockWithRackawarePolicy() throws Exception {
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

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

        @Cleanup("shutdownNow")
        ExecutorService executor1 = Executors.newSingleThreadExecutor();
        @Cleanup("shutdownNow")
        ExecutorService executor2 = Executors.newSingleThreadExecutor();

        CountDownLatch count = new CountDownLatch(2);

        executor1.submit(() -> {
            try {
                Method handleUpdates =
                        BookieRackAffinityMapping.class.getDeclaredMethod("handleUpdates", Notification.class);
                handleUpdates.setAccessible(true);
                Notification n =
                        new Notification(NotificationType.Modified, BOOKIE_INFO_ROOT_PATH);
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < 2_000) {
                    handleUpdates.invoke(mapping, n);
                }
                count.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        executor2.submit(() -> {
            Set<BookieId> writableBookies = new HashSet<>();
            writableBookies.add(bookie1.toBookieId());
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 2_000) {
                repp.onClusterChanged(writableBookies, Collections.emptySet());
                repp.onClusterChanged(Collections.emptySet(), Collections.emptySet());
            }
            count.countDown();
        });

        assertTrue(count.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testZKEventListenersOrdering() throws Exception {
        @Cleanup
        PulsarRegistrationClient pulsarRegistrationClient =
                new PulsarRegistrationClient(store, "/ledgers");
        DefaultBookieAddressResolver defaultBookieAddressResolver =
                new DefaultBookieAddressResolver(pulsarRegistrationClient);
        // Create and configure the mapping
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        mapping.setBookieAddressResolver(defaultBookieAddressResolver);
        mapping.setConf(bkClientConf);

        // Create RackawareEnsemblePlacementPolicy and initialize it
        @Cleanup("stop")
        HashedWheelTimer timer = getTestHashedWheelTimer(bkClientConf);
        RackawareEnsemblePlacementPolicy repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(bkClientConf, Optional.of(mapping), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, defaultBookieAddressResolver);
        mapping.registerRackChangeListener(repp);

        // Create a BookieWatcherImpl instance via reflection
        Class<?> watcherClazz = Class.forName("org.apache.bookkeeper.client.BookieWatcherImpl");
        Constructor<?> constructor = watcherClazz.getDeclaredConstructor(
                ClientConfiguration.class,
                EnsemblePlacementPolicy.class,
                RegistrationClient.class,
                BookieAddressResolver.class,
                StatsLogger.class);
        constructor.setAccessible(true);
        Object watcher = constructor.newInstance(
                bkClientConf,
                repp,
                pulsarRegistrationClient,
                defaultBookieAddressResolver,
                NullStatsLogger.INSTANCE
        );
        Method initMethod = watcherClazz.getDeclaredMethod("initialBlockingBookieRead");
        initMethod.setAccessible(true);
        initMethod.invoke(watcher);

        // Prepare a BookiesRackConfiguration that maps bookie1 -> /rack0
        BookieInfo bi = BookieInfo.builder().rack("/rack0").build();
        BookiesRackConfiguration racks = new BookiesRackConfiguration();
        racks.updateBookie("group1", bookie1.toString(), bi);

        // Create a mock cache for racks /bookies
        MetadataCache<BookiesRackConfiguration> mockCache = mock(MetadataCache.class);
        Field f = BookieRackAffinityMapping.class.getDeclaredField("bookieMappingCache");
        f.setAccessible(true);
        f.set(mapping, mockCache);
        when(mockCache.get(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(racks)));

        // Inject the writable bookie into PulsarRegistrationClient
        Field writableField = PulsarRegistrationClient.class.getDeclaredField("writableBookieInfo");
        writableField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<BookieId, Versioned<BookieServiceInfo>> writableBookieInfo =
                (Map<BookieId, Versioned<BookieServiceInfo>>) writableField.get(pulsarRegistrationClient);
        writableBookieInfo.put(
                bookie1.toBookieId(),
                new Versioned<>(BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookie1.toString()), Version.NEW)
        );

        // watcher.processWritableBookiesChanged runs FIRST triggering Rackaware ensemble policy listener → incorrect
        // ordering
        Method procMethod =
                watcherClazz.getDeclaredMethod("processWritableBookiesChanged", java.util.Set.class);
        procMethod.setAccessible(true);
        Set<BookieId> ids = new HashSet<>();
        ids.add(bookie1.toBookieId());
        procMethod.invoke(watcher, ids);

        // BookieRackAffinityMapping rack mapping update runs SECOND → delayed rack info
        Method processRackUpdateMethod = BookieRackAffinityMapping.class.getDeclaredMethod("processRackUpdate",
                BookiesRackConfiguration.class, List.class);
        processRackUpdateMethod.setAccessible(true);
        processRackUpdateMethod.invoke(mapping, racks, List.of(bookie1.toBookieId()));

        // mapping.resolve now has correct rack (mapping is updated)
//        List<String> resolved = mapping.resolve(Lists.newArrayList(bookie1.getHostName()));
//        assertEquals(resolved.get(0), "/rack0",
//                "Expected mapping to have /rack0 after update before watcher ran");

        // -------------------
        // NOW CHECK REPP INTERNAL STATE
        // -------------------
        // BookieNode.getNetworkLocation()
        Class<?> clazz1 = Class.forName("org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy");
        Field field1 = clazz1.getDeclaredField("knownBookies");
        field1.setAccessible(true);
        Map<BookieId, BookieNode> knownBookies = (Map<BookieId, BookieNode>) field1.get(repp);
        BookieNode bn = knownBookies.get(bookie1.toBookieId());
        // Rack info update is delayed but because of new callback the rackinfo on ensemble policy should be updated.
        assertEquals(bn.getNetworkLocation(), "/rack0",
                "Network location should match /rack0 on bookie");
    }

    private static HashedWheelTimer getTestHashedWheelTimer(ClientConfiguration bkClientConf) {
        return new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                bkClientConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                bkClientConf.getTimeoutTimerNumTicks());
    }
}
