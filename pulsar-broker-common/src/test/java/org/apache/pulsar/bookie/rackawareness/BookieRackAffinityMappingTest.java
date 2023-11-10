/**
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
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

    private BookieSocketAddress BOOKIE1 = null;
    private BookieSocketAddress BOOKIE2 = null;
    private BookieSocketAddress BOOKIE3 = null;
    private MetadataStore store;

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
        BOOKIE1 = new BookieSocketAddress("127.0.0.1:3181");
        BOOKIE2 = new BookieSocketAddress("127.0.0.2:3181");
        BOOKIE3 = new BookieSocketAddress("127.0.0.3:3181");
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        store.close();
    }

    @Test
    public void testBasic() throws Exception {
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"/rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"/rack1\", \"hostname\": \"bookie2.example.com\"}}}";
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(), Optional.empty()).join();

        // Case1: ZKCache is given
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));

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
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"/\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"\", \"hostname\": \"bookie2.example.com\"}}}";

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(), Optional.empty()).join();

        // Case1: ZKCache is given
        BookieRackAffinityMapping mapping1 = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf1 = new ClientConfiguration();
        bkClientConf1.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping1.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping1.setConf(bkClientConf1);
        List<String> racks = mapping1
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));

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

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("/rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("/rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
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

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertNull(racks.get(2));

        // add info for BOOKIE3 and check if the mapping picks up the change
        Map<BookieSocketAddress, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put("group2", secondaryBookieGroup);
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();
        Awaitility.await().untilAsserted(() -> {
            List<String> r = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
            assertEquals(r.get(0), "/rack0");
            assertEquals(r.get(1), "/rack1");
            assertEquals(r.get(2), "/rack0");
        });
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "{}".getBytes(),
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
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"/rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"/rack1\", \"hostname\": \"bookie2.example.com\"}}}";
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                data.getBytes(), Optional.empty()).join();

        // Case1: ZKCache is given
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        Field field = BookieRackAffinityMapping.class.getDeclaredField("racksWithHost");
        field.setAccessible(true);

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        PulsarRegistrationClient pulsarRegistrationClient =
                new PulsarRegistrationClient(store, "/ledgers");
        DefaultBookieAddressResolver defaultBookieAddressResolver =
                new DefaultBookieAddressResolver(pulsarRegistrationClient);

        mapping.setBookieAddressResolver(defaultBookieAddressResolver);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()))
                .stream().filter(Objects::nonNull).collect(Collectors.toList());
        assertEquals(racks.size(), 0);

        HashedWheelTimer timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                bkClientConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                bkClientConf.getTimeoutTimerNumTicks());

        RackawareEnsemblePlacementPolicy repp = new RackawareEnsemblePlacementPolicy();
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
        bookieIds.add(BOOKIE1.toBookieId());

        Field field2 = BookieServiceInfoSerde.class.getDeclaredField("INSTANCE");
        field2.setAccessible(true);
        BookieServiceInfoSerde serviceInfoSerde = (BookieServiceInfoSerde) field2.get(null);

        BookieServiceInfo bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(BOOKIE1.toString());
        store.put("/ledgers/available/" + BOOKIE1, serviceInfoSerde.serialize("", bookieServiceInfo),
                Optional.of(-1L)).get();

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> ((BookiesRackConfiguration)field.get(mapping)).get("group1").size() == 1);
        racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()))
                .stream().filter(Objects::nonNull).collect(Collectors.toList());
        assertEquals(racks.size(), 1);
        assertEquals(racks.get(0), "/rack0");
        assertEquals(knownBookies.size(), 1);
        assertEquals(knownBookies.get(BOOKIE1.toBookieId()).getNetworkLocation(), "/rack0");

        bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(BOOKIE2.toString());
        store.put("/ledgers/available/" + BOOKIE2, serviceInfoSerde.serialize("", bookieServiceInfo),
                Optional.of(-1L)).get();
        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> ((BookiesRackConfiguration)field.get(mapping)).get("group1").size() == 2);

        racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()))
                .stream().filter(Objects::nonNull).collect(Collectors.toList());
        assertEquals(racks.size(), 2);
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(knownBookies.size(), 2);
        assertEquals(knownBookies.get(BOOKIE1.toBookieId()).getNetworkLocation(), "/rack0");
        assertEquals(knownBookies.get(BOOKIE2.toBookieId()).getNetworkLocation(), "/rack1");

        bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(BOOKIE3.toString());
        store.put("/ledgers/available/" + BOOKIE3, serviceInfoSerde.serialize("", bookieServiceInfo),
                Optional.of(-1L)).get();
        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> ((BookiesRackConfiguration)field.get(mapping)).get("group1").size() == 2);

        racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()))
                .stream().filter(Objects::nonNull).collect(Collectors.toList());
        assertEquals(racks.size(), 2);
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(knownBookies.size(), 3);
        assertEquals(knownBookies.get(BOOKIE1.toBookieId()).getNetworkLocation(), "/rack0");
        assertEquals(knownBookies.get(BOOKIE2.toBookieId()).getNetworkLocation(), "/rack1");
        assertEquals(knownBookies.get(BOOKIE3.toBookieId()).getNetworkLocation(), "/default-rack");

        timer.stop();
    }

    @Test
    public void testNoDeadlockWithRackawarePolicy() throws Exception {
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);

        @Cleanup("stop")
        HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                bkClientConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                bkClientConf.getTimeoutTimerNumTicks());

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
                        new Notification(NotificationType.Modified, BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH);
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
            writableBookies.add(BOOKIE1.toBookieId());
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 2_000) {
                repp.onClusterChanged(writableBookies, Collections.emptySet());
                repp.onClusterChanged(Collections.emptySet(), Collections.emptySet());
            }
            count.countDown();
        });

        assertTrue(count.await(3, TimeUnit.SECONDS));
    }
}
