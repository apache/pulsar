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

import static org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.RACK_CONFIGURATION_SNAPSHOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.netty.util.HashedWheelTimer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.proto.BookieAddressResolver.BookieIdNotResolvedException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.VersionedConfiguration;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IsolatedBookieEnsemblePlacementPolicyTest {

    private static final String BOOKIE1 = "127.0.0.1:3181";
    private static final String BOOKIE2 = "127.0.0.2:3181";
    private static final String BOOKIE3 = "127.0.0.3:3181";
    private static final String BOOKIE4 = "127.0.0.4:3181";
    private static final String BOOKIE5 = "127.0.0.5:3181";
    private static final int RACK_CONFIGURATION_LOAD_TIMEOUT_SECONDS = 30;
    private MetadataStore store;
    private final List<IsolatedBookieEnsemblePlacementPolicy> policies = new ArrayList<>();

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();
    Set<BookieId> writableBookies = new HashSet<>();
    Set<BookieId> readOnlyBookies = new HashSet<>();
    List<String> isolationGroups = new ArrayList<>();

    HashedWheelTimer timer;

    @BeforeMethod
    public void setUp() throws Exception {
        timer = new HashedWheelTimer();
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());

        writableBookies.add(new BookieSocketAddress(BOOKIE1).toBookieId());
        writableBookies.add(new BookieSocketAddress(BOOKIE2).toBookieId());
        writableBookies.add(new BookieSocketAddress(BOOKIE3).toBookieId());
        writableBookies.add(new BookieSocketAddress(BOOKIE4).toBookieId());
        isolationGroups.add("group1");
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        policies.forEach(IsolatedBookieEnsemblePlacementPolicy::uninitalize);
        policies.clear();
        writableBookies.clear();
        isolationGroups.clear();
        store.close();
        timer.stop();
    }

    @Test
    public void testNonRegionBookie() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put("group1", mainBookieGroup);


        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy.newEnsemble(2, 2, 2,
                Collections.emptyMap(), new HashSet<>()).getResult();
        assertFalse(ensemble.contains(new BookieSocketAddress(BOOKIE3).toBookieId()));
        assertFalse(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMetadataStoreCases() throws Exception {
        String path = BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
        store.close();
        store = mock(MetadataStoreExtended.class);
        MetadataCacheImpl<BookiesRackConfiguration> cache = mock(MetadataCacheImpl.class);
        doReturn(cache).when(store).getMetadataCache(BookiesRackConfiguration.class);
        var configuration = new BookiesRackConfiguration();
        for (BookieId bookie : writableBookies) {
            configuration.updateBookie("group1", bookie.toString(), BookieInfo.builder().rack("rack0").build());
        }
        var updated = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var deleted = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        when(cache.get(path)).thenReturn(CompletableFuture.completedFuture(Optional.of(configuration)))
                .thenReturn(updated).thenReturn(deleted);

        var conf = new ClientConfiguration();
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        conf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        var policy = createIsolationPolicy(conf);
        policy.onClusterChanged(writableBookies, readOnlyBookies);
        var groups = Pair.of(Set.of("group1"), Set.<String>of());
        ArgumentCaptor<Consumer<Notification>> listener = ArgumentCaptor.captor();
        verify(store).registerCancellableListener(listener.capture());

        listener.getValue().accept(new Notification(NotificationType.Modified, path));
        assertThat(policy.getExcludedBookiesWithIsolationGroups(2, groups)).isEmpty();
        configuration.updateBookie("group2", BOOKIE3, BookieInfo.builder().rack("rack1").build());
        updated.complete(Optional.of(configuration));
        Awaitility.await().untilAsserted(() -> assertThat(policy.getExcludedBookiesWithIsolationGroups(2, groups))
                .containsExactly(BookieId.parse(BOOKIE3)));

        listener.getValue().accept(new Notification(NotificationType.Deleted, path));
        assertThat(policy.getExcludedBookiesWithIsolationGroups(2, groups)).containsExactly(BookieId.parse(BOOKIE3));
        deleted.complete(Optional.empty());
        Awaitility.await().untilAsserted(() -> assertThat(policy.getExcludedBookiesWithIsolationGroups(2, groups))
                .isEmpty());
        verify(cache, times(3)).get(path);
    }

    @Test
    public void testBasic() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy.newEnsemble(3, 3, 2,
                Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));

        ensemble = isolationPolicy.newEnsemble(1, 1, 1,
                Collections.emptyMap(), new HashSet<>()).getResult();
        assertFalse(ensemble.contains(new BookieSocketAddress(BOOKIE3).toBookieId()));

        try {
            isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(),
                    new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        Set<BookieId> bookieToExclude = new HashSet<>();
        bookieToExclude.add(new BookieSocketAddress(BOOKIE1).toBookieId());
        try {
            isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(),
                    bookieToExclude).getResult();
        } catch (BKNotEnoughBookiesException e) {
            Assert.assertEquals(e.getMessage(), "Not enough non-faulty bookies available");
        }


        secondaryBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack0").build());
        bookieMapping.put("group2", secondaryBookieGroup);

        updateBookieInfo(isolationPolicy, jsonMapper.writeValueAsBytes(bookieMapping));

        ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(),
                null).getResult();

        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(),
                    new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        try {
            isolationPolicy.replaceBookie(3, 3, 3, Collections.emptyMap(), ensemble,
                    new BookieSocketAddress(BOOKIE5).toBookieId(), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }
        bookieToExclude = new HashSet<>();
        bookieToExclude.add(new BookieSocketAddress(BOOKIE1).toBookieId());

        ensemble = isolationPolicy.newEnsemble(1, 1, 1, Collections.emptyMap(),
                bookieToExclude).getResult();
        BookieId chosenBookie = isolationPolicy.replaceBookie(1, 1, 1, Collections.emptyMap(),
                ensemble, ensemble.get(0), new HashSet<>()).getResult();
        assertEquals(new BookieSocketAddress(BOOKIE1).toBookieId(), chosenBookie);
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());

        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}, \"" + BOOKIE4
                + "\": {\"rack\": \"rack2\", \"hostname\": \"bookie4.example.com\"}}}";

        updateBookieInfo(isolationPolicy, data.getBytes(StandardCharsets.UTF_8));

        List<BookieId> ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(),
                new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(),
                    new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }
    }

    @Test
    public void testBookieInfoChange() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> mainBookieGroup = new HashMap<>();
        Map<String, BookieInfo> secondaryBookieGroup = new HashMap<>();

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());
        secondaryBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());
        secondaryBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack2").build());

        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy.newEnsemble(2, 2, 2,
                Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        mainBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        secondaryBookieGroup.remove(BOOKIE3);
        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        updateBookieInfo(isolationPolicy, jsonMapper.writeValueAsBytes(bookieMapping));

        ensemble = isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(),
                new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE3).toBookieId()));
    }

    @Test
    public void testNoIsolationGroup() throws Exception {
        // BOOKIE4 belongs to no group, so it stays available to ledgers whose metadata has empty isolation groups
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}}}";

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(StandardCharsets.UTF_8),
                Optional.empty()).join();

        Awaitility.await().until(() -> store.exists(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH).join());

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());

        BookieId bookie1Id = new BookieSocketAddress(BOOKIE1).toBookieId();
        BookieId bookie2Id = new BookieSocketAddress(BOOKIE2).toBookieId();
        BookieId bookie3Id = new BookieSocketAddress(BOOKIE3).toBookieId();
        BookieId bookie4Id = new BookieSocketAddress(BOOKIE4).toBookieId();
        // when we set strictBookieAffinityEnabled=true and
        // some namespace not set ISOLATION_BOOKIE_GROUPS there will set "" by default.
        Map<String, Object> placementPolicyProperties1 = new HashMap<>();
        placementPolicyProperties1.put(
                IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "");
        placementPolicyProperties1.put(
                IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "");
        EnsemblePlacementPolicyConfig policyConfig = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                placementPolicyProperties1
        );
        Map<String, byte[]> customMetadata1 = new HashMap<>();
        customMetadata1.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());

        BookieId replaceBookie1 = isolationPolicy.replaceBookie(3, 3, 3, customMetadata1,
                Arrays.asList(bookie1Id, bookie2Id, bookie3Id), bookie3Id, null).getResult();
        assertEquals(replaceBookie1, bookie4Id);

        // when ISOLATION_BOOKIE_GROUPS miss.
        Map<String, Object> placementPolicyProperties2 = new HashMap<>();
        EnsemblePlacementPolicyConfig policyConfig2 = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                placementPolicyProperties2
        );
        Map<String, byte[]> customMetadata2 = new HashMap<>();
        customMetadata2.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig2.encode());
        BookieId replaceBookie2 = isolationPolicy.replaceBookie(3, 3, 3, customMetadata2,
                Arrays.asList(bookie1Id, bookie2Id, bookie3Id), bookie3Id, null).getResult();
        assertEquals(replaceBookie2, bookie4Id);
    }

    /**
     * A client without default isolation groups must still apply the groups carried by the ledger metadata, and must
     * not isolate ledgers that carry none.
     */
    @Test
    public void testMetadataIsolationGroupsWithoutDefaultGroups() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        group1.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());
        Map<String, BookieInfo> group2 = new HashMap<>();
        group2.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());
        group2.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());
        bookieMapping.put("group1", group1);
        bookieMapping.put("group2", group2);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        Map<String, Object> placementPolicyProperties = new HashMap<>();
        placementPolicyProperties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group2");
        EnsemblePlacementPolicyConfig policyConfig = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class, placementPolicyProperties);
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());

        // Bookies are picked randomly without isolation, so repeat to make a lucky pick very unlikely
        for (int i = 0; i < 20; i++) {
            List<BookieId> ensemble = isolationPolicy.newEnsemble(2, 2, 2, customMetadata, new HashSet<>())
                    .getResult();
            assertEquals(new HashSet<>(ensemble), Set.of(new BookieSocketAddress(BOOKIE3).toBookieId(),
                    new BookieSocketAddress(BOOKIE4).toBookieId()));
        }

        List<BookieId> ensemble = isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>())
                .getResult();
        assertEquals(new HashSet<>(ensemble), writableBookies);
    }

    /**
     * validates overlapped bookies between default-groups and isolated-groups.
     *
     * <pre>
     * a. default-group has all 5 bookies.
     * b. 3 of the default-group bookies have been added to isolated-group without being removed from default-group.
     * c. isolated-policy-placement should be identify those 3 overlapped bookies and remove them from excluded bookies.
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testOverlappedBookies() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> defaultBookieGroup = new HashMap<>();
        final String isolatedGroup = "isolatedGroup";

        defaultBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        defaultBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE5, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> isolatedBookieGroup = new HashMap<>();
        isolatedBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack1").build());
        isolatedBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());
        isolatedBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put("default", defaultBookieGroup);
        bookieMapping.put(isolatedGroup, isolatedBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy
                .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));
    }

    @Test
    public void testSecondaryIsolationGroupsBookies() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> defaultBookieGroup = new HashMap<>();
        final String isolatedGroup = "primaryGroup";
        final String secondaryIsolatedGroup = "secondaryGroup";

        defaultBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        defaultBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE5, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> primaryIsolatedBookieGroup = new HashMap<>();
        // Use the same rack as in the default group to avoid non-deterministic rack
        // resolution when the same bookie appears in multiple groups with different racks.
        primaryIsolatedBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());

        Map<String, BookieInfo> secondaryIsolatedBookieGroup = new HashMap<>();
        secondaryIsolatedBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());
        secondaryIsolatedBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put("default", defaultBookieGroup);
        bookieMapping.put(isolatedGroup, primaryIsolatedBookieGroup);
        bookieMapping.put(secondaryIsolatedGroup, secondaryIsolatedBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                secondaryIsolatedGroup);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy
                .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));
    }

    @DataProvider
    public Object[][] placementOperations() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "placementOperations")
    public void testPlacementDuringClusterChange(boolean replaceBookie) throws Exception {
        BookieId primaryBookie = BookieId.parse(BOOKIE1);
        BookieId leavingPrimaryBookie = BookieId.parse(BOOKIE2);
        BookieId ungroupedBookie = BookieId.parse(BOOKIE3);
        BookieId joiningPrimaryBookie = BookieId.parse(BOOKIE4);
        Map<String, Map<String, BookieInfo>> bookieMapping = Map.of("group1", Map.of(
                BOOKIE1, BookieInfo.builder().rack("rack0").build(),
                BOOKIE2, BookieInfo.builder().rack("rack1").build(),
                BOOKIE4, BookieInfo.builder().rack("rack1").build()));
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        CountDownLatch clusterChangePaused = new CountDownLatch(1);
        CountDownLatch finishClusterChange = new CountDownLatch(1);
        AtomicReference<Thread> clusterChangeThread = new AtomicReference<>();
        BookieAddressResolver resolver = bookieId -> {
            if (bookieId.equals(joiningPrimaryBookie) && Thread.currentThread() == clusterChangeThread.get()) {
                // onClusterChanged holds the write lock here and has already removed the old primary bookie.
                clusterChangePaused.countDown();
                try {
                    if (!finishClusterChange.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to finish the cluster change");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new BookieIdNotResolvedException(bookieId, e);
                }
            }
            return BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER.resolve(bookieId);
        };
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1");
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, resolver);
        isolationPolicy.getInitialRackConfigurationLoadFuture().get(30, TimeUnit.SECONDS);
        isolationPolicy.onClusterChanged(Set.of(primaryBookie, leavingPrimaryBookie, ungroupedBookie), Set.of());

        FutureTask<Set<BookieId>> clusterChange = new FutureTask<>(() -> {
            clusterChangeThread.set(Thread.currentThread());
            return isolationPolicy.onClusterChanged(Set.of(primaryBookie, joiningPrimaryBookie, ungroupedBookie),
                    Set.of());
        });
        Set<BookieId> excludedBookies = new HashSet<>();
        FutureTask<List<BookieId>> placement = new FutureTask<>(() -> {
            if (replaceBookie) {
                return List.of(isolationPolicy.replaceBookie(2, 2, 2, Map.of(),
                        List.of(primaryBookie, leavingPrimaryBookie), leavingPrimaryBookie, excludedBookies)
                        .getResult());
            }
            return isolationPolicy.newEnsemble(2, 2, 2, Map.of(), excludedBookies).getResult();
        });
        Thread updateThread = new Thread(clusterChange, "isolation-cluster-change");
        Thread placementThread = new Thread(placement, "isolation-placement");
        try {
            updateThread.start();
            assertThat(clusterChangePaused.await(10, TimeUnit.SECONDS)).as("cluster update reached address resolution")
                    .isTrue();
            placementThread.start();
            // Both versions eventually wait for the write lock. Before the fix, isolation has already used
            // the incomplete topology by then and incorrectly allowed the ungrouped fallback.
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> placementThread.getState() == Thread.State.WAITING);
            finishClusterChange.countDown();
            assertThat(clusterChange.get(10, TimeUnit.SECONDS)).containsExactly(leavingPrimaryBookie);
            List<BookieId> result = placement.get(10, TimeUnit.SECONDS);
            assertThat(excludedBookies).as("a complete primary group must keep ungrouped bookies excluded")
                    .contains(ungroupedBookie);
            if (replaceBookie) {
                assertThat(result).containsExactly(joiningPrimaryBookie);
            } else {
                assertThat(result).containsExactlyInAnyOrder(primaryBookie, joiningPrimaryBookie);
            }
        } finally {
            finishClusterChange.countDown();
            updateThread.join(TimeUnit.SECONDS.toMillis(10));
            placementThread.join(TimeUnit.SECONDS.toMillis(10));
            isolationPolicy.uninitalize();
            assertThat(updateThread.isAlive()).as("cluster update thread stopped").isFalse();
            assertThat(placementThread.isAlive()).as("placement thread stopped").isFalse();
        }
    }

    @DataProvider
    public Object[][] placementClusterChanges() {
        return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
    }

    @Test(dataProvider = "placementClusterChanges")
    public void testClusterChangeAfterIsolationExclusions(boolean replaceBookie, boolean primaryBookieLeaves)
            throws Exception {
        BookieId primaryBookie = BookieId.parse(BOOKIE1);
        BookieId changingPrimaryBookie = BookieId.parse(BOOKIE2);
        BookieId ungroupedBookie = BookieId.parse(BOOKIE3);
        BookieId offlinePrimaryBookie = BookieId.parse(BOOKIE4);
        Map<String, Map<String, BookieInfo>> bookieMapping = Map.of("group1", Map.of(
                BOOKIE1, BookieInfo.builder().rack("rack0").build(),
                BOOKIE2, BookieInfo.builder().rack("rack1").build(),
                BOOKIE4, BookieInfo.builder().rack("rack1").build()));
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1");
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        Set<BookieId> initialBookies = primaryBookieLeaves
                ? Set.of(primaryBookie, changingPrimaryBookie, ungroupedBookie)
                : Set.of(primaryBookie, changingPrimaryBookie);
        isolationPolicy.onClusterChanged(initialBookies, Set.of());

        CountDownLatch exclusionsCalculated = new CountDownLatch(1);
        CountDownLatch finishPlacement = new CountDownLatch(1);
        Set<BookieId> excludedBookies = new HashSet<>() {
            private boolean paused;

            @Override
            public boolean addAll(Collection<? extends BookieId> bookies) {
                boolean changed = super.addAll(bookies);
                if (!paused) {
                    paused = true;
                    // The caller's exclusion set is updated after isolation calculation and before BK selection.
                    exclusionsCalculated.countDown();
                    try {
                        if (!finishPlacement.await(30, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Timed out waiting to finish placement");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting to finish placement", e);
                    }
                }
                return changed;
            }
        };
        if (!primaryBookieLeaves) {
            // Caller exclusions do not relax isolation: the joining ungrouped bookie must remain ineligible.
            excludedBookies.add(changingPrimaryBookie);
        }
        BookieId bookieToReplace = primaryBookieLeaves ? offlinePrimaryBookie : changingPrimaryBookie;
        FutureTask<List<BookieId>> placement = new FutureTask<>(() -> {
            if (replaceBookie) {
                return List.of(isolationPolicy.replaceBookie(2, 2, 2, Map.of(),
                        List.of(primaryBookie, bookieToReplace), bookieToReplace, excludedBookies).getResult());
            }
            return isolationPolicy.newEnsemble(2, 2, 2, Map.of(), excludedBookies).getResult();
        });
        Set<BookieId> updatedBookies = primaryBookieLeaves
                ? Set.of(primaryBookie, ungroupedBookie)
                : Set.of(primaryBookie, changingPrimaryBookie, ungroupedBookie);
        FutureTask<Set<BookieId>> clusterChange = new FutureTask<>(
                () -> isolationPolicy.onClusterChanged(updatedBookies, Set.of()));
        Thread placementThread = new Thread(placement, "isolation-placement-before-selection");
        Thread updateThread = new Thread(clusterChange, "isolation-cluster-change-before-selection");
        try {
            placementThread.start();
            assertThat(exclusionsCalculated.await(10, TimeUnit.SECONDS)).as("isolation exclusions calculated")
                    .isTrue();
            updateThread.start();
            // With the fix the writer queues behind placement. Before the fix the update completes in the gap.
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> clusterChange.isDone() || updateThread.getState() == Thread.State.WAITING);
            finishPlacement.countDown();
            if (primaryBookieLeaves) {
                List<BookieId> result = placement.get(10, TimeUnit.SECONDS);
                if (replaceBookie) {
                    assertThat(result).containsExactly(changingPrimaryBookie);
                } else {
                    assertThat(result).containsExactlyInAnyOrder(primaryBookie, changingPrimaryBookie);
                }
            } else {
                assertThatThrownBy(() -> placement.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(BKNotEnoughBookiesException.class);
            }
            Set<BookieId> expectedDeadBookies = primaryBookieLeaves ? Set.of(changingPrimaryBookie) : Set.of();
            assertThat(clusterChange.get(10, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderElementsOf(expectedDeadBookies);
        } finally {
            finishPlacement.countDown();
            placementThread.join(TimeUnit.SECONDS.toMillis(10));
            updateThread.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(placementThread.isAlive()).as("placement thread stopped").isFalse();
            assertThat(updateThread.isAlive()).as("cluster update thread stopped").isFalse();
        }
    }

    @Test
    public void testOfflineSecondaryBookieFallsBackToUngroupedBookie() throws Exception {
        final String primaryGroup = "primaryGroup";
        final String secondaryGroup = "secondaryGroup";
        BookieId primaryBookie = new BookieSocketAddress(BOOKIE1).toBookieId();
        BookieId ungroupedBookie = new BookieSocketAddress(BOOKIE2).toBookieId();
        BookieId offlineSecondaryBookie = new BookieSocketAddress(BOOKIE3).toBookieId();

        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> primaryBookieGroup = new HashMap<>();
        primaryBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        Map<String, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        bookieMapping.put(primaryGroup, primaryBookieGroup);
        bookieMapping.put(secondaryGroup, secondaryBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, primaryGroup);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                secondaryGroup);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(Sets.newHashSet(primaryBookie, ungroupedBookie), Collections.emptySet());

        List<BookieId> ensemble = isolationPolicy
                .newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertEquals(new HashSet<>(ensemble), Sets.newHashSet(primaryBookie, ungroupedBookie));

        BookieId replacement = isolationPolicy.replaceBookie(2, 2, 2, Collections.emptyMap(),
                Arrays.asList(primaryBookie, offlineSecondaryBookie), offlineSecondaryBookie,
                new HashSet<>()).getResult();
        assertEquals(replacement, ungroupedBookie);

        isolationPolicy.onClusterChanged(Collections.singleton(primaryBookie), Collections.emptySet());
        assertThatThrownBy(() -> isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()))
                .as("Placement must fail without a writable ungrouped bookie")
                .isInstanceOf(BKNotEnoughBookiesException.class);
    }

    @Test
    public void testSecondaryIsolationGroupsBookiesNegative() throws Exception {

        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> defaultBookieGroup = new HashMap<>();
        final String isolatedGroup = "primaryGroup";
        final String secondaryIsolatedGroup = "secondaryGroup";

        defaultBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        defaultBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());
        defaultBookieGroup.put(BOOKIE5, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> primaryIsolatedBookieGroup = new HashMap<>();
        primaryIsolatedBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put("default", defaultBookieGroup);
        bookieMapping.put(isolatedGroup, primaryIsolatedBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                secondaryIsolatedGroup);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        try {
            isolationPolicy
                    .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
            fail("Should have thrown BKNotEnoughBookiesException");
        } catch (BKNotEnoughBookiesException ne) {
            // Ok..
        }
    }

    /**
     * test case for auto-recovery.
     * When the auto-recovery trigger from bookkeeper, we need to make sure the placement policy can read from
     * custom metadata and apply it when choosing the new bookie.
     */
    @Test
    public void testTheIsolationPolicyUsingCustomMetadata() throws Exception {
        // We configure two groups for the isolation policy, one is the 'primary' group, and the another is
        // 'secondary' group.
        // We put bookie1, bookie2, bookie3 into the 'primary' group, and put bookie4 into the 'secondary' group.
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> primaryIsolationBookieGroups = new HashMap<>();
        String primaryGroupName = "primary";
        String secondaryGroupName = "secondary";
        primaryIsolationBookieGroups.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        primaryIsolationBookieGroups.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());
        primaryIsolationBookieGroups.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> secondaryIsolationBookieGroups = new HashMap<>();
        secondaryIsolationBookieGroups.put(BOOKIE4, BookieInfo.builder().rack("rack0").build());
        bookieMapping.put(primaryGroupName, primaryIsolationBookieGroups);
        bookieMapping.put(secondaryGroupName, secondaryIsolationBookieGroups);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        // prepare a custom placement policy and put it into the custom metadata. The isolation policy should decode
        // from the custom metadata and apply it to the get excluded list method.
        Map<String, Object> placementPolicyProperties = new HashMap<>();
        placementPolicyProperties.put(
            IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, primaryGroupName);
        placementPolicyProperties.put(
            IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, secondaryGroupName);
        EnsemblePlacementPolicyConfig policyConfig = new EnsemblePlacementPolicyConfig(
            IsolatedBookieEnsemblePlacementPolicy.class,
            placementPolicyProperties
        );
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());

        // do the test logic
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, primaryGroupName);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        // we assume we have an ensemble list which is consist with bookie1 and bookie3, and bookie3 is broken.
        // we want to get a replace bookie from the 'primary' group and that should be bookie2. Because we only have
        // bookie1, bookie2, and bookie3 in the 'primary' group.
        BookieId bookie1Id = new BookieSocketAddress(BOOKIE1).toBookieId();
        BookieId bookie2Id = new BookieSocketAddress(BOOKIE2).toBookieId();
        BookieId bookie3Id = new BookieSocketAddress(BOOKIE3).toBookieId();
        BookieId bookieId = isolationPolicy.replaceBookie(2, 1, 1, customMetadata,
            Arrays.asList(bookie1Id, bookie3Id), bookie3Id, null).getResult();
        assertEquals(bookieId, bookie2Id);
    }

    @Test
    public void testDefaultIsolationPolicyNotCovered() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        final String defaultIsolatedGroup = "Group1";
        final String defaultSecondaryIsolatedGroup = "Group2";
        final String customIsolatedGroup = "Group2";

        Map<String, BookieInfo> group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        group1.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());

        Map<String, BookieInfo> group2 = new HashMap<>();
        group2.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        group2.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());

        Set<BookieId> bookieIdGroup1 = new HashSet<>();
        bookieIdGroup1.add(new BookieSocketAddress(BOOKIE1).toBookieId());
        bookieIdGroup1.add(new BookieSocketAddress(BOOKIE2).toBookieId());
        Set<BookieId> bookieIdGroup2 = new HashSet<>();
        bookieIdGroup2.add(new BookieSocketAddress(BOOKIE3).toBookieId());
        bookieIdGroup2.add(new BookieSocketAddress(BOOKIE4).toBookieId());

        bookieMapping.put(defaultIsolatedGroup, group1);
        bookieMapping.put(defaultSecondaryIsolatedGroup, group2);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, defaultIsolatedGroup);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                defaultSecondaryIsolatedGroup);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        Map<String, Object> placementPolicyProperties = new HashMap<>();
        placementPolicyProperties.put(
                IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, customIsolatedGroup);
        placementPolicyProperties.put(
                IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "");

        EnsemblePlacementPolicyConfig policyConfig = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                placementPolicyProperties
        );
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());

        List<BookieId> customBookieList = isolationPolicy
                .newEnsemble(2, 2, 2, customMetadata, new HashSet<>()).getResult();
        assertEquals(bookieIdGroup2.containsAll(customBookieList), true);
        List<BookieId> defaultBookieList = isolationPolicy
                .newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertEquals(bookieIdGroup1.containsAll(defaultBookieList), true);
    }

    @Test
    public void testGetExcludedBookiesWithIsolationGroups() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        final String isolationGroup1 = "Group1";
        final String isolationGroup2 = "Group2";
        final String isolationGroup3 = "Group3";

        Map<String, BookieInfo> group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        group1.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());

        Map<String, BookieInfo> group2 = new HashMap<>();
        group2.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        group2.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put(isolationGroup1, group1);
        bookieMapping.put(isolationGroup2, group2);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroup1);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                isolationGroup2);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        /* Test common cases */
        MutablePair<Set<String>, Set<String>> groups = new MutablePair<>();
        groups.setLeft(Sets.newHashSet(isolationGroup1));
        groups.setRight(Sets.newHashSet(""));
        Set<BookieId> blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(2, groups);
        assertEquals(blacklist.size(), 2);

        groups.setLeft(Sets.newHashSet(isolationGroup1));
        groups.setRight(Sets.newHashSet(isolationGroup2));
        blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(2, groups);
        assertEquals(blacklist.size(), 2);

        groups.setLeft(Sets.newHashSet(isolationGroup1));
        groups.setRight(Sets.newHashSet(isolationGroup2));
        blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(3, groups);
        assertTrue(blacklist.isEmpty());

        /* Test a bookie belongs to multiple isolation groups */
        group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());

        group2 = new HashMap<>();
        group2.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());
        group2.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        group2.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> group3 = new HashMap<>();
        group3.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put(isolationGroup1, group1);
        bookieMapping.put(isolationGroup2, group2);
        bookieMapping.put(isolationGroup3, group3);

        updateBookieInfo(isolationPolicy, jsonMapper.writeValueAsBytes(bookieMapping));

        groups.setLeft(Sets.newHashSet(isolationGroup1, isolationGroup3));
        groups.setRight(Sets.newHashSet(""));
        blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(2, groups);
        assertEquals(blacklist.size(), 3);

        /* Test a bookie belongs to multiple isolation groups and totalAvailableBookiesInPrimaryGroup < ensembleSize */
        groups.setLeft(Sets.newHashSet(isolationGroup1, isolationGroup3));
        groups.setRight(Sets.newHashSet(isolationGroup2));
        blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(2, groups);
        assertTrue(blacklist.isEmpty());

        /* Test some bookies not set rack config */
        group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());

        group2 = new HashMap<>();
        group2.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put(isolationGroup1, group1);
        bookieMapping.put(isolationGroup2, group2);

        updateBookieInfo(isolationPolicy, jsonMapper.writeValueAsBytes(bookieMapping));

        groups.setLeft(Sets.newHashSet(isolationGroup1));
        groups.setRight(Sets.newHashSet(isolationGroup2));
        blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(2, groups);
        assertEquals(blacklist.size(), 2);

        groups.setLeft(Sets.newHashSet(isolationGroup1));
        groups.setRight(Sets.newHashSet(isolationGroup2));
        blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(3, groups);
        assertTrue(blacklist.isEmpty());

        /* Test some bookies not set rack config and totalAvailableBookiesFromPrimaryAndSecondary < ensembleSize */
        group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());

        group2 = new HashMap<>();
        group2.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put(isolationGroup1, group1);
        bookieMapping.put(isolationGroup2, group2);

        updateBookieInfo(isolationPolicy, jsonMapper.writeValueAsBytes(bookieMapping));

        groups.setLeft(Sets.newHashSet(isolationGroup1));
        groups.setRight(Sets.newHashSet(isolationGroup2));
        blacklist = isolationPolicy.getExcludedBookiesWithIsolationGroups(2, groups);
        assertTrue(blacklist.isEmpty());
    }

    /**
     * Regression test for the NPE reported in the stack trace below. When custom metadata carries an
     * {@link EnsemblePlacementPolicyConfig} whose policy class does NOT match
     * {@link IsolatedBookieEnsemblePlacementPolicy}, the old {@code getIsolationGroup()} returned a
     * {@code MutablePair} with {@code null} left/right, which caused a {@link NullPointerException} in
     * {@code getExcludedBookiesWithIsolationGroups} when {@code getLeft().contains(...)} was called.
     *
     * <pre>
     * java.lang.NullPointerException: Cannot invoke "java.util.Set.contains(Object)"
     *     because the return value of "org.apache.commons.lang3.tuple.Pair.getLeft()" is null
     *     at IsolatedBookieEnsemblePlacementPolicy.getExcludedBookiesWithIsolationGroups(...)
     *     at IsolatedBookieEnsemblePlacementPolicy.getExcludedBookies(...)
     *     at IsolatedBookieEnsemblePlacementPolicy.replaceBookie(...)
     * </pre>
     */
    @Test
    public void testReplaceBookieWithNonMatchingPolicyClassShouldNotThrowNPE() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        group1.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());
        group1.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());
        group1.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());
        bookieMapping.put("group1", group1);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1");
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        // Use a policy class that does NOT match IsolatedBookieEnsemblePlacementPolicy.
        // In the old code this caused getIsolationGroup() to return a MutablePair with null left/right,
        // triggering NPE at the getLeft().contains() call in getExcludedBookiesWithIsolationGroups.
        EnsemblePlacementPolicyConfig policyConfig = new EnsemblePlacementPolicyConfig(
                RackawareEnsemblePlacementPolicy.class, Collections.emptyMap());
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());

        BookieId bookie1Id = new BookieSocketAddress(BOOKIE1).toBookieId();
        BookieId bookie2Id = new BookieSocketAddress(BOOKIE2).toBookieId();

        // Must not throw NullPointerException; BKNotEnoughBookiesException is acceptable.
        isolationPolicy.replaceBookie(2, 2, 2, customMetadata,
                Arrays.asList(bookie1Id, bookie2Id), bookie2Id, null);
    }

    /**
     * Verifies that {@link IsolatedBookieEnsemblePlacementPolicy#getIsolationGroup} treats
     * {@link ZkIsolatedBookieEnsemblePlacementPolicy} (a subclass) exactly like
     * {@link IsolatedBookieEnsemblePlacementPolicy} itself when reading isolation groups from
     * {@link EnsemblePlacementPolicyConfig} properties.
     *
     * <p>Legacy Pulsar clusters may have persisted {@code EnsemblePlacementPolicyConfig} entries whose
     * {@code policyClass} field is set to {@code ZkIsolatedBookieEnsemblePlacementPolicy}.  The
     * {@code isAssignableFrom} check in {@code getIsolationGroup} must recognise this subclass so that
     * the isolation groups are read from the stored properties rather than falling back to the
     * policy-level defaults.
     */
    @Test
    public void testGetIsolationGroupWithZkCompatiblePolicyClass() throws Exception {
        // Group1 → default isolation group configured on the policy.
        // Group2 → isolation group carried inside the custom metadata (ZkIsolated class).
        final String defaultGroup = "Group1";
        final String customGroup = "Group2";

        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> group1 = new HashMap<>();
        group1.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        group1.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());
        Map<String, BookieInfo> group2 = new HashMap<>();
        group2.put(BOOKIE3, BookieInfo.builder().rack("rack1").build());
        group2.put(BOOKIE4, BookieInfo.builder().rack("rack1").build());
        bookieMapping.put(defaultGroup, group1);
        bookieMapping.put(customGroup, group2);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, defaultGroup);
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = createIsolationPolicy(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        // --- unit-level: getIsolationGroup should parse properties, not fall back to defaults ---
        Map<String, Object> props = new HashMap<>();
        props.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, customGroup);
        props.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "secondaryGroup");
        EnsemblePlacementPolicyConfig zkConfig = new EnsemblePlacementPolicyConfig(
                ZkIsolatedBookieEnsemblePlacementPolicy.class, props);

        Pair<Set<String>, Set<String>> groups = isolationPolicy.getIsolationGroup(zkConfig);
        assertEquals(groups.getLeft(), Sets.newHashSet(customGroup),
                "primary group must be read from ZkIsolated config properties");
        assertEquals(groups.getRight(), Sets.newHashSet("secondaryGroup"),
                "secondary group must be read from ZkIsolated config properties");

        // --- integration-level: newEnsemble must select bookies from the ZkIsolated config group ---
        Map<String, Object> placementPolicyProperties = new HashMap<>();
        placementPolicyProperties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, customGroup);
        placementPolicyProperties.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "");
        EnsemblePlacementPolicyConfig policyConfig = new EnsemblePlacementPolicyConfig(
                ZkIsolatedBookieEnsemblePlacementPolicy.class, placementPolicyProperties);
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());

        Set<BookieId> bookieIdGroup2 = new HashSet<>();
        bookieIdGroup2.add(new BookieSocketAddress(BOOKIE3).toBookieId());
        bookieIdGroup2.add(new BookieSocketAddress(BOOKIE4).toBookieId());

        List<BookieId> ensemble = isolationPolicy
                .newEnsemble(2, 2, 2, customMetadata, new HashSet<>()).getResult();
        assertTrue(bookieIdGroup2.containsAll(ensemble),
                "ensemble should come from " + customGroup + " (ZkIsolated config), got " + ensemble);

        // Sanity-check: without custom metadata the default group1 bookies are chosen.
        Set<BookieId> bookieIdGroup1 = new HashSet<>();
        bookieIdGroup1.add(new BookieSocketAddress(BOOKIE1).toBookieId());
        bookieIdGroup1.add(new BookieSocketAddress(BOOKIE2).toBookieId());
        List<BookieId> defaultEnsemble = isolationPolicy
                .newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(bookieIdGroup1.containsAll(defaultEnsemble),
                "default ensemble should come from " + defaultGroup + ", got " + defaultEnsemble);
    }

    @Test
    public void testPlacementUsesGroupRevisionAppliedToRackTopology() throws Exception {
        String path = BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
        var rack1 = BookieInfo.builder().rack("/rack1").build();
        var rack2 = BookieInfo.builder().rack("/rack2").build();
        var configuration = new BookiesRackConfiguration();
        configuration.updateBookie("primary", BOOKIE1, rack1);
        configuration.updateBookie("other", BOOKIE2, rack2);
        store.put(path, jsonMapper.writeValueAsBytes(configuration), Optional.empty()).join();

        var pauseNextLookup = new AtomicBoolean();
        var lookupStarted = new CompletableFuture<Void>();
        var resumeLookup = new CompletableFuture<Void>();
        BookieAddressResolver resolver = bookie -> {
            if (pauseNextLookup.compareAndSet(true, false)) {
                lookupStarted.complete(null);
                resumeLookup.orTimeout(10, TimeUnit.SECONDS).join();
            }
            return BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER.resolve(bookie);
        };
        try (var snapshot = new BookieRackConfigurationSnapshot(store, 30_000)) {
            snapshot.start();
            snapshot.initialRead().get(5, TimeUnit.SECONDS);
            var conf = new ClientConfiguration();
            conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
            conf.setProperty(RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS,
                    BookieRackAffinityMapping.class.getName());
            conf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "primary");
            var policy = new IsolatedBookieEnsemblePlacementPolicy();
            policy.initialize(conf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                    NullStatsLogger.INSTANCE, resolver);
            try {
                policy.onClusterChanged(Set.of(BookieId.parse(BOOKIE1), BookieId.parse(BOOKIE2)), Set.of());
                configuration.updateBookie("other", BOOKIE1, rack2);
                configuration.updateBookie("primary", BOOKIE2, rack1);
                pauseNextLookup.set(true);
                store.put(path, jsonMapper.writeValueAsBytes(configuration), Optional.empty()).join();
                lookupStarted.get(5, TimeUnit.SECONDS);

                assertThat(snapshot.latest().configuration().get("primary")).containsOnlyKeys(BOOKIE2);
                assertThat(policy.newEnsemble(1, 1, 1, Map.of(), null).getResult())
                        .containsExactly(BookieId.parse(BOOKIE1));
                resumeLookup.complete(null);
                Awaitility.await().untilAsserted(() -> assertThat(policy.newEnsemble(1, 1, 1, Map.of(), null)
                        .getResult()).containsExactly(BookieId.parse(BOOKIE2)));
            } finally {
                resumeLookup.complete(null);
                policy.uninitalize();
            }
        }
    }

    @DataProvider
    public Object[][] coldPlacementOperations() {
        return new Object[][]{{false, null}, {true, null}, {false, "*"}, {true, "*"}};
    }

    @SuppressWarnings("unchecked")
    @Test(dataProvider = "coldPlacementOperations")
    public void testIsolatedPlacementRequiresRackConfiguration(boolean replaceBookie, String defaultGroup)
            throws Exception {
        store.close();
        store = mock(MetadataStoreExtended.class);
        MetadataCacheImpl<BookiesRackConfiguration> cache = mock(MetadataCacheImpl.class);
        doReturn(cache).when(store).getMetadataCache(BookiesRackConfiguration.class);
        String path = BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
        var initialRead = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var retry = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        when(cache.get(path)).thenReturn(initialRead).thenReturn(retry);

        var conf = new ClientConfiguration();
        conf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        if (defaultGroup != null) {
            conf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, defaultGroup);
        }
        var policy = new IsolatedBookieEnsemblePlacementPolicy();
        policy.initialize(conf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        try {
            policy.onClusterChanged(writableBookies, readOnlyBookies);
            var ledgerMetadata = isolationMetadata("group1");
            assertThat(policy.newEnsemble(4, 4, 4, Map.of(), null).getResult())
                    .containsExactlyInAnyOrderElementsOf(writableBookies);
            assertThat(policy.newEnsemble(4, 4, 4, isolationMetadata("*"), null).getResult())
                    .containsExactlyInAnyOrderElementsOf(writableBookies);
            assertThatThrownBy(() -> placeBookies(policy, ledgerMetadata, replaceBookie))
                    .isInstanceOf(BKNotEnoughBookiesException.class).cause().hasMessageContaining("/bookies");
            assertThatThrownBy(() -> placeBookies(policy, isolationMetadata(""), replaceBookie))
                    .isInstanceOf(BKNotEnoughBookiesException.class);

            initialRead.completeExceptionally(new MetadataStoreException("rack configuration read failed"));
            Awaitility.await().untilAsserted(() -> verify(cache, times(2)).get(path));
            assertThat(policy.getInitialRackConfigurationLoadFuture()).isCompletedExceptionally();
            assertThatThrownBy(() -> placeBookies(policy, ledgerMetadata, replaceBookie))
                    .isInstanceOf(BKNotEnoughBookiesException.class);

            var configuration = new BookiesRackConfiguration();
            configuration.put("group1", Map.of(BOOKIE1, BookieInfo.builder().rack("rack0").build(),
                    BOOKIE2, BookieInfo.builder().rack("rack1").build()));
            configuration.put("group2", Map.of(BOOKIE3, BookieInfo.builder().rack("rack0").build(),
                    BOOKIE4, BookieInfo.builder().rack("rack1").build()));
            retry.complete(Optional.of(configuration));
            Awaitility.await().until(() -> policy.latestRackConfiguration() != null);
            var expected = replaceBookie ? List.of(BookieId.parse(BOOKIE2))
                    : List.of(BookieId.parse(BOOKIE1), BookieId.parse(BOOKIE2));
            assertThat(placeBookies(policy, ledgerMetadata, replaceBookie))
                    .containsExactlyInAnyOrderElementsOf(expected);
            verify(cache, times(2)).get(path);
        } finally {
            policy.uninitalize();
        }
    }

    private static Map<String, byte[]> isolationMetadata(String group)
            throws EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException {
        var config = new EnsemblePlacementPolicyConfig(IsolatedBookieEnsemblePlacementPolicy.class,
                Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, group));
        return Map.of(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, config.encode());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSnapshotRetiresTimedOutReadAndIgnoresLateCompletion() throws Exception {
        store.close();
        store = mock(MetadataStoreExtended.class);
        MetadataCacheImpl<BookiesRackConfiguration> cache = mock(MetadataCacheImpl.class);
        doReturn(cache).when(store).getMetadataCache(BookiesRackConfiguration.class);
        var path = BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
        var stuckRead = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var freshConfiguration = new BookiesRackConfiguration();
        freshConfiguration.updateBookie("group1", BOOKIE1, BookieInfo.builder().rack("rack0").build());
        when(cache.get(path)).thenReturn(stuckRead)
                .thenReturn(CompletableFuture.completedFuture(Optional.of(freshConfiguration)));

        try (var snapshot = new BookieRackConfigurationSnapshot(store, 50)) {
            var first = snapshot.refresh();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).cause()
                    .isInstanceOf(TimeoutException.class)
                    .hasMessage("Timed out after 50 ms reading bookie isolation metadata at /bookies");
            assertThat(stuckRead).isNotDone();
            assertThat(snapshot.latest()).isNull();

            snapshot.refresh().get(5, TimeUnit.SECONDS);
            verify(cache).invalidate(path);
            var latest = snapshot.latest();
            assertThat(latest.configuration().get("group1").get(BOOKIE1).getRack()).isEqualTo("rack0");
            stuckRead.complete(Optional.empty());
            assertThat(snapshot.latest()).isSameAs(latest);
        }
        verify(cache).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSnapshotCloseFailsWaiterDuringCacheReadInitiation() throws Exception {
        store.close();
        store = mock(MetadataStoreExtended.class);
        MetadataCacheImpl<BookiesRackConfiguration> cache = mock(MetadataCacheImpl.class);
        doReturn(cache).when(store).getMetadataCache(BookiesRackConfiguration.class);
        var entered = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        when(cache.get(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH)).thenAnswer(invocation -> {
            entered.complete(null);
            release.get(5, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(Optional.empty());
        });

        try (var snapshot = new BookieRackConfigurationSnapshot(store, TimeUnit.SECONDS.toMillis(10))) {
            var initiating = CompletableFuture.supplyAsync(snapshot::refresh);
            try {
                entered.get(5, TimeUnit.SECONDS);
                var waiting = snapshot.initialRead();
                snapshot.close();
                assertThatThrownBy(() -> waiting.get(5, TimeUnit.SECONDS)).cause()
                        .isInstanceOf(IllegalStateException.class);
                verify(cache, never()).close();
            } finally {
                release.complete(null);
            }
            var retiredRead = initiating.get(5, TimeUnit.SECONDS);
            assertThat(retiredRead).isCompletedExceptionally();
            assertThat(snapshot.latest()).isNull();
        }
        verify(cache).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStartedSnapshotRetriesAnInitialFailureWithoutAConsumer() throws Exception {
        store.close();
        store = mock(MetadataStoreExtended.class);
        MetadataCacheImpl<BookiesRackConfiguration> cache = mock(MetadataCacheImpl.class);
        Runnable unregister = mock(Runnable.class);
        when(store.registerCancellableListener(any())).thenReturn(unregister);
        doReturn(cache).when(store).getMetadataCache(BookiesRackConfiguration.class);
        var path = BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
        var firstRead = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        var retryRead = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        when(cache.get(path)).thenReturn(firstRead).thenReturn(retryRead);

        try (var snapshot = new BookieRackConfigurationSnapshot(store, TimeUnit.SECONDS.toMillis(10))) {
            snapshot.start();
            snapshot.start();
            var initialLoad = snapshot.initialLoad();
            var loadFailure = new MetadataStoreException("Initial load failed");
            var initialRead = snapshot.initialRead();
            firstRead.completeExceptionally(loadFailure);
            assertThatThrownBy(() -> initialRead.get(5, TimeUnit.SECONDS)).cause().isSameAs(loadFailure);
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(cache, times(2)).get(path));
            assertThat(initialLoad).isNotDone();
            retryRead.complete(Optional.empty());
            initialLoad.get(5, TimeUnit.SECONDS);
            assertThat(snapshot.hasLoaded()).isTrue();
            assertThatThrownBy(() -> snapshot.initialRead().get(5, TimeUnit.SECONDS)).cause().isSameAs(loadFailure);
            assertThat(snapshot.latest().configuration()).isEmpty();
            verify(store).registerCancellableListener(any());
        }
        verify(unregister).run();
        verify(cache).close();
    }

    @Test
    public void testStartedSnapshotPublishesMetadataChangesWithoutPlacement() throws Exception {
        var path = BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
        var updates = new LinkedBlockingQueue<VersionedConfiguration>();
        try (var snapshot = new BookieRackConfigurationSnapshot(store, TimeUnit.SECONDS.toMillis(10))) {
            var cancel = snapshot.subscribe(updates::add);
            snapshot.start();
            snapshot.initialLoad().get(5, TimeUnit.SECONDS);
            var initial = updates.poll(5, TimeUnit.SECONDS);
            assertThat(initial).isNotNull();
            assertThat(initial.configuration()).isEmpty();

            var configuration = new BookiesRackConfiguration();
            configuration.updateBookie("group1", BOOKIE1, BookieInfo.builder().rack("rack0").build());
            store.put(path, jsonMapper.writeValueAsBytes(configuration), Optional.empty()).get(5, TimeUnit.SECONDS);
            var created = updates.poll(5, TimeUnit.SECONDS);
            assertThat(created).isNotNull();
            assertThat(created.configuration().get("group1").get(BOOKIE1).getRack()).isEqualTo("rack0");

            configuration.updateBookie("group2", BOOKIE1, BookieInfo.builder().rack("rack1").build());
            store.put(path, jsonMapper.writeValueAsBytes(configuration), Optional.empty()).get(5, TimeUnit.SECONDS);
            var updated = updates.poll(5, TimeUnit.SECONDS);
            assertThat(updated).isNotNull();
            assertThat(updated.version()).isGreaterThan(created.version());
            assertThat(updated.configuration()).containsOnlyKeys("group2");
            assertThat(updated.configuration().get("group2").get(BOOKIE1).getRack()).isEqualTo("rack1");

            store.delete(path, Optional.empty()).get(5, TimeUnit.SECONDS);
            var deleted = updates.poll(5, TimeUnit.SECONDS);
            assertThat(deleted).isNotNull();
            assertThat(deleted.version()).isGreaterThan(updated.version());
            assertThat(deleted.configuration()).isEmpty();
            cancel.run();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPoliciesShareSnapshotWithoutOwningIt() throws Exception {
        store.close();
        store = mock(MetadataStoreExtended.class);
        MetadataCacheImpl<BookiesRackConfiguration> cache = mock(MetadataCacheImpl.class);
        doReturn(cache).when(store).getMetadataCache(BookiesRackConfiguration.class);
        var path = BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
        var read = new CompletableFuture<Optional<BookiesRackConfiguration>>();
        when(cache.get(path)).thenReturn(read);

        try (var snapshot = new BookieRackConfigurationSnapshot(store, TimeUnit.SECONDS.toMillis(10))) {
            snapshot.start();
            var conf = new ClientConfiguration();
            conf.setProperty(RACK_CONFIGURATION_SNAPSHOT, snapshot);
            conf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "group1");
            var firstPolicy = new IsolatedBookieEnsemblePlacementPolicy();
            var secondPolicy = new IsolatedBookieEnsemblePlacementPolicy();
            firstPolicy.initialize(conf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
            secondPolicy.initialize(conf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
            try {
                var first = firstPolicy.getInitialRackConfigurationLoadFuture();
                var second = secondPolicy.getInitialRackConfigurationLoadFuture();
                assertThat(first.cancel(false)).isTrue();
                assertThat(second).isNotDone();
                verify(store).getMetadataCache(BookiesRackConfiguration.class);
                read.complete(Optional.empty());
                second.get(5, TimeUnit.SECONDS);
                firstPolicy.onClusterChanged(writableBookies, readOnlyBookies);
                firstPolicy.newEnsemble(4, 4, 4, Map.of(), null);
                verify(cache).get(path);
            } finally {
                firstPolicy.uninitalize();
                secondPolicy.uninitalize();
            }
            verify(cache, never()).close();
        }
        verify(cache).close();
    }

    private static List<BookieId> placeBookies(IsolatedBookieEnsemblePlacementPolicy policy,
                                             Map<String, byte[]> metadata, boolean replaceBookie)
            throws BKNotEnoughBookiesException {
        if (replaceBookie) {
            BookieId bookieToReplace = BookieId.parse(BOOKIE3);
            return List.of(policy.replaceBookie(2, 2, 2, metadata, List.of(BookieId.parse(BOOKIE1), bookieToReplace),
                    bookieToReplace, null).getResult());
        }
        return policy.newEnsemble(2, 2, 2, metadata, null).getResult();
    }

    /**
     * Initializes the policy and waits for the initial configuration before testing placement with a snapshot.
     */
    private IsolatedBookieEnsemblePlacementPolicy createIsolationPolicy(ClientConfiguration bkClientConf)
            throws Exception {
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        policies.add(isolationPolicy);
        isolationPolicy.getInitialRackConfigurationLoadFuture().get(RACK_CONFIGURATION_LOAD_TIMEOUT_SECONDS,
                TimeUnit.SECONDS);
        return isolationPolicy;
    }

    private void updateBookieInfo(IsolatedBookieEnsemblePlacementPolicy isolationPolicy, byte[] bookieInfo) {
        var previous = isolationPolicy.latestRackConfiguration();
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, bookieInfo, Optional.empty()).join();
        Awaitility.await().until(() -> isolationPolicy.latestRackConfiguration().version() > previous.version());
    }
}
