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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.netty.util.HashedWheelTimer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IsolatedBookieEnsemblePlacementPolicyTest {

    private static final String BOOKIE1 = "127.0.0.1:3181";
    private static final String BOOKIE2 = "127.0.0.2:3181";
    private static final String BOOKIE3 = "127.0.0.3:3181";
    private static final String BOOKIE4 = "127.0.0.4:3181";
    private static final String BOOKIE5 = "127.0.0.5:3181";
    private MetadataStore store;

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
        writableBookies.clear();
        isolationGroups.clear();
        store.close();
        timer.stop();
    }

    @Test
    public void testMetadataStoreCases() throws Exception {
        Map<String, BookieInfo> mainBookieGroup = new HashMap<>();
        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());

        store = mock(MetadataStoreExtended.class);
        MetadataCacheImpl cache = mock(MetadataCacheImpl.class);
        when(store.getMetadataCache(BookiesRackConfiguration.class)).thenReturn(cache);
        CompletableFuture<Optional<BookiesRackConfiguration>> initialFuture = new CompletableFuture<>();
        //The initialFuture only has group1.
        BookiesRackConfiguration rackConfiguration1 = new BookiesRackConfiguration();
        rackConfiguration1.put("group1", mainBookieGroup);
        initialFuture.complete(Optional.of(rackConfiguration1));

        long waitTime = 2000;
        CompletableFuture<Optional<BookiesRackConfiguration>> waitingCompleteFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            //The waitingCompleteFuture has group1 and group2.
            BookiesRackConfiguration rackConfiguration2 = new BookiesRackConfiguration();
            rackConfiguration2.put("group1", mainBookieGroup);
            rackConfiguration2.put("group2", secondaryBookieGroup);
            waitingCompleteFuture.complete(Optional.of(rackConfiguration2));
        }).start();

        long longWaitTime = 4000;
        CompletableFuture<Optional<BookiesRackConfiguration>> emptyFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(longWaitTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            //The emptyFuture means that the zk node /bookies already be removed.
            emptyFuture.complete(Optional.empty());
        }).start();

        //Return different future means that cache expire.
        when(cache.get(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH))
                .thenReturn(initialFuture).thenReturn(initialFuture)
                .thenReturn(waitingCompleteFuture).thenReturn(waitingCompleteFuture)
                .thenReturn(emptyFuture).thenReturn(emptyFuture);

        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        MutablePair<Set<String>, Set<String>> groups = new MutablePair<>();
        groups.setLeft(Sets.newHashSet("group1"));
        groups.setRight(new HashSet<>());

        //initialFuture, the future is waiting done.
        Set<BookieId> blacklist =
                isolationPolicy.getBlacklistedBookiesWithIsolationGroups(2, groups);
        assertTrue(blacklist.isEmpty());

        //waitingCompleteFuture, the future is waiting done.
        blacklist =
                isolationPolicy.getBlacklistedBookiesWithIsolationGroups(2, groups);
        assertTrue(blacklist.isEmpty());

        Thread.sleep(waitTime);

        //waitingCompleteFuture, the future is already done.
        blacklist =
                isolationPolicy.getBlacklistedBookiesWithIsolationGroups(2, groups);
        assertFalse(blacklist.isEmpty());
        assertEquals(blacklist.size(), 1);
        BookieId excludeBookie = blacklist.iterator().next();
        assertEquals(excludeBookie.toString(), BOOKIE3);

        //emptyFuture, the future is waiting done.
        blacklist =
                isolationPolicy.getBlacklistedBookiesWithIsolationGroups(2, groups);
        assertFalse(blacklist.isEmpty());
        assertEquals(blacklist.size(), 1);
        excludeBookie = blacklist.iterator().next();
        assertEquals(excludeBookie.toString(), BOOKIE3);

        Thread.sleep(longWaitTime - waitTime);

        //emptyFuture, the future is already done.
        blacklist =
                isolationPolicy.getBlacklistedBookiesWithIsolationGroups(2, groups);
        assertTrue(blacklist.isEmpty());
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

        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy.newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));

        ensemble = isolationPolicy.newEnsemble(1, 1, 1, Collections.emptyMap(), new HashSet<>()).getResult();
        assertFalse(ensemble.contains(new BookieSocketAddress(BOOKIE3).toBookieId()));

        try {
            isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        Set<BookieId> bookieToExclude = new HashSet<>();
        bookieToExclude.add(new BookieSocketAddress(BOOKIE1).toBookieId());
        ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), bookieToExclude).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));

        secondaryBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack0").build());
        bookieMapping.put("group2", secondaryBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), null).getResult();

        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>());
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

        ensemble = isolationPolicy.newEnsemble(1, 1, 1, Collections.emptyMap(), bookieToExclude).getResult();
        BookieId chosenBookie = isolationPolicy.replaceBookie(1, 1, 1, Collections.emptyMap(),
                ensemble, ensemble.get(0), new HashSet<>()).getResult();
        assertEquals(new BookieSocketAddress(BOOKIE1).toBookieId(), chosenBookie);
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());

        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}, \"" + BOOKIE4
                + "\": {\"rack\": \"rack2\", \"hostname\": \"bookie4.example.com\"}}}";

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(StandardCharsets.UTF_8),
                Optional.empty()).join();

        List<BookieId> ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>());
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

        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
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

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        ensemble = isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE3).toBookieId()));
    }

    @Test
    public void testNoIsolationGroup() throws Exception {
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}, \"" + BOOKIE4
                + "\": {\"rack\": \"rack2\", \"hostname\": \"bookie4.example.com\"}}}";

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(StandardCharsets.UTF_8),
                Optional.empty()).join();

        Awaitility.await().until(() -> store.exists(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH).join());

        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());

        BookieId bookie1Id = new BookieSocketAddress(BOOKIE1).toBookieId();
        BookieId bookie2Id = new BookieSocketAddress(BOOKIE2).toBookieId();
        BookieId bookie3Id = new BookieSocketAddress(BOOKIE3).toBookieId();
        BookieId bookie4Id = new BookieSocketAddress(BOOKIE4).toBookieId();
        // when we set strictBookieAffinityEnabled=true and some namespace not set ISOLATION_BOOKIE_GROUPS there will set "" by default.
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
                Arrays.asList(bookie1Id,bookie2Id,bookie3Id), bookie3Id, null).getResult();
        assertEquals(replaceBookie1, bookie4Id);

        // when ISOLATION_BOOKIE_GROUPS miss.
        Map<String, Object> placementPolicyProperties2 = new HashMap<>();
        EnsemblePlacementPolicyConfig policyConfig2 = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                placementPolicyProperties2
        );
        Map<String, byte[]> customMetadata2 = new HashMap<>();
        customMetadata2.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());
        BookieId replaceBookie2 = isolationPolicy.replaceBookie(3, 3, 3, customMetadata2,
                Arrays.asList(bookie1Id,bookie2Id,bookie3Id), bookie3Id, null).getResult();
        assertEquals(replaceBookie2, bookie4Id);
    }

    /**
     * validates overlapped bookies between default-groups and isolated-groups.
     *
     * <pre>
     * a. default-group has all 5 bookies.
     * b. 3 of the default-group bookies have been added to isolated-group without being removed from default-group.
     * c. isolated-policy-placement should be identify those 3 overlapped bookies and exclude them from blacklisted bookies.
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

        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
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
        primaryIsolatedBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack1").build());

        Map<String, BookieInfo> secondaryIsolatedBookieGroup = new HashMap<>();
        secondaryIsolatedBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack0").build());
        secondaryIsolatedBookieGroup.put(BOOKIE4, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put("default", defaultBookieGroup);
        bookieMapping.put(isolatedGroup, primaryIsolatedBookieGroup);
        bookieMapping.put(secondaryIsolatedGroup, secondaryIsolatedBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, secondaryIsolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy
                .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));
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

        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                secondaryIsolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
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
        // from the custom metadata and apply it to the get black list method.
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
        IsolatedBookieEnsemblePlacementPolicy isolationPolicy = new IsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        bkClientConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, primaryGroupName);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
            NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
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
}
