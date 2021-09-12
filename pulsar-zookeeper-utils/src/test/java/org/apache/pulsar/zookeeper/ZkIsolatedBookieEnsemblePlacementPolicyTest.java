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
package org.apache.pulsar.zookeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZkIsolatedBookieEnsemblePlacementPolicyTest {

    private static final String BOOKIE1 = "127.0.0.1:3181";
    private static final String BOOKIE2 = "127.0.0.2:3181";
    private static final String BOOKIE3 = "127.0.0.3:3181";
    private static final String BOOKIE4 = "127.0.0.4:3181";
    private static final String BOOKIE5 = "127.0.0.5:3181";
    private ZookeeperServerTest localZkS;
    private ZooKeeper localZkc;

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();
    Set<BookieId> writableBookies = new HashSet<>();
    Set<BookieId> readOnlyBookies = new HashSet<>();
    List<String> isolationGroups = new ArrayList<>();

    HashedWheelTimer timer;

    @BeforeMethod
    public void setUp() throws Exception {
        timer = new HashedWheelTimer();
        localZkS = new ZookeeperServerTest(0);
        localZkS.start();

        localZkc = ZooKeeperClient.newBuilder().connectString("127.0.0.1" + ":" + localZkS.getZookeeperPort()).build();
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
        localZkS.close();
        timer.stop();
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

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) != null);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
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

        byte[] data = jsonMapper.writeValueAsBytes(bookieMapping);
        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data, -1);

        Awaitility.await().until(() -> Arrays
                .equals(data, localZkc.getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null)));

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

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());

        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}, \"" + BOOKIE4
                + "\": {\"rack\": \"rack2\", \"hostname\": \"bookie4.example.com\"}}}";

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await().until(() -> Arrays.equals(data.getBytes(StandardCharsets.UTF_8),
                localZkc.getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null)));

        List<BookieId> ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
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

        byte[] data = jsonMapper.writeValueAsBytes(bookieMapping);
        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await().until(() -> Arrays.equals(data,
                localZkc.getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null)));

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setZkServers("127.0.0.1" + ":" + localZkS.getZookeeperPort());
        bkClientConf.setZkTimeout(1000);
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
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

        byte[] data2 = jsonMapper.writeValueAsBytes(bookieMapping);
        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data2, -1);

        // wait for the zk to notify and update the mappings
        Awaitility.await().until(() -> Arrays
                .equals(data2, localZkc.getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null)));

        ensemble = isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE3).toBookieId()));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) == null);

        isolationPolicy.newEnsemble(1, 1, 1, Collections.emptyMap(), new HashSet<>());
    }

    @Test
    public void testNoIsolationGroup() throws Exception {
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}, \"" + BOOKIE4
                + "\": {\"rack\": \"rack2\", \"hostname\": \"bookie4.example.com\"}}}";

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) != null);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());
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

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) != null);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy
                .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
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

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) != null);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, secondaryIsolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieId> ensemble = isolationPolicy
                .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2).toBookieId()));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4).toBookieId()));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
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

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) != null);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
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

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
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

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
            jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) != null);

        // prepare a custom placement policy and put it into the custom metadata. The isolation policy should decode
        // from the custom metadata and apply it to the get black list method.
        Map<String, Object> placementPolicyProperties = new HashMap<>();
        placementPolicyProperties.put(
            ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, primaryGroupName);
        placementPolicyProperties.put(
            ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, secondaryGroupName);
        EnsemblePlacementPolicyConfig policyConfig = new EnsemblePlacementPolicyConfig(
            ZkIsolatedBookieEnsemblePlacementPolicy.class,
            placementPolicyProperties
        );
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, policyConfig.encode());

        // do the test logic
        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, primaryGroupName);
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
