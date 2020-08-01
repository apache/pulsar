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

import java.util.ArrayList;
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
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
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
    Set<BookieSocketAddress> writableBookies = new HashSet<>();
    Set<BookieSocketAddress> readOnlyBookies = new HashSet<>();
    List<String> isolationGroups = new ArrayList<>();

    HashedWheelTimer timer;

    @BeforeMethod
    public void setUp() throws Exception {
        timer = new HashedWheelTimer();
        localZkS = new ZookeeperServerTest(0);
        localZkS.start();

        localZkc = ZooKeeperClient.newBuilder().connectString("127.0.0.1" + ":" + localZkS.getZookeeperPort()).build();
        writableBookies.add(new BookieSocketAddress(BOOKIE1));
        writableBookies.add(new BookieSocketAddress(BOOKIE2));
        writableBookies.add(new BookieSocketAddress(BOOKIE3));
        writableBookies.add(new BookieSocketAddress(BOOKIE4));
        isolationGroups.add("group1");
    }

    @AfterMethod
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

        mainBookieGroup.put(BOOKIE1, new BookieInfo("rack0", null));
        mainBookieGroup.put(BOOKIE2, new BookieInfo("rack1", null));

        Map<String, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(BOOKIE3, new BookieInfo("rack0", null));

        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieSocketAddress> ensemble = isolationPolicy.newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4)));

        ensemble = isolationPolicy.newEnsemble(1, 1, 1, Collections.emptyMap(), new HashSet<>()).getResult();
        assertFalse(ensemble.contains(new BookieSocketAddress(BOOKIE3)));

        try {
            isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        Set<BookieSocketAddress> bookieToExclude = new HashSet<>();
        bookieToExclude.add(new BookieSocketAddress(BOOKIE1));
        ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), bookieToExclude).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

        secondaryBookieGroup.put(BOOKIE4, new BookieInfo("rack0", null));
        bookieMapping.put("group2", secondaryBookieGroup);

        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                -1);

        Thread.sleep(100);

        ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), null).getResult();

        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        try {
            isolationPolicy.replaceBookie(3, 3, 3, Collections.emptyMap(), ensemble,
                    new BookieSocketAddress(BOOKIE5), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }
        bookieToExclude = new HashSet<>();
        bookieToExclude.add(new BookieSocketAddress(BOOKIE1));

        ensemble = isolationPolicy.newEnsemble(1, 1, 1, Collections.emptyMap(), bookieToExclude).getResult();
        BookieSocketAddress chosenBookie = isolationPolicy.replaceBookie(1, 1, 1, Collections.emptyMap(),
                ensemble, ensemble.get(0), new HashSet<>()).getResult();
        assertEquals(new BookieSocketAddress(BOOKIE1), chosenBookie);

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, 4, Collections.emptyMap(), new HashSet<>());

        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}, \"" + BOOKIE4
                + "\": {\"rack\": \"rack2\", \"hostname\": \"bookie4.example.com\"}}}";

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        List<BookieSocketAddress> ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

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

        mainBookieGroup.put(BOOKIE1, new BookieInfo("rack0", null));
        mainBookieGroup.put(BOOKIE2, new BookieInfo("rack1", null));
        secondaryBookieGroup.put(BOOKIE3, new BookieInfo("rack0", null));
        secondaryBookieGroup.put(BOOKIE4, new BookieInfo("rack2", null));

        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setZkServers("127.0.0.1" + ":" + localZkS.getZookeeperPort());
        bkClientConf.setZkTimeout(1000);
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL, NullStatsLogger.INSTANCE);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieSocketAddress> ensemble = isolationPolicy.newEnsemble(2, 2, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

        try {
            isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        mainBookieGroup.put(BOOKIE3, new BookieInfo("rack1", null));
        secondaryBookieGroup.remove(BOOKIE3);
        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                -1);

        // wait for the zk to notify and update the mappings
        Thread.sleep(100);

        ensemble = isolationPolicy.newEnsemble(3, 3, 3, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE3)));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);

        Thread.sleep(100);

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

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE);
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

        defaultBookieGroup.put(BOOKIE1, new BookieInfo("rack0", null));
        defaultBookieGroup.put(BOOKIE2, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE3, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE4, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE5, new BookieInfo("rack1", null));

        Map<String, BookieInfo> isolatedBookieGroup = new HashMap<>();
        isolatedBookieGroup.put(BOOKIE1, new BookieInfo("rack1", null));
        isolatedBookieGroup.put(BOOKIE2, new BookieInfo("rack0", null));
        isolatedBookieGroup.put(BOOKIE4, new BookieInfo("rack0", null));

        bookieMapping.put("default", defaultBookieGroup);
        bookieMapping.put(isolatedGroup, isolatedBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieSocketAddress> ensemble = isolationPolicy
                .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4)));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }

    @Test
    public void testSecondaryIsolationGroupsBookies() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> defaultBookieGroup = new HashMap<>();
        final String isolatedGroup = "primaryGroup";
        final String secondaryIsolatedGroup = "secondaryGroup";

        defaultBookieGroup.put(BOOKIE1, new BookieInfo("rack0", null));
        defaultBookieGroup.put(BOOKIE2, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE3, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE4, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE5, new BookieInfo("rack1", null));

        Map<String, BookieInfo> primaryIsolatedBookieGroup = new HashMap<>();
        primaryIsolatedBookieGroup.put(BOOKIE1, new BookieInfo("rack1", null));

        Map<String, BookieInfo> secondaryIsolatedBookieGroup = new HashMap<>();
        secondaryIsolatedBookieGroup.put(BOOKIE2, new BookieInfo("rack0", null));
        secondaryIsolatedBookieGroup.put(BOOKIE4, new BookieInfo("rack0", null));

        bookieMapping.put("default", defaultBookieGroup);
        bookieMapping.put(isolatedGroup, primaryIsolatedBookieGroup);
        bookieMapping.put(secondaryIsolatedGroup, secondaryIsolatedBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, secondaryIsolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        List<BookieSocketAddress> ensemble = isolationPolicy
                .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4)));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }

    @Test
    public void testSecondaryIsolationGroupsBookiesNegative() throws Exception {

        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> defaultBookieGroup = new HashMap<>();
        final String isolatedGroup = "primaryGroup";
        final String secondaryIsolatedGroup = "secondaryGroup";

        defaultBookieGroup.put(BOOKIE1, new BookieInfo("rack0", null));
        defaultBookieGroup.put(BOOKIE2, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE3, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE4, new BookieInfo("rack1", null));
        defaultBookieGroup.put(BOOKIE5, new BookieInfo("rack1", null));

        Map<String, BookieInfo> primaryIsolatedBookieGroup = new HashMap<>();
        primaryIsolatedBookieGroup.put(BOOKIE1, new BookieInfo("rack1", null));

        bookieMapping.put("default", defaultBookieGroup);
        bookieMapping.put(isolatedGroup, primaryIsolatedBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolatedGroup);
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                secondaryIsolatedGroup);
        isolationPolicy.initialize(bkClientConf, Optional.empty(), timer, SettableFeatureProvider.DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        try {
            List<BookieSocketAddress> ensemble = isolationPolicy
                    .newEnsemble(3, 3, 2, Collections.emptyMap(), new HashSet<>()).getResult();
            Assert.fail("Should have thrown BKNotEnoughBookiesException");
        } catch (BKNotEnoughBookiesException ne) {
            // Ok..
        }

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }
}
