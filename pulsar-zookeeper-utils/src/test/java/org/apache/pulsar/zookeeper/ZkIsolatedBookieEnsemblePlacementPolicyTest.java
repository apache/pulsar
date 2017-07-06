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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.BookieInfo;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ZkIsolatedBookieEnsemblePlacementPolicyTest {

    private static final String BOOKIE1 = "127.0.0.1:3181";
    private static final String BOOKIE2 = "127.0.0.2:3181";
    private static final String BOOKIE3 = "127.0.0.3:3181";
    private static final String BOOKIE4 = "127.0.0.4:3181";
    private static final String BOOKIE5 = "127.0.0.5:3181";
    private ZookeeperServerTest localZkS;
    private ZooKeeper localZkc;

    private final int LOCAL_ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();
    Set<BookieSocketAddress> writableBookies = new HashSet<>();
    Set<BookieSocketAddress> readOnlyBookies = new HashSet<>();
    List<String> isolationGroups = new ArrayList<>();

    @BeforeMethod
    public void setUp() throws Exception {
        localZkS = new ZookeeperServerTest(LOCAL_ZOOKEEPER_PORT);
        localZkS.start();
        ZooKeeperWatcherBase zkWatcherBase = new ZooKeeperWatcherBase(10000);
        localZkc = ZkUtils.createConnectedZookeeperClient("127.0.0.1" + ":" + LOCAL_ZOOKEEPER_PORT, zkWatcherBase);
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
    }

    @Test
    public void testBasic() throws Exception {
        Map<String, Map<String, BookieInfo>> bookieMapping = new HashMap<>();
        Map<String, BookieInfo> mainBookieGroup = new HashMap<>();

        BookieInfo bookieInfo0 = new BookieInfo();
        bookieInfo0.setRack("rack0");
        mainBookieGroup.put(BOOKIE1, bookieInfo0);

        BookieInfo bookieInfo1 = new BookieInfo();
        bookieInfo1.setRack("rack1");
        mainBookieGroup.put(BOOKIE2, bookieInfo1);

        Map<String, BookieInfo> secondaryBookieGroup = new HashMap<>();
        BookieInfo bookieInfo2 = new BookieInfo();
        bookieInfo2.setRack("rack0");
        secondaryBookieGroup.put(BOOKIE3, bookieInfo2);

        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache(localZkc) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        ArrayList<BookieSocketAddress> ensemble = isolationPolicy.newEnsemble(3, 3, new HashSet<>());
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4)));

        ensemble = isolationPolicy.newEnsemble(1, 1, new HashSet<>());
        assertFalse(ensemble.contains(new BookieSocketAddress(BOOKIE3)));

        try {
            isolationPolicy.newEnsemble(4, 4, new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        Set<BookieSocketAddress> bookieToExclude = new HashSet<>();
        bookieToExclude.add(new BookieSocketAddress(BOOKIE1));
        ensemble = isolationPolicy.newEnsemble(2, 2, bookieToExclude);
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE4)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

        BookieInfo bookieInfo3 = new BookieInfo();
        bookieInfo3.setRack("rack0");
        secondaryBookieGroup.put(BOOKIE4, bookieInfo3);
        bookieMapping.put("group2", secondaryBookieGroup);

        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                -1);

        Thread.sleep(100);

        ensemble = isolationPolicy.newEnsemble(2, 2, null);

        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

        try {
            isolationPolicy.newEnsemble(3, 3, new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        try {
            isolationPolicy.replaceBookie(new BookieSocketAddress(BOOKIE5), new HashSet<>(ensemble), new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        bookieToExclude = new HashSet<>();
        bookieToExclude.add(new BookieSocketAddress(BOOKIE1));
        ensemble = isolationPolicy.newEnsemble(1, 1, bookieToExclude);
        BookieSocketAddress chosenBookie = isolationPolicy.replaceBookie(new BookieSocketAddress(BOOKIE5),
                new HashSet<>(ensemble), null);
        assertTrue(chosenBookie.equals(new BookieSocketAddress(BOOKIE1)));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache(localZkc) {
        });
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, new HashSet<>());

        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"rack1\", \"hostname\": \"bookie2.example.com\"}}, \"group2\": {\"" + BOOKIE3
                + "\": {\"rack\": \"rack0\", \"hostname\": \"bookie3.example.com\"}, \"" + BOOKIE4
                + "\": {\"rack\": \"rack2\", \"hostname\": \"bookie4.example.com\"}}}";

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ArrayList<BookieSocketAddress> ensemble = isolationPolicy.newEnsemble(2, 2, new HashSet<>());
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

        try {
            isolationPolicy.newEnsemble(3, 3, new HashSet<>());
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

        BookieInfo bookieInfo0 = new BookieInfo();
        bookieInfo0.setRack("rack0");
        mainBookieGroup.put(BOOKIE1, bookieInfo0);

        BookieInfo bookieInfo1 = new BookieInfo();
        bookieInfo1.setRack("rack1");
        mainBookieGroup.put(BOOKIE2, bookieInfo1);

        BookieInfo bookieInfo2 = new BookieInfo();
        bookieInfo2.setRack("rack0");
        secondaryBookieGroup.put(BOOKIE3, bookieInfo2);

        BookieInfo bookieInfo3 = new BookieInfo();
        bookieInfo3.setRack("rack2");
        secondaryBookieGroup.put(BOOKIE4, bookieInfo3);

        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(100);

        ZkIsolatedBookieEnsemblePlacementPolicy isolationPolicy = new ZkIsolatedBookieEnsemblePlacementPolicy();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setZkServers("127.0.0.1" + ":" + LOCAL_ZOOKEEPER_PORT);
        bkClientConf.setZkTimeout(1000);
        bkClientConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, isolationGroups);
        isolationPolicy.initialize(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        ArrayList<BookieSocketAddress> ensemble = isolationPolicy.newEnsemble(2, 2, new HashSet<>());
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));

        try {
            isolationPolicy.newEnsemble(3, 3, new HashSet<>());
            fail("should not pass");
        } catch (BKNotEnoughBookiesException e) {
            // ok
        }

        mainBookieGroup.put(BOOKIE3, bookieInfo2);
        secondaryBookieGroup.remove(BOOKIE3);
        bookieMapping.put("group1", mainBookieGroup);
        bookieMapping.put("group2", secondaryBookieGroup);

        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                -1);

        // wait for the zk to notify and update the mappings
        Thread.sleep(100);

        ensemble = isolationPolicy.newEnsemble(3, 3, new HashSet<>());
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE1)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE2)));
        assertTrue(ensemble.contains(new BookieSocketAddress(BOOKIE3)));

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);

        Thread.sleep(100);

        isolationPolicy.newEnsemble(1, 1, new HashSet<>());
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
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache(localZkc) {
        });
        isolationPolicy.initialize(bkClientConf);
        isolationPolicy.onClusterChanged(writableBookies, readOnlyBookies);

        isolationPolicy.newEnsemble(4, 4, new HashSet<>());
    }
}
