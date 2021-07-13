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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZkBookieRackAffinityMappingTest {

    private BookieSocketAddress BOOKIE1 = null;
    private BookieSocketAddress BOOKIE2 = null;
    private BookieSocketAddress BOOKIE3 = null;
    private ZookeeperServerTest localZkS;
    private ZooKeeper localZkc;

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    @BeforeMethod
    public void setUp() throws Exception {
        localZkS = new ZookeeperServerTest(0);
        localZkS.start();
        localZkc = ZooKeeperClient.newBuilder().connectString("127.0.0.1:" + localZkS.getZookeeperPort()).build();
        BOOKIE1 = new BookieSocketAddress("127.0.0.1:3181");
        BOOKIE2 = new BookieSocketAddress("127.0.0.2:3181");
        BOOKIE3 = new BookieSocketAddress("127.0.0.3:3181");
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        localZkS.close();
    }

    @Test
    public void testBasic() throws Exception {
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"/rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"/rack1\", \"hostname\": \"bookie2.example.com\"}}}";
        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // Case1: ZKCache is given
        ZkBookieRackAffinityMapping mapping1 = new ZkBookieRackAffinityMapping();
        ClientConfiguration bkClientConf1 = new ClientConfiguration();
        bkClientConf1.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        assertNull(bkClientConf1.getProperty(ZkBookieRackAffinityMapping.ZK_DATA_CACHE_BK_RACK_CONF_INSTANCE));
        mapping1.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping1.setConf(bkClientConf1);
        List<String> racks1 = mapping1
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));
        assertEquals(racks1.get(0), "/rack0");
        assertEquals(racks1.get(1), "/rack1");
        assertEquals(racks1.get(2), null);
        assertNotNull(bkClientConf1.getProperty(ZkBookieRackAffinityMapping.ZK_DATA_CACHE_BK_RACK_CONF_INSTANCE));

        // Case 2: ZkServers and ZkTimeout are given (ZKCache will be constructed in
        // ZkBookieRackAffinityMapping#setConf)
        ZkBookieRackAffinityMapping mapping2 = new ZkBookieRackAffinityMapping();
        ClientConfiguration bkClientConf2 = new ClientConfiguration();
        bkClientConf2.setZkServers("127.0.0.1" + ":" + localZkS.getZookeeperPort());
        bkClientConf2.setZkTimeout(1000);
        mapping2.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping2.setConf(bkClientConf2);
        List<String> racks2 = mapping2
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));
        assertEquals(racks2.get(0), "/rack0");
        assertEquals(racks2.get(1), "/rack1");
        assertEquals(racks2.get(2), null);

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        ZkBookieRackAffinityMapping mapping = new ZkBookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        assertEquals(racks.get(0), null);
        assertEquals(racks.get(1), null);
        assertEquals(racks.get(2), null);

        Map<String, Map<BookieSocketAddress, BookieInfo>> bookieMapping = new HashMap<>();
        Map<BookieSocketAddress, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("/rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("/rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Awaitility.await()
                .until(() -> localZkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) != null);

        racks = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(racks.get(2), null);

        localZkc.delete(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, -1);
    }

    @Test
    public void testBookieInfoChange() throws Exception {
        Map<String, Map<BookieSocketAddress, BookieInfo>> bookieMapping = new HashMap<>();
        Map<BookieSocketAddress, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        ZkUtils.createFullPathOptimistic(localZkc, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                jsonMapper.writeValueAsBytes(bookieMapping), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ZkBookieRackAffinityMapping mapping = new ZkBookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, new ZooKeeperCache("test", localZkc, 30) {
        });
        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(racks.get(2), null);

        // add info for BOOKIE3 and check if the mapping picks up the change
        Map<BookieSocketAddress, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put("group2", secondaryBookieGroup);
        byte[] data = jsonMapper.writeValueAsBytes(bookieMapping);
        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data,-1);

        // wait for the zk to notify and update the mappings
        Awaitility.await().until(() -> Arrays
                .equals(data, localZkc.getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null)));

        racks = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(racks.get(2), "/rack0");

        localZkc.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "{}".getBytes(), -1);

        Awaitility.await().until(() -> Arrays.equals("{}".getBytes(),
                localZkc.getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null)));

        racks = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        assertEquals(racks.get(0), null);
        assertEquals(racks.get(1), null);
        assertEquals(racks.get(2), null);
    }
}
