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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.testng.annotations.Test;

@Test
public class LocalZooKeeperConnectionServiceTest {

    @Test
    void testSimpleZooKeeperConnection() throws Exception {
        MockedZooKeeperClientFactoryImpl mockZkClientFactory = new MockedZooKeeperClientFactoryImpl();
        LocalZooKeeperConnectionService localZkConnectionService = new LocalZooKeeperConnectionService(
                mockZkClientFactory, "dummy", 1000);
        localZkConnectionService.start(null);

        // Get ZooKeeper client
        MockZooKeeper zk = (MockZooKeeper) localZkConnectionService.getLocalZooKeeper();

        // Check status
        assertTrue(zk.getState().isConnected());

        // Create persistent node
        LocalZooKeeperConnectionService.checkAndCreatePersistNode(zk, "/path1");
        assertNotNull(zk.exists("/path1", false));

        // Delete and re-create existing node
        zk.setSessionId(-1L); // The sessionId must be set to except 0L in order to re-create.
        LocalZooKeeperConnectionService.createIfAbsent(zk, "/path1", "data1", CreateMode.EPHEMERAL, true);
        assertEquals(zk.getData("/path1", null, null), "data1".getBytes());

        // Try to create existing node (nothing should happen)
        LocalZooKeeperConnectionService.checkAndCreatePersistNode(zk, "/path1");
        assertNotNull(zk.exists("/path1", false));

        // Create new node (data is given as String)
        LocalZooKeeperConnectionService.createIfAbsent(zk, "/path2", "data2", CreateMode.EPHEMERAL);
        assertNotNull(zk.exists("/path2", false));
        assertEquals(zk.getData("/path2", null, null), "data2".getBytes());

        // Create new node (data is given as bytes)
        LocalZooKeeperConnectionService.createIfAbsent(zk, "/path3", "data3".getBytes(), CreateMode.EPHEMERAL);
        assertNotNull(zk.exists("/path3", false));
        assertEquals(zk.getData("/path3", null, null), "data3".getBytes());

        // delete nodes
        LocalZooKeeperConnectionService.deleteIfExists(zk, "/path1", -1);
        assertNull(zk.exists("/path1", false));
        LocalZooKeeperConnectionService.deleteIfExists(zk, "/path2", -1);
        assertNull(zk.exists("/path2", false));
        LocalZooKeeperConnectionService.deleteIfExists(zk, "/path3", -1);
        assertNull(zk.exists("/path3", false));

        // delete not existing node
        LocalZooKeeperConnectionService.deleteIfExists(zk, "/not_exist", -1);

        // Try to create invalid node (nothing should happen)
        LocalZooKeeperConnectionService.checkAndCreatePersistNode(zk, "/////");
        assertNull(zk.exists("//////", false));

        localZkConnectionService.close();

        mockZkClientFactory.close();
    }

    @Test
    void testSimpleZooKeeperConnectionFail() throws Exception {
        LocalZooKeeperConnectionService localZkConnectionService = new LocalZooKeeperConnectionService(
                new ZookeeperClientFactoryImpl(), "dummy", 1000);
        try {
            localZkConnectionService.start(null);
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Failed to establish session with local ZK"));
        }
        localZkConnectionService.close();
    }
}