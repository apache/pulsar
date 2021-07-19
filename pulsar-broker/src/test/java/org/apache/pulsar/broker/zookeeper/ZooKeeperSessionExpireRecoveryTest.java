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
package org.apache.pulsar.broker.zookeeper;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ZooKeeperSessionExpireRecoveryTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Verify we are able to recover when receiving a SessionExpired event on global ZK session
     */
    @Test
    public void testSessionExpired() throws Exception {
        admin.clusters().createCluster("my-cluster", ClusterData.builder().serviceUrl("test-url").build());

        assertTrue(Sets.newHashSet(admin.clusters().getClusters()).contains("my-cluster"));

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.CREATE
                    && path.equals("/admin/clusters/my-cluster-2");
            });

        assertTrue(Sets.newHashSet(admin.clusters().getClusters()).contains("my-cluster"));

        try {
            admin.clusters().createCluster("my-cluster-2", ClusterData.builder().serviceUrl("test-url").build());
            fail("Should have failed, because global zk is down");
        } catch (PulsarAdminException e) {
            // Ok
        }

        admin.clusters().createCluster("cluster-2", ClusterData.builder().serviceUrl("test-url").build());
    }
}
