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
package org.apache.pulsar.proxy.server;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.MoreExecutors;

public class BrokerDiscoveryProviderTest {

    private final String DUMMY_VALUE = "DUMMY_VALUE";
    private final String TEST_CLUSTER_NAME = "test";

    private MockZooKeeper mockZooKeeper;
    private ZooKeeperClientFactory mockZooKeeperClientFactory;
    private BrokerDiscoveryProvider brokerDiscoveryProvider;
    private ClusterData testCluster;

    @BeforeClass
    public void setup() throws Exception {
        mockZooKeeper = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        mockZooKeeperClientFactory = new ZooKeeperClientFactory() {
            @Override
            public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                    int zkSessionTimeoutMillis) {
                return CompletableFuture.completedFuture(mockZooKeeper);
            }
        };

        List<ACL> dummyAclList = new ArrayList<ACL>(0);
        testCluster = new ClusterData("http://localhost:8080", "https://localhost:8443");
        ZkUtils.createFullPathOptimistic(mockZooKeeper, "/admin/clusters/" + TEST_CLUSTER_NAME,
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(testCluster), dummyAclList,
                CreateMode.PERSISTENT);

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(DUMMY_VALUE);

        brokerDiscoveryProvider = new BrokerDiscoveryProvider(proxyConfig, mockZooKeeperClientFactory);
    }

    @AfterClass
    public void cleanup() throws Exception {
        brokerDiscoveryProvider.close();
        mockZooKeeper.close();
    }

    @Test
    public void testClusterMetadata() throws Exception {
        ClusterData clusterData = brokerDiscoveryProvider.getClusterMetadata(TEST_CLUSTER_NAME).get(10L,
                TimeUnit.SECONDS);
        Assert.assertEquals(clusterData, testCluster);
    }

    @Test
    public void testNonExistingClusterMetadata() throws Exception {
        try {
            brokerDiscoveryProvider.getClusterMetadata("non-existing").get(10L, TimeUnit.SECONDS);
            Assert.fail("Should fail");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
        }
    }

}
