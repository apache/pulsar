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
package org.apache.pulsar.discovery.service.web;

import static org.apache.pulsar.broker.resources.MetadataStoreCacheLoader.LOADBALANCE_BROKERS_ROOT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.pulsar.broker.resources.MetadataStoreCacheLoader;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.zookeeper.KeeperException;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class MetadataStoreCacheLoaderTest extends BaseZKStarterTest {

    @BeforeMethod
    private void init() throws Exception {
        start();
    }

    @AfterMethod(alwaysRun = true)
    private void cleanup() throws Exception {
        close();
    }

    /**
     * Create znode for available broker in ZooKeeper and updates it again to verify ZooKeeper cache update
     *
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    @Test
    public void testZookeeperCacheLoader() throws InterruptedException, KeeperException, Exception {

        PulsarResources resources = new PulsarResources(zkStore, null);
        @SuppressWarnings("resource")
        MetadataStoreCacheLoader zkLoader = new MetadataStoreCacheLoader(resources, 30_000);

        List<String> brokers = Lists.newArrayList("broker-1:15000", "broker-2:15000", "broker-3:15000");
        for (int i = 0; i < brokers.size(); i++) {
            try {
                LoadManagerReport report = i % 2 == 0 ? getSimpleLoadManagerLoadReport(brokers.get(i))
                        : getModularLoadManagerLoadReport(brokers.get(i));
                zkStore.put(LOADBALANCE_BROKERS_ROOT + "/" + brokers.get(i),
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(report), Optional.of(-1L))
                        .join();
            } catch (Exception e) {
                fail("failed while creating broker znodes");
            }
        }

        Awaitility.await().untilAsserted(() -> assertEquals(zkLoader.getAvailableBrokers().size(), 3));

        // 2. get available brokers from ZookeeperCacheLoader
        List<LoadManagerReport> list = zkLoader.getAvailableBrokers();

        // 3. verify retrieved broker list
        Set<String> cachedBrokers = list.stream().map(loadReport -> loadReport.getWebServiceUrl())
                .collect(Collectors.toSet());
        Assert.assertEquals(list.size(), brokers.size());
        Assert.assertTrue(brokers.containsAll(cachedBrokers));

        // 4.a add new broker
        final String newBroker = "broker-4:15000";
        LoadManagerReport report = getSimpleLoadManagerLoadReport(newBroker);
        zkStore.put(LOADBALANCE_BROKERS_ROOT + "/" + newBroker,
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(report), Optional.of(-1L))
                .join();
        brokers.add(newBroker);

        Thread.sleep(100); // wait for 100 msec: to get cache updated

        // 4.b. get available brokers from ZookeeperCacheLoader
        list = zkLoader.getAvailableBrokers();

        // 4.c. verify retrieved broker list
        cachedBrokers = list.stream().map(loadReport -> loadReport.getWebServiceUrl()).collect(Collectors.toSet());
        Assert.assertEquals(list.size(), brokers.size());
        Assert.assertTrue(brokers.containsAll(cachedBrokers));

    }

    private LoadReport getSimpleLoadManagerLoadReport(String brokerUrl) {
        return new LoadReport(brokerUrl, null, null, null);
    }

    private LocalBrokerData getModularLoadManagerLoadReport(String brokerUrl) {
        return new LocalBrokerData(brokerUrl, null, null, null);
    }
}
