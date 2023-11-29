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
package org.apache.pulsar.broker.loadbalance.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author hezhangjian
 */
@Slf4j
@Test(groups = "broker")
public class BundleSplitterTaskTest {

    private LocalBookkeeperEnsemble bkEnsemble;

    private PulsarService pulsar;

    @BeforeMethod
    void setup() throws Exception {
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        // Start broker
        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config.setClusterName("use");
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());

        config.setAdvertisedAddress("localhost");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        pulsar = new PulsarService(config);
        pulsar.start();
    }

    @Test
    public void testSplitTaskWhenTopicJustOne() {
        final BundleSplitterTask bundleSplitterTask = new BundleSplitterTask();
        LoadData loadData = new LoadData();

        LocalBrokerData brokerData = new LocalBrokerData();
        Map<String, NamespaceBundleStats> lastStats = new HashMap<>();
        final NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = 1;
        lastStats.put("ten/ns/0x00000000_0x80000000", namespaceBundleStats);
        brokerData.setLastStats(lastStats);
        loadData.getBrokerData().put("broker", new BrokerData(brokerData));

        BundleData bundleData = new BundleData();
        TimeAverageMessageData averageMessageData = new TimeAverageMessageData();
        averageMessageData.setMsgRateIn(pulsar.getConfiguration().getLoadBalancerNamespaceBundleMaxMsgRate());
        averageMessageData.setMsgRateOut(1);
        bundleData.setLongTermData(averageMessageData);
        loadData.getBundleData().put("ten/ns/0x00000000_0x80000000", bundleData);

        final Set<String> bundlesToSplit = bundleSplitterTask.findBundlesToSplit(loadData, pulsar);
        Assert.assertEquals(bundlesToSplit.size(), 0);
    }

    @Test
    public void testLoadBalancerNamespaceMaximumBundles() throws Exception {
        pulsar.getConfiguration().setLoadBalancerNamespaceMaximumBundles(3);

        final BundleSplitterTask bundleSplitterTask = new BundleSplitterTask();
        LoadData loadData = new LoadData();

        LocalBrokerData brokerData = new LocalBrokerData();
        Map<String, NamespaceBundleStats> lastStats = new HashMap<>();
        final NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = 5;
        lastStats.put("ten/ns/0x00000000_0x20000000", namespaceBundleStats);

        final NamespaceBundleStats namespaceBundleStats2 = new NamespaceBundleStats();
        namespaceBundleStats2.topics = 5;
        lastStats.put("ten/ns/0x20000000_0x40000000", namespaceBundleStats2);

        final NamespaceBundleStats namespaceBundleStats3 = new NamespaceBundleStats();
        namespaceBundleStats3.topics = 5;
        lastStats.put("ten/ns/0x40000000_0x60000000", namespaceBundleStats3);

        brokerData.setLastStats(lastStats);
        loadData.getBrokerData().put("broker", new BrokerData(brokerData));

        BundleData bundleData1 = new BundleData();
        TimeAverageMessageData averageMessageData1 = new TimeAverageMessageData();
        averageMessageData1.setMsgRateIn(pulsar.getConfiguration().getLoadBalancerNamespaceBundleMaxMsgRate() * 2);
        averageMessageData1.setMsgRateOut(1);
        bundleData1.setLongTermData(averageMessageData1);
        loadData.getBundleData().put("ten/ns/0x00000000_0x20000000", bundleData1);

        BundleData bundleData2 = new BundleData();
        TimeAverageMessageData averageMessageData2 = new TimeAverageMessageData();
        averageMessageData2.setMsgRateIn(pulsar.getConfiguration().getLoadBalancerNamespaceBundleMaxMsgRate() * 2);
        averageMessageData2.setMsgRateOut(1);
        bundleData2.setLongTermData(averageMessageData2);
        loadData.getBundleData().put("ten/ns/0x20000000_0x40000000", bundleData2);

        BundleData bundleData3 = new BundleData();
        TimeAverageMessageData averageMessageData3 = new TimeAverageMessageData();
        averageMessageData3.setMsgRateIn(pulsar.getConfiguration().getLoadBalancerNamespaceBundleMaxMsgRate() * 2);
        averageMessageData3.setMsgRateOut(1);
        bundleData3.setLongTermData(averageMessageData3);
        loadData.getBundleData().put("ten/ns/0x40000000_0x60000000", bundleData3);

        int currentBundleCount = pulsar.getNamespaceService().getBundleCount(NamespaceName.get("ten/ns"));
        final Set<String> bundlesToSplit = bundleSplitterTask.findBundlesToSplit(loadData, pulsar);
        Assert.assertEquals(bundlesToSplit.size() + currentBundleCount,
                pulsar.getConfiguration().getLoadBalancerNamespaceMaximumBundles());
    }


    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        pulsar.close();
        bkEnsemble.stop();
    }

}
