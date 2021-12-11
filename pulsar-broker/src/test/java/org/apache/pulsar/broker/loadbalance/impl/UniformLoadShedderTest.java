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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.NamespaceResources.IsolationPolicyResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.impl.MinAvailablePolicy;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@Test(groups = "broker")
public class UniformLoadShedderTest {

    private final UniformLoadShedder shedder = new UniformLoadShedder();
    private final ServiceConfiguration conf;

    public UniformLoadShedderTest() {
        conf = new ServiceConfiguration();
    }

    @Test
    public void testBrokerWithSingleBundle() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));
        broker1.setBundles(Sets.newHashSet("bundle-1"));

        BundleData bundle1 = new BundleData();
        TimeAverageMessageData db1 = new TimeAverageMessageData();
        db1.setMsgThroughputIn(1000);
        db1.setMsgThroughputOut(1000);
        bundle1.setShortTermData(db1);
        loadData.getBundleData().put("bundle-1", bundle1);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));

        assertTrue(shedder.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokerWithMsgRateIn() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        LocalBrokerData broker2 = new LocalBrokerData();
        LocalBrokerData broker3 = new LocalBrokerData();

        // broker-1 with 80K msgRate
        updateBundleData(loadData, "broker1", broker1, "bundle11", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle12", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle13", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle14", 10000, 10000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle15", 10000, 10000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle16", 10000, 10000, true);
        // broker-2 with 40K msgRate
        updateBundleData(loadData, "broker2", broker2, "bundle21", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle22", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle23", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle24", 5000, 5000, true);
        // broker-3 with 20K msgRate
        updateBundleData(loadData, "broker3", broker3, "bundle31", 5000, 5000, true);
        updateBundleData(loadData, "broker3", broker3, "bundle32", 5000, 5000, true);

        Multimap<String, String> bundlesToUnload = shedder.findBundlesForUnloading(loadData, conf);

        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker1"), Lists.newArrayList("bundle14", "bundle13"));
    }

    @Test
    public void testBrokerWithMsgRateOut() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        LocalBrokerData broker2 = new LocalBrokerData();
        LocalBrokerData broker3 = new LocalBrokerData();

        // broker-1 with 700MB throughput
        updateBundleData(loadData, "broker1", broker1, "bundle11", 50, 50, false);
        updateBundleData(loadData, "broker1", broker1, "bundle12", 100, 100, false);
        updateBundleData(loadData, "broker1", broker1, "bundle13", 50, 100, false);
        updateBundleData(loadData, "broker1", broker1, "bundle14", 100, 100, false);
        updateBundleData(loadData, "broker1", broker1, "bundle15", 100, 100, false);
        // broker-2 with 400MB throughput
        updateBundleData(loadData, "broker2", broker2, "bundle21", 50, 50, false);
        updateBundleData(loadData, "broker2", broker2, "bundle22", 50, 50, false);
        updateBundleData(loadData, "broker2", broker2, "bundle23", 50, 50, false);
        updateBundleData(loadData, "broker2", broker2, "bundle24", 50, 50, false);
        // broker-3 with 200MB throughput
        updateBundleData(loadData, "broker3", broker3, "bundle31", 50, 50, false);
        updateBundleData(loadData, "broker3", broker3, "bundle32", 50, 50, false);

        Multimap<String, String> bundlesToUnload = shedder.findBundlesForUnloading(loadData, conf);

        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker1"), Lists.newArrayList("bundle14", "bundle11"));
    }

    @Test
    public void testBrokerWithoutIsolationGroup() throws Exception {
        conf.setClusterName("test");
        PulsarService pulsar = mock(PulsarService.class);
        PulsarResources resources = mock(PulsarResources.class);
        doReturn(resources).when(pulsar).getPulsarResources();
        NamespaceResources nsResouce = mock(NamespaceResources.class);
        doReturn(nsResouce).when(resources).getNamespaceResources();
        IsolationPolicyResources isolationPolicies = mock(IsolationPolicyResources.class);
        doReturn(isolationPolicies).when(nsResouce).getIsolationPolicies();
        NamespaceIsolationPolicies nsIsolation = mock(NamespaceIsolationPolicies.class);
        Optional<NamespaceIsolationPolicies> policies = Optional.empty();
        doReturn(policies).when(isolationPolicies).getIsolationDataPolicies(anyString());

        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        LocalBrokerData broker2 = new LocalBrokerData();
        LocalBrokerData broker3 = new LocalBrokerData();

        // broker-1 with 80K msgRate
        updateBundleData(loadData, "broker1", broker1, "bundle11", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle12", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle13", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle14", 10000, 10000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle15", 10000, 10000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle16", 10000, 10000, true);
        // broker-2 with 40K msgRate
        updateBundleData(loadData, "broker2", broker2, "bundle21", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle22", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle23", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle24", 5000, 5000, true);
        // broker-3 with 20K msgRate
        updateBundleData(loadData, "broker3", broker3, "bundle31", 5000, 5000, true);
        updateBundleData(loadData, "broker3", broker3, "bundle32", 5000, 5000, true);

        Multimap<String, String> bundlesToUnload = shedder.findBundlesForUnloading(pulsar, loadData, conf);

        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker1"), Lists.newArrayList("bundle14", "bundle13"));
    }

    @Test
    public void testBrokerWithIsolationGroup() throws Exception {

        conf.setClusterName("test");
        PulsarService pulsar = mock(PulsarService.class);
        PulsarResources resources = mock(PulsarResources.class);
        doReturn(resources).when(pulsar).getPulsarResources();
        NamespaceResources nsResouce = mock(NamespaceResources.class);
        doReturn(nsResouce).when(resources).getNamespaceResources();
        IsolationPolicyResources isolationPolicies = mock(IsolationPolicyResources.class);
        doReturn(isolationPolicies).when(nsResouce).getIsolationPolicies();
        // get available brokers
        LoadManager manager = mock(LoadManager.class);
        doReturn(Sets.newHashSet("broker1", "broker2", "broker3")).when(manager).getAvailableBrokers();
        AtomicReference<LoadManager> loadManager = new AtomicReference<>(manager);
        doReturn(loadManager).when(pulsar).getLoadManager();
        // isolation
        NamespaceIsolationPolicies nsIsolation = mock(NamespaceIsolationPolicies.class);
        Optional<NamespaceIsolationPolicies> policies = Optional.of(nsIsolation);
        NamespaceIsolationDataImpl isolationImpl = mock(NamespaceIsolationDataImpl.class);
        doReturn(Collections.singletonMap("policy1", isolationImpl)).when(nsIsolation).getPolicies();
        doReturn(policies).when(isolationPolicies).getIsolationDataPolicies(anyString());
        doReturn(Lists.newArrayList()).when(isolationImpl).getSecondary();
        AutoFailoverPolicyType type = AutoFailoverPolicyType.min_available;
        Map<String, String> params = new HashMap<>();
        params.put(MinAvailablePolicy.MIN_LIMIT_KEY, "10");
        params.put(MinAvailablePolicy.USAGE_THRESHOLD_KEY, "10");
        doReturn(AutoFailoverPolicyData.builder().policyType(type).parameters(params).build()).when(isolationImpl)
                .getAutoFailoverPolicy();

        Optional<NamespaceIsolationPolicies> nsPoliciesResult = pulsar.getPulsarResources().getNamespaceResources()
                .getIsolationPolicies().getIsolationDataPolicies("test");

        doReturn(Lists.newArrayList()).when(isolationImpl).getNamespaces();

        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        LocalBrokerData broker2 = new LocalBrokerData();
        LocalBrokerData broker3 = new LocalBrokerData();

        // broker-1 with 80K msgRate
        updateBundleData(loadData, "broker1", broker1, "bundle11", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle12", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle13", 5000, 5000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle14", 10000, 10000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle15", 10000, 10000, true);
        updateBundleData(loadData, "broker1", broker1, "bundle16", 10000, 10000, true);
        // broker-2 with 40K msgRate
        updateBundleData(loadData, "broker2", broker2, "bundle21", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle22", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle23", 5000, 5000, true);
        updateBundleData(loadData, "broker2", broker2, "bundle24", 5000, 5000, true);
        // broker-3 with 20K msgRate
        updateBundleData(loadData, "broker3", broker3, "bundle31", 5000, 5000, true);
        updateBundleData(loadData, "broker3", broker3, "bundle32", 5000, 5000, true);

        // (1) isolation broker2,3
        doReturn(Lists.newArrayList("broker2", "broker3")).when(isolationImpl).getPrimary();
        Multimap<String, String> bundlesToUnload = shedder.findBundlesForUnloading(pulsar, loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker2"), Lists.newArrayList("bundle24"));

        // (2) isolation broker1,3
        doReturn(Lists.newArrayList("broker1", "broker3")).when(isolationImpl).getPrimary();
        bundlesToUnload = shedder.findBundlesForUnloading(pulsar, loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker1"), Lists.newArrayList("bundle14", "bundle13"));

        // (3) isolation broker1,2
        doReturn(Lists.newArrayList("broker1", "broker2")).when(isolationImpl).getPrimary();
        bundlesToUnload = shedder.findBundlesForUnloading(pulsar, loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker1"), Lists.newArrayList("bundle14"));
    }

    private void updateBundleData(LoadData loadData, String brokerName, LocalBrokerData brokerData, String bundleName,
            int rateIn, int rateOut, boolean msgRate) {
        Map<String, BundleData> bundleData = loadData.getBundleData();
        BundleData bundle1 = new BundleData();
        TimeAverageMessageData bundleData1 = new TimeAverageMessageData();
        if (msgRate) {
            bundleData1.setMsgRateIn(rateIn);
            bundleData1.setMsgRateOut(rateOut);
            bundle1.setShortTermData(bundleData1);
        } else {
            bundleData1.setMsgThroughputIn(rateIn);
            bundleData1.setMsgThroughputOut(rateOut);
            bundle1.setShortTermData(bundleData1);
        }
        bundleData.put(bundleName, bundle1);

        if (msgRate) {
            brokerData.setMsgRateIn(brokerData.getMsgRateIn() + rateIn);
            brokerData.setMsgRateOut(brokerData.getMsgRateOut() + rateOut);
        } else {
            brokerData.setMsgThroughputIn(brokerData.getMsgThroughputIn() + rateIn);
            ;
            brokerData.setMsgThroughputOut(brokerData.getMsgThroughputOut() + rateOut);
        }
        brokerData.getBundles().add(bundleName);

        BrokerData brokerData1 = new BrokerData(brokerData);
        loadData.getBrokerData().put(brokerName, brokerData1);
    }
}
