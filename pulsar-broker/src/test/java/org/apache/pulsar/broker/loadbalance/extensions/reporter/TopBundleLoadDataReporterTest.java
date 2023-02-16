/*
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
package org.apache.pulsar.broker.loadbalance.extensions.reporter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.models.TopKBundles;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarStats;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopBundleLoadDataReporterTest {
    PulsarService pulsar;
    LoadDataStore store;
    BrokerService brokerService;
    PulsarStats pulsarStats;
    Map<String, NamespaceBundleStats> bundleStats;
    ServiceConfiguration config;

    @BeforeMethod
    void setup() {
        config = new ServiceConfiguration();
        pulsar = mock(PulsarService.class);
        store = mock(LoadDataStore.class);
        brokerService = mock(BrokerService.class);
        pulsarStats = mock(PulsarStats.class);
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(config).when(pulsar).getConfiguration();
        doReturn(pulsarStats).when(brokerService).getPulsarStats();
        doReturn(CompletableFuture.completedFuture(null)).when(store).pushAsync(any(), any());

        bundleStats = new HashMap<>();
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put("bundle-1", stats1);
        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put("bundle-2", stats2);
        doReturn(bundleStats).when(brokerService).getBundleStats();
    }

    public void testZeroUpdatedAt() {
        doReturn(0l).when(pulsarStats).getUpdatedAt();
        var target = new TopBundleLoadDataReporter(pulsar, "", store);
        assertNull(target.generateLoadData());
    }

    public void testGenerateLoadData() throws IllegalAccessException {
        doReturn(1l).when(pulsarStats).getUpdatedAt();
        config.setLoadBalancerBundleLoadReportPercentage(100);
        var target = new TopBundleLoadDataReporter(pulsar, "", store);
        var expected = new TopKBundles();
        expected.update(bundleStats, 2);
        assertEquals(target.generateLoadData(), expected.getLoadData());

        config.setLoadBalancerBundleLoadReportPercentage(50);
        FieldUtils.writeDeclaredField(target, "lastBundleStatsUpdatedAt", 0l, true);
        expected = new TopKBundles();
        expected.update(bundleStats, 1);
        assertEquals(target.generateLoadData(), expected.getLoadData());

        config.setLoadBalancerBundleLoadReportPercentage(1);
        FieldUtils.writeDeclaredField(target, "lastBundleStatsUpdatedAt", 0l, true);
        expected = new TopKBundles();
        expected.update(bundleStats, 1);
        assertEquals(target.generateLoadData(), expected.getLoadData());

        doReturn(new HashMap()).when(brokerService).getBundleStats();
        FieldUtils.writeDeclaredField(target, "lastBundleStatsUpdatedAt", 0l, true);
        expected = new TopKBundles();
        assertEquals(target.generateLoadData(), expected.getLoadData());
    }


    public void testReportForce()  {
        var target = new TopBundleLoadDataReporter(pulsar, "broker-1", store);
        target.reportAsync(false);
        verify(store, times(0)).pushAsync(any(), any());
        target.reportAsync(true);
        verify(store, times(1)).pushAsync("broker-1", new TopBundlesLoadData());

    }

    public void testReport(){
        var target = new TopBundleLoadDataReporter(pulsar, "broker-1", store);
        doReturn(1l).when(pulsarStats).getUpdatedAt();
        var expected = new TopKBundles();
        expected.update(bundleStats, 1);
        target.reportAsync(false);
        verify(store, times(1)).pushAsync("broker-1", expected.getLoadData());
    }

}
