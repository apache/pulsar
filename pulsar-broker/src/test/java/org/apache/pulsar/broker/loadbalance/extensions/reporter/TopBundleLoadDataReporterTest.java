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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.VERSION_ID_INIT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.models.TopKBundles;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarStats;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.awaitility.Awaitility;
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
    private NamespaceResources.IsolationPolicyResources isolationPolicyResources;
    private PulsarResources pulsarResources;
    private LocalPoliciesResources localPoliciesResources;
    String bundle1 = "my-tenant/my-namespace1/0x00000000_0x0FFFFFFF";
    String bundle2 = "my-tenant/my-namespace2/0x00000000_0x0FFFFFFF";
    String bundle = bundle1;
    String broker = "broker-1";

    @BeforeMethod
    void setup() throws MetadataStoreException {
        config = new ServiceConfiguration();
        config.setLoadBalancerDebugModeEnabled(true);
        pulsar = mock(PulsarService.class);
        store = mock(LoadDataStore.class);
        brokerService = mock(BrokerService.class);
        pulsarStats = mock(PulsarStats.class);
        pulsarResources = mock(PulsarResources.class);
        isolationPolicyResources = mock(NamespaceResources.IsolationPolicyResources.class);
        var namespaceResources = mock(NamespaceResources.class);
        localPoliciesResources = mock(LocalPoliciesResources.class);

        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(config).when(pulsar).getConfiguration();
        doReturn(pulsarStats).when(brokerService).getPulsarStats();
        doReturn(CompletableFuture.completedFuture(null)).when(store).pushAsync(any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(store).removeAsync(any());

        doReturn(pulsarResources).when(pulsar).getPulsarResources();
        doReturn(namespaceResources).when(pulsarResources).getNamespaceResources();
        doReturn(isolationPolicyResources).when(namespaceResources).getIsolationPolicies();
        doReturn(Optional.empty()).when(isolationPolicyResources).getIsolationDataPolicies(any());

        doReturn(localPoliciesResources).when(pulsarResources).getLocalPolicies();
        doReturn(Optional.empty()).when(localPoliciesResources).getLocalPolicies(any());

        bundleStats = new HashMap<>();
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put(bundle1, stats1);
        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put(bundle2, stats2);
        doReturn(bundleStats).when(brokerService).getBundleStats();
    }

    public void testZeroUpdatedAt() {
        doReturn(0l).when(pulsarStats).getUpdatedAt();
        var target = new TopBundleLoadDataReporter(pulsar, "", store);
        assertNull(target.generateLoadData());
    }

    public void testGenerateLoadData() throws IllegalAccessException {
        doReturn(1l).when(pulsarStats).getUpdatedAt();
        config.setLoadBalancerMaxNumberOfBundlesInBundleLoadReport(2);
        var target = new TopBundleLoadDataReporter(pulsar, "", store);
        var expected = new TopKBundles(pulsar);
        expected.update(bundleStats, 2);
        assertEquals(target.generateLoadData(), expected.getLoadData());

        config.setLoadBalancerMaxNumberOfBundlesInBundleLoadReport(1);
        FieldUtils.writeDeclaredField(target, "lastBundleStatsUpdatedAt", 0l, true);
        expected = new TopKBundles(pulsar);
        expected.update(bundleStats, 1);
        assertEquals(target.generateLoadData(), expected.getLoadData());

        config.setLoadBalancerMaxNumberOfBundlesInBundleLoadReport(0);
        FieldUtils.writeDeclaredField(target, "lastBundleStatsUpdatedAt", 0l, true);

        expected = new TopKBundles(pulsar);
        expected.update(bundleStats, 0);
        assertEquals(target.generateLoadData(), expected.getLoadData());

        doReturn(new HashMap()).when(brokerService).getBundleStats();
        FieldUtils.writeDeclaredField(target, "lastBundleStatsUpdatedAt", 0l, true);
        expected = new TopKBundles(pulsar);
        assertEquals(target.generateLoadData(), expected.getLoadData());
    }


    public void testReportForce()  {
        var target = new TopBundleLoadDataReporter(pulsar, broker, store);
        target.reportAsync(false);
        verify(store, times(0)).pushAsync(any(), any());
        target.reportAsync(true);
        verify(store, times(1)).pushAsync(broker, new TopBundlesLoadData());

    }

    public void testReport(){
        pulsar.getConfiguration().setLoadBalancerMaxNumberOfBundlesInBundleLoadReport(1);
        var target = new TopBundleLoadDataReporter(pulsar, broker, store);
        doReturn(1l).when(pulsarStats).getUpdatedAt();
        var expected = new TopKBundles(pulsar);
        expected.update(bundleStats, 1);
        target.reportAsync(false);
        verify(store, times(1)).pushAsync(broker, expected.getLoadData());
    }

    @Test
    public void testTombstone() throws IllegalAccessException {

        var target = spy(new TopBundleLoadDataReporter(pulsar, broker, store));

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Assigning, broker, VERSION_ID_INIT), null);
        verify(store, times(0)).removeAsync(eq(broker));
        verify(target, times(0)).tombstone();

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Deleted, broker, VERSION_ID_INIT), null);
        verify(store, times(0)).removeAsync(eq(broker));
        verify(target, times(0)).tombstone();


        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Init, broker, VERSION_ID_INIT), null);
        verify(store, times(0)).removeAsync(eq(broker));
        verify(target, times(0)).tombstone();

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, broker, VERSION_ID_INIT), null);
        verify(store, times(0)).removeAsync(eq(broker));
        verify(target, times(0)).tombstone();

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, "broker-2", broker, VERSION_ID_INIT),
                new RuntimeException());
        verify(store, times(0)).removeAsync(eq(broker));
        verify(target, times(0)).tombstone();

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, "broker-2", broker, VERSION_ID_INIT), null);
        Awaitility.waitAtMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(target, times(1)).tombstone();
            verify(store, times(1)).removeAsync(eq(broker));
        });

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, "broker-2", broker, VERSION_ID_INIT), null);
        Awaitility.waitAtMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(target, times(2)).tombstone();
            verify(store, times(1)).removeAsync(eq(broker));
        });

        FieldUtils.writeDeclaredField(target, "tombstoneDelayInMillis", 0, true);
        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Splitting, "broker-2", broker, VERSION_ID_INIT), null);
        Awaitility.waitAtMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(target, times(3)).tombstone();
            verify(store, times(2)).removeAsync(eq(broker));
        });

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, broker, VERSION_ID_INIT), null);
        Awaitility.waitAtMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(target, times(4)).tombstone();
            verify(store, times(3)).removeAsync(eq(broker));
        });
    }
}
