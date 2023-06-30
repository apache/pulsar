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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarStats;
import org.apache.pulsar.broker.stats.BrokerStats;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.awaitility.Awaitility;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerLoadDataReporterTest {
    PulsarService pulsar;
    LoadDataStore store;
    BrokerService brokerService;
    PulsarStats pulsarStats;
    ServiceConfiguration config;
    BrokerStats brokerStats;
    SystemResourceUsage usage;
    String broker = "broker1";
    String bundle = "bundle1";
    ScheduledExecutorService executor;

    @BeforeMethod
    void setup() {
        config = new ServiceConfiguration();
        config.setLoadBalancerDebugModeEnabled(true);
        pulsar = mock(PulsarService.class);
        store = mock(LoadDataStore.class);
        brokerService = mock(BrokerService.class);
        pulsarStats = mock(PulsarStats.class);
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(config).when(pulsar).getConfiguration();
        executor = Executors
                .newSingleThreadScheduledExecutor(new
                        ExecutorProvider.ExtendedThreadFactory("pulsar-load-manager"));
        doReturn(executor).when(pulsar).getLoadManagerExecutor();
        doReturn(pulsarStats).when(brokerService).getPulsarStats();
        brokerStats = new BrokerStats(0);
        brokerStats.topics = 6;
        brokerStats.bundleCount = 5;
        brokerStats.msgRateIn = 3;
        brokerStats.msgRateOut = 4;
        brokerStats.msgThroughputIn = 1;
        brokerStats.msgThroughputOut = 2;
        doReturn(pulsarStats).when(brokerService).getPulsarStats();
        doReturn(brokerStats).when(pulsarStats).getBrokerStats();
        doReturn(CompletableFuture.completedFuture(null)).when(store).pushAsync(any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(store).removeAsync(any());

        usage = new SystemResourceUsage();
        usage.setCpu(new ResourceUsage(1.0, 100.0));
        usage.setMemory(new ResourceUsage(800.0, 200.0));
        usage.setDirectMemory(new ResourceUsage(2.0, 100.0));
        usage.setBandwidthIn(new ResourceUsage(3.0, 100.0));
        usage.setBandwidthOut(new ResourceUsage(4.0, 100.0));
    }

    @AfterMethod
    void shutdown(){
        executor.shutdown();
    }

    public void testGenerate() throws IllegalAccessException {
        try (MockedStatic<LoadManagerShared> mockLoadManagerShared = Mockito.mockStatic(LoadManagerShared.class)) {
            mockLoadManagerShared.when(() -> LoadManagerShared.getSystemResourceUsage(any())).thenReturn(usage);
            doReturn(0l).when(pulsarStats).getUpdatedAt();
            var target = new BrokerLoadDataReporter(pulsar, "", store);
            var expected = new BrokerLoadData();
            expected.update(usage, 1, 2, 3, 4, 5, 6, config);
            FieldUtils.writeDeclaredField(expected, "updatedAt", 0l, true);
            var actual = target.generateLoadData();
            FieldUtils.writeDeclaredField(actual, "updatedAt", 0l, true);
            assertEquals(actual, expected);
        }
    }

    public void testReport() throws IllegalAccessException {
        try (MockedStatic<LoadManagerShared> mockLoadManagerShared = Mockito.mockStatic(LoadManagerShared.class)) {
            mockLoadManagerShared.when(() -> LoadManagerShared.getSystemResourceUsage(any())).thenReturn(usage);
            var target = new BrokerLoadDataReporter(pulsar, "broker-1", store);
            var localData = (BrokerLoadData) FieldUtils.readDeclaredField(target, "localData", true);
            localData.setReportedAt(System.currentTimeMillis());
            var lastData = (BrokerLoadData) FieldUtils.readDeclaredField(target, "lastData", true);
            lastData.update(usage, 1, 2, 3, 4, 5, 6, config);
            target.reportAsync(false);
            verify(store, times(0)).pushAsync(any(), any());

            target.reportAsync(true);
            verify(store, times(1)).pushAsync(eq("broker-1"), any());

            target.reportAsync(false);
            verify(store, times(1)).pushAsync(eq("broker-1"), any());

            localData.setReportedAt(0l);
            target.reportAsync(false);
            verify(store, times(2)).pushAsync(eq("broker-1"), any());

            lastData.update(usage, 10000, 2, 3, 4, 5, 6, config);
            target.reportAsync(false);
            verify(store, times(3)).pushAsync(eq("broker-1"), any());
        }
    }

    @Test
    public void testTombstone() throws IllegalAccessException, InterruptedException {

        var target = spy(new BrokerLoadDataReporter(pulsar, broker, store));

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
            var localData = (BrokerLoadData) FieldUtils.readDeclaredField(target, "localData", true);
            assertEquals(localData, new BrokerLoadData());
        });

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, "broker-2", broker, VERSION_ID_INIT), null);
        Awaitility.waitAtMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(target, times(2)).tombstone();
            verify(store, times(1)).removeAsync(eq(broker));
            var localData = (BrokerLoadData) FieldUtils.readDeclaredField(target, "localData", true);
            assertEquals(localData, new BrokerLoadData());
        });

        FieldUtils.writeDeclaredField(target, "tombstoneDelayInMillis", 0, true);
        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Splitting, "broker-2", broker, VERSION_ID_INIT), null);
        Awaitility.waitAtMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(target, times(3)).tombstone();
            verify(store, times(2)).removeAsync(eq(broker));
            var localData = (BrokerLoadData) FieldUtils.readDeclaredField(target, "localData", true);
            assertEquals(localData, new BrokerLoadData());
        });

        target.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, broker, VERSION_ID_INIT), null);
        Awaitility.waitAtMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(target, times(4)).tombstone();
            verify(store, times(3)).removeAsync(eq(broker));
            var localData = (BrokerLoadData) FieldUtils.readDeclaredField(target, "localData", true);
            assertEquals(localData, new BrokerLoadData());
        });
    }
}
