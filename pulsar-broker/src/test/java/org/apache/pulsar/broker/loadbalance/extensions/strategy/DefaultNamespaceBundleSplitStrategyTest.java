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
package org.apache.pulsar.broker.loadbalance.extensions.strategy;

import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import com.google.common.hash.Hashing;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarStats;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class DefaultNamespaceBundleSplitStrategyTest {

    PulsarService pulsar;
    ExtensibleLoadManagerWrapper loadManagerWrapper;
    ExtensibleLoadManagerImpl loadManager;
    ServiceUnitStateChannel channel;
    BrokerService brokerService;
    PulsarStats pulsarStats;
    Map<String, NamespaceBundleStats> bundleStats;
    ServiceConfiguration config;
    NamespaceBundleFactory namespaceBundleFactory;
    NamespaceService namespaceService;

    LoadManagerContext loadManagerContext;

    BrokerRegistry brokerRegistry;

    String bundle1 = "tenant/namespace/0x00000000_0xFFFFFFFF";
    String bundle2 = "tenant/namespace/0x00000000_0x0FFFFFFF";
    Long splitBoundary1 = 0x7fffffffL;
    Long splitBoundary2 = 0x07ffffffL;
    String childBundle12 = "0x7fffffff_0xffffffff";
    String childBundle11 = "0x00000000_0x7fffffff";
    String childBundle22 = "0x07ffffff_0x0fffffff";
    String childBundle21 = "0x00000000_0x07ffffff";
    String broker = "broker-1";

    @BeforeMethod
    void setup() {
        config = new ServiceConfiguration();
        config.setLoadBalancerDebugModeEnabled(true);
        config.setLoadBalancerNamespaceMaximumBundles(100);
        config.setLoadBalancerNamespaceBundleMaxTopics(100);
        config.setLoadBalancerNamespaceBundleMaxSessions(100);
        config.setLoadBalancerNamespaceBundleMaxMsgRate(100);
        config.setLoadBalancerNamespaceBundleMaxBandwidthMbytes(100);
        config.setLoadBalancerMaxNumberOfBundlesToSplitPerCycle(1);
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(3);

        pulsar = mock(PulsarService.class);
        brokerService = mock(BrokerService.class);
        pulsarStats = mock(PulsarStats.class);
        namespaceService = mock(NamespaceService.class);

        loadManagerContext = mock(LoadManagerContext.class);
        brokerRegistry = mock(BrokerRegistry.class);
        loadManagerWrapper = mock(ExtensibleLoadManagerWrapper.class);
        loadManager = mock(ExtensibleLoadManagerImpl.class);
        channel = mock(ServiceUnitStateChannelImpl.class);


        doReturn(mock(MetadataStoreExtended.class)).when(pulsar).getLocalMetadataStore();
        namespaceBundleFactory = spy(new NamespaceBundleFactory(pulsar, Hashing.crc32()));
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(config).when(pulsar).getConfiguration();
        doReturn(pulsarStats).when(brokerService).getPulsarStats();
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(namespaceBundleFactory).when(namespaceService).getNamespaceBundleFactory();
        doReturn(brokerRegistry).when(loadManagerContext).brokerRegistry();
        doReturn(broker).when(brokerRegistry).getBrokerId();
        doReturn(new AtomicReference(loadManagerWrapper)).when(pulsar).getLoadManager();
        doReturn(loadManager).when(loadManagerWrapper).get();
        doReturn(channel).when(loadManager).getServiceUnitStateChannel();
        doReturn(true).when(channel).isOwner(any());

        var namespaceBundle1 = namespaceBundleFactory.getBundle(
                LoadManagerShared.getNamespaceNameFromBundleName(bundle1),
                LoadManagerShared.getBundleRangeFromBundleName(bundle1));
        var namespaceBundle2 = namespaceBundleFactory.getBundle(
                LoadManagerShared.getNamespaceNameFromBundleName(bundle2),
                LoadManagerShared.getBundleRangeFromBundleName(bundle2));
        doReturn(CompletableFuture.completedFuture(
                List.of(splitBoundary1))).when(namespaceService).getSplitBoundary(
                        eq(namespaceBundle1), eq((List<Long>)null), any());
        doReturn(CompletableFuture.completedFuture(
                List.of(splitBoundary2))).when(namespaceService).getSplitBoundary(
                        eq(namespaceBundle2), eq((List<Long>)null), any());

        bundleStats = new LinkedHashMap<>();
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.topics = 5;
        bundleStats.put(bundle1, stats1);
        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.topics = 5;
        bundleStats.put(bundle2, stats2);
        doReturn(bundleStats).when(brokerService).getBundleStats();
    }

    public void testNamespaceBundleSplitConditionThreshold() {
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(0);
        bundleStats.values().forEach(v -> v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() + 1);
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(new SplitCounter());
        var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
        assertEquals(actual.size(), 1);
    }


    public void testNotEnoughTopics() {
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(0);
        bundleStats.values().forEach(v -> v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() + 1);
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(new SplitCounter());
        bundleStats.values().forEach(v -> v.topics = 1);
        var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
        var expected = Set.of();
        assertEquals(actual, expected);
    }

    public void testNamespaceMaximumBundles() throws Exception {
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(0);
        bundleStats.values().forEach(v -> v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() + 1);
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(new SplitCounter());
        doReturn(config.getLoadBalancerNamespaceMaximumBundles()).when(namespaceService).getBundleCount(any());
        var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
        var expected = Set.of();
        assertEquals(actual, expected);
    }

    public void testEmptyBundleStats() {
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(0);
        bundleStats.values().forEach(v -> v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() + 1);
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(new SplitCounter());
        bundleStats.clear();
        var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
        var expected = Set.of();
        assertEquals(actual, expected);
    }

    public void testNoBundleOwner() {
        var counter = spy(new SplitCounter());
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(0);
        bundleStats.values().forEach(v -> v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() + 1);
        doReturn(false).when(channel).isOwner(any());
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(counter);
        var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
        var expected = Set.of();
        assertEquals(actual, expected);
        verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
    }

    public void testError() throws Exception {
        var counter = spy(new SplitCounter());
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(0);
        bundleStats.values().forEach(v -> v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() + 1);
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(counter);
        doThrow(new RuntimeException()).when(namespaceService).getBundleCount(any());
        var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
        var expected = Set.of();
        assertEquals(actual, expected);
        verify(counter, times(2)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
    }

    public void testSplittingBundle() {
        var counter = spy(new SplitCounter());
        config.setLoadBalancerNamespaceBundleSplitConditionHitCountThreshold(0);
        bundleStats.values().forEach(v -> v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() + 1);
        doReturn(Map.of("tenant/namespace/0x00000000_0xFFFFFFFF",
                new ServiceUnitStateData(ServiceUnitState.Splitting, broker, 1)).entrySet())
                .when(channel).getOwnershipEntrySet();
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(counter);
        var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
        var expected = Set.of();
        assertEquals(actual, expected);
        verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
    }

    public void testMaxMsgRate() {
        var counter = spy(new SplitCounter());
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(counter);
        int threshold = config.getLoadBalancerNamespaceBundleSplitConditionHitCountThreshold();
        bundleStats.values().forEach(v -> {
            v.msgRateOut = config.getLoadBalancerNamespaceBundleMaxMsgRate() / 2 + 1;
            v.msgRateIn = config.getLoadBalancerNamespaceBundleMaxMsgRate() / 2 + 1;
        });
        for (int i = 0; i < threshold + 2; i++) {
            var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
            if (i == threshold) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle1, broker, Map.of(
                        childBundle11, Optional.empty(), childBundle12, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.MsgRate);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else if (i == threshold + 1) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle2, broker, Map.of(
                        childBundle21, Optional.empty(), childBundle22, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.MsgRate);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else {
                assertEquals(actual, Set.of());
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            }
        }
    }

    public void testMaxTopics() {
        var counter = spy(new SplitCounter());
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(counter);
        int threshold = config.getLoadBalancerNamespaceBundleSplitConditionHitCountThreshold();
        bundleStats.values().forEach(v -> v.topics = config.getLoadBalancerNamespaceBundleMaxTopics() + 1);
        for (int i = 0; i < threshold + 2; i++) {
            var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
            if (i == threshold) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle1, broker, Map.of(
                        childBundle11, Optional.empty(), childBundle12, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.Topics);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else if (i == threshold + 1) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle2, broker, Map.of(
                        childBundle21, Optional.empty(), childBundle22, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.Topics);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else {
                assertEquals(actual, Set.of());
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            }
        }
    }

    public void testMaxSessions() {
        var counter = spy(new SplitCounter());
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(counter);
        int threshold = config.getLoadBalancerNamespaceBundleSplitConditionHitCountThreshold();
        bundleStats.values().forEach(v -> {
            v.producerCount = config.getLoadBalancerNamespaceBundleMaxSessions() / 2 + 1;
            v.consumerCount = config.getLoadBalancerNamespaceBundleMaxSessions() / 2 + 1;
        });
        for (int i = 0; i < threshold + 2; i++) {
            var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
            if (i == threshold) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle1, broker, Map.of(
                        childBundle11, Optional.empty(), childBundle12, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.Sessions);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else if (i == threshold + 1) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle2, broker, Map.of(
                        childBundle21, Optional.empty(), childBundle22, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.Sessions);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else {
                assertEquals(actual, Set.of());
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            }
        }
    }

    public void testMaxBandwidthMbytes() {
        var counter = spy(new SplitCounter());
        var strategy = new DefaultNamespaceBundleSplitStrategyImpl(counter);
        int threshold = config.getLoadBalancerNamespaceBundleSplitConditionHitCountThreshold();
        bundleStats.values().forEach(v -> {
            v.msgThroughputOut = config.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * 1024 * 1024 / 2 + 1;
            v.msgThroughputIn = config.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * 1024 * 1024 / 2 + 1;
        });
        for (int i = 0; i < threshold + 2; i++) {
            var actual = strategy.findBundlesToSplit(loadManagerContext, pulsar);
            if (i == threshold) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle1, broker, Map.of(
                        childBundle11, Optional.empty(), childBundle12, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.Bandwidth);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else if (i == threshold + 1) {
                SplitDecision decision1 = new SplitDecision();
                Split split = new Split(bundle2, broker, Map.of(
                        childBundle21, Optional.empty(), childBundle22, Optional.empty()));
                decision1.setSplit(split);
                decision1.succeed(SplitDecision.Reason.Bandwidth);

                assertEquals(actual, Set.of(decision1));
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            } else {
                assertEquals(actual, Set.of());
                verify(counter, times(0)).update(eq(SplitDecision.Label.Failure), eq(Unknown));
            }
        }
    }

}
