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
package org.apache.pulsar.broker.loadbalance.extensions.scheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.manager.SplitManager;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.strategy.NamespaceBundleSplitStrategy;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.stats.Metrics;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SplitSchedulerTest {

    PulsarService pulsar;
    ServiceConfiguration config;
    NamespaceBundleFactory namespaceBundleFactory;
    LoadManagerContext context;
    ServiceUnitStateChannel channel;
    NamespaceBundleSplitStrategy strategy;
    String bundle1 = "tenant/namespace/0x00000000_0xFFFFFFFF";
    String bundle2 = "tenant/namespace/0x00000000_0x0FFFFFFF";

    String childBundle12 = "tenant/namespace/0x7fffffff_0xffffffff";

    String childBundle11 = "tenant/namespace/0x00000000_0x7fffffff";

    String childBundle22 = "tenant/namespace/0x7fffffff_0x0fffffff";

    String childBundle21 = "tenant/namespace/0x00000000_0x7fffffff";

    String broker = "broker-1";
    SplitDecision decision1;
    SplitDecision decision2;

    @BeforeMethod
    public void setUp() {

        config = new ServiceConfiguration();
        config.setLoadBalancerDebugModeEnabled(true);

        pulsar = mock(PulsarService.class);
        namespaceBundleFactory = mock(NamespaceBundleFactory.class);
        context = mock(LoadManagerContext.class);
        channel = mock(ServiceUnitStateChannel.class);
        strategy = mock(NamespaceBundleSplitStrategy.class);

        doReturn(config).when(pulsar).getConfiguration();
        doReturn(true).when(namespaceBundleFactory).canSplitBundle(any());
        doReturn(CompletableFuture.completedFuture(null)).when(channel).publishSplitEventAsync(any());

        decision1 = new SplitDecision();
        Split split = new Split(bundle1, broker, Map.of(
                childBundle11, Optional.empty(), childBundle12, Optional.empty()));
        decision1.setSplit(split);
        decision1.succeed(SplitDecision.Reason.MsgRate);

        decision2 = new SplitDecision();
        Split split2 = new Split(bundle2, broker, Map.of(
                childBundle21, Optional.empty(), childBundle22, Optional.empty()));
        decision2.setSplit(split2);
        decision2.succeed(SplitDecision.Reason.Sessions);
        Set<SplitDecision> decisions = Set.of(decision1, decision2);
        doReturn(decisions).when(strategy).findBundlesToSplit(any(), any());
    }

    @Test(timeOut = 30 * 1000)
    public void testExecuteSuccess() {
        AtomicReference<List<Metrics>> reference = new AtomicReference();
        SplitCounter counter = new SplitCounter();
        SplitManager manager = mock(SplitManager.class);
        SplitScheduler scheduler = new SplitScheduler(pulsar, channel, manager, counter, reference, context, strategy);
        doAnswer((invocation)->{
            var decision = invocation.getArgument(2, SplitDecision.class);
            counter.update(decision);
            return CompletableFuture.completedFuture(null);
        }).when(manager).waitAsync(any(), any(), any(), anyLong(), any());
        scheduler.execute();

        var counterExpected = new SplitCounter();
        counterExpected.update(decision1);
        counterExpected.update(decision2);
        verify(channel, times(1)).publishSplitEventAsync(eq(decision1.getSplit()));
        verify(channel, times(1)).publishSplitEventAsync(eq(decision2.getSplit()));

        assertEquals(reference.get().toString(), counterExpected.toMetrics(pulsar.getAdvertisedAddress()).toString());

        // Test empty splits.
        Set<SplitDecision> emptyUnload = Set.of();
        doReturn(emptyUnload).when(strategy).findBundlesToSplit(any(), any());

        scheduler.execute();
        verify(channel, times(2)).publishSplitEventAsync(any());
        assertEquals(reference.get().toString(), counterExpected.toMetrics(pulsar.getAdvertisedAddress()).toString());
    }

    @Test(timeOut = 30 * 1000)
    public void testExecuteFailure() {
        AtomicReference<List<Metrics>> reference = new AtomicReference();
        SplitCounter counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        SplitScheduler scheduler = new SplitScheduler(pulsar, channel, manager, counter, reference, context, strategy);
        doReturn(CompletableFuture.failedFuture(new RuntimeException())).when(channel).publishSplitEventAsync(any());

        scheduler.execute();


        var counterExpected = new SplitCounter();
        counterExpected.update(SplitDecision.Label.Failure, SplitDecision.Reason.Unknown);
        counterExpected.update(SplitDecision.Label.Failure, SplitDecision.Reason.Unknown);
        verify(channel, times(1)).publishSplitEventAsync(eq(decision1.getSplit()));
        verify(channel, times(1)).publishSplitEventAsync(eq(decision2.getSplit()));

        assertEquals(reference.get().toString(), counterExpected.toMetrics(pulsar.getAdvertisedAddress()).toString());
    }


    @Test(timeOut = 30 * 1000)
    public void testDisableLoadBalancer() {

        config.setLoadBalancerEnabled(false);
        SplitScheduler scheduler = new SplitScheduler(pulsar, channel, null, null, null, context, strategy);

        scheduler.execute();

        verify(strategy, times(0)).findBundlesToSplit(any(), any());

        config.setLoadBalancerEnabled(true);
        config.setLoadBalancerAutoBundleSplitEnabled(false);
        scheduler.execute();

        verify(strategy, times(0)).findBundlesToSplit(any(), any());
    }
}
