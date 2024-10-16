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

import com.google.common.collect.Lists;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.manager.UnloadManager;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.stats.Metrics;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Test(groups = "broker")
public class UnloadSchedulerTest {
    private PulsarService pulsar;
    private ScheduledExecutorService loadManagerExecutor;

    public LoadManagerContext setupContext(){
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);
        return ctx;
    }

    @BeforeMethod
    public void setUp() {
        this.pulsar = mock(PulsarService.class);
        loadManagerExecutor = Executors
                .newSingleThreadScheduledExecutor(new
                        ExecutorProvider.ExtendedThreadFactory("pulsar-load-manager"));
        doReturn(loadManagerExecutor)
                .when(pulsar).getLoadManagerExecutor();
    }

    @AfterMethod
    public void tearDown() {
        this.loadManagerExecutor.shutdown();
    }

    @Test(timeOut = 30 * 1000)
    public void testExecuteSuccess() {
        AtomicReference<List<Metrics>> reference = new AtomicReference<>();
        UnloadCounter counter = new UnloadCounter();
        LoadManagerContext context = setupContext();
        BrokerRegistry registry = context.brokerRegistry();
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        UnloadManager unloadManager = mock(UnloadManager.class);
        PulsarService pulsar = mock(PulsarService.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        doReturn(CompletableFuture.completedFuture(true)).when(channel).isChannelOwnerAsync();
        doReturn(CompletableFuture.completedFuture(Lists.newArrayList("broker-1", "broker-2")))
                .when(registry).getAvailableBrokersAsync();
        doReturn(CompletableFuture.completedFuture(null)).when(channel).publishUnloadEventAsync(any());
        doReturn(CompletableFuture.completedFuture(null)).when(unloadManager)
                .waitAsync(any(), any(), any(), anyLong(), any());
        UnloadDecision decision = new UnloadDecision();
        Unload unload = new Unload("broker-1", "bundle-1");
        decision.setUnload(unload);
        decision.setLabel(UnloadDecision.Label.Success);
        doReturn(Set.of(decision)).when(unloadStrategy).findBundlesForUnloading(any(), any(), any());

        UnloadScheduler scheduler = new UnloadScheduler(pulsar, loadManagerExecutor, unloadManager, context,
                channel, unloadStrategy, counter, reference);

        scheduler.execute();

        verify(channel, times(1)).publishUnloadEventAsync(eq(unload));

        // Test empty unload.
        UnloadDecision emptyUnload = new UnloadDecision();
        doReturn(Set.of(emptyUnload)).when(unloadStrategy).findBundlesForUnloading(any(), any(), any());

        scheduler.execute();

        verify(channel, times(1)).publishUnloadEventAsync(eq(unload));
    }

    @Test(timeOut = 30 * 1000)
    public void testExecuteMoreThenOnceWhenFirstNotDone() throws InterruptedException {
        AtomicReference<List<Metrics>> reference = new AtomicReference<>();
        UnloadCounter counter = new UnloadCounter();
        LoadManagerContext context = setupContext();
        BrokerRegistry registry = context.brokerRegistry();
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        UnloadManager unloadManager = mock(UnloadManager.class);
        PulsarService pulsar = mock(PulsarService.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        doReturn(CompletableFuture.completedFuture(true)).when(channel).isChannelOwnerAsync();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(1);
        doAnswer(__ -> CompletableFuture.supplyAsync(() -> {
                try {
                    // Delay 5 seconds to finish.
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return Lists.newArrayList("broker-1", "broker-2");
            }, executor)).when(registry).getAvailableBrokersAsync();
        UnloadScheduler scheduler = new UnloadScheduler(pulsar, loadManagerExecutor, unloadManager, context,
                channel, unloadStrategy, counter, reference);
        @Cleanup("shutdownNow")
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            executorService.execute(() -> {
                scheduler.execute();
                latch.countDown();
            });
        }
        latch.await();

        verify(registry, times(5)).getAvailableBrokersAsync();
    }

    @Test(timeOut = 30 * 1000)
    public void testDisableLoadBalancer() {
        AtomicReference<List<Metrics>> reference = new AtomicReference<>();
        UnloadCounter counter = new UnloadCounter();
        LoadManagerContext context = setupContext();
        context.brokerConfiguration().setLoadBalancerEnabled(false);
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        UnloadManager unloadManager = mock(UnloadManager.class);
        PulsarService pulsar = mock(PulsarService.class);
        UnloadScheduler scheduler = new UnloadScheduler(pulsar, loadManagerExecutor, unloadManager, context,
                channel, unloadStrategy, counter, reference);
        scheduler.execute();

        verify(channel, times(0)).isChannelOwnerAsync();

        context.brokerConfiguration().setLoadBalancerEnabled(true);
        context.brokerConfiguration().setLoadBalancerSheddingEnabled(false);
        scheduler.execute();

        verify(channel, times(0)).isChannelOwnerAsync();
    }

    @Test(timeOut = 30 * 1000)
    public void testNotChannelOwner() {
        AtomicReference<List<Metrics>> reference = new AtomicReference<>();
        UnloadCounter counter = new UnloadCounter();
        LoadManagerContext context = setupContext();
        context.brokerConfiguration().setLoadBalancerEnabled(false);
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        UnloadManager unloadManager = mock(UnloadManager.class);
        PulsarService pulsar = mock(PulsarService.class);
        UnloadScheduler scheduler = new UnloadScheduler(pulsar, loadManagerExecutor, unloadManager, context,
                channel, unloadStrategy, counter, reference);
        doReturn(CompletableFuture.completedFuture(false)).when(channel).isChannelOwnerAsync();

        scheduler.execute();

        verify(context.brokerRegistry(), times(0)).getAvailableBrokersAsync();
    }

    public LoadManagerContext getContext(){
        var ctx = mock(LoadManagerContext.class);
        var registry = mock(BrokerRegistry.class);
        var conf = new ServiceConfiguration();
        doReturn(conf).when(ctx).brokerConfiguration();
        doReturn(registry).when(ctx).brokerRegistry();
        return ctx;
    }
}
