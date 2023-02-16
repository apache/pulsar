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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker")
public class UnloadSchedulerTest {

   private ScheduledExecutorService loadManagerExecutor;

    public LoadManagerContext setupContext(){
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);
        return ctx;
    }

    @BeforeMethod
    public void setUp() {
        this.loadManagerExecutor = Executors
                .newSingleThreadScheduledExecutor(new ExecutorProvider.ExtendedThreadFactory("pulsar-load-manager"));
    }

    @AfterMethod
    public void tearDown() {
        this.loadManagerExecutor.shutdown();
    }

    @Test(timeOut = 30 * 1000)
    public void testExecuteSuccess() {
        LoadManagerContext context = setupContext();
        BrokerRegistry registry = context.brokerRegistry();
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        doReturn(CompletableFuture.completedFuture(true)).when(channel).isChannelOwnerAsync();
        doReturn(CompletableFuture.completedFuture(Lists.newArrayList("broker-1", "broker-2")))
                .when(registry).getAvailableBrokersAsync();
        doReturn(CompletableFuture.completedFuture(null)).when(channel).publishUnloadEventAsync(any());
        UnloadDecision decision = new UnloadDecision();
        Unload unload = new Unload("broker-1", "bundle-1");
        decision.getUnloads().put("broker-1", unload);
        doReturn(decision).when(unloadStrategy).findBundlesForUnloading(any(), any(), any());

        UnloadScheduler scheduler = new UnloadScheduler(loadManagerExecutor, context, channel, unloadStrategy);

        scheduler.execute();

        verify(channel, times(1)).publishUnloadEventAsync(eq(unload));

        // Test empty unload.
        UnloadDecision emptyUnload = new UnloadDecision();
        doReturn(emptyUnload).when(unloadStrategy).findBundlesForUnloading(any(), any(), any());

        scheduler.execute();

        verify(channel, times(1)).publishUnloadEventAsync(eq(unload));
    }

    @Test(timeOut = 30 * 1000)
    public void testExecuteMoreThenOnceWhenFirstNotDone() throws InterruptedException {
        LoadManagerContext context = setupContext();
        BrokerRegistry registry = context.brokerRegistry();
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        doReturn(CompletableFuture.completedFuture(true)).when(channel).isChannelOwnerAsync();
        doAnswer(__ -> CompletableFuture.supplyAsync(() -> {
                try {
                    // Delay 5 seconds to finish.
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return Lists.newArrayList("broker-1", "broker-2");
            }, Executors.newFixedThreadPool(1))).when(registry).getAvailableBrokersAsync();
        UnloadScheduler scheduler = new UnloadScheduler(loadManagerExecutor, context, channel, unloadStrategy);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            executorService.execute(() -> {
                scheduler.execute();
                latch.countDown();
            });
        }
        latch.await();

        verify(registry, times(1)).getAvailableBrokersAsync();
    }

    @Test(timeOut = 30 * 1000)
    public void testDisableLoadBalancer() {
        LoadManagerContext context = setupContext();
        context.brokerConfiguration().setLoadBalancerEnabled(false);
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        UnloadScheduler scheduler = new UnloadScheduler(loadManagerExecutor, context, channel, unloadStrategy);

        scheduler.execute();

        verify(channel, times(0)).isChannelOwnerAsync();

        context.brokerConfiguration().setLoadBalancerEnabled(true);
        context.brokerConfiguration().setLoadBalancerSheddingEnabled(false);
        scheduler.execute();

        verify(channel, times(0)).isChannelOwnerAsync();
    }

    @Test(timeOut = 30 * 1000)
    public void testNotChannelOwner() {
        LoadManagerContext context = setupContext();
        context.brokerConfiguration().setLoadBalancerEnabled(false);
        ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
        NamespaceUnloadStrategy unloadStrategy = mock(NamespaceUnloadStrategy.class);
        UnloadScheduler scheduler = new UnloadScheduler(loadManagerExecutor, context, channel, unloadStrategy);
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
