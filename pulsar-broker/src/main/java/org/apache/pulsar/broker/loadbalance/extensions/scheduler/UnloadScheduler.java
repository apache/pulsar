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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Reflections;

@Slf4j
public class UnloadScheduler implements LoadManagerScheduler {

    private final NamespaceUnloadStrategy namespaceUnloadStrategy;

    private final ScheduledExecutorService loadManagerExecutor;

    private final LoadManagerContext context;

    private final ServiceUnitStateChannel channel;

    private final ServiceConfiguration conf;

    private volatile ScheduledFuture<?> task;

    private final Map<String, Long> recentlyUnloadedBundles;

    private final Map<String, Long> recentlyUnloadedBrokers;

    private volatile CompletableFuture<Void> currentRunningFuture = null;

    public UnloadScheduler(ScheduledExecutorService loadManagerExecutor,
                           LoadManagerContext context,
                           ServiceUnitStateChannel channel) {
        this(loadManagerExecutor, context, channel, createNamespaceUnloadStrategy(context.brokerConfiguration()));
    }

    @VisibleForTesting
    protected UnloadScheduler(ScheduledExecutorService loadManagerExecutor,
                              LoadManagerContext context,
                              ServiceUnitStateChannel channel,
                              NamespaceUnloadStrategy strategy) {
        this.namespaceUnloadStrategy = strategy;
        this.recentlyUnloadedBundles = new HashMap<>();
        this.recentlyUnloadedBrokers = new HashMap<>();
        this.loadManagerExecutor = loadManagerExecutor;
        this.context = context;
        this.conf = context.brokerConfiguration();
        this.channel = channel;
    }

    @Override
    public synchronized void execute() {
        boolean debugMode = conf.isLoadBalancerDebugModeEnabled() || log.isDebugEnabled();
        if (debugMode) {
            log.info("Load balancer enabled: {}, Shedding enabled: {}.",
                    conf.isLoadBalancerEnabled(), conf.isLoadBalancerSheddingEnabled());
        }
        if (!isLoadBalancerSheddingEnabled()) {
            if (debugMode) {
                log.info("The load balancer or load balancer shedding already disabled. Skipping.");
            }
            return;
        }
        if (currentRunningFuture != null && !currentRunningFuture.isDone()) {
            if (debugMode) {
                log.info("Auto namespace unload is running. Skipping.");
            }
            return;
        }
        // Remove bundles who have been unloaded for longer than the grace period from the recently unloaded map.
        final long timeout = System.currentTimeMillis()
                - TimeUnit.MINUTES.toMillis(conf.getLoadBalancerSheddingGracePeriodMinutes());
        recentlyUnloadedBundles.keySet().removeIf(e -> recentlyUnloadedBundles.get(e) < timeout);

        this.currentRunningFuture = channel.isChannelOwnerAsync().thenCompose(isChannelOwner -> {
            if (!isChannelOwner) {
                if (debugMode) {
                    log.info("Current broker is not channel owner. Skipping.");
                }
                return CompletableFuture.completedFuture(null);
            }
            return context.brokerRegistry().getAvailableBrokersAsync().thenCompose(availableBrokers -> {
                if (debugMode) {
                   log.info("Available brokers: {}", availableBrokers);
                }
                if (availableBrokers.size() <= 1) {
                    log.info("Only 1 broker available: no load shedding will be performed. Skipping.");
                    return CompletableFuture.completedFuture(null);
                }
                final UnloadDecision unloadDecision = namespaceUnloadStrategy
                        .findBundlesForUnloading(context, recentlyUnloadedBundles, recentlyUnloadedBrokers);
                if (debugMode) {
                    log.info("[{}] Unload decision result: {}",
                            namespaceUnloadStrategy.getClass().getSimpleName(), unloadDecision.toString());
                }
                if (unloadDecision.getUnloads().isEmpty()) {
                    if (debugMode) {
                        log.info("[{}] Unload decision unloads is empty. Skipping.",
                                namespaceUnloadStrategy.getClass().getSimpleName());
                    }
                    return CompletableFuture.completedFuture(null);
                }
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                unloadDecision.getUnloads().forEach((broker, unload) -> {
                    log.info("[{}] Unloading bundle: {}", namespaceUnloadStrategy.getClass().getSimpleName(), unload);
                    futures.add(channel.publishUnloadEventAsync(unload).thenAccept(__ -> {
                        recentlyUnloadedBundles.put(unload.serviceUnit(), System.currentTimeMillis());
                        recentlyUnloadedBrokers.put(unload.sourceBroker(), System.currentTimeMillis());
                    }));
                });
                return FutureUtil.waitForAll(futures).exceptionally(ex -> {
                    log.error("[{}] Namespace unload has exception.",
                            namespaceUnloadStrategy.getClass().getSimpleName(), ex);
                    return null;
                });
            });
        });
    }

    @Override
    public void start() {
        long loadSheddingInterval = TimeUnit.MINUTES
                .toMillis(conf.getLoadBalancerSheddingIntervalMinutes());
        this.task = loadManagerExecutor.scheduleAtFixedRate(
                this::execute, loadSheddingInterval, loadSheddingInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (this.task != null) {
            this.task.cancel(false);
        }
        this.recentlyUnloadedBundles.clear();
        this.recentlyUnloadedBrokers.clear();
    }

    private static NamespaceUnloadStrategy createNamespaceUnloadStrategy(ServiceConfiguration conf) {
        try {
            return Reflections.createInstance(conf.getLoadBalancerLoadSheddingStrategy(), NamespaceUnloadStrategy.class,
                    Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            log.error("Error when trying to create namespace unload strategy: {}",
                    conf.getLoadBalancerLoadPlacementStrategy(), e);
        }
        log.error("create namespace unload strategy failed. using TransferShedder instead.");
        return new TransferShedder();
    }

    private boolean isLoadBalancerSheddingEnabled() {
        return conf.isLoadBalancerEnabled() && conf.isLoadBalancerSheddingEnabled();
    }
}
