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

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.manager.UnloadManager;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Reflections;

@CustomLog
public class UnloadScheduler implements LoadManagerScheduler {

    private final NamespaceUnloadStrategy namespaceUnloadStrategy;

    private final ScheduledExecutorService loadManagerExecutor;

    private final PulsarService pulsar;

    private final UnloadManager unloadManager;

    private final LoadManagerContext context;

    private final ServiceUnitStateChannel channel;

    private final ServiceConfiguration conf;

    private final UnloadCounter counter;

    private final AtomicReference<List<Metrics>> unloadMetrics;

    private long counterLastUpdatedAt = 0;

    private volatile ScheduledFuture<?> task;

    private final Set<String> unloadBrokers;

    private final Map<String, Long> recentlyUnloadedBundles;

    private final Map<String, Long> recentlyUnloadedBrokers;

    public UnloadScheduler(PulsarService pulsar,
                           ScheduledExecutorService loadManagerExecutor,
                           UnloadManager unloadManager,
                           LoadManagerContext context,
                           ServiceUnitStateChannel channel,
                           UnloadCounter counter,
                           AtomicReference<List<Metrics>> unloadMetrics) {
        this(pulsar, loadManagerExecutor, unloadManager, context, channel,
                createNamespaceUnloadStrategy(pulsar), counter, unloadMetrics);
    }

    @VisibleForTesting
    protected UnloadScheduler(PulsarService pulsar,
                              ScheduledExecutorService loadManagerExecutor,
                              UnloadManager unloadManager,
                              LoadManagerContext context,
                              ServiceUnitStateChannel channel,
                              NamespaceUnloadStrategy strategy,
                              UnloadCounter counter,
                              AtomicReference<List<Metrics>> unloadMetrics) {
        this.pulsar = pulsar;
        this.namespaceUnloadStrategy = strategy;
        this.recentlyUnloadedBundles = new HashMap<>();
        this.recentlyUnloadedBrokers = new HashMap<>();
        this.unloadBrokers = new HashSet<>();
        this.loadManagerExecutor = loadManagerExecutor;
        this.counter = counter;
        this.unloadMetrics = unloadMetrics;
        this.unloadManager = unloadManager;
        this.context = context;
        this.conf = context.brokerConfiguration();
        this.channel = channel;
    }

    @Override
    public synchronized void execute() {
        boolean debugMode = conf.isLoadBalancerDebugModeEnabled();
        if (debugMode) {
            log.info().attr("loadBalancerEnabled", conf.isLoadBalancerEnabled())
                    .attr("sheddingEnabled", conf.isLoadBalancerSheddingEnabled())
                    .log("Load balancer status");
        }
        if (!isLoadBalancerSheddingEnabled()) {
            if (debugMode) {
                log.info("The load balancer or load balancer shedding already disabled. Skipping.");
            }
            return;
        }
        // Remove bundles who have been unloaded for longer than the grace period from the recently unloaded map.
        final long timeout = System.currentTimeMillis()
                - TimeUnit.MINUTES.toMillis(conf.getLoadBalancerSheddingGracePeriodMinutes());
        recentlyUnloadedBundles.keySet().removeIf(e -> recentlyUnloadedBundles.get(e) < timeout);

        long asyncOpTimeoutMs = conf.getNamespaceBundleUnloadingTimeoutMs();
        synchronized (namespaceUnloadStrategy) {
            try {
                Boolean isChannelOwner = channel.isChannelOwnerAsync().get(asyncOpTimeoutMs, TimeUnit.MILLISECONDS);
                if (!isChannelOwner) {
                    if (debugMode) {
                        log.info("Current broker is not channel owner. Skipping.");
                    }
                    return;
                }
                List<String> availableBrokers = context.brokerRegistry().getAvailableBrokersAsync()
                        .get(asyncOpTimeoutMs, TimeUnit.MILLISECONDS);
                if (debugMode) {
                    log.info().attr("broker", availableBrokers).log("Available brokers");
                }
                if (availableBrokers.size() <= 1) {
                    log.info("Only 1 broker available: no load shedding will be performed. Skipping.");
                    return;
                }
                final Set<UnloadDecision> decisions = namespaceUnloadStrategy
                        .findBundlesForUnloading(context, recentlyUnloadedBundles, recentlyUnloadedBrokers);
                if (debugMode) {
                    log.info().attr("strategy", namespaceUnloadStrategy.getClass().getSimpleName())
                            .attr("result", decisions).log("Unload decision result");
                }
                if (decisions.isEmpty()) {
                    if (debugMode) {
                        log.info().attr("strategy", namespaceUnloadStrategy.getClass().getSimpleName())
                                .log("Unload decision unloads is empty. Skipping");
                    }
                    return;
                }
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                unloadBrokers.clear();
                decisions.forEach(decision -> {
                    if (decision.getLabel() == Success) {
                        Unload unload = decision.getUnload();
                        log.info().attr("strategy", namespaceUnloadStrategy.getClass().getSimpleName())
                                .attr("bundle", unload).log("Unloading bundle");
                        futures.add(unloadManager.waitAsync(channel.publishUnloadEventAsync(unload),
                                        unload.serviceUnit(), decision, asyncOpTimeoutMs, TimeUnit.MILLISECONDS)
                                .thenAccept(__ -> {
                                    unloadBrokers.add(unload.sourceBroker());
                                    recentlyUnloadedBundles.put(unload.serviceUnit(), System.currentTimeMillis());
                                    recentlyUnloadedBrokers.put(unload.sourceBroker(), System.currentTimeMillis());
                                }));
                    }
                });
                FutureUtil.waitForAll(futures)
                        .whenComplete((__, ex) -> counter.updateUnloadBrokerCount(unloadBrokers.size()))
                        .get(asyncOpTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                log.error().attr("strategy", namespaceUnloadStrategy.getClass().getSimpleName())
                        .exception(ex).log("Namespace unload has exception");
            } finally {
                if (counter.updatedAt() > counterLastUpdatedAt) {
                    unloadMetrics.set(counter.toMetrics(pulsar.getAdvertisedAddress()));
                    counterLastUpdatedAt = counter.updatedAt();
                }
            }
        }
    }

    @Override
    public void start() {
        if (this.task == null) {
            long loadSheddingInterval = TimeUnit.MINUTES
                    .toMillis(conf.getLoadBalancerSheddingIntervalMinutes());
            this.task = loadManagerExecutor.scheduleAtFixedRate(
                    this::execute, loadSheddingInterval, loadSheddingInterval, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        if (this.task != null) {
            this.task.cancel(false);
            this.task = null;
        }
        this.recentlyUnloadedBundles.clear();
        this.recentlyUnloadedBrokers.clear();
    }

    private static NamespaceUnloadStrategy createNamespaceUnloadStrategy(PulsarService pulsar) {
        ServiceConfiguration conf = pulsar.getConfiguration();
        NamespaceUnloadStrategy unloadStrategy;
        try {
            unloadStrategy = Reflections.createInstance(conf.getLoadBalancerLoadSheddingStrategy(),
                    NamespaceUnloadStrategy.class,
                    Thread.currentThread().getContextClassLoader());
            log.info().attr("strategy", unloadStrategy.getClass().getCanonicalName())
                    .log("Created namespace unload strategy");
        } catch (Exception e) {
            log.error().attr("strategy", conf.getLoadBalancerLoadSheddingStrategy())
                    .attr("fallback", TransferShedder.class.getCanonicalName()).exception(e)
                    .log("Error when trying to create namespace unload strategy");
            unloadStrategy = new TransferShedder();
        }
        unloadStrategy.initialize(pulsar);
        return unloadStrategy;
    }

    private boolean isLoadBalancerSheddingEnabled() {
        return conf.isLoadBalancerEnabled() && conf.isLoadBalancerSheddingEnabled();
    }
}
