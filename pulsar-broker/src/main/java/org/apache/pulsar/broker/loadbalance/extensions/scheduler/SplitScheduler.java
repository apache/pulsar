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

import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Success;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.manager.SplitManager;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.extensions.strategy.DefaultNamespaceBundleSplitStrategyImpl;
import org.apache.pulsar.broker.loadbalance.extensions.strategy.NamespaceBundleSplitStrategy;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Service Unit(e.g. bundles) Split scheduler.
 */
@Slf4j
public class SplitScheduler implements LoadManagerScheduler {

    private final PulsarService pulsar;

    private final ScheduledExecutorService loadManagerExecutor;

    private final LoadManagerContext context;

    private final ServiceConfiguration conf;

    private final ServiceUnitStateChannel serviceUnitStateChannel;

    private final NamespaceBundleSplitStrategy bundleSplitStrategy;

    private final SplitCounter counter;

    private final SplitManager splitManager;

    private final AtomicReference<List<Metrics>> splitMetrics;

    private volatile ScheduledFuture<?> task;

    private long counterLastUpdatedAt = 0;

    public SplitScheduler(PulsarService pulsar,
                          ServiceUnitStateChannel serviceUnitStateChannel,
                          SplitManager splitManager,
                          SplitCounter counter,
                          AtomicReference<List<Metrics>> splitMetrics,
                          LoadManagerContext context,
                          NamespaceBundleSplitStrategy bundleSplitStrategy) {
        this.pulsar = pulsar;
        this.loadManagerExecutor = pulsar.getLoadManagerExecutor();
        this.splitManager = splitManager;
        this.counter = counter;
        this.splitMetrics = splitMetrics;
        this.context = context;
        this.conf = pulsar.getConfiguration();
        this.bundleSplitStrategy = bundleSplitStrategy;
        this.serviceUnitStateChannel = serviceUnitStateChannel;
    }

    public SplitScheduler(PulsarService pulsar,
                          ServiceUnitStateChannel serviceUnitStateChannel,
                          SplitManager splitManager,
                          SplitCounter counter,
                          AtomicReference<List<Metrics>> splitMetrics,
                          LoadManagerContext context) {
        this(pulsar, serviceUnitStateChannel, splitManager, counter, splitMetrics, context,
                new DefaultNamespaceBundleSplitStrategyImpl(counter));
    }

    @Override
    public void execute() {
        boolean debugMode = ExtensibleLoadManagerImpl.debug(conf, log);
        if (debugMode) {
            log.info("Load balancer enabled: {}, Split enabled: {}.",
                    conf.isLoadBalancerEnabled(), conf.isLoadBalancerAutoBundleSplitEnabled());
        }

        if (!isLoadBalancerAutoBundleSplitEnabled()) {
            if (debugMode) {
                log.info("The load balancer or load balancer split already disabled. Skipping.");
            }
            return;
        }

        synchronized (bundleSplitStrategy) {
            final Set<SplitDecision> decisions = bundleSplitStrategy.findBundlesToSplit(context, pulsar);
            if (debugMode) {
                log.info("Split Decisions: {}", decisions);
            }
            if (!decisions.isEmpty()) {
                // currently following the unloading timeout
                var asyncOpTimeoutMs = conf.getNamespaceBundleUnloadingTimeoutMs();
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (SplitDecision decision : decisions) {
                    if (decision.getLabel() == Success) {
                        var split = decision.getSplit();
                        futures.add(
                                splitManager.waitAsync(
                                        serviceUnitStateChannel.publishSplitEventAsync(split),
                                        split.serviceUnit(),
                                        decision,
                                        asyncOpTimeoutMs, TimeUnit.MILLISECONDS)
                        );
                    }
                }
                try {
                    FutureUtil.waitForAll(futures)
                            .get(asyncOpTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (Throwable e) {
                    log.error("Failed to wait for split events to persist.", e);
                }
            } else {
                if (debugMode) {
                    log.info("BundleSplitStrategy returned no bundles to split.");
                }
            }
        }

        if (counter.updatedAt() > counterLastUpdatedAt) {
            splitMetrics.set(counter.toMetrics(pulsar.getAdvertisedAddress()));
            counterLastUpdatedAt = counter.updatedAt();
        }
    }

    @Override
    public void start() {
        long interval = TimeUnit.MINUTES
                .toMillis(conf.getLoadBalancerSplitIntervalMinutes());
        task = loadManagerExecutor.scheduleAtFixedRate(() -> {
            try {
                execute();
                var debugMode = ExtensibleLoadManagerImpl.debug(conf, log);
                if (debugMode) {
                    StringJoiner joiner = new StringJoiner("\n");
                    joiner.add("### OwnershipEntrySet start ###");
                    serviceUnitStateChannel.getOwnershipEntrySet().forEach(e -> joiner.add(e.toString()));
                    joiner.add("### OwnershipEntrySet end ###");
                    log.info(joiner.toString());
                }
            } catch (Throwable e) {
                log.error("Failed to run the split job.", e);
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
    }

    private boolean isLoadBalancerAutoBundleSplitEnabled() {
        return conf.isLoadBalancerEnabled() && conf.isLoadBalancerAutoBundleSplitEnabled();
    }

}
