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

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MinMaxPriorityQueue;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load shedding strategy that unloads bundles from the highest loaded brokers.
 * This strategy is only configurable in the broker load balancer extenstions introduced by
 * PIP-192[https://github.com/apache/pulsar/issues/16691].
 *
 * This load shedding strategy has the following goals:
 * 1. Distribute bundle load across brokers in order to make the standard deviation of the avg resource usage,
 * std(exponential-moving-avg(max(cpu, memory, network, throughput)) for each broker) below the target,
 * configurable by loadBalancerBrokerLoadTargetStd.
 * 2. Use the transfer protocol to transfer bundle load from the highest loaded to the lowest loaded brokers,
 * if configured by loadBalancerTransferEnabled=true.
 * 3. Avoid repeated bundle unloading by recomputing historical broker resource usage after unloading and also
 * skipping the bundles that are recently unloaded.
 * 4. Prioritize unloading bundles to underloaded brokers when their message throughput is zero(new brokers).
 * 5. Do not use outdated broker load data (configurable by loadBalancerBrokerLoadDataTTLInSeconds).
 * 6. Give enough time for each broker to recompute its load after unloading
 * (configurable by loadBalanceUnloadDelayInSeconds)
 * 7. Do not transfer bundles with namespace isolation policies or anti-affinity group policies.
 * 8. Limit the max number of brokers to transfer bundle load for each cycle,
 * (loadBalancerMaxNumberOfBrokerTransfersPerCycle).
 * 9. Print more logs with a debug option(loadBalancerDebugModeEnabled=true).
 */
public class TransferShedder implements NamespaceUnloadStrategy {
    private static final Logger log = LoggerFactory.getLogger(TransferShedder.class);
    private static final double KB = 1024;
    private final LoadStats stats = new LoadStats();
    private final PulsarService pulsar;
    private final SimpleResourceAllocationPolicies allocationPolicies;

    private final UnloadDecision decision = new UnloadDecision();

    @VisibleForTesting
    public TransferShedder(){
        this.pulsar = null;
        this.allocationPolicies = null;
    }

    public TransferShedder(PulsarService pulsar){
        this.pulsar = pulsar;
        this.allocationPolicies = new SimpleResourceAllocationPolicies(pulsar);
    }


    @Getter
    @Accessors(fluent = true)
    static class LoadStats {
        private double sum;
        private double sqSum;
        private int totalBrokers;
        private double avg;
        private double std;
        private MinMaxPriorityQueue<String> minBrokers;
        private MinMaxPriorityQueue<String> maxBrokers;
        private LoadDataStore<BrokerLoadData> loadDataStore;

        LoadStats() {
            this.minBrokers = MinMaxPriorityQueue.orderedBy((a, b) -> Double.compare(
                    loadDataStore.get((String) b).get().getWeightedMaxEMA(),
                    loadDataStore.get((String) a).get().getWeightedMaxEMA())).create();
            this.maxBrokers = MinMaxPriorityQueue.orderedBy((a, b) -> Double.compare(
                    loadDataStore.get((String) a).get().getWeightedMaxEMA(),
                    loadDataStore.get((String) b).get().getWeightedMaxEMA())).create();
        }

        private void update(double sum, double sqSum, int totalBrokers) {
            this.sum = sum;
            this.sqSum = sqSum;
            this.totalBrokers = totalBrokers;

            if (totalBrokers == 0) {
                this.avg = 0;
                this.std = 0;
                minBrokers.clear();
                maxBrokers.clear();
            } else {
                this.avg = sum / totalBrokers;
                this.std = Math.sqrt(sqSum / totalBrokers - avg * avg);
            }
        }

        void offload(double max, double min, double offload) {
            sqSum -= max * max + min * min;
            double maxd = Math.max(0, max - offload);
            double mind = min + offload;
            sqSum += maxd * maxd + mind * mind;
            std = Math.sqrt(sqSum / totalBrokers - avg * avg);
        }

        void clear(){
            sum = 0.0;
            sqSum = 0.0;
            totalBrokers = 0;
            avg = 0.0;
            std = 0.0;
            minBrokers.clear();
            maxBrokers.clear();
        }

        Optional<UnloadDecision.Reason> update(final LoadDataStore<BrokerLoadData> loadStore,
                                               Map<String, Long> recentlyUnloadedBrokers,
                                               final ServiceConfiguration conf) {


            UnloadDecision.Reason decisionReason = null;
            double sum = 0.0;
            double sqSum = 0.0;
            int totalBrokers = 0;
            int maxTransfers = conf.getLoadBalancerMaxNumberOfBrokerTransfersPerCycle();
            long now = System.currentTimeMillis();
            for (Map.Entry<String, BrokerLoadData> entry : loadStore.entrySet()) {
                BrokerLoadData localBrokerData = entry.getValue();
                String broker = entry.getKey();

                // We don't want to use the outdated load data.
                if (now - localBrokerData.getUpdatedAt()
                        > conf.getLoadBalancerBrokerLoadDataTTLInSeconds() * 1000) {
                    log.warn(
                            "Ignoring broker:{} load update because the load data timestamp:{} is too old.",
                            broker, localBrokerData.getUpdatedAt());
                    decisionReason = OutDatedData;
                    continue;
                }

                // Also, we should give enough time for each broker to recompute its load after transfers.
                if (recentlyUnloadedBrokers.containsKey(broker)) {
                    if (localBrokerData.getUpdatedAt() - recentlyUnloadedBrokers.get(broker)
                            < conf.getLoadBalanceUnloadDelayInSeconds() * 1000) {
                        log.warn(
                                "Broker:{} load data timestamp:{} is too early since "
                                        + "the last transfer timestamp:{}. Stop unloading.",
                                broker, localBrokerData.getUpdatedAt(), recentlyUnloadedBrokers.get(broker));
                        update(0.0, 0.0, 0);
                        return Optional.of(CoolDown);
                    } else {
                        recentlyUnloadedBrokers.remove(broker);
                    }
                }

                double load = localBrokerData.getWeightedMaxEMA();

                minBrokers.offer(broker);
                if (minBrokers.size() > maxTransfers) {
                    minBrokers.poll();
                }
                maxBrokers.offer(broker);
                if (maxBrokers.size() > maxTransfers) {
                    maxBrokers.poll();
                }
                sum += load;
                sqSum += load * load;
                totalBrokers++;
            }


            if (totalBrokers == 0) {
                if (decisionReason == null) {
                    decisionReason = NoBrokers;
                }
                update(0.0, 0.0, 0);
                return Optional.of(decisionReason);
            }

            update(sum, sqSum, totalBrokers);
            return Optional.empty();
        }

        boolean hasTransferableBrokers() {
            return !(maxBrokers.isEmpty() || minBrokers.isEmpty()
                    || maxBrokers.peekLast().equals(minBrokers().peekLast()));
        }

        void setLoadDataStore(LoadDataStore<BrokerLoadData> loadDataStore) {
            this.loadDataStore = loadDataStore;
        }

        @Override
        public String toString() {
            return String.format(
                    "sum:%.2f, sqSum:%.2f, avg:%.2f, std:%.2f, totalBrokers:%d, "
                            + "minBrokers:%s, maxBrokers:%s",
                    sum, sqSum, avg, std, totalBrokers, minBrokers, maxBrokers);
        }
    }


    @Override
    public UnloadDecision findBundlesForUnloading(LoadManagerContext context,
                                                  Map<String, Long> recentlyUnloadedBundles,
                                                  Map<String, Long> recentlyUnloadedBrokers) {
        final var conf = context.brokerConfiguration();
        decision.clear();
        stats.clear();
        var selectedBundlesCache = decision.getUnloads();

        try {
            final var loadStore = context.brokerLoadDataStore();
            stats.setLoadDataStore(loadStore);
            boolean debugMode = conf.isLoadBalancerDebugModeEnabled() || log.isDebugEnabled();

            var skipReason = stats.update(context.brokerLoadDataStore(), recentlyUnloadedBrokers, conf);
            if (!skipReason.isEmpty()) {
                decision.skip(skipReason.get());
                log.warn("Failed to update load stat. Reason:{}. Stop unloading.", decision.getReason());
                return decision;
            }
            decision.setLoadAvg(stats.avg);
            decision.setLoadStd(stats.std);

            if (debugMode) {
                log.info("brokers' load stats:{}", stats);
            }

            // success metrics
            int numOfOverloadedBrokers = 0;
            int numOfUnderloadedBrokers = 0;

            // skip metrics
            int numOfBrokersWithEmptyLoadData = 0;
            int numOfBrokersWithFewBundles = 0;

            final double targetStd = conf.getLoadBalancerBrokerLoadTargetStd();
            boolean transfer = conf.isLoadBalancerTransferEnabled();
            while (true) {
                if (!stats.hasTransferableBrokers()) {
                    if (debugMode) {
                        log.info("Exhausted target transfer brokers. Stop unloading");
                    }
                    break;
                }
                if (stats.std() <= targetStd) {
                    if (hasMsgThroughput(context, stats.minBrokers.peekLast())) {
                        if (debugMode) {
                            log.info("std:{} <= targetStd:{} and minBroker:{} has msg throughput. Stop unloading.",
                                    stats.std, targetStd, stats.minBrokers.peekLast());
                        }
                        break;
                    } else {
                        numOfUnderloadedBrokers++;
                    }
                } else {
                    numOfOverloadedBrokers++;
                }

                String maxBroker = stats.maxBrokers().pollLast();
                String minBroker = stats.minBrokers().pollLast();
                Optional<BrokerLoadData> maxBrokerLoadData = context.brokerLoadDataStore().get(maxBroker);
                Optional<BrokerLoadData> minBrokerLoadData = context.brokerLoadDataStore().get(minBroker);
                if (maxBrokerLoadData.isEmpty()) {
                    log.error("maxBroker:{} maxBrokerLoadData is empty. Skip unloading from this max broker.",
                            maxBroker);
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }
                if (minBrokerLoadData.isEmpty()) {
                    log.error("minBroker:{} minBrokerLoadData is empty. Skip unloading to this min broker.", minBroker);
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }

                double max = maxBrokerLoadData.get().getWeightedMaxEMA();
                double min = minBrokerLoadData.get().getWeightedMaxEMA();
                double offload = (max - min) / 2;
                BrokerLoadData brokerLoadData = maxBrokerLoadData.get();
                double brokerThroughput = brokerLoadData.getMsgThroughputIn() + brokerLoadData.getMsgThroughputOut();
                double offloadThroughput = brokerThroughput * offload;

                if (debugMode) {
                    log.info(
                            "Attempting to shed load from broker:{}{}, which has the max resource "
                                    + "usage {}%, targetStd:{},"
                                    + " -- Offloading {}%, at least {} KByte/s of traffic, left throughput {} KByte/s",
                            maxBroker, transfer ? " to broker:" + minBroker : "",
                            100 * max, targetStd,
                            offload * 100, offloadThroughput / KB, (brokerThroughput - offloadThroughput) / KB);
                }

                double trafficMarkedToOffload = 0;
                boolean atLeastOneBundleSelected = false;

                Optional<TopBundlesLoadData> bundlesLoadData = context.topBundleLoadDataStore().get(maxBroker);
                if (bundlesLoadData.isEmpty() || bundlesLoadData.get().getTopBundlesLoadData().isEmpty()) {
                    log.error("maxBroker:{} topBundlesLoadData is empty. Skip unloading from this broker.", maxBroker);
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }

                var topBundlesLoadData = bundlesLoadData.get().getTopBundlesLoadData();
                if (topBundlesLoadData.size() > 1) {
                    int remainingTopBundles = topBundlesLoadData.size();
                    for (var e : topBundlesLoadData) {
                        String bundle = e.bundleName();
                        if (!recentlyUnloadedBundles.containsKey(bundle) && isTransferable(bundle)) {
                            var bundleData = e.stats();
                            double throughput = bundleData.msgThroughputIn + bundleData.msgThroughputOut;
                            if (remainingTopBundles > 1
                                    && (trafficMarkedToOffload < offloadThroughput
                                    || !atLeastOneBundleSelected)) {
                                if (transfer) {
                                    selectedBundlesCache.put(maxBroker,
                                            new Unload(maxBroker, bundle,
                                                    Optional.of(minBroker)));
                                } else {
                                    selectedBundlesCache.put(maxBroker,
                                            new Unload(maxBroker, bundle));
                                }
                                trafficMarkedToOffload += throughput;
                                atLeastOneBundleSelected = true;
                                remainingTopBundles--;
                            }
                        }
                    }
                    if (!atLeastOneBundleSelected) {
                        numOfBrokersWithFewBundles++;
                    }
                } else if (topBundlesLoadData.size() == 1) {
                    numOfBrokersWithFewBundles++;
                    log.warn(
                            "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                    + "No Load Shedding will be done on this broker",
                            topBundlesLoadData.iterator().next(), maxBroker);
                } else {
                    numOfBrokersWithFewBundles++;
                    log.warn("Broker {} is overloaded despite having no bundles", maxBroker);
                }

                if (trafficMarkedToOffload > 0) {
                    stats.offload(max, min, offload);
                    if (debugMode) {
                        log.info(
                                String.format("brokers' load stats:%s, after offload{max:%.2f, min:%.2f, offload:%.2f}",
                                        stats, max, min, offload));
                    }
                }
            }

            if (debugMode) {
                log.info("selectedBundlesCache:{}", selectedBundlesCache);
            }

            if (decision.getUnloads().isEmpty()) {
                decision.skip(
                        numOfOverloadedBrokers,
                        numOfUnderloadedBrokers,
                        numOfBrokersWithEmptyLoadData,
                        numOfBrokersWithFewBundles);
            } else {
                decision.succeed(
                        numOfOverloadedBrokers,
                        numOfUnderloadedBrokers);
            }
        } catch (Throwable e) {
            log.error("Failed to process unloading. ", e);
            decision.fail();
        }

        return decision;
    }


    private boolean hasMsgThroughput(LoadManagerContext context, String broker) {
        var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
        if (brokerLoadDataOptional.isEmpty()) {
            return false;
        }
        var brokerLoadData = brokerLoadDataOptional.get();
        return brokerLoadData.getMsgThroughputIn() + brokerLoadData.getMsgThroughputOut() > 0.0;
    }


    private boolean isTransferable(String bundle) {
        if (pulsar == null || allocationPolicies == null) {
            return true;
        }
        NamespaceName namespace = NamespaceName.get(LoadManagerShared.getNamespaceNameFromBundleName(bundle));
        if (allocationPolicies.areIsolationPoliciesPresent(namespace)) {
            return false;
        }

        try {
            var localPoliciesOptional = pulsar
                    .getPulsarResources().getLocalPolicies().getLocalPolicies(namespace);
            if (localPoliciesOptional.isPresent() && StringUtils.isNotBlank(
                    localPoliciesOptional.get().namespaceAntiAffinityGroup)) {
                return false;
            }
        } catch (MetadataStoreException e) {
            log.error("Failed to get localPolicies. Assumes that bundle:{} is not transferable.", bundle, e);
            return false;
        }
        return true;
    }
}
