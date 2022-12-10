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
import com.google.common.collect.MinMaxPriorityQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
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
 * configurable by loadBalancerExtentionsTransferShedderTargetLoadStd.
 * 2. Use the transfer protocol to transfer bundle load from the highest loaded to the lowest loaded brokers,
 * if configured by loadBalancerExtentionsTransferShedderTransferEnabled=true.
 * 3. Avoid repeated bundle unloading by recomputing historical broker resource usage after unloading and also
 * skipping the bundles that are recently unloaded.
 * 4. Prioritize unloading bundles to underloaded brokers when their message throughput is zero(new brokers).
 * 5. Do not use outdated broker load data
 * (configurable by loadBalancerExtentionsTransferShedderLoadUpdateMaxWaitingTimeInSeconds).
 * 6. Give enough time for each broker to recompute its load after unloading
 * (configurable by loadBalancerExtentionsTransferShedderBrokerLoadDataUpdateMinWaitingTimeAfterUnloadingInSeconds)
 * 7. Do not transfer bundles with namespace isolation policies or anti-affinity group policies.
 * 8. Limit the max number of brokers to transfer bundle load for each cycle,
 * (loadBalancerExtentionsTransferShedderMaxNumberOfBrokerTransfersPerCycle).
 * 9. Print more logs with a debug option(loadBalancerExtentionsTransferShedderDebugModeEnabled=true).
 */
public class TransferShedder implements NamespaceUnloadStrategy {
    private static final Logger log = LoggerFactory.getLogger(TransferShedder.class);
    private static final double KB = 1024;
    private final List<Unload> selectedBundlesCache = new ArrayList<>();
    private final Map<String, Double> brokerAvgResourceUsage = new HashMap<>();
    private final LoadStats stats = new LoadStats(brokerAvgResourceUsage);
    private final PulsarService pulsar;
    private final SimpleResourceAllocationPolicies allocationPolicies;

    @VisibleForTesting
    public TransferShedder(){
        this.pulsar = null;
        this.allocationPolicies = null;
    }

    public TransferShedder(PulsarService pulsar){
        this.pulsar = pulsar;
        this.allocationPolicies = new SimpleResourceAllocationPolicies(pulsar);
    }

    private static int toPercentage(double usage) {
        return (int) (usage * 100);
    }

    @ToString
    static class LoadStats {
        double sum;
        double sqSum;
        int totalBrokers;
        double avg;
        double std;
        MinMaxPriorityQueue<String> minBrokers;
        MinMaxPriorityQueue<String> maxBrokers;
        Map<String, Double> brokerAvgResourceUsage;

        LoadStats(Map<String, Double> brokerAvgResourceUsage) {
            this.brokerAvgResourceUsage = brokerAvgResourceUsage;
            this.minBrokers = MinMaxPriorityQueue.orderedBy((a, b) -> Double.compare(
                    brokerAvgResourceUsage.get(b),
                    brokerAvgResourceUsage.get(a))).create();
            this.maxBrokers = MinMaxPriorityQueue.orderedBy((a, b) -> Double.compare(
                    brokerAvgResourceUsage.get(a),
                    brokerAvgResourceUsage.get(b))).create();
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

        void update(final LoadDataStore<BrokerLoadData> loadData, final double historyPercentage,
                                 Map<String, Long> recentlyUnloadedBrokers,
                                 final ServiceConfiguration conf, boolean debugMode) {

            double sum = 0.0;
            double sqSum = 0.0;
            int totalBrokers = 0;
            int maxTransfers = conf.getLoadBalancerExtentionsTransferShedderMaxNumberOfBrokerTransfersPerCycle();
            long now = System.currentTimeMillis();
            for (Map.Entry<String, BrokerLoadData> entry : loadData.entrySet()) {
                BrokerLoadData localBrokerData = entry.getValue();
                String broker = entry.getKey();

                // We don't want to use the outdated load data.
                if (now - localBrokerData.getLastUpdatedAt()
                        > conf.getLoadBalancerExtentionsTransferShedderLoadUpdateMaxWaitingTimeInSeconds()
                        * 1000) {
                    log.warn(
                            "Skipped broker:{} load update because the load data timestamp:{} is too old.",
                            broker, localBrokerData.getLastUpdatedAt());
                    continue;
                }

                // Also, we should give enough time for each broker to recompute its load after transfers.
                if (recentlyUnloadedBrokers.containsKey(broker)
                        && localBrokerData.getLastUpdatedAt() - recentlyUnloadedBrokers.get(broker)
            < conf.getLoadBalancerExtentionsTransferShedderBrokerLoadDataUpdateMinWaitingTimeAfterUnloadingInSeconds()
                        * 1000) {
                    log.warn(
                            "Broker:{} load update because the load data timestamp:{} is too early since "
                                    + "the last transfer timestamp:{}. Stop unloading.",
                            broker, localBrokerData.getLastUpdatedAt(), recentlyUnloadedBrokers.get(broker));
                    update(0.0, 0.0, 0);
                    return;
                }

                double load = updateAvgResourceUsage(broker, localBrokerData, recentlyUnloadedBrokers,
                        historyPercentage, conf, debugMode);

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
                update(0.0, 0.0, 0);
                return;
            }

            update(sum, sqSum, totalBrokers);
        }

        private double updateAvgResourceUsage(String broker, BrokerLoadData brokerLoadData,
                                              Map<String, Long> recentlyUnloadedBrokers,
                                              final double historyPercentage, final ServiceConfiguration conf,
                                              boolean debugMode) {
            Double historyUsage =
                    brokerAvgResourceUsage.get(broker);
            double resourceUsage = brokerLoadData.getMaxResourceUsageWithWeight(
                    conf.getLoadBalancerCPUResourceWeight(),
                    conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                    conf.getLoadBalancerBandwithInResourceWeight(),
                    conf.getLoadBalancerBandwithOutResourceWeight());

            historyUsage = historyUsage == null
                    ? resourceUsage : historyUsage * historyPercentage + (1 - historyPercentage) * resourceUsage;

            if (recentlyUnloadedBrokers.remove(broker) != null) {
                // recompute the historical usage after the transfer.
                historyUsage = resourceUsage;
            }

            if (debugMode) {
                log.info("{} broker load: historyUsage={}%, resourceUsage={}%",
                        broker,
                        historyUsage == null ? 0 : toPercentage(historyUsage),
                        toPercentage(resourceUsage));
            }

            brokerAvgResourceUsage.put(broker, historyUsage);
            return historyUsage;
        }



        @Override
        public String toString() {
            return String.format(
                    "sum:%.2f, sqSum:%.2f, avg:%.2f, std:%.2f, totalBrokers:%d, minBrokers:%s, maxBrokers:%s",
                    sum, sqSum, avg, std, totalBrokers, minBrokers, maxBrokers);
        }
    }

    @Override
    public List<Unload> findBundlesForUnloading(LoadManagerContext context,
                                                Map<String, Long> recentlyUnloadedBundles,
                                                Map<String, Long> recentlyUnloadedBrokers) {
        final var conf = context.brokerConfiguration();
        selectedBundlesCache.clear();
        stats.clear();
        boolean debugMode = conf.isLoadBalancerExtentionsTransferShedderDebugModeEnabled();

        brokerAvgResourceUsage.entrySet()
                .removeIf(e -> context.brokerLoadDataStore().get(e.getKey()).isEmpty());

        stats.update(
                context.brokerLoadDataStore(), conf.getLoadBalancerHistoryResourcePercentage(), recentlyUnloadedBrokers,
                conf, debugMode);

        if (debugMode) {
            log.info("brokers' load stats:{}", stats);
        }

        final double targetStd = conf.getLoadBalancerExtentionsTransferShedderTargetLoadStd();
        boolean transfer = conf.isLoadBalancerExtentionsTransferShedderTransferEnabled();
        while (true) {
            if (stats.maxBrokers.isEmpty() || stats.minBrokers.isEmpty()
                    || stats.maxBrokers.peekLast().equals(stats.minBrokers.peekLast())) {
                if (debugMode) {
                    log.info("Exhausted target transfer brokers. Stop unloading");
                }
                break;
            }
            if (stats.std <= targetStd && hasMsgThroughput(context, stats.minBrokers.peekLast())) {
                if (debugMode) {
                    log.info("std:{} <= targetStd:{} and minBroker:{} has msg throughput. Stop unloading.",
                            stats.std, targetStd, stats.minBrokers.peekLast());
                }
                break;
            }

            String maxBroker = stats.maxBrokers.pollLast();
            String minBroker = stats.minBrokers.pollLast();
            double max = brokerAvgResourceUsage.get(maxBroker);
            double min = brokerAvgResourceUsage.get(minBroker);
            Optional<BrokerLoadData> maxBrokerLoadData = context.brokerLoadDataStore().get(maxBroker);
            if (maxBrokerLoadData.isEmpty()) {
                log.error("maxBroker:{} maxBrokerLoadData is empty. Skip unloading from this broker.", maxBroker);
                continue;
            }
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

            MutableDouble trafficMarkedToOffload = new MutableDouble(0);
            MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);

            Optional<TopBundlesLoadData> bundlesLoadData = context.topBundleLoadDataStore().get(maxBroker);
            if (bundlesLoadData.isEmpty() || bundlesLoadData.get().getTopBundlesLoadData().isEmpty()) {
                log.error("maxBroker:{} topBundlesLoadData is empty. Skip unloading from this broker.", maxBroker);
                continue;
            }

            var topBundlesLoadData = bundlesLoadData.get().getTopBundlesLoadData();
            if (topBundlesLoadData.size() > 1) {
                MutableInt remainingTopBundles = new MutableInt();
                topBundlesLoadData.stream()
                        .filter(e ->
                                !recentlyUnloadedBundles.containsKey(e.bundleName()) && isTransferable(e.bundleName())
                        ).map((e) -> {
                            String bundle = e.bundleName();
                            var bundleData = e.stats();
                            double throughput = bundleData.msgThroughputIn + bundleData.msgThroughputOut;
                            remainingTopBundles.increment();
                            return Pair.of(bundle, throughput);
                        }).sorted((e1, e2) ->
                                Double.compare(e2.getRight(), e1.getRight())
                        ).forEach(e -> {
                            if (remainingTopBundles.getValue() > 1
                                    && (trafficMarkedToOffload.doubleValue() < offloadThroughput
                                    || atLeastOneBundleSelected.isFalse())) {
                                if (transfer) {
                                    selectedBundlesCache.add(
                                            new Unload(maxBroker, e.getLeft(),
                                                    Optional.of(minBroker)));
                                } else {
                                    selectedBundlesCache.add(
                                            new Unload(maxBroker, e.getLeft()));
                                }

                                trafficMarkedToOffload.add(e.getRight());
                                atLeastOneBundleSelected.setTrue();
                                remainingTopBundles.decrement();
                            }
                        });
            } else if (topBundlesLoadData.size() == 1) {
                log.warn(
                        "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                + "No Load Shedding will be done on this broker",
                        topBundlesLoadData.iterator().next(), maxBroker);
            } else {
                log.warn("Broker {} is overloaded despite having no bundles", maxBroker);
            }

            if (trafficMarkedToOffload.getValue() > 0) {
                stats.offload(max, min, offload);
                if (debugMode) {
                    log.info(String.format("brokers' load stats:%s, after offload{max:%.2f, min:%.2f, offload:%.2f}",
                            stats, max, min, offload));
                }
            }
        }

        if (debugMode) {
            log.info("selectedBundlesCache:{}", selectedBundlesCache);
        }

        return selectedBundlesCache;
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
