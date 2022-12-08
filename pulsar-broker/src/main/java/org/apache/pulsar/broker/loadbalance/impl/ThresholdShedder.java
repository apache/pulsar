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
package org.apache.pulsar.broker.loadbalance.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load shedding strategy that unloads any broker that exceeds the average resource utilization of all brokers by a
 * configured threshold. As a consequence, this strategy tends to distribute load among all brokers. It does this by
 * first computing the average resource usage per broker for the whole cluster. The resource usage for each broker is
 * calculated using the following method:
 * {@link LocalBrokerData#getMaxResourceUsageWithWeight(double, double, double, double, double)}. The weights
 * for each resource are configurable. Historical observations are included in the running average based on the broker's
 * setting for loadBalancerHistoryResourcePercentage. Once the average resource usage is calculated, a broker's
 * current/historical usage is compared to the average broker usage. If a broker's usage is greater than the average
 * usage per broker plus the loadBalancerBrokerThresholdShedderPercentage, this load shedder proposes removing
 * enough bundles to bring the unloaded broker 5% below the current average broker usage. Note that recently
 * unloaded bundles are not unloaded again.
 */
public class ThresholdShedder implements LoadSheddingStrategy {
    private static final Logger log = LoggerFactory.getLogger(ThresholdShedder.class);
    private final Multimap<String, String> selectedBundlesCache = ArrayListMultimap.create();
    private static final double ADDITIONAL_THRESHOLD_PERCENT_MARGIN = 0.05;

    private static final double LOWER_BOUNDARY_THRESHOLD_MARGIN = 0.5;

    private static final double MB = 1024 * 1024;
    private final Map<String, Double> brokerAvgResourceUsage = new HashMap<>();

    @Override
    public Multimap<String, String> findBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
        selectedBundlesCache.clear();
        final double threshold = conf.getLoadBalancerBrokerThresholdShedderPercentage() / 100.0;
        final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();
        final double minThroughputThreshold = conf.getLoadBalancerBundleUnloadMinThroughputThreshold() * MB;

        final double avgUsage = getBrokerAvgUsage(loadData, conf.getLoadBalancerHistoryResourcePercentage(), conf);

        if (avgUsage == 0) {
            log.warn("average max resource usage is 0");
            return selectedBundlesCache;
        }

        loadData.getBrokerData().forEach((broker, brokerData) -> {
            final LocalBrokerData localData = brokerData.getLocalData();
            final double currentUsage = brokerAvgResourceUsage.getOrDefault(broker, 0.0);

            if (currentUsage < avgUsage + threshold) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] broker is not overloaded, ignoring at this point", broker);
                }
                return;
            }

            double percentOfTrafficToOffload =
                    currentUsage - avgUsage - threshold + ADDITIONAL_THRESHOLD_PERCENT_MARGIN;
            double brokerCurrentThroughput = localData.getMsgThroughputIn() + localData.getMsgThroughputOut();
            double minimumThroughputToOffload = brokerCurrentThroughput * percentOfTrafficToOffload;

            if (minimumThroughputToOffload < minThroughputThreshold) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] broker is planning to shed throughput {} MByte/s less than "
                                    + "minimumThroughputThreshold {} MByte/s, skipping bundle unload.",
                            broker, minimumThroughputToOffload / MB, minThroughputThreshold / MB);
                }
                return;
            }

            log.info(
                    "Attempting to shed load on {}, which has max resource usage above avgUsage  and threshold {}%"
                            + " > {}% + {}% -- Offloading at least {} MByte/s of traffic, left throughput {} MByte/s",
                    broker, 100 * currentUsage, 100 * avgUsage, 100 * threshold, minimumThroughputToOffload / MB,
                    (brokerCurrentThroughput - minimumThroughputToOffload) / MB);

            if (localData.getBundles().size() > 1) {
                filterAndSelectBundle(loadData, recentlyUnloadedBundles, broker, localData, minimumThroughputToOffload);
            } else if (localData.getBundles().size() == 1) {
                log.warn(
                        "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                + "No Load Shedding will be done on this broker",
                        localData.getBundles().iterator().next(), broker);
            } else {
                log.warn("Broker {} is overloaded despite having no bundles", broker);
            }
        });
        if (selectedBundlesCache.isEmpty() && conf.isLowerBoundarySheddingEnabled()) {
            tryLowerBoundaryShedding(loadData, conf);
        }
        return selectedBundlesCache;
    }

    private void filterAndSelectBundle(LoadData loadData, Map<String, Long> recentlyUnloadedBundles, String broker,
                                       LocalBrokerData localData, double minimumThroughputToOffload) {
        MutableDouble trafficMarkedToOffload = new MutableDouble(0);
        MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);
        loadData.getBundleDataForLoadShedding().entrySet().stream()
                .map((e) -> {
                    String bundle = e.getKey();
                    BundleData bundleData = e.getValue();
                    TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                    double throughput = shortTermData.getMsgThroughputIn() + shortTermData.getMsgThroughputOut();
                    return Pair.of(bundle, throughput);
                }).filter(e ->
                        !recentlyUnloadedBundles.containsKey(e.getLeft())
                ).filter(e ->
                        localData.getBundles().contains(e.getLeft())
                ).sorted((e1, e2) ->
                        Double.compare(e2.getRight(), e1.getRight())
                ).forEach(e -> {
                    if (trafficMarkedToOffload.doubleValue() < minimumThroughputToOffload
                            || atLeastOneBundleSelected.isFalse()) {
                        selectedBundlesCache.put(broker, e.getLeft());
                        trafficMarkedToOffload.add(e.getRight());
                        atLeastOneBundleSelected.setTrue();
                    }
                });
    }

    private double getBrokerAvgUsage(final LoadData loadData, final double historyPercentage,
                                     final ServiceConfiguration conf) {
        double totalUsage = 0.0;
        int totalBrokers = 0;

        for (Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            LocalBrokerData localBrokerData = entry.getValue().getLocalData();
            String broker = entry.getKey();
            totalUsage += updateAvgResourceUsage(broker, localBrokerData, historyPercentage, conf);
            totalBrokers++;
        }

        return totalBrokers > 0 ? totalUsage / totalBrokers : 0;
    }

    private double updateAvgResourceUsage(String broker, LocalBrokerData localBrokerData,
                                          final double historyPercentage, final ServiceConfiguration conf) {
        Double historyUsage =
                brokerAvgResourceUsage.get(broker);
        double resourceUsage = localBrokerData.getMaxResourceUsageWithWeight(
                conf.getLoadBalancerCPUResourceWeight(),
                conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                conf.getLoadBalancerBandwithInResourceWeight(),
                conf.getLoadBalancerBandwithOutResourceWeight());
        historyUsage = historyUsage == null
                ? resourceUsage : historyUsage * historyPercentage + (1 - historyPercentage) * resourceUsage;

        brokerAvgResourceUsage.put(broker, historyUsage);
        return historyUsage;
    }

    private void tryLowerBoundaryShedding(LoadData loadData, ServiceConfiguration conf) {
        // Select the broker with the most resource usage.
        final double threshold = conf.getLoadBalancerBrokerThresholdShedderPercentage() / 100.0;
        final double avgUsage = getBrokerAvgUsage(loadData, conf.getLoadBalancerHistoryResourcePercentage(), conf);
        Pair<Boolean, String> result = getMaxUsageBroker(loadData, threshold, avgUsage);
        boolean hasBrokerBelowLowerBound = result.getLeft();
        String maxUsageBroker = result.getRight();
        BrokerData brokerData = loadData.getBrokerData().get(maxUsageBroker);
        if (brokerData == null) {
            log.info("Load data is null or bundle <=1, skipping bundle unload.");
            return;
        }
        if (!hasBrokerBelowLowerBound) {
            log.info("No broker is below the lower bound, threshold is {}, "
                            + "avgUsage usage is {}, max usage of Broker {} is {}",
                    threshold, avgUsage, maxUsageBroker,
                    brokerAvgResourceUsage.getOrDefault(maxUsageBroker, 0.0));
            return;
        }
        LocalBrokerData localData = brokerData.getLocalData();
        double brokerCurrentThroughput = localData.getMsgThroughputIn() + localData.getMsgThroughputOut();
        double minimumThroughputToOffload = brokerCurrentThroughput * threshold * LOWER_BOUNDARY_THRESHOLD_MARGIN;
        double minThroughputThreshold = conf.getLoadBalancerBundleUnloadMinThroughputThreshold() * MB;
        if (minThroughputThreshold > minimumThroughputToOffload) {
            log.info("broker {} in lower boundary shedding is planning to shed throughput {} MByte/s less than "
                            + "minimumThroughputThreshold {} MByte/s, skipping bundle unload.",
                    maxUsageBroker, minimumThroughputToOffload / MB, minThroughputThreshold / MB);
            return;
        }
        filterAndSelectBundle(loadData, loadData.getRecentlyUnloadedBundles(), maxUsageBroker, localData,
                minimumThroughputToOffload);
    }

    private Pair<Boolean, String> getMaxUsageBroker(
            LoadData loadData, double threshold, double avgUsage) {
        String maxUsageBrokerName = "";
        double maxUsage = avgUsage - threshold;
        boolean hasBrokerBelowLowerBound = false;
        for (Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            String broker = entry.getKey();
            BrokerData brokerData = entry.getValue();
            double currentUsage = brokerAvgResourceUsage.getOrDefault(broker, 0.0);
            // Select the broker with the most resource usage.
            if (currentUsage > maxUsage && brokerData.getLocalData() != null
                    && brokerData.getLocalData().getBundles().size() > 1) {
                maxUsage = currentUsage;
                maxUsageBrokerName = broker;
            }
            // Whether any brokers with low usage in the cluster.
            if (currentUsage < avgUsage - threshold) {
                hasBrokerBelowLowerBound = true;
            }
        }
        return Pair.of(hasBrokerBelowLowerBound, maxUsageBrokerName);
    }

}
