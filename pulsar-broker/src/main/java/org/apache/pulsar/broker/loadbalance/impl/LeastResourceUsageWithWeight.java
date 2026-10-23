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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import lombok.CustomLog;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * Placement strategy which selects a broker based on which one has the least resource usage with weight.
 * This strategy takes into account the historical load percentage and short-term load percentage, and thus will not
 * cause cluster fluctuations due to short-term load jitter.
 */
@CustomLog
public class LeastResourceUsageWithWeight implements ModularLoadManagerStrategy {
    private static final double MAX_RESOURCE_USAGE = 1.0d;
    // Maintain this list to reduce object creation.
    private final ArrayList<String> bestBrokers;
    private final Map<String, Double> brokerAvgResourceUsageWithWeight;

    public LeastResourceUsageWithWeight() {
        this.bestBrokers = new ArrayList<>();
        this.brokerAvgResourceUsageWithWeight = new HashMap<>();
    }

    // A broker's max resource usage with weight using its historical load and short-term load data with weight.
    @SuppressWarnings("deprecation")
    private double getMaxResourceUsageWithWeight(final String broker, final BrokerData brokerData,
                                         final ServiceConfiguration conf) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final double maxUsageWithWeight =
                updateAndGetMaxResourceUsageWithWeight(broker, brokerData, conf);

        if (maxUsageWithWeight > overloadThreshold) {
            final LocalBrokerData localData = brokerData.getLocalData();
            log.warn()
                    .attr("broker", broker)
                    .attr("maxUsageWithWeightPercentage", maxUsageWithWeight * 100)
                    .attr("cpuPercentage", localData.getCpu().percentUsage())
                    .attr("memoryPercentage", localData.getMemory().percentUsage())
                    .attr("directMemoryPercentage", localData.getDirectMemory().percentUsage())
                    .attr("bandwidthInPercentage", localData.getBandwidthIn().percentUsage())
                    .attr("bandwidthOutPercentage", localData.getBandwidthOut().percentUsage())
                    .attr("cpuWeight", conf.getLoadBalancerCPUResourceWeight())
                    .attr("memoryWeight", conf.getLoadBalancerMemoryResourceWeight())
                    .attr("directMemoryWeight", conf.getLoadBalancerDirectMemoryResourceWeight())
                    .attr("bandwidthInWeight", conf.getLoadBalancerBandwidthInResourceWeight())
                    .attr("bandwidthOutWeight", conf.getLoadBalancerBandwidthOutResourceWeight())
                    .log("Broker is overloaded");
        }

        log.debug().attr("broker", () -> brokerData.getLocalData().getWebServiceUrl())
                .attr("percentage", maxUsageWithWeight * 100)
                .log("Broker has max resource usage with weight percentage");
        return maxUsageWithWeight;
    }

    /**
     * Update and get the max resource usage with weight of broker according to the service configuration.
     *
     * @param broker     the broker name.
     * @param brokerData The broker load data.
     * @param conf       The service configuration.
     * @return the max resource usage with weight of broker
     */
    @SuppressWarnings("deprecation")
    private double updateAndGetMaxResourceUsageWithWeight(String broker, BrokerData brokerData,
                                                          ServiceConfiguration conf) {
        final double historyPercentage = conf.getLoadBalancerHistoryResourcePercentage();
        Double historyUsage = brokerAvgResourceUsageWithWeight.get(broker);
        LocalBrokerData localData = brokerData.getLocalData();
        // If the broker restarted or MsgRate is 0, should use current resourceUsage to cover the historyUsage
        if (localData.getBundles().size() == 0 || (localData.getMsgRateIn() == 0 && localData.getMsgRateOut() == 0)){
            historyUsage = null;
        }
        double resourceUsage = brokerData.getLocalData().getMaxResourceUsageWithWeight(
                conf.getLoadBalancerCPUResourceWeight(),
                conf.getLoadBalancerDirectMemoryResourceWeight(),
                conf.getLoadBalancerBandwidthInResourceWeight(),
                conf.getLoadBalancerBandwidthOutResourceWeight());
        historyUsage = historyUsage == null
                ? resourceUsage : historyUsage * historyPercentage + (1 - historyPercentage) * resourceUsage;
        log.debug()
                .attr("broker", broker)
                .attr("historyUsage", historyUsage)
                .attr("historyPercentage", historyPercentage)
                .attr("cpuWeight", conf.getLoadBalancerCPUResourceWeight())
                .attr("memoryWeight", conf.getLoadBalancerMemoryResourceWeight())
                .attr("directMemoryWeight", conf.getLoadBalancerDirectMemoryResourceWeight())
                .attr("bandwidthInWeight", conf.getLoadBalancerBandwidthInResourceWeight())
                .attr("bandwidthOutWeight", conf.getLoadBalancerBandwidthOutResourceWeight())
                .log("Broker get max resource usage with weight");
        brokerAvgResourceUsageWithWeight.put(broker, historyUsage);
        return historyUsage;
    }

    /**
     * Find a suitable broker to assign the given bundle to.
     * This method is not thread safety.
     *
     * @param candidates     The candidates for which the bundle may be assigned.
     * @param bundleToAssign The data for the bundle to assign.
     * @param loadData       The load data from the leader broker.
     * @param conf           The service configuration.
     * @return The name of the selected broker as it appears on ZooKeeper.
     */
    @Override
    public synchronized Optional<String> selectBroker(Set<String> candidates, BundleData bundleToAssign,
                                                      LoadData loadData,
                                                      ServiceConfiguration conf) {
        if (candidates.isEmpty()) {
            log.info().attr("bundle", bundleToAssign)
                    .log("There are no available brokers as candidates at this point for bundle");
            return Optional.empty();
        }

        bestBrokers.clear();
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
        double totalUsage = 0.0d;
        for (String broker : candidates) {
            BrokerData brokerData = loadData.getBrokerData().get(broker);
            double usageWithWeight = getMaxResourceUsageWithWeight(broker, brokerData, conf);
            totalUsage += usageWithWeight;
        }

        final double avgUsage = totalUsage / candidates.size();
        final double diffThreshold =
                conf.getLoadBalancerAverageResourceUsageDifferenceThresholdPercentage() / 100.0;
        candidates.forEach(broker -> {
            Double avgResUsage = brokerAvgResourceUsageWithWeight.getOrDefault(broker, MAX_RESOURCE_USAGE);
            if ((avgResUsage + diffThreshold <= avgUsage)) {
                bestBrokers.add(broker);
            }
        });

        if (bestBrokers.isEmpty()) {
            // Assign randomly as all brokers are overloaded.
            log.warn().attr("candidatesSize", candidates.size()).log("Assign randomly as all brokers are overloaded");
            bestBrokers.addAll(candidates);
        }

        log.debug().attr("bestBrokersSize", bestBrokers.size()).attr("bestBrokers", bestBrokers)
                .attr("candidates", candidates)
                .log("Selected best brokers from candidates");
        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }
    @Override
    public synchronized void onActiveBrokersChange(Set<String> activeBrokers) {
        brokerAvgResourceUsageWithWeight.keySet().removeIf((key) -> !activeBrokers.contains(key));
    }
}
