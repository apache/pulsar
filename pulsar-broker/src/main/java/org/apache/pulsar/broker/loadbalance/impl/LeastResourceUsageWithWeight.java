/**
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
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class LeastResourceUsageWithWeight implements ModularLoadManagerStrategy {
    // Maintain this list to reduce object creation.
    private final ArrayList<String> bestBrokers;
    private final Map<String, Double> brokerAvgResourceUsageWithWeight;

    public LeastResourceUsageWithWeight() {
        this.bestBrokers = new ArrayList<>();
        this.brokerAvgResourceUsageWithWeight = new HashMap<>();
    }

    // A broker's max resource usage with weight using its historical load and short-term load data with weight.
    private double getMaxResourceUsageWithWeight(final String broker, final BrokerData brokerData,
                                         final ServiceConfiguration conf) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final double maxUsageWithWeight =
                updateAndGetMaxResourceUsageWithWeight(broker, brokerData, conf);

        if (maxUsageWithWeight > overloadThreshold) {
            final LocalBrokerData localData = brokerData.getLocalData();
            log.warn(
                    "Broker {} is overloaded, max resource usage with weight percentage: {}%, "
                            + "CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                            + "BANDWIDTH OUT: {}%, CPU weight: {}, MEMORY weight: {}, DIRECT MEMORY weight: {}, "
                            + "BANDWIDTH IN weight: {}, BANDWIDTH OUT weight: {}",
                    broker, maxUsageWithWeight * 100,
                    localData.getCpu().percentUsage(), localData.getMemory().percentUsage(),
                    localData.getDirectMemory().percentUsage(), localData.getBandwidthIn().percentUsage(),
                    localData.getBandwidthOut().percentUsage(), conf.getLoadBalancerCPUResourceWeight(),
                    conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                    conf.getLoadBalancerBandwithInResourceWeight(),
                    conf.getLoadBalancerBandwithOutResourceWeight());
        }

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has max resource usage with weight percentage: {}%",
                    brokerData.getLocalData().getWebServiceUrl(), maxUsageWithWeight * 100);
        }
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
    private double updateAndGetMaxResourceUsageWithWeight(String broker, BrokerData brokerData,
                                                          ServiceConfiguration conf) {
        final double historyPercentage = conf.getLoadBalancerHistoryResourcePercentage();
        Double historyUsage = brokerAvgResourceUsageWithWeight.get(broker);
        double resourceUsage = brokerData.getLocalData().getMaxResourceUsageWithWeight(
                conf.getLoadBalancerCPUResourceWeight(),
                conf.getLoadBalancerMemoryResourceWeight(),
                conf.getLoadBalancerDirectMemoryResourceWeight(),
                conf.getLoadBalancerBandwithInResourceWeight(),
                conf.getLoadBalancerBandwithOutResourceWeight());
        historyUsage = historyUsage == null
                ? resourceUsage : historyUsage * historyPercentage + (1 - historyPercentage) * resourceUsage;
        if (log.isDebugEnabled()) {
            log.debug(
                    "Broker {} get max resource usage with weight: {}, history resource percentage: {}%, CPU weight: "
                            + "{}, MEMORY weight: {}, DIRECT MEMORY weight: {}, BANDWIDTH IN weight: {}, BANDWIDTH "
                            + "OUT weight: {} ",
                    broker, historyUsage, historyPercentage, conf.getLoadBalancerCPUResourceWeight(),
                    conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                    conf.getLoadBalancerBandwithInResourceWeight(),
                    conf.getLoadBalancerBandwithOutResourceWeight());
        }
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
    public Optional<String> selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData,
                                         ServiceConfiguration conf) {
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
        brokerAvgResourceUsageWithWeight.forEach((broker, avgResUsage) -> {
            if (avgResUsage + diffThreshold <= avgUsage) {
                bestBrokers.add(broker);
            }
        });

        if (bestBrokers.isEmpty()) {
            // Assign randomly as all brokers are overloaded.
            log.warn("Assign randomly as all {} brokers are overloaded.", candidates.size());
            bestBrokers.addAll(candidates);
        }

        if (bestBrokers.isEmpty()) {
            // If still, it means there are no available brokers at this point.
            log.error("There are no available brokers as candidates at this point for bundle: {}", bundleToAssign);
            return Optional.empty();
        }

        if (log.isDebugEnabled()) {
            log.debug("Selected {} best brokers: {} from candidate brokers: {}", bestBrokers.size(), bestBrokers,
                    candidates);
        }
        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }
}
