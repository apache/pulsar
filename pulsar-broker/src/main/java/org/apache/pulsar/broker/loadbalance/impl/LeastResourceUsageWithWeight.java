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
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Placement strategy which selects a broker based on which one has the least resource usage with weight.
 * This strategy takes into account the historical load percentage and short-term load percentage, and thus will not
 * cause cluster fluctuations due to short-term load jitter.
 */
public class LeastResourceUsageWithWeight implements ModularLoadManagerStrategy {
    private static Logger log = LoggerFactory.getLogger(LeastResourceUsageWithWeight.class);

    // Maintain this list to reduce object creation.
    private final ArrayList<String> bestBrokers;
    private final Map<String, Double> brokerAvgResourceUsageWithWeight;

    public LeastResourceUsageWithWeight() {
        this.bestBrokers = new ArrayList<>();
        this.brokerAvgResourceUsageWithWeight = new HashMap<>();
    }

    // Form a score for a broker using its historical load and short-term load data with weight.
    // Any broker at (or above) the overload threshold will have a score of POSITIVE_INFINITY.
    private double getScore(final String broker, final BrokerData brokerData, final ServiceConfiguration conf) {
        final double overloadThresholdPercentage = conf.getLoadBalancerBrokerOverloadedThresholdPercentage();
        final double maxUsageWithWeightPercentage =
                updateAndGetMaxResourceUsageWithWeight(broker, brokerData, conf) * 100;

        if (maxUsageWithWeightPercentage > overloadThresholdPercentage) {
            log.warn("Broker {} is overloaded: max resource usage with weight percentage: {}%",
                    brokerData.getLocalData().getWebServiceUrl(), maxUsageWithWeightPercentage);
            return Double.POSITIVE_INFINITY;
        }

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has max resource usage with weight percentage: {}%",
                    brokerData.getLocalData().getWebServiceUrl(), maxUsageWithWeightPercentage);
        }
        return maxUsageWithWeightPercentage;
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
        double minScore = Double.POSITIVE_INFINITY;
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.

        for (String broker : candidates) {
            final BrokerData brokerData = loadData.getBrokerData().get(broker);
            final double score = getScore(broker, brokerData, conf);
            if (score == Double.POSITIVE_INFINITY) {
                final LocalBrokerData localData = brokerData.getLocalData();
                log.warn(
                        "Broker {} is overloaded: CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                                + "BANDWIDTH OUT: {}%, CPU weight: {}, MEMORY weight: {}, DIRECT MEMORY weight: {}, "
                                + "BANDWIDTH IN weight: {}, BANDWIDTH OUT weight: {}",
                        broker, localData.getCpu().percentUsage(), localData.getMemory().percentUsage(),
                        localData.getDirectMemory().percentUsage(), localData.getBandwidthIn().percentUsage(),
                        localData.getBandwidthOut().percentUsage(), conf.getLoadBalancerCPUResourceWeight(),
                        conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                        conf.getLoadBalancerBandwithInResourceWeight(),
                        conf.getLoadBalancerBandwithOutResourceWeight());
            }
            if (score < minScore) {
                bestBrokers.clear();
                bestBrokers.add(broker);
                minScore = score;
            } else if (score == minScore) {
                bestBrokers.add(broker);
            }
        }
        if (bestBrokers.isEmpty()) {
            // Assign randomly if all brokers are overloaded.
            log.warn("Assign randomly if all {} brokers are overloaded.", candidates.size());
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
