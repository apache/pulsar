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
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Placement strategy which selects a broker based on which one has resource usage below average load.
 */
public class ThresholdLoadManagerStrategy implements ModularLoadManagerStrategy {
    private static final Logger log = LoggerFactory.getLogger(ThresholdLoadManagerStrategy.class);

    // Maintain this list to reduce object creation.
    private final ArrayList<String> bestBrokers;

    // candidate broker -> broker usage.
    private final Map<String, Double> brokerUsages;

    public ThresholdLoadManagerStrategy() {
        bestBrokers = new ArrayList<>();
        brokerUsages = new HashMap<>();
    }

    private static double getScore(final BrokerData brokerData, final ServiceConfiguration conf) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final double maxUsage = brokerData.getLocalData().getMaxResourceUsage();
        if (maxUsage > overloadThreshold) {
            log.warn("Broker {} is overloaded: max usage={}", brokerData.getLocalData().getWebServiceUrl(), maxUsage);
            return Double.POSITIVE_INFINITY;
        }

        double brokerResourceUsage = brokerData.getLocalData().getMaxResourceUsageWithWeight(
                conf.getLoadBalancerCPUResourceWeight(), conf.getLoadBalancerMemoryResourceWeight(),
                conf.getLoadBalancerDirectMemoryResourceWeight(), conf.getLoadBalancerBandwithInResourceWeight(),
                conf.getLoadBalancerBandwithOutResourceWeight());

        if (log.isDebugEnabled()) {
            log.debug("Broker {} resource usage {}",
                    brokerData.getLocalData().getWebServiceUrl(), brokerResourceUsage);
        }
        return brokerResourceUsage;
    }

    /**
     * Find a suitable broker to assign the given bundle to.
     *
     * @param candidates
     *            The candidates for which the bundle may be assigned.
     * @param bundleToAssign
     *            The data for the bundle to assign.
     * @param loadData
     *            The load data from the leader broker.
     * @param conf
     *            The service configuration.
     * @return The name of the selected broker as it appears on ZooKeeper.
     */
    @Override
    public Optional<String> selectBroker(final Set<String> candidates, final BundleData bundleToAssign,
                                         final LoadData loadData,
                                         final ServiceConfiguration conf) {
        bestBrokers.clear();
        brokerUsages.clear();

        // Maintain of list of all the brokers below average load, and then randomly
        // select one of them at the end.
        double sumScore = 0.0;
        for (String broker : candidates) {
            final BrokerData brokerData = loadData.getBrokerData().get(broker);
            final double score = getScore(brokerData, conf);
            if (score == Double.POSITIVE_INFINITY) {
                final LocalBrokerData localData = brokerData.getLocalData();
                log.warn(
                        "Broker {} is overloaded: CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                                + "BANDWIDTH OUT: {}%",
                        broker, localData.getCpu().percentUsage(), localData.getMemory().percentUsage(),
                        localData.getDirectMemory().percentUsage(), localData.getBandwidthIn().percentUsage(),
                        localData.getBandwidthOut().percentUsage());
                continue;
            }
            sumScore += score;
            brokerUsages.put(broker, score);
        }

        if (brokerUsages.isEmpty()) {
            // All brokers are overloaded.
            // Assign randomly in this case.
            bestBrokers.addAll(candidates);
        } else {
            double avgUsage = sumScore / brokerUsages.size();
            for (Map.Entry<String, Double> entry : brokerUsages.entrySet()) {
                if (entry.getValue() <= avgUsage) {
                    bestBrokers.add(entry.getKey());
                }
            }
        }

        if (bestBrokers.isEmpty()) {
            // If still, it means there are no available brokers at this point
            return Optional.empty();
        }

        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }
}
