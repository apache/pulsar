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
package org.apache.pulsar.broker.loadbalance.extensions.strategy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.common.naming.ServiceUnitId;

/**
 * Placement strategy which selects a broker based on which one has the least resource usage with weight.
 * This strategy takes into account the historical load percentage and short-term load percentage, and thus will not
 * cause cluster fluctuations due to short-term load jitter.
 */
@Slf4j
public class LeastResourceUsageWithWeight implements BrokerSelectionStrategy {
    // Maintain this list to reduce object creation.
    private final ArrayList<String> bestBrokers;
    private final Set<String> noLoadDataBrokers;

    public LeastResourceUsageWithWeight() {
        this.bestBrokers = new ArrayList<>();
        this.noLoadDataBrokers = new HashSet<>();
    }

    // A broker's max resource usage with weight using its historical load and short-term load data with weight.
    private double getMaxResourceUsageWithWeight(final String broker, final BrokerLoadData brokerLoadData,
                                                 final ServiceConfiguration conf, boolean debugMode) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final var maxUsageWithWeight = brokerLoadData.getWeightedMaxEMA();


        if (maxUsageWithWeight > overloadThreshold) {
            log.warn(
                    "Broker {} is overloaded, brokerLoad({}%) > overloadThreshold({}%). load data:{{}}",
                    broker,
                    maxUsageWithWeight * 100,
                    overloadThreshold * 100,
                    brokerLoadData.toString(conf));
        } else if (debugMode) {
            log.info("Broker {} load data:{{}}", broker, brokerLoadData.toString(conf));
        }


        return maxUsageWithWeight;
    }


    /**
     * Find a suitable broker to assign the given bundle to.
     * This method is not thread safety.
     *
     * @param candidates     The candidates for which the bundle may be assigned.
     * @param bundleToAssign The data for the bundle to assign.
     * @param context       The load manager context.
     * @return The name of the selected broker as it appears on ZooKeeper.
     */
    @Override
    public Optional<String> select(
            Set<String> candidates, ServiceUnitId bundleToAssign, LoadManagerContext context) {
        var conf = context.brokerConfiguration();
        if (candidates.isEmpty()) {
            log.info("There are no available brokers as candidates at this point for bundle: {}", bundleToAssign);
            return Optional.empty();
        }

        bestBrokers.clear();
        noLoadDataBrokers.clear();
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
        double totalUsage = 0.0d;

        // TODO: use loadBalancerDebugModeEnabled too.
        boolean debugMode = log.isDebugEnabled();
        for (String broker : candidates) {
            var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
            if (brokerLoadDataOptional.isEmpty()) {
                log.warn("There is no broker load data for broker:{}. Skipping this broker. Phase one", broker);
                noLoadDataBrokers.add(broker);
                continue;
            }

            var brokerLoadData = brokerLoadDataOptional.get();

            double usageWithWeight =
                    getMaxResourceUsageWithWeight(broker, brokerLoadData, context.brokerConfiguration(), debugMode);
            totalUsage += usageWithWeight;
        }

        if (candidates.size() > noLoadDataBrokers.size()) {
            final double avgUsage = totalUsage / (candidates.size() - noLoadDataBrokers.size());
            final double diffThreshold =
                    conf.getLoadBalancerAverageResourceUsageDifferenceThresholdPercentage() / 100.0;
            if (debugMode) {
                log.info("Computed avgUsage:{}, diffThreshold:{}", avgUsage, diffThreshold);
            }
            for (String broker : candidates) {
                var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
                if (brokerLoadDataOptional.isEmpty()) {
                    log.warn("There is no broker load data for broker:{}. Skipping this broker. Phase two", broker);
                    continue;
                }
                double avgResUsage = brokerLoadDataOptional.get().getWeightedMaxEMA();
                if ((avgResUsage + diffThreshold <= avgUsage && !noLoadDataBrokers.contains(broker))) {
                    bestBrokers.add(broker);
                }
            }
        }

        if (bestBrokers.isEmpty()) {
            // Assign randomly as all brokers are overloaded.
            log.warn("Assign randomly as none of the brokers are underloaded. candidatesSize:{}, "
                    + "noLoadDataBrokersSize:{}", candidates.size(), noLoadDataBrokers.size());
            for (String broker : candidates) {
                bestBrokers.add(broker);
            }
        }

        if (debugMode) {
            log.info("Selected {} best brokers: {} from candidate brokers: {}, noLoadDataBrokers:{}",
                    bestBrokers.size(), bestBrokers,
                    candidates,
                    noLoadDataBrokers);
        }
        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }
}
