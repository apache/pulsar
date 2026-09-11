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
import javax.annotation.concurrent.ThreadSafe;
import lombok.CustomLog;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.common.naming.ServiceUnitId;

/**
 * Placement strategy which selects a broker based on which one has the least resource usage with weight.
 * This strategy takes into account the historical load percentage and short-term load percentage, and thus will not
 * cause cluster fluctuations due to short-term load jitter.
 */
@ThreadSafe
@CustomLog
public class LeastResourceUsageWithWeight implements BrokerSelectionStrategy {
    // Maintain this list to reduce object creation.
    private final ThreadLocal<ArrayList<String>> bestBrokers;
    private final ThreadLocal<HashSet<String>> noLoadDataBrokers;

    public LeastResourceUsageWithWeight() {
        this.bestBrokers = ThreadLocal.withInitial(ArrayList::new);
        this.noLoadDataBrokers = ThreadLocal.withInitial(HashSet::new);
    }

    // A broker's max resource usage with weight using its historical load and short-term load data with weight.
    private double getMaxResourceUsageWithWeight(final String broker, final BrokerLoadData brokerLoadData,
                                                 final ServiceConfiguration conf, boolean debugMode) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final var maxUsageWithWeight = brokerLoadData.getWeightedMaxEMA();


        if (maxUsageWithWeight > overloadThreshold) {
            log.warnf(
                    "Broker %s is overloaded, brokerLoad(%s%%) > overloadThreshold(%s%%). load data:{%s}",
                    broker,
                    maxUsageWithWeight * 100,
                    overloadThreshold * 100,
                    brokerLoadData.toString(conf));
        } else if (debugMode) {
            log.info().attr("broker", broker).attr("loadData", brokerLoadData.toString(conf)).log("Broker load data");
        }


        return maxUsageWithWeight;
    }


    /**
     * Find a suitable broker to assign the given bundle to.
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
            log.warn().attr("bundle", bundleToAssign)
                    .log("There are no available brokers as candidates at this point for bundle");
            return Optional.empty();
        }

        ArrayList<String> bestBrokers = this.bestBrokers.get();
        HashSet<String> noLoadDataBrokers = this.noLoadDataBrokers.get();

        bestBrokers.clear();
        noLoadDataBrokers.clear();
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
        double totalUsage = 0.0d;

        boolean debugMode = conf.isLoadBalancerDebugModeEnabled();
        for (String broker : candidates) {
            var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
            if (brokerLoadDataOptional.isEmpty()) {
                log.warn().attr("broker", broker)
                        .log("There is no broker load data for broker. Skipping this broker. Phase one");
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
                log.info().attr("avgUsage", avgUsage).attr("diffThreshold", diffThreshold)
                        .log("Computed avgUsage and diffThreshold");
            }
            for (String broker : candidates) {
                var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
                if (brokerLoadDataOptional.isEmpty()) {
                    log.warn().attr("broker", broker)
                            .log("There is no broker load data for broker. Skipping this broker. Phase two");
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
            if (debugMode) {
                log.info().attr("candidatesSize", candidates.size())
                        .attr("noLoadDataBrokersSize", noLoadDataBrokers.size())
                        .log("Assign randomly as none of the brokers are underloaded");
            }
            bestBrokers.addAll(candidates);
        }

        if (debugMode) {
            log.info()
                    .attr("bestBrokersSize", bestBrokers.size())
                    .attr("bestBrokers", bestBrokers)
                    .attr("candidates", candidates)
                    .attr("noLoadDataBrokers", noLoadDataBrokers)
                    .log("Selected best brokers from candidates");
        }
        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }
}
