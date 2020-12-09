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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Placement strategy which selects a broker based on which one has the least long term message rate.
 */
public class LeastLongTermMessageRate implements ModularLoadManagerStrategy {
    private static Logger log = LoggerFactory.getLogger(LeastLongTermMessageRate.class);

    // Maintain this list to reduce object creation.
    private ArrayList<String> bestBrokers;

    public LeastLongTermMessageRate(final ServiceConfiguration conf) {
        bestBrokers = new ArrayList<>();
    }

    // Form a score for a broker using its preallocated bundle data and time average data.
    // This is done by summing all preallocated long-term message rates and adding them to the broker's overall
    // long-term message rate, which is itself the sum of the long-term message rate of every allocated bundle.
    // Any broker at (or above) the overload threshold will have a score of POSITIVE_INFINITY.
    private static double getScore(final BrokerData brokerData, final ServiceConfiguration conf) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final double maxUsage = brokerData.getLocalData().getMaxResourceUsage();
        if (maxUsage > overloadThreshold) {
            log.warn("Broker {} is overloaded: max usage={}", brokerData.getLocalData().getWebServiceUrl(), maxUsage);
            return Double.POSITIVE_INFINITY;
        }

        double totalMessageRate = 0;
        for (BundleData bundleData : brokerData.getPreallocatedBundleData().values()) {
            final TimeAverageMessageData longTermData = bundleData.getLongTermData();
            totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
        }

        // calculate estimated score
        final TimeAverageBrokerData timeAverageData = brokerData.getTimeAverageData();
        final double timeAverageLongTermMessageRate = timeAverageData.getLongTermMsgRateIn()
                + timeAverageData.getLongTermMsgRateOut();
        final double totalMessageRateEstimate = totalMessageRate + timeAverageLongTermMessageRate;

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has long term message rate {}",
                    brokerData.getLocalData().getWebServiceUrl(), totalMessageRateEstimate);
        }
        return totalMessageRateEstimate;
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
        double minScore = Double.POSITIVE_INFINITY;
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
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

            }
            if (score < minScore) {
                // Clear best brokers since this score beats the other brokers.
                bestBrokers.clear();
                bestBrokers.add(broker);
                minScore = score;
            } else if (score == minScore) {
                // Add this broker to best brokers since it ties with the best score.
                bestBrokers.add(broker);
            }
        }
        if (bestBrokers.isEmpty()) {
            // All brokers are overloaded.
            // Assign randomly in this case.
            bestBrokers.addAll(candidates);
        }

        if (bestBrokers.isEmpty()) {
            // If still, it means there are no available brokers at this point
            return Optional.empty();
        }

        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }
}
