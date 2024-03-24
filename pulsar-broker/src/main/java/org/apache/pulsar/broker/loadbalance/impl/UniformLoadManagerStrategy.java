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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Placement strategy which selects a broker based on which one has the least long/short term message rate/throughput.
 * Corresponds to the {@link UniformLoadShedder} in the load balancer.
 * */
public class UniformLoadManagerStrategy implements ModularLoadManagerStrategy {
    private static final Logger log = LoggerFactory.getLogger(UniformLoadManagerStrategy.class);

    // Maintain this list to reduce object creation.
    private ArrayList<String> bestBrokers;

    public UniformLoadManagerStrategy() {
        bestBrokers = new ArrayList<>();
    }

    // Form a score for a broker using its pre-allocated bundle data and time average data.
    // This is done by summing all pre-allocated short/long-term message rate/throughput and adding them to the
    // broker's overall short/long-term message rate/throughput, which is itself the sum of the short/long-term message
    // rate/throughput of every allocated bundle.
    // Any broker at (or above) the overload threshold will have a score of POSITIVE_INFINITY.
    private static double getScore(final BrokerData brokerData, final ServiceConfiguration conf) {
        final double maxUsage = brokerData.getLocalData().getMaxResourceUsage();
        if (maxUsage > conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0) {
            log.warn("Broker {} is overloaded: max usage={}", brokerData.getLocalData().getWebServiceUrl(), maxUsage);
            return Double.POSITIVE_INFINITY;
        }
        UniformPlacementType placementType = getPlacementType(conf);
        double totalMessageRate = 0;
        for (BundleData bundleData : brokerData.getPreallocatedBundleData().values()) {
            final TimeAverageMessageData shortTermData = bundleData.getShortTermData();
            final TimeAverageMessageData longTermData = bundleData.getLongTermData();
            switch (placementType) {
                case LEAST_SHORT_TERM_MSG_RATE ->
                        totalMessageRate += shortTermData.getMsgRateIn() + shortTermData.getMsgRateOut();
                case LEAST_SHORT_TERM_MSG_THROUGHPUT ->
                        totalMessageRate += shortTermData.getMsgThroughputIn() + shortTermData.getMsgThroughputOut();
                case LEAST_LONG_TERM_MSG_RATE ->
                        totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
                case LEAST_LONG_TERM_MSG_THROUGHPUT ->
                        totalMessageRate += longTermData.getMsgThroughputIn() + longTermData.getMsgThroughputOut();
            }
        }

        // calculate estimated score
        final TimeAverageBrokerData timeAverageData = brokerData.getTimeAverageData();
        switch (placementType) {
            case LEAST_SHORT_TERM_MSG_RATE -> totalMessageRate += timeAverageData.getShortTermMsgRateIn()
                    + timeAverageData.getShortTermMsgRateOut();
            case LEAST_SHORT_TERM_MSG_THROUGHPUT -> totalMessageRate += timeAverageData.getShortTermMsgThroughputIn()
                    + timeAverageData.getShortTermMsgThroughputOut();
            case LEAST_LONG_TERM_MSG_RATE -> totalMessageRate += timeAverageData.getLongTermMsgRateIn()
                    + timeAverageData.getLongTermMsgRateOut();
            case LEAST_LONG_TERM_MSG_THROUGHPUT -> totalMessageRate += timeAverageData.getLongTermMsgThroughputIn()
                    + timeAverageData.getLongTermMsgThroughputOut();
        }

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has long term message rate {}",
                    brokerData.getLocalData().getWebServiceUrl(), totalMessageRate);
        }
        return totalMessageRate;
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
            final LoadData loadData, final ServiceConfiguration conf) {
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

    public static UniformPlacementType getPlacementType(ServiceConfiguration conf) {
        return switch (conf.getLoadBalancerUniformPlacementStrategyType()) {
            case "least_long_term_msg_throughput" -> UniformPlacementType.LEAST_LONG_TERM_MSG_THROUGHPUT;
            case "least_short_term_msg_rate" -> UniformPlacementType.LEAST_SHORT_TERM_MSG_RATE;
            case "least_short_term_msg_throughput" -> UniformPlacementType.LEAST_SHORT_TERM_MSG_THROUGHPUT;
            default -> UniformPlacementType.LEAST_LONG_TERM_MSG_RATE;
        };
    }

    public enum UniformPlacementType {
        LEAST_LONG_TERM_MSG_RATE,
        LEAST_LONG_TERM_MSG_THROUGHPUT,
        LEAST_SHORT_TERM_MSG_RATE,
        LEAST_SHORT_TERM_MSG_THROUGHPUT
    }
}
