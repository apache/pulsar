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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * This strategy tends to distribute load uniformly across all brokers. This strategy checks load difference between
 * broker with highest load and broker with lowest load. If the difference is higher than configured thresholds
 * {@link ServiceConfiguration#getLoadBalancerMsgRateDifferenceShedderThreshold()} or
 * {@link ServiceConfiguration#loadBalancerMsgThroughputMultiplierDifferenceShedderThreshold()} then it finds out
 * bundles which can be unloaded to distribute traffic evenly across all brokers.
 *
 */
@Slf4j
public class UniformLoadShedder implements LoadSheddingStrategy {

    private static final int MB = 1024 * 1024;
    private static final double MAX_UNLOAD_PERCENTAGE = 0.2;
    private static final int MIN_UNLOAD_MESSAGE = 1000;
    private static final int MIN_UNLOAD_THROUGHPUT = 1 * MB;
    private final Multimap<String, String> selectedBundlesCache = ArrayListMultimap.create();
    private static final double EPS = 1e-6;

    /**
     * Attempt to shed some bundles off every broker which is overloaded.
     *
     * @param loadData
     *            The load data to used to make the unloading decision.
     * @param conf
     *            The service configuration.
     * @return A map from bundles to unload to the brokers on which they are loaded.
     */
    @Override
    public Multimap<String, String> findBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
        selectedBundlesCache.clear();
        Map<String, BrokerData> brokersData = loadData.getBrokerData();
        Map<String, BundleData> loadBundleData = loadData.getBundleDataForLoadShedding();
        Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();

        MutableObject<String> overloadedBroker = new MutableObject<>();
        MutableObject<String> underloadedBroker = new MutableObject<>();
        MutableDouble maxMsgRate = new MutableDouble(-1);
        MutableDouble maxThroughputRate = new MutableDouble(-1);
        MutableDouble minMsgRate = new MutableDouble(Integer.MAX_VALUE);
        MutableDouble minThroughputRate = new MutableDouble(Integer.MAX_VALUE);
        brokersData.forEach((broker, data) -> {
            //broker with one bundle can't be considered for bundle unloading
            if (data.getLocalData().getBundles().size() <= 1) {
                return;
            }

            double msgRate = data.getLocalData().getMsgRateIn() + data.getLocalData().getMsgRateOut();
            double throughputRate = data.getLocalData().getMsgThroughputIn()
                    + data.getLocalData().getMsgThroughputOut();
            if (msgRate > maxMsgRate.getValue() || throughputRate > maxThroughputRate.getValue()) {
                overloadedBroker.setValue(broker);
                maxMsgRate.setValue(msgRate);
                maxThroughputRate.setValue(throughputRate);
            }
            if (msgRate < minMsgRate.getValue() || throughputRate < minThroughputRate.getValue()) {
                underloadedBroker.setValue(broker);
                minMsgRate.setValue(msgRate);
                minThroughputRate.setValue(throughputRate);
            }
        });

        // find the difference between two brokers based on msgRate and throughout and check if the load distribution
        // discrepancy is higher than threshold. if that matches then try to unload bundle from overloaded brokers to
        // give chance of uniform load distribution.
        if (minMsgRate.getValue() <= EPS && minMsgRate.getValue() >= -EPS) {
            minMsgRate.setValue(1.0);
        }
        if (minThroughputRate.getValue() <= EPS && minThroughputRate.getValue() >= -EPS) {
            minThroughputRate.setValue(1.0);
        }
        double msgRateDifferencePercentage = ((maxMsgRate.getValue() - minMsgRate.getValue()) * 100)
                / (minMsgRate.getValue());
        double msgThroughputDifferenceRate = maxThroughputRate.getValue() / minThroughputRate.getValue();

        // if the threshold matches then find out how much load needs to be unloaded by considering number of msgRate
        // and throughput.
        boolean isMsgRateThresholdExceeded = conf.getLoadBalancerMsgRateDifferenceShedderThreshold() > 0
                && msgRateDifferencePercentage > conf.getLoadBalancerMsgRateDifferenceShedderThreshold();
        boolean isMsgThroughputThresholdExceeded = conf
                .getLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold() > 0
                && msgThroughputDifferenceRate > conf
                        .getLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold();

        if (isMsgRateThresholdExceeded || isMsgThroughputThresholdExceeded) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Found bundles for uniform load balancing. "
                                + "overloaded broker {} with (msgRate,throughput)= ({},{}) "
                                + "and underloaded broker {} with (msgRate,throughput)= ({},{})",
                        overloadedBroker.getValue(), maxMsgRate.getValue(), maxThroughputRate.getValue(),
                        underloadedBroker.getValue(), minMsgRate.getValue(), minThroughputRate.getValue());
            }
            MutableInt msgRateRequiredFromUnloadedBundles = new MutableInt(
                    (int) ((maxMsgRate.getValue() - minMsgRate.getValue()) * MAX_UNLOAD_PERCENTAGE));
            MutableInt msgThroughputRequiredFromUnloadedBundles = new MutableInt(
                    (int) ((maxThroughputRate.getValue() - minThroughputRate.getValue()) * MAX_UNLOAD_PERCENTAGE));
            LocalBrokerData overloadedBrokerData = brokersData.get(overloadedBroker.getValue()).getLocalData();

            if (overloadedBrokerData.getBundles().size() > 1
                && (msgRateRequiredFromUnloadedBundles.getValue() >= MIN_UNLOAD_MESSAGE
                    || msgThroughputRequiredFromUnloadedBundles.getValue() >= MIN_UNLOAD_THROUGHPUT)) {
                // Sort bundles by throughput, then pick the bundle which can help to reduce load uniformly with
                // under-loaded broker
                loadBundleData.entrySet().stream()
                        .filter(e -> overloadedBrokerData.getBundles().contains(e.getKey()))
                        .map((e) -> {
                            String bundle = e.getKey();
                            BundleData bundleData = e.getValue();
                            TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                            double throughput = isMsgRateThresholdExceeded
                                    ? shortTermData.getMsgRateIn() + shortTermData.getMsgRateOut()
                                    : shortTermData.getMsgThroughputIn() + shortTermData.getMsgThroughputOut();
                            return Triple.of(bundle, bundleData, throughput);
                        }).filter(e -> !recentlyUnloadedBundles.containsKey(e.getLeft()))
                        .sorted((e1, e2) -> Double.compare(e2.getRight(), e1.getRight())).forEach((e) -> {
                            String bundle = e.getLeft();
                            BundleData bundleData = e.getMiddle();
                            TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                            double throughput = shortTermData.getMsgThroughputIn()
                                    + shortTermData.getMsgThroughputOut();
                            double bundleMsgRate = shortTermData.getMsgRateIn() + shortTermData.getMsgRateOut();
                            if (isMsgRateThresholdExceeded) {
                                if (bundleMsgRate <= (msgRateRequiredFromUnloadedBundles.getValue()
                                        + 1000/* delta */)) {
                                    log.info("Found bundle to unload with msgRate {}", bundleMsgRate);
                                    msgRateRequiredFromUnloadedBundles.add(-bundleMsgRate);
                                    selectedBundlesCache.put(overloadedBroker.getValue(), bundle);
                                }
                            } else {
                                if (throughput <= (msgThroughputRequiredFromUnloadedBundles.getValue())) {
                                    log.info("Found bundle to unload with throughput {}", throughput);
                                    msgThroughputRequiredFromUnloadedBundles.add(-throughput);
                                    selectedBundlesCache.put(overloadedBroker.getValue(), bundle);
                                }
                            }
                        });
            }
        }

        return selectedBundlesCache;
    }
}
