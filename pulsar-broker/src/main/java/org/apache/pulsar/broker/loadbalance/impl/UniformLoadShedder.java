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
import java.util.Map;
import lombok.CustomLog;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

/**
 * This strategy tends to distribute load uniformly across all brokers. This strategy checks load difference between
 * broker with the highest load and broker with the lowest load. If the difference is higher than configured thresholds
 * {@link ServiceConfiguration#getLoadBalancerMsgRateDifferenceShedderThreshold()} or
 * {@link ServiceConfiguration#getLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold()} then it finds out
 * bundles which can be unloaded to distribute traffic evenly across all brokers.
 *
 */
@CustomLog
public class UniformLoadShedder implements LoadSheddingStrategy {
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

        MutableObject<String> msgRateOverloadedBroker = new MutableObject<>();
        MutableObject<String> msgThroughputOverloadedBroker = new MutableObject<>();
        MutableObject<String> msgRateUnderloadedBroker = new MutableObject<>();
        MutableObject<String> msgThroughputUnderloadedBroker = new MutableObject<>();
        MutableDouble maxMsgRate = new MutableDouble(-1);
        MutableDouble maxThroughput = new MutableDouble(-1);
        MutableDouble minMsgRate = new MutableDouble(Integer.MAX_VALUE);
        MutableDouble minThroughput = new MutableDouble(Integer.MAX_VALUE);

        brokersData.forEach((broker, data) -> {
            double msgRate = data.getLocalData().getMsgRateIn() + data.getLocalData().getMsgRateOut();
            double throughputRate = data.getLocalData().getMsgThroughputIn()
                    + data.getLocalData().getMsgThroughputOut();
            if (msgRate > maxMsgRate.doubleValue()) {
                msgRateOverloadedBroker.setValue(broker);
                maxMsgRate.setValue(msgRate);
            }

            if (throughputRate > maxThroughput.doubleValue()) {
                msgThroughputOverloadedBroker.setValue(broker);
                maxThroughput.setValue(throughputRate);
            }

            if (msgRate < minMsgRate.doubleValue()) {
                msgRateUnderloadedBroker.setValue(broker);
                minMsgRate.setValue(msgRate);
            }

            if (throughputRate < minThroughput.doubleValue()) {
                msgThroughputUnderloadedBroker.setValue(broker);
                minThroughput.setValue(throughputRate);
            }
        });

        // find the difference between two brokers based on msgRate and throughout and check if the load distribution
        // discrepancy is higher than threshold. if that matches then try to unload bundle from overloaded brokers to
        // give chance of uniform load distribution.
        if (minMsgRate.doubleValue() <= EPS && minMsgRate.doubleValue() >= -EPS) {
            minMsgRate.setValue(1.0);
        }
        if (minThroughput.doubleValue() <= EPS && minThroughput.doubleValue() >= -EPS) {
            minThroughput.setValue(1.0);
        }
        double msgRateDifferencePercentage = ((maxMsgRate.doubleValue() - minMsgRate.doubleValue()) * 100)
                / (minMsgRate.doubleValue());
        double msgThroughputDifferenceRate = maxThroughput.doubleValue() / minThroughput.doubleValue();

        // if the threshold matches then find out how much load needs to be unloaded by considering number of msgRate
        // and throughput.
        boolean isMsgRateThresholdExceeded = conf.getLoadBalancerMsgRateDifferenceShedderThreshold() > 0
                && msgRateDifferencePercentage > conf.getLoadBalancerMsgRateDifferenceShedderThreshold();
        boolean isMsgThroughputThresholdExceeded = conf
                .getLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold() > 0
                && msgThroughputDifferenceRate > conf
                .getLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold();

        if (isMsgRateThresholdExceeded || isMsgThroughputThresholdExceeded) {
            MutableInt msgRateRequiredFromUnloadedBundles = new MutableInt(
                    (int) ((maxMsgRate.doubleValue() - minMsgRate.doubleValue()) * conf.getMaxUnloadPercentage()));
            MutableInt msgThroughputRequiredFromUnloadedBundles = new MutableInt(
                    (int) ((maxThroughput.doubleValue() - minThroughput.doubleValue())
                            * conf.getMaxUnloadPercentage()));
            if (isMsgRateThresholdExceeded) {
                log.debugf("Found bundles for uniform load balancing. "
                                + "msgRate overloaded broker: %s with msgRate: %s, "
                                + "msgRate underloaded broker: %s with msgRate: %s",
                        msgRateOverloadedBroker.get(), maxMsgRate.doubleValue(),
                        msgRateUnderloadedBroker.get(), minMsgRate.doubleValue());
                LocalBrokerData overloadedBrokerData =
                        brokersData.get(msgRateOverloadedBroker.get()).getLocalData();
                if (overloadedBrokerData.getBundles().size() > 1
                        && (msgRateRequiredFromUnloadedBundles.intValue() >= conf.getMinUnloadMessage())) {
                    // Sort bundles by msgRate, then pick the bundle which can help to reduce load uniformly with
                    // under-loaded broker
                    loadBundleData.entrySet().stream()
                            .filter(e -> overloadedBrokerData.getBundles().contains(e.getKey()))
                            .map((e) -> {
                                String bundle = e.getKey();
                                TimeAverageMessageData shortTermData = e.getValue().getShortTermData();
                                double msgRate = shortTermData.getMsgRateIn() + shortTermData.getMsgRateOut();
                                return Pair.of(bundle, msgRate);
                            }).filter(e -> !recentlyUnloadedBundles.containsKey(e.getLeft()))
                            .sorted((e1, e2) -> Double.compare(e2.getRight(), e1.getRight())).forEach((e) -> {
                                if (conf.getMaxUnloadBundleNumPerShedding() != -1
                                        && selectedBundlesCache.size() >= conf.getMaxUnloadBundleNumPerShedding()) {
                                    return;
                                }
                                String bundle = e.getLeft();
                                double bundleMsgRate = e.getRight();
                                if (bundleMsgRate <= msgRateRequiredFromUnloadedBundles.intValue()) {
                                    log.info().attr("bundle", bundleMsgRate).log("Found bundle to unload with msgRate");
                                    msgRateRequiredFromUnloadedBundles.add(-bundleMsgRate);
                                    selectedBundlesCache.put(msgRateOverloadedBroker.get(), bundle);
                                }
                            });
                }
            } else {
                log.debugf("Found bundles for uniform load balancing. "
                                + "msgThroughput overloaded broker: %s with msgThroughput %s, "
                                + "msgThroughput underloaded broker: %s with msgThroughput: %s",
                        msgThroughputOverloadedBroker.get(), maxThroughput.doubleValue(),
                        msgThroughputUnderloadedBroker.get(), minThroughput.doubleValue());
                LocalBrokerData overloadedBrokerData =
                        brokersData.get(msgThroughputOverloadedBroker.get()).getLocalData();
                if (overloadedBrokerData.getBundles().size() > 1
                        &&
                        msgThroughputRequiredFromUnloadedBundles.intValue() >= conf.getMinUnloadMessageThroughput()) {
                    // Sort bundles by throughput, then pick the bundle which can help to reduce load uniformly with
                    // under-loaded broker
                    loadBundleData.entrySet().stream()
                            .filter(e -> overloadedBrokerData.getBundles().contains(e.getKey()))
                            .map((e) -> {
                                String bundle = e.getKey();
                                TimeAverageMessageData shortTermData = e.getValue().getShortTermData();
                                double msgThroughput = shortTermData.getMsgThroughputIn()
                                        + shortTermData.getMsgThroughputOut();
                                return Pair.of(bundle, msgThroughput);
                            }).filter(e -> !recentlyUnloadedBundles.containsKey(e.getLeft()))
                            .sorted((e1, e2) -> Double.compare(e2.getRight(), e1.getRight())).forEach((e) -> {
                                if (conf.getMaxUnloadBundleNumPerShedding() != -1
                                        && selectedBundlesCache.size() >= conf.getMaxUnloadBundleNumPerShedding()) {
                                    return;
                                }
                                String bundle = e.getLeft();
                                double msgThroughput = e.getRight();
                                if (msgThroughput <= msgThroughputRequiredFromUnloadedBundles.intValue()) {
                                    log.info().attr("msgThroughput", msgThroughput)
                                            .log("Found bundle to unload with msgThroughput");
                                    msgThroughputRequiredFromUnloadedBundles.add(-msgThroughput);
                                    selectedBundlesCache.put(msgThroughputOverloadedBroker.get(), bundle);
                                }
                            });
                }

            }
        }

        return selectedBundlesCache;
    }
}
