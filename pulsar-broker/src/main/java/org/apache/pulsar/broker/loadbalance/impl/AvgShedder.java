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
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import lombok.CustomLog;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

@CustomLog
public class AvgShedder implements LoadSheddingStrategy, ModularLoadManagerStrategy {
    // map bundle to broker.
    private final Map<BundleData, String> bundleBrokerMap = new HashMap<>();
    // map broker to Scores. scores:0-100
    private final Map<String, Double> brokerScoreMap = new HashMap<>();
    // map broker hit count for high threshold/low threshold
    private final Map<String, MutableInt> brokerHitCountForHigh = new HashMap<>();
    private final Map<String, MutableInt> brokerHitCountForLow = new HashMap<>();
    private static final double MB = 1024 * 1024;

    @Override
    public Multimap<String, String> findBundlesForUnloading(LoadData loadData, ServiceConfiguration conf) {
        // result returned by shedding, map broker to bundles.
        Multimap<String, String> selectedBundlesCache = ArrayListMultimap.create();

        // configuration for shedding.
        final double minThroughputThreshold = conf.getMinUnloadMessageThroughput();
        final double minMsgThreshold = conf.getMinUnloadMessage();
        final double maxUnloadPercentage = conf.getMaxUnloadPercentage();
        final double lowThreshold = conf.getLoadBalancerAvgShedderLowThreshold();
        final double highThreshold = conf.getLoadBalancerAvgShedderHighThreshold();
        final int hitCountHighThreshold = conf.getLoadBalancerAvgShedderHitCountHighThreshold();
        final int hitCountLowThreshold = conf.getLoadBalancerAvgShedderHitCountLowThreshold();
        log.debug().attr("highThreshold", highThreshold).attr("lowThreshold", lowThreshold)
                .attr("hitCountHighThreshold", hitCountHighThreshold)
                .attr("hitCountLowThreshold", hitCountLowThreshold).attr("minMsgThreshold", minMsgThreshold)
                .attr("minThroughputThreshold", minThroughputThreshold)
                .log("Shedding configuration");

        List<String> brokers = calculateScoresAndSort(loadData, conf);
        log.info().attr("brokers", brokers).log("sorted broker list");

        // find broker pairs for shedding.
        List<Pair<String, String>> pairs = findBrokerPairs(brokers, lowThreshold, highThreshold);
        log.info().attr("brokerHitCountForHigh", brokerHitCountForHigh)
                .attr("brokerHitCountForLow", brokerHitCountForLow)
                .log("Broker hit counts");
        if (pairs.isEmpty()) {
            log.debug("there is no any overload broker, no need to shedding bundles.");
            brokerHitCountForHigh.clear();
            brokerHitCountForLow.clear();
            return selectedBundlesCache;
        }

        // choosing bundles to unload.
        for (Pair<String, String> pair : pairs) {
            String overloadedBroker = pair.getRight();
            String underloadedBroker = pair.getLeft();

            // check hit count for high threshold and low threshold.
            if (!(brokerHitCountForHigh.computeIfAbsent(underloadedBroker, __ -> new MutableInt(0))
                    .intValue() >= hitCountHighThreshold)
                    && !(brokerHitCountForHigh.computeIfAbsent(overloadedBroker, __ -> new MutableInt(0))
                    .intValue() >= hitCountHighThreshold)
                    && !(brokerHitCountForLow.computeIfAbsent(underloadedBroker, __ -> new MutableInt(0))
                    .intValue() >= hitCountLowThreshold)
                    && !(brokerHitCountForLow.computeIfAbsent(overloadedBroker, __ -> new MutableInt(0))
                    .intValue() >= hitCountLowThreshold)) {
                continue;
            }

            // if hit, remove entry.
            brokerHitCountForHigh.remove(underloadedBroker);
            brokerHitCountForHigh.remove(overloadedBroker);
            brokerHitCountForLow.remove(underloadedBroker);
            brokerHitCountForLow.remove(overloadedBroker);

            // select bundle for unloading.
            selectBundleForUnloading(loadData, overloadedBroker, underloadedBroker, minThroughputThreshold,
                    minMsgThreshold, maxUnloadPercentage, selectedBundlesCache);
        }
        return selectedBundlesCache;
    }

    private void selectBundleForUnloading(LoadData loadData, String overloadedBroker, String underloadedBroker,
                                          double minThroughputThreshold, double minMsgThreshold,
                                          double maxUnloadPercentage, Multimap<String, String> selectedBundlesCache) {
        // calculate how much throughput to unload.
        LocalBrokerData minLocalBrokerData = loadData.getBrokerData().get(underloadedBroker).getLocalData();
        LocalBrokerData maxLocalBrokerData = loadData.getBrokerData().get(overloadedBroker).getLocalData();

        double minMsgRate = minLocalBrokerData.getMsgRateIn() + minLocalBrokerData.getMsgRateOut();
        double maxMsgRate = maxLocalBrokerData.getMsgRateIn() + maxLocalBrokerData.getMsgRateOut();

        double minThroughput = minLocalBrokerData.getMsgThroughputIn() + minLocalBrokerData.getMsgThroughputOut();
        double maxThroughput = maxLocalBrokerData.getMsgThroughputIn() + maxLocalBrokerData.getMsgThroughputOut();

        double msgRequiredFromUnloadedBundles = (maxMsgRate - minMsgRate) * maxUnloadPercentage;
        double throughputRequiredFromUnloadedBundles = (maxThroughput - minThroughput) * maxUnloadPercentage;

        boolean isMsgRateToOffload;
        MutableDouble trafficMarkedToOffload = new MutableDouble(0);

        if (msgRequiredFromUnloadedBundles > minMsgThreshold) {
            isMsgRateToOffload = true;
            trafficMarkedToOffload.setValue(msgRequiredFromUnloadedBundles);
        } else if (throughputRequiredFromUnloadedBundles > minThroughputThreshold) {
            isMsgRateToOffload = false;
            trafficMarkedToOffload.setValue(throughputRequiredFromUnloadedBundles);
        } else {
            log.infof(
                    "broker:[%s] is planning to shed bundles to broker:[%s],but the throughput %s MByte/s is "
                            + "less than minimumThroughputThreshold %s MByte/s, and the msgRate %s rate/s"
                            + " is also less than minimumMsgRateThreshold %s rate/s, skipping bundle unload.",
                    overloadedBroker, underloadedBroker, throughputRequiredFromUnloadedBundles / MB,
                    minThroughputThreshold / MB, msgRequiredFromUnloadedBundles, minMsgThreshold);
            return;
        }

        if (maxLocalBrokerData.getBundles().size() == 1) {
            log.warn().attr("bundle", maxLocalBrokerData.getBundles().iterator().next())
                    .attr("broker", overloadedBroker)
                    .log("HIGH USAGE WARNING: Sole namespace bundle is overloading broker. "
                            + "No Load Shedding will be done on this broker");
        } else if (maxLocalBrokerData.getBundles().isEmpty()) {
            log.warn().attr("broker", overloadedBroker).log("Broker is overloaded despite having no bundles");
        }

        // do shedding
        log.infof(
                "broker:[%s] is planning to shed bundles to broker:[%s]. "
                        + "maxBroker stat:scores:%s, throughput:%s, msgRate:%s. "
                        + "minBroker stat:scores:%s, throughput:%s, msgRate:%s. "
                        + "isMsgRateToOffload:%s,  trafficMarkedToOffload:%s",
                overloadedBroker, underloadedBroker, brokerScoreMap.get(overloadedBroker), maxThroughput,
                maxMsgRate, brokerScoreMap.get(underloadedBroker), minThroughput, minMsgRate,
                isMsgRateToOffload, trafficMarkedToOffload);

        loadData.getBundleDataForLoadShedding().entrySet().stream().filter(e ->
                maxLocalBrokerData.getBundles().contains(e.getKey())
        ).filter(e ->
                !loadData.getRecentlyUnloadedBundles().containsKey(e.getKey())
        ).map((e) -> {
            BundleData bundleData = e.getValue();
            TimeAverageMessageData shortTermData = bundleData.getShortTermData();
            double traffic = isMsgRateToOffload
                    ? shortTermData.getMsgRateIn() + shortTermData.getMsgRateOut()
                    : shortTermData.getMsgThroughputIn() + shortTermData.getMsgThroughputOut();
            return Pair.of(e, traffic);
        }).sorted((e1, e2) ->
                Double.compare(e2.getRight(), e1.getRight())
        ).forEach(e -> {
            Map.Entry<String, BundleData> bundle = e.getLeft();
            double traffic = e.getRight();
            if (traffic > 0 && traffic <= trafficMarkedToOffload.doubleValue()) {
                selectedBundlesCache.put(overloadedBroker, bundle.getKey());
                bundleBrokerMap.put(bundle.getValue(), underloadedBroker);
                trafficMarkedToOffload.add(-traffic);
                log.debug().attr("bundle", bundle).attr("isMsgRateToOffload", isMsgRateToOffload)
                        .attr("traffic", traffic)
                        .log("Found bundle to unload");
            }
        });
    }

    @Override
    public void onActiveBrokersChange(Set<String> activeBrokers) {
        LoadSheddingStrategy.super.onActiveBrokersChange(activeBrokers);
    }

    private List<String> calculateScoresAndSort(LoadData loadData, ServiceConfiguration conf) {
        brokerScoreMap.clear();

        // calculate scores of brokers.
        for (Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            LocalBrokerData localBrokerData = entry.getValue().getLocalData();
            String broker = entry.getKey();
            Double score = calculateScores(localBrokerData, conf);
            brokerScoreMap.put(broker, score);
            log.info().attr("broker", broker).attr("score", score)
                    .attr("throughput", localBrokerData.getMsgThroughputIn() + localBrokerData.getMsgThroughputOut())
                    .attr("messageRate", localBrokerData.getMsgRateIn() + localBrokerData.getMsgRateOut())
                    .log("Broker score");
        }

        // sort brokers by scores.
        return brokerScoreMap.entrySet().stream().sorted(Comparator.comparingDouble(Map.Entry::getValue))
                .map(Map.Entry::getKey).toList();
    }

    private Double calculateScores(LocalBrokerData localBrokerData, final ServiceConfiguration conf) {
        return localBrokerData.getMaxResourceUsageWithWeight(
                conf.getLoadBalancerCPUResourceWeight(),
                conf.getLoadBalancerDirectMemoryResourceWeight(),
                conf.getLoadBalancerBandwidthInResourceWeight(),
                conf.getLoadBalancerBandwidthOutResourceWeight()) * 100;
    }

    private List<Pair<String, String>> findBrokerPairs(List<String> brokers,
                                                       double lowThreshold, double highThreshold) {
        List<Pair<String, String>> pairs = new LinkedList<>();
        int i = 0, j = brokers.size() - 1;
        while (i <= j) {
            String maxBroker = brokers.get(j);
            String minBroker = brokers.get(i);
            if (brokerScoreMap.get(maxBroker) - brokerScoreMap.get(minBroker) < lowThreshold) {
                brokerHitCountForHigh.remove(maxBroker);
                brokerHitCountForHigh.remove(minBroker);

                brokerHitCountForLow.remove(maxBroker);
                brokerHitCountForLow.remove(minBroker);
            } else {
                pairs.add(Pair.of(minBroker, maxBroker));
                if (brokerScoreMap.get(maxBroker) - brokerScoreMap.get(minBroker) < highThreshold) {
                    brokerHitCountForLow.computeIfAbsent(minBroker, k -> new MutableInt(0)).increment();
                    brokerHitCountForLow.computeIfAbsent(maxBroker, k -> new MutableInt(0)).increment();

                    brokerHitCountForHigh.remove(maxBroker);
                    brokerHitCountForHigh.remove(minBroker);
                } else {
                    brokerHitCountForLow.computeIfAbsent(minBroker, k -> new MutableInt(0)).increment();
                    brokerHitCountForLow.computeIfAbsent(maxBroker, k -> new MutableInt(0)).increment();

                    brokerHitCountForHigh.computeIfAbsent(minBroker, k -> new MutableInt(0)).increment();
                    brokerHitCountForHigh.computeIfAbsent(maxBroker, k -> new MutableInt(0)).increment();
                }
            }
            i++;
            j--;
        }
        return pairs;
    }

    @Override
    public Optional<String> selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData,
                                         ServiceConfiguration conf) {
        final var brokerToUnload = bundleBrokerMap.getOrDefault(bundleToAssign, null);
        if (brokerToUnload == null || !candidates.contains(bundleBrokerMap.get(bundleToAssign))) {
            // cluster initializing or broker is shutdown
            if (!bundleBrokerMap.containsKey(bundleToAssign)) {
                log.debug("cluster is initializing");
            } else {
                log.debug().attr("broker", bundleBrokerMap.get(bundleToAssign)).attr("candidates", candidates)
                        .log("expected broker is shutdown");
            }
            String broker = getExpectedBroker(candidates, bundleToAssign);
            bundleBrokerMap.put(bundleToAssign, broker);
            return Optional.of(broker);
        } else {
            return Optional.of(brokerToUnload);
        }
    }

    private static String getExpectedBroker(Collection<String> brokers, BundleData bundle) {
        List<String> sortedBrokers = new ArrayList<>(brokers);
        Collections.sort(sortedBrokers);

        try {
            // use random number as input of hashing function to avoid special case that,
            // if there is 4 brokers running in the cluster,and add broker5,and shutdown broker3,
            // then all bundles belonging to broker3 will be loaded on the same broker.
            final long hashcode = Hashing.crc32().hashString(String.valueOf(new Random().nextInt()),
                    StandardCharsets.UTF_8).padToLong();
            final int index = (int) (Math.abs(hashcode) % sortedBrokers.size());
            log.debug().attr("brokers", sortedBrokers).attr("bundle", bundle).attr("hashCode", hashcode)
                    .attr("index", index)
                    .log("Assignment details");
            return sortedBrokers.get(index);
        } catch (Throwable e) {
            // theoretically this logic branch should not be executed
            log.error().attr("bundle", bundle).exception(e).log("Bundle format is invalid");
            return sortedBrokers.get(Math.abs(bundle.hashCode()) % sortedBrokers.size());
        }
    }
}
