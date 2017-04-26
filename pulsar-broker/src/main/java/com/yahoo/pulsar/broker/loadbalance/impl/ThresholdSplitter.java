/*
 * Copyright 2016 Yahoo Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.yahoo.pulsar.broker.loadbalance.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.MessageData;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.BundleSplitStrategy;
import com.yahoo.pulsar.broker.loadbalance.LoadData;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Determines which bundles should be split based on various thresholds.
 */
public class ThresholdSplitter implements BundleSplitStrategy {
    private static final Logger log = LoggerFactory.getLogger(BundleSplitStrategy.class);
    private final Set<String> bundleCache;

    /**
     * Construct a ThresholdSplitter.
     * 
     * @param pulsar
     *            Service to construct from.
     */
    public ThresholdSplitter(final PulsarService pulsar) {
        bundleCache = new HashSet<>();
    }

    /**
     * Determines which bundles should be split based on various thresholds.
     * 
     * @param loadData
     *            Load data to base decisions on (does not have benefit of preallocated data since this may not be the
     *            leader broker).
     * @param localData
     *            Local data for the broker we are splitting on.
     * @param pulsar
     *            Service to use.
     * @return All bundles who have exceeded configured thresholds in number of topics, number of sessions, total
     *         message rates, or total throughput.
     */
    @Override
    public Set<String> findBundlesToSplit(final LoadData loadData, final LocalBrokerData localData,
            final PulsarService pulsar) {
        bundleCache.clear();
        final ServiceConfiguration conf = pulsar.getConfiguration();
        int maxBundleCount = conf.getLoadBalancerNamespaceMaximumBundles();
        long maxBundleTopics = conf.getLoadBalancerNamespaceBundleMaxTopics();
        long maxBundleSessions = conf.getLoadBalancerNamespaceBundleMaxSessions();
        long maxBundleMsgRate = conf.getLoadBalancerNamespaceBundleMaxMsgRate();
        long maxBundleBandwidth = conf.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * LoadManagerShared.MIBI;
        for (final Map.Entry<String, NamespaceBundleStats> entry : localData.getLastStats().entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            double totalMessageRate = 0;
            double totalMessageThroughput = 0;
            // Attempt to consider long-term message data, otherwise effectively ignore.
            if (loadData.getBundleData().containsKey(bundle)) {
                final MessageData longTermData = loadData.getBundleData().get(bundle).getLongTermData()
                        .getMessageData();
                totalMessageRate = longTermData.totalMsgRate();
                totalMessageThroughput = longTermData.totalMsgThroughput();
            }
            if (stats.topics > maxBundleTopics || stats.consumerCount + stats.producerCount > maxBundleSessions
                    || totalMessageRate > maxBundleMsgRate || totalMessageThroughput > maxBundleBandwidth) {
                final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                try {
                    final int bundleCount = pulsar.getNamespaceService().getBundleCount(new NamespaceName(namespace));
                    if (bundleCount < maxBundleCount) {
                        bundleCache.add(bundle);
                    } else {
                        log.warn("Could not split namespace bundle {} because namespace {} has too many bundles: {}",
                                bundle, namespace, bundleCount);
                    }
                } catch (Exception e) {
                    log.warn("Error while getting bundle count for namespace {}", namespace, e);
                }
            }
        }
        return bundleCache;
    }
}
