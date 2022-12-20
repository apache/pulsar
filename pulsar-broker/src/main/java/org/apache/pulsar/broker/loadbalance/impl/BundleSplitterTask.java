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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.BundleSplitStrategy;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines which bundles should be split based on various thresholds.
 */
public class BundleSplitterTask implements BundleSplitStrategy {
    private static final Logger log = LoggerFactory.getLogger(BundleSplitStrategy.class);
    private final Set<String> bundleCache;

    private final Map<String, Integer> namespaceBundleCount;


    /**
     * Construct a BundleSplitterTask.
     *
     */
    public BundleSplitterTask() {
        bundleCache = new HashSet<>();
        namespaceBundleCount = new HashMap<>();
    }

    /**
     * Determines which bundles should be split based on various thresholds.
     *
     * @param loadData
     *            Load data to base decisions on (does not have benefit of preallocated data since this may not be the
     *            leader broker).
     * @param pulsar
     *            Service to use.
     * @return All bundles who have exceeded configured thresholds in number of topics, number of sessions, total
     *         message rates, or total throughput.
     */
    @Override
    public Set<String> findBundlesToSplit(final LoadData loadData, final PulsarService pulsar) {
        bundleCache.clear();
        namespaceBundleCount.clear();
        final ServiceConfiguration conf = pulsar.getConfiguration();
        int maxBundleCount = conf.getLoadBalancerNamespaceMaximumBundles();
        long maxBundleTopics = conf.getLoadBalancerNamespaceBundleMaxTopics();
        long maxBundleSessions = conf.getLoadBalancerNamespaceBundleMaxSessions();
        long maxBundleMsgRate = conf.getLoadBalancerNamespaceBundleMaxMsgRate();
        long maxBundleBandwidth = conf.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * LoadManagerShared.MIBI;

        loadData.getBrokerData().forEach((broker, brokerData) -> {
            LocalBrokerData localData = brokerData.getLocalData();
            for (final Map.Entry<String, NamespaceBundleStats> entry : localData.getLastStats().entrySet()) {
                final String bundle = entry.getKey();
                final NamespaceBundleStats stats = entry.getValue();
                if (stats.topics < 2) {
                    log.info("The count of topics on the bundle {} is less than 2，skip split!", bundle);
                    continue;
                }
                double totalMessageRate = 0;
                double totalMessageThroughput = 0;
                // Attempt to consider long-term message data, otherwise effectively ignore.
                if (loadData.getBundleData().containsKey(bundle)) {
                    final TimeAverageMessageData longTermData = loadData.getBundleData().get(bundle).getLongTermData();
                    totalMessageRate = longTermData.totalMsgRate();
                    totalMessageThroughput = longTermData.totalMsgThroughput();
                }
                if (stats.topics > maxBundleTopics || (maxBundleSessions > 0 && (stats.consumerCount
                        + stats.producerCount > maxBundleSessions))
                        || totalMessageRate > maxBundleMsgRate || totalMessageThroughput > maxBundleBandwidth) {
                    final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                    try {
                        final int bundleCount = pulsar.getNamespaceService()
                                .getBundleCount(NamespaceName.get(namespace));
                        if ((bundleCount + namespaceBundleCount.getOrDefault(namespace, 0))
                                < maxBundleCount) {
                            log.info("The bundle {} is considered to be unload. Topics: {}/{}, Sessions: ({}+{})/{}, "
                                            + "Message Rate: {}/{} (msgs/s), Message Throughput: {}/{} (MB/s)",
                                    bundle, stats.topics, maxBundleTopics, stats.producerCount, stats.consumerCount,
                                    maxBundleSessions, totalMessageRate, maxBundleMsgRate,
                                    totalMessageThroughput / LoadManagerShared.MIBI,
                                    maxBundleBandwidth / LoadManagerShared.MIBI);
                            bundleCache.add(bundle);
                            int bundleNum = namespaceBundleCount.getOrDefault(namespace, 0);
                            namespaceBundleCount.put(namespace, bundleNum + 1);
                        } else {
                            log.warn(
                                    "Could not split namespace bundle {} because namespace {} has too many bundles: {}",
                                    bundle, namespace, bundleCount);
                        }
                    } catch (Exception e) {
                        log.warn("Error while getting bundle count for namespace {}", namespace, e);
                    }
                }
            }
        });
        return bundleCache;
    }
}
