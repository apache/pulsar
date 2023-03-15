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

import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Bandwidth;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.MsgRate;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Sessions;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Topics;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Determines which bundles should be split based on various thresholds.
 *
 * Migrate from {@link org.apache.pulsar.broker.loadbalance.impl.BundleSplitterTask}
 */
@Slf4j
public class DefaultNamespaceBundleSplitStrategyImpl implements NamespaceBundleSplitStrategy {
    private final Set<SplitDecision> decisionCache;
    private final Map<String, Integer> namespaceBundleCount;
    private final Map<String, Integer> bundleHighTrafficFrequency;
    private final SplitCounter counter;

    public DefaultNamespaceBundleSplitStrategyImpl(SplitCounter counter) {
        decisionCache = new HashSet<>();
        namespaceBundleCount = new HashMap<>();
        bundleHighTrafficFrequency = new HashMap<>();
        this.counter = counter;

    }

    @Override
    public Set<SplitDecision> findBundlesToSplit(LoadManagerContext context, PulsarService pulsar) {
        decisionCache.clear();
        namespaceBundleCount.clear();
        final ServiceConfiguration conf = pulsar.getConfiguration();
        int maxBundleCount = conf.getLoadBalancerNamespaceMaximumBundles();
        long maxBundleTopics = conf.getLoadBalancerNamespaceBundleMaxTopics();
        long maxBundleSessions = conf.getLoadBalancerNamespaceBundleMaxSessions();
        long maxBundleMsgRate = conf.getLoadBalancerNamespaceBundleMaxMsgRate();
        long maxBundleBandwidth = conf.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * LoadManagerShared.MIBI;
        long maxSplitCount = conf.getLoadBalancerMaxNumberOfBundlesToSplitPerCycle();
        long splitConditionThreshold = conf.getLoadBalancerNamespaceBundleSplitConditionThreshold();
        boolean debug = log.isDebugEnabled() || conf.isLoadBalancerDebugModeEnabled();

        Map<String, NamespaceBundleStats> bundleStatsMap = pulsar.getBrokerService().getBundleStats();
        NamespaceBundleFactory namespaceBundleFactory =
                pulsar.getNamespaceService().getNamespaceBundleFactory();

        // clean bundleHighTrafficFrequency
        bundleHighTrafficFrequency.keySet().retainAll(bundleStatsMap.keySet());

        for (var entry : bundleStatsMap.entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            if (stats.topics < 2) {
                if (debug) {
                    log.info("The count of topics on the bundle {} is less than 2, skip split!", bundle);
                }
                continue;
            }

            final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
            final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
            if (!namespaceBundleFactory
                    .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                if (debug) {
                    log.info("Can't split the bundle:{}. invalid bundle range:{}. ", bundle, bundleRange);
                }
                counter.update(Failure, Unknown);
                continue;
            }

            double totalMessageRate = stats.msgRateIn + stats.msgRateOut;
            double totalMessageThroughput = stats.msgThroughputIn + stats.msgThroughputOut;
            int totalSessionCount = stats.consumerCount + stats.producerCount;
            SplitDecision.Reason reason = Unknown;
            if (stats.topics > maxBundleTopics) {
                reason = Topics;
            } else if (maxBundleSessions > 0 && (totalSessionCount > maxBundleSessions)) {
                reason = Sessions;
            } else if (totalMessageRate > maxBundleMsgRate) {
                reason = MsgRate;
            } else if (totalMessageThroughput > maxBundleBandwidth) {
                reason = Bandwidth;
            }

            if (reason != Unknown) {
                bundleHighTrafficFrequency.put(bundle, bundleHighTrafficFrequency.getOrDefault(bundle, 0) + 1);
            } else {
                bundleHighTrafficFrequency.remove(bundle);
            }

            if (bundleHighTrafficFrequency.getOrDefault(bundle, 0) > splitConditionThreshold) {
                final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                try {
                    final int bundleCount = pulsar.getNamespaceService()
                            .getBundleCount(NamespaceName.get(namespace));
                    if ((bundleCount + namespaceBundleCount.getOrDefault(namespace, 0))
                            < maxBundleCount) {
                        if (debug) {
                            log.info("The bundle {} is considered to split. Topics: {}/{}, Sessions: ({}+{})/{}, "
                                            + "Message Rate: {}/{} (msgs/s), Message Throughput: {}/{} (MB/s)",
                                    bundle, stats.topics, maxBundleTopics, stats.producerCount, stats.consumerCount,
                                    maxBundleSessions, totalMessageRate, maxBundleMsgRate,
                                    totalMessageThroughput / LoadManagerShared.MIBI,
                                    maxBundleBandwidth / LoadManagerShared.MIBI);
                        }
                        var decision = new SplitDecision();
                        decision.setSplit(new Split(bundle, context.brokerRegistry().getBrokerId(), new HashMap<>()));
                        decision.succeed(reason);
                        decisionCache.add(decision);
                        int bundleNum = namespaceBundleCount.getOrDefault(namespace, 0);
                        namespaceBundleCount.put(namespace, bundleNum + 1);
                        bundleHighTrafficFrequency.remove(bundle);
                        // Clear namespace bundle-cache
                        namespaceBundleFactory.invalidateBundleCache(NamespaceName.get(namespaceName));
                        if (decisionCache.size() == maxSplitCount) {
                            if (debug) {
                                log.info("Too many bundles to split in this split cycle {} / {}. Stop.",
                                        decisionCache.size(), maxSplitCount);
                            }
                            break;
                        }
                    } else {
                        if (debug) {
                            log.info(
                                    "Could not split namespace bundle {} because namespace {} has too many bundles:"
                                            + "{}", bundle, namespace, bundleCount);
                        }
                    }
                } catch (Exception e) {
                    counter.update(Failure, Unknown);
                    log.warn("Error while computing bundle splits for namespace {}", namespace, e);
                }
            }
        }
        return decisionCache;
    }
}
