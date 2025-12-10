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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Determines which bundles should be split based on various thresholds.
 *
 * Migrate from {@link org.apache.pulsar.broker.loadbalance.impl.BundleSplitterTask}
 */
@Slf4j
public class DefaultNamespaceBundleSplitStrategyImpl implements NamespaceBundleSplitStrategy {
    private static final String CANNOT_CONTINUE_SPLIT_MSG = "Can't continue the split cycle.";
    private static final String CANNOT_SPLIT_BUNDLE_MSG = "Can't split broker:%s.";
    private final Set<SplitDecision> decisionCache;
    private final Map<String, Integer> namespaceBundleCount;
    private final Map<String, Integer> splitConditionHitCounts;
    private final Map<String, String> splittingBundles;
    private final SplitCounter counter;

    public DefaultNamespaceBundleSplitStrategyImpl(SplitCounter counter) {
        decisionCache = new HashSet<>();
        namespaceBundleCount = new HashMap<>();
        splitConditionHitCounts = new HashMap<>();
        splittingBundles = new HashMap<>();
        this.counter = counter;

    }

    @Override
    public Set<SplitDecision> findBundlesToSplit(LoadManagerContext context, PulsarService pulsar) {
        decisionCache.clear();
        namespaceBundleCount.clear();
        splittingBundles.clear();
        final ServiceConfiguration conf = pulsar.getConfiguration();
        int maxBundleCount = conf.getLoadBalancerNamespaceMaximumBundles();
        long maxBundleTopics = conf.getLoadBalancerNamespaceBundleMaxTopics();
        long maxBundleSessions = conf.getLoadBalancerNamespaceBundleMaxSessions();
        long maxBundleMsgRate = conf.getLoadBalancerNamespaceBundleMaxMsgRate();
        long maxBundleBandwidth = conf.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * LoadManagerShared.MIBI;
        long maxSplitCount = conf.getLoadBalancerMaxNumberOfBundlesToSplitPerCycle();
        long splitConditionHitCountThreshold = conf.getLoadBalancerNamespaceBundleSplitConditionHitCountThreshold();
        boolean debug = log.isDebugEnabled() || conf.isLoadBalancerDebugModeEnabled();
        var channel = ServiceUnitStateChannelImpl.get(pulsar);

        for (var etr : channel.getOwnershipEntrySet()) {
            var eData = etr.getValue();
            if (eData.state() == ServiceUnitState.Splitting) {
                String bundle = etr.getKey();
                final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
                splittingBundles.put(bundle, bundleRange);
            }
        }

        Map<String, NamespaceBundleStats> bundleStatsMap = pulsar.getBrokerService().getBundleStats();
        NamespaceBundleFactory namespaceBundleFactory =
                pulsar.getNamespaceService().getNamespaceBundleFactory();

        // clean splitConditionHitCounts
        splitConditionHitCounts.keySet().retainAll(bundleStatsMap.keySet());

        for (var entry : bundleStatsMap.entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            if (stats.topics < 2) {
                if (debug) {
                    log.info(String.format(CANNOT_SPLIT_BUNDLE_MSG
                            + " The topic count is less than 2.", bundle));
                }
                continue;
            }

            if (!channel.isOwner(bundle)) {
                if (debug) {
                    log.warn(String.format(CANNOT_SPLIT_BUNDLE_MSG
                            + " This broker is not the owner.", bundle));
                }
                continue;
            }

            final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
            final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
            if (!namespaceBundleFactory
                    .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                if (debug) {
                    log.info(String.format(CANNOT_SPLIT_BUNDLE_MSG
                            + " Invalid bundle range:%s.", bundle, bundleRange));
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
                splitConditionHitCounts.put(bundle, splitConditionHitCounts.getOrDefault(bundle, 0) + 1);
            } else {
                splitConditionHitCounts.remove(bundle);
            }

            if (splitConditionHitCounts.getOrDefault(bundle, 0) <= splitConditionHitCountThreshold) {
                if (debug) {
                    log.info(String.format(
                            CANNOT_SPLIT_BUNDLE_MSG
                                    + " Split condition hit count: %d is"
                                    + " less than or equal to threshold: %d. "
                                    + "Topics: %d/%d, "
                                    + "Sessions: (%d+%d)/%d, "
                                    + "Message Rate: %.2f/%d (msgs/s), "
                                    + "Message Throughput: %.2f/%d (MB/s).",
                            bundle,
                            splitConditionHitCounts.getOrDefault(bundle, 0),
                            splitConditionHitCountThreshold,
                            stats.topics, maxBundleTopics,
                            stats.producerCount, stats.consumerCount, maxBundleSessions,
                            totalMessageRate, maxBundleMsgRate,
                            totalMessageThroughput / LoadManagerShared.MIBI,
                            maxBundleBandwidth / LoadManagerShared.MIBI
                    ));
                }
                continue;
            }

            final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
            try {
                final int bundleCount = pulsar.getNamespaceService()
                        .getBundleCount(NamespaceName.get(namespace));
                if ((bundleCount + namespaceBundleCount.getOrDefault(namespace, 0))
                        >= maxBundleCount) {
                    if (debug) {
                        log.info(String.format(CANNOT_SPLIT_BUNDLE_MSG + " Namespace:%s has too many bundles:%d",
                                bundle, namespace, bundleCount));
                    }
                    continue;
                }
            } catch (Exception e) {
                counter.update(Failure, Unknown);
                log.warn("Failed to get bundle count in namespace:{}", namespace, e);
                continue;
            }

            var ranges = bundleRange.split("_");
            var foundSplittingBundle = false;
            for (var etr : splittingBundles.entrySet()) {
                var splittingBundle = etr.getKey();
                if (splittingBundle.startsWith(namespace)) {
                    var splittingBundleRange = etr.getValue();
                    if (splittingBundleRange.startsWith(ranges[0])
                            || splittingBundleRange.endsWith(ranges[1])) {
                        if (debug) {
                            log.info(String.format(CANNOT_SPLIT_BUNDLE_MSG
                                    + " (parent) bundle:%s is in Splitting state.", bundle, splittingBundle));
                        }
                        foundSplittingBundle = true;
                        break;
                    }
                }
            }
            if (foundSplittingBundle) {
                continue;
            }

            if (debug) {
                log.info(String.format(
                        "Splitting bundle: %s. "
                                + "Topics: %d/%d, "
                                + "Sessions: (%d+%d)/%d, "
                                + "Message Rate: %.2f/%d (msgs/s), "
                                + "Message Throughput: %.2f/%d (MB/s)",
                        bundle,
                        stats.topics, maxBundleTopics,
                        stats.producerCount, stats.consumerCount, maxBundleSessions,
                        totalMessageRate, maxBundleMsgRate,
                        totalMessageThroughput / LoadManagerShared.MIBI,
                        maxBundleBandwidth / LoadManagerShared.MIBI
                ));
            }
            var decision = new SplitDecision();
            var namespaceService = pulsar.getNamespaceService();
            var namespaceBundle = namespaceService.getNamespaceBundleFactory()
                    .getBundle(namespaceName, bundleRange);
            NamespaceBundleSplitAlgorithm algorithm =
                    namespaceService.getNamespaceBundleSplitAlgorithmByName(
                            conf.getDefaultNamespaceBundleSplitAlgorithm());
            List<Long> splitBoundary = null;
            try {
                splitBoundary = namespaceService
                        .getSplitBoundary(namespaceBundle,  null, algorithm)
                        .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            } catch (Throwable e) {
                counter.update(Failure, Unknown);
                log.warn(String.format(CANNOT_SPLIT_BUNDLE_MSG + " Failed to get split boundaries.", bundle), e);
                continue;
            }
            if (splitBoundary == null) {
                counter.update(Failure, Unknown);
                log.warn(String.format(CANNOT_SPLIT_BUNDLE_MSG + " The split boundaries is null.", bundle));
                continue;
            }
            if (splitBoundary.size() != 1) {
                counter.update(Failure, Unknown);
                log.warn(String.format(CANNOT_SPLIT_BUNDLE_MSG + " The size of split boundaries is not 1. "
                        + "splitBoundary:%s", bundle, splitBoundary));
                continue;
            }

            var parentRange = namespaceBundle.getKeyRange();
            var leftChildBundle = namespaceBundleFactory.getBundle(namespaceBundle.getNamespaceObject(),
                    NamespaceBundleFactory.getRange(parentRange.lowerEndpoint(), splitBoundary.get(0)));
            var rightChildBundle = namespaceBundleFactory.getBundle(namespaceBundle.getNamespaceObject(),
                    NamespaceBundleFactory.getRange(splitBoundary.get(0), parentRange.upperEndpoint()));
            Map<String, Optional<String>> splitServiceUnitToDestBroker = Map.of(
                    leftChildBundle.getBundleRange(), Optional.empty(),
                    rightChildBundle.getBundleRange(), Optional.empty());
            decision.setSplit(new Split(bundle, context.brokerRegistry().getBrokerId(), splitServiceUnitToDestBroker));
            decision.succeed(reason);
            decisionCache.add(decision);
            int bundleNum = namespaceBundleCount.getOrDefault(namespace, 0);
            namespaceBundleCount.put(namespace, bundleNum + 1);
            splitConditionHitCounts.remove(bundle);
            // Clear namespace bundle-cache
            namespaceBundleFactory.invalidateBundleCache(NamespaceName.get(namespaceName));
            if (decisionCache.size() == maxSplitCount) {
                if (debug) {
                    log.info(CANNOT_CONTINUE_SPLIT_MSG
                                    + "Too many bundles split in this cycle {} / {}.",
                            decisionCache.size(), maxSplitCount);
                }
                break;
            }

        }
        return decisionCache;
    }
}
