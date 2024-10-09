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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains code which in shared between the two load manager implementations.
 */
public class LoadManagerShared {
    public static final Logger LOG = LoggerFactory.getLogger(LoadManagerShared.class);

    // Value of prefix "mibi" (e.g., number of bytes in a mibibyte).
    public static final int MIBI = 1024 * 1024;

    // Cache for primary brokers according to policies.
    private static final FastThreadLocal<Set<String>> localPrimariesCache = new FastThreadLocal<Set<String>>() {
        @Override
        protected Set<String> initialValue() throws Exception {
            return new HashSet<>();
        }
    };

    // Cache for shard brokers according to policies.
    private static final FastThreadLocal<Set<String>> localSecondaryCache = new FastThreadLocal<Set<String>>() {
        @Override
        protected Set<String> initialValue() throws Exception {
            return new HashSet<>();
        }
    };

    private static final String DEFAULT_DOMAIN = "default";

    // Don't allow construction: static method namespace only.
    private LoadManagerShared() {
    }

    // Determines the brokers available for the given service unit according to the given policies.
    // The brokers are put into brokerCandidateCache.
    public static void applyNamespacePolicies(final ServiceUnitId serviceUnit,
            final SimpleResourceAllocationPolicies policies, final Set<String> brokerCandidateCache,
            final Set<String> availableBrokers, final BrokerTopicLoadingPredicate brokerTopicLoadingPredicate) {
        Set<String> primariesCache = localPrimariesCache.get();
        primariesCache.clear();

        Set<String> secondaryCache = localSecondaryCache.get();
        secondaryCache.clear();

        NamespaceName namespace = serviceUnit.getNamespaceObject();
        boolean isIsolationPoliciesPresent = policies.areIsolationPoliciesPresent(namespace);
        boolean isNonPersistentTopic = (serviceUnit instanceof NamespaceBundle)
                ? ((NamespaceBundle) serviceUnit).hasNonPersistentTopic() : false;
        if (isIsolationPoliciesPresent) {
            LOG.debug("Isolation Policies Present for namespace - [{}]", namespace.toString());
        }
        for (final String brokerId : availableBrokers) {
            String brokerHost;
            try {
                brokerHost = parseBrokerHost(brokerId);
            } catch (IllegalArgumentException e) {
                LOG.error("Unable to parse host from {}", brokerId, e);
                continue;
            }
            // todo: in future check if the resource unit has resources to take the namespace
            if (isIsolationPoliciesPresent) {
                // note: serviceUnitID is namespace name and ResourceID is brokerName
                if (policies.isPrimaryBroker(namespace, brokerHost)) {
                    primariesCache.add(brokerId);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added Primary Broker - [{}] as possible Candidates for"
                                + " namespace - [{}] with policies", brokerHost, namespace.toString());
                    }
                } else if (policies.isSecondaryBroker(namespace, brokerHost)) {
                    secondaryCache.add(brokerId);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Added Shared Broker - [{}] as possible "
                                        + "Candidates for namespace - [{}] with policies",
                                brokerHost, namespace.toString());
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping Broker - [{}] not primary broker and not shared" + " for namespace - [{}] ",
                                brokerHost, namespace.toString());
                    }

                }
            } else {
                // non-persistent topic can be assigned to only those brokers that enabled for non-persistent topic
                if (isNonPersistentTopic && !brokerTopicLoadingPredicate.isEnableNonPersistentTopics(brokerId)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Filter broker- [{}] because it doesn't support non-persistent namespace - [{}]",
                                brokerHost, namespace.toString());
                    }
                } else if (!isNonPersistentTopic && !brokerTopicLoadingPredicate.isEnablePersistentTopics(brokerId)) {
                    // persistent topic can be assigned to only brokers that enabled for persistent-topic
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Filter broker- [{}] because broker only supports non-persistent namespace - [{}]",
                                brokerHost, namespace.toString());
                    }
                } else if (policies.isSharedBroker(brokerHost)) {
                    secondaryCache.add(brokerId);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added Shared Broker - [{}] as possible Candidates for namespace - [{}]",
                                brokerHost, namespace.toString());
                    }
                }
            }
        }
        if (isIsolationPoliciesPresent) {
            brokerCandidateCache.addAll(primariesCache);
            if (policies.shouldFailoverToSecondaries(namespace, primariesCache.size())) {
                LOG.debug(
                        "Not enough of primaries [{}] available for namespace - [{}], "
                                + "adding shared [{}] as possible candidate owners",
                        primariesCache.size(), namespace.toString(), secondaryCache.size());
                brokerCandidateCache.addAll(secondaryCache);
            }
        } else {
            LOG.debug(
                    "Policies not present for namespace - [{}] so only "
                            + "considering shared [{}] brokers for possible owner",
                    namespace.toString(), secondaryCache.size());
            brokerCandidateCache.addAll(secondaryCache);
        }
    }

    private static String parseBrokerHost(String brokerId) {
        // use last index to support ipv6 addresses
        int lastIdx = brokerId.lastIndexOf(':');
        if (lastIdx > -1) {
            return brokerId.substring(0, lastIdx);
        } else {
            throw new IllegalArgumentException("Invalid brokerId: " + brokerId);
        }
    }

    public static CompletableFuture<Set<String>> applyNamespacePoliciesAsync(
            final ServiceUnitId serviceUnit, final SimpleResourceAllocationPolicies policies,
            final Set<String> availableBrokers, final BrokerTopicLoadingPredicate brokerTopicLoadingPredicate) {
        NamespaceName namespace = serviceUnit.getNamespaceObject();
        return policies.areIsolationPoliciesPresentAsync(namespace).thenApply(isIsolationPoliciesPresent -> {
            final Set<String> brokerCandidateCache = new HashSet<>();
            Set<String> primariesCache = localPrimariesCache.get();
            primariesCache.clear();

            Set<String> secondaryCache = localSecondaryCache.get();
            secondaryCache.clear();
            boolean isNonPersistentTopic = (serviceUnit instanceof NamespaceBundle)
                    ? ((NamespaceBundle) serviceUnit).hasNonPersistentTopic() : false;
            if (isIsolationPoliciesPresent) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Isolation Policies Present for namespace - [{}]", namespace.toString());
                }
            }
            for (final String brokerId : availableBrokers) {
                String brokerHost;
                try {
                    brokerHost = parseBrokerHost(brokerId);
                } catch (IllegalArgumentException e) {
                    LOG.error("Unable to parse host from {}", brokerId, e);
                    continue;
                }
                // todo: in future check if the resource unit has resources to take the namespace
                if (isIsolationPoliciesPresent) {
                    // note: serviceUnitID is namespace name and ResourceID is brokerName
                    if (policies.isPrimaryBroker(namespace, brokerHost)) {
                        primariesCache.add(brokerId);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Added Primary Broker - [{}] as possible Candidates for"
                                    + " namespace - [{}] with policies", brokerHost, namespace.toString());
                        }
                    } else if (policies.isSecondaryBroker(namespace, brokerHost)) {
                        secondaryCache.add(brokerId);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Added Shared Broker - [{}] as possible "
                                            + "Candidates for namespace - [{}] with policies",
                                    brokerHost, namespace.toString());
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Skipping Broker - [{}] not primary broker and not shared"
                                            + " for namespace - [{}] ", brokerHost, namespace.toString());
                        }

                    }
                } else {
                    // non-persistent topic can be assigned to only those brokers that enabled for non-persistent topic
                    if (isNonPersistentTopic && !brokerTopicLoadingPredicate.isEnableNonPersistentTopics(brokerId)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Filter broker- [{}] because it doesn't support non-persistent namespace - [{}]",
                                    brokerId, namespace.toString());
                        }
                    } else if (!isNonPersistentTopic && !brokerTopicLoadingPredicate
                            .isEnablePersistentTopics(brokerId)) {
                        // persistent topic can be assigned to only brokers that enabled for persistent-topic
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Filter broker- [{}] because broker only supports non-persistent "
                                            + "namespace - [{}]", brokerId, namespace.toString());
                        }
                    } else if (policies.isSharedBroker(brokerHost)) {
                        secondaryCache.add(brokerId);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Added Shared Broker - [{}] as possible Candidates for namespace - [{}]",
                                    brokerHost, namespace.toString());
                        }
                    }
                }
            }
            if (isIsolationPoliciesPresent) {
                brokerCandidateCache.addAll(primariesCache);
                if (policies.shouldFailoverToSecondaries(namespace, primariesCache.size())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Not enough of primaries [{}] available for namespace - [{}], "
                                        + "adding shared [{}] as possible candidate owners",
                                primariesCache.size(), namespace.toString(), secondaryCache.size());
                    }
                    brokerCandidateCache.addAll(secondaryCache);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Policies not present for namespace - [{}] so only "
                                    + "considering shared [{}] brokers for possible owner",
                            namespace.toString(), secondaryCache.size());
                }
                brokerCandidateCache.addAll(secondaryCache);
            }
            return brokerCandidateCache;
        });
    }

    // From a full bundle name, extract the bundle range.
    public static String getBundleRangeFromBundleName(String bundleName) {
        // the bundle format is property/cluster/namespace/0x00000000_0xFFFFFFFF
        int pos = bundleName.lastIndexOf("/");
        checkArgument(pos != -1);
        return bundleName.substring(pos + 1);
    }

    // From a full bundle name, extract the namespace name.
    public static String getNamespaceNameFromBundleName(String bundleName) {
        // the bundle format is property/cluster/namespace/0x00000000_0xFFFFFFFF
        int pos = bundleName.lastIndexOf('/');
        checkArgument(pos != -1);
        return bundleName.substring(0, pos);
    }

    // Get the system resource usage for this broker.
    public static SystemResourceUsage getSystemResourceUsage(final BrokerHostUsage brokerHostUsage) {
        SystemResourceUsage systemResourceUsage = brokerHostUsage.getBrokerHostUsage();

        // Override System memory usage and limit with JVM heap usage and limit
        double maxHeapMemoryInBytes = Runtime.getRuntime().maxMemory();
        double memoryUsageInBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        double memoryUsage = memoryUsageInBytes / MIBI;
        double memoryLimit = maxHeapMemoryInBytes / MIBI;
        systemResourceUsage.setMemory(new ResourceUsage(memoryUsage, memoryLimit));

        // Collect JVM direct memory
        systemResourceUsage.setDirectMemory(new ResourceUsage((double) (getJvmDirectMemoryUsed() / MIBI),
                (double) (DirectMemoryUtils.jvmMaxDirectMemory() / MIBI)));

        return systemResourceUsage;
    }

    /**
     * If load balancing is enabled, load shedding is enabled by default unless forced off by dynamic configuration.
     *
     * @return true by default
     */
    public static boolean isLoadSheddingEnabled(final PulsarService pulsar) {
        return pulsar.getConfiguration().isLoadBalancerEnabled()
                && pulsar.getConfiguration().isLoadBalancerSheddingEnabled();
    }

    /**
     * Removes the brokers which have more bundles assigned to them in the same namespace as the incoming bundle than at
     * least one other available broker from consideration.
     *
     * @param assignedBundleName
     *            Name of bundle to be assigned.
     * @param candidates
     *            BrokersBase available for placement.
     * @param brokerToNamespaceToBundleRange
     *            Map from brokers to namespaces to bundle ranges.
     */
    public static void removeMostServicingBrokersForNamespace(
            final String assignedBundleName,
            final Set<String> candidates,
            final BundleRangeCache brokerToNamespaceToBundleRange) {
        if (candidates.isEmpty()) {
            return;
        }

        final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
        int leastBundles = Integer.MAX_VALUE;

        for (final String broker : candidates) {
            int bundles = brokerToNamespaceToBundleRange.getBundleRangeCount(broker, namespaceName);
            leastBundles = Math.min(leastBundles, bundles);
            if (leastBundles == 0) {
                break;
            }
        }

        // Since `brokerToNamespaceToBundleRange` can be updated by other threads,
        // `leastBundles` may differ from the actual value.

        final int finalLeastBundles = leastBundles;
        candidates.removeIf(broker ->
                brokerToNamespaceToBundleRange.getBundleRangeCount(broker, namespaceName) > finalLeastBundles);
    }

    /**
     * It tries to filter out brokers which own namespace with same anti-affinity-group as given namespace. If all the
     * domains own namespace with same anti-affinity group then it will try to keep brokers with domain that has least
     * number of namespaces. It also tries to keep brokers which has least number of namespace with in domain.
     * eg.
     * <pre>
     * Before:
     * Domain-count  BrokersBase-count
     * ____________  ____________
     * d1-3          b1-2,b2-1
     * d2-3          b3-2,b4-1
     * d3-4          b5-2,b6-2
     *
     * After filtering: "candidates" brokers
     * Domain-count  BrokersBase-count
     * ____________  ____________
     * d1-3          b2-1
     * d2-3          b4-1
     *
     * "candidate" broker to own anti-affinity-namespace = b2 or b4
     *
     * </pre>
     *
     * @param pulsar
     * @param assignedBundleName
     * @param candidates
     * @param brokerToNamespaceToBundleRange
     */
    public static void filterAntiAffinityGroupOwnedBrokers(
            final PulsarService pulsar, final String assignedBundleName,
            final Set<String> candidates,
            final BundleRangeCache brokerToNamespaceToBundleRange,
            Map<String, String> brokerToDomainMap) {
        if (candidates.isEmpty()) {
            return;
        }
        final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
        try {
            final Map<String, Integer> brokerToAntiAffinityNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(pulsar,
                    namespaceName, brokerToNamespaceToBundleRange).get(30, TimeUnit.SECONDS);
            filterAntiAffinityGroupOwnedBrokers(pulsar, candidates, brokerToDomainMap,
                    brokerToAntiAffinityNamespaceCount);
        } catch (Exception e) {
            LOG.error("Failed to filter anti-affinity group namespace {}", e.getMessage());
        }
    }

    private static void filterAntiAffinityGroupOwnedBrokers(
            final PulsarService pulsar,
            final Set<String> candidates,
            Map<String, String> brokerToDomainMap,
            Map<String, Integer> brokerToAntiAffinityNamespaceCount) {
            if (brokerToAntiAffinityNamespaceCount == null) {
                // none of the broker owns anti-affinity-namespace so, none of the broker will be filtered
                return;
            }
            if (pulsar.getConfiguration().isFailureDomainsEnabled()) {
                // this will remove all the brokers which are part of domains that don't have least number of
                // anti-affinity-namespaces
                filterDomainsNotHavingLeastNumberAntiAffinityNamespaces(brokerToAntiAffinityNamespaceCount, candidates,
                        brokerToDomainMap);
            }
            // now, "candidates" has list of brokers which are part of domain that can accept this namespace. now,
            // with in these domains, remove brokers which don't have least number of namespaces. so, brokers with least
            // number of namespace can be selected
            int leastNamespaceCount = Integer.MAX_VALUE;
            for (final String broker : candidates) {
                if (brokerToAntiAffinityNamespaceCount.containsKey(broker)) {
                    Integer namespaceCount = brokerToAntiAffinityNamespaceCount.get(broker);
                    if (namespaceCount == null) {
                        // Assume that when the namespace is absent, there are no namespace assigned to
                        // that broker.
                        leastNamespaceCount = 0;
                        break;
                    }
                    leastNamespaceCount = Math.min(leastNamespaceCount, namespaceCount);
                } else {
                    // Assume non-present brokers have 0 bundles.
                    leastNamespaceCount = 0;
                    break;
                }
            }
            // filter out broker based on namespace distribution
            if (leastNamespaceCount == 0) {
                candidates.removeIf(broker -> brokerToAntiAffinityNamespaceCount.containsKey(broker)
                        && brokerToAntiAffinityNamespaceCount.get(broker) > 0);
            } else {
                final int finalLeastNamespaceCount = leastNamespaceCount;
                candidates
                        .removeIf(broker -> brokerToAntiAffinityNamespaceCount.get(broker) != finalLeastNamespaceCount);
            }
    }

    public static void filterAntiAffinityGroupOwnedBrokers(
            final PulsarService pulsar, final String assignedBundleName,
            final Set<String> candidates,
            Set<Map.Entry<String, ServiceUnitStateData>> bundleOwnershipData,
            Map<String, String> brokerToDomainMap) {
        if (candidates.isEmpty()) {
            return;
        }
        final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
        try {
            final Map<String, Integer> brokerToAntiAffinityNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(
                    pulsar, namespaceName, bundleOwnershipData)
                    .get(30, TimeUnit.SECONDS);
            filterAntiAffinityGroupOwnedBrokers(pulsar, candidates, brokerToDomainMap,
                    brokerToAntiAffinityNamespaceCount);
        } catch (Exception e) {
            LOG.error("Failed to filter anti-affinity group namespace {}", e.getMessage());
        }
    }

    public static CompletableFuture<Void> filterAntiAffinityGroupOwnedBrokersAsync(
            final PulsarService pulsar, final String assignedBundleName,
            final Set<String> candidates,
            Set<Map.Entry<String, ServiceUnitStateData>> bundleOwnershipData,
            Map<String, String> brokerToDomainMap
    ) {
        if (candidates.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
        return getAntiAffinityNamespaceOwnedBrokers(pulsar, namespaceName, bundleOwnershipData)
                .thenAccept(brokerToAntiAffinityNamespaceCount ->
                        filterAntiAffinityGroupOwnedBrokers(pulsar, candidates,
                                brokerToDomainMap, brokerToAntiAffinityNamespaceCount));
    }

    /**
     * It computes least number of namespace owned by any of the domain and then it filters out all the domains that own
     * namespaces more than this count.
     *
     * @param brokerToAntiAffinityNamespaceCount
     * @param candidates
     * @param brokerToDomainMap
     */
    private static void filterDomainsNotHavingLeastNumberAntiAffinityNamespaces(
            Map<String, Integer> brokerToAntiAffinityNamespaceCount, Set<String> candidates,
            Map<String, String> brokerToDomainMap) {

        if (brokerToDomainMap == null || brokerToDomainMap.isEmpty()) {
            return;
        }

        final Map<String, Integer> domainNamespaceCount = new HashMap<>();
        int leastNamespaceCount = Integer.MAX_VALUE;
        candidates.forEach(broker -> {
            final String domain = brokerToDomainMap.getOrDefault(broker, DEFAULT_DOMAIN);
            final int count = brokerToAntiAffinityNamespaceCount.getOrDefault(broker, 0);
            domainNamespaceCount.compute(domain, (domainName, nsCount) -> nsCount == null ? count : nsCount + count);
        });
        // find leastNameSpaceCount
        for (Entry<String, Integer> domainNsCountEntry : domainNamespaceCount.entrySet()) {
            if (domainNsCountEntry.getValue() < leastNamespaceCount) {
                leastNamespaceCount = domainNsCountEntry.getValue();
            }
        }
        final int finalLeastNamespaceCount = leastNamespaceCount;
        // only keep domain brokers which has leastNamespaceCount
        candidates.removeIf(broker -> {
            Integer nsCount = domainNamespaceCount.get(brokerToDomainMap.getOrDefault(broker, DEFAULT_DOMAIN));
            return nsCount != null && nsCount != finalLeastNamespaceCount;
        });
    }

    /**
     * It returns map of broker and count of namespace that are belong to the same anti-affinity group as given.
     *
     * @param pulsar
     * @param namespaceName
     * @param brokerToNamespaceToBundleRange
     * @return
     */
    public static CompletableFuture<Map<String, Integer>> getAntiAffinityNamespaceOwnedBrokers(
            final PulsarService pulsar, final String namespaceName,
            final BundleRangeCache brokerToNamespaceToBundleRange) {

        CompletableFuture<Map<String, Integer>> antiAffinityNsBrokersResult = new CompletableFuture<>();
        getNamespaceAntiAffinityGroupAsync(pulsar, namespaceName)
                .thenAccept(antiAffinityGroupOptional -> {
            if (antiAffinityGroupOptional.isEmpty()) {
                antiAffinityNsBrokersResult.complete(null);
                return;
            }
            final String antiAffinityGroup = antiAffinityGroupOptional.get();
            final Map<String, Integer> brokerToAntiAffinityNamespaceCount = new ConcurrentHashMap<>();
            final var brokerToNamespaces = brokerToNamespaceToBundleRange.getBrokerToNamespacesMap();
            FutureUtil.waitForAll(brokerToNamespaces.entrySet().stream().flatMap(e -> {
                final var broker = e.getKey();
                return e.getValue().stream().map(namespace -> {
                    final var future = new CompletableFuture<Void>();
                    countAntiAffinityNamespaceOwnedBrokers(broker, namespace, future,
                            pulsar, antiAffinityGroup, brokerToAntiAffinityNamespaceCount);
                    return future;
                });
            }).toList()).thenAccept(__ -> antiAffinityNsBrokersResult.complete(brokerToAntiAffinityNamespaceCount));
        }).exceptionally(ex -> {
            // namespace-policies has not been created yet
            antiAffinityNsBrokersResult.complete(null);
            return null;
        });
        return antiAffinityNsBrokersResult;
    }

    private static void countAntiAffinityNamespaceOwnedBrokers(
            String broker, String ns, CompletableFuture<Void> future, PulsarService pulsar,
            String targetAntiAffinityGroup, Map<String, Integer> brokerToAntiAffinityNamespaceCount) {
                    getNamespaceAntiAffinityGroupAsync(pulsar, ns)
                            .thenAccept(antiAffinityGroupOptional -> {
                        if (antiAffinityGroupOptional.isPresent()
                                && targetAntiAffinityGroup.equalsIgnoreCase(antiAffinityGroupOptional.get())) {
                            brokerToAntiAffinityNamespaceCount.compute(broker,
                                    (brokerName, count) -> count == null ? 1 : count + 1);
                        }
                        future.complete(null);
                    }).exceptionally(ex -> {
                        future.complete(null);
                        return null;
                    });
    }

    public static CompletableFuture<Map<String, Integer>> getAntiAffinityNamespaceOwnedBrokers(
            final PulsarService pulsar, final String namespaceName,
            Set<Map.Entry<String, ServiceUnitStateData>> bundleOwnershipData) {

        CompletableFuture<Map<String, Integer>> antiAffinityNsBrokersResult = new CompletableFuture<>();
        getNamespaceAntiAffinityGroupAsync(pulsar, namespaceName)
                .thenAccept(antiAffinityGroupOptional -> {
            if (antiAffinityGroupOptional.isEmpty()) {
                antiAffinityNsBrokersResult.complete(null);
                return;
            }
            final String antiAffinityGroup = antiAffinityGroupOptional.get();
            final Map<String, Integer> brokerToAntiAffinityNamespaceCount = new ConcurrentHashMap<>();
            final List<CompletableFuture<Void>> futures = new ArrayList<>();

            bundleOwnershipData
                    .forEach(etr -> {
                        var stateData = etr.getValue();
                        var bundle = etr.getKey();
                        if (stateData.state() == ServiceUnitState.Owned
                                && StringUtils.isNotBlank(stateData.dstBroker())) {
                            CompletableFuture<Void> future = new CompletableFuture<>();
                            futures.add(future);
                            countAntiAffinityNamespaceOwnedBrokers
                                    (stateData.dstBroker(),
                                            LoadManagerShared.getNamespaceNameFromBundleName(bundle),
                                            future, pulsar,
                                            antiAffinityGroup, brokerToAntiAffinityNamespaceCount);
                        }
                    });

            FutureUtil.waitForAll(futures)
                    .thenAccept(r -> antiAffinityNsBrokersResult.complete(brokerToAntiAffinityNamespaceCount));
        }).exceptionally(ex -> {
            // namespace-policies has not been created yet
            antiAffinityNsBrokersResult.complete(null);
            return null;
        });
        return antiAffinityNsBrokersResult;
    }

    public static CompletableFuture<Optional<String>> getNamespaceAntiAffinityGroupAsync(
            PulsarService pulsar, String namespaceName) {
        return pulsar.getPulsarResources().getLocalPolicies().getLocalPoliciesAsync(NamespaceName.get(namespaceName))
                .thenApply(localPoliciesOptional -> {
                    if (localPoliciesOptional.isPresent()
                            && StringUtils.isNotBlank(localPoliciesOptional.get().namespaceAntiAffinityGroup)) {
                        return Optional.of(localPoliciesOptional.get().namespaceAntiAffinityGroup);
                    }
                    return Optional.empty();
                });
    }


    public static Optional<String> getNamespaceAntiAffinityGroup(
            PulsarService pulsar, String namespaceName) throws MetadataStoreException {
        var localPoliciesOptional =
                pulsar.getPulsarResources().getLocalPolicies().getLocalPolicies(NamespaceName.get(namespaceName));
        if (localPoliciesOptional.isPresent()
                && StringUtils.isNotBlank(localPoliciesOptional.get().namespaceAntiAffinityGroup)) {
            return Optional.of(localPoliciesOptional.get().namespaceAntiAffinityGroup);
        }
        return Optional.empty();
    }


    /**
     *
     * It checks if given anti-affinity namespace should be unloaded by broker due to load-shedding. If all the brokers
     * are owning same number of anti-affinity namespaces then unloading this namespace again ends up at the same broker
     * from which it was unloaded. So, this util checks that given namespace should be unloaded only if it can be loaded
     * by different broker.
     *
     * @param namespace
     * @param currentBroker
     * @param pulsar
     * @param brokerToNamespaceToBundleRange
     * @param candidateBrokers
     * @return
     * @throws Exception
     */
    public static boolean shouldAntiAffinityNamespaceUnload(
            String namespace, String currentBroker,
            final PulsarService pulsar,
            final BundleRangeCache brokerToNamespaceToBundleRange,
            Set<String> candidateBrokers) throws Exception {

        Map<String, Integer> brokerNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(pulsar, namespace,
                brokerToNamespaceToBundleRange).get(10, TimeUnit.SECONDS);
        return shouldAntiAffinityNamespaceUnload(currentBroker, candidateBrokers, brokerNamespaceCount);
    }

    private static boolean shouldAntiAffinityNamespaceUnload(
            String currentBroker,
            Set<String> candidateBrokers,
            Map<String, Integer> brokerNamespaceCount) {
        if (brokerNamespaceCount != null && !brokerNamespaceCount.isEmpty()) {
            int leastNsCount = Integer.MAX_VALUE;
            int currentBrokerNsCount = 0;

            for (String broker : candidateBrokers) {
                int nsCount = brokerNamespaceCount.getOrDefault(broker, 0);
                if (currentBroker.equals(broker)) {
                    currentBrokerNsCount = nsCount;
                }
                if (leastNsCount > nsCount) {
                    leastNsCount = nsCount;
                }
            }
            // check if there is any other broker has less number of ns
            if (leastNsCount == 0 || currentBrokerNsCount > leastNsCount) {
                return true;
            }
            // check if all the brokers having same number of ns-count then broker can't unload
            int leastNsOwnerBrokers = 0;
            for (String broker : candidateBrokers) {
                if (leastNsCount == brokerNamespaceCount.getOrDefault(broker, 0)) {
                    leastNsOwnerBrokers++;
                }
            }
            // if all candidate brokers own same-number of ns then broker can't unload
            return candidateBrokers.size() != leastNsOwnerBrokers;
        }
        return true;
    }

    public static boolean shouldAntiAffinityNamespaceUnload(
            String namespace, String bundle, String currentBroker,
            final PulsarService pulsar,
            Set<Map.Entry<String, ServiceUnitStateData>> bundleOwnershipData,
            Set<String> candidateBrokers) throws Exception {

        Map<String, Integer> brokerNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(
                pulsar, namespace, bundleOwnershipData)
                .get(10, TimeUnit.SECONDS);
        return shouldAntiAffinityNamespaceUnload(currentBroker, candidateBrokers, brokerNamespaceCount);
    }

    public interface BrokerTopicLoadingPredicate {
        boolean isEnablePersistentTopics(String brokerId);

        boolean isEnableNonPersistentTopics(String brokerId);
    }

    /**
     * It filters out brokers which owns topic higher than configured threshold at
     * ServiceConfiguration.loadBalancerBrokerMaxTopics. <br/>
     * if all the brokers own topic higher than threshold then it resets the list with original broker candidates
     *
     * @param brokerCandidateCache
     * @param loadData
     * @param loadBalancerBrokerMaxTopics
     */
    public static void filterBrokersWithLargeTopicCount(Set<String> brokerCandidateCache, LoadData loadData,
            int loadBalancerBrokerMaxTopics) {
        Set<String> filteredBrokerCandidates = brokerCandidateCache.stream().filter((broker) -> {
            BrokerData brokerData = loadData.getBrokerData().get(broker);
            long totalTopics = brokerData != null && brokerData.getPreallocatedBundleData() != null
                    ? brokerData.getPreallocatedBundleData().values().stream()
                            .mapToLong((preAllocatedBundle) -> preAllocatedBundle.getTopics()).sum()
                            + brokerData.getLocalData().getNumTopics()
                    : 0;
            return totalTopics <= loadBalancerBrokerMaxTopics;
        }).collect(Collectors.toSet());

        if (!filteredBrokerCandidates.isEmpty()) {
            brokerCandidateCache.clear();
            brokerCandidateCache.addAll(filteredBrokerCandidates);
        }
    }

    public static void refreshBrokerToFailureDomainMap(PulsarService pulsar,
                                                       Map<String, String> brokerToFailureDomainMap) {
        if (!pulsar.getConfiguration().isFailureDomainsEnabled()) {
            return;
        }
        ClusterResources.FailureDomainResources fdr =
                pulsar.getPulsarResources().getClusterResources().getFailureDomainResources();
        String clusterName = pulsar.getConfiguration().getClusterName();
        try {
            synchronized (brokerToFailureDomainMap) {
                Map<String, String> tempBrokerToFailureDomainMap = new HashMap<>();
                for (String domainName : fdr.listFailureDomains(clusterName)) {
                    try {
                        Optional<FailureDomainImpl> domain = fdr.getFailureDomain(clusterName, domainName);
                        if (domain.isPresent()) {
                            for (String broker : domain.get().brokers) {
                                tempBrokerToFailureDomainMap.put(broker, domainName);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to get domain {}", domainName, e);
                    }
                }
                brokerToFailureDomainMap.clear();
                brokerToFailureDomainMap.putAll(tempBrokerToFailureDomainMap);
            }
            LOG.info("Cluster domain refreshed {}", brokerToFailureDomainMap);
        } catch (Exception e) {
            LOG.warn("Failed to get domain-list for cluster {}", e.getMessage());
        }
    }

    public static NamespaceBundle getNamespaceBundle(PulsarService pulsar, String bundle) {
        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        return pulsar.getNamespaceService().getNamespaceBundleFactory().getBundle(namespaceName, bundleRange);
    }
}
