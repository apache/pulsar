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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;
import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
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

    // update LoadReport at most every 5 seconds
    public static final long LOAD_REPORT_UPDATE_MINIMUM_INTERVAL = TimeUnit.SECONDS.toMillis(5);

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
        for (final String broker : availableBrokers) {
            final String brokerUrlString = String.format("http://%s", broker);
            URL brokerUrl;
            try {
                brokerUrl = new URL(brokerUrlString);
            } catch (MalformedURLException e) {
                LOG.error("Unable to parse brokerUrl from ResourceUnitId", e);
                continue;
            }
            // todo: in future check if the resource unit has resources to take the namespace
            if (isIsolationPoliciesPresent) {
                // note: serviceUnitID is namespace name and ResourceID is brokerName
                if (policies.isPrimaryBroker(namespace, brokerUrl.getHost())) {
                    primariesCache.add(broker);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added Primary Broker - [{}] as possible Candidates for"
                                + " namespace - [{}] with policies", brokerUrl.getHost(), namespace.toString());
                    }
                } else if (policies.isSecondaryBroker(namespace, brokerUrl.getHost())) {
                    secondaryCache.add(broker);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Added Shared Broker - [{}] as possible "
                                        + "Candidates for namespace - [{}] with policies",
                                brokerUrl.getHost(), namespace.toString());
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping Broker - [{}] not primary broker and not shared" + " for namespace - [{}] ",
                                brokerUrl.getHost(), namespace.toString());
                    }

                }
            } else {
                // non-persistent topic can be assigned to only those brokers that enabled for non-persistent topic
                if (isNonPersistentTopic
                        && !brokerTopicLoadingPredicate.isEnableNonPersistentTopics(brokerUrlString)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Filter broker- [{}] because it doesn't support non-persistent namespace - [{}]",
                                brokerUrl.getHost(), namespace.toString());
                    }
                } else if (!isNonPersistentTopic
                        && !brokerTopicLoadingPredicate.isEnablePersistentTopics(brokerUrlString)) {
                    // persistent topic can be assigned to only brokers that enabled for persistent-topic
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Filter broker- [{}] because broker only supports non-persistent namespace - [{}]",
                                brokerUrl.getHost(), namespace.toString());
                    }
                } else if (policies.isSharedBroker(brokerUrl.getHost())) {
                    secondaryCache.add(broker);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added Shared Broker - [{}] as possible Candidates for namespace - [{}]",
                                brokerUrl.getHost(), namespace.toString());
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

    /**
     * Using the given bundles, populate the namespace to bundle range map.
     *
     * @param bundles
     *            Bundles with which to populate.
     * @param target
     *            Map to fill.
     */
    public static void fillNamespaceToBundlesMap(final Set<String> bundles,
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>> target) {
        bundles.forEach(bundleName -> {
            final String namespaceName = getNamespaceNameFromBundleName(bundleName);
            final String bundleRange = getBundleRangeFromBundleName(bundleName);
            target.computeIfAbsent(namespaceName, k -> new ConcurrentOpenHashSet<>()).add(bundleRange);
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
    public static SystemResourceUsage getSystemResourceUsage(final BrokerHostUsage brokerHostUsage) throws IOException {
        SystemResourceUsage systemResourceUsage = brokerHostUsage.getBrokerHostUsage();

        // Override System memory usage and limit with JVM heap usage and limit
        long maxHeapMemoryInBytes = Runtime.getRuntime().maxMemory();
        long memoryUsageInBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        systemResourceUsage.memory.usage = (double) memoryUsageInBytes / MIBI;
        systemResourceUsage.memory.limit = (double) maxHeapMemoryInBytes / MIBI;

        // Collect JVM direct memory
        systemResourceUsage.directMemory.usage = (double) (getJvmDirectMemoryUsed() / MIBI);
        systemResourceUsage.directMemory.limit =
                (double) (io.netty.util.internal.PlatformDependent.maxDirectMemory() / MIBI);

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
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>>
                    brokerToNamespaceToBundleRange) {
        if (candidates.isEmpty()) {
            return;
        }

        final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
        int leastBundles = Integer.MAX_VALUE;

        for (final String broker : candidates) {
            int bundles = (int) brokerToNamespaceToBundleRange
                    .computeIfAbsent(broker, k -> new ConcurrentOpenHashMap<>())
                    .computeIfAbsent(namespaceName, k -> new ConcurrentOpenHashSet<>()).size();
            leastBundles = Math.min(leastBundles, bundles);
            if (leastBundles == 0) {
                break;
            }
        }

        // Since `brokerToNamespaceToBundleRange` can be updated by other threads,
        // `leastBundles` may differ from the actual value.

        final int finalLeastBundles = leastBundles;
        candidates.removeIf(
                broker -> brokerToNamespaceToBundleRange.computeIfAbsent(broker, k -> new ConcurrentOpenHashMap<>())
                        .computeIfAbsent(namespaceName, k -> new ConcurrentOpenHashSet<>()).size() > finalLeastBundles);
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
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>>
                    brokerToNamespaceToBundleRange,
            Map<String, String> brokerToDomainMap) {
        if (candidates.isEmpty()) {
            return;
        }
        final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
        try {
            final Map<String, Integer> brokerToAntiAffinityNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(pulsar,
                    namespaceName, brokerToNamespaceToBundleRange).get(30, TimeUnit.SECONDS);
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
            int leastNamaespaceCount = Integer.MAX_VALUE;
            for (final String broker : candidates) {
                if (brokerToAntiAffinityNamespaceCount.containsKey(broker)) {
                    Integer namespaceCount = brokerToAntiAffinityNamespaceCount.get(broker);
                    if (namespaceCount == null) {
                        // Assume that when the namespace is absent, there are no namespace assigned to
                        // that broker.
                        leastNamaespaceCount = 0;
                        break;
                    }
                    leastNamaespaceCount = Math.min(leastNamaespaceCount, namespaceCount);
                } else {
                    // Assume non-present brokers have 0 bundles.
                    leastNamaespaceCount = 0;
                    break;
                }
            }
            // filter out broker based on namespace distribution
            if (leastNamaespaceCount == 0) {
                candidates.removeIf(broker -> brokerToAntiAffinityNamespaceCount.containsKey(broker)
                        && brokerToAntiAffinityNamespaceCount.get(broker) > 0);
            } else {
                final int finalLeastNamespaceCount = leastNamaespaceCount;
                candidates
                        .removeIf(broker -> brokerToAntiAffinityNamespaceCount.get(broker) != finalLeastNamespaceCount);
            }
        } catch (Exception e) {
            LOG.error("Failed to filter anti-affinity group namespace {}", e.getMessage());
        }
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

        final Map<String, Integer> domainNamespaceCount = Maps.newHashMap();
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
     * {@param namespaceName}
     *
     * @param pulsar
     * @param namespaceName
     * @param brokerToNamespaceToBundleRange
     * @return
     */
    public static CompletableFuture<Map<String, Integer>> getAntiAffinityNamespaceOwnedBrokers(
            final PulsarService pulsar, final String namespaceName,
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>>
                    brokerToNamespaceToBundleRange) {

        CompletableFuture<Map<String, Integer>> antiAffinityNsBrokersResult = new CompletableFuture<>();

        pulsar.getPulsarResources().getLocalPolicies().getLocalPoliciesAsync(NamespaceName.get(namespaceName))
                .thenAccept(policies -> {
            if (!policies.isPresent() || StringUtils.isBlank(policies.get().namespaceAntiAffinityGroup)) {
                antiAffinityNsBrokersResult.complete(null);
                return;
            }
            final String antiAffinityGroup = policies.get().namespaceAntiAffinityGroup;
            final Map<String, Integer> brokerToAntiAffinityNamespaceCount = new ConcurrentHashMap<>();
            final List<CompletableFuture<Void>> futures = Lists.newArrayList();
            brokerToNamespaceToBundleRange.forEach((broker, nsToBundleRange) -> {
                nsToBundleRange.forEach((ns, bundleRange) -> {
                    if (bundleRange.isEmpty()) {
                        return;
                    }

                    CompletableFuture<Void> future = new CompletableFuture<>();
                    futures.add(future);

                    pulsar.getPulsarResources().getLocalPolicies().getLocalPoliciesAsync(NamespaceName.get(ns))
                            .thenAccept(nsPolicies -> {
                        if (nsPolicies.isPresent()
                                && antiAffinityGroup.equalsIgnoreCase(nsPolicies.get().namespaceAntiAffinityGroup)) {
                            brokerToAntiAffinityNamespaceCount.compute(broker,
                                    (brokerName, count) -> count == null ? 1 : count + 1);
                        }
                        future.complete(null);
                    }).exceptionally(ex -> {
                        future.complete(null);
                        return null;
                    });
                });
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

    /**
     *
     * It checks if given anti-affinity namespace should be unloaded by broker due to load-shedding. If all the brokers
     * are owning same number of anti-affinity namespaces then unloading this namespace again ends up at the same broker
     * from which it was unloaded. So, this util checks that given namespace should be unloaded only if it can be loaded
     * by different broker.
     *
     * @param namespace
     * @param bundle
     * @param currentBroker
     * @param pulsar
     * @param brokerToNamespaceToBundleRange
     * @param candidateBrokers
     * @return
     * @throws Exception
     */
    public static boolean shouldAntiAffinityNamespaceUnload(
            String namespace, String bundle, String currentBroker,
            final PulsarService pulsar,
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>>
                    brokerToNamespaceToBundleRange,
            Set<String> candidateBrokers) throws Exception {

        Map<String, Integer> brokerNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(pulsar, namespace,
                brokerToNamespaceToBundleRange).get(10, TimeUnit.SECONDS);
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

    public interface BrokerTopicLoadingPredicate {
        boolean isEnablePersistentTopics(String brokerUrl);

        boolean isEnableNonPersistentTopics(String brokerUrl);
    }

    /**
     * It filters out brokers which owns topic higher than configured threshold at
     * {@link ServiceConfiguration.loadBalancerBrokerMaxTopics}. <br/>
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
}
