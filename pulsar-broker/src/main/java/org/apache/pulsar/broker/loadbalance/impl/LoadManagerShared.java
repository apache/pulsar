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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.stats.metrics.JvmMetrics;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains code which in shared between the two load manager implementations.
 */
public class LoadManagerShared {
    public static final Logger log = LoggerFactory.getLogger(LoadManagerShared.class);

    // Value of prefix "mibi" (e.g., number of bytes in a mibibyte).
    public static final int MIBI = 1024 * 1024;

    // Cache for primary brokers according to policies.
    private static final Set<String> primariesCache = new HashSet<>();

    // Cache for shard brokers according to policies.
    private static final Set<String> sharedCache = new HashSet<>();

    // Don't allow construction: static method namespace only.
    private LoadManagerShared() {
    }

    // Determines the brokers available for the given service unit according to the given policies.
    // The brokers are put into brokerCandidateCache.
    public static synchronized void applyPolicies(final ServiceUnitId serviceUnit,
            final SimpleResourceAllocationPolicies policies, final Set<String> brokerCandidateCache,
            final Set<String> availableBrokers) {
        primariesCache.clear();
        sharedCache.clear();
        NamespaceName namespace = serviceUnit.getNamespaceObject();
        boolean isIsolationPoliciesPresent = policies.IsIsolationPoliciesPresent(namespace);
        if (isIsolationPoliciesPresent) {
            log.debug("Isolation Policies Present for namespace - [{}]", namespace.toString());
        }
        for (final String broker : availableBrokers) {
            final String brokerUrlString = String.format("http://%s", broker);
            URL brokerUrl;
            try {
                brokerUrl = new URL(brokerUrlString);
            } catch (MalformedURLException e) {
                log.error("Unable to parse brokerUrl from ResourceUnitId - [{}]", e);
                continue;
            }
            // todo: in future check if the resource unit has resources to take
            // the namespace
            if (isIsolationPoliciesPresent) {
                // note: serviceUnitID is namespace name and ResourceID is
                // brokerName
                if (policies.isPrimaryBroker(namespace, brokerUrl.getHost())) {
                    primariesCache.add(broker);
                    if (log.isDebugEnabled()) {
                        log.debug("Added Primary Broker - [{}] as possible Candidates for"
                                + " namespace - [{}] with policies", brokerUrl.getHost(), namespace.toString());
                    }
                } else if (policies.isSharedBroker(brokerUrl.getHost())) {
                    sharedCache.add(broker);
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Added Shared Broker - [{}] as possible "
                                        + "Candidates for namespace - [{}] with policies",
                                brokerUrl.getHost(), namespace.toString());
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Skipping Broker - [{}] not primary broker and not shared" + " for namespace - [{}] ",
                                brokerUrl.getHost(), namespace.toString());
                    }

                }
            } else {
                if (policies.isSharedBroker(brokerUrl.getHost())) {
                    sharedCache.add(broker);
                    log.debug("Added Shared Broker - [{}] as possible Candidates for namespace - [{}]",
                            brokerUrl.getHost(), namespace.toString());
                }
            }
        }
        if (isIsolationPoliciesPresent) {
            brokerCandidateCache.addAll(primariesCache);
            if (policies.shouldFailoverToSecondaries(namespace, primariesCache.size())) {
                log.debug(
                        "Not enough of primaries [{}] available for namespace - [{}], "
                                + "adding shared [{}] as possible candidate owners",
                        primariesCache.size(), namespace.toString(), sharedCache.size());
                brokerCandidateCache.addAll(sharedCache);
            }
        } else {
            log.debug(
                    "Policies not present for namespace - [{}] so only "
                            + "considering shared [{}] brokers for possible owner",
                    namespace.toString(), sharedCache.size());
            brokerCandidateCache.addAll(sharedCache);
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
    public static void fillNamespaceToBundlesMap(final Set<String> bundles, final Map<String, Set<String>> target) {
        bundles.forEach(bundleName -> {
            final String namespaceName = getNamespaceNameFromBundleName(bundleName);
            final String bundleRange = getBundleRangeFromBundleName(bundleName);
            target.computeIfAbsent(namespaceName, k -> new HashSet<>()).add(bundleRange);
        });
    }

    // From a full bundle name, extract the bundle range.
    public static String getBundleRangeFromBundleName(String bundleName) {
        // the bundle format is property/cluster/namespace/0x00000000_0xFFFFFFFF
        int pos = bundleName.lastIndexOf("/");
        checkArgument(pos != -1);
        return bundleName.substring(pos + 1, bundleName.length());
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
        systemResourceUsage.directMemory.usage = (double) (JvmMetrics.getJvmDirectMemoryUsed() / MIBI);
        systemResourceUsage.directMemory.limit = (double) (sun.misc.VM.maxDirectMemory() / MIBI);

        return systemResourceUsage;
    }

    /**
     * If load balancing is enabled, load shedding is enabled by default unless forced off by setting a flag in global
     * zk /admin/flags/load-shedding-unload-disabled
     *
     * @return false by default, unload is allowed in load shedding true if zk flag is set, unload is disabled
     */
    public static boolean isUnloadDisabledInLoadShedding(final PulsarService pulsar) {
        if (!pulsar.getConfiguration().isLoadBalancerEnabled()) {
            return true;
        }

        boolean unloadDisabledInLoadShedding = false;
        try {
            unloadDisabledInLoadShedding = pulsar.getGlobalZkCache()
                    .exists(AdminResource.LOAD_SHEDDING_UNLOAD_DISABLED_FLAG_PATH);
        } catch (Exception e) {
            log.warn("Unable to fetch contents of [{}] from global zookeeper",
                    AdminResource.LOAD_SHEDDING_UNLOAD_DISABLED_FLAG_PATH, e);
        }
        return unloadDisabledInLoadShedding;
    }

    /**
     * Removes the brokers which have more bundles assigned to them in the same namespace as the incoming bundle than at
     * least one other available broker from consideration.
     * 
     * @param assignedBundleName
     *            Name of bundle to be assigned.
     * @param candidates
     *            Brokers available for placement.
     * @param brokerToNamespaceToBundleRange
     *            Map from brokers to namespaces to bundle ranges.
     */
    public static void removeMostServicingBrokersForNamespace(final String assignedBundleName,
            final Set<String> candidates, final Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange) {
        if (candidates.isEmpty()) {
            return;
        }
        final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
        int leastBundles = Integer.MAX_VALUE;
        for (final String broker : candidates) {
            if (brokerToNamespaceToBundleRange.containsKey(broker)) {
                final Set<String> bundleRanges = brokerToNamespaceToBundleRange.get(broker).get(namespaceName);
                if (bundleRanges == null) {
                    // Assume that when the namespace is absent, there are no bundles for this namespace assigned to
                    // that broker.
                    leastBundles = 0;
                    break;
                }
                leastBundles = Math.min(leastBundles, bundleRanges.size());
            } else {
                // Assume non-present brokers have 0 bundles.
                leastBundles = 0;
                break;
            }
        }
        if (leastBundles == 0) {
            // By assumption, the namespace name will not be present if there are no bundles in the namespace
            // assigned to the broker.
            candidates.removeIf(broker -> brokerToNamespaceToBundleRange.containsKey(broker)
                    && brokerToNamespaceToBundleRange.get(broker).containsKey(namespaceName));
        } else {
            final int finalLeastBundles = leastBundles;
            // We may safely assume that each broker has at least one bundle for this namespace.
            // Note that this case is far less likely since it implies that there are at least as many bundles for this
            // namespace as brokers.
            candidates.removeIf(broker -> brokerToNamespaceToBundleRange.get(broker).get(namespaceName)
                    .size() != finalLeastBundles);
        }
    }
}
