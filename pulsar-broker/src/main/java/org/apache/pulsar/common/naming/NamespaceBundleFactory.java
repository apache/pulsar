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
package org.apache.pulsar.common.naming;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;
import static org.apache.pulsar.common.policies.data.Policies.FIRST_BOUNDARY;
import static org.apache.pulsar.common.policies.data.Policies.LAST_BOUNDARY;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.stats.CacheMetricsCollector;
import org.apache.pulsar.zookeeper.ZooKeeperCacheListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.hash.HashFunction;

public class NamespaceBundleFactory implements ZooKeeperCacheListener<LocalPolicies> {
    private static final Logger LOG = LoggerFactory.getLogger(NamespaceBundleFactory.class);

    private final HashFunction hashFunc;

    private final AsyncLoadingCache<NamespaceName, NamespaceBundles> bundlesCache;

    private final PulsarService pulsar;

    public NamespaceBundleFactory(PulsarService pulsar, HashFunction hashFunc) {
        this.hashFunc = hashFunc;

        this.bundlesCache = Caffeine.newBuilder()
                .recordStats()
                .buildAsync((NamespaceName namespace, Executor executor) -> {
            String path = AdminResource.joinPath(LOCAL_POLICIES_ROOT, namespace.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Loading cache with bundles for {}", namespace);
            }

            if (pulsar == null || pulsar.getConfigurationCache() == null) {
                return CompletableFuture.completedFuture(getBundles(namespace, null));
            }

            CompletableFuture<NamespaceBundles> future = new CompletableFuture<>();
            // Read the static bundle data from the policies
            pulsar.getLocalZkCacheService().policiesCache().getWithStatAsync(path).thenAccept(result -> {
                // If no policies defined for namespace, assume 1 single bundle
                BundlesData bundlesData = result.map(Entry::getKey).map(p -> p.bundles).orElse(null);
                NamespaceBundles namespaceBundles = getBundles(
                    namespace, bundlesData, result.map(Entry::getValue).map(s -> s.getVersion()).orElse(-1));

                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Get bundles from getLocalZkCacheService: path: {},  bundles: {}, version: {}",
                        namespace, path,
                        (bundlesData != null && bundlesData.boundaries != null) ? bundlesData.toString() : "null",
                        namespaceBundles.getVersion());
                }

                future.complete(namespaceBundles);
            }).exceptionally(ex -> {
                future.completeExceptionally(ex);
                return null;
            });
            return future;
        });

        CacheMetricsCollector.CAFFEINE.addCache("bundles", this.bundlesCache);

        // local-policies have been changed which has contains namespace bundles
        pulsar.getLocalZkCacheService().policiesCache()
                .registerListener((String path, LocalPolicies data, Stat stat) -> {
                    String[] paths = path.split(LOCAL_POLICIES_ROOT + "/");
                    if (paths.length == 2) {
                        invalidateBundleCache(NamespaceName.get(paths[1]));
                    }
                });

        if (pulsar != null && pulsar.getConfigurationCache() != null) {
            pulsar.getLocalZkCacheService().policiesCache().registerListener(this);
        }

        this.pulsar = pulsar;
    }

    @Override
    public void onUpdate(String path, LocalPolicies data, Stat stat) {
        final NamespaceName namespace = NamespaceName.get(getNamespaceFromPoliciesPath(path));

        try {
            LOG.info("Policy updated for namespace {}, refreshing the bundle cache.", namespace);
            // invalidate the bundle cache to fetch new bundle data from the policies
            bundlesCache.synchronous().invalidate(namespace);
        } catch (Exception e) {
            LOG.error("Failed to update the policy change for ns {}", namespace, e);
        }
    }

    /**
     * checks if the local broker is the owner of the namespace bundle
     *
     * @param nsBundle
     * @return
     */
    private boolean isOwner(NamespaceBundle nsBundle) {
        if (pulsar != null) {
            return pulsar.getNamespaceService().getOwnershipCache().getOwnedBundle(nsBundle) != null;
        }
        return false;
    }

    public void invalidateBundleCache(NamespaceName namespace) {
        pulsar.getLocalZkCacheService().policiesCache().invalidate(
                AdminResource.joinPath(LOCAL_POLICIES_ROOT, namespace.toString()));
        bundlesCache.synchronous().invalidate(namespace);
    }

    public CompletableFuture<NamespaceBundles> getBundlesAsync(NamespaceName nsname) {
        return bundlesCache.get(nsname);
    }

    public NamespaceBundles getBundles(NamespaceName nsname) throws Exception {
        return bundlesCache.synchronous().get(nsname);
    }

    public Optional<NamespaceBundles> getBundlesIfPresent(NamespaceName nsname) throws Exception {
        return Optional.ofNullable(bundlesCache.synchronous().getIfPresent(nsname));
    }

    public NamespaceBundle getBundle(NamespaceName nsname, Range<Long> hashRange) {
        return new NamespaceBundle(nsname, hashRange, this);
    }

    public NamespaceBundle getBundle(String namespace, String bundleRange) {
        checkArgument(bundleRange.contains("_"), "Invalid bundle range");
        String[] boundaries = bundleRange.split("_");
        Long lowerEndpoint = Long.decode(boundaries[0]);
        Long upperEndpoint = Long.decode(boundaries[1]);
        Range<Long> hashRange = Range.range(lowerEndpoint, BoundType.CLOSED, upperEndpoint,
                (upperEndpoint.equals(NamespaceBundles.FULL_UPPER_BOUND)) ? BoundType.CLOSED : BoundType.OPEN);
        return getBundle(NamespaceName.get(namespace), hashRange);
    }

    public NamespaceBundle getFullBundle(NamespaceName fqnn) throws Exception {
        return bundlesCache.synchronous().get(fqnn).getFullBundle();
    }

    public CompletableFuture<NamespaceBundle> getFullBundleAsync(NamespaceName fqnn) {
        return bundlesCache.get(fqnn).thenApply(NamespaceBundles::getFullBundle);
    }

    public long getLongHashCode(String name) {
        return this.hashFunc.hashString(name, Charsets.UTF_8).padToLong();
    }

    public NamespaceBundles getBundles(NamespaceName nsname, BundlesData bundleData) {
        return getBundles(nsname, bundleData, -1);
    }

    public NamespaceBundles getBundles(NamespaceName nsname, BundlesData bundleData, long version) {
        long[] partitions;
        if (bundleData == null) {
            partitions = new long[] { Long.decode(FIRST_BOUNDARY), Long.decode(LAST_BOUNDARY) };
        } else {
            partitions = new long[bundleData.boundaries.size()];
            for (int i = 0; i < bundleData.boundaries.size(); i++) {
                partitions[i] = Long.decode(bundleData.boundaries.get(i));
            }
        }
        return new NamespaceBundles(nsname, partitions, this, version);
    }

    public static BundlesData getBundlesData(NamespaceBundles bundles) throws Exception {
        if (bundles == null) {
            return new BundlesData();
        } else {
            List<String> boundaries = Arrays.stream(bundles.partitions).boxed().map(p -> format("0x%08x", p))
                    .collect(Collectors.toList());
            return new BundlesData(boundaries);
        }
    }

    /**
     * Fetches {@link NamespaceBundles} from cache for a given namespace. finds target bundle, split into numBundles and
     * returns new {@link NamespaceBundles} with newly split bundles into it.
     *
     * @param targetBundle
     *            {@link NamespaceBundle} needs to be split
     * @param numBundles
     *            split into numBundles
     * @param splitBoundary
     *            split into 2 numBundles by the given split key. The given split key must between the key range of the
     *            given split bundle.
     * @return List of split {@link NamespaceBundle} and {@link NamespaceBundles} that contains final bundles including
     *         split bundles for a given namespace
     */
    public Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles(NamespaceBundle targetBundle, int numBundles, Long splitBoundary) {
        checkArgument(canSplitBundle(targetBundle), "%s bundle can't be split further", targetBundle);
        if (splitBoundary != null) {
            checkArgument(splitBoundary > targetBundle.getLowerEndpoint() && splitBoundary < targetBundle.getUpperEndpoint(),
                "The given fixed key must between the key range of the %s bundle", targetBundle);
            numBundles = 2;
        }
        checkNotNull(targetBundle, "can't split null bundle");
        checkNotNull(targetBundle.getNamespaceObject(), "namespace must be present");
        NamespaceName nsname = targetBundle.getNamespaceObject();
        NamespaceBundles sourceBundle = bundlesCache.synchronous().get(nsname);

        final int lastIndex = sourceBundle.partitions.length - 1;

        final long[] partitions = new long[sourceBundle.partitions.length + (numBundles - 1)];
        int pos = 0;
        int splitPartition = -1;
        final Range<Long> range = targetBundle.getKeyRange();
        for (int i = 0; i < lastIndex; i++) {
            if (sourceBundle.partitions[i] == range.lowerEndpoint()
                    && (range.upperEndpoint() == sourceBundle.partitions[i + 1])) {
                splitPartition = i;
                Long maxVal = sourceBundle.partitions[i + 1];
                Long minVal = sourceBundle.partitions[i];
                Long segSize = splitBoundary == null ? (maxVal - minVal) / numBundles : splitBoundary - minVal;
                partitions[pos++] = minVal;
                Long curPartition = minVal + segSize;
                for (int j = 0; j < numBundles - 1; j++) {
                    partitions[pos++] = curPartition;
                    curPartition += segSize;
                }
            } else {
                partitions[pos++] = sourceBundle.partitions[i];
            }
        }
        partitions[pos] = sourceBundle.partitions[lastIndex];
        if (splitPartition != -1) {
            // keep version of sourceBundle
            NamespaceBundles splittedNsBundles = new NamespaceBundles(nsname, partitions, this, sourceBundle.getVersion());
            List<NamespaceBundle> splittedBundles = splittedNsBundles.getBundles().subList(splitPartition,
                    (splitPartition + numBundles));
            return new ImmutablePair<NamespaceBundles, List<NamespaceBundle>>(splittedNsBundles, splittedBundles);
        }
        return null;
    }

    public boolean canSplitBundle(NamespaceBundle bundle) {
        Range<Long> range = bundle.getKeyRange();
        return range.upperEndpoint() - range.lowerEndpoint() > 1;
    }

    public static void validateFullRange(SortedSet<String> partitions) {
        checkArgument(partitions.first().equals(FIRST_BOUNDARY) && partitions.last().equals(LAST_BOUNDARY));
    }

    public static NamespaceBundleFactory createFactory(PulsarService pulsar, HashFunction hashFunc) {
        return new NamespaceBundleFactory(pulsar, hashFunc);
    }

    public static boolean isFullBundle(String bundleRange) {
        return bundleRange.equals(String.format("%s_%s", FIRST_BOUNDARY, LAST_BOUNDARY));
    }

    public static String getDefaultBundleRange() {
        return String.format("%s_%s", FIRST_BOUNDARY, LAST_BOUNDARY);
    }

    /*
     * @param path - path for the namespace policies ex. /admin/policies/prop/cluster/namespace
     *
     * @returns namespace with path, ex. prop/cluster/namespace
     */
    public static String getNamespaceFromPoliciesPath(String path) {
        if (path.isEmpty()) {
            return path;
        }
        // String before / is considered empty string by splitter
        Iterable<String> splitter = Splitter.on("/").limit(6).split(path);
        Iterator<String> i = splitter.iterator();
        // skip first three - "","admin", "policies"
        i.next();
        i.next();
        i.next();
        // prop, cluster, namespace
        return Joiner.on("/").join(i);
    }

}
