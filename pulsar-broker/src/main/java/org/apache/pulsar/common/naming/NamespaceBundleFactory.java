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
import static org.apache.pulsar.common.policies.data.Policies.FIRST_BOUNDARY;
import static org.apache.pulsar.common.policies.data.Policies.LAST_BOUNDARY;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.hash.HashFunction;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.stats.CacheMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceBundleFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NamespaceBundleFactory.class);

    private final HashFunction hashFunc;

    private final AsyncLoadingCache<NamespaceName, NamespaceBundles> bundlesCache;

    private final PulsarService pulsar;
    private final MetadataCache<Policies> policiesCache;

    public NamespaceBundleFactory(PulsarService pulsar, HashFunction hashFunc) {
        this.hashFunc = hashFunc;

        this.bundlesCache = Caffeine.newBuilder()
                .recordStats()
                .buildAsync(this::loadBundles);

        CacheMetricsCollector.CAFFEINE.addCache("bundles", this.bundlesCache);

        pulsar.getLocalMetadataStore().registerListener(this::handleMetadataStoreNotification);

        this.pulsar = pulsar;
        this.policiesCache = pulsar.getConfigurationMetadataStore().getMetadataCache(Policies.class);
    }

    private CompletableFuture<NamespaceBundles> loadBundles(NamespaceName namespace, Executor executor) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading cache with bundles for {}", namespace);
        }

        if (pulsar == null) {
            return CompletableFuture.completedFuture(getBundles(namespace, Optional.empty()));
        }

        CompletableFuture<NamespaceBundles> future = new CompletableFuture<>();
        // Read the static bundle data from the policies
        pulsar.getPulsarResources().getLocalPolicies().getLocalPoliciesWithVersion(namespace).thenAccept(result -> {

            if (result.isPresent()) {
                try {
                    future.complete(readBundles(namespace,
                            result.get().getValue(), result.get().getStat().getVersion()));
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            } else {
                // If no local policies defined for namespace, copy from global config
                copyToLocalPolicies(namespace)
                        .thenAccept(b -> future.complete(b))
                        .exceptionally(ex -> {
                            future.completeExceptionally(ex);
                            return null;
                        });
            }
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });
        return future;
    }

    private NamespaceBundles readBundles(NamespaceName namespace, LocalPolicies localPolicies, long version)
            throws IOException {
        NamespaceBundles namespaceBundles = getBundles(namespace,
                Optional.of(Pair.of(localPolicies, version)));
        if (LOG.isDebugEnabled()) {
            LOG.debug("[{}] Get bundles from getLocalZkCacheService: bundles: {}, version: {}",
                    namespace,
                    (localPolicies.bundles.getBoundaries() != null) ? localPolicies.bundles : "null",
                    namespaceBundles.getVersion());
        }
        return namespaceBundles;
    }

    private CompletableFuture<NamespaceBundles> copyToLocalPolicies(NamespaceName namespace) {

        return pulsar.getPulsarResources().getNamespaceResources().getPoliciesAsync(namespace)
                .thenCompose(optPolicies -> {
                    if (!optPolicies.isPresent()) {
                        return CompletableFuture.completedFuture(getBundles(namespace, Optional.empty()));
                    }

                    Policies policies = optPolicies.get();
                    LocalPolicies localPolicies = new LocalPolicies(policies.bundles,
                            null,
                            null);

                    return pulsar.getPulsarResources().getLocalPolicies()
                            .createLocalPoliciesAsync(namespace, localPolicies)
                            .thenApply(stat -> getBundles(namespace,
                                    Optional.of(Pair.of(localPolicies, 0L))));
                });
    }

    private void handleMetadataStoreNotification(Notification n) {
        if (LocalPoliciesResources.isLocalPoliciesPath(n.getPath())) {
            try {
                final Optional<NamespaceName> namespace = NamespaceName.getIfValid(
                        getNamespaceFromPoliciesPath(n.getPath()));
                if (namespace.isPresent()) {
                    LOG.info("Policy updated for namespace {}, refreshing the bundle cache.", namespace);
                    // Trigger a background refresh to fetch new bundle data from the policies
                    bundlesCache.synchronous().invalidate(namespace.get());
                }
            } catch (Exception e) {
                LOG.error("Failed to update the policy change for path {}", n.getPath(), e);
            }
        }
    }

    public void invalidateBundleCache(NamespaceName namespace) {
        bundlesCache.synchronous().invalidate(namespace);
    }

    public CompletableFuture<NamespaceBundles> getBundlesAsync(NamespaceName nsname) {
        return bundlesCache.get(nsname);
    }

    public NamespaceBundle getBundlesWithHighestTopics(NamespaceName nsname) {
        try {
            return getBundlesWithHighestTopicsAsync(nsname).get(PulsarResources.DEFAULT_OPERATION_TIMEOUT_SEC,
                    TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.info("failed to derive bundle for {}", nsname, e);
            throw new IllegalStateException(e instanceof ExecutionException ? e.getCause() : e);
        }
    }

    public CompletableFuture<NamespaceBundle> getBundlesWithHighestTopicsAsync(NamespaceName nsname) {
        return pulsar.getPulsarResources().getTopicResources().listPersistentTopicsAsync(nsname).thenCompose(topics -> {
            return bundlesCache.get(nsname).handle((bundles, e) -> {
                Map<String, Integer> countMap = new HashMap<>();
                NamespaceBundle resultBundle = null;
                int maxCount = 0;
                for (String topic : topics) {
                    NamespaceBundle bundle = bundles.findBundle(TopicName.get(topic));
                    String bundleRange = bundles.findBundle(TopicName.get(topic)).getBundleRange();
                    int count = countMap.getOrDefault(bundleRange, 0) + 1;
                    countMap.put(bundleRange, count);
                    if (count > maxCount) {
                        maxCount = count;
                        resultBundle = bundle;
                    }
                }
                return resultBundle;
            });
        });
    }

    public NamespaceBundles getBundles(NamespaceName nsname) {
        return bundlesCache.synchronous().get(nsname);
    }

    public Optional<NamespaceBundles> getBundlesIfPresent(NamespaceName nsname) {
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
        return new NamespaceBundles(nsname, this, Optional.empty(), NamespaceBundles.getPartitions(bundleData));
    }

    private NamespaceBundles getBundles(NamespaceName nsname, Optional<Pair<LocalPolicies, Long>> localPolicies) {
        return new NamespaceBundles(nsname, this, localPolicies);
    }

    /**
     * Fetches {@link NamespaceBundles} from cache for a given namespace. finds target bundle, split into numBundles and
     * returns new {@link NamespaceBundles} with newly split bundles into it.
     *
     * @param targetBundle
     *            {@link NamespaceBundle} needs to be split
     * @param argNumBundles
     *            split into numBundles
     * @param splitBoundary
     *            split into 2 numBundles by the given split key. The given split key must between the key range of the
     *            given split bundle.
     * @return List of split {@link NamespaceBundle} and {@link NamespaceBundles} that contains final bundles including
     *         split bundles for a given namespace
     */
    public CompletableFuture<Pair<NamespaceBundles, List<NamespaceBundle>>> splitBundles(
            NamespaceBundle targetBundle, int argNumBundles, Long splitBoundary) {
        checkArgument(canSplitBundle(targetBundle), "%s bundle can't be split further", targetBundle);
        if (splitBoundary != null) {
            checkArgument(splitBoundary > targetBundle.getLowerEndpoint()
                            && splitBoundary < targetBundle.getUpperEndpoint(),
                    "The given fixed key must between the key range of the %s bundle", targetBundle);
            argNumBundles = 2;
        }
        checkNotNull(targetBundle, "can't split null bundle");
        checkNotNull(targetBundle.getNamespaceObject(), "namespace must be present");
        NamespaceName nsname = targetBundle.getNamespaceObject();

        final int numBundles = argNumBundles;

        return bundlesCache.get(nsname).thenApply(sourceBundle -> {
            final int lastIndex = sourceBundle.partitions.length - 1;

            final long[] partitions = new long[sourceBundle.partitions.length + (numBundles - 1)];
            int pos = 0;
            int splitPartition = -1;
            final Range<Long> range = targetBundle.getKeyRange();
            for (int i = 0; i < lastIndex; i++) {
                if (sourceBundle.partitions[i] == range.lowerEndpoint()
                        && (range.upperEndpoint() == sourceBundle.partitions[i + 1])) {
                    splitPartition = i;
                    long maxVal = sourceBundle.partitions[i + 1];
                    long minVal = sourceBundle.partitions[i];
                    long segSize = splitBoundary == null ? (maxVal - minVal) / numBundles : splitBoundary - minVal;
                    partitions[pos++] = minVal;
                    long curPartition = minVal + segSize;
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
                NamespaceBundles splitNsBundles =
                        new NamespaceBundles(nsname, this, sourceBundle.getLocalPolicies(), partitions);
                List<NamespaceBundle> splitBundles = splitNsBundles.getBundles().subList(splitPartition,
                        (splitPartition + numBundles));
                return new ImmutablePair<>(splitNsBundles, splitBundles);
            }

            return null;
        });
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
