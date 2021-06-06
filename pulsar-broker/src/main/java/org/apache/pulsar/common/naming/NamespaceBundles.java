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
import static org.apache.pulsar.common.policies.data.Policies.FIRST_BOUNDARY;
import static org.apache.pulsar.common.policies.data.Policies.LAST_BOUNDARY;
import com.google.common.base.Objects;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;

public class NamespaceBundles {
    private final NamespaceName nsname;
    private final ArrayList<NamespaceBundle> bundles;
    private final NamespaceBundleFactory factory;

    protected final long[] partitions;

    public static final Long FULL_LOWER_BOUND = 0x00000000L;
    public static final Long FULL_UPPER_BOUND = 0xffffffffL;
    private final NamespaceBundle fullBundle;

    private final Optional<Pair<LocalPolicies, Long>> localPolicies;

    public NamespaceBundles(NamespaceName nsname, NamespaceBundleFactory factory,
                            Optional<Pair<LocalPolicies, Long>> localPolicies) {
        this(nsname, factory, localPolicies, getPartitions(localPolicies.map(Pair::getLeft)));
    }

    NamespaceBundles(NamespaceName nsname, NamespaceBundleFactory factory,
                     Optional<Pair<LocalPolicies, Long>> localPolicies, Collection<Long> partitions) {
        this(nsname, factory, localPolicies, getPartitions(partitions));
    }

    NamespaceBundles(NamespaceName nsname, NamespaceBundleFactory factory,
                     Optional<Pair<LocalPolicies, Long>> localPolicies, long[] partitions) {
        // check input arguments
        this.nsname = checkNotNull(nsname);
        this.factory = checkNotNull(factory);
        this.localPolicies = localPolicies;
        checkArgument(partitions.length > 0, "Can't create bundles w/o partition boundaries");

        // calculate bundles based on partition boundaries
        this.bundles = Lists.newArrayList();
        fullBundle = new NamespaceBundle(nsname,
                Range.range(FULL_LOWER_BOUND, BoundType.CLOSED, FULL_UPPER_BOUND, BoundType.CLOSED), factory);

        if (partitions.length == 1) {
            throw new IllegalArgumentException("Need to specify at least 2 boundaries");
        }

        this.partitions = partitions;
        long lowerBound = partitions[0];
        for (int i = 1; i < partitions.length; i++) {
            long upperBound = partitions[i];
            checkArgument(upperBound >= lowerBound);
            Range<Long> newRange = null;
            if (i != partitions.length - 1) {
                newRange = Range.range(lowerBound, BoundType.CLOSED, upperBound, BoundType.OPEN);
            } else {
                // last one has a closed right end
                newRange = Range.range(lowerBound, BoundType.CLOSED, upperBound, BoundType.CLOSED);
            }
            bundles.add(new NamespaceBundle(nsname, newRange, factory));
            lowerBound = upperBound;
        }
    }

    public NamespaceBundle findBundle(TopicName topicName) {
        checkArgument(this.nsname.equals(topicName.getNamespaceObject()));
        long hashCode = factory.getLongHashCode(topicName.toString());
        NamespaceBundle bundle = getBundle(hashCode);
        if (topicName.getDomain().equals(TopicDomain.non_persistent)) {
            bundle.setHasNonPersistentTopic(true);
        }
        return bundle;
    }

    public List<NamespaceBundle> getBundles() {
        return bundles;
    }

    public int size() {
        return bundles.size();
    }

    public void validateBundle(NamespaceBundle nsBundle) throws Exception {
        int idx = Arrays.binarySearch(partitions, nsBundle.getLowerEndpoint());
        checkArgument(idx >= 0, "Cannot find bundle in the bundles list");
        checkArgument(nsBundle.getUpperEndpoint().equals(bundles.get(idx).getUpperEndpoint()),
                "Invalid upper boundary for bundle");
    }

    public NamespaceBundle getFullBundle() {
        return fullBundle;
    }

    protected NamespaceBundle getBundle(long hash) {
        int idx = Arrays.binarySearch(partitions, hash);
        int lowerIdx = idx < 0 ? -(idx + 2) : idx;
        return bundles.get(lowerIdx);
    }

    private static long[] convertPartitions(SortedSet<Long> partitionsSet) {
        checkNotNull(partitionsSet);
        long[] partitions = new long[partitionsSet.size()];
        int idx = 0;
        for (long p : partitionsSet) {
            partitions[idx++] = p;
        }

        return partitions;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nsname, bundles);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof NamespaceBundles) {
            NamespaceBundles other = (NamespaceBundles) obj;
            return (Objects.equal(this.nsname, other.nsname) && Objects.equal(this.bundles, other.bundles));
        }
        return false;
    }

    public Optional<Pair<LocalPolicies, Long>> getLocalPolicies() {
        return localPolicies;
    }

    public Optional<Long> getVersion() {
        return localPolicies.map(Pair::getRight);
    }

    static long[] getPartitions(BundlesData bundlesData) {
        if (bundlesData == null) {
            return new long[]{Long.decode(FIRST_BOUNDARY), Long.decode(LAST_BOUNDARY)};
        } else {
            List<String> boundaries = bundlesData.getBoundaries();
            long[] partitions = new long[boundaries.size()];
            for (int i = 0; i < boundaries.size(); i++) {
                partitions[i] = Long.decode(boundaries.get(i));
            }

            return partitions;
        }
    }

    private static long[] getPartitions(Optional<LocalPolicies> lp) {
        return getPartitions(lp.map(x -> x.bundles).orElse(null));
    }

    private static long[] getPartitions(Collection<Long> partitions) {
        long[] res = new long[partitions.size()];
        int i = 0;
        for (long p : partitions) {
            res[i++] = p;
        }
        return res;
    }

    public BundlesData getBundlesData() {
        List<String> boundaries = Arrays.stream(partitions)
                .boxed()
                .map(p -> format("0x%08x", p))
                .collect(Collectors.toList());
        return BundlesData.builder()
                .boundaries(boundaries)
                .numBundles(boundaries.size() - 1)
                .build();
    }

    public LocalPolicies toLocalPolicies() {
        return new LocalPolicies(this.getBundlesData(),
                localPolicies.map(lp -> lp.getLeft().bookieAffinityGroup).orElse(null),
                localPolicies.map(lp -> lp.getLeft().namespaceAntiAffinityGroup).orElse(null));
    }
}
