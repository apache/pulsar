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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;

import com.google.common.base.Objects;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

public class NamespaceBundles {
    private final NamespaceName nsname;
    private final ArrayList<NamespaceBundle> bundles;
    private final NamespaceBundleFactory factory;
    private final long version;

    protected final long[] partitions;

    public static final Long FULL_LOWER_BOUND = 0x00000000L;
    public static final Long FULL_UPPER_BOUND = 0xffffffffL;
    private final NamespaceBundle fullBundle;

    public NamespaceBundles(NamespaceName nsname, SortedSet<Long> partitionsSet, NamespaceBundleFactory factory)
            throws Exception {
        this(nsname, convertPartitions(partitionsSet), factory);
    }

    public NamespaceBundles(NamespaceName nsname, long[] partitions, NamespaceBundleFactory factory, long version) {
        // check input arguments
        this.nsname = checkNotNull(nsname);
        this.factory = checkNotNull(factory);
        this.version = version;
        checkArgument(partitions.length > 0, "Can't create bundles w/o partition boundaries");

        // calculate bundles based on partition boundaries
        this.bundles = Lists.newArrayList();
        fullBundle = new NamespaceBundle(nsname,
            Range.range(FULL_LOWER_BOUND, BoundType.CLOSED, FULL_UPPER_BOUND, BoundType.CLOSED), factory);

        if (partitions.length > 0) {
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
        } else {
            this.partitions = new long[] { 0l };
            bundles.add(fullBundle);
        }
    }

    public NamespaceBundles(NamespaceName nsname, long[] partitions, NamespaceBundleFactory factory) {
        this(nsname, partitions, factory, -1);
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

    private static final long[] convertPartitions(SortedSet<Long> partitionsSet) {
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

    public long getVersion() {
        return version;
    }
}
