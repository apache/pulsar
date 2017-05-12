/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.common.naming.NamespaceName;

public class LoadBalancerTestingUtils {
    public static NamespaceBundle[] makeBundles(final NamespaceBundleFactory nsFactory, final String property,
            final String cluster, final String namespace, final int numBundles) {
        final NamespaceBundle[] result = new NamespaceBundle[numBundles];
        final NamespaceName namespaceName = new NamespaceName(property, cluster, namespace);
        for (int i = 0; i < numBundles - 1; ++i) {
            final long lower = NamespaceBundles.FULL_UPPER_BOUND * i / numBundles;
            final long upper = NamespaceBundles.FULL_UPPER_BOUND * (i + 1) / numBundles;
            result[i] = nsFactory.getBundle(namespaceName, Range.range(lower, BoundType.CLOSED, upper, BoundType.OPEN));
        }
        result[numBundles - 1] = nsFactory.getBundle(namespaceName,
                Range.range(NamespaceBundles.FULL_UPPER_BOUND * (numBundles - 1) / numBundles, BoundType.CLOSED,
                        NamespaceBundles.FULL_UPPER_BOUND, BoundType.CLOSED));
        return result;
    }
}
