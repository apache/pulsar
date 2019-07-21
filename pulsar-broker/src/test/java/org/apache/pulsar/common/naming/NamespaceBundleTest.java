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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.testng.annotations.Test;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;

public class NamespaceBundleTest {
    private final NamespaceBundleFactory factory = getNamespaceBundleFactory();

    @Test
    public void testConstructor() {
        try {
            new NamespaceBundle(null, null, null);
            fail("Should have failed w/ null pointer exception");
        } catch (NullPointerException npe) {
            // OK, expected
        }

        try {
            new NamespaceBundle(NamespaceName.get("pulsar.old.ns"), null, null);
            fail("Should have failed w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            new NamespaceBundle(NamespaceName.get("pulsar/use/ns"),
                    Range.range(0L, BoundType.CLOSED, 0L, BoundType.OPEN), null);
            fail("Should have failed w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            new NamespaceBundle(NamespaceName.get("pulsar/use/ns"), Range.range(0L, BoundType.OPEN, 1L, BoundType.OPEN),
                    null);
            fail("Should have failed w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            new NamespaceBundle(NamespaceName.get("pulsar/use/ns"),
                    Range.range(1L, BoundType.CLOSED, 1L, BoundType.OPEN), null);
            fail("Should have failed w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            new NamespaceBundle(NamespaceName.get("pulsar/use/ns"),
                    Range.range(0L, BoundType.CLOSED, 1L, BoundType.CLOSED), null);
            fail("Should have failed w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            new NamespaceBundle(NamespaceName.get("pulsar/use/ns"),
                    Range.range(0L, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND, BoundType.OPEN), null);
            fail("Should have failed w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            new NamespaceBundle(NamespaceName.get("pulsar/use/ns"),
                    Range.range(0L, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND, BoundType.CLOSED), null);
            fail("Should have failed w/ null pointer exception");
        } catch (NullPointerException npe) {
            // OK, expected
        }

        NamespaceBundle bundle = new NamespaceBundle(NamespaceName.get("pulsar/use/ns"),
                Range.range(0L, BoundType.CLOSED, 1L, BoundType.OPEN), factory);
        assertTrue(bundle.getKeyRange().lowerEndpoint().equals(0L));
        assertEquals(bundle.getKeyRange().lowerBoundType(), BoundType.CLOSED);
        assertTrue(bundle.getKeyRange().upperEndpoint().equals(1L));
        assertEquals(bundle.getKeyRange().upperBoundType(), BoundType.OPEN);
        assertEquals(bundle.getNamespaceObject().toString(), "pulsar/use/ns");
    }

    @SuppressWarnings("unchecked")
    private NamespaceBundleFactory getNamespaceBundleFactory() {
        PulsarService pulsar = mock(PulsarService.class);
        LocalZooKeeperCacheService localZkCache = mock(LocalZooKeeperCacheService.class);
        ZooKeeperDataCache<LocalPolicies> poilciesCache = mock(ZooKeeperDataCache.class);
        when(pulsar.getLocalZkCacheService()).thenReturn(localZkCache);
        when(localZkCache.policiesCache()).thenReturn(poilciesCache);
        doNothing().when(poilciesCache).registerListener(any());
        return NamespaceBundleFactory.createFactory(pulsar, Hashing.crc32());
    }

    @Test
    public void testGetBundle() throws Exception {
        NamespaceBundle bundle = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED));
        assertNotNull(bundle);
        NamespaceBundle bundle2 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED));
        // Don't call equals and make sure those two are the same instance
        assertEquals(bundle, bundle2);

    }

    @Test
    public void testCompareTo() throws Exception {
        NamespaceBundle bundle = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        NamespaceBundle bundle2 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0x20000000l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        try {
            bundle.compareTo(bundle2);
            fail("Should have failed due to overlap ranges");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        NamespaceBundle bundle0 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x10000000L, BoundType.OPEN));
        assertTrue(bundle0.compareTo(bundle2) < 0);
        assertTrue(bundle2.compareTo(bundle0) > 0);
        NamespaceBundle bundle1 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x20000000L, BoundType.OPEN));
        assertTrue(bundle1.compareTo(bundle2) < 0);

        NamespaceBundle bundle3 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertTrue(bundle.compareTo(bundle3) == 0);

        NamespaceBundle otherBundle = factory.getBundle(NamespaceName.get("pulsar/use/ns2"),
                Range.range(0x10000000l, BoundType.CLOSED, 0x30000000L, BoundType.OPEN));
        assertTrue(otherBundle.compareTo(bundle0) > 0);
    }

    @Test
    public void testEquals() throws Exception {
        NamespaceBundle bundle = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        NamespaceBundle bundle2 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0x20000000l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertTrue(!bundle.equals(bundle2));

        NamespaceBundle bundle0 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertTrue(bundle0.equals(bundle));

        NamespaceBundle otherBundle = factory.getBundle(NamespaceName.get("pulsar/use/ns2"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertTrue(!otherBundle.equals(bundle));
    }

    @Test
    public void testIncludes() throws Exception {
        TopicName topicName = TopicName.get("persistent://pulsar/use/ns1/topic-1");
        Long hashKey = factory.getLongHashCode(topicName.toString());
        Long upper = Math.max(hashKey + 1, NamespaceBundles.FULL_UPPER_BOUND);
        BoundType upperType = upper.equals(NamespaceBundles.FULL_UPPER_BOUND) ? BoundType.CLOSED : BoundType.OPEN;
        NamespaceBundle bundle = factory.getBundle(topicName.getNamespaceObject(),
                Range.range(hashKey / 2, BoundType.CLOSED, upper, upperType));
        assertTrue(bundle.includes(topicName));
        bundle = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(upper, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND, BoundType.CLOSED));
        assertTrue(!bundle.includes(topicName));

        NamespaceBundle otherBundle = factory.getBundle(NamespaceName.get("pulsar/use/ns2"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertTrue(!otherBundle.includes(topicName));
    }

    @Test
    public void testToString() throws Exception {
        NamespaceBundle bundle0 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x10000000L, BoundType.OPEN));
        assertEquals(bundle0.toString(), "pulsar/use/ns1/0x00000000_0x10000000");
        bundle0 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0x10000000l, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND, BoundType.CLOSED));
        assertEquals(bundle0.toString(), "pulsar/use/ns1/0x10000000_0xffffffff");
    }
}
