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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-naming")
public class NamespaceBundleTest {
    private NamespaceBundleFactory factory;

    @BeforeClass(alwaysRun = true)
    protected void initializeFactory() {
        factory = getNamespaceBundleFactory();
    }

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
        assertEquals((long) bundle.getKeyRange().lowerEndpoint(), 0L);
        assertEquals(bundle.getKeyRange().lowerBoundType(), BoundType.CLOSED);
        assertEquals((long) bundle.getKeyRange().upperEndpoint(), 1L);
        assertEquals(bundle.getKeyRange().upperBoundType(), BoundType.OPEN);
        assertEquals(bundle.getNamespaceObject().toString(), "pulsar/use/ns");
    }

    @SuppressWarnings("unchecked")
    private NamespaceBundleFactory getNamespaceBundleFactory() {
        PulsarService pulsar = mock(PulsarService.class);
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(store);
        when(pulsar.getConfigurationMetadataStore()).thenReturn(store);
        return NamespaceBundleFactory.createFactory(pulsar, Hashing.crc32());
    }

    @Test
    public void testGetBundle() {
        NamespaceBundle bundle = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED));
        assertNotNull(bundle);
        NamespaceBundle bundle2 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED));
        // Don't call equals and make sure those two are the same instance
        assertEquals(bundle, bundle2);

    }

    @Test
    public void testCompareTo() {
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
        assertEquals(bundle.compareTo(bundle3), 0);

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
        assertNotEquals(bundle2, bundle);

        NamespaceBundle bundle0 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertEquals(bundle, bundle0);

        NamespaceBundle otherBundle = factory.getBundle(NamespaceName.get("pulsar/use/ns2"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertNotEquals(bundle, otherBundle);
    }

    @Test
    public void testIncludes() {
        TopicName topicName = TopicName.get("persistent://pulsar/use/ns1/topic-1");
        Long hashKey = factory.getLongHashCode(topicName.toString());
        Long upper = Math.max(hashKey + 1, NamespaceBundles.FULL_UPPER_BOUND);
        BoundType upperType = upper.equals(NamespaceBundles.FULL_UPPER_BOUND) ? BoundType.CLOSED : BoundType.OPEN;
        NamespaceBundle bundle = factory.getBundle(topicName.getNamespaceObject(),
                Range.range(hashKey / 2, BoundType.CLOSED, upper, upperType));
        assertTrue(bundle.includes(topicName));
        bundle = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(upper, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND, BoundType.CLOSED));
        assertFalse(bundle.includes(topicName));

        NamespaceBundle otherBundle = factory.getBundle(NamespaceName.get("pulsar/use/ns2"),
                Range.range(0l, BoundType.CLOSED, 0x40000000L, BoundType.OPEN));
        assertFalse(otherBundle.includes(topicName));
    }

    @Test
    public void testToString() {
        NamespaceBundle bundle0 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0l, BoundType.CLOSED, 0x10000000L, BoundType.OPEN));
        assertEquals(bundle0.toString(), "pulsar/use/ns1/0x00000000_0x10000000");
        bundle0 = factory.getBundle(NamespaceName.get("pulsar/use/ns1"),
                Range.range(0x10000000l, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND, BoundType.CLOSED));
        assertEquals(bundle0.toString(), "pulsar/use/ns1/0x10000000_0xffffffff");
    }
}
