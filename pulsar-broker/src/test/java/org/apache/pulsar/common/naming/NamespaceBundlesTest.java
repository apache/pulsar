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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-naming")
public class NamespaceBundlesTest {

    private NamespaceBundleFactory factory;

    @BeforeMethod(alwaysRun = true)
    protected void initializeFactory() {
        factory = getNamespaceBundleFactory();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConstructor() throws Exception {

        long[] partitions = new long[]{0L, 0x10000000L, 0x40000000L, 0xffffffffL};

        NamespaceBundles bundles = new NamespaceBundles(NamespaceName.get("pulsar/use/ns2"), factory, Optional.empty(), partitions);
        Field partitionField = NamespaceBundles.class.getDeclaredField("partitions");
        Field nsField = NamespaceBundles.class.getDeclaredField("nsname");
        Field bundlesField = NamespaceBundles.class.getDeclaredField("bundles");
        partitionField.setAccessible(true);
        nsField.setAccessible(true);
        bundlesField.setAccessible(true);
        long[] partFld = (long[]) partitionField.get(bundles);
        // the same instance
        assertEquals(partitions.length, partFld.length);
        NamespaceName nsFld = (NamespaceName) nsField.get(bundles);
        assertEquals(nsFld.toString(), "pulsar/use/ns2");
        ArrayList<NamespaceBundle> bundleList = (ArrayList<NamespaceBundle>) bundlesField.get(bundles);
        assertEquals(bundleList.size(), 3);
        assertEquals(bundleList.get(0),
                factory.getBundle(nsFld, Range.range(0L, BoundType.CLOSED, 0x10000000L, BoundType.OPEN)));
        assertEquals(bundleList.get(1),
                factory.getBundle(nsFld, Range.range(0x10000000L, BoundType.CLOSED, 0x40000000L, BoundType.OPEN)));
        assertEquals(bundleList.get(2),
                factory.getBundle(nsFld, Range.range(0x40000000L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED)));
    }

    @SuppressWarnings("unchecked")
    private NamespaceBundleFactory getNamespaceBundleFactory() {
        PulsarService pulsar = mock(PulsarService.class);
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(store);
        when(pulsar.getConfigurationMetadataStore()).thenReturn(store);

        PulsarResources resources = mock(PulsarResources.class);
        when(pulsar.getPulsarResources()).thenReturn(resources);
        when(resources.getLocalPolicies()).thenReturn(mock(LocalPoliciesResources.class));
        when(resources.getLocalPolicies().getLocalPoliciesWithVersion(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.empty()));

        when(resources.getNamespaceResources()).thenReturn(mock(NamespaceResources.class));
        when(resources.getNamespaceResources().getPoliciesAsync(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.empty()));
        return NamespaceBundleFactory.createFactory(pulsar, Hashing.crc32());
    }

    @Test
    public void testFindBundle() throws Exception {
        SortedSet<Long> partitions = Sets.newTreeSet();
        partitions.add(0L);
        partitions.add(0x40000000L);
        partitions.add(0xa0000000L);
        partitions.add(0xb0000000L);
        partitions.add(0xc0000000L);
        partitions.add(0xffffffffL);
        NamespaceBundles bundles = new NamespaceBundles(NamespaceName.get("pulsar/global/ns1"),
                factory, Optional.empty(), partitions);
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundle bundle = bundles.findBundle(topicName);
        assertTrue(bundle.includes(topicName));

        topicName = TopicName.get("persistent://pulsar/use/ns2/topic-2");
        try {
            bundles.findBundle(topicName);
            fail("Should have failed due to mismatched namespace name");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        Long hashKey = factory.getLongHashCode(topicName.toString());
        // The following code guarantees that we have at least two ranges after the hashKey till the end
        SortedSet<Long> tailSet = partitions.tailSet(hashKey);
        tailSet.add(hashKey);
        // Now, remove the first range to ensure the hashKey is not included in <code>newPar</code>
        Iterator<Long> iter = tailSet.iterator();
        iter.next();
        SortedSet<Long> newPar = tailSet.tailSet(iter.next());

        try {
            bundles = new NamespaceBundles(topicName.getNamespaceObject(), factory, Optional.empty(), newPar);
            bundles.findBundle(topicName);
            fail("Should have failed due to out-of-range");
        } catch (IndexOutOfBoundsException iae) {
            // OK, expected
        }
    }

    @Test
    public void testSplitBundles() throws Exception {
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = factory.getBundles(nsname);
        NamespaceBundle bundle = bundles.findBundle(topicName);
        final int numberSplitBundles = 4;
        // (1) split in 4
        Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles = factory.splitBundles(bundle, numberSplitBundles,
                null).join();
        // existing_no_bundles(1) +
        // additional_new_split_bundle(4) -
        // parent_target_bundle(1)
        int totalExpectedSplitBundles = bundles.getBundles().size() + numberSplitBundles - 1;
        validateSplitBundlesRange(bundles.getFullBundle(), splitBundles.getRight());
        assertEquals(totalExpectedSplitBundles, splitBundles.getLeft().getBundles().size());

        // (2) split in 4: first bundle from above split bundles
        NamespaceBundleFactory utilityFactory = getNamespaceBundleFactory();
        NamespaceBundles bundles2 = splitBundles.getLeft();
        NamespaceBundle testChildBundle = bundles2.getBundles().get(0);

        Pair<NamespaceBundles, List<NamespaceBundle>> splitChildBundles =
                splitBundlesUtilFactory(
                        utilityFactory,
                        nsname,
                        bundles2,
                        testChildBundle,
                        numberSplitBundles);
        // existing_no_bundles(4) +
        // additional_new_split_bundle(4) -
        // parent_target_bundle(1)
        totalExpectedSplitBundles = bundles2.getBundles().size() + numberSplitBundles - 1;
        validateSplitBundlesRange(testChildBundle, splitChildBundles.getRight());
        assertEquals(totalExpectedSplitBundles, splitChildBundles.getLeft().getBundles().size());

        // (3) split in 3: second bundle from above split bundles
        NamespaceBundle testChildBundl2 = bundles2.getBundles().get(1);

        Pair<NamespaceBundles, List<NamespaceBundle>> splitChildBundles2 =
                splitBundlesUtilFactory(
                        utilityFactory,
                        nsname,
                        bundles2,
                        testChildBundl2,
                        3);
        // existing_no_bundles(4) +
        // additional_new_split_bundle(3) -
        // parent_target_bundle(1)
        totalExpectedSplitBundles = bundles2.getBundles().size() + 3 - 1;
        validateSplitBundlesRange(testChildBundl2, splitChildBundles2.getRight());
        assertEquals(totalExpectedSplitBundles, splitChildBundles2.getLeft().getBundles().size());
    }

    @Test
    public void testSplitBundleInTwo() throws Exception {
        final int NO_BUNDLES = 2;
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = factory.getBundles(nsname);
        NamespaceBundle bundle = bundles.findBundle(topicName);
        // (1) split : [0x00000000,0xffffffff] => [0x00000000_0x7fffffff,0x7fffffff_0xffffffff]
        Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles = factory.splitBundles(bundle, NO_BUNDLES,
                null).join();
        assertNotNull(splitBundles);
        assertBundleDivideInTwo(bundle, splitBundles.getRight());

        // (2) split: [0x00000000,0x7fffffff] => [0x00000000_0x3fffffff,0x3fffffff_0x7fffffff],
        // [0x7fffffff,0xffffffff] => [0x7fffffff_0xbfffffff,0xbfffffff_0xffffffff]
        NamespaceBundleFactory utilityFactory = getNamespaceBundleFactory();
        assertBundles(utilityFactory, nsname, bundle, splitBundles, NO_BUNDLES);

        // (3) split: [0x00000000,0x3fffffff] => [0x00000000_0x1fffffff,0x1fffffff_0x3fffffff],
        // [0x3fffffff,0x7fffffff] => [0x3fffffff_0x5fffffff,0x5fffffff_0x7fffffff]
        Pair<NamespaceBundles, List<NamespaceBundle>> splitChildBundles = splitBundlesUtilFactory(utilityFactory,
                nsname, splitBundles.getLeft(), splitBundles.getRight().get(0), NO_BUNDLES);
        assertBundles(utilityFactory, nsname, splitBundles.getRight().get(0), splitChildBundles, NO_BUNDLES);

        // (4) split: [0x7fffffff,0xbfffffff] => [0x7fffffff_0x9fffffff,0x9fffffff_0xbfffffff],
        // [0xbfffffff,0xffffffff] => [0xbfffffff_0xdfffffff,0xdfffffff_0xffffffff]
        splitChildBundles = splitBundlesUtilFactory(utilityFactory, nsname, splitBundles.getLeft(),
                splitBundles.getRight().get(1), NO_BUNDLES);
        assertBundles(utilityFactory, nsname, splitBundles.getRight().get(1), splitChildBundles, NO_BUNDLES);

    }

    @Test
    public void testSplitBundleByFixBoundary() throws Exception {
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        NamespaceBundles bundles = factory.getBundles(nsname);
        NamespaceBundle bundleToSplit = bundles.getBundles().get(0);

        try {
            factory.splitBundles(bundleToSplit, 0, bundleToSplit.getLowerEndpoint());
        } catch (IllegalArgumentException e) {
            //No-op
        }
        try {
            factory.splitBundles(bundleToSplit, 0, bundleToSplit.getUpperEndpoint());
        } catch (IllegalArgumentException e) {
            //No-op
        }

        Long fixBoundary = bundleToSplit.getLowerEndpoint() + 10;
        Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles = factory.splitBundles(bundleToSplit,
                0, fixBoundary).join();
        assertEquals(splitBundles.getRight().get(0).getLowerEndpoint(), bundleToSplit.getLowerEndpoint());
        assertEquals(splitBundles.getRight().get(1).getLowerEndpoint().longValue(), bundleToSplit.getLowerEndpoint() + fixBoundary);
    }

    private void validateSplitBundlesRange(NamespaceBundle fullBundle, List<NamespaceBundle> splitBundles) {
        assertNotNull(fullBundle);
        assertNotNull(splitBundles);
        Range<Long> fullRange = fullBundle.getKeyRange();
        Range<Long> span = splitBundles.get(0).getKeyRange();
        for (NamespaceBundle bundle : splitBundles) {
            span = span.span(bundle.getKeyRange());
        }
        assertEquals(span, fullRange);
    }

    @SuppressWarnings("unchecked")
    private Pair<NamespaceBundles, List<NamespaceBundle>> splitBundlesUtilFactory(NamespaceBundleFactory utilityFactory,
            NamespaceName nsname, NamespaceBundles bundles, NamespaceBundle targetBundle, int numBundles)
            throws Exception {
        Field bCacheField = NamespaceBundleFactory.class.getDeclaredField("bundlesCache");
        bCacheField.setAccessible(true);
        ((AsyncLoadingCache<NamespaceName, NamespaceBundles>) bCacheField.get(utilityFactory)).put(nsname,
                CompletableFuture.completedFuture(bundles));
        return utilityFactory.splitBundles(targetBundle, numBundles, null).join();
    }

    private void assertBundles(NamespaceBundleFactory utilityFactory,
                               NamespaceName nsname,
                               NamespaceBundle bundle,
                               Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles,
                               int numBundles) throws Exception {

        NamespaceBundle bundle1 = splitBundles.getRight().get(0);
        NamespaceBundle bundle2 = splitBundles.getRight().get(1);

        NamespaceBundles nspaceBundles = splitBundles.getLeft();
        Pair<NamespaceBundles, List<NamespaceBundle>> bundle1Split = splitBundlesUtilFactory(utilityFactory, nsname,
                nspaceBundles, bundle1, numBundles);
        assertBundleDivideInTwo(bundle1, bundle1Split.getRight());

        Pair<NamespaceBundles, List<NamespaceBundle>> bundle2Split = splitBundlesUtilFactory(utilityFactory, nsname,
                nspaceBundles, bundle2, numBundles);
        assertBundleDivideInTwo(bundle2, bundle2Split.getRight());

    }

    private void assertBundleDivideInTwo(NamespaceBundle bundle,
                                         List<NamespaceBundle> bundles) {
        assertEquals(bundles.size(), 2);
        String[] range = bundle.getBundleRange().split("_");
        long lower = Long.decode(range[0]);
        long upper = Long.decode(range[1]);
        long middle = ((upper - lower) / 2) + lower;

        String lRange = String.format("0x%08x_0x%08x", lower, middle);
        String uRange = String.format("0x%08x_0x%08x", middle, upper);
        assertEquals(lRange, bundles.get(0).getBundleRange());
        assertEquals(uRange, bundles.get(1).getBundleRange());
        log.info("[{},{}] => [{},{}]", range[0], range[1], lRange, uRange);
    }

    private static final Logger log = LoggerFactory.getLogger(NamespaceBundlesTest.class);
}
