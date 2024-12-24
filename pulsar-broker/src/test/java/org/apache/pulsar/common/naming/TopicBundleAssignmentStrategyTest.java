/*
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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.PoliciesUtil;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-naming")
public class TopicBundleAssignmentStrategyTest {
    @BeforeMethod
    public void setUp() {
        // clean up the values of static member variables in memory
        try {
            Field field = TopicBundleAssignmentFactory.class.getDeclaredField("strategy");
            field.setAccessible(true);
            field.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testStrategyFactory() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setTopicBundleAssignmentStrategy(
                "org.apache.pulsar.common.naming.TopicBundleAssignmentStrategyTest$TestStrategy");
        PulsarService pulsarService = mock(PulsarService.class);
        doReturn(conf).when(pulsarService).getConfiguration();
        TopicBundleAssignmentStrategy strategy = TopicBundleAssignmentFactory.create(pulsarService);
        NamespaceBundle bundle = strategy.findBundle(null, null);
        Range<Long> keyRange = Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED);
        String range = String.format("0x%08x_0x%08x", keyRange.lowerEndpoint(), keyRange.upperEndpoint());
        Assert.assertEquals(bundle.getBundleRange(), range);
        Assert.assertEquals(bundle.getNamespaceObject(), NamespaceName.get("my/test"));
    }

    private static class TestStrategy implements TopicBundleAssignmentStrategy {
        @Override
        public NamespaceBundle findBundle(TopicName topicName, NamespaceBundles namespaceBundles) {
            Range<Long> range = Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED);
            return new NamespaceBundle(NamespaceName.get("my/test"), range,
                    mock(NamespaceBundleFactory.class));
        }

        @Override
        public long getHashCode(String name) {
            return 0;
        }

        @Override
        public void init(PulsarService pulsarService) {
        }
    }

    @Test
    public void testRoundRobinBundleAssigner() {
        int DEFALUT_BUNDLE_NUM = 128;

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setTopicBundleAssignmentStrategy(
                "org.apache.pulsar.common.naming.TopicBundleAssignmentStrategyTest$RoundRobinBundleAssigner");
        conf.setDefaultNumberOfNamespaceBundles(DEFALUT_BUNDLE_NUM);
        PulsarService pulsarService = mock(PulsarService.class);
        doReturn(conf).when(pulsarService).getConfiguration();
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        when(pulsarService.getLocalMetadataStore()).thenReturn(store);
        when(pulsarService.getConfigurationMetadataStore()).thenReturn(store);
        NamespaceBundleFactory factoryNew = NamespaceBundleFactory.createFactory(pulsarService, Hashing.sha256());
        NamespaceService namespaceService = mock(NamespaceService.class);
        when(namespaceService.getNamespaceBundleFactory()).thenReturn(factoryNew);
        when(pulsarService.getNamespaceService()).thenReturn(namespaceService);
        BundlesData bundlesData = PoliciesUtil.getBundles(DEFALUT_BUNDLE_NUM);
        LocalPolicies localPolicies = new LocalPolicies(bundlesData, null, null);
        NamespaceBundles bundles = new NamespaceBundles(NamespaceName.get("pulsar/global/ns1"),
                factoryNew, Optional.of(Pair.of(localPolicies, (long) DEFALUT_BUNDLE_NUM)));
        Set<NamespaceBundle> alreadyAssignNamespaceBundle = new HashSet<NamespaceBundle>();
        for (int i = 0; i < DEFALUT_BUNDLE_NUM; i++) {
            TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-partition-" + i);
            NamespaceBundle bundle = bundles.findBundle(topicName);
            assertTrue(bundle.includes(topicName));
            //new hash func will make topic partition assign to different bundle as possible
            assertFalse(alreadyAssignNamespaceBundle.contains(bundle));
            alreadyAssignNamespaceBundle.add(bundle);
        }
    }

    public static class RoundRobinBundleAssigner implements TopicBundleAssignmentStrategy {
        PulsarService pulsar;

        @Override
        public NamespaceBundle findBundle(TopicName topicName, NamespaceBundles namespaceBundles) {
            NamespaceBundle bundle = namespaceBundles.getBundle(getHashCode(topicName.toString()));
            if (topicName.getDomain().equals(TopicDomain.non_persistent)) {
                bundle.setHasNonPersistentTopic(true);
            }
            return bundle;
        }

        @Override
        public long getHashCode(String name) {
            // use topic name without partition id to decide the first hash value
            TopicName topicName = TopicName.get(name);
            long currentPartitionTopicHash =
                    pulsar.getNamespaceService().getNamespaceBundleFactory().getHashFunc()
                            .hashString(topicName.getPartitionedTopicName(), Charsets.UTF_8).padToLong();

            // if the topic is a non partition topic, use topic name to calculate the hashcode
            if (!topicName.isPartitioned()) {
                return currentPartitionTopicHash;
            }

            //  a pieces of bundle size with default * partiton id
            double targetPartitionRangeSize =
                    (double) NamespaceBundles.FULL_UPPER_BOUND / (double) pulsar.getConfiguration()
                            .getDefaultNumberOfNamespaceBundles() * (double) topicName.getPartitionIndex();

            // new hash func will make topic partition assign to different bundle as possible
            return (currentPartitionTopicHash + Math.round(targetPartitionRangeSize))
                    % NamespaceBundles.FULL_UPPER_BOUND;
        }

        @Override
        public void init(PulsarService pulsarService) {
            this.pulsar = pulsarService;
        }
    }
}
