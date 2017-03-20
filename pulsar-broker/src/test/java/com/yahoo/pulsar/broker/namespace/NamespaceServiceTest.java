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
package com.yahoo.pulsar.broker.namespace;

import static com.yahoo.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;
import static com.yahoo.pulsar.broker.web.PulsarWebResource.joinPath;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.data.Stat;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.yahoo.pulsar.broker.service.BrokerTestBase;
import com.yahoo.pulsar.broker.service.Topic;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashMap;

public class NamespaceServiceTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSplitAndOwnBundles() throws Exception {

        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
        doNothing().when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);
        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = new NamespaceName("pulsar/global/ns1");
        DestinationName dn = DestinationName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(dn);

        // Split bundle and take ownership of split bundles
        CompletableFuture<Void> result = namespaceService.splitAndOwnBundle(originalBundle);

        try {
            result.get();
        } catch (Exception e) {
            // make sure: no failure
            fail("split bundle faild", e);
        }
        NamespaceBundleFactory bundleFactory = this.pulsar.getNamespaceService().getNamespaceBundleFactory();
        NamespaceBundles updatedNsBundles = bundleFactory.getBundles(nsname);

        // new updated bundles shouldn't be null
        assertNotNull(updatedNsBundles);
        List<NamespaceBundle> bundleList = updatedNsBundles.getBundles();
        assertNotNull(bundles);

        NamespaceBundleFactory utilityFactory = NamespaceBundleFactory.createFactory(Hashing.crc32());

        // (1) validate bundleFactory-cache has newly split bundles and removed old parent bundle
        Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles = splitBundles(utilityFactory, nsname, bundles,
                originalBundle);
        assertNotNull(splitBundles);
        Set<NamespaceBundle> splitBundleSet = new HashSet<>(splitBundles.getRight());
        splitBundleSet.removeAll(bundleList);
        assertTrue(splitBundleSet.isEmpty());

        // (2) validate LocalZookeeper policies updated with newly created split
        // bundles
        String path = joinPath(LOCAL_POLICIES_ROOT, nsname.toString());
        byte[] content = this.pulsar.getLocalZkCache().getZooKeeper().getData(path, null, new Stat());
        Policies policies = ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);
        NamespaceBundles localZkBundles = bundleFactory.getBundles(nsname, policies.bundles);
        assertTrue(updatedNsBundles.equals(localZkBundles));
        log.info("Policies: {}", policies);

        // (3) validate ownership of new split bundles by local owner
        bundleList.stream().forEach(b -> {
            try {
                byte[] data = this.pulsar.getLocalZkCache().getZooKeeper().getData(ServiceUnitZkUtils.path(b), null,
                        new Stat());
                NamespaceEphemeralData node = ObjectMapperFactory.getThreadLocal().readValue(data,
                        NamespaceEphemeralData.class);
                Assert.assertEquals(node.getNativeUrl(), this.pulsar.getBrokerServiceUrl());
            } catch (Exception e) {
                fail("failed to setup ownership", e);
            }
        });

    }

    @Test
    public void testSplitMapWithRefreshedStatMap() throws Exception {

        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());

        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getCursors()).thenReturn(Lists.newArrayList());

        doNothing().when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = new NamespaceName("pulsar/global/ns1");
        DestinationName dn = DestinationName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(dn);

        PersistentTopic topic = new PersistentTopic(dn.toString(), ledger, pulsar.getBrokerService());
        Method method = pulsar.getBrokerService().getClass().getDeclaredMethod("addTopicToStatsMaps",
                DestinationName.class, PersistentTopic.class);
        method.setAccessible(true);
        method.invoke(pulsar.getBrokerService(), dn, topic);
        String nspace = originalBundle.getNamespaceObject().toString();
        List<PersistentTopic> list = this.pulsar.getBrokerService().getAllTopicsFromNamespaceBundle(nspace,
                originalBundle.toString());
        assertNotNull(list);

        // Split bundle and take ownership of split bundles
        CompletableFuture<Void> result = namespaceService.splitAndOwnBundle(originalBundle);
        try {
            result.get();
        } catch (Exception e) {
            // make sure: no failure
            fail("split bundle faild", e);
        }

        try {
            // old bundle should be removed from status-map
            list = this.pulsar.getBrokerService().getAllTopicsFromNamespaceBundle(nspace, originalBundle.toString());
            fail();
        } catch (NullPointerException ne) {
            // OK
        }

        // status-map should be updated with new split bundles
        NamespaceBundle splitBundle = pulsar.getNamespaceService().getBundle(dn);
        assertTrue(!CollectionUtils.isEmpty(
                this.pulsar.getBrokerService().getAllTopicsFromNamespaceBundle(nspace, splitBundle.toString())));

    }

    @Test
    public void testIsServiceUnitDisabled() throws Exception {

        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());

        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getCursors()).thenReturn(Lists.newArrayList());

        doNothing().when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = new NamespaceName("pulsar/global/ns1");
        DestinationName dn = DestinationName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(dn);

        assertFalse(namespaceService.isNamespaceBundleDisabled(originalBundle));

    }

    @Test
    public void testremoveOwnershipNamespaceBundle() throws Exception {

        OwnershipCache ownershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());

        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getCursors()).thenReturn(Lists.newArrayList());

        doNothing().when(ownershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), ownershipCache);

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = new NamespaceName("prop/use/ns1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);

        NamespaceBundle bundle = bundles.getBundles().get(0);
        assertNotNull(ownershipCache.tryAcquiringOwnership(bundle));
        assertNotNull(ownershipCache.getOwnedBundle(bundle));
        ownershipCache.removeOwnership(bundles).get();
        assertNull(ownershipCache.getOwnedBundle(bundle));
    }

    @Test
    public void testUnloadNamespaceBundleFailure() throws Exception {

        final String topicName = "persistent://my-property/use/my-ns/my-topic1";
        ConsumerConfiguration conf = new ConsumerConfiguration();
        Consumer consumer = pulsarClient.subscribe(topicName, "my-subscriber-name", conf);
        ConcurrentOpenHashMap<String, CompletableFuture<Topic>> topics = pulsar.getBrokerService().getTopics();
        Topic spyTopic = spy(topics.get(topicName).get());
        topics.clear();
        CompletableFuture<Topic> topicFuture = CompletableFuture.completedFuture(spyTopic);
        // add mock topic
        topics.put(topicName, topicFuture);
        doAnswer(new Answer<CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> answer(InvocationOnMock invocation) throws Throwable {
                CompletableFuture<Void> result = new CompletableFuture<>();
                result.completeExceptionally(new RuntimeException("first time failed"));
                return result;
            }
        }).when(spyTopic).close();
        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(DestinationName.get(topicName));
        try {
            pulsar.getNamespaceService().unloadNamespaceBundle(bundle);
        } catch (Exception e) {
            // fail
            fail(e.getMessage());
        }
        try {
            pulsar.getLocalZkCache().getZooKeeper().getData(ServiceUnitZkUtils.path(bundle), null, null);
            fail("it should fail as node is not present");
        } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
            // ok
        }
    }
    
    @SuppressWarnings("unchecked")
    private Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles(NamespaceBundleFactory utilityFactory,
            NamespaceName nsname, NamespaceBundles bundles, NamespaceBundle targetBundle) throws Exception {
        Field bCacheField = NamespaceBundleFactory.class.getDeclaredField("bundlesCache");
        bCacheField.setAccessible(true);
        ((AsyncLoadingCache<NamespaceName, NamespaceBundles>) bCacheField.get(utilityFactory)).put(nsname,
                CompletableFuture.completedFuture(bundles));
        return utilityFactory.splitBundles(targetBundle, 2);
    }

    private static final Logger log = LoggerFactory.getLogger(NamespaceServiceTest.class);
}
