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
package org.apache.pulsar.broker.namespace;

import static org.apache.pulsar.broker.namespace.NamespaceService.HEARTBEAT_NAMESPACE_PATTERN;
import static org.apache.pulsar.broker.namespace.NamespaceService.HEARTBEAT_NAMESPACE_PATTERN_V2;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;

@Test(groups = "broker")
public class NamespaceServiceTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSplitAndOwnBundles() throws Exception {

        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
        doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);
        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(topicName);

        // Split bundle and take ownership of split bundles
        CompletableFuture<Void> result = namespaceService.splitAndOwnBundle(
                originalBundle,
                false,
                NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO);

        try {
            result.get();
        } catch (Exception e) {
            // make sure: no failure
            fail("split bundle failed", e);
        }
        NamespaceBundleFactory bundleFactory = this.pulsar.getNamespaceService().getNamespaceBundleFactory();
        NamespaceBundles updatedNsBundles = bundleFactory.getBundles(nsname);

        // new updated bundles shouldn't be null
        assertNotNull(updatedNsBundles);
        List<NamespaceBundle> bundleList = updatedNsBundles.getBundles();
        assertNotNull(bundles);

        NamespaceBundleFactory utilityFactory = NamespaceBundleFactory.createFactory(pulsar, Hashing.crc32());

        // (1) validate bundleFactory-cache has newly split bundles and removed old parent bundle
        Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles = splitBundles(utilityFactory, nsname, bundles,
                originalBundle);
        assertNotNull(splitBundles);
        Set<NamespaceBundle> splitBundleSet = new HashSet<>(splitBundles.getRight());
        splitBundleSet.removeAll(bundleList);
        assertTrue(splitBundleSet.isEmpty());

        // (2) validate LocalZookeeper policies updated with newly created split
        // bundles
        LocalPolicies policies = pulsar.getPulsarResources().getLocalPolicies().getLocalPolicies(nsname).get();
        NamespaceBundles localZkBundles = bundleFactory.getBundles(nsname, policies.bundles);
        assertEquals(localZkBundles, updatedNsBundles);
        log.info("Policies: {}", policies);

        // (3) validate ownership of new split bundles by local owner
        bundleList.forEach(b -> {
            try {
                byte[] data = this.pulsar.getLocalMetadataStore().get(ServiceUnitUtils.path(b)).join().get().getValue();
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

        doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(topicName);

        PersistentTopic topic = new PersistentTopic(topicName.toString(), ledger, pulsar.getBrokerService());
        topic.initialize().join();
        Method method = pulsar.getBrokerService().getClass().getDeclaredMethod("addTopicToStatsMaps",
                TopicName.class, Topic.class);
        method.setAccessible(true);
        method.invoke(pulsar.getBrokerService(), topicName, topic);
        String nspace = originalBundle.getNamespaceObject().toString();
        List<Topic> list = this.pulsar.getBrokerService().getAllTopicsFromNamespaceBundle(nspace,
                originalBundle.toString());
        assertNotNull(list);

        // Split bundle and take ownership of split bundles
        CompletableFuture<Void> result = namespaceService.splitAndOwnBundle(
                originalBundle,
                false,
                NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO);
        try {
            result.get();
        } catch (Exception e) {
            // make sure: no failure
            fail("split bundle failed", e);
        }

        // old bundle should be removed from status-map
        list = this.pulsar.getBrokerService().getAllTopicsFromNamespaceBundle(nspace, originalBundle.toString());
        assertTrue(list.isEmpty());

        // status-map should be updated with new split bundles
        NamespaceBundle splitBundle = pulsar.getNamespaceService().getBundle(topicName);
        assertFalse(CollectionUtils.isEmpty(
            this.pulsar.getBrokerService()
                .getAllTopicsFromNamespaceBundle(nspace, splitBundle.toString())));

    }

    @Test
    public void testIsServiceUnitDisabled() throws Exception {

        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());

        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getCursors()).thenReturn(Lists.newArrayList());

        doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(topicName);

        assertFalse(namespaceService.isNamespaceBundleDisabled(originalBundle));

    }

    @Test
    public void testRemoveOwnershipNamespaceBundle() throws Exception {

        OwnershipCache ownershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());

        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getCursors()).thenReturn(Lists.newArrayList());

        doReturn(CompletableFuture.completedFuture(null)).when(ownershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), ownershipCache);

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get("prop/use/ns1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);

        NamespaceBundle bundle = bundles.getBundles().get(0);
        ownershipCache.tryAcquiringOwnership(bundle).get();
        assertNotNull(ownershipCache.getOwnedBundle(bundle));
        ownershipCache.removeOwnership(bundles).get();
        assertNull(ownershipCache.getOwnedBundle(bundle));
    }

    @Test
    public void testUnloadNamespaceBundleFailure() throws Exception {

        final String topicName = "persistent://my-property/use/my-ns/my-topic1";
        pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name").subscribe();

        ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics = pulsar.getBrokerService()
                .getTopics();
        Topic spyTopic = spy(topics.get(topicName).get().get());
        topics.clear();
        CompletableFuture<Optional<Topic>> topicFuture = CompletableFuture.completedFuture(Optional.of(spyTopic));
        // add mock topic
        topics.put(topicName, topicFuture);
        doAnswer((Answer<CompletableFuture<Void>>) invocation -> {
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(new RuntimeException("first time failed"));
            return result;
        }).when(spyTopic).close(false);
        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));

        pulsar.getNamespaceService().unloadNamespaceBundle(bundle).join();

        Optional<GetResult> res = this.pulsar.getLocalMetadataStore().get(ServiceUnitUtils.path(bundle)).join();
        assertFalse(res.isPresent());
    }

    /**
     * It verifies that unloading bundle will timeout and will not hung even if one of the topic-unloading stuck.
     *
     * @throws Exception
     */
    @Test(timeOut = 6000)
    public void testUnloadNamespaceBundleWithStuckTopic() throws Exception {

        final String topicName = "persistent://my-property/use/my-ns/my-topic1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();
        ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics = pulsar.getBrokerService().getTopics();
        Topic spyTopic = spy(topics.get(topicName).get().get());
        topics.clear();
        CompletableFuture<Optional<Topic>> topicFuture = CompletableFuture.completedFuture(Optional.of(spyTopic));
        // add mock topic
        topics.put(topicName, topicFuture);
        // return uncompleted future as close-topic result.
        doAnswer((Answer<CompletableFuture<Void>>) invocation -> new CompletableFuture<Void>()).when(spyTopic).close(false);
        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));

        // try to unload bundle whose topic will be stuck
        pulsar.getNamespaceService().unloadNamespaceBundle(bundle, 1, TimeUnit.SECONDS).join();

        Optional<GetResult> res = this.pulsar.getLocalMetadataStore().get(ServiceUnitUtils.path(bundle)).join();
        assertFalse(res.isPresent());
        consumer.close();
    }

    /**
     * <pre>
     *  It verifies that namespace service deserialize the load-report based on load-manager which active.
     *  1. write candidate1- load report using {@link LoadReport} which is used by SimpleLoadManagerImpl
     *  2. Write candidate2- load report using {@link LocalBrokerData} which is used by ModularLoadManagerImpl
     *  3. try to get Lookup Result based on active load-manager
     * </pre>
     * @throws Exception
     */
    @Test
    public void testLoadReportDeserialize() throws Exception {

        final String candidateBroker1 = "http://localhost:8000";
        final String candidateBroker2 = "http://localhost:3000";
        LoadReport lr = new LoadReport(null, null, candidateBroker1, null);
        LocalBrokerData ld = new LocalBrokerData(null, null, candidateBroker2, null);
        URI uri1 = new URI(candidateBroker1);
        URI uri2 = new URI(candidateBroker2);
        String path1 = String.format("%s/%s:%s", LoadManager.LOADBALANCE_BROKERS_ROOT, uri1.getHost(), uri1.getPort());
        String path2 = String.format("%s/%s:%s", LoadManager.LOADBALANCE_BROKERS_ROOT, uri2.getHost(), uri2.getPort());

        pulsar.getLocalMetadataStore().put(path1,
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(lr),
                Optional.empty(),
                EnumSet.of(CreateOption.Ephemeral)
                ).join();
        pulsar.getLocalMetadataStore().put(path2,
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(ld),
                Optional.empty(),
                EnumSet.of(CreateOption.Ephemeral)
        ).join();
        LookupResult result1 = pulsar.getNamespaceService().createLookupResult(candidateBroker1, false, null).get();

        // update to new load manager
        LoadManager oldLoadManager = pulsar.getLoadManager()
                .getAndSet(new ModularLoadManagerWrapper(new ModularLoadManagerImpl()));
        oldLoadManager.stop();
        LookupResult result2 = pulsar.getNamespaceService().createLookupResult(candidateBroker2, false, null).get();
        Assert.assertEquals(result1.getLookupData().getBrokerUrl(), candidateBroker1);
        Assert.assertEquals(result2.getLookupData().getBrokerUrl(), candidateBroker2);
        System.out.println(result2);
    }

    @Test
    public void testCreateLookupResult() throws Exception {

        final String candidateBroker = "pulsar://localhost:6650";
        final String listenerUrl = "pulsar://localhost:7000";
        final String listenerUrlTls = "pulsar://localhost:8000";
        final String listener = "listenerName";
        Map<String, AdvertisedListener> advertisedListeners = Maps.newHashMap();
        advertisedListeners.put(listener, AdvertisedListener.builder().brokerServiceUrl(new URI(listenerUrl)).brokerServiceUrlTls(new URI(listenerUrlTls)).build());
        LocalBrokerData ld = new LocalBrokerData(null, null, candidateBroker, null, advertisedListeners);
        URI uri = new URI(candidateBroker);
        String path = String.format("%s/%s:%s", LoadManager.LOADBALANCE_BROKERS_ROOT, uri.getHost(), uri.getPort());

        pulsar.getLocalMetadataStore().put(path,
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(ld),
                Optional.empty(),
                EnumSet.of(CreateOption.Ephemeral)).join();

        LookupResult noListener = pulsar.getNamespaceService().createLookupResult(candidateBroker, false, null).get();
        LookupResult withListener = pulsar.getNamespaceService().createLookupResult(candidateBroker, false, listener).get();

        Assert.assertEquals(noListener.getLookupData().getBrokerUrl(), candidateBroker);
        Assert.assertEquals(withListener.getLookupData().getBrokerUrl(), listenerUrl);
        Assert.assertEquals(withListener.getLookupData().getBrokerUrlTls(), listenerUrlTls);
        System.out.println(withListener);
    }

    @Test
    public void testCreateNamespaceWithDefaultNumberOfBundles() throws Exception {
        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
        doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);
        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(topicName);

        // Split bundle and take ownership of split bundles
        CompletableFuture<Void> result = namespaceService.splitAndOwnBundle(originalBundle, false, NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO);

        try {
            result.get();
        } catch (Exception e) {
            // make sure: no failure
            fail("split bundle failed", e);
        }
        NamespaceBundleFactory bundleFactory = this.pulsar.getNamespaceService().getNamespaceBundleFactory();
        NamespaceBundles updatedNsBundles = bundleFactory.getBundles(nsname);

        // new updated bundles shouldn't be null
        assertNotNull(updatedNsBundles);
        List<NamespaceBundle> bundleList = updatedNsBundles.getBundles();
        assertNotNull(bundles);

        NamespaceBundleFactory utilityFactory = NamespaceBundleFactory.createFactory(pulsar, Hashing.crc32());

        // (1) validate bundleFactory-cache has newly split bundles and removed old parent bundle
        Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles = splitBundles(utilityFactory, nsname, bundles,
                originalBundle);
        assertNotNull(splitBundles);
        Set<NamespaceBundle> splitBundleSet = new HashSet<>(splitBundles.getRight());
        splitBundleSet.removeAll(bundleList);
        assertTrue(splitBundleSet.isEmpty());

        // (2) validate LocalZookeeper policies updated with newly created split
        // bundles
        LocalPolicies policies = this.pulsar.getPulsarResources().getLocalPolicies().getLocalPolicies(nsname).get();
        NamespaceBundles localZkBundles = bundleFactory.getBundles(nsname, policies.bundles);
        assertEquals(localZkBundles, updatedNsBundles);
        log.info("Policies: {}", policies);

        // (3) validate ownership of new split bundles by local owner
        bundleList.forEach(b -> {
            try {
                byte[] data = this.pulsar.getLocalMetadataStore().get(ServiceUnitUtils.path(b)).join().get().getValue();
                NamespaceEphemeralData node = ObjectMapperFactory.getThreadLocal().readValue(data,
                        NamespaceEphemeralData.class);
                Assert.assertEquals(node.getNativeUrl(), this.pulsar.getBrokerServiceUrl());
            } catch (Exception e) {
                fail("failed to setup ownership", e);
            }
        });

    }

    @Test
    public void testRemoveOwnershipAndSplitBundle() throws Exception {
        OwnershipCache ownershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
        doReturn(CompletableFuture.completedFuture(null)).when(ownershipCache).disableOwnership(any(NamespaceBundle.class));

        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), ownershipCache);

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get("pulsar/global/ns1");
        TopicName topicName = TopicName.get("persistent://pulsar/global/ns1/topic-1");
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        NamespaceBundle originalBundle = bundles.findBundle(topicName);

        CompletableFuture<Void> result1 = namespaceService.splitAndOwnBundle(originalBundle, false, NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO);
        try {
            result1.get();
        } catch (Exception e) {
            fail("split bundle failed", e);
        }

        NamespaceBundles updatedNsBundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);
        assertNotNull(updatedNsBundles);
        NamespaceBundle splittedBundle = updatedNsBundles.findBundle(topicName);

        updatedNsBundles.getBundles().stream().filter(bundle -> !bundle.equals(splittedBundle)).forEach(bundle -> {
            try {
                ownershipCache.removeOwnership(bundle).get();
            } catch (Exception e) {
                fail("failed to remove ownership", e);
            }
        });

        CompletableFuture<Void> result2 = namespaceService.splitAndOwnBundle(splittedBundle, true, NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO);
        try {
            result2.get();
        } catch (Exception e) {
            // make sure: NPE does not occur
            fail("split bundle failed", e);
        }
    }

    @Test
    public void testSplitLargestBundle() throws Exception {
        String namespace = "prop/test/ns-abc2";
        String topic = "persistent://" + namespace + "/t1-";
        int totalTopics = 100;

        BundlesData bundleData = BundlesData.builder().numBundles(10).build();
        admin.namespaces().createNamespace(namespace, bundleData);
        Consumer<byte[]>[] consumers = new Consumer[totalTopics];
        for (int i = 0; i < totalTopics; i++) {
            consumers[i] = pulsarClient.newConsumer().topic(topic + i).subscriptionName("my-subscriber-name")
                    .subscribe();
        }

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get(namespace);
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);

        Map<String, Integer> topicCount = Maps.newHashMap();
        int maxTopics = 0;
        String maxBundle = null;
        for (int i = 0; i < totalTopics; i++) {
            String bundle = bundles.findBundle(TopicName.get(topic + i)).getBundleRange();
            int count = topicCount.getOrDefault(bundle, 0) + 1;
            topicCount.put(bundle, count);
            if (count > maxTopics) {
                maxTopics = count;
                maxBundle = bundle;
            }
        }

        String largestBundle = namespaceService.getNamespaceBundleFactory().getBundleWithHighestTopics(nsname)
                .getBundleRange();

        assertEquals(maxBundle, largestBundle);

        for (int i = 0; i < totalTopics; i++) {
            consumers[i].close();
        }

        admin.namespaces().splitNamespaceBundle(namespace, Policies.BundleType.LARGEST.toString(), false, null);

        for (NamespaceBundle bundle : namespaceService.getNamespaceBundleFactory().getBundles(nsname).getBundles()) {
            assertNotEquals(bundle.getBundleRange(), maxBundle);
        }
    }

    /**
     * Test bundle split with hot bundle which is serving highest load.
     *
     * @throws Exception
     */
    @Test
    public void testSplitBundleWithHighestThroughput() throws Exception {

        conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        restartBroker();
        String namespace = "prop/test/ns-abc2";
        String topic = "persistent://" + namespace + "/t1-";
        int totalTopics = 100;

        BundlesData bundleData = BundlesData.builder().numBundles(10).build();
        admin.namespaces().createNamespace(namespace, bundleData);
        Consumer<byte[]>[] consumers = new Consumer[totalTopics];
        for (int i = 0; i < totalTopics; i++) {
            consumers[i] = pulsarClient.newConsumer().topic(topic + i).subscriptionName("my-subscriber-name")
                    .subscribe();
        }

        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceName nsname = NamespaceName.get(namespace);
        NamespaceBundles bundles = namespaceService.getNamespaceBundleFactory().getBundles(nsname);

        String bundle = bundles.findBundle(TopicName.get(topic + "0")).getBundleRange();
        String path = ModularLoadManagerImpl.getBundleDataPath(bundle);
        NamespaceBundleStats defaultStats = new NamespaceBundleStats();
        defaultStats.msgThroughputIn = 100000;
        defaultStats.msgThroughputOut = 100000;
        BundleData bd = new BundleData(10, 19, defaultStats );
        byte[] data = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(bd);
        pulsar.getLocalMetadataStore().put(path, data, Optional.empty());
        
        String hotBundle = namespaceService.getNamespaceBundleFactory().getBundleWithHighestThroughput(nsname)
                .getBundleRange();

        assertEquals(bundle, hotBundle);
        
        for (int i = 0; i < totalTopics; i++) {
            consumers[i].close();
        }

        admin.namespaces().splitNamespaceBundle(namespace, Policies.BundleType.HOT.toString(), false, null);

        for (NamespaceBundle b : namespaceService.getNamespaceBundleFactory().getBundles(nsname).getBundles()) {
            assertNotEquals(b.getBundleRange(), hotBundle);
        }
    }

    @Test
    public void testHeartbeatNamespaceMatch() throws Exception {
        NamespaceName namespaceName = NamespaceService.getHeartbeatNamespace(pulsar.getAdvertisedAddress(), conf);
        NamespaceBundle namespaceBundle = pulsar.getNamespaceService().getNamespaceBundleFactory().getFullBundle(namespaceName);
        assertTrue(NamespaceService.isSystemServiceNamespace(
                        NamespaceBundle.getBundleNamespace(namespaceBundle.toString())));
    }

    @SuppressWarnings("unchecked")
    private Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles(NamespaceBundleFactory utilityFactory,
            NamespaceName nsname, NamespaceBundles bundles, NamespaceBundle targetBundle) throws Exception {
        Field bCacheField = NamespaceBundleFactory.class.getDeclaredField("bundlesCache");
        bCacheField.setAccessible(true);
        ((AsyncLoadingCache<NamespaceName, NamespaceBundles>) bCacheField.get(utilityFactory)).put(nsname,
                CompletableFuture.completedFuture(bundles));
        return utilityFactory.splitBundles(targetBundle, 2, null).join();
    }

    private static final Logger log = LoggerFactory.getLogger(NamespaceServiceTest.class);
}
