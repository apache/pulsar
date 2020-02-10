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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.hash.Hashing;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.MockZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnershipCacheTest {

    private PulsarService pulsar;
    private ServiceConfiguration config;
    private String selfBrokerUrl;
    private ZooKeeperCache zkCache;
    private LocalZooKeeperCacheService localCache;
    private NamespaceBundleFactory bundleFactory;
    private NamespaceService nsService;
    private BrokerService brokerService;
    private OrderedScheduler executor;
    private MockZooKeeper zkc;

    @BeforeMethod
    public void setup() throws Exception {
        final int port = 8080;
        selfBrokerUrl = "tcp://localhost:" + port;
        pulsar = mock(PulsarService.class);
        config = mock(ServiceConfiguration.class);
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("test").build();
        zkc = MockZooKeeper.newInstance();
        zkCache = new LocalZooKeeperCache(zkc, 30, executor);
        localCache = spy(new LocalZooKeeperCacheService(zkCache, null));
        ZooKeeperDataCache<LocalPolicies> poilciesCache = mock(ZooKeeperDataCache.class);
        when(pulsar.getLocalZkCacheService()).thenReturn(localCache);
        when(localCache.policiesCache()).thenReturn(poilciesCache);
        doNothing().when(poilciesCache).registerListener(any());

        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
        nsService = mock(NamespaceService.class);
        brokerService = mock(BrokerService.class);
        doReturn(CompletableFuture.completedFuture(1)).when(brokerService).unloadServiceUnit(any(), anyBoolean());

        doReturn(zkCache).when(pulsar).getLocalZkCache();
        doReturn(localCache).when(pulsar).getLocalZkCacheService();
        doReturn(config).when(pulsar).getConfiguration();
        doReturn(nsService).when(pulsar).getNamespaceService();
        doReturn(Optional.ofNullable(new Integer(port))).when(config).getBrokerServicePort();
        doReturn(Optional.ofNullable(null)).when(config).getWebServicePort();
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(selfBrokerUrl).when(pulsar).getSafeBrokerServiceUrl();
    }

    @AfterMethod
    public void teardown() throws Exception {
        executor.shutdown();
        zkCache.stop();
        zkc.shutdown();
    }

    @Test
    public void testConstructor() {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, null);
        assertNotNull(cache);
        assertNotNull(cache.getOwnedBundles());
    }

    @Test
    public void testDisableOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, null);

        NamespaceBundle testBundle = bundleFactory.getFullBundle(NamespaceName.get("pulsar/test/ns-1"));
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertFalse(data1.isDisabled());
        cache.disableOwnership(testBundle);
        // force the next read to get directly from ZK
        // localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testNs));
        data1 = cache.getOwnerAsync(testBundle).get().get();
        assertTrue(data1.isDisabled());
    }

    @Test
    public void testGetOrSetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, null);
        NamespaceBundle testFullBundle = bundleFactory.getFullBundle(NamespaceName.get("pulsar/test/ns-2"));
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testFullBundle).get().isPresent());

        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testFullBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        // case 2: the local broker owned the namespace and disabled, getOrSetOwner() should not change it
        OwnedBundle nsObj = cache.getOwnedBundle(testFullBundle);
        // this would disable the ownership
        doReturn(cache).when(nsService).getOwnershipCache();
        nsObj.handleUnloadRequest(pulsar, 5, TimeUnit.SECONDS);
        Thread.sleep(1000);

        // case 3: some other broker owned the namespace, getOrSetOwner() should return other broker's URL
        // The only chance that we lost an already existing ephemeral node is when the broker dies or unload has
        // succeeded in both cases, the ownerInfoCache will be updated (i.e. invalidated the entry)
        localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testFullBundle));
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testFullBundle),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://localhost:8080", "https://localhost:4443", false));
        data1 = cache.tryAcquiringOwnership(testFullBundle).get();
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());

    }

    @Test
    public void testGetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, null);
        NamespaceBundle testBundle = bundleFactory.getFullBundle(NamespaceName.get("pulsar/test/ns-3"));
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());
        // case 2: someone owns the namespace
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testBundle),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://otherhost:8080", "https://otherhost:4443", false));

        // try to acquire, which will load the read-only cache
        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testBundle).get();

        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());
        // Now do getOwner and compare w/ the returned values
        NamespaceEphemeralData readOnlyData = cache.getOwnerAsync(testBundle).get().get();
        assertEquals(data1, readOnlyData);

        MockZooKeeper mockZk = (MockZooKeeper) zkCache.getZooKeeper();
        mockZk.failNow(KeeperException.Code.NONODE);
        Optional<NamespaceEphemeralData> res = cache
                .getOwnerAsync(bundleFactory.getFullBundle(NamespaceName.get("pulsar/test/ns-none"))).get();
        assertFalse(res.isPresent());
    }

    @Test
    public void testGetOwnedServiceUnit() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, null);
        NamespaceName testNs = NamespaceName.get("pulsar/test/ns-5");
        NamespaceBundle testBundle = bundleFactory.getFullBundle(testNs);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        try {
            checkNotNull(cache.getOwnedBundle(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }
        // case 2: someone else owns the namespace
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testBundle),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://otherhost:8080", "https://otherhost:4443", false));
        try {
            checkNotNull(cache.getOwnedBundle(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }

        Thread.sleep(500);

        // try to acquire, which will load the read-only cache
        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());
        try {
            checkNotNull(cache.getOwnedBundle(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }
        // case 3: this broker owns the namespace
        // delete the ephemeral node by others
        zkCache.getZooKeeper().delete(ServiceUnitZkUtils.path(testBundle), -1);
        // force to read directly from ZK
        localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testBundle));
        data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        assertNotNull(cache.getOwnedBundle(testBundle));
    }

    @Test
    public void testGetOwnedServiceUnits() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, null);
        NamespaceName testNs = NamespaceName.get("pulsar/test/ns-6");
        NamespaceBundle testBundle = bundleFactory.getFullBundle(testNs);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        assertTrue(cache.getOwnedBundles().isEmpty());

        // case 2: someone else owns the namespace
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testBundle),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://otherhost:8080", "https://otherhost:4443", false));
        assertTrue(cache.getOwnedBundles().isEmpty());

        Thread.sleep(500);

        // try to acquire, which will load the read-only cache
        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());
        assertTrue(cache.getOwnedBundles().isEmpty());
        // case 3: this broker owns the namespace
        // delete the ephemeral node by others
        zkCache.getZooKeeper().delete(ServiceUnitZkUtils.path(testBundle), -1);
        // force to read directly from ZK
        localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testBundle));
        data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        assertEquals(cache.getOwnedBundles().size(), 1);
    }

    @Test
    public void testRemoveOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, null);
        NamespaceName testNs = NamespaceName.get("pulsar/test/ns-7");
        NamespaceBundle bundle = bundleFactory.getFullBundle(testNs);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(bundle).get().isPresent());

        cache.removeOwnership(bundle).get();
        assertTrue(cache.getOwnedBundles().isEmpty());

        // case 2: this broker owns the namespace
        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(bundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        assertEquals(cache.getOwnedBundles().size(), 1);
        cache.removeOwnership(bundle);
        Thread.sleep(500);
        assertTrue(cache.getOwnedBundles().isEmpty());

        Thread.sleep(500);

        try {
            zkCache.getZooKeeper().getData(ServiceUnitZkUtils.path(bundle), null, null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }
    }

}
