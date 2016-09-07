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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.yahoo.pulsar.broker.PulsarService.host;
import static com.yahoo.pulsar.broker.PulsarService.webAddress;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.MockZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.hash.Hashing;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.cache.LocalZooKeeperCacheService;
import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;

public class OwnershipCacheTest {

    private PulsarService pulsar;
    private ServiceConfiguration config;
    private String selfBrokerUrl;
    private ZooKeeperCache zkCache;
    private LocalZooKeeperCacheService localCache;
    private NamespaceBundleFactory bundleFactory;
    private NamespaceService nsService;
    private BrokerService brokerService;
    private OrderedSafeExecutor executor;

    @BeforeMethod
    public void setup() throws Exception {
        final int port = 8080;
        selfBrokerUrl = "tcp://localhost:" + port;
        pulsar = mock(PulsarService.class);
        config = mock(ServiceConfiguration.class);
        executor = new OrderedSafeExecutor(1, "test");
        zkCache = new LocalZooKeeperCache(MockZooKeeper.newInstance(), executor);
        localCache = new LocalZooKeeperCacheService(zkCache, null);
        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
        nsService = mock(NamespaceService.class);
        brokerService = mock(BrokerService.class);
        doReturn(CompletableFuture.completedFuture(1)).when(brokerService).unloadServiceUnit(anyObject());

        doReturn(zkCache).when(pulsar).getLocalZkCache();
        doReturn(localCache).when(pulsar).getLocalZkCacheService();
        doReturn(config).when(pulsar).getConfiguration();
        doReturn(nsService).when(pulsar).getNamespaceService();
        doReturn(port).when(config).getBrokerServicePort();
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(host(config)).when(pulsar).getHost();
        doReturn(webAddress(config)).when(pulsar).getWebServiceAddress();
        doReturn(selfBrokerUrl).when(pulsar).getBrokerServiceUrl();
    }

    @AfterMethod
    public void teardown() throws Exception {
        executor.shutdown();
    }

    @Test
    public void testConstructor() {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        assertNotNull(cache);
        assertNotNull(cache.getOwnedServiceUnits());
    }

    @Test
    public void testDisableOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-1");
        try {
            assertTrue(cache.getOwner(testNs) == null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }
        NamespaceEphemeralData data1 = cache.getOrSetOwner(testNs);
        assertTrue(!data1.isDisabled());
        cache.disableOwnership(testNs);
        // force the next read to get directly from ZK
        // localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testNs));
        data1 = cache.getOwner(testNs);
        assertTrue(data1.isDisabled());
    }

    @Test
    public void testGetOrSetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceBundle testFullBundle = bundleFactory.getFullBundle(new NamespaceName("pulsar/test/ns-2"));
        // case 1: no one owns the namespace
        try {
            assertTrue(cache.getOwner(testFullBundle) == null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }
        NamespaceEphemeralData data1 = cache.getOrSetOwner(testFullBundle);
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertTrue(!data1.isDisabled());
        // case 2: the local broker owned the namespace and disabled, getOrSetOwner() should not change it
        OwnedServiceUnit nsObj = cache.getOwnedServiceUnit(testFullBundle);
        // this would disable the ownership
        doReturn(cache).when(nsService).getOwnershipCache();
        nsObj.handleUnloadRequest(pulsar);
        Thread.sleep(1000);

        data1 = cache.getOrSetOwner(testFullBundle);
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        // case 3: some other broker owned the namespace, getOrSetOwner() should return other broker's URL
        zkCache.getZooKeeper().delete(ServiceUnitZkUtils.path(testFullBundle), -1);
        // The only chance that we lost an already existing ephemeral node is when the broker dies or unload has
        // succeeded in both cases, the ownerInfoCache will be updated (i.e. invalidated the entry)
        localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testFullBundle));
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testFullBundle),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://localhost:8080", "https://localhost:4443", false));
        data1 = cache.getOrSetOwner(testFullBundle);
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertTrue(!data1.isDisabled());

    }

    @Test
    public void testGetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-3");
        // case 1: no one owns the namespace
        try {
            assertTrue(cache.getOwner(testNs) == null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }
        // case 2: someone owns the namespace
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testNs),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://otherhost:8080", "https://otherhost:4443", false));
        // try to acquire, which will load the read-only cache
        NamespaceEphemeralData data1 = cache.getOrSetOwner(testNs);
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertTrue(!data1.isDisabled());
        // Now do getOwner and compare w/ the returned values
        NamespaceEphemeralData readOnlyData = cache.getOwner(testNs);
        assertEquals(data1, readOnlyData);

        MockZooKeeper mockZk = (MockZooKeeper) zkCache.getZooKeeper();
        mockZk.failNow(KeeperException.Code.NONODE);
        try {
            cache.getOwner(new NamespaceName("pulsar/test/ns-none"));
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK, expected
        }
    }

    @Test
    public void testGetOwnedServiceUnit() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-5");
        NamespaceBundle testBundle = bundleFactory.getFullBundle(testNs);
        // case 1: no one owns the namespace
        try {
            assertTrue(cache.getOwner(testBundle) == null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }
        try {
            checkNotNull(cache.getOwnedServiceUnit(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }
        // case 2: someone else owns the namespace
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testBundle),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://otherhost:8080", "https://otherhost:4443", false));
        try {
            checkNotNull(cache.getOwnedServiceUnit(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }
        // try to acquire, which will load the read-only cache
        NamespaceEphemeralData data1 = cache.getOrSetOwner(testBundle);
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertTrue(!data1.isDisabled());
        try {
            checkNotNull(cache.getOwnedServiceUnit(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }
        // case 3: this broker owns the namespace
        // delete the ephemeral node by others
        zkCache.getZooKeeper().delete(ServiceUnitZkUtils.path(testBundle), -1);
        // force to read directly from ZK
        localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testBundle));
        data1 = cache.getOrSetOwner(testBundle);
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertTrue(!data1.isDisabled());
        assertNotNull(cache.getOwnedServiceUnit(testBundle));
    }

    @Test
    public void testGetOwnedServiceUnits() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-6");
        NamespaceBundle testBundle = bundleFactory.getFullBundle(testNs);
        // case 1: no one owns the namespace
        try {
            assertTrue(cache.getOwner(testBundle) == null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }

        assertTrue(cache.getOwnedServiceUnits().isEmpty());

        // case 2: someone else owns the namespace
        ServiceUnitZkUtils.acquireNameSpace(zkCache.getZooKeeper(), ServiceUnitZkUtils.path(testBundle),
                new NamespaceEphemeralData("pulsar://otherhost:8881", "pulsar://otherhost:8884",
                        "http://otherhost:8080", "https://otherhost:4443", false));
        assertTrue(cache.getOwnedServiceUnits().isEmpty());
        // try to acquire, which will load the read-only cache
        NamespaceEphemeralData data1 = cache.getOrSetOwner(testBundle);
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertTrue(!data1.isDisabled());
        assertTrue(cache.getOwnedServiceUnits().isEmpty());
        // case 3: this broker owns the namespace
        // delete the ephemeral node by others
        zkCache.getZooKeeper().delete(ServiceUnitZkUtils.path(testBundle), -1);
        // force to read directly from ZK
        localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testBundle));
        data1 = cache.getOrSetOwner(testBundle);
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertTrue(!data1.isDisabled());
        assertTrue(cache.getOwnedServiceUnits().size() == 1);
    }

    @Test
    public void testRemoveOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-7");
        NamespaceBundle bundle = bundleFactory.getFullBundle(testNs);
        // case 1: no one owns the namespace
        try {
            assertTrue(cache.getOwner(bundle) == null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }

        cache.removeOwnership(bundle);
        assertTrue(cache.getOwnedServiceUnits().isEmpty());

        // case 2: this broker owns the namespace
        NamespaceEphemeralData data1 = cache.getOrSetOwner(bundle);
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertTrue(!data1.isDisabled());
        assertTrue(cache.getOwnedServiceUnits().size() == 1);
        cache.removeOwnership(bundle);
        assertTrue(cache.getOwnedServiceUnits().isEmpty());
        try {
            zkCache.getZooKeeper().getData(ServiceUnitZkUtils.path(bundle), null, null);
            fail("Should have failed");
        } catch (NoNodeException nne) {
            // OK
        }
    }

}
