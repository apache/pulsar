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
import static org.apache.pulsar.broker.PulsarService.webAddress;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.OwnedBundle;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.namespace.ServiceUnitZkUtils;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.MockZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.hash.Hashing;

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
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setup() throws Exception {
        final int port = 8080;
        selfBrokerUrl = "tcp://localhost:" + port;
        pulsar = mock(PulsarService.class);
        config = mock(ServiceConfiguration.class);
        executor = new OrderedSafeExecutor(1, "test");
        scheduledExecutor = Executors.newScheduledThreadPool(2);
        zkCache = new LocalZooKeeperCache(MockZooKeeper.newInstance(), executor, scheduledExecutor);
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
        doReturn(webAddress(config)).when(pulsar).getWebServiceAddress();
        doReturn(selfBrokerUrl).when(pulsar).getBrokerServiceUrl();
    }

    @AfterMethod
    public void teardown() throws Exception {
        executor.shutdown();
        scheduledExecutor.shutdown();
    }

    @Test
    public void testConstructor() {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        assertNotNull(cache);
        assertNotNull(cache.getOwnedBundles());
    }

    @Test
    public void testDisableOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);

        NamespaceBundle testBundle = bundleFactory.getFullBundle(new NamespaceName("pulsar/test/ns-1"));
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertTrue(!data1.isDisabled());
        cache.disableOwnership(testBundle);
        // force the next read to get directly from ZK
        // localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testNs));
        data1 = cache.getOwnerAsync(testBundle).get().get();
        assertTrue(data1.isDisabled());
    }

    @Test
    public void testGetOrSetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceBundle testFullBundle = bundleFactory.getFullBundle(new NamespaceName("pulsar/test/ns-2"));
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testFullBundle).get().isPresent());

        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testFullBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertTrue(!data1.isDisabled());
        // case 2: the local broker owned the namespace and disabled, getOrSetOwner() should not change it
        OwnedBundle nsObj = cache.getOwnedBundle(testFullBundle);
        // this would disable the ownership
        doReturn(cache).when(nsService).getOwnershipCache();
        nsObj.handleUnloadRequest(pulsar);
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
        assertTrue(!data1.isDisabled());

    }

    @Test
    public void testGetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceBundle testBundle = bundleFactory.getFullBundle(new NamespaceName("pulsar/test/ns-3"));
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
        assertTrue(!data1.isDisabled());
        // Now do getOwner and compare w/ the returned values
        NamespaceEphemeralData readOnlyData = cache.getOwnerAsync(testBundle).get().get();
        assertEquals(data1, readOnlyData);

        MockZooKeeper mockZk = (MockZooKeeper) zkCache.getZooKeeper();
        mockZk.failNow(KeeperException.Code.NONODE);
        Optional<NamespaceEphemeralData> res = cache
                .getOwnerAsync(bundleFactory.getFullBundle(new NamespaceName("pulsar/test/ns-none"))).get();
        assertFalse(res.isPresent());
    }

    @Test
    public void testGetOwnedServiceUnit() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-5");
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
        assertTrue(!data1.isDisabled());
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
        assertTrue(!data1.isDisabled());
        assertNotNull(cache.getOwnedBundle(testBundle));
    }

    @Test
    public void testGetOwnedServiceUnits() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-6");
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
        assertTrue(!data1.isDisabled());
        assertTrue(cache.getOwnedBundles().isEmpty());
        // case 3: this broker owns the namespace
        // delete the ephemeral node by others
        zkCache.getZooKeeper().delete(ServiceUnitZkUtils.path(testBundle), -1);
        // force to read directly from ZK
        localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testBundle));
        data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertTrue(!data1.isDisabled());
        assertTrue(cache.getOwnedBundles().size() == 1);
    }

    @Test
    public void testRemoveOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory);
        NamespaceName testNs = new NamespaceName("pulsar/test/ns-7");
        NamespaceBundle bundle = bundleFactory.getFullBundle(testNs);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(bundle).get().isPresent());

        cache.removeOwnership(bundle).get();
        assertTrue(cache.getOwnedBundles().isEmpty());

        // case 2: this broker owns the namespace
        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(bundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertTrue(!data1.isDisabled());
        assertTrue(cache.getOwnedBundles().size() == 1);
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
