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
package org.apache.pulsar.broker.namespace;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class OwnershipCacheTest {

    private PulsarService pulsar;
    private ServiceConfiguration config;
    private String selfBrokerUrl;
    private NamespaceBundleFactory bundleFactory;
    private NamespaceService nsService;
    private BrokerService brokerService;
    private OrderedScheduler executor;
    private MetadataStoreExtended store;
    private MetadataStoreExtended otherStore;
    private CoordinationService coordinationService;
    private ZookeeperServerTest zookeeperServer;

    @BeforeMethod
    public void setup() throws Exception {
        final int port = 8080;
        selfBrokerUrl = "tcp://localhost:" + port;
        pulsar = mock(PulsarService.class);
        config = new ServiceConfiguration();
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("test").build();
        zookeeperServer = new ZookeeperServerTest(0);
        zookeeperServer.start();

        store = MetadataStoreExtended.create(zookeeperServer.getHostPort(),
                MetadataStoreConfig.builder().sessionTimeoutMillis(5000).build());
        coordinationService = new CoordinationServiceImpl(store);
        otherStore = MetadataStoreExtended.create(zookeeperServer.getHostPort(),
                MetadataStoreConfig.builder().sessionTimeoutMillis(5000).build());
        when(pulsar.getConfigurationMetadataStore()).thenReturn(store);

        when(pulsar.getLocalMetadataStore()).thenReturn(store);
        when(pulsar.getConfigurationMetadataStore()).thenReturn(store);
        when(pulsar.getCoordinationService()).thenReturn(coordinationService);

        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
        nsService = mock(NamespaceService.class);
        doReturn(CompletableFuture.completedFuture(null)).when(nsService)
                .unloadNamespaceBundle(any(NamespaceBundle.class));
        brokerService = mock(BrokerService.class);
        doReturn(CompletableFuture.completedFuture(1)).when(brokerService)
                .unloadServiceUnit(any(), anyBoolean(), anyBoolean(), anyLong(), any());
        doReturn(CompletableFuture.completedFuture(1)).when(brokerService)
                .unloadServiceUnit(any(), anyBoolean(), anyBoolean(), anyLong(), any(), any());
        doReturn(Map.of()).when(brokerService).getTopicFuturesInBundle(any());

        doReturn(config).when(pulsar).getConfiguration();
        doReturn(nsService).when(pulsar).getNamespaceService();
        config.setBrokerServicePort(Optional.of(port));
        config.setWebServicePort(Optional.empty());
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(selfBrokerUrl).when(pulsar).getBrokerServiceUrl();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        executor.shutdownNow();
        coordinationService.close();
        store.close();
        otherStore.close();
        zookeeperServer.close();
    }

    @Test
    public void testConstructor() {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        assertNotNull(cache);
        assertNotNull(cache.getOwnedBundles());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDisableOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);

        NamespaceBundle testBundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-1"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertFalse(data1.isDisabled());
        cache.disableOwnership(testBundle).get();
        // force the next read to get directly from ZK
        // localCache.ownerInfoCache().invalidate(ServiceUnitZkUtils.path(testNs));
        data1 = cache.getOwnerAsync(testBundle).get().get();
        assertTrue(data1.isDisabled());
    }

    @Test
    public void testGetOrSetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceBundle testFullBundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-2"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testFullBundle).get().isPresent());

        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testFullBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        // case 2: the local broker owned the namespace and disabled, getOrSetOwner() should not change it
        OwnedBundle nsObj = cache.getOwnedBundle(testFullBundle);
        // this would disable the ownership
        doReturn(cache).when(nsService).getOwnershipCache();
        nsObj.handleUnloadRequest(pulsar, 5, TimeUnit.SECONDS).join();

        // case 3: some other broker owned the namespace, getOrSetOwner() should return other broker's URL
        // The only chance that we lost an already existing ephemeral node is when the broker dies or unload has
        // succeeded in both cases, the ownerInfoCache will be updated (i.e. invalidated the entry)
        @Cleanup
        MetadataStoreExtended otherStore = MetadataStoreExtended.create(zookeeperServer.getHostPort(),
                MetadataStoreConfig.builder().sessionTimeoutMillis(5000).build());
        otherStore.put(ServiceUnitUtils.path(testFullBundle),
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                        new NamespaceEphemeralData("otherhost:8881", "pulsar://otherhost:8881",
                                "pulsar://otherhost:8884",
                                "http://localhost:8080",
                                "https://localhost:4443", false)),
                Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral))
                .join();

        try {
            cache.tryAcquiringOwnership(testFullBundle).get();
            fail("Should fail to acquire");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.LockBusyException.class);
        }

        data1 = cache.getOwnerAsync(testFullBundle).join().get();
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());
    }

    @Test
    public void testGetOwner() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceBundle testBundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-3"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());
        // case 2: someone owns the namespace

        @Cleanup
        MetadataStoreExtended otherStore = MetadataStoreExtended.create(zookeeperServer.getHostPort(),
                MetadataStoreConfig.builder().sessionTimeoutMillis(5000).build());
        otherStore.put(ServiceUnitUtils.path(testBundle),
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                        new NamespaceEphemeralData("otherhost:8881", "pulsar://otherhost:8881",
                                "pulsar://otherhost:8884",
                                "http://localhost:8080",
                                "https://localhost:4443", false)),
                Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral)).join();

        // try to acquire, which will load the read-only cache
        try {
            cache.tryAcquiringOwnership(testBundle).get();
            fail("Should fail to acquire");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.LockBusyException.class);
        }

        NamespaceEphemeralData data1 = cache.getOwnerAsync(testBundle).join().get();

        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());
        // Now do getOwner and compare w/ the returned values
        NamespaceEphemeralData readOnlyData = cache.getOwnerAsync(testBundle).get().get();
        assertEquals(data1, readOnlyData);


        NamespaceBundle noneBundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-none"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        Optional<NamespaceEphemeralData> res = cache
                .getOwnerAsync(noneBundle).get();
        assertFalse(res.isPresent());
    }

    @Test
    public void testGetOwnedServiceUnit() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceName testNs = NamespaceName.get("pulsar/ns-5");
        NamespaceBundle testBundle = new NamespaceBundle(testNs,
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        try {
            Objects.requireNonNull(cache.getOwnedBundle(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }
        // case 2: someone else owns the namespace
        otherStore.put(ServiceUnitUtils.path(testBundle),
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                        new NamespaceEphemeralData("otherhost:8881", "pulsar://otherhost:8881",
                                "pulsar://otherhost:8884",
                                "http://localhost:8080",
                                "https://localhost:4443", false)),
                Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral)).join();
        try {
            Objects.requireNonNull(cache.getOwnedBundle(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }

        // try to acquire, which will load the read-only cache
        try {
            cache.tryAcquiringOwnership(testBundle).get();
            fail("Should fail to acquire");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.LockBusyException.class);
        }

        NamespaceEphemeralData data1 = cache.getOwnerAsync(testBundle).join().get();

        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());
        try {
            Objects.requireNonNull(cache.getOwnedBundle(testBundle));
            fail("Should have failed");
        } catch (NullPointerException npe) {
            // OK for not owned namespace
        }
        // case 3: this broker owns the namespace
        // delete the ephemeral node by others
        otherStore.delete(ServiceUnitUtils.path(testBundle), Optional.empty()).join();

        data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        assertNotNull(cache.getOwnedBundle(testBundle));
    }

    @Test
    public void testGetOwnedServiceUnits() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceName testNs = NamespaceName.get("pulsar/ns-6");
        NamespaceBundle testBundle = new NamespaceBundle(testNs,
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        assertTrue(cache.getOwnedBundles().isEmpty());

        // case 2: someone else owns the namespace
        otherStore.put(ServiceUnitUtils.path(testBundle),
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                        new NamespaceEphemeralData("otherhost:8881", "pulsar://otherhost:8881",
                                "pulsar://otherhost:8884",
                                "http://otherhost:8080",
                                "https://otherhost:4443", false)),
                Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral)).join();
        assertTrue(cache.getOwnedBundles().isEmpty());

        Thread.sleep(500);

        // try to acquire, which will load the read-only cache
        try {
            cache.tryAcquiringOwnership(testBundle).get();
            fail("Should fail to acquire");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.LockBusyException.class);
        }

        NamespaceEphemeralData data1 = cache.getOwnerAsync(testBundle).join().get();
        assertEquals(data1.getNativeUrl(), "pulsar://otherhost:8881");
        assertEquals(data1.getNativeUrlTls(), "pulsar://otherhost:8884");
        assertFalse(data1.isDisabled());
        assertTrue(cache.getOwnedBundles().isEmpty());
        // case 3: this broker owns the namespace
        // delete the ephemeral node by others
        otherStore.delete(ServiceUnitUtils.path(testBundle), Optional.empty()).join();
        data1 = cache.tryAcquiringOwnership(testBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        assertEquals(cache.getOwnedBundles().size(), 1);
    }

    @Test
    public void testRemoveOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceName testNs = NamespaceName.get("pulsar/ns-7");
        NamespaceBundle bundle = new NamespaceBundle(testNs,
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
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
        Awaitility.await().untilAsserted(() -> {
            assertTrue(cache.getOwnedBundles().isEmpty());
            assertFalse(store.exists(ServiceUnitUtils.path(bundle)).join());
            assertNull(cache.getLocallyAcquiredLocks().get(bundle));
        });
    }

    @Test
    public void testReestablishOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceBundle testFullBundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-8"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        String testFullBundlePath = ServiceUnitUtils.path(testFullBundle);

        // no one owns the namespace
        assertFalse(cache.getOwnerAsync(testFullBundle).get().isPresent());
        assertNull(cache.getOwnedBundle(testFullBundle));

        // this broker owns the namespace
        NamespaceEphemeralData data1 = cache.tryAcquiringOwnership(testFullBundle).get();
        assertEquals(data1.getNativeUrl(), selfBrokerUrl);
        assertFalse(data1.isDisabled());
        assertNotNull(cache.getOwnedBundle(testFullBundle));

        // invalidate cache, reestablish ownership through query ownership
        NamespaceEphemeralData data2 = cache.getOwnerAsync(testFullBundle).get().get();
        assertEquals(data2.getNativeUrl(), selfBrokerUrl);
        assertFalse(data2.isDisabled());
        assertNotNull(cache.getOwnedBundle(testFullBundle));

        // invalidate cache, reestablish ownership through acquire ownership
        cache.invalidateLocalOwnerCache();
        assertNull(cache.getOwnedBundle(testFullBundle));
        NamespaceEphemeralData data3 = cache.tryAcquiringOwnership(testFullBundle).get();
        assertEquals(data3.getNativeUrl(), selfBrokerUrl);
        assertFalse(data3.isDisabled());
        assertNotNull(cache.getOwnedBundle(testFullBundle));

        assertTrue(cache.checkOwnershipAsync(testFullBundle).get());
        assertEquals(data2.getNativeUrl(), selfBrokerUrl);
        assertFalse(data2.isDisabled());
        assertNotNull(cache.getOwnedBundle(testFullBundle));
    }

    @Test
    public void testStaleUnloadDoesNotReleaseReacquiredOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        doReturn(cache).when(nsService).getOwnershipCache();
        NamespaceBundle bundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-stale-unload"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);

        cache.tryAcquiringOwnership(bundle).get();
        OwnedBundle staleOwnedBundle = cache.getOwnedBundle(bundle);
        assertNotNull(staleOwnedBundle);

        // Simulate the lock-expiry self-heal path: the expiry callback invalidated the local cache while the
        // unload it triggered is still in flight, and a concurrent lookup re-acquires the bundle in between.
        cache.invalidateLocalOwnerCache(bundle);
        cache.tryAcquiringOwnership(bundle).get();
        OwnedBundle reacquiredOwnedBundle = cache.getOwnedBundle(bundle);
        assertNotNull(reacquiredOwnedBundle);
        assertNotSame(reacquiredOwnedBundle, staleOwnedBundle);
        ResourceLock<NamespaceEphemeralData> reacquiredLock = cache.getLocallyAcquiredLocks().get(bundle);
        assertNotNull(reacquiredLock);

        // The stale unload chain now runs its remaining steps against the old OwnedBundle instance.
        staleOwnedBundle.handleUnloadRequest(pulsar, 5, TimeUnit.SECONDS).join();

        // The re-acquired ownership must survive the stale unload untouched.
        assertSame(cache.getLocallyAcquiredLocks().get(bundle), reacquiredLock);
        assertTrue(store.exists(ServiceUnitUtils.path(bundle)).join());
        assertTrue(reacquiredOwnedBundle.isActive());
        assertTrue(cache.checkOwnershipAsync(bundle).get());
    }

    @Test
    public void testTryAcquiringOwnershipWaitsForInFlightOwnedBundleRelease() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceBundle bundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-ownedbundle-release-inflight"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);

        cache.tryAcquiringOwnership(bundle).get();
        OwnedBundle gen1 = cache.getOwnedBundle(bundle);
        assertNotNull(gen1);

        // Mirror what OwnedBundle.handleUnloadRequest does at the very end of a normal unload: release via the
        // generation-aware, OwnedBundle-keyed overload. Do NOT wait for it to finish before racing an acquire
        // against it below.
        CompletableFuture<Void> removeFuture = cache.removeOwnership(gen1);

        // A concurrent lookup racing the in-flight release must be queued behind it by the barrier, not see the
        // stale, concurrently-releasing generation.
        NamespaceEphemeralData reacquired = cache.tryAcquiringOwnership(bundle).get();
        OwnedBundle afterReacquire = cache.getOwnedBundle(bundle);

        removeFuture.get(10, TimeUnit.SECONDS);

        assertNotSame(afterReacquire, gen1,
                "tryAcquiringOwnership returned the stale, concurrently-releasing generation instead of a fresh one");
        assertTrue(afterReacquire.isActive(), "reacquired OwnedBundle should be active");
        assertSame(cache.getLocallyAcquiredLocks().get(bundle), afterReacquire.getResourceLock());
    }

    @Test
    public void testRemoveOwnershipWithAcquisitionInFlight() throws Exception {
        // Gate the lock acquisition so the test can invoke removeOwnership while the acquisition is in flight
        LockManager<NamespaceEphemeralData> realLockManager =
                coordinationService.getLockManager(NamespaceEphemeralData.class);
        CompletableFuture<Void> gate = new CompletableFuture<>();
        LockManager<NamespaceEphemeralData> gatedLockManager = new LockManager<NamespaceEphemeralData>() {
            @Override
            public CompletableFuture<Optional<NamespaceEphemeralData>> readLock(String path) {
                return realLockManager.readLock(path);
            }

            @Override
            public CompletableFuture<ResourceLock<NamespaceEphemeralData>> acquireLock(String path,
                                                                                       NamespaceEphemeralData value) {
                return gate.thenCompose(__ -> realLockManager.acquireLock(path, value));
            }

            @Override
            public CompletableFuture<List<String>> listLocks(String path) {
                return realLockManager.listLocks(path);
            }

            @Override
            public CompletableFuture<Void> asyncClose() {
                return realLockManager.asyncClose();
            }

            @Override
            public void close() throws Exception {
                realLockManager.close();
            }
        };
        CoordinationService gatedCoordinationService = mock(CoordinationService.class);
        doReturn(gatedLockManager).when(gatedCoordinationService).getLockManager(NamespaceEphemeralData.class);
        doReturn(gatedCoordinationService).when(pulsar).getCoordinationService();

        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceBundle bundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-inflight-remove"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);

        CompletableFuture<NamespaceEphemeralData> acquireFuture = cache.tryAcquiringOwnership(bundle);
        assertFalse(acquireFuture.isDone());

        CompletableFuture<Void> removeFuture = cache.removeOwnership(bundle);
        gate.complete(null);
        acquireFuture.join();
        removeFuture.get(10, TimeUnit.SECONDS);

        // After removeOwnership reported success and the in-flight acquisition settled, the broker must not
        // silently retain (zombie) ownership.
        Awaitility.await().untilAsserted(() -> {
            assertTrue(cache.getLocallyAcquiredLocks().isEmpty());
            assertTrue(cache.getOwnedBundles().isEmpty());
            assertFalse(store.exists(ServiceUnitUtils.path(bundle)).join());
        });
    }

    @Test
    public void testExpiredLockIsRemovedFromLocallyAcquiredLocks() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceBundle bundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-expired-lock"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);

        cache.tryAcquiringOwnership(bundle).get();
        ResourceLock<NamespaceEphemeralData> lock = cache.getLocallyAcquiredLocks().get(bundle);
        assertNotNull(lock);

        // The lock dies without going through removeOwnership, like on a metadata session expiry
        lock.release().join();

        Awaitility.await().untilAsserted(() -> {
            assertTrue(cache.getOwnedBundles().isEmpty());
            assertTrue(cache.getLocallyAcquiredLocks().isEmpty());
        });
    }

    @Test
    public void testExpiryBeforePublicationDoesNotLeaveActiveZombieOwnership() throws Exception {
        // A ResourceLock whose expiry future is *already* completed by the time the loader attaches its
        // lock-expiry listener: this deterministically forces the listener to run synchronously, inside the
        // cache loader's thenApply, before the loader returns and the cache's own future for this bundle is
        // published as done.
        ResourceLock<NamespaceEphemeralData> alreadyExpiredLock = new ResourceLock<>() {
            @Override
            public String getPath() {
                return "/dummy";
            }

            @Override
            public NamespaceEphemeralData getValue() {
                return null;
            }

            @Override
            public CompletableFuture<Void> updateValue(NamespaceEphemeralData newValue) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> release() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> getLockExpiredFuture() {
                return CompletableFuture.completedFuture(null);
            }
        };
        // Gate the lock acquisition itself so it settles only after tryAcquiringOwnership(bundle) has already
        // returned to the caller: this ensures the cache has genuinely registered its (not yet done) future for
        // the bundle before the loader's thenApply — and the already-expired lock's listener inside it — runs,
        // matching how a real ZK acquisition callback fires on a different thread than the initial get() call.
        CompletableFuture<Void> gate = new CompletableFuture<>();
        LockManager<NamespaceEphemeralData> raceyLockManager = new LockManager<>() {
            @Override
            public CompletableFuture<Optional<NamespaceEphemeralData>> readLock(String path) {
                return CompletableFuture.completedFuture(Optional.empty());
            }

            @Override
            public CompletableFuture<ResourceLock<NamespaceEphemeralData>> acquireLock(String path,
                    NamespaceEphemeralData value) {
                return gate.thenApply(ignore -> alreadyExpiredLock);
            }

            @Override
            public CompletableFuture<List<String>> listLocks(String path) {
                return CompletableFuture.completedFuture(List.of());
            }

            @Override
            public CompletableFuture<Void> asyncClose() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void close() {
            }
        };
        CoordinationService raceyCoordinationService = mock(CoordinationService.class);
        doReturn(raceyLockManager).when(raceyCoordinationService).getLockManager(NamespaceEphemeralData.class);
        doReturn(raceyCoordinationService).when(pulsar).getCoordinationService();

        OwnershipCache cache = new OwnershipCache(this.pulsar, nsService);
        NamespaceBundle bundle = new NamespaceBundle(NamespaceName.get("pulsar/ns-expiry-before-publication"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);

        CompletableFuture<NamespaceEphemeralData> acquireFuture = cache.tryAcquiringOwnership(bundle);
        assertFalse(acquireFuture.isDone());

        gate.complete(null);

        // Expiry won the race: the acquisition itself must fail instead of publishing an OwnedBundle whose lock
        // is already gone.
        try {
            acquireFuture.get(10, TimeUnit.SECONDS);
            fail("acquisition should fail when the lock expired before ownership could be published");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }

        // A failed load must not leave any trace behind: no cache entry claiming ownership, and no lock
        // bookkeeping.
        Awaitility.await().untilAsserted(() -> {
            assertNull(cache.getOwnedBundle(bundle),
                    "cache still claims active ownership for a bundle whose lock expired before publication");
            assertTrue(cache.getLocallyAcquiredLocks().isEmpty());
            assertFalse(cache.checkOwnershipAsync(bundle).get());
        });
    }

}
