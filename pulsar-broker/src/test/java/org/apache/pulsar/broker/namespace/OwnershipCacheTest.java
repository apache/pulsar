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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import java.util.EnumSet;
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
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class OwnershipCacheTest {
    private static final Logger log = LoggerFactory.getLogger(OwnershipCacheTest.class);

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
        config = mock(ServiceConfiguration.class);
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
        brokerService = mock(BrokerService.class);
        doReturn(CompletableFuture.completedFuture(1)).when(brokerService)
                .unloadServiceUnit(any(), anyBoolean(), anyLong(), any());

        doReturn(config).when(pulsar).getConfiguration();
        doReturn(nsService).when(pulsar).getNamespaceService();
        doReturn(Optional.of(port)).when(config).getBrokerServicePort();
        doReturn(Optional.empty()).when(config).getWebServicePort();
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
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);
        assertNotNull(cache);
        assertNotNull(cache.getOwnedBundles());
    }

    @Test
    public void testDisableOwnership() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);

        NamespaceBundle testBundle = new NamespaceBundle(NamespaceName.get("pulsar/test/ns-1"),
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
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);
        NamespaceBundle testFullBundle = new NamespaceBundle(NamespaceName.get("pulsar/test/ns-2"),
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
                        new NamespaceEphemeralData("pulsar://otherhost:8881",
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
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);
        NamespaceBundle testBundle = new NamespaceBundle(NamespaceName.get("pulsar/test/ns-3"),
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
                        new NamespaceEphemeralData("pulsar://otherhost:8881",
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


        NamespaceBundle noneBundle = new NamespaceBundle(NamespaceName.get("pulsar/test/ns-none"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        Optional<NamespaceEphemeralData> res = cache
                .getOwnerAsync(noneBundle).get();
        assertFalse(res.isPresent());
    }

    @Test
    public void testGetOwnedServiceUnit() throws Exception {
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);
        NamespaceName testNs = NamespaceName.get("pulsar/test/ns-5");
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
                        new NamespaceEphemeralData("pulsar://otherhost:8881",
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
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);
        NamespaceName testNs = NamespaceName.get("pulsar/test/ns-6");
        NamespaceBundle testBundle = new NamespaceBundle(testNs,
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        // case 1: no one owns the namespace
        assertFalse(cache.getOwnerAsync(testBundle).get().isPresent());

        assertTrue(cache.getOwnedBundles().isEmpty());

        // case 2: someone else owns the namespace
        otherStore.put(ServiceUnitUtils.path(testBundle),
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(
                        new NamespaceEphemeralData("pulsar://otherhost:8881",
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
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);
        NamespaceName testNs = NamespaceName.get("pulsar/test/ns-7");
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
        OwnershipCache cache = new OwnershipCache(this.pulsar, bundleFactory, nsService);
        NamespaceBundle testFullBundle = new NamespaceBundle(NamespaceName.get("pulsar/test/ns-8"),
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

}
