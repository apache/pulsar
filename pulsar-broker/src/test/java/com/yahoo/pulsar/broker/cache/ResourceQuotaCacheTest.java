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
package com.yahoo.pulsar.broker.cache;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.MockZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.hash.Hashing;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.ResourceQuota;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.cache.LocalZooKeeperCacheService;
import com.yahoo.pulsar.broker.cache.ResourceQuotaCache;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;

public class ResourceQuotaCacheTest {

    private PulsarService pulsar;
    private ZooKeeperCache zkCache;
    private LocalZooKeeperCacheService localCache;
    private NamespaceBundleFactory bundleFactory;
    private OrderedSafeExecutor executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setup() throws Exception {
        pulsar = mock(PulsarService.class);
        executor = new OrderedSafeExecutor(1, "test");
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        zkCache = new LocalZooKeeperCache(MockZooKeeper.newInstance(), executor, scheduledExecutor);
        localCache = new LocalZooKeeperCacheService(zkCache, null);
        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());

        doReturn(zkCache).when(pulsar).getLocalZkCache();
        doReturn(localCache).when(pulsar).getLocalZkCacheService();
    }

    @AfterMethod
    public void teardown() {
        executor.shutdown();
        scheduledExecutor.shutdown();
    }

    @Test
    public void testGetSetDefaultQuota() throws Exception {
        ResourceQuotaCache cache = new ResourceQuotaCache(zkCache);
        ResourceQuota quota1 = ResourceQuotaCache.getInitialQuotaValue();
        ResourceQuota quota2 = new ResourceQuota();
        quota2.setMsgRateIn(10);
        quota2.setMsgRateOut(20);
        quota2.setBandwidthIn(10000);
        quota2.setBandwidthOut(20000);
        quota2.setMemory(100);
        quota2.setDynamic(false);

        assertEquals(cache.getDefaultQuota(), quota1);
        cache.setDefaultQuota(quota2);
        assertEquals(cache.getDefaultQuota(), quota2);
    }

    @Test
    public void testGetSetBundleQuota() throws Exception {
        ResourceQuotaCache cache = new ResourceQuotaCache(zkCache);
        NamespaceBundle testBundle = bundleFactory.getFullBundle(new NamespaceName("pulsar/test/ns-2"));
        ResourceQuota quota1 = ResourceQuotaCache.getInitialQuotaValue();
        ResourceQuota quota2 = new ResourceQuota();
        quota2.setMsgRateIn(10);
        quota2.setMsgRateOut(20);
        quota2.setBandwidthIn(10000);
        quota2.setBandwidthOut(20000);
        quota2.setMemory(100);
        quota2.setDynamic(false);

        assertEquals(cache.getQuota(testBundle), quota1);
        cache.setQuota(testBundle, quota2);
        assertEquals(cache.getQuota(testBundle), quota2);
        cache.unsetQuota(testBundle);
        assertEquals(cache.getQuota(testBundle), quota1);
    }
}
