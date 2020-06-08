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
package org.apache.pulsar.broker.cache;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.hash.Hashing;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ResourceQuotaCacheTest {

    private PulsarService pulsar;
    private ZooKeeperCache zkCache;
    private LocalZooKeeperCacheService localCache;
    private NamespaceBundleFactory bundleFactory;
    private OrderedScheduler executor;
    private MockZooKeeper zkc;

    @BeforeMethod
    public void setup() throws Exception {
        pulsar = mock(PulsarService.class);
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("test").build();
        zkc = MockZooKeeper.newInstance();
        zkCache = new LocalZooKeeperCache(zkc, 30, executor);
        localCache = new LocalZooKeeperCacheService(zkCache, null);

        // set mock pulsar localzkcache
        LocalZooKeeperCacheService localZkCache = mock(LocalZooKeeperCacheService.class);
        ZooKeeperDataCache<LocalPolicies> poilciesCache = mock(ZooKeeperDataCache.class);
        when(pulsar.getLocalZkCacheService()).thenReturn(localZkCache);
        when(localZkCache.policiesCache()).thenReturn(poilciesCache);
        doNothing().when(poilciesCache).registerListener(any());
        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());

        doReturn(zkCache).when(pulsar).getLocalZkCache();
        doReturn(localCache).when(pulsar).getLocalZkCacheService();
    }

    @AfterMethod
    public void teardown() throws Exception{
        executor.shutdown();
        zkCache.stop();
        zkc.shutdown();
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
        NamespaceBundle testBundle = bundleFactory.getFullBundle(NamespaceName.get("pulsar/test/ns-2"));
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
