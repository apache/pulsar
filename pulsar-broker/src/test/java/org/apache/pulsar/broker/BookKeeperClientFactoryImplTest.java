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
package org.apache.pulsar.broker;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_VALIDATION;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_MINIMUM_REGIONS_FOR_DURABILITY;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_REGIONS_TO_WRITE;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.CachedDNSToSwitchMapping;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;

/**
 * Unit test {@link BookKeeperClientFactoryImpl}.
 */
public class BookKeeperClientFactoryImplTest {

    @Test
    public void testSetDefaultEnsemblePlacementPolicyRackAwareDisabled() {
        AtomicReference<ZooKeeperCache> rackawarePolicyZkCache = new AtomicReference<>();
        AtomicReference<ZooKeeperCache> clientIsolationZkCache = new AtomicReference<>();
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();
        ZooKeeper zkClient = mock(ZooKeeper.class);

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertNull(bkConf.getProperty(REPP_DNS_RESOLVER_CLASS));

        BookKeeperClientFactoryImpl.setDefaultEnsemblePlacementPolicy(
            rackawarePolicyZkCache,
            clientIsolationZkCache,
            bkConf,
            conf,
            zkClient
        );

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertEquals(
            bkConf.getProperty(REPP_DNS_RESOLVER_CLASS),
            ZkBookieRackAffinityMapping.class.getName());

        ((ZooKeeperCache) bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE)).stop();
    }

    @Test
    public void testSetDefaultEnsemblePlacementPolicyRackAwareEnabled() {
        AtomicReference<ZooKeeperCache> rackawarePolicyZkCache = new AtomicReference<>();
        AtomicReference<ZooKeeperCache> clientIsolationZkCache = new AtomicReference<>();
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();
        ZooKeeper zkClient = mock(ZooKeeper.class);

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertNull(bkConf.getProperty(REPP_DNS_RESOLVER_CLASS));

        conf.setBookkeeperClientRegionawarePolicyEnabled(true);

        BookKeeperClientFactoryImpl.setDefaultEnsemblePlacementPolicy(
            rackawarePolicyZkCache,
            clientIsolationZkCache,
            bkConf,
            conf,
            zkClient
        );

        assertTrue(bkConf.getBoolean(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getString(REPP_REGIONS_TO_WRITE));
        assertEquals(2, bkConf.getInt(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertTrue(bkConf.getBoolean(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertEquals(
            bkConf.getProperty(REPP_DNS_RESOLVER_CLASS),
            ZkBookieRackAffinityMapping.class.getName());

        ((ZooKeeperCache) bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE)).stop();
    }

    @Test
    public void testSetDefaultEnsemblePlacementPolicyRackAwareEnabledChangedValues() {
        AtomicReference<ZooKeeperCache> rackawarePolicyZkCache = new AtomicReference<>();
        AtomicReference<ZooKeeperCache> clientIsolationZkCache = new AtomicReference<>();
        ClientConfiguration bkConf = new ClientConfiguration();
        ServiceConfiguration conf = new ServiceConfiguration();
        ZooKeeper zkClient = mock(ZooKeeper.class);

        assertNull(bkConf.getProperty(REPP_ENABLE_VALIDATION));
        assertNull(bkConf.getProperty(REPP_REGIONS_TO_WRITE));
        assertNull(bkConf.getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertNull(bkConf.getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertNull(bkConf.getProperty(REPP_DNS_RESOLVER_CLASS));

        conf.setBookkeeperClientRegionawarePolicyEnabled(true);
        conf.getProperties().setProperty(REPP_ENABLE_VALIDATION, "false");
        conf.getProperties().setProperty(REPP_REGIONS_TO_WRITE, "region1;region2");
        conf.getProperties().setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, "4");
        conf.getProperties().setProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, "false");
        conf.getProperties().setProperty(REPP_DNS_RESOLVER_CLASS, CachedDNSToSwitchMapping.class.getName());

        BookKeeperClientFactoryImpl.setDefaultEnsemblePlacementPolicy(
            rackawarePolicyZkCache,
            clientIsolationZkCache,
            bkConf,
            conf,
            zkClient
        );

        assertFalse(bkConf.getBoolean(REPP_ENABLE_VALIDATION));
        assertEquals("region1;region2", bkConf.getString(REPP_REGIONS_TO_WRITE));
        assertEquals(4, bkConf.getInt(REPP_MINIMUM_REGIONS_FOR_DURABILITY));
        assertFalse(bkConf.getBoolean(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE));
        assertEquals(
            bkConf.getProperty(REPP_DNS_RESOLVER_CLASS),
            CachedDNSToSwitchMapping.class.getName());

        ((ZooKeeperCache) bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE)).stop();
    }

    @Test
    public void testSetDiskWeightBasedPlacementEnabled() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        assertFalse(factory.createBkClientConfiguration(conf).getDiskWeightBasedPlacementEnabled());
        conf.setBookkeeperDiskWeightBasedPlacementEnabled(true);
        assertTrue(factory.createBkClientConfiguration(conf).getDiskWeightBasedPlacementEnabled());
    }

    @Test
    public void testSetExplicitLacInterval() {
        BookKeeperClientFactoryImpl factory = new BookKeeperClientFactoryImpl();
        ServiceConfiguration conf = new ServiceConfiguration();
        assertEquals(factory.createBkClientConfiguration(conf).getExplictLacInterval(), 0);
        conf.setBookkeeperExplicitLacIntervalInMills(5);
        assertEquals(factory.createBkClientConfiguration(conf).getExplictLacInterval(), 5);
    }

}
