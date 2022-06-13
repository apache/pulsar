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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BundlesQuotasTest {

    private MetadataStore store;
    private NamespaceBundleFactory bundleFactory;

    @BeforeMethod
    public void setup() throws Exception {
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());

        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        when(pulsar.getConfigurationMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        store.close();
    }

    @Test
    public void testGetSetDefaultQuota() throws Exception {
        BundlesQuotas bundlesQuotas = new BundlesQuotas(store);
        ResourceQuota quota2 = new ResourceQuota();
        quota2.setMsgRateIn(10);
        quota2.setMsgRateOut(20);
        quota2.setBandwidthIn(10000);
        quota2.setBandwidthOut(20000);
        quota2.setMemory(100);
        quota2.setDynamic(false);

        assertEquals(bundlesQuotas.getDefaultResourceQuota().join(), BundlesQuotas.INITIAL_QUOTA);
        bundlesQuotas.setDefaultResourceQuota(quota2).join();
        assertEquals(bundlesQuotas.getDefaultResourceQuota().join(), quota2);
    }

    @Test
    public void testGetSetBundleQuota() throws Exception {
        BundlesQuotas bundlesQuotas = new BundlesQuotas(store);
        NamespaceBundle testBundle = new NamespaceBundle(NamespaceName.get("pulsar/test/ns-2"),
                Range.closedOpen(0L, (long) Integer.MAX_VALUE),
                bundleFactory);
        ResourceQuota quota2 = new ResourceQuota();
        quota2.setMsgRateIn(10);
        quota2.setMsgRateOut(20);
        quota2.setBandwidthIn(10000);
        quota2.setBandwidthOut(20000);
        quota2.setMemory(100);
        quota2.setDynamic(false);

        assertEquals(bundlesQuotas.getResourceQuota(testBundle).join(), BundlesQuotas.INITIAL_QUOTA);
        bundlesQuotas.setResourceQuota(testBundle, quota2).join();
        assertEquals(bundlesQuotas.getResourceQuota(testBundle).join(), quota2);
        bundlesQuotas.resetResourceQuota(testBundle).join();
        assertEquals(bundlesQuotas.getResourceQuota(testBundle).join(), BundlesQuotas.INITIAL_QUOTA);
    }
}
