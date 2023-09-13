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
package org.apache.pulsar.broker.resources;

import static org.apache.pulsar.broker.resources.BaseResources.joinPath;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NamespaceResourcesTest {

    private MetadataStore localStore;
    private MetadataStore configurationStore;
    private NamespaceResources namespaceResources;

    private static final String BUNDLE_DATA_BASE_PATH = "/loadbalance/bundle-data";

    @BeforeMethod
    public void setup() {
        localStore = mock(MetadataStore.class);
        configurationStore = mock(MetadataStore.class);
        namespaceResources = new NamespaceResources(localStore, configurationStore, 30);
    }

    @Test
    public void test_pathIsFromNamespace() {
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/clusters"));
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/policies"));
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/policies/my-tenant"));
        assertTrue(NamespaceResources.pathIsFromNamespace("/admin/policies/my-tenant/my-ns"));
    }

    /**
     *  Test that the bundle-data node is deleted from the local stores.
     */
    @Test
    public void testDeleteBundleDataAsync() {
        NamespaceName ns = NamespaceName.get("my-tenant/my-ns");
        String namespaceBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, ns.toString());
        namespaceResources.deleteBundleDataAsync(ns);

        String tenant="my-tenant";
        String tenantBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, tenant);
        namespaceResources.deleteBundleDataTenantAsync(tenant);

        verify(localStore).deleteRecursive(namespaceBundlePath);
        verify(localStore).deleteRecursive(tenantBundlePath);

        assertThrows(()-> verify(configurationStore).deleteRecursive(namespaceBundlePath));
        assertThrows(()-> verify(configurationStore).deleteRecursive(tenantBundlePath));
    }


}