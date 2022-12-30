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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NamespaceResourcesTest {

    private MetadataStore mockMetadataStore;
    private NamespaceResources namespaceResources;

    @BeforeMethod
    public void setUp() {
        mockMetadataStore = Mockito.mock(MetadataStore.class);
        Mockito.doReturn(Mockito.mock(MetadataCache.class)).when(mockMetadataStore).getMetadataCache(Policies.class);
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(mockMetadataStore).sync(Mockito.anyString());
        namespaceResources = new NamespaceResources(mockMetadataStore, 0);
    }
    @Test
    public void test_pathIsFromNamespace() {
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/clusters"));
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/policies"));
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/policies/my-tenant"));
        assertTrue(NamespaceResources.pathIsFromNamespace("/admin/policies/my-tenant/my-ns"));
    }

    @Test
    public void testGetPolicesAsync() {
        namespaceResources.getPoliciesAsync(NamespaceName.get("public/default"));
        Mockito.verify(mockMetadataStore, Mockito.never()).sync(Mockito.anyString());
    }

    @Test
    public void testGetPolicesAsyncAndRefresh() {
        namespaceResources.getPoliciesAsync(NamespaceName.get("public/default"), true);
        Mockito.verify(mockMetadataStore, Mockito.times(1)).sync("/admin/policies/public/default");
    }
}