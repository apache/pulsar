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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.testng.annotations.Test;

public class TenantResourcesTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void testListTenantsReportsClosedStore(String provider, Supplier<String> urlSupplier) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            TenantResources resources = new TenantResources(store, 5);
            store.close();

            assertThatThrownBy(resources::listTenants)
                    .as(provider + ": listing tenants reports the closed store")
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testActiveNamespaceCheckIgnoresInvalidNamespaceData(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            TenantResources resources = new TenantResources(store, 5);
            String tenant = "tenant-" + UUID.randomUUID();
            store.put("/admin/policies/" + tenant + "/legacy", "not-json".getBytes(StandardCharsets.UTF_8),
                    Optional.of(-1L)).get(5, TimeUnit.SECONDS);

            assertThat(resources.hasActiveNamespace(tenant))
                    .as(provider + ": invalid namespace data is ignored")
                    .succeedsWithin(Duration.ofSeconds(5)).isNull();
        }
    }

    @Test(dataProvider = "impl")
    public void testActiveNamespaceCheckReportsClosedStore(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            TenantResources resources = new TenantResources(store, 5);
            String tenant = "tenant-" + UUID.randomUUID();
            store.close();

            assertThat(resources.hasActiveNamespace(tenant))
                    .as(provider + ": checking active namespaces reports the closed store")
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testActiveNamespaceCheckPreservesNamespaceReadFailure() throws Exception {
        try (ZKMetadataStore store = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), false)) {
            String tenant = "tenant-" + UUID.randomUUID();
            String path = "/admin/policies/" + tenant + "/namespace";
            store.put(path, "{}".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get(5, TimeUnit.SECONDS);
            // Listing the parent still succeeds, but the namespace node cannot be read.
            store.getZkClient().setACL(path,
                    Collections.singletonList(new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.ANYONE_ID_UNSAFE)), -1);
            TenantResources resources = new TenantResources(store, 5);

            assertThat(resources.hasActiveNamespace(tenant))
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(MetadataStoreException.class)
                    .havingCause().isInstanceOf(KeeperException.NoAuthException.class);
        }
    }
}
