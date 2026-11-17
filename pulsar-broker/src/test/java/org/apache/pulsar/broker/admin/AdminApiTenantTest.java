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
package org.apache.pulsar.broker.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@CustomLog
public class AdminApiTenantTest extends MockedPulsarServiceBaseTest {
    private static final String CLUSTER = "test";

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        admin.clusters()
                .createCluster(CLUSTER, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testListTenantsReportsClosedStore() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            TenantResources resources = new TenantResources(store, 5);
            store.close();

            assertThatThrownBy(resources::listTenants).isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testActiveNamespaceCheckIgnoresInvalidNamespaceData() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            TenantResources resources = new TenantResources(store, 5);
            String tenant = "tenant-" + UUID.randomUUID();
            store.put("/admin/policies/" + tenant + "/legacy", "not-json".getBytes(StandardCharsets.UTF_8),
                    Optional.of(-1L)).get(5, TimeUnit.SECONDS);

            assertThat(resources.hasActiveNamespace(tenant)).succeedsWithin(Duration.ofSeconds(5)).isNull();
        }
    }

    @Test
    public void testActiveNamespaceCheckReportsClosedStore() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            TenantResources resources = new TenantResources(store, 5);
            store.close();

            assertThat(resources.hasActiveNamespace("tenant"))
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testActiveNamespaceCheckPreservesNamespaceReadFailure() throws Exception {
        try (TestZKServer server = new TestZKServer();
                ZKMetadataStore store = new ZKMetadataStore(server.getConnectionString(),
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

    @Test
    public void testListTenant() throws PulsarAdminException {
        admin.tenants().getTenants();
    }

    @Test
    public void testCreateAndDeleteTenant() throws PulsarAdminException {
        String tenant = "test-tenant-" + UUID.randomUUID();
        admin.tenants().createTenant(tenant, TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER)).build());
        List<String> tenants = admin.tenants().getTenants();
        assertTrue(tenants.contains(tenant));
        admin.tenants().deleteTenant(tenant);
        tenants = admin.tenants().getTenants();
        assertFalse(tenants.contains(tenant));
    }

    @Test
    public void testDeleteNonExistTenant() {
        String tenant = "test-non-exist-tenant-" + UUID.randomUUID();
        assertThrows(PulsarAdminException.NotFoundException.class, () -> admin.tenants().deleteTenant(tenant));
    }

    @Test
    public void testCreateTenantConflictWhenCreatedConcurrently() {
        String tenant = "test-create-tenant-concurrently-" + UUID.randomUUID();
        // Simulate a concurrent request that creates the tenant after the existence check of this request
        mockZooKeeperGlobal.failConditional(KeeperException.Code.NODEEXISTS,
                (op, path) -> op == MockZooKeeper.Op.CREATE && path.equals("/admin/policies/" + tenant));
        assertThrows(PulsarAdminException.ConflictException.class,
                () -> admin.tenants().createTenant(tenant, TenantInfo.builder()
                        .allowedClusters(Collections.singleton(CLUSTER)).build()));
    }

    @Test
    public void testCreateTenantWithNull() {
        String tenant = "test-create-tenant-with-null-value-" + UUID.randomUUID();
        // Put doesn't allow null value
        assertThrows(PulsarAdminException.class,
                () -> admin.tenants().createTenant(tenant, null));
    }

    @Test
    public void testCreateTenantWithInvalidCluster() {
        String tenant = "test-create-tenant-with-invalid-cluster-" + UUID.randomUUID();
        // clusters is empty
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.tenants().createTenant(tenant, TenantInfo.builder().build()));

        // clusters is null
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> {
                    TenantInfoImpl tenantInfo = new TenantInfoImpl();
                    tenantInfo.setAdminRoles(null);
                    tenantInfo.setAllowedClusters(null);
                    admin.tenants().createTenant(tenant, tenantInfo);
                });
    }

    @Test
    public void testUpdateTenantWithInvalidCluster() throws PulsarAdminException {
        String tenant = "test-update-tenant-with-invalid-cluster-" + UUID.randomUUID();
        admin.tenants().createTenant(tenant,
                TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER)).build());

        // clusters is empty
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.tenants().updateTenant(tenant, TenantInfo.builder().build()));

        // clusters is null
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> {
                    TenantInfoImpl tenantInfo = new TenantInfoImpl();
                    tenantInfo.setAdminRoles(null);
                    tenantInfo.setAllowedClusters(null);
                    admin.tenants().updateTenant(tenant, tenantInfo);
                });
    }
}
