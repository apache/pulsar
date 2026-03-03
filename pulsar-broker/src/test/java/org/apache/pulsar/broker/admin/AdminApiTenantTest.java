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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
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
