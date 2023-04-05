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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class AdminApiTenantTest extends MockedPulsarServiceBaseTest {
    private final String CLUSTER = "test";

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        admin.clusters()
                .createCluster(CLUSTER, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
    }

    @BeforeClass(alwaysRun = true)
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
        String tenant = "test-tenant-"+ UUID.randomUUID();
        admin.tenants().createTenant(tenant, TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER)).build());
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
}
