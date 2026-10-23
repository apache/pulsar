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
package org.apache.pulsar.tests.integration.cli.tenant;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;

public class TenantTest extends PulsarTestSuite {
    @Test
    public void testListTenantCmd() throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("tenants", "list");
        assertTrue(result.getStdout().contains("public"));
    }

    @Test
    public void testGetTenantCmd() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "get", "public");
    }

    @Test
    public void testGetNonExistTenantCmd() {
        String tenantName = randomName();
        ContainerExecException ex = expectThrows(ContainerExecException.class,
                () -> pulsarCluster.runAdminCommandOnAnyBroker("tenants", "get", tenantName));
        assertTrue(ex.getResult().getStderr().contains("Tenant does not exist"));
    }

    @Test
    public void testCreateTenantCmd() throws Exception {
        String tenantName = randomName();
        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "create", tenantName);

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("tenants", "get", tenantName);
        TenantInfo tenantInfo = jsonMapper().readValue(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertTrue(tenantInfo.getAdminRoles().isEmpty());
        assertFalse(tenantInfo.getAllowedClusters().isEmpty());

        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "delete", tenantName);
    }

    @Test
    public void testCreateTenantCmdWithAdminRolesAndAllowClustersFlags() throws Exception {
        String tenantName = randomName();
        List<String> adminRoles = Arrays.asList("role1", "role2");
        List<String> allowedClusters = Collections.singletonList(pulsarCluster.getClusterName());
        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "create", tenantName, "--admin-roles",
                String.join(",", adminRoles), "--allowed-clusters", String.join(",", allowedClusters));

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("tenants", "get", tenantName);
        TenantInfo tenantInfo = jsonMapper().readValue(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertNotNull(tenantInfo.getAdminRoles());
        assertEquals(tenantInfo.getAdminRoles().stream().sorted().toArray(), adminRoles.stream().sorted().toArray());
        assertEquals(tenantInfo.getAllowedClusters(), allowedClusters);

        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "delete", tenantName);
    }

    @Test
    public void testCreateExistTenantCmd() {
        ContainerExecException ex = expectThrows(ContainerExecException.class,
                () -> pulsarCluster.runAdminCommandOnAnyBroker("tenants", "create", "public"));
        assertTrue(ex.getResult().getStderr().contains("Tenant already exist"));
    }

    @Test
    public void testUpdateTenantCmdWithAdminRolesAndAllowedClustersFlags() throws Exception {
        String tenantName = randomName();
        List<String> adminRoles = Arrays.asList("role1", "role2");
        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "create", tenantName, "--admin-roles",
                String.join(",", adminRoles));

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("tenants", "get", tenantName);
        TenantInfo tenantInfo = jsonMapper().readValue(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertNotNull(tenantInfo.getAdminRoles());
        assertEquals(tenantInfo.getAdminRoles().stream().sorted().toArray(), adminRoles.stream().sorted().toArray());
        assertFalse(tenantInfo.getAllowedClusters().isEmpty());

        adminRoles = Arrays.asList("role3", "role4");
        List<String> allowedClusters = Collections.singletonList(pulsarCluster.getClusterName());
        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "update", tenantName, "--admin-roles",
                String.join(",", adminRoles), "--allowed-clusters", String.join(",", allowedClusters));

        result = pulsarCluster.runAdminCommandOnAnyBroker("tenants", "get", tenantName);
        tenantInfo = jsonMapper().readValue(result.getStdout(), TenantInfo.class);
        assertNotNull(tenantInfo);
        assertEquals(tenantInfo.getAdminRoles().stream().sorted().toArray(), adminRoles.stream().sorted().toArray());
        assertEquals(tenantInfo.getAllowedClusters(), allowedClusters);

        pulsarCluster.runAdminCommandOnAnyBroker("tenants", "delete", tenantName);
    }
}
