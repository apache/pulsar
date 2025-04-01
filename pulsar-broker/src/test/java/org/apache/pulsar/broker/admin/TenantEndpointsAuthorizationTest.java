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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Set;
import java.util.UUID;

@Test(groups = "broker-admin")
public class TenantEndpointsAuthorizationTest extends MockedPulsarStandalone {

    private AuthorizationService orignalAuthorizationService;
    private AuthorizationService spyAuthorizationService;

    private PulsarAdmin superUserAdmin;
    private PulsarAdmin nobodyAdmin;

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    public void setup() {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        start();
        this.superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        this.nobodyAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(NOBODY_TOKEN))
                .build();
    }

    @BeforeMethod(alwaysRun = true)
    public void before() throws IllegalAccessException {
        orignalAuthorizationService = getPulsarService().getBrokerService().getAuthorizationService();
        spyAuthorizationService = spy(orignalAuthorizationService);
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                spyAuthorizationService, true);
    }

    @AfterMethod(alwaysRun = true)
    public void after() throws IllegalAccessException {
        if (orignalAuthorizationService != null) {
            FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService", orignalAuthorizationService, true);
        }
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        if (superUserAdmin != null) {
            superUserAdmin.close();
            superUserAdmin = null;
        }
        spyAuthorizationService = null;
        orignalAuthorizationService = null;
        super.close();
    }

    @Test
    public void testListTenants() throws PulsarAdminException {
        superUserAdmin.tenants().getTenants();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowTenantOperationAsync(isNull(), Mockito.eq(TenantOperation.LIST_TENANTS), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.tenants().getTenants());
    }


    @Test
    public void testGetTenant() throws PulsarAdminException {
        String tenantName = "public";
        superUserAdmin.tenants().getTenantInfo(tenantName);
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowTenantOperationAsync(eq(tenantName), Mockito.eq(TenantOperation.GET_TENANT), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.tenants().getTenantInfo(tenantName));
    }

    @Test
    public void testUpdateTenant() throws PulsarAdminException {
        String tenantName = "public";
        superUserAdmin.tenants().updateTenant(tenantName, TenantInfo.builder()
                .allowedClusters(Set.of(getPulsarService().getConfiguration().getClusterName()))
                .adminRoles(Set.of("example")).build());
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowTenantOperationAsync(eq(tenantName), Mockito.eq(TenantOperation.UPDATE_TENANT), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.tenants()
                .updateTenant(tenantName, TenantInfo.builder().adminRoles(Set.of("example")).build()));
    }

    @Test
    public void testDeleteTenant() throws PulsarAdminException {
        String tenantName = UUID.randomUUID().toString();
        superUserAdmin.tenants().createTenant(tenantName, TenantInfo.builder()
                .allowedClusters(Set.of(getPulsarService().getConfiguration().getClusterName()))
                .adminRoles(Set.of("example")).build());

        Mockito.clearInvocations(spyAuthorizationService);
        superUserAdmin.tenants().deleteTenant(tenantName);
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowTenantOperationAsync(eq(tenantName), Mockito.eq(TenantOperation.DELETE_TENANT), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.tenants().deleteTenant(tenantName));
    }
}
