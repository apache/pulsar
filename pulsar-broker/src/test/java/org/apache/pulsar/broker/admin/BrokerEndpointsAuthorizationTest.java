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


import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.BrokerOperation;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class BrokerEndpointsAuthorizationTest extends MockedPulsarStandalone {
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
    public void testGetActiveBroker() throws PulsarAdminException {
        superUserAdmin.brokers().getActiveBrokers();
        final String brokerId = getPulsarService().getBrokerId();
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_BROKERS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getActiveBrokers());
    }

    @Test
    public void testGetActiveBrokerWithCluster() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.brokers().getActiveBrokers(clusterName);
        final String brokerId = getPulsarService().getBrokerId();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_BROKERS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getActiveBrokers(clusterName));
    }

    @Test
    public void testGetLeaderBroker() throws PulsarAdminException {
        superUserAdmin.brokers().getLeaderBroker();
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.GET_LEADER_BROKER), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getLeaderBroker());
    }

    @Test
    public void testGetOwnedNamespaces() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().getOwnedNamespaces(clusterName, brokerId);
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_OWNED_NAMESPACES), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getOwnedNamespaces(clusterName, brokerId));
    }

    @Test
    public void testUpdateDynamicConfiguration() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().updateDynamicConfiguration("maxTenants", "10");
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.UPDATE_DYNAMIC_CONFIGURATION), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().updateDynamicConfiguration("maxTenants", "10"));
    }

    @Test
    public void testDeleteDynamicConfiguration() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().deleteDynamicConfiguration("maxTenants");
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.DELETE_DYNAMIC_CONFIGURATION), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().deleteDynamicConfiguration("maxTenants"));
    }


    @Test
    public void testGetAllDynamicConfiguration() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().getAllDynamicConfigurations();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_DYNAMIC_CONFIGURATIONS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getAllDynamicConfigurations());
    }


    @Test
    public void testGetDynamicConfigurationName() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().getDynamicConfigurationNames();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_DYNAMIC_CONFIGURATIONS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getDynamicConfigurationNames());
    }


    @Test
    public void testGetRuntimeConfiguration() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().getRuntimeConfigurations();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_RUNTIME_CONFIGURATIONS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getRuntimeConfigurations());
    }


    @Test
    public void testGetInternalConfigurationData() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().getInternalConfigurationData();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.GET_INTERNAL_CONFIGURATION_DATA), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getInternalConfigurationData());
    }


    @Test
    public void testBacklogQuotaCheck() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().backlogQuotaCheck();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.CHECK_BACKLOG_QUOTA), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().backlogQuotaCheck());
    }

    @Test
    public void testHealthCheck() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        final String brokerId = getPulsarService().getBrokerId();
        superUserAdmin.brokers().healthcheck(TopicVersion.V2);
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.HEALTH_CHECK), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () ->  nobodyAdmin.brokers().healthcheck(TopicVersion.V2));
    }
}
