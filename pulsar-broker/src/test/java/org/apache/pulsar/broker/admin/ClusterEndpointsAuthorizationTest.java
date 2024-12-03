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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterOperation;
import org.apache.pulsar.common.policies.data.ClusterPolicies;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.LinkedHashSet;
import java.util.UUID;


@Test(groups = "broker-admin")
public class ClusterEndpointsAuthorizationTest extends MockedPulsarStandalone {

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
    public void testGetCluster() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().getCluster(clusterName);
        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName), eq(ClusterOperation.GET_CLUSTER), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getCluster(clusterName));
    }


    @Test
    public void testCreateCluster() throws PulsarAdminException {
        final String clusterName = UUID.randomUUID().toString();
        superUserAdmin.clusters().createCluster(clusterName, ClusterData.builder().build());
        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName), eq(ClusterOperation.CREATE_CLUSTER), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().createCluster(clusterName, ClusterData.builder().build()));
    }

    @Test
    public void testUpdateCluster() {
        final String clusterName = UUID.randomUUID().toString();
        try {
            superUserAdmin.clusters().updateCluster(clusterName, ClusterData.builder().serviceUrl("aaa").build());
        } catch (Throwable ignore) {

        }
        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName), eq(ClusterOperation.UPDATE_CLUSTER), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().updateCluster(clusterName, ClusterData.builder().build()));
    }


    @Test
    public void testGetClusterMigration() {
        final String clusterName = UUID.randomUUID().toString();
        try {
            superUserAdmin.clusters().getClusterMigration(clusterName);
        } catch (Throwable ignore) {

        }
        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName), eq(PolicyName.CLUSTER_MIGRATION),
                        eq(PolicyOperation.READ), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getClusterMigration(clusterName));
    }


    @Test
    public void testUpdateClusterMigration() throws PulsarAdminException {
        final String clusterName = UUID.randomUUID().toString();
        superUserAdmin.clusters().createCluster(clusterName, ClusterData.builder().build());
        Mockito.clearInvocations(spyAuthorizationService);

        superUserAdmin.clusters().updateClusterMigration(clusterName, false, new ClusterPolicies.ClusterUrl());
        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName), eq(PolicyName.CLUSTER_MIGRATION),
                        eq(PolicyOperation.WRITE), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters()
                        .updateClusterMigration(clusterName, false, new ClusterPolicies.ClusterUrl()));
    }

    @Test
    public void testSetPeerClusterNames() throws PulsarAdminException {
        final LinkedHashSet<String> linkedHashSet = new LinkedHashSet<>();
        linkedHashSet.add("a");
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        try {
            superUserAdmin.clusters().updatePeerClusterNames(clusterName, linkedHashSet);
        } catch (Throwable ignore) {

        }
        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName),
                        eq(ClusterOperation.UPDATE_PEER_CLUSTER), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().updatePeerClusterNames(clusterName, linkedHashSet));
    }

    @Test
    public void testGetPeerCluster() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().getPeerClusterNames(clusterName);

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName),
                        eq(ClusterOperation.GET_PEER_CLUSTER), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getPeerClusterNames(clusterName));
    }

    @Test
    public void testDeleteCluster() throws PulsarAdminException {
        final String clusterName = UUID.randomUUID().toString();
        superUserAdmin.clusters().createCluster(clusterName, ClusterData.builder().build());
        Mockito.clearInvocations(spyAuthorizationService);

        superUserAdmin.clusters().deleteCluster(clusterName);

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName),
                        eq(ClusterOperation.DELETE_CLUSTER), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().deleteCluster(clusterName));
    }


    @Test
    public void testGetNamespaceIsolationPolicies() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().getNamespaceIsolationPolicies(clusterName);

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName),
                        eq(PolicyName.NAMESPACE_ISOLATION), eq(PolicyOperation.READ), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getNamespaceIsolationPolicies(clusterName));
    }


    @Test
    public void testGetNamespaceIsolationPolicy() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().getNamespaceIsolationPolicy(clusterName, "");

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName),
                        eq(PolicyName.NAMESPACE_ISOLATION), eq(PolicyOperation.READ), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getNamespaceIsolationPolicy(clusterName, ""));
    }


    @Test
    public void testGetBrokersWithNamespaceIsolationPolicy() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().getBrokersWithNamespaceIsolationPolicy(clusterName);

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName),
                        eq(PolicyName.NAMESPACE_ISOLATION), eq(PolicyOperation.READ), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getBrokersWithNamespaceIsolationPolicy(clusterName));
    }


    @Test
    public void testGetBrokerWithNamespaceIsolationPolicy() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().getBrokerWithNamespaceIsolationPolicy(clusterName, getPulsarService().getBrokerId());

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName),
                        eq(PolicyName.NAMESPACE_ISOLATION), eq(PolicyOperation.READ), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getBrokerWithNamespaceIsolationPolicy(clusterName, ""));
    }


    @Test
    public void testSetNamespaceIsolationPolicy() {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();

        try {
            superUserAdmin.clusters().updateNamespaceIsolationPolicy(clusterName, "test",
                    NamespaceIsolationData.builder().build());
        } catch (Throwable ignore) {

        }

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName),
                        eq(PolicyName.NAMESPACE_ISOLATION), eq(PolicyOperation.WRITE), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().updateNamespaceIsolationPolicy(clusterName, "test",
                        NamespaceIsolationData.builder().build()));
    }

    @Test
    public void testDeleteNamespaceIsolationPolicy() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().deleteNamespaceIsolationPolicy(clusterName, "test");

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterPolicyOperationAsync(eq(clusterName),
                        eq(PolicyName.NAMESPACE_ISOLATION), eq(PolicyOperation.WRITE), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().deleteNamespaceIsolationPolicy(clusterName, "test"));
    }


    @Test
    public void testSetFailureDomain() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().updateFailureDomain(clusterName, "test", FailureDomain.builder().build());

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName), eq(ClusterOperation.UPDATE_FAILURE_DOMAIN), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().updateFailureDomain(clusterName,
                        "test", FailureDomain.builder().build()));
    }

    @Test
    public void testGetFailureDomains() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        superUserAdmin.clusters().getFailureDomains(clusterName);

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName), eq(ClusterOperation.GET_FAILURE_DOMAIN), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getFailureDomains(clusterName));
    }


    @Test
    public void testGetDomain() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        try {
            superUserAdmin.clusters().getFailureDomain(clusterName, "test");
        } catch (Throwable ignore) {

        }

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName), eq(ClusterOperation.GET_FAILURE_DOMAIN), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().getFailureDomain(clusterName, "test"));
    }

    @Test
    public void testDeleteFailureDomain() throws PulsarAdminException {
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        try {
            superUserAdmin.clusters().deleteFailureDomain(clusterName, "test");
        } catch (Throwable ignore) {

        }

        // test allow cluster operation
        verify(spyAuthorizationService)
                .allowClusterOperationAsync(eq(clusterName), eq(ClusterOperation.DELETE_FAILURE_DOMAIN), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());
        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> nobodyAdmin.clusters().deleteFailureDomain(clusterName, "test"));
    }


}
