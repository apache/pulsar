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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.v1.BrokerStats;
import org.apache.pulsar.broker.admin.v1.Brokers;
import org.apache.pulsar.broker.admin.v1.Clusters;
import org.apache.pulsar.broker.admin.v1.Namespaces;
import org.apache.pulsar.broker.admin.v1.PersistentTopics;
import org.apache.pulsar.broker.admin.v1.Properties;
import org.apache.pulsar.broker.admin.v1.ResourceQuotas;
import org.apache.pulsar.broker.admin.v2.SchemasResource;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AdminTest extends MockedPulsarServiceBaseTest {
    private final String configClusterName = "use";
    private Clusters clusters;
    private Properties properties;
    private Namespaces namespaces;
    private PersistentTopics persistentTopics;
    private Brokers brokers;
    private ResourceQuotas resourceQuotas;
    private BrokerStats brokerStats;
    private SchemasResource schemasResource;
    private Field uriField;

    public AdminTest() {
        super();
    }

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        conf.setClusterName(configClusterName);
        super.internalSetup();

        clusters = spy(new Clusters());
        clusters.setPulsar(pulsar);
        doReturn("test").when(clusters).clientAppId();
        doNothing().when(clusters).validateSuperUserAccess();

        properties = spy(new Properties());
        properties.setPulsar(pulsar);
        doReturn("test").when(properties).clientAppId();
        doNothing().when(properties).validateSuperUserAccess();

        namespaces = spy(new Namespaces());
        namespaces.setServletContext(new MockServletContext());
        namespaces.setPulsar(pulsar);
        doReturn("test").when(namespaces).clientAppId();
        doReturn(Sets.newTreeSet(Lists.newArrayList("use", "usw", "usc", "global"))).when(namespaces).clusters();
        doNothing().when(namespaces).validateAdminAccessForTenant("my-tenant");
        doNothing().when(namespaces).validateAdminAccessForTenant("other-tenant");
        doNothing().when(namespaces).validateAdminAccessForTenant("new-property");

        brokers = spy(new Brokers());
        brokers.setPulsar(pulsar);
        doReturn("test").when(brokers).clientAppId();
        doNothing().when(brokers).validateSuperUserAccess();

        uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);

        persistentTopics = spy(new PersistentTopics());
        persistentTopics.setServletContext(new MockServletContext());
        persistentTopics.setPulsar(pulsar);
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn("persistent").when(persistentTopics).domain();
        doReturn(Sets.newTreeSet(Lists.newArrayList("use", "usw", "usc"))).when(persistentTopics).clusters();
        doNothing().when(persistentTopics).validateAdminAccessForTenant("my-tenant");
        doNothing().when(persistentTopics).validateAdminAccessForTenant("other-tenant");
        doNothing().when(persistentTopics).validateAdminAccessForTenant("prop-xyz");

        resourceQuotas = spy(new ResourceQuotas());
        resourceQuotas.setServletContext(new MockServletContext());
        resourceQuotas.setPulsar(pulsar);

        brokerStats = spy(new BrokerStats());
        brokerStats.setServletContext(new MockServletContext());
        brokerStats.setPulsar(pulsar);

        doReturn(false).when(persistentTopics).isRequestHttps();
        doReturn(null).when(persistentTopics).originalPrincipal();
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn(mock(AuthenticationDataHttps.class)).when(persistentTopics).clientAuthData();

        schemasResource = spy(new SchemasResource());
        schemasResource.setServletContext(new MockServletContext());
        schemasResource.setPulsar(pulsar);
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
        conf.setClusterName(configClusterName);
    }

    @Test
    public void internalConfiguration() throws Exception {
        ServiceConfiguration conf = pulsar.getConfiguration();
        InternalConfigurationData expectedData = new InternalConfigurationData(
                conf.getMetadataStoreUrl(),
                conf.getConfigurationMetadataStoreUrl(),
                new ClientConfiguration().getZkLedgersRootPath(),
                conf.isBookkeeperMetadataStoreSeparated() ? conf.getBookkeeperMetadataStoreUrl() : null,
                pulsar.getWorkerConfig().map(wc -> wc.getStateStorageServiceUrl()).orElse(null));

        assertEquals(brokers.getInternalConfigurationData(), expectedData);
    }

    @Test
    public void clusters() throws Exception {
        assertEquals(clusters.getClusters(), Lists.newArrayList());
        verify(clusters, never()).validateSuperUserAccess();

        clusters.createCluster("use", ClusterDataImpl.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build());
        verify(clusters, times(1)).validateSuperUserAccess();
        // ensure to read from ZooKeeper directly
        //clusters.clustersListCache().clear();
        assertEquals(clusters.getClusters(), Lists.newArrayList("use"));

        // Check creating existing cluster
        try {
            clusters.createCluster("use", ClusterDataImpl.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build());
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.CONFLICT.getStatusCode());
        }

        // Check deleting non-existing cluster
        try {
            clusters.deleteCluster("usc");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        assertEquals(clusters.getCluster("use"), ClusterDataImpl.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build());
        verify(clusters, times(4)).validateSuperUserAccess();

        clusters.updateCluster("use", ClusterDataImpl.builder().serviceUrl("http://new-broker.messaging.use.example.com:8080").build());
        verify(clusters, times(5)).validateSuperUserAccess();

        assertEquals(clusters.getCluster("use"), ClusterData.builder().serviceUrl("http://new-broker.messaging.use.example.com:8080").build());
        verify(clusters, times(6)).validateSuperUserAccess();

        try {
            clusters.getNamespaceIsolationPolicies("use");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 404);
        }

        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "90");

        NamespaceIsolationDataImpl policyData = NamespaceIsolationDataImpl.builder()
                .namespaces(Collections.singletonList("dummy/colo/ns"))
                .primary(Collections.singletonList("localhost" + ":" + pulsar.getListenPortHTTP()))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters1)
                        .build())
                .build();
        AsyncResponse response = mock(AsyncResponse.class);
        clusters.setNamespaceIsolationPolicy(response,"use", "policy1", policyData);
        clusters.getNamespaceIsolationPolicies("use");

        try {
            clusters.deleteCluster("use");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 412);
        }

        clusters.deleteNamespaceIsolationPolicy("use", "policy1");
        assertTrue(clusters.getNamespaceIsolationPolicies("use").isEmpty());

        clusters.deleteCluster("use");
        verify(clusters, times(13)).validateSuperUserAccess();
        assertEquals(clusters.getClusters(), Lists.newArrayList());

        try {
            clusters.getCluster("use");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 404);
        }

        try {
            clusters.updateCluster("use", ClusterDataImpl.builder().build());
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 404);
        }

        try {
            clusters.getNamespaceIsolationPolicies("use");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 404);
        }

        // Test zk failures
        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET_CHILDREN
                    && path.equals("/admin/clusters");
            });
        // clear caches to load data from metadata-store again
        MetadataCacheImpl<ClusterData> clusterCache = (MetadataCacheImpl<ClusterData>) pulsar.getPulsarResources()
                .getClusterResources().getCache();
        MetadataCacheImpl isolationPolicyCache = (MetadataCacheImpl) pulsar.getPulsarResources()
                .getNamespaceResources().getIsolationPolicies().getCache();
        AbstractMetadataStore store = (AbstractMetadataStore) clusterCache.getStore();
        clusterCache.invalidateAll();
        store.invalidateAll();
        try {
            clusters.getClusters();
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.CREATE
                    && path.equals("/admin/clusters/test");
            });
        try {
            clusters.createCluster("test", ClusterDataImpl.builder().serviceUrl("http://broker.messaging.test.example.com:8080").build());
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET
                    && path.equals("/admin/clusters/test");
            });
        clusterCache.invalidateAll();
        store.invalidateAll();
        try {
            clusters.updateCluster("test", ClusterDataImpl.builder().serviceUrl("http://broker.messaging.test.example.com").build());
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET
                    && path.equals("/admin/clusters/test");
            });

        try {
            clusters.getCluster("test");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET_CHILDREN
                    && path.equals("/admin/policies");
            });

        try {
            clusters.deleteCluster("use");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET
                    && path.equals("/admin/clusters/use/namespaceIsolationPolicies");
            });
        clusterCache.invalidateAll();
        isolationPolicyCache.invalidateAll();
        store.invalidateAll();
        try {
            clusters.deleteCluster("use");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        // Check name validations
        try {
            clusters.createCluster("bf@", ClusterDataImpl.builder().serviceUrl("http://dummy.messaging.example.com").build());
            fail("should have filed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check authentication and listener name
        try {
            clusters.createCluster("auth", ClusterDataImpl.builder()
                    .serviceUrl("http://dummy.web.example.com")
                    .serviceUrlTls("")
                    .brokerServiceUrl("http://dummy.messaging.example.com")
                    .brokerServiceUrlTls("")
                    .authenticationPlugin("authenticationPlugin")
                    .authenticationParameters("authenticationParameters")
                    .listenerName("listenerName")
                    .build());
            ClusterData cluster = clusters.getCluster("auth");
            assertEquals(cluster.getAuthenticationPlugin(), "authenticationPlugin");
            assertEquals(cluster.getAuthenticationParameters(), "authenticationParameters");
            assertEquals(cluster.getListenerName(), "listenerName");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }
    }

    Object asynRequests(Consumer<TestAsyncResponse> function) throws Exception {
        TestAsyncResponse ctx = new TestAsyncResponse();
        function.accept(ctx);
        ctx.latch.await();
        if (ctx.e != null) {
            throw (Exception) ctx.e;
        }
        return ctx.response;
    }
    @Test
    public void properties() throws Throwable {
        Object response = asynRequests(ctx -> properties.getTenants(ctx));
        assertEquals(response, Lists.newArrayList());
        verify(properties, times(1)).validateSuperUserAccess();

        // create local cluster
        clusters.createCluster(configClusterName, ClusterDataImpl.builder().build());

        Set<String> allowedClusters = Sets.newHashSet();
        allowedClusters.add(configClusterName);
        TenantInfoImpl tenantInfo = TenantInfoImpl.builder()
                .adminRoles(Sets.newHashSet("role1", "role2"))
                .allowedClusters(allowedClusters)
                .build();
        response = asynRequests(ctx -> properties.createTenant(ctx, "test-property", tenantInfo));
        verify(properties, times(2)).validateSuperUserAccess();

        response = asynRequests(ctx -> properties.getTenants(ctx));
        assertEquals(response, Lists.newArrayList("test-property"));
        verify(properties, times(3)).validateSuperUserAccess();

        response = asynRequests(ctx -> properties.getTenantAdmin(ctx, "test-property"));
        assertEquals(response, tenantInfo);
        verify(properties, times(4)).validateSuperUserAccess();

        final TenantInfoImpl newPropertyAdmin = TenantInfoImpl.builder()
                .adminRoles(Sets.newHashSet("role1", "other-role"))
                .allowedClusters(allowedClusters)
                .build();
        response = asynRequests(ctx -> properties.updateTenant(ctx, "test-property", newPropertyAdmin));
        verify(properties, times(5)).validateSuperUserAccess();

        // Wait for updateTenant to take effect
        Thread.sleep(100);

        response = asynRequests(ctx -> properties.getTenantAdmin(ctx, "test-property"));
        assertEquals(response, newPropertyAdmin);
        response = asynRequests(ctx -> properties.getTenantAdmin(ctx, "test-property"));
        assertNotSame(response, tenantInfo);
        verify(properties, times(7)).validateSuperUserAccess();

        // Check creating existing property
        try {
            response = asynRequests(ctx -> properties.createTenant(ctx, "test-property", tenantInfo));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.CONFLICT.getStatusCode());
        }

        // Check non-existing property
        try {
            response = asynRequests(ctx -> properties.getTenantAdmin(ctx, "non-existing"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        try {
            response = asynRequests(ctx -> properties.updateTenant(ctx, "xxx-non-existing", newPropertyAdmin));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        // Check deleting non-existing property
        try {
            response = asynRequests(ctx -> properties.deleteTenant(ctx, "non-existing", false));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        // clear caches to load data from metadata-store again
        MetadataCacheImpl<TenantInfo> cache = (MetadataCacheImpl<TenantInfo>) pulsar.getPulsarResources()
                .getTenantResources().getCache();
        AbstractMetadataStore store = (AbstractMetadataStore) cache.getStore();
        cache.invalidateAll();
        store.invalidateAll();
        // Test zk failures
        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET_CHILDREN && path.equals("/admin/policies");
        });
        try {
            response = asynRequests(ctx -> properties.getTenants(ctx));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET && path.equals("/admin/policies/my-tenant");
        });
        try {
            response = asynRequests(ctx -> properties.getTenantAdmin(ctx, "my-tenant"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET && path.equals("/admin/policies/my-tenant");
        });
        try {
            response = asynRequests(ctx -> properties.updateTenant(ctx, "my-tenant", newPropertyAdmin));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.CREATE && path.equals("/admin/policies/test");
        });
        try {
            response = asynRequests(ctx -> properties.createTenant(ctx, "test", tenantInfo));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET_CHILDREN && path.equals("/admin/policies/test-property");
        });
        try {
            cache.invalidateAll();
            store.invalidateAll();
            response = asynRequests(ctx -> properties.deleteTenant(ctx, "test-property", false));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response = asynRequests(ctx -> properties.createTenant(ctx, "error-property", tenantInfo));

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.DELETE && path.equals("/admin/policies/error-property");
        });
        try {
            response = asynRequests(ctx -> properties.deleteTenant(ctx, "error-property", false));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response = asynRequests(ctx -> properties.deleteTenant(ctx, "test-property", false));
        response = asynRequests(ctx -> properties.deleteTenant(ctx, "error-property", false));
        response = Lists.newArrayList();
        response = asynRequests(ctx -> properties.getTenants(ctx));
        assertEquals(response, Lists.newArrayList());

        // Create a namespace to test deleting a non-empty property
        TenantInfoImpl newPropertyAdmin2 = TenantInfoImpl.builder()
                .adminRoles(Sets.newHashSet("role1", "other-role"))
                .allowedClusters(Sets.newHashSet("use"))
                .build();
        response = asynRequests(ctx -> properties.createTenant(ctx, "my-tenant", newPropertyAdmin2));

        namespaces.createNamespace("my-tenant", "use", "my-namespace", BundlesData.builder().build());

        try {
            response = asynRequests(ctx -> properties.deleteTenant(ctx, "my-tenant", false));
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }

        // Check name validation
        try {
            response = asynRequests(ctx -> properties.createTenant(ctx, "test&", tenantInfo));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check tenantInfo is null
        try {
            response = asynRequests(ctx -> properties.createTenant(ctx, "tenant-config-is-null", null));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check tenantInfo with empty cluster
        String blankCluster = "";
        Set<String> blankClusters = Sets.newHashSet(blankCluster);
        TenantInfoImpl tenantWithEmptyCluster = TenantInfoImpl.builder()
                .adminRoles(Sets.newHashSet("role1", "role2"))
                .allowedClusters(blankClusters)
                .build();
        try {
            response = asynRequests(ctx -> properties.createTenant(ctx, "tenant-config-is-empty", tenantWithEmptyCluster));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check tenantInfo contains empty cluster
        Set<String> containBlankClusters = Sets.newHashSet(blankCluster);
        containBlankClusters.add(configClusterName);
        TenantInfoImpl tenantContainEmptyCluster = TenantInfoImpl.builder()
                .adminRoles(Sets.newHashSet())
                .allowedClusters(containBlankClusters)
                .build();
        try {
            response = asynRequests(ctx -> properties.createTenant(ctx, "tenant-config-contain-empty", tenantContainEmptyCluster));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        AsyncResponse response2 = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response2, "my-tenant", "use", "my-namespace", false, false);
        ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
        verify(response2, timeout(5000).times(1)).resume(captor.capture());
        assertEquals(captor.getValue().getStatus(), Status.NO_CONTENT.getStatusCode());
        response = asynRequests(ctx -> properties.deleteTenant(ctx, "my-tenant", false));
    }

    @Test
    public void brokers() throws Exception {
        clusters.createCluster("use", ClusterDataImpl.builder()
                .serviceUrl("http://broker.messaging.use.example.com")
                .serviceUrlTls("https://broker.messaging.use.example.com:4443")
                .build());

        URI requestUri = new URI(
                "http://broker.messaging.use.example.com:8080/admin/brokers/use");
        UriInfo mockUri = mock(UriInfo.class);
        doReturn(requestUri).when(mockUri).getRequestUri();
        Field uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(brokers, mockUri);

        Set<String> activeBrokers = brokers.getActiveBrokers("use");
        assertEquals(activeBrokers.size(), 1);
        assertEquals(activeBrokers, Sets.newHashSet(pulsar.getAdvertisedAddress() + ":" + pulsar.getListenPortHTTP().get()));

        BrokerInfo leaderBroker = brokers.getLeaderBroker();
        assertEquals(leaderBroker.getServiceUrl(), pulsar.getLeaderElectionService().getCurrentLeader().map(LeaderBroker::getServiceUrl).get());
    }

    @Test
    public void resourceQuotas() throws Exception {
        // get Default Resource Quota
        ResourceQuota quota = resourceQuotas.getDefaultResourceQuota();
        assertNotNull(quota);
        assertTrue(quota.getBandwidthIn() > 0);

        // set Default Resource Quota
        double defaultBandwidth = 1000;
        quota.setBandwidthIn(defaultBandwidth);
        quota.setBandwidthOut(defaultBandwidth);
        resourceQuotas.setDefaultResourceQuota(quota);
        assertEquals(defaultBandwidth, resourceQuotas.getDefaultResourceQuota().getBandwidthIn());
        assertEquals(defaultBandwidth, resourceQuotas.getDefaultResourceQuota().getBandwidthOut());

        String property = "prop-xyz";
        String cluster = "use";
        String namespace = "ns";
        String bundleRange = "0x00000000_0xffffffff";
        Policies policies = new Policies();
        doReturn(policies).when(resourceQuotas).getNamespacePolicies(NamespaceName.get(property, cluster, namespace));
        doReturn("client-id").when(resourceQuotas).clientAppId();

        try {
            resourceQuotas.setNamespaceBundleResourceQuota(property, cluster, namespace, bundleRange, quota);
            fail();
        } catch (Exception e) {
            // OK : should fail without creating policies
        }

        try {
            resourceQuotas.removeNamespaceBundleResourceQuota(property, cluster, namespace, bundleRange);
            fail();
        } catch (Exception e) {
            // OK : should fail without creating policies
        }

        // create policies
        TenantInfoImpl admin = TenantInfoImpl.builder()
                .allowedClusters(Collections.singleton(cluster))
                .build();
        ClusterDataImpl clusterData = ClusterDataImpl.builder().serviceUrl(cluster).build();
        clusters.createCluster(cluster, clusterData );
        asynRequests(ctx -> properties.createTenant(ctx, property, admin));

        // customized bandwidth for this namespace
        double customizeBandwidth = 3000;
        quota.setBandwidthIn(customizeBandwidth);
        quota.setBandwidthOut(customizeBandwidth);

        // set and get Resource Quota
        resourceQuotas.setNamespaceBundleResourceQuota(property, cluster, namespace, bundleRange, quota);
        ResourceQuota bundleQuota = resourceQuotas.getNamespaceBundleResourceQuota(property, cluster, namespace,
                bundleRange);
        assertEquals(quota, bundleQuota);

        // remove quota which sets to default quota
        resourceQuotas.removeNamespaceBundleResourceQuota(property, cluster, namespace, bundleRange);
        bundleQuota = resourceQuotas.getNamespaceBundleResourceQuota(property, cluster, namespace, bundleRange);
        assertEquals(defaultBandwidth, bundleQuota.getBandwidthIn());
        assertEquals(defaultBandwidth, bundleQuota.getBandwidthOut());
    }

    @Test
    public void brokerStats() throws Exception {
        doReturn("client-id").when(brokerStats).clientAppId();
        Collection<Metrics> metrics = brokerStats.getMetrics();
        assertNotNull(metrics);
        LocalBrokerData loadReport = (LocalBrokerData) brokerStats.getLoadReport();
        assertNotNull(loadReport);
        assertNotNull(loadReport.getCpu());
        Collection<Metrics> mBeans = brokerStats.getMBeans();
        assertFalse(mBeans.isEmpty());
        AllocatorStats allocatorStats = brokerStats.getAllocatorStats("default");
        assertNotNull(allocatorStats);
        Map<String, Map<String, PendingBookieOpsStats>> bookieOpsStats = brokerStats.getPendingBookieOpsStats();
        assertTrue(bookieOpsStats.isEmpty());
        StreamingOutput topic = brokerStats.getTopics2();
        assertNotNull(topic);
        try {
            brokerStats.getBrokerResourceAvailability("prop", "use", "ns2");
            fail("should have failed as ModularLoadManager doesn't support it");
        } catch (RestException re) {
            // Ok
        }
    }

    @Test
    public void persistentTopics() throws Exception {

        final String property = "prop-xyz";
        final String cluster = "use";
        final String namespace = "ns";
        final String topic = "ds1";
        Policies policies = new Policies();
        doReturn(policies).when(resourceQuotas).getNamespacePolicies(NamespaceName.get(property, cluster, namespace));
        doReturn("client-id").when(resourceQuotas).clientAppId();
        // create policies
        TenantInfo admin = TenantInfo.builder()
                .allowedClusters(Collections.singleton(cluster))
                .build();
        pulsar.getPulsarResources().getTenantResources().createTenant(property, admin);
        pulsar.getPulsarResources().getNamespaceResources()
                .createPolicies(NamespaceName.get(property, cluster, namespace), new Policies());

        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.getList(response, property, cluster, namespace);
        verify(response, times(1)).resume(Lists.newArrayList());
        // create topic
        assertEquals(persistentTopics.getPartitionedTopicList(property, cluster, namespace), Lists.newArrayList());
        response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, property, cluster, namespace, topic, 5, false);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        assertEquals(persistentTopics.getPartitionedTopicList(property, cluster, namespace), Lists
                .newArrayList(String.format("persistent://%s/%s/%s/%s", property, cluster, namespace, topic)));

        TopicName topicName = TopicName.get("persistent", property, cluster, namespace, topic);
        assertEquals(persistentTopics.getPartitionedTopicMetadata(topicName, true, false).partitions, 5);

        // grant permission
        final Set<AuthAction> actions = Sets.newHashSet(AuthAction.produce);
        final String role = "test-role";
        persistentTopics.grantPermissionsOnTopic(property, cluster, namespace, topic, role, actions);
        // verify permission
        Map<String, Set<AuthAction>> permission = persistentTopics.getPermissionsOnTopic(property, cluster,
                namespace, topic);
        assertEquals(permission.get(role), actions);
        // remove permission
        persistentTopics.revokePermissionsOnTopic(property, cluster, namespace, topic, role);

        // verify removed permission
        Awaitility.await().untilAsserted(() -> {
            Map<String, Set<AuthAction>> p = persistentTopics.getPermissionsOnTopic(property, cluster, namespace, topic);
            assertTrue(p.isEmpty());
        });

    }

    @Test
    public void testRestExceptionMessage() {
        String message = "my-message";
        RestException exception = new RestException(Status.PRECONDITION_FAILED, message);
        assertEquals(exception.getMessage(), message);

    }


    @Test
    public void testUpdatePartitionedTopicCoontainedInOldTopic() throws Exception {

        final String property = "prop-xyz";
        final String cluster = "use";
        final String namespace = "ns";
        final String partitionedTopicName = "old-special-topic";
        final String partitionedTopicName2 = "special-topic";

        pulsar.getPulsarResources().getNamespaceResources()
                .createPolicies(NamespaceName.get(property, cluster, namespace), new Policies());

        AsyncResponse response1 = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response1, property, cluster, namespace, partitionedTopicName, 5, false);
        verify(response1, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        AsyncResponse response2 = mock(AsyncResponse.class);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response2, property, cluster, namespace, partitionedTopicName2, 2, false);
        verify(response2, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        persistentTopics.updatePartitionedTopic(property, cluster, namespace, partitionedTopicName2, false, false,
                false, 10);
    }


    static class TestAsyncResponse implements AsyncResponse {

        Object response;
        Throwable e;
        CountDownLatch latch = new CountDownLatch(1);

        @Override
        public boolean resume(Object response) {
            this.response = response;
            latch.countDown();
            return true;
        }

        @Override
        public boolean resume(Throwable response) {
            this.e = response;
            latch.countDown();
            return true;
        }

        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean cancel(int retryAfter) {
            return false;
        }

        @Override
        public boolean cancel(Date retryAfter) {
            return false;
        }

        @Override
        public boolean isSuspended() {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean setTimeout(long time, TimeUnit unit) {
            return false;
        }

        @Override
        public void setTimeoutHandler(TimeoutHandler handler) {

        }

        @Override
        public Collection<Class<?>> register(Class<?> callback) {
            return null;
        }

        @Override
        public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks) {
            return null;
        }

        @Override
        public Collection<Class<?>> register(Object callback) {
            return null;
        }

        @Override
        public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks) {
            return null;
        }

    }
}
