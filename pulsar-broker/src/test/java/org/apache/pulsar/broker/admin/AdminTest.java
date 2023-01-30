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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
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
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
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
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
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

        clusters = spy(Clusters.class);
        clusters.setPulsar(pulsar);
        doReturn("test").when(clusters).clientAppId();
        doNothing().when(clusters).validateSuperUserAccess();

        properties = spy(Properties.class);
        properties.setPulsar(pulsar);
        doReturn("test").when(properties).clientAppId();
        doNothing().when(properties).validateSuperUserAccess();

        namespaces = spy(Namespaces.class);
        namespaces.setServletContext(new MockServletContext());
        namespaces.setPulsar(pulsar);
        doReturn("test").when(namespaces).clientAppId();
        doReturn(Set.of("use", "usw", "usc", "global")).when(namespaces).clusters();
        doNothing().when(namespaces).validateAdminAccessForTenant("my-tenant");
        doNothing().when(namespaces).validateAdminAccessForTenant("other-tenant");
        doNothing().when(namespaces).validateAdminAccessForTenant("new-property");

        brokers = spy(Brokers.class);
        brokers.setPulsar(pulsar);
        doReturn("test").when(brokers).clientAppId();
        doNothing().when(brokers).validateSuperUserAccess();

        uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);

        persistentTopics = spy(PersistentTopics.class);
        persistentTopics.setServletContext(new MockServletContext());
        persistentTopics.setPulsar(pulsar);
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn("persistent").when(persistentTopics).domain();
        doReturn(Set.of("use", "usw", "usc")).when(persistentTopics).clusters();
        doNothing().when(persistentTopics).validateAdminAccessForTenant("my-tenant");
        doNothing().when(persistentTopics).validateAdminAccessForTenant("other-tenant");
        doNothing().when(persistentTopics).validateAdminAccessForTenant("prop-xyz");

        resourceQuotas = spy(ResourceQuotas.class);
        resourceQuotas.setServletContext(new MockServletContext());
        resourceQuotas.setPulsar(pulsar);

        brokerStats = spy(BrokerStats.class);
        brokerStats.setServletContext(new MockServletContext());
        brokerStats.setPulsar(pulsar);

        doReturn(false).when(persistentTopics).isRequestHttps();
        doReturn(null).when(persistentTopics).originalPrincipal();
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn(mock(AuthenticationDataHttps.class)).when(persistentTopics).clientAuthData();

        schemasResource = spy(SchemasResource.class);
        schemasResource.setServletContext(new MockServletContext());
        schemasResource.setPulsar(pulsar);
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setMaxTenants(10);
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
                pulsar.getWorkerConfig().map(WorkerConfig::getStateStorageServiceUrl).orElse(null));
        Object response = asyncRequests(ctx -> brokers.getInternalConfigurationData(ctx));
        assertTrue(response instanceof InternalConfigurationData);
        assertEquals(response, expectedData);
    }

    @Data
    @AllArgsConstructor
    /**
     * Internal configuration data model before  (before https://github.com/apache/pulsar/pull/14384).
     */
    private static class OldInternalConfigurationData {
        private String zookeeperServers;
        private String configurationStoreServers;
        @Deprecated
        private String ledgersRootPath;
        private String bookkeeperMetadataServiceUri;
        private String stateStorageServiceUrl;
    }

    /**
     * This test verifies that the model data changes in InternalConfigurationData are retro-compatible.
     * InternalConfigurationData is downloaded from the Function worker from a broker.
     * The broker may be still serve an "old" version of InternalConfigurationData
     * (before https://github.com/apache/pulsar/pull/14384) while the Worker already uses the new one.
     * @throws Exception
     */
    @Test
    public void internalConfigurationRetroCompatibility() throws Exception {
        OldInternalConfigurationData oldDataModel = new OldInternalConfigurationData(
                MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL(conf.getMetadataStoreUrl()),
                conf.getConfigurationMetadataStoreUrl(),
                new ClientConfiguration().getZkLedgersRootPath(),
                conf.isBookkeeperMetadataStoreSeparated() ? conf.getBookkeeperMetadataStoreUrl() : null,
                pulsar.getWorkerConfig().map(WorkerConfig::getStateStorageServiceUrl).orElse(null));

        final Map<String, Object> oldDataJson = ObjectMapperFactory.getMapper().getObjectMapper()
                .convertValue(oldDataModel, Map.class);

        final InternalConfigurationData newData = ObjectMapperFactory.getMapper().getObjectMapper()
                .convertValue(oldDataJson, InternalConfigurationData.class);

        assertEquals(newData.getMetadataStoreUrl(), conf.getMetadataStoreUrl());
        assertEquals(newData.getConfigurationMetadataStoreUrl(), oldDataModel.getConfigurationStoreServers());
        assertEquals(newData.getLedgersRootPath(), oldDataModel.getLedgersRootPath());
        assertEquals(newData.getBookkeeperMetadataServiceUri(), oldDataModel.getBookkeeperMetadataServiceUri());
        assertEquals(newData.getStateStorageServiceUrl(), oldDataModel.getStateStorageServiceUrl());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void clusters() throws Exception {
        assertEquals(asyncRequests(ctx -> clusters.getClusters(ctx)), new HashSet<>());
        verify(clusters, never()).validateSuperUserAccessAsync();

        asyncRequests(ctx -> clusters.createCluster(ctx,
                "use", ClusterDataImpl.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build()));
        // ensure to read from ZooKeeper directly
        //clusters.clustersListCache().clear();
        assertEquals(asyncRequests(ctx -> clusters.getClusters(ctx)), Set.of("use"));

        // Check creating existing cluster
        try {
            asyncRequests(ctx -> clusters.createCluster(ctx, "use",
                    ClusterDataImpl.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build()));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.CONFLICT.getStatusCode());
        }

        // Check deleting non-existing cluster
        try {
            asyncRequests(ctx -> clusters.deleteCluster(ctx, "usc"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        assertEquals(asyncRequests(ctx -> clusters.getCluster(ctx, "use")),
                ClusterDataImpl.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build());

        asyncRequests(ctx -> clusters.updateCluster(ctx, "use",
                ClusterDataImpl.builder().serviceUrl("http://new-broker.messaging.use.example.com:8080").build()));

        assertEquals(asyncRequests(ctx -> clusters.getCluster(ctx, "use")),
                ClusterData.builder().serviceUrl("http://new-broker.messaging.use.example.com:8080").build());

        try {
            asyncRequests(ctx -> clusters.getNamespaceIsolationPolicies(ctx, "use"));
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
        asyncRequests(ctx -> clusters.setNamespaceIsolationPolicy(ctx,
                "use", "policy1", policyData));
        asyncRequests(ctx -> clusters.getNamespaceIsolationPolicies(ctx, "use"));

        try {
            asyncRequests(ctx -> clusters.deleteCluster(ctx, "use"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 412);
        }

        asyncRequests(ctx -> clusters.deleteNamespaceIsolationPolicy(ctx, "use", "policy1"));
        assertTrue(((Map<String, NamespaceIsolationDataImpl>) asyncRequests(ctx ->
                clusters.getNamespaceIsolationPolicies(ctx, "use"))).isEmpty());

        asyncRequests(ctx -> clusters.deleteCluster(ctx, "use"));
        assertEquals(asyncRequests(ctx -> clusters.getClusters(ctx)), new HashSet<>());

        try {
            asyncRequests(ctx -> clusters.getCluster(ctx, "use"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 404);
        }

        try {
            asyncRequests(ctx -> clusters.updateCluster(ctx, "use", ClusterDataImpl.builder().build()));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 404);
        }

        try {
            asyncRequests(ctx -> clusters.getNamespaceIsolationPolicies(ctx, "use"));
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
            asyncRequests(ctx -> clusters.getClusters(ctx));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.CREATE
                    && path.equals("/admin/clusters/test");
            });
        try {
            asyncRequests(ctx -> clusters.createCluster(ctx, "test",
                    ClusterDataImpl.builder().serviceUrl("http://broker.messaging.test.example.com:8080").build()));
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
            asyncRequests(ctx -> clusters.updateCluster(ctx, "test",
                    ClusterDataImpl.builder().serviceUrl("http://broker.messaging.test.example.com").build()));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET
                    && path.equals("/admin/clusters/test");
            });

        try {
            asyncRequests(ctx -> clusters.getCluster(ctx, "test"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET_CHILDREN
                    && path.equals("/admin/policies");
            });

        try {
            asyncRequests(ctx -> clusters.deleteCluster(ctx, "use"));
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
            asyncRequests(ctx -> clusters.deleteCluster(ctx, "use"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        // Check name validations
        try {
            asyncRequests(ctx -> clusters.createCluster(ctx, "bf@",
                    ClusterDataImpl.builder().serviceUrl("http://dummy.messaging.example.com").build()));
            fail("should have filed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check authentication and listener name
        try {
            asyncRequests(ctx -> clusters.createCluster(ctx, "auth", ClusterDataImpl.builder()
                    .serviceUrl("http://dummy.web.example.com")
                    .brokerServiceUrl("pulsar://dummy.messaging.example.com")
                    .authenticationPlugin("authenticationPlugin")
                    .authenticationParameters("authenticationParameters")
                    .listenerName("listenerName")
                    .build()));
            ClusterData cluster = (ClusterData) asyncRequests(ctx -> clusters.getCluster(ctx, "auth"));
            assertEquals(cluster.getAuthenticationPlugin(), "authenticationPlugin");
            assertEquals(cluster.getAuthenticationParameters(), "authenticationParameters");
            assertEquals(cluster.getListenerName(), "listenerName");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }
        verify(clusters, times(24)).validateSuperUserAccessAsync();
    }

    @Test
    public void properties() throws Throwable {
        Object response = asyncRequests(ctx -> properties.getTenants(ctx));
        assertEquals(response, new ArrayList<>());
        verify(properties, times(1)).validateSuperUserAccessAsync();

        // create local cluster
        asyncRequests(ctx -> clusters.createCluster(ctx, configClusterName, ClusterDataImpl.builder().build()));

        Set<String> allowedClusters = new HashSet<>();
        allowedClusters.add(configClusterName);
        TenantInfoImpl tenantInfo = TenantInfoImpl.builder()
                .adminRoles(Set.of("role1", "role2"))
                .allowedClusters(allowedClusters)
                .build();
        response = asyncRequests(ctx -> properties.createTenant(ctx, "test-property", tenantInfo));
        verify(properties, times(2)).validateSuperUserAccessAsync();

        response = asyncRequests(ctx -> properties.getTenants(ctx));
        assertEquals(response, List.of("test-property"));
        verify(properties, times(3)).validateSuperUserAccessAsync();

        response = asyncRequests(ctx -> properties.getTenantAdmin(ctx, "test-property"));
        assertEquals(response, tenantInfo);
        verify(properties, times(4)).validateSuperUserAccessAsync();

        final TenantInfoImpl newPropertyAdmin = TenantInfoImpl.builder()
                .adminRoles(Set.of("role1", "other-role"))
                .allowedClusters(allowedClusters)
                .build();
        response = asyncRequests(ctx -> properties.updateTenant(ctx, "test-property", newPropertyAdmin));
        verify(properties, times(5)).validateSuperUserAccessAsync();

        // Wait for updateTenant to take effect
        Thread.sleep(100);

        response = asyncRequests(ctx -> properties.getTenantAdmin(ctx, "test-property"));
        assertEquals(response, newPropertyAdmin);
        response = asyncRequests(ctx -> properties.getTenantAdmin(ctx, "test-property"));
        assertNotSame(response, tenantInfo);
        verify(properties, times(7)).validateSuperUserAccessAsync();

        // Check creating existing property
        try {
            response = asyncRequests(ctx -> properties.createTenant(ctx, "test-property", tenantInfo));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.CONFLICT.getStatusCode());
        }

        // Check non-existing property
        try {
            response = asyncRequests(ctx -> properties.getTenantAdmin(ctx, "non-existing"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        try {
            response = asyncRequests(ctx -> properties.updateTenant(ctx, "xxx-non-existing", newPropertyAdmin));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        // Check deleting non-existing property
        try {
            response = asyncRequests(ctx -> properties.deleteTenant(ctx, "non-existing", false));
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
            response = asyncRequests(ctx -> properties.getTenants(ctx));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET && path.equals("/admin/policies/my-tenant");
        });
        try {
            response = asyncRequests(ctx -> properties.getTenantAdmin(ctx, "my-tenant"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET && path.equals("/admin/policies/my-tenant");
        });
        try {
            response = asyncRequests(ctx -> properties.updateTenant(ctx, "my-tenant", newPropertyAdmin));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.CREATE && path.equals("/admin/policies/test");
        });
        try {
            response = asyncRequests(ctx -> properties.createTenant(ctx, "test", tenantInfo));
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
            response = asyncRequests(ctx -> properties.deleteTenant(ctx, "test-property", false));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response = asyncRequests(ctx -> properties.createTenant(ctx, "error-property", tenantInfo));

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.DELETE && path.equals("/admin/policies/error-property");
        });
        try {
            response = asyncRequests(ctx -> properties.deleteTenant(ctx, "error-property", false));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response = asyncRequests(ctx -> properties.deleteTenant(ctx, "test-property", false));
        response = asyncRequests(ctx -> properties.deleteTenant(ctx, "error-property", false));
        response = new ArrayList<>();
        response = asyncRequests(ctx -> properties.getTenants(ctx));
        assertEquals(response, new ArrayList<>());

        // Create a namespace to test deleting a non-empty property
        TenantInfoImpl newPropertyAdmin2 = TenantInfoImpl.builder()
                .adminRoles(Set.of("role1", "other-role"))
                .allowedClusters(Set.of("use"))
                .build();
        response = asyncRequests(ctx -> properties.createTenant(ctx, "my-tenant", newPropertyAdmin2));

        response = asyncRequests(ctx -> namespaces.createNamespace(ctx,"my-tenant", "use", "my-namespace", BundlesData.builder().build()));

        try {
            response = asyncRequests(ctx -> properties.deleteTenant(ctx, "my-tenant", false));
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }

        // Check name validation
        try {
            response = asyncRequests(ctx -> properties.createTenant(ctx, "test&", tenantInfo));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check tenantInfo is null
        try {
            response = asyncRequests(ctx -> properties.createTenant(ctx, "tenant-config-is-null", null));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check tenantInfo with empty cluster
        String blankCluster = "";
        Set<String> blankClusters = Set.of(blankCluster);
        TenantInfoImpl tenantWithEmptyCluster = TenantInfoImpl.builder()
                .adminRoles(Set.of("role1", "role2"))
                .allowedClusters(blankClusters)
                .build();
        try {
            response = asyncRequests(ctx -> properties.createTenant(ctx, "tenant-config-is-empty", tenantWithEmptyCluster));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check tenantInfo contains empty cluster
        Set<String> containBlankClusters = Sets.newHashSet(blankCluster);
        containBlankClusters.add(configClusterName);
        TenantInfoImpl tenantContainEmptyCluster = TenantInfoImpl.builder()
                .adminRoles(new HashSet<>())
                .allowedClusters(containBlankClusters)
                .build();
        try {
            response = asyncRequests(ctx -> properties.createTenant(ctx, "tenant-config-contain-empty", tenantContainEmptyCluster));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check max tenant count
        int maxTenants = pulsar.getConfiguration().getMaxTenants();
        List<String> tenants = pulsar.getPulsarResources().getTenantResources().listTenants();

        for(int tenantSize = tenants.size();tenantSize < maxTenants; tenantSize++ ){
            final int tenantIndex = tenantSize;
            Response obj = (Response) asyncRequests(ctx ->
                    properties.createTenant(ctx, "test-tenant-" + tenantIndex, tenantInfo));
            Assert.assertTrue(obj.getStatus() < 400 && obj.getStatus() >= 200);
        }
        try {
            Response obj = (Response) asyncRequests(ctx ->
                    properties.createTenant(ctx, "test-tenant-" +  maxTenants, tenantInfo));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // Check creating existing property when tenant reach max count.
        try {
            response = asyncRequests(ctx -> properties.createTenant(ctx, "test-tenant-" +  (maxTenants-1), tenantInfo));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.CONFLICT.getStatusCode());
        }

        AsyncResponse response2 = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response2, "my-tenant", "use", "my-namespace", false, false);
        ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
        verify(response2, timeout(5000).times(1)).resume(captor.capture());
        assertEquals(captor.getValue().getStatus(), Status.NO_CONTENT.getStatusCode());
        response = asyncRequests(ctx -> properties.deleteTenant(ctx, "my-tenant", false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void brokers() throws Exception {
        asyncRequests(ctx -> clusters.createCluster(ctx, "use", ClusterDataImpl.builder()
                .serviceUrl("http://broker.messaging.use.example.com")
                .serviceUrlTls("https://broker.messaging.use.example.com:4443")
                .build()));

        URI requestUri = new URI(
                "http://broker.messaging.use.example.com:8080/admin/brokers/use");
        UriInfo mockUri = mock(UriInfo.class);
        doReturn(requestUri).when(mockUri).getRequestUri();
        Field uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(brokers, mockUri);
        Object res = asyncRequests(ctx -> brokers.getActiveBrokers(ctx, "use"));
        assertTrue(res instanceof Set);
        Set<String> activeBrokers = (Set<String>) res;
        assertEquals(activeBrokers.size(), 1);
        assertEquals(activeBrokers, Set.of(pulsar.getAdvertisedAddress() + ":" + pulsar.getListenPortHTTP().get()));
        Object leaderBrokerRes = asyncRequests(ctx -> brokers.getLeaderBroker(ctx));
        assertTrue(leaderBrokerRes instanceof BrokerInfo);
        BrokerInfo leaderBroker = (BrokerInfo)leaderBrokerRes;
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
        resourceQuotas.setDefaultResourceQuotaAsync(quota);
        Awaitility.await().untilAsserted(() ->
                assertEquals(defaultBandwidth, resourceQuotas.getDefaultResourceQuota().getBandwidthIn()));
        Awaitility.await().untilAsserted(() ->
                assertEquals(defaultBandwidth, resourceQuotas.getDefaultResourceQuota().getBandwidthOut()));

        String property = "prop-xyz";
        String cluster = "use";
        String namespace = "ns";
        String bundleRange = "0x00000000_0xffffffff";
        Policies policies = new Policies();
        doReturn(policies).when(resourceQuotas).getNamespacePolicies(NamespaceName.get(property, cluster, namespace));
        doReturn(CompletableFuture.completedFuture(policies)).when(resourceQuotas).getNamespacePoliciesAsync(NamespaceName.get(property, cluster, namespace));
        doReturn("client-id").when(resourceQuotas).clientAppId();

        try {
            asyncRequests(ctx -> resourceQuotas.setNamespaceBundleResourceQuota(ctx, property, cluster, namespace, bundleRange, quota));
            fail();
        } catch (Exception e) {
            // OK : should fail without creating policies
        }

        try {
            asyncRequests(ctx -> resourceQuotas.removeNamespaceBundleResourceQuota(ctx, property, cluster, namespace, bundleRange));
            fail();
        } catch (Exception e) {
            // OK : should fail without creating policies
        }

        // create policies
        TenantInfoImpl admin = TenantInfoImpl.builder()
                .allowedClusters(Collections.singleton(cluster))
                .build();
        ClusterDataImpl clusterData = ClusterDataImpl.builder().serviceUrl("http://example.pulsar").build();
        asyncRequests(ctx -> clusters.createCluster(ctx, cluster, clusterData ));
        asyncRequests(ctx -> properties.createTenant(ctx, property, admin));

        // customized bandwidth for this namespace
        double customizeBandwidth = 3000;
        quota.setBandwidthIn(customizeBandwidth);
        quota.setBandwidthOut(customizeBandwidth);

        // set and get Resource Quota
        asyncRequests(ctx -> resourceQuotas.setNamespaceBundleResourceQuota(ctx, property, cluster, namespace, bundleRange, quota));
        ResourceQuota bundleQuota = (ResourceQuota) asyncRequests(ctx -> resourceQuotas.getNamespaceBundleResourceQuota(ctx, property, cluster, namespace,
                bundleRange));
        assertEquals(quota, bundleQuota);

        // remove quota which sets to default quota
        asyncRequests(ctx -> resourceQuotas.removeNamespaceBundleResourceQuota(ctx, property, cluster, namespace, bundleRange));
        bundleQuota = (ResourceQuota) asyncRequests(ctx -> resourceQuotas.getNamespaceBundleResourceQuota(ctx, property, cluster, namespace, bundleRange));
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
        persistentTopics.getList(response, property, cluster, namespace, null);
        verify(response, timeout(5000).times(1)).resume(new ArrayList<>());
        // create topic
        response = mock(AsyncResponse.class);
        persistentTopics.getPartitionedTopicList(response, property, cluster, namespace);
        verify(response, timeout(5000).times(1)).resume(new ArrayList<>());
        response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, property, cluster, namespace, topic, 5, false);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        response = mock(AsyncResponse.class);
        persistentTopics.getPartitionedTopicList(response, property, cluster, namespace);
        verify(response, timeout(5000).times(1))
                .resume(Lists
                        .newArrayList(String.format("persistent://%s/%s/%s/%s", property, cluster, namespace, topic)));

        TopicName topicName = TopicName.get("persistent", property, cluster, namespace, topic);
        assertEquals(persistentTopics.getPartitionedTopicMetadata(topicName, true, false).partitions, 5);

        // grant permission
        final Set<AuthAction> actions = Set.of(AuthAction.produce);
        final String role = "test-role";
        response = mock(AsyncResponse.class);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.grantPermissionsOnTopic(response, property, cluster, namespace, topic, role, actions);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        // verify permission
        response = mock(AsyncResponse.class);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.getPermissionsOnTopic(response, property, cluster, namespace, topic);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Map<String, Set<AuthAction>> permission = (Map<String, Set<AuthAction>>) responseCaptor.getValue();
        assertEquals(permission.get(role), actions);
        // remove permission
        response = mock(AsyncResponse.class);
        persistentTopics.revokePermissionsOnTopic(response, property, cluster, namespace, topic, role);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        // verify removed permission
        Awaitility.await().untilAsserted(() -> {
            AsyncResponse response1 = mock(AsyncResponse.class);
            ArgumentCaptor<Response> responseCaptor1 = ArgumentCaptor.forClass(Response.class);
            persistentTopics.getPermissionsOnTopic(response1, property, cluster, namespace, topic);
            verify(response1, timeout(5000).times(1)).resume(responseCaptor1.capture());
            Map<String, Set<AuthAction>> p = (Map<String, Set<AuthAction>>) responseCaptor1.getValue();
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

        persistentTopics.updatePartitionedTopic(response2, property, cluster, namespace, partitionedTopicName2, false, false,
                false, 10);
    }

    @Test
    public void test500Error() throws Exception {
        final String property = "prop-xyz";
        final String cluster = "use";
        final String namespace = "ns";
        final String partitionedTopicName = "error-500-topic";
        AsyncResponse response1 = mock(AsyncResponse.class);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        NamespaceName namespaceName = NamespaceName.get(property, cluster, namespace);
        NamespaceService ns = spy(pulsar.getNamespaceService());
        Field namespaceField = pulsar.getClass().getDeclaredField("nsService");
        namespaceField.setAccessible(true);
        namespaceField.set(pulsar, ns);
        CompletableFuture<List<String>> future = new CompletableFuture();
        future.completeExceptionally(new RuntimeException("500 error contains error message"));
        doReturn(future).when(ns).getListOfTopics(namespaceName, CommandGetTopicsOfNamespace.Mode.ALL);
        persistentTopics.createPartitionedTopic(response1, property, cluster, namespace, partitionedTopicName, 5, false);
        verify(response1, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        Assert.assertTrue(((ErrorData)responseCaptor.getValue().getResponse().getEntity()).reason.contains("500 error contains error message"));
    }
}
