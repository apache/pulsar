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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.admin.v1.Namespaces;
import org.apache.pulsar.broker.admin.v1.PersistentTopics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.internal.BaseResource;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespacesTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(NamespacesTest.class);
    private Namespaces namespaces;

    private List<NamespaceName> testLocalNamespaces;
    private List<NamespaceName> testGlobalNamespaces;
    private final String testTenant = "my-tenant";
    private final String testOtherTenant = "other-tenant";
    private final String testLocalCluster = "use";
    private final String testOtherCluster = "usc";

    public static final long THREE_MINUTE_MILLIS = 180000;

    protected NamespaceService nsSvc;
    protected Field uriField;
    protected UriInfo uriInfo;

    public NamespacesTest() {
        super();
    }

    @BeforeClass
    public void initNamespace() throws Exception {
        testLocalNamespaces = Lists.newArrayList();
        testGlobalNamespaces = Lists.newArrayList();

        testLocalNamespaces.add(NamespaceName.get(this.testTenant, this.testLocalCluster, "test-namespace-1"));
        testLocalNamespaces.add(NamespaceName.get(this.testTenant, this.testLocalCluster, "test-namespace-2"));
        testLocalNamespaces.add(NamespaceName.get(this.testTenant, this.testOtherCluster, "test-other-namespace-1"));
        testLocalNamespaces.add(NamespaceName.get(this.testOtherTenant, this.testLocalCluster, "test-namespace-1"));

        testGlobalNamespaces.add(NamespaceName.get(this.testTenant, "global", "test-global-ns1"));

        uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriInfo = mock(UriInfo.class);
    }

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        resetConfig();
        conf.setClusterName(testLocalCluster);
        super.internalSetup();

        namespaces = spy(new Namespaces());
        namespaces.setServletContext(new MockServletContext());
        namespaces.setPulsar(pulsar);
        doReturn(false).when(namespaces).isRequestHttps();
        doReturn("test").when(namespaces).clientAppId();
        doReturn(null).when(namespaces).originalPrincipal();
        doReturn(null).when(namespaces).clientAuthData();
        doReturn(Sets.newTreeSet(Lists.newArrayList("use", "usw", "usc", "global"))).when(namespaces).clusters();

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl("http://broker-use.com:8080").build());
        admin.clusters().createCluster("usw", ClusterData.builder().serviceUrl("http://broker-usw.com:8080").build());
        admin.clusters().createCluster("usc", ClusterData.builder().serviceUrl("http://broker-usc.com:8080").build());
        admin.tenants().createTenant(this.testTenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("use", "usc", "usw")));
        admin.tenants().createTenant(this.testOtherTenant,
                new TenantInfoImpl(Sets.newHashSet("role3", "role4"), Sets.newHashSet("use", "usc", "usw")));

        createTestNamespaces(this.testLocalNamespaces, BundlesData.builder().build());
        createGlobalTestNamespaces(this.testTenant, this.testGlobalNamespaces.get(0).getLocalName(),
                BundlesData.builder().build());

        doThrow(new RestException(Status.UNAUTHORIZED, "unauthorized")).when(namespaces)
                .validateTenantOperation(this.testOtherTenant, null);

        doThrow(new RestException(Status.UNAUTHORIZED, "unauthorized")).when(namespaces)
                .validateNamespacePolicyOperation(NamespaceName.get("other-tenant/use/test-namespace-1"),
                        PolicyName.PERSISTENCE, PolicyOperation.WRITE);

        doThrow(new RestException(Status.UNAUTHORIZED, "unauthorized")).when(namespaces)
                .validateNamespacePolicyOperation(NamespaceName.get("other-tenant/use/test-namespace-1"),
                        PolicyName.RETENTION, PolicyOperation.WRITE);

        nsSvc = pulsar.getNamespaceService();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
        conf.setClusterName(testLocalCluster);
    }

    @Test
    public void testCreateNamespaces() throws Exception {
        try {
            namespaces.createNamespace(this.testTenant, "other-colo", "my-namespace",
                    BundlesData.builder().build());
            fail("should have failed");
        } catch (RestException e) {
            // Ok, cluster doesn't exist
        }

        List<NamespaceName> nsnames = Lists.newArrayList();
        nsnames.add(NamespaceName.get(this.testTenant, "use", "create-namespace-1"));
        nsnames.add(NamespaceName.get(this.testTenant, "use", "create-namespace-2"));
        nsnames.add(NamespaceName.get(this.testTenant, "usc", "create-other-namespace-1"));
        createTestNamespaces(nsnames, BundlesData.builder().build());

        try {
            namespaces.createNamespace(this.testTenant, "use", "create-namespace-1",
                    BundlesData.builder().build());
            fail("should have failed");
        } catch (RestException e) {
            // Ok, namespace already exists
        }

        try {
            namespaces.createNamespace("non-existing-tenant", "use", "create-namespace-1",
                    BundlesData.builder().build());
            fail("should have failed");
        } catch (RestException e) {
            // Ok, tenant doesn't exist
        }

        try {
            namespaces.createNamespace(this.testTenant, "use", "create-namespace-#",
                    BundlesData.builder().build());
            fail("should have failed");
        } catch (RestException e) {
            // Ok, invalid namespace name
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.CREATE
                    && path.equals("/admin/policies/my-tenant/use/my-namespace-3");
            });
        try {
            namespaces.createNamespace(this.testTenant, "use", "my-namespace-3", BundlesData.builder().build());
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }
    }

    @Test
    public void testGetNamespaces() throws Exception {
        List<String> expectedList = Lists.newArrayList(this.testLocalNamespaces.get(0).toString(),
                this.testLocalNamespaces.get(1).toString());
        expectedList.sort(null);
        assertEquals(namespaces.getNamespacesForCluster(this.testTenant, this.testLocalCluster), expectedList);
        expectedList = Lists.newArrayList(this.testLocalNamespaces.get(0).toString(),
                this.testLocalNamespaces.get(1).toString(), this.testLocalNamespaces.get(2).toString(),
                this.testGlobalNamespaces.get(0).toString());
        expectedList.sort(null);
        assertEquals(namespaces.getTenantNamespaces(this.testTenant), expectedList);

        try {
            // check the tenant name is valid
            namespaces.getTenantNamespaces(this.testTenant + "/default");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        try {
            namespaces.getTenantNamespaces("non-existing-tenant");
            fail("should have failed");
        } catch (RestException e) {
            // Ok, does not exist
        }

        try {
            namespaces.getNamespacesForCluster(this.testTenant, "other-cluster");
            fail("should have failed");
        } catch (RestException e) {
            // Ok, does not exist
        }

        // ZK Errors
        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET_CHILDREN
                    && path.equals("/admin/policies/my-tenant");
            });
        // clear caches to load data from metadata-store again
        MetadataCacheImpl<TenantInfo> tenantCache = (MetadataCacheImpl<TenantInfo>) pulsar.getPulsarResources()
                .getTenantResources().getCache();
        AbstractMetadataStore store = (AbstractMetadataStore) tenantCache.getStore();
        tenantCache.invalidateAll();
        store.invalidateAll();
        try {
            namespaces.getTenantNamespaces(this.testTenant);
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET_CHILDREN
                    && path.equals("/admin/policies/my-tenant/use");
            });
        try {
            namespaces.getNamespacesForCluster(this.testTenant, this.testLocalCluster);
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }

    }

    @Test(enabled = false)
    public void testGrantAndRevokePermissions() throws Exception {
        Policies expectedPolicies = new Policies();
        assertEquals(namespaces.getPolicies(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies);
        assertEquals(namespaces.getPermissions(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies.auth_policies.getNamespaceAuthentication());

        namespaces.grantPermissionOnNamespace(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName(), "my-role", EnumSet.of(AuthAction.produce));

        expectedPolicies.auth_policies.getNamespaceAuthentication().put("my-role", EnumSet.of(AuthAction.produce));
        assertEquals(namespaces.getPolicies(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies);
        assertEquals(namespaces.getPermissions(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies.auth_policies.getNamespaceAuthentication());

        namespaces.grantPermissionOnNamespace(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName(), "other-role", EnumSet.of(AuthAction.consume));
        expectedPolicies.auth_policies.getNamespaceAuthentication().put("other-role", EnumSet.of(AuthAction.consume));
        assertEquals(namespaces.getPolicies(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies);
        assertEquals(namespaces.getPermissions(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies.auth_policies.getNamespaceAuthentication());

        namespaces.revokePermissionsOnNamespace(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName(), "my-role");
        expectedPolicies.auth_policies.getNamespaceAuthentication().remove("my-role");
        assertEquals(namespaces.getPolicies(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies);
        assertEquals(namespaces.getPermissions(this.testTenant, this.testLocalCluster,
                this.testLocalNamespaces.get(0).getLocalName()), expectedPolicies.auth_policies.getNamespaceAuthentication());

        // Non-existing namespaces
        try {
            namespaces.getPolicies(this.testTenant, this.testLocalCluster, "non-existing-namespace-1");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        try {
            namespaces.getPermissions(this.testTenant, this.testLocalCluster, "non-existing-namespace-1");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        try {
            namespaces.grantPermissionOnNamespace(this.testTenant, this.testLocalCluster, "non-existing-namespace-1",
                    "my-role", EnumSet.of(AuthAction.produce));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        try {
            namespaces.revokePermissionsOnNamespace(this.testTenant, this.testLocalCluster,
                    "non-existing-namespace-1", "my-role");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        NamespaceName testNs = this.testLocalNamespaces.get(1);

        mockZooKeeper.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                // test is disabled and failing so I can't see what paths are needed here
                // if it ever gets enabled and fixed, first check what is expected and update these
                // paths
                log.info("Condition1: {} {}", op, path);
                return true;
            });

        try {
            namespaces.getPolicies(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName());
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }

        mockZooKeeper.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                // test is disabled and failing so I can't see what paths are needed here
                // if it ever gets enabled and fixed, first check what is expected and update these
                // paths
                log.info("Condition2: {} {}", op, path);
                return true;
            });
        try {
            namespaces.getPermissions(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName());
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }

        mockZooKeeper.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                // test is disabled and failing so I can't see what paths are needed here
                // if it ever gets enabled and fixed, first check what is expected and update these
                // paths
                log.info("Condition3: {} {}", op, path);
                return true;
            });
        try {
            namespaces.grantPermissionOnNamespace(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(),
                    "other-role", EnumSet.of(AuthAction.consume));
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }

        mockZooKeeper.failConditional(Code.BADVERSION, (op, path) -> {
                // test is disabled and failing so I can't see what paths are needed here
                // if it ever gets enabled and fixed, first check what is expected and update these
                // paths
                log.info("Condition4: {} {}", op, path);
                return true;
            });
        try {
            namespaces.grantPermissionOnNamespace(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(),
                    "other-role", EnumSet.of(AuthAction.consume));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.CONFLICT.getStatusCode());
        }

        mockZooKeeper.failConditional(Code.BADVERSION, (op, path) -> {
                // test is disabled and failing so I can't see what paths are needed here
                // if it ever gets enabled and fixed, first check what is expected and update these
                // paths
                log.info("Condition5: {} {}", op, path);
                return true;
            });
        try {
            namespaces.revokePermissionsOnNamespace(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(),
                    "other-role");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.CONFLICT.getStatusCode());
        }

        mockZooKeeper.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                // test is disabled and failing so I can't see what paths are needed here
                // if it ever gets enabled and fixed, first check what is expected and update these
                // paths
                log.info("Condition6: {} {}", op, path);
                return true;
            });
        try {
            namespaces.revokePermissionsOnNamespace(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(),
                    "other-role");
            fail("should have failed");
        } catch (RestException e) {
            // Ok
        }
    }

    @Test
    public void testGlobalNamespaceReplicationConfiguration() throws Exception {
        assertEquals(
                namespaces.getNamespaceReplicationClusters(this.testGlobalNamespaces.get(0).getTenant(),
                        this.testGlobalNamespaces.get(0).getCluster(), this.testGlobalNamespaces.get(0).getLocalName()),
                Sets.newHashSet());

        namespaces.setNamespaceReplicationClusters(this.testGlobalNamespaces.get(0).getTenant(),
                this.testGlobalNamespaces.get(0).getCluster(), this.testGlobalNamespaces.get(0).getLocalName(),
                Lists.newArrayList("use", "usw"));
        assertEquals(
                namespaces.getNamespaceReplicationClusters(this.testGlobalNamespaces.get(0).getTenant(),
                        this.testGlobalNamespaces.get(0).getCluster(), this.testGlobalNamespaces.get(0).getLocalName()),
                Lists.newArrayList("use", "usw"));

        try {
            namespaces.setNamespaceReplicationClusters(this.testGlobalNamespaces.get(0).getTenant(),
                    this.testGlobalNamespaces.get(0).getCluster(), this.testGlobalNamespaces.get(0).getLocalName(),
                    Lists.newArrayList("use", "invalid-cluster"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.FORBIDDEN.getStatusCode());
        }

        try {
            namespaces.setNamespaceReplicationClusters(this.testGlobalNamespaces.get(0).getTenant(),
                    this.testGlobalNamespaces.get(0).getCluster(), this.testGlobalNamespaces.get(0).getLocalName(),
                    Lists.newArrayList("use", "global"));
            fail("should have failed");
        } catch (RestException e) {
            // Ok, global should not be allowed in the list of replication clusters
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        try {
            namespaces.setNamespaceReplicationClusters(this.testTenant, "global",
                    this.testGlobalNamespaces.get(0).getLocalName(), Lists.newArrayList("use", "invalid-cluster"));
            fail("should have failed");
        } catch (RestException e) {
            // Ok, invalid-cluster is an invalid cluster id
            assertEquals(e.getResponse().getStatus(), Status.FORBIDDEN.getStatusCode());
        }

        admin.tenants().updateTenant(testTenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("use", "usc")));

        try {
            namespaces.setNamespaceReplicationClusters(this.testTenant, "global",
                    this.testGlobalNamespaces.get(0).getLocalName(), Lists.newArrayList("use", "usw"));
            fail("should have failed");
        } catch (RestException e) {
            // Ok, usw was not configured in the list of allowed clusters
            assertEquals(e.getResponse().getStatus(), Status.FORBIDDEN.getStatusCode());
        }

        // Sometimes watcher event consumes scheduled exception, so set to always fail to ensure exception is
        // thrown for api call.
        mockZooKeeperGlobal.setAlwaysFail(Code.SESSIONEXPIRED);

        try {
            namespaces.setNamespaceReplicationClusters(this.testTenant, "global",
                    this.testGlobalNamespaces.get(0).getLocalName(), Lists.newArrayList("use"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
        } finally {
            mockZooKeeperGlobal.unsetAlwaysFail();
        }

        // clear caches to load data from metadata-store again
        MetadataCacheImpl<Policies> policiesCache = (MetadataCacheImpl<Policies>) pulsar.getPulsarResources()
                .getNamespaceResources().getCache();
        AbstractMetadataStore store = (AbstractMetadataStore) policiesCache.getStore();

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.SET
                    && path.equals("/admin/policies/my-tenant/global/test-global-ns1");
            });

        policiesCache.invalidateAll();
        store.invalidateAll();
        try {
            namespaces.setNamespaceReplicationClusters(this.testTenant, "global",
                    this.testGlobalNamespaces.get(0).getLocalName(), Lists.newArrayList("use"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 500);
        }

        try {
            namespaces.getNamespaceReplicationClusters(this.testTenant, "global", "non-existing-ns");
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        try {
            namespaces.setNamespaceReplicationClusters(this.testTenant, "global", "non-existing-ns",
                    Lists.newArrayList("use"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
        }

        mockZooKeeperGlobal.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
                return op == MockZooKeeper.Op.GET
                    && path.equals("/admin/policies/my-tenant/global/test-global-ns1");
            });

        policiesCache.invalidateAll();
        store.invalidateAll();
        // ensure the ZooKeeper read happens, bypassing the cache
        try {
            namespaces.getNamespaceReplicationClusters(this.testTenant, "global",
                    this.testGlobalNamespaces.get(0).getLocalName());
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 500);
        }

        try {
            namespaces.getNamespaceReplicationClusters(this.testTenant, this.testLocalCluster,
                    this.testLocalNamespaces.get(0).getLocalName());
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        try {
            namespaces.setNamespaceReplicationClusters(this.testTenant, this.testLocalCluster,
                    this.testLocalNamespaces.get(0).getLocalName(), Lists.newArrayList("use"));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

    }

    @Test
    public void testGetBundles() throws Exception {
        List<String> boundaries = Lists.newArrayList("0x00000000", "0x80000000", "0xffffffff");
        BundlesData bundle = BundlesData.builder()
                .boundaries(boundaries)
                .numBundles(boundaries.size() - 1)
                .build();
        createBundledTestNamespaces(this.testTenant, this.testLocalCluster, "test-bundled-namespace-1", bundle);
        BundlesData responseData = namespaces.getBundlesData(testTenant, this.testLocalCluster,
                "test-bundled-namespace-1");

        assertEquals(responseData, bundle);
    }

    @Test
    public void testNamespacesApiRedirects() throws Exception {
        // Redirect cases
        uriField.set(namespaces, uriInfo);
        doReturn(false).when(namespaces).isLeaderBroker();

        URI uri = URI.create(pulsar.getWebServiceAddress() + "/admin/namespace/"
                + this.testLocalNamespaces.get(2).toString());
        doReturn(uri).when(uriInfo).getRequestUri();

        // Trick to force redirection
        conf.setAuthorizationEnabled(true);

        AsyncResponse response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, this.testTenant, this.testOtherCluster,
                this.testLocalNamespaces.get(2).getLocalName(), false, false);
        ArgumentCaptor<WebApplicationException> captor = ArgumentCaptor.forClass(WebApplicationException.class);
        verify(response, timeout(5000).times(1)).resume(captor.capture());
        assertEquals(captor.getValue().getResponse().getStatus(), Status.TEMPORARY_REDIRECT.getStatusCode());
        assertEquals(captor.getValue().getResponse().getLocation().toString(),
                UriBuilder.fromUri(uri).host("broker-usc.com").port(8080).toString());

        uri = URI.create(pulsar.getWebServiceAddress() + "/admin/namespace/"
                + this.testLocalNamespaces.get(2).toString() + "/unload");
        doReturn(uri).when(uriInfo).getRequestUri();

        try {
            namespaces.unloadNamespaceBundle(response, this.testTenant, this.testOtherCluster,
                    this.testLocalNamespaces.get(2).getLocalName(), "0x00000000_0xffffffff", false);
            fail("Should have raised exception to redirect request");
        } catch (WebApplicationException wae) {
            // OK
            assertEquals(wae.getResponse().getStatus(), Status.TEMPORARY_REDIRECT.getStatusCode());
            assertEquals(wae.getResponse().getLocation().toString(),
                    UriBuilder.fromUri(uri).host("broker-usc.com").port(8080).toString());
        }

        uri = URI.create(pulsar.getWebServiceAddress() + "/admin/namespace/"
                + this.testGlobalNamespaces.get(0).toString() + "/configversion");
        doReturn(uri).when(uriInfo).getRequestUri();

        // setup to redirect to another broker in the same cluster
        doReturn(Optional.of(new URL("http://otherhost" + ":" + 8080))).when(nsSvc)
                .getWebServiceUrl(Mockito.argThat(new ArgumentMatcher<NamespaceName>() {
                    @Override
                    public boolean matches(NamespaceName nsname) {
                        return nsname.equals(NamespacesTest.this.testGlobalNamespaces.get(0));
                    }
                }), Mockito.any());

        admin.namespaces().setNamespaceReplicationClusters(testGlobalNamespaces.get(0).toString(),
                Sets.newHashSet("usw"));

        uri = URI.create(pulsar.getWebServiceAddress() + "/admin/namespace/"
                + this.testLocalNamespaces.get(2).toString() + "?authoritative=false");
        doReturn(uri).when(uriInfo).getRequestUri();
        doReturn(true).when(namespaces).isLeaderBroker();

        response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, this.testLocalNamespaces.get(2).getTenant(),
                this.testLocalNamespaces.get(2).getCluster(), this.testLocalNamespaces.get(2).getLocalName(), false, false);
        captor = ArgumentCaptor.forClass(WebApplicationException.class);
        verify(response, timeout(5000).times(1)).resume(captor.capture());
        assertEquals(captor.getValue().getResponse().getStatus(), Status.TEMPORARY_REDIRECT.getStatusCode());
        assertEquals(captor.getValue().getResponse().getLocation().toString(),
                UriBuilder.fromUri(uri).host("broker-usc.com").port(8080).toString());
    }

    @Test
    public void testDeleteNamespaces() throws Exception {
        AsyncResponse response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, this.testTenant, this.testLocalCluster, "non-existing-namespace-1", false, false);
        ArgumentCaptor<RestException> errorCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(errorCaptor.capture());
        assertEquals(errorCaptor.getValue().getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());

        NamespaceName testNs = this.testLocalNamespaces.get(1);
        TopicName topicName = TopicName.get(testNs.getPersistentTopicName("my-topic"));
        ZkUtils.createFullPathOptimistic(mockZooKeeper, "/managed-ledgers/" + topicName.getPersistenceNamingEncoding(),
                new byte[0], null, null);

        // setup ownership to localhost
        URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
        LookupOptions options = LookupOptions.builder().authoritative(false).readOnly(false).requestHttps(false).build();
        doReturn(Optional.of(localWebServiceUrl)).when(nsSvc).getWebServiceUrl(testNs, options);
        doReturn(true).when(nsSvc).isServiceUnitOwned(testNs);

        response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), false, false);
        errorCaptor = ArgumentCaptor.forClass(RestException.class);
        // Ok, namespace not empty
        verify(response, timeout(5000).times(1)).resume(errorCaptor.capture());
        assertEquals(errorCaptor.getValue().getResponse().getStatus(), Status.CONFLICT.getStatusCode());

        // delete the topic from ZK
        mockZooKeeper.delete("/managed-ledgers/" + topicName.getPersistenceNamingEncoding(), -1);

        ZkUtils.createFullPathOptimistic(mockZooKeeperGlobal,
                "/admin/partitioned-topics/" + topicName.getPersistenceNamingEncoding(),
                new byte[0], null, null);

        response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), false, false);
        errorCaptor = ArgumentCaptor.forClass(RestException.class);
        // Ok, namespace not empty
        verify(response, timeout(5000).times(1)).resume(errorCaptor.capture());
        assertEquals(errorCaptor.getValue().getResponse().getStatus(), Status.CONFLICT.getStatusCode());

        mockZooKeeperGlobal.delete("/admin/partitioned-topics/" + topicName.getPersistenceNamingEncoding(), -1);

        testNs = this.testGlobalNamespaces.get(0);
        // setup ownership to localhost
        doReturn(Optional.of(localWebServiceUrl)).when(nsSvc).getWebServiceUrl(testNs, options);
        doReturn(true).when(nsSvc).isServiceUnitOwned(testNs);
        response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), false, false);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        assertEquals(responseCaptor.getValue().getStatus(), Status.NO_CONTENT.getStatusCode());

        testNs = this.testLocalNamespaces.get(0);
        // setup ownership to localhost
        doReturn(Optional.of(localWebServiceUrl)).when(nsSvc).getWebServiceUrl(testNs, options);
        doReturn(true).when(nsSvc).isServiceUnitOwned(testNs);
        response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), false, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        assertEquals(responseCaptor.getValue().getStatus(), Status.NO_CONTENT.getStatusCode());
        List<String> nsList = Lists.newArrayList(this.testLocalNamespaces.get(1).toString(),
                this.testLocalNamespaces.get(2).toString());
        nsList.sort(null);
        assertEquals(namespaces.getTenantNamespaces(this.testTenant), nsList);

        testNs = this.testLocalNamespaces.get(1);
        // setup ownership to localhost
        doReturn(Optional.of(localWebServiceUrl)).when(nsSvc).getWebServiceUrl(testNs, options);
        doReturn(true).when(nsSvc).isServiceUnitOwned(testNs);
        response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), false, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        assertEquals(responseCaptor.getValue().getStatus(), Status.NO_CONTENT.getStatusCode());
    }

    @Test
    public void testDeleteNamespaceWithBundles() throws Exception {
        URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
        String bundledNsLocal = "test-bundled-namespace-1";
        List<String> boundaries = Lists.newArrayList("0x00000000", "0x80000000", "0xffffffff");
        BundlesData bundleData = BundlesData.builder()
                .boundaries(boundaries)
                .numBundles(boundaries.size() - 1)
                .build();
        createBundledTestNamespaces(this.testTenant, this.testLocalCluster, bundledNsLocal, bundleData);
        final NamespaceName testNs = NamespaceName.get(this.testTenant, this.testLocalCluster, bundledNsLocal);

        org.apache.pulsar.client.admin.Namespaces namespacesAdmin = mock(
                org.apache.pulsar.client.admin.Namespaces.class);
        doReturn(namespacesAdmin).when(admin).namespaces();

        doReturn(null).when(nsSvc).getWebServiceUrl(Mockito.argThat(new ArgumentMatcher<NamespaceBundle>() {
            @Override
            public boolean matches(NamespaceBundle bundle) {
                return bundle.getNamespaceObject().equals(testNs);
            }
        }), Mockito.any());
        doReturn(false).when(nsSvc).isServiceUnitOwned(Mockito.argThat(new ArgumentMatcher<NamespaceBundle>() {
            @Override
            public boolean matches(NamespaceBundle bundle) {
                return bundle.getNamespaceObject().equals(testNs);
            }
        }));
        doReturn(Optional.of(mock(NamespaceEphemeralData.class))).when(nsSvc)
                .getOwner(Mockito.argThat(new ArgumentMatcher<NamespaceBundle>() {
                    @Override
                    public boolean matches(NamespaceBundle bundle) {
                        return bundle.getNamespaceObject().equals(testNs);
                    }
                }));

        CompletableFuture<Void> preconditionFailed = new CompletableFuture<>();
        ClientErrorException cee = new ClientErrorException(Status.PRECONDITION_FAILED);
        int statusCode = cee.getResponse().getStatus();
        String httpError = BaseResource.getReasonFromServer(cee);
        preconditionFailed.completeExceptionally(new PulsarAdminException.PreconditionFailedException(cee,
                httpError, statusCode));
        doReturn(preconditionFailed).when(namespacesAdmin)
                .deleteNamespaceBundleAsync(Mockito.anyString(), Mockito.anyString());

        try {
            namespaces.deleteNamespaceBundle(testTenant, testLocalCluster, bundledNsLocal, "0x00000000_0x80000000",
                    false, false);
            fail("Should have failed");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        AsyncResponse response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, testTenant, testLocalCluster, bundledNsLocal, false, false);
        ArgumentCaptor<RestException> captor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(captor.capture());
        assertEquals(captor.getValue().getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());

        NamespaceBundles nsBundles = nsSvc.getNamespaceBundleFactory().getBundles(testNs, bundleData);
        // make one bundle owned
        LookupOptions optionsHttps = LookupOptions.builder().authoritative(false).requestHttps(true).readOnly(false).build();
        doReturn(Optional.of(localWebServiceUrl)).when(nsSvc).getWebServiceUrl(nsBundles.getBundles().get(0), optionsHttps);
        doReturn(true).when(nsSvc).isServiceUnitOwned(nsBundles.getBundles().get(0));
        doReturn(CompletableFuture.completedFuture(null)).when(namespacesAdmin).deleteNamespaceBundleAsync(
                testTenant + "/" + testLocalCluster + "/" + bundledNsLocal, "0x00000000_0x80000000");

        try {
            namespaces.deleteNamespaceBundle(testTenant, testLocalCluster, bundledNsLocal, "0x80000000_0xffffffff",
                    false, false);
            fail("Should have failed");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        response = mock(AsyncResponse.class);
        namespaces.deleteNamespace(response, testTenant, testLocalCluster, bundledNsLocal, false, false);
        captor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(captor.capture());
        assertEquals(captor.getValue().getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());

        // ensure all three bundles are owned by the local broker
        for (NamespaceBundle bundle : nsBundles.getBundles()) {
            doReturn(Optional.of(localWebServiceUrl)).when(nsSvc).getWebServiceUrl(bundle, optionsHttps);
            doReturn(true).when(nsSvc).isServiceUnitOwned(bundle);
        }
        doNothing().when(namespacesAdmin).deleteNamespaceBundle(Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testUnloadNamespaces() throws Exception {
        final NamespaceName testNs = this.testLocalNamespaces.get(1);
        URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
        doReturn(Optional.of(localWebServiceUrl)).when(nsSvc)
                .getWebServiceUrl(Mockito.argThat(ns -> ns.equals(testNs)), Mockito.any());
        doReturn(true).when(nsSvc).isServiceUnitOwned(Mockito.argThat(ns -> ns.equals(testNs)));

        NamespaceBundle bundle = nsSvc.getNamespaceBundleFactory().getFullBundle(testNs);
        doNothing().when(namespaces).validateBundleOwnership(bundle, false, true);

        // The namespace unload should succeed on all the bundles
        AsyncResponse response = mock(AsyncResponse.class);
        namespaces.unloadNamespace(response, testNs.getTenant(), testNs.getCluster(), testNs.getLocalName());
        ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(captor.capture());
        assertEquals(captor.getValue().getStatus(), Status.NO_CONTENT.getStatusCode());
    }

    @Test
    public void testSplitBundles() throws Exception {
        URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
        String bundledNsLocal = "test-bundled-namespace-1";
        List<String> boundaries = Lists.newArrayList("0x00000000", "0xffffffff");
        BundlesData bundleData = BundlesData.builder()
                .boundaries(boundaries)
                .numBundles(boundaries.size() - 1)
                .build();
        createBundledTestNamespaces(this.testTenant, this.testLocalCluster, bundledNsLocal, bundleData);
        final NamespaceName testNs = NamespaceName.get(this.testTenant, this.testLocalCluster, bundledNsLocal);

        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
        doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);
        mockWebUrl(localWebServiceUrl, testNs);

        // split bundles
        try {
            AsyncResponse response = mock(AsyncResponse.class);
            namespaces.splitNamespaceBundle(response, testTenant, testLocalCluster, bundledNsLocal, "0x00000000_0xffffffff",
                    false, true);
            ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
            verify(response, timeout(5000).times(1)).resume(captor.capture());
            // verify split bundles
            BundlesData bundlesData = namespaces.getBundlesData(testTenant, testLocalCluster, bundledNsLocal);
            assertNotNull(bundlesData);
            assertEquals(bundlesData.getBoundaries().size(), 3);
            assertEquals(bundlesData.getBoundaries().get(0), "0x00000000");
            assertEquals(bundlesData.getBoundaries().get(1), "0x7fffffff");
            assertEquals(bundlesData.getBoundaries().get(2), "0xffffffff");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatus(), Status.PRECONDITION_FAILED.getStatusCode());
        }
    }

    @Test
    public void testSplitBundleWithUnDividedRange() throws Exception {
        URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
        String bundledNsLocal = "test-bundled-namespace-1";
        List<String> boundaries = Lists.newArrayList("0x00000000", "0x08375b1a", "0x08375b1b", "0xffffffff");
        BundlesData bundleData = BundlesData.builder()
                .boundaries(boundaries)
                .numBundles(boundaries.size() - 1)
                .build();
        createBundledTestNamespaces(this.testTenant, this.testLocalCluster, bundledNsLocal, bundleData);
        final NamespaceName testNs = NamespaceName.get(this.testTenant, this.testLocalCluster, bundledNsLocal);

        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
        doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);
        mockWebUrl(localWebServiceUrl, testNs);

        // split bundles
        AsyncResponse response = mock(AsyncResponse.class);
        namespaces.splitNamespaceBundle(response, testTenant, testLocalCluster, bundledNsLocal,
                "0x08375b1a_0x08375b1b", false, false);
        ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(any(RestException.class));
    }

    @Test
    public void testUnloadNamespaceWithBundles() throws Exception {
        URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
        String bundledNsLocal = "test-bundled-namespace-1";
        List<String> boundaries = Lists.newArrayList("0x00000000", "0x80000000", "0xffffffff");
        BundlesData bundleData = BundlesData.builder()
                .boundaries(boundaries)
                .numBundles(boundaries.size() - 1)
                .build();
        createBundledTestNamespaces(this.testTenant, this.testLocalCluster, bundledNsLocal, bundleData);
        final NamespaceName testNs = NamespaceName.get(this.testTenant, this.testLocalCluster, bundledNsLocal);

        doReturn(CompletableFuture.completedFuture(Optional.of(localWebServiceUrl))).when(nsSvc)
                .getWebServiceUrlAsync(Mockito.argThat(bundle -> bundle.getNamespaceObject().equals(testNs)),
                        Mockito.any());
        doReturn(true).when(nsSvc)
                .isServiceUnitOwned(Mockito.argThat(bundle -> bundle.getNamespaceObject().equals(testNs)));

        NamespaceBundles nsBundles = nsSvc.getNamespaceBundleFactory().getBundles(testNs, bundleData);
        NamespaceBundle testBundle = nsBundles.getBundles().get(0);
        // make one bundle owned
        LookupOptions optionsHttps = LookupOptions.builder().authoritative(false).requestHttps(true).readOnly(false).build();
        doReturn(CompletableFuture.completedFuture(Optional.of(localWebServiceUrl))).when(nsSvc).getWebServiceUrlAsync(testBundle, optionsHttps);
        doReturn(true).when(nsSvc).isServiceUnitOwned(testBundle);
        doReturn(CompletableFuture.completedFuture(null)).when(nsSvc).unloadNamespaceBundle(testBundle);
        AsyncResponse response = mock(AsyncResponse.class);
        namespaces.unloadNamespaceBundle(response, testTenant, testLocalCluster, bundledNsLocal, "0x00000000_0x80000000",
                false);
        verify(nsSvc, times(1)).unloadNamespaceBundle(testBundle);
        try {
            namespaces.unloadNamespaceBundle(response, testTenant, testLocalCluster, bundledNsLocal, "0x00000000_0x88000000",
                    false);
            fail("should have failed");
        } catch (RestException re) {
            // ok
        }
    }

    private void createBundledTestNamespaces(String property, String cluster, String namespace, BundlesData bundle)
            throws Exception {
        namespaces.createNamespace(property, cluster, namespace, bundle);
    }

    private void createGlobalTestNamespaces(String property, String namespace, BundlesData bundle) throws Exception {
        namespaces.createNamespace(property, "global", namespace, bundle);
    }

    private void createTestNamespaces(List<NamespaceName> nsnames, BundlesData bundle) throws Exception {
        for (NamespaceName nsName : nsnames) {
            namespaces.createNamespace(nsName.getTenant(), nsName.getCluster(), nsName.getLocalName(), bundle);
        }
    }

    @Test
    public void testValidateAdminAccessOnTenant() throws Exception {
        try {
            final String tenant = "prop";
            pulsar.getConfiguration().setAuthenticationEnabled(true);
            pulsar.getConfiguration().setAuthorizationEnabled(true);
            pulsar.getPulsarResources().getTenantResources().createTenant(tenant,
                    new TenantInfoImpl(Sets.newHashSet(namespaces.clientAppId()), Sets.newHashSet("use")));

            namespaces.validateTenantOperation(tenant, null);
        } finally {
            pulsar.getConfiguration().setAuthenticationEnabled(false);
            pulsar.getConfiguration().setAuthorizationEnabled(false);
        }
    }

    @Test
    public void testRetention() throws Exception {
        try {
            URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
            String bundledNsLocal = "test-bundled-namespace-1";
            List<String> boundaries = Lists.newArrayList("0x00000000", "0xffffffff");
            BundlesData bundleData = BundlesData.builder()
                    .boundaries(boundaries)
                    .numBundles(boundaries.size() - 1)
                    .build();
            createBundledTestNamespaces(this.testTenant, this.testLocalCluster, bundledNsLocal, bundleData);
            final NamespaceName testNs = NamespaceName.get(this.testTenant, this.testLocalCluster, bundledNsLocal);
            mockWebUrl(localWebServiceUrl, testNs);

            OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
            doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
            Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
            ownership.setAccessible(true);
            ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);
            RetentionPolicies retention = new RetentionPolicies(10, 10);
            namespaces.setRetention(this.testTenant, this.testLocalCluster, bundledNsLocal, retention);
            RetentionPolicies retention2 = namespaces.getRetention(this.testTenant, this.testLocalCluster,
                    bundledNsLocal);
            assertEquals(retention, retention2);
        } catch (RestException e) {
            fail("ValidateNamespaceOwnershipWithBundles failed");
        }
    }

    @Test
    public void testRetentionUnauthorized() throws Exception {
        try {
            NamespaceName testNs = this.testLocalNamespaces.get(3);
            RetentionPolicies retention = new RetentionPolicies(10, 10);
            namespaces.setRetention(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), retention);
            fail("Should fail");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }
    }

    @Test
    public void testPersistence() throws Exception {
        NamespaceName testNs = this.testLocalNamespaces.get(0);
        PersistencePolicies persistence1 = new PersistencePolicies(3, 2, 1, 0.0);
        namespaces.setPersistence(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), persistence1);
        PersistencePolicies persistence2 = namespaces.getPersistence(testNs.getTenant(), testNs.getCluster(),
                testNs.getLocalName());
        assertEquals(persistence2, persistence1);
    }

    @Test
    public void testPersistenceUnauthorized() throws Exception {
        try {
            NamespaceName testNs = this.testLocalNamespaces.get(3);
            PersistencePolicies persistence = new PersistencePolicies(3, 2, 1, 0.0);
            namespaces.setPersistence(testNs.getTenant(), testNs.getCluster(), testNs.getLocalName(), persistence);
            fail("Should fail");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }
    }

    @Test
    public void testValidateTopicOwnership() throws Exception {
        URL localWebServiceUrl = new URL(pulsar.getSafeWebServiceAddress());
        String bundledNsLocal = "test-bundled-namespace-1";
        List<String> boundaries = Lists.newArrayList("0x00000000", "0xffffffff");
        BundlesData bundleData = BundlesData.builder()
                .boundaries(boundaries)
                .numBundles(boundaries.size() - 1)
                .build();
        createBundledTestNamespaces(this.testTenant, this.testLocalCluster, bundledNsLocal, bundleData);
        final NamespaceName testNs = NamespaceName.get(this.testTenant, this.testLocalCluster, bundledNsLocal);
        OwnershipCache MockOwnershipCache = spy(pulsar.getNamespaceService().getOwnershipCache());
        doReturn(CompletableFuture.completedFuture(null)).when(MockOwnershipCache).disableOwnership(any(NamespaceBundle.class));
        Field ownership = NamespaceService.class.getDeclaredField("ownershipCache");
        ownership.setAccessible(true);
        ownership.set(pulsar.getNamespaceService(), MockOwnershipCache);
        TopicName topicName = TopicName.get(testNs.getPersistentTopicName("my-topic"));
        PersistentTopics topics = spy(new PersistentTopics());
        topics.setServletContext(new MockServletContext());
        topics.setPulsar(pulsar);
        doReturn(false).when(topics).isRequestHttps();
        doReturn("test").when(topics).clientAppId();
        doReturn(null).when(topics).originalPrincipal();
        doReturn(null).when(topics).clientAuthData();
        mockWebUrl(localWebServiceUrl, testNs);
        doReturn("persistent").when(topics).domain();

        topics.validateTopicName(topicName.getTenant(), topicName.getCluster(),
                topicName.getNamespacePortion(), topicName.getEncodedLocalName());
        topics.validateAdminOperationOnTopic(false);
    }

    @Test
    public void testIsLeader() throws Exception {
        assertTrue(namespaces.isLeaderBroker());
    }

    /**
     * Verifies that deleteNamespace cleans up policies(global,local), bundle cache and bundle ownership

     * @throws Exception
     */
    @Test
    public void testDeleteNamespace() throws Exception {

        final String namespace = this.testTenant + "/use/deleteNs";
        admin.namespaces().createNamespace(namespace, 100);
        assertEquals(admin.namespaces().getPolicies(namespace).bundles.getNumBundles(), 100);

        // (1) Force topic creation and namespace being loaded
        final String topicName = "persistent://" + namespace + "/my-topic";
        TopicName topic = TopicName.get(topicName);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();
        NamespaceBundle bundle1 = pulsar.getNamespaceService().getBundle(topic);
        // (2) Delete topic
        admin.topics().delete(topicName);
        // (3) Delete ns
        admin.namespaces().deleteNamespace(namespace);
        // (4) check bundle
        NamespaceBundle bundle2 = pulsar.getNamespaceService().getBundle(topic);
        assertNotEquals(bundle1.getBundleRange(), bundle2.getBundleRange());
        // returns full bundle if policies not present
        assertEquals("0x00000000_0xffffffff", bundle2.getBundleRange());

    }

    /**
     * Verifies that force deleteNamespace delete all topics in the namespace
     * @throws Exception
     */
    @Test
    public void testForceDeleteNamespace() throws Exception {
        // allow forced deletion of namespaces
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");
        String topic = namespace + "/topic";
        String non_persistent_topic = "non-persistent://" + topic;

        admin.namespaces().createNamespace(namespace, 100);

        admin.topics().createPartitionedTopic(topic, 10);

        admin.topics().createNonPartitionedTopic(non_persistent_topic);

        List<String> topicList = admin.topics().getList(namespace);
        assertFalse(topicList.isEmpty());

        try {
            admin.namespaces().deleteNamespace(namespace, false);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            // Expected: Cannot delete non empty namespace
        }

        admin.namespaces().deleteNamespace(namespace, true);
        admin.namespaces().createNamespace(namespace, 100);
        topicList = admin.topics().getList(namespace);
        assertTrue(topicList.isEmpty());

        // reset back to false
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);
    }

    @Test
    public void testForceDeleteNamespaceNotAllowed() throws Exception {
        assertFalse(pulsar.getConfiguration().isForceDeleteNamespaceAllowed());

        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");
        String topic = namespace + "/topic";
        String non_persistent_topic = "non-persistent://" + topic;

        admin.namespaces().createNamespace(namespace, 100);

        admin.topics().createPartitionedTopic(topic, 10);

        admin.topics().createNonPartitionedTopic(non_persistent_topic);

        List<String> topicList = admin.topics().getList(namespace);
        assertFalse(topicList.isEmpty());

        try {
            admin.namespaces().deleteNamespace(namespace, false);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            // Expected: Cannot delete non empty namespace
        }

        try {
            admin.namespaces().deleteNamespace(namespace, true);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            // Expected: Cannot delete due to broker is not allowed
        }

        assertTrue(admin.namespaces().getNamespaces(this.testTenant).contains(namespace));
    }

    @Test
    public void testSubscribeRate() throws Exception {
        SubscribeRate subscribeRate = new SubscribeRate(1, 5);
        String namespace = "my-tenants/my-namespace";
        admin.tenants().createTenant("my-tenants",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet(testLocalCluster)));
        admin.namespaces().createNamespace(namespace, Sets.newHashSet(testLocalCluster));
        admin.namespaces().setSubscribeRate(namespace, subscribeRate);
        assertEquals(subscribeRate, admin.namespaces().getSubscribeRate(namespace));
        String topicName = "persistent://" + namespace + "/" + "subscribe-rate";

        admin.topics().createPartitionedTopic(topicName, 2);
        pulsar.getConfiguration().setAuthorizationEnabled(false);
        Consumer<?> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("subscribe-rate")
                .subscribe();
        assertTrue(consumer.isConnected());

        // Subscribe Rate Limiter is enabled, will limited by broker
        pulsarClient.updateServiceUrl(lookupUrl.toString());
        Awaitility.await().untilAsserted(() -> assertFalse(consumer.isConnected()));

        // Out of limit period
        pulsarClient.updateServiceUrl(lookupUrl.toString());
        Awaitility.await()
                .pollDelay(Duration.ofSeconds(6))
                .untilAsserted(() -> assertTrue(consumer.isConnected()));

        // Disable Subscribe Rate Limiter
        subscribeRate = new SubscribeRate(0, 10);
        admin.namespaces().setSubscribeRate(namespace, subscribeRate);
        pulsarClient.updateServiceUrl(lookupUrl.toString());
        Awaitility.await().untilAsserted(() -> assertTrue(consumer.isConnected()));
        pulsar.getConfiguration().setAuthorizationEnabled(true);
        admin.topics().deletePartitionedTopic(topicName, true);
        admin.namespaces().deleteNamespace(namespace);
        admin.tenants().deleteTenant("my-tenants");
    }

    class MockLedgerOffloader implements LedgerOffloader {
        ConcurrentHashMap<Long, UUID> offloads = new ConcurrentHashMap<Long, UUID>();
        ConcurrentHashMap<Long, UUID> deletes = new ConcurrentHashMap<Long, UUID>();

        Set<Long> offloadedLedgers() {
            return offloads.keySet();
        }

        Set<Long> deletedOffloads() {
            return deletes.keySet();
        }

        OffloadPoliciesImpl offloadPolicies;

        public MockLedgerOffloader(OffloadPoliciesImpl offloadPolicies) {
            this.offloadPolicies = offloadPolicies;
        }

        @Override
        public String getOffloadDriverName() {
            return "mock";
        }

        @Override
        public CompletableFuture<Void> offload(ReadHandle ledger,
                                               UUID uuid,
                                               Map<String, String> extraMetadata) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            if (offloads.putIfAbsent(ledger.getId(), uuid) == null) {
                promise.complete(null);
            } else {
                promise.completeExceptionally(new Exception("Already exists exception"));
            }
            return promise;
        }

        @Override
        public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid,
                                                           Map<String, String> offloadDriverMetadata) {
            CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
            promise.completeExceptionally(new UnsupportedOperationException());
            return promise;
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uuid,
                                                       Map<String, String> offloadDriverMetadata) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            if (offloads.remove(ledgerId, uuid)) {
                deletes.put(ledgerId, uuid);
                promise.complete(null);
            } else {
                promise.completeExceptionally(new Exception("Not found"));
            }
            return promise;
        };

        @Override
        public OffloadPoliciesImpl getOffloadPolicies() {
            return offloadPolicies;
        }

        @Override
        public void close() {

        }
    }

    @Test
    public void testSetOffloadThreshold() throws Exception {
        TopicName topicName = TopicName.get("persistent", this.testTenant, "offload", "offload-topic");
        String namespace =  topicName.getNamespaceObject().toString();
        System.out.println(namespace);
        // set a default
        pulsar.getConfiguration().setManagedLedgerOffloadAutoTriggerSizeThresholdBytes(1);
        // create the namespace
        admin.namespaces().createNamespace(namespace, Sets.newHashSet(testLocalCluster));
        admin.topics().createNonPartitionedTopic(topicName.toString());

        // assert we get the default which indicates it will fall back to default
        assertEquals(-1, admin.namespaces().getOffloadThreshold(namespace));
        // the ledger config should have the expected value
        ManagedLedgerConfig ledgerConf = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
        MockLedgerOffloader offloader = new MockLedgerOffloader(OffloadPoliciesImpl.create("S3", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                admin.namespaces().getOffloadThreshold(namespace),
                pulsar.getConfiguration().getManagedLedgerOffloadDeletionLagMs(),
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY));
        ledgerConf.setLedgerOffloader(offloader);
        assertEquals(ledgerConf.getLedgerOffloader().getOffloadPolicies().getManagedLedgerOffloadThresholdInBytes(),
                new Long(-1));

        // set an override for the namespace
        admin.namespaces().setOffloadThreshold(namespace, 100);
        assertEquals(100, admin.namespaces().getOffloadThreshold(namespace));
        ledgerConf = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
        admin.namespaces().getOffloadPolicies(namespace);
        offloader = new MockLedgerOffloader(OffloadPoliciesImpl.create("S3", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                admin.namespaces().getOffloadThreshold(namespace),
                pulsar.getConfiguration().getManagedLedgerOffloadDeletionLagMs(),
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY));
        ledgerConf.setLedgerOffloader(offloader);
        assertEquals(ledgerConf.getLedgerOffloader().getOffloadPolicies().getManagedLedgerOffloadThresholdInBytes(),
                new Long(100));

        // set another negative value to disable
        admin.namespaces().setOffloadThreshold(namespace, -2);
        assertEquals(-2, admin.namespaces().getOffloadThreshold(namespace));
        ledgerConf = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
        offloader = new MockLedgerOffloader(OffloadPoliciesImpl.create("S3", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                admin.namespaces().getOffloadThreshold(namespace),
                pulsar.getConfiguration().getManagedLedgerOffloadDeletionLagMs(),
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY));
        ledgerConf.setLedgerOffloader(offloader);
        assertEquals(ledgerConf.getLedgerOffloader().getOffloadPolicies().getManagedLedgerOffloadThresholdInBytes(),
                new Long(-2));

        // set back to -1 and fall back to default
        admin.namespaces().setOffloadThreshold(namespace, -1);
        assertEquals(-1, admin.namespaces().getOffloadThreshold(namespace));
        ledgerConf = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
        offloader = new MockLedgerOffloader(OffloadPoliciesImpl.create("S3", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                admin.namespaces().getOffloadThreshold(namespace),
                pulsar.getConfiguration().getManagedLedgerOffloadDeletionLagMs(),
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY));
        ledgerConf.setLedgerOffloader(offloader);
        assertEquals(ledgerConf.getLedgerOffloader().getOffloadPolicies().getManagedLedgerOffloadThresholdInBytes(),
                new Long(-1));

        // cleanup
        admin.topics().delete(topicName.toString(), true);
        admin.namespaces().deleteNamespace(namespace);
    }

    private void mockWebUrl(URL localWebServiceUrl, NamespaceName namespace) throws Exception {
        doReturn(Optional.of(localWebServiceUrl)).when(nsSvc)
                .getWebServiceUrl(Mockito.argThat(bundle -> bundle.getNamespaceObject().equals(namespace)),
                        Mockito.any());
        doReturn(true).when(nsSvc)
                .isServiceUnitOwned(Mockito.argThat(bundle -> bundle.getNamespaceObject().equals(namespace)));
    }

    @Test
    public void testDeleteNonPartitionedTopicMultipleTimes() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");
        String topic = namespace + "/topic";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet(testLocalCluster));

        admin.topics().createNonPartitionedTopic(topic);

        admin.topics().delete(topic);

        try {
            admin.topics().delete(topic);
            fail("should have failed");
        } catch (NotFoundException e) {
            // Expected
        }

        admin.namespaces().deleteNamespace(namespace);

        try {
            admin.topics().delete(topic);
            fail("should have failed");
        } catch (NotFoundException e) {
            // Expected
        }
    }

    @Test
    public void testDeletePartitionedTopicMultipleTimes() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");
        String topic = namespace + "/topic";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet(testLocalCluster));

        admin.topics().createPartitionedTopic(topic, 3);
        assertEquals(admin.topics().getPartitionedTopicMetadata(topic).partitions, 3);

        admin.topics().deletePartitionedTopic(topic);

        try {
            admin.topics().deletePartitionedTopic(topic);
            fail("should have failed");
        } catch (NotFoundException e) {
            // Expected
        }

        admin.namespaces().deleteNamespace(namespace);

        try {
            admin.topics().deletePartitionedTopic(topic);
            fail("should have failed");
        } catch (NotFoundException e) {
            // Expected
        }
    }

    @Test
    public void testRetentionPolicyValidation() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");

        admin.namespaces().createNamespace(namespace, Sets.newHashSet(testLocalCluster));

        // should pass
        admin.namespaces().setRetention(namespace, new RetentionPolicies());
        admin.namespaces().setRetention(namespace, new RetentionPolicies(-1, -1));
        admin.namespaces().setRetention(namespace, new RetentionPolicies(1, 1));

        // should not pass validation
        assertInvalidRetentionPolicy(namespace, 1, 0);
        assertInvalidRetentionPolicy(namespace, 0, 1);
        assertInvalidRetentionPolicy(namespace, -1, 0);
        assertInvalidRetentionPolicy(namespace, 0, -1);
        assertInvalidRetentionPolicy(namespace, -2, 1);
        assertInvalidRetentionPolicy(namespace, 1, -2);

        admin.namespaces().deleteNamespace(namespace);
    }

    @Test(timeOut = THREE_MINUTE_MILLIS)
    public void testMaxTopicsPerNamespace() throws Exception {
        cleanup();
        conf.setMaxTopicsPerNamespace(15);
        super.internalSetup();

        String namespace = "testTenant/ns1";
        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet("use"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("use"));

        admin.namespaces().setMaxTopicsPerNamespace(namespace, 10);
        assertEquals(10, admin.namespaces().getMaxTopicsPerNamespace(namespace));

        // check create partitioned/non-partitioned topics using namespace policy
        String topic = "persistent://testTenant/ns1/test_create_topic_v";
        admin.topics().createPartitionedTopic(topic + "1", 2);
        admin.topics().createPartitionedTopic(topic + "2", 3);
        admin.topics().createPartitionedTopic(topic + "3", 4);
        admin.topics().createNonPartitionedTopic(topic + "4");

        try {
            admin.topics().createPartitionedTopic(topic + "5", 2);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
            assertEquals(e.getHttpError(), "Exceed maximum number of topics in namespace.");
        }

        // remove namespace policy limit, use broker configuration instead.
        admin.namespaces().removeMaxTopicsPerNamespace(namespace);
        admin.topics().createPartitionedTopic(topic + "6", 4);
        try {
            admin.topics().createPartitionedTopic(topic + "7", 3);
            fail();
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
            assertEquals(e.getHttpError(), "Exceed maximum number of topics in namespace.");
        }

        admin.namespaces().setMaxTopicsPerNamespace(namespace, 0);
        // set namespace policy to no limit
        for (int i = 0; i< 10; ++i) {
            admin.topics().createPartitionedTopic(topic + "_v" + i, 2);
            admin.topics().createNonPartitionedTopic(topic + "_vn" + i);
        }


        // check producer/consumer auto create partitioned topic
        cleanup();
        conf.setMaxTopicsPerNamespace(0);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreationType("partitioned");
        super.internalSetup();

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("use"));
        admin.namespaces().setMaxTopicsPerNamespace(namespace, 10);

        pulsarClient.newProducer().topic(topic + "1").create().close();
        pulsarClient.newProducer().topic(topic + "2").create().close();
        pulsarClient.newConsumer().topic(topic + "3").subscriptionName("test_sub").subscribe().close();

        try {
            pulsarClient.newConsumer().topic(topic + "4").subscriptionName("test_sub").subscribe().close();
            fail();
        } catch (PulsarClientException e) {
            log.info("Exception: ", e);
        }

        // remove namespace limit
        admin.namespaces().removeMaxTopicsPerNamespace(namespace);
        for (int i = 0; i < 10; ++i) {
            pulsarClient.newProducer().topic(topic + "_p" + i).create().close();
            pulsarClient.newConsumer().topic(topic + "_c" + i).subscriptionName("test_sub").subscribe().close();
        }

        // check producer/consumer auto create non-partitioned topic
        cleanup();
        conf.setMaxTopicsPerNamespace(0);
        conf.setDefaultNumPartitions(1);
        conf.setAllowAutoTopicCreationType("non-partitioned");
        super.internalSetup();

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("use"));
        admin.namespaces().setMaxTopicsPerNamespace(namespace, 3);

        pulsarClient.newProducer().topic(topic + "1").create().close();
        pulsarClient.newProducer().topic(topic + "2").create().close();
        pulsarClient.newConsumer().topic(topic + "3").subscriptionName("test_sub").subscribe().close();

        try {
            pulsarClient.newConsumer().topic(topic + "4").subscriptionName("test_sub").subscribe().close();
            fail();
        } catch (PulsarClientException e) {
            log.info("Exception: ", e);
        }

        // set namespace limit to 5
        admin.namespaces().setMaxTopicsPerNamespace(namespace, 5);
        pulsarClient.newProducer().topic(topic + "4").create().close();
        pulsarClient.newProducer().topic(topic + "5").create().close();
        try {
            pulsarClient.newConsumer().topic(topic + "6").subscriptionName("test_sub").subscribe().close();
            fail();
        } catch (PulsarClientException e) {
            log.info("Exception: ", e);
        }

        // remove namespace limit
        admin.namespaces().removeMaxTopicsPerNamespace(namespace);
        for (int i = 0; i< 10; ++i) {
            pulsarClient.newProducer().topic(topic + "_p" + i).create().close();
            pulsarClient.newConsumer().topic(topic + "_c" + i).subscriptionName("test_sub").subscribe().close();
        }

        conf.setMaxTopicsPerNamespace(0);
        conf.setDefaultNumPartitions(1);
        conf.setAllowAutoTopicCreationType("non-partitioned");
    }

    private void assertInvalidRetentionPolicy(String namespace, int retentionTimeInMinutes, int retentionSizeInMB) {
        try {
            RetentionPolicies retention = new RetentionPolicies(retentionTimeInMinutes, retentionSizeInMB);
            admin.namespaces().setRetention(namespace, retention);
            fail("Validation should have failed for " + retention);
        } catch (PulsarAdminException e) {
            assertTrue(e.getCause() instanceof BadRequestException);
            assertTrue(e.getMessage().startsWith("Invalid retention policy"));
        }
    }

    @Test
    public void testRetentionPolicyValidationAsPartOfAllPolicies() throws Exception {
        Policies policies = new Policies();
        policies.replication_clusters = Sets.newHashSet(testLocalCluster);

        assertValidRetentionPolicyAsPartOfAllPolicies(policies, 0, 0);
        assertValidRetentionPolicyAsPartOfAllPolicies(policies, -1, -1);
        assertValidRetentionPolicyAsPartOfAllPolicies(policies, 1, 1);

        // should not pass validation
        assertInvalidRetentionPolicyAsPartOfAllPolicies(policies, 1, 0);
        assertInvalidRetentionPolicyAsPartOfAllPolicies(policies, 0, 1);
        assertInvalidRetentionPolicyAsPartOfAllPolicies(policies, -1, 0);
        assertInvalidRetentionPolicyAsPartOfAllPolicies(policies, 0, -1);
        assertInvalidRetentionPolicyAsPartOfAllPolicies(policies, -2, 1);
        assertInvalidRetentionPolicyAsPartOfAllPolicies(policies, 1, -2);
    }

    @Test
    public void testSubscriptionTypesEnabled() throws PulsarAdminException, PulsarClientException {
        pulsar.getConfiguration().setAuthorizationEnabled(false);
        pulsar.getConfiguration().setTopicLevelPoliciesEnabled(false);
        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");
        String topic = namespace + "/test-subscription-enabled";
        admin.namespaces().createNamespace(namespace);
        Set<SubscriptionType> subscriptionTypes = new HashSet<>();
        subscriptionTypes.add(SubscriptionType.Shared);
        subscriptionTypes.add(SubscriptionType.Exclusive);
        subscriptionTypes.add(SubscriptionType.Failover);
        subscriptionTypes.add(SubscriptionType.Key_Shared);
        admin.namespaces().setSubscriptionTypesEnabled(namespace, subscriptionTypes);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topic)
                .subscriptionType(SubscriptionType.Shared).subscriptionName("share");

        consumerBuilder.subscribe().close();
        subscriptionTypes = admin.namespaces().getSubscriptionTypesEnabled(namespace);
        assertEquals(SubscriptionType.values().length, subscriptionTypes.size());
        // All subType this namespace support
        for (SubscriptionType value : SubscriptionType.values()) {
            assertTrue(subscriptionTypes.contains(value));
        }

        // Remove shared subType for this namespace and sub with shared subscription type fail
        subscriptionTypes.remove(SubscriptionType.Shared);
        admin.namespaces().setSubscriptionTypesEnabled(namespace, subscriptionTypes);
        assertFalse(admin.namespaces().getSubscriptionTypesEnabled(namespace).contains(SubscriptionType.Shared));

        try {
            consumerBuilder.subscribe().close();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }
        // Add shared SubType for this namespace and sub with shared subscription type success
        subscriptionTypes.add(SubscriptionType.Shared);
        admin.namespaces().setSubscriptionTypesEnabled(namespace, subscriptionTypes);
        consumerBuilder.subscribe().close();

        // remove failover SubType for this namespace and sub with failover subscription type fail
        subscriptionTypes.remove(SubscriptionType.Failover);
        admin.namespaces().setSubscriptionTypesEnabled(namespace, subscriptionTypes);
        consumerBuilder.subscriptionType(SubscriptionType.Failover);
        try {
            consumerBuilder.subscribe().close();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }

        // clear all namespace subType enabled, add failover to broker.conf and sub with shared will fail
        admin.namespaces().removeSubscriptionTypesEnabled(namespace);
        assertEquals(admin.namespaces().getSubscriptionTypesEnabled(namespace), Sets.newHashSet());
        consumerBuilder.subscriptionType(SubscriptionType.Shared);
        admin.brokers().updateDynamicConfiguration("subscriptionTypesEnabled", "Failover");
        Awaitility.await().untilAsserted(()->{
            Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
            assertTrue(((AbstractTopic) t).getHierarchyTopicPolicies().getSubscriptionTypesEnabled().getBrokerValue()
                    .contains(CommandSubscribe.SubType.Failover));
        });
        try {
            consumerBuilder.subscribe().close();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }

        // add shared to broker.conf and sub with shared will success
        admin.brokers().updateDynamicConfiguration("subscriptionTypesEnabled", "Failover,Shared");
        Awaitility.await().untilAsserted(()->{
            Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
            assertTrue(((AbstractTopic) t).getHierarchyTopicPolicies().getSubscriptionTypesEnabled().getBrokerValue()
                    .contains(CommandSubscribe.SubType.Failover));
        });
        consumerBuilder.subscribe().close();
    }

    private void assertValidRetentionPolicyAsPartOfAllPolicies(Policies policies, int retentionTimeInMinutes,
                                                               int retentionSizeInMB) throws PulsarAdminException {
        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");
        policies.retention_policies = new RetentionPolicies(retentionTimeInMinutes, retentionSizeInMB);
        admin.namespaces().createNamespace(namespace, policies);
        admin.namespaces().deleteNamespace(namespace);
    }

    private void assertInvalidRetentionPolicyAsPartOfAllPolicies(Policies policies, int retentionTimeInMinutes,
                                                                 int retentionSizeInMB) {
        String namespace = BrokerTestUtil.newUniqueName(this.testTenant + "/namespace");
        try {
            RetentionPolicies retention = new RetentionPolicies(retentionTimeInMinutes, retentionSizeInMB);
            policies.retention_policies = retention;
            admin.namespaces().createNamespace(namespace, policies);
            fail("Validation should have failed for " + retention);
        } catch (PulsarAdminException e) {
            assertTrue(e.getCause() instanceof BadRequestException);
            assertTrue(e.getMessage().startsWith("Invalid retention policy"));
        }
    }
}
