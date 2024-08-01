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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.deleteNamespaceWithRetry;
import static org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class NamespaceAuthZTest extends MockedPulsarStandalone {

    private PulsarAdmin superUserAdmin;

    private PulsarAdmin tenantManagerAdmin;

    private PulsarClient pulsarClient;

    private AuthorizationService authorizationService;

    private static final String TENANT_ADMIN_SUBJECT = UUID.randomUUID().toString();
    private static final String TENANT_ADMIN_TOKEN = Jwts.builder()
            .claim("sub", TENANT_ADMIN_SUBJECT).signWith(SECRET_KEY).compact();

    @SneakyThrows
    @BeforeClass
    public void setup() {
        getServiceConfiguration().setEnablePackagesManagement(true);
        getServiceConfiguration().setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        getServiceConfiguration().setDefaultNumberOfNamespaceBundles(1);
        getServiceConfiguration().setForceDeleteNamespaceAllowed(true);
        configureTokenAuthentication();
        configureDefaultAuthorization();
        start();
        this.superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        final TenantInfo tenantInfo = superUserAdmin.tenants().getTenantInfo("public");
        tenantInfo.getAdminRoles().add(TENANT_ADMIN_SUBJECT);
        superUserAdmin.tenants().updateTenant("public", tenantInfo);
        this.tenantManagerAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
        this.pulsarClient = super.getPulsarService().getClient();
        this.authorizationService = Mockito.spy(getPulsarService().getBrokerService().getAuthorizationService());
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                authorizationService, true);
    }


    @SneakyThrows
    @AfterClass
    public void cleanup() {
        if (superUserAdmin != null) {
            superUserAdmin.close();
            superUserAdmin = null;
        }
        if (tenantManagerAdmin != null) {
            tenantManagerAdmin.close();
            tenantManagerAdmin = null;
        }
        pulsarClient = null;
        authorizationService = null;
        close();
    }

    @AfterMethod
    public void after() throws Exception {
        deleteNamespaceWithRetry("public/default", true, superUserAdmin);
        superUserAdmin.namespaces().createNamespace("public/default");
    }

    private AtomicBoolean setAuthorizationOperationChecker(String role, NamespaceOperation operation) {
        AtomicBoolean execFlag = new AtomicBoolean(false);
        Mockito.doAnswer(invocationOnMock -> {
            String role_ = invocationOnMock.getArgument(2);
            if (role.equals(role_)) {
                NamespaceOperation operation_ = invocationOnMock.getArgument(1);
                Assert.assertEquals(operation_, operation);
            }
            execFlag.set(true);
            return invocationOnMock.callRealMethod();
        }).when(authorizationService).allowNamespaceOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any());
         return execFlag;
    }

    private void clearAuthorizationOperationChecker() {
        Mockito.doAnswer(InvocationOnMock::callRealMethod).when(authorizationService)
                .allowNamespaceOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                        Mockito.any());
    }

    private AtomicBoolean setAuthorizationPolicyOperationChecker(String role, Object policyName, Object operation) {
        AtomicBoolean execFlag = new AtomicBoolean(false);
        if (operation instanceof PolicyOperation) {
            Mockito.doAnswer(invocationOnMock -> {
                String role_ = invocationOnMock.getArgument(3);
                if (role.equals(role_)) {
                    PolicyName policyName_ = invocationOnMock.getArgument(1);
                    PolicyOperation operation_ = invocationOnMock.getArgument(2);
                    assertEquals(operation_, operation);
                    assertEquals(policyName_, policyName);
                }
                execFlag.set(true);
                return invocationOnMock.callRealMethod();
            }).when(authorizationService).allowNamespacePolicyOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any());
        } else {
            throw new IllegalArgumentException("");
        }
        return execFlag;
    }

    @SneakyThrows
    @Test
    public void testProperties() {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://public/default/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test superuser
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        superUserAdmin.namespaces().setProperties(namespace, properties);
        superUserAdmin.namespaces().setProperty(namespace, "key2", "value2");
        superUserAdmin.namespaces().getProperties(namespace);
        superUserAdmin.namespaces().getProperty(namespace, "key2");
        superUserAdmin.namespaces().removeProperty(namespace, "key2");
        superUserAdmin.namespaces().clearProperties(namespace);

        // test tenant manager
        tenantManagerAdmin.namespaces().setProperties(namespace, properties);
        tenantManagerAdmin.namespaces().setProperty(namespace, "key2", "value2");
        tenantManagerAdmin.namespaces().getProperties(namespace);
        tenantManagerAdmin.namespaces().getProperty(namespace, "key2");
        tenantManagerAdmin.namespaces().removeProperty(namespace, "key2");
        tenantManagerAdmin.namespaces().clearProperties(namespace);

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setProperties(namespace, properties));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setProperty(namespace, "key2", "value2"));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getProperties(namespace));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getProperty(namespace, "key2"));


        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeProperty(namespace, "key2"));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearProperties(namespace));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().setProperties(namespace, properties));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().setProperty(namespace, "key2", "value2"));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().getProperties(namespace));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().getProperty(namespace, "key2"));


            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().removeProperty(namespace, "key2"));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().clearProperties(namespace));

            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }
        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testTopics() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        // test super admin
        superUserAdmin.namespaces().getTopics(namespace);

        // test tenant manager
        tenantManagerAdmin.namespaces().getTopics(namespace);

        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.GET_TOPICS);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getTopics(namespace));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            if (AuthAction.consume == action || AuthAction.produce == action) {
                subAdmin.namespaces().getTopics(namespace);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().getTopics(namespace));
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testBookieAffinityGroup() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        // test super admin
        BookieAffinityGroupData bookieAffinityGroupData = BookieAffinityGroupData.builder()
                .bookkeeperAffinityGroupPrimary("aaa")
                .bookkeeperAffinityGroupSecondary("bbb")
                .build();
        superUserAdmin.namespaces().setBookieAffinityGroup(namespace, bookieAffinityGroupData);
        BookieAffinityGroupData bookieAffinityGroup = superUserAdmin.namespaces().getBookieAffinityGroup(namespace);
        Assert.assertEquals(bookieAffinityGroupData, bookieAffinityGroup);
        superUserAdmin.namespaces().deleteBookieAffinityGroup(namespace);
        bookieAffinityGroup = superUserAdmin.namespaces().getBookieAffinityGroup(namespace);
        Assert.assertNull(bookieAffinityGroup);

        // test tenant manager
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> tenantManagerAdmin.namespaces().setBookieAffinityGroup(namespace, bookieAffinityGroupData));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> tenantManagerAdmin.namespaces().getBookieAffinityGroup(namespace));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> tenantManagerAdmin.namespaces().deleteBookieAffinityGroup(namespace));

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setBookieAffinityGroup(namespace, bookieAffinityGroupData));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getBookieAffinityGroup(namespace));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().deleteBookieAffinityGroup(namespace));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().setBookieAffinityGroup(namespace, bookieAffinityGroupData));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().getBookieAffinityGroup(namespace));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().deleteBookieAffinityGroup(namespace));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }


    @Test
    public void testGetBundles() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send("message".getBytes());

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test super admin
        superUserAdmin.namespaces().getBundles(namespace);

        // test tenant manager
        tenantManagerAdmin.namespaces().getBundles(namespace);

        // test nobody
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.GET_BUNDLE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getBundles(namespace));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            if (AuthAction.consume == action || AuthAction.produce == action) {
                subAdmin.namespaces().getBundles(namespace);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().getBundles(namespace));
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testUnloadBundles() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send("message".getBytes());

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        final String defaultBundle = "0x00000000_0xffffffff";

        // test super admin
        superUserAdmin.namespaces().unloadNamespaceBundle(namespace, defaultBundle);

        // test tenant manager
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> tenantManagerAdmin.namespaces().unloadNamespaceBundle(namespace, defaultBundle));

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unloadNamespaceBundle(namespace, defaultBundle));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().unloadNamespaceBundle(namespace, defaultBundle));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testSplitBundles() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send("message".getBytes());

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        final String defaultBundle = "0x00000000_0xffffffff";

        // test super admin
        superUserAdmin.namespaces().splitNamespaceBundle(namespace, defaultBundle, false, null);

        // test tenant manager
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> tenantManagerAdmin.namespaces().splitNamespaceBundle(namespace, defaultBundle, false, null));

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().splitNamespaceBundle(namespace, defaultBundle, false, null));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().splitNamespaceBundle(namespace, defaultBundle, false, null));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testDeleteBundles() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send("message".getBytes());

        for (int i = 0; i < 3; i++) {
            superUserAdmin.namespaces()
                    .splitNamespaceBundle(namespace, Policies.BundleType.LARGEST.toString(), false, null);
        }

        BundlesData bundles = superUserAdmin.namespaces().getBundles(namespace);
        Assert.assertEquals(bundles.getNumBundles(), 4);
        List<String> boundaries = bundles.getBoundaries();
        Assert.assertEquals(boundaries.size(), 5);

        List<String> bundleRanges = new ArrayList<>();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundleRange = boundaries.get(i) + "_" + boundaries.get(i + 1);
            List<Topic> allTopicsFromNamespaceBundle = getPulsarService().getBrokerService()
                    .getAllTopicsFromNamespaceBundle(namespace, namespace + "/" + bundleRange);
            System.out.println(StringUtils.join(allTopicsFromNamespaceBundle));
            if (allTopicsFromNamespaceBundle.isEmpty()) {
                bundleRanges.add(bundleRange);
            }
        }

        // test super admin
        superUserAdmin.namespaces().deleteNamespaceBundle(namespace, bundleRanges.get(0));

        // test tenant manager
        tenantManagerAdmin.namespaces().deleteNamespaceBundle(namespace, bundleRanges.get(1));

        // test nobody
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.DELETE_BUNDLE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().deleteNamespaceBundle(namespace, bundleRanges.get(1)));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().deleteNamespaceBundle(namespace, bundleRanges.get(1)));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }
    }

    @Test
    public void testPermission() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        final String role = "sub";
        final AuthAction testAction = AuthAction.consume;

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        // test super admin
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, role, Set.of(testAction));
        Map<String, Set<AuthAction>> permissions = superUserAdmin.namespaces().getPermissions(namespace);
        Assert.assertEquals(permissions.get(role), Set.of(testAction));
        superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, role);
        permissions = superUserAdmin.namespaces().getPermissions(namespace);
        Assert.assertTrue(permissions.isEmpty());

        // test tenant manager
        tenantManagerAdmin.namespaces().grantPermissionOnNamespace(namespace, role, Set.of(testAction));
        permissions = tenantManagerAdmin.namespaces().getPermissions(namespace);
        Assert.assertEquals(permissions.get(role), Set.of(testAction));
        tenantManagerAdmin.namespaces().revokePermissionsOnNamespace(namespace, role);
        permissions = tenantManagerAdmin.namespaces().getPermissions(namespace);
        Assert.assertTrue(permissions.isEmpty());

        // test nobody
        AtomicBoolean execFlag =
                    setAuthorizationOperationChecker(subject, NamespaceOperation.GRANT_PERMISSION);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().grantPermissionOnNamespace(namespace, role, Set.of(testAction)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.GET_PERMISSION);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getPermissions(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag =
                    setAuthorizationOperationChecker(subject, NamespaceOperation.REVOKE_PERMISSION);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().revokePermissionsOnNamespace(namespace, role));
        Assert.assertTrue(execFlag.get());

        clearAuthorizationOperationChecker();

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().grantPermissionOnNamespace(namespace, role, Set.of(testAction)));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().getPermissions(namespace));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().revokePermissionsOnNamespace(namespace, role));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testPermissionOnSubscription() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        final String subscription = "my-sub";
        final String role = "sub";
        pulsarClient.newConsumer().topic(topic)
                .subscriptionName(subscription)
                .subscribe().close();


        // test super admin
        superUserAdmin.namespaces().grantPermissionOnSubscription(namespace, subscription, Set.of(role));
        Map<String, Set<String>> permissionOnSubscription =
                superUserAdmin.namespaces().getPermissionOnSubscription(namespace);
        Assert.assertEquals(permissionOnSubscription.get(subscription), Set.of(role));
        superUserAdmin.namespaces().revokePermissionOnSubscription(namespace, subscription, role);
        permissionOnSubscription = superUserAdmin.namespaces().getPermissionOnSubscription(namespace);
        Assert.assertTrue(permissionOnSubscription.isEmpty());

        // test tenant manager
        tenantManagerAdmin.namespaces().grantPermissionOnSubscription(namespace, subscription, Set.of(role));
        permissionOnSubscription = tenantManagerAdmin.namespaces().getPermissionOnSubscription(namespace);
        Assert.assertEquals(permissionOnSubscription.get(subscription), Set.of(role));
        tenantManagerAdmin.namespaces().revokePermissionOnSubscription(namespace, subscription, role);
        permissionOnSubscription = tenantManagerAdmin.namespaces().getPermissionOnSubscription(namespace);
        Assert.assertTrue(permissionOnSubscription.isEmpty());

        // test nobody
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.GRANT_PERMISSION);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().grantPermissionOnSubscription(namespace, subscription, Set.of(role)));
        Assert.assertTrue(execFlag.get());
        execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.GET_PERMISSION);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getPermissionOnSubscription(namespace));
        Assert.assertTrue(execFlag.get());
        execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.REVOKE_PERMISSION);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().revokePermissionOnSubscription(namespace, subscription, role));
        Assert.assertTrue(execFlag.get());

        clearAuthorizationOperationChecker();

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().grantPermissionOnSubscription(namespace, subscription, Set.of(role)));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().getPermissionOnSubscription(namespace));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.namespaces().revokePermissionOnSubscription(namespace, subscription, role));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testClearBacklog() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test super admin
        superUserAdmin.namespaces().clearNamespaceBacklog(namespace);

        // test tenant manager
        tenantManagerAdmin.namespaces().clearNamespaceBacklog(namespace);

        // test nobody
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.CLEAR_BACKLOG);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklog(namespace));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.namespaces().clearNamespaceBacklog(namespace);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().clearNamespaceBacklog(namespace));
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testClearNamespaceBundleBacklog() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        @Cleanup
        Producer<byte[]> batchProducer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();

        final String defaultBundle = "0x00000000_0xffffffff";

        // test super admin
        superUserAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);

        // test tenant manager
        tenantManagerAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);

        // test nobody
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.CLEAR_BACKLOG);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle));
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testUnsubscribeNamespace() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        @Cleanup
        Producer<byte[]> batchProducer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();

        pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub")
                .subscribe().close();

        // test super admin
        superUserAdmin.namespaces().unsubscribeNamespace(namespace, "sub");

        // test tenant manager
        tenantManagerAdmin.namespaces().unsubscribeNamespace(namespace, "sub");

        // test nobody
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.UNSUBSCRIBE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespace(namespace, "sub"));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.namespaces().unsubscribeNamespace(namespace, "sub");
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().unsubscribeNamespace(namespace, "sub"));
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testUnsubscribeNamespaceBundle() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random ;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        @Cleanup
        Producer<byte[]> batchProducer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();

        pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub")
                .subscribe().close();

        final String defaultBundle = "0x00000000_0xffffffff";

        // test super admin
        superUserAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, "sub");

        // test tenant manager
        tenantManagerAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, "sub");

        // test nobody
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.UNSUBSCRIBE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, "sub"));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, "sub");
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, "sub"));
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }

        superUserAdmin.topics().delete(topic, true);
    }

    @Test
    public void testPackageAPI() throws Exception {
        final String namespace = "public/default";

        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();


        File file = File.createTempFile("package-api-test", ".package");

        // testing upload api
        String packageName = "function://public/default/test@v1";
        PackageMetadata originalMetadata = PackageMetadata.builder().description("test").build();
        superUserAdmin.packages().upload(originalMetadata, packageName, file.getPath());

        // testing download api
        String downloadPath = new File(file.getParentFile(), "package-api-test-download.package").getPath();
        superUserAdmin.packages().download(packageName, downloadPath);
        File downloadFile = new File(downloadPath);
        assertTrue(downloadFile.exists());
        downloadFile.delete();

        // testing list packages api
        List<String> packages = superUserAdmin.packages().listPackages("function", "public/default");
        assertEquals(packages.size(), 1);
        assertEquals(packages.get(0), "test");

        // testing list versions api
        List<String> versions = superUserAdmin.packages().listPackageVersions(packageName);
        assertEquals(versions.size(), 1);
        assertEquals(versions.get(0), "v1");

        // testing get packages api
        PackageMetadata metadata = superUserAdmin.packages().getMetadata(packageName);
        assertEquals(metadata.getDescription(), originalMetadata.getDescription());
        assertNull(metadata.getContact());
        assertTrue(metadata.getModificationTime() > 0);
        assertTrue(metadata.getCreateTime() > 0);
        assertNull(metadata.getProperties());

        // testing update package metadata api
        PackageMetadata updatedMetadata = originalMetadata;
        updatedMetadata.setContact("test@apache.org");
        updatedMetadata.setProperties(Collections.singletonMap("key", "value"));
        superUserAdmin.packages().updateMetadata(packageName, updatedMetadata);

        superUserAdmin.packages().getMetadata(packageName);

        // ---- test tenant manager ---

        file = File.createTempFile("package-api-test", ".package");

        // test tenant manager
        packageName = "function://public/default/test@v2";
        originalMetadata = PackageMetadata.builder().description("test").build();
        tenantManagerAdmin.packages().upload(originalMetadata, packageName, file.getPath());

        // testing download api
        downloadPath = new File(file.getParentFile(), "package-api-test-download.package").getPath();
        tenantManagerAdmin.packages().download(packageName, downloadPath);
        downloadFile = new File(downloadPath);
        assertTrue(downloadFile.exists());
        downloadFile.delete();

        // testing list packages api
        packages = tenantManagerAdmin.packages().listPackages("function", "public/default");
        assertEquals(packages.size(), 1);
        assertEquals(packages.get(0), "test");

        // testing list versions api
        tenantManagerAdmin.packages().listPackageVersions(packageName);

        // testing get packages api
        tenantManagerAdmin.packages().getMetadata(packageName);

        // testing update package metadata api
        updatedMetadata = originalMetadata;
        updatedMetadata.setContact("test@apache.org");
        updatedMetadata.setProperties(Collections.singletonMap("key", "value"));
        tenantManagerAdmin.packages().updateMetadata(packageName, updatedMetadata);

        // ---- test nobody ---
        AtomicBoolean execFlag = setAuthorizationOperationChecker(subject, NamespaceOperation.PACKAGES);

        File file3 = File.createTempFile("package-api-test", ".package");

        // test tenant manager
        String packageName3 = "function://public/default/test@v3";
        PackageMetadata originalMetadata3 = PackageMetadata.builder().description("test").build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.packages().upload(originalMetadata3, packageName3, file3.getPath()));


        // testing download api
        String downloadPath3 = new File(file3.getParentFile(), "package-api-test-download.package").getPath();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.packages().download(packageName3, downloadPath3));

        // testing list packages api
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.packages().listPackages("function", "public/default"));

        // testing list versions api
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.packages().listPackageVersions(packageName3));

        // testing get packages api
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.packages().getMetadata(packageName3));

        // testing update package metadata api
        PackageMetadata updatedMetadata3 = originalMetadata;
        updatedMetadata3.setContact("test@apache.org");
        updatedMetadata3.setProperties(Collections.singletonMap("key", "value"));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.packages().updateMetadata(packageName3, updatedMetadata3));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            File file4 = File.createTempFile("package-api-test", ".package");
            String packageName4 = "function://public/default/test@v4";
            PackageMetadata originalMetadata4 = PackageMetadata.builder().description("test").build();
            String downloadPath4 = new File(file3.getParentFile(), "package-api-test-download.package").getPath();
            if (AuthAction.packages == action) {
                subAdmin.packages().upload(originalMetadata4, packageName4, file.getPath());

                // testing download api
                subAdmin.packages().download(packageName4, downloadPath4);
                downloadFile = new File(downloadPath4);
                assertTrue(downloadFile.exists());
                downloadFile.delete();

                // testing list packages api
                packages = subAdmin.packages().listPackages("function", "public/default");
                assertEquals(packages.size(), 1);
                assertEquals(packages.get(0), "test");

                // testing list versions api
                subAdmin.packages().listPackageVersions(packageName4);

                // testing get packages api
                subAdmin.packages().getMetadata(packageName4);

                // testing update package metadata api
                PackageMetadata updatedMetadata4 = originalMetadata;
                updatedMetadata4.setContact("test@apache.org");
                updatedMetadata4.setProperties(Collections.singletonMap("key", "value"));
                subAdmin.packages().updateMetadata(packageName, updatedMetadata4);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.packages().upload(originalMetadata4, packageName4, file4.getPath()));

                // testing download api
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.packages().download(packageName4, downloadPath4));

                // testing list packages api
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.packages().listPackages("function", "public/default"));

                // testing list versions api
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.packages().listPackageVersions(packageName4));

                // testing get packages api
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.packages().getMetadata(packageName4));

                // testing update package metadata api
                PackageMetadata updatedMetadata4 = originalMetadata;
                updatedMetadata4.setContact("test@apache.org");
                updatedMetadata4.setProperties(Collections.singletonMap("key", "value"));
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.packages().updateMetadata(packageName4, updatedMetadata4));
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace(namespace, subject);
        }
    }

    @Test
    @SneakyThrows
    public void testDispatchRate() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getDispatchRate(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        DispatchRate dispatchRate =
                DispatchRate.builder().dispatchThrottlingRateInByte(10).dispatchThrottlingRateInMsg(10).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setDispatchRate(namespace, dispatchRate));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeDispatchRate(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testSubscribeRate() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSubscribeRate(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSubscribeRate(namespace, new SubscribeRate()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeSubscribeRate(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testPublishRate() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getPublishRate(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setPublishRate(namespace, new PublishRate(10, 10)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removePublishRate(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testSubscriptionDispatchRate() {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSubscriptionDispatchRate(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        DispatchRate dispatchRate = DispatchRate.builder().dispatchThrottlingRateInMsg(10).dispatchThrottlingRateInByte(10).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeSubscriptionDispatchRate(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testCompactionThreshold() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.COMPACTION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getCompactionThreshold(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.COMPACTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setCompactionThreshold(namespace, 100L * 1024L *1024L));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.COMPACTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeCompactionThreshold(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testAutoTopicCreation() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_TOPIC_CREATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getAutoTopicCreation(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_TOPIC_CREATION, PolicyOperation.WRITE);
        AutoTopicCreationOverride build = AutoTopicCreationOverride.builder().allowAutoTopicCreation(true).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setAutoTopicCreation(namespace, build));
        Assert.assertTrue(execFlag.get());

        execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_TOPIC_CREATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeAutoTopicCreation(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testAutoSubscriptionCreation() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_SUBSCRIPTION_CREATION,
                PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getAutoSubscriptionCreation(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_SUBSCRIPTION_CREATION,
                PolicyOperation.WRITE);
        AutoSubscriptionCreationOverride build =
                AutoSubscriptionCreationOverride.builder().allowAutoSubscriptionCreation(true).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setAutoSubscriptionCreation(namespace, build));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_SUBSCRIPTION_CREATION,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeAutoSubscriptionCreation(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testMaxUnackedMessagesPerConsumer() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED,
                PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED,
                PolicyOperation.WRITE);
        AutoSubscriptionCreationOverride build =
                AutoSubscriptionCreationOverride.builder().allowAutoSubscriptionCreation(true).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 100));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeMaxUnackedMessagesPerConsumer(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testMaxUnackedMessagesPerSubscription() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED,
                PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxUnackedMessagesPerSubscription(namespace, 100));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeMaxUnackedMessagesPerSubscription(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testNamespaceResourceGroup() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RESOURCEGROUP,
                PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceResourceGroup(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RESOURCEGROUP,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceResourceGroup(namespace, "test-group"));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RESOURCEGROUP,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeNamespaceResourceGroup(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testDispatcherPauseOnAckStatePersistent() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.DISPATCHER_PAUSE_ON_ACK_STATE_PERSISTENT,
                        PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getDispatcherPauseOnAckStatePersistent(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DISPATCHER_PAUSE_ON_ACK_STATE_PERSISTENT,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setDispatcherPauseOnAckStatePersistent(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DISPATCHER_PAUSE_ON_ACK_STATE_PERSISTENT,
                PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeDispatcherPauseOnAckStatePersistent(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testBacklogQuota() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.BACKLOG, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getBacklogQuotaMap(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.BACKLOG, PolicyOperation.WRITE);
        BacklogQuota backlogQuota = BacklogQuota.builder().limitTime(10).limitSize(10).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setBacklogQuota(namespace, backlogQuota));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.BACKLOG, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeBacklogQuota(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testDeduplicationSnapshotInterval() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getDeduplicationSnapshotInterval(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setDeduplicationSnapshotInterval(namespace, 100));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeDeduplicationSnapshotInterval(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testMaxSubscriptionsPerTopic() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxSubscriptionsPerTopic(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxSubscriptionsPerTopic(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeMaxSubscriptionsPerTopic(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testMaxProducersPerTopic() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_PRODUCERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxProducersPerTopic(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxProducersPerTopic(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeMaxProducersPerTopic(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testMaxConsumersPerTopic() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxConsumersPerTopic(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxConsumersPerTopic(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeMaxConsumersPerTopic(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testNamespaceReplicationClusters() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceReplicationClusters(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test")));
        Assert.assertTrue(execFlag.get());
    }

        @Test
    @SneakyThrows
    public void testReplicatorDispatchRate() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION_RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getReplicatorDispatchRate(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE);
        DispatchRate build =
                    DispatchRate.builder().dispatchThrottlingRateInByte(10).dispatchThrottlingRateInMsg(10).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setReplicatorDispatchRate(namespace, build));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeReplicatorDispatchRate(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testMaxConsumersPerSubscription() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxConsumersPerSubscription(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxConsumersPerSubscription(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeMaxConsumersPerSubscription(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testOffloadThreshold() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getOffloadThreshold(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setOffloadThreshold(namespace, 10));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testOffloadPolicies() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getOffloadPolicies(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        OffloadPolicies offloadPolicies = OffloadPolicies.builder().managedLedgerOffloadThresholdInBytes(10L).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setOffloadPolicies(namespace, offloadPolicies));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeOffloadPolicies(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testMaxTopicsPerNamespace() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_TOPICS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxTopicsPerNamespace(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_TOPICS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxTopicsPerNamespace(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_TOPICS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeMaxTopicsPerNamespace(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testDeduplicationStatus() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getDeduplicationStatus(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setDeduplicationStatus(namespace, true));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeDeduplicationStatus(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testPersistence() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.PERSISTENCE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getPersistence(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.PERSISTENCE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setPersistence(namespace, new PersistencePolicies(10, 10, 10, 10)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.PERSISTENCE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removePersistence(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testNamespaceMessageTTL() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.TTL, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceMessageTTL(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.TTL, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceMessageTTL(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.TTL, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeNamespaceMessageTTL(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testSubscriptionExpirationTime() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_EXPIRATION_TIME,
                        PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSubscriptionExpirationTime(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_EXPIRATION_TIME, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSubscriptionExpirationTime(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_EXPIRATION_TIME, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeSubscriptionExpirationTime(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testDelayedDeliveryMessages() {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.DELAYED_DELIVERY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getDelayedDelivery(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testRetention() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RETENTION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getRetention(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RETENTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setRetention(namespace, new RetentionPolicies(10, 10)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RETENTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeRetention(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testInactiveTopicPolicies() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.INACTIVE_TOPIC, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getInactiveTopicPolicies(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE);
        InactiveTopicPolicies inactiveTopicPolicies = new InactiveTopicPolicies(
                InactiveTopicDeleteMode.delete_when_no_subscriptions, 10, false);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setInactiveTopicPolicies(namespace, inactiveTopicPolicies));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeInactiveTopicPolicies(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testNamespaceAntiAffinityGroup() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ANTI_AFFINITY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceAntiAffinityGroup(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ANTI_AFFINITY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceAntiAffinityGroup(namespace, "invalid-group"));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testOffloadDeleteLagMs() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getOffloadDeleteLagMs(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setOffloadDeleteLag(namespace, 100, TimeUnit.HOURS));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testOffloadThresholdInSeconds() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag =
                setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getOffloadThresholdInSeconds(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setOffloadThresholdInSeconds(namespace, 10000));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testNamespaceEntryFilters() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ENTRY_FILTERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceEntryFilters(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceEntryFilters(namespace, new EntryFilters("filter1")));
        Assert.assertTrue(execFlag.get());

                execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeNamespaceEntryFilters(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testEncryptionRequiredStatus() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ENCRYPTION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getEncryptionRequiredStatus(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ENCRYPTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setEncryptionRequiredStatus(namespace, false));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testSubscriptionTypesEnabled() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_AUTH_MODE,
                PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSubscriptionTypesEnabled(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSubscriptionTypesEnabled(namespace, Sets.newHashSet(SubscriptionType.Failover)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().removeSubscriptionTypesEnabled(namespace));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testIsAllowAutoUpdateSchema() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getIsAllowAutoUpdateSchema(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setIsAllowAutoUpdateSchema(namespace, true));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testSchemaAutoUpdateCompatibilityStrategy() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace, AutoUpdateDisabled));
        Assert.assertTrue(execFlag.get());
    }

    @Test
    @SneakyThrows
    public void testSchemaValidationEnforced() {
        final String namespace = "public/default";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSchemaValidationEnforced(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSchemaValidationEnforced(namespace, true));
        Assert.assertTrue(execFlag.get());
    }
}
