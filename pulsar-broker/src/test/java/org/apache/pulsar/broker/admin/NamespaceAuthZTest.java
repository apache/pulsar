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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
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
import org.apache.pulsar.common.policies.data.ClusterData;
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
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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

    private volatile Consumer<InvocationOnMock> allowNamespacePolicyOperationAsyncHandler;
    private volatile Consumer<InvocationOnMock> allowNamespaceOperationAsyncHandler;
    // when set, replaces the result of tenant operation checks
    private volatile Supplier<CompletableFuture<Boolean>> tenantOperationResult;

    @SneakyThrows
    @BeforeClass
    public void setup() {
        getServiceConfiguration().setEnablePackagesManagement(true);
        getServiceConfiguration().setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        getServiceConfiguration().setDefaultNumberOfNamespaceBundles(1);
        getServiceConfiguration().setForceDeleteNamespaceAllowed(true);
        getServiceConfiguration().setEnableShadowTopics(true);
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
        this.authorizationService = BrokerTestUtil.spyWithoutRecordingInvocations(
                getPulsarService().getBrokerService().getAuthorizationService());
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                authorizationService, true);
        Mockito.doAnswer(invocationOnMock -> {
            Consumer<InvocationOnMock> localAllowNamespacePolicyOperationAsyncHandler =
                    allowNamespacePolicyOperationAsyncHandler;
            if (localAllowNamespacePolicyOperationAsyncHandler != null) {
                localAllowNamespacePolicyOperationAsyncHandler.accept(invocationOnMock);
            }
            return invocationOnMock.callRealMethod();
        }).when(authorizationService).allowNamespacePolicyOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocationOnMock -> {
            Consumer<InvocationOnMock> localAllowNamespaceOperationAsyncHandler =
                    allowNamespaceOperationAsyncHandler;
            if (localAllowNamespaceOperationAsyncHandler != null) {
                localAllowNamespaceOperationAsyncHandler.accept(invocationOnMock);
            }
            return invocationOnMock.callRealMethod();
        }).when(authorizationService).allowNamespaceOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any());
        Mockito.doAnswer(invocationOnMock -> {
            Supplier<CompletableFuture<Boolean>> localTenantOperationResult = tenantOperationResult;
            return localTenantOperationResult != null
                    ? localTenantOperationResult.get() : invocationOnMock.callRealMethod();
        })
                .when(authorizationService).allowTenantOperationAsync(Mockito.any(), Mockito.any(),
                        Mockito.any(), Mockito.any());
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
        tenantOperationResult = null;
        deleteNamespaceWithRetry("public/default", true, superUserAdmin);
        superUserAdmin.namespaces().createNamespace("public/default");
        allowNamespacePolicyOperationAsyncHandler = null;
        allowNamespaceOperationAsyncHandler = null;
    }

    private AtomicBoolean setAuthorizationOperationChecker(String role, NamespaceOperation operation) {
        AtomicBoolean execFlag = new AtomicBoolean(false);
        allowNamespaceOperationAsyncHandler = invocationOnMock -> {
            String role1 = invocationOnMock.getArgument(2);
            if (role.equals(role1)) {
                NamespaceOperation operation1 = invocationOnMock.getArgument(1);
                Assert.assertEquals(operation1, operation);
            }
            execFlag.set(true);
        };
        return execFlag;
    }

    private void clearAuthorizationOperationChecker() {
        allowNamespaceOperationAsyncHandler = null;
    }

    private AtomicBoolean setAuthorizationPolicyOperationChecker(String role, Object policyName, Object operation) {
        AtomicBoolean execFlag = new AtomicBoolean(false);
        if (operation instanceof PolicyOperation) {
            allowNamespacePolicyOperationAsyncHandler = invocationOnMock -> {
                String role1 = invocationOnMock.getArgument(3);
                if (role.equals(role1)) {
                    PolicyName policyName1 = invocationOnMock.getArgument(1);
                    PolicyOperation operation1 = invocationOnMock.getArgument(2);
                    assertEquals(operation1, operation);
                    assertEquals(policyName1, policyName);
                }
                execFlag.set(true);
            };
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
    public void testNamespaceSubscriptionOperationsApplySubscriptionPolicies() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String otherSub = "other-sub";
        final String ownSub = subject + "-sub";
        final String ownSub2 = subject + "-sub2";
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        for (String sub : List.of(otherSub, ownSub, ownSub2)) {
            superUserAdmin.topics().createSubscription(topic, sub, MessageId.earliest);
        }
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes());
        }

        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);

        // subscriptions that do not match the role prefix are rejected
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                        otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespace(namespace, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, otherSub));
        assertEquals(getMsgBacklog(topic, otherSub), numMessages);

        // subscriptions that match the role prefix are allowed on the namespace and on the bundle
        subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, ownSub);
        assertEquals(getMsgBacklog(topic, ownSub), 0);
        subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle, ownSub2);
        assertEquals(getMsgBacklog(topic, ownSub2), 0);
        subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, ownSub2);
        assertNull(superUserAdmin.topics().getStats(topic).getSubscriptions().get(ownSub2));
        subAdmin.namespaces().unsubscribeNamespace(namespace, ownSub);
        assertNull(superUserAdmin.topics().getStats(topic).getSubscriptions().get(ownSub));

        // subscription roles are applied
        superUserAdmin.topics().createSubscription(topic, ownSub, MessageId.earliest);
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.None);
        superUserAdmin.namespaces().grantPermissionOnSubscription(namespace, otherSub, Set.of("other-role"));
        superUserAdmin.namespaces().grantPermissionOnSubscription(namespace, ownSub, Set.of(subject));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                        otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespace(namespace, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, otherSub));
        assertEquals(getMsgBacklog(topic, otherSub), numMessages);

        // roles listed for the subscription are allowed
        subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle, ownSub);
        assertEquals(getMsgBacklog(topic, ownSub), 0);
        subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, ownSub);
        assertNull(superUserAdmin.topics().getStats(topic).getSubscriptions().get(ownSub));

        // replicator cursor names require tenant admin permission
        final String replicatorCursor = getPulsarService().getConfiguration().getReplicatorPrefix() + ".remote";
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, replicatorCursor));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                        replicatorCursor));

        // super users and tenant admins are not affected by subscription policies
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);
        superUserAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle, otherSub);
        assertEquals(getMsgBacklog(topic, otherSub), 0);
        tenantManagerAdmin.namespaces().unsubscribeNamespace(namespace, otherSub);
        assertNull(superUserAdmin.topics().getStats(topic).getSubscriptions().get(otherSub));

        producer.close();
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    @Test
    public void testNamespaceClearBacklogForSubscriptionWithReplicator() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String localCluster = getPulsarService().getConfiguration().getClusterName();
        final String remoteCluster = "remote-" + random;
        final String replicatorCursor =
                getPulsarService().getConfiguration().getReplicatorPrefix() + "." + remoteCluster;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;

        // the remote cluster is not reachable, so the replicator keeps its backlog
        superUserAdmin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl("http://127.0.0.1:1")
                .brokerServiceUrl("pulsar://127.0.0.1:1")
                .build());
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(TENANT_ADMIN_SUBJECT))
                .allowedClusters(Set.of(localCluster, remoteCluster))
                .build());
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        // an ordinary subscription with the same name as the remote cluster
        superUserAdmin.topics().createSubscription(topic, remoteCluster, MessageId.earliest);
        superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster, remoteCluster),
                false);
        Awaitility.await().untilAsserted(() -> assertNotNull(
                superUserAdmin.topics().getStats(topic).getReplication().get(remoteCluster)));
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                numMessages));
        assertEquals(getMsgBacklog(topic, remoteCluster), numMessages);

        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));

        // the prefixed replicator cursor name is rejected for ordinary roles
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, replicatorCursor));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                        replicatorCursor));
        // on the namespace, a plain name clears the subscription with that name for ordinary roles
        subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, remoteCluster);
        assertEquals(getMsgBacklog(topic, remoteCluster), 0);
        assertEquals(getReplicationBacklog(topic, remoteCluster), numMessages);

        // on the bundle, a plain name clears the subscription with that name for ordinary roles
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                2 * numMessages));
        assertEquals(getMsgBacklog(topic, remoteCluster), numMessages);
        subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle, remoteCluster);
        assertEquals(getMsgBacklog(topic, remoteCluster), 0);
        assertEquals(getReplicationBacklog(topic, remoteCluster), 2 * numMessages);

        // without such a subscription, a plain name that resolves to the replicator is rejected for ordinary roles
        superUserAdmin.topics().deleteSubscription(topic, remoteCluster);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, remoteCluster));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                        remoteCluster));
        assertEquals(getReplicationBacklog(topic, remoteCluster), 2 * numMessages);

        // tenant admins keep the existing behaviour: a plain name clears the subscription with that name first
        superUserAdmin.topics().createSubscription(topic, remoteCluster, MessageId.earliest);
        assertEquals(getMsgBacklog(topic, remoteCluster), 2 * numMessages);
        tenantManagerAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                remoteCluster);
        assertEquals(getMsgBacklog(topic, remoteCluster), 0);
        assertEquals(getReplicationBacklog(topic, remoteCluster), 2 * numMessages);

        // without such a subscription, a plain cluster name clears the replicator backlog on the bundle ...
        superUserAdmin.topics().deleteSubscription(topic, remoteCluster);
        tenantManagerAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                remoteCluster);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));

        // ... and on the namespace
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                numMessages));
        tenantManagerAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, remoteCluster);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));

        // the prefixed replicator cursor name also works for tenant admins
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                numMessages));
        tenantManagerAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, replicatorCursor);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));

        producer.close();
        superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster), false);
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
        superUserAdmin.tenants().deleteTenant(tenant);
        superUserAdmin.clusters().deleteCluster(remoteCluster);
    }

    @Test
    public void testNamespaceClearBacklogForSubscriptionWithTopicLevelReplication() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String localCluster = getPulsarService().getConfiguration().getClusterName();
        final String remoteCluster = "remote-" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;

        // the remote cluster is not reachable, so the replicator keeps its backlog
        superUserAdmin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl("http://127.0.0.1:1")
                .brokerServiceUrl("pulsar://127.0.0.1:1")
                .build());
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(TENANT_ADMIN_SUBJECT))
                .allowedClusters(Set.of(localCluster, remoteCluster))
                .build());
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topicPolicies().setReplicationClusters(topic, List.of(localCluster, remoteCluster));
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        Awaitility.await().untilAsserted(() -> assertNotNull(
                superUserAdmin.topics().getStats(topic).getReplication().get(remoteCluster)));
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                numMessages));

        // the remote cluster is no longer allowed for the tenant, the topic level replication is still configured
        superUserAdmin.tenants().updateTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(TENANT_ADMIN_SUBJECT))
                .allowedClusters(Set.of(localCluster))
                .build());
        assertNotNull(superUserAdmin.topics().getStats(topic).getReplication().get(remoteCluster));

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));

        // the plain cluster name resolves to the replicator and is rejected for ordinary roles
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, remoteCluster));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                        remoteCluster));
        assertEquals(getReplicationBacklog(topic, remoteCluster), numMessages);

        // tenant admins keep the existing behaviour
        tenantManagerAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, remoteCluster);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));

        producer.close();
        superUserAdmin.topicPolicies().removeReplicationClusters(topic);
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
        superUserAdmin.tenants().deleteTenant(tenant);
        superUserAdmin.clusters().deleteCluster(remoteCluster);
    }

    @Test
    public void testNamespaceClearBacklogForSubscriptionWithShadowTopic() throws Exception {
        verifyNamespaceClearBacklogForSubscriptionWithShadowTopic(false);
    }

    @Test
    public void testNamespaceClearBacklogForSubscriptionWithShortShadowTopicName() throws Exception {
        verifyNamespaceClearBacklogForSubscriptionWithShadowTopic(true);
    }

    private void verifyNamespaceClearBacklogForSubscriptionWithShadowTopic(boolean shortName) throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/" + random;
        final String shadowNamespace = "public/" + random + "-shadow";
        final String topic = "persistent://" + namespace + "/" + random;
        final String shadowTopicName = shadowNamespace + "/" + random;
        // shadow topics can be configured with the full or the short topic name, the configured name is the key of
        // the shadow replicator
        final String shadowTopic = shortName ? shadowTopicName : "persistent://" + shadowTopicName;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String defaultBundle = "0x00000000_0xffffffff";
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.namespaces().createNamespace(shadowNamespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createShadowTopic("persistent://" + shadowTopicName, topic);
        superUserAdmin.topics().setShadowTopics(topic, List.of(shadowTopic));
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        sendMessages(producer, 1);
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarService().getBrokerService()
                .getTopicIfExists(topic).get().orElseThrow();
        Awaitility.await().untilAsserted(() ->
                assertTrue(persistentTopic.getShadowReplicators().containsKey(shadowTopic)));
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));

        // the shadow replicator cursor is rejected for ordinary roles
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, shadowTopic));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                        shadowTopic));

        // tenant admins keep the existing behaviour
        tenantManagerAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle,
                shadowTopic);
        tenantManagerAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, shadowTopic);

        producer.close();
        superUserAdmin.topics().removeShadowTopics(topic);
        deleteNamespaceWithRetry(shadowNamespace, true, superUserAdmin);
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    @Test
    public void testNamespaceClearBacklogForSubscriptionNamedAfterCluster() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        // a remote cluster which is allowed for the tenant, replication is not configured
        final String otherCluster = "other-" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;
        superUserAdmin.clusters().createCluster(otherCluster, ClusterData.builder()
                .serviceUrl("http://127.0.0.1:1")
                .brokerServiceUrl("pulsar://127.0.0.1:1")
                .build());
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(TENANT_ADMIN_SUBJECT))
                .allowedClusters(Set.of(getPulsarService().getConfiguration().getClusterName(), otherCluster))
                .build());
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, otherCluster, MessageId.earliest);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));

        sendMessages(producer, numMessages);
        assertEquals(getMsgBacklog(topic, otherCluster), numMessages);
        subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, otherCluster);
        assertEquals(getMsgBacklog(topic, otherCluster), 0);

        sendMessages(producer, numMessages);
        assertEquals(getMsgBacklog(topic, otherCluster), numMessages);
        subAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, defaultBundle, otherCluster);
        assertEquals(getMsgBacklog(topic, otherCluster), 0);

        producer.close();
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
        superUserAdmin.tenants().deleteTenant(tenant);
        superUserAdmin.clusters().deleteCluster(otherCluster);
    }

    @Test
    public void testClearNamespaceBacklogAppliesSubscriptionPolicies() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/" + random;
        final String partitionedTopic = "persistent://" + namespace + "/" + random + "-partitioned";
        final List<String> topics = List.of("persistent://" + namespace + "/" + random + "-1",
                "persistent://" + namespace + "/" + random + "-2",
                partitionedTopic + "-partition-0", partitionedTopic + "-partition-1");
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String otherSub = "other-sub";
        final String ownSub = subject + "-sub";
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topics.get(0));
        superUserAdmin.topics().createNonPartitionedTopic(topics.get(1));
        superUserAdmin.topics().createPartitionedTopic(partitionedTopic, 2);
        final List<Producer<byte[]>> producers = new ArrayList<>();
        for (String topic : topics) {
            superUserAdmin.topics().createSubscription(topic, otherSub, MessageId.earliest);
            superUserAdmin.topics().createSubscription(topic, ownSub, MessageId.earliest);
            producers.add(pulsarClient.newProducer().topic(topic).enableBatching(false).create());
        }
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);

        // only the subscriptions that match the role prefix are cleared, on the namespace and on the bundle
        sendMessages(producers, numMessages);
        subAdmin.namespaces().clearNamespaceBacklog(namespace);
        assertBacklogs(topics, ownSub, 0, otherSub, numMessages);
        sendMessages(producers, numMessages);
        subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
        assertBacklogs(topics, ownSub, 0, otherSub, 2 * numMessages);

        // subscription roles are applied
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.None);
        superUserAdmin.namespaces().grantPermissionOnSubscription(namespace, otherSub, Set.of("other-role"));
        sendMessages(producers, numMessages);
        subAdmin.namespaces().clearNamespaceBacklog(namespace);
        assertBacklogs(topics, ownSub, 0, otherSub, 3 * numMessages);
        sendMessages(producers, numMessages);
        subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
        assertBacklogs(topics, ownSub, 0, otherSub, 4 * numMessages);

        // without subscription policies, all subscriptions are cleared
        superUserAdmin.namespaces().revokePermissionOnSubscription(namespace, otherSub, "other-role");
        subAdmin.namespaces().clearNamespaceBacklog(namespace);
        assertBacklogs(topics, ownSub, 0, otherSub, 0);
        sendMessages(producers, numMessages);
        subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
        assertBacklogs(topics, ownSub, 0, otherSub, 0);

        // tenant admins are not affected by subscription policies
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);
        sendMessages(producers, numMessages);
        tenantManagerAdmin.namespaces().clearNamespaceBacklog(namespace);
        assertBacklogs(topics, ownSub, 0, otherSub, 0);
        sendMessages(producers, numMessages);
        tenantManagerAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
        assertBacklogs(topics, ownSub, 0, otherSub, 0);

        for (Producer<byte[]> producer : producers) {
            producer.close();
        }
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    @Test
    public void testClearNamespaceBacklogWithReplicator() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String localCluster = getPulsarService().getConfiguration().getClusterName();
        final String remoteCluster = "remote-" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String sub = "sub";
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;

        // the remote cluster is not reachable, so the replicator keeps its backlog
        superUserAdmin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl("http://127.0.0.1:1")
                .brokerServiceUrl("pulsar://127.0.0.1:1")
                .build());
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(TENANT_ADMIN_SUBJECT))
                .allowedClusters(Set.of(localCluster, remoteCluster))
                .build());
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, sub, MessageId.earliest);
        superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster, remoteCluster),
                false);
        Awaitility.await().untilAsserted(() -> assertNotNull(
                superUserAdmin.topics().getStats(topic).getReplication().get(remoteCluster)));
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));

        // ordinary roles clear the subscriptions but not the replicator, on the namespace and on the bundle
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                numMessages));
        subAdmin.namespaces().clearNamespaceBacklog(namespace);
        assertEquals(getMsgBacklog(topic, sub), 0);
        assertEquals(getReplicationBacklog(topic, remoteCluster), numMessages);
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                2 * numMessages));
        subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
        assertEquals(getMsgBacklog(topic, sub), 0);
        assertEquals(getReplicationBacklog(topic, remoteCluster), 2 * numMessages);

        // tenant admins keep clearing the replicator backlog
        tenantManagerAdmin.namespaces().clearNamespaceBacklog(namespace);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));
        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                numMessages));
        tenantManagerAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));
        assertEquals(getMsgBacklog(topic, sub), 0);

        producer.close();
        superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster), false);
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    @Test
    public void testClearNamespaceBacklogKeepsShadowReplicator() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/" + random;
        final String shadowNamespace = "public/" + random + "-shadow";
        final String topic = "persistent://" + namespace + "/" + random;
        final String shadowTopic = "persistent://" + shadowNamespace + "/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String sub = "sub";
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.namespaces().createNamespace(shadowNamespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, sub, MessageId.earliest);
        superUserAdmin.topics().createShadowTopic(shadowTopic, topic);
        superUserAdmin.topics().setShadowTopics(topic, List.of(shadowTopic));
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        sendMessages(producer, 1);
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarService().getBrokerService()
                .getTopicIfExists(topic).get().orElseThrow();
        Awaitility.await().untilAsserted(() ->
                assertTrue(persistentTopic.getShadowReplicators().containsKey(shadowTopic)));
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));
        // records the clear backlog calls on the shadow replicator
        final PersistentReplicator shadowReplicator =
                (PersistentReplicator) persistentTopic.getShadowReplicators().get(shadowTopic);
        final PersistentReplicator shadowReplicatorSpy = Mockito.spy(shadowReplicator);
        persistentTopic.getShadowReplicators().put(shadowTopic, shadowReplicatorSpy);
        try {
            // ordinary roles clear the subscriptions but not the shadow replicator
            sendMessages(producer, numMessages);
            subAdmin.namespaces().clearNamespaceBacklog(namespace);
            assertEquals(getMsgBacklog(topic, sub), 0);
            sendMessages(producer, numMessages);
            subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
            assertEquals(getMsgBacklog(topic, sub), 0);
            Mockito.verify(shadowReplicatorSpy, Mockito.never()).clearBacklog();

            // tenant admins keep clearing the shadow replicator
            tenantManagerAdmin.namespaces().clearNamespaceBacklog(namespace);
            Mockito.verify(shadowReplicatorSpy, Mockito.times(1)).clearBacklog();
            tenantManagerAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
            Mockito.verify(shadowReplicatorSpy, Mockito.times(2)).clearBacklog();
        } finally {
            persistentTopic.getShadowReplicators().replace(shadowTopic, shadowReplicatorSpy, shadowReplicator);
        }

        producer.close();
        superUserAdmin.topics().removeShadowTopics(topic);
        deleteNamespaceWithRetry(shadowNamespace, true, superUserAdmin);
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    @Test
    public void testClearNamespaceBacklogRedirectsToPeerCluster() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String localCluster = getPulsarService().getConfiguration().getClusterName();
        final String peerCluster = "peer-" + random;
        final String peerServiceUrl = "http://127.0.0.1:1";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.clusters().createCluster(peerCluster, ClusterData.builder()
                .serviceUrl(peerServiceUrl)
                .brokerServiceUrl("pulsar://127.0.0.1:1")
                .build());
        superUserAdmin.clusters().updatePeerClusterNames(localCluster, new LinkedHashSet<>(List.of(peerCluster)));
        try {
            superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                    .adminRoles(Set.of(TENANT_ADMIN_SUBJECT))
                    .allowedClusters(Set.of(localCluster, peerCluster))
                    .build());
            // the namespace is served by the peer cluster, so this cluster has no topics of it
            superUserAdmin.namespaces().createNamespace(namespace, Set.of(peerCluster));
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));

            // the request is redirected to the peer cluster instead of clearing the local topics only
            HttpRequest request = HttpRequest.newBuilder(URI.create(getPulsarService().getWebServiceAddress()
                            + "/admin/v2/namespaces/" + namespace + "/clearBacklog"))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<Void> response = HttpClient.newHttpClient().send(request,
                    HttpResponse.BodyHandlers.discarding());
            assertEquals(response.statusCode(), 307);
            assertTrue(response.headers().firstValue("Location").orElseThrow().startsWith(peerServiceUrl));

            superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster), false);
        } finally {
            superUserAdmin.clusters().updatePeerClusterNames(localCluster, null);
        }
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
        superUserAdmin.tenants().deleteTenant(tenant);
        superUserAdmin.clusters().deleteCluster(peerCluster);
    }

    @DataProvider
    public Object[][] tenantOperationResults() {
        return new Object[][]{{"unsupported"}, {"denied"}};
    }

    @Test(dataProvider = "tenantOperationResults")
    public void testClearNamespaceBacklogDoesNotDependOnTenantOperations(String tenantOperationMode)
            throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String localCluster = getPulsarService().getConfiguration().getClusterName();
        final String remoteCluster = "remote-" + random;
        final int numMessages = 5;
        createReplicatedNamespace(tenant, namespace, topic, localCluster, remoteCluster);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        tenantOperationResult = "unsupported".equals(tenantOperationMode)
                ? () -> CompletableFuture.failedFuture(new IllegalStateException("tenant operations not supported"))
                : () -> CompletableFuture.completedFuture(false);

        // super users and tenant admins are recognized with the provider's admin checks
        for (PulsarAdmin admin : List.of(superUserAdmin, tenantManagerAdmin)) {
            sendMessages(producer, numMessages);
            Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                    numMessages));
            admin.namespaces().clearNamespaceBacklogForSubscription(namespace, remoteCluster);
            Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));
            sendMessages(producer, numMessages);
            Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                    numMessages));
            admin.namespaces().clearNamespaceBacklog(namespace);
            Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster), 0));
        }

        tenantOperationResult = null;
        producer.close();
        superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster), false);
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    @Test
    public void testClearNamespaceBacklogIgnoresTenantOperationsGrantedToConsumers() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String localCluster = getPulsarService().getConfiguration().getClusterName();
        final String remoteCluster = "remote-" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String otherSub = "other-sub";
        final String ownSub = subject + "-sub";
        final String defaultBundle = "0x00000000_0xffffffff";
        final int numMessages = 5;
        createReplicatedNamespace(tenant, namespace, topic, localCluster, remoteCluster);
        superUserAdmin.topics().createSubscription(topic, otherSub, MessageId.earliest);
        superUserAdmin.topics().createSubscription(topic, ownSub, MessageId.earliest);
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .create();
        // the provider grants tenant operations to every role
        tenantOperationResult = () -> CompletableFuture.completedFuture(true);

        sendMessages(producer, numMessages);
        Awaitility.await().untilAsserted(() -> assertEquals(getReplicationBacklog(topic, remoteCluster),
                numMessages));
        subAdmin.namespaces().clearNamespaceBacklog(namespace);
        assertEquals(getMsgBacklog(topic, ownSub), 0);
        assertEquals(getMsgBacklog(topic, otherSub), numMessages);
        assertEquals(getReplicationBacklog(topic, remoteCluster), numMessages);
        subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle);
        assertEquals(getMsgBacklog(topic, otherSub), numMessages);
        assertEquals(getReplicationBacklog(topic, remoteCluster), numMessages);
        try {
            subAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, remoteCluster);
        } catch (PulsarAdminException e) {
            // rejected or ignored, the replicator keeps its backlog either way
        }
        assertEquals(getReplicationBacklog(topic, remoteCluster), numMessages);

        tenantOperationResult = null;
        producer.close();
        superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster), false);
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    private void createReplicatedNamespace(String tenant, String namespace, String topic, String localCluster,
                                           String remoteCluster) throws Exception {
        // the remote cluster is not reachable, so the replicator keeps its backlog
        superUserAdmin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl("http://127.0.0.1:1")
                .brokerServiceUrl("pulsar://127.0.0.1:1")
                .build());
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(TENANT_ADMIN_SUBJECT))
                .allowedClusters(Set.of(localCluster, remoteCluster))
                .build());
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.namespaces().setNamespaceReplicationClusters(namespace, Set.of(localCluster, remoteCluster),
                false);
        Awaitility.await().untilAsserted(() -> assertNotNull(
                superUserAdmin.topics().getStats(topic).getReplication().get(remoteCluster)));
    }

    @Test
    public void testExpireMessagesForAllSubscriptionsAppliesSubscriptionPolicies() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/" + random;
        final String prefix = "persistent://" + namespace + "/" + random;
        // each subscription is expired only once, so that no expiry is still running when it is checked
        final String topic = prefix + "-topic";
        final String partitionedTopic = prefix + "-partitioned";
        final String partitionedTopic2 = prefix + "-partitioned-2";
        final String adminTopic = prefix + "-admin";
        final String adminPartitionedTopic = prefix + "-admin-partitioned";
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        final String otherSub = "other-sub";
        final String ownSub = subject + "-sub";
        final int numMessages = 5;
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        final List<String> allTopics = new ArrayList<>();
        for (String t : List.of(topic, adminTopic)) {
            superUserAdmin.topics().createNonPartitionedTopic(t);
            allTopics.add(t);
        }
        for (String t : List.of(partitionedTopic, partitionedTopic2, adminPartitionedTopic)) {
            superUserAdmin.topics().createPartitionedTopic(t, 2);
            allTopics.add(t + "-partition-0");
            allTopics.add(t + "-partition-1");
        }
        final List<Producer<byte[]>> producers = new ArrayList<>();
        for (String t : allTopics) {
            superUserAdmin.topics().createSubscription(t, otherSub, MessageId.earliest);
            superUserAdmin.topics().createSubscription(t, ownSub, MessageId.earliest);
            producers.add(pulsarClient.newProducer().topic(t).enableBatching(false).create());
        }
        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);
        sendMessages(producers, numMessages);
        // the messages must be older than the expiry time
        Thread.sleep(1500);

        // only the subscriptions that match the role prefix are expired, on partitioned topics too
        subAdmin.topics().expireMessagesForAllSubscriptions(topic, 1);
        subAdmin.topics().expireMessagesForAllSubscriptions(partitionedTopic, 1);
        final List<String> expired = List.of(topic, partitionedTopic + "-partition-0",
                partitionedTopic + "-partition-1");
        Awaitility.await().untilAsserted(() -> assertBacklogs(expired, ownSub, 0, otherSub, numMessages));

        // the same applies to a partition
        subAdmin.topics().expireMessagesForAllSubscriptions(partitionedTopic2 + "-partition-0", 1);
        Awaitility.await().untilAsserted(() -> assertBacklogs(List.of(partitionedTopic2 + "-partition-0"),
                ownSub, 0, otherSub, numMessages));
        assertBacklogs(List.of(partitionedTopic2 + "-partition-1"), ownSub, numMessages, otherSub, numMessages);

        // tenant admins are not affected by subscription policies
        tenantManagerAdmin.topics().expireMessagesForAllSubscriptions(adminTopic, 1);
        tenantManagerAdmin.topics().expireMessagesForAllSubscriptions(adminPartitionedTopic, 1);
        Awaitility.await().untilAsserted(() -> assertBacklogs(List.of(adminTopic,
                adminPartitionedTopic + "-partition-0", adminPartitionedTopic + "-partition-1"),
                ownSub, 0, otherSub, 0));

        for (Producer<byte[]> producer : producers) {
            producer.close();
        }
        deleteNamespaceWithRetry(namespace, true, superUserAdmin);
    }

    private void assertBacklogs(List<String> topics, String sub1, long backlog1, String sub2, long backlog2)
            throws PulsarAdminException {
        for (String topic : topics) {
            assertEquals(getMsgBacklog(topic, sub1), backlog1, topic + " " + sub1);
            assertEquals(getMsgBacklog(topic, sub2), backlog2, topic + " " + sub2);
        }
    }

    private static void sendMessages(List<Producer<byte[]>> producers, int numMessages) throws Exception {
        for (Producer<byte[]> producer : producers) {
            sendMessages(producer, numMessages);
        }
    }

    private static void sendMessages(Producer<byte[]> producer, int numMessages) throws Exception {
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes());
        }
    }

    private long getMsgBacklog(String topic, String subscription) throws PulsarAdminException {
        return superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog();
    }

    private long getReplicationBacklog(String topic, String remoteCluster) throws PulsarAdminException {
        return superUserAdmin.topics().getStats(topic).getReplication().get(remoteCluster).getReplicationBacklog();
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

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.RATE, PolicyOperation.WRITE);
        DispatchRate dispatchRate = DispatchRate.builder().dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(10).build();
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
                () -> subAdmin.namespaces().setCompactionThreshold(namespace, 100L * 1024L * 1024L));
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.BACKLOG, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getBacklogQuotaMap(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.BACKLOG, PolicyOperation.WRITE);
        BacklogQuota backlogQuota = BacklogQuota.builder().limitTime(10).limitSize(10).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setBacklogQuota(namespace, backlogQuota));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.BACKLOG,
                PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getDeduplicationSnapshotInterval(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setDeduplicationSnapshotInterval(namespace, 100));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxSubscriptionsPerTopic(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxSubscriptionsPerTopic(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.MAX_PRODUCERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxProducersPerTopic(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxProducersPerTopic(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.MAX_CONSUMERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxConsumersPerTopic(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxConsumersPerTopic(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.REPLICATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceReplicationClusters(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.REPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceReplicationClusters(namespace,
                        Sets.newHashSet("test"), false));
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.REPLICATION_RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getReplicatorDispatchRate(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.REPLICATION_RATE, PolicyOperation.WRITE);
        DispatchRate build =
                    DispatchRate.builder().dispatchThrottlingRateInByte(10)
                            .dispatchThrottlingRateInMsg(10).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setReplicatorDispatchRate(namespace, build));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.REPLICATION_RATE, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.MAX_CONSUMERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxConsumersPerSubscription(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxConsumersPerSubscription(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getOffloadThreshold(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getOffloadPolicies(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.WRITE);
        OffloadPolicies offloadPolicies = OffloadPolicies.builder()
                .managedLedgerOffloadThresholdInBytes(10L).build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setOffloadPolicies(namespace, offloadPolicies));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_TOPICS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getMaxTopicsPerNamespace(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_TOPICS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setMaxTopicsPerNamespace(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.MAX_TOPICS, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.DEDUPLICATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getDeduplicationStatus(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.DEDUPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setDeduplicationStatus(namespace, true));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.DEDUPLICATION, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.PERSISTENCE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getPersistence(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.PERSISTENCE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setPersistence(namespace,
                        new PersistencePolicies(10, 10, 10, 10)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.PERSISTENCE, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.TTL, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceMessageTTL(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.TTL, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceMessageTTL(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.TTL, PolicyOperation.WRITE);
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

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SUBSCRIPTION_EXPIRATION_TIME, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSubscriptionExpirationTime(namespace, 10));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SUBSCRIPTION_EXPIRATION_TIME, PolicyOperation.WRITE);
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
                setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.DELAYED_DELIVERY, PolicyOperation.READ);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.RETENTION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getRetention(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.RETENTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setRetention(namespace,
                        new RetentionPolicies(10, 10)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.RETENTION, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.INACTIVE_TOPIC, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getInactiveTopicPolicies(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE);
        InactiveTopicPolicies inactiveTopicPolicies = new InactiveTopicPolicies(
                InactiveTopicDeleteMode.delete_when_no_subscriptions,
                10, false);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setInactiveTopicPolicies(namespace, inactiveTopicPolicies));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.ANTI_AFFINITY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceAntiAffinityGroup(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.ANTI_AFFINITY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceAntiAffinityGroup(namespace,
                        "invalid-group"));
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getOffloadDeleteLagMs(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.WRITE);
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

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.OFFLOAD, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.ENTRY_FILTERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getNamespaceEntryFilters(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setNamespaceEntryFilters(namespace,
                        new EntryFilters("filter1")));
        Assert.assertTrue(execFlag.get());

                execFlag = setAuthorizationPolicyOperationChecker(subject,
                        PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.ENCRYPTION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getEncryptionRequiredStatus(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.ENCRYPTION, PolicyOperation.WRITE);
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

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSubscriptionTypesEnabled(namespace,
                        Sets.newHashSet(SubscriptionType.Failover)));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE);
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getIsAllowAutoUpdateSchema(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setIsAllowAutoUpdateSchema(namespace, true, true));
        Assert.assertTrue(execFlag.get());
    }

    @SuppressWarnings("deprecation")
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                        AutoUpdateDisabled));
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getSchemaValidationEnforced(namespace));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().setSchemaValidationEnforced(namespace, true));
        Assert.assertTrue(execFlag.get());
    }
}
