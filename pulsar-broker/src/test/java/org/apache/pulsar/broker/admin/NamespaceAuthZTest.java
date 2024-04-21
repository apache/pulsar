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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.jsonwebtoken.Jwts;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class NamespaceAuthZTest extends MockedPulsarStandalone {

    private PulsarAdmin superUserAdmin;

    private PulsarAdmin tenantManagerAdmin;

    private PulsarClient pulsarClient;

    private AuthorizationService authorizationService;

    private AuthorizationService orignalAuthorizationService;

    private static final String TENANT_ADMIN_SUBJECT =  UUID.randomUUID().toString();
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
    }


    @SneakyThrows
    @AfterClass
    public void cleanup() {
        if (superUserAdmin != null) {
            superUserAdmin.close();
        }
        if (tenantManagerAdmin != null) {
            tenantManagerAdmin.close();
        }
        close();
    }

    @BeforeMethod
    public void before() throws IllegalAccessException {
        orignalAuthorizationService = getPulsarService().getBrokerService().getAuthorizationService();
        authorizationService = Mockito.spy(orignalAuthorizationService);
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                authorizationService, true);
    }

    @AfterMethod
    public void after() throws IllegalAccessException, PulsarAdminException {
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                orignalAuthorizationService, true);
        superUserAdmin.namespaces().deleteNamespace("public/default", true);
        superUserAdmin.namespaces().createNamespace("public/default");
    }

    private void setAuthorizationOperationChecker(String role, NamespaceOperation operation) {
        Mockito.doAnswer(invocationOnMock -> {
            String role_ = invocationOnMock.getArgument(2);
            if (role.equals(role_)) {
                NamespaceOperation operation_ = invocationOnMock.getArgument(1);
                Assert.assertEquals(operation_, operation);
            }
            return invocationOnMock.callRealMethod();
        }).when(authorizationService).allowNamespaceOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any());
    }

    @SneakyThrows
    @Test
    public void testProperties() {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/default";
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        // test super admin
        superUserAdmin.namespaces().getTopics(namespace);

        // test tenant manager
        tenantManagerAdmin.namespaces().getTopics(namespace);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getTopics(namespace));

        setAuthorizationOperationChecker(subject, NamespaceOperation.GET_TOPICS);

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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.send("message".getBytes());

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test super admin
        superUserAdmin.namespaces().getBundles(namespace);

        // test tenant manager
        tenantManagerAdmin.namespaces().getBundles(namespace);

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getBundles(namespace));

        setAuthorizationOperationChecker(subject, NamespaceOperation.GET_BUNDLE);

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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.send("message".getBytes());

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.send("message".getBytes());

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
            superUserAdmin.namespaces().splitNamespaceBundle(namespace, Policies.BundleType.LARGEST.toString(), false, null);
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
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().deleteNamespaceBundle(namespace, bundleRanges.get(1)));

        setAuthorizationOperationChecker(subject, NamespaceOperation.DELETE_BUNDLE);

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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        final String role = "sub";
        final AuthAction testAction = AuthAction.consume;

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().grantPermissionOnNamespace(namespace, role, Set.of(testAction)));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getPermissions(namespace));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().revokePermissionsOnNamespace(namespace, role));


        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            setAuthorizationOperationChecker(subject, NamespaceOperation.GRANT_PERMISSION);
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().grantPermissionOnNamespace(namespace, role, Set.of(testAction)));
            setAuthorizationOperationChecker(subject, NamespaceOperation.GET_PERMISSION);
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().getPermissions(namespace));
            setAuthorizationOperationChecker(subject, NamespaceOperation.REVOKE_PERMISSION);
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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
        Map<String, Set<String>> permissionOnSubscription = superUserAdmin.namespaces().getPermissionOnSubscription(namespace);
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
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().grantPermissionOnSubscription(namespace, subscription, Set.of(role)));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().getPermissionOnSubscription(namespace));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().revokePermissionOnSubscription(namespace, subscription, role));


        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, subject, Set.of(action));
            setAuthorizationOperationChecker(subject, NamespaceOperation.GRANT_PERMISSION);
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().grantPermissionOnSubscription(namespace, subscription, Set.of(role)));
            setAuthorizationOperationChecker(subject, NamespaceOperation.GET_PERMISSION);
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.namespaces().getPermissionOnSubscription(namespace));
            setAuthorizationOperationChecker(subject, NamespaceOperation.REVOKE_PERMISSION);
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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test super admin
        superUserAdmin.namespaces().clearNamespaceBacklog(namespace);

        // test tenant manager
        tenantManagerAdmin.namespaces().clearNamespaceBacklog(namespace);

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBacklog(namespace));

        setAuthorizationOperationChecker(subject, NamespaceOperation.CLEAR_BACKLOG);

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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
         Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().clearNamespaceBundleBacklog(namespace, defaultBundle));

         setAuthorizationOperationChecker(subject, NamespaceOperation.CLEAR_BACKLOG);

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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
         Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespace(namespace, "sub"));

         setAuthorizationOperationChecker(subject, NamespaceOperation.UNSUBSCRIBE);

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
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
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
         Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.namespaces().unsubscribeNamespaceBundle(namespace, defaultBundle, "sub"));

         setAuthorizationOperationChecker(subject, NamespaceOperation.UNSUBSCRIBE);

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


        setAuthorizationOperationChecker(subject, NamespaceOperation.PACKAGES);

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
}
