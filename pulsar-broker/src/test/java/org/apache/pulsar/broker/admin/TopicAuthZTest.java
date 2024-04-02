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

import io.jsonwebtoken.Jwts;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.Bookies;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Test(groups = "broker-admin")
public class TopicAuthZTest extends MockedPulsarStandalone {

    private PulsarAdmin superUserAdmin;

    private PulsarAdmin tenantManagerAdmin;

    private static final String TENANT_ADMIN_SUBJECT =  UUID.randomUUID().toString();
    private static final String TENANT_ADMIN_TOKEN = Jwts.builder()
            .claim("sub", TENANT_ADMIN_SUBJECT).signWith(SECRET_KEY).compact();

    @SneakyThrows
    @BeforeClass
    public void before() {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        enableTransaction();
        start();
        createTransactionCoordinatorAssign(16);
        this.superUserAdmin =PulsarAdmin.builder()
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

        superUserAdmin.tenants().createTenant("pulsar", tenantInfo);
        superUserAdmin.namespaces().createNamespace("pulsar/system");
    }

    protected void createTransactionCoordinatorAssign(int numPartitionsOfTC) throws MetadataStoreException {
        getPulsarService().getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(numPartitionsOfTC));
    }

    @SneakyThrows
    @AfterClass
    public void after() {
        if (superUserAdmin != null) {
            superUserAdmin.close();
        }
        if (tenantManagerAdmin != null) {
            tenantManagerAdmin.close();
        }
        close();
    }

    @DataProvider(name = "partitioned")
    public static Object[][] partitioned() {
        return new Object[][] {
                {true},
                {false}
        };
    }


    @SneakyThrows
    @Test
    public void testUnloadAndCompactAndTrim() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test superuser
        superUserAdmin.topics().unload(topic);
        superUserAdmin.topics().triggerCompaction(topic);
        superUserAdmin.topics().trimTopic(TopicName.get(topic).getPartition(0).getLocalName());
        superUserAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, false);

        // test tenant manager
        tenantManagerAdmin.topics().unload(topic);
        tenantManagerAdmin.topics().triggerCompaction(topic);
        tenantManagerAdmin.topics().trimTopic(TopicName.get(topic).getPartition(0).getLocalName());
        tenantManagerAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, false);

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().unload(topic));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().triggerCompaction(topic));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().trimTopic(TopicName.get(topic).getPartition(0).getLocalName()));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, false));

        // Test only super/admin can do the operation, other auth are not permitted.
        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().unload(topic));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().triggerCompaction(topic));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().trimTopic(topic));

            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, false));

            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        superUserAdmin.topics().deletePartitionedTopic(topic, true);
    }

    @Test
    @SneakyThrows
    public void testGetManagedLedgerInfo() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test superuser
        superUserAdmin.topics().getInternalInfo(topic);

        // test tenant manager
        tenantManagerAdmin.topics().getInternalInfo(topic);

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getInternalInfo(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (action == AuthAction.produce || action == AuthAction.consume) {
                subAdmin.topics().getInternalInfo(topic);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getInternalInfo(topic));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        superUserAdmin.topics().deletePartitionedTopic(topic, true);
    }

    @Test
    @SneakyThrows
    public void testGetPartitionedStatsAndInternalStats() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test superuser
        superUserAdmin.topics().getPartitionedStats(topic, false);
        superUserAdmin.topics().getPartitionedInternalStats(topic);

        // test tenant manager
        tenantManagerAdmin.topics().getPartitionedStats(topic, false);
        tenantManagerAdmin.topics().getPartitionedInternalStats(topic);

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getPartitionedStats(topic, false));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getPartitionedInternalStats(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (action == AuthAction.produce || action == AuthAction.consume) {
                subAdmin.topics().getPartitionedStats(topic, false);
                subAdmin.topics().getPartitionedInternalStats(topic);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getPartitionedStats(topic, false));

                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getPartitionedInternalStats(topic));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        superUserAdmin.topics().deletePartitionedTopic(topic, true);
    }

    @Test
    @SneakyThrows
    public void testCreateSubscriptionAndUpdateSubscriptionPropertiesAndAnalyzeSubscriptionBacklog() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createPartitionedTopic(topic, 2);
        AtomicInteger suffix = new AtomicInteger(1);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        superUserAdmin.topics().createSubscription(topic, "test-sub" + suffix.incrementAndGet(), MessageId.earliest);

        // test tenant manager
        tenantManagerAdmin.topics().createSubscription(topic, "test-sub" + suffix.incrementAndGet(), MessageId.earliest);

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().createSubscription(topic, "test-sub" + suffix.incrementAndGet(), MessageId.earliest));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (action == AuthAction.consume) {
                subAdmin.topics().createSubscription(topic, "test-sub" + suffix.incrementAndGet(), MessageId.earliest);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().createSubscription(topic, "test-sub" + suffix.incrementAndGet(), MessageId.earliest));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        // test UpdateSubscriptionProperties
        Map<String, String> properties = new HashMap<>();
        superUserAdmin.topics().createSubscription(topic, "test-sub", MessageId.earliest);
        // test superuser
        superUserAdmin.topics().updateSubscriptionProperties(topic, "test-sub" , properties);
        superUserAdmin.topics().getSubscriptionProperties(topic, "test-sub");
        superUserAdmin.topics().analyzeSubscriptionBacklog(TopicName.get(topic).getPartition(0).getLocalName(), "test-sub", Optional.empty());

        // test tenant manager
        tenantManagerAdmin.topics().updateSubscriptionProperties(topic, "test-sub" , properties);
        tenantManagerAdmin.topics().getSubscriptionProperties(topic, "test-sub");
        tenantManagerAdmin.topics().analyzeSubscriptionBacklog(TopicName.get(topic).getPartition(0).getLocalName(), "test-sub", Optional.empty());

        // test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().updateSubscriptionProperties(topic, "test-sub", properties));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getSubscriptionProperties(topic, "test-sub"));

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().analyzeSubscriptionBacklog(TopicName.get(topic).getPartition(0).getLocalName(), "test-sub", Optional.empty()));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (action == AuthAction.consume) {
                subAdmin.topics().updateSubscriptionProperties(topic, "test-sub", properties);
                subAdmin.topics().getSubscriptionProperties(topic, "test-sub");
                subAdmin.topics().analyzeSubscriptionBacklog(TopicName.get(topic).getPartition(0).getLocalName(), "test-sub", Optional.empty());
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().updateSubscriptionProperties(topic, "test-sub", properties));

                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getSubscriptionProperties(topic, "test-sub"));

                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().analyzeSubscriptionBacklog(TopicName.get(topic).getPartition(0).getLocalName(), "test-sub", Optional.empty()));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        superUserAdmin.topics().deletePartitionedTopic(topic, true);
    }

    @Test
    @SneakyThrows
    public void testCreateMissingPartition() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createPartitionedTopic(topic, 2);
        AtomicInteger suffix = new AtomicInteger(1);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        superUserAdmin.topics().createMissedPartitions(topic);

        // test tenant manager
        tenantManagerAdmin.topics().createMissedPartitions(topic);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().createMissedPartitions(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().createMissedPartitions(topic));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        superUserAdmin.topics().deletePartitionedTopic(topic, true);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testPartitionedTopicMetadata(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        superUserAdmin.topics().getPartitionedTopicMetadata(topic);

        // test tenant manager
        tenantManagerAdmin.topics().getPartitionedTopicMetadata(topic);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getPartitionedTopicMetadata(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.produce == action || AuthAction.consume == action) {
                subAdmin.topics().getPartitionedTopicMetadata(topic);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getPartitionedTopicMetadata(topic));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testGetProperties(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        superUserAdmin.topics().getProperties(topic);

        // test tenant manager
        tenantManagerAdmin.topics().getProperties(topic);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getProperties(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.produce == action || AuthAction.consume == action) {
                subAdmin.topics().getPartitionedTopicMetadata(topic);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getPartitionedTopicMetadata(topic));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testUpdateProperties(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        superUserAdmin.topics().updateProperties(topic, properties);

        // test tenant manager
        tenantManagerAdmin.topics().updateProperties(topic, properties);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().updateProperties(topic, properties));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().updateProperties(topic, properties));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testRemoveProperties(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        superUserAdmin.topics().removeProperties(topic, "key1");

        // test tenant manager
        tenantManagerAdmin.topics().removeProperties(topic, "key1");

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().removeProperties(topic, "key1"));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().removeProperties(topic, "key1"));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test
    @SneakyThrows
    public void testDeletePartitionedTopic() {
        final String random = UUID.randomUUID().toString();
        String ns = "public/default/";
        final String topic = "persistent://" + ns + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        createTopic(topic , true);
        superUserAdmin.topics().deletePartitionedTopic(topic);

        // test tenant manager
        createTopic(topic, true);
        tenantManagerAdmin.topics().deletePartitionedTopic(topic);

        createTopic(topic, true);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().deletePartitionedTopic(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(ns, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().deletePartitionedTopic(topic));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(ns, subject);
        }
        deleteTopic(topic, true);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testGetSubscription(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        superUserAdmin.topics().getSubscriptions(topic);

        // test tenant manager
        tenantManagerAdmin.topics().getSubscriptions(topic);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getSubscriptions(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().getSubscriptions(topic);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getSubscriptions(topic));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testGetInternalStats(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        if (partitioned) {
            superUserAdmin.topics().getPartitionedInternalStats(topic);
        } else {
            superUserAdmin.topics().getInternalStats(topic);
        }

        // test tenant manager
        if (partitioned) {
            tenantManagerAdmin.topics().getPartitionedInternalStats(topic);
        } else {
            tenantManagerAdmin.topics().getInternalStats(topic);

        }

        if (partitioned) {
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().getPartitionedInternalStats(topic));
        } else {
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().getInternalStats(topic));
        }

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.produce == action || AuthAction.consume == action) {
                if (partitioned) {
                    subAdmin.topics().getPartitionedInternalStats(topic);
                } else {
                    subAdmin.topics().getInternalStats(topic);
                }
            } else {
                if (partitioned) {
                    Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                            () -> subAdmin.topics().getPartitionedInternalStats(topic));

                } else {
                    Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                            () -> subAdmin.topics().getInternalStats(topic));
                }
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testDeleteSubscription(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        String subName = "test-sub";
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        superUserAdmin.topics().deleteSubscription(topic, subName);

        // test tenant manager
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        tenantManagerAdmin.topics().deleteSubscription(topic, subName);

        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().deleteSubscription(topic, subName));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().deleteSubscription(topic, "test-sub");
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().deleteSubscription(topic, "test-sub"));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testSkipAllMessage(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        String subName = "test-sub";
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        superUserAdmin.topics().skipAllMessages(topic, subName);

        // test tenant manager
        tenantManagerAdmin.topics().skipAllMessages(topic, subName);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().skipAllMessages(topic, subName));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().skipAllMessages(topic,subName);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().skipAllMessages(topic, subName));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test
    @SneakyThrows
    public void testSkipMessage() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, false);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        String subName = "test-sub";
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        superUserAdmin.topics().skipMessages(topic, subName, 1);

        // test tenant manager
        tenantManagerAdmin.topics().skipMessages(topic, subName, 1);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().skipMessages(topic, subName, 1));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().skipMessages(topic, subName, 1);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().skipMessages(topic, subName, 1));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testExpireMessagesForAllSubscriptions(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        superUserAdmin.topics().expireMessagesForAllSubscriptions(topic, 1);

        // test tenant manager
        tenantManagerAdmin.topics().expireMessagesForAllSubscriptions(topic, 1);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().expireMessagesForAllSubscriptions(topic, 1));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().expireMessagesForAllSubscriptions(topic, 1);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().expireMessagesForAllSubscriptions(topic, 1));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testResetCursor(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        String subName = "test-sub";
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        superUserAdmin.topics().resetCursor(topic, subName, System.currentTimeMillis());

        // test tenant manager
        tenantManagerAdmin.topics().resetCursor(topic, subName, System.currentTimeMillis());

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().resetCursor(topic, subName, System.currentTimeMillis()));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().resetCursor(topic, subName, System.currentTimeMillis());
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().resetCursor(topic, subName, System.currentTimeMillis()));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test
    @SneakyThrows
    public void testResetCursorOnPosition() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, false);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        String subName = "test-sub";
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        superUserAdmin.topics().resetCursor(topic, subName, MessageId.latest);

        // test tenant manager
        tenantManagerAdmin.topics().resetCursor(topic, subName, MessageId.latest);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().resetCursor(topic, subName, MessageId.latest));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().resetCursor(topic, subName, MessageId.latest);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().resetCursor(topic, subName, MessageId.latest));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testGetMessageById() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, false);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        @Cleanup
        final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        final MessageIdImpl messageId = (MessageIdImpl) producer.send("test");
        superUserAdmin.topics().getMessagesById(topic, messageId.getLedgerId(), messageId.getEntryId());

        // test tenant manager
        tenantManagerAdmin.topics().getMessagesById(topic, messageId.getLedgerId(), messageId.getEntryId());

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getMessagesById(topic, messageId.getLedgerId(), messageId.getEntryId()));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().getMessagesById(topic, messageId.getLedgerId(), messageId.getEntryId());
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getMessagesById(topic, messageId.getLedgerId(), messageId.getEntryId()));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testPeekNthMessage() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, false);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        @Cleanup
        final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
        String subName = "test-sub";
        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.send("test");
        superUserAdmin.topics().peekMessages(topic, subName, 1);

        // test tenant manager
        tenantManagerAdmin.topics().peekMessages(topic, subName, 1);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().peekMessages(topic, subName, 1));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().peekMessages(topic, subName, 1);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().peekMessages(topic, subName, 1));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testExamineMessage() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, false);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        @Cleanup
        final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.send("test");
        superUserAdmin.topics().examineMessage(topic, "latest", 1);

        // test tenant manager
        tenantManagerAdmin.topics().examineMessage(topic, "latest", 1);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().examineMessage(topic, "latest", 1));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().examineMessage(topic, "latest", 1);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().examineMessage(topic, "latest", 1));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    @Test(dataProvider = "partitioned")
    @SneakyThrows
    public void testExpireMessage(boolean partitioned) {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, partitioned);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        @Cleanup
        final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
        String subName = "test-sub";
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.send("test1");
        producer.send("test2");
        producer.send("test3");
        producer.send("test4");
        superUserAdmin.topics().expireMessages(topic, subName, 1);

        // test tenant manager
        tenantManagerAdmin.topics().expireMessages(topic, subName, 1);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().expireMessages(topic, subName, 1));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().expireMessages(topic, subName, 1);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().expireMessages(topic, subName, 1));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, partitioned);
    }

    @Test
    @SneakyThrows
    public void testExpireMessageByPosition() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, false);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        //
        @Cleanup
        final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
        String subName = "test-sub";
        superUserAdmin.topics().createSubscription(topic, subName, MessageId.latest);
        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.send("test1");
        producer.send("test2");
        producer.send("test3");
        producer.send("test4");
        superUserAdmin.topics().expireMessages(topic, subName, MessageId.earliest, false);

        // test tenant manager
        tenantManagerAdmin.topics().expireMessages(topic, subName, MessageId.earliest, false);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().expireMessages(topic, subName, MessageId.earliest, false));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.consume == action) {
                subAdmin.topics().expireMessages(topic, subName, MessageId.earliest, false);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().expireMessages(topic, subName, MessageId.earliest, false));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    public enum OperationType {
        Lookup,
        Produce,
        Consume,
        ModifyTC,
        CheckTC
    }

    private final String testTopic = "persistent://public/default/" + UUID.randomUUID().toString();
    @FunctionalInterface
    public interface ThrowingBiConsumer<T> {
        void accept(T t) throws PulsarAdminException;
    }

    @DataProvider(name = "authFunction")
    public Object[][] authFunction () throws Exception {
        String sub = "my-sub";
        createTopic(testTopic, false);
        @Cleanup final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .enableTransaction(true)
                .build();
        @Cleanup final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(testTopic).create();

        @Cleanup final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(testTopic)
                .subscriptionName(sub)
                .subscribe();

        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES)
                .build().get();
        MessageIdImpl messageId = (MessageIdImpl) producer.newMessage().value("test message").send();

        consumer.acknowledgeAsync(messageId, transaction).get();

        return new Object[][]{
                // SCHEMA
               new Object[] {
                       (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().getSchemaInfo(testTopic),
                       OperationType.Lookup
               },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().getSchemaInfo(
                                testTopic, 0),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().getAllSchemas(
                                testTopic),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().createSchema(testTopic,
                                SchemaInfo.builder().type(SchemaType.STRING).build()),
                        OperationType.Produce
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().testCompatibility(
                                testTopic, SchemaInfo.builder().type(SchemaType.STRING).build()),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().deleteSchema(
                                testTopic),
                        OperationType.Produce
                },

                // TRANSACTION

                // Modify transaction coordinator
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .abortTransaction(transaction.getTxnID()),
                        OperationType.ModifyTC
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .scaleTransactionCoordinators(17),
                        OperationType.ModifyTC
                },

                // Check transaction coordinator stats
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getCoordinatorInternalStats(1, false),
                        OperationType.CheckTC
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getCoordinatorStats(),
                        OperationType.CheckTC
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getSlowTransactionsByCoordinatorId(1, 5, TimeUnit.SECONDS),
                        OperationType.CheckTC
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionMetadata(transaction.getTxnID()),
                        OperationType.CheckTC
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .listTransactionCoordinators(),
                        OperationType.CheckTC
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getSlowTransactions(5, TimeUnit.SECONDS),
                        OperationType.CheckTC
                },


                // Check stats related to transaction buffer and transaction pending ack
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getPendingAckInternalStats(testTopic, sub, false),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getPendingAckStats(testTopic, sub, false),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getPositionStatsInPendingAck(testTopic, sub, messageId.getLedgerId(),
                                        messageId.getEntryId(), null),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionBufferInternalStats(testTopic, false),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionBufferStats(testTopic, false),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionBufferStats(testTopic, false),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionInBufferStats(transaction.getTxnID(), testTopic),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionInBufferStats(transaction.getTxnID(), testTopic),
                        OperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionInPendingAckStats(transaction.getTxnID(), testTopic, sub),
                        OperationType.Lookup
                },
        };
    }

    @Test(dataProvider = "authFunction")
    public void testSchemaAuthorization(ThrowingBiConsumer<PulsarAdmin> adminConsumer, OperationType topicOpType)
            throws Exception {
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();

        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test tenant manager
        if (topicOpType != OperationType.ModifyTC) {
            adminConsumer.accept(tenantManagerAdmin);
        }

        if (topicOpType != OperationType.CheckTC) {
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> adminConsumer.accept(subAdmin));
        }

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(testTopic, subject, Set.of(action));

            if (authActionMatchOperation(topicOpType, action)) {
                adminConsumer.accept(subAdmin);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> adminConsumer.accept(subAdmin));
            }
            superUserAdmin.topics().revokePermissions(testTopic, subject);
        }
    }


    private boolean authActionMatchOperation(OperationType operationType, AuthAction action) {
        switch (operationType) {
            case Lookup -> {
                if (AuthAction.consume == action || AuthAction.produce == action) {
                    return true;
                }
            }
            case Consume -> {
                if (AuthAction.consume == action) {
                    return true;
                }
            }
            case Produce -> {
                if (AuthAction.produce == action) {
                    return true;
                }
            }
            case ModifyTC -> {
                return false;
            }
            case CheckTC -> {
                return true;
            }
        }
        return false;
    }

    private void createTopic(String topic, boolean partitioned) throws Exception {
        if (partitioned) {
            superUserAdmin.topics().createPartitionedTopic(topic, 2);
        } else {
            superUserAdmin.topics().createNonPartitionedTopic(topic);
        }
    }

    private void deleteTopic(String topic, boolean partitioned) throws Exception {
        if (partitioned) {
            superUserAdmin.topics().deletePartitionedTopic(topic, true);
        } else {
            superUserAdmin.topics().delete(topic, true);
        }
    }
}
