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
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
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
        start();
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

    public enum TopicOperationType {
        Lookup,
        Produce,
        Consume
    }

    @FunctionalInterface
    public interface ThrowingBiConsumer<T, U> {
        void accept(T t, U u) throws PulsarAdminException;
    }

    @DataProvider(name = "authFunction")
    public static Object[][] authFunction () {
        return new Object[][]{
                // The following tests are for testing schema authorization
               new Object[] {
                       (ThrowingBiConsumer<PulsarAdmin, String>) (admin, topic) -> admin.schemas().getSchemaInfo(topic),
                       TopicOperationType.Lookup
               },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin, String>) (admin, topic) -> admin.schemas().getSchemaInfo(
                                topic, 0),
                        TopicOperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin, String>) (admin, topic) -> admin.schemas().getAllSchemas(
                                topic),
                        TopicOperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin, String>) (admin, topic) -> admin.schemas().createSchema(topic,
                                SchemaInfo.builder().type(SchemaType.STRING).build()),
                        TopicOperationType.Produce
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin, String>) (admin, topic) -> admin.schemas().testCompatibility(
                                topic, SchemaInfo.builder().type(SchemaType.STRING).build()),
                        TopicOperationType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin, String>) (admin, topic) -> admin.schemas().deleteSchema(
                                topic),
                        TopicOperationType.Produce
                },
        };
    }

    @Test(dataProvider = "authFunction")
    public void testGetSchema(ThrowingBiConsumer<PulsarAdmin, String> adminConsumer, TopicOperationType topicOpType) throws Exception {
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
        @Cleanup
        final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

        // test tenant manager
        adminConsumer.accept(tenantManagerAdmin, topic);


        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> adminConsumer.accept(subAdmin, topic));


        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));

            if (authActionMatchOperation(topicOpType, action)) {
                adminConsumer.accept(subAdmin, topic);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> adminConsumer.accept(subAdmin, topic));
            }
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }


    private boolean authActionMatchOperation(TopicOperationType topicOperationType, AuthAction action) {
        switch (topicOperationType) {
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
