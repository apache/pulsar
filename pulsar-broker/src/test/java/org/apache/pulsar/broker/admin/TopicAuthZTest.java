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

import static org.mockito.Mockito.doReturn;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.service.plugin.EntryFilterDefinition;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.broker.service.plugin.EntryFilterTest;
import org.apache.pulsar.broker.testcontext.MockEntryFilterProvider;
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
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class TopicAuthZTest extends AuthZTest {

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    public void setup() {
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
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        close();
    }

    private AtomicBoolean setAuthorizationPolicyOperationChecker(String role, Object policyName, Object operation) {
        AtomicBoolean execFlag = new AtomicBoolean(false);
        if (operation instanceof PolicyOperation ) {

            doReturn(true)
            .when(authorizationService).isValidOriginalPrincipal(Mockito.any(), Mockito.any(), Mockito.any());

            Mockito.doAnswer(invocationOnMock -> {
                String role_ = invocationOnMock.getArgument(4);
                if (role.equals(role_)) {
                    PolicyName policyName_ = invocationOnMock.getArgument(1);
                    PolicyOperation operation_ = invocationOnMock.getArgument(2);
                    Assert.assertEquals(operation_, operation);
                    Assert.assertEquals(policyName_, policyName);
                }
                execFlag.set(true);
                return invocationOnMock.callRealMethod();
            }).when(authorizationService).allowTopicPolicyOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.any());
        } else {
            throw new IllegalArgumentException("");
        }
        return execFlag;
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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.GET_STATS);

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

        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.GET_STATS);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getPartitionedInternalStats(topic));
        Assert.assertTrue(execFlag.get());

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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.CONSUME);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().updateSubscriptionProperties(topic, "test-sub", properties));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.CONSUME);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getSubscriptionProperties(topic, "test-sub"));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.CONSUME);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().analyzeSubscriptionBacklog(TopicName.get(topic).getPartition(0).getLocalName(), "test-sub", Optional.empty()));
        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, NamespaceOperation.CREATE_TOPIC);
        tenantManagerAdmin.topics().createMissedPartitions(topic);
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.LOOKUP);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().createMissedPartitions(topic));
        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.LOOKUP);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getPartitionedTopicMetadata(topic));
        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.GET_METADATA);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getProperties(topic));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            if (AuthAction.produce == action || AuthAction.consume == action) {
                subAdmin.topics().getProperties(topic);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> subAdmin.topics().getProperties(topic));
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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.UPDATE_METADATA);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().updateProperties(topic, properties));
        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.DELETE_METADATA);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().removeProperties(topic, "key1"));
        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, NamespaceOperation.DELETE_TOPIC);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().deletePartitionedTopic(topic));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace(ns, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().deletePartitionedTopic(topic));
            superUserAdmin.namespaces().revokePermissionsOnNamespace(ns, subject);
        }
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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.GET_SUBSCRIPTIONS);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getSubscriptions(topic));
        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.GET_STATS);
        if (partitioned) {
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().getPartitionedInternalStats(topic));
        } else {
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().getInternalStats(topic));
        }
        Assert.assertTrue(execFlag.get());

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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.UNSUBSCRIBE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().deleteSubscription(topic, subName));
        Assert.assertTrue(execFlag.get());

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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.SKIP);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().skipAllMessages(topic, subName));
        Assert.assertTrue(execFlag.get());

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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.SKIP);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().skipMessages(topic, subName, 1));
        Assert.assertTrue(execFlag.get());
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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.EXPIRE_MESSAGES);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().expireMessagesForAllSubscriptions(topic, 1));
        Assert.assertTrue(execFlag.get());
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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.RESET_CURSOR);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().resetCursor(topic, subName, System.currentTimeMillis()));
        Assert.assertTrue(execFlag.get());
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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.RESET_CURSOR);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().resetCursor(topic, subName, MessageId.latest));
        Assert.assertTrue(execFlag.get());
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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.PEEK_MESSAGES);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getMessagesById(topic, messageId.getLedgerId(), messageId.getEntryId()));
        Assert.assertTrue(execFlag.get());

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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.PEEK_MESSAGES);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().peekMessages(topic, subName, 1));
        Assert.assertTrue(execFlag.get());

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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.PEEK_MESSAGES);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().examineMessage(topic, "latest", 1));
        Assert.assertTrue(execFlag.get());

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

    @Test
    @SneakyThrows
    public void testExpireMessage() {
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
        superUserAdmin.topics().expireMessages(topic, subName, 1);

        // test tenant manager
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.EXPIRE_MESSAGES);
        tenantManagerAdmin.topics().expireMessages(topic, subName, 1);
        Assert.assertTrue(execFlag.get());

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
        deleteTopic(topic, false);
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

        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.EXPIRE_MESSAGES);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().expireMessages(topic, subName, MessageId.earliest, false));
        Assert.assertTrue(execFlag.get());

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




    @Test
    @SneakyThrows
    public void testSchemaCompatibility() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        createTopic(topic, false);
        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        superUserAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, true);

        // test tenant manager
        tenantManagerAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, true);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, false));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topicPolicies().getSchemaCompatibilityStrategy(topic, false));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }



    @Test
    @SneakyThrows
    public void testGetEntryFilter() {
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
        superUserAdmin.topicPolicies().getEntryFiltersPerTopic(topic, true);

        // test tenant manager
        tenantManagerAdmin.topicPolicies().getEntryFiltersPerTopic(topic, true);

        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ENTRY_FILTERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getEntryFiltersPerTopic(topic, false));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topicPolicies().getEntryFiltersPerTopic(topic, false));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testSetEntryFilter() {
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
        final EntryFilterProvider oldEntryFilterProvider = getPulsarService().getBrokerService().getEntryFilterProvider();
        @Cleanup
        final MockEntryFilterProvider testEntryFilterProvider =
                new MockEntryFilterProvider(getServiceConfiguration());

        testEntryFilterProvider
                .setMockEntryFilters(new EntryFilterDefinition(
                        "test",
                        null,
                        EntryFilterTest.class.getName()
                ));
        FieldUtils.writeField(getPulsarService().getBrokerService(),
                "entryFilterProvider", testEntryFilterProvider, true);
        final EntryFilters entryFilter = new EntryFilters("test");
        superUserAdmin.topicPolicies().setEntryFiltersPerTopic(topic, entryFilter);

        // test tenant manager
        tenantManagerAdmin.topicPolicies().setEntryFiltersPerTopic(topic, entryFilter);

        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setEntryFiltersPerTopic(topic, entryFilter));
        Assert.assertTrue(execFlag.get());

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topicPolicies().setEntryFiltersPerTopic(topic, entryFilter));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
        FieldUtils.writeField(getPulsarService().getBrokerService(),
                "entryFilterProvider", oldEntryFilterProvider, true);
    }

    @Test
    @SneakyThrows
    public void testRemoveEntryFilter() {
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
        final EntryFilterProvider oldEntryFilterProvider = getPulsarService().getBrokerService().getEntryFilterProvider();
        @Cleanup
        final MockEntryFilterProvider testEntryFilterProvider =
                new MockEntryFilterProvider(getServiceConfiguration());

        testEntryFilterProvider
                .setMockEntryFilters(new EntryFilterDefinition(
                        "test",
                        null,
                        EntryFilterTest.class.getName()
                ));
        FieldUtils.writeField(getPulsarService().getBrokerService(),
                "entryFilterProvider", testEntryFilterProvider, true);
        final EntryFilters entryFilter = new EntryFilters("test");
        superUserAdmin.topicPolicies().removeEntryFiltersPerTopic(topic);
        // test tenant manager
        tenantManagerAdmin.topicPolicies().removeEntryFiltersPerTopic(topic);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeEntryFiltersPerTopic(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topicPolicies().removeEntryFiltersPerTopic(topic));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
        FieldUtils.writeField(getPulsarService().getBrokerService(),
                "entryFilterProvider", oldEntryFilterProvider, true);
    }

    @Test
    @SneakyThrows
    public void testShadowTopic() {
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

        String shadowTopic = topic + "-shadow-topic";
        superUserAdmin.topics().createShadowTopic(shadowTopic, topic);
        superUserAdmin.topics().setShadowTopics(topic, Lists.newArrayList(shadowTopic));
        superUserAdmin.topics().getShadowTopics(topic);
        superUserAdmin.topics().removeShadowTopics(topic);


        // test tenant manager
        tenantManagerAdmin.topics().setShadowTopics(topic, Lists.newArrayList(shadowTopic));
        tenantManagerAdmin.topics().getShadowTopics(topic);
        tenantManagerAdmin.topics().removeShadowTopics(topic);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().setShadowTopics(topic, Lists.newArrayList(shadowTopic)));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getShadowTopics(topic));

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(topic, subject, Set.of(action));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().setShadowTopics(topic, Lists.newArrayList(shadowTopic)));
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> subAdmin.topics().getShadowTopics(topic));
            superUserAdmin.topics().revokePermissions(topic, subject);
        }
        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testList() {
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
        AtomicBoolean execFlag = setAuthorizationTopicOperationChecker(subject, NamespaceOperation.GET_TOPICS);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getList("public/default"));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationTopicOperationChecker(subject, NamespaceOperation.GET_TOPICS);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getPartitionedTopicList("public/default"));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testPermissionsOnTopic() {
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
        superUserAdmin.topics().getPermissions(topic);
        superUserAdmin.topics().grantPermission(topic, subject, Sets.newHashSet(AuthAction.functions));
        superUserAdmin.topics().revokePermissions(topic, subject);

        // test tenant manager
        tenantManagerAdmin.topics().getPermissions(topic);
        tenantManagerAdmin.topics().grantPermission(topic, subject, Sets.newHashSet(AuthAction.functions));
        tenantManagerAdmin.topics().revokePermissions(topic, subject);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getPermissions(topic));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().grantPermission(topic, subject, Sets.newHashSet(AuthAction.functions)));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().revokePermissions(topic, subject));

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testOffloadPolicies() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getOffloadPolicies(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setOffloadPolicies(topic, OffloadPolicies.builder().build()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeOffloadPolicies(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testMaxUnackedMessagesOnConsumer() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getMaxUnackedMessagesOnConsumer(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setMaxUnackedMessagesOnConsumer(topic, 2));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeMaxUnackedMessagesOnConsumer(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testDeduplicationSnapshotInterval() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getDeduplicationSnapshotInterval(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setDeduplicationSnapshotInterval(topic, 2));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeDeduplicationSnapshotInterval(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testInactiveTopicPolicies() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.INACTIVE_TOPIC, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getInactiveTopicPolicies(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setInactiveTopicPolicies(topic, new InactiveTopicPolicies()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeInactiveTopicPolicies(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testMaxUnackedMessagesOnSubscription() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getMaxUnackedMessagesOnSubscription(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setMaxUnackedMessagesOnSubscription(topic, 2));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeMaxUnackedMessagesOnSubscription(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testDelayedDeliveryPolicies() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DELAYED_DELIVERY, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getDelayedDeliveryPolicy(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DELAYED_DELIVERY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setDelayedDeliveryPolicy(topic, DelayedDeliveryPolicies.builder().build()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DELAYED_DELIVERY, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeDelayedDeliveryPolicy(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testAutoSubscriptionCreation() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_SUBSCRIPTION_CREATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getAutoSubscriptionCreation(topic, false));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_SUBSCRIPTION_CREATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setAutoSubscriptionCreation(topic, AutoSubscriptionCreationOverride.builder().build()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.AUTO_SUBSCRIPTION_CREATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeAutoSubscriptionCreation(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testSubscribeRate() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getSubscribeRate(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setSubscribeRate(topic, new SubscribeRate()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeSubscribeRate(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testSubscriptionTypesEnabled() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getSubscriptionTypesEnabled(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setSubscriptionTypesEnabled(topic, new HashSet<>()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeSubscriptionTypesEnabled(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testPublishRate() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getPublishRate(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setPublishRate(topic, new PublishRate()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removePublishRate(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testMaxConsumersPerSubscription() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getMaxConsumersPerSubscription(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setMaxConsumersPerSubscription(topic, 2));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeMaxConsumersPerSubscription(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testCompactionThreshold() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.COMPACTION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getCompactionThreshold(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.COMPACTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setCompactionThreshold(topic, 20000));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.COMPACTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeCompactionThreshold(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testDispatchRate() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getDispatchRate(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setDispatchRate(topic, DispatchRate.builder().build()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeDispatchRate(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testMaxConsumers() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getMaxConsumers(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setMaxConsumers(topic, 2));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeMaxConsumers(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testMaxProducers() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_PRODUCERS, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getMaxProducers(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setMaxProducers(topic, 2));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeMaxProducers(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testReplicatorDispatchRate() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION_RATE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getReplicatorDispatchRate(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setReplicatorDispatchRate(topic, DispatchRate.builder().build()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeReplicatorDispatchRate(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testPersistence() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.PERSISTENCE, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getPersistence(topic));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.PERSISTENCE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setPersistence(topic, new PersistencePolicies()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.PERSISTENCE, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removePersistence(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testRetention() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RETENTION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getRetention(topic, false));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RETENTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setRetention(topic, new RetentionPolicies()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.RETENTION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeRetention(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testDeduplication() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getDeduplicationStatus(topic, false));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setDeduplicationStatus(topic, false));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.DEDUPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeDeduplicationStatus(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testMessageTTL() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.TTL, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getMessageTTL(topic, false));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.TTL, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setMessageTTL(topic, 2));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.TTL, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeMessageTTL(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testBacklogQuota() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.BACKLOG, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().getBacklogQuotaMap(topic, false));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.BACKLOG, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setBacklogQuota(topic, BacklogQuota.builder().build()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.BACKLOG, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().removeBacklogQuota(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }

    @Test
    @SneakyThrows
    public void testReplicationClusters() {
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
        AtomicBoolean execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION, PolicyOperation.READ);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().getReplicationClusters(topic, false));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().setReplicationClusters(topic, new ArrayList<>()));
        Assert.assertTrue(execFlag.get());

        execFlag = setAuthorizationPolicyOperationChecker(subject, PolicyName.REPLICATION, PolicyOperation.WRITE);
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topics().removeReplicationClusters(topic));
        Assert.assertTrue(execFlag.get());

        deleteTopic(topic, false);
    }
}
