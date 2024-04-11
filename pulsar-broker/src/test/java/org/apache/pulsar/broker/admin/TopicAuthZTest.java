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
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
}
