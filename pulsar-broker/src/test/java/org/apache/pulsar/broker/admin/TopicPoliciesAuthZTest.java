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

import static org.awaitility.Awaitility.await;
import io.jsonwebtoken.Jwts;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public final class TopicPoliciesAuthZTest extends MockedPulsarStandalone {

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
            superUserAdmin = null;
        }
        if (tenantManagerAdmin != null) {
            tenantManagerAdmin.close();
            tenantManagerAdmin = null;
        }
        close();
    }


    @SneakyThrows
    @Test
    public void testRetention() {
        final String random = UUID.randomUUID().toString();
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
        final RetentionPolicies definedRetentionPolicy = new RetentionPolicies(1, 1);
        // test superuser
        superUserAdmin.topicPolicies().setRetention(topic, definedRetentionPolicy);

        // because the topic policies is eventual consistency, we should wait here
        await().untilAsserted(() -> {
            final RetentionPolicies receivedRetentionPolicy = superUserAdmin.topicPolicies().getRetention(topic);
             Assert.assertEquals(receivedRetentionPolicy, definedRetentionPolicy);
        });
        superUserAdmin.topicPolicies().removeRetention(topic);

        await().untilAsserted(() -> {
            final RetentionPolicies retention = superUserAdmin.topicPolicies().getRetention(topic);
            Assert.assertNull(retention);
        });

        // test tenant manager

        tenantManagerAdmin.topicPolicies().setRetention(topic, definedRetentionPolicy);
        await().untilAsserted(() -> {
            final RetentionPolicies receivedRetentionPolicy = tenantManagerAdmin.topicPolicies().getRetention(topic);
            Assert.assertEquals(receivedRetentionPolicy, definedRetentionPolicy);
        });
        tenantManagerAdmin.topicPolicies().removeRetention(topic);
        await().untilAsserted(() -> {
            final RetentionPolicies retention = tenantManagerAdmin.topicPolicies().getRetention(topic);
            Assert.assertNull(retention);
        });

        // test nobody

        try {
            subAdmin.topicPolicies().getRetention(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {

            subAdmin.topicPolicies().setRetention(topic, definedRetentionPolicy);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            subAdmin.topicPolicies().removeRetention(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        // test sub user with permissions
        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace("public/default",
                    subject, Set.of(action));
            try {
                subAdmin.topicPolicies().getRetention(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {

                subAdmin.topicPolicies().setRetention(topic, definedRetentionPolicy);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {
                subAdmin.topicPolicies().removeRetention(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace("public/default", subject);
        }
    }

    @SneakyThrows
    @Test
    public void testOffloadPolicy() {
        final String random = UUID.randomUUID().toString();
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

        // mocked data
        final OffloadPoliciesImpl definedOffloadPolicies = new OffloadPoliciesImpl();
        definedOffloadPolicies.setManagedLedgerOffloadThresholdInBytes(100L);
        definedOffloadPolicies.setManagedLedgerOffloadThresholdInSeconds(100L);
        definedOffloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(200L);
        definedOffloadPolicies.setManagedLedgerOffloadDriver("s3");
        definedOffloadPolicies.setManagedLedgerOffloadBucket("buck");

        // test superuser
        superUserAdmin.topicPolicies().setOffloadPolicies(topic, definedOffloadPolicies);

        // because the topic policies is eventual consistency, we should wait here
        await().untilAsserted(() -> {
            final OffloadPolicies offloadPolicy = superUserAdmin.topicPolicies().getOffloadPolicies(topic);
            Assert.assertEquals(offloadPolicy, definedOffloadPolicies);
        });
        superUserAdmin.topicPolicies().removeOffloadPolicies(topic);

        await().untilAsserted(() -> {
            final OffloadPolicies offloadPolicy = superUserAdmin.topicPolicies().getOffloadPolicies(topic);
            Assert.assertNull(offloadPolicy);
        });

        // test tenant manager

        tenantManagerAdmin.topicPolicies().setOffloadPolicies(topic, definedOffloadPolicies);
        await().untilAsserted(() -> {
            final OffloadPolicies offloadPolicy = tenantManagerAdmin.topicPolicies().getOffloadPolicies(topic);
            Assert.assertEquals(offloadPolicy, definedOffloadPolicies);
        });
        tenantManagerAdmin.topicPolicies().removeOffloadPolicies(topic);
        await().untilAsserted(() -> {
            final OffloadPolicies offloadPolicy = tenantManagerAdmin.topicPolicies().getOffloadPolicies(topic);
            Assert.assertNull(offloadPolicy);
        });

        // test nobody

        try {
            subAdmin.topicPolicies().getOffloadPolicies(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {

            subAdmin.topicPolicies().setOffloadPolicies(topic, definedOffloadPolicies);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            subAdmin.topicPolicies().removeOffloadPolicies(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        // test sub user with permissions
        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace("public/default",
                    subject, Set.of(action));
            try {
                subAdmin.topicPolicies().getOffloadPolicies(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {

                subAdmin.topicPolicies().setOffloadPolicies(topic, definedOffloadPolicies);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {
                subAdmin.topicPolicies().removeOffloadPolicies(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace("public/default", subject);
        }
    }

    @SneakyThrows
    @Test
    public void testMaxUnackedMessagesOnConsumer() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        // mocked data
        int definedUnackedMessagesOnConsumer = 100;

        // test superuser
        superUserAdmin.topicPolicies().setMaxUnackedMessagesOnConsumer(topic, definedUnackedMessagesOnConsumer);

        // because the topic policies is eventual consistency, we should wait here
        await().untilAsserted(() -> {
            final int unackedMessagesOnConsumer = superUserAdmin.topicPolicies()
                    .getMaxUnackedMessagesOnConsumer(topic);
            Assert.assertEquals(unackedMessagesOnConsumer, definedUnackedMessagesOnConsumer);
        });
        superUserAdmin.topicPolicies().removeMaxUnackedMessagesOnConsumer(topic);

        await().untilAsserted(() -> {
            final Integer unackedMessagesOnConsumer = superUserAdmin.topicPolicies().getMaxUnackedMessagesOnConsumer(topic);
            Assert.assertNull(unackedMessagesOnConsumer);
        });

        // test tenant manager

        tenantManagerAdmin.topicPolicies().setMaxUnackedMessagesOnConsumer(topic, definedUnackedMessagesOnConsumer);
        await().untilAsserted(() -> {
            final int unackedMessagesOnConsumer = tenantManagerAdmin.topicPolicies().getMaxUnackedMessagesOnConsumer(topic);
            Assert.assertEquals(unackedMessagesOnConsumer, definedUnackedMessagesOnConsumer);
        });
        tenantManagerAdmin.topicPolicies().removeMaxUnackedMessagesOnConsumer(topic);
        await().untilAsserted(() -> {
            final Integer unackedMessagesOnConsumer = tenantManagerAdmin.topicPolicies().getMaxUnackedMessagesOnConsumer(topic);
            Assert.assertNull(unackedMessagesOnConsumer);
        });

        // test nobody

        try {
            subAdmin.topicPolicies().getMaxUnackedMessagesOnConsumer(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {

            subAdmin.topicPolicies().setMaxUnackedMessagesOnConsumer(topic, definedUnackedMessagesOnConsumer);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            subAdmin.topicPolicies().removeMaxUnackedMessagesOnConsumer(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        // test sub user with permissions
        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace("public/default",
                    subject, Set.of(action));
            try {
                subAdmin.topicPolicies().getMaxUnackedMessagesOnConsumer(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {

                subAdmin.topicPolicies().setMaxUnackedMessagesOnConsumer(topic, definedUnackedMessagesOnConsumer);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {
                subAdmin.topicPolicies().removeMaxUnackedMessagesOnConsumer(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace("public/default", subject);
        }
    }

    @SneakyThrows
    @Test
    public void testMaxUnackedMessagesOnSubscription() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();

        // mocked data
        int definedUnackedMessagesOnConsumer = 100;

        // test superuser
        superUserAdmin.topicPolicies().setMaxUnackedMessagesOnSubscription(topic, definedUnackedMessagesOnConsumer);

        // because the topic policies is eventual consistency, we should wait here
        await().untilAsserted(() -> {
            final int unackedMessagesOnConsumer = superUserAdmin.topicPolicies()
                    .getMaxUnackedMessagesOnSubscription(topic);
            Assert.assertEquals(unackedMessagesOnConsumer, definedUnackedMessagesOnConsumer);
        });
        superUserAdmin.topicPolicies().removeMaxUnackedMessagesOnSubscription(topic);

        await().untilAsserted(() -> {
            final Integer unackedMessagesOnConsumer = superUserAdmin.topicPolicies()
                    .getMaxUnackedMessagesOnSubscription(topic);
            Assert.assertNull(unackedMessagesOnConsumer);
        });

        // test tenant manager

        tenantManagerAdmin.topicPolicies().setMaxUnackedMessagesOnSubscription(topic, definedUnackedMessagesOnConsumer);
        await().untilAsserted(() -> {
            final int unackedMessagesOnConsumer = tenantManagerAdmin.topicPolicies().getMaxUnackedMessagesOnSubscription(topic);
            Assert.assertEquals(unackedMessagesOnConsumer, definedUnackedMessagesOnConsumer);
        });
        tenantManagerAdmin.topicPolicies().removeMaxUnackedMessagesOnSubscription(topic);
        await().untilAsserted(() -> {
            final Integer unackedMessagesOnConsumer = tenantManagerAdmin.topicPolicies()
                    .getMaxUnackedMessagesOnSubscription(topic);
            Assert.assertNull(unackedMessagesOnConsumer);
        });

        // test nobody

        try {
            subAdmin.topicPolicies().getMaxUnackedMessagesOnSubscription(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {

            subAdmin.topicPolicies().setMaxUnackedMessagesOnSubscription(topic, definedUnackedMessagesOnConsumer);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            subAdmin.topicPolicies().removeMaxUnackedMessagesOnSubscription(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        // test sub user with permissions
        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.namespaces().grantPermissionOnNamespace("public/default",
                    subject, Set.of(action));
            try {
                subAdmin.topicPolicies().getMaxUnackedMessagesOnSubscription(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {

                subAdmin.topicPolicies().setMaxUnackedMessagesOnSubscription(topic, definedUnackedMessagesOnConsumer);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }

            try {
                subAdmin.topicPolicies().removeMaxUnackedMessagesOnSubscription(topic);
                Assert.fail("unexpected behaviour");
            } catch (PulsarAdminException ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
            }
            superUserAdmin.namespaces().revokePermissionsOnNamespace("public/default", subject);
        }

    }
}
