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
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.awaitility.Awaitility.await;


public final class TopicPoliciesAuthZTest extends MockedPulsarStandalone {

    private PulsarAdmin superUserAdmin;

    private PulsarAdmin tenantManagerAdmin;

    private static final String TENANT_ADMIN_SUBJECT =  UUID.randomUUID().toString();
    private static final String TENANT_ADMIN_TOKEN = Jwts.builder()
            .claim("sub", TENANT_ADMIN_SUBJECT).signWith(SECRET_KEY).compact();

    @SneakyThrows
    @BeforeClass
    public void before() {
        loadTokenAuthentication();
        loadDefaultAuthorization();
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

}
