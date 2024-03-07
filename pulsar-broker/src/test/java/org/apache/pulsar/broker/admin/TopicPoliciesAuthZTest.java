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

import static org.awaitility.Awaitility.await;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class TopicPoliciesAuthZTest extends MockedPulsarServiceBaseTest {

    private PulsarAdmin superUserAdmin;

    private PulsarAdmin tenantManagerAdmin;

    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String TENANT_ADMIN_SUBJECT =  UUID.randomUUID().toString();
    private static final String TENANT_ADMIN_TOKEN = Jwts.builder()
            .claim("sub", TENANT_ADMIN_SUBJECT).signWith(SECRET_KEY).compact();


    private static final String BROKER_INTERNAL_CLIENT_SUBJECT = "broker_internal";
    private static final String BROKER_INTERNAL_CLIENT_TOKEN = Jwts.builder()
            .claim("sub", BROKER_INTERNAL_CLIENT_SUBJECT).signWith(SECRET_KEY).compact();
    private static final String SUPER_USER_SUBJECT = "super-user";
    private static final String SUPER_USER_TOKEN = Jwts.builder()
            .claim("sub", SUPER_USER_SUBJECT).signWith(SECRET_KEY).compact();
    private static final String NOBODY_SUBJECT =  "nobody";
    private static final String NOBODY_TOKEN = Jwts.builder()
            .claim("sub", NOBODY_SUBJECT).signWith(SECRET_KEY).compact();


    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());
        conf.setSuperUserRoles(Sets.newHashSet(SUPER_USER_SUBJECT, BROKER_INTERNAL_CLIENT_SUBJECT));
        conf.setAuthenticationEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        // internal client
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("token", BROKER_INTERNAL_CLIENT_TOKEN);
        final String brokerClientAuthParamStr = ObjectMapperFactory.getThreadLocal()
                .writeValueAsString(brokerClientAuthParams);
        conf.setBrokerClientAuthenticationParameters(brokerClientAuthParamStr);

        Properties properties = conf.getProperties();
        if (properties == null) {
            properties = new Properties();
            conf.setProperties(properties);
        }
        properties.put("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));

        internalSetup();
        setupDefaultTenantAndNamespace();

        this.superUserAdmin =PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        final TenantInfo tenantInfo = superUserAdmin.tenants().getTenantInfo("public");
        tenantInfo.getAdminRoles().add(TENANT_ADMIN_SUBJECT);
        superUserAdmin.tenants().updateTenant("public", tenantInfo);
        this.tenantManagerAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getWebServiceAddress())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        pulsarAdminBuilder.authentication(new AuthenticationToken(SUPER_USER_TOKEN));
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
     internalCleanup();
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
                .serviceHttpUrl(pulsar.getWebServiceAddress())
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
                    subject, Sets.newHashSet(action));
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
    public void testMaxUnackedMessagesOnConsumer() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;
        final String subject = UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        @Cleanup final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getWebServiceAddress())
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
                    subject, Sets.newHashSet(action));
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
                .serviceHttpUrl(pulsar.getWebServiceAddress())
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
                    subject, Sets.newHashSet(action));
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
