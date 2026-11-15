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
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Authorization of the topic-level policies of a scalable topic, written through {@code /admin/v2/topic}.
 */
@Test(groups = "broker-admin")
public class ScalableTopicPoliciesAuthZTest extends AuthZTest {

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    public void setup() {
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
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        close();
    }

    private String newScalableTopic() throws Exception {
        String topic = "topic://public/default/" + UUID.randomUUID();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        return topic;
    }

    @Test
    public void testTenantAdminCanReadAndWrite() throws Exception {
        String topic = newScalableTopic();
        RetentionPolicies retention = new RetentionPolicies(1, 1);
        tenantManagerAdmin.topicPolicies().setRetention(topic, retention);
        await().untilAsserted(() ->
                Assert.assertEquals(tenantManagerAdmin.topicPolicies().getRetention(topic), retention));
    }

    @Test
    public void testUnauthorizedRoleIsRejected() throws Exception {
        String topic = newScalableTopic();
        String token = Jwts.builder().claim("sub", UUID.randomUUID().toString()).signWith(SECRET_KEY).compact();
        @Cleanup
        PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> subAdmin.topicPolicies().setRetention(topic, new RetentionPolicies(1, 1)));
    }
}
