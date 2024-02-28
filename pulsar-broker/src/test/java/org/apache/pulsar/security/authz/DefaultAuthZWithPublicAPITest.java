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
package org.apache.pulsar.security.authz;

import io.jsonwebtoken.Jwts;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class DefaultAuthZWithPublicAPITest extends MockedPulsarStandalone {

    private static final String USER1_SUBJECT =  "user1";
    private static final String USER1_TOKEN = Jwts.builder()
            .claim("sub", USER1_SUBJECT).signWith(SECRET_KEY).compact();

    private PulsarAdmin user1Admin;

    private PulsarAdmin superUserAdmin;
    @SneakyThrows
    @BeforeClass
    public void before() {
        loadTokenAuthentication();
        loadDefaultAuthorization();
        start();
        this.user1Admin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(USER1_TOKEN))
                .build();
        this.superUserAdmin =PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
    }


    @SneakyThrows
    @AfterClass
    public void after() {
        close();
    }



    @SneakyThrows
    @Test
    public void testConsumeWithTopicPolicyRetention() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;

        // grant consume permission to user 1, it can lookup and consume messages
        superUserAdmin.namespaces().grantPermissionOnNamespace("public/default",
                USER1_SUBJECT, Set.of(AuthAction.consume));
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        // the user 1 shouldn't touch retention policy
        try {
            user1Admin.topicPolicies().getRetention(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            final RetentionPolicies policies = new RetentionPolicies(1, 1);
            user1Admin.topicPolicies().setRetention(topic, policies);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            user1Admin.topicPolicies().removeRetention(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }
    }
}
