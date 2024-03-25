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
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Test(groups = "broker-admin")
public class NamespaceAuthZTest extends MockedPulsarStandalone {

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
}
