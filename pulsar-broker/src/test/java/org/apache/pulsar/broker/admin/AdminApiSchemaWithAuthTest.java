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

import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import javax.crypto.SecretKey;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit tests for schema admin api.
 */
@Slf4j
@Test(groups = "broker-admin")
public class AdminApiSchemaWithAuthTest extends MockedPulsarServiceBaseTest {

    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String ADMIN_TOKEN = Jwts.builder().setSubject("admin").signWith(SECRET_KEY).compact();
    private static final String PRODUCE_CONSUME_TOKEN = Jwts.builder().setSubject("producer+consumer").signWith(SECRET_KEY).compact();

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);
        conf.setAuthenticationProviders(providers);
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();

        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(brokerUrl != null
                        ? brokerUrl.toString() : brokerUrlTls.toString())
                .authentication(AuthenticationToken.class.getName(),
                        ADMIN_TOKEN);
        admin = Mockito.spy(pulsarAdminBuilder.build());

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("schematest", tenantInfo);
        admin.namespaces().createNamespace("schematest/test", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetCreateDeleteSchema() throws Exception {
        String topicName = "persistent://schematest/test/testCreateSchema";
        PulsarAdmin adminWithoutPermission = PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl != null ? brokerUrl.toString() : brokerUrlTls.toString())
                .build();
        PulsarAdmin adminWithAdminPermission = PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl != null ? brokerUrl.toString() : brokerUrlTls.toString())
                .authentication(AuthenticationToken.class.getName(), ADMIN_TOKEN)
                .build();
        PulsarAdmin adminWithProducerConsumePermission = PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl != null ? brokerUrl.toString() : brokerUrlTls.toString())
                .authentication(AuthenticationToken.class.getName(), PRODUCE_CONSUME_TOKEN)
                .build();
        SchemaInfo si = Schema.BOOL.getSchemaInfo();
        try {
            adminWithoutPermission.schemas().createSchema(topicName, si);
            fail("Should have failed");
        } catch (Exception ignore) {
        }
        admin.topics().grantPermission(topicName, "producer+consumer", EnumSet.of(AuthAction.produce, AuthAction.consume));
        adminWithAdminPermission.schemas().createSchema(topicName, si);
        try {
            adminWithoutPermission.schemas().getSchemaInfo(topicName);
            fail("Should have failed");
        } catch (Exception ignore) {
        }
        admin.topics().grantPermission(topicName, "consumer", EnumSet.of(AuthAction.consume));
        SchemaInfo readSi = adminWithProducerConsumePermission.schemas().getSchemaInfo(topicName);
        assertEquals(readSi, si);
        try {
            adminWithoutPermission.schemas().getSchemaInfo(topicName, 0);
            fail("Should have failed");
        } catch (Exception ignore) {
        }
        readSi = adminWithProducerConsumePermission.schemas().getSchemaInfo(topicName, 0);
        assertEquals(readSi, si);
        List<SchemaInfo> allSchemas = adminWithProducerConsumePermission.schemas().getAllSchemas(topicName);
        assertEquals(allSchemas.size(), 1);
        SchemaInfo schemaInfo2 = Schema.BOOL.getSchemaInfo();
        try {
            adminWithoutPermission.schemas().testCompatibility(topicName, schemaInfo2);
            fail("Should have failed");
        } catch (Exception ignore) {
        }
        boolean compatibility = adminWithAdminPermission.schemas().testCompatibility(topicName, schemaInfo2).isCompatibility();
        assertTrue(compatibility);
        try {
            adminWithoutPermission.schemas().getVersionBySchema(topicName, si);
            fail("Should have failed");
        } catch (Exception ignore) {
        }
        Long versionBySchema = adminWithProducerConsumePermission.schemas().getVersionBySchema(topicName, si);
        assertEquals(versionBySchema, Long.valueOf(0L));
        try {
            adminWithoutPermission.schemas().deleteSchema(topicName);
            fail("Should have failed");
        } catch (Exception ignore) {
        }
        admin.topics().grantPermission(topicName, "producer", EnumSet.of(AuthAction.produce));
        adminWithAdminPermission.schemas().deleteSchema(topicName);
    }
}
