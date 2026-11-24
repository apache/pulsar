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

import static org.assertj.core.api.Assertions.assertThat;
import io.jsonwebtoken.Jwts;
import java.util.List;
import java.util.Set;
import javax.crypto.SecretKey;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.MultiRolesTokenAuthorizationProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class SchemaCompatibilityTenantAdminTest extends MockedPulsarServiceBaseTest {
    private static final SecretKey KEY = Jwts.SIG.HS256.key().build();
    private static final String TOPIC = "persistent://example-tenant/example-namespace/events";

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder builder) {
        builder.authentication(new AuthenticationToken(Jwts.builder().subject("root").signWith(KEY).compact()));
    }

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        conf.setAuthorizationProvider(MultiRolesTokenAuthorizationProvider.class.getName());
        conf.setSuperUserRoles(Set.of("root"));
        conf.getProperties().setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(KEY));
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        internalSetup();

        String cluster = conf.getClusterName();
        admin.clusters().createCluster(cluster, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("example-tenant", TenantInfo.builder()
                .adminRoles(Set.of("alice")).allowedClusters(Set.of(cluster)).build());
        admin.namespaces().createNamespace("example-tenant/example-namespace", Set.of(cluster));
        admin.topics().createNonPartitionedTopic(TOPIC);
        admin.schemas().createSchema(TOPIC, Schema.BOOL.getSchemaInfo());
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void tenantAdminWithoutRolesClaimCanCheckCompatibility() throws Exception {
        assertCanCheckCompatibility(Jwts.builder().subject("alice").signWith(KEY).compact());
    }

    @Test
    public void tenantAdminWithRolesClaimCanCheckCompatibility() throws Exception {
        assertCanCheckCompatibility(Jwts.builder().subject("alice").claim("roles", List.of("alice"))
                .signWith(KEY).compact());
    }

    private void assertCanCheckCompatibility(String token) throws Exception {
        try (PulsarAdmin alice = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(new AuthenticationToken(token)).build()) {
            assertThat(alice.schemas().testCompatibility(TOPIC, Schema.BOOL.getSchemaInfo()).isCompatibility())
                    .isTrue();
        }
    }
}
