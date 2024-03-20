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

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class BaseAuthZTest extends MockedPulsarServiceBaseTest {
    protected static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String TENANT_ADMIN_SUBJECT = UUID.randomUUID().toString();
    private static final String TENANT_ADMIN_TOKEN = Jwts.builder()
            .claim("sub", TENANT_ADMIN_SUBJECT).signWith(SECRET_KEY).compact();
    private static final String BROKER_INTERNAL_CLIENT_SUBJECT = "broker_internal";
    private static final String BROKER_INTERNAL_CLIENT_TOKEN = Jwts.builder()
            .claim("sub", BROKER_INTERNAL_CLIENT_SUBJECT).signWith(SECRET_KEY).compact();
    private static final String SUPER_USER_SUBJECT = "super-user";
    private static final String SUPER_USER_TOKEN = Jwts.builder()
            .claim("sub", SUPER_USER_SUBJECT).signWith(SECRET_KEY).compact();
    private static final String NOBODY_SUBJECT = "nobody";
    private static final String NOBODY_TOKEN = Jwts.builder()
            .claim("sub", NOBODY_SUBJECT).signWith(SECRET_KEY).compact();
    protected PulsarAdmin superUserAdmin;
    protected PulsarAdmin tenantManagerAdmin;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());
        conf.setSuperUserRoles(new HashSet<>(Arrays.asList(SUPER_USER_SUBJECT, BROKER_INTERNAL_CLIENT_SUBJECT)));
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(new HashSet<>(Arrays.asList(AuthenticationProviderToken.class.getName())));
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

        this.superUserAdmin = PulsarAdmin.builder()
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
}
