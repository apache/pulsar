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
package org.apache.pulsar.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import javax.crypto.SecretKey;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;


public abstract class MockedPulsarStandalone implements AutoCloseable {

    @Getter
    private final ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
    private PulsarTestContext pulsarTestContext;

    @Getter
    private PulsarService pulsarService;
    private PulsarAdmin serviceInternalAdmin;


    {
        serviceConfiguration.setClusterName(TEST_CLUSTER_NAME);
        serviceConfiguration.setBrokerShutdownTimeoutMs(0L);
        serviceConfiguration.setBrokerServicePort(Optional.of(0));
        serviceConfiguration.setBrokerServicePortTls(Optional.of(0));
        serviceConfiguration.setAdvertisedAddress("localhost");
        serviceConfiguration.setWebServicePort(Optional.of(0));
        serviceConfiguration.setWebServicePortTls(Optional.of(0));
        serviceConfiguration.setNumExecutorThreadPoolSize(5);
        serviceConfiguration.setExposeBundlesMetricsInPrometheus(true);
    }


    protected static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    private static final String BROKER_INTERNAL_CLIENT_SUBJECT = "broker_internal";
    private static final String BROKER_INTERNAL_CLIENT_TOKEN = Jwts.builder()
            .claim("sub", BROKER_INTERNAL_CLIENT_SUBJECT).signWith(SECRET_KEY).compact();
    protected static final String SUPER_USER_SUBJECT = "super-user";
    protected static final String SUPER_USER_TOKEN = Jwts.builder()
            .claim("sub", SUPER_USER_SUBJECT).signWith(SECRET_KEY).compact();
    protected static final String NOBODY_SUBJECT =  "nobody";
    protected static final String NOBODY_TOKEN = Jwts.builder()
            .claim("sub", NOBODY_SUBJECT).signWith(SECRET_KEY).compact();


    @SneakyThrows
    protected void configureTokenAuthentication() {
        serviceConfiguration.setAuthenticationEnabled(true);
        serviceConfiguration.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        // internal client
        serviceConfiguration.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("token", BROKER_INTERNAL_CLIENT_TOKEN);
        final String brokerClientAuthParamStr = MAPPER.writeValueAsString(brokerClientAuthParams);
        serviceConfiguration.setBrokerClientAuthenticationParameters(brokerClientAuthParamStr);

        Properties properties = serviceConfiguration.getProperties();
        if (properties == null) {
            properties = new Properties();
            serviceConfiguration.setProperties(properties);
        }
        properties.put("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));

    }



    protected void configureDefaultAuthorization() {
        serviceConfiguration.setAuthorizationEnabled(true);
        serviceConfiguration.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());
        serviceConfiguration.setSuperUserRoles(Set.of(SUPER_USER_SUBJECT, BROKER_INTERNAL_CLIENT_SUBJECT));
    }


    @SneakyThrows
    protected void start() {
        this.pulsarTestContext = PulsarTestContext.builder()
                .spyByDefault()
                .config(serviceConfiguration)
                .withMockZookeeper(false)
                .build();
        this.pulsarService = pulsarTestContext.getPulsarService();
        this.serviceInternalAdmin = pulsarService.getAdminClient();
        setupDefaultTenantAndNamespace();
    }

    private void setupDefaultTenantAndNamespace() throws Exception {
        if (!serviceInternalAdmin.clusters().getClusters().contains(TEST_CLUSTER_NAME)) {
            serviceInternalAdmin.clusters().createCluster(TEST_CLUSTER_NAME,
                    ClusterData.builder().serviceUrl(pulsarService.getWebServiceAddress()).build());
        }
        if (!serviceInternalAdmin.tenants().getTenants().contains(DEFAULT_TENANT)) {
            serviceInternalAdmin.tenants().createTenant(DEFAULT_TENANT, TenantInfo.builder().allowedClusters(
                    Sets.newHashSet(TEST_CLUSTER_NAME)).build());
        }
        if (!serviceInternalAdmin.namespaces().getNamespaces(DEFAULT_TENANT).contains(DEFAULT_NAMESPACE)) {
            serviceInternalAdmin.namespaces().createNamespace(DEFAULT_NAMESPACE);
        }
    }


    @Override
    public void close() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
        }
    }

    // Utils
    protected static final ObjectMapper mapper = new ObjectMapper();

    // Static name
    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = "public/default";
    private static final String TEST_CLUSTER_NAME = "test-standalone";

    private static final ObjectMapper MAPPER = ObjectMapperFactory.getMapper().getObjectMapper();
}
