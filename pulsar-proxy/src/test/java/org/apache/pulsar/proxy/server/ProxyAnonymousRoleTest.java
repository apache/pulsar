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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Base64;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "proxy")
public class ProxyAnonymousRoleTest extends ProducerConsumerBase {
    private final String ANONYMOUS_ROLE = "proxy-connector";
    private final String CLIENT_ROLE = "client";
    private final String ADMIN_ROLE = "admin";
    private final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private final String ADMIN_TOKEN = Jwts.builder().setSubject(ADMIN_ROLE).signWith(SECRET_KEY).compact();
    private final String CLIENT_TOKEN = Jwts.builder().setSubject(CLIENT_ROLE).signWith(SECRET_KEY).compact();

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // enable auth&auth and use JWT at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64," + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add(ANONYMOUS_ROLE);
        superUserRoles.add(ADMIN_ROLE);
        conf.setSuperUserRoles(superUserRoles);
        conf.setAnonymousUserRole(ANONYMOUS_ROLE);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("proxy-authorization");
        conf.setNumExecutorThreadPoolSize(5);

        super.init();
        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.getProperties().setProperty("tokenSecretKey", "data:;base64," + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setAuthenticationProviders(providers);

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(
                        PulsarConfigurationLoader.convertFrom(proxyConfig))));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
    }

    private void startProxy() throws Exception {
        proxyService.start();
    }

    @Test
    public void testAuthorizationWithProxyAsAnonymousRoleWithOriginalRole() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        initAdminClientWithBrokerUrl();

        String namespaceName = "my-property/proxy-authorization/my-ns";
        admin.clusters().createCluster("proxy-authorization", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        @Cleanup
        PulsarClient proxyClient = createPulsarClientWithProxyUrl(PulsarClient.builder());

        // client role does not have permission to consume to a topic
        assertThatThrownBy(() -> proxyClient.newConsumer()
                .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe());

        // grant consume permission to client role
        admin.namespaces().grantPermissionOnNamespace(namespaceName, CLIENT_ROLE,
                Sets.newHashSet(AuthAction.consume));
        proxyClient.newConsumer()
                .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
    }
    private void initAdminClientWithBrokerUrl() throws Exception {
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(AuthenticationFactory.token(ADMIN_TOKEN)).build());
    }

    private PulsarClient createPulsarClientWithProxyUrl(ClientBuilder clientBuilder)
            throws PulsarClientException {
        return clientBuilder.serviceUrl(proxyService.getServiceUrl())
                .authentication(AuthenticationFactory.token(CLIENT_TOKEN))
                .build();
    }
}
