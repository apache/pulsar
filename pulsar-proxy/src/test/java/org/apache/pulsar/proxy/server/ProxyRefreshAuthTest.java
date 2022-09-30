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
package org.apache.pulsar.proxy.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class ProxyRefreshAuthTest extends ProducerConsumerBase {
    private final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();

        // enable tls and auth&auth at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setProxyRoles(Collections.singleton("Proxy"));
        conf.setAdvertisedAddress(null);
        conf.setAuthenticateOriginalAuthData(true);
        conf.setBrokerServicePort(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);

        conf.setAuthenticationProviders(Collections.singleton(AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        conf.setProperties(properties);

        conf.setClusterName("proxy-authorization");
        conf.setNumExecutorThreadPoolSize(5);

        conf.setAuthenticationRefreshCheckSeconds(1);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.init();

        admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress())
                .authentication(new AuthenticationToken(
                        () -> AuthTokenUtils.createToken(SECRET_KEY, "client", Optional.empty()))).build();
        String namespaceName = "my-tenant/my-ns";
        admin.clusters().createCluster("proxy-authorization",
                ClusterData.builder().serviceUrlTls(brokerUrlTls.toString()).build());
        admin.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setAdvertisedAddress(null);

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));

        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(
                AuthTokenUtils.createToken(SECRET_KEY, "Proxy", Optional.empty()));
        proxyConfig.setAuthenticationProviders(Collections.singleton(AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        proxyConfig.setProperties(properties);

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(
                        PulsarConfigurationLoader.convertFrom(proxyConfig))));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
    }

    private void startProxy(boolean forwardAuthData) throws Exception {
        pulsar.getConfiguration().setAuthenticateOriginalAuthData(forwardAuthData);
        proxyConfig.setForwardAuthorizationCredentials(forwardAuthData);
        proxyService.start();
    }

    @DataProvider
    Object[] forwardAuthDataProvider() {
        return new Object[]{true, false};
    }

    @Test(dataProvider = "forwardAuthDataProvider")
    public void testAuthDataRefresh(boolean forwardAuthData) throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy(forwardAuthData);

        AuthenticationToken authenticationToken = new AuthenticationToken(() -> {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 1);
            return AuthTokenUtils.createToken(SECRET_KEY, "client", Optional.of(calendar.getTime()));
        });

        pulsarClient = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .authentication(authenticationToken)
                .build();

        String topic = "persistent://my-tenant/my-ns/my-topic1";
        @Cleanup
        Producer<byte[]> ignored = spy(pulsarClient.newProducer()
                .topic(topic).create());

        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Set<CompletableFuture<ClientCnx>> connections = pulsarClientImpl.getCnxPool().getConnections();

        Awaitility.await().during(4, SECONDS).untilAsserted(() -> {
            pulsarClient.getPartitionsForTopic(topic).get();
            assertTrue(connections.stream().allMatch(n -> {
                try {
                    ClientCnx clientCnx = n.get();
                    long timestamp = clientCnx.getLastDisconnectedTimestamp();
                    return timestamp == 0;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        });
    }
}
