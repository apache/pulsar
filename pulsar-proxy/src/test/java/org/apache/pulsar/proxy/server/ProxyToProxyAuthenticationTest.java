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

import static org.assertj.core.api.Assertions.assertThat;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class ProxyToProxyAuthenticationTest extends ProducerConsumerBase {
    private static final String CLUSTER_NAME = "test";

    private static final String ADMIN_ROLE = "admin";
    private static final String PROXY_ROLE = "proxy";
    private static final String BROKER_ROLE = "broker";
    private static final String CLIENT_ROLE = "client";
    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    private static final String ADMIN_TOKEN = Jwts.builder().setSubject(ADMIN_ROLE).signWith(SECRET_KEY).compact();
    private static final String PROXY_TOKEN = Jwts.builder().setSubject(PROXY_ROLE).signWith(SECRET_KEY).compact();
    private static final String BROKER_TOKEN = Jwts.builder().setSubject(BROKER_ROLE).signWith(SECRET_KEY).compact();
    private static final String CLIENT_TOKEN = Jwts.builder().setSubject(CLIENT_ROLE).signWith(SECRET_KEY).compact();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticateOriginalAuthData(false);
        conf.setAuthenticationEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add(ADMIN_ROLE);
        superUserRoles.add(PROXY_ROLE);
        superUserRoles.add(BROKER_ROLE);
        conf.setSuperUserRoles(superUserRoles);
        conf.setProxyRoles(Collections.singleton(PROXY_ROLE));

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters(BROKER_TOKEN);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName(CLUSTER_NAME);
        super.init();

        admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).authentication(
                AuthenticationFactory.token(ADMIN_TOKEN)).build();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private ProxyService createProxyService(String serviceUrl) throws Exception {
        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setAuthenticateOriginalAuthData(true);
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(serviceUrl);
        proxyConfig.setClusterName(CLUSTER_NAME);

        proxyConfig.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));

        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(PROXY_TOKEN);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        proxyConfig.setAuthenticationProviders(providers);
        AuthenticationService authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        Authentication proxyClientAuthentication =
                AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                        proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();
        ProxyService proxyService = new ProxyService(proxyConfig, authenticationService, proxyClientAuthentication);
        proxyService.start();
        return proxyService;
    }

    @Test
    public void testClientConnectsThroughTwoAuthenticatedProxiesToBroker() throws Exception {
        @Cleanup
        ProxyService az1Proxy = createProxyService(pulsar.getBrokerServiceUrl());
        String az1ProxyServiceUrl = az1Proxy.getServiceUrl();
        @Cleanup
        ProxyService az2Proxy = createProxyService(az1ProxyServiceUrl);
        @Cleanup
        PulsarClient pulsarClient =
                PulsarClient.builder().serviceUrl(az2Proxy.getServiceUrl())
                        .authentication(AuthenticationFactory.token(CLIENT_TOKEN))
                        .build();
        String topic = TopicName.get("test-topic").toString();
        String subscription1 = "test-subscription";
        @Cleanup
        Consumer<byte[]> consumer1 =
                pulsarClient.newConsumer().topic(topic).subscriptionName(subscription1).subscribe();
        String subscription2 = "test2-subscription";
        @Cleanup
        Consumer<byte[]> consumer2 =
                pulsarClient.newConsumer().topic(topic).subscriptionName(subscription2).subscribe();

        CompletableFuture<Optional<Topic>> topicIfExists = pulsar.getBrokerService().getTopicIfExists(topic);
        assertThat(topicIfExists).succeedsWithin(3, TimeUnit.SECONDS);
        Topic topicRef = topicIfExists.get().orElseThrow();
        topicRef.getSubscriptions().forEach((key, value) -> {
            ServerCnx cnx = (ServerCnx) value.getConsumers().get(0).cnx();
            assertThat(cnx.getAuthRole()).isEqualTo(PROXY_ROLE);
            assertThat(cnx.getBinaryAuthSession().getOriginalPrincipal()).isEqualTo(CLIENT_ROLE);
        });

        consumer1.close();
        consumer2.close();
    }
}
