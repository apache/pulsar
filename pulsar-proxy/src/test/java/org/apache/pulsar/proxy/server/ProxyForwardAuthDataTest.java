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

import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import lombok.Cleanup;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.proxy.server.ProxyRolesEnforcementTest.BasicAuthentication;
import org.apache.pulsar.proxy.server.ProxyRolesEnforcementTest.BasicAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxyForwardAuthDataTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyForwardAuthDataTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        conf.setBrokerClientAuthenticationParameters("authParam:broker");
        conf.setAuthenticateOriginalAuthData(true);

        Set<String> superUserRoles = new HashSet<String>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<String>();
        providers.add(BasicAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("test");
        Set<String> proxyRoles = new HashSet<String>();
        proxyRoles.add("proxy");
        conf.setProxyRoles(proxyRoles);

        super.init();

        createAdminClient();
        producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testForwardAuthData() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Step 1: Create Admin Client

        // create a client which connects to proxy and pass authData
        String namespaceName = "my-property/my-ns";
        String topicName = "persistent://my-property/my-ns/my-topic1";
        String subscriptionName = "my-subscriber-name";
        String clientAuthParams = "authParam:client";
        String proxyAuthParams = "authParam:proxy";

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        // Step 2: Run Pulsar Proxy without forwarding authData - expect Exception
        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(proxyAuthParams);

        Set<String> providers = new HashSet<>();
        providers.add(BasicAuthenticationProvider.class.getName());
        proxyConfig.setAuthenticationProviders(providers);

        AuthenticationService authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        try (ProxyService proxyService = new ProxyService(proxyConfig, authenticationService)) {
            proxyService.start();
            try (PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), clientAuthParams)) {
                proxyClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
                Assert.fail("Shouldn't be able to subscribe, auth required");
            } catch (Exception e) {
                // expected behaviour
            }
        }

        // Step 3: Create proxy with forwardAuthData enabled
        proxyConfig.setForwardAuthorizationCredentials(true);
        authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        @Cleanup
        ProxyService proxyService = new ProxyService(proxyConfig, authenticationService);
        proxyService.start();

        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), clientAuthParams);
        proxyClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe().close();
    }

    private void createAdminClient() throws PulsarClientException {
        String adminAuthParams = "authParam:admin";
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(BasicAuthentication.class.getName(), adminAuthParams).build());
    }

    private PulsarClient createPulsarClient(String proxyServiceUrl, String authParams) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(proxyServiceUrl)
                .authentication(BasicAuthentication.class.getName(), authParams).build();
    }
}
