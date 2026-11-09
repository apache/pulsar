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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.collect.Sets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.namespace.TopicExistsInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.TestAuthorizationProvider;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.proxy.server.ProxyRolesEnforcementTest.BasicAuthentication;
import org.apache.pulsar.proxy.server.ProxyRolesEnforcementTest.BasicAuthenticationProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Through the proxy, topic auto-creation is subject to the authorization provider for the original principal, not
 * only for the proxy role.
 */
public class ProxyTopicAutoCreationAuthorizationTest extends ProducerConsumerBase {

    private static final String CLUSTER_NAME = "test";
    private static final String NAMESPACE = "my-property/my-ns";
    private static final String SUPER_USER_ROLE = "admin";
    private static final String PROXY_ROLE = "proxy";
    private static final String CREATOR_ROLE = "creator";
    private static final String USER_ROLE = "user";

    private Authentication proxyClientAuthentication;
    private ProxyService proxyService;

    /**
     * Lets the client roles and the proxy produce, consume and look up, and every role but {@link #USER_ROLE}
     * trigger topic auto-creation.
     */
    public static class AutoCreationAuthorizationProvider extends TestAuthorizationProvider {

        private static final Set<String> ROLES = Set.of(PROXY_ROLE, CREATOR_ROLE, USER_ROLE);

        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role,
                                                                   TopicOperation operation,
                                                                   AuthenticationDataSource authData) {
            return CompletableFuture.completedFuture(ROLES.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> allowTopicAutoCreationAsync(TopicName topic, String role,
                                                                      AuthenticationDataSource authData) {
            return CompletableFuture.completedFuture(!USER_ROLE.equals(role));
        }
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationProviders(Set.of(BasicAuthenticationProvider.class.getName()));
        conf.setAuthorizationProvider(AutoCreationAuthorizationProvider.class.getName());
        conf.setSuperUserRoles(Set.of(SUPER_USER_ROLE));
        conf.setProxyRoles(Set.of(PROXY_ROLE));
        conf.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        conf.setBrokerClientAuthenticationParameters("authParam:" + SUPER_USER_ROLE);
        conf.setAllowAutoTopicCreation(true);
        conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setClusterName(CLUSTER_NAME);
        super.init();

        closeAdmin();
        admin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(BasicAuthentication.class.getName(), "authParam:" + SUPER_USER_ROLE).build();
        admin.clusters().createCluster(CLUSTER_NAME,
                ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE, Sets.newHashSet(CLUSTER_NAME));

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters("authParam:" + PROXY_ROLE);
        proxyConfig.setAuthenticationProviders(Set.of(BasicAuthenticationProvider.class.getName()));
        proxyConfig.setClusterName(CLUSTER_NAME);
        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();
        proxyService = new ProxyService(proxyConfig,
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig)),
                proxyClientAuthentication);
        proxyService.start();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (proxyService != null) {
            proxyService.close();
            proxyService = null;
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
            proxyClientAuthentication = null;
        }
        super.internalCleanup();
    }

    private PulsarClient newProxiedClient(String role) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .authentication(BasicAuthentication.class.getName(), "authParam:" + role)
                .operationTimeout(3, TimeUnit.SECONDS)
                .build();
    }

    private boolean topicExists(String topic) throws Exception {
        TopicExistsInfo info = pulsar.getNamespaceService().checkTopicExistsAsync(TopicName.get(topic)).get();
        try {
            return info.isExists();
        } finally {
            info.recycle();
        }
    }

    @Test(timeOut = 60000)
    public void testAutoCreationFollowsOriginalPrincipal() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/proxied-auto-creation";
        @Cleanup
        PulsarClient userClient = newProxiedClient(USER_ROLE);
        @Cleanup
        PulsarClient creatorClient = newProxiedClient(CREATOR_ROLE);

        assertThatThrownBy(() -> userClient.newProducer().topic(topic).create())
                .as("a proxied producer of a role that may not create topics, although the proxy may")
                .isInstanceOf(PulsarClientException.TopicDoesNotExistException.class);
        assertThat(topicExists(topic)).as("topic created by a refused proxied producer").isFalse();

        creatorClient.newProducer().topic(topic).create().close();
        assertThat(topicExists(topic)).as("topic created by an allowed proxied producer").isTrue();

        userClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
    }
}
