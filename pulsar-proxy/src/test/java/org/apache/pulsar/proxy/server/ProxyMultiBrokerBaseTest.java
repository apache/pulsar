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

import static org.apache.pulsar.proxy.server.ProxyServiceStarter.addWebServerHandlers;
import static org.mockito.Mockito.doReturn;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;

@Slf4j
public abstract class ProxyMultiBrokerBaseTest extends MultiBrokerBaseTest {
    private Authentication proxyClientAuthentication;
    private ProxyService proxyService;
    private AuthenticationService authenticationService;
    private WebServer server;
    private WebSocketService webSocketService;
    protected PulsarAdmin proxiedAdmin;

    @Override
    public int numberOfAdditionalBrokers() {
        return 2;
    }

    private ProxyConfiguration initializeProxyConfig() {
        var proxyConfig = new ProxyConfiguration();
        proxyConfig.setNumIOThreads(8);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);
        return proxyConfig;
    }

    @Override
    protected void additionalSetup() throws Exception {
        var proxyConfig = initializeProxyConfig();
        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();
        authenticationService = new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyService = BrokerTestUtil.spyWithoutRecordingInvocations(new ProxyService(proxyConfig,
                authenticationService,
                proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();
        proxyService.start();
        server = new WebServer(proxyConfig, authenticationService);

        AtomicReference<WebSocketService> webSocketServiceRef = new AtomicReference<>();
        addWebServerHandlers(server, proxyConfig, proxyService, proxyService.getDiscoveryProvider(),
                webSocketServiceRef,
                proxyClientAuthentication);
        webSocketService = webSocketServiceRef.get();

        // start web-service
        server.start();

        proxiedAdmin = PulsarAdmin.builder().serviceHttpUrl(getProxyHttpServiceUrl()).build();
        proxiedAdmin = customizeProxiedAdmin(proxiedAdmin);
    }

    protected PulsarAdmin customizeProxiedAdmin(PulsarAdmin proxiedAdmin) {
        // disable redirects by default so that AdminProxyHandler behavior can be verified
        ((PulsarAdminImpl) proxiedAdmin).getAsyncHttpConnector().setFollowRedirects(false);
        ((PulsarAdminImpl) proxiedAdmin).getAsyncConnectorProvider().setFollowRedirects(false);
        return proxiedAdmin;
    }

    @Override
    protected void additionalCleanup() throws Exception {
        if (proxiedAdmin != null) {
            proxiedAdmin.close();
        }
        if (proxyService != null) {
            proxyService.close();
        }
        if (server != null) {
            server.stop();
        }
        if (webSocketService != null) {
            webSocketService.close();
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
        if (authenticationService != null) {
            authenticationService.close();
        }
    }

    public String getProxyHttpServiceUrl() {
        return server.getServiceUri().toString();
    }

    public String getProxyServiceUrl() {
        return proxyService.getServiceUrl();
    }
}
