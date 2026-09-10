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
import static org.mockito.Mockito.doReturn;
import com.google.common.collect.Sets;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.CustomLog;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServletWithClassLoader;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlets;
import org.apache.pulsar.broker.web.plugin.servlet.JakartaAdditionalServlet;
import org.apache.pulsar.broker.web.plugin.servlet.LegacyJavaxAdditionalServlet;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Verifies that {@code AdditionalServlet} plugins are served by the proxy's filter chain, whichever servlet API
 * they are written against. Legacy {@code javax.servlet} plugins are adapted to {@code jakarta.servlet} and
 * registered in the same Jetty environment as every other servlet, so the {@code AuthenticationFilter} the
 * proxy installs when authentication is enabled applies to them too (PIP-472).
 */
@CustomLog
public class ProxyAdditionalServletFilterChainTest extends MockedPulsarServiceBaseTest {

    private static final String JAVAX_BASE_PATH = "/metrics/javax";
    private static final String JAKARTA_BASE_PATH = "/metrics/jakarta";
    private static final String PARAM_VALUE = "hello";

    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private ProxyService proxyService;
    private WebServer proxyWebServer;
    private Authentication proxyClientAuthentication;
    private OkHttpClient httpClient;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.auth.MockAuthenticationProvider"));

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig)),
                proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();
        proxyService.start();

        AdditionalServlets additionalServlets = Mockito.mock(AdditionalServlets.class);
        Map<String, AdditionalServletWithClassLoader> servlets = new HashMap<>();
        servlets.put("javax-proxy-servlet", new AdditionalServletWithClassLoader(
                new LegacyJavaxAdditionalServlet(JAVAX_BASE_PATH), null));
        servlets.put("jakarta-proxy-servlet", new AdditionalServletWithClassLoader(
                new JakartaAdditionalServlet(JAKARTA_BASE_PATH), null));
        Mockito.when(additionalServlets.getServlets()).thenReturn(servlets);
        Mockito.when(proxyService.getProxyAdditionalServlets()).thenReturn(additionalServlets);

        proxyWebServer = new WebServer(proxyConfig,
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig)));
        ProxyServiceStarter.addWebServerHandlers(proxyWebServer, proxyConfig, proxyService, null,
                proxyClientAuthentication);
        proxyWebServer.start();

        httpClient = new OkHttpClient();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
            httpClient = null;
        }
        internalCleanup();
        if (proxyService != null) {
            proxyService.close();
        }
        if (proxyWebServer != null) {
            proxyWebServer.stop();
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    @Test
    public void testJavaxAdditionalServletGoesThroughTheFilterChain() throws Exception {
        assertThat(statusOf(JAVAX_BASE_PATH, false))
                .as("unauthenticated request to a javax.servlet additional servlet")
                .isEqualTo(HttpServletResponse.SC_UNAUTHORIZED);

        assertThat(bodyOf(JAVAX_BASE_PATH))
                .isEqualTo(LegacyJavaxAdditionalServlet.expectedResponse(JAVAX_BASE_PATH, PARAM_VALUE));
    }

    @Test
    public void testJakartaAdditionalServletGoesThroughTheFilterChain() throws Exception {
        assertThat(statusOf(JAKARTA_BASE_PATH, false))
                .as("unauthenticated request to a jakarta.servlet additional servlet")
                .isEqualTo(HttpServletResponse.SC_UNAUTHORIZED);

        assertThat(bodyOf(JAKARTA_BASE_PATH))
                .isEqualTo(JakartaAdditionalServlet.expectedResponse(JAKARTA_BASE_PATH, PARAM_VALUE));
    }

    private int statusOf(String basePath, boolean authenticated) throws IOException {
        try (Response response = get(basePath, authenticated)) {
            return response.code();
        }
    }

    private String bodyOf(String basePath) throws IOException {
        try (Response response = get(basePath, true)) {
            assertThat(response.code()).isEqualTo(HttpServletResponse.SC_OK);
            return response.body().string().trim();
        }
    }

    private Response get(String basePath, boolean authenticated) throws IOException {
        okhttp3.Request.Builder request = new okhttp3.Request.Builder()
                .get()
                .url("http://localhost:" + proxyWebServer.getListenPortHTTP().get() + basePath
                        + "?" + LegacyJavaxAdditionalServlet.QUERY_PARAM + "=" + PARAM_VALUE);
        if (authenticated) {
            // MockAuthenticationProvider accepts a "<result>.<result>" principal in the mockuser header
            request.header("mockuser", "pass.pass");
        }
        return httpClient.newCall(request.build()).execute();
    }
}
