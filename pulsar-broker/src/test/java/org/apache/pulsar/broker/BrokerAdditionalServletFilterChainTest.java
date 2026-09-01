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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Sets;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlet;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServletWithClassLoader;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlets;
import org.apache.pulsar.broker.web.plugin.servlet.LegacyJavaxAdditionalServlet;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Verifies that {@code AdditionalServlet} plugins are served by the broker's filter chain, whichever servlet
 * API they are written against. Legacy {@code javax.servlet} plugins are adapted to {@code jakarta.servlet} and
 * registered in the same Jetty environment as every other servlet, so the {@code AuthenticationFilter} the
 * broker installs when authentication is enabled applies to them too (PIP-472).
 */
@Test(groups = "broker")
public class BrokerAdditionalServletFilterChainTest extends MockedPulsarServiceBaseTest {

    private static final String JAVAX_BASE_PATH = "/additional/servlet/javax";
    private static final String JAKARTA_BASE_PATH = "/additional/servlet/jakarta";
    private static final String PARAM_VALUE = "hello";

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.auth.MockAuthenticationProvider"));
        internalSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Override
    protected void beforePulsarStart(PulsarService pulsar) throws Exception {
        Map<String, AdditionalServletWithClassLoader> servlets = new HashMap<>();
        servlets.put("javax-servlet", new AdditionalServletWithClassLoader(
                new LegacyJavaxAdditionalServlet(JAVAX_BASE_PATH), null));
        servlets.put("jakarta-servlet", new AdditionalServletWithClassLoader(
                new JakartaAdditionalServlet(), null));

        AdditionalServlets additionalServlets = Mockito.mock(AdditionalServlets.class);
        Mockito.when(additionalServlets.getServlets()).thenReturn(servlets);
        Mockito.when(pulsar.getBrokerAdditionalServlets()).thenReturn(additionalServlets);
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

        assertThat(bodyOf(JAKARTA_BASE_PATH)).isEqualTo(PARAM_VALUE);
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
                .url(pulsar.getWebServiceAddress() + basePath
                        + "?" + LegacyJavaxAdditionalServlet.QUERY_PARAM + "=" + PARAM_VALUE);
        if (authenticated) {
            // MockAuthenticationProvider accepts a "<result>.<result>" principal in the mockuser header
            request.header("mockuser", "pass.pass");
        }
        return new OkHttpClient().newCall(request.build()).execute();
    }

    /**
     * An additional servlet written against the {@code jakarta.servlet} API, echoing back the query parameter.
     */
    private static class JakartaAdditionalServlet extends HttpServlet implements AdditionalServlet {

        @Override
        public void loadConfig(PulsarConfiguration pulsarConfiguration) {
            // No config to load
        }

        @Override
        public String getBasePath() {
            return JAKARTA_BASE_PATH;
        }

        @Override
        public AdditionalServletType getServletType() {
            return AdditionalServletType.JAKARTA_SERVLET;
        }

        @Override
        public Object getServletInstance() {
            return this;
        }

        @Override
        public void close() {
            // Nothing to close
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
            byte[] body = request.getParameter(LegacyJavaxAdditionalServlet.QUERY_PARAM)
                    .getBytes(StandardCharsets.UTF_8);
            response.setContentType("text/plain");
            response.setContentLength(body.length);
            response.getOutputStream().write(body);
            response.getOutputStream().flush();
        }
    }
}
