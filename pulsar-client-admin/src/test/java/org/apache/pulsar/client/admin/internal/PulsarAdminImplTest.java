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
package org.apache.pulsar.client.admin.internal;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.util.List;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PulsarAdminImpl}.
 */
public class PulsarAdminImplTest {

    WireMockServer server;

    @BeforeClass(alwaysRun = true)
    void beforeClass() {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig()
                .port(0));
        server.start();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    @BeforeMethod
    public void beforeEach() {
        server.resetAll();
    }

    @Test
    public void testAuthDisabledWhenAuthNotSpecifiedAnywhere() {
        assertThat(createAdminAndGetAuth(new ClientConfigurationData()))
                .isInstanceOf(AuthenticationDisabled.class);
    }

    @Test
    public void testAuthFromConfUsedWhenConfHasAuth() {
        Authentication auth = mock(Authentication.class);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setAuthentication(auth);
        assertThat(createAdminAndGetAuth(conf)).isSameAs(auth);
    }

    @SneakyThrows
    private Authentication createAdminAndGetAuth(ClientConfigurationData conf) {
        try (PulsarAdminImpl admin = new PulsarAdminImpl("http://localhost:8080", conf, null)) {
            return admin.auth;
        }
    }

    @Test
    @SneakyThrows
    public void testPulsarAdminAsyncHttpConnectorSuccessWithoutRetry() {
        int readTimeoutMs = 5000;
        int requestTimeoutMs = 5000;
        int serverDelayMs = 3000;
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("read-success-without-retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("success")
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")
                        .withFixedDelay(serverDelayMs)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());
        conf.setConnectionTimeoutMs(2000);
        conf.setReadTimeoutMs(readTimeoutMs);
        conf.setRequestTimeoutMs(requestTimeoutMs);

        @Cleanup
        PulsarAdminImpl admin = new PulsarAdminImpl(conf.getServiceUrl(), conf, null);
        List<String> clusters = admin.clusters().getClusters();
        assertThat(clusters).containsOnly("test-cluster");

        server.verify(1, getRequestedFor(urlEqualTo("/admin/v2/clusters")));
        String scenarioState = server.getAllScenarios().getScenarios().stream()
                .filter(scenario -> "read-success-without-retry".equals(scenario.getName())).findFirst().get()
                .getState();
        assertEquals(scenarioState, "success");
    }

    @Test
    @SneakyThrows
    public void testPulsarAdminAsyncHttpConnectorTimeoutWithoutRetry() {
        int readTimeoutMs = 5000;
        int requestTimeoutMs = 5000;
        int serverDelayMs = 8000;

        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("read-timeout-without-retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("end")
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")
                        .withFixedDelay(serverDelayMs)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());
        conf.setConnectionTimeoutMs(2000);
        conf.setReadTimeoutMs(readTimeoutMs);
        conf.setRequestTimeoutMs(requestTimeoutMs);

        @Cleanup
        PulsarAdminImpl admin = new PulsarAdminImpl(conf.getServiceUrl(), conf, null);
        Assert.expectThrows(PulsarAdminException.TimeoutException.class, () -> {
            admin.clusters().getClusters();
        });

        server.verify(1, getRequestedFor(urlEqualTo("/admin/v2/clusters")));
        String scenarioState = server.getAllScenarios().getScenarios().stream()
                .filter(scenario -> "read-timeout-without-retry".equals(scenario.getName())).findFirst().get()
                .getState();
        assertEquals(scenarioState, "end");
    }

    @Test
    @SneakyThrows
    public void testPulsarAdminAsyncHttpConnectorSuccessWithRetry() {
        int readTimeoutMs = 5000;
        int requestTimeoutMs = 10000;
        int serverDelayMs = 7000;
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("read-success-with-retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("first-call")
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")
                        .withFixedDelay(serverDelayMs)));

        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("read-success-with-retry")
                .whenScenarioStateIs("first-call")
                .willSetStateTo("success")
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());
        conf.setConnectionTimeoutMs(2000);
        conf.setReadTimeoutMs(readTimeoutMs);
        conf.setRequestTimeoutMs(requestTimeoutMs);

        @Cleanup
        PulsarAdminImpl admin = new PulsarAdminImpl(conf.getServiceUrl(), conf, null);
        List<String> clusters = admin.clusters().getClusters();
        assertThat(clusters).containsOnly("test-cluster");

        server.verify(2, getRequestedFor(urlEqualTo("/admin/v2/clusters")));
        String scenarioState = server.getAllScenarios().getScenarios().stream()
                .filter(scenario -> "read-success-with-retry".equals(scenario.getName())).findFirst().get()
                .getState();
        assertEquals(scenarioState, "success");
    }

    @Test
    @SneakyThrows
    public void testPulsarAdminAsyncHttpConnectorTimeoutWithRetry() {
        int readTimeoutMs = 5000;
        int requestTimeoutMs = 14000;
        int serverDelayMs = 6000;
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("read-timeout-with-retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("first-call")
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")
                        .withFixedDelay(serverDelayMs)));

        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("read-timeout-with-retry")
                .whenScenarioStateIs("first-call")
                .willSetStateTo("second-call")
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")
                        .withFixedDelay(serverDelayMs)));

        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("read-timeout-with-retry")
                .whenScenarioStateIs("second-call")
                .willSetStateTo("end")
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")
                        .withFixedDelay(serverDelayMs)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());
        conf.setConnectionTimeoutMs(2000);
        conf.setReadTimeoutMs(readTimeoutMs);
        conf.setRequestTimeoutMs(requestTimeoutMs);

        @Cleanup
        PulsarAdminImpl admin = new PulsarAdminImpl(conf.getServiceUrl(), conf, null);
        Assert.expectThrows(PulsarAdminException.TimeoutException.class, () -> {
            admin.clusters().getClusters();
        });

        server.verify(3, getRequestedFor(urlEqualTo("/admin/v2/clusters")));
        String scenarioState = server.getAllScenarios().getScenarios().stream()
                .filter(scenario -> "read-timeout-with-retry".equals(scenario.getName())).findFirst().get()
                .getState();
        assertEquals(scenarioState, "end");
    }

}
