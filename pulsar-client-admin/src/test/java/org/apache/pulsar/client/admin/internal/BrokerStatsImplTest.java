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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BrokerStatsImplTest {
    private WireMockServer server;

    @BeforeClass(alwaysRun = true)
    void beforeClass() {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        server.start();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    @AfterMethod(alwaysRun = true)
    void resetAllMocks() {
        server.resetAll();
    }

    /**
     * A broker without broker selection support only serves the local stats routes and ignores unknown query
     * params. Selecting a broker through it must fail instead of returning that broker's own stats.
     */
    @Test
    public void testSelectedBrokerFailsOnBrokerWithoutSelectionSupport() throws Exception {
        server.stubFor(get(urlPathEqualTo("/admin/v2/broker-stats/metrics"))
                .willReturn(aResponse().withHeader("Content-Type", "application/json").withBody("[]")));

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + server.port()).build();

        assertEquals(admin.brokerStats().getMetrics(), "[]");
        assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.brokerStats().getMetrics("other-broker:8080"));
        server.verify(getRequestedFor(urlEqualTo("/admin/v2/broker-stats/brokers/other-broker:8080/metrics")));
    }

    @Test
    public void testSelectedBrokerUsesBrokerRoute() throws Exception {
        server.stubFor(get(anyUrl()).willReturn(aResponse().withStatus(404)));
        server.stubFor(get(urlEqualTo("/admin/v2/broker-stats/brokers/b2:8080/mbeans"))
                .willReturn(aResponse().withHeader("Content-Type", "application/json").withBody("[\"b2\"]")));

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + server.port()).build();

        assertEquals(admin.brokerStats().getMBeans("b2:8080"), "[\"b2\"]");
        // An empty selector keeps the local route.
        assertThrows(PulsarAdminException.NotFoundException.class, () -> admin.brokerStats().getMBeans(""));
        server.verify(getRequestedFor(urlEqualTo("/admin/v2/broker-stats/mbeans")));
    }
}
