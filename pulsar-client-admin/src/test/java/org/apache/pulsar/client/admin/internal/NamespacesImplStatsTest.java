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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NamespacesImplStatsTest {

    private static final String STATS_PATH = "/admin/v2/namespaces/my-tenant/my-namespace/stats";

    private WireMockServer server;
    private String adminUrl;

    @BeforeClass(alwaysRun = true)
    void beforeClass() {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig()
                .containerThreads(8)
                .port(0));
        server.start();
        adminUrl = "http://localhost:" + server.port();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    @BeforeMethod(alwaysRun = true)
    void beforeMethod() {
        server.resetAll();
    }

    @Test
    public void testGetTopicsStatsDeserializesTopicStats() throws Exception {
        server.stubFor(get(urlEqualTo(STATS_PATH))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\n"
                                + "  \"persistent://my-tenant/my-namespace/topic-1\": "
                                + "{\"msgRateIn\": 1.5, \"msgInCounter\": 10},\n"
                                + "  \"persistent://my-tenant/my-namespace/topic-2\": "
                                + "{\"msgRateIn\": 2.5, \"msgInCounter\": 20}\n"
                                + "}")));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        Map<String, TopicStats> stats = admin.namespaces().getTopicsStats("my-tenant/my-namespace");

        assertThat(stats).hasSize(2);
        TopicStats first = stats.get("persistent://my-tenant/my-namespace/topic-1");
        assertThat(first).isNotNull();
        assertThat(first.getMsgRateIn()).isEqualTo(1.5);
        assertThat(first.getMsgInCounter()).isEqualTo(10);
        assertThat(stats.get("persistent://my-tenant/my-namespace/topic-2").getMsgRateIn()).isEqualTo(2.5);
        server.verify(getRequestedFor(urlEqualTo(STATS_PATH)));
    }

    @Test
    public void testGetTopicsStatsAsync() throws Exception {
        server.stubFor(get(urlEqualTo(STATS_PATH))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{}")));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        assertThat(admin.namespaces().getTopicsStatsAsync("my-tenant/my-namespace"))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .satisfies(stats -> assertThat(stats).isEmpty());
        server.verify(getRequestedFor(urlEqualTo(STATS_PATH)));
    }

    @Test
    public void testGetTopicsStatsReturnsErrorForMissingNamespace() throws Exception {
        server.stubFor(get(urlEqualTo(STATS_PATH))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"reason\":\"Namespace does not exist\"}")));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        assertThatThrownBy(() -> admin.namespaces().getTopicsStats("my-tenant/my-namespace"))
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
    }
}
