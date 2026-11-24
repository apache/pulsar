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
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PulsarAdminImpl}.
 */
public class PulsarAdminImplTest {

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

    @Test
    public void testAsyncRequestIsBoundedByRequestTimeout() throws Exception {
        @Cleanup("stop")
        WireMockServer server = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        server.start();
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .willReturn(okJson("[\"test-cluster\"]").withFixedDelay(5000)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());
        conf.setReadTimeoutMs(10000);
        conf.setRequestTimeoutMs(1000);

        @Cleanup
        PulsarAdminImpl admin = new PulsarAdminImpl(conf.getServiceUrl(), conf, null);
        CompletableFuture<List<String>> clusters = admin.clusters().getClustersAsync();
        // The server answers after 5 s, within the read timeout: only the 1 s request timeout can fail the call
        // before then
        assertThatThrownBy(() -> clusters.get(4, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
    }

    @SneakyThrows
    private Authentication createAdminAndGetAuth(ClientConfigurationData conf) {
        try (PulsarAdminImpl admin = new PulsarAdminImpl("http://localhost:8080", conf, null)) {
            return admin.auth;
        }
    }
}
