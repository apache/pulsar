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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockOIDCIdentityProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: admin-only OAuth2. A {@link PulsarAdmin} built without a {@link PulsarClient} must
 * acquire its OAuth2 token through the framework HTTP client bound by {@code PulsarAdminImpl} (mirroring
 * {@code PulsarClientImpl}), not the removed private OAuth2 {@code AsyncHttpClient}. Building the admin runs
 * {@code auth.start()}, which resolves the IdP metadata, and the first {@code getAuthData()} fetches the
 * token — both over the framework client. If the binding were missing, {@code getHttpClient()} would throw
 * "OAuth2 requires the authentication to be initialized by a PulsarClient/PulsarAdmin".
 */
@Test(groups = "broker-impl")
public class AdminOnlyOAuth2AuthTest {

    private static final String CREDENTIALS_FILE =
            "./src/test/resources/authentication/token/credentials_file.json";
    private static final String AUDIENCE = "my-pulsar-cluster";

    private MockOIDCIdentityProvider server;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        server = new MockOIDCIdentityProvider("super-secret-client-secret", AUDIENCE, 30000);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void adminOnlyOAuth2AcquiresTokenViaFrameworkClient() throws Exception {
        Path credentials = Paths.get(CREDENTIALS_FILE).toAbsolutePath();
        Authentication auth = AuthenticationFactoryOAuth2.clientCredentials(
                new URL(server.getIssuer()), credentials.toUri().toURL(), AUDIENCE);

        // Building the admin runs auth.start() (IdP metadata resolution); with no PulsarClient in play, that
        // must go over the framework HTTP client bound by PulsarAdminImpl. A missing binding would throw
        // here. The dummy service URL is never contacted during token acquisition.
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:65535")
                .authentication(auth)
                .build();

        // The first getAuthData() fetches the token over the same framework client; a Bearer token surfaces
        // through the auth data's HTTP headers.
        AuthenticationDataProvider data = auth.getAuthData();
        assertThat(data.hasDataForHttp()).isTrue();
        Set<Map.Entry<String, String>> headers = data.getHttpHeaders();
        assertThat(headers).anyMatch(h -> "Authorization".equals(h.getKey())
                && h.getValue() != null && h.getValue().startsWith("Bearer "));
    }
}
