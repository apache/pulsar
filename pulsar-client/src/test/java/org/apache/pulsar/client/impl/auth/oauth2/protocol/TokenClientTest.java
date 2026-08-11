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
package org.apache.pulsar.client.impl.auth.oauth2.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotNull;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.AuthenticationHttpClient;
import org.apache.pulsar.client.api.AuthenticationHttpRequest;
import org.apache.pulsar.client.api.AuthenticationHttpResponse;
import org.testng.annotations.Test;

/**
 * Token client exchange token mock test.
 */
public class TokenClientTest {

    @Test
    public void exchangeClientCredentialsSuccessByScopeTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        MockAuthenticationHttpClient httpClient = new MockAuthenticationHttpClient(successResponse());
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, httpClient);
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .audience("test-audience")
                .clientId("test-client-id")
                .clientSecret("test-client-secret")
                .scope("test-scope")
                .authMethod(TokenEndpointAuthMethod.CLIENT_SECRET_POST)
                .build();
        String body = tokenClient.buildClientCredentialsBody(request);
        assertThat(body)
                .contains("grant_type=")
                .contains("client_id=")
                .contains("client_secret=")
                .contains("audience=")
                .contains("scope=");
        TokenResult tr = tokenClient.exchangeClientCredentials(request);
        assertNotNull(tr);
        assertThat(new String(httpClient.request.getBody(), StandardCharsets.UTF_8)).isEqualTo(body);
    }

    @Test
    public void exchangeClientCredentialsSuccessWithoutOptionalClientCredentialsTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        MockAuthenticationHttpClient httpClient = new MockAuthenticationHttpClient(successResponse());
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, httpClient);
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .clientId("test-client-id")
                .clientSecret("test-client-secret")
                .build();
        String body = tokenClient.buildClientCredentialsBody(request);
        assertThat(body)
                .contains("grant_type=")
                .contains("client_id=")
                .contains("client_secret=");
        TokenResult tr = tokenClient.exchangeClientCredentials(request);
        assertNotNull(tr);
        assertThat(new String(httpClient.request.getBody(), StandardCharsets.UTF_8)).isEqualTo(body);
    }

    @Test
    public void exchangeTlsClientAuthSuccessTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        MockAuthenticationHttpClient httpClient = new MockAuthenticationHttpClient(successResponse());
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, httpClient);
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .clientId("test-client-id")
                .audience("test-audience")
                .scope("test-scope")
                .authMethod(TokenEndpointAuthMethod.TLS_CLIENT_AUTH)
                .build();
        String body = tokenClient.buildClientCredentialsBody(request);
        assertThat(body)
                .contains("grant_type=")
                .contains("client_id=")
                .contains("audience=")
                .contains("scope=")
                .doesNotContain("client_secret=");
        TokenResult tr = tokenClient.exchangeClientCredentials(request);
        assertNotNull(tr);
        assertThat(new String(httpClient.request.getBody(), StandardCharsets.UTF_8)).isEqualTo(body);
    }

    @Test
    public void exchangeTlsClientAuthSuccessWithoutOptionalParamsTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        MockAuthenticationHttpClient httpClient = new MockAuthenticationHttpClient(successResponse());
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, httpClient);
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .clientId("test-client-id")
                .authMethod(TokenEndpointAuthMethod.TLS_CLIENT_AUTH)
                .build();
        String body = tokenClient.buildClientCredentialsBody(request);
        assertThat(body)
                .contains("grant_type=")
                .contains("client_id=")
                .doesNotContain("client_secret=");
        TokenResult tr = tokenClient.exchangeClientCredentials(request);
        assertNotNull(tr);
        assertThat(new String(httpClient.request.getBody(), StandardCharsets.UTF_8)).isEqualTo(body);
    }

    private static AuthenticationHttpResponse successResponse() {
        TokenResult tokenResult = new TokenResult();
        tokenResult.setAccessToken("test-access-token");
        tokenResult.setIdToken("test-id");
        return new AuthenticationHttpResponse(200, "OK", new Gson().toJson(tokenResult).getBytes());
    }

    private static final class MockAuthenticationHttpClient implements AuthenticationHttpClient {
        private final AuthenticationHttpResponse response;
        private AuthenticationHttpRequest request;

        private MockAuthenticationHttpClient(AuthenticationHttpResponse response) {
            this.response = response;
        }

        @Override
        public CompletableFuture<AuthenticationHttpResponse> execute(AuthenticationHttpRequest request) {
            this.request = request;
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public void close() throws IOException {
            // No-op.
        }
    }
}
