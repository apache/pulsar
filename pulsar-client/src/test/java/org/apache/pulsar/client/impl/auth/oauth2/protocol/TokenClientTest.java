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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.http.HttpRequest;
import org.apache.pulsar.http.HttpResponse;
import org.apache.pulsar.http.PulsarHttpClient;
import org.testng.annotations.Test;

/**
 * Token client exchange token mock test (PIP-478: TokenClient now drives a framework {@link PulsarHttpClient}).
 */
public class TokenClientTest {

    private static PulsarHttpClient mockHttpClientReturning(TokenResult tokenResult) {
        PulsarHttpClient httpClient = mock(PulsarHttpClient.class);
        byte[] json = new Gson().toJson(tokenResult).getBytes();
        when(httpClient.execute(any(HttpRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(HttpResponse.of(200, Map.of(), json)));
        return httpClient;
    }

    private static TokenResult sampleTokenResult() {
        TokenResult tokenResult = new TokenResult();
        tokenResult.setAccessToken("test-access-token");
        tokenResult.setIdToken("test-id");
        return tokenResult;
    }

    @Test
    public void exchangeClientCredentialsSuccessByScopeTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, mockHttpClientReturning(sampleTokenResult()));
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
    }

    @Test
    public void exchangeClientCredentialsSuccessWithoutOptionalClientCredentialsTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, mockHttpClientReturning(sampleTokenResult()));
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
    }

    @Test
    public void exchangeTlsClientAuthSuccessTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, mockHttpClientReturning(sampleTokenResult()));
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
    }

    @Test
    public void exchangeTlsClientAuthSuccessWithoutOptionalParamsTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, mockHttpClientReturning(sampleTokenResult()));
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
    }
}
