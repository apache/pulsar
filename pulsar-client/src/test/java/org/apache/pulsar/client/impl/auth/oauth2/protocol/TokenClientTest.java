/**
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

import com.google.gson.Gson;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Token client exchange token mock test.
 */
public class TokenClientTest {

    @Test
    @SuppressWarnings("unchecked")
    public void exchangeClientCredentialsSuccessByScopeTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        DefaultAsyncHttpClient defaultAsyncHttpClient = mock(DefaultAsyncHttpClient.class);
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, defaultAsyncHttpClient);
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .audience("test-audience")
                .clientId("test-client-id")
                .clientSecret("test-client-secret")
                .scope("test-scope")
                .build();
        String body = tokenClient.buildClientCredentialsBody(request);
        BoundRequestBuilder boundRequestBuilder = mock(BoundRequestBuilder.class);
        Response response = mock(Response.class);
        ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(defaultAsyncHttpClient.preparePost(url.toString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setHeader("Accept", "application/json")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setHeader("Content-Type", "application/x-www-form-urlencoded")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(body)).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        TokenResult tokenResult = new TokenResult();
        tokenResult.setAccessToken("test-access-token");
        tokenResult.setIdToken("test-id");
        when(response.getResponseBodyAsBytes()).thenReturn(new Gson().toJson(tokenResult).getBytes());
        TokenResult tr = tokenClient.exchangeClientCredentials(request);
        Assert.assertNotNull(tr);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void exchangeClientCredentialsSuccessWithoutOptionalClientCredentialsTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        DefaultAsyncHttpClient defaultAsyncHttpClient = mock(DefaultAsyncHttpClient.class);
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url, defaultAsyncHttpClient);
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .clientId("test-client-id")
                .clientSecret("test-client-secret")
                .build();
        String body = tokenClient.buildClientCredentialsBody(request);
        BoundRequestBuilder boundRequestBuilder = mock(BoundRequestBuilder.class);
        Response response = mock(Response.class);
        ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(defaultAsyncHttpClient.preparePost(url.toString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setHeader("Accept", "application/json")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setHeader("Content-Type", "application/x-www-form-urlencoded")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(body)).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        TokenResult tokenResult = new TokenResult();
        tokenResult.setAccessToken("test-access-token");
        tokenResult.setIdToken("test-id");
        when(response.getResponseBodyAsBytes()).thenReturn(new Gson().toJson(tokenResult).getBytes());
        TokenResult tr = tokenClient.exchangeClientCredentials(request);
        Assert.assertNotNull(tr);
    }
}
