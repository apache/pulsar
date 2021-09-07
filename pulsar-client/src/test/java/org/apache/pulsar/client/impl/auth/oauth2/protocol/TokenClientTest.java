package org.apache.pulsar.client.impl.auth.oauth2.protocol;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

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
        TokenClient tokenClient = new TokenClient(url);
        tokenClient.httpClient = defaultAsyncHttpClient;
        Map<String, String> bodyMap = new TreeMap<>();
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .audience("test-audience")
                .clientId("test-client-id")
                .clientSecret("test-client-secret")
                .scope("test-scope")
                .build();
        bodyMap.put("grant_type", "client_credentials");
        bodyMap.put("client_id", request.getClientId());
        bodyMap.put("client_secret", request.getClientSecret());
        bodyMap.put("audience", request.getAudience());
        if (!StringUtils.isBlank(request.getScope())) {
            bodyMap.put("scope", request.getScope());
        }
        String body = bodyMap.entrySet().stream()
            .map(e -> {
                try {
                    return URLEncoder.encode(
                            e.getKey(),
                            "UTF-8") + '=' + URLEncoder.encode(e.getValue(), "UTF-8");
                } catch (UnsupportedEncodingException e1) {
                    throw new RuntimeException(e1);
                }
            })
        .collect(Collectors.joining("&"));
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
    public void exchangeClientCredentialsFailedByScopeTest() throws
            IOException, TokenExchangeException, ExecutionException, InterruptedException {
        DefaultAsyncHttpClient defaultAsyncHttpClient = mock(DefaultAsyncHttpClient.class);
        URL url = new URL("http://localhost");
        TokenClient tokenClient = new TokenClient(url);
        tokenClient.httpClient = defaultAsyncHttpClient;
        Map<String, String> bodyMap = new TreeMap<>();
        ClientCredentialsExchangeRequest request = ClientCredentialsExchangeRequest.builder()
                .audience("test-audience")
                .clientId("test-client-id")
                .clientSecret("test-client-secret")
                .scope("test-scope")
                .build();
        bodyMap.put("grant_type", "client_credentials");
        bodyMap.put("client_id", request.getClientId());
        bodyMap.put("client_secret", request.getClientSecret());
        bodyMap.put("audience", request.getAudience());
        String body = bodyMap.entrySet().stream()
                .map(e -> {
                    try {
                        return URLEncoder.encode(
                                e.getKey(),
                                "UTF-8") + '=' + URLEncoder.encode(e.getValue(), "UTF-8");
                    } catch (UnsupportedEncodingException e1) {
                        throw new RuntimeException(e1);
                    }
                })
                .collect(Collectors.joining("&"));
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
        try {
            tokenClient.exchangeClientCredentials(request);
            fail("Because the body is missing the scope parameter, it should fail.");
        } catch (NullPointerException e) {
            // Skip this exception
        }
    }
}
