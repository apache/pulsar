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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;

/**
 * A client for an OAuth 2.0 token endpoint.
 */
public class TokenClient implements ClientCredentialsExchanger {

    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    protected static final int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    private final URL tokenUrl;
    private final AsyncHttpClient httpClient;

    public TokenClient(URL tokenUrl) {
        this(tokenUrl, null);
    }

    TokenClient(URL tokenUrl, AsyncHttpClient httpClient) {
        if (httpClient == null) {
            DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
            confBuilder.setUseProxyProperties(true);
            confBuilder.setFollowRedirect(true);
            confBuilder.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_IN_SECONDS * 1000);
            confBuilder.setReadTimeout(DEFAULT_READ_TIMEOUT_IN_SECONDS * 1000);
            confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
            AsyncHttpClientConfig config = confBuilder.build();
            this.httpClient = new DefaultAsyncHttpClient(config);
        } else {
            this.httpClient = httpClient;
        }
        this.tokenUrl = tokenUrl;
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

    /**
     * Constructing http request parameters.
     * @param req object with relevant request parameters
     * @return Generate the final request body from a map.
     */
    String buildClientCredentialsBody(ClientCredentialsExchangeRequest req) {
        Map<String, String> bodyMap = new TreeMap<>();
        bodyMap.put("grant_type", "client_credentials");
        bodyMap.put("client_id", req.getClientId());
        bodyMap.put("client_secret", req.getClientSecret());
        // Only set audience and scope if they are non-empty.
        if (!StringUtils.isBlank(req.getAudience())) {
            bodyMap.put("audience", req.getAudience());
        }
        if (!StringUtils.isBlank(req.getScope())) {
            bodyMap.put("scope", req.getScope());
        }
        return bodyMap.entrySet().stream()
                .map(e -> {
                    try {
                        return URLEncoder.encode(e.getKey(), "UTF-8") + '=' + URLEncoder.encode(e.getValue(), "UTF-8");
                    } catch (UnsupportedEncodingException e1) {
                        throw new RuntimeException(e1);
                    }
                })
                .collect(Collectors.joining("&"));
    }

    /**
     * Performs a token exchange using client credentials.
     * @param req the client credentials request details.
     * @return a token result
     * @throws TokenExchangeException
     */
    public TokenResult exchangeClientCredentials(ClientCredentialsExchangeRequest req)
            throws TokenExchangeException, IOException {
        String body = buildClientCredentialsBody(req);

        try {

            Response res = httpClient.preparePost(tokenUrl.toString())
                    .setHeader("Accept", "application/json")
                    .setHeader("Content-Type", "application/x-www-form-urlencoded")
                    .setBody(body)
                    .execute()
                    .get();

            switch (res.getStatusCode()) {
            case 200:
                return ObjectMapperFactory.getMapper().reader().readValue(res.getResponseBodyAsBytes(),
                        TokenResult.class);

            case 400: // Bad request
            case 401: // Unauthorized
                throw new TokenExchangeException(
                        ObjectMapperFactory.getMapper().reader().readValue(res.getResponseBodyAsBytes(),
                                TokenError.class));

            default:
                throw new IOException(
                        "Failed to perform HTTP request. res: " + res.getStatusCode() + " " + res.getStatusText());
            }



        } catch (InterruptedException | ExecutionException e1) {
            throw new IOException(e1);
        }
    }
}
