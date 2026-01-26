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

import io.netty.resolver.NameResolver;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

/**
 * A client for an OAuth 2.0 token endpoint.
 */
public class TokenClient implements ClientCredentialsExchanger {

    private final URL tokenUrl;
    private final AsyncHttpClient httpClient;
    private final NameResolver<InetAddress> nameResolver;

    public TokenClient(URL tokenUrl, AsyncHttpClient httpClient, NameResolver<InetAddress> nameResolver) {
        this.httpClient = httpClient;
        this.tokenUrl = tokenUrl;
        this.nameResolver = nameResolver;
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

    /**
     * Constructing http request parameters.
     *
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
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + '='
                        + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
    }

    /**
     * Performs a token exchange using client credentials.
     *
     * @param req the client credentials request details.
     * @return a token result
     * @throws TokenExchangeException
     */
    public TokenResult exchangeClientCredentials(ClientCredentialsExchangeRequest req)
            throws TokenExchangeException, IOException {
        String body = buildClientCredentialsBody(req);

        try {
            BoundRequestBuilder requestBuilder = httpClient.preparePost(tokenUrl.toString())
                    .setHeader("Accept", "application/json")
                    .setHeader("Content-Type", "application/x-www-form-urlencoded")
                    .setBody(body);
            if (nameResolver != null) {
                requestBuilder.setNameResolver(nameResolver);
            }
            Response res = requestBuilder.execute().get();

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
            if (e1 instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IOException(e1);
        }
    }
}
