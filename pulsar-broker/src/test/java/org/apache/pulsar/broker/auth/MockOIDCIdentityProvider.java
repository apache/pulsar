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
package org.apache.pulsar.broker.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.security.Keys;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mock OIDC (and therefore OAuth2) server for testing. Note that the client_id is mapped to the token's subject claim.
 */
public class MockOIDCIdentityProvider {
    private final WireMockServer server;
    private final PublicKey publicKey;
    private final String audience;
    public MockOIDCIdentityProvider(String clientSecret, String audience, long tokenTTLMillis) {
        this.audience = audience;
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
        publicKey = keyPair.getPublic();
        server = new WireMockServer(wireMockConfig().port(0)
                .extensions(new OAuth2Transformer(keyPair, tokenTTLMillis)));
        server.start();

        // Set up a correct openid-configuration that points to the next stub
        server.stubFor(
                get(urlEqualTo("/.well-known/openid-configuration"))
                        .willReturn(aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody("""
                                        {
                                          "issuer": "%s",
                                          "token_endpoint": "%s/oauth/token"
                                        }
                                        """.replace("%s", server.baseUrl()))));

        // Only respond when the client sends the expected request body
        server.stubFor(post(urlEqualTo("/oauth/token"))
                .withRequestBody(matching(".*grant_type=client_credentials.*"))
                .withRequestBody(matching(".*audience=" + URLEncoder.encode(audience, StandardCharsets.UTF_8) + ".*"))
                .withRequestBody(matching(".*client_id=.*"))
                .withRequestBody(matching(".*client_secret=" + clientSecret + "(&.*|$)"))
                .willReturn(aResponse().withTransformers("o-auth-token-transformer").withStatus(200)));
    }

    public void stop() {
        server.stop();
    }

    public String getBase64EncodedPublicKey() {
        return Base64.getEncoder().encodeToString(publicKey.getEncoded());
    }

    public String getIssuer() {
        return server.baseUrl();
    }

    class OAuth2Transformer extends ResponseTransformer {

        private final PrivateKey privateKey;
        private final long tokenTTL;

        private final Pattern clientIdToRolePattern = Pattern.compile("client_id=([A-Za-z0-9-]*)(&|$)");

        OAuth2Transformer(KeyPair key, long tokenTTLMillis) {
            this.privateKey = key.getPrivate();
            this.tokenTTL = tokenTTLMillis;
        }

        @Override
        public Response transform(Request request, Response response, FileSource files, Parameters parameters) {
            Matcher m = clientIdToRolePattern.matcher(request.getBodyAsString());
            if (m.find()) {
                String role = m.group(1);
                return Response.Builder.like(response).but().body("""
                        {
                          "access_token": "%s",
                          "expires_in": %d,
                          "token_type":"Bearer"
                        }
                        """.formatted(generateToken(role),
                        TimeUnit.MILLISECONDS.toSeconds(tokenTTL))).build();
            } else {
                return Response.Builder.like(response).but().body("Invalid request").status(400).build();
            }
        }

        @Override
        public String getName() {
            return "o-auth-token-transformer";
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }

        private String generateToken(String role) {
            long now = System.currentTimeMillis();
            DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
            defaultJwtBuilder.setHeaderParam("typ", "JWT");
            defaultJwtBuilder.setHeaderParam("alg", "RS256");
            defaultJwtBuilder.setIssuer(server.baseUrl());
            defaultJwtBuilder.setSubject(role);
            defaultJwtBuilder.setAudience(audience);
            defaultJwtBuilder.setIssuedAt(new Date(now));
            defaultJwtBuilder.setNotBefore(new Date(now));
            defaultJwtBuilder.setExpiration(new Date(now + tokenTTL));
            defaultJwtBuilder.signWith(privateKey);
            return defaultJwtBuilder.compact();
        }
    }
}
