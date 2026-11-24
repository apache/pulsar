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
package org.apache.pulsar.client.impl.auth.oauth2;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication;
import org.apache.pulsar.client.impl.auth.v5.V5AuthContexts;

/**
 * v5-native OAuth 2.0 authentication (PIP-478). A single-pass, short-lived bearer credential served over
 * both transports: the binary protocol sends the current access token as {@code auth_data}, and HTTP
 * sends {@code Authorization: Bearer <token>}. The built-in v4 {@code AuthenticationOAuth2} drives this
 * body on the async binary path; the OAuth2 token acquisition and the early-refresh machinery (built on
 * {@code Flow} / {@code FlowBase}) remain on the v4 shim, which supplies the current access token to this
 * body via the access-token supplier.
 *
 * <p>Token acquisition performs network I/O. As with the existing v4 OAuth2 code, the supplier may block
 * on the underlying flow; off-loading the blocking call belongs to the framework layer, so
 * this body introduces no threading of its own.
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable}, this
 * concrete built-in body is serializable so the v4 {@code AuthenticationOAuth2} shim (whose interface
 * requires {@code Serializable}, for Functions/connector frameworks) round-trips. The access-token
 * supplier is itself serializable.
 */
public class OAuth2AuthenticationV5 implements SinglePassAuthentication, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = "token";
    /** The HTTP header used to carry the bearer token. */
    public static final String HTTP_HEADER_NAME = "Authorization";
    /** The Pulsar HTTP header that names the auth method. */
    public static final String PULSAR_AUTH_METHOD_NAME = "X-Pulsar-Auth-Method-Name";

    private final Supplier<String> accessTokenSupplier;

    // Late-bound at initializeAsync(...): the client's bounded blocking executor, onto which the
    // (network-blocking) access-token fetch is off-loaded so it never runs on the Netty event loop
    // (PIP-478). Null when used outside a client, in which case the fetch runs inline.
    private transient volatile Executor blockingExecutor;

    /**
     * @param accessTokenSupplier supplies the current (cached/refreshed) access token on each call
     */
    public OAuth2AuthenticationV5(Supplier<String> accessTokenSupplier) {
        this.accessTokenSupplier = accessTokenSupplier;
    }

    @Override
    public String authMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
        this.blockingExecutor = ctx.blockingExecutor();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
        return V5AuthContexts.supplyBlocking(blockingExecutor,
                () -> new BinaryAuthData(accessToken().getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
        return V5AuthContexts.supplyBlocking(blockingExecutor, () -> {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(PULSAR_AUTH_METHOD_NAME, AUTH_METHOD_NAME);
            headers.put(HTTP_HEADER_NAME, "Bearer " + accessToken());
            return HttpAuthHeaders.of(headers);
        });
    }

    @Override
    public void close() {
    }

    private String accessToken() {
        return accessTokenSupplier.get();
    }
}
