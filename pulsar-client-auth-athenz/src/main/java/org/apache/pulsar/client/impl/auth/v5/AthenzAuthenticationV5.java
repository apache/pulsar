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
package org.apache.pulsar.client.impl.auth.v5;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication;

/**
 * v5-native Athenz authentication (PIP-478). A single-pass credential served over both transports: the
 * binary protocol sends the current ZTS role token as {@code auth_data}, and HTTP sends the role token
 * under the configured Athenz role header. The built-in v4 {@code AuthenticationAthenz} drives this body
 * on the async binary path; the ZTS role-token acquisition and the 90-minute role-token cache remain on
 * the v4 shim, which supplies the current role token and the HTTP role header to this body via the
 * role-token provider.
 *
 * <p>Role-token acquisition performs network I/O. As with the existing v4 Athenz code, the provider may
 * block on the ZTS client; off-loading the blocking call belongs to the framework layer, so
 * this body introduces no threading of its own.
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable}, this
 * concrete built-in body is serializable so the v4 {@code AuthenticationAthenz} shim (whose interface
 * requires {@code Serializable}, for Functions/connector frameworks) round-trips. The role-token provider
 * is itself serializable.
 */
public class AthenzAuthenticationV5 implements SinglePassAuthentication, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = "athenz";

    private final RoleTokenProvider roleTokenProvider;

    // Late-bound at initializeAsync(...): the client's bounded blocking executor, onto which the
    // (ZTS-blocking) role-token fetch is off-loaded so it never runs on the Netty event loop
    // (PIP-478). Null when used outside a client, in which case the fetch runs inline.
    private transient volatile Executor blockingExecutor;

    /**
     * @param roleTokenProvider supplies the current role token and its HTTP header name on each call
     */
    public AthenzAuthenticationV5(RoleTokenProvider roleTokenProvider) {
        this.roleTokenProvider = roleTokenProvider;
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
                () -> new BinaryAuthData(roleTokenProvider.roleToken().getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
        return V5AuthContexts.supplyBlocking(blockingExecutor, () -> {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(roleTokenProvider.roleHeaderName(), roleTokenProvider.roleToken());
            return HttpAuthHeaders.of(headers);
        });
    }

    @Override
    public void close() {
    }

    /**
     * Supplies the current Athenz role token (acquiring/refreshing as needed) and the HTTP header name
     * under which it is sent. Implementations must be serializable.
     */
    public interface RoleTokenProvider extends Serializable {
        /**
         * @return the current ZTS role token
         */
        String roleToken();

        /**
         * @return the HTTP header name carrying the role token
         */
        String roleHeaderName();
    }
}
