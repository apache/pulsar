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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;

/**
 * Built-in "no authentication" plugin for the v5 client (PIP-478) — the v5-native counterpart of the v4
 * {@code AuthenticationDisabled}. It declares the {@code "none"} authentication method name and carries an
 * empty binary payload.
 *
 * <p>This is the v5 body for the default, unauthenticated deployment, and it exists because the generic
 * v4&rarr;v5 bridge cannot serve that case. {@link LegacyV4AuthenticationAdapter}'s credential subtype
 * advertises the binary capability only when the wrapped plugin actually produces command data, and
 * {@code AuthenticationDisabled.getAuthData()} returns an empty {@code AuthenticationDataNull}. A bridged
 * no-auth plugin therefore exposes no {@link BinaryAuthDataProvider} at all, which is indistinguishable
 * from a plugin that cannot authenticate a binary connection. Routing {@code "none"} here keeps the
 * unauthenticated hot path both drivable and allocation-free: no executor hop, no v4 probe, no credential
 * I/O — matching PIP-478's requirement that the default no-auth path not be routed through the blocking
 * executor machinery.
 *
 * <p>Only the binary capability is declared. On the HTTP transport "no credential" is expressed by sending
 * no authentication headers at all, which is what omitting {@code HttpAuthHeadersProvider} means.
 */
public class NoAuthentication implements Authentication, BinaryAuthDataProvider {

    /**
     * The authentication method name for a connection that carries no credential.
     */
    public static final String DEFAULT_AUTH_METHOD_NAME = "none";

    /** The shared instance; this plugin is stateless. */
    public static final NoAuthentication INSTANCE = new NoAuthentication();

    private static final BinaryAuthData EMPTY = new BinaryAuthData(new byte[0]);

    /**
     * Create a no-op authentication plugin advertising the {@code "none"} method name.
     */
    public NoAuthentication() {
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext initContext) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String authMethodName() {
        return DEFAULT_AUTH_METHOD_NAME;
    }

    @Override
    public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext callContext) {
        return CompletableFuture.completedFuture(EMPTY);
    }
}
