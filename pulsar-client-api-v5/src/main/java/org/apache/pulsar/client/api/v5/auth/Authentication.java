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
package org.apache.pulsar.client.api.v5.auth;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Pluggable authentication provider for the Pulsar v5 client (PIP-478).
 *
 * <p>The core interface carries only lifecycle. The actual credential work lives on opt-in
 * <em>capability</em> interfaces — segregated by transport (Pulsar binary protocol vs HTTP) and by
 * style (single-pass vs multi-round challenge/response) — that an implementation declares only when it
 * supports them:
 * <ul>
 *   <li>{@link BinaryAuthDataProvider} — single-pass credential for the binary protocol</li>
 *   <li>{@link HttpAuthHeadersProvider} — single-pass credential for HTTP</li>
 *   <li>{@link BinaryAuthChallengeHandler} — multi-round challenge/response for the binary protocol</li>
 *   <li>{@link HttpAuthChallengeHandler} — SASL-style multi-round over HTTP</li>
 * </ul>
 * The convenience composite {@link SinglePassAuthentication} bundles the common single-pass
 * combination.
 *
 * <p>All capability methods are asynchronous ({@link CompletableFuture}), so credential acquisition
 * never blocks the Netty event loop.
 */
public interface Authentication extends AutoCloseable {

    /**
     * Configuration step. Called once by the framework AFTER no-arg construction when the plugin was
     * loaded reflectively from {@code authPluginClassName} + {@code authParams}. NOT called when the
     * plugin was constructed programmatically by the user — that path presumes the instance is already
     * configured. Default: no-op. Implementations override to read their parameters.
     *
     * <p>Configuration is intentionally separate from {@link #initializeAsync}: configuration is static
     * plugin-level data (file paths, URLs, scopes); initialization gives the plugin runtime services
     * and may do I/O.
     *
     * @param authParams the parsed authentication parameters
     */
    default void configure(Map<String, String> authParams) {
    }

    /**
     * Initialization step. Called once by the framework with runtime services after configuration. May
     * do I/O; the returned future completes when the implementation is ready to serve credentials.
     * Never throws synchronously: all failures are reported by completing the returned future
     * exceptionally, never by throwing on the calling thread.
     *
     * @param ctx the runtime services context
     * @return a future completing when the plugin is ready
     */
    CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx);

    /**
     * Capability factory — the framework's ONLY discovery mechanism. The framework never uses
     * {@code instanceof} on a plugin instance; it asks this method for each capability it may drive. An
     * implementation may therefore serve a capability from a separate internal class, or forward a
     * wrapped delegate's capability, rather than implementing it on this type. The default
     * implementation returns {@code this} when the plugin implements the requested interface directly,
     * covering the common case with no override.
     *
     * <p>Contract: results are STABLE once {@link #initializeAsync} has completed — the framework may
     * look a capability up once and cache it for the client's lifetime. The framework never queries
     * capabilities before {@code initializeAsync} completes; a result returned before initialization is
     * unspecified. One object may serve several
     * capabilities. Capability objects are owned by this plugin and released by {@link #close()}; the
     * framework never closes them individually. All capability methods must tolerate concurrent
     * invocation (multiple connections authenticate in parallel); only the rounds of a single exchange
     * are serialized by the framework.
     *
     * @param kind the capability interface to look up
     * @param <T>  the capability type
     * @return the capability if supported, otherwise {@link Optional#empty()}
     */
    default <T> Optional<T> capability(Class<T> kind) {
        return kind.isInstance(this) ? Optional.of(kind.cast(this)) : Optional.empty();
    }

    @Override
    default void close() throws Exception {
    }
}
