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
package org.apache.pulsar.client.impl.v5;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication;
import org.apache.pulsar.client.impl.auth.v5.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.client.impl.auth.v5.NoAuthentication;
import org.apache.pulsar.client.impl.auth.v5.V5AuthenticationLoader;
import org.apache.pulsar.client.impl.auth.v5.V5BinaryAuthenticationDriver;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * Verifies the string-config reflective load path (PIP-478 In-Scope #2): a v5-native
 * {@link Authentication} deployed by class name is instantiated, configured with the parsed
 * {@code authParams}, and drives the Pulsar binary transport through the
 * {@link V5BinaryAuthenticationDriver} — where before it was blind-cast to the v4 SPI and threw
 * {@link ClassCastException}. A legacy v4 class still loads through the {@link LegacyV4AuthenticationAdapter}.
 */
public class V5AuthenticationLoaderTest {

    /** A v5-native single-pass plugin, loadable by class name (public + public no-arg ctor for reflection). */
    public static final class FakeV5SinglePass implements SinglePassAuthentication {

        private volatile Map<String, String> configuredParams;
        private volatile boolean initialized;

        public FakeV5SinglePass() {
        }

        @Override
        public void configure(Map<String, String> authParams) {
            this.configuredParams = Map.copyOf(authParams);
        }

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            this.initialized = true;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String authMethodName() {
            return "fake-v5";
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            String user = configuredParams == null ? "" : configuredParams.getOrDefault("user", "");
            return CompletableFuture.completedFuture(new BinaryAuthData(("token:" + user).getBytes(UTF_8)));
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
            return CompletableFuture.completedFuture(HttpAuthHeaders.of("Authorization", "Bearer fake"));
        }
    }

    /** A minimal legacy v4 credential plugin, loadable by class name. */
    @SuppressWarnings("deprecation")
    public static final class FakeV4Plugin implements org.apache.pulsar.client.api.Authentication {

        public FakeV4Plugin() {
        }

        @Override
        public String getAuthMethodName() {
            return "fake-v4";
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            return null;
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void v5NativePluginLoadedByNameIsConfiguredAndDrivesBinaryTransport() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV5SinglePass.class.getName(),
                "user:alice,realm:test");

        // The v5-native plugin is returned directly (not blind-cast to v4) and configure() ran with the
        // parsed key:val params.
        assertThat(auth).isInstanceOf(FakeV5SinglePass.class);
        assertThat(((FakeV5SinglePass) auth).configuredParams)
                .containsEntry("user", "alice")
                .containsEntry("realm", "test");

        // It drives the binary transport through the driver ClientCnx consumes — no v5->v4 wrapping.
        AuthenticationExchange exchange =
                new V5BinaryAuthenticationDriver(auth).newAuthenticationExchange("broker-1.example.com");
        AuthData credential = exchange.getAuthDataAsync().get();
        assertThat(((FakeV5SinglePass) auth).initialized).isTrue();
        assertThat(exchange.authMethodName()).isEqualTo("fake-v5");
        assertThat(new String(credential.getBytes(), UTF_8)).isEqualTo("token:alice");
    }

    @Test
    public void v5NativePluginAcceptsJsonParams() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV5SinglePass.class.getName(),
                "{\"user\":\"bob\"}");
        assertThat(auth).isInstanceOf(FakeV5SinglePass.class);
        assertThat(((FakeV5SinglePass) auth).configuredParams).containsEntry("user", "bob");
    }

    @Test
    public void v5NativePluginLoadedByNameWithParamsMap() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV5SinglePass.class.getName(),
                Map.of("user", "carol"));
        assertThat(auth).isInstanceOf(FakeV5SinglePass.class);
        assertThat(((FakeV5SinglePass) auth).configuredParams).containsEntry("user", "carol");
    }

    @Test
    public void v4PluginStillLoadsThroughLegacyAdapter() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV4Plugin.class.getName(), "");

        // A v4 class is bridged, not returned raw: it is a LegacyV4AuthenticationAdapter wrapping the v4
        // plugin, so unwrapV4 recovers the original v4 instance.
        assertThat(auth).isNotInstanceOf(FakeV5SinglePass.class);
        assertThat(LegacyV4AuthenticationAdapter.unwrapV4(auth))
                .get()
                .isInstanceOf(FakeV4Plugin.class);
    }

    @Test
    public void blankClassNameLoadsDisabledAuthentication() throws Exception {
        Authentication auth = V5AuthenticationLoader.create("", "");
        // Blank routes to the v4 path, which yields AuthenticationDisabled — and the "none" method name
        // resolves to the v5-native NoAuthentication rather than a bridged v4 plugin (PIP-478).
        assertThat(auth).isInstanceOf(NoAuthentication.class);
        assertThat(LegacyV4AuthenticationAdapter.unwrapV4(auth))
                .as("the v5-native no-auth body wraps nothing; there is no v4 plugin left to recover")
                .isEmpty();
    }

    /**
     * The unauthenticated case must be able to authenticate a binary connection. It is the only built-in
     * whose v4 form carries neither command data nor HTTP data, so the generic bridge — which advertises
     * {@link BinaryAuthDataProvider} only for a plugin that actually produced command data — resolves it to
     * a plugin with no binary capability at all. Driving that plugin natively fails the connection outright,
     * so "none" is routed to the v5-native body instead.
     */
    @Test
    public void theUnauthenticatedCaseCanDriveABinaryConnection() throws Exception {
        Authentication auth = V5AuthenticationLoader.create("", "");

        assertThat(auth.capability(BinaryAuthDataProvider.class))
                .as("a natively-driven client with no auth configured must still be able to connect")
                .isPresent();
        BinaryAuthDataProvider binary = auth.capability(BinaryAuthDataProvider.class).orElseThrow();
        assertThat(binary.authMethodName()).isEqualTo("none");
        assertThat(binary.getAuthDataAsync(null).get().bytes())
                .as("and it carries an empty credential")
                .isEmpty();
    }
}
