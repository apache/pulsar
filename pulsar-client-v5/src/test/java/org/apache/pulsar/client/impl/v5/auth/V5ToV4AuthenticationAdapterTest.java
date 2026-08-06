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
package org.apache.pulsar.client.impl.v5.auth;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.ChallengeResponse;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * Verifies that {@link V5ToV4AuthenticationAdapter} exposes a v5 plugin through the v4
 * {@code Authentication} interface that {@code ClientCnx} drives, drives one exchange-scoped call
 * context across all challenge rounds, routes the refresh sentinel to the credential provider (never
 * the challenge handler), fails loudly when the wrapped plugin cannot serve the binary transport, and
 * refuses serialization with an actionable message (v5 plugins are deliberately not {@code Serializable}).
 */
public class V5ToV4AuthenticationAdapterTest {

    private static V5ToV4AuthenticationAdapter wrap(Authentication v5) {
        return new V5ToV4AuthenticationAdapter(v5);
    }

    private static V5ToV4AuthenticationAdapter wrap() {
        return wrap(new TlsAuthentication());
    }

    @Test
    public void exposesV5MethodNameAndEmptyCredentialThroughV4Interface() throws Exception {
        V5ToV4AuthenticationAdapter adapter = wrap();
        assertThat(adapter.getAuthMethodName()).isEqualTo("tls");
        // start() drives the v5 initializeAsync(...) — a no-op for the mTLS plugin — and must not throw.
        adapter.start();

        AuthenticationDataProvider data = adapter.getAuthData("broker-1.example.com");
        // mTLS carries an empty binary payload; the certificate is presented at the TLS handshake.
        assertThat(data.getCommandData()).isEmpty();
    }

    @Test
    public void challengeStatePersistsAcrossRoundsThroughOneExchange() throws Exception {
        CountingChallengePlugin plugin = new CountingChallengePlugin();
        V5ToV4AuthenticationAdapter adapter = wrap(plugin);
        adapter.start();

        AuthenticationExchange exchange = adapter.newAuthenticationExchange("broker-1.example.com");
        // Initial round seeds the conversation state in the call-context state slot.
        exchange.getAuthDataAsync().get();
        AuthData round1 = exchange.authenticateAsync(AuthData.of("challenge-1".getBytes(UTF_8))).get();
        AuthData round2 = exchange.authenticateAsync(AuthData.of("challenge-2".getBytes(UTF_8))).get();

        // The counter advances 1 -> 2 only because both challenge rounds see the SAME state slot; a
        // per-call context (the bug this fix addresses) would reset it to 1 each round.
        assertThat(new String(round1.getBytes(), UTF_8)).isEqualTo("round-1");
        assertThat(new String(round2.getBytes(), UTF_8)).isEqualTo("round-2");
        assertThat(plugin.challengeCalls).isEqualTo(2);
    }

    @Test
    public void refreshSentinelRoutesToDataProviderNotChallengeHandler() throws Exception {
        CountingChallengePlugin plugin = new CountingChallengePlugin();
        V5ToV4AuthenticationAdapter adapter = wrap(plugin);
        adapter.start();

        AuthenticationExchange exchange = adapter.newAuthenticationExchange("broker-1.example.com");
        exchange.getAuthDataAsync().get();
        int challengeCallsBefore = plugin.challengeCalls;

        AuthData refreshed = exchange.authenticateAsync(AuthData.REFRESH_AUTH_DATA).get();

        // Refresh means "produce the current credential again": the challenge handler must NOT be called,
        // and BinaryAuthDataProvider is re-invoked instead.
        assertThat(plugin.challengeCalls).isEqualTo(challengeCallsBefore);
        assertThat(plugin.dataCalls).isEqualTo(2);
        assertThat(new String(refreshed.getBytes(), UTF_8)).isEqualTo("init");
    }

    @Test
    public void emptyChallengeRoutesToChallengeHandlerNotDataProvider() throws Exception {
        CountingChallengePlugin plugin = new CountingChallengePlugin();
        V5ToV4AuthenticationAdapter adapter = wrap(plugin);
        adapter.start();

        AuthenticationExchange exchange = adapter.newAuthenticationExchange("broker-1.example.com");
        // Seed the conversation state, then present an empty challenge payload.
        exchange.getAuthDataAsync().get();
        int dataCallsBefore = plugin.dataCalls;

        AuthData response = exchange.authenticateAsync(AuthData.of(new byte[0])).get();

        // An empty payload is a legitimate challenge round (PIP-478 rule 3), not the refresh sentinel: the
        // challenge handler answers it and BinaryAuthDataProvider is NOT re-invoked.
        assertThat(plugin.challengeCalls).isEqualTo(1);
        assertThat(plugin.dataCalls).isEqualTo(dataCallsBefore);
        assertThat(new String(response.getBytes(), UTF_8)).isEqualTo("round-1");
    }

    @Test
    public void startFailsLoudlyWhenBinaryCapabilityIsMissing() {
        // A v5 plugin exposing no capabilities cannot serve the binary transport; using it there is an
        // error (PIP-478 binary routing rule 1) — never a silent "none"/empty credential.
        Authentication noBinary = ctx -> CompletableFuture.completedFuture(null);
        V5ToV4AuthenticationAdapter adapter = wrap(noBinary);

        assertThatThrownBy(adapter::start).isInstanceOf(
                org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException.class);
    }

    @Test
    public void bridgesSinglePassHttpHeadersFromV5Capability() throws Exception {
        V5ToV4AuthenticationAdapter adapter = wrap(new HttpHeaderPlugin());
        adapter.start();

        AuthenticationDataProvider data = adapter.getAuthData("broker-1.example.com");

        // Without this bridge the v4 HTTP lookup path attaches no Authorization header and an
        // authenticated http:// lookup GET returns 401 (the PIP-478 regression this fix closes).
        assertThat(data.hasDataForHttp()).isTrue();
        Map<String, String> headers = new LinkedHashMap<>();
        data.getHttpHeaders().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        // HttpAuthHeaders canonicalises header names to lower case.
        assertThat(headers).containsEntry("authorization", "Bearer jwt-token");
    }

    @Test
    public void reportsNoHttpDataWhenPluginHasNoHttpHeaderCapability() throws Exception {
        // The mTLS plugin exposes only BinaryAuthDataProvider; it contributes no single-pass HTTP header,
        // so the v4 HTTP path must report none rather than attaching an empty/absent credential.
        V5ToV4AuthenticationAdapter adapter = wrap();
        adapter.start();

        AuthenticationDataProvider data = adapter.getAuthData("broker-1.example.com");

        assertThat(data.hasDataForHttp()).isFalse();
        assertThat(data.getHttpHeaders()).isNull();
    }

    @Test
    public void refusesSerializationWithActionableMessage() {
        assertThatThrownBy(() -> new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(wrap()))
                .isInstanceOf(NotSerializableException.class)
                .hasMessageContaining("authPluginClassName");
    }

    /**
     * A fake v5 challenge/response plugin that keeps a per-exchange round counter in the call-context
     * state slot, so a test can prove the same context (and slot) is reused across rounds.
     */
    private static final class CountingChallengePlugin
            implements Authentication, BinaryAuthDataProvider, BinaryAuthChallengeHandler {

        private static final class Counter {
            private int rounds;
        }

        private int dataCalls;
        private int challengeCalls;

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String authMethodName() {
            return "counting";
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            dataCalls++;
            ctx.setStateObject(Counter.class, new Counter());
            return CompletableFuture.completedFuture(new BinaryAuthData("init".getBytes(UTF_8)));
        }

        @Override
        public CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext ctx,
                AuthChallenge authChallenge) {
            challengeCalls++;
            Counter counter = ctx.getStateObject(Counter.class).orElseThrow(
                    () -> new IllegalStateException("conversation state did not persist across rounds"));
            counter.rounds++;
            return CompletableFuture.completedFuture(
                    new ChallengeResponse(("round-" + counter.rounds).getBytes(UTF_8)));
        }
    }

    /**
     * A fake v5 token-style plugin that serves the binary transport and a single-pass HTTP
     * {@code Authorization: Bearer <jwt>} header through the {@link HttpAuthHeadersProvider} capability.
     */
    private static final class HttpHeaderPlugin
            implements Authentication, BinaryAuthDataProvider, HttpAuthHeadersProvider {

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String authMethodName() {
            return "token";
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            return CompletableFuture.completedFuture(new BinaryAuthData("jwt-token".getBytes(UTF_8)));
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
            return CompletableFuture.completedFuture(HttpAuthHeaders.of("Authorization", "Bearer jwt-token"));
        }
    }
}
