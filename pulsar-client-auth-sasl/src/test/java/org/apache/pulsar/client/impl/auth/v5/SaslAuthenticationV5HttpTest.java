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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN_EXPIRED;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_STATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_TYPE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_CLIENT_INIT;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_COMPLETE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_NEGOTIATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER_CHECK_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_TYPE_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 3d: drive the real {@link SaslAuthenticationV5} SASL-over-HTTP body through the real
 * {@link HttpAuthenticationDriver}, backed by a fake client-side SASL provider and a fake server transport
 * that mimics {@code AuthenticationProviderSasl.authenticateHttpRequest}. This exercises the ported header
 * state machine ({@code Init} → {@code ING} → {@code Done}, {@code SASL-Token} / {@code SASL-Server-ID} /
 * role token) and, crucially, the state-slot persistence: one {@code PulsarSaslClient} conversation must
 * span every round of one exchange and survive into the final role-token replay — no Kerberos or network.
 */
public class SaslAuthenticationV5HttpTest {

    private static final URI ADMIN_URI = URI.create("https://broker.example:8443/admin/v2/tenants/x");

    @Test
    public void twoRoundExchangeReplaysRoleToken() throws Exception {
        CountingFactory factory = new CountingFactory();
        FakeSaslServer server = new FakeSaslServer(2, "role-token-abc");
        HttpAuthenticationDriver driver =
                new HttpAuthenticationDriver(new SaslAuthenticationV5(factory), null, server);

        HttpAuthHeaders realRequestHeaders = driver.authenticateAsync(ADMIN_URI, null, Duration.ofSeconds(30)).get();

        // The real request replays the validated role token with State=Done and the SASL type header.
        assertThat(realRequestHeaders.get(SASL_AUTH_ROLE_TOKEN)).hasValue("role-token-abc");
        assertThat(realRequestHeaders.get(SASL_HEADER_STATE)).hasValue(SASL_STATE_COMPLETE);
        assertThat(realRequestHeaders.get(SASL_HEADER_TYPE)).hasValue(SASL_TYPE_VALUE);

        // Exactly one PulsarSaslClient conversation was created for the whole exchange (state-slot persistence)
        // and it advanced twice — the INIT frame plus one negotiation round.
        assertThat(factory.createCalls).isEqualTo(1);
        assertThat(factory.created.get(0).authenticateCalls).isEqualTo(2);

        // The two warmup rounds were bodiless GETs to the original URI, carrying Init then ING.
        assertThat(server.uris).hasSize(2).allMatch(ADMIN_URI::equals);
        assertThat(server.requestStates).containsExactly(SASL_STATE_CLIENT_INIT, SASL_STATE_NEGOTIATE);

        // Round 1 carried the initial SASL token; round 2 echoed the server id and carried the next token.
        assertThat(server.requests.get(0).get(SASL_AUTH_TOKEN)).isPresent();
        assertThat(server.requests.get(0).get(SASL_HEADER_TYPE)).hasValue(SASL_TYPE_VALUE);
        assertThat(server.requests.get(1).get(SASL_STATE_SERVER)).hasValue("1");
        assertThat(server.requests.get(1).get(SASL_AUTH_TOKEN)).isPresent();
    }

    @Test
    public void singleRoundExchangeCompletesImmediately() throws Exception {
        CountingFactory factory = new CountingFactory();
        FakeSaslServer server = new FakeSaslServer(1, "rt-1");
        HttpAuthenticationDriver driver =
                new HttpAuthenticationDriver(new SaslAuthenticationV5(factory), null, server);

        HttpAuthHeaders realRequestHeaders = driver.authenticateAsync(ADMIN_URI, null, Duration.ofSeconds(30)).get();

        assertThat(realRequestHeaders.get(SASL_AUTH_ROLE_TOKEN)).hasValue("rt-1");
        assertThat(realRequestHeaders.get(SASL_HEADER_STATE)).hasValue(SASL_STATE_COMPLETE);
        assertThat(factory.createCalls).isEqualTo(1);
        assertThat(factory.created.get(0).authenticateCalls).isEqualTo(1);
        assertThat(server.requestStates).containsExactly(SASL_STATE_CLIENT_INIT);
    }

    @Test
    public void secondRequestReplaysCachedRoleTokenWithServerCheckToken() throws Exception {
        // PIP-478 FIX C: the framework HTTP driver reuses ONE body across requests. The first request runs a
        // full negotiation and caches the role token on the body; the second request must replay that cached
        // token with State=ServerCheckToken instead of restarting a fresh Kerberos Init negotiation (the v4
        // AuthenticationSasl cross-request cache behavior).
        CountingFactory factory = new CountingFactory();
        RoleTokenServer server = new RoleTokenServer("role-token-abc");
        HttpAuthenticationDriver driver =
                new HttpAuthenticationDriver(new SaslAuthenticationV5(factory), null, server);

        // First request: full negotiation issues and caches the role token.
        HttpAuthHeaders first = driver.authenticateAsync(ADMIN_URI, null, Duration.ofSeconds(30)).get();
        assertThat(first.get(SASL_AUTH_ROLE_TOKEN)).hasValue("role-token-abc");
        assertThat(server.requestStates).containsExactly(SASL_STATE_CLIENT_INIT);

        server.reset();

        // Second request on the SAME driver/body: replayed with ServerCheckToken, NOT a fresh Init.
        HttpAuthHeaders second = driver.authenticateAsync(ADMIN_URI, null, Duration.ofSeconds(30)).get();
        assertThat(second.get(SASL_AUTH_ROLE_TOKEN)).hasValue("role-token-abc");
        assertThat(second.get(SASL_HEADER_STATE)).hasValue(SASL_STATE_COMPLETE);
        assertThat(server.requestStates).containsExactly(SASL_STATE_SERVER_CHECK_TOKEN);
        // The single replayed warmup round carried the cached role token — no Init/negotiation round.
        assertThat(server.requests.get(0).get(SASL_AUTH_ROLE_TOKEN)).hasValue("role-token-abc");
    }

    @Test
    public void expiredCachedRoleTokenTriggersFreshNegotiation() throws Exception {
        // PIP-478 FIX C: when the server reports the replayed cached token expired (SaslAuthRoleTokenExpired),
        // the body drops the cache and renegotiates from Init, ending with a freshly issued role token.
        CountingFactory factory = new CountingFactory();
        RoleTokenServer server = new RoleTokenServer("role-token-1");
        HttpAuthenticationDriver driver =
                new HttpAuthenticationDriver(new SaslAuthenticationV5(factory), null, server);

        // First request caches role-token-1.
        driver.authenticateAsync(ADMIN_URI, null, Duration.ofSeconds(30)).get();

        server.reset();
        server.expireNextCheckThenIssue("role-token-2");

        // Second request: ServerCheckToken -> server says EXPIRED -> renegotiate from Init -> role-token-2.
        HttpAuthHeaders second = driver.authenticateAsync(ADMIN_URI, null, Duration.ofSeconds(30)).get();
        assertThat(second.get(SASL_AUTH_ROLE_TOKEN)).hasValue("role-token-2");
        assertThat(second.get(SASL_HEADER_STATE)).hasValue(SASL_STATE_COMPLETE);
        assertThat(server.requestStates).containsExactly(SASL_STATE_SERVER_CHECK_TOKEN, SASL_STATE_CLIENT_INIT);
    }

    // ---- fakes ----

    /** The client-side SASL data provider: deterministic tokens, no Kerberos; counts advances. */
    private static final class FakeSaslClient implements AuthenticationDataProvider {
        private static final long serialVersionUID = 1L;
        int authenticateCalls;

        @Override
        public boolean hasDataForHttp() {
            return true;
        }

        @Override
        public Set<Entry<String, String>> getHttpHeaders() {
            return Collections.singletonMap(SASL_HEADER_TYPE, SASL_TYPE_VALUE).entrySet();
        }

        @Override
        public AuthData authenticate(AuthData commandData) {
            authenticateCalls++;
            return AuthData.of(("client-token-" + authenticateCalls).getBytes(UTF_8));
        }
    }

    private static final class CountingFactory implements SaslAuthenticationV5.SaslProviderFactory {
        private static final long serialVersionUID = 1L;
        int createCalls;
        final List<FakeSaslClient> created = new ArrayList<>();

        @Override
        public AuthenticationDataProvider create(String brokerHost) {
            createCalls++;
            FakeSaslClient client = new FakeSaslClient();
            created.add(client);
            return client;
        }
    }

    /**
     * A fake transport that mimics the broker's SASL-over-HTTP servlet: it {@code 401}s with a NEGOTIATE
     * challenge until {@code roundsToComplete} client tokens have arrived, then {@code 200}s with a role
     * token. Only the warmup rounds pass through here — the driver's final role-token request goes back to
     * the caller, not the transport.
     */
    private static final class FakeSaslServer implements HttpChallengeTransport {
        private final int roundsToComplete;
        private final String issuedRoleToken;
        private int serverStep;
        final List<URI> uris = new ArrayList<>();
        final List<String> requestStates = new ArrayList<>();
        final List<HttpAuthHeaders> requests = new ArrayList<>();

        FakeSaslServer(int roundsToComplete, String issuedRoleToken) {
            this.roundsToComplete = roundsToComplete;
            this.issuedRoleToken = issuedRoleToken;
        }

        @Override
        public CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout) {
            uris.add(uri);
            requests.add(requestHeaders);
            String state = requestHeaders.get(SASL_HEADER_STATE).orElse(null);
            requestStates.add(state);
            if (SASL_STATE_CLIENT_INIT.equalsIgnoreCase(state)) {
                serverStep = 1;
            } else if (SASL_STATE_NEGOTIATE.equalsIgnoreCase(state)) {
                serverStep++;
            } else {
                return CompletableFuture.completedFuture(new Result(400, HttpAuthHeaders.empty()));
            }
            if (serverStep >= roundsToComplete) {
                Map<String, String> headers = new LinkedHashMap<>();
                headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
                headers.put(SASL_HEADER_STATE, SASL_STATE_COMPLETE);
                headers.put(SASL_STATE_SERVER, "1");
                headers.put(SASL_AUTH_ROLE_TOKEN, issuedRoleToken);
                return CompletableFuture.completedFuture(new Result(200, HttpAuthHeaders.of(headers)));
            }
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
            headers.put(SASL_HEADER_STATE, SASL_STATE_NEGOTIATE);
            headers.put(SASL_STATE_SERVER, "1");
            headers.put(SASL_AUTH_TOKEN, Base64.getEncoder().encodeToString(("server-" + serverStep).getBytes(UTF_8)));
            return CompletableFuture.completedFuture(new Result(401, HttpAuthHeaders.of(headers)));
        }
    }

    /**
     * A fake server that also honors the role-token replay path (State=ServerCheckToken), used to exercise
     * the FIX C cross-request cache. Init negotiation completes in one round. A ServerCheckToken is answered
     * {@code 200} (validated) unless {@link #expireNextCheckThenIssue} armed an expiry, in which case the
     * first ServerCheckToken is answered {@code 401} EXPIRED and the following Init issues the rotated token.
     */
    private static final class RoleTokenServer implements HttpChallengeTransport {
        final List<String> requestStates = new ArrayList<>();
        final List<HttpAuthHeaders> requests = new ArrayList<>();
        private String roleToken;
        private boolean expireNextCheck;
        private String rotatedRoleToken;

        RoleTokenServer(String roleToken) {
            this.roleToken = roleToken;
        }

        void reset() {
            requestStates.clear();
            requests.clear();
        }

        void expireNextCheckThenIssue(String rotatedRoleToken) {
            this.expireNextCheck = true;
            this.rotatedRoleToken = rotatedRoleToken;
        }

        @Override
        public CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout) {
            requests.add(requestHeaders);
            String state = requestHeaders.get(SASL_HEADER_STATE).orElse(null);
            requestStates.add(state);
            if (SASL_STATE_SERVER_CHECK_TOKEN.equalsIgnoreCase(state)) {
                if (expireNextCheck) {
                    expireNextCheck = false;
                    Map<String, String> headers = new LinkedHashMap<>();
                    headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
                    headers.put(SASL_HEADER_STATE, SASL_AUTH_ROLE_TOKEN_EXPIRED);
                    return CompletableFuture.completedFuture(new Result(401, HttpAuthHeaders.of(headers)));
                }
                return complete(roleToken);
            }
            if (SASL_STATE_CLIENT_INIT.equalsIgnoreCase(state)) {
                if (rotatedRoleToken != null) {
                    roleToken = rotatedRoleToken;
                    rotatedRoleToken = null;
                }
                return complete(roleToken);
            }
            return CompletableFuture.completedFuture(new Result(400, HttpAuthHeaders.empty()));
        }

        private CompletableFuture<Result> complete(String issuedRoleToken) {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
            headers.put(SASL_HEADER_STATE, SASL_STATE_COMPLETE);
            headers.put(SASL_STATE_SERVER, "1");
            headers.put(SASL_AUTH_ROLE_TOKEN, issuedRoleToken);
            return CompletableFuture.completedFuture(new Result(200, HttpAuthHeaders.of(headers)));
        }
    }
}
