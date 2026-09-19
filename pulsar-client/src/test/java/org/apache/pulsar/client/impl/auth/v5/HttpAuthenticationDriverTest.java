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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 3d: the shared {@link HttpAuthenticationDriver} state machine, exercised with a fake
 * challenge handler and a fake transport so the round-counting, bounds, error propagation, GET re-issue,
 * and role-token replay are validated without any network, JAX-RS, or SASL/Kerberos machinery.
 */
public class HttpAuthenticationDriverTest {

    private static final URI URI_A = URI.create("https://broker-a.example:8443/lookup/v2/topic/x");

    @Test
    public void multiRoundReachesOkAndReplaysRoleToken() throws Exception {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        FakeTransport transport = new FakeTransport();
        transport.script(unauthorized("s1"), unauthorized("s2"), ok("ROLE-XYZ"));

        HttpAuthenticationDriver driver = new HttpAuthenticationDriver(auth, null, null);
        assertThat(driver.supportsHttpChallenge()).isTrue();

        HttpAuthHeaders result = driver.authenticateAsync(URI_A, transport, Duration.ofSeconds(30)).get();

        // The validated role token is replayed onto the real request.
        assertThat(result.get("role-token")).contains("ROLE-XYZ");
        // Three challenge rounds ran; the same state-slot conversation accumulated all three, then survived
        // into getHttpHeadersAsync (rounds-seen == 3 proves cross-round + into-final slot persistence).
        assertThat(result.get("rounds-seen")).contains("3");
        assertThat(auth.respondCalls).isEqualTo(3);
        assertThat(transport.callCount).isEqualTo(3);
        // Every round was re-issued as a GET to the original URI (the v4 SASL takeover preserved).
        assertThat(transport.uris).allMatch(URI_A::equals);
        // The first round saw no server challenge; later rounds saw the prior response's server-state.
        assertThat(auth.serverStatesSeen).containsExactly("none", "s1", "s2");
    }

    @Test
    public void singleRoundImmediateOk() throws Exception {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        FakeTransport transport = new FakeTransport();
        transport.script(ok("RT-1"));

        HttpAuthHeaders result = new HttpAuthenticationDriver(auth, null, null)
                .authenticateAsync(URI_A, transport, Duration.ofSeconds(30)).get();

        assertThat(result.get("role-token")).contains("RT-1");
        assertThat(transport.callCount).isEqualTo(1);
    }

    @Test
    public void maxRoundsAbort() {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        FakeTransport transport = new FakeTransport();
        transport.always(unauthorized("loop")); // a server that never completes the exchange

        assertThatThrownBy(() -> new HttpAuthenticationDriver(auth, null, null)
                .authenticateAsync(URI_A, transport, Duration.ofSeconds(30)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PulsarClientException.AuthenticationException.class)
                .hasMessageContaining("maximum of " + HttpAuthenticationDriver.MAX_CHALLENGE_ROUNDS);

        // The cap bounds the total number of requests sent.
        assertThat(transport.callCount).isEqualTo(HttpAuthenticationDriver.MAX_CHALLENGE_ROUNDS);
    }

    @Test
    public void timeoutBudgetAbortBeforeAnyRequest() {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        FakeTransport transport = new FakeTransport();
        transport.always(ok("RT"));

        // A zero budget is already exhausted by the time the first round checks the deadline.
        assertThatThrownBy(() -> new HttpAuthenticationDriver(auth, null, null)
                .authenticateAsync(URI_A, transport, Duration.ZERO).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PulsarClientException.AuthenticationException.class)
                .hasMessageContaining("timeout budget");

        assertThat(transport.callCount).isZero();
        assertThat(auth.respondCalls).isZero();
    }

    @Test
    public void hangingTransportRoundIsBoundedByTheRemainingBudget() {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        // A transport whose underlying client applies no read timeout: the future never completes.
        HttpChallengeTransport hanging = (uri, headers, timeout) -> new CompletableFuture<>();

        assertThatThrownBy(() -> new HttpAuthenticationDriver(auth, null, null)
                .authenticateAsync(URI_A, hanging, Duration.ofMillis(200)).get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PulsarClientException.AuthenticationException.class)
                .hasMessageContaining("timed out");
    }

    @Test
    public void hangingChallengeEvaluationIsBoundedByTheRemainingBudget() {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        auth.hangOnRespond = true; // the plugin's challenge evaluation never completes
        FakeTransport transport = new FakeTransport();
        transport.always(ok("RT")); // the transport would answer instantly, but the round never reaches it

        assertThatThrownBy(() -> new HttpAuthenticationDriver(auth, null, null)
                .authenticateAsync(URI_A, transport, Duration.ofMillis(200)).get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PulsarClientException.AuthenticationException.class)
                .hasMessageContaining("timed out");

        // The GET is never issued: the hang is in the evaluation stage, before the transport is reached.
        assertThat(transport.callCount).isZero();
    }

    @Test
    public void pluginFailurePropagatesWithoutRetry() {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        auth.failOnRound = 2; // round 0 succeeds (-> 401), round 1's respond throws
        FakeTransport transport = new FakeTransport();
        transport.script(unauthorized("s1"), unauthorized("s2"), ok("RT"));

        assertThatThrownBy(() -> new HttpAuthenticationDriver(auth, null, null)
                .authenticateAsync(URI_A, transport, Duration.ofSeconds(30)).get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("plugin boom on round 2");

        // Exactly one request was sent (round 0); the plugin failure fails the request with no driver retry.
        assertThat(transport.callCount).isEqualTo(1);
    }

    @Test
    public void nonOkNonUnauthorizedStatusFails() {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        FakeTransport transport = new FakeTransport();
        transport.script(new HttpChallengeTransport.Result(500, HttpAuthHeaders.empty()));

        assertThatThrownBy(() -> new HttpAuthenticationDriver(auth, null, null)
                .authenticateAsync(URI_A, transport, Duration.ofSeconds(30)).get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("failed with status 500");
        assertThat(transport.callCount).isEqualTo(1);
    }

    @Test
    public void defaultTransportUsedWhenCallerSuppliesNone() throws Exception {
        FakeChallengeAuth auth = new FakeChallengeAuth();
        FakeTransport defaultTransport = new FakeTransport();
        defaultTransport.script(ok("RT-DEFAULT"));

        HttpAuthHeaders result = new HttpAuthenticationDriver(auth, null, defaultTransport)
                .authenticateAsync(URI_A, null, Duration.ofSeconds(30)).get();

        assertThat(result.get("role-token")).contains("RT-DEFAULT");
        assertThat(defaultTransport.callCount).isEqualTo(1);
    }

    @Test
    public void pluginWithoutChallengeCapabilityIsRejected() {
        Authentication noChallenge = new Authentication() {
            @Override
            public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
                return CompletableFuture.completedFuture(null);
            }
        };
        HttpAuthenticationDriver driver = new HttpAuthenticationDriver(noChallenge, null, null);
        assertThat(driver.supportsHttpChallenge()).isFalse();

        assertThatThrownBy(() -> driver.authenticateAsync(URI_A, new FakeTransport(), Duration.ofSeconds(30)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PulsarClientException.UnsupportedAuthenticationException.class);
    }

    // ---- fakes ----

    private static HttpChallengeTransport.Result unauthorized(String serverState) {
        return new HttpChallengeTransport.Result(401, HttpAuthHeaders.of("server-state", serverState));
    }

    private static HttpChallengeTransport.Result ok(String roleToken) {
        return new HttpChallengeTransport.Result(200, HttpAuthHeaders.of("role-token", roleToken));
    }

    /** A fake SASL-style handler: tracks rounds, accumulates cross-round state in the call-context slot. */
    private static final class FakeChallengeAuth
            implements Authentication, HttpAuthChallengeHandler, HttpAuthHeadersProvider {
        int respondCalls;
        int failOnRound = -1;
        boolean hangOnRespond;
        final List<String> serverStatesSeen = new ArrayList<>();

        private static final class Conversation {
            final List<String> serverStates = new ArrayList<>();
        }

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> respondToHttpChallengeAsync(HttpAuthCallContext ctx) {
            respondCalls++;
            if (hangOnRespond) {
                return new CompletableFuture<>(); // the challenge evaluation never completes
            }
            Conversation conv = ctx.getStateObject(Conversation.class).orElse(null);
            if (conv == null) {
                conv = new Conversation();
                ctx.setStateObject(Conversation.class, conv);
            }
            String serverState = ctx.serverChallengeHeaders().flatMap(h -> h.get("server-state")).orElse("none");
            conv.serverStates.add(serverState);
            serverStatesSeen.add(serverState);
            if (respondCalls == failOnRound) {
                return CompletableFuture.failedFuture(new RuntimeException("plugin boom on round " + respondCalls));
            }
            return CompletableFuture.completedFuture(
                    HttpAuthHeaders.of("x-client-round", String.valueOf(respondCalls)));
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
            String roleToken = ctx.serverChallengeHeaders().flatMap(h -> h.get("role-token")).orElse(null);
            if (roleToken == null) {
                return CompletableFuture.failedFuture(new RuntimeException("no role token in terminal response"));
            }
            Conversation conv = ctx.getStateObject(Conversation.class).orElse(null);
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put("role-token", roleToken);
            headers.put("rounds-seen", String.valueOf(conv == null ? -1 : conv.serverStates.size()));
            return CompletableFuture.completedFuture(HttpAuthHeaders.of(headers));
        }
    }

    /** A fake transport that returns scripted responses and records the requests it saw. */
    private static final class FakeTransport implements HttpChallengeTransport {
        final Deque<Result> scripted = new ArrayDeque<>();
        Result fallback;
        int callCount;
        final List<URI> uris = new ArrayList<>();
        final List<HttpAuthHeaders> requestHeaders = new ArrayList<>();

        void script(Result... results) {
            for (Result r : results) {
                scripted.add(r);
            }
        }

        void always(Result result) {
            this.fallback = result;
        }

        @Override
        public CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout) {
            callCount++;
            uris.add(uri);
            this.requestHeaders.add(requestHeaders);
            Result r = Optional.ofNullable(scripted.poll()).orElse(fallback);
            if (r == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("no scripted response"));
            }
            return CompletableFuture.completedFuture(r);
        }
    }
}
