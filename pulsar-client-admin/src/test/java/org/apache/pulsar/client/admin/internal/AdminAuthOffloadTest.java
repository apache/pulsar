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
package org.apache.pulsar.client.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.apache.pulsar.client.impl.auth.v5.AsyncHttpAuthenticationProvider;
import org.apache.pulsar.client.impl.auth.v5.HttpAuthenticationDriver;
import org.apache.pulsar.client.impl.auth.v5.HttpChallengeTransport;
import org.testng.annotations.Test;

/**
 * PIP-478: the admin's v4 authentication composition must not run on the thread that issued the admin
 * call.
 *
 * <p>{@code HttpClient} (the lookup path) has off-loaded the identical composition since the core
 * migration, naming the self-deadlock it avoids; {@code BaseResource} was the one place left where a
 * blocking {@code getAuthData()} — an OAuth2 or Athenz shim refreshing its credential over synchronous
 * HTTP — ran wherever the caller happened to be, which for a broker calling its own admin client is a
 * request-handling thread.
 *
 * <p>The same pre-request stage also decides which HTTP client the v5 SASL-style warmup rounds go over,
 * so the lending of the admin's challenge transport is pinned here too.
 */
public class AdminAuthOffloadTest {

    /**
     * A v4 plugin whose credential call records the thread it ran on, standing in for one that blocks
     * there.
     */
    private static class ThreadRecordingAuthentication implements Authentication {

        private final AtomicReference<Thread> getAuthDataThread = new AtomicReference<>();

        @Override
        public String getAuthMethodName() {
            return "thread-recording";
        }

        @Override
        @Deprecated
        public AuthenticationDataProvider getAuthData() {
            getAuthDataThread.set(Thread.currentThread());
            return new AuthenticationDataProvider() {
                @Override
                public boolean hasDataForHttp() {
                    return true;
                }

                @Override
                public Set<Map.Entry<String, String>> getHttpHeaders() {
                    return Set.of(Map.entry("X-Test", "value"));
                }
            };
        }

        @Override
        @Deprecated
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() throws PulsarClientException {
        }

        @Override
        public void close() {
        }
    }

    /**
     * A v4 plugin of the multi-round shape: it completes the authentication stage from a thread of its own,
     * the way a challenge-response plugin completes it from its HTTP callback thread, and records where the
     * second hook — {@code newRequestHeader} — then ran.
     */
    private static class AsyncStageAuthentication extends ThreadRecordingAuthentication {

        private final CompletableFuture<CompletableFuture<Map<String, String>>> stage = new CompletableFuture<>();
        private final AtomicReference<Thread> newRequestHeaderThread = new AtomicReference<>();

        @Override
        public void authenticationStage(String requestUrl, AuthenticationDataProvider authData,
                                        Map<String, String> previousResHeaders,
                                        CompletableFuture<Map<String, String>> authFuture) {
            // Hand the stage back to the test rather than completing it here, so the continuation is
            // registered before the completion happens. Completing it from a thread started here would race:
            // an already-completed stage runs a plain continuation on the *registering* thread, which is the
            // blocking executor, and the assertion below would then hold whether or not the hop exists.
            stage.complete(authFuture);
        }

        @Override
        public Set<Map.Entry<String, String>> newRequestHeader(String hostName,
                                                               AuthenticationDataProvider authData,
                                                               Map<String, String> previousResHeaders) {
            newRequestHeaderThread.set(Thread.currentThread());
            return Set.of(Map.entry("X-Test", "value"));
        }
    }

    /**
     * A v5-native body of the SASL-over-HTTP shape, reduced to what the driver needs: one challenge round,
     * then the server's role token replayed onto the real request. No Kerberos, no network.
     *
     * <p>The v5 {@code Authentication} stays fully qualified: this file is mostly about the v4 interface
     * of the same simple name, which the plugins above implement.
     */
    private static final class FakeChallengeBody implements org.apache.pulsar.client.api.v5.auth.Authentication,
            HttpAuthChallengeHandler, HttpAuthHeadersProvider {

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> respondToHttpChallengeAsync(HttpAuthCallContext ctx) {
            return CompletableFuture.completedFuture(HttpAuthHeaders.of("sasl-token", "client"));
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
            return CompletableFuture.completedFuture(HttpAuthHeaders.of("role-token",
                    ctx.serverChallengeHeaders().flatMap(headers -> headers.get("role-token")).orElse("none")));
        }
    }

    /** A transport that records the URIs it served and answers {@code 401} once, then {@code 200}. */
    private static final class RecordingTransport implements HttpChallengeTransport {
        final List<URI> uris = new ArrayList<>();

        @Override
        public CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout) {
            uris.add(uri);
            return CompletableFuture.completedFuture(uris.size() == 1
                    ? new Result(401, HttpAuthHeaders.of("server-state", "negotiate"))
                    : new Result(200, HttpAuthHeaders.of("role-token", "RT")));
        }
    }

    /**
     * A v4 plugin exposing a v5-native SASL-over-HTTP body, the shape {@code AuthenticationSasl} presents —
     * including a default transport of its own, which is the one that ignores the admin's TLS settings.
     */
    private static final class ChallengeAuthentication extends ThreadRecordingAuthentication
            implements AsyncHttpAuthenticationProvider {

        final RecordingTransport ownTransport = new RecordingTransport();
        private final HttpAuthenticationDriver driver =
                new HttpAuthenticationDriver(new FakeChallengeBody(), null, ownTransport);

        @Override
        public Optional<HttpAuthenticationDriver> httpAuthenticationDriver() {
            return Optional.of(driver);
        }
    }

    /** The minimal concrete resource needed to reach the protected header composition. */
    private static class TestResource extends BaseResource {
        TestResource(Authentication auth) {
            super(auth, 30_000L);
        }

        CompletableFuture<Map<String, String>> headers(URI uri) {
            return computeAuthHeaders(uri);
        }
    }

    @Test
    public void v4CredentialResolutionRunsOffTheCallerThread() throws Exception {
        ThreadRecordingAuthentication auth = new ThreadRecordingAuthentication();
        TestResource resource = new TestResource(auth);

        Map<String, String> headers = resource.headers(URI.create("http://broker.example:8080/admin/v2/clusters"))
                .get(30, TimeUnit.SECONDS);

        // The composition still produces the plugin's headers verbatim — off-loading must not change what
        // a third-party v4 plugin contributes.
        assertThat(headers).containsExactly(Map.entry("X-Test", "value"));
        assertThat(auth.getAuthDataThread.get())
                .as("the v4 credential must be resolved off the thread that issued the admin call")
                .isNotNull()
                .isNotSameAs(Thread.currentThread());
    }

    @Test
    public void aPluginContributingNoHttpDataStillYieldsNoHeaders() throws Exception {
        Authentication auth = new ThreadRecordingAuthentication() {
            @Override
            public AuthenticationDataProvider getAuthData() {
                return new AuthenticationDataProvider() {
                    @Override
                    public boolean hasDataForHttp() {
                        return false;
                    }
                };
            }
        };

        Map<String, String> headers = new TestResource(auth)
                .headers(URI.create("http://broker.example:8080/admin/v2/clusters")).get(30, TimeUnit.SECONDS);

        assertThat(headers).isNull();
    }

    /**
     * The composition belongs on the pool of the admin that owns the plugin, not on the framework's
     * process-wide fallback: a stalled identity provider reached by one admin must not occupy the threads
     * every other client in the JVM shares. Built through a real {@link PulsarAdminImpl} so what is pinned
     * is that the admin actually lends its pool to the resources it constructs, not merely that
     * {@link BaseResource} would use one if given it.
     */
    @Test
    public void theV4CompositionRunsOnTheOwningAdminsOwnPool() throws Exception {
        ThreadRecordingAuthentication auth = new ThreadRecordingAuthentication();
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://broker.example:8080")
                .authentication(auth)
                .build()) {
            // Same package, so the protected composition is reachable on the admin's own resource. No request
            // is issued: computeAuthHeaders is the pre-request stage.
            BaseResource resource = (BaseResource) admin.clusters();
            resource.computeAuthHeaders(URI.create("http://broker.example:8080/admin/v2/clusters"))
                    .get(30, TimeUnit.SECONDS);

            assertThat(auth.getAuthDataThread.get()).isNotNull();
            assertThat(auth.getAuthDataThread.get().getName())
                    .as("the v4 credential must resolve on the admin's own bounded pool")
                    .startsWith("pulsar-admin-auth-blocking");
        }
    }

    /**
     * Lending is opt-in per construction site, so a resource added to {@link PulsarAdminImpl} later without
     * the wrapper would compile, run, and silently fall back to the shared pool and to the plugin's own
     * HTTP client — the fix undone with no symptom, in the transport's case until someone puts a private
     * CA in front of the broker. Walking the admin's own accessors is what turns that into a test failure;
     * pinning one resource would not, since the twenty-fourth is the one that would be missed.
     */
    @Test
    public void everyResourceTheAdminExposesIsLentTheAdminsState() throws Exception {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://broker.example:8080").build()) {
            Map<String, BaseResource> resources = new LinkedHashMap<>();
            for (Method accessor : PulsarAdmin.class.getMethods()) {
                if (accessor.getParameterCount() != 0 || Modifier.isStatic(accessor.getModifiers())
                        || accessor.getReturnType() == void.class) {
                    continue;
                }
                if (accessor.invoke(admin) instanceof BaseResource resource) {
                    resources.put(accessor.getName(), resource);
                }
            }
            // The one resource accessor that takes an argument, so the loop above cannot reach it.
            resources.put("topicPolicies(true)", (BaseResource) admin.topicPolicies(true));

            Executor pool = ((PulsarAdminImpl) admin).blockingAuthExecutorForTest();
            Set<HttpChallengeTransport> transports = Collections.newSetFromMap(new IdentityHashMap<>());
            List<String> notLentThePool = new ArrayList<>();
            List<String> notLentATransport = new ArrayList<>();
            resources.forEach((name, resource) -> {
                if (resource.blockingAuthExecutorForTest() != pool) {
                    notLentThePool.add(name);
                }
                if (resource.challengeTransportForTest() == null) {
                    notLentATransport.add(name);
                } else {
                    transports.add(resource.challengeTransportForTest());
                }
            });

            assertThat(notLentThePool).as("admin resources left on the shared pool").isEmpty();
            assertThat(notLentATransport).as("admin resources left on the plugin's own HTTP client").isEmpty();
            // One transport, not one per resource: it is the admin's single AsyncHttpClient that carries the
            // configuration, so a second instance would mean a second place for the pairing to drift.
            assertThat(transports).as("the admin's resources must share one transport").hasSize(1);
            assertThat(resources)
                    .as("the walk must actually reach the admin's resources, not silently filter them all out")
                    .hasSizeGreaterThanOrEqualTo(20);
        }
    }

    /**
     * The SASL warmup rounds must go over the admin's own HTTP client. The built-in plugin's default
     * transport is a bare {@code ClientBuilder.newClient()}, which knows nothing of the admin's trust
     * store, so against a broker behind a private CA the warmup fails where the real request would have
     * succeeded. The headers come out the same either way, so the transport identity is what pins it.
     */
    @Test
    public void theSaslWarmupRunsOverTheLentTransport() throws Exception {
        URI uri = URI.create("https://broker.example:8443/admin/v2/clusters");
        ChallengeAuthentication auth = new ChallengeAuthentication();
        RecordingTransport lent = new RecordingTransport();
        TestResource resource = new TestResource(auth);
        resource.setChallengeTransport(lent);

        assertThat(resource.headers(uri).get(30, TimeUnit.SECONDS))
                .containsExactly(Map.entry("role-token", "RT"));
        assertThat(lent.uris).as("the lent transport must serve every challenge round")
                .containsExactly(uri, uri);
        assertThat(auth.ownTransport.uris)
                .as("the plugin's own HTTP client must not be used once the admin lent a transport")
                .isEmpty();
    }

    /**
     * A resource built outside a {@link PulsarAdminImpl} — a test, an embedder — has no transport to
     * borrow, and must keep authenticating over the plugin's own HTTP client rather than failing.
     */
    @Test
    public void aResourceWithNoLentTransportFallsBackToThePlugin() throws Exception {
        URI uri = URI.create("https://broker.example:8443/admin/v2/clusters");
        ChallengeAuthentication auth = new ChallengeAuthentication();

        assertThat(new TestResource(auth).headers(uri).get(30, TimeUnit.SECONDS))
                .containsExactly(Map.entry("role-token", "RT"));
        assertThat(auth.ownTransport.uris).containsExactly(uri, uri);
    }

    /**
     * {@code newRequestHeader} is the second synchronous v4 hook. A plugin that completes the stage
     * asynchronously completes it from a thread of its own — its HTTP callback thread — and a plain
     * continuation would run the hook there, so off-loading the credential resolution alone would leave half
     * the composition where PIP-478 says it must not be.
     */
    @Test
    public void theNewRequestHeaderHookIsOffLoadedToo() throws Exception {
        AsyncStageAuthentication auth = new AsyncStageAuthentication();
        ExecutorService lentPool = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "lent-auth-pool"));
        try {
            TestResource resource = new TestResource(auth);
            resource.setBlockingAuthExecutor(lentPool);

            CompletableFuture<Map<String, String>> composed =
                    resource.headers(URI.create("http://broker.example:8080/admin/v2/clusters"));

            // The plugin's own thread completes the stage, standing in for its HTTP callback thread.
            CompletableFuture<Map<String, String>> stage = auth.stage.get(30, TimeUnit.SECONDS);
            Thread completer = new Thread(() -> stage.complete(Map.of()), "stage-completer");
            completer.start();
            completer.join();

            assertThat(composed.get(30, TimeUnit.SECONDS)).containsExactly(Map.entry("X-Test", "value"));
            assertThat(auth.newRequestHeaderThread.get())
                    .as("the second v4 hook must not run on the thread the plugin completed the stage from")
                    .isNotNull()
                    .isNotSameAs(completer);
            assertThat(auth.newRequestHeaderThread.get().getName()).isEqualTo("lent-auth-pool");
        } finally {
            lentPool.shutdownNow();
        }
    }
}
