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
package org.apache.pulsar.proxy.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.auth.v5.BinaryAuthenticationDriver;
import org.apache.pulsar.client.impl.auth.v5.V5AuthenticationLoader;
import org.apache.pulsar.client.impl.auth.v5.V5BinaryAuthenticationDriver;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.FeatureFlags;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: the proxy's broker-client credential must not be resolved on the Netty event loop, and the
 * exchange it is resolved through must have its rounds serialized and bounded.
 *
 * <p>Both properties live entirely inside {@code DirectProxyHandler.ProxyBackendHandler}, so they are pinned
 * here against an {@link EmbeddedChannel} rather than through a running proxy: an embedded event loop only
 * runs the tasks submitted to it when {@code runPendingTasks()} is called, which makes "did this run on the
 * event loop or off it" an assertion rather than a race. An end-to-end proxy fixture can observe neither —
 * the handshake completes the same way whichever thread the plugin was called on.
 */
public class DirectProxyHandlerAuthTest {

    private static final String AUTH_METHOD = "thread-recording";
    private static final String BROKER_HOST = "broker.example:6650";

    private ProxyConfiguration proxyConfig;
    private ProxyService service;
    private ProxyConnection proxyConnection;
    private EmbeddedChannel inboundChannel;
    private EmbeddedChannel backendChannel;
    private ThreadRecordingAuthentication plugin;

    /**
     * A v4 plugin that records the thread each of its credential calls runs on, standing in for one that
     * blocks there — an OAuth2 token endpoint round trip, an Athenz ZTS fetch, a GSSAPI exchange with the KDC.
     * It answers challenges too, so a challenge round reaches {@code authenticate} a second time.
     */
    private static final class ThreadRecordingAuthentication implements Authentication {

        private final List<String> credentialThreads = new CopyOnWriteArrayList<>();

        @Override
        public String getAuthMethodName() {
            return AUTH_METHOD;
        }

        @Override
        public AuthenticationDataProvider getAuthData(String brokerHostName) {
            return new AuthenticationDataProvider() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public AuthData authenticate(AuthData data) {
                    credentialThreads.add(Thread.currentThread().getName());
                    return AuthData.of("credential".getBytes(UTF_8));
                }
            };
        }

        @SuppressWarnings("deprecation")
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

    /**
     * A driver whose rounds the test completes by hand, for the guards that are about <em>when</em> a round is
     * serviced rather than about which thread services it.
     */
    private static final class ScriptedDriver implements BinaryAuthenticationDriver {

        private final Supplier<CompletableFuture<AuthData>> connectCredential;
        private final Supplier<CompletableFuture<AuthData>> challengeResponse;
        private final AtomicInteger challengeRounds = new AtomicInteger();

        private ScriptedDriver(Supplier<CompletableFuture<AuthData>> connectCredential,
                               Supplier<CompletableFuture<AuthData>> challengeResponse) {
            this.connectCredential = connectCredential;
            this.challengeResponse = challengeResponse;
        }

        @Override
        public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
            return new AuthenticationExchange() {
                @Override
                public CompletableFuture<AuthData> getAuthDataAsync() {
                    return connectCredential.get();
                }

                @Override
                public String authMethodName() {
                    return AUTH_METHOD;
                }

                @Override
                public CompletableFuture<AuthData> authenticateAsync(AuthData challenge) {
                    challengeRounds.incrementAndGet();
                    return challengeResponse.get();
                }
            };
        }
    }

    @BeforeMethod
    public void setUp() {
        proxyConfig = new ProxyConfiguration();
        plugin = new ThreadRecordingAuthentication();
        inboundChannel = new EmbeddedChannel();

        ChannelHandlerContext inboundCtx = mock(ChannelHandlerContext.class);
        when(inboundCtx.channel()).thenReturn(inboundChannel);

        service = mock(ProxyService.class);
        when(service.getConfiguration()).thenReturn(proxyConfig);

        proxyConnection = mock(ProxyConnection.class);
        when(proxyConnection.ctx()).thenReturn(inboundCtx);
        when(proxyConnection.getClientAuthentication()).thenReturn(plugin);
        proxyConnection.clientVersion = "test-client";
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (backendChannel != null) {
            backendChannel.finishAndReleaseAll();
        }
        inboundChannel.finishAndReleaseAll();
    }

    /**
     * Build the backend handler on an embedded channel and activate it, which is what starts the connect
     * round. The outbound channel is published before registration so the handler has somewhere to write.
     *
     * @param driver the authentication driver the handler resolves its credential through
     * @return the backend handler under test
     * @throws Exception if activating the channel fails
     */
    private DirectProxyHandler.ProxyBackendHandler activateBackend(BinaryAuthenticationDriver driver)
            throws Exception {
        when(service.getProxyClientAuthenticationDriver()).thenReturn(driver);
        DirectProxyHandler directProxyHandler = new DirectProxyHandler(service, proxyConnection);
        DirectProxyHandler.ProxyBackendHandler backend = directProxyHandler.new ProxyBackendHandler(
                proxyConfig, Commands.getCurrentProtocolVersion(), BROKER_HOST, new FeatureFlags());
        backendChannel = new EmbeddedChannel(false, false, backend);
        directProxyHandler.outboundChannel = backendChannel;
        backendChannel.register();
        return backend;
    }

    private BinaryAuthenticationDriver realDriver() {
        return new V5BinaryAuthenticationDriver(V5AuthenticationLoader.forStartedV4Plugin(plugin));
    }

    private static CommandAuthChallenge challenge(String payload) {
        CommandAuthChallenge command = new CommandAuthChallenge();
        command.setProtocolVersion(Commands.getCurrentProtocolVersion())
                .setChallenge()
                .setAuthData(payload.getBytes(UTF_8))
                .setAuthMethodName(AUTH_METHOD);
        return command;
    }

    /** Drain the embedded loop until the handler has written the command it owes, or fail. */
    private void awaitCommandWritten(int expectedCommands) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            backendChannel.runPendingTasks();
            assertThat(backendChannel.outboundMessages()).hasSize(expectedCommands);
        });
    }

    @Test
    public void theConnectCredentialIsResolvedOffTheEventLoop() throws Exception {
        activateBackend(realDriver());
        awaitCommandWritten(1);

        assertThat(plugin.credentialThreads)
                .as("the v4 credential for CommandConnect must not be resolved on the channel's event loop")
                .singleElement().asString().startsWith("pulsar-auth-blocking-shared");
    }

    /**
     * The second half of the composition. Off-loading only the connect credential would leave every challenge
     * round — a SASL exchange with the KDC, say — running on the loop that carries all of this proxy's
     * multiplexed connections.
     */
    @Test
    public void theChallengeRoundIsResolvedOffTheEventLoop() throws Exception {
        DirectProxyHandler.ProxyBackendHandler backend = activateBackend(realDriver());
        awaitCommandWritten(1);

        backend.handleAuthChallenge(challenge("server-round-1"));
        awaitCommandWritten(2);

        assertThat(plugin.credentialThreads)
                .as("both v4 credential calls must be resolved off the channel's event loop")
                .hasSize(2)
                .allSatisfy(thread -> assertThat(thread).startsWith("pulsar-auth-blocking-shared"));
    }

    /**
     * {@code AuthenticationExchange} is single-round and non-thread-safe, and serializing its rounds is the
     * caller's obligation. While the frame decoder is still running, two challenge frames arriving in one read
     * reach {@code handleAuthChallenge} in the same event-loop turn — the sequence reproduced here — and
     * without the guard both would drive the same exchange before either resolution completed.
     */
    @Test
    public void aChallengeArrivingWhileARoundIsInFlightIsDropped() throws Exception {
        CompletableFuture<AuthData> connectCredential = new CompletableFuture<>();
        ScriptedDriver driver = new ScriptedDriver(() -> connectCredential,
                () -> CompletableFuture.completedFuture(AuthData.of("response".getBytes(UTF_8))));
        DirectProxyHandler.ProxyBackendHandler backend = activateBackend(driver);

        // The connect round is still resolving: nothing has completed it yet.
        backend.handleAuthChallenge(challenge("early-challenge"));
        backendChannel.runPendingTasks();
        assertThat(driver.challengeRounds).as("a challenge received mid-round must not re-enter the exchange")
                .hasValue(0);

        // Once the round in flight lands, the next challenge is serviced normally.
        connectCredential.complete(AuthData.of("credential".getBytes(UTF_8)));
        awaitCommandWritten(1);
        backend.handleAuthChallenge(challenge("later-challenge"));
        awaitCommandWritten(2);
        assertThat(driver.challengeRounds).as("a challenge received between rounds must be serviced")
                .hasValue(1);
    }

    /**
     * A broker that answers every {@code CommandAuthResponse} with another challenge would otherwise loop
     * against the proxy forever, and each round now also schedules credential work onto a blocking pool.
     */
    @Test
    public void anEndlesslyChallengingBrokerIsCutOffAtTheRoundCap() throws Exception {
        ScriptedDriver driver = new ScriptedDriver(
                () -> CompletableFuture.completedFuture(AuthData.of("credential".getBytes(UTF_8))),
                () -> CompletableFuture.completedFuture(AuthData.of("response".getBytes(UTF_8))));
        DirectProxyHandler.ProxyBackendHandler backend = activateBackend(driver);
        awaitCommandWritten(1);

        for (int round = 1; round <= DirectProxyHandler.MAX_AUTH_CHALLENGE_ROUNDS + 1; round++) {
            backend.handleAuthChallenge(challenge("server-round-" + round));
            backendChannel.runPendingTasks();
        }

        assertThat(driver.challengeRounds)
                .as("the exchange must stop at the round cap rather than answering challenges forever")
                .hasValue(DirectProxyHandler.MAX_AUTH_CHALLENGE_ROUNDS);
        assertThat(backendChannel.isOpen())
                .as("exceeding the round cap must close the backend connection")
                .isFalse();
    }
}
