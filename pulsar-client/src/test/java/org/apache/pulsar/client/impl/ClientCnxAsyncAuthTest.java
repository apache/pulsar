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
package org.apache.pulsar.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: verifies the {@code ClientCnx} async authentication carve-out against an
 * {@link AsyncAuthenticationDriver}-capable plugin. Guards the acceptance invariants of the
 * single-continuation refactor — the connection's {@code authenticationDataProvider} observable
 * ({@code getCommandData()}) reflects the freshly-resolved credential on connect and on a broker-pushed
 * REFRESH, a REFRESH does not disconnect ({@code getLastDisconnectedTimestamp()} unchanged), and a
 * credential failure never throws synchronously on the event loop — plus the continuation guards:
 * a connect credential/write failure fails the connection future with the mapped v4 cause
 * (not channelInactive's generic message), the continuation runs on the channel's event executor, and a
 * stale or post-close continuation no-ops instead of clobbering the credential or writing to a dead
 * channel.
 */
public class ClientCnxAsyncAuthTest {

    private EventLoopGroup eventLoop;
    private EventLoop executor;
    private ChannelHandlerContext ctx;
    private Channel channel;

    @BeforeMethod
    void setup() {
        eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxAsyncAuth"));
        executor = eventLoop.next();
        ctx = mock(ChannelHandlerContext.class);
        channel = mock(Channel.class);
        when(ctx.writeAndFlush(any())).thenAnswer(args -> mock(ChannelFuture.class));
        when(ctx.channel()).thenReturn(channel);
        when(ctx.executor()).thenReturn(executor);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 6650));
        // A live channel reports isActive() == true; the async-auth continuation guard (PIP-478) keys
        // off this to skip writes to a closed channel, so an unstubbed (false) mock would wrongly suppress
        // every continuation.
        when(channel.isActive()).thenReturn(true);
    }

    @AfterMethod(alwaysRun = true)
    void cleanup() {
        if (eventLoop != null) {
            eventLoop.shutdownGracefully();
        }
    }

    // Generous default so the async-auth resolution timeout (ClientCnx bounds getAuthDataAsync with
    // operationTimeoutMs) does NOT race the credential/write outcomes the deterministic-completion tests
    // assert. A 1 ms budget could otherwise fire under slow CI before the off-loop completion lands and
    // fail the connection future with "Authentication did not complete within 1 ms" instead of the
    // expected cause. Production uses a ~30 s operationTimeout, so this mirrors real behavior; only the
    // timeout-specific test opts into a short budget via the overload below.
    private static final long DEFAULT_OPERATION_TIMEOUT_MS = 30_000;

    private ClientCnx newCnx(Authentication auth) {
        return newCnx(auth, DEFAULT_OPERATION_TIMEOUT_MS);
    }

    private ClientCnx newCnx(Authentication auth, long operationTimeoutMs) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setKeepAliveIntervalSeconds(0);
        conf.setOperationTimeoutMs(operationTimeoutMs);
        conf.setAuthentication(auth);
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        cnx.setRemoteHostName("localhost");
        return cnx;
    }

    private ClientCnx connectedCnx(Supplier<String> tokenSupplier) throws Exception {
        // A real async-capable built-in plugin (AuthenticationToken implements AsyncAuthenticationDriver via
        // its v5-native body). With no bound framework services its credential resolves inline, so the
        // connect continuation runs synchronously on the calling thread.
        AuthenticationToken auth = new AuthenticationToken(tokenSupplier);
        assertThat(auth).isInstanceOf(AsyncAuthenticationDriver.class);
        ClientCnx cnx = newCnx(auth);
        cnx.channelActive(ctx);
        return cnx;
    }

    @Test
    void refreshRepublishesCredentialWithoutDisconnect() throws Exception {
        // Every resolution hands out a fresh token, so connect and each REFRESH observe distinct data.
        AtomicInteger counter = new AtomicInteger();
        ClientCnx cnx = connectedCnx(() -> "token-" + counter.incrementAndGet());

        // Connect resolved the first credential and published it as the observable data provider.
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("token-1");
        long lastDisconnect = cnx.getLastDisconnectedTimestamp();

        // Broker pushes the REFRESH sentinel: the async path must open a fresh exchange, re-produce the
        // current credential, and republish the data provider — without disconnecting.
        cnx.handleAuthChallenge(refreshChallenge());
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("token-2");
        assertThat(cnx.getLastDisconnectedTimestamp()).isEqualTo(lastDisconnect);

        // A second REFRESH re-produces again, still on the single assignment site.
        cnx.handleAuthChallenge(refreshChallenge());
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("token-3");
        assertThat(cnx.getLastDisconnectedTimestamp()).isEqualTo(lastDisconnect);
    }

    @Test
    void connectCredentialFailureClosesChannelWithoutThrowing() {
        // A plugin whose credential acquisition throws must not throw synchronously on the event loop; the
        // connect continuation closes the channel gracefully instead (PIP-478 error model).
        assertThatCode(() -> connectedCnx(() -> {
            throw new RuntimeException("credential provider is down");
        })).doesNotThrowAnyException();
        verify(ctx, timeout(5000)).close();
    }

    @Test
    void continuationRunsOnEventExecutorNotTheCompletingThread() throws Exception {
        // An async driver whose credential completes off the event loop: the continuation must hop back
        // onto the channel's event executor (never run on the completing thread).
        ControllableAuthDriver auth = new ControllableAuthDriver();
        ClientCnx cnx = newCnx(auth);
        AtomicReference<Thread> writeThread = new AtomicReference<>();
        when(ctx.writeAndFlush(any())).thenAnswer(inv -> {
            writeThread.set(Thread.currentThread());
            return successWrite();
        });

        onEventLoop(() -> cnx.channelActive(ctx));
        ControllableExchange connect = auth.lastExchange;
        assertThat(connect.authData).isNotCompleted();

        AtomicReference<Thread> completerThread = new AtomicReference<>();
        completeOffLoop(connect.authData, "connect", completerThread);
        drainEventLoop();

        assertThat(writeThread.get()).isNotNull();
        assertThat(executor.inEventLoop(writeThread.get())).isTrue();
        assertThat(writeThread.get()).isNotEqualTo(completerThread.get());
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("connect");
    }

    @Test
    void staleRoundCompletionIsIgnored() throws Exception {
        // A superseded round (an older REFRESH whose credential lands after a newer REFRESH opened a fresh
        // exchange) must not clobber the published credential or send a stale response (PIP-478).
        ControllableAuthDriver auth = new ControllableAuthDriver();
        ClientCnx cnx = newCnx(auth);
        AtomicInteger writes = new AtomicInteger();
        when(ctx.writeAndFlush(any())).thenAnswer(inv -> {
            writes.incrementAndGet();
            return successWrite();
        });

        onEventLoop(() -> cnx.channelActive(ctx));
        ControllableExchange connect = auth.lastExchange;
        completeOffLoop(connect.authData, "connect", new AtomicReference<>());
        drainEventLoop();
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("connect");

        // REFRESH #1 opens exchange E1 and starts a round that stays pending.
        onEventLoop(() -> cnx.handleAuthChallenge(refreshChallenge()));
        ControllableExchange e1 = auth.lastExchange;
        // REFRESH #2 supersedes with a fresh exchange E2 (a REFRESH is allowed to restart authentication).
        onEventLoop(() -> cnx.handleAuthChallenge(refreshChallenge()));
        ControllableExchange e2 = auth.lastExchange;
        assertThat(e2).isNotSameAs(e1);

        // The newer round lands first: it republishes and sends its response.
        completeOffLoop(e2.authData, "refresh-2", new AtomicReference<>());
        drainEventLoop();
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("refresh-2");
        int writesAfterCurrentRound = writes.get();

        // The superseded (stale) round lands late: it must be ignored — no credential clobber, no write.
        completeOffLoop(e1.authData, "refresh-1", new AtomicReference<>());
        drainEventLoop();
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("refresh-2");
        assertThat(writes.get()).isEqualTo(writesAfterCurrentRound);
        assertThat(cnx.connectionFuture().isCompletedExceptionally()).isFalse();
    }

    @Test
    void asyncConnectCredentialFailureFailsConnectionFutureWithMappedCause() throws Exception {
        // PIP-478: a connect credential failure on the async path must fail the connection future with
        // the (already v4-mapped) cause the driver reports — not channelInactive's generic
        // "Connection already closed" — and must write no CONNECT frame.
        ControllableAuthDriver auth = new ControllableAuthDriver();
        ClientCnx cnx = newCnx(auth);

        onEventLoop(() -> cnx.channelActive(ctx));
        ControllableExchange connect = auth.lastExchange;

        PulsarClientException.AuthenticationException cause =
                new PulsarClientException.AuthenticationException("bad credentials");
        failOffLoop(connect.authData, cause);
        drainEventLoop();

        assertThat(cnx.connectionFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> cnx.connectionFuture().get(5, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(cause);
        verify(ctx, timeout(5000)).close();
        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    void continuationAfterChannelCloseDoesNotWrite() throws Exception {
        // PIP-478: if the channel closes while the credential is still resolving, the late continuation
        // must not write to the dead channel.
        ControllableAuthDriver auth = new ControllableAuthDriver();
        ClientCnx cnx = newCnx(auth);

        onEventLoop(() -> cnx.channelActive(ctx));
        ControllableExchange connect = auth.lastExchange;

        // Channel goes down before the credential resolves.
        when(channel.isActive()).thenReturn(false);

        completeOffLoop(connect.authData, "connect", new AtomicReference<>());
        drainEventLoop();

        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    void connectWriteFailureFailsConnectionFutureWithWriteCause() throws Exception {
        // PIP-478: a terminal connect-write failure must fail the connection future with the write's
        // cause before the channel is closed.
        ControllableAuthDriver auth = new ControllableAuthDriver();
        ClientCnx cnx = newCnx(auth);
        RuntimeException writeCause = new RuntimeException("socket write failed");
        when(ctx.writeAndFlush(any())).thenAnswer(inv -> failedWrite(writeCause));

        onEventLoop(() -> cnx.channelActive(ctx));
        ControllableExchange connect = auth.lastExchange;
        completeOffLoop(connect.authData, "connect", new AtomicReference<>());
        drainEventLoop();

        assertThat(cnx.connectionFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> cnx.connectionFuture().get(5, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(writeCause);
        verify(ctx, timeout(5000)).close();
    }

    @Test
    void asyncConnectCredentialTimeoutFailsConnectionInsteadOfHanging() throws Exception {
        // PIP-478: an async driver whose getAuthDataAsync() never completes must not wedge the handshake.
        // The operation-timeout budget (operationTimeoutMs = 1 here) bounds the async resolution, so the
        // connection future fails with a mapped auth exception instead of hanging forever. This is the only
        // test that deliberately relies on the short budget firing, so it opts into 1 ms explicitly.
        ControllableAuthDriver auth = new ControllableAuthDriver();
        ClientCnx cnx = newCnx(auth, 1);

        onEventLoop(() -> cnx.channelActive(ctx));
        // connect.authData is deliberately never completed; only the timeout can complete the resolution.
        assertThatThrownBy(() -> cnx.connectionFuture().get(5, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PulsarClientException.AuthenticationException.class);
        verify(ctx, timeout(5000)).close();
        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    void exceedingMaxChallengeRoundsFailsConnection() throws Exception {
        // PIP-478: a broker (or plugin) that keeps issuing challenges without ever terminating must not
        // loop forever. After MAX_AUTH_CHALLENGE_ROUNDS challenge rounds the connection fails.
        AlwaysRespondingDriver auth = new AlwaysRespondingDriver();
        ClientCnx cnx = newCnx(auth);
        onEventLoop(() -> cnx.channelActive(ctx));

        // Each non-refresh challenge resolves inline (completed future), so the previous round is finished
        // before the next arrives (not dropped by the serialize-or-drop guard). The cap is not yet exceeded.
        for (int i = 0; i < ClientCnx.MAX_AUTH_CHALLENGE_ROUNDS; i++) {
            onEventLoop(() -> cnx.handleAuthChallenge(saslChallenge()));
        }
        assertThat(cnx.connectionFuture().isCompletedExceptionally())
                .as("the connection survives up to the round cap").isFalse();

        // One more challenge exceeds the cap and fails the connection with a mapped auth exception.
        onEventLoop(() -> cnx.handleAuthChallenge(saslChallenge()));
        assertThat(cnx.connectionFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> cnx.connectionFuture().get(5, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PulsarClientException.AuthenticationException.class)
                .hasMessageContaining("maximum of " + ClientCnx.MAX_AUTH_CHALLENGE_ROUNDS);
        verify(ctx, timeout(5000)).close();
    }

    @Test
    void refreshResetsChallengeRoundCounter() throws Exception {
        // PIP-478: a broker-pushed REFRESH opens a fresh exchange, so it resets the challenge-round
        // counter — a long-lived connection that periodically refreshes is never starved by the cap.
        AlwaysRespondingDriver auth = new AlwaysRespondingDriver();
        ClientCnx cnx = newCnx(auth);
        onEventLoop(() -> cnx.channelActive(ctx));

        // Drive challenge rounds up to (but not over) the cap, then REFRESH to reset, then drive the same
        // number again. Without the reset this second batch would exceed the cap.
        for (int i = 0; i < ClientCnx.MAX_AUTH_CHALLENGE_ROUNDS; i++) {
            onEventLoop(() -> cnx.handleAuthChallenge(saslChallenge()));
        }
        onEventLoop(() -> cnx.handleAuthChallenge(refreshChallenge()));
        for (int i = 0; i < ClientCnx.MAX_AUTH_CHALLENGE_ROUNDS; i++) {
            onEventLoop(() -> cnx.handleAuthChallenge(saslChallenge()));
        }
        assertThat(cnx.connectionFuture().isCompletedExceptionally())
                .as("REFRESH reset the counter, so the second batch stays within the cap").isFalse();
    }

    private static CommandAuthChallenge refreshChallenge() {
        CommandAuthChallenge challenge = new CommandAuthChallenge();
        challenge.setChallenge()
                .setAuthData(AuthData.REFRESH_AUTH_DATA_BYTES)
                .setAuthMethodName("controllable");
        return challenge;
    }

    private static CommandAuthChallenge saslChallenge() {
        CommandAuthChallenge challenge = new CommandAuthChallenge();
        challenge.setChallenge()
                .setAuthData("challenge".getBytes(UTF_8))
                .setAuthMethodName("controllable");
        return challenge;
    }

    /** Run a task on the channel's event loop and wait for it to finish (propagating any failure). */
    private void onEventLoop(ThrowingRunnable task) throws Exception {
        executor.submit(() -> {
            try {
                task.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).get(5, SECONDS);
    }

    /** Wait until the event loop has drained the continuation that a just-completed credential scheduled. */
    private void drainEventLoop() throws Exception {
        onEventLoop(() -> { });
    }

    /** Complete an auth-data future from a dedicated (non event-loop) thread, recording that thread. */
    private static void completeOffLoop(CompletableFuture<AuthData> future, String value,
            AtomicReference<Thread> completerThread) throws InterruptedException {
        Thread t = new Thread(() -> {
            completerThread.set(Thread.currentThread());
            future.complete(AuthData.of(value.getBytes(UTF_8)));
        }, "off-loop-credential");
        t.start();
        t.join();
    }

    /** Fail an auth-data future from a dedicated (non event-loop) thread. */
    private static void failOffLoop(CompletableFuture<AuthData> future, Throwable cause)
            throws InterruptedException {
        Thread t = new Thread(() -> future.completeExceptionally(cause), "off-loop-credential");
        t.start();
        t.join();
    }

    private static ChannelFuture successWrite() {
        return listenerInvokingWrite(true, null);
    }

    private static ChannelFuture failedWrite(Throwable cause) {
        return listenerInvokingWrite(false, cause);
    }

    /**
     * A {@link ChannelFuture} mock that synchronously invokes any listener added to it — so the connect /
     * challenge write continuations (which key their success/failure handling off the listener callback)
     * actually run.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ChannelFuture listenerInvokingWrite(boolean success, Throwable cause) {
        ChannelFuture future = mock(ChannelFuture.class);
        when(future.isSuccess()).thenReturn(success);
        when(future.cause()).thenReturn(cause);
        when(future.addListener(any())).thenAnswer(inv -> {
            GenericFutureListener listener = inv.getArgument(0);
            listener.operationComplete(future);
            return future;
        });
        return future;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * A minimal {@link AsyncAuthenticationDriver}-capable v4 {@link Authentication} whose exchanges hand
     * back futures the test completes explicitly (from an off-event-loop thread), so the harness can drive
     * the pending / stale / post-close continuation paths deterministically.
     */
    // Implements the deprecated v4 Authentication SPI (configure(Map)) by design, like the real plugins.
    @SuppressWarnings("deprecation")
    private static final class ControllableAuthDriver implements Authentication, AsyncAuthenticationDriver {

        private static final long serialVersionUID = 1L;

        private final transient Queue<ControllableExchange> exchanges = new ConcurrentLinkedQueue<>();
        private transient volatile ControllableExchange lastExchange;

        @Override
        public String getAuthMethodName() {
            return "controllable";
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

        @Override
        public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
            ControllableExchange exchange = new ControllableExchange();
            exchanges.add(exchange);
            lastExchange = exchange;
            return exchange;
        }
    }

    private static final class ControllableExchange implements AuthenticationExchange {

        private final CompletableFuture<AuthData> authData = new CompletableFuture<>();
        private final CompletableFuture<AuthData> challengeResponse = new CompletableFuture<>();

        @Override
        public CompletableFuture<AuthData> getAuthDataAsync() {
            return authData;
        }

        @Override
        public CompletableFuture<AuthData> authenticateAsync(AuthData challengeOrRefresh) {
            return challengeResponse;
        }
    }

    /**
     * An {@link AsyncAuthenticationDriver} whose exchanges always resolve inline with a non-terminal
     * credential — so it answers every challenge and never completes the handshake. Used to drive the
     * challenge-round cap deterministically (each round resolves synchronously, so rounds are not dropped by
     * the serialize-or-drop guard).
     */
    @SuppressWarnings("deprecation")
    private static final class AlwaysRespondingDriver implements Authentication, AsyncAuthenticationDriver {

        private static final long serialVersionUID = 1L;

        @Override
        public String getAuthMethodName() {
            return "controllable";
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

        @Override
        public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
            return new AuthenticationExchange() {
                @Override
                public CompletableFuture<AuthData> getAuthDataAsync() {
                    return CompletableFuture.completedFuture(AuthData.of("init".getBytes(UTF_8)));
                }

                @Override
                public CompletableFuture<AuthData> authenticateAsync(AuthData challengeOrRefresh) {
                    return CompletableFuture.completedFuture(AuthData.of("response".getBytes(UTF_8)));
                }
            };
        }
    }
}
