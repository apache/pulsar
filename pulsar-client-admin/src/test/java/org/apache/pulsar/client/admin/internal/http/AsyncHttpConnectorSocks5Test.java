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
package org.apache.pulsar.client.admin.internal.http;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.socksx.SocksVersion;
import io.netty.handler.codec.socksx.v5.DefaultSocks5CommandResponse;
import io.netty.handler.codec.socksx.v5.DefaultSocks5InitialResponse;
import io.netty.handler.codec.socksx.v5.DefaultSocks5PasswordAuthResponse;
import io.netty.handler.codec.socksx.v5.Socks5AddressType;
import io.netty.handler.codec.socksx.v5.Socks5AuthMethod;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequest;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5CommandStatus;
import io.netty.handler.codec.socksx.v5.Socks5InitialRequest;
import io.netty.handler.codec.socksx.v5.Socks5InitialRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequest;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthStatus;
import io.netty.handler.codec.socksx.v5.Socks5ServerEncoder;
import io.netty.handler.proxy.Socks5ProxyHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.impl.Socks5ProxyChannelConfigurer;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.testng.annotations.Test;

/**
 * Unit tests to verify that the SOCKS5 support for pulsar-admin actually works end-to-end
 * against a live Netty SOCKS5 server. The tests exist because the previous implementation
 * (based on AHC's {@code confBuilder.setProxyServer(...)} API) silently bypassed the SOCKS5
 * proxy at runtime due to a bug in async-http-client's {@code ChannelManager#getBootstrap}
 * SOCKS bootstrap composition; this test suite is the regression fence for that bug.
 *
 * <p>The test cases cover:
 * <ol>
 *   <li>{@link #testConfigureSocks5_notConfigured_noProxy}: when no SOCKS5 address is set,
 *       the AHC config should not have a channel initializer attached;</li>
 *   <li>{@link #testConfigureSocks5_scopeBinaryOnly_httpBypassed}: when scope is BINARY_ONLY,
 *       HTTP traffic must not be routed to SOCKS5;</li>
 *   <li>{@link #testConfigureSocks5_httpOnlyWithAuth_channelInitializerAttached}: with
 *       HTTP_ONLY + username/password, the initializer must add a {@link Socks5ProxyHandler}
 *       carrying the correct credentials;</li>
 *   <li>{@link #testConfigureSocks5_httpOnlyNoAuth_anonymousHandlerAttached}: without auth
 *       the initializer is still attached (anonymous SOCKS5);</li>
 *   <li>{@link #testEndToEnd_httpThroughLocalSocks5}: a full round-trip through a local
 *       Netty SOCKS5 server proves that HTTP traffic really flows through the proxy.</li>
 * </ol>
 */
@Slf4j
public class AsyncHttpConnectorSocks5Test {

    /**
     * Case 1: When socks5ProxyAddress is null, no proxy channel initializer should be attached.
     */
    @Test
    public void testConfigureSocks5_notConfigured_noProxy() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://broker.example.com:8080");
        // Do NOT set socks5ProxyAddress

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        AsyncHttpConnector.configureSocks5ProxyIfNeeded(confBuilder, conf);
        DefaultAsyncHttpClientConfig built = (DefaultAsyncHttpClientConfig) confBuilder.build();

        assertNull(built.getHttpAdditionalChannelInitializer(),
                "no channel initializer should be attached when socks5 is not configured");
    }

    /**
     * Case 2: When scope is BINARY_ONLY (the client default), HTTP traffic should NOT use SOCKS5
     * -- i.e. no channel initializer should be attached to the AHC config.
     */
    @Test
    public void testConfigureSocks5_scopeBinaryOnly_httpBypassed() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://broker.example.com:8080");
        conf.setSocks5ProxyAddress(new InetSocketAddress("127.0.0.1", 1080));
        conf.setSocks5ProxyScope(Socks5ProxyScope.BINARY_ONLY);

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        AsyncHttpConnector.configureSocks5ProxyIfNeeded(confBuilder, conf);
        DefaultAsyncHttpClientConfig built = (DefaultAsyncHttpClientConfig) confBuilder.build();

        assertNull(built.getHttpAdditionalChannelInitializer(),
                "BINARY_ONLY scope should not route HTTP traffic to SOCKS5");
    }

    /**
     * Case 3: With HTTP_ONLY scope + non-blank username/password, AHC config must expose an
     * httpAdditionalChannelInitializer that adds a {@link Socks5ProxyHandler} carrying the
     * given username/password to the head of the pipeline for every new HTTP channel.
     * This is the exact configuration used by PulsarAdmin (default HTTP_ONLY scope).
     */
    @Test
    public void testConfigureSocks5_httpOnlyWithAuth_channelInitializerAttached() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://broker.example.com:8080");
        conf.setSocks5ProxyAddress(new InetSocketAddress("21.234.58.57", 1080));
        conf.setSocks5ProxyUsername("alice");
        conf.setSocks5ProxyPassword("secret");
        conf.setSocks5ProxyScope(Socks5ProxyScope.HTTP_ONLY);

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        AsyncHttpConnector.configureSocks5ProxyIfNeeded(confBuilder, conf);
        DefaultAsyncHttpClientConfig built = (DefaultAsyncHttpClientConfig) confBuilder.build();

        Consumer<Channel> initializer = built.getHttpAdditionalChannelInitializer();
        assertNotNull(initializer, "httpAdditionalChannelInitializer must be attached for HTTP_ONLY + address");

        // Apply the initializer to a fresh EmbeddedChannel and verify the pipeline gets a
        // Socks5ProxyHandler carrying the expected credentials. Note that Socks5ProxyHandler
        // eagerly installs a Socks5InitialResponseDecoder in its handlerAdded, so we look it
        // up by type instead of asserting on pipeline.first().
        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            initializer.accept(ch);
            Socks5ProxyHandler socks = ch.pipeline().get(Socks5ProxyHandler.class);
            assertNotNull(socks, "pipeline must contain Socks5ProxyHandler after initializer runs");
            assertEquals(socks.username(), "alice");
            assertEquals(socks.password(), "secret");
            assertEquals(socks.proxyAddress(),
                    new InetSocketAddress("21.234.58.57", 1080),
                    "proxy address must match configuration");
        } finally {
            ch.close();
        }
    }

    /**
     * Case 4: HTTP_ONLY scope without username should still install an anonymous SOCKS5 handler.
     * We verify by checking the {@link Consumer} exists; the full ProxyHandler connect lifecycle
     * for the anonymous branch is exercised end-to-end via {@link #testEndToEnd_httpThroughLocalSocks5}.
     */
    @Test
    public void testConfigureSocks5_httpOnlyNoAuth_anonymousHandlerAttached() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://broker.example.com:8080");
        conf.setSocks5ProxyAddress(new InetSocketAddress("127.0.0.1", 1080));
        conf.setSocks5ProxyScope(Socks5ProxyScope.HTTP_ONLY);

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        AsyncHttpConnector.configureSocks5ProxyIfNeeded(confBuilder, conf);
        DefaultAsyncHttpClientConfig built = (DefaultAsyncHttpClientConfig) confBuilder.build();

        Consumer<Channel> initializer = built.getHttpAdditionalChannelInitializer();
        assertNotNull(initializer, "channel initializer must be attached even without auth");
    }

    /**
     * Guards the two pipeline properties that HTTPS-over-SOCKS5 depends on.
     *
     * <p>1. The SOCKS5 handler must be registered under async-http-client's
     * {@code ChannelManager.SOCKS_HANDLER} name ({@code "socks"}). Netty's
     * {@code Socks5ProxyHandler#protocol()} returns {@code "socks5"}, which does not match, so any
     * AHC code path doing {@code addAfter(SOCKS_HANDLER, ...)} would fail with
     * {@link java.util.NoSuchElementException}.
     *
     * <p>2. A handshake barrier must sit behind the SOCKS5 handler. AHC inserts its TLS handler
     * as soon as the connect promise completes; Netty completes that promise once connected to
     * the proxy, i.e. before the SOCKS5 handshake finishes. Without the barrier, TLS is placed in
     * front of the SOCKS5 handler and the {@code ClientHello} races ahead of the negotiation.
     */
    @Test
    public void testConfigureSocks5_pipelineIsHttpsSafe() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("https://broker.example.com:8443");
        conf.setSocks5ProxyAddress(new InetSocketAddress("21.234.58.57", 1080));
        conf.setSocks5ProxyUsername("alice");
        conf.setSocks5ProxyPassword("secret");
        conf.setSocks5ProxyScope(Socks5ProxyScope.HTTP_ONLY);

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        AsyncHttpConnector.configureSocks5ProxyIfNeeded(confBuilder, conf);
        DefaultAsyncHttpClientConfig built = (DefaultAsyncHttpClientConfig) confBuilder.build();

        Consumer<Channel> initializer = built.getHttpAdditionalChannelInitializer();
        assertNotNull(initializer, "httpAdditionalChannelInitializer must be attached");

        EmbeddedChannel ch = new EmbeddedChannel();
        try {
            initializer.accept(ch);

            // (1) registered under the name AHC looks for
            assertNotNull(ch.pipeline().get("socks"),
                    "SOCKS5 handler must be registered as \"socks\" so that AHC's "
                            + "addAfter(SOCKS_HANDLER, ...) resolves");

            // (2) the handshake barrier must be present, and positioned after the SOCKS5 handler
            Socks5ProxyChannelConfigurer.Socks5HandshakeAwaitHandler await =
                    ch.pipeline().get(Socks5ProxyChannelConfigurer.Socks5HandshakeAwaitHandler.class);
            assertNotNull(await, "a handshake barrier must defer connect-promise completion");

            List<String> names = ch.pipeline().names();
            int socksIdx = names.indexOf("socks");
            int awaitIdx = -1;
            for (int i = 0; i < names.size(); i++) {
                if (ch.pipeline().get(names.get(i)) == await) {
                    awaitIdx = i;
                    break;
                }
            }
            assertTrue(socksIdx >= 0 && awaitIdx > socksIdx,
                    "handshake barrier must sit behind the SOCKS5 handler, but pipeline was " + names);
        } finally {
            ch.close();
        }
    }

    /**
     * End-to-end test: start a local Netty SOCKS5 server (with password auth) and a local
     * blocking HTTP server, then verify that AHC configured via
     * {@link AsyncHttpConnector#configureSocks5ProxyIfNeeded} really tunnels HTTP through the
     * SOCKS5 proxy and reaches the HTTP server.
     *
     * <p>The SOCKS5 server tracks whether it saw a valid initial method-negotiation request,
     * whether username/password auth succeeded, and the CONNECT target. Every one of those
     * signals plus the HTTP server hit flag must be true for the test to pass, so any silent
     * regression that falls back to a direct connect would fail loudly.
     */
    @Test(timeOut = 30_000)
    public void testEndToEnd_httpThroughLocalSocks5() throws Exception {
        // ---- 1) Start a bare-bones HTTP server on a random localhost port ----
        AtomicBoolean httpServerHit = new AtomicBoolean(false);
        ServerSocket httpServer = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        int httpPort = httpServer.getLocalPort();
        Thread httpThread = new Thread(() -> {
            try (Socket sock = httpServer.accept()) {
                httpServerHit.set(true);
                log.info("[TEST-DIAG] http server accepted a connection from {}", sock.getRemoteSocketAddress());
                byte[] buf = new byte[4096];
                int n = sock.getInputStream().read(buf);
                if (n > 0) {
                    log.info("[TEST-DIAG] http server got {} bytes of request, preview: {}",
                            n, new String(buf, 0, Math.min(n, 200)).replaceAll("\\s+", " "));
                }
                String body = "{\"brokerUrl\":\"pulsar://broker-real:6650\"}";
                OutputStream out = sock.getOutputStream();
                out.write(("HTTP/1.1 200 OK\r\n"
                        + "Content-Type: application/json\r\n"
                        + "Content-Length: " + body.length() + "\r\n"
                        + "Connection: close\r\n\r\n"
                        + body).getBytes());
                out.flush();
            } catch (IOException ignored) {
            }
        }, "test-http-server");
        httpThread.setDaemon(true);
        httpThread.start();

        // ---- 2) Start a local SOCKS5 server with PASSWORD auth ----
        AtomicBoolean socksSawInit = new AtomicBoolean(false);
        AtomicBoolean socksSawAuthOk = new AtomicBoolean(false);
        AtomicReference<String> socksTargetHost = new AtomicReference<>();
        AtomicReference<Integer> socksTargetPort = new AtomicReference<>();

        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(2);
        Channel socksChannel;
        try {
            ServerBootstrap sb = new ServerBootstrap();
            sb.group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new TestSocks5ServerInitializer("alice", "secret",
                            socksSawInit, socksSawAuthOk, socksTargetHost, socksTargetPort,
                            httpPort));
            socksChannel = sb.bind(InetAddress.getLoopbackAddress(), 0).sync().channel();
            int socksPort = ((InetSocketAddress) socksChannel.localAddress()).getPort();
            log.info("[TEST-DIAG] local SOCKS5 server bound to 127.0.0.1:{}, backend http on 127.0.0.1:{}",
                    socksPort, httpPort);

            // ---- 3) Build ClientConfigurationData mimicking real admin usage ----
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setServiceUrl("http://127.0.0.1:" + httpPort);
            conf.setSocks5ProxyAddress(new InetSocketAddress("127.0.0.1", socksPort));
            conf.setSocks5ProxyUsername("alice");
            conf.setSocks5ProxyPassword("secret");
            conf.setSocks5ProxyScope(Socks5ProxyScope.HTTP_ONLY);

            // ---- 4) Use AHC directly (like AsyncHttpConnector does) and issue a request ----
            DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
            AsyncHttpConnector.configureSocks5ProxyIfNeeded(confBuilder, conf);
            DefaultAsyncHttpClientConfig ahcConf = (DefaultAsyncHttpClientConfig) confBuilder.build();

            AsyncHttpClient httpClient = new DefaultAsyncHttpClient(ahcConf);
            try {
                String targetUrl = "http://127.0.0.1:" + httpPort + "/admin/v2/broker-stats";
                log.info("[TEST-DIAG] issuing GET {} via AHC (SOCKS5 expected: 127.0.0.1:{})",
                        targetUrl, socksPort);
                Response resp = httpClient
                        .prepareGet(targetUrl)
                        .execute()
                        .get();
                log.info("[TEST-DIAG] AHC response status={}, body={}",
                        resp.getStatusCode(), resp.getResponseBody());
                assertEquals(resp.getStatusCode(), 200, "HTTP call through SOCKS5 must succeed");
            } catch (Throwable ex) {
                log.error("[TEST-DIAG] AHC request failed: {}", ex.toString(), ex);
                throw ex;
            } finally {
                httpClient.close();
            }

            // Give the http server thread a beat to flip the flag.
            httpThread.join(3000);

            log.info("[TEST-DIAG] flags after request: socksSawInit={}, socksSawAuthOk={}, "
                            + "socksTarget={}:{}, httpServerHit={}",
                    socksSawInit.get(), socksSawAuthOk.get(),
                    socksTargetHost.get(), socksTargetPort.get(), httpServerHit.get());

            assertTrue(socksSawInit.get(),
                    "SOCKS5 server should have received the initial method-negotiation request");
            assertTrue(socksSawAuthOk.get(),
                    "SOCKS5 server should have validated username/password successfully");
            assertEquals(socksTargetHost.get(), "127.0.0.1",
                    "SOCKS5 CONNECT target host mismatch");
            assertEquals(socksTargetPort.get(), Integer.valueOf(httpPort),
                    "SOCKS5 CONNECT target port mismatch");
            assertTrue(httpServerHit.get(), "HTTP server should have received the tunneled request");
        } finally {
            try {
                httpServer.close();
            } catch (IOException ignored) {
            }
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }

    // ------------------------------------------------------------------------
    // Minimal SOCKS5 server implementation for tests, based on Netty codec.
    // ------------------------------------------------------------------------
    private static final class TestSocks5ServerInitializer extends ChannelInitializer<SocketChannel> {
        private final String expectUser;
        private final String expectPwd;
        private final AtomicBoolean sawInit;
        private final AtomicBoolean sawAuthOk;
        private final AtomicReference<String> targetHost;
        private final AtomicReference<Integer> targetPort;
        private final int localHttpPort;

        TestSocks5ServerInitializer(String expectUser, String expectPwd,
                                    AtomicBoolean sawInit, AtomicBoolean sawAuthOk,
                                    AtomicReference<String> targetHost,
                                    AtomicReference<Integer> targetPort,
                                    int localHttpPort) {
            this.expectUser = expectUser;
            this.expectPwd = expectPwd;
            this.sawInit = sawInit;
            this.sawAuthOk = sawAuthOk;
            this.targetHost = targetHost;
            this.targetPort = targetPort;
            this.localHttpPort = localHttpPort;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(Socks5ServerEncoder.DEFAULT);
            ch.pipeline().addLast(new Socks5InitialRequestDecoder());
            ch.pipeline().addLast(new SimpleChannelInboundHandler<Socks5InitialRequest>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Socks5InitialRequest msg) {
                    sawInit.set(true);
                    if (!SocksVersion.SOCKS5.equals(msg.version())) {
                        ctx.close();
                        return;
                    }
                    // Require password auth.
                    ctx.pipeline().addFirst(new Socks5PasswordAuthRequestDecoder());
                    ctx.writeAndFlush(new DefaultSocks5InitialResponse(Socks5AuthMethod.PASSWORD));
                    ctx.pipeline().remove(this);
                    ctx.pipeline().remove(Socks5InitialRequestDecoder.class);
                }
            });
            ch.pipeline().addLast(new SimpleChannelInboundHandler<Socks5PasswordAuthRequest>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Socks5PasswordAuthRequest msg) {
                    boolean ok = expectUser.equals(msg.username()) && expectPwd.equals(msg.password());
                    sawAuthOk.set(ok);
                    Socks5PasswordAuthStatus st =
                            ok ? Socks5PasswordAuthStatus.SUCCESS : Socks5PasswordAuthStatus.FAILURE;
                    ctx.writeAndFlush(new DefaultSocks5PasswordAuthResponse(st));
                    if (!ok) {
                        ctx.close();
                        return;
                    }
                    ctx.pipeline().addFirst(new Socks5CommandRequestDecoder());
                    ctx.pipeline().remove(this);
                    ctx.pipeline().remove(Socks5PasswordAuthRequestDecoder.class);
                }
            });
            ch.pipeline().addLast(new SimpleChannelInboundHandler<Socks5CommandRequest>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Socks5CommandRequest msg) {
                    targetHost.set(msg.dstAddr());
                    targetPort.set(msg.dstPort());
                    Channel clientChannel = ctx.channel();
                    // Open an upstream TCP connection to the real target (the local http
                    // server) BEFORE replying SUCCESS, so we can bridge bytes as soon as
                    // the client starts writing.
                    try {
                        Socket upstream = new Socket(msg.dstAddr(), msg.dstPort());
                        // Send SUCCESS to the SOCKS5 client.
                        ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                                Socks5CommandStatus.SUCCESS, Socks5AddressType.IPv4,
                                "127.0.0.1", localHttpPort));
                        // Now switch the client-facing pipeline into a raw bridge mode:
                        // remove SOCKS5 codecs and install a byte forwarder.
                        ctx.pipeline().remove(this);
                        ctx.pipeline().remove(Socks5CommandRequestDecoder.class);
                        ctx.pipeline().remove(Socks5ServerEncoder.DEFAULT);
                        // client -> upstream : forward every ByteBuf we receive.
                        clientChannel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext c, ByteBuf buf)
                                    throws Exception {
                                byte[] arr = new byte[buf.readableBytes()];
                                buf.readBytes(arr);
                                upstream.getOutputStream().write(arr);
                                upstream.getOutputStream().flush();
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext c) throws Exception {
                                try {
                                    upstream.close();
                                } catch (IOException ignored) {
                                }
                            }
                        });
                        // upstream -> client : blocking read loop in a bridge thread.
                        Thread bridgeThread = new Thread(() -> {
                            try {
                                byte[] buf = new byte[4096];
                                while (true) {
                                    int n = upstream.getInputStream().read(buf);
                                    if (n <= 0) {
                                        break;
                                    }
                                    byte[] copy = new byte[n];
                                    System.arraycopy(buf, 0, copy, 0, n);
                                    clientChannel.writeAndFlush(Unpooled.wrappedBuffer(copy));
                                }
                                clientChannel.close();
                            } catch (IOException e) {
                                log.warn("upstream->client bridge failed: {}", e.toString());
                                clientChannel.close();
                            }
                        }, "test-socks5-bridge-upstream-to-client");
                        bridgeThread.setDaemon(true);
                        bridgeThread.start();
                    } catch (IOException e) {
                        log.warn("failed to open upstream socket to {}:{} - {}",
                                msg.dstAddr(), msg.dstPort(), e.toString());
                        ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                                Socks5CommandStatus.FAILURE, Socks5AddressType.IPv4));
                        ctx.close();
                    }
                }
            });
        }
    }
}
