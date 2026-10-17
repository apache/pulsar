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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.proxy.Socks5ProxyHandler;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.commons.lang3.StringUtils;

/**
 * Installs a Netty {@link Socks5ProxyHandler} on an async-http-client channel so that HTTP(S)
 * traffic is tunnelled through a SOCKS5 proxy.
 *
 * <p><b>Why not async-http-client's own {@code setProxyServer(...)}?</b> AHC 2.x builds a
 * dedicated bootstrap for SOCKS proxies in {@code ChannelManager#getBootstrap} by wrapping the
 * regular HTTP {@code ChannelInitializer} in a second one:
 *
 * <pre>{@code
 * socksBootstrap.handler(new ChannelInitializer<Channel>() {
 *     public void handlerAdded(ChannelHandlerContext ctx) {
 *         httpBootstrapHandler.handlerAdded(ctx);   // inner initializer runs first ...
 *         super.handlerAdded(ctx);                  // ... outer one is already too late
 *     }
 *     protected void initChannel(Channel channel) {
 *         channel.pipeline().addFirst(SOCKS_HANDLER, socksProxyHandler);
 *     }
 * });
 * }</pre>
 *
 * <p>Both initializers share a single {@link ChannelHandlerContext}. The inner
 * {@code handlerAdded} triggers {@code ChannelInitializer#initChannel}, which removes that
 * context from the pipeline in its {@code finally} block. When the outer {@code handlerAdded}
 * then runs, the context is already removed and the outer {@code initChannel} is never invoked,
 * so {@code SOCKS_HANDLER} never reaches the pipeline. The request is then sent <em>directly</em>
 * to the target host: the proxy is silently bypassed while the call still succeeds, which makes
 * the failure invisible to callers.
 *
 * <p>Therefore the handler is installed directly, mirroring what
 * {@code PulsarChannelInitializer#initSocks5IfConfig} does for the binary protocol.
 *
 * <p><b>HTTPS handling.</b> AHC inserts its {@code SslHandler} from
 * {@code NettyConnectListener} as soon as the channel's connect promise completes, using
 * {@code pipeline.addFirst(SSL_HANDLER, ...)} whenever it is unaware of a proxy. Netty's
 * {@link io.netty.handler.proxy.ProxyHandler} completes that promise once the TCP connection to
 * the <em>proxy</em> is established -- long before the SOCKS5 handshake has finished. TLS would
 * therefore be placed ahead of the SOCKS5 handler and its {@code ClientHello} would race in front
 * of the SOCKS5 negotiation, breaking every HTTPS-over-SOCKS5 request.
 *
 * <p>To avoid that, {@link Socks5HandshakeAwaitHandler} withholds completion of the connect
 * promise until {@link Socks5ProxyHandler#connectFuture()} succeeds. By the time AHC adds the
 * TLS handler the SOCKS5 handshake is complete and the proxy handler has removed its own codecs,
 * so it transparently forwards the encrypted bytes.
 */
public final class Socks5ProxyChannelConfigurer {

    /**
     * Pipeline name used for the SOCKS5 handler. Deliberately equal to
     * {@code org.asynchttpclient.netty.channel.ChannelManager#SOCKS_HANDLER} so that any AHC code
     * path performing {@code addAfter(SOCKS_HANDLER, ...)} resolves instead of throwing
     * {@link java.util.NoSuchElementException}. Note that Netty's
     * {@code Socks5ProxyHandler#protocol()} returns {@code "socks5"}, which would <em>not</em>
     * match.
     */
    public static final String SOCKS_HANDLER_NAME = "socks";

    private static final String SOCKS_AWAIT_HANDLER_NAME = "socks-handshake-await";

    private Socks5ProxyChannelConfigurer() {
    }

    /**
     * Installs the SOCKS5 handler (and its handshake barrier) at the head of the given channel.
     */
    public static void install(Channel channel, InetSocketAddress socks5Address, String username, String password) {
        if (channel.pipeline().get(SOCKS_HANDLER_NAME) != null) {
            return;
        }
        Socks5ProxyHandler socks5ProxyHandler = StringUtils.isNotBlank(username)
                ? new Socks5ProxyHandler(socks5Address, username, password)
                : new Socks5ProxyHandler(socks5Address);
        channel.pipeline().addFirst(SOCKS_HANDLER_NAME, socks5ProxyHandler);
        channel.pipeline().addAfter(SOCKS_HANDLER_NAME, SOCKS_AWAIT_HANDLER_NAME,
                new Socks5HandshakeAwaitHandler(socks5ProxyHandler));
    }

    /**
     * Defers completion of the channel's connect promise until the SOCKS5 handshake has finished,
     * so that handlers added on connect completion (notably AHC's TLS handler) are installed after
     * the tunnel is established rather than in front of the pending handshake.
     */
    public static final class Socks5HandshakeAwaitHandler extends ChannelOutboundHandlerAdapter {

        private final Socks5ProxyHandler proxyHandler;

        Socks5HandshakeAwaitHandler(Socks5ProxyHandler proxyHandler) {
            this.proxyHandler = proxyHandler;
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) {
            // The TCP-level promise only reports transport failures; success is deliberately not
            // propagated here because the tunnel is not usable until the handshake completes.
            ChannelPromise transportPromise = ctx.newPromise();
            transportPromise.addListener((ChannelFuture future) -> {
                if (!future.isSuccess()) {
                    promise.tryFailure(future.cause());
                }
            });
            proxyHandler.connectFuture().addListener(future -> {
                if (future.isSuccess()) {
                    promise.trySuccess();
                } else {
                    promise.tryFailure(future.cause());
                }
            });
            ctx.connect(remoteAddress, localAddress, transportPromise);
        }
    }
}
