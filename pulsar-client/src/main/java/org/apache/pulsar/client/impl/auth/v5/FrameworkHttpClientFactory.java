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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.http.PulsarHttpClientFactory;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Realm;
import org.asynchttpclient.SslEngineFactory;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;

/**
 * The framework-owned {@link PulsarHttpClientFactory} (PIP-478): builds AsyncHttpClient-backed
 * {@link PulsarHttpClient} instances for authentication plugins (OAuth2's token endpoint, Athenz's ZTS)
 * that <em>share</em> the owning {@code PulsarClient}'s Netty event loop, timer and DNS resolver — fixing
 * the v4 problem where {@code AuthenticationOAuth2} spun up private {@code DefaultAsyncHttpClient}
 * instances with their own event loops (Motivation #3). One factory exists per {@code PulsarClient} and
 * is closed with it; {@link PulsarHttpClient#close()} is idempotent, and closing the factory closes every
 * instance it still owns.
 *
 * <p><b>Placement.</b> This lives in {@code pulsar-client} (not {@code pulsar-client-v5}) so that
 * {@code PulsarClientImpl} can construct it directly with its shared resources; {@code pulsar-client}
 * already {@code api}-depends on {@code pulsar-client-api-v5} (which re-exports the
 * {@code pulsar-http-client-api} HTTP SPI), so the SPI types are on its classpath. The HTTP <em>backend</em>
 * is deliberately framework-owned and not pluggable (see the PIP).
 *
 * <p><b>Shared resources, resolved lazily.</b> The event loop / timer / DNS resolver / TLS factory are
 * supplied as {@link Supplier}s that close over the owning client and are read at {@code newHttpClient}
 * time, not at construction — because the factory is bound into the authentication driver early in client
 * construction, before some of those resources exist. By the time a plugin actually requests a client
 * (after {@code start()} on a fully-constructed client) they are all ready.
 *
 * <p><b>TLS rotation.</b> AsyncHttpClient fixes its TLS configuration at instance build, so when the
 * client is on the new PIP-478 TLS path the factory installs an {@link SslEngineFactory} over a volatile
 * {@link SslContext} snapshot and subscribes to the instance's {@link PulsarHttpClientConfig#tlsPurpose()}
 * — each new connection's engine is created from the most recently delivered context, so rotated material
 * takes effect for new connections. A bounded {@code connectionTtl} caps how long an established pooled
 * connection can keep using pre-rotation material, and AsyncHttpClient's idle-connection timeout evicts
 * idle pooled connections; rotation is therefore effective within those bounds, not merely "eventually".
 * (AsyncHttpClient exposes no public API to force-flush idle pooled connections on the reload callback, so
 * the idle-timeout / TTL bounds are the mechanism.) On the legacy path (no {@code PulsarTlsFactory}) a
 * cluster-purpose client maps the config's {@code tls*} behavioural flags; other purposes (the OAuth2 IdP,
 * a different trust domain) use the platform default trust store, matching v4 behaviour.
 */
@CustomLog
public final class FrameworkHttpClientFactory implements PulsarHttpClientFactory, AutoCloseable {

    private final Supplier<EventLoopGroup> eventLoopGroup;
    private final Supplier<Timer> timer;
    private final Supplier<NameResolver<InetAddress>> nameResolver;
    private final Supplier<PulsarTlsFactory> tlsFactory;
    private final ClientConfigurationData conf;
    private final String clientInstanceId;

    private final Object lock = new Object();
    private final Set<PulsarHttpClient> openClients = new HashSet<>();
    private boolean closed;

    /**
     * @param eventLoopGroup the owning client's shared Netty event loop (never {@code null} at call time)
     * @param timer          the owning client's shared timer ({@code null} tolerated: AsyncHttpClient makes
     *                       its own)
     * @param nameResolver   the owning client's shared DNS resolver ({@code null} tolerated: AsyncHttpClient
     *                       resolves itself)
     * @param tlsFactory     the owning client's TLS factory on the new path, or {@code null} for legacy TLS
     *                       mapping
     * @param conf           the client configuration (legacy TLS flags, SOCKS5 proxy)
     * @param clientInstanceId a stable id of the owning client, for logging
     */
    public FrameworkHttpClientFactory(Supplier<EventLoopGroup> eventLoopGroup, Supplier<Timer> timer,
            Supplier<NameResolver<InetAddress>> nameResolver, Supplier<PulsarTlsFactory> tlsFactory,
            ClientConfigurationData conf, String clientInstanceId) {
        this.eventLoopGroup = eventLoopGroup;
        this.timer = timer;
        this.nameResolver = nameResolver;
        this.tlsFactory = tlsFactory;
        this.conf = conf;
        this.clientInstanceId = clientInstanceId;
    }

    /**
     * @return whether a PIP-478 TLS factory is available (the owning client is on the new TLS path), so a
     *         served {@link PulsarHttpClient} resolves its purpose's context from the factory — e.g. the
     *         folded {@code CLIENT_OAUTH2} IdP material — rather than the platform default trust store
     *         (PIP-478).
     */
    public boolean hasTlsFactory() {
        return tlsFactory.get() != null;
    }

    @Override
    public PulsarHttpClient newHttpClient(PulsarHttpClientConfig config) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("PulsarHttpClientFactory for " + clientInstanceId + " is closed");
            }
            DefaultAsyncHttpClientConfig.Builder builder = baseBuilder(config);
            TlsHandle<SslContext> tlsSubscription = configureTls(builder, config);
            builder.setEventLoopGroup(eventLoopGroup.get());
            Timer sharedTimer = timer.get();
            if (sharedTimer != null) {
                builder.setNettyTimer(sharedTimer);
            }
            configureSocks5(builder);
            AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient(builder.build());
            // The self-deregistering runnable needs the client instance, which does not exist yet; capture it
            // through a holder so close() removes exactly this instance from the tracking set.
            FrameworkHttpClient[] ref = new FrameworkHttpClient[1];
            FrameworkHttpClient client = new FrameworkHttpClient(asyncHttpClient, config, resolveNameResolver(),
                    tlsSubscription, () -> deregister(ref[0]));
            ref[0] = client;
            openClients.add(client);
            return client;
        }
    }

    private DefaultAsyncHttpClientConfig.Builder baseBuilder(PulsarHttpClientConfig config) {
        DefaultAsyncHttpClientConfig.Builder b = new DefaultAsyncHttpClientConfig.Builder();
        // Mirror org.apache.pulsar.client.impl.HttpClient's configuration approach.
        b.setCookieStore(null);
        b.setUseProxyProperties(true);
        // Let AHC follow redirects itself, as v4's OAuth2 client did: these instances serve auth-plugin
        // traffic (OAuth2 IdP metadata / token requests) that carries no Authorization header, so the
        // Authorization strip AHC >= 2.14.5 applies on cross-origin redirects (CVE-2026-40490 fix) has
        // nothing to strip — unlike org.apache.pulsar.client.impl.HttpClient, which must follow redirects
        // manually to re-authenticate per hop.
        b.setFollowRedirect(true);
        b.setMaxRedirects(5);
        b.setConnectTimeout(config.connectTimeout());
        b.setReadTimeout(config.readTimeout());
        b.setRequestTimeout(config.requestTimeout());
        b.setUserAgent(config.userAgent());
        return b;
    }

    /**
     * Configure TLS on the builder for the instance's purpose and return the live TLS subscription (to be
     * disposed with the client), or {@code null} on the legacy path.
     */
    private TlsHandle<SslContext> configureTls(DefaultAsyncHttpClientConfig.Builder builder,
            PulsarHttpClientConfig config) {
        PulsarTlsFactory factory = tlsFactory.get();
        if (factory != null) {
            // New PIP-478 path: subscribe to the purpose's Netty SslContext and build each engine from the
            // most recently delivered (possibly rotated) context.
            VolatileContext holder = new VolatileContext();
            TlsHandle<SslContext> subscription = TlsContextAcquisition.acquireNettyContext(factory,
                    config.tlsPurpose(), clientSynthesisSpec(config), ctx -> holder.context = ctx)
                    .join()
                    .orElseThrow(() -> new IllegalStateException(
                            "Client TLS factory supplied no Netty SslContext for purpose " + config.tlsPurpose()));
            builder.setSslEngineFactory(new SslEngineFactory() {
                @Override
                public SSLEngine newSslEngine(AsyncHttpClientConfig ahcConfig, String peerHost, int peerPort) {
                    // Client mode, SNI and baked-in hostname verification all come from the Netty context. Pin
                    // the context across newEngine so a concurrent rotation cannot free the native OpenSSL
                    // context mid-build (use-after-free guard).
                    return TlsContextAcquisition.withPinnedContext(() -> holder.context,
                            ctx -> ctx.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort));
                }
            });
            // Bound how long pre-rotation material survives on established pooled connections.
            builder.setConnectionTtl(Duration.ofMillis(TlsContextAcquisition.httpTlsRotationConnectionTtlMillis()));
            return subscription;
        }
        // Legacy path: only cluster (CLIENT_DEFAULT) traffic maps the client's tls* behavioural flags; a
        // different trust domain (the OAuth2 IdP) uses the platform default trust store, as v4 did.
        if (TlsPurpose.CLIENT_DEFAULT.equals(config.tlsPurpose())) {
            builder.setUseInsecureTrustManager(conf.isTlsAllowInsecureConnection());
            builder.setDisableHttpsEndpointIdentificationAlgorithm(!conf.isTlsHostnameVerificationEnable());
        }
        return null;
    }

    /**
     * The synthesis spec used when a custom factory supplies only the JDK {@code SSLContext} for the
     * instance's purpose. Cluster traffic ({@link TlsPurpose#CLIENT_DEFAULT}) carries the client's
     * {@code tlsHostnameVerificationEnable} flag, mirroring the legacy path; other trust domains (the OAuth2
     * IdP) verify hostnames — the AsyncHttpClient default and the v5 {@code TlsPolicy} secure default.
     */
    private TlsSynthesisSpec clientSynthesisSpec(PulsarHttpClientConfig config) {
        boolean enableHostnameVerification = !TlsPurpose.CLIENT_DEFAULT.equals(config.tlsPurpose())
                || conf.isTlsHostnameVerificationEnable();
        return TlsSynthesisSpec.client(enableHostnameVerification);
    }

    private void configureSocks5(DefaultAsyncHttpClientConfig.Builder builder) {
        if (conf == null || conf.getSocks5ProxyAddress() == null) {
            return;
        }
        Socks5ProxyScope scope = conf.getSocks5ProxyScope();
        if (scope == null || !scope.appliesToHttp()) {
            return;
        }
        ProxyServer.Builder proxyBuilder = new ProxyServer.Builder(
                conf.getSocks5ProxyAddress().getHostString(), conf.getSocks5ProxyAddress().getPort())
                .setProxyType(ProxyType.SOCKS_V5);
        String username = conf.getSocks5ProxyUsername();
        if (StringUtils.isNotBlank(username)) {
            proxyBuilder.setRealm(new Realm.Builder(username, conf.getSocks5ProxyPassword())
                    .setScheme(Realm.AuthScheme.BASIC).build());
        }
        builder.setProxyServer(proxyBuilder.build());
    }

    private NameResolver<InetAddress> resolveNameResolver() {
        try {
            return nameResolver.get();
        } catch (Throwable t) {
            // The shared resolver is not ready yet (a plugin fetching during client construction); fall back
            // to AsyncHttpClient's own resolver for this instance.
            return null;
        }
    }

    private void deregister(PulsarHttpClient client) {
        synchronized (lock) {
            // close() may have already cleared the set; identity removal is then a no-op.
            openClients.remove(client);
        }
    }

    @Override
    public void close() {
        List<PulsarHttpClient> toClose;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = new ArrayList<>(openClients);
            openClients.clear();
        }
        // Close outside the lock; each client's close() calls deregister() which then finds the set cleared.
        for (PulsarHttpClient client : toClose) {
            try {
                client.close();
            } catch (Throwable t) {
                log.warn().exception(t).log("Failed to close framework HTTP client during factory shutdown");
            }
        }
    }

    /** A mutable single-slot holder for the current (possibly rotated) Netty {@link SslContext}. */
    private static final class VolatileContext {
        private volatile SslContext context;
    }
}
