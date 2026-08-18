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

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslContext;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.impl.auth.v5.AsyncHttpAuthenticationProvider;
import org.apache.pulsar.client.impl.auth.v5.HttpAuthenticationDriver;
import org.apache.pulsar.client.impl.auth.v5.HttpChallengeTransport;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Realm;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.SslEngineFactory;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;


@CustomLog
public class HttpClient implements Closeable {

    private static final String ORIGINAL_PRINCIPAL_HEADER = "X-Original-Principal";
    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    protected static final int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    protected final AsyncHttpClient httpClient;
    protected final ServiceNameResolver serviceNameResolver;
    private final NameResolver<InetAddress> nameResolver;
    protected final Authentication authentication;
    protected final ClientConfigurationData clientConf;
    private final Executor blockingAuthExecutor;
    // PIP-478 (new TLS path): a subscription to the CLIENT_DEFAULT SslContext whose callback
    // updates the volatile below on rotation; the AsyncHttpClient SslEngineFactory builds engines from it.
    private TlsHandle<SslContext> tlsSubscription;
    private volatile SslContext clientSslContext;

    protected HttpClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, Timer timer,
                         NameResolver<InetAddress> nameResolver)
            throws PulsarClientException {
        this(conf, eventLoopGroup, timer, nameResolver, null);
    }

    protected HttpClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, Timer timer,
                         NameResolver<InetAddress> nameResolver, Executor blockingAuthExecutor)
            throws PulsarClientException {
        // Direct execution when no offload executor is supplied (tests, standalone constructions): the
        // production path (PulsarClientImpl -> HttpLookupService) always supplies the client's blocking
        // auth executor, which computeAuthHeaders needs to keep v4 auth work off the event loop.
        this.blockingAuthExecutor = blockingAuthExecutor != null ? blockingAuthExecutor : Runnable::run;
        this.authentication = conf.getAuthentication();
        this.clientConf = conf;
        this.serviceNameResolver = new PulsarServiceNameResolver(conf.getServiceUrlQuarantineInitDurationMs(),
                conf.getServiceUrlQuarantineMaxDurationMs());
        this.nameResolver = nameResolver;
        this.serviceNameResolver.updateServiceUrl(conf.getServiceUrl());

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        // Follow redirects manually in executeGet(...) so we can re-invoke authentication per hop and
        // carry the Authorization header across cross-origin redirects. async-http-client >= 2.14.5
        // (CVE-2026-40490 fix) strips the Authorization header when it follows redirects itself; Pulsar
        // HTTP lookups routinely redirect to another broker's httpUrl/httpUrlTls which is a different
        // host/port, i.e. cross-origin.
        confBuilder.setFollowRedirect(false);
        confBuilder.setMaxRedirects(conf.getMaxLookupRedirects());
        confBuilder.setConnectTimeout(Duration.ofSeconds(DEFAULT_CONNECT_TIMEOUT_IN_SECONDS));
        confBuilder.setReadTimeout(Duration.ofSeconds(DEFAULT_READ_TIMEOUT_IN_SECONDS));
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s%s",
                PulsarVersion.getVersion(),
                (conf.getDescription() == null ? "" : ("-" + conf.getDescription()))
        ));
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(InetSocketAddress remoteAddress, Request ahcRequest,
                                     HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5)
                       && super.keepAlive(remoteAddress, ahcRequest, request, response);
            }
        });

        if ("https".equals(serviceNameResolver.getServiceUri().getServiceName())) {
            try {
                // PIP-478: an https client always has isUseTls() forced true, so PulsarClientImpl has already
                // resolved the CLIENT_DEFAULT TLS factory onto conf (the only path since PIP-337 removal).
                setupHttpsWithTlsFactory(conf.getTlsFactory(), confBuilder);
            } catch (Exception e) {
                throw new PulsarClientException.InvalidConfigurationException(e);
            }
        }
        confBuilder.setEventLoopGroup(eventLoopGroup);
        confBuilder.setNettyTimer(timer);
        configureSocks5ProxyIfNeeded(confBuilder, conf);
        AsyncHttpClientConfig config = confBuilder.build();
        httpClient = new DefaultAsyncHttpClient(config);

        log.debug().attr("url", conf.getServiceUrl()).log("Using HTTP url");
    }

    /**
     * New PIP-478 HTTPS TLS setup: subscribe once to the {@link TlsPurpose#CLIENT_DEFAULT}
     * Netty {@code SslContext} — the callback updates a volatile on rotation — and back AsyncHttpClient's
     * {@link SslEngineFactory} with it. The AsyncHttpClient {@code SslEngineFactory} is invoked
     * synchronously per connection, so (unlike the binary path's one-shot form) a live subscription plus
     * a volatile snapshot is used here. Hostname verification, insecure trust, ciphers and protocols are
     * baked into the factory-built context per the client {@code TlsPolicy}, so the AsyncHttpClient
     * endpoint-identification / insecure-trust-manager flags are not applied.
     *
     * <p>Acquisition goes through {@link TlsContextAcquisition} so a custom factory that supplies only the
     * JDK {@code SSLContext} is served a framework-synthesized Netty context carrying the client's
     * hostname-verification setting.
     */
    private void setupHttpsWithTlsFactory(PulsarTlsFactory factory,
            DefaultAsyncHttpClientConfig.Builder confBuilder) throws Exception {
        this.tlsSubscription = TlsContextAcquisition.acquireNettyContext(factory, TlsPurpose.CLIENT_DEFAULT,
                        TlsSynthesisSpec.client(clientConf.isTlsHostnameVerificationEnable()),
                        ctx -> this.clientSslContext = ctx)
                .get()
                .orElseThrow(() -> new IllegalStateException(
                        "Client TLS factory supplied no Netty SslContext for purpose "
                                + TlsPurpose.CLIENT_DEFAULT));
        confBuilder.setSslEngineFactory(new SslEngineFactory() {
            @Override
            public javax.net.ssl.SSLEngine newSslEngine(AsyncHttpClientConfig config, String peerHost,
                    int peerPort) {
                // Build the engine from the current (possibly rotated) factory-owned context, pinning it across
                // newEngine so a concurrent rotation cannot free the native OpenSSL context mid-build
                // (use-after-free guard). Client mode, SNI and baked-in hostname verification all come from the
                // Netty context.
                return TlsContextAcquisition.withPinnedContext(() -> clientSslContext,
                        ctx -> ctx.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort));
            }
        });
        // Bound how long an established pooled HTTPS connection keeps pre-rotation material: new
        // connections pick up rotation via the SslEngineFactory above, but a pooled connection would otherwise
        // hold pre-rotation trust/cert indefinitely. Aligns with FrameworkHttpClientFactory.
        confBuilder.setConnectionTtl(Duration.ofMillis(TlsContextAcquisition.httpTlsRotationConnectionTtlMillis()));
    }

    String getServiceUrl() {
        return this.serviceNameResolver.getServiceUrl();
    }

    public InetSocketAddress resolveHost() {
        return serviceNameResolver.resolveHost();
    }

    void setServiceUrl(String serviceUrl) throws PulsarClientException {
        this.serviceNameResolver.updateServiceUrl(serviceUrl);
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
        // PIP-478: release the CLIENT_DEFAULT TLS subscription (the factory itself is owned and
        // closed by PulsarClientImpl).
        if (tlsSubscription != null) {
            tlsSubscription.dispose();
        }
    }

    public <T> CompletableFuture<T> get(String path, Class<T> clazz) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            URI hostUri = serviceNameResolver.resolveHostUri();
            String requestUrl = new URL(hostUri.toURL(), path).toString();
            InetSocketAddress originalHost = InetSocketAddress.createUnresolved(hostUri.getHost(), hostUri.getPort());
            executeGet(requestUrl, originalHost, clientConf.getMaxLookupRedirects(), future, clazz);
        } catch (Exception e) {
            log.warn().attr("path", path).exceptionMessage(e).log("Failed to initiate HTTP get request");
            if (e instanceof PulsarClientException) {
                future.completeExceptionally(e);
            } else {
                future.completeExceptionally(new PulsarClientException(e));
            }
        }

        return future;
    }

    private <T> void executeGet(String requestUrl, InetSocketAddress originalHost,
                                int redirectsRemaining, CompletableFuture<T> future, Class<T> clazz) {
        try {
            URI currentUri = URI.create(requestUrl);

            computeAuthHeaders(currentUri).whenComplete((authHeaders, ex) -> {
                if (ex != null) {
                    serviceNameResolver.markHostAvailability(originalHost, false);
                    log.warn().attr("requestUrl", requestUrl)
                            .exceptionMessage(ex)
                            .log("Failed to perform http request at authentication stage");
                    future.completeExceptionally(toPulsarClientException(ex));
                    return;
                }

                BoundRequestBuilder builder = httpClient.prepareGet(requestUrl)
                        // share the DNS resolver and cache with Pulsar client
                        .setNameResolver(nameResolver)
                        .setHeader("Accept", "application/json");

                if (authHeaders != null) {
                    authHeaders.forEach((name, value) -> builder.addHeader(name, value));
                }

                // Add X-Original-Principal header if originalPrincipal is configured (for proxy scenarios)
                if (clientConf.getOriginalPrincipal() != null) {
                    builder.addHeader(ORIGINAL_PRINCIPAL_HEADER, clientConf.getOriginalPrincipal());
                }

                builder.execute().toCompletableFuture().whenComplete((response2, t) -> {
                    if (t != null) {
                        serviceNameResolver.markHostAvailability(originalHost, false);
                        log.warn().attr("requestUrl", requestUrl)
                                .exceptionMessage(t)
                                .log("Failed to perform http request");
                        future.completeExceptionally(new PulsarClientException(t));
                        return;
                    }
                    serviceNameResolver.markHostAvailability(originalHost, true);

                    int statusCode = response2.getStatusCode();
                    if (isRedirectStatusCode(statusCode)) {
                        handleRedirect(requestUrl, currentUri, response2, originalHost,
                                redirectsRemaining, future, clazz);
                        return;
                    }

                    // request not success
                    if (statusCode != HttpURLConnection.HTTP_OK) {
                        String errorReason = response2.getStatusText();
                        if ("application/json".equals(response2.getContentType()) || "text/json".equals(
                                response2.getContentType())) {
                            String responseBody = response2.getResponseBody();
                            try {
                                JsonNode jsonNode =
                                        ObjectMapperFactory.getMapper().getObjectMapper().readTree(responseBody);
                                if (jsonNode.has("reason") && jsonNode.get("reason").isTextual()) {
                                    errorReason = jsonNode.get("reason").asText();
                                } else if (jsonNode.has("message") && jsonNode.get("message").isTextual()) {
                                    errorReason = jsonNode.get("message").asText();
                                }
                            } catch (IOException e) {
                                // ignore
                                    log.debug().attr("requestUrl", requestUrl)
                                            .attr("response", responseBody)
                                            .log("Failed to parse error response");
                            }
                        }
                        log.warn().attr("requestUrl", requestUrl)
                                .attr("failed", errorReason)
                                .log("HTTP get request failed");
                        Exception e;
                        if (statusCode == HttpURLConnection.HTTP_NOT_FOUND) {
                            e = new NotFoundException("Not found: " + errorReason);
                        } else {
                            e = new PulsarClientException("HTTP get request failed: " + errorReason);
                        }
                        future.completeExceptionally(e);
                        return;
                    }

                    try {
                        T data = ObjectMapperFactory.getMapper().reader().readValue(
                                response2.getResponseBodyAsBytes(), clazz);
                        future.complete(data);
                    } catch (Exception e) {
                        log.warn().attr("requestUrl", requestUrl)
                                .exceptionMessage(e)
                                .log("Error during HTTP get request");
                        future.completeExceptionally(new PulsarClientException(e));
                    }
                });
            });
        } catch (Exception e) {
            log.warn().attr("requestUrl", requestUrl).exceptionMessage(e).log("HTTP request setup failed");
            if (e instanceof PulsarClientException) {
                future.completeExceptionally(e);
            } else {
                future.completeExceptionally(new PulsarClientException(e));
            }
        }
    }

    /**
     * Compute the authentication headers to attach to the outgoing lookup {@code GET}, or a future of
     * {@code null} when the plugin contributes none (PIP-478).
     *
     * <p>When the plugin exposes the v5-native SASL-over-HTTP capability (via the
     * {@link AsyncHttpAuthenticationProvider} bridge), the framework {@link HttpAuthenticationDriver} runs
     * the bounded {@code 401}→resubmit→{@code 200} exchange over this client's shared AsyncHttpClient
     * (a bodiless {@code GET} to the original URI each round) and yields the validated role-token headers.
     * Otherwise the deprecated v4 {@code authenticationStage(...)} / {@code newRequestHeader(...)} hooks
     * run verbatim, preserving behaviour for third-party plugins and single-pass built-ins.
     */
    private CompletableFuture<Map<String, String>> computeAuthHeaders(URI uri) {
        try {
            if (authentication instanceof AsyncHttpAuthenticationProvider provider) {
                Optional<HttpAuthenticationDriver> driver = provider.httpAuthenticationDriver()
                        .filter(HttpAuthenticationDriver::supportsHttpChallenge);
                if (driver.isPresent()) {
                    long lookupTimeoutMs = clientConf.getLookupTimeoutMs();
                    Duration budget = Duration.ofMillis(lookupTimeoutMs > 0 ? lookupTimeoutMs : 60_000L);
                    return driver.get().authenticateAsync(uri, new AhcChallengeTransport(), budget)
                            .thenApply(headers -> (headers == null || headers.isEmpty()) ? null : headers.asMap());
                }
            }
            // The deprecated v4 hooks may block: an OAuth2 shim's getAuthData() refreshes its token with a
            // synchronous HTTP exchange served by this client's own shared event-loop group, and this method
            // can run inside an AHC completion callback on a shared Netty IO thread (the manual-redirect
            // path) — a self-deadlock that stalls the loop until the request timeout. Offload the whole v4
            // composition to the client's blocking auth executor (PIP-478).
            return CompletableFuture.supplyAsync(() -> v4AuthHeaders(uri), blockingAuthExecutor)
                    .thenCompose(headers -> headers);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private CompletableFuture<Map<String, String>> v4AuthHeaders(URI uri) {
        try {
            AuthenticationDataProvider authData = authentication.getAuthData(uri.getHost());
            if (!authData.hasDataForHttp()) {
                return CompletableFuture.completedFuture(null);
            }
            // v4 SASL-style multi-round hook: authenticationStage warms the role token, then newRequestHeader
            // composes the real request's headers. Non-SASL single-pass plugins complete the stage with null
            // and newRequestHeader returns their static HTTP headers.
            CompletableFuture<Map<String, String>> stage = new CompletableFuture<>();
            authentication.authenticationStage(uri.toString(), authData, null, stage);
            return stage.thenApply(respHeaders -> {
                try {
                    Set<Entry<String, String>> headers =
                            authentication.newRequestHeader(uri.toString(), authData, respHeaders);
                    if (headers == null) {
                        return null;
                    }
                    Map<String, String> map = new LinkedHashMap<>();
                    headers.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
                    return map;
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private static PulsarClientException toPulsarClientException(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
        return cause instanceof PulsarClientException pce ? pce : new PulsarClientException(cause);
    }

    /**
     * A {@link HttpChallengeTransport} backed by this client's shared AsyncHttpClient (PIP-478):
     * the SASL warmup rounds re-use the same event-loop / DNS resolver as the real lookup request. Each
     * round is a bodiless {@code GET} to the original URI.
     */
    private final class AhcChallengeTransport implements HttpChallengeTransport {
        @Override
        public CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout) {
            CompletableFuture<Result> resultFuture = new CompletableFuture<>();
            try {
                BoundRequestBuilder builder = httpClient.prepareGet(uri.toString())
                        .setNameResolver(nameResolver)
                        .setHeader("Accept", "application/json");
                requestHeaders.asMap().forEach((name, value) -> builder.addHeader(name, value));
                if (timeout != null && !timeout.isNegative() && !timeout.isZero()) {
                    builder.setRequestTimeout(timeout);
                }
                builder.execute().toCompletableFuture().whenComplete((response, t) -> {
                    if (t != null) {
                        resultFuture.completeExceptionally(t);
                        return;
                    }
                    Map<String, String> headers = new LinkedHashMap<>();
                    response.getHeaders().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
                    resultFuture.complete(new Result(response.getStatusCode(), HttpAuthHeaders.of(headers)));
                });
            } catch (Throwable t) {
                resultFuture.completeExceptionally(t);
            }
            return resultFuture;
        }
    }

    private <T> void handleRedirect(String requestUrl, URI currentUri,
                                    Response response,
                                    InetSocketAddress originalHost, int redirectsRemaining,
                                    CompletableFuture<T> future, Class<T> clazz) {
        String location = response.getHeader("Location");
        if (location == null || location.isEmpty()) {
            future.completeExceptionally(new PulsarClientException(
                    "HTTP redirect " + response.getStatusCode() + " without Location header: " + requestUrl));
            return;
        }
        if (redirectsRemaining <= 0) {
            future.completeExceptionally(new PulsarClientException(
                    "Maximum redirects exceeded (" + clientConf.getMaxLookupRedirects()
                            + ") while following HTTP redirect for " + requestUrl));
            return;
        }
        String newUrl;
        try {
            newUrl = currentUri.resolve(location).toString();
        } catch (Exception e) {
            future.completeExceptionally(new PulsarClientException(
                    "Invalid redirect Location \"" + location + "\" for " + requestUrl));
            return;
        }
        executeGet(newUrl, originalHost, redirectsRemaining - 1, future, clazz);
    }

    private static boolean isRedirectStatusCode(int statusCode) {
        return statusCode == HttpURLConnection.HTTP_MOVED_PERM   // 301
                || statusCode == HttpURLConnection.HTTP_MOVED_TEMP   // 302
                || statusCode == HttpURLConnection.HTTP_SEE_OTHER    // 303
                || statusCode == 307                                 // Temporary Redirect
                || statusCode == 308;                                // Permanent Redirect
    }

    /**
     * Configure SOCKS5 proxy on the async-http-client builder when the proxy address is set and
     * the configured {@link Socks5ProxyScope} includes HTTP traffic.
     *
     * <p>The default scope for {@code PulsarClient} is {@link Socks5ProxyScope#BINARY_ONLY}, so
     * HTTP lookups and failover HTTP clients will NOT use the proxy unless the caller explicitly
     * sets the scope to {@link Socks5ProxyScope#HTTP_ONLY} or {@link Socks5ProxyScope#BOTH}.
     */
    private static void configureSocks5ProxyIfNeeded(DefaultAsyncHttpClientConfig.Builder confBuilder,
                                                     ClientConfigurationData conf) {
        if (conf == null) {
            return;
        }
        InetSocketAddress socks5Address = conf.getSocks5ProxyAddress();
        if (socks5Address == null) {
            return;
        }
        if (!conf.getSocks5ProxyScope().appliesToHttp()) {
            return;
        }
        ProxyServer.Builder proxyBuilder =
                new ProxyServer.Builder(socks5Address.getHostString(), socks5Address.getPort())
                        .setProxyType(ProxyType.SOCKS_V5);
        String socks5Username = conf.getSocks5ProxyUsername();
        if (StringUtils.isNotBlank(socks5Username)) {
            Realm realm = new Realm.Builder(socks5Username, conf.getSocks5ProxyPassword())
                    .setScheme(Realm.AuthScheme.BASIC)
                    .build();
            proxyBuilder.setRealm(realm);
        }
        confBuilder.setProxyServer(proxyBuilder.build());
        log.info().attr("proxy", socks5Address).log("Pulsar client HTTP lookup is using SOCKS5 proxy");
    }

}
