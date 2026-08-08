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

import static org.asynchttpclient.util.HttpConstants.Methods.GET;
import static org.asynchttpclient.util.HttpConstants.Methods.HEAD;
import static org.asynchttpclient.util.HttpConstants.Methods.OPTIONS;
import static org.asynchttpclient.util.HttpConstants.ResponseStatusCodes.FOUND_302;
import static org.asynchttpclient.util.HttpConstants.ResponseStatusCodes.MOVED_PERMANENTLY_301;
import static org.asynchttpclient.util.HttpConstants.ResponseStatusCodes.PERMANENT_REDIRECT_308;
import static org.asynchttpclient.util.HttpConstants.ResponseStatusCodes.SEE_OTHER_303;
import static org.asynchttpclient.util.HttpConstants.ResponseStatusCodes.TEMPORARY_REDIRECT_307;
import static org.asynchttpclient.util.MiscUtils.isNonEmpty;
import com.google.common.annotations.VisibleForTesting;
import com.spotify.futures.ConcurrencyReducer;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslContext;
import io.netty.resolver.NameResolver;
import io.netty.util.concurrent.DefaultThreadFactory;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response.Status;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;
import lombok.CustomLog;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.impl.PulsarClientSharedResourcesImpl;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.ServiceNameResolver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.tls.ClientTlsFactorySupport;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.DnsResolverUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Realm;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.SslEngineFactory;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;
import org.asynchttpclient.uri.Uri;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.client.spi.AsyncConnectorCallback;
import org.glassfish.jersey.client.spi.Connector;

/**
 * Customized Jersey client connector with multi-host support.
 */
@CustomLog
public class AsyncHttpConnector implements Connector, AsyncHttpRequestExecutor {
    private static final TimeoutException REQUEST_TIMEOUT_EXCEPTION =
            FutureUtil.createTimeoutException("Request timeout", AsyncHttpConnector.class, "retryOrTimeout(...)");
    private static final int DEFAULT_MAX_QUEUE_SIZE_PER_HOST = 10000;
    @Getter
    private final AsyncHttpClient httpClient;
    private final Duration requestTimeout;
    private final int maxRetries;
    private final ServiceNameResolver serviceNameResolver;
    private final ScheduledExecutorService delayer = Executors.newScheduledThreadPool(1,
            new DefaultThreadFactory("delayer"));
    private final boolean acceptGzipCompression;
    @Getter
    private final NameResolver<InetAddress> nameResolver;
    private final EventLoopGroup eventLoopGroup;
    private final boolean createdEventLoopGroup;
    private final Map<String, ConcurrencyReducer<Response>> concurrencyReducers = new ConcurrentHashMap<>();
    // PIP-478 (new TLS path): the resolved PulsarTlsFactory (this connector owns and closes it),
    // a live subscription to the CLIENT_DEFAULT SslContext whose callback refreshes the volatile below on
    // rotation, and the single-thread executor that drives the factory's material rotation / blocking loads.
    private PulsarTlsFactory tlsFactory;
    private TlsHandle<SslContext> tlsFactorySubscription;
    private volatile SslContext tlsFactorySslContext;
    private ScheduledExecutorService tlsFactoryExecutor;
    @Getter
    @Setter
    private boolean followRedirects = true;

    public AsyncHttpConnector(Client client, ClientConfigurationData conf, int autoCertRefreshTimeSeconds,
                              boolean acceptGzipCompression) {
        this((int) client.getConfiguration().getProperty(ClientProperties.CONNECT_TIMEOUT),
                (int) client.getConfiguration().getProperty(ClientProperties.READ_TIMEOUT),
                PulsarAdminImpl.DEFAULT_REQUEST_TIMEOUT_SECONDS * 1000,
                autoCertRefreshTimeSeconds,
                conf, acceptGzipCompression, null);
    }

    @SneakyThrows
    public AsyncHttpConnector(int connectTimeoutMs, int readTimeoutMs,
                              int requestTimeoutMs,
                              int autoCertRefreshTimeSeconds, ClientConfigurationData conf,
                              boolean acceptGzipCompression,
                              PulsarClientSharedResourcesImpl sharedResources) {
        Validate.notEmpty(conf.getServiceUrl(), "Service URL is not provided");
        serviceNameResolver = new PulsarServiceNameResolver();
        String serviceUrl = conf.getServiceUrl();
        serviceNameResolver.updateServiceUrl(serviceUrl);
        this.acceptGzipCompression = acceptGzipCompression;
        SharedResourceHolder sharedResourceHolder =
                buildResourcesIfConfigured(sharedResources);
        this.nameResolver = sharedResourceHolder.getNameResolver();
        this.eventLoopGroup = sharedResourceHolder.getEventLoopGroup();
        this.createdEventLoopGroup = sharedResourceHolder.isCreateEventLoop();
        boolean initialized = false;
        try {
            AsyncHttpClientConfig asyncHttpClientConfig =
                    createAsyncHttpClientConfig(conf, connectTimeoutMs, readTimeoutMs, requestTimeoutMs,
                            autoCertRefreshTimeSeconds, sharedResources);
            httpClient = createAsyncHttpClient(asyncHttpClientConfig);
            initialized = true;
        } finally {
            if (!initialized) {
                // Ctor-throw cleanup: createAsyncHttpClientConfig may already have allocated the TLS
                // factory and its NON-daemon rotation executor (and, if we created it, the event loop group);
                // release them here since close() will not run on a half-constructed connector — otherwise the
                // non-daemon executor keeps the JVM alive.
                releasePartialResources();
            }
        }
        this.requestTimeout = requestTimeoutMs > 0 ? Duration.ofMillis(requestTimeoutMs) : null;
        this.maxRetries = httpClient.getConfig().getMaxRequestRetry();
    }

    /** Release resources that may have been allocated before a constructor failure. */
    private void releasePartialResources() {
        if (tlsFactorySubscription != null) {
            tlsFactorySubscription.dispose();
        }
        if (tlsFactory != null) {
            tlsFactory.close();
        }
        if (tlsFactoryExecutor != null) {
            tlsFactoryExecutor.shutdownNow();
        }
        if (createdEventLoopGroup && eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
            eventLoopGroup.shutdownGracefully();
        }
    }

    private SharedResourceHolder buildResourcesIfConfigured(
            PulsarClientSharedResourcesImpl sharedResources) {
        EventLoopGroup eventLoopGroup = null;
        NameResolver<InetAddress> nameResolver = null;
        boolean createdEventLoopGroup = false;
        if (sharedResources != null && sharedResources.getDnsResolverGroup() != null) {
            if (sharedResources.getIoEventLoopGroup() != null) {
                eventLoopGroup = sharedResources.getIoEventLoopGroup();
            } else {
                // build an EventLoopGroup with default value
                eventLoopGroup = EventLoopUtil.newEventLoopGroup(
                        Runtime.getRuntime().availableProcessors(), false,
                        new ExecutorProvider.ExtendedThreadFactory("pulsar-admin-client-io",
                                Thread.currentThread().isDaemon()));
                createdEventLoopGroup = true;
            }
            nameResolver = DnsResolverUtil.adaptToNameResolver(
                    sharedResources.getDnsResolverGroup().createAddressResolver(eventLoopGroup));
        } else {
            return SharedResourceHolder.EMPTY;
        }
        return new SharedResourceHolder(nameResolver, eventLoopGroup, createdEventLoopGroup);
    }

    private AsyncHttpClientConfig createAsyncHttpClientConfig(ClientConfigurationData conf, int connectTimeoutMs,
                                                              int readTimeoutMs,
                                                              int requestTimeoutMs,
                                                              int autoCertRefreshTimeSeconds,
                                                              PulsarClientSharedResourcesImpl sharedResources)
            throws GeneralSecurityException, IOException {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        configureAsyncHttpClientConfig(conf, connectTimeoutMs,
                readTimeoutMs, requestTimeoutMs, confBuilder, sharedResources);
        if (conf.getServiceUrl().startsWith("https://")) {
            configureAsyncHttpClientWithTlsFactory(confBuilder, resolveNewTlsFactory(conf), conf);
        }
        AsyncHttpClientConfig asyncHttpClientConfig = confBuilder.build();
        return asyncHttpClientConfig;
    }

    private void configureAsyncHttpClientConfig(ClientConfigurationData conf, int connectTimeoutMs, int readTimeoutMs,
                                                int requestTimeoutMs,
                                                DefaultAsyncHttpClientConfig.Builder confBuilder,
                                                PulsarClientSharedResourcesImpl sharedResources) {
        if (conf.getConnectionsPerBroker() > 0) {
            confBuilder.setMaxConnectionsPerHost(conf.getConnectionsPerBroker());
            // Use the request timeout value for acquireFreeChannelTimeout so that we don't need to add
            // yet another configuration property. When the ConcurrencyReducer is in use, it shouldn't be necessary to
            // wait for a free channel since the ConcurrencyReducer will queue the requests.
            confBuilder.setAcquireFreeChannelTimeout(conf.getRequestTimeoutMs());
        }
        if (conf.getConnectionMaxIdleSeconds() > 0) {
            confBuilder.setPooledConnectionIdleTimeout(Duration.ofSeconds(conf.getConnectionMaxIdleSeconds()));
        }
        if (sharedResources != null) {
            if (this.eventLoopGroup != null) {
                confBuilder.setEventLoopGroup(this.eventLoopGroup);
            }
            if (sharedResources.getTimer() != null) {
                confBuilder.setNettyTimer(sharedResources.getTimer());
            }
        }
        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(false);
        confBuilder.setRequestTimeout(Duration.ofMillis(conf.getRequestTimeoutMs()));
        confBuilder.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        confBuilder.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s%s",
                PulsarVersion.getVersion(),
                (conf.getDescription() == null ? "" : ("-" + conf.getDescription()))
        ));
        confBuilder.setRequestTimeout(Duration.ofMillis(requestTimeoutMs));
        confBuilder.setIoThreadsCount(conf.getNumIoThreads());
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(InetSocketAddress remoteAddress, Request ahcRequest,
                                     HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5)
                        && super.keepAlive(remoteAddress, ahcRequest, request, response);
            }
        });
        // This flag only governs AsyncHttpClient's default engine factory; the https path installs its own
        // SslEngineFactory with hostname verification baked into the factory-built Netty context (see
        // configureAsyncHttpClientWithTlsFactory).
        confBuilder.setDisableHttpsEndpointIdentificationAlgorithm(!conf.isTlsHostnameVerificationEnable());
        configureSocks5ProxyIfNeeded(confBuilder, conf);
    }

    protected AsyncHttpClient createAsyncHttpClient(AsyncHttpClientConfig asyncHttpClientConfig) {
        return new DefaultAsyncHttpClient(asyncHttpClientConfig);
    }

    /**
     * Resolve the new-SPI TLS factory for an https admin URL (the only path since the PIP-337 removal):
     * a {@link PulsarTlsFactory} adopted through the {@code tlsFactory} seam (the broker's admin-client
     * attach — see {@code PulsarService.getCreateAdminClientBuilder}), or the built-in file-based factory
     * composed from the {@code tls*} fields. The connector owns the resolved factory (and its rotation
     * executor) and closes both in {@link #close()}.
     */
    @SneakyThrows
    private PulsarTlsFactory resolveNewTlsFactory(ClientConfigurationData conf) {
        // The factory needs a framework scheduler for material-rotation polling and an executor for blocking
        // material loading; the admin connector owns a small single-thread scheduled executor for both (there
        // is no shared client scheduler here, unlike the PulsarClientImpl funnel).
        this.tlsFactoryExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-admin-tls-factory"));
        PulsarTlsFactory factory = ClientTlsFactorySupport.resolveClientTlsFactory(conf, tlsFactoryExecutor,
                tlsFactoryExecutor, conf.getOpenTelemetry());
        this.tlsFactory = factory;
        return factory;
    }

    /**
     * New PIP-478 HTTPS TLS setup: subscribe once to the {@link TlsPurpose#CLIENT_DEFAULT} Netty
     * {@code SslContext} — the callback refreshes a volatile on rotation — and back the AsyncHttpClient
     * {@link SslEngineFactory} with it, mirroring {@code org.apache.pulsar.client.impl.HttpClient} and the
     * framework {@code FrameworkHttpClientFactory}. Each connection's engine is built from the most recently
     * delivered (possibly rotated) context; hostname verification, insecure trust, ciphers and protocols are
     * baked into the factory-built context per the {@code TlsPolicy}, so the AsyncHttpClient
     * endpoint-identification / insecure-trust-manager flags are not applied on this path.
     *
     * <p>Acquisition goes through {@link TlsContextAcquisition} so a custom factory that supplies only the
     * JDK {@code SSLContext} is served a framework-synthesized Netty context carrying the client's
     * hostname-verification setting.
     */
    @SneakyThrows
    private void configureAsyncHttpClientWithTlsFactory(DefaultAsyncHttpClientConfig.Builder confBuilder,
                                                        PulsarTlsFactory factory, ClientConfigurationData conf) {
        this.tlsFactorySubscription = TlsContextAcquisition.acquireNettyContext(factory, TlsPurpose.CLIENT_DEFAULT,
                        TlsSynthesisSpec.client(conf.isTlsHostnameVerificationEnable()),
                        ctx -> this.tlsFactorySslContext = ctx)
                .get()
                .orElseThrow(() -> new IllegalStateException(
                        "Admin TLS factory supplied no Netty SslContext for purpose " + TlsPurpose.CLIENT_DEFAULT));
        confBuilder.setSslEngineFactory(new SslEngineFactory() {
            @Override
            public SSLEngine newSslEngine(AsyncHttpClientConfig ahcConfig, String peerHost, int peerPort) {
                // Client mode, SNI and baked-in hostname verification all come from the Netty context. Pin the
                // context across newEngine so a concurrent rotation cannot free the native OpenSSL context
                // mid-build (use-after-free guard).
                return TlsContextAcquisition.withPinnedContext(() -> tlsFactorySslContext,
                        ctx -> ctx.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort));
            }
        });
        // Bound how long an established pooled HTTPS connection keeps pre-rotation material: new
        // connections pick up rotation via the SslEngineFactory above, but a pooled connection would otherwise
        // hold pre-rotation trust/cert indefinitely. Aligns with FrameworkHttpClientFactory.
        confBuilder.setConnectionTtl(Duration.ofMillis(TlsContextAcquisition.httpTlsRotationConnectionTtlMillis()));
    }

    /**
     * @return the resolved PIP-478 TLS factory when this connector is on the new TLS path, or {@code null} on
     *         the legacy PIP-337 path
     */
    @VisibleForTesting
    public PulsarTlsFactory getTlsFactory() {
        return tlsFactory;
    }

    /**
     * @return the AsyncHttpClient connection-TTL (millis) bounding how long a pooled HTTPS connection keeps
     *         pre-rotation TLS material on the new factory path (negative when unset, i.e. the plaintext path)
     */
    @VisibleForTesting
    public int getConnectionTtlMs() {
        return (int) httpClient.getConfig().getConnectionTtl().toMillis();
    }

    @Override
    public ClientResponse apply(ClientRequest jerseyRequest) {
        CompletableFuture<ClientResponse> future = new CompletableFuture<>();
        apply(jerseyRequest, new AsyncConnectorCallback() {
            @Override
            public void response(ClientResponse response) {
                future.complete(response);
            }

            @Override
            public void failure(Throwable failure) {
                future.completeExceptionally(failure);
            }
        });
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new ProcessingException(e.getCause());
        }
    }

    private URI replaceWithNew(InetSocketAddress address, URI uri) {
        String originalUri = uri.toString();
        String newUri = (originalUri.split(":")[0] + "://")
                + address.getHostString() + ":"
                + address.getPort()
                + uri.getRawPath();
        if (uri.getRawQuery() != null) {
            newUri += "?" + uri.getRawQuery();
        }
        return URI.create(newUri);
    }

    @Override
    public Future<?> apply(ClientRequest jerseyRequest, AsyncConnectorCallback callback) {
        CompletableFuture<Response> responseFuture = retryOrTimeOut(jerseyRequest);
        responseFuture.whenComplete(((response, throwable) -> {
            if (throwable != null) {
                callback.failure(throwable);
            } else {
                ClientResponse jerseyResponse =
                        new ClientResponse(Status.fromStatusCode(response.getStatusCode()), jerseyRequest);
                jerseyResponse.setStatusInfo(new jakarta.ws.rs.core.Response.StatusType() {
                    @Override
                    public int getStatusCode() {
                        return response.getStatusCode();
                    }

                    @Override
                    public Status.Family getFamily() {
                        return Status.Family.familyOf(response.getStatusCode());
                    }

                    @Override
                    public String getReasonPhrase() {
                        if (response.hasResponseBody()) {
                            return response.getResponseBody();
                        }
                        return response.getStatusText();
                    }
                });
                response.getHeaders().forEach(e -> jerseyResponse.header(e.getKey(), e.getValue()));
                if (response.hasResponseBody()) {
                    jerseyResponse.setEntityStream(response.getResponseBodyAsStream());
                }
                try {
                    callback.response(jerseyResponse);
                } catch (Exception ex) {
                    log.error().exception(ex).attr("response", jerseyResponse)
                            .log("Failed to handle the http response");
                }
            }
        }));
        return responseFuture;
    }

    private CompletableFuture<Response> retryOrTimeOut(ClientRequest request) {
        final CompletableFuture<Response> resultFuture = new CompletableFuture<>();
        retryOperation(resultFuture, () -> oneShot(serviceNameResolver.resolveHost(), request), maxRetries);
        if (requestTimeout != null) {
            FutureUtil.addTimeoutHandling(resultFuture, requestTimeout, delayer, () -> REQUEST_TIMEOUT_EXCEPTION);
        }
        return resultFuture;
    }

    // TODO: There are problems with this solution since AsyncHttpClient already contains logic to retry requests.
    // This solution doesn't contain backoff handling.
    private <T> void retryOperation(
            final CompletableFuture<T> resultFuture,
            final Supplier<CompletableFuture<T>> operation,
            final int retries) {

        if (!resultFuture.isDone()) {
            final CompletableFuture<T> operationFuture = operation.get();

            operationFuture.whenComplete(
                    (t, throwable) -> {
                        if (throwable != null) {
                            throwable = FutureUtil.unwrapCompletionException(throwable);
                            if (throwable instanceof CancellationException) {
                                resultFuture.completeExceptionally(
                                        new RetryException("Operation future was cancelled.", throwable));
                            } else if (throwable instanceof MaxRedirectException) {
                                // don't retry on max redirect
                                resultFuture.completeExceptionally(throwable);
                            } else {
                                if (retries > 0) {
                                    log.debug().attr("remainingRetries", retries)
                                            .log("Retrying operation");
                                    retryOperation(
                                            resultFuture,
                                            operation,
                                            retries - 1);
                                } else {
                                    log.debug().exception(throwable)
                                            .log("Number of retries has been exhausted. Failing the operation");
                                    resultFuture.completeExceptionally(
                                            new RetryException("Could not complete the operation. Number of retries "
                                                    + "has been exhausted. Failed reason: " + throwable.getMessage(),
                                                    throwable));
                                }
                            }
                        } else {
                            resultFuture.complete(t);
                        }
                    });

            resultFuture.whenComplete(
                    (t, throwable) -> operationFuture.cancel(false));
        }
    }

    /**
     * Retry Exception.
     */
    public static class RetryException extends Exception {
        public RetryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class MaxRedirectException extends Exception {
        public MaxRedirectException(String msg) {
            super(msg, null, true, false);
        }
    }

    protected CompletableFuture<Response> oneShot(InetSocketAddress host, ClientRequest request) {
        Request preparedRequest;
        try {
            preparedRequest = prepareRequest(host, request);
        } catch (IOException e) {
            return FutureUtil.failedFuture(e);
        }
        return executeRequest(preparedRequest);
    }

    public CompletableFuture<Response> executeRequest(Request request) {
        return executeRequest(request, () -> new AsyncCompletionHandlerBase());
    }

    public CompletableFuture<Response> executeRequest(Request request,
                                                      Supplier<AsyncHandler<Response>> handlerSupplier) {
        return executeRequest(request, handlerSupplier, 0);
    }

    private CompletableFuture<Response> executeRequest(Request request,
                                                       Supplier<AsyncHandler<Response>> handlerSupplier,
                                                       int redirectCount) {
        int maxRedirects = httpClient.getConfig().getMaxRedirects();
        if (redirectCount > maxRedirects) {
            return FutureUtil.failedFuture(
                    new MaxRedirectException("Maximum redirect reached: " + maxRedirects + " uri:" + request.getUri()));
        }
        CompletableFuture<Response> responseFuture;
        if (httpClient.getConfig().getMaxConnectionsPerHost() > 0) {
            String hostAndPort = request.getUri().getHost() + ":" + request.getUri().getPort();
            ConcurrencyReducer<Response> responseConcurrencyReducer = concurrencyReducers.computeIfAbsent(hostAndPort,
                    h -> ConcurrencyReducer.create(httpClient.getConfig().getMaxConnectionsPerHost(),
                            DEFAULT_MAX_QUEUE_SIZE_PER_HOST));
            responseFuture = responseConcurrencyReducer.add(() -> doExecuteRequest(request, handlerSupplier));
        } else {
            responseFuture = doExecuteRequest(request, handlerSupplier);
        }
        CompletableFuture<Response> futureWithRedirect = responseFuture.thenCompose(response -> {
            if (followRedirects && isRedirectStatusCode(response.getStatusCode())) {
                return executeRedirect(request, response, handlerSupplier, redirectCount);
            }
            return CompletableFuture.completedFuture(response);
        });
        futureWithRedirect.whenComplete((response, throwable) -> {
            // propagate cancellation or timeout to the original response future
            responseFuture.cancel(false);
        });
        return futureWithRedirect;
    }

    private CompletableFuture<Response> executeRedirect(Request request, Response response,
                                                        Supplier<AsyncHandler<Response>> handlerSupplier,
                                                        int redirectCount) {
        String originalMethod = request.getMethod();
        int statusCode = response.getStatusCode();
        boolean switchToGet = !originalMethod.equals(GET)
                && !originalMethod.equals(OPTIONS) && !originalMethod.equals(HEAD) && (
                statusCode == MOVED_PERMANENTLY_301 || statusCode == SEE_OTHER_303 || statusCode == FOUND_302);
        boolean keepBody = statusCode == TEMPORARY_REDIRECT_307 || statusCode == PERMANENT_REDIRECT_308;
        String location = response.getHeader(HttpHeaders.LOCATION);
        Uri newUri = Uri.create(request.getUri(), location);
        BoundRequestBuilder builder = httpClient.prepareRequest(request);
        if (switchToGet) {
            builder.setMethod(GET);
        }
        if (this.nameResolver != null) {
            builder.setNameResolver(this.nameResolver);
        }
        builder.setUri(newUri);
        if (keepBody) {
            builder.setCharset(request.getCharset());
            if (isNonEmpty(request.getFormParams())) {
                builder.setFormParams(request.getFormParams());
            } else if (request.getStringData() != null) {
                builder.setBody(request.getStringData());
            } else if (request.getByteData() != null) {
                builder.setBody(request.getByteData());
            } else if (request.getByteBufferData() != null) {
                builder.setBody(request.getByteBufferData());
            } else if (request.getBodyGenerator() != null) {
                builder.setBody(request.getBodyGenerator());
            } else if (isNonEmpty(request.getBodyParts())) {
                builder.setBodyParts(request.getBodyParts());
            }
        } else {
            builder.resetFormParams();
            builder.resetNonMultipartData();
            builder.resetMultipartData();
            io.netty.handler.codec.http.HttpHeaders headers = new DefaultHttpHeaders();
            headers.add(request.getHeaders());
            headers.remove(HttpHeaders.CONTENT_LENGTH);
            headers.remove(HttpHeaders.CONTENT_TYPE);
            headers.remove(HttpHeaders.CONTENT_ENCODING);
            builder.setHeaders(headers);
        }
        return executeRequest(builder.build(), handlerSupplier, redirectCount + 1);
    }

    private static boolean isRedirectStatusCode(int statusCode) {
        return statusCode == MOVED_PERMANENTLY_301 || statusCode == FOUND_302 || statusCode == SEE_OTHER_303
                || statusCode == TEMPORARY_REDIRECT_307 || statusCode == PERMANENT_REDIRECT_308;
    }

    private CompletableFuture<Response> doExecuteRequest(Request request,
                                                         Supplier<AsyncHandler<Response>> handlerSupplier) {
        ListenableFuture<Response> responseFuture =
                httpClient.executeRequest(request, handlerSupplier.get());
        CompletableFuture<Response> completableFuture = responseFuture.toCompletableFuture();
        completableFuture.whenComplete((response, throwable) -> {
            throwable = FutureUtil.unwrapCompletionException(throwable);
            if (throwable != null && (throwable instanceof CancellationException
                    || throwable instanceof TimeoutException)) {
                // abort the request if the future is cancelled or timed out
                responseFuture.abort(throwable);
            }
        });
        return completableFuture;
    }

    private Request prepareRequest(InetSocketAddress host, ClientRequest request) throws IOException {
        ClientRequest currentRequest = new ClientRequest(request);
        URI newUri = replaceWithNew(host, currentRequest.getUri());
        currentRequest.setUri(newUri);

        BoundRequestBuilder builder =
                httpClient.prepare(currentRequest.getMethod(), currentRequest.getUri().toString());

        if (this.nameResolver != null) {
            builder.setNameResolver(this.nameResolver);
        }
        if (currentRequest.hasEntity()) {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            currentRequest.setStreamProvider(contentLength -> outStream);
            currentRequest.writeEntity();
            builder.setBody(outStream.toByteArray());
        }

        currentRequest.getHeaders().forEach((key, headers) -> {
            if (!HttpHeaders.USER_AGENT.equals(key)) {
                builder.addHeader(key, headers);
            }
        });

        if (acceptGzipCompression) {
            builder.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");
        }

        return builder.build();
    }

    @Override
    public String getName() {
        return "Pulsar-Admin";
    }

    /**
     * Configure SOCKS5 proxy for the underlying Netty-based async-http-client if
     * {@link ClientConfigurationData#getSocks5ProxyAddress()} is set. The configuration keys
     * (socks5ProxyAddress / socks5ProxyUsername / socks5ProxyPassword) are shared with the
     * pulsar-client module so that admin and client behave consistently.
     *
     * <p>async-http-client's {@link ProxyServer} with {@link ProxyType#SOCKS_V5} is backed by
     * Netty's {@code Socks5ProxyHandler}, which is injected into the channel pipeline when
     * establishing a new connection.
     */
    @VisibleForTesting
    static void configureSocks5ProxyIfNeeded(DefaultAsyncHttpClientConfig.Builder confBuilder,
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
        log.info().attr("proxy", socks5Address).log("Pulsar admin client is using SOCKS5 proxy");
    }

    @Override
    public void close() {
        try {
            httpClient.close();
            delayer.shutdownNow();
            // PIP-478 (new TLS path): release the CLIENT_DEFAULT subscription, close the factory and
            // stop its rotation executor (all no-ops when the admin URL is not https).
            if (tlsFactorySubscription != null) {
                tlsFactorySubscription.dispose();
            }
            if (tlsFactory != null) {
                tlsFactory.close();
            }
            if (tlsFactoryExecutor != null) {
                tlsFactoryExecutor.shutdownNow();
            }
            if (createdEventLoopGroup && eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
                eventLoopGroup.shutdownGracefully();
            }
        } catch (IOException e) {
            log.warn().exception(e).log("Failed to close http client");
        }
    }

    @Data
    private static class SharedResourceHolder {
        static final SharedResourceHolder EMPTY = new SharedResourceHolder(null, null, false);

        final NameResolver<InetAddress> nameResolver;
        final EventLoopGroup eventLoopGroup;
        final boolean createEventLoop;

        SharedResourceHolder(NameResolver<InetAddress> nameResolver,
                             EventLoopGroup eventLoopGroup,
                             boolean createEventLoop) {
            this.nameResolver = nameResolver;
            this.eventLoopGroup = eventLoopGroup;
            this.createEventLoop = createEventLoop;
        }
    }

}
