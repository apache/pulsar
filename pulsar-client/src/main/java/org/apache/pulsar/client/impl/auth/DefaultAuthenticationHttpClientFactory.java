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
package org.apache.pulsar.client.impl.auth;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.AuthenticationHttpClient;
import org.apache.pulsar.client.api.AuthenticationHttpClientConfig;
import org.apache.pulsar.client.api.AuthenticationHttpClientFactory;
import org.apache.pulsar.client.api.AuthenticationHttpRequest;
import org.apache.pulsar.client.api.AuthenticationHttpResponse;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.PulsarHttpAsyncSslEngineFactory;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.asynchttpclient.SslEngineFactory;

@CustomLog
public class DefaultAuthenticationHttpClientFactory implements AuthenticationHttpClientFactory {
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_AUTO_CERT_REFRESH_DURATION = Duration.ofSeconds(300);

    private final Supplier<EventLoopGroup> eventLoopGroupSupplier;
    private final Timer timer;
    private final Supplier<NameResolver<InetAddress>> nameResolverSupplier;

    public DefaultAuthenticationHttpClientFactory(EventLoopGroup eventLoopGroup,
                                                  Timer timer,
                                                  NameResolver<InetAddress> nameResolver) {
        this(() -> eventLoopGroup, timer, () -> nameResolver);
    }

    public static DefaultAuthenticationHttpClientFactory withLazyResources(
            Supplier<EventLoopGroup> eventLoopGroupSupplier,
            Timer timer,
            Supplier<NameResolver<InetAddress>> nameResolverSupplier) {
        return new DefaultAuthenticationHttpClientFactory(eventLoopGroupSupplier, timer, nameResolverSupplier);
    }

    private DefaultAuthenticationHttpClientFactory(Supplier<EventLoopGroup> eventLoopGroupSupplier,
                                                   Timer timer,
                                                   Supplier<NameResolver<InetAddress>> nameResolverSupplier) {
        this.eventLoopGroupSupplier = eventLoopGroupSupplier;
        this.timer = timer;
        this.nameResolverSupplier = nameResolverSupplier;
    }

    @Override
    public AuthenticationHttpClient create(AuthenticationHttpClientConfig config) throws PulsarClientException {
        AuthenticationHttpClientConfig resolvedConfig = config == null
                ? AuthenticationHttpClientConfig.builder().build()
                : config;
        try {
            ClientResources resources = createAsyncHttpClient(resolvedConfig);
            return new DefaultAuthenticationHttpClient(resources.httpClient, resources.nameResolver,
                    resources.sslFactory, resources.sslRefreshScheduler);
        } catch (Exception e) {
            throw new PulsarClientException.InvalidConfigurationException(e);
        }
    }

    private ClientResources createAsyncHttpClient(AuthenticationHttpClientConfig config) throws Exception {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(resolveDuration(config.getConnectTimeout(), DEFAULT_CONNECT_TIMEOUT));
        confBuilder.setReadTimeout(resolveDuration(config.getReadTimeout(), DEFAULT_READ_TIMEOUT));
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        if (timer != null) {
            confBuilder.setNettyTimer(timer);
        }

        PulsarSslFactory sslFactory = null;
        ScheduledExecutorService sslRefreshScheduler = null;
        try {
            sslFactory = configureSsl(config, confBuilder);
            sslRefreshScheduler = scheduleSslContextRefreshIfEnabled(sslFactory,
                    resolveDuration(config.getAutoCertRefreshDuration(), DEFAULT_AUTO_CERT_REFRESH_DURATION));
            EventLoopGroup eventLoopGroup = eventLoopGroupSupplier == null ? null : eventLoopGroupSupplier.get();
            if (eventLoopGroup != null) {
                confBuilder.setEventLoopGroup(eventLoopGroup);
            }
            NameResolver<InetAddress> nameResolver = nameResolverSupplier == null ? null : nameResolverSupplier.get();
            AsyncHttpClient httpClient = new DefaultAsyncHttpClient(confBuilder.build());
            return new ClientResources(httpClient, nameResolver, sslFactory, sslRefreshScheduler);
        } catch (Exception e) {
            closeSslResources(sslRefreshScheduler, sslFactory, e);
            throw e;
        }
    }

    private Duration resolveDuration(Duration configuredValue, Duration defaultValue) {
        return configuredValue == null ? defaultValue : configuredValue;
    }

    private PulsarSslFactory configureSsl(AuthenticationHttpClientConfig config,
                                         DefaultAsyncHttpClientConfig.Builder confBuilder) throws Exception {
        String certFile = config.getTlsCertificateFilePath();
        String keyFile = config.getTlsKeyFilePath();
        boolean hasCertFile = StringUtils.isNotBlank(certFile);
        boolean hasKeyFile = StringUtils.isNotBlank(keyFile);
        if (hasCertFile != hasKeyFile) {
            throw new IllegalArgumentException("Invalid TLS client certificate configuration: tlsCertificateFilePath "
                    + "and tlsKeyFilePath must be provided together");
        }
        if (hasCertFile) {
            PulsarSslConfiguration sslConfiguration = PulsarSslConfiguration.builder()
                    .tlsCertificateFilePath(certFile)
                    .tlsKeyFilePath(keyFile)
                    .tlsTrustCertsFilePath(config.getTrustCertsFilePath())
                    .allowInsecureConnection(false)
                    .serverMode(false)
                    .isHttps(true)
                    .build();
            PulsarSslFactory sslFactory = new DefaultPulsarSslFactory();
            sslFactory.initialize(sslConfiguration);
            sslFactory.createInternalSslContext();
            SslEngineFactory sslEngineFactory = new PulsarHttpAsyncSslEngineFactory(sslFactory, null, true);
            confBuilder.setSslEngineFactory(sslEngineFactory);
            return sslFactory;
        } else if (StringUtils.isNotBlank(config.getTrustCertsFilePath())) {
            try {
                confBuilder.setSslContext(SslContextBuilder.forClient()
                        .trustManager(new File(config.getTrustCertsFilePath()))
                        .build());
            } catch (SSLException | RuntimeException e) {
                log.error().exception(e).log("Could not set trustCertsFilePath");
            }
        }
        return null;
    }

    private ScheduledExecutorService scheduleSslContextRefreshIfEnabled(PulsarSslFactory sslFactory,
                                                                        Duration refreshInterval) {
        long refreshSeconds = refreshInterval == null ? 0 : refreshInterval.getSeconds();
        if (sslFactory == null || refreshSeconds <= 0) {
            return null;
        }
        ScheduledExecutorService sslRefreshScheduler = Executors.newSingleThreadScheduledExecutor(
                new ExecutorProvider.ExtendedThreadFactory("oauth2-tls-cert-refresher", true));
        sslRefreshScheduler.scheduleWithFixedDelay(() -> refreshSslContext(sslFactory),
                refreshSeconds, refreshSeconds, TimeUnit.SECONDS);
        log.debug().attr("refreshSeconds", refreshSeconds).log("Scheduled TLS certificate refresh");
        return sslRefreshScheduler;
    }

    private void closeSslResources(ScheduledExecutorService sslRefreshScheduler, PulsarSslFactory sslFactory,
                                   Exception originalException) {
        if (sslRefreshScheduler != null) {
            try {
                sslRefreshScheduler.shutdownNow();
            } catch (Exception e) {
                originalException.addSuppressed(e);
            }
        }
        if (sslFactory != null) {
            try {
                sslFactory.close();
            } catch (Exception e) {
                originalException.addSuppressed(e);
            }
        }
    }

    private void refreshSslContext(PulsarSslFactory sslFactory) {
        try {
            sslFactory.update();
            log.debug("Successfully refreshed SSL context");
        } catch (Exception e) {
            log.error().exception(e).log("Failed to refresh SSL context");
        }
    }

    private static final class DefaultAuthenticationHttpClient implements AuthenticationHttpClient {
        private final AsyncHttpClient httpClient;
        private final NameResolver<InetAddress> nameResolver;
        private final PulsarSslFactory sslFactory;
        private final ScheduledExecutorService sslRefreshScheduler;

        private DefaultAuthenticationHttpClient(AsyncHttpClient httpClient,
                                                NameResolver<InetAddress> nameResolver,
                                                PulsarSslFactory sslFactory,
                                                ScheduledExecutorService sslRefreshScheduler) {
            this.httpClient = httpClient;
            this.nameResolver = nameResolver;
            this.sslFactory = sslFactory;
            this.sslRefreshScheduler = sslRefreshScheduler;
        }

        @Override
        public CompletableFuture<AuthenticationHttpResponse> execute(AuthenticationHttpRequest request) {
            if (request == null) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("request cannot be null"));
            }
            try {
                BoundRequestBuilder requestBuilder = newRequestBuilder(request);
                request.getHeaders().forEach(requestBuilder::setHeader);
                if (request.getBody() != null) {
                    requestBuilder.setBody(request.getBody());
                }
                if (nameResolver != null) {
                    requestBuilder.setNameResolver(nameResolver);
                }
                return requestBuilder.execute().toCompletableFuture().thenApply(this::toResponse);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        private BoundRequestBuilder newRequestBuilder(AuthenticationHttpRequest request) {
            switch (request.getMethod()) {
                case GET:
                    return httpClient.prepareGet(request.getUrl());
                case POST:
                    return httpClient.preparePost(request.getUrl());
                default:
                    throw new IllegalArgumentException("Unsupported HTTP method: " + request.getMethod());
            }
        }

        private AuthenticationHttpResponse toResponse(Response response) {
            return new AuthenticationHttpResponse(response.getStatusCode(), response.getStatusText(),
                    response.getResponseBodyAsBytes());
        }

        @Override
        public void close() throws IOException {
            try {
                if (sslRefreshScheduler != null) {
                    sslRefreshScheduler.shutdownNow();
                }
                if (sslFactory != null) {
                    sslFactory.close();
                }
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                httpClient.close();
            }
        }
    }

    private record ClientResources(AsyncHttpClient httpClient, NameResolver<InetAddress> nameResolver,
                                   PulsarSslFactory sslFactory,
                                   ScheduledExecutorService sslRefreshScheduler) {
    }
}
