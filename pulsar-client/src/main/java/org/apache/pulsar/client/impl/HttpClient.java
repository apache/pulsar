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

import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.PulsarHttpAsyncSslEngineFactory;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.SslEngineFactory;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;


@Slf4j
public class HttpClient implements Closeable {

    private static final String ORIGINAL_PRINCIPAL_HEADER = "X-Original-Principal";
    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    protected static final int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    protected final AsyncHttpClient httpClient;
    protected final ServiceNameResolver serviceNameResolver;
    protected final Authentication authentication;
    protected final ClientConfigurationData clientConf;
    protected ScheduledExecutorService executorService;
    protected PulsarSslFactory sslFactory;

    protected HttpClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this.authentication = conf.getAuthentication();
        this.serviceNameResolver = new PulsarServiceNameResolver();
        this.clientConf = conf;
        this.serviceNameResolver.updateServiceUrl(conf.getServiceUrl());

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(true);
        confBuilder.setMaxRedirects(conf.getMaxLookupRedirects());
        confBuilder.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_IN_SECONDS * 1000);
        confBuilder.setReadTimeout(DEFAULT_READ_TIMEOUT_IN_SECONDS * 1000);
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
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
                // Set client key and certificate if available
                this.executorService = Executors
                        .newSingleThreadScheduledExecutor(new ExecutorProvider
                                .ExtendedThreadFactory("httpclient-ssl-refresh"));
                PulsarSslConfiguration sslConfiguration =
                        buildSslConfiguration(conf, serviceNameResolver.resolveHostUri().getHost());
                this.sslFactory = (PulsarSslFactory) Class.forName(conf.getSslFactoryPlugin())
                        .getConstructor().newInstance();
                this.sslFactory.initialize(sslConfiguration);
                this.sslFactory.createInternalSslContext();
                if (conf.getAutoCertRefreshSeconds() > 0) {
                    this.executorService.scheduleWithFixedDelay(this::refreshSslContext,
                            conf.getAutoCertRefreshSeconds(),
                            conf.getAutoCertRefreshSeconds(), TimeUnit.SECONDS);
                }
                String hostname = conf.isTlsHostnameVerificationEnable() ? null : serviceNameResolver
                        .resolveHostUri().getHost();
                SslEngineFactory sslEngineFactory = new PulsarHttpAsyncSslEngineFactory(this.sslFactory, hostname);
                confBuilder.setSslEngineFactory(sslEngineFactory);



                confBuilder.setUseInsecureTrustManager(conf.isTlsAllowInsecureConnection());
                confBuilder.setDisableHttpsEndpointIdentificationAlgorithm(!conf.isTlsHostnameVerificationEnable());
            } catch (Exception e) {
                throw new PulsarClientException.InvalidConfigurationException(e);
            }
        }
        confBuilder.setEventLoopGroup(eventLoopGroup);
        AsyncHttpClientConfig config = confBuilder.build();
        httpClient = new DefaultAsyncHttpClient(config);

        log.debug("Using HTTP url: {}", conf.getServiceUrl());
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
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    public <T> CompletableFuture<T> get(String path, Class<T> clazz) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            URI hostUri = serviceNameResolver.resolveHostUri();
            String requestUrl = new URL(hostUri.toURL(), path).toString();
            String remoteHostName = hostUri.getHost();
            AuthenticationDataProvider authData = authentication.getAuthData(remoteHostName);

            CompletableFuture<Map<String, String>>  authFuture = new CompletableFuture<>();

            // bring a authenticationStage for sasl auth.
            if (authData.hasDataForHttp()) {
                authentication.authenticationStage(requestUrl, authData, null, authFuture);
            } else {
                authFuture.complete(null);
            }

            // auth complete, do real request
            authFuture.whenComplete((respHeaders, ex) -> {
                if (ex != null) {
                    log.warn("[{}] Failed to perform http request at authentication stage: {}",
                        requestUrl, ex.getMessage());
                    future.completeExceptionally(new PulsarClientException(ex));
                    return;
                }

                // auth complete, use a new builder
                BoundRequestBuilder builder = httpClient.prepareGet(requestUrl)
                    .setHeader("Accept", "application/json");

                if (authData.hasDataForHttp()) {
                    Set<Entry<String, String>> headers;
                    try {
                        headers = authentication.newRequestHeader(requestUrl, authData, respHeaders);
                    } catch (Exception e) {
                        log.warn("[{}] Error during HTTP get headers: {}", requestUrl, e.getMessage());
                        future.completeExceptionally(new PulsarClientException(e));
                        return;
                    }
                    if (headers != null) {
                        headers.forEach(entry -> builder.addHeader(entry.getKey(), entry.getValue()));
                    }
                }

                // Add X-Original-Principal header if originalPrincipal is configured (for proxy scenarios)
                if (clientConf.getOriginalPrincipal() != null) {
                    builder.addHeader(ORIGINAL_PRINCIPAL_HEADER, clientConf.getOriginalPrincipal());
                }

                builder.execute().toCompletableFuture().whenComplete((response2, t) -> {
                    if (t != null) {
                        log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                        future.completeExceptionally(new PulsarClientException(t));
                        return;
                    }

                    // request not success
                    if (response2.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        log.warn("[{}] HTTP get request failed: {}", requestUrl, response2.getStatusText());
                        Exception e;
                        if (response2.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            e = new NotFoundException("Not found: " + response2.getStatusText());
                        } else {
                            e = new PulsarClientException("HTTP get request failed: " + response2.getStatusText());
                        }
                        future.completeExceptionally(e);
                        return;
                    }

                    try {
                        T data = ObjectMapperFactory.getMapper().reader().readValue(
                                response2.getResponseBodyAsBytes(), clazz);
                        future.complete(data);
                    } catch (Exception e) {
                        log.warn("[{}] Error during HTTP get request: {}", requestUrl, e.getMessage());
                        future.completeExceptionally(new PulsarClientException(e));
                    }
                });
            });
        } catch (Exception e) {
            log.warn("[{}]PulsarClientImpl: {}", path, e.getMessage());
            if (e instanceof PulsarClientException) {
                future.completeExceptionally(e);
            } else {
                future.completeExceptionally(new PulsarClientException(e));
            }
        }

        return future;
    }

    protected PulsarSslConfiguration buildSslConfiguration(ClientConfigurationData config, String host)
            throws PulsarClientException {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getSslProvider())
                .tlsKeyStoreType(config.getTlsKeyStoreType())
                .tlsKeyStorePath(config.getTlsKeyStorePath())
                .tlsKeyStorePassword(config.getTlsKeyStorePassword())
                .tlsTrustStoreType(config.getTlsTrustStoreType())
                .tlsTrustStorePath(config.getTlsTrustStorePath())
                .tlsTrustStorePassword(config.getTlsTrustStorePassword())
                .tlsCiphers(config.getTlsCiphers())
                .tlsProtocols(config.getTlsProtocols())
                .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getTlsCertificateFilePath())
                .tlsKeyFilePath(config.getTlsKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(false)
                .tlsEnabledWithKeystore(config.isUseKeyStoreTls())
                .tlsCustomParams(config.getSslFactoryPluginParams())
                .authData(config.getAuthentication().getAuthData(host))
                .serverMode(false)
                .isHttps(true)
                .build();
    }

    protected void refreshSslContext() {
        try {
            this.sslFactory.update();
        } catch (Exception e) {
            log.error("Failed to refresh SSL context", e);
        }
    }
}
