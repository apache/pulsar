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
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
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
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.api.Socks5ProxyScope;
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
    protected ScheduledExecutorService executorService;
    protected PulsarSslFactory sslFactory;

    protected HttpClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, Timer timer,
                         NameResolver<InetAddress> nameResolver)
            throws PulsarClientException {
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
        confBuilder.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_IN_SECONDS * 1000);
        confBuilder.setReadTimeout(DEFAULT_READ_TIMEOUT_IN_SECONDS * 1000);
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
        confBuilder.setNettyTimer(timer);
        configureSocks5ProxyIfNeeded(confBuilder, conf);
        AsyncHttpClientConfig config = confBuilder.build();
        httpClient = new DefaultAsyncHttpClient(config);

        log.debug().attr("url", conf.getServiceUrl()).log("Using HTTP url");
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
            String remoteHostName = currentUri.getHost();
            AuthenticationDataProvider authData = authentication.getAuthData(remoteHostName);

            CompletableFuture<Map<String, String>> authFuture = new CompletableFuture<>();

            // bring a authenticationStage for sasl auth.
            if (authData.hasDataForHttp()) {
                authentication.authenticationStage(requestUrl, authData, null, authFuture);
            } else {
                authFuture.complete(null);
            }

            authFuture.whenComplete((respHeaders, ex) -> {
                if (ex != null) {
                    serviceNameResolver.markHostAvailability(originalHost, false);
                    log.warn().attr("requestUrl", requestUrl)
                            .exceptionMessage(ex)
                            .log("Failed to perform http request at authentication stage");
                    future.completeExceptionally(new PulsarClientException(ex));
                    return;
                }

                BoundRequestBuilder builder = httpClient.prepareGet(requestUrl)
                        // share the DNS resolver and cache with Pulsar client
                        .setNameResolver(nameResolver)
                        .setHeader("Accept", "application/json");

                if (authData.hasDataForHttp()) {
                    Set<Entry<String, String>> headers;
                    try {
                        headers = authentication.newRequestHeader(requestUrl, authData, respHeaders);
                    } catch (Exception e) {
                        log.warn().attr("requestUrl", requestUrl)
                                .exceptionMessage(e)
                                .log("Error during HTTP get headers");
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
            log.error().exception(e).log("Failed to refresh SSL context");
        }
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
