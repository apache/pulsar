/**
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

import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.WithSNISslEngineFactory;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.netty.ssl.JsseSslEngineFactory;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.client.spi.AsyncConnectorCallback;
import org.glassfish.jersey.client.spi.Connector;

/**
 * Customized Jersey client connector with multi-host support.
 */
@Slf4j
public class AsyncHttpConnector implements Connector {
    private static final TimeoutException READ_TIMEOUT_EXCEPTION =
            FutureUtil.createTimeoutException("Read timeout", AsyncHttpConnector.class, "retryOrTimeout(...)");
    @Getter
    private final AsyncHttpClient httpClient;
    private final Duration readTimeout;
    private final int maxRetries;
    private final PulsarServiceNameResolver serviceNameResolver;
    private final ScheduledExecutorService delayer = Executors.newScheduledThreadPool(1,
            new DefaultThreadFactory("delayer"));

    public AsyncHttpConnector(Client client, ClientConfigurationData conf, int autoCertRefreshTimeSeconds) {
        this((int) client.getConfiguration().getProperty(ClientProperties.CONNECT_TIMEOUT),
                (int) client.getConfiguration().getProperty(ClientProperties.READ_TIMEOUT),
                PulsarAdminImpl.DEFAULT_REQUEST_TIMEOUT_SECONDS * 1000,
                autoCertRefreshTimeSeconds,
                conf);
    }

    @SneakyThrows
    public AsyncHttpConnector(int connectTimeoutMs, int readTimeoutMs,
                              int requestTimeoutMs,
                              int autoCertRefreshTimeSeconds, ClientConfigurationData conf) {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(true);
        confBuilder.setRequestTimeout(conf.getRequestTimeoutMs());
        confBuilder.setConnectTimeout(connectTimeoutMs);
        confBuilder.setReadTimeout(readTimeoutMs);
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        confBuilder.setRequestTimeout(requestTimeoutMs);
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

        serviceNameResolver = new PulsarServiceNameResolver();
        if (conf != null && StringUtils.isNotBlank(conf.getServiceUrl())) {
            serviceNameResolver.updateServiceUrl(conf.getServiceUrl());
            if (conf.getServiceUrl().startsWith("https://")) {
                // Set client key and certificate if available
                AuthenticationDataProvider authData = conf.getAuthentication().getAuthData();

                if (conf.isUseKeyStoreTls()) {
                    KeyStoreParams params = authData.hasDataForTls() ? authData.getTlsKeyStoreParams() : null;

                    final SSLContext sslCtx = KeyStoreSSLContext.createClientSslContext(
                            conf.getSslProvider(),
                            params != null ? params.getKeyStoreType() : null,
                            params != null ? params.getKeyStorePath() : null,
                            params != null ? params.getKeyStorePassword() : null,
                            conf.isTlsAllowInsecureConnection(),
                            conf.getTlsTrustStoreType(),
                            conf.getTlsTrustStorePath(),
                            conf.getTlsTrustStorePassword(),
                            conf.getTlsCiphers(),
                            conf.getTlsProtocols());

                    JsseSslEngineFactory sslEngineFactory = new JsseSslEngineFactory(sslCtx);
                    confBuilder.setSslEngineFactory(sslEngineFactory);
                } else {
                    SslProvider sslProvider = null;
                    if (conf.getSslProvider() != null) {
                        sslProvider = SslProvider.valueOf(conf.getSslProvider());
                    }
                    SslContext sslCtx = null;
                    if (authData.hasDataForTls()) {
                        sslCtx = authData.getTlsTrustStoreStream() == null
                                ? SecurityUtility.createAutoRefreshSslContextForClient(
                                sslProvider,
                                conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath(), authData.getTlsCerificateFilePath(),
                                authData.getTlsPrivateKeyFilePath(), null, autoCertRefreshTimeSeconds, delayer)
                                : SecurityUtility.createNettySslContextForClient(
                                sslProvider,
                                conf.isTlsAllowInsecureConnection(),
                                authData.getTlsTrustStoreStream(), authData.getTlsCertificates(),
                                authData.getTlsPrivateKey(),
                                conf.getTlsCiphers(),
                                conf.getTlsProtocols());
                    } else {
                        sslCtx = SecurityUtility.createNettySslContextForClient(
                                sslProvider,
                                conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath(),
                                conf.getTlsCiphers(),
                                conf.getTlsProtocols());
                    }
                    confBuilder.setSslContext(sslCtx);
                    if (!conf.isTlsHostnameVerificationEnable()) {
                        confBuilder.setSslEngineFactory(new WithSNISslEngineFactory(serviceNameResolver
                                .resolveHostUri().getHost()));
                    }
                }
            }
            confBuilder.setDisableHttpsEndpointIdentificationAlgorithm(!conf.isTlsHostnameVerificationEnable());
        }
        httpClient = new DefaultAsyncHttpClient(confBuilder.build());
        this.readTimeout = Duration.ofMillis(readTimeoutMs);
        this.maxRetries = httpClient.getConfig().getMaxRequestRetry();
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
            log.error(e.getMessage());
        }
        return null;
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
                jerseyResponse.setStatusInfo(new javax.ws.rs.core.Response.StatusType() {
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
                        return response.getStatusText();
                    }
                });
                response.getHeaders().forEach(e -> jerseyResponse.header(e.getKey(), e.getValue()));
                if (response.hasResponseBody()) {
                    jerseyResponse.setEntityStream(response.getResponseBodyAsStream());
                }
                callback.response(jerseyResponse);
            }
        }));
        return responseFuture;
    }

    private CompletableFuture<Response> retryOrTimeOut(ClientRequest request) {
        final CompletableFuture<Response> resultFuture = new CompletableFuture<>();
        retryOperation(resultFuture, () -> oneShot(serviceNameResolver.resolveHost(), request), maxRetries);
        CompletableFuture<Response> timeoutAfter = FutureUtil.createFutureWithTimeout(readTimeout, delayer,
                () -> READ_TIMEOUT_EXCEPTION);
        return resultFuture.applyToEither(timeoutAfter, Function.identity());
    }

    private <T> void retryOperation(
            final CompletableFuture<T> resultFuture,
            final Supplier<CompletableFuture<T>> operation,
            final int retries) {

        if (!resultFuture.isDone()) {
            final CompletableFuture<T> operationFuture = operation.get();

            operationFuture.whenComplete(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable instanceof CancellationException) {
                                resultFuture.completeExceptionally(
                                        new RetryException("Operation future was cancelled.", throwable));
                            } else {
                                if (retries > 0) {
                                    retryOperation(
                                            resultFuture,
                                            operation,
                                            retries - 1);
                                } else {
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

    private CompletableFuture<Response> oneShot(InetSocketAddress host, ClientRequest request) {
        ClientRequest currentRequest = new ClientRequest(request);
        URI newUri = replaceWithNew(host, currentRequest.getUri());
        currentRequest.setUri(newUri);

        BoundRequestBuilder builder =
                httpClient.prepare(currentRequest.getMethod(), currentRequest.getUri().toString());

        if (currentRequest.hasEntity()) {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            currentRequest.setStreamProvider(contentLength -> outStream);
            try {
                currentRequest.writeEntity();
            } catch (IOException e) {
                CompletableFuture<Response> r = new CompletableFuture<>();
                r.completeExceptionally(e);
                return r;
            }

            builder.setBody(outStream.toByteArray());
        }

        currentRequest.getHeaders().forEach((key, headers) -> {
            if (!HttpHeaders.USER_AGENT.equals(key)) {
                builder.addHeader(key, headers);
            }
        });

        return builder.execute().toCompletableFuture();
    }

    @Override
    public String getName() {
        return "Pulsar-Admin";
    }

    @Override
    public void close() {
        try {
            httpClient.close();
            delayer.shutdownNow();
        } catch (IOException e) {
            log.warn("Failed to close http client", e);
        }
    }

}
