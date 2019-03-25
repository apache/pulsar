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
package org.apache.pulsar.client.impl;

import com.google.common.util.concurrent.MoreExecutors;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslContext;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClient implements Closeable {

    protected final static int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    protected final static int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    protected final AsyncHttpClient httpClient;
    protected final ServiceNameResolver serviceNameResolver;
    protected final Authentication authentication;

    protected HttpClient(String serviceUrl, Authentication authentication,
            EventLoopGroup eventLoopGroup, boolean tlsAllowInsecureConnection, String tlsTrustCertsFilePath)
            throws PulsarClientException {
        this(serviceUrl, authentication, eventLoopGroup, tlsAllowInsecureConnection,
                tlsTrustCertsFilePath, DEFAULT_CONNECT_TIMEOUT_IN_SECONDS, DEFAULT_READ_TIMEOUT_IN_SECONDS);
    }

    protected HttpClient(String serviceUrl, Authentication authentication,
            EventLoopGroup eventLoopGroup, boolean tlsAllowInsecureConnection, String tlsTrustCertsFilePath,
            int connectTimeoutInSeconds, int readTimeoutInSeconds) throws PulsarClientException {
        this.authentication = authentication;
        this.serviceNameResolver = new PulsarServiceNameResolver();
        this.serviceNameResolver.updateServiceUrl(serviceUrl);

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(connectTimeoutInSeconds * 1000);
        confBuilder.setReadTimeout(readTimeoutInSeconds * 1000);
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(Request ahcRequest, HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5) && super.keepAlive(ahcRequest, request, response);
            }
        });

        if ("https".equals(serviceNameResolver.getServiceUri().getServiceName())) {
            try {
                SslContext sslCtx = null;

                // Set client key and certificate if available
                AuthenticationDataProvider authData = authentication.getAuthData();
                if (authData.hasDataForTls()) {
                    sslCtx = SecurityUtility.createNettySslContextForClient(tlsAllowInsecureConnection, tlsTrustCertsFilePath,
                            authData.getTlsCertificates(), authData.getTlsPrivateKey());
                } else {
                    sslCtx = SecurityUtility.createNettySslContextForClient(tlsAllowInsecureConnection, tlsTrustCertsFilePath);
                }

                confBuilder.setSslContext(sslCtx);
                confBuilder.setUseInsecureTrustManager(tlsAllowInsecureConnection);
            } catch (Exception e) {
                throw new PulsarClientException.InvalidConfigurationException(e);
            }
        }
        confBuilder.setEventLoopGroup(eventLoopGroup);
        AsyncHttpClientConfig config = confBuilder.build();
        httpClient = new DefaultAsyncHttpClient(config);

        log.debug("Using HTTP url: {}", serviceUrl);
    }

    String getServiceUrl() {
        return this.serviceNameResolver.getServiceUrl();
    }

    void setServiceUrl(String serviceUrl) throws PulsarClientException {
        this.serviceNameResolver.updateServiceUrl(serviceUrl);
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    public <T> CompletableFuture<T> get(String path, Class<T> clazz) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            String requestUrl = new URL(serviceNameResolver.resolveHostUri().toURL(), path).toString();
            AuthenticationDataProvider authData = authentication.getAuthData();
            BoundRequestBuilder builder = httpClient.prepareGet(requestUrl);

            // Add headers for authentication if any
            if (authData.hasDataForHttp()) {
                for (Map.Entry<String, String> header : authData.getHttpHeaders()) {
                    builder.setHeader(header.getKey(), header.getValue());
                }
            }

            final ListenableFuture<Response> responseFuture = builder.setHeader("Accept", "application/json")
                    .execute(new AsyncCompletionHandler<Response>() {

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            return response;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                            future.completeExceptionally(new PulsarClientException(t));
                        }
                    });

            responseFuture.addListener(() -> {
                try {
                    Response response = responseFuture.get();
                    if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        log.warn("[{}] HTTP get request failed: {}", requestUrl, response.getStatusText());
                        Exception e;
                        if (response.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            e = new NotFoundException("Not found: " + response.getStatusText());
                        } else {
                            e = new PulsarClientException("HTTP get request failed: " + response.getStatusText());
                        }
                        future.completeExceptionally(e);
                        return;
                    }

                    T data = ObjectMapperFactory.getThreadLocal().readValue(response.getResponseBodyAsBytes(), clazz);
                    future.complete(data);
                } catch (Exception e) {
                    log.warn("[{}] Error during HTTP get request: {}", requestUrl, e.getMessage());
                    future.completeExceptionally(new PulsarClientException(e));
                }
            }, MoreExecutors.directExecutor());

        } catch (Exception e) {
            log.warn("[{}] Failed to get authentication data for lookup: {}", path, e.getMessage());
            if (e instanceof PulsarClientException) {
                future.completeExceptionally(e);
            } else {
                future.completeExceptionally(new PulsarClientException(e));
            }
        }

        return future;

    }

    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);
}
