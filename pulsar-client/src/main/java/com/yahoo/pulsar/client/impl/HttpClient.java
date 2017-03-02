/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;
import org.asynchttpclient.*;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.AuthenticationDataProvider;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.common.util.SecurityUtility;

public class HttpClient implements Closeable {

    protected final static int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    protected final static int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    protected final AsyncHttpClient httpClient;
    protected final URL url;
    protected final Authentication authentication;

    protected HttpClient(String serviceUrl, Authentication authentication, EventLoopGroup eventLoopGroup,
            boolean tlsAllowInsecureConnection, String tlsTrustCertsFilePath) throws PulsarClientException {
        this(serviceUrl, authentication, eventLoopGroup, tlsAllowInsecureConnection, tlsTrustCertsFilePath,
                DEFAULT_CONNECT_TIMEOUT_IN_SECONDS, DEFAULT_READ_TIMEOUT_IN_SECONDS);
    }

    protected HttpClient(String serviceUrl, Authentication authentication, EventLoopGroup eventLoopGroup,
            boolean tlsAllowInsecureConnection, String tlsTrustCertsFilePath, int connectTimeoutInSeconds,
            int readTimeoutInSeconds) throws PulsarClientException {
        this.authentication = authentication;
        try {
            // Ensure trailing "/" on url
            url = new URL(serviceUrl);
        } catch (MalformedURLException e) {
            throw new PulsarClientException.InvalidServiceURL(e);
        }

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(connectTimeoutInSeconds * 1000);
        confBuilder.setReadTimeout(readTimeoutInSeconds * 1000);
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", getPulsarClientVersion()));
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(Request ahcRequest, HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.getStatus().code() / 100 != 5) && super.keepAlive(ahcRequest, request, response);
            }
        });

        if ("https".equals(url.getProtocol())) {
            try {
                SslContext sslCtx = null;

                // Set client key and certificate if available
                AuthenticationDataProvider authData = authentication.getAuthData();
                if (authData.hasDataForTls()) {
                    sslCtx = SecurityUtility.createNettySslContext(tlsAllowInsecureConnection, tlsTrustCertsFilePath,
                            authData.getTlsCertificates(), authData.getTlsPrivateKey());
                } else {
                    sslCtx = SecurityUtility.createNettySslContext(tlsAllowInsecureConnection, tlsTrustCertsFilePath);
                }

                confBuilder.setSslContext(sslCtx);
                confBuilder.setAcceptAnyCertificate(tlsAllowInsecureConnection);
            } catch (Exception e) {
                throw new PulsarClientException.InvalidConfigurationException(e);
            }
        }
        confBuilder.setEventLoopGroup(eventLoopGroup);
        AsyncHttpClientConfig config = confBuilder.build();
        httpClient = new DefaultAsyncHttpClient(config);

        log.debug("Using HTTP url: {}", this.url);
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    public <T> CompletableFuture<T> get(String path, Class<T> clazz) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            String requestUrl = new URL(url, path).toString();
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
                        future.completeExceptionally(
                                new PulsarClientException("HTTP get request failed: " + response.getStatusText()));
                        return;
                    }

                    T data = ObjectMapperFactory.getThreadLocal().readValue(response.getResponseBodyAsBytes(), clazz);
                    future.complete(data);
                } catch (Exception e) {
                    log.warn("[{}] Error during HTTP get request: {}", requestUrl, e.getMessage());
                    future.completeExceptionally(new PulsarClientException(e));
                }
            }, MoreExecutors.sameThreadExecutor());

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

    /**
     * Looks for a file called pulsar-client-version.properties and returns the client version
     *
     * @return client version or unknown version depending on whether the file is found or not.
     */
    private static String getPulsarClientVersion() {
        String path = "/pulsar-client-version.properties";
        String unknownClientIdentifier = "UnknownClient";

        try {
            InputStream stream = HttpClient.class.getResourceAsStream(path);
            if (stream == null) {
                return unknownClientIdentifier;
            }
            Properties props = new Properties();
            try {
                props.load(stream);
                String version = (String) props.get("pulsar-client-version");
                return version;
            } catch (IOException e) {
                return unknownClientIdentifier;
            } finally {
                stream.close();
            }
        } catch (Throwable t) {
            return unknownClientIdentifier;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);
}
