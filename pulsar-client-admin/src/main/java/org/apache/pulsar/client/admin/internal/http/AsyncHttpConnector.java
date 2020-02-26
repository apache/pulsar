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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.SecurityUtility;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.client.spi.AsyncConnectorCallback;
import org.glassfish.jersey.client.spi.Connector;

@Slf4j
public class AsyncHttpConnector implements Connector, AsyncConnectorCallback {

    @Getter
    private final AsyncHttpClient httpClient;
    private final PulsarServiceNameResolver serviceNameResolver;

    private Set<InetSocketAddress> triedAddressees;

    public AsyncHttpConnector(Client client, ClientConfigurationData conf) {
        this((int) client.getConfiguration().getProperty(ClientProperties.CONNECT_TIMEOUT),
                (int) client.getConfiguration().getProperty(ClientProperties.READ_TIMEOUT),
                PulsarAdmin.DEFAULT_REQUEST_TIMEOUT_SECONDS * 1000,
                conf);
    }

    @SneakyThrows
    public AsyncHttpConnector(int connectTimeoutMs, int readTimeoutMs,
                              int requestTimeoutMs, ClientConfigurationData conf) {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setFollowRedirect(true);
        confBuilder.setRequestTimeout(conf.getRequestTimeoutMs());
        confBuilder.setConnectTimeout(connectTimeoutMs);
        confBuilder.setReadTimeout(readTimeoutMs);
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        confBuilder.setRequestTimeout(requestTimeoutMs);
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(Request ahcRequest, HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5) && super.keepAlive(ahcRequest, request, response);
            }
        });

        serviceNameResolver = new PulsarServiceNameResolver();
        if (conf != null && StringUtils.isNotBlank(conf.getServiceUrl())) {
            serviceNameResolver.updateServiceUrl(conf.getServiceUrl());
            if (conf.getServiceUrl().startsWith("https://")) {

                SslContext sslCtx = null;

                // Set client key and certificate if available
                AuthenticationDataProvider authData = conf.getAuthentication().getAuthData();
                if (authData.hasDataForTls()) {
                    sslCtx = SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection() || !conf.isTlsHostnameVerificationEnable(),
                                                                            conf.getTlsTrustCertsFilePath(), authData.getTlsCertificates(), authData.getTlsPrivateKey());
                } else {
                    sslCtx = SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection() || !conf.isTlsHostnameVerificationEnable(),
                                                                            conf.getTlsTrustCertsFilePath());
                }

                confBuilder.setSslContext(sslCtx);
            }
        }
        httpClient = new DefaultAsyncHttpClient(confBuilder.build());
    }

    public ClientResponse apply(ClientRequest jerseyRequest) {
        final CompletableFuture<ClientResponse> resp = new CompletableFuture<>();

        apply(jerseyRequest, new AsyncConnectorCallback() {
            @Override
            public void response(ClientResponse clientResponse) {
                resp.complete(clientResponse);
            }

            @Override
            public void failure(Throwable throwable) {
                resp.completeExceptionally(throwable);
            }
        });

        try {
            return resp.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new ProcessingException(e);
        }
    }

    private URI replaceWithNew(InetSocketAddress address, URI uri) {
        String originalUri = uri.toString();
        String newUri = (originalUri.split(":")[0] + "://")
                        + address.getHostName() + ":"
                        + address.getPort()
                        + uri.getRawPath();
        if (uri.getRawQuery() != null) {
            newUri += "?" + uri.getRawQuery();
        }
        return URI.create(newUri);
    }

    @Override
    public Future<?> apply(ClientRequest jerseyRequest, AsyncConnectorCallback callback) {
        final CompletableFuture<ClientResponse> respFuture = new CompletableFuture<>();
        triedAddressees = new HashSet<>();
        doNextRetry(jerseyRequest, callback, respFuture);
        return respFuture;
    }

    private void doNextRetry(ClientRequest jerseyRequest, AsyncConnectorCallback callback, CompletableFuture<ClientResponse> respFuture) {
        InetSocketAddress nextTriedAddress = serviceNameResolver.resolveHost();
        log.info("Trying to use address [{}] to send request.", nextTriedAddress.toString());
        if (triedAddressees.contains(nextTriedAddress)) {
            Exception triedAllException = new ProcessingException("All addresses are tried and failed");
            callback.failure(triedAllException);
            respFuture.completeExceptionally(triedAllException);
            return;
        }
        triedAddressees.add(nextTriedAddress);
        URI requestUri = replaceWithNew(nextTriedAddress, jerseyRequest.getUri());
        ClientRequest req = new ClientRequest(jerseyRequest);
        req.setUri(requestUri);
        doRequestAsync(req, this).whenComplete(((clientResponse, throwable) -> {
            if (throwable != null) {
                doNextRetry(jerseyRequest, callback, respFuture);
            } else {
                callback.response(clientResponse);
                respFuture.complete(clientResponse);
            }
        }));
    }

    private CompletableFuture<ClientResponse> doRequestAsync(ClientRequest jerseyRequest, AsyncConnectorCallback callback) {
        final CompletableFuture<ClientResponse> future = new CompletableFuture<>();

        BoundRequestBuilder builder = httpClient.prepare(jerseyRequest.getMethod(), jerseyRequest.getUri().toString());

        if (jerseyRequest.hasEntity()) {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            jerseyRequest.setStreamProvider(contentLength -> outStream);
            try {
                jerseyRequest.writeEntity();
            } catch (IOException e) {
                future.completeExceptionally(e);
                return future;
            }

            builder.setBody(outStream.toByteArray());
        }

        jerseyRequest.getHeaders().forEach((key, headers) -> {
            if (!HttpHeaders.USER_AGENT.equals(key)) {
                builder.addHeader(key, headers);
            }
        });

        builder.execute(new AsyncCompletionHandler<Response>() {
            @Override
            public Response onCompleted(Response response) throws Exception {
                ClientResponse jerseyResponse = new ClientResponse(Status.fromStatusCode(response.getStatusCode()),
                    jerseyRequest);
                response.getHeaders().forEach(e -> jerseyResponse.header(e.getKey(), e.getValue()));
                if (response.hasResponseBody()) {
                    jerseyResponse.setEntityStream(response.getResponseBodyAsStream());
                }
                callback.response(jerseyResponse);
                future.complete(jerseyResponse);
                return response;
            }

            @Override
            public void onThrowable(Throwable t) {
                callback.failure(t);
                future.completeExceptionally(t);
            }
        });

        return future;
    }

    @Override
    public String getName() {
        return "Pulsar-Admin";
    }

    @Override
    public void close() {
        try {
            httpClient.close();
        } catch (IOException e) {
            log.warn("Failed to close http client", e);
        }
    }

    @Override
    public void response(ClientResponse clientResponse) {
        // do nothing
    }

    @Override
    public void failure(Throwable throwable) {
        // do nothing
    }
}
