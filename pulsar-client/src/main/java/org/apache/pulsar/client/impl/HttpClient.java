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

import com.fasterxml.jackson.core.util.JacksonFeature;
import io.netty.channel.EventLoopGroup;
import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.internal.http.AsyncHttpConnectorProvider;
import org.apache.pulsar.common.net.ServiceURI;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPartFeature;


@Slf4j
public class HttpClient implements Closeable {

    private final ServiceNameResolver serviceNameResolver;
    protected final Authentication authentication;
    private final Client client;
    private volatile WebTarget root;

    protected HttpClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this.authentication = conf.getAuthentication();

        this.serviceNameResolver = new PulsarServiceNameResolver();
        this.serviceNameResolver.updateServiceUrl(conf.getServiceUrl());

        AsyncHttpConnectorProvider asyncConnectorProvider = new AsyncHttpConnectorProvider(conf,
                conf.getAutoCertRefreshSeconds(), true,  this.serviceNameResolver);

        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);
        httpConfig.connectorProvider(asyncConnectorProvider);

        ClientBuilder clientBuilder = ClientBuilder.newBuilder()
                .withConfig(httpConfig)
                .connectTimeout(conf.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(conf.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .register(JacksonFeature.class);

        client = clientBuilder.build();
        String serviceUri = ServiceURI.create(conf.getServiceUrl()).selectOne();
        root = client.target(serviceUri);

        log.debug("Using HTTP url: {}", serviceUri);
    }

    String getServiceUrl() {
        return this.serviceNameResolver.getServiceUrl();
    }

    public InetSocketAddress resolveHost() {
        return serviceNameResolver.resolveHost();
    }

    void setServiceUrl(String serviceUrl) throws PulsarClientException {
        this.serviceNameResolver.updateServiceUrl(serviceUrl);
        root = client.target(serviceNameResolver.resolveHostUri());
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    public <T> CompletableFuture<T> get(Function<WebTarget, WebTarget> webTargetFn, Class<T> clazz) {
        WebTarget finalWebTarget = webTargetFn.apply(root);

        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            String requestUrl = finalWebTarget.getUri().toString();
            String remoteHostName = finalWebTarget.getUri().getHost();
            AuthenticationDataProvider authData = authentication.getAuthData(remoteHostName);

            CompletableFuture<Map<String, String>>  authFuture = new CompletableFuture<>();

            // bring a authenticationStage for sasl auth.
            if (authData.hasDataForHttp()) {
                authentication.authenticationStage(finalWebTarget, authData, null, authFuture);
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
                Builder builder = finalWebTarget.request("application/json");

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
                        headers.forEach(entry -> builder.header(entry.getKey(), entry.getValue()));
                    }
                }

                builder.async().get(new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                            try {
                                T data = response.readEntity(clazz);
                                future.complete(data);
                            } catch (Exception e) {
                                log.warn("[{}] Error during HTTP get request: {}", requestUrl, e.getMessage());
                                future.completeExceptionally(new PulsarClientException(e));
                            }
                        } else {
                            log.warn("[{}] HTTP get request failed: {}", requestUrl, response.getStatusInfo());
                            Exception e;
                            if (response.getStatus() == HttpURLConnection.HTTP_NOT_FOUND) {
                                e = new NotFoundException("Not found: " + response.getStatusInfo());
                            } else {
                                e = new PulsarClientException("HTTP get request failed: " + response.getStatusInfo());
                            }
                            future.completeExceptionally(e);
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        log.warn("[{}] Failed to perform http request: {}", requestUrl, throwable.getMessage());
                        future.completeExceptionally(new PulsarClientException(throwable));
                    }
                });
            });
        } catch (Exception e) {
            log.warn("[{}]PulsarClientImpl: {}", finalWebTarget.getUri(), e.getMessage());
            if (e instanceof PulsarClientException) {
                future.completeExceptionally(e);
            } else {
                future.completeExceptionally(new PulsarClientException(e));
            }
        }

        return future;
    }
}
