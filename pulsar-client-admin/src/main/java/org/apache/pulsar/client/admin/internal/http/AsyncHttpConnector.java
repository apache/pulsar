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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
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
public class AsyncHttpConnector implements Connector {

    private final AsyncHttpClient httpClient;

    @SneakyThrows
    public AsyncHttpConnector(Client client, ClientConfigurationData conf) {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout((int) client.getConfiguration().getProperty(ClientProperties.CONNECT_TIMEOUT));
        confBuilder.setReadTimeout((int) client.getConfiguration().getProperty(ClientProperties.READ_TIMEOUT));
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(Request ahcRequest, HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5) && super.keepAlive(ahcRequest, request, response);
            }
        });

        if (conf != null && StringUtils.isNotBlank(conf.getServiceUrl())
                && conf.getServiceUrl().startsWith("https://")) {

            SslContext sslCtx = null;

            // Set client key and certificate if available
            AuthenticationDataProvider authData = conf.getAuthentication().getAuthData();
            if (authData.hasDataForTls()) {
                sslCtx = SecurityUtility.createNettySslContextForClient(
                        conf.isTlsAllowInsecureConnection() || !conf.isTlsHostnameVerificationEnable(),
                        conf.getTlsTrustCertsFilePath(),
                        authData.getTlsCertificates(), authData.getTlsPrivateKey());
            } else {
                sslCtx = SecurityUtility.createNettySslContextForClient(
                        conf.isTlsAllowInsecureConnection() || !conf.isTlsHostnameVerificationEnable(),
                        conf.getTlsTrustCertsFilePath());
            }

            confBuilder.setSslContext(sslCtx);
        }
        httpClient = new DefaultAsyncHttpClient(confBuilder.build());
    }

    @Override
    public ClientResponse apply(ClientRequest jerseyRequest) {
        CompletableFuture<ClientResponse> future = new CompletableFuture<>();

        try {
            Future<?> resultFuture = apply(jerseyRequest, new AsyncConnectorCallback() {
                @Override
                public void response(ClientResponse response) {
                    future.complete(response);
                }

                @Override
                public void failure(Throwable failure) {
                    future.completeExceptionally(failure);
                }
            });

            Integer timeout = ClientProperties.getValue(
                    jerseyRequest.getConfiguration().getProperties(),
                    ClientProperties.READ_TIMEOUT, 0);

            if (timeout != null && timeout > 0) {
                resultFuture.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                resultFuture.get();
            }
        } catch (ExecutionException ex) {
            Throwable e = ex.getCause() == null ? ex : ex.getCause();
            throw new ProcessingException(e.getMessage(), e);
        } catch (Exception ex) {
            throw new ProcessingException(ex.getMessage(), ex);
        }

        return future.join();
    }

    @Override
    public Future<?> apply(ClientRequest jerseyRequest, AsyncConnectorCallback callback) {
        final CompletableFuture<Object> future = new CompletableFuture<>();

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

}
