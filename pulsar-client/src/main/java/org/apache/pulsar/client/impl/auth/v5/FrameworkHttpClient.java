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
package org.apache.pulsar.client.impl.auth.v5;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.resolver.NameResolver;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.pulsar.http.HttpRequest;
import org.apache.pulsar.http.HttpResponse;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.tls.TlsHandle;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

/**
 * An {@link AsyncHttpClient}-backed {@link PulsarHttpClient} (PIP-478).
 *
 * <p>A thin adapter that maps the framework {@link HttpRequest}/{@link HttpResponse} value types onto
 * AsyncHttpClient's request/response API. It is used for both framework-managed instances (built by
 * {@link FrameworkHttpClientFactory} over the client's <em>shared</em> event loop / timer / DNS resolver)
 * and the deprecated legacy fallback path a v4 auth plugin uses when the framework services are not bound.
 *
 * <p>When a shared {@link NameResolver} is supplied it is set on every request so DNS resolution and its
 * cache are shared with the owning {@code PulsarClient} (mirroring {@code HttpClient}'s per-request
 * {@code setNameResolver}); when {@code null} (the legacy fallback) AsyncHttpClient uses its own resolver.
 *
 * <p>Responses are fully buffered, but accumulation is bounded while streaming: as soon as the received
 * body exceeds {@link PulsarHttpClientConfig#maxResponseBodyBytes()} the exchange is aborted and the
 * returned future completes exceptionally with an {@link IOException} — an oversized (or unbounded)
 * response is never held in memory, and each response of an AHC-followed redirect chain is bounded
 * independently.
 */
@CustomLog
public final class FrameworkHttpClient implements PulsarHttpClient {

    private final AsyncHttpClient asyncHttpClient;
    private final PulsarHttpClientConfig config;
    // Shared DNS resolver from the owning client, or null for the legacy fallback (AHC resolves itself).
    private final NameResolver<InetAddress> nameResolver;
    // The new-path TLS subscription (CLIENT_OAUTH2 / CLIENT_DEFAULT SslContext), or null on the legacy path.
    private final TlsHandle<?> tlsSubscription;
    // Deregisters this client from its factory on close(), or null for the legacy fallback (no factory).
    private final Runnable onClose;
    private final AtomicBoolean closed = new AtomicBoolean();

    public FrameworkHttpClient(AsyncHttpClient asyncHttpClient, PulsarHttpClientConfig config,
            NameResolver<InetAddress> nameResolver, TlsHandle<?> tlsSubscription, Runnable onClose) {
        this.asyncHttpClient = asyncHttpClient;
        this.config = config;
        this.nameResolver = nameResolver;
        this.tlsSubscription = tlsSubscription;
        this.onClose = onClose;
    }

    @Override
    public CompletableFuture<HttpResponse> execute(HttpRequest request) {
        // A CompletableFuture-returning method must not throw synchronously (CODING.md): convert any
        // request-building error into an exceptionally-completed future.
        final org.asynchttpclient.Request ahcRequest;
        try {
            ahcRequest = toAhcRequest(request);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
        return asyncHttpClient.executeRequest(ahcRequest, new BoundedResponseHandler())
                .toCompletableFuture()
                .thenCompose(this::toHttpResponse);
    }

    /**
     * Aggregates the response like AsyncHttpClient's default handler, but fails the exchange as soon as the
     * accumulated body exceeds {@link PulsarHttpClientConfig#maxResponseBodyBytes()}: throwing from
     * {@code onBodyPartReceived} aborts the connection and completes the future exceptionally, so the cap
     * bounds memory rather than being checked after full aggregation.
     */
    private final class BoundedResponseHandler extends AsyncCompletionHandlerBase {
        private long receivedBytes;

        @Override
        public State onStatusReceived(HttpResponseStatus status) throws Exception {
            // A new response on this exchange restarts the accumulation, so each response of a redirect
            // chain is bounded independently (matching the builder reset in the superclass).
            receivedBytes = 0;
            return super.onStatusReceived(status);
        }

        @Override
        public State onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
            receivedBytes += content.length();
            if (receivedBytes > config.maxResponseBodyBytes()) {
                throw new IOException("HTTP response body exceeds the configured maximum of "
                        + config.maxResponseBodyBytes() + " bytes");
            }
            return super.onBodyPartReceived(content);
        }
    }

    private org.asynchttpclient.Request toAhcRequest(HttpRequest request) {
        RequestBuilder builder = new RequestBuilder(request.method().name())
                .setUrl(request.uri().toString());
        if (nameResolver != null) {
            // Share the DNS resolver and its cache with the owning PulsarClient.
            builder.setNameResolver(nameResolver);
        }

        request.headers().forEach(builder::setHeader);

        request.body().ifPresent(body -> {
            if (body instanceof HttpRequest.Bytes bytes) {
                builder.setBody(bytes.content());
                if (bytes.contentType() != null) {
                    builder.setHeader(HttpHeaderNames.CONTENT_TYPE, bytes.contentType());
                }
            } else {
                throw new IllegalArgumentException("Unsupported request body type: " + body.getClass().getName());
            }
        });

        return builder.build();
    }

    private CompletableFuture<HttpResponse> toHttpResponse(Response response) {
        byte[] body = response.getResponseBodyAsBytes();
        // Final guard only: BoundedResponseHandler already aborts the exchange while streaming.
        if (body != null && body.length > config.maxResponseBodyBytes()) {
            return CompletableFuture.failedFuture(new IOException(
                    "HTTP response body of " + body.length + " bytes exceeds the configured maximum of "
                            + config.maxResponseBodyBytes() + " bytes"));
        }
        Map<String, String> headers = new LinkedHashMap<>();
        response.getHeaders().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        return CompletableFuture.completedFuture(HttpResponse.of(response.getStatusCode(), headers, body));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (onClose != null) {
            try {
                onClose.run();
            } catch (Throwable t) {
                log.warn().exception(t).log("Failed to deregister framework HTTP client from its factory");
            }
        }
        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            log.warn().exception(e).log("Failed to close AsyncHttpClient");
        }
        if (tlsSubscription != null) {
            // The TLS factory itself is owned and closed by PulsarClientImpl; release only this subscription.
            tlsSubscription.dispose();
        }
    }
}
