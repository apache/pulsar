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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.http.HttpRequest;
import org.apache.pulsar.http.HttpResponse;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the framework AsyncHttpClient-backed {@link PulsarHttpClient} factory (PIP-478 stage 3c): request /
 * response mapping, redirect following, the streaming response-body cap, and the factory / client close
 * semantics. TLS-by-purpose is
 * exercised end-to-end by the OAuth2 gate; here the legacy (plaintext) path is used against a local server.
 */
public class FrameworkHttpClientFactoryTest {

    private HttpServer server;
    private String baseUrl;
    private EventLoopGroup eventLoopGroup;
    private Timer timer;

    @BeforeMethod
    public void setup() throws IOException {
        eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("test-http"));
        timer = new HashedWheelTimer(new DefaultThreadFactory("test-http-timer"));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/echo-get", exchange -> {
            byte[] body = "hello-get".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("X-Test-Header", "abc");
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/echo-post", exchange -> {
            byte[] received = readAll(exchange);
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            byte[] body = ("posted:" + new String(received, StandardCharsets.UTF_8) + ":ct=" + contentType)
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/big", exchange -> {
            byte[] body = new byte[64 * 1024];
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/redirect", exchange -> {
            exchange.getResponseHeaders().add("Location", baseUrl + "/echo-get");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/redirect-big", exchange -> {
            exchange.getResponseHeaders().add("Location", baseUrl + "/big");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/unbounded", exchange -> {
            // Chunked (unknown-length) response streamed until the client aborts at its response-body cap;
            // the iteration bound is a safety net so an undetected abort cannot hang the test.
            exchange.sendResponseHeaders(200, 0);
            byte[] chunk = new byte[1024];
            try (var os = exchange.getResponseBody()) {
                for (int i = 0; i < 8 * 1024; i++) {
                    os.write(chunk);
                    os.flush();
                }
            } catch (IOException expected) {
                // The client closed the connection mid-stream.
            }
        });
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (server != null) {
            server.stop(0);
        }
        if (timer != null) {
            timer.stop();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).await(10, TimeUnit.SECONDS);
        }
    }

    private static byte[] readAll(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return is.readAllBytes();
        }
    }

    private FrameworkHttpClientFactory newFactory() {
        // Legacy TLS path (tlsFactory supplier returns null); plaintext HTTP, so TLS is not exercised.
        return new FrameworkHttpClientFactory(() -> eventLoopGroup, () -> timer, () -> null, () -> null,
                new ClientConfigurationData(), "test-client");
    }

    private static PulsarHttpClientConfig.Builder genericConfig() {
        return PulsarHttpClientConfig.builder(TlsPurpose.CLIENT_OAUTH2);
    }

    @Test
    public void testGet() throws Exception {
        try (FrameworkHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().build());
            HttpRequest request = HttpRequest.builder(
                    HttpRequest.Method.GET, URI.create(baseUrl + "/echo-get")).build();
            HttpResponse response = client.execute(request).get(30, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.bodyAsString()).isEqualTo("hello-get");
            // Header lookup is case-insensitive.
            assertThat(response.header("x-test-header")).contains("abc");
            client.close();
        }
    }

    @Test
    public void testPostBytes() throws Exception {
        try (FrameworkHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().build());
            HttpRequest request = HttpRequest.builder(HttpRequest.Method.POST, URI.create(baseUrl + "/echo-post"))
                    .body(new HttpRequest.Bytes("payload".getBytes(StandardCharsets.UTF_8), "application/json"))
                    .build();
            HttpResponse response = client.execute(request).get(30, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isEqualTo(201);
            assertThat(response.bodyAsString()).isEqualTo("posted:payload:ct=application/json");
            client.close();
        }
    }

    @Test
    public void testRedirectIsFollowed() throws Exception {
        try (FrameworkHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().build());
            HttpRequest request = HttpRequest.builder(
                    HttpRequest.Method.GET, URI.create(baseUrl + "/redirect")).build();
            // AHC follows the 302 itself (v4 parity); the caller sees the final response.
            HttpResponse response = client.execute(request).get(30, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.bodyAsString()).isEqualTo("hello-get");
            client.close();
        }
    }

    @Test
    public void testStreamingBodyAbortedAtCap() throws Exception {
        try (FrameworkHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().maxResponseBodyBytes(1024).build());
            HttpRequest request = HttpRequest.builder(
                    HttpRequest.Method.GET, URI.create(baseUrl + "/unbounded")).build();
            // The chunked stream has no Content-Length, so only the streaming bound can stop it: the
            // exchange is aborted at the cap instead of the (would-be multi-MiB) body being aggregated.
            assertThatThrownBy(() -> client.execute(request).get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IOException.class)
                    .cause().hasMessageContaining("exceeds the configured maximum of 1024 bytes");
            client.close();
        }
    }

    @Test
    public void testRedirectedResponseEnforcesCap() throws Exception {
        try (FrameworkHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().maxResponseBodyBytes(1024).build());
            HttpRequest request = HttpRequest.builder(
                    HttpRequest.Method.GET, URI.create(baseUrl + "/redirect-big")).build();
            assertThatThrownBy(() -> client.execute(request).get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IOException.class)
                    .cause().hasMessageContaining("exceeds the configured maximum");
            client.close();
        }
    }

    @Test
    public void testMaxResponseBodyBytesOverflowFails() throws Exception {
        try (FrameworkHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().maxResponseBodyBytes(1024).build());
            HttpRequest request = HttpRequest.builder(
                    HttpRequest.Method.GET, URI.create(baseUrl + "/big")).build();
            assertThatThrownBy(() -> client.execute(request).get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IOException.class)
                    .cause().hasMessageContaining("exceeds the configured maximum");
            client.close();
        }
    }

    @Test
    public void testFactoryCloseClosesInstancesAndRejectsNew() throws Exception {
        FrameworkHttpClientFactory factory = newFactory();
        PulsarHttpClient client = factory.newHttpClient(genericConfig().build());
        // Sanity: works before close.
        HttpRequest request = HttpRequest.builder(HttpRequest.Method.GET, URI.create(baseUrl + "/echo-get")).build();
        assertThat(client.execute(request).get(30, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
        factory.close();
        factory.close(); // idempotent
        // The factory-owned client was closed; a further request fails rather than succeeding silently.
        assertThatThrownBy(() -> client.execute(request).get(30, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
        client.close(); // idempotent even after the factory closed it
        // New clients are rejected once the factory is closed.
        assertThatThrownBy(() -> factory.newHttpClient(genericConfig().build()))
                .isInstanceOf(IllegalStateException.class);
    }
}
