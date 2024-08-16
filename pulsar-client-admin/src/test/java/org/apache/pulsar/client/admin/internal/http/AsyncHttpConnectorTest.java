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
package org.apache.pulsar.client.admin.internal.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.spi.AsyncConnectorCallback;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.internal.PropertiesDelegate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AsyncHttpConnectorTest {
    WireMockServer server;
    ConcurrencyTestTransformer concurrencyTestTransformer = new ConcurrencyTestTransformer();

    private static class CopyRequestBodyToResponseBodyTransformer extends ResponseTransformer {
        @Override
        public com.github.tomakehurst.wiremock.http.Response transform(
                com.github.tomakehurst.wiremock.http.Request request,
                com.github.tomakehurst.wiremock.http.Response response, FileSource fileSource, Parameters parameters) {
            return com.github.tomakehurst.wiremock.http.Response.Builder.like(response)
                    .body(request.getBodyAsString())
                    .build();
        }

        @Override
        public String getName() {
            return "copy-body";
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }
    }

    private static class ConcurrencyTestTransformer extends ResponseTransformer {
        private static final long DELAY_MS = 100;
        private final AtomicInteger concurrencyCounter = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Override
        public com.github.tomakehurst.wiremock.http.Response transform(
                com.github.tomakehurst.wiremock.http.Request request,
                com.github.tomakehurst.wiremock.http.Response response, FileSource fileSource, Parameters parameters) {
            int currentCounter = concurrencyCounter.incrementAndGet();
            maxConcurrency.updateAndGet(v -> Math.max(v, currentCounter));
            try {
                try {
                    Thread.sleep(DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return com.github.tomakehurst.wiremock.http.Response.Builder.like(response)
                        .body(String.valueOf(currentCounter))
                        .build();
            } finally {
                concurrencyCounter.decrementAndGet();
            }
        }

        public int getMaxConcurrency() {
            return maxConcurrency.get();
        }

        @Override
        public String getName() {
            return "concurrency-test";
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }
    }

    @BeforeClass(alwaysRun = true)
    void beforeClass() throws IOException {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig()
                .extensions(new CopyRequestBodyToResponseBodyTransformer(), concurrencyTestTransformer)
                .containerThreads(100)
                .port(0));
        server.start();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    static class TestClientRequest extends ClientRequest {
        public TestClientRequest(URI uri, ClientConfig clientConfig, PropertiesDelegate propertiesDelegate) {
            super(uri, clientConfig, propertiesDelegate);
        }
    }

    @Test
    public void testShouldStopRetriesWhenTimeoutOccurs() throws IOException, ExecutionException, InterruptedException {
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .inScenario("once")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("next")
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test-cluster\"]")));

        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                        .inScenario("once")
                        .whenScenarioStateIs("next")
                        .willSetStateTo("retried")
                .willReturn(aResponse().withStatus(500)));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());

        int requestTimeout = 500;

        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        Executor delayedExecutor = runnable -> {
            scheduledExecutor.schedule(runnable, requestTimeout, TimeUnit.MILLISECONDS);
        };
        @Cleanup
        AsyncHttpConnector connector = new AsyncHttpConnector(5000, requestTimeout,
                requestTimeout, 0, conf, false) {
            @Override
            protected CompletableFuture<Response> oneShot(InetSocketAddress host, ClientRequest request) {
                // delay the response to simulate a timeout
                return super.oneShot(host, request)
                        .thenApplyAsync(response -> {
                            return response;
                        }, delayedExecutor);
            }
        };

        JerseyClient jerseyClient = JerseyClientBuilder.createClient();
        ClientConfig clientConfig = jerseyClient.getConfiguration();
        PropertiesDelegate propertiesDelegate = new MapPropertiesDelegate();
        URI requestUri = URI.create("http://localhost:" + server.port() + "/admin/v2/clusters");
        ClientRequest request = new TestClientRequest(requestUri, clientConfig, propertiesDelegate);
        request.setMethod("GET");
        CompletableFuture<ClientResponse> future = new CompletableFuture<>();
        connector.apply(request, new AsyncConnectorCallback() {
            @Override
            public void response(ClientResponse response) {
                future.complete(response);
            }

            @Override
            public void failure(Throwable failure) {
                future.completeExceptionally(failure);
            }
        });
        Thread.sleep(2 * requestTimeout);
        String scenarioState =
                server.getAllScenarios().getScenarios().stream().filter(scenario -> "once".equals(scenario.getName()))
                        .findFirst().get().getState();
        assertEquals(scenarioState, "next");
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testMaxRedirects() {
        // Redirect to itself to test max redirects
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .willReturn(aResponse()
                        .withStatus(301)
                        .withHeader("Location", "http://localhost:" + server.port() + "/admin/v2/clusters")));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());

        @Cleanup
        AsyncHttpConnector connector = new AsyncHttpConnector(5000, 5000,
                5000, 0, conf, false);

        Request request = new RequestBuilder("GET")
                .setUrl("http://localhost:" + server.port() + "/admin/v2/clusters")
                .build();

        try {
            connector.executeRequest(request).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof AsyncHttpConnector.MaxRedirectException);
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    void testRelativeRedirect() throws ExecutionException, InterruptedException {
        doTestRedirect("path2");
    }

    @Test
    void testAbsoluteRedirect() throws ExecutionException, InterruptedException {
        doTestRedirect("/path2");
    }

    @Test
    void testUrlRedirect() throws ExecutionException, InterruptedException {
        doTestRedirect("http://localhost:" + server.port() + "/path2");
    }

    private void doTestRedirect(String location) throws InterruptedException, ExecutionException {
        server.stubFor(get(urlEqualTo("/path1"))
                .willReturn(aResponse()
                        .withStatus(301)
                        .withHeader("Location", location)));

        server.stubFor(get(urlEqualTo("/path2"))
                .willReturn(aResponse()
                        .withBody("OK")));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());

        @Cleanup
        AsyncHttpConnector connector = new AsyncHttpConnector(5000, 5000,
                5000, 0, conf, false);

        Request request = new RequestBuilder("GET")
                .setUrl("http://localhost:" + server.port() + "/path1")
                .build();

        Response response = connector.executeRequest(request).get();
        assertEquals(response.getResponseBody(), "OK");
    }

    @Test
    void testRedirectWithBody() throws ExecutionException, InterruptedException {
        server.stubFor(post(urlEqualTo("/path1"))
                .willReturn(aResponse()
                        .withStatus(307)
                        .withHeader("Location", "/path2")));

        server.stubFor(post(urlEqualTo("/path2"))
                .willReturn(aResponse()
                        .withTransformers("copy-body")));

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("http://localhost:" + server.port());

        @Cleanup
        AsyncHttpConnector connector = new AsyncHttpConnector(5000, 5000,
                5000, 0, conf, false);

        Request request = new RequestBuilder("POST")
                .setUrl("http://localhost:" + server.port() + "/path1")
                .setBody("Hello world!")
                .build();

        Response response = connector.executeRequest(request).get();
        assertEquals(response.getResponseBody(), "Hello world!");
    }

    @Test
    void testMaxConnections() throws ExecutionException, InterruptedException {
        server.stubFor(post(urlEqualTo("/concurrency-test"))
                .willReturn(aResponse()
                        .withTransformers("concurrency-test")));

        ClientConfigurationData conf = new ClientConfigurationData();
        int maxConnections = 10;
        conf.setConnectionsPerBroker(maxConnections);
        conf.setServiceUrl("http://localhost:" + server.port());

        @Cleanup
        AsyncHttpConnector connector = new AsyncHttpConnector(5000, 5000,
                5000, 0, conf, false);

        Request request = new RequestBuilder("POST")
                .setUrl("http://localhost:" + server.port() + "/concurrency-test")
                .build();

        List<CompletableFuture<Response>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(connector.executeRequest(request));
        }
        FutureUtil.waitForAll(futures).get();
        int maxConcurrency = concurrencyTestTransformer.getMaxConcurrency();
        assertTrue(maxConcurrency > maxConnections / 2 && maxConcurrency <= maxConnections,
                "concurrency didn't get limited as expected (max: " + maxConcurrency + ")");
    }
}