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
package org.apache.pulsar.client.admin.internal;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AsyncGetRequestTest {

    WireMockServer server;
    String adminUrl;

    @BeforeClass(alwaysRun = true)
    void beforeClass() throws IOException {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig()
                .containerThreads(8)
                .port(0));
        server.start();
        adminUrl = "http://localhost:" + server.port();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testAsyncGetRequest_200OK_WithBody() throws Exception {
        // Mock successful response with body
        server.stubFor(get(urlEqualTo("/200-with-body"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"name\":\"test-namespace\"}")));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        NamespacesImpl namespaces = (NamespacesImpl) admin.namespaces();
        CompletableFuture<Object> getRequest =
                namespaces.asyncGetRequest(admin.getRoot().path("/200-with-body"), Object.class);

        assertThat(getRequest).succeedsWithin(3, TimeUnit.SECONDS);

        // Verify the request was made
        server.verify(getRequestedFor(urlEqualTo("/200-with-body")));
    }

    @Test
    public void testAsyncGetRequest_204NoContent() throws Exception {
        // Mock 204 No Content response
        server.stubFor(get(urlEqualTo("/204"))
                .willReturn(aResponse()
                        .withStatus(204)));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        NamespacesImpl namespaces = (NamespacesImpl) admin.namespaces();
        CompletableFuture<Object> getRequest =
                namespaces.asyncGetRequest(admin.getRoot().path("/204"), Object.class);

        assertThat(getRequest).succeedsWithin(3, TimeUnit.SECONDS);

        // Verify the request was made
        server.verify(getRequestedFor(urlEqualTo("/204")));
    }

    @Test
    public void testAsyncGetRequest_404NotFound() throws Exception {
        // Mock 404 Not Found response
        server.stubFor(get(urlEqualTo("/404"))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"reason\":\"Not found\"}")));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        NamespacesImpl namespaces = (NamespacesImpl) admin.namespaces();
        CompletableFuture<Object> getRequest =
                namespaces.asyncGetRequest(admin.getRoot().path("/404"), Object.class);

        assertThat(getRequest)
                .failsWithin(3, TimeUnit.SECONDS)
                .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                .withCauseInstanceOf(PulsarAdminException.class);

        server.verify(getRequestedFor(urlEqualTo("/404")));
    }

    @Test
    public void testAsyncGetRequest_500InternalServerError() throws Exception {
        // Mock 500 Internal Server Error response
        server.stubFor(get(urlEqualTo("/500"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("Internal Server Error")));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        NamespacesImpl namespaces = (NamespacesImpl) admin.namespaces();
        CompletableFuture<Object> getRequest =
                namespaces.asyncGetRequest(admin.getRoot().path("/500"), Object.class);

        assertThat(getRequest)
                .failsWithin(3, TimeUnit.SECONDS)
                .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                .withCauseInstanceOf(PulsarAdminException.class);

        server.verify(getRequestedFor(urlEqualTo("/500")));
    }

    @Test
    public void testAsyncGetRequest_EmptyResponseBody_With200() throws Exception {
        // Mock 200 with empty body
        server.stubFor(get(urlEqualTo("/200-empty"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("")));

        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        NamespacesImpl namespaces = (NamespacesImpl) admin.namespaces();
        CompletableFuture<Object> getRequest =
                namespaces.asyncGetRequest(admin.getRoot().path("/200-empty"), Object.class);

        // Should handle empty body gracefully
        assertThat(getRequest).succeedsWithin(3, TimeUnit.SECONDS);

        server.verify(getRequestedFor(urlEqualTo("/200-empty")));
    }
}