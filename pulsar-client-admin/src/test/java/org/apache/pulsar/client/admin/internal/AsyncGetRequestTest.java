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
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.CURSOR_STATS_LOCAL_ONLY_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import jakarta.ws.rs.client.WebTarget;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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

    @DataProvider
    public Object[][] unsetNamespaceStringValues() {
        return new Object[][] {
                {"antiAffinity"},
                {"resourcegroup"},
                {"configversion"},
        };
    }

    @Test(dataProvider = "unsetNamespaceStringValues")
    public void testUnsetNamespaceStringValueMapsToNull(String part) throws Exception {
        String namespace = "t/ns-" + part;
        String url = "/admin/v2/namespaces/" + namespace + "/" + part;

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        // An async JAX-RS resource that resumes with null answers 200 with an empty body
        server.stubFor(get(urlEqualTo(url)).willReturn(aResponse().withStatus(200).withBody("")));
        assertThat(getNamespaceStringValue(admin, part, namespace)).isNull();

        server.stubFor(get(urlEqualTo(url)).willReturn(aResponse().withStatus(204)));
        assertThat(getNamespaceStringValue(admin, part, namespace)).isNull();

        server.stubFor(get(urlEqualTo(url)).willReturn(stringBody("my-value")));
        assertThat(getNamespaceStringValue(admin, part, namespace)).isEqualTo("my-value");
    }

    @Test
    public void testEmptyBodyIsNotMappedToNullForOtherStringGetters() throws Exception {
        // The empty -> null mapping is per method, not a rule of the shared FutureCallback
        server.stubFor(get(urlEqualTo("/admin/v2/brokers/version"))
                .willReturn(aResponse().withStatus(200).withBody("")));

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        assertThat(admin.brokers().getVersion()).isEmpty();
    }

    @Test
    public void testSubscriptionCursorInternalStatsRequestAndResponse() throws Exception {
        String path = "/admin/v2/persistent/t/ns/stats-topic/subscription/sub%2B%252B%252F%25252F/internalStats";
        server.stubFor(get(urlEqualTo(path + "?includeIndividualDeletedMessages=false"))
                .withHeader(CURSOR_STATS_LOCAL_ONLY_HEADER, absent())
                .willReturn(stringBody("""
                        {"markDeletePosition":"1:2","totalNonContiguousDeletedMessagesRange":1}
                        """)));
        server.stubFor(get(urlEqualTo(path + "?includeIndividualDeletedMessages=true"))
                .withHeader(CURSOR_STATS_LOCAL_ONLY_HEADER, absent())
                .willReturn(stringBody("{\"individuallyDeletedMessages\":\"[(1:3..1:4]]\"}")));

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            Topics topics = admin.topics();
            var summary = topics.getSubscriptionCursorInternalStats("persistent://t/ns/stats-topic", "sub +/%2F");
            assertThat(summary.markDeletePosition).isEqualTo("1:2");
            assertThat(summary.individuallyDeletedMessages).isNull();
            assertThat(summary.totalNonContiguousDeletedMessagesRange).isEqualTo(1);

            var detailed = topics.getSubscriptionCursorInternalStatsAsync(
                    "persistent://t/ns/stats-topic", "sub +/%2F", true).get(3, TimeUnit.SECONDS);
            assertThat(detailed.individuallyDeletedMessages).isEqualTo("[(1:3..1:4]]");
        }
    }

    @Test
    public void testLocalSubscriptionCursorStatsHeaderSurvivesOwnershipRedirect() throws Exception {
        String path = "/admin/v2/persistent/t/ns/stats-topic-partition-0/subscription/sub/internalStats"
                + "?includeIndividualDeletedMessages=true";
        String redirect = path + "&authoritative=true";
        server.stubFor(get(urlEqualTo(path))
                .withHeader(CURSOR_STATS_LOCAL_ONLY_HEADER, equalTo("true"))
                .willReturn(aResponse().withStatus(307).withHeader("Location", adminUrl + redirect)));
        server.stubFor(get(urlEqualTo(redirect))
                .withHeader(CURSOR_STATS_LOCAL_ONLY_HEADER, equalTo("true"))
                .willReturn(stringBody("{\"individuallyDeletedMessages\":\"[(1:3..1:4]]\"}")));

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            TopicsImpl topics = (TopicsImpl) admin.topics();
            var stats = topics.getLocalSubscriptionCursorInternalStatsAsync(
                    "persistent://t/ns/stats-topic-partition-0", "sub", true).get(3, TimeUnit.SECONDS);
            assertThat(stats.individuallyDeletedMessages).isEqualTo("[(1:3..1:4]]");
        }
    }

    @Test
    public void testLocalSubscriptionCursorStatsPreservesOldOwnerNotFound() throws Exception {
        String topic = "persistent://t/ns/old-owner-topic-partition-0";
        String path = "/admin/v2/persistent/t/ns/old-owner-topic-partition-0/subscription/sub/internalStats"
                + "?includeIndividualDeletedMessages=false";
        String redirect = path + "&authoritative=true";
        server.stubFor(get(urlEqualTo(path))
                .withHeader(CURSOR_STATS_LOCAL_ONLY_HEADER, equalTo("true"))
                .willReturn(aResponse().withStatus(307).withHeader("Location", adminUrl + redirect)));
        server.stubFor(get(urlEqualTo(redirect))
                .willReturn(aResponse().withStatus(404).withHeader("Content-Type", "text/html")
                        .withBody("<html><body>Not Found</body></html>")));

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            TopicsImpl topics = (TopicsImpl) admin.topics();
            var request = topics.getLocalSubscriptionCursorInternalStatsAsync(topic, "sub", false);
            assertThatThrownBy(() -> request.get(3, TimeUnit.SECONDS))
                    .cause().isInstanceOfSatisfying(PulsarAdminException.NotFoundException.class,
                            error -> assertThat(error.getStatusCode()).isEqualTo(404));
        }
    }

    @Test
    public void testExpiredPartitionWaitingForOAuth2RefreshSkipsTokenRequest() throws Exception {
        server.stubFor(get(urlEqualTo("/.well-known/openid-configuration")).willReturn(
                stringBody("{\"token_endpoint\":\"" + adminUrl + "/token\"}")));
        server.stubFor(get(urlEqualTo("/credentials")).willReturn(
                stringBody("{\"client_id\":\"client\",\"client_secret\":\"secret\"}")));
        server.stubFor(post(urlEqualTo("/token")).willReturn(stringBody("{\"error\":\"invalid_client\"}")
                .withStatus(401)));
        var auth = AuthenticationFactoryOAuth2.clientCredentialsBuilder()
                .issuerUrl(URI.create(adminUrl).toURL())
                .credentialsUrl(URI.create(adminUrl + "/credentials").toURL())
                .build();
        try (var admin = (PulsarAdminImpl) PulsarAdmin.builder().serviceHttpUrl(adminUrl).authentication(auth).build();
             var executor = Executors.newSingleThreadExecutor()) {
            var topics = (TopicsImpl) admin.topics();
            topics.setBlockingAuthExecutor(executor);
            var worker = executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            var partitionRequest = new CompletableFuture<Void>();
            CompletableFuture<?> prepared;
            synchronized (auth) {
                prepared = topics.requestAsync(admin.getRoot(), partitionRequest);
                var threads = ManagementFactory.getThreadMXBean();
                await().until(() -> {
                    var info = threads.getThreadInfo(worker.threadId());
                    return info != null && info.getThreadState() == Thread.State.BLOCKED
                            && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(auth);
                });
                partitionRequest.cancel(false);
                // The active refresh fails, leaving the token cache empty when the partition gets the lock.
                assertThatThrownBy(() -> auth.getAuthData("localhost"))
                        .isInstanceOf(PulsarClientException.AuthenticationException.class);
            }
            assertThatThrownBy(() -> prepared.get(5, TimeUnit.SECONDS)).isInstanceOf(CancellationException.class);
            server.verify(1, postRequestedFor(urlEqualTo("/token")));
        }
    }

    @Test
    public void testPartitionedSubscriptionCursorStatsParsesPartialResultAndPreservesRestFailure() throws Exception {
        String topic = "persistent://t/ns/partitioned-stats-topic";
        String path = "/admin/v2/persistent/t/ns/partitioned-stats-topic/subscription/sub/partitioned-internalStats";
        server.stubFor(get(urlEqualTo(path + "?includeIndividualDeletedMessages=false"))
                .withHeader(CURSOR_STATS_LOCAL_ONLY_HEADER, absent())
                .willReturn(stringBody("""
                        {
                          "metadata": {"partitions": 2},
                          "complete": false,
                          "partitions": {"persistent://t/ns/partitioned-stats-topic-partition-0": {"state":"Open"}},
                          "errors": {"persistent://t/ns/partitioned-stats-topic-partition-1":
                              {"statusCode":404,"errorCode":"NOT_FOUND","reason":"Subscription not found"}}
                        }
                        """)));
        server.stubFor(get(urlEqualTo(path.replace("/sub/", "/missing/") + "?includeIndividualDeletedMessages=true"))
                .willReturn(stringBody("{\"reason\":\"No subscription cursor stats found\"}").withStatus(404)));

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            var result = admin.topics().getPartitionedSubscriptionCursorInternalStatsAsync(topic, "sub")
                    .get(3, TimeUnit.SECONDS);
            assertThat(result.metadata.partitions).isEqualTo(2);
            assertThat(result.complete).isFalse();
            assertThat(result.partitions.get(topic + "-partition-0").state).isEqualTo("Open");
            assertThat(result.errors.get(topic + "-partition-1").errorCode).isEqualTo("NOT_FOUND");
            assertThatThrownBy(() ->
                    admin.topics().getPartitionedSubscriptionCursorInternalStats(topic, "missing", true))
                    .isInstanceOf(PulsarAdminException.NotFoundException.class);
        }
    }

    @Test
    public void testSubscriptionCursorStatsValidationFailsThroughFuture() {
        Topics topics = new TopicsImpl(mock(WebTarget.class), null, 0);
        assertThatThrownBy(() -> topics.getSubscriptionCursorInternalStatsAsync("persistent://t/ns", "sub").join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> topics.getPartitionedSubscriptionCursorInternalStatsAsync(
                "persistent://t/ns/stats-topic", "").join()).hasCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> topics.getSubscriptionCursorInternalStatsAsync(
                "non-persistent://t/ns/stats-topic", "sub").join()).hasCauseInstanceOf(IllegalArgumentException.class);
    }

    private static ResponseDefinitionBuilder stringBody(String value) {
        return aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(value);
    }

    private static String getNamespaceStringValue(PulsarAdmin admin, String part, String namespace)
            throws PulsarAdminException {
        switch (part) {
            case "antiAffinity":
                return admin.namespaces().getNamespaceAntiAffinityGroup(namespace);
            case "resourcegroup":
                return admin.namespaces().getNamespaceResourceGroup(namespace);
            case "configversion":
                return admin.namespaces().getReplicationConfigVersion(namespace);
            default:
                throw new IllegalArgumentException(part);
        }
    }
}
