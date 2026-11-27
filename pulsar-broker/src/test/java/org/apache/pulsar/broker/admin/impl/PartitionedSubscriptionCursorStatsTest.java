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
package org.apache.pulsar.broker.admin.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.ssl.SslHandshakeTimeoutException;
import jakarta.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.GettingAuthenticationDataException;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.MaxRedirectException;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.RetryException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.PulsarClientException.InvalidConfigurationException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
import org.apache.pulsar.utils.TestLogAppender;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class PartitionedSubscriptionCursorStatsTest {
    private static final TopicName TOPIC = TopicName.get("persistent://public/default/cursor-stats");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    public void testBoundedFanOutStopsAtDeadline() throws Exception {
        var executor = mock(ScheduledExecutorService.class);
        var expire = new AtomicReference<Runnable>();
        doAnswer(invocation -> {
            expire.set(invocation.getArgument(0));
            return mock(ScheduledFuture.class);
        }).when(executor).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        try (var worker = Executors.newSingleThreadExecutor()) {
            doAnswer(invocation -> {
                worker.execute(invocation.getArgument(0));
                return null;
            }).when(executor).execute(any(Runnable.class));

            var partitions = new CopyOnWriteArrayList<String>();
            var requests = new CopyOnWriteArrayList<CompletableFuture<CursorStats>>();
            var uncancellable = new CompletableFuture<CursorStats>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }
            };
            var response = PartitionedSubscriptionCursorStats.collect(TOPIC, new PartitionedTopicMetadata(32), false,
                    ignored -> CompletableFuture.completedFuture(name -> {
                        var request = requests.size() == 1 ? uncancellable : new CompletableFuture<CursorStats>();
                        partitions.add(name);
                        requests.add(request);
                        return request;
                    }), executor, TIMEOUT);

            await().untilAsserted(() -> assertThat(requests).hasSize(16));
            worker.submit(() -> { }).get(10, TimeUnit.SECONDS);
            assertThat(requests).hasSize(16);
            String successfulPartition = partitions.getFirst();
            requests.getFirst().complete(new CursorStats());
            await().untilAsserted(() -> assertThat(requests).hasSize(17));
            worker.submit(() -> { }).get(10, TimeUnit.SECONDS);

            expire.get().run();
            var result = response.get(10, TimeUnit.SECONDS);
            assertThat(result.complete).isFalse();
            assertThat(result.partitions).containsOnlyKeys(successfulPartition);
            assertThat(result.errors).hasSize(31).allSatisfy((partition, error) -> {
                assertThat(error.statusCode).isEqualTo(503);
                assertThat(error.errorCode).isEqualTo("TIMEOUT");
            });

            assertThat(requests.subList(2, requests.size())).allMatch(CompletableFuture::isCancelled);
            assertThat(uncancellable.complete(new CursorStats())).isTrue();
            worker.submit(() -> { }).get(10, TimeUnit.SECONDS);
            assertThat(requests).hasSize(17);
            assertThat(response.join().partitions).containsOnlyKeys(successfulPartition);
            assertThat(response.join().errors).hasSize(31);
        }
    }

    @Test
    public void testDeadlineIncludesWaitingForInternalAdminClient() {
        TopicName topic = mock(TopicName.class);
        var requestCompletion = new AtomicReference<CompletableFuture<Void>>();
        var initialization = new CompletableFuture<Function<String, CompletableFuture<CursorStats>>>();
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            var response = PartitionedSubscriptionCursorStats.collect(topic, new PartitionedTopicMetadata(2), false,
                    deadline -> {
                        requestCompletion.set(deadline);
                        return initialization;
                    }, executor, Duration.ofMillis(100));
            assertThatThrownBy(() -> response.get(10, TimeUnit.SECONDS))
                    .cause().isInstanceOfSatisfying(RestException.class,
                            error -> assertThat(error.getResponse().getStatus()).isEqualTo(503));
            assertThat(requestCompletion.get()).isCompletedExceptionally();
            assertThat(initialization).isNotDone();
            verifyNoInteractions(topic);
        }
    }

    @Test
    public void testPreparationFailurePreservesStatus() {
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            var unavailable = new RestException(Status.SERVICE_UNAVAILABLE, "Broker admin client unavailable");
            var response = PartitionedSubscriptionCursorStats.collect(TOPIC, new PartitionedTopicMetadata(2), false,
                    ignored -> CompletableFuture.failedFuture(unavailable), executor, TIMEOUT);
            assertThatThrownBy(response::join).hasCause(unavailable);
        }
    }

    @Test
    public void testUnexpectedCoordinatorFailurePreservesCause() {
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            var failure = new IllegalStateException("coordinator failure");
            var response = PartitionedSubscriptionCursorStats.collect(TOPIC, new PartitionedTopicMetadata(2), false,
                    ignored -> CompletableFuture.failedFuture(failure), executor, TIMEOUT);
            assertThatThrownBy(response::join).hasCause(failure);
        }
    }

    @Test
    public void testPartialResponseClassifiesAndSanitizesFailures() throws Exception {
        Map<Integer, String> statuses = Map.ofEntries(
                Map.entry(401, "AUTHORIZATION_FAILED"), Map.entry(403, "AUTHORIZATION_FAILED"),
                Map.entry(404, "NOT_FOUND"), Map.entry(408, "TIMEOUT"), Map.entry(504, "TIMEOUT"),
                Map.entry(412, "PRECONDITION_FAILED"), Map.entry(307, "UNAVAILABLE"), Map.entry(429, "UNAVAILABLE"),
                Map.entry(502, "UNAVAILABLE"), Map.entry(503, "UNAVAILABLE"), Map.entry(409, "INTERNAL_ERROR"));
        record Failure(int status, String code, Throwable error) { }
        List<Failure> failures = new ArrayList<>();
        statuses.forEach((status, code) -> failures.add(
                new Failure(status, code, new PulsarAdminException(null, "private upstream response", status))));
        failures.addAll(List.of(
                new Failure(503, "UNAVAILABLE", credentialFailure(new PulsarClientException.AuthenticationException(
                        "private token failure").initCause(new ConnectException("private connection failure")))),
                new Failure(503, "TIMEOUT", new PulsarAdminException(new TimeoutException("private upstream timeout"))),
                new Failure(503, "TIMEOUT", new PulsarAdminException(new RetryException("private retry",
                        new ConnectTimeoutException("private connection timeout")))),
                new Failure(503, "TIMEOUT", new PulsarAdminException(new RetryException("private retry",
                        new SslHandshakeTimeoutException("private TLS handshake timeout")))),
                new Failure(503, "UNAVAILABLE", new PulsarAdminException(new ConnectException("private address"))),
                new Failure(503, "UNAVAILABLE", new PulsarAdminException(new MaxRedirectException("private URL"))),
                new Failure(500, "INTERNAL_ERROR", new PulsarAdminException(new JsonParseException(null, "private"))),
                new Failure(500, "INTERNAL_ERROR", new IllegalStateException("private coordinator details")),
                new Failure(500, "INTERNAL_ERROR", credentialFailure(new IllegalStateException("private plugin bug"))),
                new Failure(500, "INTERNAL_ERROR", credentialFailure(new IOException("private credential I/O"))),
                new Failure(503, "TIMEOUT", credentialFailure(new TimeoutException("private credential timeout"))),
                new Failure(503, "TIMEOUT", credentialFailure(new PulsarClientException.TimeoutException("private"))),
                new Failure(503, "TIMEOUT", credentialFailure(
                        new org.apache.pulsar.client.api.v5.PulsarClientException.TimeoutException("private"))),
                new Failure(500, "INTERNAL_ERROR", credentialFailure(
                        new PulsarClientException.InvalidConfigurationException("private configuration"))),
                new Failure(500, "INTERNAL_ERROR", credentialFailure(
                        new InvalidConfigurationException("private configuration")))));
        int count = failures.size() + 1;
        try (var logs = TestLogAppender.create(PartitionedSubscriptionCursorStats.class);
             var executor = Executors.newScheduledThreadPool(2)) {
            var response = PartitionedSubscriptionCursorStats.collect(TOPIC, new PartitionedTopicMetadata(count),
                    false, ignored -> CompletableFuture.completedFuture(name -> {
                        int partition = TopicName.getPartitionIndex(name);
                        if (partition == 0) {
                            return CompletableFuture.completedFuture(new CursorStats());
                        }
                        return CompletableFuture.failedFuture(failures.get(partition - 1).error());
                    }), executor, TIMEOUT).get(10, TimeUnit.SECONDS);
            assertThat(response.complete).isFalse();
            assertThat(response.partitions).containsOnlyKeys(TOPIC.getPartition(0).toString());
            assertThat(response.errors).hasSize(count - 1);
            for (int i = 0; i < failures.size(); i++) {
                var failure = failures.get(i);
                assertPartitionError(response, i + 1, failure.status(), failure.code());
            }
            assertThat(logs.getEvents())
                    .filteredOn(event -> failures.stream().anyMatch(failure -> failure.error() == event.getThrown()))
                    .singleElement().satisfies(event -> assertThat(failures)
                            .anyMatch(failure -> failure.status() == 500 && failure.error() == event.getThrown()));
        }
    }

    private static GettingAuthenticationDataException credentialFailure(Throwable cause) {
        // TopicsImpl wraps the error from BaseResource's credential preparation.
        return new GettingAuthenticationDataException(new PulsarClientException(cause));
    }

    @Test
    public void testCredentialRejectionAndPartitionAuthorizationFailure() throws Exception {
        @Cleanup("stop")
        var server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        server.stubFor(get(urlEqualTo("/.well-known/openid-configuration")).willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withBody("{\"token_endpoint\":\"" + server.baseUrl() + "/token\"}")));
        server.stubFor(post(urlEqualTo("/token")).willReturn(aResponse().withStatus(401)
                .withHeader("Content-Type", "application/json").withBody("{\"error\":\"invalid_client\"}")));
        server.stubFor(get(urlPathEqualTo("/admin/v2/persistent/public/default/cursor-stats-partition-2"
                + "/subscription/sub/internalStats")).willReturn(aResponse().withStatus(401)));

        var auth = AuthenticationFactoryOAuth2.clientCredentialsBuilder()
                .issuerUrl(URI.create(server.baseUrl()).toURL())
                .credentialsUrl(getClass().getResource("/authentication/token/credentials_file.json"))
                .audience("pulsar")
                .build();
        try (var oauth = PulsarAdmin.builder().serviceHttpUrl(server.baseUrl()).authentication(auth).build();
             var plain = PulsarAdmin.builder().serviceHttpUrl(server.baseUrl()).build();
             var executor = Executors.newScheduledThreadPool(2)) {
            var response = PartitionedSubscriptionCursorStats.collect(TOPIC, new PartitionedTopicMetadata(3), false,
                    ignored -> CompletableFuture.completedFuture(partition -> {
                        if (partition.equals(TOPIC.getPartition(0).toString())) {
                            return CompletableFuture.completedFuture(new CursorStats());
                        }
                        var admin = partition.equals(TOPIC.getPartition(1).toString()) ? oauth : plain;
                        return ((TopicsImpl) admin.topics()).getLocalSubscriptionCursorInternalStatsAsync(
                                partition, "sub", false);
                    }), executor, TIMEOUT).get(10, TimeUnit.SECONDS);
            assertPartitionError(response, 1, 500, "INTERNAL_ERROR");
            assertPartitionError(response, 2, 401, "AUTHORIZATION_FAILED");
            server.verify(1, postRequestedFor(urlEqualTo("/token")));
        }
    }

    @Test
    public void testNoSuccessProducesTopLevelError() {
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            var notFound = new PulsarAdminException(null, "private upstream response", 404);
            var metadata = new PartitionedTopicMetadata(2);
            var allMissing = PartitionedSubscriptionCursorStats.collect(TOPIC, metadata, false,
                    ignored -> CompletableFuture.completedFuture(name -> CompletableFuture.failedFuture(notFound)),
                    executor, TIMEOUT);
            assertThatThrownBy(allMissing::join).hasCauseInstanceOf(RestException.class)
                    .cause().isInstanceOfSatisfying(RestException.class,
                            error -> assertThat(error.getResponse().getStatus()).isEqualTo(404));

            var mixed = PartitionedSubscriptionCursorStats.collect(TOPIC, metadata, false,
                    ignored -> CompletableFuture.completedFuture(name -> CompletableFuture.failedFuture(
                            TopicName.getPartitionIndex(name) == 0 ? notFound : new TimeoutException())),
                    executor, TIMEOUT);
            assertThatThrownBy(mixed::join).hasCauseInstanceOf(RestException.class)
                    .cause().isInstanceOfSatisfying(RestException.class,
                            error -> assertThat(error.getResponse().getStatus()).isEqualTo(503));
        }
    }

    @Test
    public void testPreconditionFailureSuggestsRangesOnlyWhenOmitted() throws Exception {
        var failure = new PulsarAdminException(null, "private owner precondition", 412);
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            for (boolean includeRanges : List.of(false, true)) {
                var metadata = new PartitionedTopicMetadata(2);
                var partial = PartitionedSubscriptionCursorStats.collect(TOPIC, metadata, includeRanges,
                        ignored -> CompletableFuture.completedFuture(name -> TopicName.getPartitionIndex(name) == 0
                                ? CompletableFuture.completedFuture(new CursorStats())
                                : CompletableFuture.failedFuture(failure)), executor, TIMEOUT)
                        .get(10, TimeUnit.SECONDS);
                var error = partial.errors.get(TOPIC.getPartition(1).toString());
                assertThat(error.statusCode).isEqualTo(412);
                assertThat(error.errorCode).isEqualTo("PRECONDITION_FAILED");
                assertRangeHint(error.reason, includeRanges);

                var unavailable = PartitionedSubscriptionCursorStats.collect(TOPIC, metadata, includeRanges,
                        ignored -> CompletableFuture.completedFuture(name -> CompletableFuture.failedFuture(failure)),
                        executor, TIMEOUT);
                assertThatThrownBy(unavailable::join).cause().isInstanceOfSatisfying(RestException.class, ex -> {
                    assertThat(ex.getResponse().getStatus()).isEqualTo(503);
                    assertRangeHint(ex.getMessage(), includeRanges);
                });
            }
        }
    }

    private static void assertRangeHint(String reason, boolean includeRanges) {
        assertThat(reason).doesNotContain("private");
        if (includeRanges) {
            assertThat(reason).doesNotContain("includeIndividualDeletedMessages");
        } else {
            assertThat(reason).contains("If the storage cursor only supports complete snapshots, retry with "
                    + "includeIndividualDeletedMessages=true");
        }
    }

    private static void assertPartitionError(PartitionedSubscriptionCursorInternalStats response, int partition,
                                             int expectedStatus, String expectedCode) {
        var error = response.errors.get(TOPIC.getPartition(partition).toString());
        assertThat(error.statusCode).isEqualTo(expectedStatus);
        assertThat(error.errorCode).isEqualTo(expectedCode);
        assertThat(error.reason).isNotBlank().doesNotContain("private");
    }
}
