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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import com.fasterxml.jackson.core.JsonParseException;
import io.netty.channel.ConnectTimeoutException;
import java.net.ConnectException;
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
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.MaxRedirectException;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.RetryException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
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
            var response = PartitionedSubscriptionCursorStats.collect(TOPIC, new PartitionedTopicMetadata(32), name -> {
                var request = requests.size() == 1 ? uncancellable : new CompletableFuture<CursorStats>();
                partitions.add(name);
                requests.add(request);
                return request;
            }, executor, TIMEOUT);

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
    public void testPartialResponseClassifiesAndSanitizesFailures() throws Exception {
        Map<Integer, String> statuses = Map.ofEntries(
                Map.entry(401, "AUTHORIZATION_FAILED"), Map.entry(403, "AUTHORIZATION_FAILED"),
                Map.entry(404, "NOT_FOUND"), Map.entry(408, "TIMEOUT"), Map.entry(504, "TIMEOUT"),
                Map.entry(412, "PRECONDITION_FAILED"), Map.entry(307, "UNAVAILABLE"), Map.entry(429, "UNAVAILABLE"),
                Map.entry(502, "UNAVAILABLE"), Map.entry(503, "UNAVAILABLE"), Map.entry(409, "INTERNAL_ERROR"));
        var remoteStatuses = new ArrayList<>(statuses.keySet());
        List<Throwable> localErrors = List.of(
                new PulsarAdminException(new TimeoutException("private upstream timeout")),
                new PulsarAdminException(new RetryException("private retry",
                        new ConnectTimeoutException("private connection timeout"))),
                new PulsarAdminException(new ConnectException("private internal address")),
                new PulsarAdminException(new MaxRedirectException("private internal URL")),
                new PulsarAdminException(new JsonParseException(null, "private response data")),
                new IllegalStateException("private coordinator details"));
        List<String> localCodes = List.of(
                "TIMEOUT", "TIMEOUT", "UNAVAILABLE", "UNAVAILABLE", "INTERNAL_ERROR", "INTERNAL_ERROR");
        int localOffset = remoteStatuses.size() + 1;
        int count = localOffset + localErrors.size();
        try (var executor = Executors.newScheduledThreadPool(2)) {
            var response = PartitionedSubscriptionCursorStats.collect(TOPIC, new PartitionedTopicMetadata(count),
                    name -> {
                        int partition = TopicName.getPartitionIndex(name);
                        if (partition == 0) {
                            return CompletableFuture.completedFuture(new CursorStats());
                        }
                        if (partition < localOffset) {
                            return CompletableFuture.failedFuture(new PulsarAdminException(
                                    null, "private upstream response", remoteStatuses.get(partition - 1)));
                        }
                        return CompletableFuture.failedFuture(localErrors.get(partition - localOffset));
                    }, executor, TIMEOUT).get(10, TimeUnit.SECONDS);
            assertThat(response.complete).isFalse();
            assertThat(response.partitions).containsOnlyKeys(TOPIC.getPartition(0).toString());
            assertThat(response.errors).hasSize(count - 1);
            for (int i = 0; i < remoteStatuses.size(); i++) {
                int status = remoteStatuses.get(i);
                assertPartitionError(response, i + 1, status, statuses.get(status));
            }
            for (int i = 0; i < localCodes.size(); i++) {
                String code = localCodes.get(i);
                assertPartitionError(response, localOffset + i, code.equals("INTERNAL_ERROR") ? 500 : 503, code);
            }
        }
    }

    @Test
    public void testNoSuccessProducesTopLevelError() {
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            var notFound = new PulsarAdminException(null, "private upstream response", 404);
            var metadata = new PartitionedTopicMetadata(2);
            var allMissing = PartitionedSubscriptionCursorStats.collect(TOPIC, metadata,
                    name -> CompletableFuture.failedFuture(notFound), executor, TIMEOUT);
            assertThatThrownBy(allMissing::join).hasCauseInstanceOf(RestException.class)
                    .cause().isInstanceOfSatisfying(RestException.class,
                            error -> assertThat(error.getResponse().getStatus()).isEqualTo(404));

            var mixed = PartitionedSubscriptionCursorStats.collect(TOPIC, metadata,
                    name -> CompletableFuture.failedFuture(TopicName.getPartitionIndex(name) == 0
                            ? notFound : new TimeoutException()), executor, TIMEOUT);
            assertThatThrownBy(mixed::join).hasCauseInstanceOf(RestException.class)
                    .cause().isInstanceOfSatisfying(RestException.class,
                            error -> assertThat(error.getResponse().getStatus()).isEqualTo(503));
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
