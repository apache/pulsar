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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.channel.ConnectTimeoutException;
import jakarta.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.MaxRedirectException;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.RetryException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats.PartitionError;
import org.apache.pulsar.common.util.FutureUtil;

final class PartitionedSubscriptionCursorStats {
    private static final int MAX_CONCURRENT_REQUESTS = 16;

    private final TopicName topic;
    private final PartitionedTopicMetadata metadata;
    private final Function<String, CompletableFuture<CursorStats>> request;
    private final ScheduledExecutorService executor;
    private final long deadlineNanos;
    private final CompletableFuture<Void> completion;
    private final AtomicInteger nextPartition = new AtomicInteger();
    private final Map<Integer, CompletableFuture<CursorStats>> inFlight = new HashMap<>();
    private PartitionResult[] results;

    private record PartitionResult(CursorStats stats, PartitionError error) {
    }

    private PartitionedSubscriptionCursorStats(TopicName topic, PartitionedTopicMetadata metadata,
                                               Function<String, CompletableFuture<CursorStats>> request,
                                               ScheduledExecutorService executor, Duration timeout) {
        this.topic = topic;
        this.metadata = metadata;
        this.request = request;
        this.executor = executor;
        this.results = new PartitionResult[metadata.partitions];
        this.deadlineNanos = System.nanoTime() + timeout.toNanos();
        this.completion = FutureUtil.createFutureWithTimeout(timeout, executor, TimeoutException::new);
    }

    static CompletableFuture<PartitionedSubscriptionCursorInternalStats> collect(
            TopicName topic, PartitionedTopicMetadata metadata,
            Function<String, CompletableFuture<CursorStats>> request,
            ScheduledExecutorService executor, Duration timeout) {
        return FutureUtil.supplySafely(() -> {
            var collection = new PartitionedSubscriptionCursorStats(topic, metadata, request, executor, timeout);
            return collection.collect();
        });
    }

    private CompletableFuture<PartitionedSubscriptionCursorInternalStats> collect() {
        var workers = new CompletableFuture<?>[Math.min(MAX_CONCURRENT_REQUESTS, metadata.partitions)];
        for (int worker = 0; worker < workers.length; worker++) {
            workers[worker] = requestNextAsync();
        }
        CompletableFuture.allOf(workers).whenComplete((unused, error) -> {
            if (error == null) {
                completion.complete(null);
            } else {
                completion.completeExceptionally(error);
            }
        });
        return completion.handle((unused, error) -> {
            PartitionResult[] snapshot;
            List<CompletableFuture<CursorStats>> outstanding;
            synchronized (this) {
                snapshot = results;
                results = null;
                outstanding = List.copyOf(inFlight.values());
                inFlight.clear();
            }
            // Pending auth and HTTP callbacks need not retain already collected cursor data.
            outstanding.forEach(future -> future.cancel(false));
            if (error != null && !(error instanceof TimeoutException)) {
                throw new RestException(Status.INTERNAL_SERVER_ERROR, "Failed to collect partition cursor stats");
            }
            return buildResponse(snapshot);
        });
    }

    private boolean isFinished() {
        return completion.isDone() || System.nanoTime() - deadlineNanos >= 0;
    }

    private CompletableFuture<Void> requestNextAsync() {
        return CompletableFuture.completedFuture(null).thenComposeAsync(unused -> {
            int index = nextPartition.getAndIncrement();
            if (index >= metadata.partitions || isFinished()) {
                return CompletableFuture.completedFuture(null);
            }
            String partition = topic.getPartition(index).toString();
            CompletableFuture<CursorStats> future = FutureUtil.supplySafely(() -> request.apply(partition));
            if (!trackRequest(index, future)) {
                future.cancel(false);
                return CompletableFuture.completedFuture(null);
            }
            return future.handle((stats, error) -> {
                if (!isFinished()) {
                    recordResult(index, partitionResult(stats, error));
                }
                return null;
            }).thenCompose(ignored -> isFinished() ? CompletableFuture.completedFuture(null) : requestNextAsync());
        }, executor);
    }

    private synchronized boolean trackRequest(int index, CompletableFuture<CursorStats> future) {
        if (isFinished()) {
            return false;
        }
        inFlight.put(index, future);
        return true;
    }

    private synchronized void recordResult(int index, PartitionResult result) {
        inFlight.remove(index);
        if (!isFinished()) {
            results[index] = result;
        }
    }

    private PartitionedSubscriptionCursorInternalStats buildResponse(PartitionResult[] snapshot) {
        Map<String, CursorStats> partitions = new HashMap<>();
        Map<String, PartitionError> errors = new HashMap<>();
        for (int i = 0; i < snapshot.length; i++) {
            String partition = topic.getPartition(i).toString();
            PartitionResult result = snapshot[i];
            if (result == null) {
                errors.put(partition, new PartitionError(503, "TIMEOUT", "Partition request timed out"));
            } else if (result.error() != null) {
                errors.put(partition, result.error());
            } else {
                partitions.put(partition, result.stats());
            }
        }
        if (partitions.isEmpty()) {
            if (errors.values().stream().allMatch(error -> error.statusCode == 404)) {
                throw new RestException(Status.NOT_FOUND, "Subscription cursor not found on any partition");
            }
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Could not retrieve any partition cursor stats");
        }
        return new PartitionedSubscriptionCursorInternalStats(metadata, partitions, errors);
    }

    private static PartitionResult partitionResult(CursorStats stats, Throwable error) {
        if (error == null && stats != null) {
            return new PartitionResult(stats, null);
        }
        return new PartitionResult(null, partitionError(error));
    }

    private static PartitionError partitionError(Throwable error) {
        Throwable cause = FutureUtil.unwrapCompletionException(error);
        if (cause instanceof PulsarAdminException adminException) {
            int status = adminException.getStatusCode();
            // Admin exceptions for failures without a response use a synthetic 500.
            if (status != 500 || adminException.getHttpError() != null) {
                return errorForStatus(status);
            }
        }
        boolean transportFailure = false;
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current instanceof JsonProcessingException) {
                return errorForStatus(500);
            }
            if (current instanceof PulsarAdminException.TimeoutException || current instanceof TimeoutException
                    || current instanceof SocketTimeoutException || current instanceof ConnectTimeoutException) {
                return new PartitionError(503, "TIMEOUT", "Partition request timed out");
            }
            transportFailure |= current instanceof PulsarAdminException.ConnectException
                    || current instanceof IOException || current instanceof UnresolvedAddressException
                    || current instanceof MaxRedirectException || current instanceof RetryException
                    || current instanceof CancellationException;
        }
        return errorForStatus(transportFailure ? 503 : 500);
    }

    private static PartitionError errorForStatus(int status) {
        return switch (status) {
            case 401, 403 -> new PartitionError(status, "AUTHORIZATION_FAILED", "Partition access denied");
            case 404 -> new PartitionError(status, "NOT_FOUND", "Partition topic or subscription not found");
            case 408, 504 -> new PartitionError(status, "TIMEOUT", "Partition request timed out");
            case 412 -> new PartitionError(status, "PRECONDITION_FAILED", "Partition request precondition failed");
            case 301, 302, 303, 307, 308, 429, 502, 503 ->
                    new PartitionError(status, "UNAVAILABLE", "Partition is unavailable");
            default -> new PartitionError(status, "INTERNAL_ERROR", "Partition cursor stats request failed");
        };
    }
}
