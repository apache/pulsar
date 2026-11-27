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
import io.netty.handler.ssl.SslHandshakeTimeoutException;
import jakarta.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.net.ssl.SSLException;
import lombok.CustomLog;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.MaxRedirectException;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector.RetryException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.PulsarClientException.ConnectException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats.PartitionError;
import org.apache.pulsar.common.util.FutureUtil;

@CustomLog
final class PartitionedSubscriptionCursorStats {
    private static final int MAX_CONCURRENT_REQUESTS = 16;

    private final TopicName topic;
    private final PartitionedTopicMetadata metadata;
    private final boolean includeIndividualDeletedMessages;
    private final ScheduledExecutorService executor;
    private final long deadlineNanos;
    private final CompletableFuture<Void> completion;
    private final AtomicInteger nextPartition = new AtomicInteger();
    private final AtomicBoolean loggedLocalFailure = new AtomicBoolean();
    private final Map<Integer, CompletableFuture<CursorStats>> inFlight = new HashMap<>();
    private PartitionResult[] results;

    private record PartitionResult(CursorStats stats, PartitionError error) {
    }

    private PartitionedSubscriptionCursorStats(TopicName topic, PartitionedTopicMetadata metadata,
                                               boolean includeIndividualDeletedMessages,
                                               ScheduledExecutorService executor, Duration timeout) {
        this.topic = topic;
        this.metadata = metadata;
        this.includeIndividualDeletedMessages = includeIndividualDeletedMessages;
        this.executor = executor;
        this.results = new PartitionResult[metadata.partitions];
        this.deadlineNanos = System.nanoTime() + timeout.toNanos();
        this.completion = FutureUtil.createFutureWithTimeout(timeout, executor, TimeoutException::new);
    }

    static CompletableFuture<PartitionedSubscriptionCursorInternalStats> collect(
            TopicName topic, PartitionedTopicMetadata metadata, boolean includeIndividualDeletedMessages,
            Function<CompletableFuture<Void>, CompletableFuture<Function<String, CompletableFuture<CursorStats>>>>
                    prepareRequest,
            ScheduledExecutorService executor, Duration timeout) {
        return FutureUtil.supplySafely(() -> {
            var collection = new PartitionedSubscriptionCursorStats(topic, metadata, includeIndividualDeletedMessages,
                    executor, timeout);
            var request = FutureUtil.supplySafely(() -> prepareRequest.apply(collection.completion));
            return collection.collect(request);
        });
    }

    private CompletableFuture<PartitionedSubscriptionCursorInternalStats> collect(
            CompletableFuture<Function<String, CompletableFuture<CursorStats>>> request) {
        // The same deadline covers getting the internal client and collecting the partition responses.
        FutureUtil.completeAfter(completion, request.thenCompose(this::requestPartitions));
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
            Throwable cause = FutureUtil.unwrapCompletionException(error);
            if (cause != null && !(cause instanceof TimeoutException)) {
                throw FutureUtil.wrapToCompletionException(cause);
            }
            return buildResponse(snapshot);
        });
    }

    private CompletableFuture<Void> requestPartitions(Function<String, CompletableFuture<CursorStats>> request) {
        if (isFinished()) {
            return CompletableFuture.completedFuture(null);
        }
        var workers = new CompletableFuture<?>[Math.min(MAX_CONCURRENT_REQUESTS, metadata.partitions)];
        for (int worker = 0; worker < workers.length; worker++) {
            workers[worker] = requestNextAsync(request);
        }
        return CompletableFuture.allOf(workers);
    }

    private boolean isFinished() {
        return completion.isDone() || System.nanoTime() - deadlineNanos >= 0;
    }

    private CompletableFuture<Void> requestNextAsync(Function<String, CompletableFuture<CursorStats>> request) {
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
                    recordResult(index, partitionResult(partition, stats, error));
                }
                return null;
            }).thenCompose(ignored -> isFinished() ? CompletableFuture.completedFuture(null)
                    : requestNextAsync(request));
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
        if (Arrays.stream(snapshot).noneMatch(result -> result != null && result.stats() != null)) {
            if (Arrays.stream(snapshot).allMatch(result -> result != null && result.error().statusCode == 404)) {
                throw new RestException(Status.NOT_FOUND, "Subscription cursor not found on any partition");
            }
            boolean preconditionFailed = Arrays.stream(snapshot)
                    .anyMatch(result -> result != null && result.error().statusCode == 412);
            String reason = "Could not retrieve any partition cursor stats";
            throw new RestException(Status.SERVICE_UNAVAILABLE, preconditionFailed ? withRangeHint(reason) : reason);
        }

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
        return new PartitionedSubscriptionCursorInternalStats(metadata, partitions, errors);
    }

    private PartitionResult partitionResult(String partition, CursorStats stats, Throwable error) {
        if (error == null && stats != null) {
            return new PartitionResult(stats, null);
        }
        return new PartitionResult(null, partitionError(partition, error));
    }

    private PartitionError partitionError(String partition, Throwable error) {
        Throwable cause = FutureUtil.unwrapCompletionException(error);
        if (cause instanceof PulsarAdminException adminException) {
            int status = adminException.getStatusCode();
            // Admin exceptions for failures without a response use a synthetic 500.
            if (status != 500 || adminException.getHttpError() != null) {
                return errorForStatus(status);
            }
        }
        // Before the partition GET, IOException can mean a bad local credential file or a plugin rejection.
        // Report a transport failure at that stage only if the plugin supplied a recognizable cause.
        boolean preparingCredentials = cause instanceof PulsarAdminException.GettingAuthenticationDataException;
        boolean transportFailure = false;
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current instanceof JsonProcessingException) {
                return localInternalError(partition, cause);
            }
            if (current instanceof PulsarAdminException.TimeoutException || current instanceof TimeoutException
                    || current instanceof PulsarClientException.TimeoutException
                    || current instanceof org.apache.pulsar.client.api.v5.PulsarClientException.TimeoutException
                    || current instanceof SocketTimeoutException || current instanceof ConnectTimeoutException
                    || current instanceof SslHandshakeTimeoutException) {
                return new PartitionError(503, "TIMEOUT", "Partition request timed out");
            }
            transportFailure |= current instanceof PulsarAdminException.ConnectException
                    || current instanceof PulsarClientException.ConnectException || current instanceof ConnectException
                    || current instanceof SocketException || current instanceof UnknownHostException
                    || current instanceof SSLException || current instanceof UnresolvedAddressException
                    || current instanceof MaxRedirectException || current instanceof RetryException
                    || current instanceof CancellationException || (!preparingCredentials
                            && current instanceof IOException);
        }
        return transportFailure ? errorForStatus(503) : localInternalError(partition, cause);
    }

    private PartitionError localInternalError(String partition, Throwable cause) {
        // A shared credential or decoding failure can affect every partition in the request.
        if (cause != null && loggedLocalFailure.compareAndSet(false, true)) {
            log.warn().attr("partition", partition).exception(cause).log("Failed to get partition cursor stats");
        }
        return errorForStatus(500);
    }

    private String withRangeHint(String reason) {
        return includeIndividualDeletedMessages ? reason : reason
                + ". If the storage cursor only supports complete snapshots, retry with "
                + PersistentTopicsBase.INCLUDE_RANGES_OPTION;
    }

    private PartitionError errorForStatus(int status) {
        return switch (status) {
            case 401, 403 -> new PartitionError(status, "AUTHORIZATION_FAILED", "Partition access denied");
            case 404 -> new PartitionError(status, "NOT_FOUND", "Partition topic or subscription not found");
            case 408, 504 -> new PartitionError(status, "TIMEOUT", "Partition request timed out");
            case 412 -> new PartitionError(status, "PRECONDITION_FAILED",
                    withRangeHint("Partition request precondition failed"));
            case 301, 302, 303, 307, 308, 429, 502, 503 ->
                    new PartitionError(status, "UNAVAILABLE", "Partition is unavailable");
            default -> new PartitionError(status, "INTERNAL_ERROR", "Partition cursor stats request failed");
        };
    }
}
