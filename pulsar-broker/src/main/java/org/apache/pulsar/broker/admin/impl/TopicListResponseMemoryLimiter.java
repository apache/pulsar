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

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.core.Response.Status;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.topiclistlimit.TopicListMemoryLimiter;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
import org.apache.pulsar.broker.topiclistlimit.TopicListing;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterUtil;
import org.apache.pulsar.common.semaphore.AsyncSemaphore;
import org.apache.pulsar.common.util.FutureUtil;
import org.jspecify.annotations.Nullable;

/**
 * Applies the topic list heap memory limit (PIP-442) to REST topic listing requests.
 *
 * <p>The heap memory permits are held until the {@link AsyncResponse} has completed, since the topic list is
 * referenced until the response has been written. The request is treated as cancelled once the response is done
 * (for example, after the async response has timed out), so that a request that is no longer waited for doesn't
 * list the topics, and permits that are granted after the response has completed are released immediately.
 * A response can combine several listings, each of them holds its own permits until the response completes.
 */
public final class TopicListResponseMemoryLimiter {

    /**
     * The 503 response of a topic listing that was cancelled while it was waiting for permits, for example since
     * the response had already completed.
     */
    static final class RequestCancelledException extends RestException {
        private static final long serialVersionUID = 1L;

        RequestCancelledException() {
            super(Status.SERVICE_UNAVAILABLE, "Request was cancelled");
        }
    }

    /**
     * Checks whether a topic listing failed since it was cancelled while it was waiting for permits.
     *
     * @param ex the failure, possibly wrapped in a {@link CompletionException}
     * @return true if the request was cancelled
     */
    public static boolean isRequestCancelled(Throwable ex) {
        return FutureUtil.unwrapCompletionException(ex) instanceof RequestCancelledException;
    }
    private TopicListResponseMemoryLimiter() {
    }

    /**
     * Lists topics while holding heap memory permits for the estimated size of the topic list.
     *
     * @param brokerService the broker service
     * @param response the async response of the REST request, the permits are released when it completes
     * @param listing the topic listing, which identifies the topic list size estimate
     * @param properties the topic listing properties (PIP-452), null when not provided
     * @param topicsSupplier supplier that performs the topic listing
     * @return a future that completes with the topic list
     */
    public static CompletableFuture<List<String>> listTopics(BrokerService brokerService, AsyncResponse response,
                                                             TopicListing listing,
                                                             @Nullable Map<String, String> properties,
                                                             Supplier<CompletableFuture<List<String>>>
                                                                     topicsSupplier) {
        return listTopicsAfterCollecting(brokerService, response, listing, properties,
                () -> CompletableFuture.completedFuture(null), __ -> topicsSupplier.get());
    }

    /**
     * Lists topics while holding heap memory permits for the estimated size of the topic list, after collecting
     * part of the topics from other topic listings without holding permits.
     *
     * <p>Use this for a listing that combines topics from topic listings that apply the topic list memory limit
     * themselves, such as the bundle topic listings of the bundle owners, which can be this broker, with topics
     * that it lists itself. The size estimate is checked before collecting, so that a listing whose estimate is
     * larger than the limit fails without collecting. The permits are acquired once for the whole topic list
     * after collecting, so that the listing doesn't hold permits while waiting for the other listings.
     *
     * @param brokerService the broker service
     * @param response the async response of the REST request, the permits are released when it completes
     * @param listing the topic listing, which identifies the topic list size estimate
     * @param properties the topic listing properties (PIP-452), null when not provided
     * @param collector supplier that collects the topics from the other listings
     * @param topicsSupplier function that performs the topic listing, given the collected topics
     * @return a future that completes with the topic list
     */
    public static <CollectedT> CompletableFuture<List<String>> listTopicsAfterCollecting(
            BrokerService brokerService, AsyncResponse response, TopicListing listing,
            @Nullable Map<String, String> properties, Supplier<CompletableFuture<CollectedT>> collector,
            Function<CollectedT, CompletableFuture<List<String>>> topicsSupplier) {
        TopicListSizeResultCache sizeCache = brokerService.getTopicListSizeResultCache();
        TopicListSizeResultCache.ResultHolder listSizeHolder = sizeCache.getTopicListSize(listing, properties);
        AsyncDualMemoryLimiterImpl limiter = brokerService.getMaxTopicListInFlightLimiter();
        ResponsePermits permits = new ResponsePermits(limiter, response, listSizeHolder);
        response.register(permits);
        CompletableFuture<Long> sizeFuture = listSizeHolder.getSizeAsync();
        if (!sizeFuture.isDone()) {
            // The concurrent requests of a listing without a size estimate wait for the first request's estimate.
            // When it completes, on a metadata store thread, their continuations would run one after another on
            // that thread, so continue on the limiter's completion executor instead.
            sizeFuture = sizeFuture.thenApplyAsync(Function.identity(), limiter.getCompletionExecutor());
        }
        return sizeFuture
                .thenCompose(initialSize -> {
                    if (permits.isCancelled()) {
                        return CompletableFuture.failedFuture(ResponsePermits.cancelled());
                    }
                    if (initialSize > sizeCache.getSizeLimit()) {
                        // the permits can't be granted, skip collecting
                        return permits.acquire(initialSize).thenCompose(__ -> topicsSupplier.apply(null));
                    }
                    return collector.get().thenCompose(collected -> permits.acquire(initialSize)
                            .thenCompose(__ -> topicsSupplier.apply(collected)));
                })
                .thenCompose(topicList -> {
                    long actualSize = TopicListMemoryLimiter.estimateTopicListSize(topicList);
                    listSizeHolder.updateSize(actualSize);
                    return permits.update(actualSize).thenApply(___ -> topicList);
                })
                .whenComplete((__, t) -> {
                    if (t != null) {
                        // let concurrent requests waiting for the size estimate proceed
                        listSizeHolder.resetIfInitializing();
                    }
                });
    }

    /**
     * Lists topics by collecting them from other topic listings, and then holds heap memory permits for the
     * collected topic list until the response completes.
     *
     * <p>Use this for a listing that gets its topics from topic listings that apply the topic list memory limit
     * themselves, such as the bundle topic listings of the bundle owners, which can be this broker. Unlike
     * {@link #listTopics}, no permits are held while the topics are collected: the collected topic lists are
     * covered by the permits of the other listings until they have responded, and holding permits for an
     * estimate here could leave too few permits for a listing on this broker that this request waits for.
     *
     * @param brokerService the broker service
     * @param response the async response of the REST request, the permits are released when it completes
     * @param topicsSupplier supplier that collects the topics from the other listings
     * @return a future that completes with the topic list
     */
    public static CompletableFuture<List<String>> listTopicsFromOtherListings(
            BrokerService brokerService, AsyncResponse response,
            Supplier<CompletableFuture<List<String>>> topicsSupplier) {
        ResponsePermits permits = new ResponsePermits(brokerService.getMaxTopicListInFlightLimiter(), response, null);
        response.register(permits);
        if (permits.isCancelled()) {
            return CompletableFuture.failedFuture(ResponsePermits.cancelled());
        }
        return topicsSupplier.get().thenCompose(topicList ->
                permits.acquire(TopicListMemoryLimiter.estimateTopicListSize(topicList)).thenApply(__ -> topicList));
    }

    /**
     * Holds the heap memory permits of a single REST request until its {@link AsyncResponse} completes.
     */
    private static final class ResponsePermits implements CompletionCallback {
        private final AsyncDualMemoryLimiter limiter;
        private final AsyncResponse response;
        private final TopicListSizeResultCache.ResultHolder listSizeHolder;
        // guarded by this
        private boolean completed;
        // guarded by this
        private AsyncDualMemoryLimiterPermit permit;

        ResponsePermits(AsyncDualMemoryLimiter limiter, AsyncResponse response,
                        TopicListSizeResultCache.ResultHolder listSizeHolder) {
            this.limiter = limiter;
            this.response = response;
            this.listSizeHolder = listSizeHolder;
        }

        boolean isCancelled() {
            synchronized (this) {
                if (completed) {
                    return true;
                }
            }
            // the response is resumed only after the topic list has been retrieved, so a response that is
            // already done has been cancelled or has timed out
            return response.isDone();
        }

        CompletableFuture<Void> acquire(long size) {
            return holdOrFail(() -> limiter.acquire(size, AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY,
                    this::isCancelled));
        }

        CompletableFuture<Void> update(long size) {
            AsyncDualMemoryLimiterPermit currentPermit;
            synchronized (this) {
                currentPermit = permit;
            }
            if (currentPermit == null) {
                // the response has already completed and the permits have been released
                return CompletableFuture.failedFuture(cancelled());
            }
            return holdOrFail(() -> limiter.update(currentPermit, size, this::isCancelled));
        }

        private CompletableFuture<Void> holdOrFail(
                Supplier<CompletableFuture<AsyncDualMemoryLimiterPermit>> permitFutureSupplier) {
            return AsyncDualMemoryLimiterUtil.requestPermits(permitFutureSupplier).handle((acquiredPermit, t) -> {
                if (t != null) {
                    String message = FutureUtil.unwrapCompletionException(t)
                            instanceof AsyncSemaphore.PermitAcquireLimitExceededException
                            ? "Failed since the topic list is larger than the topic list heap memory limit"
                            : "Failed due to heap memory limit exceeded";
                    throw new CompletionException(new RestException(Status.TOO_MANY_REQUESTS, message));
                }
                hold(acquiredPermit);
                // checked after storing the permit: if the response completes concurrently, either onComplete or
                // this check releases the permit
                if (isCancelled()) {
                    // don't continue processing a request that has been cancelled while waiting for the permits
                    releaseHeldPermit();
                    throw cancelled();
                }
                return null;
            });
        }

        private static CompletionException cancelled() {
            return new CompletionException(new RequestCancelledException());
        }

        /**
         * Stores the permit so that it gets released when the response completes.
         */
        private void hold(AsyncDualMemoryLimiterPermit newPermit) {
            AsyncDualMemoryLimiterPermit previousPermit;
            synchronized (this) {
                previousPermit = permit;
                permit = newPermit;
            }
            if (previousPermit != null && previousPermit != newPermit) {
                // after a permit update the previous permit has no remaining permits and releasing it is a no-op,
                // it's released in case of an unexpected state to avoid leaking permits
                limiter.release(previousPermit);
            }
        }

        private void releaseHeldPermit() {
            AsyncDualMemoryLimiterPermit permitToRelease;
            synchronized (this) {
                permitToRelease = permit;
                permit = null;
            }
            if (permitToRelease != null) {
                limiter.release(permitToRelease);
            }
        }

        @Override
        public void onComplete(Throwable throwable) {
            if (throwable != null && listSizeHolder != null) {
                listSizeHolder.resetIfInitializing();
            }
            synchronized (this) {
                completed = true;
            }
            releaseHeldPermit();
        }
    }
}
