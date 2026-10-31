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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.topiclistlimit.TopicListMemoryLimiter;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
import org.apache.pulsar.broker.topiclistlimit.TopicListing;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.LimitType;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicListResponseMemoryLimiterTest {
    private static final String NAMESPACE = "tenant/ns";
    private static final long MAX_HEAP_MEMORY = 100_000;
    private static final List<String> TOPICS =
            List.of("persistent://tenant/ns/topic-1", "persistent://tenant/ns/topic-2");

    private AsyncDualMemoryLimiterImpl limiter;
    private TopicListSizeResultCache sizeCache;
    private BrokerService brokerService;
    private AsyncResponse response;
    private AtomicBoolean responseDone;
    private AtomicReference<CompletionCallback> completionCallback;

    @BeforeMethod
    public void setup() {
        limiter = new AsyncDualMemoryLimiterImpl(MAX_HEAP_MEMORY, 10, 5000, MAX_HEAP_MEMORY, 10, 5000);
        sizeCache = new TopicListSizeResultCache();
        brokerService = mock(BrokerService.class);
        when(brokerService.getMaxTopicListInFlightLimiter()).thenAnswer(__ -> limiter);
        when(brokerService.getTopicListSizeResultCache()).thenReturn(sizeCache);
        responseDone = new AtomicBoolean();
        completionCallback = new AtomicReference<>();
        response = mock(AsyncResponse.class);
        when(response.isDone()).thenAnswer(__ -> responseDone.get());
        doAnswer(invocation -> {
            completionCallback.set(invocation.getArgument(0));
            return List.of();
        }).when(response).register(any(Object.class));
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (limiter != null) {
            limiter.close();
            limiter = null;
        }
    }

    private long acquiredHeapPermits() {
        return limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits();
    }

    private void completeResponse(Throwable throwable) {
        responseDone.set(true);
        completionCallback.get().onComplete(throwable);
    }

    private CompletableFuture<List<String>> listTopics(Supplier<CompletableFuture<List<String>>> topicsSupplier) {
        return TopicListResponseMemoryLimiter.listTopics(brokerService, response,
                TopicListing.namespaceTopics(NAMESPACE, Mode.PERSISTENT), null, topicsSupplier);
    }

    @Test
    public void testPermitsAreHeldUntilResponseCompletes() {
        CompletableFuture<List<String>> result = listTopics(() -> CompletableFuture.completedFuture(TOPICS));

        assertThat(result).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(TOPICS);
        // the topic list is referenced until the response has been written
        assertThat(acquiredHeapPermits()).isEqualTo(TopicListMemoryLimiter.estimateTopicListSize(TOPICS));

        completeResponse(null);
        assertThat(acquiredHeapPermits()).isZero();
    }

    @Test
    public void testRequestsWaitingForTheSizeEstimateContinueOnTheCompletionExecutor() {
        // the first request lists the topics without a size estimate, the concurrent requests wait for its estimate
        CompletableFuture<List<String>> firstListing = new CompletableFuture<>();
        CompletableFuture<List<String>> first = listTopics(() -> firstListing);
        List<CompletableFuture<String>> listingThreads = new ArrayList<>();
        List<CompletableFuture<List<String>>> waiting = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            CompletableFuture<String> listingThread = new CompletableFuture<>();
            listingThreads.add(listingThread);
            waiting.add(listTopics(() -> {
                listingThread.complete(Thread.currentThread().getName());
                return CompletableFuture.completedFuture(TOPICS);
            }));
        }

        // completing the first listing, here on the test thread, completes the size estimate
        firstListing.complete(TOPICS);

        assertThat(first).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(TOPICS);
        String testThread = Thread.currentThread().getName();
        for (int i = 0; i < waiting.size(); i++) {
            assertThat(waiting.get(i)).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(TOPICS);
            // the waiting requests don't continue one after another on the thread that completed the estimate
            assertThat(listingThreads.get(i)).isCompleted();
            assertThat(listingThreads.get(i).join()).isNotEqualTo(testThread)
                    .startsWith("async-dual-memory-limiter-completion");
        }
    }

    @Test
    public void testDoesNotListTopicsWhenResponseCompletesWhileWaitingForTheCompletionExecutor() {
        // gate the completion executor so that the continuation of a request waiting for the size estimate stays
        // queued until the test runs it
        Queue<Runnable> queuedTasks = new ConcurrentLinkedQueue<>();
        AsyncDualMemoryLimiterImpl gatedLimiter = spy(limiter);
        doReturn((Executor) queuedTasks::add).when(gatedLimiter).getCompletionExecutor();
        when(brokerService.getMaxTopicListInFlightLimiter()).thenReturn(gatedLimiter);

        // the first request lists the topics without a size estimate, the second request waits for its estimate
        CompletableFuture<List<String>> firstListing = new CompletableFuture<>();
        CompletableFuture<List<String>> first = listTopics(() -> firstListing);
        CompletionCallback firstCompletionCallback = completionCallback.get();
        AsyncResponse waitingResponse = mock(AsyncResponse.class);
        AtomicBoolean waitingResponseDone = new AtomicBoolean();
        AtomicReference<CompletionCallback> waitingCompletionCallback = new AtomicReference<>();
        when(waitingResponse.isDone()).thenAnswer(__ -> waitingResponseDone.get());
        doAnswer(invocation -> {
            waitingCompletionCallback.set(invocation.getArgument(0));
            return List.of();
        }).when(waitingResponse).register(any(Object.class));
        AtomicInteger listingCalls = new AtomicInteger();
        CompletableFuture<List<String>> waiting = TopicListResponseMemoryLimiter.listTopics(brokerService,
                waitingResponse, TopicListing.namespaceTopics(NAMESPACE, Mode.PERSISTENT), null, () -> {
                    listingCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(TOPICS);
                });

        // completing the first listing completes the size estimate and queues the waiting request's continuation
        firstListing.complete(TOPICS);
        assertThat(first).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(TOPICS);
        assertThat(queuedTasks).hasSize(1);
        assertThat(waiting).isNotDone();

        // the waiting request's response times out before its continuation runs
        waitingResponseDone.set(true);
        waitingCompletionCallback.get().onComplete(null);
        Runnable task;
        while ((task = queuedTasks.poll()) != null) {
            task.run();
        }

        assertThat(waiting).failsWithin(Duration.ofSeconds(5));
        assertThat(listingCalls).hasValue(0);
        // only the first request holds permits until its response completes
        assertThat(acquiredHeapPermits()).isEqualTo(TopicListMemoryLimiter.estimateTopicListSize(TOPICS));
        responseDone.set(true);
        firstCompletionCallback.onComplete(null);
        assertThat(acquiredHeapPermits()).isZero();
    }

    @Test
    public void testDoesNotListTopicsWhenResponseIsAlreadyDone() {
        responseDone.set(true);
        AtomicInteger listingCalls = new AtomicInteger();

        CompletableFuture<List<String>> result = listTopics(() -> {
            listingCalls.incrementAndGet();
            return CompletableFuture.completedFuture(TOPICS);
        });

        assertThat(result).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .havingRootCause().isInstanceOf(RestException.class);
        assertThat(listingCalls).hasValue(0);
        assertThat(acquiredHeapPermits()).isZero();
        // concurrent requests waiting for the size estimate must not be left waiting
        assertThat(sizeCache.getTopicListSize(NAMESPACE, Mode.PERSISTENT).getSizeAsync()).isDone();
    }

    @Test
    public void testDoesNotListTopicsWhenResponseTimesOutWhileWaitingForPermits() throws Exception {
        // occupy all permits so that the request waits in the queue
        AsyncDualMemoryLimiterPermit blockingPermit =
                limiter.acquire(MAX_HEAP_MEMORY, LimitType.HEAP_MEMORY, () -> false).get(5, TimeUnit.SECONDS);
        AtomicInteger listingCalls = new AtomicInteger();
        CompletableFuture<List<String>> result = listTopics(() -> {
            listingCalls.incrementAndGet();
            return CompletableFuture.completedFuture(TOPICS);
        });
        Awaitility.await().until(() -> limiter.getLimiter(LimitType.HEAP_MEMORY).getQueueSize() == 1);

        // an async response timeout completes the response with a mapped response, without a throwable
        completeResponse(null);
        limiter.release(blockingPermit);

        assertThat(result).failsWithin(Duration.ofSeconds(5));
        assertThat(listingCalls).hasValue(0);
        assertThat(acquiredHeapPermits()).isZero();
    }

    @Test
    public void testPermitsAreReleasedWhenResponseCompletesDuringListing() {
        CompletableFuture<List<String>> result = listTopics(() -> {
            assertThat(acquiredHeapPermits()).isPositive();
            completeResponse(null);
            return CompletableFuture.completedFuture(TOPICS);
        });

        assertThat(result).failsWithin(Duration.ofSeconds(5));
        assertThat(acquiredHeapPermits()).isZero();
    }

    @Test
    public void testPermitGrantedAfterResponseCompletionIsReleased() {
        // simulate a permit grant that races with the completion of the response
        AsyncDualMemoryLimiterImpl mockedLimiter = mock(AsyncDualMemoryLimiterImpl.class);
        AsyncDualMemoryLimiterPermit permit = mock(AsyncDualMemoryLimiterPermit.class);
        when(mockedLimiter.acquire(anyLong(), any(), any())).thenAnswer(__ -> {
            completionCallback.get().onComplete(null);
            return CompletableFuture.completedFuture(permit);
        });
        when(brokerService.getMaxTopicListInFlightLimiter()).thenReturn(mockedLimiter);
        AtomicInteger listingCalls = new AtomicInteger();

        CompletableFuture<List<String>> result = listTopics(() -> {
            listingCalls.incrementAndGet();
            return CompletableFuture.completedFuture(TOPICS);
        });

        assertThat(result).failsWithin(Duration.ofSeconds(5));
        assertThat(listingCalls).hasValue(0);
        verify(mockedLimiter, times(1)).release(permit);
    }

    @Test
    public void testPermitGrantedAfterResponseIsDoneIsReleased() {
        // simulate a permit grant that races with the cancellation of the response, before the completion
        // callback has been called
        AsyncDualMemoryLimiterImpl mockedLimiter = mock(AsyncDualMemoryLimiterImpl.class);
        AsyncDualMemoryLimiterPermit permit = mock(AsyncDualMemoryLimiterPermit.class);
        when(mockedLimiter.acquire(anyLong(), any(), any())).thenAnswer(__ -> {
            responseDone.set(true);
            return CompletableFuture.completedFuture(permit);
        });
        when(brokerService.getMaxTopicListInFlightLimiter()).thenReturn(mockedLimiter);
        AtomicInteger listingCalls = new AtomicInteger();

        CompletableFuture<List<String>> result = listTopics(() -> {
            listingCalls.incrementAndGet();
            return CompletableFuture.completedFuture(TOPICS);
        });

        assertThat(result).failsWithin(Duration.ofSeconds(5));
        assertThat(listingCalls).hasValue(0);
        verify(mockedLimiter, times(1)).release(permit);
        // the cancellation is recognized as the limiter's own 503, unlike other 503 responses
        Throwable failure = result.handle((__, t) -> t).join();
        assertThat(TopicListResponseMemoryLimiter.isRequestCancelled(failure)).isTrue();
        assertThat(TopicListResponseMemoryLimiter.isRequestCancelled(
                new RestException(Response.Status.SERVICE_UNAVAILABLE, "not ready"))).isFalse();
    }

    @Test
    public void testTopicListLargerThanLimitFailsWithTooManyRequests() {
        List<String> hugeTopicList = List.of("persistent://tenant/ns/" + "t".repeat((int) MAX_HEAP_MEMORY));

        // the limiter rejects the update to the actual size synchronously
        CompletableFuture<List<String>> result =
                listTopics(() -> CompletableFuture.completedFuture(hugeTopicList));
        assertThat(result).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .havingRootCause().isInstanceOfSatisfying(RestException.class,
                        e -> assertThat(e.getResponse().getStatus()).isEqualTo(429));
        completeResponse(new RuntimeException("failed"));
        assertThat(acquiredHeapPermits()).isZero();

        // the cached estimate is now larger than the limit and the initial acquisition is rejected synchronously
        responseDone.set(false);
        CompletableFuture<List<String>> result2 = listTopics(() -> CompletableFuture.completedFuture(TOPICS));
        assertThat(result2).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .havingRootCause().isInstanceOfSatisfying(RestException.class,
                        e -> assertThat(e.getResponse().getStatus()).isEqualTo(429));
    }

    @Test
    public void testPermitAcquireFailureFailsWithTooManyRequests() throws Exception {
        limiter.close();
        // a queue size of 1 with a queued request causes the next request to fail
        limiter = new AsyncDualMemoryLimiterImpl(MAX_HEAP_MEMORY, 1, 5000, MAX_HEAP_MEMORY, 1, 5000);
        AsyncDualMemoryLimiterPermit blockingPermit =
                limiter.acquire(MAX_HEAP_MEMORY, LimitType.HEAP_MEMORY, () -> false).get(5, TimeUnit.SECONDS);
        CompletableFuture<AsyncDualMemoryLimiterPermit> queued =
                limiter.acquire(1, LimitType.HEAP_MEMORY, () -> false);

        CompletableFuture<List<String>> result = listTopics(() -> CompletableFuture.completedFuture(TOPICS));

        assertThat(result).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .havingRootCause().isInstanceOfSatisfying(RestException.class,
                        e -> assertThat(e.getResponse().getStatus()).isEqualTo(429));
        limiter.release(blockingPermit);
        limiter.release(queued.get(5, TimeUnit.SECONDS));
        assertThat(acquiredHeapPermits()).isZero();
    }

    @Test
    public void testListTopicsFromOtherListingsHoldsPermitsOnlyAfterListing() {
        AtomicLong permitsDuringListing = new AtomicLong(-1);
        CompletableFuture<List<String>> result = TopicListResponseMemoryLimiter.listTopicsFromOtherListings(
                brokerService, response, () -> {
                    permitsDuringListing.set(acquiredHeapPermits());
                    return CompletableFuture.completedFuture(TOPICS);
                });

        assertThat(result).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(TOPICS);
        // no permits are held while the topics are collected from the other listings
        assertThat(permitsDuringListing).hasValue(0);
        // permits for the collected list are held until the response has been written
        assertThat(acquiredHeapPermits()).isEqualTo(TopicListMemoryLimiter.estimateTopicListSize(TOPICS));
        completeResponse(null);
        assertThat(acquiredHeapPermits()).isZero();
    }

    @Test
    public void testListTopicsFromOtherListingsSkipsListingWhenResponseIsAlreadyDone() {
        responseDone.set(true);
        AtomicInteger listingCalls = new AtomicInteger();

        CompletableFuture<List<String>> result = TopicListResponseMemoryLimiter.listTopicsFromOtherListings(
                brokerService, response, () -> {
                    listingCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(TOPICS);
                });

        assertThat(result).failsWithin(Duration.ofSeconds(5));
        assertThat(listingCalls).hasValue(0);
        assertThat(acquiredHeapPermits()).isZero();
    }

    @Test
    public void testOversizedListingFailsWithoutWaitingForPermits() throws Exception {
        sizeCache = new TopicListSizeResultCache(MAX_HEAP_MEMORY, () -> true);
        when(brokerService.getTopicListSizeResultCache()).thenReturn(sizeCache);
        TopicListSizeResultCache.ResultHolder holder =
                sizeCache.getTopicListSize(NAMESPACE, Mode.PERSISTENT);
        // a previous listing measured a topic list larger than the limit
        holder.updateSize(MAX_HEAP_MEMORY * 2);
        // another topic listing is in flight
        AsyncDualMemoryLimiterPermit otherListingPermit =
                limiter.acquire(1, LimitType.HEAP_MEMORY, () -> false).get(5, TimeUnit.SECONDS);
        AtomicInteger listingCalls = new AtomicInteger();

        long startNanos = System.nanoTime();
        CompletableFuture<List<String>> result = listTopics(() -> {
            listingCalls.incrementAndGet();
            return CompletableFuture.completedFuture(TOPICS);
        });

        // fails right away with 429 instead of waiting in the queue until no other topic list is in flight
        assertThat(result).failsWithin(Duration.ofSeconds(2)).withThrowableThat()
                .havingRootCause().isInstanceOfSatisfying(RestException.class,
                        e -> assertThat(e.getResponse().getStatus()).isEqualTo(429));
        assertThat(Duration.ofNanos(System.nanoTime() - startNanos)).isLessThan(Duration.ofSeconds(2));
        assertThat(listingCalls).hasValue(0);
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getQueueSize()).isZero();
        limiter.release(otherListingPermit);
    }

    @Test
    public void testListTopicsAfterCollectingSkipsCollectingWhenResponseIsAlreadyDone() {
        responseDone.set(true);
        AtomicInteger collectorCalls = new AtomicInteger();
        TopicListing listing = TopicListing.namespaceTopics(NAMESPACE, Mode.ALL);

        CompletableFuture<List<String>> result = TopicListResponseMemoryLimiter.listTopicsAfterCollecting(
                brokerService, response, listing, null, () -> {
                    collectorCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(TOPICS);
                }, CompletableFuture::completedFuture);

        assertThat(result).failsWithin(Duration.ofSeconds(5));
        assertThat(collectorCalls).hasValue(0);
        assertThat(result).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .havingRootCause().isInstanceOf(TopicListResponseMemoryLimiter.RequestCancelledException.class);
        assertThat(acquiredHeapPermits()).isZero();
        // Cancelling the initializing request must allow a later live request to obtain an estimate.
        assertThat(sizeCache.getTopicListSize(listing, null).getSizeAsync()).isDone();
    }

    @Test
    public void testListTopicsAfterCollectingSkipsCollectingWhenEstimateIsLargerThanLimit() {
        sizeCache = new TopicListSizeResultCache(MAX_HEAP_MEMORY, () -> true);
        when(brokerService.getTopicListSizeResultCache()).thenReturn(sizeCache);
        TopicListing listing = TopicListing.namespaceTopics(NAMESPACE, Mode.ALL);
        // a previous listing measured a topic list larger than the limit
        sizeCache.getTopicListSize(listing, null).updateSize(MAX_HEAP_MEMORY * 2);
        AtomicInteger collectorCalls = new AtomicInteger();
        AtomicInteger listingCalls = new AtomicInteger();

        CompletableFuture<List<String>> result = TopicListResponseMemoryLimiter.listTopicsAfterCollecting(
                brokerService, response, listing, null, () -> {
                    collectorCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(TOPICS);
                }, collected -> {
                    listingCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(collected);
                });

        assertThat(result).failsWithin(Duration.ofSeconds(2)).withThrowableThat()
                .havingRootCause().isInstanceOfSatisfying(RestException.class,
                        e -> assertThat(e.getResponse().getStatus()).isEqualTo(429));
        assertThat(collectorCalls).hasValue(0);
        assertThat(listingCalls).hasValue(0);
    }

    @Test
    public void testListTopicsAfterCollectingAcquiresPermitsAfterCollecting() {
        AtomicLong permitsWhileCollecting = new AtomicLong(-1);
        CompletableFuture<List<String>> result = TopicListResponseMemoryLimiter.listTopicsAfterCollecting(
                brokerService, response, TopicListing.namespaceTopics(NAMESPACE, Mode.ALL), null, () -> {
                    permitsWhileCollecting.set(acquiredHeapPermits());
                    return CompletableFuture.completedFuture(TOPICS);
                }, CompletableFuture::completedFuture);

        assertThat(result).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(TOPICS);
        assertThat(permitsWhileCollecting).hasValue(0);
        assertThat(acquiredHeapPermits()).isEqualTo(TopicListMemoryLimiter.estimateTopicListSize(TOPICS));
        completeResponse(null);
        assertThat(acquiredHeapPermits()).isZero();
    }
}
