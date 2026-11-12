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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.CustomLog;
import org.apache.commons.collections4.ListUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicListener;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.apache.pulsar.common.semaphore.AsyncSemaphore;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.awaitility.Awaitility;
import org.jspecify.annotations.NonNull;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class TopicListServiceTest {

    private TopicListService topicListService;
    private ServerCnx connection;
    private CompletableFuture<List<String>> topicListFuture;
    private Semaphore lookupSemaphore;
    private TopicResources topicResources;
    private final TopicsPattern.RegexImplementation topicsPatternImplementation =
            TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK;
    private EventLoop eventLoop;
    private PulsarCommandSender pulsarCommandSender;
    private Consumer<Notification> notificationConsumer;
    private AsyncDualMemoryLimiterImpl memoryLimiter;
    private ScheduledExecutorService scheduledExecutorService;
    private PulsarService pulsar;
    private NamespaceService namespaceService;
    private TopicListSizeResultCache.ResultHolder resultHolder;
    private AtomicBoolean connectionActive;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        lookupSemaphore = new Semaphore(1);
        lookupSemaphore.acquire();
        topicListFuture = new CompletableFuture<>();

        AtomicReference<Consumer<Notification>> listenerRef = new AtomicReference<>();
        MetadataStoreExtended metadataStore = mock(MetadataStoreExtended.class);
        doAnswer(invocationOnMock -> {
            listenerRef.set(invocationOnMock.getArgument(0));
            return (Runnable) () -> { };
        }).when(metadataStore).registerCancellableListener(any());
        topicResources = spy(new TopicResources(metadataStore));
        notificationConsumer = listenerRef.get();

        pulsar = mock(PulsarService.class);
        namespaceService = mock(NamespaceService.class);
        when(pulsar.getNamespaceService()).thenReturn(namespaceService);
        doAnswer(invocationOnMock -> topicListFuture)
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));
        when(pulsar.getPulsarResources()).thenReturn(mock(PulsarResources.class));
        when(pulsar.getPulsarResources().getTopicResources()).thenReturn(topicResources);

        BrokerService brokerService = mock(BrokerService.class);
        when(pulsar.getBrokerService()).thenReturn(brokerService);
        TopicListSizeResultCache topicListSizeResultCache = mock(TopicListSizeResultCache.class);
        when(brokerService.getTopicListSizeResultCache()).thenReturn(topicListSizeResultCache);
        when(brokerService.getTopicsPatternCache()).thenReturn(new TopicsPatternCache());
        resultHolder = mock(TopicListSizeResultCache.ResultHolder.class);
        doReturn(resultHolder).when(topicListSizeResultCache).getTopicListSize(anyString(), any());
        doReturn(CompletableFuture.completedFuture(1L)).when(resultHolder).getSizeAsync();

        memoryLimiter = new AsyncDualMemoryLimiterImpl(1_000_000, 10000, 500, 1_000_000, 10000, 500);
        doReturn(memoryLimiter).when(brokerService).getMaxTopicListInFlightLimiter();

        connection = mock(ServerCnx.class);
        when(connection.getRemoteAddress()).thenReturn(new InetSocketAddress(10000));
        pulsarCommandSender = mock(PulsarCommandSender.class);
        when(connection.getCommandSender()).thenReturn(pulsarCommandSender);
        connectionActive = new AtomicBoolean(true);
        when(connection.isActive()).thenAnswer(__ -> connectionActive.get());
        when(pulsarCommandSender.sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsarCommandSender.sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(connection.ctx()).thenReturn(ctx);
        EventExecutor executor = spy(ImmediateEventExecutor.INSTANCE);
        doReturn(executor).when(ctx).executor();
        doAnswer(invocationOnMock -> {
            scheduledExecutorService.schedule(invocationOnMock.<Runnable>getArgument(0),
                    invocationOnMock.getArgument(1), invocationOnMock.getArgument(2));
            return mock(ScheduledFuture.class);
        }).when(executor).schedule(any(Runnable.class), anyLong(), any());
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        doAnswer(invocationOnMock -> {
            scheduledExecutorService.schedule(invocationOnMock.<Runnable>getArgument(0),
                    invocationOnMock.getArgument(1), invocationOnMock.getArgument(2));
            return mock(ScheduledFuture.class);
        }).when(eventLoop).schedule(any(Runnable.class), anyLong(), any());

        topicListService = newTopicListService();

    }

    private @NonNull TopicListService newTopicListService() {
        return new TopicListService(pulsar, connection, true, 30);
    }

    private @NonNull TopicListService newTopicListService(int topicListUpdateMaxQueueSize) {
        return new TopicListService(pulsar, connection, true, 30,
                topicListUpdateMaxQueueSize);
    }

    @AfterMethod(alwaysRun = true)
    void cleanup() {
        if (memoryLimiter != null) {
            memoryLimiter.close();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @DataProvider
    public Object[][] rejectedReservations() {
        return new Object[][] {{"invalid-pattern"}, {"too-long"}, {"disabled"}};
    }

    @Test(dataProvider = "rejectedReservations")
    public void testRejectedReservedRequestCompletesAndRemovesItsFuture(String failure) {
        if (failure.equals("disabled")) {
            topicListService = new TopicListService(pulsar, connection, false, 30);
        }
        String pattern = failure.equals("invalid-pattern") ? "tenant/ns/[" : "tenant/ns/" + "a".repeat(31);
        TopicListService.WatchTopicListRequest request = topicListService.beginWatchTopicListRequest(13);
        assertThat(topicListService.getWatcherFuture(13)).isSameAs(request.watcherFuture());
        assertThat(request.watcherFuture()).isNotDone();

        topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 7, pattern,
                topicsPatternImplementation, null, lookupSemaphore, request);

        assertThat(request.watcherFuture()).isCompletedExceptionally();
        assertThat(topicListService.getWatcherFuture(13)).isNull();
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
        verify(topicResources, never()).registerPersistentTopicListener(any());
    }

    @Test
    public void testInactivateFailsReservationBeforeAuthorizationCompletes() {
        TopicListService.WatchTopicListRequest old = topicListService.beginWatchTopicListRequest(13);
        topicListService.inactivate();
        assertThat(old.watcherFuture()).isCompletedExceptionally();
        assertThat(topicListService.getWatcherFuture(13)).isNull();
        TopicListService.WatchTopicListRequest retry = topicListService.beginWatchTopicListRequest(13);

        topicListService.failWatchTopicListRequest(old, new IllegalStateException("Late authorization failure"));

        assertThat(retry.watcherFuture()).isNotSameAs(old.watcherFuture()).isNotDone();
        assertThat(topicListService.getWatcherFuture(13)).isSameAs(retry.watcherFuture());
        topicListService.inactivate();
    }

    @DataProvider
    public Object[][] authorizedRetryInitializationResults() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "authorizedRetryInitializationResults")
    public void testAuthorizedRetryClaimsReservationAndCoalescesPendingInitialization(boolean initializationFails) {
        TopicListService.WatchTopicListRequest original = topicListService.beginWatchTopicListRequest(13);
        TopicListService.WatchTopicListRequest retry = topicListService.beginWatchTopicListRequest(13);
        List<String> topics = List.of("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2");
        List<String> matchingTopics = List.of(topics.get(0));
        try {
            // The retry authorizes independently while the first request is still waiting for authorization.
            topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 8, "tenant/ns/topic1",
                    topicsPatternImplementation, null, lookupSemaphore, retry);
            verify(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));
            assertThat(topicListFuture).isNotDone();
            // the watcher is registered before the listing, so that it receives the topic changes the listing misses
            verify(topicResources).registerPersistentTopicListener(any());

            TopicListService.WatchTopicListRequest duplicate = topicListService.beginWatchTopicListRequest(13);
            topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 9, "tenant/ns/topic.*",
                    topicsPatternImplementation, null, lookupSemaphore, duplicate);
            topicListService.failWatchTopicListRequest(original,
                    new IllegalStateException("Late authorization failure"));

            // Once initialization starts, further authorized requests must share the ongoing listing.
            verify(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));
            verify(topicResources).registerPersistentTopicListener(any());
            verify(pulsarCommandSender, never())
                    .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any());
            assertThat(topicListService.getWatcherFuture(13)).isSameAs(retry.watcherFuture()).isNotDone();
            assertThat(lookupSemaphore.availablePermits()).isZero();

            if (initializationFails) {
                topicListFuture.completeExceptionally(new IllegalStateException("Topic listing failed"));

                assertThat(retry.watcherFuture()).isCompletedExceptionally();
                verify(topicResources).deregisterPersistentTopicListener(any(TopicListener.class));
                verify(pulsarCommandSender).sendErrorResponse(eq(8L), any(), anyString());
                verify(pulsarCommandSender).sendErrorResponse(eq(9L), any(), anyString());
                verify(pulsarCommandSender, never())
                        .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any());
                assertThat(topicListService.getWatcherFuture(13)).isNull();
            } else {
                topicListFuture.complete(topics);

                assertThat(retry.watcherFuture()).succeedsWithin(Duration.ofSeconds(2));
                verify(topicResources).registerPersistentTopicListener(any());
                verify(topicResources, never()).deregisterPersistentTopicListener(any(TopicListener.class));
                verify(pulsarCommandSender).sendWatchTopicListSuccess(eq(8L), eq(13L),
                        eq(TopicList.calculateHash(matchingTopics)), eq(matchingTopics), any(), any());
                verify(pulsarCommandSender).sendWatchTopicListSuccess(eq(9L), eq(13L),
                        eq(TopicList.calculateHash(matchingTopics)), eq(matchingTopics), any(), any());
                verify(pulsarCommandSender, never()).sendErrorResponse(anyLong(), any(), anyString());
                assertThat(topicListService.getWatcherFuture(13)).isSameAs(retry.watcherFuture());
            }
            assertThat(lookupSemaphore.availablePermits()).isEqualTo(2);
        } finally {
            topicListService.deleteTopicListWatcher(13L);
            topicListFuture.completeExceptionally(new IllegalStateException("Test finished"));
        }
    }

    @DataProvider
    public Object[][] initializationTimeoutBeforeClose() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "initializationTimeoutBeforeClose")
    public void testOldInitializationRetryCannotUseNewWatcherGeneration(boolean timeoutFirst) throws Exception {
        List<Runnable> retries = new ArrayList<>();
        EventExecutor executor = connection.ctx().executor();
        doAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            return mock(ScheduledFuture.class);
        }).when(executor).schedule(any(Runnable.class), anyLong(), any());
        CompletableFuture<Long> oldEstimate = new CompletableFuture<>();
        if (timeoutFirst) {
            oldEstimate.completeExceptionally(new AsyncSemaphore.PermitAcquireTimeoutException("Heap wait timed out"));
        }
        doReturn(oldEstimate).when(resultHolder).getSizeAsync();
        topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 7, "tenant/ns/topic.*",
                topicsPatternImplementation, null, lookupSemaphore);
        CompletableFuture<TopicListService.TopicListWatcher> old = topicListService.getWatcherFuture(13);
        assertThat(old).isNotDone();
        assertThat(retries).hasSize(timeoutFirst ? 1 : 0);
        topicListService.deleteTopicListWatcher(13L);
        assertThat(old).isCompletedExceptionally();
        doReturn(CompletableFuture.completedFuture(1L)).when(resultHolder).getSizeAsync();
        topicListFuture.complete(List.of("persistent://tenant/ns/topic1"));
        lookupSemaphore.acquire();
        TopicListService.WatchTopicListRequest replacement = topicListService.beginWatchTopicListRequest(13);
        topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 9, "tenant/ns/topic.*",
                topicsPatternImplementation, null, lookupSemaphore, replacement);
        assertThat(replacement.watcherFuture()).succeedsWithin(Duration.ofSeconds(2));
        clearInvocations(namespaceService, topicResources);

        if (timeoutFirst) {
            retries.get(0).run();
        } else {
            oldEstimate.completeExceptionally(new AsyncSemaphore.PermitAcquireTimeoutException("Late heap timeout"));
            assertThat(retries).isEmpty();
        }

        verify(namespaceService, never()).getListOfUserTopics(any(), any());
        verify(topicResources, never()).registerPersistentTopicListener(any());
        assertThat(topicListService.getWatcherFuture(13)).isSameAs(replacement.watcherFuture());
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testLateSuccessSendFailureCannotDeleteNewWatcherGeneration() throws Exception {
        CompletableFuture<Void> oldResponse = new CompletableFuture<>();
        doReturn(oldResponse, CompletableFuture.completedFuture(null)).when(pulsarCommandSender)
                .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any());
        List<String> topics = List.of("persistent://tenant/ns/topic1");
        TopicListService.TopicListWatcher old = createWatcher(topics);
        topicListService.deleteTopicListWatcher(13L);
        lookupSemaphore.acquire();
        TopicListService.TopicListWatcher replacement = createWatcher(topics);
        CompletableFuture<TopicListService.TopicListWatcher> replacementFuture = topicListService.getWatcherFuture(13);
        assertThat(replacement).isNotSameAs(old);
        clearInvocations(topicResources);

        oldResponse.completeExceptionally(new IllegalStateException("Old response write failed"));

        assertThat(topicListService.getWatcherFuture(13)).isSameAs(replacementFuture);
        verify(topicResources, never()).deregisterPersistentTopicListener(replacement);
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
    }

    @DataProvider
    public Object[][] successResponseTimeoutBeforeClose() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "successResponseTimeoutBeforeClose")
    public void testOldSuccessResponsePermitRetryCannotUseNewWatcherGeneration(boolean timeoutFirst) throws Exception {
        List<Runnable> retries = new ArrayList<>();
        doAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            return mock(ScheduledFuture.class);
        }).when(eventLoop).schedule(any(Runnable.class), anyLong(), any());
        CompletableFuture<Void> oldResponse = new CompletableFuture<>();
        AtomicReference<BooleanSupplier> oldCancelled = new AtomicReference<>();
        AtomicReference<Function<Throwable, CompletableFuture<Void>>> oldPermitErrorHandler = new AtomicReference<>();
        doAnswer(invocation -> {
            if (oldCancelled.get() == null) {
                oldCancelled.set(invocation.getArgument(5));
                oldPermitErrorHandler.set(invocation.getArgument(4));
                return oldResponse;
            }
            return CompletableFuture.completedFuture(null);
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(eq(7L), eq(13L), anyString(), any(), any(), any());
        List<String> topics = List.of("persistent://tenant/ns/topic1");
        TopicListService.TopicListWatcher old = createWatcher(topics);
        assertThat(oldCancelled.get().getAsBoolean()).isFalse();
        if (timeoutFirst) {
            FutureUtil.completeAfter(oldResponse, oldPermitErrorHandler.get().apply(
                    new AsyncSemaphore.PermitAcquireTimeoutException("Direct memory wait timed out")));
            assertThat(retries).hasSize(1);
        }

        topicListService.handleWatchTopicListClose(new CommandWatchTopicListClose()
                .setRequestId(8).setWatcherId(13));
        lookupSemaphore.acquire();
        topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 9,
                "persistent://tenant/ns/topic.*", topicsPatternImplementation, null, lookupSemaphore);
        CompletableFuture<TopicListService.TopicListWatcher> replacementFuture = topicListService.getWatcherFuture(13);
        assertThat(replacementFuture).succeedsWithin(Duration.ofSeconds(2)).isNotSameAs(old);
        clearInvocations(topicResources);

        if (timeoutFirst) {
            retries.remove(0).run();
        } else {
            FutureUtil.completeAfter(oldResponse, oldPermitErrorHandler.get().apply(
                    new AsyncSemaphore.PermitAcquireTimeoutException("Late direct memory timeout")));
        }

        assertThat(oldResponse).failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(AsyncSemaphore.PermitAcquireCancelledException.class);
        assertThat(oldCancelled.get().getAsBoolean()).isTrue();
        assertThat(retries).isEmpty();
        verify(pulsarCommandSender, times(1))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), anyString(), any(), any(), any());
        verify(pulsarCommandSender)
                .sendWatchTopicListSuccess(eq(9L), eq(13L), anyString(), eq(topics), any(), any());
        verify(pulsarCommandSender).sendErrorResponse(eq(7L), any(ServerError.class), anyString());
        assertThat(topicListService.getWatcherFuture(13)).isSameAs(replacementFuture);
        verify(topicResources, never()).deregisterPersistentTopicListener(replacementFuture.join());
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
    }

    @DataProvider
    public Object[][] updateTimeoutBeforeClose() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "updateTimeoutBeforeClose")
    public void testOldUpdatePermitRetryCannotUseNewWatcherGeneration(boolean timeoutFirst) throws Exception {
        List<Runnable> retries = new ArrayList<>();
        doAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            return mock(ScheduledFuture.class);
        }).when(eventLoop).schedule(any(Runnable.class), anyLong(), any());
        String staleTopic = "persistent://tenant/ns/topic2";
        List<String> staleNewTopics = List.of(staleTopic);
        CompletableFuture<Void> oldUpdate = new CompletableFuture<>();
        AtomicReference<BooleanSupplier> oldCancelled = new AtomicReference<>();
        AtomicReference<Function<Throwable, CompletableFuture<Void>>> oldPermitErrorHandler = new AtomicReference<>();
        doAnswer(invocation -> {
            if (oldCancelled.get() == null) {
                oldCancelled.set(invocation.getArgument(5));
                oldPermitErrorHandler.set(invocation.getArgument(4));
                return oldUpdate;
            }
            return CompletableFuture.completedFuture(null);
        }).when(pulsarCommandSender).sendWatchTopicListUpdate(eq(13L), eq(staleNewTopics), any(), anyString(),
                any(), any());
        List<String> topics = List.of("persistent://tenant/ns/topic1");
        TopicListService.TopicListWatcher old = createWatcher(topics, 7, "tenant/ns/topic.*");
        old.onTopicEvent(staleTopic, NotificationType.Created);
        assertThat(oldCancelled.get().getAsBoolean()).isFalse();
        if (timeoutFirst) {
            FutureUtil.completeAfter(oldUpdate, oldPermitErrorHandler.get().apply(
                    new AsyncSemaphore.PermitAcquireTimeoutException("Direct memory wait timed out")));
            assertThat(retries).hasSize(1);
        }

        topicListService.handleWatchTopicListClose(new CommandWatchTopicListClose()
                .setRequestId(8).setWatcherId(13));
        lookupSemaphore.acquire();
        TopicListService.TopicListWatcher replacement = createWatcher(topics, 9, "tenant/ns/topic.*");
        CompletableFuture<TopicListService.TopicListWatcher> replacementFuture = topicListService.getWatcherFuture(13);
        assertThat(replacement).isNotSameAs(old);

        if (timeoutFirst) {
            retries.remove(0).run();
        } else {
            FutureUtil.completeAfter(oldUpdate, oldPermitErrorHandler.get().apply(
                    new AsyncSemaphore.PermitAcquireTimeoutException("Late direct memory timeout")));
        }

        assertThat(oldUpdate).succeedsWithin(Duration.ofSeconds(2));
        assertThat(retries).isEmpty();
        verify(pulsarCommandSender, times(1)).sendWatchTopicListUpdate(eq(13L), eq(staleNewTopics), any(), anyString(),
                any(), any());
        assertThat(oldCancelled.get().getAsBoolean()).isTrue();
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic3"));
        List<String> freshTopics = List.of("persistent://tenant/ns/topic3");
        verify(pulsarCommandSender).sendWatchTopicListUpdate(eq(13L), eq(freshTopics), eq(List.of()),
                eq(TopicList.calculateHash(List.of(topics.get(0), freshTopics.get(0)))), any(), any());
        assertThat(replacement.getMatchingTopics()).containsExactlyInAnyOrder(topics.get(0), freshTopics.get(0));
        assertThat(topicListService.getWatcherFuture(13)).isSameAs(replacementFuture);
        verify(topicResources).registerPersistentTopicListener(old);
        verify(topicResources).deregisterPersistentTopicListener(old);
        verify(topicResources).registerPersistentTopicListener(replacement);
        verify(topicResources, never()).deregisterPersistentTopicListener(replacement);
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
        assertThat(memoryLimiter.getLimiter(AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY).getAcquiredPermits())
                .isZero();
        assertThat(memoryLimiter.getLimiter(AsyncDualMemoryLimiter.LimitType.DIRECT_MEMORY).getAcquiredPermits())
                .isZero();
    }

    @Test
    public void testOldUpdateIsCancelledBeforeWatcherCloseCompletes() throws Exception {
        List<Runnable> retries = new ArrayList<>();
        doAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            return mock(ScheduledFuture.class);
        }).when(eventLoop).schedule(any(Runnable.class), anyLong(), any());
        String staleTopic = "persistent://tenant/ns/topic2";
        List<String> staleNewTopics = List.of(staleTopic);
        CompletableFuture<Void> oldUpdate = new CompletableFuture<>();
        AtomicReference<BooleanSupplier> oldCancelled = new AtomicReference<>();
        AtomicReference<Function<Throwable, CompletableFuture<Void>>> oldPermitErrorHandler = new AtomicReference<>();
        doAnswer(invocation -> {
            oldCancelled.set(invocation.getArgument(5));
            oldPermitErrorHandler.set(invocation.getArgument(4));
            return oldUpdate;
        }).when(pulsarCommandSender).sendWatchTopicListUpdate(eq(13L), eq(staleNewTopics), any(), anyString(),
                any(), any());
        List<String> topics = List.of("persistent://tenant/ns/topic1");
        TopicListService.TopicListWatcher old = createWatcher(topics, 7, "tenant/ns/topic.*");
        old.onTopicEvent(staleTopic, NotificationType.Created);
        assertThat(oldCancelled.get().getAsBoolean()).isFalse();
        AtomicReference<TopicListService.TopicListWatcher> replacementRef = new AtomicReference<>();
        AtomicReference<Boolean> cancelledDuringClose = new AtomicReference<>();
        AtomicReference<Throwable> closeHookFailure = new AtomicReference<>();
        doAnswer(invocation -> {
            try {
                // The old slot is removed, but close() has not set the old watcher's closed flag yet.
                lookupSemaphore.acquire();
                replacementRef.set(createWatcher(topics, 9, "tenant/ns/topic.*"));
                cancelledDuringClose.set(oldCancelled.get().getAsBoolean());
                FutureUtil.completeAfter(oldUpdate, oldPermitErrorHandler.get().apply(
                        new AsyncSemaphore.PermitAcquireTimeoutException("Timeout during watcher close")));
            } catch (Throwable failure) {
                closeHookFailure.set(failure);
            } finally {
                invocation.callRealMethod();
            }
            return null;
        }).when(topicResources).deregisterPersistentTopicListener(old);

        topicListService.handleWatchTopicListClose(new CommandWatchTopicListClose()
                .setRequestId(8).setWatcherId(13));

        assertThat(closeHookFailure.get()).isNull();
        TopicListService.TopicListWatcher replacement = replacementRef.get();
        assertThat(replacement).isNotNull().isNotSameAs(old);
        assertThat(oldUpdate).succeedsWithin(Duration.ofSeconds(2));
        assertThat(retries).isEmpty();
        assertThat(cancelledDuringClose.get()).isTrue();
        verify(pulsarCommandSender, times(1)).sendWatchTopicListUpdate(eq(13L), eq(staleNewTopics), any(), anyString(),
                any(), any());
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic3"));
        verify(pulsarCommandSender).sendWatchTopicListUpdate(eq(13L),
                eq(List.of("persistent://tenant/ns/topic3")), eq(List.of()), anyString(), any(), any());
        assertThat(replacement.getMatchingTopics())
                .containsExactlyInAnyOrder("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic3");
        assertThat(topicListService.getWatcherFuture(13)).isCompletedWithValue(replacement);
        verify(topicResources).registerPersistentTopicListener(replacement);
        verify(topicResources).deregisterPersistentTopicListener(old);
        verify(topicResources, never()).deregisterPersistentTopicListener(replacement);
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
        assertThat(memoryLimiter.getLimiter(AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY).getAcquiredPermits())
                .isZero();
        assertThat(memoryLimiter.getLimiter(AsyncDualMemoryLimiter.LimitType.DIRECT_MEMORY).getAcquiredPermits())
                .isZero();
    }

    @Test
    public void testQueuedUpdateCannotUseNewWatcherGeneration() throws Exception {
        List<String> topics = List.of("persistent://tenant/ns/topic1");
        TopicListService.TopicListWatcher old = createWatcher(topics, 7, "tenant/ns/topic.*");
        List<Runnable> updates = new ArrayList<>();
        EventExecutor executor = connection.ctx().executor();
        doAnswer(invocation -> {
            updates.add(invocation.getArgument(0));
            return null;
        }).when(executor).execute(any(Runnable.class));
        old.onTopicEvent("persistent://tenant/ns/topic2", NotificationType.Created);
        assertThat(updates).hasSize(1);
        verify(pulsarCommandSender, never())
                .sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));

        topicListService.handleWatchTopicListClose(new CommandWatchTopicListClose()
                .setRequestId(8).setWatcherId(13));
        lookupSemaphore.acquire();
        TopicListService.TopicListWatcher replacement = createWatcher(topics, 9, "tenant/ns/topic.*");
        CompletableFuture<TopicListService.TopicListWatcher> replacementFuture = topicListService.getWatcherFuture(13);
        assertThat(replacement).isNotSameAs(old);

        updates.remove(0).run();

        verify(pulsarCommandSender, never())
                .sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic3"));
        verify(pulsarCommandSender).sendWatchTopicListUpdate(eq(13L),
                eq(List.of("persistent://tenant/ns/topic3")), eq(List.of()), anyString(), any(), any());
        assertThat(replacement.getMatchingTopics())
                .containsExactlyInAnyOrder("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic3");
        assertThat(topicListService.getWatcherFuture(13)).isSameAs(replacementFuture);
        verify(topicResources).registerPersistentTopicListener(old);
        verify(topicResources).deregisterPersistentTopicListener(old);
        verify(topicResources).registerPersistentTopicListener(replacement);
        verify(topicResources, never()).deregisterPersistentTopicListener(replacement);
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testCommandWatchSuccessResponse() {

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        String hash = TopicList.calculateHash(topics);
        topicListFuture.complete(topics);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(1, lookupSemaphore.availablePermits()));
        verify(topicResources).registerPersistentTopicListener(any(TopicListService.TopicListWatcher.class));
        Collection<String> expectedTopics = new ArrayList<>(topics);
        verify(connection.getCommandSender()).sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics),
                any(), any());
    }

    @Test
    public void testCommandWatchSuccessResponseWhenOutOfPermits() throws ExecutionException, InterruptedException {
        // acquire all permits
        AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit permit =
                memoryLimiter.acquire(1_000_000, AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY,
                                Boolean.FALSE::booleanValue)
                        .get();
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        String hash = TopicList.calculateHash(topics);
        topicListFuture.complete(topics);
        // wait for acquisition to timeout a few times
        Thread.sleep(2000);
        // release the permits
        memoryLimiter.release(permit);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(1, lookupSemaphore.availablePermits()));
        verify(topicResources).registerPersistentTopicListener(any(TopicListService.TopicListWatcher.class));
        Collection<String> expectedTopics = new ArrayList<>(topics);
        verify(connection.getCommandSender()).sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics),
                any(), any());
    }

    @Test
    public void testCancelledWatchResetsTopicListSizeEstimate() throws Exception {
        // acquire all permits so that the watcher's permit request waits in the queue
        AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit permit =
                memoryLimiter.acquire(1_000_000, AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY,
                                Boolean.FALSE::booleanValue)
                        .get();
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                memoryLimiter.getLimiter(AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY).getQueueSize(), 1));
        // the connection closes while the request waits for permits
        connectionActive.set(false);
        memoryLimiter.release(permit);
        // concurrent requests waiting for the size estimate of the cancelled request must be able to proceed
        Awaitility.await().untilAsserted(() -> verify(resultHolder).resetIfInitializing());
        verify(namespaceService, never()).getListOfUserTopics(any(), any());
    }

    @Test
    public void testCommandWatchErrorResponse() {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.completeExceptionally(new PulsarServerException("Error"));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(1, lookupSemaphore.availablePermits()));
        // the watcher registered before the listing is deregistered
        ArgumentCaptor<TopicListener> watcher = ArgumentCaptor.forClass(TopicListener.class);
        verify(topicResources).registerPersistentTopicListener(watcher.capture());
        verify(topicResources).deregisterPersistentTopicListener(watcher.getValue());
        verifyNoMoreInteractions(topicResources);
        verify(connection.getCommandSender()).sendErrorResponse(eq(7L), any(ServerError.class),
                eq(PulsarServerException.class.getCanonicalName() + ": Error"));
    }

    @Test
    public void testCommandWatchTopicListCloseRemovesListener() {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        CommandWatchTopicListClose watchTopicListClose = new CommandWatchTopicListClose()
                .setRequestId(8)
                .setWatcherId(13);
        topicListService.handleWatchTopicListClose(watchTopicListClose);

        verify(topicResources).deregisterPersistentTopicListener(any(TopicListService.TopicListWatcher.class));
    }

    @Test
    public void testCommandWatchSuccessDirectMemoryAcquirePermitsRetries() {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        String hash = TopicList.calculateHash(topics);
        AtomicInteger failureCount = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            if (failureCount.incrementAndGet() < 3) {
                Throwable failure = new AsyncSemaphore.PermitAcquireTimeoutException("Acquire timed out");
                Function<Throwable, CompletableFuture<Void>> permitAcquireErrorHandler =
                        invocationOnMock.getArgument(4);
                return permitAcquireErrorHandler.apply(failure);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any());
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));
        Collection<String> expectedTopics = new ArrayList<>(topics);
        verify(connection.getCommandSender(), timeout(2000L).times(3))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics), any(), any());
    }

    @Test
    public void testCommandWatchUpdate() {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        List<String> newTopics = Collections.singletonList("persistent://tenant/ns/topic2");
        String hash = TopicList.calculateHash(ListUtils.union(topics, newTopics));
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        verify(connection.getCommandSender(), timeout(2000L))
                .sendWatchTopicListUpdate(eq(13L), eq(newTopics), any(), eq(hash), any(), any());

        hash = TopicList.calculateHash(newTopics);
        notificationConsumer.accept(
                new Notification(NotificationType.Deleted, "/managed-ledgers/tenant/ns/persistent/topic1"));
        verify(connection.getCommandSender(), timeout(2000L))
                .sendWatchTopicListUpdate(eq(13L), eq(List.of()), eq(topics), eq(hash), any(), any());
    }

    @Test
    public void testCommandWatchUpdateRetries() {
        AtomicInteger failureCount = new AtomicInteger(0);
        // Set up the stubbing before starting async work to avoid races with Mockito stubbing state.
        doAnswer(invocationOnMock -> {
            List<String> newTopicsArg = invocationOnMock.getArgument(1);
            if (!newTopicsArg.isEmpty() && failureCount.incrementAndGet() < 3) {
                Throwable failure = new AsyncSemaphore.PermitAcquireTimeoutException("Acquire timed out");
                Function<Throwable, CompletableFuture<Void>> permitAcquireErrorHandler =
                        invocationOnMock.getArgument(4);
                return permitAcquireErrorHandler.apply(failure);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).when(pulsarCommandSender).sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        List<String> newTopics = Collections.singletonList("persistent://tenant/ns/topic2");
        String hash = TopicList.calculateHash(ListUtils.union(topics, newTopics));
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        notificationConsumer.accept(
                new Notification(NotificationType.Deleted, "/managed-ledgers/tenant/ns/persistent/topic2"));
        InOrder inOrder = inOrder(connection.getCommandSender());
        inOrder.verify(connection.getCommandSender(), timeout(2000L).times(3))
                .sendWatchTopicListUpdate(eq(13L), eq(newTopics), eq(List.of()), eq(hash), any(), any());
        inOrder.verify(connection.getCommandSender(), timeout(2000L).times(1))
                .sendWatchTopicListUpdate(eq(13L), eq(List.of()), eq(newTopics), any(), any(), any());
    }

    @Test
    public void testCommandWatchUpdateQueueOverflows() {
        int topicListUpdateMaxQueueSize = 10;
        topicListService = newTopicListService(topicListUpdateMaxQueueSize);
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d+",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        topicListFuture = new CompletableFuture<>();
        List<String> updatedTopics = IntStream.range(1, 101).mapToObj(i -> "persistent://tenant/ns/topic" + i).toList();
        // when the queue overflows
        for (int i = 1; i < updatedTopics.size(); i++) {
            TopicName topicName = TopicName.get(updatedTopics.get(i));
            notificationConsumer.accept(new Notification(NotificationType.Created,
                    "/managed-ledgers/" + topicName.getPersistenceNamingEncoding()));
        }
        // a new listing should be performed
        topicListFuture.complete(updatedTopics);
        // validate that the watcher's matching topics have been updated
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<TopicListService.TopicListWatcher> watcherFuture = topicListService.getWatcherFuture(13);
            assertThat(watcherFuture).isNotNull();
            assertThat(watcherFuture.join().getMatchingTopics())
                    .containsExactlyInAnyOrderElementsOf(updatedTopics);
        });
    }

    @Test
    public void testCommandWatchSuccessNoTopicsInResponseWhenHashMatches() {
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        String hash = TopicList.calculateHash(topics);
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, hash,
                lookupSemaphore);
        doReturn(CompletableFuture.completedFuture(null)).when(pulsarCommandSender)
                .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any());
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));
        Collection<String> expectedTopics = List.of();
        verify(connection.getCommandSender(), timeout(2000L).times(1))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics), any(), any());
    }

    @Test
    public void testCommandWatchSuccessTopicsInResponseWhenHashDoesntMatch() {
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        String hash = TopicList.calculateHash(topics);
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, "INVALID_HASH",
                lookupSemaphore);
        doReturn(CompletableFuture.completedFuture(null)).when(pulsarCommandSender)
                .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any());
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));
        Collection<String> expectedTopics = topics;
        verify(connection.getCommandSender(), timeout(2000L).times(1))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics), any(), any());
    }

    @Test
    public void testTopicChangesWhileListingTheInitialTopicsAreNotLost() {
        // The client watches with the hash of the topics it subscribed to. While the broker lists the namespace,
        // topic2 is created and topic3 deleted, after the listing read the namespace.
        List<String> listedTopics = List.of("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic3");
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, TopicList.calculateHash(listedTopics),
                lookupSemaphore);
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        notificationConsumer.accept(
                new Notification(NotificationType.Deleted, "/managed-ledgers/tenant/ns/persistent/topic3"));
        topicListFuture.complete(listedTopics);

        // The success response has the topics after the changes. With the listed topics, its hash would match the
        // client's, and the client would miss both changes until it reconciles, after patternAutoDiscoveryPeriod.
        List<String> expectedTopics = List.of("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2");
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));
        verify(pulsarCommandSender, timeout(2000L)).sendWatchTopicListSuccess(eq(7L), eq(13L),
                eq(TopicList.calculateHash(expectedTopics)),
                argThat(topics -> topics.size() == 2 && topics.containsAll(expectedTopics)), any(), any());
        verify(pulsarCommandSender, never())
                .sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());
    }

    @Test
    public void testSessionDisconnectAndReconnectSendsNewAndDeletedTopics() {
        // Initial topics: topic1, topic2, topic3
        List<String> initialTopics = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic2",
                "persistent://tenant/ns/topic3");

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(initialTopics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        TopicListService.TopicListWatcher watcher = topicListService.getWatcherFuture(13).join();
        assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(initialTopics);

        // Simulate session disconnect
        watcher.onSessionEvent(SessionEvent.ConnectionLost);

        // Prepare topics after reconnect: topic2 remains, topic1 and topic3 deleted, topic4 and topic5 added
        List<String> topicsAfterReconnect = List.of(
                "persistent://tenant/ns/topic2",
                "persistent://tenant/ns/topic4",
                "persistent://tenant/ns/topic5");

        topicListFuture = new CompletableFuture<>();
        NamespaceService namespaceService = pulsar.getNamespaceService();
        doAnswer(invocationOnMock -> topicListFuture)
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        // Simulate session reconnect - this should trigger a topic list refresh
        watcher.onSessionEvent(SessionEvent.Reconnected);

        // Complete the topic list future with the new topics
        topicListFuture.complete(topicsAfterReconnect);

        // Expected: deleted topics are topic1 and topic3, new topics are topic4 and topic5
        List<String> expectedDeleted = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic3");
        List<String> expectedNew = List.of(
                "persistent://tenant/ns/topic4",
                "persistent://tenant/ns/topic5");
        String expectedHash = TopicList.calculateHash(topicsAfterReconnect);

        // Verify that sendWatchTopicListUpdate is called with the correct new and deleted topics
        verify(connection.getCommandSender(), timeout(2000L))
                .sendWatchTopicListUpdate(eq(13L),
                        argThat(newTopics -> new HashSet<>(newTopics).equals(new HashSet<>(expectedNew))),
                        argThat(deletedTopics -> new HashSet<>(deletedTopics).equals(new HashSet<>(expectedDeleted))),
                        eq(expectedHash), any(), any());

        // Verify the watcher's matching topics are updated
        assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(topicsAfterReconnect);
    }

    @Test
    public void testSessionLostAndReestablishedSendsNewAndDeletedTopics() {
        // Initial topics: topic1, topic2
        List<String> initialTopics = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic2");

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(initialTopics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        TopicListService.TopicListWatcher watcher = topicListService.getWatcherFuture(13).join();

        // Simulate session lost (more severe than connection lost)
        watcher.onSessionEvent(SessionEvent.SessionLost);

        // After reconnect: all topics deleted, completely new topics added
        List<String> topicsAfterReconnect = List.of(
                "persistent://tenant/ns/topic7",
                "persistent://tenant/ns/topic8",
                "persistent://tenant/ns/topic9");

        topicListFuture = new CompletableFuture<>();
        NamespaceService namespaceService = pulsar.getNamespaceService();
        doAnswer(invocationOnMock -> topicListFuture)
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        // Simulate session reestablished
        watcher.onSessionEvent(SessionEvent.SessionReestablished);

        topicListFuture.complete(topicsAfterReconnect);

        List<String> expectedDeleted = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic2");
        List<String> expectedNew = List.of(
                "persistent://tenant/ns/topic7",
                "persistent://tenant/ns/topic8",
                "persistent://tenant/ns/topic9");
        String expectedHash = TopicList.calculateHash(topicsAfterReconnect);

        verify(connection.getCommandSender(), timeout(2000L))
                .sendWatchTopicListUpdate(eq(13L),
                        argThat(newTopics -> new HashSet<>(newTopics).equals(new HashSet<>(expectedNew))),
                        argThat(deletedTopics -> new HashSet<>(deletedTopics).equals(new HashSet<>(expectedDeleted))),
                        eq(expectedHash), any(), any());

        assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(topicsAfterReconnect);
    }

    @Test
    public void testSessionReconnectWithNoChangesDoesNotSendUpdate() {
        // Initial topics
        List<String> initialTopics = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic2");

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(initialTopics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        TopicListService.TopicListWatcher watcher = topicListService.getWatcherFuture(13).join();

        // Simulate session disconnect
        watcher.onSessionEvent(SessionEvent.ConnectionLost);

        // Topics remain the same after reconnect
        topicListFuture = new CompletableFuture<>();
        NamespaceService namespaceService = pulsar.getNamespaceService();
        doAnswer(invocationOnMock -> topicListFuture)
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        watcher.onSessionEvent(SessionEvent.Reconnected);

        // Complete with the same topics
        topicListFuture.complete(initialTopics);

        // Wait for processing
        Awaitility.await().untilAsserted(() ->
                assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(initialTopics));

        // Verify that sendWatchTopicListUpdate was NOT called (no changes to report)
        verify(connection.getCommandSender(), timeout(500L).times(0))
                .sendWatchTopicListUpdate(eq(13L), any(), any(), any(), any(), any());
    }

    @Test
    public void testConnectionOrSessionFlappingDoesNotTriggerOverlappingUpdate() {
        List<String> initialTopics = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic2");

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(initialTopics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        TopicListService.TopicListWatcher watcher = topicListService.getWatcherFuture(13).join();
        clearInvocations(namespaceService);

        topicListFuture = new CompletableFuture<>();
        watcher.onSessionEvent(SessionEvent.ConnectionLost);
        watcher.onSessionEvent(SessionEvent.Reconnected);

        Awaitility.await().untilAsserted(() -> verify(namespaceService, timeout(500L).times(1))
                .getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT)));

        watcher.onSessionEvent(SessionEvent.ConnectionLost);
        watcher.onSessionEvent(SessionEvent.Reconnected);
        watcher.onSessionEvent(SessionEvent.ConnectionLost);
        watcher.onSessionEvent(SessionEvent.Reconnected);
        watcher.onSessionEvent(SessionEvent.SessionLost);
        watcher.onSessionEvent(SessionEvent.SessionReestablished);

        verify(namespaceService, timeout(500L).times(1))
                .getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        topicListFuture.complete(initialTopics);
        Awaitility.await().untilAsserted(() ->
                assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(initialTopics));
    }

    @Test
    public void testWatchTopicListReconcileDoesntTriggerOverlappingUpdate() {
        List<String> initialTopics = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic2");

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(initialTopics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        TopicListService.TopicListWatcher watcher = topicListService.getWatcherFuture(13).join();

        clearInvocations(namespaceService);
        topicListFuture = new CompletableFuture<>();
        watcher.onSessionEvent(SessionEvent.ConnectionLost);
        watcher.onSessionEvent(SessionEvent.Reconnected);
        Awaitility.await().untilAsserted(() -> verify(namespaceService, timeout(500L).times(1))
                .getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT)));

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, TopicList.calculateHash(initialTopics),
                lookupSemaphore);

        topicListFuture.complete(initialTopics);

        verify(namespaceService, timeout(500L).times(1))
                .getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        Awaitility.await().untilAsserted(() ->
                assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(initialTopics));
    }

    @Test
    public void testWatchTopicListReconcileNoOverlappingUpdate() {
        List<String> initialTopics = List.of(
                "persistent://tenant/ns/topic1",
                "persistent://tenant/ns/topic2");

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(initialTopics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));

        TopicListService.TopicListWatcher watcher = topicListService.getWatcherFuture(13).join();

        clearInvocations(namespaceService);
        topicListFuture = new CompletableFuture<>();
        watcher.onSessionEvent(SessionEvent.ConnectionLost);
        watcher.onSessionEvent(SessionEvent.Reconnected);
        Awaitility.await().untilAsserted(() -> verify(namespaceService, timeout(500L).times(1))
                .getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT)));

        topicListFuture.complete(initialTopics);
        Awaitility.await().untilAsserted(() -> {
            assertThat(watcher.isUpdatingTopics()).isFalse();
        });

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, TopicList.calculateHash(initialTopics),
                lookupSemaphore);

        verify(namespaceService, timeout(1000L).times(2))
                .getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        Awaitility.await().untilAsserted(() ->
                assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(initialTopics));
    }

    @Test
    public void testWatchTopicListReconcileDoesntCompileTopicsPattern() {
        List<String> topics = List.of("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2");
        String hash = TopicList.calculateHash(topics);
        try (MockedStatic<TopicsPatternFactory> topicsPatternFactory =
                     mockStatic(TopicsPatternFactory.class, CALLS_REAL_METHODS)) {
            topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 7,
                    "persistent://tenant/ns/topic\\d", topicsPatternImplementation, null, lookupSemaphore);
            topicListFuture.complete(topics);
            verify(pulsarCommandSender, timeout(2000L)).sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash),
                    eq(new ArrayList<>(topics)), any(), any());

            // clients reconcile their watchers periodically by sending a watch request with the same watcher id
            topicListService.handleWatchTopicList(NamespaceName.get("tenant/ns"), 13, 8,
                    "persistent://tenant/ns/topic\\d", topicsPatternImplementation, hash, lookupSemaphore);
            verify(pulsarCommandSender, timeout(2000L)).sendWatchTopicListSuccess(eq(8L), eq(13L), eq(hash),
                    eq(Collections.emptyList()), any(), any());

            // the pattern is compiled only for the request that created the watcher
            topicsPatternFactory.verify(() -> TopicsPatternFactory.create(anyString(),
                    any(TopicsPattern.RegexImplementation.class)), times(1));
        }
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(2));
    }

    private TopicListService.TopicListWatcher createWatcher(List<String> topics) throws Exception {
        return createWatcher(topics, 7, "persistent://tenant/ns/topic\\d");
    }

    private TopicListService.TopicListWatcher createWatcher(List<String> topics, long requestId, String pattern)
            throws Exception {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                requestId,
                pattern,
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(topics);
        TopicListService.TopicListWatcher watcher =
                topicListService.getWatcherFuture(13).get(2, TimeUnit.SECONDS);
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(1));
        return watcher;
    }

    @Test
    public void testDuplicateWatchRequestCompletesWhenWatcherIsClosedDuringUpdate() throws Exception {
        TopicListService.TopicListWatcher watcher =
                createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        // start an update that doesn't complete
        topicListFuture = new CompletableFuture<>();
        assertThat(watcher.prepareUpdateTopics(null)).isTrue();
        topicListService.updateTopicListWatcher(watcher);

        // a duplicate watch request waits for the ongoing update
        lookupSemaphore.acquire();
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                8,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        assertThat(lookupSemaphore.availablePermits()).isZero();

        // closing the watcher completes the waiting request with an error and releases its lookup permit
        topicListService.deleteTopicListWatcher(13L);
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(1));
        verify(pulsarCommandSender).sendErrorResponse(eq(8L), any(ServerError.class), anyString());
        verify(pulsarCommandSender, never())
                .sendWatchTopicListSuccess(eq(8L), anyLong(), anyString(), any(), any(), any());
    }

    @Test
    public void testDuplicateWatchRequestGetsNonRetriableErrorWhenTopicListIsLargerThanMemoryLimit()
            throws Exception {
        TopicListService.TopicListWatcher watcher =
                createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        // start an update that doesn't complete yet
        topicListFuture = new CompletableFuture<>();
        assertThat(watcher.prepareUpdateTopics(null)).isTrue();
        topicListService.updateTopicListWatcher(watcher);

        // a duplicate watch request waits for the ongoing update
        lookupSemaphore.acquire();
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                8,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);

        // the estimated size of the topic list is larger than the 1_000_000 bytes heap memory limit
        topicListFuture.complete(List.of("persistent://tenant/ns/topic1" + "x".repeat(1_000_000)));

        // the waiting request gets the non-retriable error of the update, not the error of closing the watcher
        verify(pulsarCommandSender, timeout(2000L))
                .sendErrorResponse(eq(8L), eq(ServerError.NotAllowedError), anyString());
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(1));
        assertThat(topicListService.getWatcherFuture(13)).isNull();
    }

    @Test
    public void testFailedWatcherUpdateIsRetried() throws Exception {
        TopicListService.TopicListWatcher watcher =
                createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        List<String> refreshedTopics = List.of("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2");
        AtomicInteger listingCalls = new AtomicInteger();
        doAnswer(invocationOnMock -> listingCalls.incrementAndGet() == 1
                ? CompletableFuture.failedFuture(new PulsarServerException("Metadata error"))
                : CompletableFuture.completedFuture(refreshedTopics))
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        assertThat(watcher.prepareUpdateTopics(null)).isTrue();
        assertThat(topicListService.updateTopicListWatcher(watcher)).succeedsWithin(Duration.ofSeconds(10));

        assertThat(listingCalls).hasValue(2);
        assertThat(watcher.isUpdatingTopics()).isFalse();
        assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(refreshedTopics);
    }

    @Test
    public void testWatcherIsClosedWhenTopicListIsLargerThanMemoryLimit() throws Exception {
        TopicListService.TopicListWatcher watcher =
                createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        // the estimated size of the topic list is larger than the 1_000_000 bytes heap memory limit
        List<String> hugeTopicList = List.of("persistent://tenant/ns/topic1" + "x".repeat(1_000_000));
        doReturn(CompletableFuture.completedFuture(hugeTopicList))
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        assertThat(watcher.prepareUpdateTopics(null)).isTrue();
        Throwable failure = topicListService.updateTopicListWatcher(watcher)
                .handle((__, t) -> t).get(10, TimeUnit.SECONDS);
        assertThat(FutureUtil.unwrapCompletionException(failure))
                .isInstanceOf(AsyncSemaphore.PermitAcquireLimitExceededException.class);

        assertThat(topicListService.getWatcherFuture(13)).isNull();
        verify(topicResources).deregisterPersistentTopicListener(watcher);
        assertThat(memoryLimiter.getLimiter(AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY).getAcquiredPermits())
                .isZero();
    }

    @Test
    public void testWatchSendsAreCancelledWhenWatcherIsClosed() throws Exception {
        AtomicReference<BooleanSupplier> successCancelledRef = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            successCancelledRef.set(invocationOnMock.getArgument(5));
            return CompletableFuture.completedFuture(null);
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(),
                any());
        AtomicReference<BooleanSupplier> updateCancelledRef = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            updateCancelledRef.set(invocationOnMock.getArgument(5));
            return CompletableFuture.completedFuture(null);
        }).when(pulsarCommandSender).sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());
        createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        Awaitility.await().untilAsserted(() -> assertThat(updateCancelledRef.get()).isNotNull());
        assertThat(successCancelledRef.get().getAsBoolean()).isFalse();
        assertThat(updateCancelledRef.get().getAsBoolean()).isFalse();

        topicListService.deleteTopicListWatcher(13L);

        // sends that are still waiting for direct memory permits are cancelled
        assertThat(successCancelledRef.get().getAsBoolean()).isTrue();
        assertThat(updateCancelledRef.get().getAsBoolean()).isTrue();
    }

    @Test
    public void testWatcherIsDeletedWhenSendingSuccessResponseFails() throws Exception {
        doReturn(CompletableFuture.failedFuture(new RuntimeException("Sending failed")))
                .when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(),
                        any());
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(Collections.singletonList("persistent://tenant/ns/topic1"));

        Awaitility.await().untilAsserted(() -> assertThat(topicListService.getWatcherFuture(13)).isNull());
        verify(topicResources).deregisterPersistentTopicListener(any(TopicListService.TopicListWatcher.class));
        // the request gets an error response instead of waiting for a response that never comes
        verify(pulsarCommandSender).sendErrorResponse(eq(7L), any(ServerError.class), eq("Sending failed"));
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testWatchRequestGetsErrorWhenSuccessResponseIsLargerThanDirectMemoryLimit() throws Exception {
        doAnswer(invocationOnMock -> {
            Function<Throwable, CompletableFuture<Void>> permitAcquireErrorHandler = invocationOnMock.getArgument(4);
            return permitAcquireErrorHandler.apply(new AsyncSemaphore.PermitAcquireLimitExceededException(
                    "Requested permits are larger than the limit", new IllegalArgumentException()));
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(),
                any());
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(Collections.singletonList("persistent://tenant/ns/topic1"));

        Awaitility.await().untilAsserted(() -> assertThat(topicListService.getWatcherFuture(13)).isNull());
        verify(pulsarCommandSender, timeout(2000L))
                .sendErrorResponse(eq(7L), eq(ServerError.NotAllowedError), anyString());
        // the success response isn't retried
        verify(pulsarCommandSender, times(1))
                .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(), any());
        assertThat(lookupSemaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testWatchRequestGetsErrorWhenWatcherIsClosedBeforeSuccessResponseIsSent() throws Exception {
        doAnswer(invocationOnMock -> {
            // the watcher is closed while the success response waits for direct memory permits, the permit
            // request is then cancelled
            topicListService.deleteTopicListWatcher(13L);
            BooleanSupplier isCancelled = invocationOnMock.getArgument(5);
            assertThat(isCancelled.getAsBoolean()).isTrue();
            Function<Throwable, CompletableFuture<Void>> permitAcquireErrorHandler = invocationOnMock.getArgument(4);
            return permitAcquireErrorHandler.apply(
                    new AsyncSemaphore.PermitAcquireCancelledException("Permit acquisition was cancelled"));
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(),
                any());
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(Collections.singletonList("persistent://tenant/ns/topic1"));

        // the connection is still active, so the request gets an error response instead of no response
        verify(pulsarCommandSender, timeout(2000L)).sendErrorResponse(eq(7L), any(ServerError.class), anyString());
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(1));
    }

    @Test
    public void testDuplicateWatchRequestGetsErrorWhenUpdateFails() throws Exception {
        TopicListService.TopicListWatcher watcher =
                createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        // the update keeps failing until the listing is allowed to succeed
        AtomicBoolean listingFails = new AtomicBoolean(true);
        List<String> refreshedTopics = List.of("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2");
        doAnswer(invocationOnMock -> listingFails.get()
                ? CompletableFuture.failedFuture(new PulsarServerException("Metadata error"))
                : CompletableFuture.completedFuture(refreshedTopics))
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));

        // a duplicate watch request triggers an update and waits for it
        lookupSemaphore.acquire();
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                8,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);

        // the request gets an error response after the first failure instead of waiting for the retries
        verify(pulsarCommandSender, timeout(5000L)).sendErrorResponse(eq(8L), any(ServerError.class), anyString());
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(1));
        assertThat(watcher.isUpdatingTopics()).isTrue();

        // the update keeps being retried and completes once the listing succeeds
        listingFails.set(false);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(watcher.isUpdatingTopics()).isFalse();
            assertThat(watcher.getMatchingTopics()).containsExactlyInAnyOrderElementsOf(refreshedTopics);
        });
    }

    @Test
    public void testWatchRequestGetsErrorWhenWatcherIsRemovedBeforeFirstSendAttempt() throws Exception {
        createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        // the update triggered by the duplicate watch requests waits for the listing
        topicListFuture = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            long requestId = invocationOnMock.getArgument(0);
            if (requestId == 8L) {
                // the watcher is closed while the next waiting request is about to send its success response
                topicListService.deleteTopicListWatcher(13L);
            }
            return CompletableFuture.completedFuture(null);
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(),
                any());
        lookupSemaphore.release(2);
        for (long requestId = 8; requestId <= 9; requestId++) {
            lookupSemaphore.acquire();
            topicListService.handleWatchTopicList(
                    NamespaceName.get("tenant/ns"),
                    13,
                    requestId,
                    "persistent://tenant/ns/topic\\d",
                    topicsPatternImplementation, null,
                    lookupSemaphore);
        }

        topicListFuture.complete(Collections.singletonList("persistent://tenant/ns/topic1"));

        // request 9 finds the watcher removed before its first send attempt and gets an error response
        verify(pulsarCommandSender, timeout(2000L)).sendErrorResponse(eq(9L), any(ServerError.class), anyString());
        verify(pulsarCommandSender, never())
                .sendWatchTopicListSuccess(eq(9L), anyLong(), anyString(), any(), any(), any());
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(3));
    }

    @Test
    public void testWaitingWatchRequestRespondsWithSnapshotOfItsUpdate() throws Exception {
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        TopicListService.TopicListWatcher watcher = createWatcher(topics);
        // the update triggered by the duplicate watch requests waits for the listing
        topicListFuture = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            long requestId = invocationOnMock.getArgument(0);
            if (requestId == 8L) {
                // the metadata store connection is lost after the update has completed, which clears the matching
                // topics until the next update, while the next waiting request is about to respond
                watcher.onSessionEvent(SessionEvent.ConnectionLost);
            }
            return CompletableFuture.completedFuture(null);
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any(),
                any());
        lookupSemaphore.release(2);
        for (long requestId = 8; requestId <= 9; requestId++) {
            lookupSemaphore.acquire();
            topicListService.handleWatchTopicList(
                    NamespaceName.get("tenant/ns"),
                    13,
                    requestId,
                    "persistent://tenant/ns/topic\\d",
                    topicsPatternImplementation, null,
                    lookupSemaphore);
        }

        topicListFuture.complete(topics);

        // request 9 responds with the matching topics of the completed update, not with the cleared list
        verify(pulsarCommandSender, timeout(2000L)).sendWatchTopicListSuccess(eq(9L), eq(13L),
                eq(TopicList.calculateHash(topics)), eq(new ArrayList<>(topics)), any(), any());
    }

    @Test
    public void testFailedWatchRequestClosesTheWatcherItCreated() throws Exception {
        AtomicReference<TopicListService.TopicListWatcher> createdWatcher = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            invocationOnMock.callRealMethod();
            TopicListService.TopicListWatcher watcher = invocationOnMock.getArgument(0);
            createdWatcher.set(watcher);
            // the metadata store connection is lost while the watcher is being created, which starts an update
            watcher.onSessionEvent(SessionEvent.ConnectionLost);
            return null;
        }).when(topicResources).registerPersistentTopicListener(any(TopicListener.class));
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        topicListFuture.complete(Collections.singletonList("persistent://tenant/ns/topic1"));
        // the permits are granted and the watcher is created asynchronously
        Awaitility.await().untilAsserted(() -> assertThat(createdWatcher.get()).isNotNull());
        // the watch request waits for the update since the matching topics are incomplete
        Awaitility.await().untilAsserted(() -> assertThat(topicListService.getWatcherFuture(13)).isCompleted());
        assertThat(lookupSemaphore.availablePermits()).isZero();

        // the update fails, which fails the waiting watch request
        doReturn(CompletableFuture.failedFuture(new PulsarServerException("Metadata error")))
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));
        topicListService.updateTopicListWatcher(createdWatcher.get());

        // the request gets an error response, and the watcher that it created is removed, deregistered and closed
        verify(pulsarCommandSender, timeout(2000L)).sendErrorResponse(eq(7L), any(ServerError.class), anyString());
        Awaitility.await().untilAsserted(() -> assertThat(lookupSemaphore.availablePermits()).isEqualTo(1));
        assertThat(topicListService.getWatcherFuture(13)).isNull();
        verify(topicResources, timeout(2000L)).deregisterPersistentTopicListener(createdWatcher.get());
    }

    @Test
    public void testTopicChangesAreSentAfterPendingSuccessResponse() throws Exception {
        TopicListService.TopicListWatcher watcher =
                createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        // the success response of a watch request that waited for an update is being sent
        watcher.successResponseSendingStarted();
        assertThat(watcher.prepareUpdateTopics(null)).isTrue();
        // completing the update doesn't send the queued topic changes before the success response
        watcher.updateTopics(Collections.singletonList("persistent://tenant/ns/topic1"));

        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        Thread.sleep(500);
        verify(pulsarCommandSender, never())
                .sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());

        // the topic change is sent after the success response has been sent
        watcher.successResponseSendingCompleted();
        verify(pulsarCommandSender, timeout(2000L))
                .sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());
    }

    @Test
    public void testTopicChangesAreQueuedWhileUpdateIsInProgress() throws Exception {
        TopicListService.TopicListWatcher watcher =
                createWatcher(Collections.singletonList("persistent://tenant/ns/topic1"));
        // an update starts, for example after a metadata store session event
        assertThat(watcher.prepareUpdateTopics(null)).isTrue();
        // a send that was in progress before the update completes
        watcher.sendingCompleted();

        // a topic change during the update isn't sent, since the matching topics are being rebuilt
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        Thread.sleep(500);
        verify(pulsarCommandSender, never())
                .sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any(), any());
    }
}
