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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.PulsarResources;
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
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.awaitility.Awaitility;
import org.jspecify.annotations.NonNull;
import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
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

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        lookupSemaphore = new Semaphore(1);
        lookupSemaphore.acquire();
        topicListFuture = new CompletableFuture<>();

        AtomicReference<Consumer<Notification>> listenerRef = new AtomicReference<>();
        MetadataStoreExtended metadataStore = mock(MetadataStoreExtended.class);
        doAnswer(invocationOnMock -> {
            listenerRef.set(invocationOnMock.getArgument(0));
            return null;
        }).when(metadataStore).registerListener(any());
        topicResources = spy(new TopicResources(metadataStore));
        notificationConsumer = listenerRef.get();

        pulsar = mock(PulsarService.class);
        NamespaceService namespaceService = mock(NamespaceService.class);
        when(pulsar.getNamespaceService()).thenReturn(namespaceService);
        doAnswer(invocationOnMock -> topicListFuture)
                .when(namespaceService).getListOfUserTopics(any(), eq(CommandGetTopicsOfNamespace.Mode.PERSISTENT));
        when(pulsar.getPulsarResources()).thenReturn(mock(PulsarResources.class));
        when(pulsar.getPulsarResources().getTopicResources()).thenReturn(topicResources);

        BrokerService brokerService = mock(BrokerService.class);
        when(pulsar.getBrokerService()).thenReturn(brokerService);
        TopicListSizeResultCache topicListSizeResultCache = mock(TopicListSizeResultCache.class);
        when(brokerService.getTopicListSizeResultCache()).thenReturn(topicListSizeResultCache);
        TopicListSizeResultCache.ResultHolder resultHolder = mock(TopicListSizeResultCache.ResultHolder.class);
        doReturn(resultHolder).when(topicListSizeResultCache).getTopicListSize(anyString(), any());
        doReturn(CompletableFuture.completedFuture(1L)).when(resultHolder).getSizeAsync();

        memoryLimiter = new AsyncDualMemoryLimiterImpl(1_000_000, 10000, 500, 1_000_000, 10000, 500);
        doReturn(memoryLimiter).when(brokerService).getMaxTopicListInFlightLimiter();

        connection = mock(ServerCnx.class);
        when(connection.getRemoteAddress()).thenReturn(new InetSocketAddress(10000));
        pulsarCommandSender = mock(PulsarCommandSender.class);
        when(connection.getCommandSender()).thenReturn(pulsarCommandSender);
        when(connection.isActive()).thenReturn(true);
        when(pulsarCommandSender.sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsarCommandSender.sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any()))
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
                any());
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
                any());
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
        verifyNoInteractions(topicResources);
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
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any());
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));
        Collection<String> expectedTopics = new ArrayList<>(topics);
        verify(connection.getCommandSender(), timeout(2000L).times(3))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics), any());
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
                .sendWatchTopicListUpdate(eq(13L), eq(newTopics), any(), eq(hash), any());

        hash = TopicList.calculateHash(newTopics);
        notificationConsumer.accept(
                new Notification(NotificationType.Deleted, "/managed-ledgers/tenant/ns/persistent/topic1"));
        verify(connection.getCommandSender(), timeout(2000L))
                .sendWatchTopicListUpdate(eq(13L), eq(List.of()), eq(topics), eq(hash), any());
    }

    @Test
    public void testCommandWatchUpdateRetries() {
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
        AtomicInteger failureCount = new AtomicInteger(0);
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
        }).when(pulsarCommandSender).sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any());
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        notificationConsumer.accept(
                new Notification(NotificationType.Deleted, "/managed-ledgers/tenant/ns/persistent/topic2"));
        InOrder inOrder = inOrder(connection.getCommandSender());
        inOrder.verify(connection.getCommandSender(), timeout(2000L).times(3))
                .sendWatchTopicListUpdate(eq(13L), eq(newTopics), eq(List.of()), eq(hash), any());
        inOrder.verify(connection.getCommandSender(), timeout(2000L).times(1))
                .sendWatchTopicListUpdate(eq(13L), eq(List.of()), eq(newTopics), any(), any());
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
                .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any());
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));
        Collection<String> expectedTopics = List.of();
        verify(connection.getCommandSender(), timeout(2000L).times(1))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics), any());
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
                .sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any());
        topicListFuture.complete(topics);
        assertThat(topicListService.getWatcherFuture(13)).succeedsWithin(Duration.ofSeconds(2));
        Collection<String> expectedTopics = topics;
        verify(connection.getCommandSender(), timeout(2000L).times(1))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(expectedTopics), any());
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
                        eq(expectedHash), any());

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
                        eq(expectedHash), any());

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
                .sendWatchTopicListUpdate(eq(13L), any(), any(), any(), any());
    }
}