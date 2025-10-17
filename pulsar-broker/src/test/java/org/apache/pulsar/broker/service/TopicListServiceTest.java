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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.collections4.ListUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.semaphore.AsyncSemaphore;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        lookupSemaphore = new Semaphore(1);
        lookupSemaphore.acquire();
        topicListFuture = new CompletableFuture<>();

        AtomicReference<Consumer<Notification>> listenerRef = new AtomicReference<>();
        MetadataStore metadataStore = mock(MetadataStore.class);
        doAnswer(invocationOnMock -> {
            listenerRef.set(invocationOnMock.getArgument(0));
            return null;
        }).when(metadataStore).registerListener(any());
        topicResources = spy(new TopicResources(metadataStore));
        notificationConsumer = listenerRef.get();

        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getNamespaceService()).thenReturn(mock(NamespaceService.class));
        when(pulsar.getPulsarResources()).thenReturn(mock(PulsarResources.class));
        when(pulsar.getPulsarResources().getTopicResources()).thenReturn(topicResources);
        when(pulsar.getNamespaceService().getListOfPersistentTopics(any())).thenReturn(topicListFuture);

        connection = mock(ServerCnx.class);
        when(connection.getRemoteAddress()).thenReturn(new InetSocketAddress(10000));
        pulsarCommandSender = mock(PulsarCommandSender.class);
        when(connection.getCommandSender()).thenReturn(pulsarCommandSender);
        when(connection.isActive()).thenReturn(true);
        when(pulsarCommandSender.sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsarCommandSender.sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));


        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(connection.ctx()).thenReturn(ctx);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);

        topicListService = new TopicListService(pulsar, connection, true, 30);

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
        Assert.assertEquals(1, lookupSemaphore.availablePermits());
        verify(topicResources).registerPersistentTopicListener(
                eq(NamespaceName.get("tenant/ns")), any(TopicListService.TopicListWatcher.class));
        verify(connection.getCommandSender()).sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(topics), any());
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
        Assert.assertEquals(1, lookupSemaphore.availablePermits());
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

        CommandWatchTopicListClose watchTopicListClose = new CommandWatchTopicListClose()
                .setRequestId(8)
                .setWatcherId(13);
        topicListService.handleWatchTopicListClose(watchTopicListClose);
        verify(topicResources).deregisterPersistentTopicListener(any(TopicListService.TopicListWatcher.class));
    }

    @Test
    public void testCommandWatchSuccessRetries() {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                "persistent://tenant/ns/topic\\d",
                topicsPatternImplementation, null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        String hash = TopicList.calculateHash(topics);
        doAnswer(invocationOnMock -> {
            Runnable runnable = invocationOnMock.getArgument(0);
            // run immediately
            runnable.run();
            return mock(ScheduledFuture.class);
        }).when(eventLoop).schedule(any(Runnable.class), anyLong(), any());
        AtomicInteger failureCount = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            if (failureCount.incrementAndGet() < 3) {
                Throwable failure = new AsyncSemaphore.PermitAcquireTimeoutException("Acquire timed out");
                Consumer<Throwable> permitAcquireErrorHandler = invocationOnMock.getArgument(4);
                permitAcquireErrorHandler.accept(failure);
                return CompletableFuture.failedFuture(failure);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).when(pulsarCommandSender).sendWatchTopicListSuccess(anyLong(), anyLong(), anyString(), any(), any());
        topicListFuture.complete(topics);
        verify(connection.getCommandSender(), times(3))
                .sendWatchTopicListSuccess(eq(7L), eq(13L), eq(hash), eq(topics), any());
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

        List<String> newTopics = Collections.singletonList("persistent://tenant/ns/topic2");
        String hash = TopicList.calculateHash(ListUtils.union(topics, newTopics));
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        verify(connection.getCommandSender())
                .sendWatchTopicListUpdate(eq(13L), eq(newTopics), any(), eq(hash), any());

        hash = TopicList.calculateHash(newTopics);
        notificationConsumer.accept(
                new Notification(NotificationType.Deleted, "/managed-ledgers/tenant/ns/persistent/topic1"));
        verify(connection.getCommandSender())
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

        List<String> newTopics = Collections.singletonList("persistent://tenant/ns/topic2");
        String hash = TopicList.calculateHash(ListUtils.union(topics, newTopics));
        doAnswer(invocationOnMock -> {
            Runnable runnable = invocationOnMock.getArgument(0);
            // run immediately
            runnable.run();
            return mock(ScheduledFuture.class);
        }).when(eventLoop).schedule(any(Runnable.class), anyLong(), any());
        AtomicInteger failureCount = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            if (failureCount.incrementAndGet() < 3) {
                Throwable failure = new AsyncSemaphore.PermitAcquireTimeoutException("Acquire timed out");
                Consumer<Throwable> permitAcquireErrorHandler = invocationOnMock.getArgument(4);
                permitAcquireErrorHandler.accept(failure);
                return CompletableFuture.failedFuture(failure);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).when(pulsarCommandSender).sendWatchTopicListUpdate(anyLong(), any(), any(), anyString(), any());
        notificationConsumer.accept(
                new Notification(NotificationType.Created, "/managed-ledgers/tenant/ns/persistent/topic2"));
        verify(connection.getCommandSender(), times(3))
                .sendWatchTopicListUpdate(eq(13L), eq(newTopics), eq(List.of()), eq(hash), any());
    }
}
