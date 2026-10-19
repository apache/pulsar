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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ScalableTopicsWatcherSessionMetricsTest {
    private static final NamespaceName NAMESPACE = NamespaceName.get("tenant/ns");

    private ScalableTopicResources resources;
    private ScalableTopicMetrics metrics;
    private ScalableTopicMetrics.Request request;
    private ChannelHandlerContext ctx;
    private ServerCnx cnx;
    private ScheduledExecutorService scheduler;
    private ScalableTopicsWatcherSession session;
    private CompletableFuture<List<String>> initialListing;
    private final Queue<ChannelPromise> writes = new ArrayDeque<>();
    private final Queue<Runnable> scheduled = new ArrayDeque<>();

    @BeforeMethod
    public void setUp() {
        session = null;
        writes.clear();
        scheduled.clear();
        resources = mock(ScalableTopicResources.class);
        metrics = mock(ScalableTopicMetrics.class);
        request = mock(ScalableTopicMetrics.Request.class);
        ctx = mock(ChannelHandlerContext.class);
        cnx = mock(ServerCnx.class);
        scheduler = mock(ScheduledExecutorService.class);
        when(cnx.ctx()).thenReturn(ctx);
        when(resources.namespacePath(NAMESPACE)).thenReturn("/topics/tenant/ns");
        initialListing = new CompletableFuture<>();
        when(resources.findScalableTopicsByPropertiesAsync(NAMESPACE, Map.of())).thenReturn(initialListing);
        when(resources.getScalableTopicMetadataAsync(any(), anyBoolean())).thenReturn(
                CompletableFuture.completedFuture(Optional.of(ScalableTopicMetadata.builder().build())));
        when(ctx.writeAndFlush(any())).thenAnswer(invocation -> {
            ByteBuf frame = invocation.getArgument(0);
            frame.release();
            ChannelPromise promise = new DefaultChannelPromise(mock(Channel.class), ImmediateEventExecutor.INSTANCE);
            writes.add(promise);
            return promise;
        });
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            scheduled.add(invocation.getArgument(0));
            return null;
        });
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (session != null) {
            session.close();
        }
        writes.forEach(promise -> promise.tryFailure(new IllegalStateException("Test finished")));
    }

    private void createSession(String clientHash) {
        session = new ScalableTopicsWatcherSession(1L, NAMESPACE, Map.of(), clientHash,
                cnx, resources, scheduler, metrics, request);
    }

    @Test
    public void initializationIsCountedAndConcurrentCloseCountsOnce() throws Exception {
        createSession(null);
        verify(metrics).sessionOpened(ScalableTopicMetrics.WatchType.NAMESPACE);
        CompletableFuture<Void> started = session.start();
        assertThat(started).as("initial listing is still in flight").isNotDone();
        verify(metrics, never()).sessionClosed(any());

        CompletableFuture.allOf(CompletableFuture.runAsync(session::close),
                CompletableFuture.runAsync(session::close)).get(10, TimeUnit.SECONDS);
        initialListing.complete(List.of());
        started.get(10, TimeUnit.SECONDS);
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
        verify(request).failure();
        verify(request, never()).success();
        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    public void closeBeforeStartRetainsOriginalRegistrationBehavior() {
        createSession(null);
        session.close();
        CompletableFuture<Void> started = session.start();
        initialListing.complete(List.of());
        assertThat(started).isCompleted();
        verify(resources).registerNamespaceListener(session);
        verify(resources).deregisterNamespaceListener(session);
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
        verify(request).failure();
    }

    @Test
    public void closeRacingRegistrationRetainsOriginalLateListenerBehavior() {
        createSession(null);
        Set<ScalableTopicResources.NamespaceListener> listeners = new HashSet<>();
        doAnswer(invocation -> {
            session.close(); // The close wins before the registration becomes visible.
            listeners.add(session);
            return null;
        }).when(resources).registerNamespaceListener(session);
        doAnswer(invocation -> {
            listeners.remove(session);
            return null;
        }).when(resources).deregisterNamespaceListener(session);

        CompletableFuture<Void> started = session.start();
        assertThat(started).isNotDone();
        assertThat(listeners).as("metrics must not repair the pre-existing late-registration leak")
                .containsExactly(session);
        verify(resources).deregisterNamespaceListener(session);
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
        verify(request).failure();
    }

    @Test
    public void failedStartDoesNotCloseSessionOrCompleteRequest() {
        createSession(null);
        CompletableFuture<Void> started = session.start();
        initialListing.completeExceptionally(new IllegalStateException("Listing failed"));
        assertThat(started).isCompletedExceptionally();
        verify(metrics, never()).sessionClosed(any());
        verify(resources, never()).deregisterNamespaceListener(any());
        verifyNoInteractions(request);
        session.close();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
        verify(resources).deregisterNamespaceListener(session);
        verify(request).failure();
        verify(metrics, never()).recordWatchUpdate(anyBoolean());
    }

    @Test
    public void initialRequestWaitsForWriteCompletion() {
        createSession(null);
        session.start();
        initialListing.complete(List.of());
        verifyNoInteractions(request);
        writes.remove().setSuccess();
        session.close();
        verify(request).success();
        verify(request, never()).failure();
        verify(metrics, never()).recordWatchUpdate(anyBoolean());
    }

    @Test
    public void failedSnapshotWriteFailsRequestWithoutClosingSession() {
        createSession(null);
        session.start();
        initialListing.complete(List.of());
        writes.remove().setFailure(new IllegalStateException("Write failed"));
        verify(metrics, never()).sessionClosed(any());
        verify(resources, never()).deregisterNamespaceListener(any());
        session.onNotification(new Notification(NotificationType.Created, "/topics/tenant/ns/topic"));
        scheduled.remove().run();
        writes.remove().setSuccess();
        verify(ctx, times(2)).writeAndFlush(any());
        session.close();
        verify(request).failure();
        verify(request, never()).success();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
        verify(metrics, never()).recordWatchUpdate(anyBoolean());
    }

    @Test
    public void closeBeforeSnapshotWriteCompletesDoesNotCompleteRequestTwice() {
        createSession(null);
        session.start();
        initialListing.complete(List.of());
        session.close();
        writes.remove().setSuccess();
        verify(request).failure();
        verify(request, never()).success();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
    }

    @Test
    public void matchingReconnectHashSucceedsWithoutResponse() {
        createSession(TopicList.calculateHash(List.of("topic://tenant/ns/topic")));
        session.start();
        initialListing.complete(List.of("topic://tenant/ns/topic"));
        session.close();
        verify(request).success();
        verify(request, never()).failure();
        verify(ctx, never()).writeAndFlush(any());
        verify(metrics, never()).recordWatchUpdate(anyBoolean());
    }

    @Test
    public void namespaceDiffsAreNotLayoutUpdates() {
        createSession(null);
        session.start();
        initialListing.complete(List.of());
        writes.remove().setSuccess();
        session.onNotification(new Notification(NotificationType.Created, "/topics/tenant/ns/topic"));
        scheduled.remove().run();
        writes.remove().setSuccess();
        session.onNotification(new Notification(NotificationType.Deleted, "/topics/tenant/ns/topic"));
        scheduled.remove().run();
        writes.remove().setFailure(new IllegalStateException("Diff write failed"));
        verify(ctx, times(3)).writeAndFlush(any());
        verify(request).success();
        verify(request, never()).failure();
        verify(metrics, never()).recordWatchUpdate(anyBoolean());
    }

    @Test
    public void metricOnlyStopDoesNotCloseSessionOrStopItsWork() {
        createSession(null);
        session.stopMetricsTracking();
        session.stopMetricsTracking();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
        verify(resources, never()).deregisterNamespaceListener(any());
        verifyNoInteractions(request);

        session.start();
        initialListing.complete(List.of());
        writes.remove().setSuccess();
        session.onNotification(new Notification(NotificationType.Created, "/topics/tenant/ns/topic"));
        scheduled.remove().run();
        writes.remove().setSuccess();
        verify(ctx, times(2)).writeAndFlush(any());
        verify(resources).registerNamespaceListener(session);
        verify(resources).getScalableTopicMetadataAsync(any(), anyBoolean());
        verify(resources, never()).deregisterNamespaceListener(any());
        verify(request).success();
        verify(request, never()).failure();
        verify(metrics, never()).recordWatchUpdate(anyBoolean());

        session.close();
        session.close();
        verify(resources).deregisterNamespaceListener(session);
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
    }

    @Test
    public void synchronousStartFailurePropagatesWithoutLifecycleChanges() {
        createSession(null);
        IllegalStateException failure = new IllegalStateException("Registration failed");
        doThrow(failure).when(resources).registerNamespaceListener(session);
        assertThatThrownBy(session::start).isSameAs(failure);
        verify(metrics, never()).sessionClosed(any());
        verify(resources, never()).deregisterNamespaceListener(any());
        verifyNoInteractions(request);
    }

    @Test
    public void synchronousSnapshotWriteFailureDoesNotCloseSession() {
        createSession(null);
        IllegalStateException failure = new IllegalStateException("Write failed");
        doAnswer(invocation -> {
            ByteBuf frame = invocation.getArgument(0);
            frame.release();
            throw failure;
        }).when(ctx).writeAndFlush(any());
        CompletableFuture<Void> started = session.start();
        initialListing.complete(List.of());
        assertThat(started).isCompletedExceptionally();
        verify(metrics, never()).sessionClosed(any());
        verify(resources, never()).deregisterNamespaceListener(any());
        verifyNoInteractions(request);
    }

}
