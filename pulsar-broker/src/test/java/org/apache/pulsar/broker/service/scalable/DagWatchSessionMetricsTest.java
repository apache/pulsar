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
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class DagWatchSessionMetricsTest {
    private static final TopicName TOPIC = TopicName.get("topic://tenant/ns/topic");
    private static final ScalableTopicLayoutResponse RESPONSE =
            new ScalableTopicLayoutResponse(1L, Map.of(), null, null, null, null);

    private ScalableTopicResources resources;
    private ScalableTopicMetrics metrics;
    private ScalableTopicMetrics.Request request;
    private ChannelHandlerContext ctx;
    private MetadataStore store;
    private DagWatchSession session;
    private CompletableFuture<Optional<ScalableTopicMetadata>> initialRead;
    private final Queue<ChannelPromise> writes = new ArrayDeque<>();

    @BeforeMethod
    public void setUp() {
        writes.clear();
        resources = mock(ScalableTopicResources.class);
        metrics = mock(ScalableTopicMetrics.class);
        request = mock(ScalableTopicMetrics.Request.class);
        ctx = mock(ChannelHandlerContext.class);
        store = mock(MetadataStore.class);
        BrokerService brokerService = mock(BrokerService.class);
        ServerCnx cnx = mock(ServerCnx.class);
        when(brokerService.getScalableTopicMetrics()).thenReturn(metrics);
        when(cnx.ctx()).thenReturn(ctx);
        when(resources.topicPath(TOPIC)).thenReturn("/topics/tenant/ns/topic");
        when(resources.controllerLockPath(TOPIC)).thenReturn("/topics/tenant/ns/topic/controller");
        when(resources.getStore()).thenReturn(store);
        when(store.get(any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        initialRead = new CompletableFuture<>();
        when(resources.getScalableTopicMetadataAsync(TOPIC, true)).thenReturn(initialRead);
        when(ctx.writeAndFlush(any())).thenAnswer(invocation -> {
            ByteBuf frame = invocation.getArgument(0);
            frame.release();
            ChannelPromise promise = new DefaultChannelPromise(mock(Channel.class), ImmediateEventExecutor.INSTANCE);
            writes.add(promise);
            return promise;
        });
        session = new DagWatchSession(1L, TOPIC, cnx, resources, brokerService, false, request);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        session.close();
        writes.forEach(promise -> promise.tryFailure(new IllegalStateException("Test finished")));
    }

    @Test
    public void initializationIsCountedAndConcurrentCloseCountsOnce() throws Exception {
        verify(metrics).sessionOpened(ScalableTopicMetrics.WatchType.TOPIC);
        CompletableFuture<ScalableTopicLayoutResponse> started = session.start();
        assertThat(started).as("initial metadata is still in flight").isNotDone();
        verify(metrics, never()).sessionClosed(any());

        CompletableFuture.allOf(CompletableFuture.runAsync(session::close),
                CompletableFuture.runAsync(session::close)).get(10, TimeUnit.SECONDS);
        initialRead.complete(Optional.of(ScalableTopicMetadata.builder().build()));
        session.pushInitialResponse(started.get(10, TimeUnit.SECONDS));

        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
        verify(resources, times(2)).deregisterPathListener(session);
        verify(request).failure();
        verify(request, never()).success();
        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    public void closeBeforeStartRetainsOriginalRegistrationBehavior() {
        session.close();
        CompletableFuture<ScalableTopicLayoutResponse> started = session.start();
        initialRead.complete(Optional.of(ScalableTopicMetadata.builder().build()));
        assertThat(started).isCompleted();
        verify(resources).registerPathListener(session);
        verify(resources).deregisterPathListener(session);
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
        verify(request).failure();
    }

    @Test
    public void closeRacingRegistrationRetainsOriginalLateListenerBehavior() {
        Set<ScalableTopicResources.MetadataPathListener> listeners = new HashSet<>();
        doAnswer(invocation -> {
            session.close(); // The close wins before the registration becomes visible.
            listeners.add(session);
            return null;
        }).when(resources).registerPathListener(session);
        doAnswer(invocation -> {
            listeners.remove(session);
            return null;
        }).when(resources).deregisterPathListener(session);

        CompletableFuture<ScalableTopicLayoutResponse> started = session.start();
        assertThat(started).isNotDone();
        assertThat(listeners).as("metrics must not repair the pre-existing late-registration leak")
                .containsExactly(session);
        verify(resources).deregisterPathListener(session);
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
        verify(request).failure();
    }

    @Test
    public void failedStartDoesNotCloseSessionOrCompleteRequest() {
        CompletableFuture<ScalableTopicLayoutResponse> started = session.start();
        initialRead.completeExceptionally(new IllegalStateException("Metadata unavailable"));
        assertThat(started).isCompletedExceptionally();
        verify(resources, never()).deregisterPathListener(any());
        verify(metrics, never()).sessionClosed(any());
        verifyNoInteractions(request);
        session.close();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
        verify(resources).deregisterPathListener(session);
        verify(request).failure();
        verify(metrics, never()).recordWatchUpdate(true);
        verify(metrics, never()).recordWatchUpdate(false);
    }

    @Test
    public void initialRequestWaitsForWriteAndSnapshotIsNotAnUpdate() {
        session.pushInitialResponse(RESPONSE);
        verifyNoInteractions(request);
        verify(metrics, never()).recordWatchUpdate(true);
        verify(metrics, never()).recordWatchUpdate(false);

        writes.remove().setSuccess();
        session.close();
        verify(request).success();
        verify(request, never()).failure();
        verify(metrics, never()).recordWatchUpdate(true);
        verify(metrics, never()).recordWatchUpdate(false);
    }

    @Test
    public void failedInitialWriteFailsRequestWithoutClosingSession() {
        session.pushInitialResponse(RESPONSE);
        writes.remove().setFailure(new IllegalStateException("Write failed"));
        verify(resources, never()).deregisterPathListener(any());
        verify(metrics, never()).sessionClosed(any());
        session.pushUpdate(RESPONSE);
        writes.remove().setSuccess();
        verify(ctx, times(2)).writeAndFlush(any());
        session.close();
        verify(request).failure();
        verify(request, never()).success();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
        verify(metrics, never()).recordWatchUpdate(false);
    }

    @Test
    public void closeBeforeInitialWriteCompletesDoesNotCompleteRequestTwice() {
        session.pushInitialResponse(RESPONSE);
        session.close();
        writes.remove().setSuccess();
        verify(request).failure();
        verify(request, never()).success();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
    }

    @Test
    public void eachSubsequentWriteRecordsItsCompletionStatus() {
        session.pushInitialResponse(RESPONSE);
        writes.remove().setSuccess();
        session.pushUpdate(RESPONSE);
        session.pushUpdate(RESPONSE);
        verify(metrics, never()).recordWatchUpdate(true);
        verify(metrics, never()).recordWatchUpdate(false);

        writes.remove().setSuccess();
        writes.remove().setFailure(new IllegalStateException("Write failed"));
        verify(metrics).recordWatchUpdate(true);
        verify(metrics).recordWatchUpdate(false);
        verify(request).success();
        session.close();
        session.pushUpdate(RESPONSE);
        verify(ctx, times(3)).writeAndFlush(any());
    }

    @Test
    public void notificationOvertakingInitialReadCompletesFirstResponseRequest() {
        CompletableFuture<ScalableTopicLayoutResponse> started = session.start();
        session.onMetadataChanged(ScalableTopicMetadata.builder().build());
        verifyNoInteractions(request);
        writes.remove().setSuccess();
        verify(request).success();
        verify(metrics, never()).recordWatchUpdate(true);

        initialRead.complete(Optional.of(ScalableTopicMetadata.builder().build()));
        session.pushInitialResponse(started.getNow(null));
        writes.remove().setSuccess();
        verify(request).success();
        verify(metrics, never()).recordWatchUpdate(true);

        session.pushUpdate(RESPONSE);
        writes.remove().setSuccess();
        verify(metrics).recordWatchUpdate(true);
    }

    @Test
    public void failedNotificationReadCountsAsFailedUpdate() {
        session.onNotification(new Notification(NotificationType.Modified, "/topics/tenant/ns/topic"));
        initialRead.completeExceptionally(new IllegalStateException("Metadata read failed"));
        verify(metrics).recordWatchUpdate(false);
        verify(ctx, never()).writeAndFlush(any());
        verifyNoInteractions(request);
    }

    @Test
    public void asynchronousLayoutBuildFailureCountsAsFailedUpdate() {
        CompletableFuture<Optional<GetResult>> controllerRead =
                new CompletableFuture<>();
        when(store.get(any())).thenReturn(controllerRead);
        session.onMetadataChanged(ScalableTopicMetadata.builder().build());
        verify(metrics, never()).recordWatchUpdate(false);
        controllerRead.completeExceptionally(new IllegalStateException("Controller read failed"));
        verify(metrics).recordWatchUpdate(false);
        verify(ctx, never()).writeAndFlush(any());
        verifyNoInteractions(request);
    }

    @Test
    public void synchronousLayoutBuildFailureCountsAsFailedUpdate() {
        ScalableTopicMetadata invalid = mock(ScalableTopicMetadata.class);
        when(invalid.getSegments()).thenThrow(new IllegalStateException("Invalid layout"));
        assertThatThrownBy(() -> session.onMetadataChanged(invalid))
                .isInstanceOf(IllegalStateException.class).hasMessage("Invalid layout");
        verify(metrics).recordWatchUpdate(false);
        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    public void metricOnlyStopDoesNotCloseSessionOrStopItsWork() {
        session.stopMetricsTracking();
        session.stopMetricsTracking();
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
        verify(resources, never()).deregisterPathListener(any());
        verifyNoInteractions(request);

        CompletableFuture<ScalableTopicLayoutResponse> started = session.start();
        initialRead.complete(Optional.of(ScalableTopicMetadata.builder().build()));
        session.pushInitialResponse(started.getNow(null));
        writes.remove().setSuccess();
        session.onMetadataChanged(ScalableTopicMetadata.builder().build());
        writes.remove().setSuccess();
        session.onNotification(new Notification(NotificationType.Modified, "/topics/tenant/ns/topic"));
        writes.remove().setSuccess();
        verify(ctx, times(3)).writeAndFlush(any());
        verify(store, times(3)).get(any());
        verify(resources, times(2)).getScalableTopicMetadataAsync(TOPIC, true);
        verify(resources, never()).deregisterPathListener(any());
        verify(request).success();
        verify(request, never()).failure();
        verify(metrics, never()).recordWatchUpdate(true);
        verify(metrics, never()).recordWatchUpdate(false);

        session.close();
        session.close();
        verify(resources, times(2)).deregisterPathListener(session);
        verify(metrics).sessionClosed(ScalableTopicMetrics.WatchType.TOPIC);
    }

    @Test
    public void synchronousStartFailurePropagatesWithoutLifecycleChanges() {
        IllegalStateException failure = new IllegalStateException("Registration failed");
        doThrow(failure).when(resources).registerPathListener(session);
        assertThatThrownBy(session::start).isSameAs(failure);
        verify(metrics, never()).sessionClosed(any());
        verify(resources, never()).deregisterPathListener(any());
        verifyNoInteractions(request);
    }

    @Test
    public void synchronousNotificationReadFailurePropagatesAfterRecording() {
        IllegalStateException failure = new IllegalStateException("Read failed");
        when(resources.getScalableTopicMetadataAsync(TOPIC, true)).thenThrow(failure);
        assertThatThrownBy(() -> session.onNotification(
                new Notification(NotificationType.Modified, "/topics/tenant/ns/topic"))).isSameAs(failure);
        verify(metrics).recordWatchUpdate(false);
        verify(metrics, never()).sessionClosed(any());
        verify(resources, never()).deregisterPathListener(any());
        verifyNoInteractions(request);
    }

    @Test
    public void synchronousPushFailurePropagatesWithoutClosingSession() {
        IllegalStateException failure = new IllegalStateException("Write failed");
        doAnswer(invocation -> {
            ByteBuf frame = invocation.getArgument(0);
            frame.release();
            throw failure;
        }).when(ctx).writeAndFlush(any());
        assertThatThrownBy(() -> session.pushInitialResponse(RESPONSE)).isSameAs(failure);
        verify(request).failure();
        verify(metrics, never()).sessionClosed(any());
        verify(resources, never()).deregisterPathListener(any());
        assertThatThrownBy(() -> session.pushUpdate(RESPONSE)).isSameAs(failure);
        verify(ctx, times(2)).writeAndFlush(any());
        verify(metrics).recordWatchUpdate(false);
    }

}
