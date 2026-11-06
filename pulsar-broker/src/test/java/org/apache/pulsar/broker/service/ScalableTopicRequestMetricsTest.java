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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.VoidChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.scalable.ConsumerAssignment;
import org.apache.pulsar.broker.service.scalable.ConsumerSession;
import org.apache.pulsar.broker.service.scalable.DagWatchSession;
import org.apache.pulsar.broker.service.scalable.ScalableTopicMetrics;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.broker.service.scalable.ScalableTopicsWatcherSession;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.ScalableConsumerAssignment;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 30000)
public class ScalableTopicRequestMetricsTest {
    private PulsarTestContext context;
    private BrokerService brokerService;
    private ServerCnx cnx;
    private EmbeddedChannel channel;
    private CollectorRegistry registry;
    private ScalableTopicMetrics metrics;
    private ServiceConfiguration config;
    private final List<DagWatchSession> dagSessions = new ArrayList<>();
    private final List<ScalableTopicsWatcherSession> namespaceSessions = new ArrayList<>();

    @BeforeMethod
    public void setup() throws Exception {
        dagSessions.clear();
        namespaceSessions.clear();
        config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0));
        config.setBacklogQuotaCheckEnabled(false);
        context = PulsarTestContext.builderForNonStartableContext().config(config).spyByDefault().build();
        brokerService = context.getBrokerService();
        registry = new CollectorRegistry();
        metrics = new ScalableTopicMetrics(OpenTelemetry.noop().getMeter("test"), "test", registry);
        when(brokerService.getScalableTopicMetrics()).thenReturn(metrics);
    }

    private void connect() {
        connect(new ServerCnx(context.getPulsarService()));
    }

    private void connect(ServerCnx connection) {
        cnx = connection;
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(5 * 1024 * 1024, 0, 4, 0, 4), cnx);
        channel.writeInbound(Commands.newConnect("none", "", null));
        assertThat(readResponse().getType()).isEqualTo(BaseCommand.Type.CONNECTED);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
        dagSessions.forEach(DagWatchSession::close);
        namespaceSessions.forEach(ScalableTopicsWatcherSession::close);
        if (metrics != null) {
            metrics.close();
        }
        if (context != null) {
            context.close();
        }
    }

    @Test
    public void disabledRequestsAreFailures() {
        config.setScalableTopicsEnabled(false);
        connect();
        channel.writeInbound(Commands.newScalableTopicLookup(1, "topic://public/default/t"));
        readResponse();
        channel.writeInbound(Commands.newWatchScalableTopics(2, "public/default", null, null));
        readResponse();
        channel.writeInbound(Commands.newScalableTopicSubscribe(3, "topic://public/default/t", "sub", "c", 1,
                ScalableConsumerType.STREAM));
        readResponse();
        channel.writeInbound(Commands.newScalableTopicUnsubscribe(4, 1));
        readResponse();
        for (String type : List.of("lookup", "watch_topics", "subscribe", "unsubscribe")) {
            assertRequestCount(type, "failure", 1);
            assertRequestCount(type, "success", 0);
        }
        assertSessions("topic", 0);
        assertSessions("namespace", 0);
    }

    @Test
    public void authorizationFailureIsMeasuredWithoutOpeningWatch() {
        var authorization = mock(AuthorizationService.class);
        when(brokerService.isAuthorizationEnabled()).thenReturn(true);
        when(brokerService.getAuthorizationService()).thenReturn(authorization);
        var allowed = new CompletableFuture<Boolean>();
        when(authorization.allowTopicOperationAsync(any(), any(), any(), any(), any(), any()))
                .thenReturn(allowed);
        connect();
        channel.writeInbound(Commands.newScalableTopicLookup(1, "topic://public/default/t"));
        channel.runPendingTasks();
        assertRequestCount("lookup", "failure", 0);
        assertSessions("topic", 0);
        allowed.complete(false);
        assertThat(readResponse().getScalableTopicUpdate().hasError()).isTrue();
        assertRequestCount("lookup", "failure", 1);
        assertSessions("topic", 0);
    }

    @Test
    public void metricsPreserveLookupStartAfterLateAuthorization() {
        var resources = spyScalableResources();
        doReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(0)))
                .when(brokerService).fetchPartitionedTopicMetadataAsync(any());
        var authorization = mock(AuthorizationService.class);
        when(brokerService.isAuthorizationEnabled()).thenReturn(true);
        when(brokerService.getAuthorizationService()).thenReturn(authorization);
        var allowed = new CompletableFuture<Boolean>();
        when(authorization.allowTopicOperationAsync(any(), any(), any(), any(), any(), any()))
                .thenReturn(allowed);
        connect();
        channel.writeInbound(Commands.newScalableTopicLookup(1, "persistent://public/default/t"));
        channel.close();
        allowed.complete(true);
        // Authorization still runs its original continuation, even after disconnect.
        verify(resources).registerPathListener(any());
        verify(resources, never()).deregisterPathListener(any());
        channel.runPendingTasks();
        assertRequestCount("lookup", "failure", 1);
        assertSessions("topic", 0);
    }

    @Test
    public void metricsPreserveSubscribeRegistrationAfterLateAuthorization() {
        var authorization = mock(AuthorizationService.class);
        when(brokerService.isAuthorizationEnabled()).thenReturn(true);
        when(brokerService.getAuthorizationService()).thenReturn(authorization);
        var allowed = new CompletableFuture<Boolean>();
        when(authorization.allowTopicOperationAsync(any(), any(), any(), any(), any(), any()))
                .thenReturn(allowed);
        var service = mock(ScalableTopicService.class);
        when(brokerService.getScalableTopicService()).thenReturn(service);
        when(service.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new ConsumerAssignment(1, List.of())));
        connect();
        channel.writeInbound(Commands.newScalableTopicSubscribe(1, "topic://public/default/t", "sub", "c", 1,
                ScalableConsumerType.STREAM));
        channel.close();
        allowed.complete(true);
        // The metrics observer must not introduce an authorization executor hop or skip registration.
        verify(service).registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any());
        channel.runPendingTasks();
        assertRequestCount("subscribe", "failure", 1);
        assertRequestCount("subscribe", "success", 0);
    }

    @Test
    public void lookupReplacementCloseAndDisconnect() {
        var resources = spyScalableResources();
        doReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(0)))
                .when(brokerService).fetchPartitionedTopicMetadataAsync(any());
        connect();
        for (int i = 0; i < 2; i++) {
            channel.writeInbound(Commands.newScalableTopicLookup(1, "persistent://public/default/t"));
            assertThat(readResponse().getScalableTopicUpdate().hasError()).isFalse();
            assertSessions("topic", 1);
        }
        verify(resources, times(2)).registerPathListener(any());
        verify(resources, never()).deregisterPathListener(any());
        assertRequestCount("lookup", "success", 2);
        assertThat(sample("pulsar_scalable_topic_watch_update_total",
                new String[]{"cluster", "operation_status"}, new String[]{"test", "success"})).isZero();
        channel.writeInbound(Commands.newScalableTopicClose(1));
        channel.writeInbound(Commands.newScalableTopicClose(1));
        assertSessions("topic", 0);
        verify(resources, times(1)).deregisterPathListener(any());
        channel.writeInbound(Commands.newScalableTopicLookup(2, "persistent://public/default/t"));
        readResponse();
        assertSessions("topic", 1);
        channel.close();
        assertSessions("topic", 0);
    }

    @Test
    public void namespaceReplacementAndDisconnect() {
        var resources = spyScalableResources();
        connect();
        for (int i = 0; i < 2; i++) {
            channel.writeInbound(Commands.newWatchScalableTopics(1, "public/default", null, null));
            assertThat(readResponse().getWatchScalableTopicsUpdate().hasError()).isFalse();
            assertSessions("namespace", 1);
        }
        verify(resources, times(2)).registerNamespaceListener(any());
        verify(resources, never()).deregisterNamespaceListener(any());
        assertRequestCount("watch_topics", "success", 2);
        channel.writeInbound(Commands.newWatchScalableTopicsClose(1));
        channel.writeInbound(Commands.newWatchScalableTopicsClose(1));
        assertSessions("namespace", 0);
        verify(resources, times(1)).deregisterNamespaceListener(any());
        channel.writeInbound(Commands.newWatchScalableTopics(2, "public/default", null, null));
        readResponse();
        channel.close();
        assertSessions("namespace", 0);
    }

    @Test
    public void subscribeMeasuresFirstResponseNotRegistrationCompletion() {
        var service = mock(ScalableTopicService.class);
        when(brokerService.getScalableTopicService()).thenReturn(service);
        var registration = new CompletableFuture<ConsumerAssignment>();
        when(service.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenAnswer(invocation -> {
                    ServerCnx connection = invocation.getArgument(5);
                    connection.sendScalableConsumerAssignment(1,
                            ConsumerSession.toProto(new ConsumerAssignment(1, List.of())));
                    return registration;
                });
        connect();
        channel.writeInbound(Commands.newScalableTopicSubscribe(1, "topic://public/default/t", "sub", "c", 1,
                ScalableConsumerType.STREAM));
        assertThat(readResponse().getScalableTopicSubscribeResponse().hasError()).isFalse();
        assertThat(registration).isNotDone();
        assertRequestCount("subscribe", "success", 1);
        cnx.sendScalableConsumerAssignment(1, ConsumerSession.toProto(new ConsumerAssignment(2, List.of())));
        readResponse();
        registration.completeExceptionally(new IllegalStateException("failure after response"));
        channel.runPendingTasks();
        assertRequestCount("subscribe", "success", 1);
        assertRequestCount("subscribe", "failure", 0);
    }

    @Test
    public void subscribeFailureAndUnsubscribeRetry() {
        var service = mock(ScalableTopicService.class);
        when(brokerService.getScalableTopicService()).thenReturn(service);
        when(service.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed")))
                .thenReturn(CompletableFuture.completedFuture(new ConsumerAssignment(1, List.of())));
        connect();
        for (int i = 0; i < 2; i++) {
            channel.writeInbound(Commands.newScalableTopicSubscribe(i, "topic://public/default/t", "sub", "c", 1,
                    ScalableConsumerType.STREAM));
            readResponse();
        }
        assertRequestCount("subscribe", "failure", 1);
        assertRequestCount("subscribe", "success", 1);
        when(service.unregisterConsumer(any(), anyString(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed")))
                .thenReturn(CompletableFuture.completedFuture(null));
        for (int i = 0; i < 3; i++) {
            channel.writeInbound(Commands.newScalableTopicUnsubscribe(10 + i, 1));
            readResponse();
        }
        assertRequestCount("unsubscribe", "failure", 1);
        assertRequestCount("unsubscribe", "success", 2);
    }

    @Test
    public void successfulWriteKeepsItsOutcomeWhenDisconnectPrecedesListenerNotification() {
        class DeferredWriteCnx extends ServerCnx {
            private ChannelHandlerContext output;

            DeferredWriteCnx() {
                super(ScalableTopicRequestMetricsTest.this.context.getPulsarService());
            }

            @Override
            public ChannelHandlerContext ctx() {
                return output == null ? super.ctx() : output;
            }
        }
        var service = mock(ScalableTopicService.class);
        when(brokerService.getScalableTopicService()).thenReturn(service);
        when(service.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new ConsumerAssignment(1, List.of())));
        var connection = new DeferredWriteCnx();
        connect(connection);
        Deque<Runnable> notifications = new ArrayDeque<>();
        EventLoop notifier = mock(EventLoop.class);
        doAnswer(invocation -> {
            notifications.addLast(invocation.getArgument(0));
            return null;
        }).when(notifier).execute(any(Runnable.class));
        Channel promiseChannel = mock(Channel.class);
        when(promiseChannel.eventLoop()).thenReturn(notifier);
        ChannelHandlerContext output = mock(ChannelHandlerContext.class);
        when(output.channel()).thenReturn(channel);
        when(output.voidPromise()).thenReturn(new VoidChannelPromise(promiseChannel, true));
        doAnswer(invocation -> {
            ByteBuf response = invocation.getArgument(0);
            response.release();
            ChannelPromise promise = invocation.getArgument(1);
            promise.setSuccess();
            return promise;
        }).when(output).writeAndFlush(any(), any(ChannelPromise.class));
        connection.output = output;
        channel.writeInbound(Commands.newScalableTopicSubscribe(1, "topic://public/default/t", "sub", "c", 1,
                ScalableConsumerType.STREAM));
        channel.runPendingTasks();
        assertThat(notifications).as("the write succeeded but its listeners are queued").hasSize(1);
        assertRequestCount("subscribe", "success", 0);
        assertRequestCount("subscribe", "failure", 0);
        channel.close();
        assertRequestCount("subscribe", "failure", 0);
        notifications.removeFirst().run();
        assertRequestCount("subscribe", "success", 1);
        assertRequestCount("subscribe", "failure", 0);
    }

    @Test
    public void customSenderOverridesKeepOriginalDispatchWithoutInventedWriteMetrics() {
        class CustomSenderCnx extends ServerCnx {
            private PulsarCommandSender alternate;

            CustomSenderCnx() {
                super(ScalableTopicRequestMetricsTest.this.context.getPulsarService());
            }

            @Override
            public PulsarCommandSender getCommandSender() {
                return alternate == null ? super.getCommandSender() : alternate;
            }
        }
        var service = mock(ScalableTopicService.class);
        when(brokerService.getScalableTopicService()).thenReturn(service);
        when(service.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new ConsumerAssignment(1, List.of())));
        var connection = new CustomSenderCnx();
        connect(connection);
        AtomicInteger responses = new AtomicInteger();
        connection.alternate = new PulsarCommandSenderImpl(null, connection,
                brokerService.getMaxTopicListInFlightLimiter()) {
            @Override
            public void sendScalableTopicSubscribeResponse(long requestId, ScalableConsumerAssignment assignment) {
                assertThat(requestId).isEqualTo(1);
                assertThat(assignment.getLayoutEpoch()).isEqualTo(1);
                responses.incrementAndGet();
            }
        };
        channel.writeInbound(Commands.newScalableTopicSubscribe(1, "topic://public/default/t", "sub", "c", 1,
                ScalableConsumerType.STREAM));
        channel.runPendingTasks();
        assertThat(responses.get()).as("the old virtual sender override still receives the response").isEqualTo(1);
        assertThat(channel.outboundMessages()).isEmpty();
        assertRequestCount("subscribe", "success", 0);
        assertRequestCount("subscribe", "failure", 0);
        channel.close();
        assertRequestCount("subscribe", "failure", 0);
    }

    private ScalableTopicResources spyScalableResources() {
        var resources = spy(context.getPulsarService().getPulsarResources().getScalableTopicResources());
        var pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getScalableTopicResources()).thenReturn(resources);
        doReturn(pulsarResources).when(context.getPulsarService()).getPulsarResources();
        doAnswer(invocation -> {
            dagSessions.add(invocation.getArgument(0));
            return invocation.callRealMethod();
        }).when(resources).registerPathListener(any());
        doAnswer(invocation -> {
            namespaceSessions.add(invocation.getArgument(0));
            return invocation.callRealMethod();
        }).when(resources).registerNamespaceListener(any());
        return resources;
    }

    private void assertRequestCount(String type, String status, double expected) {
        assertThat(sample("pulsar_scalable_topic_request_duration_seconds_count",
                new String[]{"cluster", "request_type", "operation_status"}, new String[]{"test", type, status}))
                .as("%s %s requests", type, status).isEqualTo(expected);
    }

    private void assertSessions(String type, double expected) {
        assertThat(sample("pulsar_scalable_topic_watch_session_count",
                new String[]{"cluster", "watch_type"}, new String[]{"test", type}))
                .as("open %s watches", type).isEqualTo(expected);
    }

    private double sample(String name, String[] labels, String[] values) {
        Double value = registry.getSampleValue(name, labels, values);
        return value == null ? 0 : value;
    }

    private BaseCommand readResponse() {
        await().until(() -> {
            channel.runPendingTasks();
            return !channel.outboundMessages().isEmpty();
        });
        ByteBuf frame = channel.readOutbound();
        try {
            frame.readInt();
            int commandSize = frame.readInt();
            var command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            return command;
        } finally {
            frame.release();
        }
    }
}
