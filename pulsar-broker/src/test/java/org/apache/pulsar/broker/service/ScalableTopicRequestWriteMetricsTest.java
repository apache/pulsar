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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.scalable.ConsumerAssignment;
import org.apache.pulsar.broker.service.scalable.ConsumerSession;
import org.apache.pulsar.broker.service.scalable.ScalableTopicMetrics;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.BaseCommand.Type;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** The request observation must follow the first response write, not command construction or registration. */
@Test(groups = "broker", timeOut = 30000)
public class ScalableTopicRequestWriteMetricsTest {
    private static final String TOPIC = "topic://public/default/request-write-metrics";
    private static final long CONSUMER_ID = 7;
    private static final long REQUEST_ID = 42;

    private PulsarTestContext context;
    private ServiceConfiguration config;
    private BrokerService broker;
    private ScalableTopicService scalableService;
    private ScalableTopicMetrics metrics;
    private CollectorRegistry registry;
    private EmbeddedChannel channel;
    private ServerCnx cnx;
    private ResponseWriteGate gate;
    private final List<Type> interceptedTypes = new ArrayList<>();

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        channel = null;
        config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0));
        config.setBacklogQuotaCheckEnabled(false);
        context = PulsarTestContext.builderForNonStartableContext().config(config).spyByDefault().build();
        broker = context.getBrokerService();
        interceptedTypes.clear();
        var interceptor = mock(BrokerInterceptor.class);
        when(broker.getInterceptor()).thenReturn(interceptor);
        doAnswer(invocation -> {
            BaseCommand command = invocation.getArgument(0);
            // Commands may be recycled immediately after interception; retain only the immutable type.
            interceptedTypes.add(command.getType());
            return null;
        }).when(interceptor).onPulsarCommand(any(), any());
        registry = new CollectorRegistry();
        metrics = new ScalableTopicMetrics(OpenTelemetry.noop().getMeter("write-test"), "test", registry);
        when(broker.getScalableTopicMetrics()).thenReturn(metrics);
        scalableService = mock(ScalableTopicService.class);
        when(broker.getScalableTopicService()).thenReturn(scalableService);
        when(scalableService.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(assignment(1)));
        gate = new ResponseWriteGate();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (channel != null) {
            gate.discardPending();
            channel.finishAndReleaseAll();
        }
        if (metrics != null) {
            metrics.close();
        }
        if (context != null) {
            context.close();
        }
    }

    @DataProvider
    public Object[][] writeOutcomes() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "writeOutcomes")
    public void coordinatorResponseWaitsForWriteWithoutChangingAssignmentOrder(boolean writeSucceeds) {
        var registration = new CompletableFuture<ConsumerAssignment>();
        when(scalableService.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenAnswer(invocation -> {
                    ServerCnx connection = invocation.getArgument(5);
                    connection.sendScalableConsumerAssignment(CONSUMER_ID, ConsumerSession.toProto(assignment(1)));
                    return registration;
                });
        connect();
        gate.hold(Type.SCALABLE_TOPIC_SUBSCRIBE_RESPONSE, REQUEST_ID);
        subscribe(REQUEST_ID);
        waitForHeldResponse();
        assertThat(registration).isNotDone();
        assertUnobserved("subscribe");

        // A rebalance while the subscribe write is pending must remain an update, not answer the request again.
        cnx.sendScalableConsumerAssignment(CONSUMER_ID, ConsumerSession.toProto(assignment(2)));
        channel.runPendingTasks();
        assertThat(gate.commands()).extracting(BaseCommand::getType)
                .containsExactly(Type.SCALABLE_TOPIC_SUBSCRIBE_RESPONSE, Type.SCALABLE_TOPIC_ASSIGNMENT_UPDATE);
        assertUnobserved("subscribe");

        completeResponseWrite(writeSucceeds);
        assertOnlyOutcome("subscribe", writeSucceeds);
        if (writeSucceeds) {
            assertThat(readResponse().getScalableTopicSubscribeResponse().getAssignment().getLayoutEpoch())
                    .isEqualTo(1);
            gate.succeedNext();
            assertThat(readResponse().getScalableTopicAssignmentUpdate().getAssignment().getLayoutEpoch()).isEqualTo(2);
        }
        registration.completeExceptionally(new IllegalStateException("registration failed after its response"));
        channel.runPendingTasks();
        assertOnlyOutcome("subscribe", writeSucceeds);
        if (writeSucceeds) {
            assertThat(gate.commands()).isEmpty();
            assertThat(channel.outboundMessages()).isEmpty();
        }
    }

    @Test(dataProvider = "writeOutcomes")
    public void fallbackSubscribeResponseWaitsForWrite(boolean writeSucceeds) {
        var registration = new CompletableFuture<ConsumerAssignment>();
        when(scalableService.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                .thenReturn(registration);
        connect();
        gate.hold(Type.SCALABLE_TOPIC_SUBSCRIBE_RESPONSE, REQUEST_ID);
        subscribe(REQUEST_ID);
        channel.runPendingTasks();
        assertThat(gate.commands()).isEmpty();
        assertUnobserved("subscribe");

        registration.complete(assignment(1));
        waitForHeldResponse();
        assertUnobserved("subscribe");
        completeResponseWrite(writeSucceeds);
        assertOnlyOutcome("subscribe", writeSucceeds);
        if (writeSucceeds) {
            assertThat(readResponse().getScalableTopicSubscribeResponse().hasError()).isFalse();
        }
        channel.runPendingTasks();
        assertOnlyOutcome("subscribe", writeSucceeds);
    }

    @Test(dataProvider = "writeOutcomes")
    public void unknownUnsubscribeResponseWaitsForWrite(boolean writeSucceeds) {
        connect();
        gate.hold(Type.SUCCESS, REQUEST_ID);
        channel.writeInbound(Commands.newScalableTopicUnsubscribe(REQUEST_ID, CONSUMER_ID));
        waitForHeldResponse();
        assertUnobserved("unsubscribe");
        completeResponseWrite(writeSucceeds);
        assertOnlyOutcome("unsubscribe", writeSucceeds);
        if (writeSucceeds) {
            assertThat(readResponse().getSuccess().getRequestId()).isEqualTo(REQUEST_ID);
        }
    }

    @DataProvider
    public Object[][] unregisterAndWriteOutcomes() {
        return new Object[][]{{true, true}, {true, false}, {false, true}, {false, false}};
    }

    @Test(dataProvider = "unregisterAndWriteOutcomes")
    public void registeredUnsubscribeResponseWaitsForWrite(boolean unregisterSucceeds, boolean writeSucceeds) {
        var unregistration = new CompletableFuture<Void>();
        when(scalableService.unregisterConsumer(any(), anyString(), anyString(), anyLong()))
                .thenReturn(unregistration);
        connect();
        subscribe(1);
        assertThat(readResponse().getScalableTopicSubscribeResponse().hasError()).isFalse();
        gate.hold(unregisterSucceeds ? Type.SUCCESS : Type.ERROR, REQUEST_ID);
        channel.writeInbound(Commands.newScalableTopicUnsubscribe(REQUEST_ID, CONSUMER_ID));
        channel.runPendingTasks();
        assertThat(gate.commands()).isEmpty();
        assertUnobserved("unsubscribe");

        if (unregisterSucceeds) {
            unregistration.complete(null);
        } else {
            unregistration.completeExceptionally(new IllegalStateException("unregister failed"));
        }
        waitForHeldResponse();
        assertUnobserved("unsubscribe");
        completeResponseWrite(writeSucceeds);
        assertOnlyOutcome("unsubscribe", unregisterSucceeds && writeSucceeds);
        verify(scalableService).unregisterConsumer(any(), anyString(), anyString(), anyLong());
        if (writeSucceeds) {
            BaseCommand response = readResponse();
            assertThat(response.getType()).isEqualTo(unregisterSucceeds ? Type.SUCCESS : Type.ERROR);
            if (!unregisterSucceeds) {
                assertThat(response.getError().getError()).isEqualTo(ServerError.UnknownError);
            }
        }
    }

    @DataProvider
    public Object[][] subscribeErrors() {
        return new Object[][]{{"disabled", ServerError.NotAllowedError}, {"invalid", ServerError.InvalidTopicName},
                {"unavailable", ServerError.ServiceNotReady}, {"registration", ServerError.UnknownError}};
    }

    @Test(dataProvider = "subscribeErrors")
    public void subscribeErrorsWaitForTheirErrorResponseWrite(String error, ServerError expectedError) {
        if (error.equals("disabled")) {
            config.setScalableTopicsEnabled(false);
        } else if (error.equals("unavailable")) {
            when(broker.getScalableTopicService()).thenReturn(null);
        } else if (error.equals("registration")) {
            when(scalableService.registerConsumer(any(), anyString(), anyString(), anyLong(), any(), any()))
                    .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("register failed")));
        }
        connect();
        gate.hold(Type.SCALABLE_TOPIC_SUBSCRIBE_RESPONSE, REQUEST_ID);
        channel.writeInbound(Commands.newScalableTopicSubscribe(REQUEST_ID,
                error.equals("invalid") ? "bad-domain://public/default/topic" : TOPIC,
                "subscription", "consumer", CONSUMER_ID, ScalableConsumerType.STREAM));
        waitForHeldResponse();
        assertThat(gate.commands().getFirst().getScalableTopicSubscribeResponse().getError()).isEqualTo(expectedError);
        assertUnobserved("subscribe");
        gate.succeedNext();
        readResponse();
        assertOnlyOutcome("subscribe", false);
    }

    @DataProvider
    public Object[][] classicResponseTypes() {
        return new Object[][]{{Type.SUCCESS}, {Type.ERROR}};
    }

    @Test(dataProvider = "classicResponseTypes")
    public void untrackedClassicResponsesKeepVoidPromiseAndSingleInterception(Type type) {
        connect();
        gate.hold(type, REQUEST_ID);
        if (type == Type.SUCCESS) {
            cnx.getCommandSender().sendSuccessResponse(REQUEST_ID);
        } else {
            cnx.getCommandSender().sendErrorResponse(REQUEST_ID, ServerError.UnknownError, "classic error");
        }
        waitForHeldResponse();
        assertThat(gate.pending.getFirst().promise().isVoid()).as("untracked writes retain the original void promise")
                .isTrue();
        assertThat(interceptedTypes).containsExactly(type);
        assertUnobserved("subscribe");
        assertUnobserved("unsubscribe");
        gate.succeedNext();
        assertThat(readResponse().getType()).isEqualTo(type);
        assertThat(channel.isActive()).isTrue();
        assertThat(interceptedTypes).as("completion must not intercept the same command a second time")
                .containsExactly(type);
        assertUnobserved("subscribe");
        assertUnobserved("unsubscribe");
    }

    @Test
    public void disabledUnsubscribeErrorWaitsForWriteWithoutAddingInterception() {
        config.setScalableTopicsEnabled(false);
        connect();
        gate.hold(Type.ERROR, REQUEST_ID);
        channel.writeInbound(Commands.newScalableTopicUnsubscribe(REQUEST_ID, CONSUMER_ID));
        waitForHeldResponse();
        assertThat(gate.commands().getFirst().getError().getError()).isEqualTo(ServerError.NotAllowedError);
        assertThat(interceptedTypes).as("disabled unsubscribe retains its original direct-write path").isEmpty();
        assertUnobserved("unsubscribe");
        gate.succeedNext();
        assertThat(readResponse().getError().getError()).isEqualTo(ServerError.NotAllowedError);
        assertOnlyOutcome("unsubscribe", false);
        assertThat(channel.isActive()).isTrue();
        assertThat(interceptedTypes).isEmpty();
    }

    private void connect() {
        cnx = new ServerCnx(context.getPulsarService());
        channel = new EmbeddedChannel(gate,
                new LengthFieldBasedFrameDecoder(5 * 1024 * 1024, 0, 4, 0, 4), cnx);
        channel.writeInbound(Commands.newConnect("none", "", null));
        assertThat(readResponse().getType()).isEqualTo(Type.CONNECTED);
        interceptedTypes.clear();
    }

    private void subscribe(long requestId) {
        channel.writeInbound(Commands.newScalableTopicSubscribe(requestId, TOPIC, "subscription", "consumer",
                CONSUMER_ID, ScalableConsumerType.STREAM));
    }

    private static ConsumerAssignment assignment(long epoch) {
        return new ConsumerAssignment(epoch, List.of());
    }

    private void waitForHeldResponse() {
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            channel.runPendingTasks();
            return !gate.commands().isEmpty();
        });
        assertThat(channel.outboundMessages()).as("response has not reached the outbound transport").isEmpty();
    }

    private void completeResponseWrite(boolean success) {
        if (success) {
            gate.succeedNext();
        } else {
            var failure = new IOException("injected response write failure");
            gate.failNext(failure);
            channel.runPendingTasks();
            assertThat(gate.failures).as("the original void-promise failure still reaches the pipeline")
                    .containsExactly(failure);
            assertThat(channel.isActive()).as("retain the original transport failure behavior").isFalse();
        }
        channel.runPendingTasks();
    }

    private void assertUnobserved(String type) {
        assertThat(count(type, "success")).as("%s success before response completion", type).isZero();
        assertThat(count(type, "failure")).as("%s failure before response completion", type).isZero();
    }

    private void assertOnlyOutcome(String type, boolean success) {
        assertThat(count(type, "success")).isEqualTo(success ? 1 : 0);
        assertThat(count(type, "failure")).isEqualTo(success ? 0 : 1);
    }

    private double count(String type, String status) {
        Double result = registry.getSampleValue("pulsar_scalable_topic_request_duration_seconds_count",
                new String[]{"cluster", "request_type", "operation_status"}, new String[]{"test", type, status});
        return result == null ? 0 : result;
    }

    private BaseCommand readResponse() {
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            channel.runPendingTasks();
            return !channel.outboundMessages().isEmpty();
        });
        ByteBuf frame = channel.readOutbound();
        try {
            return decode(frame);
        } finally {
            frame.release();
        }
    }

    private static BaseCommand decode(ByteBuf frame) {
        // A non-retained duplicate has independent indices; decoding must not consume or release the held frame.
        ByteBuf copy = frame.duplicate();
        copy.readInt();
        int commandSize = copy.readInt();
        BaseCommand command = new BaseCommand();
        command.parseFrom(copy, commandSize);
        return new BaseCommand().copyFrom(command);
    }

    private record PendingWrite(ByteBuf frame, ChannelPromise promise, BaseCommand command) {}

    private static final class ResponseWriteGate extends ChannelDuplexHandler {
        private final Deque<PendingWrite> pending = new ArrayDeque<>();
        private final List<Throwable> failures = new ArrayList<>();
        private ChannelHandlerContext ctx;
        private Type heldType;
        private long heldRequestId;

        @Override
        public void handlerAdded(ChannelHandlerContext context) {
            ctx = context;
        }

        private void hold(Type type, long requestId) {
            heldType = type;
            heldRequestId = requestId;
        }

        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
            if (message instanceof ByteBuf frame) {
                BaseCommand command = decode(frame);
                if (!pending.isEmpty() || matches(command)) {
                    pending.addLast(new PendingWrite(frame, promise, command));
                    heldType = null;
                    return;
                }
            }
            context.write(message, promise);
        }

        @Override
        public void flush(ChannelHandlerContext context) {
            if (pending.isEmpty()) {
                context.flush();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            failures.add(cause);
            context.fireExceptionCaught(cause);
        }

        private boolean matches(BaseCommand command) {
            if (command.getType() != heldType) {
                return false;
            }
            return switch (command.getType()) {
                case SCALABLE_TOPIC_SUBSCRIBE_RESPONSE ->
                        command.getScalableTopicSubscribeResponse().getRequestId() == heldRequestId;
                case SUCCESS -> command.getSuccess().getRequestId() == heldRequestId;
                case ERROR -> command.getError().getRequestId() == heldRequestId;
                default -> false;
            };
        }

        private List<BaseCommand> commands() {
            return pending.stream().map(PendingWrite::command).toList();
        }

        private void succeedNext() {
            PendingWrite write = pending.removeFirst();
            ctx.writeAndFlush(write.frame(), write.promise());
        }

        private void failNext(Throwable cause) {
            PendingWrite write = pending.removeFirst();
            write.frame().release();
            write.promise().tryFailure(cause);
        }

        private void discardPending() {
            while (!pending.isEmpty()) {
                failNext(new ClosedChannelException());
            }
        }
    }
}
