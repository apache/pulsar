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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.apache.pulsar.broker.service.ServerCnxThrottleTracker.ThrottleType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.netty.PulsarFlowControlHandler;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InProgressRequestFrameFlowControlTest {
    @Test
    public void shouldDeliverPooledSiblingAfterTopicLimitClearsWhileMemoryIsThrottled() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.onFrame = count -> {
                if (count == 1) {
                    pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
                }
            };
            pipeline.channel.writeInbound(frames(2));
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(1);
            assertThat(pipeline.tracker.throttledCount()).isEqualTo(2);
            pipeline.releaseFrames();
            assertThat(pipeline.controller.getRetainedFrameBytes()).isEqualTo(128);
            int readsBefore = pipeline.reads;

            pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(1);
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
            assertThat(pipeline.reads).isEqualTo(readsBefore);
            pipeline.releaseFrames();
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.controller.getRetainedFrameBytes()).isZero();
            assertThat(pipeline.channel.config().isAutoRead()).isTrue();
        }
    }

    @DataProvider
    public Object[][] businessLimits() {
        List<Object[]> cases = new ArrayList<>();
        for (ThrottleType type : ThrottleType.values()) {
            if (type != ThrottleType.InProgressRequestFrameMemoryExceeded) {
                cases.add(new Object[] {type, false});
                cases.add(new Object[] {type, true});
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "businessLimits")
    public void shouldRespectEveryBusinessLimitInEitherReleaseOrder(ThrottleType type, boolean memoryClearsFirst)
            throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.onFrame = count -> {
                if (count == 1) {
                    pipeline.tracker.markThrottled(type);
                }
            };
            pipeline.channel.writeInbound(frames(2));
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(1);
            int readsBefore = pipeline.reads;
            pipeline.channel.read();
            assertThat(pipeline.frames).hasSize(1);
            assertThat(pipeline.reads).isEqualTo(readsBefore);

            if (memoryClearsFirst) {
                pipeline.controller.setMode(InProgressRequestFrameMemoryLimitMode.DISABLED);
                pipeline.channel.runPendingTasks();
                assertThat(pipeline.frames).hasSize(1);
                assertThat(pipeline.channel.config().isAutoRead()).isFalse();
            }
            pipeline.tracker.unmarkThrottled(type);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(2);
            assertThat(pipeline.channel.config().isAutoRead()).isEqualTo(memoryClearsFirst);
            if (!memoryClearsFirst) {
                assertThat(pipeline.reads).isEqualTo(readsBefore);
            }
        }
    }

    @Test
    public void shouldStopAgainDuringDrainAndWaitForAllBusinessLimits() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.onFrame = count -> {
                if (count == 1) {
                    pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
                    pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
                    pipeline.tracker.markThrottled(ThrottleType.ConnectionMaxPendingPublishRequestsExceeded);
                } else if (count == 2) {
                    pipeline.tracker.markThrottled(ThrottleType.ResourceGroupPublishRate);
                }
            };
            pipeline.channel.writeInbound(frames(3));
            pipeline.channel.runPendingTasks();
            int readsBefore = pipeline.reads;
            pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            pipeline.tracker.unmarkThrottled(ThrottleType.ConnectionMaxPendingPublishRequestsExceeded);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(1);
            pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            // Resumption must not run reentrantly inside the state-change callback.
            assertThat(pipeline.frames).hasSize(1);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(2);
            pipeline.tracker.unmarkThrottled(ThrottleType.ResourceGroupPublishRate);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(3);
            assertThat(pipeline.reads).isEqualTo(readsBefore);
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
        }
    }

    @Test
    public void shouldRecheckBusinessLimitBeforeScheduledDrain() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.onFrame = count -> {
                if (count == 1) {
                    pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
                }
            };
            pipeline.channel.writeInbound(frames(2));
            pipeline.channel.runPendingTasks();
            pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            pipeline.tracker.markThrottled(ThrottleType.BrokerPublishRate);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(1);
            pipeline.tracker.unmarkThrottled(ThrottleType.BrokerPublishRate);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(2);
        }
    }

    @Test
    public void shouldDeliverAlreadyReadFramesDuringMemoryOnlyThrottleWithoutReadingAnEmptyQueue() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.channel.writeInbound(frames(1));
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
            int readsBefore = pipeline.reads;
            pipeline.channel.writeInbound(frames(2));
            assertThat(pipeline.frames).hasSize(3);
            pipeline.flowControl.drainQueuedMessages();
            pipeline.channel.read();
            assertThat(pipeline.reads).isEqualTo(readsBefore);
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
        }
    }

    @DataProvider
    public Object[][] partialFrames() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "partialFrames")
    public void shouldContinuePartialFrameAfterCompleteSibling(boolean partialHeader, boolean initiallyTopicThrottled)
            throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            if (initiallyTopicThrottled) {
                pipeline.onFrame = count -> {
                    if (count == 1) {
                        pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
                    }
                };
            }
            ByteBuf input = frames(1);
            if (partialHeader) {
                input.writeShort(0);
            } else {
                input.writeInt(8).writeZero(3);
            }
            int readsBefore = pipeline.pausedReads;
            pipeline.channel.writeInbound(input);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(1);
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
            if (initiallyTopicThrottled) {
                assertThat(pipeline.pausedReads).isEqualTo(readsBefore);
                pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
                pipeline.channel.runPendingTasks();
            }
            assertThat(pipeline.pausedReads).isEqualTo(readsBefore + 1);
            ByteBuf remaining = Unpooled.directBuffer(16, 16);
            if (partialHeader) {
                remaining.writeShort(8).writeZero(8);
            } else {
                remaining.writeZero(5);
            }
            pipeline.channel.writeInbound(remaining);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(2);
            assertThat(pipeline.frames.get(1).readableBytes()).isEqualTo(8);
            assertThat(pipeline.pausedReads).isEqualTo(readsBefore + 1);
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
        }
    }

    @Test
    public void shouldContinueMixedReadWhileMemoryAlreadyThrottledAndCoalescePartialOnlyReads() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.channel.writeInbound(frames(1));
            pipeline.channel.runPendingTasks();
            int readsBefore = pipeline.reads;
            pipeline.channel.writeInbound(frames(1).writeInt(8).writeZero(1));
            assertThat(pipeline.reads).isEqualTo(readsBefore + 1);
            // No complete frame: Netty itself issues exactly one continuation read.
            pipeline.channel.writeInbound(Unpooled.directBuffer(1, 1).writeZero(1));
            assertThat(pipeline.reads).isEqualTo(readsBefore + 2);
            pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
            pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.reads).isEqualTo(readsBefore + 2);
            pipeline.channel.writeInbound(Unpooled.directBuffer(6, 6).writeZero(6));
            assertThat(pipeline.frames).hasSize(3);
            assertThat(pipeline.reads).isEqualTo(readsBefore + 2);
        }
    }

    @Test
    public void shouldCompleteInitialPartialFrameUnderMemoryThrottle() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.channel.writeInbound(frames(0).writeInt(8).writeByte(0));
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).isEmpty();
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
            assertThat(pipeline.pausedReads).isEqualTo(1);
            pipeline.channel.writeInbound(Unpooled.directBuffer(7, 7).writeZero(7));
            assertThat(pipeline.frames).hasSize(1);
            assertThat(pipeline.pausedReads).isEqualTo(1);
            pipeline.releaseFrames();
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.controller.getRetainedFrameBytes()).isZero();
            assertThat(pipeline.channel.config().isAutoRead()).isTrue();
        }
    }

    @Test
    public void shouldNotContinuePartialFrameWhenDrainTriggersAnotherBusinessLimit() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.onFrame = count -> {
                if (count == 1) {
                    pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
                } else if (count == 2) {
                    pipeline.tracker.markThrottled(ThrottleType.ConnectionMaxPendingPublishRequestsExceeded);
                }
            };
            pipeline.channel.writeInbound(frames(2).writeInt(8).writeByte(0));
            pipeline.channel.runPendingTasks();
            int readsBefore = pipeline.reads;
            pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(2);
            assertThat(pipeline.reads).isEqualTo(readsBefore);
            pipeline.tracker.unmarkThrottled(ThrottleType.ConnectionMaxPendingPublishRequestsExceeded);
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.reads).isEqualTo(readsBefore + 1);
            pipeline.channel.writeInbound(Unpooled.directBuffer(7, 7).writeZero(7));
            assertThat(pipeline.frames).hasSize(3);
            assertThat(pipeline.reads).isEqualTo(readsBefore + 1);
        }
    }

    @Test
    public void shouldRejectOversizedFrameAfterContinuingPartialHeader() throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.channel.writeInbound(frames(1).writeShort(0));
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.channel.config().isAutoRead()).isFalse();
            // Complete the header with a length greater than the configured frame limit, including padding.
            ByteBuf remaining = Unpooled.directBuffer(2, 2).writeShort(1024 + Commands.MESSAGE_SIZE_FRAME_PADDING);
            assertThatThrownBy(() -> pipeline.channel.writeInbound(remaining))
                    .isInstanceOf(TooLongFrameException.class);
        }
    }

    @DataProvider
    public Object[][] closeTimings() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "closeTimings")
    public void shouldReleaseQueueWhenClosedBeforeOrDuringDrain(boolean duringDrain) throws Exception {
        try (Pipeline pipeline = new Pipeline()) {
            pipeline.onFrame = count -> {
                if (count == 1) {
                    pipeline.tracker.markThrottled(ThrottleType.TopicPublishRate);
                } else if (duringDrain && count == 2) {
                    pipeline.channel.pipeline().close();
                }
            };
            pipeline.channel.writeInbound(frames(3).writeInt(8).writeByte(0));
            pipeline.channel.runPendingTasks();
            int readsBefore = pipeline.reads;
            pipeline.tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
            if (!duringDrain) {
                // EmbeddedChannel.close() runs queued tasks before closing; close through the pipeline to
                // exercise a close event that precedes the already-scheduled resume task.
                pipeline.channel.pipeline().close();
            }
            pipeline.channel.runPendingTasks();
            assertThat(pipeline.frames).hasSize(duringDrain ? 2 : 1);
            pipeline.releaseFrames();
            assertThat(pipeline.controller.getRetainedFrameBytes()).isZero();
            assertThat(pipeline.reads).isEqualTo(readsBefore);
        }
    }

    @Test
    public void shouldApplyLatestThresholdToExistingAndNewChannels() throws Exception {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 512, () -> 0, Long.MAX_VALUE);
        try (Pipeline first = new Pipeline(controller); Pipeline second = new Pipeline(controller)) {
            first.channel.writeInbound(frames(1));
            second.channel.writeInbound(frames(1));
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(256);
            assertThat(first.channel.config().isAutoRead()).isTrue();
            assertThat(second.channel.config().isAutoRead()).isTrue();

            controller.updateLimitBytes(128);
            first.channel.runPendingTasks();
            second.channel.runPendingTasks();
            assertThat(first.channel.config().isAutoRead()).isFalse();
            assertThat(second.channel.config().isAutoRead()).isFalse();
            try (Pipeline third = new Pipeline(controller)) {
                third.channel.runPendingTasks();
                // A new connection is subject to the same global pressure even before retaining any frame.
                assertThat(third.channel.config().isAutoRead()).isFalse();
                assertThat(first.channel.isActive()).isTrue();
                assertThat(second.channel.isActive()).isTrue();
                assertThat(third.channel.isActive()).isTrue();
                controller.updateLimitBytes(512);
                first.channel.runPendingTasks();
                second.channel.runPendingTasks();
                third.channel.runPendingTasks();
                assertThat(first.channel.config().isAutoRead()).isTrue();
                assertThat(second.channel.config().isAutoRead()).isTrue();
                assertThat(third.channel.config().isAutoRead()).isTrue();
                assertThat(controller.getRetainedFrameBytes()).isEqualTo(256);
                first.releaseFrames();
                second.releaseFrames();
            }
        }
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    private static ByteBuf frames(int count) {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(128, 128);
        for (int i = 0; i < count; i++) {
            buffer.writeInt(8).writeZero(8);
        }
        return buffer;
    }

    private static final class Pipeline implements AutoCloseable {
        private final InProgressRequestFrameMemoryController controller;
        private final ServerCnx cnx = mock(ServerCnx.class);
        private final ServerCnxThrottleTracker tracker = new ServerCnxThrottleTracker(cnx);
        private final PulsarFlowControlHandler flowControl =
                new PulsarFlowControlHandler(tracker::isMessageDeliveryPaused);
        private final List<ByteBuf> frames = new ArrayList<>();
        private final EmbeddedChannel channel = new EmbeddedChannel();
        private IntConsumer onFrame = count -> { };
        private int delivered;
        private int reads;
        private int pausedReads;

        Pipeline() throws Exception {
            this(new InProgressRequestFrameMemoryController(
                    InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE));
        }

        Pipeline(InProgressRequestFrameMemoryController controller) throws Exception {
            this.controller = controller;
            when(cnx.ctx()).thenAnswer(invocation -> channel.pipeline().context(cnx));
            when(cnx.isActive()).thenAnswer(invocation -> channel.isActive());
            when(cnx.getBrokerService()).thenReturn(mock(BrokerService.class));
            when(cnx.getThrottleTracker()).thenReturn(tracker);
            doAnswer(invocation -> {
                frames.add(invocation.getArgument(1));
                onFrame.accept(++delivered);
                return null;
            }).when(cnx).channelRead(ArgumentMatchers.any(), ArgumentMatchers.any());
            doAnswer(invocation -> {
                ChannelHandlerContext ctx = invocation.getArgument(0);
                ctx.fireExceptionCaught(invocation.getArgument(1));
                return null;
            }).when(cnx).exceptionCaught(ArgumentMatchers.any(), ArgumentMatchers.any());
            channel.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                @Override
                public void read(ChannelHandlerContext ctx) {
                    reads++;
                    if (!ctx.channel().config().isAutoRead()) {
                        pausedReads++;
                    }
                    ctx.read();
                }
            });
            channel.pipeline().addLast(new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
            channel.pipeline().addLast(flowControl);
            channel.pipeline().addLast(ServerCnx.SERVER_CNX_HANDLER_NAME, cnx);
        }

        void releaseFrames() {
            frames.forEach(ByteBuf::release);
            frames.clear();
        }

        @Override
        public void close() {
            try {
                channel.finishAndReleaseAll();
            } finally {
                releaseFrames();
            }
            if (controller.getRegisteredListenerCount() == 0) {
                assertThat(controller.getRetainedFrameBytes()).isZero();
            }
        }
    }
}
