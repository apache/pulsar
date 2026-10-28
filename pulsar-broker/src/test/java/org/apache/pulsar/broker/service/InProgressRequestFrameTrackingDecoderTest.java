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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteOrder;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InProgressRequestFrameTrackingDecoderTest {

    @DataProvider
    public Object[][] bufferTypes() {
        return new Object[][] { { false }, { true } };
    }

    @Test
    public void shouldNotCountHeapBuffersAsDirectMemory() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        try {
            assertThat(channel.writeInbound(Unpooled.buffer(128, 128).writeInt(8).writeZero(8))).isTrue();
            ByteBuf frame = channel.readInbound();
            assertThat(controller.getRetainedFrameBytes()).isZero();
            frame.release();
        } finally {
            channel.finishAndReleaseAll();
        }
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldUseLatestThrottleStateWhenEventLoopNotificationsAreDelayed() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        ServerCnx serverCnx = mock(ServerCnx.class);
        ServerCnxThrottleTracker throttleTracker = mock(ServerCnxThrottleTracker.class);
        when(serverCnx.getThrottleTracker()).thenReturn(throttleTracker);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));
        channel.pipeline().addLast(ServerCnx.SERVER_CNX_HANDLER_NAME, serverCnx);
        try {
            controller.acquire(101);
            controller.onInboundRead();
            assertThat(limiter.isThrottled()).isTrue();
            controller.release(101);
            assertThat(limiter.isThrottled()).isFalse();

            channel.runPendingTasks();
            verify(throttleTracker, never()).markThrottled(
                    ServerCnxThrottleTracker.ThrottleType.InProgressRequestFrameMemoryExceeded);
            // Both transitions coalesce into one task that applies only the latest state.
            verify(throttleTracker).unmarkThrottled(
                    ServerCnxThrottleTracker.ThrottleType.InProgressRequestFrameMemoryExceeded);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test(dataProvider = "bufferTypes")
    public void shouldRetainRootCapacityUntilAllFramesAreReleased(boolean pooled) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));
        ByteBuf firstFrame = null;
        ByteBuf secondFrame = null;
        try {
            assertThat(channel.writeInbound(frames(pooled, 128, 8, 8))).isTrue();
            firstFrame = channel.readInbound();
            secondFrame = channel.readInbound();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            assertThat(limiter.isThrottled()).isTrue();

            firstFrame.release();
            firstFrame = null;
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);

            secondFrame.release();
            secondFrame = null;
            assertThat(controller.getRetainedFrameBytes()).isZero();
            assertThat(limiter.isThrottled()).isFalse();
        } finally {
            ReferenceCountUtil.safeRelease(firstFrame);
            ReferenceCountUtil.safeRelease(secondFrame);
            channel.finishAndReleaseAll();
        }
    }

    @Test(dataProvider = "bufferTypes")
    public void shouldWaitForRetainedDuplicateBeforeReleasingRootCapacity(boolean pooled) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        ByteBuf frame = null;
        ByteBuf duplicate = null;
        try {
            assertThat(channel.writeInbound(frames(pooled, 128, 8))).isTrue();
            frame = channel.readInbound();
            duplicate = frame.retainedDuplicate();
            frame.release();
            frame = null;

            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            duplicate.release();
            duplicate = null;
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            ReferenceCountUtil.safeRelease(frame);
            ReferenceCountUtil.safeRelease(duplicate);
            channel.finishAndReleaseAll();
        }
    }

    @Test(dataProvider = "bufferTypes")
    public void shouldWaitForRetainedFrameBeforeReleasingRootCapacity(boolean pooled) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        try {
            assertThat(channel.writeInbound(frames(pooled, 128, 8))).isTrue();
            ByteBuf frame = channel.readInbound();
            frame.retain();
            frame.release();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            frame.release();
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test(dataProvider = "bufferTypes")
    public void shouldTrackNestedViewsAndReleaseMultipleReferences(boolean pooled) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        ByteBuf frame = null;
        ByteBuf duplicate = null;
        try {
            assertThat(channel.writeInbound(frames(pooled, 128, 8))).isTrue();
            frame = channel.readInbound();
            duplicate = frame.retainedDuplicate();
            duplicate.retain(2);
            ByteBuf view = duplicate.slice().asReadOnly().order(ByteOrder.LITTLE_ENDIAN);
            frame.release();
            frame = null;
            duplicate.release(2);
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            view.release();
            duplicate = null;
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            ReferenceCountUtil.safeRelease(frame);
            ReferenceCountUtil.safeRelease(duplicate);
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldAccountForCumulationCapacityGrowthWithinAPooledAllocation() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        try {
            ByteBuf input = PooledByteBufAllocator.DEFAULT.directBuffer(17, 1024).writeInt(32).writeZero(4);
            assertThat(channel.writeInbound(input)).isFalse();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(17);

            assertThat(channel.writeInbound(Unpooled.directBuffer(12, 12).writeZero(12))).isFalse();
            assertThat(input.capacity()).isGreaterThan(17);
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(input.capacity());
        } finally {
            channel.finishAndReleaseAll();
        }
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldAccountForIncompleteCumulationAndReleaseItOnClose() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        try {
            ByteBuf partialFrame = Unpooled.directBuffer(128, 128).writeInt(8).writeZero(4);
            assertThat(channel.writeInbound(partialFrame)).isFalse();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            assertThat(controller.getLimiter().isThrottled()).isTrue();
        } finally {
            channel.finishAndReleaseAll();
        }
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldAccountForExpandedIncompleteCumulation() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 1, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        try {
            ByteBuf firstPart = Unpooled.directBuffer(8, 8).writeInt(16).writeZero(4);
            ByteBuf secondPart = Unpooled.directBuffer(128, 128).writeZero(4);
            assertThat(channel.writeInbound(firstPart)).isFalse();
            assertThat(channel.writeInbound(secondPart)).isFalse();
            assertThat(controller.getRetainedFrameBytes()).isPositive();
        } finally {
            channel.finishAndReleaseAll();
        }
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test(dataProvider = "bufferTypes")
    public void shouldTrackFramesAfterTheirConnectionCloses(boolean pooled) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        ByteBuf frame = null;
        try {
            assertThat(channel.writeInbound(frames(pooled, 128, 8))).isTrue();
            frame = channel.readInbound();
            assertThat(controller.getRegisteredListenerCount()).isEqualTo(1);
            channel.finishAndReleaseAll();
            assertThat(controller.getRegisteredListenerCount()).isZero();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            frame.release();
            frame = null;
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            ReferenceCountUtil.safeRelease(frame);
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldReleaseRootCapacityWhenChannelClosesWithUnreadFrames() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 64, () -> 0, Long.MAX_VALUE);
        EmbeddedChannel channel = new EmbeddedChannel(
                new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        try {
            assertThat(channel.writeInbound(frames(false, 128, 8))).isTrue();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
        } finally {
            channel.finishAndReleaseAll();
        }
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test(dataProvider = "bufferTypes")
    public void shouldKeepTrackingRetainedFramesAcrossModeChangesAndConnectionClose(boolean pooled) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.DISABLED, 100, () -> {
                    throw new AssertionError("Forced modes must not sample direct-memory pressure");
                }, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));
        ServerCnx serverCnx = mock(ServerCnx.class);
        ServerCnxThrottleTracker throttleTracker = mock(ServerCnxThrottleTracker.class);
        when(serverCnx.getThrottleTracker()).thenReturn(throttleTracker);
        ByteBuf frame = null;
        ByteBuf duplicate = null;
        try {
            assertThat(channel.writeInbound(frames(pooled, 128, 8))).isTrue();
            frame = channel.readInbound();
            duplicate = frame.retainedDuplicate();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            assertThat(limiter.isThrottled()).isFalse();

            channel.pipeline().addLast(ServerCnx.SERVER_CNX_HANDLER_NAME, serverCnx);
            controller.setMode(InProgressRequestFrameMemoryLimitMode.ENABLED);
            assertThat(limiter.isThrottled()).isTrue();
            channel.runPendingTasks();
            verify(throttleTracker).markThrottled(
                    ServerCnxThrottleTracker.ThrottleType.InProgressRequestFrameMemoryExceeded);

            controller.setMode(InProgressRequestFrameMemoryLimitMode.DISABLED);
            channel.runPendingTasks();
            verify(throttleTracker).unmarkThrottled(
                    ServerCnxThrottleTracker.ThrottleType.InProgressRequestFrameMemoryExceeded);
            assertThat(limiter.isThrottled()).isFalse();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            channel.finishAndReleaseAll();
            frame.release();
            frame = null;
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(128);
            duplicate.release();
            duplicate = null;
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            ReferenceCountUtil.safeRelease(frame);
            ReferenceCountUtil.safeRelease(duplicate);
            channel.finishAndReleaseAll();
        }
    }

    private static ByteBuf frames(boolean pooled, int capacity, int... payloadLengths) {
        ByteBuf input = pooled ? PooledByteBufAllocator.DEFAULT.directBuffer(capacity, capacity)
                : Unpooled.directBuffer(capacity, capacity);
        for (int payloadLength : payloadLengths) {
            input.writeInt(payloadLength);
            input.writeZero(payloadLength);
        }
        return input;
    }
}
