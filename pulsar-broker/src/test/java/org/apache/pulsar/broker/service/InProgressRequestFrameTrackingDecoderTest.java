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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

public class InProgressRequestFrameTrackingDecoderTest {

    @Test
    public void shouldTrackUsageWhenLimitingIsDisabled() {
        AtomicLong totalUsedBytes = new AtomicLong();
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, false,
                totalUsedBytes);

        limiter.acquire(64);

        assertThat(limiter.usedBytes()).isEqualTo(64);
        assertThat(totalUsedBytes).hasValue(64);
        assertThat(limiter.isThrottled()).isFalse();

        limiter.release(64);
        assertThat(totalUsedBytes).hasValue(0);
    }

    @Test
    public void shouldAutomaticallyEnableAndDisableLimitingBasedOnDirectMemoryUsage() {
        AtomicLong totalUsedBytes = new AtomicLong();
        AtomicLong directMemoryUsedBytes = new AtomicLong(85);
        InProgressRequestFrameMemoryLimiterState state = new InProgressRequestFrameMemoryLimiterState(false,
                totalUsedBytes, 50, directMemoryUsedBytes::get, 100);
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(100, totalUsedBytes,
                state);

        limiter.acquire(51);
        limiter.checkAutoLimit();

        assertThat(state.isAutomaticallyEnabled()).isTrue();
        assertThat(limiter.isEnabled()).isTrue();

        directMemoryUsedBytes.set(64);
        limiter.release(51);

        assertThat(state.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.isEnabled()).isFalse();
    }

    @Test
    public void shouldThrottleAllLimiterGenerationsWhenTheAutomaticGlobalLimitIsExceeded() {
        AtomicLong totalUsedBytes = new AtomicLong();
        AtomicLong directMemoryUsedBytes = new AtomicLong(85);
        InProgressRequestFrameMemoryLimiterState state = new InProgressRequestFrameMemoryLimiterState(false,
                totalUsedBytes, 50, directMemoryUsedBytes::get, 100);
        InProgressRequestFrameMemoryLimiter firstLimiter = new InProgressRequestFrameMemoryLimiter(1_000,
                totalUsedBytes, state);
        InProgressRequestFrameMemoryLimiter secondLimiter = new InProgressRequestFrameMemoryLimiter(1_000, totalUsedBytes,
                state);
        firstLimiter.registerThrottleStateListener("first", ignored -> { });
        secondLimiter.registerThrottleStateListener("second", ignored -> { });

        firstLimiter.acquire(51);
        firstLimiter.checkAutoLimit();
        assertThat(state.isAutomaticallyEnabled()).isTrue();
        assertThat(firstLimiter.isThrottled()).isFalse();
        assertThat(secondLimiter.isThrottled()).isFalse();

        secondLimiter.acquire(50);
        assertThat(firstLimiter.isThrottled()).isTrue();
        assertThat(secondLimiter.isThrottled()).isTrue();

        secondLimiter.release(50);
        assertThat(firstLimiter.isThrottled()).isTrue();
        assertThat(secondLimiter.isThrottled()).isTrue();
        firstLimiter.release(1);
        assertThat(firstLimiter.isThrottled()).isFalse();
        assertThat(secondLimiter.isThrottled()).isFalse();
    }

    @Test
    public void shouldClearAutomaticLimitAndStopCheckingMemoryWhenUserEnablesLimiting() {
        AtomicLong totalUsedBytes = new AtomicLong();
        AtomicLong directMemoryUsedBytes = new AtomicLong(85);
        InProgressRequestFrameMemoryLimiterState state = new InProgressRequestFrameMemoryLimiterState(false,
                totalUsedBytes, 50, directMemoryUsedBytes::get, 100);
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(100, totalUsedBytes,
                state);

        limiter.acquire(51);
        limiter.checkAutoLimit();
        assertThat(state.isAutomaticallyEnabled()).isTrue();

        state.setUserEnabled(true);
        limiter.checkAutoLimit();

        assertThat(state.isUserEnabled()).isTrue();
        assertThat(state.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.isEnabled()).isTrue();
    }

    @Test
    public void shouldDisableAutomaticLimitWhenTheAutomaticThresholdIsDisabled() {
        AtomicLong totalUsedBytes = new AtomicLong();
        AtomicLong directMemoryUsedBytes = new AtomicLong(85);
        InProgressRequestFrameMemoryLimiterState state = new InProgressRequestFrameMemoryLimiterState(false,
                totalUsedBytes, 50, directMemoryUsedBytes::get, 100);
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(100, totalUsedBytes,
                state);

        limiter.acquire(51);
        limiter.checkAutoLimit();
        assertThat(state.isAutomaticallyEnabled()).isTrue();

        state.setAutomaticEnableThresholdBytes(-1);

        assertThat(state.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.isEnabled()).isFalse();
    }

    @Test
    public void shouldNotCountHeapBuffersAsDirectMemory() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));

        assertThat(channel.writeInbound(Unpooled.buffer(128, 128).writeInt(8).writeZero(8))).isTrue();
        ByteBuf frame = channel.readInbound();

        assertThat(limiter.usedBytes()).isZero();
        frame.release();
        assertThat(channel.finishAndReleaseAll()).isFalse();
    }

    @Test
    public void shouldRetainRootCapacityUntilAllFramesAreReleased() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));
        ByteBuf input = frames(128, 8, 8);

        assertThat(channel.writeInbound(input)).isTrue();
        ByteBuf firstFrame = channel.readInbound();
        ByteBuf secondFrame = channel.readInbound();
        assertThat(limiter.usedBytes()).isEqualTo(128);
        assertThat(limiter.isThrottled()).isTrue();

        firstFrame.release();
        assertThat(limiter.usedBytes()).isEqualTo(128);

        secondFrame.release();
        assertThat(limiter.usedBytes()).isZero();
        assertThat(limiter.isThrottled()).isFalse();
        assertThat(channel.finishAndReleaseAll()).isFalse();
    }

    @Test
    public void shouldWaitForRetainedDuplicateBeforeReleasingRootCapacity() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));

        assertThat(channel.writeInbound(frames(128, 8))).isTrue();
        ByteBuf frame = channel.readInbound();
        ByteBuf duplicate = frame.retainedDuplicate();
        frame.release();

        assertThat(limiter.usedBytes()).isEqualTo(128);
        duplicate.release();
        assertThat(limiter.usedBytes()).isZero();
        assertThat(channel.finishAndReleaseAll()).isFalse();
    }

    @Test
    public void shouldWaitForRetainedFrameBeforeReleasingRootCapacity() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));

        assertThat(channel.writeInbound(frames(128, 8))).isTrue();
        ByteBuf frame = channel.readInbound();
        frame.retain();
        frame.release();

        assertThat(limiter.usedBytes()).isEqualTo(128);
        frame.release();
        assertThat(limiter.usedBytes()).isZero();
        assertThat(channel.finishAndReleaseAll()).isFalse();
    }

    @Test
    public void shouldAccountForIncompleteCumulationAndReleaseItOnClose() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));
        ByteBuf partialFrame = Unpooled.directBuffer(128, 128).writeInt(8).writeZero(4);

        assertThat(channel.writeInbound(partialFrame)).isFalse();
        assertThat(limiter.usedBytes()).isEqualTo(128);
        assertThat(limiter.isThrottled()).isTrue();

        assertThat(channel.finishAndReleaseAll()).isFalse();
        assertThat(limiter.usedBytes()).isZero();
    }

    @Test
    public void shouldAccountForExpandedIncompleteCumulation() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(1, true);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));
        ByteBuf firstPart = Unpooled.directBuffer(8, 8).writeInt(16).writeZero(4);
        ByteBuf secondPart = Unpooled.directBuffer(128, 128).writeZero(4);

        assertThat(channel.writeInbound(firstPart)).isFalse();
        assertThat(channel.writeInbound(secondPart)).isFalse();
        assertThat(limiter.usedBytes()).isPositive();

        assertThat(channel.finishAndReleaseAll()).isFalse();
        assertThat(limiter.usedBytes()).isZero();
    }

    @Test
    public void shouldOnlyNotifyConnectionsWhenThrottleStateChanges() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        AtomicInteger notifications = new AtomicInteger();
        limiter.registerThrottleStateListener("connection", ignored -> notifications.incrementAndGet());

        limiter.acquire(65);
        assertThat(notifications).hasValue(1);
        limiter.acquire(1);
        assertThat(notifications).hasValue(1);
        limiter.release(34);
        assertThat(notifications).hasValue(2);
    }

    @Test
    public void shouldNotifyNewListenerOnceWhenItRegistersWhileThrottled() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        limiter.acquire(65);
        AtomicInteger notifications = new AtomicInteger();

        limiter.registerThrottleStateListener("connection", ignored -> notifications.incrementAndGet());

        assertThat(notifications).hasValue(1);
    }

    @Test
    public void shouldReleaseRootCapacityWhenChannelClosesWithUnreadFrames() {
        InProgressRequestFrameMemoryLimiter limiter = new InProgressRequestFrameMemoryLimiter(64, true);
        EmbeddedChannel channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, limiter));

        assertThat(channel.writeInbound(frames(128, 8))).isTrue();
        assertThat(limiter.usedBytes()).isEqualTo(128);
        assertThat(channel.finishAndReleaseAll()).isTrue();
        assertThat(limiter.usedBytes()).isZero();
    }

    private static ByteBuf frames(int capacity, int... payloadLengths) {
        ByteBuf input = Unpooled.directBuffer(capacity, capacity);
        for (int payloadLength : payloadLengths) {
            input.writeInt(payloadLength);
            input.writeZero(payloadLength);
        }
        return input;
    }
}
