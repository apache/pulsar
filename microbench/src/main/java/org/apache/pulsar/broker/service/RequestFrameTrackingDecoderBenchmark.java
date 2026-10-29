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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

/** Isolates root tracking from throttling policy, with both event-loop decoding and downstream ownership changes. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class RequestFrameTrackingDecoderBenchmark {
    @State(Scope.Thread)
    public static class DecoderState {
        private InProgressRequestFrameMemoryController controller;
        private EmbeddedChannel channel;

        @Setup
        public void setup() {
            controller = controller();
            channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
        }

        @TearDown
        public void cleanup() {
            channel.finishAndReleaseAll();
            if (controller.getRetainedFrameBytes() != 0) {
                throw new IllegalStateException("Unbalanced decoded root accounting");
            }
        }
    }

    @State(Scope.Benchmark)
    public static class SharedFrames {
        // Distinguish contention on one Netty view from different frames sharing only the tracked root.
        @Param({"sharedFrame", "siblingFrames", "independentRoots"})
        public String ownership;
        private InProgressRequestFrameMemoryController controller;
        private EmbeddedChannel channel;
        private ByteBuf[] frames;
        private long expectedBytes;

        @Setup
        public void setup() {
            controller = controller();
            channel = new EmbeddedChannel(new InProgressRequestFrameTrackingDecoder(1024, controller.getLimiter()));
            int roots = "independentRoots".equals(ownership) ? 16 : 1;
            int framesPerRoot = "siblingFrames".equals(ownership) ? 16 : 1;
            int capacity = framesPerRoot == 1 ? 128 : 256;
            expectedBytes = (long) capacity * roots;
            frames = new ByteBuf[roots * framesPerRoot];
            for (int i = 0; i < roots; i++) {
                ByteBuf input = PooledByteBufAllocator.DEFAULT.directBuffer(capacity, capacity);
                for (int j = 0; j < framesPerRoot; j++) {
                    input.writeInt(8).writeLong(j);
                }
                channel.writeInbound(input);
                for (int j = 0; j < framesPerRoot; j++) {
                    frames[i * framesPerRoot + j] = channel.readInbound();
                }
            }
        }

        @TearDown
        public void cleanup() {
            if (controller.getRetainedFrameBytes() != expectedBytes) {
                throw new IllegalStateException("Root capacity was returned while a frame was still retained");
            }
            for (ByteBuf frame : frames) {
                frame.release();
            }
            channel.finishAndReleaseAll();
            if (controller.getRetainedFrameBytes() != 0) {
                throw new IllegalStateException("Unbalanced shared root accounting");
            }
        }
    }

    @Benchmark
    public boolean decodeAndRelease(DecoderState state) {
        state.channel.writeInbound(PooledByteBufAllocator.DEFAULT.directBuffer(128, 128)
                .writeInt(8).writeLong(1).writeInt(8).writeLong(2));
        ByteBuf first = state.channel.readInbound();
        ByteBuf second = state.channel.readInbound();
        first.release();
        return second.release();
    }

    @Benchmark
    public boolean retainedView(SharedFrames state, ThreadParams thread) {
        ByteBuf frame = state.frames[thread.getThreadIndex() & (state.frames.length - 1)];
        return frame.retainedDuplicate().release();
    }

    private static InProgressRequestFrameMemoryController controller() {
        return new InProgressRequestFrameMemoryController(InProgressRequestFrameMemoryLimitMode.TRACKING,
                Long.MAX_VALUE, () -> 0, Long.MAX_VALUE);
    }
}
