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

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Measures contention between I/O and downstream release threads sharing a broker's accounting controller. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
@State(Scope.Benchmark)
public class RequestFrameMemoryControllerBenchmark {
    @Param({"disabled", "manual", "automatic"})
    public String mode;
    private InProgressRequestFrameMemoryController controller;

    @Setup(Level.Trial)
    public void setup() {
        long limitBytes = 1024L * 1024 * 1024;
        long maximumDirectMemory = 4 * limitBytes;
        controller = new InProgressRequestFrameMemoryController("manual".equals(mode), limitBytes,
                () -> "automatic".equals(mode) ? maximumDirectMemory * 9 / 10 : 0, maximumDirectMemory);
        controller.getCurrentLimiter().registerThrottleStateListener(this, () -> { });
        if ("automatic".equals(mode)) {
            long activationBytes = limitBytes * 3 / 2 + 1;
            controller.acquire(activationBytes);
            controller.onInboundRead();
            controller.release(activationBytes);
        }
    }

    @Benchmark
    public long accountInboundRoot() {
        controller.acquire(64 * 1024);
        controller.onInboundRead();
        controller.release(64 * 1024);
        return controller.getRetainedFrameBytes();
    }

    @TearDown(Level.Trial)
    public void cleanup() {
        if (controller.getRetainedFrameBytes() != 0) {
            throw new IllegalStateException("Unbalanced root accounting");
        }
        controller.getCurrentLimiter().unregisterThrottleStateListener(this);
    }
}
