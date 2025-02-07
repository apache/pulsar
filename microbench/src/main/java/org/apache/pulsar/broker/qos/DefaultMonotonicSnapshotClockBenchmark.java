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

package org.apache.pulsar.broker.qos;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class DefaultMonotonicSnapshotClockBenchmark {
    private DefaultMonotonicSnapshotClock monotonicSnapshotClock =
            new DefaultMonotonicSnapshotClock(TimeUnit.MILLISECONDS.toNanos(1), System::nanoTime);

    @TearDown(Level.Iteration)
    public void teardown() {
        monotonicSnapshotClock.close();
    }

    @Threads(1)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanos001Threads(Blackhole blackhole) {
        consumeTokenAndGetTokens(blackhole, false);
    }

    @Threads(10)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanos010Threads(Blackhole blackhole) {
        consumeTokenAndGetTokens(blackhole, false);
    }

    @Threads(100)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanos100Threads(Blackhole blackhole) {
        consumeTokenAndGetTokens(blackhole, false);
    }

    @Threads(1)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanosRequestSnapshot001Threads(Blackhole blackhole) {
        consumeTokenAndGetTokens(blackhole, true);
    }

    @Threads(10)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanosRequestSnapshot010Threads(Blackhole blackhole) {
        consumeTokenAndGetTokens(blackhole, true);
    }

    @Threads(100)
    @Benchmark
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
    public void getTickNanosRequestSnapshot100Threads(Blackhole blackhole) {
        consumeTokenAndGetTokens(blackhole, true);
    }

    private void consumeTokenAndGetTokens(Blackhole blackhole, boolean requestSnapshot) {
        // blackhole is used to ensure that the compiler doesn't do dead code elimination
        blackhole.consume(monotonicSnapshotClock.getTickNanos(requestSnapshot));
    }
}
