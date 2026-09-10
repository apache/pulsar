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
package org.apache.pulsar.testclient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.openjdk.jmh.annotations.Warmup;

/** Measures actual perf-producer accounting and future completion, without SDK/network work. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PerfSendCompletionBenchmark {
    @Param({"false", "true"})
    public boolean completed;

    @Param({"false", "true"})
    public boolean recordLatency;

    private PerformanceProducerV4 producer;
    private AtomicLong totalSent;
    private byte[] payload;
    private long warmupEndTime;

    @Setup
    public void setup() {
        producer = new PerformanceProducerV4();
        totalSent = new AtomicLong();
        payload = new byte[128];
        warmupEndTime = recordLatency ? 0 : Long.MAX_VALUE;
    }

    @Benchmark
    public CompletableFuture<Void> sendCompletion() {
        CompletableFuture<Object> send = new CompletableFuture<>();
        long sendTime = System.nanoTime();
        if (completed) {
            send.complete(null);
        }
        CompletableFuture<Void> result =
                producer.trackSendCompletion(send, payload, totalSent, sendTime, warmupEndTime);
        if (!completed) {
            send.complete(null);
        }
        return result;
    }
}
