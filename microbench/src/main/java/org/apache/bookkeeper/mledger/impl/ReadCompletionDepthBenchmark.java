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
package org.apache.bookkeeper.mledger.impl;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.SingleThreadExecutor;
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

/** Measures bounded callback chains and overflow scheduling, without ledger/cache work or producer queue pressure. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ReadCompletionDepthBenchmark {
    @Param({"ledger", "commonPool"})
    public String overflowExecutor;

    @Param({"10", "1"})
    public int maxDepth;

    @Param({"100"})
    public int chainLength;

    private static final FastThreadLocal<int[]> COMPLETION_DEPTH = new FastThreadLocal<>() {
        @Override
        protected int[] initialValue() {
            return new int[1];
        }
    };

    private SingleThreadExecutor ledger;
    private Executor overflow;

    @Setup
    public void setup() {
        ledger = new SingleThreadExecutor(new DefaultThreadFactory("ledger-depth-benchmark"));
        overflow = switch (overflowExecutor) {
            case "ledger" -> ledger;
            case "commonPool" -> ForkJoinPool.commonPool();
            default -> throw new IllegalArgumentException("Unknown overflow executor: " + overflowExecutor);
        };
    }

    @Benchmark
    public int completeReadChain() {
        CompletableFuture<Integer> completed = new CompletableFuture<>();
        completeWithDepthLimit(chainLength, completed);
        return completed.join();
    }

    private void completeWithDepthLimit(int remaining, CompletableFuture<Integer> completed) {
        int[] depth = COMPLETION_DEPTH.get();
        if (depth[0] >= maxDepth) {
            // Recheck on the destination, as the real completion path does.
            overflow.execute(() -> completeWithDepthLimit(remaining, completed));
            return;
        }
        depth[0]++;
        try {
            if (remaining == 1) {
                completed.complete(chainLength);
            } else {
                completeWithDepthLimit(remaining - 1, completed);
            }
        } finally {
            depth[0]--;
        }
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        ledger.shutdown();
        ledger.awaitTermination(10, TimeUnit.SECONDS);
        COMPLETION_DEPTH.remove();
    }
}
