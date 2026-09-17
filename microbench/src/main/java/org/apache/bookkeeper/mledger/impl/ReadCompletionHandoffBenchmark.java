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
import java.util.concurrent.CompletableFuture;
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

/** Measures the future-adapter handoff using real executors, without ledger/cache work or producer queue pressure. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ReadCompletionHandoffBenchmark {
    @Param({"false", "true"})
    public boolean inline;

    private SingleThreadExecutor ledger;
    private SingleThreadExecutor dispatcher;

    @Setup
    public void setup() {
        ledger = new SingleThreadExecutor(new DefaultThreadFactory("ledger-benchmark"));
        dispatcher = new SingleThreadExecutor(new DefaultThreadFactory("dispatcher-benchmark"));
    }

    @Benchmark
    public int completeRead() {
        CompletableFuture<Integer> completed = new CompletableFuture<>();
        dispatcher.execute(() -> {
            CompletableFuture<Integer> read = new CompletableFuture<>();
            if (inline) {
                read.complete(1);
            } else {
                ledger.executeOrRun(() -> read.complete(1));
            }
            read.thenAcceptAsync(completed::complete, dispatcher);
        });
        return completed.join();
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        ledger.shutdown();
        dispatcher.shutdown();
        ledger.awaitTermination(10, TimeUnit.SECONDS);
        dispatcher.awaitTermination(10, TimeUnit.SECONDS);
    }
}
