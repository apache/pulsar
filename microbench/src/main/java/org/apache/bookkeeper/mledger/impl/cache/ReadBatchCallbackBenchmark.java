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
package org.apache.bookkeeper.mledger.impl.cache;

import io.opentelemetry.api.OpenTelemetry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
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

/** Measures real entry-copy/release and read-permit accounting with per-entry or per-batch callbacks. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ReadBatchCallbackBenchmark {
    private static final long LIMIT_BYTES = 16 * 1024 * 1024;

    @Param({"1", "100", "1000"})
    public int batchSize;

    @Param({"false", "true"})
    public boolean sharedCallback;

    private EntryImpl[] cachedEntries;
    private EntryImpl[] readEntries;
    private InflightReadsLimiter limiter;
    private ScheduledExecutorService executor;

    @Setup
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        limiter = new InflightReadsLimiter(LIMIT_BYTES, 1, 0, executor, OpenTelemetry.noop());
        cachedEntries = new EntryImpl[batchSize];
        readEntries = new EntryImpl[batchSize];
        for (int i = 0; i < batchSize; i++) {
            cachedEntries[i] = EntryImpl.create(1, i, new byte[128]);
            cachedEntries[i].getPosition();
        }
    }

    @Benchmark
    public long readAndReleaseBatch() {
        InflightReadsLimiter batchLimiter = limiter;
        InflightReadsLimiter.Handle handle = batchLimiter.acquire(batchSize * 128L, null).orElseThrow();
        AtomicInteger remainingCount = new AtomicInteger(batchSize);
        Runnable shared = sharedCallback ? releaseCallback(remainingCount, batchLimiter, handle) : null;
        for (int i = 0; i < batchSize; i++) {
            EntryImpl entry = EntryImpl.create(cachedEntries[i]);
            entry.onDeallocate(sharedCallback ? shared : releaseCallback(remainingCount, batchLimiter, handle));
            readEntries[i] = entry;
        }
        for (int i = batchSize - 1; i >= 0; i--) {
            readEntries[i].release();
            readEntries[i] = null;
        }
        return batchLimiter.getRemainingBytes();
    }

    private static Runnable releaseCallback(AtomicInteger remainingCount, InflightReadsLimiter limiter,
                                            InflightReadsLimiter.Handle handle) {
        return () -> {
            if (remainingCount.decrementAndGet() <= 0) {
                limiter.release(handle);
            }
        };
    }

    @TearDown
    public void tearDown() {
        try {
            if (limiter.getRemainingBytes() != LIMIT_BYTES) {
                throw new IllegalStateException("Read permits were not released exactly once per batch");
            }
        } finally {
            for (EntryImpl entry : cachedEntries) {
                entry.release();
            }
            limiter.close();
            executor.shutdownNow();
        }
    }
}
