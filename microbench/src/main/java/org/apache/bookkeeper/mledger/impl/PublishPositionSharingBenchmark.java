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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
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

/** Isolates position construction with real pooled entry copies; does not model persistence or cache maps. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PublishPositionSharingBenchmark {
    @Param({"false", "true"})
    public boolean sharePosition;

    @Param({"false", "true"})
    public boolean cacheEntry;

    private ByteBuf data;
    private long nextEntryId;
    private volatile Position cachedPosition;
    private volatile Position confirmedPosition;

    @Setup
    public void setup() {
        if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
            throw new IllegalStateException("Run with FastThreadLocalBenchmarkExecutor in the fork JVM");
        }
        data = PooledByteBufAllocator.DEFAULT.directBuffer(128).writeZero(128);
    }

    @Benchmark
    public long completePublish() {
        long entryId = nextEntryId++;
        Position lastEntry = sharePosition ? PositionFactory.create(1, entryId) : null;
        if (cacheEntry) {
            EntryImpl entry = sharePosition ? EntryImpl.create(lastEntry, data, 0)
                    : EntryImpl.create(1, entryId, data, 0);
            try {
                EntryImpl copy = EntryImpl.createWithRetainedDuplicate(entry.getPosition(), entry.getDataBuffer(), 0);
                try {
                    cachedPosition = copy.getPosition();
                } finally {
                    copy.release();
                }
            } finally {
                entry.release();
            }
        }
        if (!sharePosition) {
            lastEntry = PositionFactory.create(1, entryId);
        }
        confirmedPosition = lastEntry;
        return lastEntry.getEntryId();
    }

    @TearDown
    public void tearDown() {
        cachedPosition = null;
        confirmedPosition = null;
        data.release();
    }
}
