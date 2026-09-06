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
package org.apache.pulsar.common.util.collections;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * JMH benchmark for {@link LongBitmap} ({@link ConcurrentRoaringBitmap}), compared against
 * the pre-PR bitmap implementations it replaces:
 * <ul>
 *   <li>{@link Roaring64Bitmap} — previously used in {@code InMemoryDelayedDeliveryTracker}.</li>
 *   <li>{@link RoaringBitmap} — previously used in {@code ConsumerNameIndexTracker},
 *       {@code DrainingHashesTracker}, and the bucket delayed-delivery family.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>{@code
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar LongBitmapBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(time = 2, iterations = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 3, iterations = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class LongBitmapBenchmark {

    @Param({"1000", "100000"})
    public int bitmapSize;

    private LongBitmap longBitmap;
    private RoaringBitmap roaringBitmap;
    private Roaring64Bitmap roaring64Bitmap;

    private final AtomicLong nextValue = new AtomicLong();

    @Setup(Level.Trial)
    public void setup() {
        longBitmap = LongBitmaps.create();
        roaringBitmap = new RoaringBitmap();
        roaring64Bitmap = new Roaring64Bitmap();
        for (int i = 0; i < bitmapSize; i++) {
            longBitmap.add(i);
            roaringBitmap.add(i);
            roaring64Bitmap.addLong(i);
        }
        nextValue.set(bitmapSize);
    }

    @Benchmark
    @Threads(1)
    public boolean longBitmapAddSingleThread() {
        long v = nextValue.getAndIncrement();
        return longBitmap.checkedAdd(v);
    }

    @Benchmark
    @Threads(1)
    public boolean roaringBitmapAddSingleThread() {
        long v = nextValue.getAndIncrement();
        return roaringBitmap.checkedAdd((int) v);
    }

    @Benchmark
    @Threads(1)
    public boolean roaring64BitmapAddSingleThread() {
        long v = nextValue.getAndIncrement();
        boolean existed = roaring64Bitmap.contains(v);
        roaring64Bitmap.addLong(v);
        return !existed;
    }

    @Benchmark
    @Threads(1)
    public boolean longBitmapContainsSingleThread() {
        return longBitmap.contains(nextValue.getAndIncrement() % bitmapSize);
    }

    @Benchmark
    @Threads(1)
    public boolean roaringBitmapContainsSingleThread() {
        return roaringBitmap.contains((int) (nextValue.getAndIncrement() % bitmapSize));
    }

    @Benchmark
    @Threads(1)
    public boolean roaring64BitmapContainsSingleThread() {
        return roaring64Bitmap.contains(nextValue.getAndIncrement() % bitmapSize);
    }

    // Bare RoaringBitmap variants are not thread-safe and are omitted below.

    @Benchmark
    @Threads(4)
    public boolean longBitmapAdd4Threads() {
        long v = nextValue.getAndIncrement();
        return longBitmap.checkedAdd(v);
    }

    @Benchmark
    @Threads(4)
    public void longBitmapContains4Threads(Blackhole bh) {
        int v = (int) (Thread.currentThread().getId() * 31 + System.nanoTime());
        bh.consume(longBitmap.contains(Math.abs(v) % bitmapSize));
    }
}
