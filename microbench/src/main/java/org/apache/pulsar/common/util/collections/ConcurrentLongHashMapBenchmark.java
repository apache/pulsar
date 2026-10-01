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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
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

/**
 * Benchmarks for {@link ConcurrentLongHashMap}.
 *
 * <p>Compares two implementations:
 * <ul>
 *   <li>{@code clhm} – {@link ConcurrentLongHashMap} (immutable Table snapshot, primitive long
 *       keys, zero allocation on the key path),</li>
 *   <li>{@code chm} – {@link java.util.concurrent.ConcurrentHashMap} as the JDK baseline.</li>
 * </ul>
 *
 * <p>Workload mix:
 * <ul>
 *   <li>{@link #getHit}/{@link #getMiss}/{@link #putRemove} – single-thread basics.</li>
 *   <li>{@link #concurrentGetHit} – read-only, 16 threads.</li>
 *   <li>{@link #concurrentMixedReader}/{@link #concurrentMixedWriter} – an asymmetric concurrent
 *       group: 12 readers + 4 writers operating on disjoint key partitions, so any concurrency
 *       bug (torn rehash, lost-update, partial-publish) shows up either as a JMH error or a
 *       reduced ops/sec number on the suspect implementation.</li>
 *   <li>{@link #concurrentExpandShrinkWriter}/{@link #concurrentExpandShrinkReader} – starts the
 *       map at the smallest legal capacity and hammers a single section with put/remove so that
 *       every writer constantly forces a rehash. Highest-pressure rehash-vs-read benchmark; this
 *       is the workload that originally surfaced the OOB race in the pre-Table design.</li>
 *   <li>{@link #boxingGetHit}/{@link #boxingPutGetRemove}/{@link #boxingConcurrentGetHit}/
 *       {@link #boxingConcurrentPutRemove} – use keys above the {@code Long.valueOf} cache range
 *       so every CHM operation has to allocate a fresh boxed Long. Run with {@code -prof gc} to
 *       see the alloc-rate divergence vs the primitive-long map.</li>
 * </ul>
 *
 * <p>Run from the repo root:
 * <pre>{@code
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar \
 *      "ConcurrentLongHashMapBenchmark.concurrentExpandShrink" -prof gc
 * }</pre>
 */
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
public class ConcurrentLongHashMapBenchmark {

    /**
     * Shared benchmark state for the steady-state benchmarks (the map is fully populated up
     * front and the workload only mutates keys outside the resident set).
     */
    @State(Scope.Benchmark)
    public static class MapState {
        @Param({"clhm", "chm"})
        private String implementation;

        @Param({"1024", "65536"})
        private int entries;

        private long[] presentKeys;
        private long[] absentKeys;
        private ConcurrentLongHashMap<String> clhm;
        private ConcurrentHashMap<Long, String> chm;
        private AtomicLong writeKey;

        @Setup(Level.Trial)
        public void setup() {
            presentKeys = new long[entries];
            absentKeys = new long[entries];
            clhm = ConcurrentLongHashMap.<String>newBuilder()
                    .expectedItems(entries)
                    .concurrencyLevel(16)
                    .build();
            chm = new ConcurrentHashMap<>(entries, 0.66f, 16);

            for (int i = 0; i < entries; i++) {
                long key = i;
                presentKeys[i] = key;
                absentKeys[i] = key ^ Long.MIN_VALUE;
                clhm.put(key, "value");
                chm.put(key, "value");
            }

            writeKey = new AtomicLong(1L << 48);
        }

        String get(long key) {
            return "clhm".equals(implementation) ? clhm.get(key) : chm.get(key);
        }

        void put(long key, String value) {
            if ("clhm".equals(implementation)) {
                clhm.put(key, value);
            } else {
                chm.put(key, value);
            }
        }

        void remove(long key) {
            if ("clhm".equals(implementation)) {
                clhm.remove(key);
            } else {
                chm.remove(key);
            }
        }

        long nextWriteKey() {
            return writeKey.getAndIncrement();
        }
    }

    /**
     * Per-thread cursor state.
     */
    @State(Scope.Thread)
    public static class CursorState {
        private int index;

        int next(int length) {
            int value = index;
            index = value + 1;
            return value & (length - 1);
        }
    }

    /**
     * Independent key-stream state for concurrent benchmarks. Each writer thread owns a unique
     * partition of the long key-space so the mutations don't trample each other and the steady
     * state remains bounded; readers also walk a private cursor so they don't hot-spot a single
     * bucket.
     */
    @State(Scope.Thread)
    public static class WriterState {
        private static final AtomicLong NEXT_PARTITION = new AtomicLong();
        private long base;
        private long offset;
        // Keep a small per-writer working set so put + remove pair cleanly without unbounded growth.
        private static final int WORKING_SET = 1024;

        @Setup(Level.Iteration)
        public void setup() {
            base = NEXT_PARTITION.getAndIncrement() << 40;
            offset = 0;
        }

        long nextKey() {
            long k = base + (offset & (WORKING_SET - 1));
            offset++;
            return k;
        }
    }

    @Benchmark
    public void getHit(MapState map, CursorState cursor, Blackhole blackhole) {
        blackhole.consume(map.get(map.presentKeys[cursor.next(map.presentKeys.length)]));
    }

    @Benchmark
    public void getMiss(MapState map, CursorState cursor, Blackhole blackhole) {
        blackhole.consume(map.get(map.absentKeys[cursor.next(map.absentKeys.length)]));
    }

    @Benchmark
    public void putRemove(MapState map, Blackhole blackhole) {
        long key = map.nextWriteKey();
        map.put(key, "value");
        blackhole.consume(map.get(key));
        map.remove(key);
    }

    @Benchmark
    @Threads(16)
    public void concurrentGetHit(MapState map, CursorState cursor, Blackhole blackhole) {
        blackhole.consume(map.get(map.presentKeys[cursor.next(map.presentKeys.length)]));
    }

    /** Reader half of the asymmetric mixed-workload group: 12 reader threads. */
    @Benchmark
    @Group("concurrentMixed")
    @GroupThreads(12)
    public void concurrentMixedReader(MapState map, CursorState cursor, Blackhole blackhole) {
        blackhole.consume(map.get(map.presentKeys[cursor.next(map.presentKeys.length)]));
    }

    /** Writer half of the asymmetric mixed-workload group: 4 writer threads. */
    @Benchmark
    @Group("concurrentMixed")
    @GroupThreads(4)
    public void concurrentMixedWriter(MapState map, WriterState w, Blackhole blackhole) {
        long key = w.nextKey();
        map.put(key, "value");
        blackhole.consume(map.get(key));
        map.remove(key);
    }

    /**
     * Holds an aggressively-shrinking, single-section map. Each writer thread's put/remove pair
     * crosses the expand and shrink thresholds, so the rehash code path is exercised on nearly
     * every operation. Reader threads chase the writers to surface read-vs-rehash races. This is
     * the workload that originally surfaced the OOB race in the pre-Table design.
     */
    @State(Scope.Benchmark)
    public static class ChurningMapState {
        @Param({"clhm", "chm"})
        private String implementation;

        private ConcurrentLongHashMap<String> clhm;
        private ConcurrentHashMap<Long, String> chm;

        @Setup(Level.Iteration)
        public void setup() {
            clhm = ConcurrentLongHashMap.<String>newBuilder()
                    .expectedItems(2)
                    .concurrencyLevel(1)
                    .autoShrink(true)
                    .mapIdleFactor(0.25f)
                    .build();
            chm = new ConcurrentHashMap<>(4, 0.66f, 1);
        }

        String get(long key) {
            return "clhm".equals(implementation) ? clhm.get(key) : chm.get(key);
        }

        String put(long key, String value) {
            return "clhm".equals(implementation) ? clhm.put(key, value) : chm.put(key, value);
        }

        String remove(long key) {
            return "clhm".equals(implementation) ? clhm.remove(key) : chm.remove(key);
        }
    }

    /** Writer driving constant expand+shrink on a single section. */
    @Benchmark
    @Group("concurrentExpandShrink")
    @GroupThreads(4)
    public void concurrentExpandShrinkWriter(ChurningMapState map, WriterState w, Blackhole bh) {
        long k1 = w.nextKey();
        long k2 = w.nextKey();
        bh.consume(map.put(k1, "v"));
        bh.consume(map.put(k2, "v"));
        bh.consume(map.remove(k1));
        bh.consume(map.remove(k2));
    }

    /** Reader chasing the writers; reads must not throw or return torn values. */
    @Benchmark
    @Group("concurrentExpandShrink")
    @GroupThreads(4)
    public void concurrentExpandShrinkReader(ChurningMapState map, WriterState w, Blackhole bh) {
        bh.consume(map.get(w.nextKey()));
    }

    // ------------------------------------------------------------------------------------------
    // Boxing-impact workload
    //
    // Pulsar's actual usage of ConcurrentLongHashMap stores object values (CompletableFuture,
    // Producer, Consumer, ...) and the choice to keep a primitive-long map instead of switching
    // to ConcurrentHashMap<Long, V> hinges on whether the long->Long autobox on every operation
    // materially hurts throughput and GC pressure in practice.
    //
    // To make that visible to JMH we have to defeat the JDK's Long.valueOf cache (which short-
    // circuits values in [-128, 127] to a shared instance). Keys here are seeded above the cache
    // range and monotonically increase, so every CHM operation has to allocate a fresh boxed
    // Long; the primitive-long map sees zero allocation on the key path. Run with `-prof gc` to
    // see the alloc-rate divergence on top of the throughput numbers.
    // ------------------------------------------------------------------------------------------

    @State(Scope.Benchmark)
    public static class BoxingMapState {
        @Param({"clhm", "chm"})
        private String implementation;

        @Param({"1024", "65536"})
        private int entries;

        // Lock granularity. Override with `-p concurrency=...` to match CHM's bucket-level
        // striping (default JDK CHM uses one synchronized monitor per bucket, so concurrency=1024
        // on a 1024-bucket map effectively gives the primitive map a comparable lock count).
        @Param({"16"})
        private int concurrency;

        private long[] presentKeys;
        private ConcurrentLongHashMap<String> clhm;
        private ConcurrentHashMap<Long, String> chm;

        @Setup(Level.Trial)
        public void setup() {
            presentKeys = new long[entries];
            int cl = Math.min(concurrency, entries); // builder requires expectedItems >= concurrencyLevel
            clhm = ConcurrentLongHashMap.<String>newBuilder()
                    .expectedItems(entries).concurrencyLevel(cl).build();
            chm = new ConcurrentHashMap<>(entries, 0.66f, cl);

            // Start the key space well above 127 so Long.valueOf cannot serve from its cache.
            // Use an odd stride so consecutive keys land in different sections / cache lines.
            final long base = 1L << 32;
            for (int i = 0; i < entries; i++) {
                long key = base + ((long) i) * 31L;
                presentKeys[i] = key;
                clhm.put(key, "value");
                chm.put(key, "value");
            }
        }

        String get(long key) {
            return "clhm".equals(implementation) ? clhm.get(key) : chm.get(key);
        }

        String put(long key, String value) {
            return "clhm".equals(implementation) ? clhm.put(key, value) : chm.put(key, value);
        }

        String remove(long key) {
            return "clhm".equals(implementation) ? clhm.remove(key) : chm.remove(key);
        }
    }

    @State(Scope.Thread)
    public static class BoxingCursor {
        private int index;

        int next(int length) {
            int v = index;
            index = v + 1;
            return v & (length - 1);
        }
    }

    @Benchmark
    public void boxingGetHit(BoxingMapState map, BoxingCursor cur, Blackhole bh) {
        bh.consume(map.get(map.presentKeys[cur.next(map.presentKeys.length)]));
    }

    @Benchmark
    @Threads(16)
    public void boxingConcurrentGetHit(BoxingMapState map, BoxingCursor cur, Blackhole bh) {
        bh.consume(map.get(map.presentKeys[cur.next(map.presentKeys.length)]));
    }

    @Benchmark
    public void boxingPutGetRemove(BoxingMapState map, BoxingCursor cur, Blackhole bh) {
        long key = map.presentKeys[cur.next(map.presentKeys.length)];
        bh.consume(map.put(key, "value"));
        bh.consume(map.get(key));
        bh.consume(map.remove(key));
        bh.consume(map.put(key, "value"));
    }

    @State(Scope.Thread)
    public static class BoxingWriterState {
        private static final AtomicLong NEXT_PARTITION = new AtomicLong();
        private long base;
        private long offset;

        @Setup(Level.Iteration)
        public void setup() {
            base = (1L << 40) | (NEXT_PARTITION.getAndIncrement() << 32);
            offset = 0;
        }

        long nextKey() {
            return base + (offset++) * 31L;
        }
    }

    @Benchmark
    @Threads(16)
    public void boxingConcurrentPutRemove(BoxingMapState map, BoxingWriterState w, Blackhole bh) {
        long key = w.nextKey();
        bh.consume(map.put(key, "value"));
        bh.consume(map.remove(key));
    }
}
