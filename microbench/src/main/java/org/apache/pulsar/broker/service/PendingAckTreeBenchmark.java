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

import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongSortedMap;
import java.util.concurrent.TimeUnit;
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

/** Compares sorted maps for a moving pending-ACK window; excludes the unchanged outer ledger map and locking. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PendingAckTreeBenchmark {
    @Param({"RB", "AVL"})
    public String implementation;

    @Param({"1024", "65536"})
    public int windowSize;

    @Param({"false", "true"})
    public boolean shuffled;

    private Long2LongSortedMap map;
    private long[] entryIds;
    private long nextEntryId;
    private int cursor;
    private long packedValue;

    @Setup
    public void setup() {
        map = "AVL".equals(implementation) ? new Long2LongAVLTreeMap() : new Long2LongRBTreeMap();
        map.defaultReturnValue(PendingAckValues.PACKED_NOT_FOUND);
        packedValue = PendingAckValues.pack(1, 0);
        entryIds = new long[windowSize];
        for (int i = 0; i < windowSize; i++) {
            entryIds[i] = i;
            map.put(i, packedValue);
        }
        nextEntryId = windowSize;
    }

    private int nextIndex() {
        int next = cursor++;
        // Odd multiplier permutes all indices in these power-of-two windows without an RNG in the hot path.
        return (shuffled ? next * 4051 : next) & (windowSize - 1);
    }

    @Benchmark
    public long lookup() {
        return map.get(entryIds[nextIndex()]);
    }

    @Benchmark
    public long rotateWindow() {
        int index = nextIndex();
        long oldEntryId = entryIds[index];
        long value = map.get(oldEntryId);
        map.remove(oldEntryId);
        long newEntryId = nextEntryId++;
        map.put(newEntryId, packedValue);
        entryIds[index] = newEntryId;
        return value;
    }

    @Benchmark
    public long scanPrefix() {
        long sum = 0;
        for (Long2LongMap.Entry entry : map.headMap(windowSize / 50).long2LongEntrySet()) {
            sum += entry.getLongKey();
        }
        return sum;
    }
}
