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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class MessageDeduplicationSequenceCheckBenchmark {

    @State(Scope.Benchmark)
    public static class SequenceMaps {
        private final ConcurrentMap<String, Long> monitorDistinct = createMap();
        private final ConcurrentMap<String, Long> atomicDistinct = createMap();
        private final ConcurrentMap<String, Long> monitorShared = createMap();
        private final ConcurrentMap<String, Long> atomicShared = createMap();

        private static ConcurrentMap<String, Long> createMap() {
            ConcurrentMap<String, Long> map = new ConcurrentHashMap<>();
            for (int i = 0; i < 64; i++) {
                map.put("producer-" + i, 0L);
            }
            return map;
        }
    }

    @State(Scope.Thread)
    public static class ProducerAccess {
        private String producerName;
        private long sequenceId;

        @org.openjdk.jmh.annotations.Setup
        public void setup(ThreadParams threadParams) {
            producerName = "producer-" + threadParams.getThreadIndex();
        }

        long nextSequenceId() {
            return ++sequenceId;
        }
    }

    @Benchmark
    @Threads(1)
    public void monitorDistinctSingle(SequenceMaps maps, ProducerAccess access) {
        updateWithMonitor(maps.monitorDistinct, access.producerName, access.nextSequenceId());
    }

    @Benchmark
    @Threads(1)
    public void atomicDistinctSingle(SequenceMaps maps, ProducerAccess access) {
        updateAtomically(maps.atomicDistinct, access.producerName, access.nextSequenceId());
    }

    @Benchmark
    @Threads(16)
    public void monitorDistinctConcurrent(SequenceMaps maps, ProducerAccess access) {
        updateWithMonitor(maps.monitorDistinct, access.producerName, access.nextSequenceId());
    }

    @Benchmark
    @Threads(16)
    public void atomicDistinctConcurrent(SequenceMaps maps, ProducerAccess access) {
        updateAtomically(maps.atomicDistinct, access.producerName, access.nextSequenceId());
    }

    @Benchmark
    @Threads(16)
    public void monitorSharedProducer(SequenceMaps maps) {
        incrementWithMonitor(maps.monitorShared, "producer-0");
    }

    @Benchmark
    @Threads(16)
    public void atomicSharedProducer(SequenceMaps maps) {
        incrementAtomically(maps.atomicShared, "producer-0");
    }

    private static void updateWithMonitor(ConcurrentMap<String, Long> sequenceIds,
                                          String producerName, long sequenceId) {
        synchronized (sequenceIds) {
            Long previous = sequenceIds.get(producerName);
            if (previous == null || sequenceId > previous) {
                sequenceIds.put(producerName, sequenceId);
            }
        }
    }

    private static void updateAtomically(ConcurrentMap<String, Long> sequenceIds,
                                         String producerName, long sequenceId) {
        Long boxedSequenceId = sequenceId;
        while (true) {
            Long previous = sequenceIds.get(producerName);
            if (previous != null && sequenceId <= previous) {
                return;
            }
            if (previous == null) {
                if (sequenceIds.putIfAbsent(producerName, boxedSequenceId) == null) {
                    return;
                }
            } else if (sequenceIds.replace(producerName, previous, boxedSequenceId)) {
                return;
            }
        }
    }

    private static void incrementWithMonitor(ConcurrentMap<String, Long> sequenceIds, String producerName) {
        synchronized (sequenceIds) {
            sequenceIds.put(producerName, sequenceIds.get(producerName) + 1);
        }
    }

    private static void incrementAtomically(ConcurrentMap<String, Long> sequenceIds, String producerName) {
        while (true) {
            Long previous = sequenceIds.get(producerName);
            Long next = previous + 1;
            if (sequenceIds.replace(producerName, previous, next)) {
                return;
            }
        }
    }
}
