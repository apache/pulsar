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
package org.apache.pulsar.broker.delayed.bucket;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Simplified JMH Benchmarks for BucketDelayedDeliveryTracker thread safety improvements.
 * This benchmark focuses on the core StampedLock optimistic read performance without
 * complex dependencies on the full BucketDelayedDeliveryTracker implementation.
 * Run with: mvn exec:java -Dexec.mainClass="org.openjdk.jmh.Main"
 *           -Dexec.args="BucketDelayedDeliveryTrackerSimpleBenchmark"
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class BucketDelayedDeliveryTrackerSimpleBenchmark {

    @Param({"1", "2", "4", "8", "16"})
    public int threadCount;

    private StampedLock stampedLock;
    private boolean testData = true;
    private volatile long counter = 0;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        stampedLock = new StampedLock();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        // Cleanup if needed
    }

    // =============================================================================
    // STAMPED LOCK OPTIMISTIC READ BENCHMARKS
    // =============================================================================

    @Benchmark
    @Threads(1)
    public boolean benchmarkOptimisticReadSingleThreaded() {
        // Simulate optimistic read like in containsMessage()
        long stamp = stampedLock.tryOptimisticRead();
        boolean result = testData; // Simulate reading shared data

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = testData;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    @Benchmark
    @Threads(2)
    public boolean benchmarkOptimisticReadMultiThreaded() {
        long stamp = stampedLock.tryOptimisticRead();
        boolean result = testData;

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = testData;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    @Benchmark
    @Threads(8)
    public boolean benchmarkOptimisticReadHighConcurrency() {
        long stamp = stampedLock.tryOptimisticRead();
        boolean result = testData;

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = testData;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    @Benchmark
    @Threads(16)
    public boolean benchmarkOptimisticReadExtremeConcurrency() {
        long stamp = stampedLock.tryOptimisticRead();
        boolean result = testData;

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = testData;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    // =============================================================================
    // READ:WRITE RATIO BENCHMARKS (as requested)
    // =============================================================================

    @Benchmark
    @Threads(4)
    public boolean benchmarkReadWrite10_90() {
        // 10:90 read:write ratio simulation
        if (ThreadLocalRandom.current().nextInt(100) < 10) {
            // Read operation
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            // Write operation
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkReadWrite20_80() {
        // 20:80 read:write ratio
        if (ThreadLocalRandom.current().nextInt(100) < 20) {
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkReadWrite40_60() {
        // 40:60 read:write ratio
        if (ThreadLocalRandom.current().nextInt(100) < 40) {
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkReadWrite50_50() {
        // 50:50 read:write ratio
        if (ThreadLocalRandom.current().nextInt(100) < 50) {
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkReadWrite60_40() {
        // 60:40 read:write ratio
        if (ThreadLocalRandom.current().nextInt(100) < 60) {
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkReadWrite80_20() {
        // 80:20 read:write ratio
        if (ThreadLocalRandom.current().nextInt(100) < 80) {
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkReadWrite90_10() {
        // 90:10 read:write ratio - most realistic for production
        if (ThreadLocalRandom.current().nextInt(100) < 90) {
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    // =============================================================================
    // HIGH CONCURRENCY SCENARIOS
    // =============================================================================

    @Benchmark
    @Threads(8)
    public boolean benchmarkReadWrite90_10_HighConcurrency() {
        // 90:10 read:write ratio with high concurrency
        if (ThreadLocalRandom.current().nextInt(100) < 90) {
            long stamp = stampedLock.tryOptimisticRead();
            boolean result = testData;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                try {
                    result = testData;
                } finally {
                    stampedLock.unlockRead(stamp);
                }
            }
            return result;
        } else {
            long stamp = stampedLock.writeLock();
            try {
                testData = !testData;
                counter++;
                return testData;
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }

    @Benchmark
    @Threads(16)
    public boolean benchmarkOptimisticReadContention() {
        // High contention scenario to test optimistic read fallback behavior
        long stamp = stampedLock.tryOptimisticRead();
        boolean result = testData;

        // Simulate some computation
        if (ThreadLocalRandom.current().nextInt(1000) == 0) {
            Thread.yield(); // Occasionally yield to increase contention
        }

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = testData;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }
}