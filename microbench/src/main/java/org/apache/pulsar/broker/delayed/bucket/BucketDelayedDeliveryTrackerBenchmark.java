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

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Clock;
import java.util.NavigableSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.MockPersistentDispatcher;
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
 * Enhanced JMH Benchmarks for BucketDelayedDeliveryTracker with ReentrantReadWriteLock.
 * This benchmark tests the performance improvements made by transitioning from
 * StampedLock to ReentrantReadWriteLock for fine-grained concurrency control.
 * <p>
 * Run with: mvn exec:java -Dexec.mainClass="org.openjdk.jmh.Main"
 *           -Dexec.args="BucketDelayedDeliveryTrackerBenchmark"
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class BucketDelayedDeliveryTrackerBenchmark {

    @Param({"90_10", "80_20", "70_30", "50_50"})
    public String readWriteRatio;

    @Param({"1000", "5000", "10000"})
    public int initialMessages;

    private BucketDelayedDeliveryTracker tracker;
    private Timer timer;
    private MockBucketSnapshotStorage storage;
    private MockPersistentDispatcher dispatcher;
    private AtomicLong messageIdGenerator;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        setupMockComponents();
        createTracker();
        preloadMessages();
        messageIdGenerator = new AtomicLong(initialMessages + 1);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (tracker != null) {
            tracker.close();
        }
        if (timer != null) {
            timer.stop();
        }
    }

    private void setupMockComponents() throws Exception {
        timer = new HashedWheelTimer(new DefaultThreadFactory("test-delayed-delivery"), 100, TimeUnit.MILLISECONDS);
        storage = new MockBucketSnapshotStorage();
        dispatcher = MockPersistentDispatcher.create();
    }

    private void createTracker() throws Exception {
        tracker = new BucketDelayedDeliveryTracker(
                dispatcher, timer, 1000, Clock.systemUTC(), true, storage,
                20, 1000, 100, 50
        );
    }

    private void preloadMessages() {
        // Preload messages to create realistic test conditions
        long baseTime = System.currentTimeMillis() + 10000; // Future delivery
        for (int i = 1; i <= initialMessages; i++) {
            tracker.addMessage(i, i, baseTime + i * 1000);
        }
    }

    // =============================================================================
    // READ-WRITE RATIO BENCHMARKS
    // =============================================================================

    @Benchmark
    public boolean benchmarkMixedOperations() {
        String[] parts = readWriteRatio.split("_");
        int readPercentage = Integer.parseInt(parts[0]);

        if (ThreadLocalRandom.current().nextInt(100) < readPercentage) {
            // Read operations
            return performReadOperation();
        } else {
            // Write operations
            return performWriteOperation();
        }
    }

    private boolean performReadOperation() {
        int operation = ThreadLocalRandom.current().nextInt(3);
        switch (operation) {
            case 0:
                // containsMessage
                long ledgerId = ThreadLocalRandom.current().nextLong(1, initialMessages + 100);
                long entryId = ThreadLocalRandom.current().nextLong(1, 1000);
                return tracker.containsMessage(ledgerId, entryId);
            case 1:
                // nextDeliveryTime
                try {
                    tracker.nextDeliveryTime();
                    return true;
                } catch (Exception e) {
                    return false;
                }
            case 2:
                // getNumberOfDelayedMessages
                long count = tracker.getNumberOfDelayedMessages();
                return count >= 0;
            default:
                return false;
        }
    }

    private boolean performWriteOperation() {
        long id = messageIdGenerator.getAndIncrement();
        long deliverAt = System.currentTimeMillis() + ThreadLocalRandom.current().nextLong(5000, 30000);
        return tracker.addMessage(id, id % 1000, deliverAt);
    }

    // =============================================================================
    // SPECIFIC OPERATION BENCHMARKS
    // =============================================================================

    @Benchmark
    @Threads(8)
    public boolean benchmarkConcurrentContainsMessage() {
        long ledgerId = ThreadLocalRandom.current().nextLong(1, initialMessages + 100);
        long entryId = ThreadLocalRandom.current().nextLong(1, 1000);
        return tracker.containsMessage(ledgerId, entryId);
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkConcurrentAddMessage() {
        long id = messageIdGenerator.getAndIncrement();
        long deliverAt = System.currentTimeMillis() + ThreadLocalRandom.current().nextLong(10000, 60000);
        return tracker.addMessage(id, id % 1000, deliverAt);
    }

    @Benchmark
    @Threads(2)
    public NavigableSet<Position> benchmarkConcurrentGetScheduledMessages() {
        // Create some messages ready for delivery
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            long id = messageIdGenerator.getAndIncrement();
            tracker.addMessage(id, id % 100, currentTime - 1000);
        }
        return tracker.getScheduledMessages(10);
    }

    @Benchmark
    @Threads(16)
    public long benchmarkConcurrentNextDeliveryTime() {
        try {
            return tracker.nextDeliveryTime();
        } catch (Exception e) {
            return -1;
        }
    }

    @Benchmark
    @Threads(1)
    public long benchmarkGetNumberOfDelayedMessages() {
        return tracker.getNumberOfDelayedMessages();
    }

    // =============================================================================
    // HIGH CONTENTION SCENARIOS
    // =============================================================================

    @Benchmark
    @Threads(32)
    public boolean benchmarkHighContentionMixedOperations() {
        return benchmarkMixedOperations();
    }

    @Benchmark
    @Threads(16)
    public boolean benchmarkContentionReads() {
        return performReadOperation();
    }

    @Benchmark
    @Threads(8)
    public boolean benchmarkContentionWrites() {
        return performWriteOperation();
    }

    // =============================================================================
    // THROUGHPUT BENCHMARKS
    // =============================================================================

    @Benchmark
    @Threads(1)
    public boolean benchmarkSingleThreadedThroughput() {
        return benchmarkMixedOperations();
    }

    @Benchmark
    @Threads(4)
    public boolean benchmarkMediumConcurrencyThroughput() {
        return benchmarkMixedOperations();
    }

    @Benchmark
    @Threads(8)
    public boolean benchmarkHighConcurrencyThroughput() {
        return benchmarkMixedOperations();
    }

}
