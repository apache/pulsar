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
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ActiveManagedCursorContainerImpl;
import org.apache.bookkeeper.mledger.impl.MockManagedCursor;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.NoopDelayedDeliveryContext;
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
@Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
@Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
@Fork(1)
public class BucketDelayedDeliveryTrackerBenchmark {

    @Param({"90_10", "80_20", "70_30", "50_50"})
    public String readWriteRatio;

    @Param({"1000", "5000", "8000"})
    public int initialMessages;

    private BucketDelayedDeliveryTracker tracker;
    private Timer timer;
    private MockBucketSnapshotStorage storage;
    private NoopDelayedDeliveryContext context;
    private AtomicLong messageIdGenerator;
    /**
     * Maximum number of additional unique (ledgerId, entryId) positions to
     * introduce per trial on top of {@link #initialMessages}. This allows
     * controlling the memory footprint of the benchmark while still applying
     * sustained write pressure to the tracker.
     *
     * <p>Use {@code -p maxAdditionalUniqueMessages=...} on the JMH command line
     * to tune the load. The default value is conservative for local runs.</p>
     */
    @Param({"1000000"})
    public long maxAdditionalUniqueMessages;
    /**
     * Upper bound on the absolute message id that will be used to derive
     * (ledgerId, entryId) positions during a single trial.
     */
    private long maxUniqueMessageId;
    /**
     * In real Pulsar usage, {@link DelayedDeliveryTracker#addMessage(long, long, long)} is invoked
     * by a single dispatcher thread and messages arrive in order of (ledgerId, entryId).
     * <p>
     * To reflect this invariant in the benchmark, all write operations that end up calling
     * {@code tracker.addMessage(...)} are serialized via this mutex so that the tracker only
     * ever observes a single writer with monotonically increasing ids, even when JMH runs the
     * benchmark method with multiple threads.
     */
    private final Object writeMutex = new Object();

    @Setup(Level.Trial)
    public void setup() throws Exception {
        setupMockComponents();
        createTracker();
        preloadMessages();
        messageIdGenerator = new AtomicLong(initialMessages + 1);
        // Allow a bounded number of additional unique messages per trial to avoid
        // unbounded memory growth while still stressing the indexing logic.
        maxUniqueMessageId = initialMessages + maxAdditionalUniqueMessages;
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

        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        MockManagedCursor cursor = MockManagedCursor.createCursor(container, "test-cursor",
                PositionFactory.create(0, 0));
        // Use the same "<topic> / <cursor>" naming pattern as real dispatchers,
        // so that Bucket.asyncSaveBucketSnapshot can correctly derive topicName.
        String dispatcherName = "persistent://public/default/jmh-topic / " + cursor.getName();
        context = new NoopDelayedDeliveryContext(dispatcherName, cursor);
    }

    private void createTracker() throws Exception {
        tracker = new BucketDelayedDeliveryTracker(
                context, timer, 1000, Clock.systemUTC(), true, storage,
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

    /**
     * Serialize calls to {@link BucketDelayedDeliveryTracker#addMessage(long, long, long)} and
     * ensure (ledgerId, entryId) are generated in a strictly increasing sequence, matching the
     * real dispatcher single-threaded behaviour.
     */
    private boolean addMessageSequential(long deliverAt, int entryIdModulo) {
        synchronized (writeMutex) {
            long id = messageIdGenerator.getAndIncrement();
            // Limit the number of distinct positions that are introduced into the tracker
            // to keep memory usage bounded. Once the upper bound is reached, we re-use
            // the last position id so that subsequent calls behave like updates to
            // existing messages and are short-circuited by containsMessage checks.
            long boundedId = Math.min(id, maxUniqueMessageId);
            long ledgerId = boundedId;
            long entryId = boundedId % entryIdModulo;
            return tracker.addMessage(ledgerId, entryId, deliverAt);
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
        long deliverAt = System.currentTimeMillis() + ThreadLocalRandom.current().nextLong(5000, 30000);
        return addMessageSequential(deliverAt, 1000);
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
        long deliverAt = System.currentTimeMillis() + ThreadLocalRandom.current().nextLong(10000, 60000);
        return addMessageSequential(deliverAt, 1000);
    }

    @Benchmark
    @Threads(2)
    public NavigableSet<Position> benchmarkConcurrentGetScheduledMessages() {
        // Create some messages ready for delivery
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            addMessageSequential(currentTime - 1000, 100);
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
