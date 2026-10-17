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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ActiveManagedCursorContainerImpl;
import org.apache.bookkeeper.mledger.impl.MockManagedCursor;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the latency of the add that rolls a full mutable bucket to a new ledger.
 *
 * <p>The invocation setup preloads the full bucket outside the measured interval. The direct mode is a
 * same-code blocking baseline; the async mode measures the seal-and-swap path used by the broker factory.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :microbench:shadowJar
 *   java -jar microbench/build/libs/microbench-*-benchmarks.jar \
 *       BucketDelayedDeliveryTrackerSealBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
@State(Scope.Thread)
public class BucketDelayedDeliveryTrackerSealBenchmark {

    private static final long FUTURE_DELIVERY_BASE_TIME_MILLIS = 4102444800000L;

    @Param({"10000", "100000", "1000000"})
    public int indexesPerBucket;

    @Param({"direct", "async"})
    public String buildMode;

    private BucketDelayedDeliveryTracker tracker;
    private Timer timer;
    private MockBucketSnapshotStorage storage;
    private ExecutorService snapshotBuildExecutor;

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception {
        timer = new HashedWheelTimer(new DefaultThreadFactory("seal-rollover-timer"),
                100, TimeUnit.MILLISECONDS);
        storage = new MockBucketSnapshotStorage();
        storage.start();

        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        MockManagedCursor cursor = MockManagedCursor.createCursor(container, "seal-rollover-cursor",
                PositionFactory.create(0, 0));
        String dispatcherName = "persistent://public/default/seal-rollover / " + cursor.getName();
        NoopDelayedDeliveryContext context = new NoopDelayedDeliveryContext(dispatcherName, cursor);

        Executor executor;
        snapshotBuildExecutor = null;
        if ("async".equals(buildMode)) {
            snapshotBuildExecutor = Executors.newSingleThreadExecutor(
                    new DefaultThreadFactory("seal-rollover-snapshot-builder"));
            executor = snapshotBuildExecutor;
        } else {
            executor = Runnable::run;
        }
        tracker = new BucketDelayedDeliveryTracker(context, timer, 1_000, Clock.systemUTC(), true, storage,
                indexesPerBucket, TimeUnit.MINUTES.toMillis(5), 5_000, -1, executor);

        for (int entryId = 0; entryId < indexesPerBucket; entryId++) {
            tracker.addMessage(1, entryId, FUTURE_DELIVERY_BASE_TIME_MILLIS + entryId);
        }
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() throws Exception {
        if (tracker != null) {
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (tracker.getBucketsCount().get() == 0 && System.nanoTime() < deadlineNanos) {
                Thread.sleep(1);
            }
            if (tracker.getBucketsCount().get() != 1) {
                throw new IllegalStateException("The rollover snapshot did not commit");
            }
            tracker.close();
        }
        if (snapshotBuildExecutor != null) {
            snapshotBuildExecutor.shutdown();
            if (!snapshotBuildExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                snapshotBuildExecutor.shutdownNow();
            }
        }
        if (storage != null) {
            storage.close();
        }
        if (timer != null) {
            timer.stop();
        }
    }

    @Benchmark
    public boolean sealRolloverAddLatency() {
        return tracker.addMessage(2, 0, FUTURE_DELIVERY_BASE_TIME_MILLIS);
    }
}
