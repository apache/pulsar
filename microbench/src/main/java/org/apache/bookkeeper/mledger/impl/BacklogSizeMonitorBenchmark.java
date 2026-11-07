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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
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
 * Samples acquisition of the ManagedLedgerImpl monitor while a background thread collects backlog estimates.
 * The benchmarked critical section is a synthetic write, not internalAsyncAddEntry or an end-to-end write.
 * Statistics run at a fixed target rate; the trial fails if an interval is missed.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class BacklogSizeMonitorBenchmark {
    @Param({"none", "perSubscription", "snapshot"})
    private String statsMode;

    @Param({"1"})
    private int statsPerSecond;

    @Param({"500"})
    private int ledgerCount;

    @Param({"10000"})
    private int subscriptionCount;

    private BacklogSizeSnapshotBenchmark fixture;
    private volatile boolean running;
    private volatile Throwable statsFailure;
    private volatile long statsResult;
    private Thread statsThread;
    private long syntheticWrites;
    private long missedIntervals;

    @Setup(Level.Trial)
    public void setup() {
        fixture = new BacklogSizeSnapshotBenchmark();
        fixture.ledgerCount = ledgerCount;
        fixture.subscriptionCount = subscriptionCount;
        fixture.setup();
        running = true;
        if (!"none".equals(statsMode)) {
            statsThread = new Thread(this::collectStats, "backlog-stats-benchmark");
            statsThread.setDaemon(true);
            statsThread.start();
        }
    }

    private void collectStats() {
        long period = TimeUnit.SECONDS.toNanos(1) / statsPerSecond;
        long next = System.nanoTime();
        try {
            while (running) {
                long wait = next - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                    continue;
                }
                statsResult = "snapshot".equals(statsMode) ? fixture.snapshot() : fixture.perSubscription();
                next += period;
                long now = System.nanoTime();
                if (next <= now) {
                    long missed = (now - next) / period + 1;
                    missedIntervals += missed;
                    next += missed * period;
                }
            }
        } catch (Throwable t) {
            statsFailure = t;
        }
    }

    @Benchmark
    public long simulatedWriteMonitor() {
        synchronized (fixture.ledger) {
            return ++syntheticWrites;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        running = false;
        if (statsThread != null) {
            LockSupport.unpark(statsThread);
            statsThread.join();
        }
        if (statsFailure != null) {
            throw new IllegalStateException("Stats collection failed", statsFailure);
        }
        if (missedIntervals != 0) {
            throw new IllegalStateException("Stats collection missed " + missedIntervals + " intervals");
        }
    }
}
