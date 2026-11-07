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
package org.apache.pulsar.broker.transaction.timeout;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures timeout bookkeeping for a steady stream of quickly completed transactions.
 *
 * <p>One logical clock tick represents one new transaction. The active pool completes a randomly
 * selected transaction on each tick, giving an average lifetime of {@code activeCount / rate}
 * seconds and out-of-order completions. Both implementations perform the same active-pool work.
 * The legacy queue keeps completed transactions until their deadline; the indexed queue removes
 * them on completion. Expiration work is included in both measurements.
 *
 * <p>The legacy queue holds {@code rate * timeoutSeconds} records (24 bytes each before capacity
 * slack); the indexed queue holds approximately {@code rate * lifetimeMillis / 1000} records plus
 * its primitive index. The 1-hour scenario requires about 864 MB for the legacy records alone.
 * Run with {@code -prof gc} to measure allocation per transaction; that profiler does not measure
 * retained queue memory. These benchmarks exclude metadata lookup, exception and future allocation
 * when an already completed transaction expires, so the legacy result understates that cost.
 * The expiration-only benchmarks keep every transaction until its deadline, measuring the index
 * overhead when both queues retain the same number of records.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class TransactionTimeoutQueueBenchmark {
    private static final int BATCH_SIZE = 1024;

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public long retainCompletedTransactions(LegacyState state) {
        long completedAndExpired = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            long sequenceId = state.nextSequenceId++;
            completedAndExpired += state.completeTransaction(sequenceId);
            while (!state.queue.isEmpty() && state.queue.peekN1() <= sequenceId) {
                completedAndExpired += state.queue.peekN3();
                state.queue.pop();
            }
            state.queue.add(sequenceId + state.timeoutTransactions, 1, sequenceId);
        }
        return completedAndExpired;
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public long removeCompletedTransactions(IndexedState state) {
        long completed = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            long sequenceId = state.nextSequenceId++;
            long completedSequenceId = state.completeTransaction(sequenceId);
            while (!state.queue.isEmpty() && state.queue.peekTimeout() <= sequenceId) {
                state.queue.pop();
            }
            if (state.queue.remove(completedSequenceId)) {
                completed += completedSequenceId;
            }
            state.queue.add(sequenceId, sequenceId + state.timeoutTransactions);
        }
        return completed;
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public long expireLegacyTransactions(LegacyExpirationState state) {
        long expired = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            long sequenceId = state.nextSequenceId++;
            expired += state.queue.peekN3();
            state.queue.pop();
            state.queue.add(sequenceId + state.pendingTransactions, 1, sequenceId);
        }
        return expired;
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public long expireIndexedTransactions(IndexedExpirationState state) {
        long expired = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            long sequenceId = state.nextSequenceId++;
            expired += state.queue.peekSequenceId();
            state.queue.pop();
            state.queue.add(sequenceId, sequenceId + state.pendingTransactions);
        }
        return expired;
    }

    @State(Scope.Thread)
    public abstract static class WorkloadState {
        @Param({"10000"})
        public int transactionsPerSecond;

        @Param({"60", "3600"})
        public int timeoutSeconds;

        @Param({"100", "1000"})
        public int lifetimeMillis;

        long nextSequenceId;
        long timeoutTransactions;
        int activeCount;
        private long[] activeTransactions;
        private int random;

        void initializeWorkload() {
            timeoutTransactions = (long) transactionsPerSecond * timeoutSeconds;
            activeCount = Math.toIntExact((long) transactionsPerSecond * lifetimeMillis / 1000);
            if (activeCount <= 0 || activeCount >= timeoutTransactions) {
                throw new IllegalArgumentException("Expected a positive transaction lifetime below its timeout");
            }
            nextSequenceId = timeoutTransactions;
            random = 1;
            activeTransactions = new long[activeCount];
            for (int i = 0; i < activeCount; i++) {
                activeTransactions[i] = nextSequenceId - activeCount + i;
            }
        }

        long completeTransaction(long sequenceId) {
            random = random * 1664525 + 1013904223;
            int slot = (random & Integer.MAX_VALUE) % activeCount;
            long completed = activeTransactions[slot];
            activeTransactions[slot] = sequenceId;
            return completed;
        }
    }

    @State(Scope.Thread)
    public static class LegacyState extends WorkloadState {
        private TripleLongPriorityQueue queue;

        @Setup(Level.Trial)
        public void setup() {
            initializeWorkload();
            queue = new TripleLongPriorityQueue();
            for (long sequenceId = 0; sequenceId < timeoutTransactions; sequenceId++) {
                queue.add(sequenceId + timeoutTransactions, 1, sequenceId);
            }
        }

        @TearDown(Level.Trial)
        public void cleanup() {
            queue.close();
        }
    }

    @State(Scope.Thread)
    public static class IndexedState extends WorkloadState {
        private TransactionTimeoutQueue queue;

        @Setup(Level.Trial)
        public void setup() {
            initializeWorkload();
            queue = new TransactionTimeoutQueue();
            for (long sequenceId = nextSequenceId - activeCount; sequenceId < nextSequenceId; sequenceId++) {
                queue.add(sequenceId, sequenceId + timeoutTransactions);
            }
        }

        @TearDown(Level.Trial)
        public void cleanup() {
            queue.clear();
        }
    }

    @State(Scope.Thread)
    public abstract static class ExpirationState {
        @Param({"1000", "10000"})
        public int pendingTransactions;

        long nextSequenceId;
    }

    @State(Scope.Thread)
    public static class LegacyExpirationState extends ExpirationState {
        private TripleLongPriorityQueue queue;

        @Setup(Level.Trial)
        public void setup() {
            queue = new TripleLongPriorityQueue();
            for (long sequenceId = 0; sequenceId < pendingTransactions; sequenceId++) {
                queue.add(sequenceId + pendingTransactions, 1, sequenceId);
            }
            nextSequenceId = pendingTransactions;
        }

        @TearDown(Level.Trial)
        public void cleanup() {
            queue.close();
        }
    }

    @State(Scope.Thread)
    public static class IndexedExpirationState extends ExpirationState {
        private TransactionTimeoutQueue queue;

        @Setup(Level.Trial)
        public void setup() {
            queue = new TransactionTimeoutQueue();
            for (long sequenceId = 0; sequenceId < pendingTransactions; sequenceId++) {
                queue.add(sequenceId, sequenceId + pendingTransactions);
            }
            nextSequenceId = pendingTransactions;
        }

        @TearDown(Level.Trial)
        public void cleanup() {
            queue.clear();
        }
    }
}
