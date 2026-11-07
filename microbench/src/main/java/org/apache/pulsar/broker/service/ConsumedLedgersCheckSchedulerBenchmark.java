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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
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
 * Measures submission cost for a complete maintenance round on loaded persistent topics. The paced mode runs the
 * production scheduler; burst inlines the original BrokerService checkConsumedLedgers/forEachTopic traversal.
 *
 * <p>Every ledger stub counts both calls as worker submissions, modeling topics with durable cursors. The timer stub
 * retains only the next task and advances it without sleeping, skipping the fixed 20-millisecond delay between
 * topic submissions and any wait between rounds. The result includes mock-call overhead and excludes real executor
 * scheduling costs. Stub-only mocks avoid retaining invocation histories across benchmark rounds.
 *
 * <p>The returned value is the largest submission batch: 2 * topicCount for burst, at most 2 for paced. This models
 * the queue growth from one batch before workers drain it; it does not measure a real worker queue, maintenance task
 * execution time, or application read/write latency. Those require a running broker and profiling.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ConsumedLedgersCheckSchedulerBenchmark {
    @State(Scope.Thread)
    public static class RoundState {
        @Param({"100", "10000"})
        public int topicCount;

        @Param({"burst", "paced"})
        public String submissionMode;

        private final Map<String, CompletableFuture<Optional<Topic>>> topics = new ConcurrentHashMap<>();
        private ConsumedLedgersCheckScheduler scheduler;
        private Runnable pendingTask;
        private int submissions;
        private int maximumBatchSubmissions;
        private boolean paced;

        @Setup(Level.Trial)
        public void setup() {
            paced = submissionMode.equals("paced");
            ManagedLedger ledger = mock(ManagedLedger.class, withSettings().stubOnly());
            doAnswer(invocation -> {
                submissions++;
                return null;
            }).when(ledger).trimConsumedLedgersInBackground(any());
            doAnswer(invocation -> {
                submissions++;
                return null;
            }).when(ledger).rolloverCursorsInBackground();
            for (int i = 0; i < topicCount; i++) {
                PersistentTopic topic = mock(PersistentTopic.class, withSettings().stubOnly());
                when(topic.getManagedLedger()).thenReturn(ledger);
                topics.put("persistent://public/default/topic-" + i,
                        CompletableFuture.completedFuture(Optional.of(topic)));
            }
            if (paced) {
                ScheduledExecutorService executor = mock(ScheduledExecutorService.class, withSettings().stubOnly());
                ScheduledFuture<?> future = mock(ScheduledFuture.class, withSettings().stubOnly());
                doAnswer(invocation -> {
                    if (pendingTask != null) {
                        throw new AssertionError("More than one maintenance timer is pending");
                    }
                    pendingTask = invocation.getArgument(0);
                    return future;
                }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
                scheduler = new ConsumedLedgersCheckScheduler(topics);
                scheduler.start(executor, Duration.ofSeconds(120));
            }
        }

        private void runNextBatch() {
            Runnable task = pendingTask;
            pendingTask = null;
            if (task == null) {
                throw new AssertionError("Maintenance timer was not rescheduled");
            }
            int previousSubmissions = submissions;
            task.run();
            maximumBatchSubmissions = Math.max(maximumBatchSubmissions, submissions - previousSubmissions);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (scheduler != null) {
                scheduler.close();
            }
            if (submissions != 2 * topicCount || maximumBatchSubmissions != (paced ? 2 : 2 * topicCount)) {
                throw new AssertionError("Maintenance submissions did not match a complete round");
            }
        }
    }

    @Benchmark
    public int submitRound(RoundState state) {
        state.submissions = 0;
        state.maximumBatchSubmissions = 0;
        if (state.paced) {
            int batches = 0;
            while (state.submissions < 2 * state.topicCount && batches++ < state.topicCount) {
                state.runNextBatch();
            }
        } else {
            // Original BrokerService entry point, with no maintenance algorithm reproduced in the benchmark.
            state.topics.forEach((name, future) -> BrokerService.extractTopic(future).ifPresent(topic -> {
                if (topic instanceof PersistentTopic persistentTopic) {
                    Optional.ofNullable(persistentTopic.getManagedLedger()).ifPresent(ledger -> {
                        ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
                        ledger.rolloverCursorsInBackground();
                    });
                }
            }));
            state.maximumBatchSubmissions = state.submissions;
        }
        if (state.submissions != 2 * state.topicCount) {
            throw new AssertionError("Maintenance round did not submit every topic");
        }
        return state.maximumBatchSubmissions;
    }
}
