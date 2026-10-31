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
package org.apache.pulsar.client.impl;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

/** Measures partial batch ACK updates on one shared tracker, excluding network writes and receipt completion. */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class BatchIndexAcknowledgmentBenchmark {
    @Param({"1", "128"})
    public int entriesPerThread;

    private final AtomicInteger expectedEntries = new AtomicInteger();
    private PulsarClientImpl client;
    private ExecutorProvider executorProvider;
    private BenchmarkConsumer consumer;
    private PersistentAcknowledgmentsGroupingTracker tracker;

    @Setup
    @SuppressWarnings("deprecation")
    public void setup() throws Exception {
        client = (PulsarClientImpl) PulsarClient.builder().serviceUrl("pulsar://localhost:6650")
                .ioThreads(1).listenerThreads(1).statsInterval(0, TimeUnit.SECONDS).build();
        executorProvider = new ExecutorProvider(1, "batch-ack-benchmark", true);
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("benchmark");
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.HOURS.toMicros(1));
        conf.setMaxAcknowledgmentGroupSize(Integer.MAX_VALUE);
        conf.setBatchIndexAckEnabled(true);
        conf.setAckReceiptEnabled(false);
        consumer = new BenchmarkConsumer(client, conf, executorProvider);
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, client.eventLoopGroup());
        expectedEntries.set(0);
    }

    @State(Scope.Thread)
    public static class BatchIds {
        private BatchMessageIdImpl[] ids;
        private int next;

        @Setup
        public void setup(BatchIndexAcknowledgmentBenchmark shared, ThreadParams thread) {
            ids = new BatchMessageIdImpl[shared.entriesPerThread];
            for (int i = 0; i < ids.length; i++) {
                // Each thread has distinct batches, so their input ACK-set and bitmap locks cannot hide
                // contention on tracker-wide synchronization. The other 63 indices stay unacknowledged.
                ids[i] = new BatchMessageIdImpl(1000 + thread.getThreadIndex(), i, 0, 0, 64,
                        BatchMessageIdImpl.newAckSet(64));
                shared.tracker.addAcknowledgment(ids[i], AckType.Individual, Collections.emptyMap());
            }
            shared.expectedEntries.addAndGet(ids.length);
        }
    }

    @Benchmark
    public CompletableFuture<Void> acknowledgePartialBatch(BatchIds batches) {
        BatchMessageIdImpl id = batches.ids[batches.next];
        batches.next = (batches.next + 1) % batches.ids.length;
        return tracker.addAcknowledgment(id, AckType.Individual, Collections.emptyMap());
    }

    @TearDown
    public void cleanup() throws Exception {
        int pending = tracker == null ? 0 : tracker.pendingIndividualBatchIndexAcks.size();
        try {
            try {
                if (tracker != null) {
                    tracker.close();
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        } finally {
            try {
                if (client != null) {
                    client.close();
                }
            } finally {
                if (executorProvider != null) {
                    executorProvider.shutdownNow();
                }
            }
        }
        if (pending != expectedEntries.get()) {
            throw new IllegalStateException("Unexpected number of pending partial batch ACKs");
        }
    }

    private static final class BenchmarkConsumer extends ConsumerImpl<byte[]> {
        BenchmarkConsumer(PulsarClientImpl client, ConsumerConfigurationData<byte[]> conf,
                          ExecutorProvider executorProvider) {
            super(client, "persistent://public/default/batch-ack-benchmark", conf, executorProvider,
                    0, false, false, new CompletableFuture<>(), null, 0, Schema.BYTES, null, true);
        }

        @Override
        void grabCnx() {
            // Exercise the normal consumer callbacks without starting a broker connection.
        }
    }
}
