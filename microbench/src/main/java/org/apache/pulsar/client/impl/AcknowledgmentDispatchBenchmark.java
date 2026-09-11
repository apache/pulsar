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
import org.openjdk.jmh.infra.Blackhole;

/** Exercises the real ACK tracker with a disconnected consumer; excludes network and receipt completion. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class AcknowledgmentDispatchBenchmark {
    @Param({"1", "1000"})
    public int entries;

    @Param({"insert", "duplicate"})
    public String pendingIds;

    private PulsarClientImpl client;
    private ExecutorProvider executorProvider;
    private BenchmarkConsumer consumer;
    private PersistentAcknowledgmentsGroupingTracker tracker;
    private MessageIdImpl[] messageIds;

    @Setup
    public void setup() throws Exception {
        client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .ioThreads(1).listenerThreads(1).statsInterval(0, TimeUnit.SECONDS).build();
        executorProvider = new ExecutorProvider(1, "ack-dispatch-benchmark", true);
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("benchmark");
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.HOURS.toMicros(1));
        conf.setMaxAcknowledgmentGroupSize(Integer.MAX_VALUE);
        consumer = new BenchmarkConsumer(client, conf, executorProvider);
        tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, client.eventLoopGroup());
        messageIds = new MessageIdImpl[entries];
        for (int i = 0; i < entries; i++) {
            messageIds[i] = new MessageIdImpl(1000, i, 0);
            if ("duplicate".equals(pendingIds)) {
                tracker.addAcknowledgment(messageIds[i], AckType.Individual, Collections.emptyMap());
            }
        }
    }

    @Benchmark
    public int acknowledgeGroup(Blackhole blackhole) {
        for (MessageIdImpl id : messageIds) {
            blackhole.consume(tracker.addAcknowledgment(id, AckType.Individual, Collections.emptyMap()));
        }
        int pending = tracker.getPendingIndividualAcksSize();
        if (pending != entries) {
            throw new IllegalStateException("Unexpected pending ACK count: " + pending);
        }
        if ("insert".equals(pendingIds)) {
            // The disconnected flush does not serialize commands; clear bounds the pending set between groups.
            tracker.flushAndClean();
        }
        return pending;
    }

    @TearDown
    public void tearDown() throws Exception {
        try {
            if (tracker != null) {
                tracker.close();
            }
            if (consumer != null) {
                consumer.close();
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
    }

    private static final class BenchmarkConsumer extends ConsumerImpl<byte[]> {
        BenchmarkConsumer(PulsarClientImpl client, ConsumerConfigurationData<byte[]> conf,
                          ExecutorProvider executorProvider) {
            super(client, "persistent://public/default/ack-dispatch-benchmark", conf, executorProvider,
                    0, false, false, new CompletableFuture<>(), null, 0, Schema.BYTES, null, true);
        }

        @Override
        void grabCnx() {
            // Suppress connection startup while retaining the normal consumer/tracker code; no network is needed.
        }
    }
}
