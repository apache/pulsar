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
package org.apache.pulsar.broker.stats.prometheus;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.prometheus.client.Collector;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedCursorMXBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.stats.metrics.ManagedCursorMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
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
 * Measures one generation of the managed-ledger and optional cursor Prometheus metrics.
 *
 * <p>The fixtures provide stable stats without disk access, backlog computation, or a running broker. Both paths
 * perform the production aggregation and text encoding, but this benchmark does not measure an entire scrape,
 * compression, or transport. The output buffer is expanded during setup and reused so its allocation does not
 * obscure the intermediate allocations removed by direct generation. Use {@code -prof gc} to measure bytes/op.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
public class ManagedLedgerMetricsBenchmark {

    private static final String CLUSTER = "benchmark";

    @Param({"1000", "10000"})
    public int ledgerCount;

    @Param({"100"})
    public int namespaceCount;

    @Param({"0", "10"})
    public int cursorsPerLedger;

    private PulsarService pulsar;
    private ByteBuf buffer;

    @Setup(Level.Trial)
    public void setup() {
        Map<String, ManagedLedger> ledgers = new LinkedHashMap<>();
        for (int i = 0; i < ledgerCount; i++) {
            String ledgerName = "tenant/namespace-" + (i % namespaceCount) + "/persistent/topic-" + i;
            List<ManagedCursor> cursors = new ArrayList<>(cursorsPerLedger);
            for (int j = 0; j < cursorsPerLedger; j++) {
                String cursorName = "subscription-" + j;
                FixtureCursorStats cursorStats = new FixtureCursorStats();
                ManagedCursor cursor = (ManagedCursor) Proxy.newProxyInstance(ManagedCursor.class.getClassLoader(),
                        new Class<?>[]{ManagedCursor.class}, (proxy, method, args) -> switch (method.getName()) {
                            case "getName" -> cursorName;
                            case "getStats" -> cursorStats;
                            case "getTotalNonContiguousDeletedMessagesRange" -> 7;
                            default -> throw new UnsupportedOperationException(method.getName());
                        });
                cursors.add(cursor);
            }
            FixtureLedgerStats ledgerStats = new FixtureLedgerStats(i, cursorsPerLedger);
            ManagedLedger ledger = (ManagedLedger) Proxy.newProxyInstance(ManagedLedger.class.getClassLoader(),
                    new Class<?>[]{ManagedLedger.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "getName" -> ledgerName;
                        case "getStats" -> ledgerStats;
                        case "getCursors" -> cursors;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
            ledgers.put(ledgerName, ledger);
        }

        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setStatsPeriodSeconds(60);
        ManagedLedgerFactory factory = (ManagedLedgerFactory) Proxy.newProxyInstance(
                ManagedLedgerFactory.class.getClassLoader(), new Class<?>[]{ManagedLedgerFactory.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getManagedLedgers" -> ledgers;
                    case "getConfig" -> factoryConfig;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName(CLUSTER);
        config.setMetadataStoreUrl("memory:metrics-benchmark");
        config.setNumIOThreads(1);
        config.setNumExecutorThreadPoolSize(1);
        pulsar = new PulsarService(config) {
            @Override
            public ManagedLedgerFactory getDefaultManagedLedgerFactory() {
                return factory;
            }
        };

        buffer = Unpooled.directBuffer(1024 * 1024);
        // Preallocate the output storage and warm class initialization outside the measured operation.
        legacy();
        direct();
        buffer.clear();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        try {
            if (buffer != null) {
                buffer.release();
            }
        } finally {
            if (pulsar != null) {
                pulsar.close();
            }
        }
    }

    @Benchmark
    public int legacy() {
        buffer.clear();
        SimpleTextOutputStream stream = new SimpleTextOutputStream(buffer);
        PrometheusMetricsGenerator.parseMetricsToPrometheusMetrics(new ManagedLedgerMetrics(pulsar).generate(),
                CLUSTER, Collector.Type.GAUGE, stream);
        if (cursorsPerLedger > 0) {
            PrometheusMetricsGenerator.parseMetricsToPrometheusMetrics(new ManagedCursorMetrics(pulsar).generate(),
                    CLUSTER, Collector.Type.GAUGE, stream);
        }
        return buffer.writerIndex();
    }

    @Benchmark
    public int direct() {
        buffer.clear();
        SimpleTextOutputStream stream = new SimpleTextOutputStream(buffer);
        new ManagedLedgerMetrics(pulsar).generate(stream);
        if (cursorsPerLedger > 0) {
            new ManagedCursorMetrics(pulsar).generate(stream);
        }
        return buffer.writerIndex();
    }

    private static final class FixtureLedgerStats extends ManagedLedgerMBeanImpl {
        private final long storedSize;
        private final long backlog;
        private final double writeRate;
        private final long[] switchLatencyBuckets = new long[ENTRY_LATENCY_BUCKETS_USEC.length + 1];

        private FixtureLedgerStats(int index, int cursorCount) {
            super(null);
            storedSize = 1_000_000_000_000L + index;
            backlog = 1000L * cursorCount;
            writeRate = 0.00000012345 + index;
            for (int i = 0; i < switchLatencyBuckets.length; i++) {
                switchLatencyBuckets[i] = i + 1;
            }
            for (long boundary : ENTRY_LATENCY_BUCKETS_USEC) {
                getInternalAddEntryLatencyBuckets().addValue(boundary);
                getInternalLedgerAddEntryLatencyBuckets().addValue(boundary);
            }
            for (long boundary : ENTRY_SIZE_BUCKETS_BYTES) {
                getInternalEntrySizeBuckets().addValue(boundary);
            }
            getInternalAddEntryLatencyBuckets().addValue(Long.MAX_VALUE);
            getInternalLedgerAddEntryLatencyBuckets().addValue(Long.MAX_VALUE);
            getInternalEntrySizeBuckets().addValue(Long.MAX_VALUE);
            addReadEntriesSample(1234, 9876543);
            recordReadEntriesOpsCacheMisses(123, 987654);
            recordAddEntryError();
            recordReadEntriesError();
            addMarkDeleteOp();
            refreshStats(60, TimeUnit.SECONDS);
        }

        @Override
        public long getStoredMessagesSize() {
            return storedSize;
        }

        @Override
        public long getNumberOfMessagesInBacklog() {
            return backlog;
        }

        @Override
        public double getAddEntryMessagesRate() {
            return writeRate;
        }

        @Override
        public double getAddEntryBytesRate() {
            return writeRate * 1024;
        }

        @Override
        public double getAddEntryWithReplicasBytesRate() {
            return writeRate * 3072;
        }

        @Override
        public long[] getLedgerSwitchLatencyBuckets() {
            return switchLatencyBuckets;
        }
    }

    private static final class FixtureCursorStats extends ManagedCursorMXBeanImpl {

        private FixtureCursorStats() {
            super(null);
            for (int i = 0; i < 256; i++) {
                persistToLedger(true);
                persistToZookeeper(true);
            }
            persistToLedger(false);
            persistToZookeeper(false);
            addReadCursorLedgerSize(1_000_000_000_000L);
        }

        @Override
        public long getWriteCursorLedgerSize() {
            return 3_000_000_000_000L;
        }

        @Override
        public long getWriteCursorLedgerLogicalSize() {
            return 1_000_000_000_000L;
        }
    }
}
