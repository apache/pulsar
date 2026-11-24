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
package org.apache.pulsar.broker.stats.metrics;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

public class ManagedCursorMetrics extends AbstractMetrics {

    private static final String[] PROMETHEUS_METRIC_NAMES = {
            "pulsar_ml_cursor_nonContiguousDeletedMessagesRange",
            "pulsar_ml_cursor_persistLedgerSucceed",
            "pulsar_ml_cursor_persistLedgerErrors",
            "pulsar_ml_cursor_persistZookeeperSucceed",
            "pulsar_ml_cursor_persistZookeeperErrors",
            "pulsar_ml_cursor_writeLedgerSize",
            "pulsar_ml_cursor_writeLedgerLogicalSize",
            "pulsar_ml_cursor_readLedgerSize"
    };

    private Map<String, String> dimensionMap;
    private List<Metrics> metricsCollection;

    public ManagedCursorMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metricsCollection = new ArrayList<>();
        this.dimensionMap = new HashMap<>();
    }

    @Override
    public synchronized List<Metrics> generate() {
        return aggregate();
    }


    /**
     * Writes each cursor's Prometheus samples without retaining an intermediate metrics object.
     */
    public synchronized void generate(SimpleTextOutputStream stream) {
        String cluster = getLocalClusterName();
        ByteBuf[] buffers = new ByteBuf[PROMETHEUS_METRIC_NAMES.length];
        SimpleTextOutputStream[] metricStreams = new SimpleTextOutputStream[PROMETHEUS_METRIC_NAMES.length];
        try {
            for (Map.Entry<String, ManagedLedger> entry : getManagedLedgers().entrySet()) {
                String ledgerName = entry.getKey();
                String namespace = parseNamespaceFromLedgerName(ledgerName);
                for (ManagedCursor cursor : entry.getValue().getCursors()) {
                    if (metricStreams[0] == null) {
                        for (int i = 0; i < metricStreams.length; i++) {
                            // Track buffers before constructing streams to allow cleanup after partial initialization.
                            buffers[i] = PulsarByteBufAllocator.DEFAULT.directBuffer();
                            metricStreams[i] = new SimpleTextOutputStream(buffers[i]);
                            metricStreams[i].write("# TYPE ").write(PROMETHEUS_METRIC_NAMES[i]).write(" gauge\n");
                        }
                    }
                    ManagedCursorMXBean stats = cursor.getStats();
                    String cursorName = cursor.getName();
                    writeSample(metricStreams[0], PROMETHEUS_METRIC_NAMES[0],
                            cursor.getTotalNonContiguousDeletedMessagesRange(),
                            cluster, namespace, ledgerName, cursorName);
                    writeSample(metricStreams[1], PROMETHEUS_METRIC_NAMES[1], stats.getPersistLedgerSucceed(),
                            cluster, namespace, ledgerName, cursorName);
                    writeSample(metricStreams[2], PROMETHEUS_METRIC_NAMES[2], stats.getPersistLedgerErrors(),
                            cluster, namespace, ledgerName, cursorName);
                    writeSample(metricStreams[3], PROMETHEUS_METRIC_NAMES[3], stats.getPersistZookeeperSucceed(),
                            cluster, namespace, ledgerName, cursorName);
                    writeSample(metricStreams[4], PROMETHEUS_METRIC_NAMES[4], stats.getPersistZookeeperErrors(),
                            cluster, namespace, ledgerName, cursorName);
                    writeSample(metricStreams[5], PROMETHEUS_METRIC_NAMES[5], stats.getWriteCursorLedgerSize(),
                            cluster, namespace, ledgerName, cursorName);
                    writeSample(metricStreams[6], PROMETHEUS_METRIC_NAMES[6], stats.getWriteCursorLedgerLogicalSize(),
                            cluster, namespace, ledgerName, cursorName);
                    writeSample(metricStreams[7], PROMETHEUS_METRIC_NAMES[7], stats.getReadCursorLedgerSize(),
                            cluster, namespace, ledgerName, cursorName);
                }
            }
            // A TYPE and all its samples must be contiguous, even when there are multiple cursors.
            for (int i = 0; i < buffers.length; i++) {
                if (buffers[i] != null) {
                    stream.write(buffers[i]);
                    buffers[i].release();
                    buffers[i] = null;
                }
            }
        } finally {
            for (ByteBuf buffer : buffers) {
                if (buffer != null) {
                    buffer.release();
                }
            }
        }
    }

    private static void writeSample(SimpleTextOutputStream stream, String name, long value,
                                    String cluster, String namespace, String ledgerName, String cursorName) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\", cursor_name=\"").write(cursorName)
                .write("\", ledger_name=\"").write(ledgerName).write("\", namespace=\"").write(namespace)
                .write("\"} ").write(value).write('\n');
    }

    /**
     * Aggregation by namespace, ledger, cursor.
     *
     * @return List<Metrics>
     */
    private List<Metrics> aggregate() {
        metricsCollection.clear();
        for (Map.Entry<String, ManagedLedger> e : getManagedLedgers().entrySet()) {
            String ledgerName = e.getKey();
            ManagedLedger ledger = e.getValue();
            String namespace = parseNamespaceFromLedgerName(ledgerName);

            Iterator<ManagedCursor> cursorIterator = ledger.getCursors().iterator();

            while (cursorIterator.hasNext()) {
                ManagedCursor cursor = cursorIterator.next();
                ManagedCursorMXBean cStats = cursor.getStats();
                dimensionMap.clear();
                dimensionMap.put("namespace", namespace);
                dimensionMap.put("ledger_name", ledgerName);
                dimensionMap.put("cursor_name", cursor.getName());
                Metrics metrics = createMetrics(dimensionMap);
                metrics.put("brk_ml_cursor_nonContiguousDeletedMessagesRange",
                        (long) cursor.getTotalNonContiguousDeletedMessagesRange());
                metrics.put("brk_ml_cursor_persistLedgerSucceed", cStats.getPersistLedgerSucceed());
                metrics.put("brk_ml_cursor_persistLedgerErrors", cStats.getPersistLedgerErrors());
                metrics.put("brk_ml_cursor_persistZookeeperSucceed", cStats.getPersistZookeeperSucceed());
                metrics.put("brk_ml_cursor_persistZookeeperErrors", cStats.getPersistZookeeperErrors());
                metrics.put("brk_ml_cursor_writeLedgerSize", cStats.getWriteCursorLedgerSize());
                metrics.put("brk_ml_cursor_writeLedgerLogicalSize", cStats.getWriteCursorLedgerLogicalSize());
                metrics.put("brk_ml_cursor_readLedgerSize", cStats.getReadCursorLedgerSize());
                metricsCollection.add(metrics);
            }
        }
        return metricsCollection;
    }
}
