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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

public class ManagedLedgerMetrics extends AbstractMetrics {

    private List<Metrics> metricsCollection;
    private Map<Metrics, List<ManagedLedger>> ledgersByDimensionMap;
    // temp map to prepare aggregation metrics
    private Map<String, Double> tempAggregatedMetricsMap;
    private static final Buckets
            BRK_ML_ADDENTRYLATENCYBUCKETS = new Buckets("brk_ml_AddEntryLatencyBuckets",
            ENTRY_LATENCY_BUCKETS_MS);
    private static final Buckets BRK_ML_LEDGERADDENTRYLATENCYBUCKETS = new Buckets(
            "brk_ml_LedgerAddEntryLatencyBuckets", ENTRY_LATENCY_BUCKETS_MS);
    private static final Buckets BRK_ML_LEDGERSWITCHLATENCYBUCKETS = new Buckets(
            "brk_ml_LedgerSwitchLatencyBuckets", ENTRY_LATENCY_BUCKETS_MS);

    private static final Buckets
            BRK_ML_ENTRYSIZEBUCKETS = new Buckets("brk_ml_EntrySizeBuckets", ENTRY_SIZE_BUCKETS_BYTES);

    private static final PrometheusBuckets ADD_ENTRY_LATENCY =
            new PrometheusBuckets("pulsar_ml_AddEntryLatencyBuckets", ENTRY_LATENCY_BUCKETS_MS);
    private static final PrometheusBuckets LEDGER_ADD_ENTRY_LATENCY =
            new PrometheusBuckets("pulsar_ml_LedgerAddEntryLatencyBuckets", ENTRY_LATENCY_BUCKETS_MS);
    private static final PrometheusBuckets LEDGER_SWITCH_LATENCY =
            new PrometheusBuckets("pulsar_ml_LedgerSwitchLatencyBuckets", ENTRY_LATENCY_BUCKETS_MS);
    private static final PrometheusBuckets ENTRY_SIZE =
            new PrometheusBuckets("pulsar_ml_EntrySizeBuckets", ENTRY_SIZE_BUCKETS_BYTES);

    private int statsPeriodSeconds;

    public ManagedLedgerMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metricsCollection = new ArrayList<>();
        this.ledgersByDimensionMap = new HashMap<>();
        this.tempAggregatedMetricsMap = new HashMap<>();
        this.statsPeriodSeconds = pulsar.getDefaultManagedLedgerFactory().getConfig().getStatsPeriodSeconds();
    }

    @Override
    public synchronized List<Metrics> generate() {

        // get the current snapshot of ledgers by NS dimension
        return aggregate(groupLedgersByDimension());

    }

    /**
     * Writes the Prometheus metrics without constructing the legacy metrics maps.
     */
    public synchronized void generate(SimpleTextOutputStream stream) {
        Map<String, AggregatedManagedLedgerStats> statsByNamespace = new HashMap<>();
        int period = statsPeriodSeconds > 0 ? statsPeriodSeconds : 1;
        for (Entry<String, ManagedLedger> entry : getManagedLedgers().entrySet()) {
            String namespace = parseNamespaceFromLedgerName(entry.getKey());
            statsByNamespace.computeIfAbsent(namespace, __ -> new AggregatedManagedLedgerStats())
                    .add(entry.getValue().getStats(), period);
        }
        if (statsByNamespace.isEmpty()) {
            return;
        }

        String cluster = getLocalClusterName();
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_AddEntryBytesRate", s -> s.addEntryBytesRate);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_AddEntryWithReplicasBytesRate",
                s -> s.addEntryWithReplicasBytesRate);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_AddEntryErrors", s -> s.addEntryErrors);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_AddEntryMessagesRate", s -> s.addEntryMessagesRate);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_AddEntrySucceed", s -> s.addEntrySucceed);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_NumberOfMessagesInBacklog",
                s -> s.numberOfMessagesInBacklog);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_ReadEntriesBytesRate", s -> s.readEntriesBytesRate);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_ReadEntriesErrors", s -> s.readEntriesErrors);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_ReadEntriesRate", s -> s.readEntriesRate);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_ReadEntriesOpsCacheMissesRate",
                s -> s.readEntriesOpsCacheMissesRate);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_ReadEntriesSucceeded", s -> s.readEntriesSucceeded);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_StoredMessagesSize", s -> s.storedMessagesSize);
        writeMetric(stream, statsByNamespace, cluster, "pulsar_ml_MarkDeleteRate", s -> s.markDeleteRate);
        ADD_ENTRY_LATENCY.write(stream, statsByNamespace, cluster, s -> s.addEntryLatency);
        LEDGER_ADD_ENTRY_LATENCY.write(stream, statsByNamespace, cluster, s -> s.ledgerAddEntryLatency);
        LEDGER_SWITCH_LATENCY.write(stream, statsByNamespace, cluster, s -> s.ledgerSwitchLatency);
        ENTRY_SIZE.write(stream, statsByNamespace, cluster, s -> s.entrySize);
    }

    private static void writeMetric(SimpleTextOutputStream stream,
                                    Map<String, AggregatedManagedLedgerStats> statsByNamespace,
                                    String cluster, String name, ToDoubleFunction<AggregatedManagedLedgerStats> value) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        for (Entry<String, AggregatedManagedLedgerStats> entry : statsByNamespace.entrySet()) {
            stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"")
                    .write(entry.getKey()).write("\"} ")
                    .write(Double.toString(value.applyAsDouble(entry.getValue()))).write('\n');
        }
    }

    private static final class AggregatedManagedLedgerStats {
        private double addEntryBytesRate;
        private double addEntryWithReplicasBytesRate;
        private double addEntryErrors;
        private double addEntryMessagesRate;
        private double addEntrySucceed;
        private double numberOfMessagesInBacklog;
        private double readEntriesBytesRate;
        private double readEntriesErrors;
        private double readEntriesRate;
        private double readEntriesOpsCacheMissesRate;
        private double readEntriesSucceeded;
        private double storedMessagesSize;
        private double markDeleteRate;
        private final double[] addEntryLatency = new double[ENTRY_LATENCY_BUCKETS_MS.length + 1];
        private final double[] ledgerAddEntryLatency = new double[ENTRY_LATENCY_BUCKETS_MS.length + 1];
        private final double[] ledgerSwitchLatency = new double[ENTRY_LATENCY_BUCKETS_MS.length + 1];
        private final double[] entrySize = new double[ENTRY_SIZE_BUCKETS_BYTES.length + 1];

        private void add(ManagedLedgerMXBean stats, int period) {
            addEntryBytesRate += stats.getAddEntryBytesRate();
            addEntryWithReplicasBytesRate += stats.getAddEntryWithReplicasBytesRate();
            addEntryErrors += stats.getAddEntryErrors();
            addEntryMessagesRate += stats.getAddEntryMessagesRate();
            addEntrySucceed += stats.getAddEntrySucceed();
            numberOfMessagesInBacklog += stats.getNumberOfMessagesInBacklog();
            readEntriesBytesRate += stats.getReadEntriesBytesRate();
            readEntriesErrors += stats.getReadEntriesErrors();
            readEntriesRate += stats.getReadEntriesRate();
            readEntriesOpsCacheMissesRate += stats.getReadEntriesOpsCacheMissesRate();
            readEntriesSucceeded += stats.getReadEntriesSucceeded();
            storedMessagesSize += stats.getStoredMessagesSize();
            addBuckets(addEntryLatency, stats.getAddEntryLatencyBuckets(), period);
            addBuckets(ledgerAddEntryLatency, stats.getLedgerAddEntryLatencyBuckets(), period);
            addBuckets(ledgerSwitchLatency, stats.getLedgerSwitchLatencyBuckets(), period);
            addBuckets(entrySize, stats.getEntrySizeBuckets(), period);
            markDeleteRate += stats.getMarkDeleteRate();
        }

        private static void addBuckets(double[] result, long[] values, int period) {
            if (values == null) {
                return;
            }
            if (values.length != result.length) {
                throw new RuntimeException("Bucket boundary and value array length mismatch");
            }
            for (int i = 0; i < result.length; i++) {
                // Preserve the legacy rounding: divide each ledger's count before summing.
                result[i] += (double) values[i] / period;
            }
        }
    }

    private static final class PrometheusBuckets {
        private final String name;
        private final String overflowName;
        private final String[] quantiles;

        private PrometheusBuckets(String name, double[] boundaries) {
            this.name = name;
            this.overflowName = name + "_OVERFLOW";
            this.quantiles = new String[boundaries.length];
            for (int i = 0; i < boundaries.length; i++) {
                quantiles[i] = Double.toString(i == 0 ? 0.0 : boundaries[i - 1]) + "_" + boundaries[i];
            }
        }

        private void write(SimpleTextOutputStream stream,
                           Map<String, AggregatedManagedLedgerStats> statsByNamespace, String cluster,
                           Function<AggregatedManagedLedgerStats, double[]> buckets) {
            stream.write("# TYPE ").write(name).write(" gauge\n");
            for (Entry<String, AggregatedManagedLedgerStats> entry : statsByNamespace.entrySet()) {
                double[] values = buckets.apply(entry.getValue());
                for (int i = 0; i < quantiles.length; i++) {
                    stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"")
                            .write(entry.getKey()).write("\", quantile=\"").write(quantiles[i]).write("\"} ")
                            .write(Double.toString(values[i])).write('\n');
                }
            }
            stream.write("# TYPE ").write(overflowName).write(" gauge\n");
            for (Entry<String, AggregatedManagedLedgerStats> entry : statsByNamespace.entrySet()) {
                stream.write(overflowName).write("{cluster=\"").write(cluster).write("\", namespace=\"")
                        .write(entry.getKey()).write("\"} ")
                        .write(Double.toString(buckets.apply(entry.getValue())[quantiles.length])).write('\n');
            }
        }
    }

    /**
     * Aggregation by namespace (not thread-safe).
     *
     * @param ledgersByDimension
     * @return
     */
    private List<Metrics> aggregate(Map<Metrics, List<ManagedLedger>> ledgersByDimension) {

        metricsCollection.clear();

        for (Entry<Metrics, List<ManagedLedger>> e : ledgersByDimension.entrySet()) {
            Metrics metrics = e.getKey();
            List<ManagedLedger> ledgers = e.getValue();

            // prepare aggregation map
            tempAggregatedMetricsMap.clear();

            // generate the collections by each metrics and then apply the aggregation

            for (ManagedLedger ledger : ledgers) {
                ManagedLedgerMXBean lStats = ledger.getStats();

                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_AddEntryBytesRate",
                        lStats.getAddEntryBytesRate());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_AddEntryWithReplicasBytesRate",
                        lStats.getAddEntryWithReplicasBytesRate());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_AddEntryErrors",
                        (double) lStats.getAddEntryErrors());

                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_AddEntryMessagesRate",
                        lStats.getAddEntryMessagesRate());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_AddEntrySucceed",
                        (double) lStats.getAddEntrySucceed());

                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_NumberOfMessagesInBacklog",
                        (double) lStats.getNumberOfMessagesInBacklog());

                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_ReadEntriesBytesRate",
                        lStats.getReadEntriesBytesRate());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_ReadEntriesErrors",
                        (double) lStats.getReadEntriesErrors());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_ReadEntriesRate",
                        lStats.getReadEntriesRate());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_ReadEntriesOpsCacheMissesRate",
                        lStats.getReadEntriesOpsCacheMissesRate());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_ReadEntriesSucceeded",
                        (double) lStats.getReadEntriesSucceeded());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_StoredMessagesSize",
                        (double) lStats.getStoredMessagesSize());

                // handle bucket entries initialization here
                BRK_ML_ADDENTRYLATENCYBUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        lStats.getAddEntryLatencyBuckets(),
                        statsPeriodSeconds);
                BRK_ML_LEDGERADDENTRYLATENCYBUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        lStats.getLedgerAddEntryLatencyBuckets(),
                        statsPeriodSeconds);
                BRK_ML_LEDGERSWITCHLATENCYBUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        lStats.getLedgerSwitchLatencyBuckets(),
                        statsPeriodSeconds);
                BRK_ML_ENTRYSIZEBUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        lStats.getEntrySizeBuckets(),
                        statsPeriodSeconds);
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_MarkDeleteRate",
                        lStats.getMarkDeleteRate());
            }

            // SUM up collections of each metrics

            for (Entry<String, Double> ma : tempAggregatedMetricsMap.entrySet()) {

                metrics.put(ma.getKey(), ma.getValue());
            }

            metricsCollection.add(metrics);
        }

        return metricsCollection;
    }

    /**
     * Build a map of dimensions key to list of topic stats (not thread-safe).
     * <p>
     *
     * @return
     */
    private Map<Metrics, List<ManagedLedger>> groupLedgersByDimension() {

        ledgersByDimensionMap.clear();

        // get the current topics statistics from StatsBrokerFilter
        // Map : topic-name->dest-stat

        for (Entry<String, ManagedLedger> e : getManagedLedgers().entrySet()) {

            String ledgerName = e.getKey();
            ManagedLedger ledger = e.getValue();

            // we want to aggregate by NS dimension

            String namespace = parseNamespaceFromLedgerName(ledgerName);

            Metrics metrics = createMetricsByDimension(namespace);

            populateDimensionMap(ledgersByDimensionMap, metrics, ledger);
        }

        return ledgersByDimensionMap;
    }
}
