/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.stats.metrics;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.stats.Metrics;

public class ManagedLedgerMetrics extends AbstractMetrics {

    public ManagedLedgerMetrics(PulsarService pulsar) {
        super(pulsar);
    }

    @Override
    public List<Metrics> generate() {

        // get the current snapshot of ledgers by NS dimension

        Map<Metrics, List<ManagedLedgerImpl>> ledgersByDimension = groupLedgersByDimension();

        return aggregate(ledgersByDimension);

    }

    /**
     * Aggregation by namespace
     * 
     * @param ledgersByDimension
     * @return
     */
    private List<Metrics> aggregate(Map<Metrics, List<ManagedLedgerImpl>> ledgersByDimension) {

        List<Metrics> metricsCollection = Lists.newArrayList();

        for (Entry<Metrics, List<ManagedLedgerImpl>> e : ledgersByDimension.entrySet()) {
            Metrics metrics = e.getKey();
            List<ManagedLedgerImpl> ledgers = e.getValue();

            // prepare aggregation map

            Map<String, List<Double>> aggregatedMetricsMap = Maps.newHashMap();

            // generate the collections by each metrics and then apply the aggregation

            for (ManagedLedgerImpl ledger : ledgers) {
                ManagedLedgerMXBean lStats = ledger.getStats();

                populateAggregationMap(aggregatedMetricsMap, "brk_ml_AddEntryBytesRate", lStats.getAddEntryBytesRate());
                populateAggregationMap(aggregatedMetricsMap, "brk_ml_AddEntryErrors",
                        (double) lStats.getAddEntryErrors());

                populateAggregationMap(aggregatedMetricsMap, "brk_ml_AddEntryMessagesRate",
                        lStats.getAddEntryMessagesRate());
                populateAggregationMap(aggregatedMetricsMap, "brk_ml_AddEntrySucceed",
                        (double) lStats.getAddEntrySucceed());

                populateAggregationMap(aggregatedMetricsMap, "brk_ml_NumberOfMessagesInBacklog",
                        (double) lStats.getNumberOfMessagesInBacklog());

                populateAggregationMap(aggregatedMetricsMap, "brk_ml_ReadEntriesBytesRate",
                        lStats.getReadEntriesBytesRate());
                populateAggregationMap(aggregatedMetricsMap, "brk_ml_ReadEntriesErrors",
                        (double) lStats.getReadEntriesErrors());
                populateAggregationMap(aggregatedMetricsMap, "brk_ml_ReadEntriesRate", lStats.getReadEntriesRate());
                populateAggregationMap(aggregatedMetricsMap, "brk_ml_ReadEntriesSucceeded",
                        (double) lStats.getReadEntriesSucceeded());
                populateAggregationMap(aggregatedMetricsMap, "brk_ml_StoredMessagesSize",
                        (double) lStats.getStoredMessagesSize());

                // handle bucket entries initialization here
                populateBucketEntries(aggregatedMetricsMap, "brk_ml_AddEntryLatencyBuckets", ENTRY_LATENCY_BUCKETS_MS,
                        lStats.getAddEntryLatencyBuckets());

                populateBucketEntries(aggregatedMetricsMap, "brk_ml_LedgerSwitchLatencyBuckets",
                        ENTRY_LATENCY_BUCKETS_MS, lStats.getLedgerSwitchLatencyBuckets());

                populateBucketEntries(aggregatedMetricsMap, "brk_ml_EntrySizeBuckets", ENTRY_SIZE_BUCKETS_BYTES,
                        lStats.getEntrySizeBuckets());
                populateAggregationMap(aggregatedMetricsMap, "brk_ml_MarkDeleteRate", lStats.getMarkDeleteRate());
            }

            // SUM up collections of each metrics

            for (Entry<String, List<Double>> ma : aggregatedMetricsMap.entrySet()) {

                // sum
                String metricsName = ma.getKey();
                Double metricsValue = sum(ma.getValue());

                metrics.put(metricsName, metricsValue);
            }

            metricsCollection.add(metrics);
        }

        return metricsCollection;
    }

    /**
     * Build a map of dimensions key to list of destination stats
     * <p>
     * 
     * @return
     */
    private Map<Metrics, List<ManagedLedgerImpl>> groupLedgersByDimension() {

        Map<Metrics, List<ManagedLedgerImpl>> ledgersByDimensionMap = Maps.newHashMap();

        // get the current destinations statistics from StatsBrokerFilter
        // Map : destination-name->dest-stat

        Map<String, ManagedLedgerImpl> ledgersMap = getManagedLedgers();

        for (Entry<String, ManagedLedgerImpl> e : ledgersMap.entrySet()) {

            String ledgerName = e.getKey();
            ManagedLedgerImpl ledger = e.getValue();

            // we want to aggregate by NS dimension

            String namespace = parseNamespaceFromLedgerName(ledgerName);

            Metrics metrics = createMetricsByDimension(namespace);

            populateDimensionMap(ledgersByDimensionMap, metrics, ledger);
        }

        return ledgersByDimensionMap;
    }
}
