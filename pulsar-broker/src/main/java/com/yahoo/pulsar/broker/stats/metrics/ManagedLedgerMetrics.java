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

    private List<Metrics> metricsCollection;
    private Map<Metrics, List<ManagedLedgerImpl>> ledgersByDimensionMap;
    // temp map to prepare aggregation metrics
    private Map<String, Double> tempAggregatedMetricsMap;
    
    public ManagedLedgerMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metricsCollection = Lists.newArrayList();
        this.ledgersByDimensionMap = Maps.newHashMap();
        this.tempAggregatedMetricsMap = Maps.newHashMap();
    }

    @Override
    public synchronized List<Metrics> generate() {

        // get the current snapshot of ledgers by NS dimension
        return aggregate(groupLedgersByDimension());

    }

    /**
     * Aggregation by namespace (not thread-safe)
     * 
     * @param ledgersByDimension
     * @return
     */
    private List<Metrics> aggregate(Map<Metrics, List<ManagedLedgerImpl>> ledgersByDimension) {

        metricsCollection.clear();
        
        for (Entry<Metrics, List<ManagedLedgerImpl>> e : ledgersByDimension.entrySet()) {
            Metrics metrics = e.getKey();
            List<ManagedLedgerImpl> ledgers = e.getValue();

            // prepare aggregation map
            tempAggregatedMetricsMap.clear();

            // generate the collections by each metrics and then apply the aggregation

            for (ManagedLedgerImpl ledger : ledgers) {
                ManagedLedgerMXBean lStats = ledger.getStats();

                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_AddEntryBytesRate", lStats.getAddEntryBytesRate());
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
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_ReadEntriesRate", lStats.getReadEntriesRate());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_ReadEntriesSucceeded",
                        (double) lStats.getReadEntriesSucceeded());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_StoredMessagesSize",
                        (double) lStats.getStoredMessagesSize());

                // handle bucket entries initialization here
                populateBucketEntries(tempAggregatedMetricsMap, "brk_ml_AddEntryLatencyBuckets",
                        ENTRY_LATENCY_BUCKETS_MS, lStats.getAddEntryLatencyBuckets());

                populateBucketEntries(tempAggregatedMetricsMap, "brk_ml_LedgerSwitchLatencyBuckets",
                        ENTRY_LATENCY_BUCKETS_MS, lStats.getLedgerSwitchLatencyBuckets());

                populateBucketEntries(tempAggregatedMetricsMap, "brk_ml_EntrySizeBuckets", ENTRY_SIZE_BUCKETS_BYTES,
                        lStats.getEntrySizeBuckets());
                populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ml_MarkDeleteRate", lStats.getMarkDeleteRate());
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
     * Build a map of dimensions key to list of destination stats (not thread-safe)
     * <p>
     * 
     * @return
     */
    private Map<Metrics, List<ManagedLedgerImpl>> groupLedgersByDimension() {

        ledgersByDimensionMap.clear();
        
        // get the current destinations statistics from StatsBrokerFilter
        // Map : destination-name->dest-stat

        for (Entry<String, ManagedLedgerImpl> e : getManagedLedgers().entrySet()) {

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
