/**
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import org.apache.bookkeeper.mledger.impl.LedgerOffloaderMXBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.stats.Rate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

public class LedgerOffloaderMetrics extends AbstractMetrics {

    private Map<String, Double> tempAggregatedMetricsMap;
    private List<Metrics> metricsCollection;
    private LedgerOffloader ledgerOffloader;
    private LedgerOffloaderMXBean mbean;

    protected static final double[] WRITE_TO_STORAGE_BUCKETS_MS =
            new double[LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC.length];

    static {
        for (int i = 0; i < LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC.length; i++) {
            WRITE_TO_STORAGE_BUCKETS_MS[i] = LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC[i] / 1000.0;
        }
    }

    private static final Buckets WRITE_TO_STORAGE_BUCKETS =
            new Buckets("brk_ledgeroffloader_writeToStorageBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    public LedgerOffloaderMetrics(PulsarService pulsar, LedgerOffloader ledgerOffloader) {
        super(pulsar);
        this.ledgerOffloader = ledgerOffloader;
        this.metricsCollection = Lists.newArrayList();
        this.tempAggregatedMetricsMap = Maps.newHashMap();
        this.mbean = ledgerOffloader.getStats();
    }

    @Override
    public synchronized List<Metrics> generate() {
        return aggregate();
    }


    /**
     * Aggregation by namespace, ledger, cursor.
     *
     * @return List<Metrics>
     */
    private List<Metrics> aggregate() {
        metricsCollection.clear();

        Set<String> managedLedgerNames = new HashSet<>();
        managedLedgerNames.addAll(mbean.getOffloadTimes().keySet());
        managedLedgerNames.addAll(mbean.getOffloadErrors().keySet());

        managedLedgerNames.addAll(mbean.getReadOffloadRates().keySet());
        managedLedgerNames.addAll(mbean.getReadOffloadErrors().keySet());

        managedLedgerNames.addAll(mbean.getWriteToStorageLatencyBuckets().keySet());
        managedLedgerNames.addAll(mbean.getWriteToStorageRates().keySet());
        managedLedgerNames.addAll(mbean.getWriteToStorageErrors().keySet());
        managedLedgerNames.addAll(mbean.getBuildJcloundIndexLatency().keySet());
        managedLedgerNames.addAll(mbean.getBuildJcloundIndexErrors().keySet());


        for (String managedLedgerName : managedLedgerNames) {
            Map<String, String> dimensionMap = Maps.newHashMap();
            dimensionMap.put("namespace", TopicName.get(managedLedgerName).getNamespace());
            dimensionMap.put("topic", managedLedgerName);
            Metrics metrics = createMetrics(dimensionMap);

            tempAggregatedMetricsMap.clear();

            // offload time
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_offloadError",
                    (double) mbean.getOffloadErrors().getOrDefault(managedLedgerName, new LongAdder()).longValue());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_offloadTime",
                    (double) mbean.getOffloadTimes().getOrDefault(managedLedgerName, new LongAdder()).longValue());
            // offload : write to storage offload
            StatsBuckets statsBuckets = mbean.getWriteToStorageLatencyBuckets().get(managedLedgerName);
            if (statsBuckets != null) {
                WRITE_TO_STORAGE_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(),
                        ledgerOffloader.getOffloadPolicies().getRefreshStatsInterval());
            }
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_writeRate",
                    mbean.getWriteToStorageRates().getOrDefault(managedLedgerName, new Rate()).getValueRate());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_writeError",
                    (double) mbean.getWriteToStorageErrors().getOrDefault(managedLedgerName, new LongAdder()).longValue());
            StatsBuckets buildIndexBuckets = mbean.getBuildJcloundIndexLatency().get(managedLedgerName);
            if (buildIndexBuckets != null) {
                WRITE_TO_STORAGE_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        buildIndexBuckets.getBuckets(),
                        ledgerOffloader.getOffloadPolicies().getRefreshStatsInterval());
            }
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_buildIndexError",
                    (double) mbean.getBuildJcloundIndexErrors().getOrDefault(managedLedgerName, new LongAdder()).longValue());

            // readOffload
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_readOffloadError",
                    (double) mbean.getReadOffloadErrors().getOrDefault(managedLedgerName, new LongAdder()).longValue());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_readOffloadRate",
                    mbean.getReadOffloadRates().getOrDefault(managedLedgerName, new Rate()).getValueRate());

            // streaming offload
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_streamingWriteRate",
                    mbean.getStreamingWriteToStorageRates().getOrDefault(managedLedgerName, new Rate()).getValueRate());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_streamingWriteError",
                    (double) mbean.getStreamingWriteToStorageErrors().getOrDefault(managedLedgerName, new LongAdder()).longValue());

            for (Entry<String, Double> ma : tempAggregatedMetricsMap.entrySet()) {
                metrics.put(ma.getKey(), ma.getValue());
            }
            metricsCollection.add(metrics);
        }
        return metricsCollection;
    }
}
