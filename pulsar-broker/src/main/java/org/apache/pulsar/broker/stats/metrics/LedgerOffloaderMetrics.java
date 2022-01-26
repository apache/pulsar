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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import org.apache.bookkeeper.mledger.impl.LedgerOffloaderMXBeanImpl;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;

@Slf4j
public class LedgerOffloaderMetrics extends AbstractMetrics {
    private final List<Metrics> metricsCollection;
    private Map<String, Double> tempAggregatedMetricsMap;
    private boolean isTopicLevel;
    private String topicName;
    private String namespace;
    private final double periodInSeconds;

    protected static final double[] WRITE_TO_STORAGE_BUCKETS_MS =
            new double[LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC.length];

    private static final List<String> RATE_METRICS_KEYS = new ArrayList<>(3);

    static {
        for (int i = 0; i < LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC.length; i++) {
            WRITE_TO_STORAGE_BUCKETS_MS[i] = LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC[i] / 1000.0;
        }
        RATE_METRICS_KEYS.add("brk_ledgeroffloader_writeRate");
        RATE_METRICS_KEYS.add("brk_ledgeroffloader_readOffloadRate");
        RATE_METRICS_KEYS.add("brk_ledgeroffloader_streamingWriteRate");
    }

    private static final Buckets WRITE_TO_STORAGE_BUCKETS =
            new Buckets("brk_ledgeroffloader_writeToStorageBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    private static final Buckets READ_LEDGER_LATENCY_BUCKETS =
            new Buckets("brk_ledgeroffloader_readLedgerLatencyBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    private static final Buckets READ_OFFLOAD_INDEX_LATENCY_BUCKETS =
            new Buckets("brk_readOffload_indexLatencyBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    private static final Buckets READ_OFFLOAD_DATA_LATENCY_BUCKETS =
            new Buckets("brk_readOffload_dataLatencyBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    //when the offload metrics was collected
    private static long collectedUtc = System.currentTimeMillis();

    public LedgerOffloaderMetrics(PulsarService pulsar, boolean isTopicLevel, String namespace, String topicName) {
        super(pulsar);
        this.metricsCollection = Lists.newArrayList();
        this.isTopicLevel = isTopicLevel;
        this.namespace = namespace;
        this.topicName = topicName;
        this.tempAggregatedMetricsMap = Maps.newHashMap();
        this.periodInSeconds = (System.currentTimeMillis() - collectedUtc) / 1000D;
    }

    @Override
    public synchronized List<Metrics> generate() {
        return isTopicLevel ? aggregateInTopicLevel() : aggregateByNameSpace();
    }

    private void aggregate(Map<String, Double> tempAggregatedMetricsMap, String topicName) {
        String managedLedgerName = "";
        try {
            Optional<Topic> existedTopic = pulsar.getBrokerService().getTopicIfExists(topicName).get();
            if (!existedTopic.isPresent()) {
                return;
            }
            Topic topic = existedTopic.get();
            if (!(topic instanceof PersistentTopic)) {
                return;
            }
            LedgerOffloader ledgerOffloader =
                    ((PersistentTopic) topic).getManagedLedger().getConfig().getLedgerOffloader();
            LedgerOffloaderMXBean mbean = ledgerOffloader.getStats();
            managedLedgerName = ((PersistentTopic) topic).getManagedLedger().getName();
            if (ledgerOffloader == NullLedgerOffloader.instance_ || mbean == null) {
                return;
            }

            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_offloadError",
                    mbean.getOffloadErrors(managedLedgerName));
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_offloadTime",
                    mbean.getOffloadTime(managedLedgerName));
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_writeRate",
                    mbean.getOffloadBytes(managedLedgerName));
            StatsBuckets statsBuckets = mbean.getReadLedgerLatencyBuckets(managedLedgerName);
            if (statsBuckets != null) {
                READ_LEDGER_LATENCY_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(), 1);
            }
            statsBuckets = mbean.getWriteToStorageLatencyBuckets(managedLedgerName);
            if (statsBuckets != null) {
                WRITE_TO_STORAGE_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(), 1);
            }
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_writeError",
                    mbean.getWriteToStorageErrors(managedLedgerName));

            statsBuckets = mbean.getReadOffloadIndexLatencyBuckets(managedLedgerName);
            if (statsBuckets != null) {
                READ_OFFLOAD_INDEX_LATENCY_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(), 1);
            }
            statsBuckets = mbean.getReadOffloadDataLatencyBuckets(managedLedgerName);
            if (statsBuckets != null) {
                READ_OFFLOAD_DATA_LATENCY_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(), 1);
            }
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_readOffloadError",
                    mbean.getReadOffloadErrors(managedLedgerName));
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_readOffloadRate",
                    mbean.getReadOffloadBytes(managedLedgerName));
            // streaming offload
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_streamingWriteRate",
                    mbean.getStreamingWriteToStorageBytes(managedLedgerName));
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_streamingWriteError",
                    mbean.getStreamingWriteToStorageErrors(managedLedgerName));
        } catch (Exception e) {
            log.error("aggregate ledger offload metrics for topic: {} managedLedgerName {} failed ",
                    topicName, managedLedgerName, e);
        }

    }

    /**
     * Aggregation by topic.
     *
     * @return List<Metrics>
     */
    private List<Metrics> aggregateInTopicLevel() {
        metricsCollection.clear();
        tempAggregatedMetricsMap.clear();
        Map<String, String> dimensionMap = new HashMap<>(4);
        dimensionMap.put("namespace", TopicName.get(topicName).getNamespace());
        dimensionMap.put("topic", topicName);
        Metrics metrics = createMetrics(dimensionMap);
        aggregate(tempAggregatedMetricsMap, topicName);
        this.parseMetricsValue(metrics);
        metricsCollection.add(metrics);
        return metricsCollection;
    }

    /**
     * Aggregation by namespace.
     *
     * @return List<Metrics>
     */
    private List<Metrics> aggregateByNameSpace() {
        metricsCollection.clear();
        tempAggregatedMetricsMap.clear();

        Map<String, String> dimensionMap = new HashMap<>(2);
        dimensionMap.put("namespace", namespace);

        Metrics metrics = createMetrics(dimensionMap);

        pulsar.getBrokerService().getMultiLayerTopicMap().get(namespace).forEach((bundle, topicsMap) -> {
            topicsMap.forEach((topicName, topic) -> {
                aggregate(tempAggregatedMetricsMap, topicName);
            });
        });
        this.parseMetricsValue(metrics);
        metricsCollection.add(metrics);
        return metricsCollection;
    }


    private void parseMetricsValue(Metrics metrics) {
        for (Map.Entry<String, Double> ma : tempAggregatedMetricsMap.entrySet()) {
            String metricsKey = ma.getKey();
            double value = ma.getValue();
            value = RATE_METRICS_KEYS.contains(metricsKey) ? value / this.periodInSeconds : value;
            metrics.put(metricsKey, value);
        }
    }


    public static void resetCollectedUtc() {
        collectedUtc = System.currentTimeMillis();
    }
}