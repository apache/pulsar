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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import org.apache.bookkeeper.mledger.impl.LedgerOffloaderMXBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.stats.Rate;

@Slf4j
public class LedgerOffloaderMetrics extends AbstractMetrics {
    private final List<Metrics> metricsCollection;
    private Map<String, Double> tempAggregatedMetricsMap;
    private boolean isTopicLevel;
    private String topicName;
    private String nameSpace;
    private int statsPeriodSeconds;

    protected static final double[] WRITE_TO_STORAGE_BUCKETS_MS =
            new double[LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC.length];

    static {
        for (int i = 0; i < LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC.length; i++) {
            WRITE_TO_STORAGE_BUCKETS_MS[i] = LedgerOffloaderMXBeanImpl.READ_ENTRY_LATENCY_BUCKETS_USEC[i] / 1000.0;
        }
    }

    private static final Buckets WRITE_TO_STORAGE_BUCKETS =
            new Buckets("brk_ledgeroffloader_writeToStorageBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    private static final Buckets READ_LEDGER_LATENCY_BUCKETS =
            new Buckets("brk_ledgeroffloader_readLedgerLatencyBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    private static final Buckets READ_OFFLOAD_INDEX_LATENCY_BUCKETS =
            new Buckets("brk_readOffload_indexLatencyBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    private static final Buckets READ_OFFLOAD_DATA_LATENCY_BUCKETS =
            new Buckets("brk_readOffload_dataLatencyBuckets", WRITE_TO_STORAGE_BUCKETS_MS);

    public LedgerOffloaderMetrics(PulsarService pulsar, boolean isTopicLevel, String nameSpace, String topicName) {
        super(pulsar);
        this.metricsCollection = Lists.newArrayList();
        this.isTopicLevel = isTopicLevel;
        this.nameSpace = nameSpace;
        this.topicName = topicName;
        this.tempAggregatedMetricsMap = Maps.newHashMap();
        statsPeriodSeconds = ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory())
                .getConfig().getStatsPeriodSeconds();
    }

    @Override
    public synchronized List<Metrics> generate() {
            if (isTopicLevel) {
                return aggregateInTopicLevel();
            } else {
                return aggregateByNameSpace();
            }
    }


    private Map<String, Double> aggregate(Map<String, Double> tempAggregatedMetricsMap, String topicName) {
        String managedLedgerName = "";
        try {
            Optional<Topic> existedTopic = pulsar.getBrokerService().getTopicIfExists(topicName).get();
            if (!existedTopic.isPresent()) {
                return tempAggregatedMetricsMap;
            }
            Topic topic = existedTopic.get();
            if (!(topic instanceof PersistentTopic)) {
                return tempAggregatedMetricsMap;
            }
            LedgerOffloader ledgerOffloader =
                    ((PersistentTopic) topic).getManagedLedger().getConfig().getLedgerOffloader();
            LedgerOffloaderMXBean mbean = ledgerOffloader.getStats();
            managedLedgerName = ((PersistentTopic) topic).getManagedLedger().getName();
            if (ledgerOffloader == NullLedgerOffloader.INSTANCE || mbean == null) {
                return tempAggregatedMetricsMap;
            }
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_offloadError",
                    (double) mbean.getOffloadErrors().getOrDefault(managedLedgerName, new Rate()).getCount());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_offloadTime",
                    mbean.getOffloadTimes().getOrDefault(managedLedgerName, new Rate()).getAverageValue());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_writeRate",
                    mbean.getOffloadRates().getOrDefault(managedLedgerName, new Rate()).getValueRate());
            StatsBuckets statsBuckets = mbean.getReadLedgerLatencyBuckets().get(managedLedgerName);
            if (statsBuckets != null) {
                READ_LEDGER_LATENCY_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(),
                        statsPeriodSeconds);
            }
            statsBuckets = mbean.getWriteToStorageLatencyBuckets().get(managedLedgerName);
            if (statsBuckets != null) {
                WRITE_TO_STORAGE_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(),
                        statsPeriodSeconds);
            }
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_writeError",
                    (double) mbean.getWriteToStorageErrors().getOrDefault(managedLedgerName, new Rate()).getCount());

            statsBuckets = mbean.getReadOffloadIndexLatencyBuckets().get(managedLedgerName);
            if (statsBuckets != null) {
                READ_OFFLOAD_INDEX_LATENCY_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(),
                        statsPeriodSeconds);
            }
            statsBuckets = mbean.getReadOffloadDataLatencyBuckets().get(managedLedgerName);
            if (statsBuckets != null) {
                READ_OFFLOAD_DATA_LATENCY_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                        statsBuckets.getBuckets(),
                        statsPeriodSeconds);
            }

            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_readOffloadError",
                    (double) mbean.getReadOffloadErrors().getOrDefault(managedLedgerName, new Rate()).getCount());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_readOffloadRate",
                    mbean.getReadOffloadRates().getOrDefault(managedLedgerName, new Rate()).getValueRate());
            // streaming offload
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_streamingWriteRate",
                    mbean.getStreamingWriteToStorageRates().getOrDefault(managedLedgerName, new Rate()).getValueRate());
            populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ledgeroffloader_streamingWriteError",
                    (double) mbean.getStreamingWriteToStorageErrors().
                            getOrDefault(managedLedgerName, new Rate()).getCount());
        } catch (Exception e) {
            log.error("aggregate ledger offload metrics for topic: {} managedLedgerName {} failed ", topicName, managedLedgerName, e);
        } finally {
            return tempAggregatedMetricsMap;
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
        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("namespace", TopicName.get(topicName).getNamespace());
        dimensionMap.put("topic", topicName);
        Metrics metrics = createMetrics(dimensionMap);
        aggregate(tempAggregatedMetricsMap, topicName);
        for (Entry<String, Double> ma : tempAggregatedMetricsMap.entrySet()) {
            metrics.put(ma.getKey(), ma.getValue());
        }
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

        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("namespace", nameSpace);

        Metrics metrics = createMetrics(dimensionMap);

        pulsar.getBrokerService().getMultiLayerTopicMap().get(nameSpace).forEach((bundle, topicsMap) -> {
            topicsMap.forEach((topicName, topic) -> {
                aggregate(tempAggregatedMetricsMap, topicName);
            });
        });

        for (Entry<String, Double> ma : tempAggregatedMetricsMap.entrySet()) {
            metrics.put(ma.getKey(), ma.getValue());
        }
        metricsCollection.add(metrics);

        return metricsCollection;
    }
}
