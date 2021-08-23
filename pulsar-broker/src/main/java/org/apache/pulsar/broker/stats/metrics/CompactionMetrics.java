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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;
import org.apache.pulsar.compaction.CompactorMXBeanImpl;

@Slf4j
public class CompactionMetrics extends AbstractMetrics {

    private static final Buckets
            BRK_COMPACTION_LATENCY_BUCKETS = new Buckets("brk_compaction_latencyBuckets",
            ENTRY_LATENCY_BUCKETS_MS);

    private List<Metrics> metricsCollection;
    private Map<String, Double> tempAggregatedMetricsMap;
    private Map<Metrics, List<String>> topicsByDimensionMap;
    private Optional<CompactorMXBean> stats;
    private int statsPeriodSeconds;

    public CompactionMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metricsCollection = Lists.newArrayList();
        this.topicsByDimensionMap = Maps.newHashMap();
        this.tempAggregatedMetricsMap = Maps.newHashMap();
        this.stats = getCompactorMXBean();
        this.statsPeriodSeconds = ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory())
                .getConfig().getStatsPeriodSeconds();
    }

    @Override
    public synchronized List<Metrics> generate() {
        return aggregate(groupTopicsByDimension());
    }


    /**
     * Aggregation by namespace.
     *
     * @return List<Metrics>
     */
    private List<Metrics> aggregate(Map<Metrics, List<String>> topicsByDimension) {
        metricsCollection.clear();
        if (stats.isPresent()) {
            CompactorMXBeanImpl compactorMXBean = (CompactorMXBeanImpl) stats.get();
            for (Map.Entry<Metrics, List<String>> entry : topicsByDimension.entrySet()) {
                Metrics metrics = entry.getKey();
                List<String> topics = entry.getValue();
                tempAggregatedMetricsMap.clear();
                for (String topic : topics) {
                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_compaction_removedEventCount",
                            compactorMXBean.getCompactionRemovedEventCount(topic));

                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_compaction_succeedCount",
                            compactorMXBean.getCompactionSucceedCount(topic));

                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_compaction_failedCount",
                            compactorMXBean.getCompactionFailedCount(topic));

                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_compaction_durationTimeInMills",
                            compactorMXBean.getCompactionDurationTimeInMills(topic));

                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_compaction_readThroughput",
                            compactorMXBean.getCompactionReadThroughput(topic));

                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_compaction_writeThroughput",
                            compactorMXBean.getCompactionWriteThroughput(topic));

                    BRK_COMPACTION_LATENCY_BUCKETS.populateBucketEntries(tempAggregatedMetricsMap,
                            compactorMXBean.getCompactionLatencyBuckets(topic),
                            statsPeriodSeconds);

                    CompletableFuture<Optional<Topic>> topicHandle = pulsar.getBrokerService().getTopicIfExists(topic);
                    Optional<Topic> topicOp = BrokerService.extractTopic(topicHandle);
                    if (topicOp.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) topicOp.get();
                        Optional<CompactedTopicImpl.CompactedTopicContext> compactedTopicContext = persistentTopic
                                .getCompactedTopicContext();
                        if (compactedTopicContext.isPresent()) {
                            LedgerHandle ledger = compactedTopicContext.get().getLedger();
                            long entries = ledger.getLastAddConfirmed() + 1;
                            long size = ledger.getLength();

                            populateAggregationMapWithSum(tempAggregatedMetricsMap,
                                    "brk_compaction_compactedEntriesCount", entries);

                            populateAggregationMapWithSum(tempAggregatedMetricsMap,
                                    "brk_compaction_compactedEntriesSize", size);
                        }
                    }
                }
                for (Map.Entry<String, Double> ma : tempAggregatedMetricsMap.entrySet()) {
                    metrics.put(ma.getKey(), ma.getValue());
                }
                metricsCollection.add(metrics);
            }
            compactorMXBean.reset();
        }

        return metricsCollection;
    }

    private Map<Metrics, List<String>> groupTopicsByDimension() {
        topicsByDimensionMap.clear();
        if (stats.isPresent()) {
            CompactorMXBeanImpl compactorMXBean = (CompactorMXBeanImpl) stats.get();
            tempAggregatedMetricsMap.clear();
            for (String topic : compactorMXBean.getTopics()) {
                String namespace = TopicName.get(topic).getNamespace();
                Metrics metrics = super.createMetricsByDimension(namespace);
                populateDimensionMap(topicsByDimensionMap, metrics, topic);
            }
        }
        return topicsByDimensionMap;
    }

    private Optional<CompactorMXBean> getCompactorMXBean() {
        Compactor compactor = null;
        try {
            compactor = pulsar.getCompactor(false);
        } catch (PulsarServerException e) {
            log.error("get compactor error", e);
        }
        return Optional.ofNullable(compactor).map(c -> c.getStats());
    }

    private void populateDimensionMap(Map<Metrics, List<String>> topicsByDimensionMap, Metrics metrics, String topic) {
        if (!topicsByDimensionMap.containsKey(metrics)) {
            topicsByDimensionMap.put(metrics, Lists.newArrayList(topic));
        } else {
            topicsByDimensionMap.get(metrics).add(topic);
        }
    }
}
