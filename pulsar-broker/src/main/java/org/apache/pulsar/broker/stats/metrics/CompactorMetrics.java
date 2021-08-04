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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;

@Slf4j
public class CompactorMetrics extends AbstractMetrics {

    private List<Metrics> metricsCollection;
    private Map<String, Double> tempAggregatedMetricsMap;
    private Map<Metrics, List<String>> topicsByDimensionMap;
    private CompactorMXBean stats;

    public CompactorMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metricsCollection = Lists.newArrayList();
        this.topicsByDimensionMap = Maps.newHashMap();
        this.tempAggregatedMetricsMap = Maps.newHashMap();
        this.stats = getCompactorMXBean();
    }

    @Override
    public synchronized List<Metrics> generate() {
        return aggregate(groupTopicsByDimension());
    }


    /**
     * Aggregation by namespace, ledger, cursor.
     *
     * @return List<Metrics>
     */
    private List<Metrics> aggregate(Map<Metrics, List<String>> topicsByDimension) {
        metricsCollection.clear();
        if (stats != null) {
            for (Map.Entry<Metrics, List<String>> e : topicsByDimension.entrySet()) {
                Metrics metrics = e.getKey();
                List<String> topics = e.getValue();
                tempAggregatedMetricsMap.clear();
                for (String topic : topics) {
                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ct_CompactedSucceed",
                            stats.getCompactTopicSucceed(topic));

                    populateAggregationMapWithSum(tempAggregatedMetricsMap, "brk_ct_CompactedError",
                            stats.getCompactTopicError(topic));
                }
                for (Map.Entry<String, Double> ma : tempAggregatedMetricsMap.entrySet()) {
                    metrics.put(ma.getKey(), ma.getValue());
                }
                metricsCollection.add(metrics);
            }
            Metrics metrics = createMetricsByDimension(pulsar.getConfiguration().getClusterName());
            metrics.put("brk_ct_CompactedRate", stats.getCompactRate());
            metrics.put("brk_ct_CompactedRate", stats.getCompactBytesRate());
            metricsCollection.add(metrics);
        }

        return metricsCollection;
    }

    protected Metrics createMetricsByDimension(String cluster) {
        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("cluster", cluster);
        return createMetrics(dimensionMap);
    }

    private Map<Metrics, List<String>> groupTopicsByDimension() {
        topicsByDimensionMap.clear();
        if (stats != null) {
            tempAggregatedMetricsMap.clear();
            for (String topic : stats.getCompactedTopics()) {
                String namespace = TopicName.get(topic).getNamespace();
                Metrics metrics = super.createMetricsByDimension(namespace);
                populateDimensionMap(topicsByDimensionMap, metrics, topic);
            }
        }
        return topicsByDimensionMap;
    }

    private CompactorMXBean getCompactorMXBean() {
        Compactor compactor = null;
        try {
            compactor = pulsar.getCompactor();
        } catch (PulsarServerException e) {
            log.error("get compactor error", e);
        }
        return Optional.ofNullable(compactor).map(c -> c.getStats()).orElse(null);
    }

    private void populateDimensionMap(Map<Metrics, List<String>> topicsByDimensionMap, Metrics metrics, String topic) {
        if (!topicsByDimensionMap.containsKey(metrics)) {
            topicsByDimensionMap.put(metrics, Lists.newArrayList(topic));
        } else {
            topicsByDimensionMap.get(metrics).add(topic);
        }
    }
}
