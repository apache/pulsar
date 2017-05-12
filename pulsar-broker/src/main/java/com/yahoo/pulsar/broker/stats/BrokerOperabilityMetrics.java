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
package com.yahoo.pulsar.broker.stats;

import com.google.common.collect.Maps;
import com.yahoo.pulsar.common.stats.Metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class BrokerOperabilityMetrics {
    private final List<Metrics> metricsList;
    private final String localCluster;
    private final TopicLoadStats topicLoadStats;
    private final String brokerName;

    public BrokerOperabilityMetrics(String localCluster, String brokerName) {
        this.metricsList = new ArrayList<>();
        this.localCluster = localCluster;
        this.topicLoadStats = new TopicLoadStats();
        this.brokerName = brokerName;
    }

    public List<Metrics> getMetrics() {
        generate();
        return metricsList;
    }

    private void generate() {
        metricsList.add(getTopicLoadMetrics());
    }

    Metrics getTopicLoadMetrics() {
        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("broker", brokerName);
        dimensionMap.put("cluster", localCluster);
        dimensionMap.put("metric", "topic_load_times");
        Metrics dMetrics = Metrics.create(dimensionMap);

        topicLoadStats.updateStats();

        dMetrics.put("brk_topic_load_time_mean_ms", topicLoadStats.meanTopicLoadMs);
        dMetrics.put("brk_topic_load_time_median_ms", topicLoadStats.medianTopicLoadMs);
        dMetrics.put("brk_topic_load_time_95percentile_ms", topicLoadStats.topicLoad95Ms);
        dMetrics.put("brk_topic_load_time_99_percentile_ms", topicLoadStats.topicLoad99Ms);
        dMetrics.put("brk_topic_load_time_99_9_percentile_ms", topicLoadStats.topicLoad999Ms);
        dMetrics.put("brk_topic_load_time_99_99_percentile_ms", topicLoadStats.topicsLoad9999Ms);
        dMetrics.put("brk_topic_load_rate_s",
                (1000 * topicLoadStats.topicLoadCounts) / topicLoadStats.elapsedIntervalMs);

        return dMetrics;
    }

    public void reset() {
        metricsList.clear();
    }

    public void recordTopicLoadTimeValue(long topicLoadLatencyMs) {
        topicLoadStats.recordTopicLoadTimeValue(topicLoadLatencyMs);
    }
}
