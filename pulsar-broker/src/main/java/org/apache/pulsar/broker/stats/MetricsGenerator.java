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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerCacheMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.common.stats.JvmMetrics;
import org.apache.pulsar.common.stats.Metrics;

public class MetricsGenerator {
    private final PulsarService pulsar;

    private final JvmMetrics jvmMetrics;

    public MetricsGenerator(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.jvmMetrics = JvmMetrics.create(pulsar.getExecutor(), "brk",
                pulsar.getConfiguration().getJvmGCMetricsLoggerClassName());
    }

    public Collection<Metrics> generate() {
        return merge(collect());
    }

    private List<Metrics> collect() {
        List<Metrics> metricsCollection = new ArrayList<Metrics>();

        // add the collectors here
        metricsCollection.addAll(jvmMetrics.generate());
        metricsCollection.addAll(new ManagedLedgerCacheMetrics(pulsar).generate());
        metricsCollection.addAll(new ManagedLedgerMetrics(pulsar).generate());
        metricsCollection.addAll(pulsar.getBrokerService().getTopicMetrics());
        metricsCollection.addAll(pulsar.getLoadManager().get().getLoadBalancingMetrics());

        return metricsCollection;
    }

    private Collection<Metrics> merge(List<Metrics> metricsCollection) {

        // map by dimension map -> metrics
        // since dimension map is unique
        Map<Map<String, String>, Metrics> mergedMetrics = Maps.newHashMap();

        for (Metrics metrics : metricsCollection) {
            Map<String, String> dimensionKey = metrics.getDimensions();

            if (!mergedMetrics.containsKey(dimensionKey)) {
                // create new one
                mergedMetrics.put(dimensionKey, metrics);
            } else {
                // merge all metrics to existing metrics as dimensions match
                mergedMetrics.get(dimensionKey).putAll(metrics.getMetrics());
            }
        }

        return mergedMetrics.values();
    }
}
