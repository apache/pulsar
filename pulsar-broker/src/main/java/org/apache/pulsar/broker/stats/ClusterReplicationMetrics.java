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

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

/**
 */
public class ClusterReplicationMetrics {
    private final List<Metrics> metricsList;
    private final String localCluster;
    private final ConcurrentOpenHashMap<String, ReplicationMetrics> metricsMap;
    public static final String SEPARATOR = "_";
    public final boolean metricsEnabled;

    public ClusterReplicationMetrics(String localCluster, boolean metricsEnabled) {
        metricsList = new ArrayList<>();
        this.localCluster = localCluster;
        metricsMap = new ConcurrentOpenHashMap<>();
        this.metricsEnabled = metricsEnabled;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public ReplicationMetrics get(String namespaceCluster) {
        return metricsMap.get(namespaceCluster);
    }

    public void put(String namespaceCluster, ReplicationMetrics replicationMetrics) {
        metricsMap.put(namespaceCluster, replicationMetrics);
    }

    public String getKeyName(String namespace, String cluster) {
        return namespace + SEPARATOR + cluster;
    }

    public void remove(String namespaceCluster) {
        ReplicationMetrics replicationMetrics = metricsMap.get(namespaceCluster);
        if (replicationMetrics != null) {
            replicationMetrics.recycle();
            metricsMap.remove(namespaceCluster);
        }
    }

    public List<Metrics> get() {
        generate();
        return metricsList;
    }

    private void generate() {
        metricsMap.forEach((key, replicationMetrics) -> {
            int splitPoint = key.lastIndexOf(SEPARATOR);
            add(key.substring(0, splitPoint), key.substring(splitPoint + 1), replicationMetrics);
            replicationMetrics.reset();
        });
    }

    private void add(String namespace, String remote, ReplicationMetrics replicationMetrics) {
        metricsList.add(replicationMetrics.add(namespace, localCluster, remote));
    }

    public void reset() {
        metricsList.clear();
    }
}
