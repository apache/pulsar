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
package org.apache.pulsar.websocket.stats;

import static org.apache.pulsar.websocket.ProducerHandler.ENTRY_LATENCY_BUCKETS_USEC;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.websocket.WebSocketService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It periodically generates stats metrics of proxy service,
 *
 */
public class ProxyStats {

    private final WebSocketService service;
    private final JvmMetrics jvmMetrics;
    private ConcurrentOpenHashMap<String, ProxyNamespaceStats> topicStats;
    private List<Metrics> metricsCollection;
    private List<Metrics> tempMetricsCollection;

    public ProxyStats(WebSocketService service) {
        super();
        this.service = service;
        this.jvmMetrics = new JvmMetrics(service);
        this.topicStats = new ConcurrentOpenHashMap<>();
        this.metricsCollection = Lists.newArrayList();
        this.tempMetricsCollection = Lists.newArrayList();
        // schedule stat generation task every 1 minute
        service.getExecutor().scheduleAtFixedRate(() -> generate(), 120, 60, TimeUnit.SECONDS);
    }

    /**
     * generates stats-metrics of proxy service and updates metricsCollection cache with latest stats.
     */
    public synchronized void generate() {
        if (log.isDebugEnabled()) {
            log.debug("Start generating proxy metrics");
        }

        topicStats.clear();

        service.getProducers().forEach((topic, handlers) -> {
            if (log.isDebugEnabled()) {
                log.debug("Collect stats from {} producer handlers for topic {}", handlers.size(), topic);
            }

            final String namespaceName = TopicName.get(topic).getNamespace();
            ProxyNamespaceStats nsStat = topicStats.computeIfAbsent(namespaceName, ns -> new ProxyNamespaceStats());
            handlers.forEach(handler -> {
                nsStat.numberOfMsgPublished += handler.getAndResetNumMsgsSent();
                nsStat.numberOfBytesPublished += handler.getAndResetNumBytesSent();
                nsStat.numberOfPublishFailure += handler.getAndResetNumMsgsFailed();
                handler.getPublishLatencyStatsUSec().refresh();
                nsStat.publishMsgLatency.addAll(handler.getPublishLatencyStatsUSec());
            });
        });
        service.getConsumers().forEach((topic, handlers) -> {
            if (log.isDebugEnabled()) {
                log.debug("Collect stats from {} consumer handlers for topic {}", handlers.size(), topic);
            }

            final String namespaceName = TopicName.get(topic).getNamespace();
            ProxyNamespaceStats nsStat = topicStats.computeIfAbsent(namespaceName, ns -> new ProxyNamespaceStats());
            handlers.forEach(handler -> {
                nsStat.numberOfMsgDelivered += handler.getAndResetNumMsgsAcked();
                nsStat.numberOfBytesDelivered += handler.getAndResetNumBytesDelivered();
                nsStat.numberOfMsgsAcked += handler.getAndResetNumMsgsAcked();
            });
        });

        tempMetricsCollection.clear();
        topicStats.forEach((namespace, stats) -> {
            if (log.isDebugEnabled()) {
                log.debug("Add ns-stats of namespace {} to metrics", namespace);
            }
            tempMetricsCollection.add(stats.add(namespace));
        });

        // add jvm-metrics
        if (log.isDebugEnabled()) {
            log.debug("Add jvm-stats to metrics");
        }
        tempMetricsCollection.add(jvmMetrics.generate());

        // swap tempmetrics to stat-metrics
        List<Metrics> tempRef = metricsCollection;
        metricsCollection = tempMetricsCollection;
        tempMetricsCollection = tempRef;

        if (log.isDebugEnabled()) {
            log.debug("Complete generating proxy metrics");
        }
    }

    public List<Metrics> getMetrics() {
        return metricsCollection;
    }

    private static class ProxyNamespaceStats {

        public long numberOfMsgPublished;
        public long numberOfBytesPublished;
        public long numberOfPublishFailure;
        public StatsBuckets publishMsgLatency;

        public long numberOfMsgDelivered;
        public long numberOfBytesDelivered;
        public long numberOfMsgsAcked;

        public ProxyNamespaceStats() {
            this.publishMsgLatency = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
        }

        public Metrics add(String namespace) {

            publishMsgLatency.refresh();
            long[] latencyBuckets = publishMsgLatency.getBuckets();

            Map<String, String> dimensionMap = Maps.newHashMap();
            dimensionMap.put("namespace", namespace);
            Metrics dMetrics = Metrics.create(dimensionMap);
            dMetrics.put("ns_msg_publish_rate", numberOfMsgPublished);
            dMetrics.put("ns_byte_publish_rate", numberOfBytesPublished);
            dMetrics.put("ns_msg_failure_rate", numberOfPublishFailure);
            dMetrics.put("ns_msg_deliver_rate", numberOfMsgDelivered);
            dMetrics.put("ns_byte_deliver_rate", numberOfBytesDelivered);
            dMetrics.put("ns_msg_ack_rate", numberOfMsgsAcked);
            for (int i = 0; i < latencyBuckets.length; i++) {
                final String latencyBucket = i >= ENTRY_LATENCY_BUCKETS_USEC.size()
                        ? ENTRY_LATENCY_BUCKETS_USEC.get(ENTRY_LATENCY_BUCKETS_USEC.size() - 1) + "_higher"
                        : Long.toString(ENTRY_LATENCY_BUCKETS_USEC.get(i));
                dMetrics.put("ns_msg_publish_latency_" + latencyBucket, latencyBuckets[i]);
            }
            return dMetrics;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyStats.class);

}
